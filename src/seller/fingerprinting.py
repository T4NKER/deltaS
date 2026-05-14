import hashlib
import hmac
import pandas as pd
import numpy as np
from typing import List, Dict
from src.seller.watermarking import compute_row_anchors_batch
from src.utils.settings import get_settings
from src.seller._constants import (
    WATERMARK_BYTE_MULTIPLIER, WATERMARK_SEED_MOD, MICROSECONDS_PER_SECOND,
    FLOAT_PRECISION_PLACE, DEFAULT_VERIFICATION_TOLERANCE,
    MIN_FINGERPRINT_CARDINALITY,
)

def _get_fingerprint_secret() -> bytes:
    settings = get_settings()
    base_secret = settings.get_watermark_secret_bytes()
    return hmac.new(base_secret, b"domain:fingerprint", hashlib.sha256).digest()

def generate_buyer_fingerprint(buyer_id: int, share_id: int) -> Dict:
    secret = _get_fingerprint_secret()
    if not isinstance(secret, bytes):
        raise ValueError("Fingerprint secret must be bytes")

    base_message = f"{buyer_id}:{share_id}".encode('utf-8')
    base_hmac = hmac.new(secret, base_message, hashlib.sha256).hexdigest()
    component_message = f"{base_message}:numeric".encode('utf-8')
    component_hash = hmac.new(secret, component_message, hashlib.sha256).hexdigest()

    return {
        "buyer_id": buyer_id,
        "share_id": share_id,
        "base_hash": (base_hmac[:32] or "0" * 32).ljust(32, '0'),
        "component_hash": component_hash[:16],
    }

def apply_fingerprint_to_dataframe(
    df: pd.DataFrame,
    fingerprint: Dict,
    anchor_columns: List[str],
    is_trial: bool = False
) -> pd.DataFrame:
    df = df.copy()

    if df.empty:
        return df

    available_anchor_cols = [col for col in anchor_columns if col in df.columns]
    if not available_anchor_cols:
        raise ValueError(f"None of the anchor columns are available. Requested: {anchor_columns}, Available: {list(df.columns)}")

    anchor_set = set(available_anchor_cols)

    all_anchors = compute_row_anchors_batch(df, available_anchor_cols)
    row_anchors = [int(a) for a in all_anchors]

    if is_trial:
        base_hash = fingerprint["base_hash"]
        df['_watermark_id'] = [
            int(hashlib.sha256(f"{base_hash}:{ra}".encode('utf-8')).hexdigest()[:8], 16)
            for ra in row_anchors
        ]

    timestamp_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

    if timestamp_cols:
        base_hash_int = int(fingerprint["base_hash"][:8], 16)
        base_hash_str = (fingerprint["base_hash"] or "0" * 32)[:32].ljust(32, '0')
        watermark_bytes = [int(base_hash_str[i:i+2], 16) for i in range(0, 8, 2)]
        num_wb = len(watermark_bytes)

        for col in timestamp_cols:
            ts_values = df[col].values
            new_ts = []
            for i, ts in enumerate(ts_values):
                if pd.isna(ts):
                    new_ts.append(ts)
                    continue
                ra = row_anchors[i]
                combined_seed = (base_hash_int + ra) % (2**32)
                wb = watermark_bytes[ra % num_wb]
                target_us = (wb * WATERMARK_BYTE_MULTIPLIER + combined_seed % WATERMARK_SEED_MOD) % MICROSECONDS_PER_SECOND
                ts_pd = pd.Timestamp(ts)
                current_us = ts_pd.microsecond
                new_us = (current_us // 10000) * 10000 + (target_us % 10000)
                new_ts.append(ts_pd.replace(microsecond=new_us))
            df[col] = new_ts

    component_hash_int = int(fingerprint["component_hash"][:8], 16)

    for col in df.columns:
        if col in ['_watermark_id'] or col in timestamp_cols:
            continue
        if col in anchor_set:
            continue

        if pd.api.types.is_numeric_dtype(df[col]) and df[col].nunique() >= MIN_FINGERPRINT_CARDINALITY:
            df[col] = _embed_fingerprint_in_numeric_batch(df[col], component_hash_int, row_anchors)

    return df

def _embed_fingerprint_in_numeric_batch(
    series: pd.Series,
    component_hash_int: int,
    row_anchors: List[int],
) -> pd.Series:
    values = series.values.copy()
    n = len(values)

    if pd.api.types.is_integer_dtype(series):
        for i in range(n):
            slot_value = (row_anchors[i] ^ component_hash_int) % 10
            values[i] = (int(values[i]) // 10) * 10 + slot_value
    elif pd.api.types.is_float_dtype(series):
        for i in range(n):
            original_val = float(values[i])
            original_str = f"{original_val:.10f}"
            parts = original_str.split('.')
            if len(parts[1]) > FLOAT_PRECISION_PLACE:
                decimal_part = parts[1]
                bit_value = (component_hash_int >> (i % 32)) & 1
                new_decimal = decimal_part[:FLOAT_PRECISION_PLACE] + str(bit_value) + decimal_part[FLOAT_PRECISION_PLACE+1:]
                values[i] = float(parts[0] + '.' + new_decimal)

    return pd.Series(values, index=series.index, name=series.name)

def verify_fingerprint(
    df: pd.DataFrame,
    fingerprint: Dict,
    anchor_columns: List[str],
    tolerance: float = DEFAULT_VERIFICATION_TOLERANCE
) -> Dict:
    if df.empty:
        return {"found": False, "reason": "DataFrame is empty"}

    available_anchor_cols = [col for col in anchor_columns if col in df.columns]
    if not available_anchor_cols:
        return {"found": False, "reason": f"Anchor columns missing: {anchor_columns}"}

    results = {
        "found": False,
        "fingerprint": fingerprint,
        "overall_match_rate": 0.0
    }

    timestamp_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

    all_anchors = compute_row_anchors_batch(df, available_anchor_cols)

    if timestamp_cols:
        timestamp_matches = 0
        timestamp_checked = 0

        for i, (idx, row) in enumerate(df.iterrows()):
            timestamp_checked += 1
            row_anchor = int(all_anchors[i])

            base_hash_int = int(fingerprint["base_hash"][:8], 16)
            combined_seed = (base_hash_int + row_anchor) % (2**32)

            base_hash = (fingerprint["base_hash"] or "0" * 32)[:32].ljust(32, '0')
            watermark_bytes = [int(base_hash[i:i+2], 16) for i in range(0, 8, 2)]
            watermark_byte = watermark_bytes[row_anchor % len(watermark_bytes)]
            expected_microseconds = (watermark_byte * WATERMARK_BYTE_MULTIPLIER + combined_seed % WATERMARK_SEED_MOD) % 10000

            for col in timestamp_cols:
                if pd.notna(row[col]):
                    actual_microseconds = row[col].microsecond % 10000
                    diff = abs(actual_microseconds - expected_microseconds)
                    if diff < 100 or abs(diff - 10000) < 100:
                        timestamp_matches += 1
                        break

        timestamp_match_rate = timestamp_matches / timestamp_checked if timestamp_checked > 0 else 0.0
        results["timestamp"] = {
            "found": timestamp_match_rate >= tolerance,
            "matches": timestamp_matches,
            "checked": timestamp_checked,
            "match_rate": timestamp_match_rate
        }

    if '_watermark_id' in df.columns:
        watermark_id_matches = 0
        watermark_id_checked = 0

        for i, (idx, row) in enumerate(df.iterrows()):
            watermark_id_checked += 1
            row_anchor = int(all_anchors[i])

            base_hash = fingerprint["base_hash"]
            combined = f"{base_hash}:{row_anchor}".encode('utf-8')
            expected_watermark_id = int(hashlib.sha256(combined).hexdigest()[:8], 16)

            if row['_watermark_id'] == expected_watermark_id:
                watermark_id_matches += 1

        watermark_id_match_rate = watermark_id_matches / watermark_id_checked if watermark_id_checked > 0 else 0.0
        results["watermark_column"] = {
            "found": watermark_id_match_rate >= 0.5,
            "matches": watermark_id_matches,
            "checked": watermark_id_checked,
            "match_rate": watermark_id_match_rate
        }

    MIN_CARDINALITY = 10
    anchor_set = set(available_anchor_cols)
    row_anchors = [int(a) for a in all_anchors]
    numeric_cols = [
        col for col in df.columns
        if col not in anchor_set
        and col not in timestamp_cols
        and col != '_watermark_id'
        and pd.api.types.is_numeric_dtype(df[col])
        and df[col].nunique() >= MIN_CARDINALITY
    ]

    if numeric_cols and fingerprint.get("component_hash"):
        total_checks = 0
        total_matches = 0

        component_hash_int = int(fingerprint["component_hash"][:8], 16)

        for col in numeric_cols:
            series = df[col]
            values = series.values
            is_int = pd.api.types.is_integer_dtype(series)
            is_float = pd.api.types.is_float_dtype(series)

            if not (is_int or is_float):
                continue

            for i in range(len(values)):
                val = values[i]
                if pd.isna(val):
                    continue

                if is_int:
                    expected_digit = (row_anchors[i] ^ component_hash_int) % 10
                    actual_digit = int(val) % 10
                    total_checks += 1
                    if actual_digit == expected_digit:
                        total_matches += 1
                elif is_float:
                    val_str = f"{float(val):.10f}"
                    parts = val_str.split('.')
                    if len(parts[1]) > FLOAT_PRECISION_PLACE:
                        expected_bit = (component_hash_int >> (i % 32)) & 1
                        actual_bit = int(parts[1][FLOAT_PRECISION_PLACE])
                        total_checks += 1
                        if actual_bit == expected_bit:
                            total_matches += 1

        numeric_match_rate = total_matches / total_checks if total_checks > 0 else 0.0
        results["numeric"] = {
            "found": numeric_match_rate >= tolerance,
            "matches": total_matches,
            "checked": total_checks,
            "match_rate": numeric_match_rate
        }

    overall_match_rate = 0.0
    match_count = 0

    if "timestamp" in results:
        overall_match_rate += results["timestamp"]["match_rate"]
        match_count += 1

    if "watermark_column" in results:
        overall_match_rate += results["watermark_column"]["match_rate"]
        match_count += 1

    if "numeric" in results:
        overall_match_rate += results["numeric"]["match_rate"]
        match_count += 1

    if match_count > 0:
        overall_match_rate /= match_count

    results["overall_match_rate"] = overall_match_rate
    results["found"] = overall_match_rate >= tolerance

    return results

def evaluate_robustness(
    original_df: pd.DataFrame,
    fingerprint: Dict,
    anchor_columns: List[str],
    attacks: List[str] = None,
    tolerance: float = DEFAULT_VERIFICATION_TOLERANCE,
) -> Dict:
    if attacks is None:
        attacks = [
            "row_deletion", "projection", "categorical_projection",
            "perturbation", "value_filter", "timestamp_rounding",
            "column_aggregation", "numeric_noise",
        ]

    results = {
        "fingerprint": fingerprint,
        "attacks": {}
    }

    for attack in attacks:
        if attack == "row_deletion":
            attacked_df = original_df.sample(frac=0.8, random_state=42)
        elif attack == "projection":
            cols_to_keep = anchor_columns + [col for col in original_df.columns if pd.api.types.is_datetime64_any_dtype(original_df[col])]
            attacked_df = original_df[cols_to_keep]
        elif attack == "perturbation":
            attacked_df = original_df.copy()
            for col in attacked_df.columns:
                if pd.api.types.is_numeric_dtype(attacked_df[col]):
                    noise = np.random.normal(0, attacked_df[col].std() * 0.01, len(attacked_df))
                    attacked_df[col] = attacked_df[col] + noise
        elif attack == "value_filter":
            attacked_df = original_df.copy()
            numeric_cols = [col for col in attacked_df.columns if pd.api.types.is_numeric_dtype(attacked_df[col])]
            if numeric_cols:
                filter_col = numeric_cols[0]
                median_val = attacked_df[filter_col].median()
                attacked_df = attacked_df[attacked_df[filter_col] >= median_val]
            if attacked_df.empty:
                results["attacks"][attack] = {"survived": False, "match_rate": 0.0, "reason": "Filter removed all rows"}
                continue
        elif attack == "timestamp_rounding":
            attacked_df = original_df.copy()
            for col in attacked_df.columns:
                if pd.api.types.is_datetime64_any_dtype(attacked_df[col]):
                    attacked_df[col] = attacked_df[col].dt.floor('s')
        elif attack == "column_aggregation":
            attacked_df = original_df.copy()
            numeric_cols = [col for col in attacked_df.columns if pd.api.types.is_numeric_dtype(attacked_df[col]) and col not in anchor_columns]
            if len(numeric_cols) >= 2:
                attacked_df[numeric_cols[0]] = attacked_df[numeric_cols[0]] + attacked_df[numeric_cols[1]]
                attacked_df = attacked_df.drop(columns=[numeric_cols[1]])
        elif attack == "numeric_noise":
            attacked_df = original_df.copy()
            for col in attacked_df.columns:
                if pd.api.types.is_numeric_dtype(attacked_df[col]):
                    noise = np.random.normal(0, attacked_df[col].std() * 0.05, len(attacked_df))
                    attacked_df[col] = attacked_df[col] + noise
        elif attack == "categorical_projection":
            cols_to_keep = list(anchor_columns)
            for col in original_df.columns:
                if col in anchor_columns:
                    continue
                if pd.api.types.is_datetime64_any_dtype(original_df[col]):
                    cols_to_keep.append(col)
                elif pd.api.types.is_object_dtype(original_df[col]) or pd.api.types.is_string_dtype(original_df[col]):
                    cols_to_keep.append(col)
                elif pd.api.types.is_numeric_dtype(original_df[col]) and original_df[col].nunique() < 10:
                    cols_to_keep.append(col)
            attacked_df = original_df[cols_to_keep]
        else:
            continue

        verification = verify_fingerprint(
            attacked_df,
            fingerprint,
            anchor_columns,
            tolerance=tolerance,
        )
        results["attacks"][attack] = {
            "survived": verification["found"],
            "match_rate": verification["overall_match_rate"]
        }

    return results

def evaluate_collusion(
    original_df: pd.DataFrame,
    anchor_columns: List[str],
    buyer_ids: List[int] = None,
    share_ids: List[int] = None,
    tolerance: float = DEFAULT_VERIFICATION_TOLERANCE,
) -> Dict:
    if buyer_ids is None:
        buyer_ids = [1, 2, 3]
    if share_ids is None:
        share_ids = list(buyer_ids)
    if len(buyer_ids) != len(share_ids):
        raise ValueError("buyer_ids and share_ids must be the same length")

    fingerprints = [
        generate_buyer_fingerprint(buyer_id=b, share_id=s)
        for b, s in zip(buyer_ids, share_ids)
    ]
    fingerprinted_dfs = [
        apply_fingerprint_to_dataframe(original_df.copy(), fp, anchor_columns)
        for fp in fingerprints
    ]

    common = [c for c in fingerprinted_dfs[0].columns if all(c in d.columns for d in fingerprinted_dfs[1:])]
    numeric_common = [c for c in common if pd.api.types.is_numeric_dtype(fingerprinted_dfs[0][c])]

    colluded = fingerprinted_dfs[0][common].copy()
    for col in numeric_common:
        stacked = np.stack([d[col].values for d in fingerprinted_dfs], axis=0)
        colluded[col] = np.mean(stacked, axis=0)

    per_buyer = []
    max_rate = 0.0
    for b, s, fp in zip(buyer_ids, share_ids, fingerprints):
        r = verify_fingerprint(
            colluded,
            fp,
            anchor_columns,
            tolerance=tolerance,
        )
        rate = float(r.get("overall_match_rate", 0.0))
        per_buyer.append({
            "buyer_id": b,
            "share_id": s,
            "found": bool(r.get("found", False)),
            "match_rate": rate,
        })
        max_rate = max(max_rate, rate)

    return {
        "n_colluders": len(buyer_ids),
        "per_buyer": per_buyer,
        "max_match_rate": max_rate,
    }

def compute_false_positive_rate(
    anchor_columns: List[str],
    num_trials: int = 1000,
    num_rows: int = 50
) -> Dict:
    false_positives = 0
    total_trials = num_trials

    for trial in range(num_trials):
        np.random.seed(trial)
        random_data = {}
        for col in anchor_columns:
            random_data[col] = np.random.randint(0, 10000, num_rows)
        random_data['timestamp_col'] = pd.date_range('2020-01-01', periods=num_rows, freq='h')
        random_data['value_col'] = np.random.randn(num_rows) * 100

        random_df = pd.DataFrame(random_data)

        fake_fingerprint = generate_buyer_fingerprint(buyer_id=trial + 9999, share_id=trial + 9999)
        result = verify_fingerprint(random_df, fake_fingerprint, anchor_columns, tolerance=DEFAULT_VERIFICATION_TOLERANCE)

        if result["found"]:
            false_positives += 1

    fpr = false_positives / total_trials if total_trials > 0 else 0.0

    return {
        "false_positive_rate": fpr,
        "false_positives": false_positives,
        "total_trials": total_trials,
    }
