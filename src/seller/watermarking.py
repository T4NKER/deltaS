import hashlib
import hmac
import pandas as pd
import pyarrow as pa
import numpy as np
from datetime import datetime, timedelta
from src.utils.settings import get_settings
from src.utils.data_utils import normalize_value_for_anchor, detect_timestamp_columns
from src.seller._constants import (
    WATERMARK_LENGTH, DEFAULT_WATERMARK, MAX_ANCHOR_COLUMNS, DEFAULT_ANCHOR_COLUMNS,
    PSEUDOROW_RATIO, NOISE_BAND_FRACTION,
)

def get_watermark_secret() -> bytes:
    settings = get_settings()
    return settings.get_watermark_secret_bytes()

def detect_anchor_columns_from_schema(schema: pa.Schema, sensitive_columns: list = None) -> list:
    all_cols = [field.name for field in schema]
    sensitive_columns = sensitive_columns or []

    timestamp_like = [col for col in all_cols if 'timestamp' in col.lower() or 'time' in col.lower() or 'date' in col.lower()]
    pii_like = [col for col in all_cols if any(term in col.lower() for term in ['email', 'phone', 'isikukood', 'ik', 'personal_code', 'personal_id', 'name', 'nimi', 'address', 'aadress', 'ip', 'epost', 'telefon'])]
    volatile_like = [col for col in all_cols if any(term in col.lower() for term in ['score', 'rating', 'price', 'amount', 'balance'])]

    excluded_lower = set([col.lower() for col in timestamp_like + pii_like + volatile_like + ['_watermark_id']] + [col.lower() if isinstance(col, str) else col for col in sensitive_columns])

    id_like = [col for col in all_cols if col.lower() not in excluded_lower and any(term in col.lower() for term in ['id', 'key', 'pk', 'uuid', 'guid'])]
    categorical_like = [col for col in all_cols if col.lower() not in excluded_lower and any(term in col.lower() for term in ['type', 'status', 'category', 'code', 'country', 'region', 'state'])]

    anchor_cols = id_like[:3] + categorical_like[:2]

    if not anchor_cols:
        remaining = [col for col in all_cols if col.lower() not in excluded_lower]
        anchor_cols = remaining[:min(5, len(remaining))]

    if not anchor_cols:
        remaining_all = [col for col in all_cols if col != '_watermark_id']
        anchor_cols = remaining_all[:min(5, len(remaining_all))]

    if not anchor_cols:
        raise ValueError("No suitable anchor columns found in schema. Please specify anchor_columns explicitly.")

    return anchor_cols[:MAX_ANCHOR_COLUMNS]

def generate_watermark(seller_id: int, dataset_id: int, nonce: str = "") -> str:
    secret = get_watermark_secret()
    if not isinstance(secret, bytes):
        raise ValueError("Watermark secret must be bytes")
    message = f"seller:{seller_id}:dataset:{dataset_id}:{nonce}".encode('utf-8')
    hmac_hash = hmac.new(secret, message, hashlib.sha256).hexdigest()[:WATERMARK_LENGTH]
    return hmac_hash.ljust(WATERMARK_LENGTH, '0')

def compute_row_anchor(row: pd.Series, df_dtypes: pd.Series = None, anchor_columns: list = None) -> int:
    row_filtered = row.drop('_watermark_id') if '_watermark_id' in row.index else row

    if anchor_columns:
        available = [c for c in anchor_columns if c in row_filtered.index]
        if not available:
            raise ValueError(
                f"None of the anchor columns are available in row. "
                f"Requested: {anchor_columns}, Available: {list(row_filtered.index)}"
            )
        cols_to_use = sorted(available)
    else:
        cols_to_use = sorted(row_filtered.index)

    parts = []
    for col in cols_to_use:
        value = row_filtered[col]
        dtype = df_dtypes[col] if (df_dtypes is not None and col in df_dtypes.index) else type(value)
        parts.append(f"{col}:{normalize_value_for_anchor(value, dtype)}")

    row_str = '|'.join(parts)
    return int(hashlib.sha256(row_str.encode('utf-8')).hexdigest()[:16], 16)


def compute_row_anchors_batch(df: pd.DataFrame, anchor_columns: list) -> np.ndarray:
    cols = sorted([c for c in anchor_columns if c in df.columns])
    if not cols:
        raise ValueError(f"No anchor columns found in DataFrame. Requested: {anchor_columns}")

    parts = []
    for col in cols:
        series = df[col]
        if pd.api.types.is_integer_dtype(series):
            normalized = col + ':' + series.astype(str)
        elif pd.api.types.is_float_dtype(series):
            normalized = col + ':' + series.map(lambda v: f"{v:.10f}" if pd.notna(v) else "NULL")
        elif pd.api.types.is_datetime64_any_dtype(series):
            normalized = col + ':' + series.dt.strftime('%Y-%m-%dT%H:%M:%S.%f').fillna('NULL')
        elif pd.api.types.is_bool_dtype(series):
            normalized = col + ':' + series.map(lambda v: 'TRUE' if v else 'FALSE')
        else:
            normalized = col + ':' + series.astype(str).fillna('NULL')
        parts.append(normalized)

    keys = parts[0]
    for p in parts[1:]:
        keys = keys + '|' + p

    anchors = keys.apply(lambda s: int(hashlib.sha256(s.encode('utf-8')).hexdigest()[:16], 16))
    return anchors.values.astype(np.uint64)

def _generate_numeric_pseudorow_value(col_series: pd.Series, watermark_byte: int, i: int, byte_idx: int, watermark_bytes: list, is_integer: bool):
    if col_series.empty:
        return (watermark_byte * 100 + i) if is_integer else (watermark_byte * 100 + i) / 100.0

    min_val = col_series.min()
    max_val = col_series.max()
    mean_val = col_series.mean()
    std_val = col_series.std() if len(col_series) > 1 else abs(max_val - min_val) / 4

    base_value = mean_val + (watermark_byte - 128) * std_val / 50
    base_value = max(min_val, min(max_val, base_value))

    if is_integer:
        lsb_watermark = watermark_bytes[(i + byte_idx) % len(watermark_bytes)] % 10
        return (int(base_value) // 10) * 10 + lsb_watermark
    else:
        lsb_watermark = watermark_bytes[(i + byte_idx) % len(watermark_bytes)] % 100
        return round(base_value, 2) + (lsb_watermark / 10000.0)

def _generate_datetime_pseudorow_value(col_series: pd.Series, watermark_byte: int, i: int, watermark_seed: int, is_string: bool = False):
    if col_series.empty:
        base_date = datetime(2020, 1, 1)
        days_offset = watermark_byte * 10 + (watermark_seed % 100) + i
        result = base_date + timedelta(days=days_offset)
        return result.isoformat() if is_string else result

    min_date = col_series.min() if not is_string else pd.to_datetime(col_series).min()
    max_date = col_series.max() if not is_string else pd.to_datetime(col_series).max()
    date_range = (max_date - min_date).days if max_date != min_date else 365
    date_range = max(1, date_range)

    days_offset = (watermark_byte * 7 + watermark_seed % 100) % date_range
    result = min_date + timedelta(days=days_offset)
    return result.isoformat() if is_string else result

def _generate_string_pseudorow_value(col_series: pd.Series, watermark_byte: int, i: int, watermark_seed: int):
    if col_series.empty:
        return None

    unique_vals = col_series.unique()
    if len(unique_vals) == 0:
        return col_series.iloc[0] if not col_series.empty else None

    first_val = unique_vals[0]
    if isinstance(first_val, str) and ('T' in first_val or '-' in first_val[:10]):
        try:
            pd.to_datetime(first_val)
            return _generate_datetime_pseudorow_value(col_series, watermark_byte, i, watermark_seed, is_string=True)
        except Exception:
            pass

    idx = (watermark_byte + i) % len(unique_vals)
    return unique_vals[idx]

def generate_pseudorows(df: pd.DataFrame, watermark: str, num_pseudorows: int = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    if num_pseudorows is None:
        num_pseudorows = max(1, len(df) // PSEUDOROW_RATIO)

    watermark = (watermark or DEFAULT_WATERMARK)[:WATERMARK_LENGTH].ljust(WATERMARK_LENGTH, '0')
    watermark_seed = int(watermark[:8], 16)
    watermark_bytes = [int(watermark[i:i+2], 16) for i in range(0, WATERMARK_LENGTH, 2)]

    pseudorows = []
    for i in range(num_pseudorows):
        row = {}
        byte_idx = i % len(watermark_bytes)
        watermark_byte = watermark_bytes[byte_idx]

        for col in df.columns:
            col_series = df[col].dropna()

            if pd.api.types.is_integer_dtype(df[col]):
                row[col] = _generate_numeric_pseudorow_value(col_series, watermark_byte, i, byte_idx, watermark_bytes, is_integer=True)
            elif pd.api.types.is_float_dtype(df[col]):
                row[col] = _generate_numeric_pseudorow_value(col_series, watermark_byte, i, byte_idx, watermark_bytes, is_integer=False)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                row[col] = _generate_datetime_pseudorow_value(col_series, watermark_byte, i, watermark_seed)
            elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                row[col] = _generate_string_pseudorow_value(col_series, watermark_byte, i, watermark_seed)
            else:
                row[col] = col_series.iloc[(watermark_byte + i) % len(col_series)] if not col_series.empty else None

        pseudorows.append(row)

    if not pseudorows:
        return pd.DataFrame()

    try:
        pseudorows_df = pd.DataFrame(pseudorows)
        for col in df.columns:
            if col in pseudorows_df.columns:
                pseudorows_df[col] = pseudorows_df[col].astype(df[col].dtype, errors='ignore')
        return pseudorows_df
    except Exception as e:
        print(f"Warning: Failed to create pseudorows DataFrame: {e}")
        return pd.DataFrame()

def _prepare_anchor_columns(df: pd.DataFrame, anchor_columns: list = None) -> list:
    if anchor_columns:
        available_anchor_cols = [col for col in anchor_columns if col in df.columns]
        if available_anchor_cols:
            return available_anchor_cols

    try:
        schema = pa.Schema.from_pandas(df)
        anchor_columns = detect_anchor_columns_from_schema(schema)
        if anchor_columns:
            return anchor_columns
    except Exception as e:
        print(f"Warning: Failed to auto-detect anchor columns: {e}")

    available_cols = [col for col in df.columns if col != '_watermark_id']
    if not available_cols:
        raise ValueError("No columns available for anchor computation")

    return available_cols[:DEFAULT_ANCHOR_COLUMNS]

def _prepare_watermark_data(watermark: str) -> tuple:
    watermark = (watermark or DEFAULT_WATERMARK)[:WATERMARK_LENGTH].ljust(WATERMARK_LENGTH, '0')
    watermark_seed = int(watermark[:8], 16)
    watermark_bytes = [int(watermark[i:i+2], 16) for i in range(0, WATERMARK_LENGTH, 2)]
    return watermark, watermark_seed, watermark_bytes

def _apply_timestamp_watermark(df: pd.DataFrame, timestamp_cols: list, row_anchors: np.ndarray, watermark_bytes: list, watermark_seed: int):
    for col in timestamp_cols:
        if col not in df.columns:
            continue

        mask = df[col].notna()
        if not mask.any():
            continue

        original_ts = df.loc[mask, col]
        row_anchors_masked = row_anchors[mask.values]

        anchor_bytes = np.array([int(a) % len(watermark_bytes) for a in row_anchors_masked])
        watermark_byte_values = np.array([watermark_bytes[b] for b in anchor_bytes])

        target_upper = (watermark_byte_values.astype(np.int64) + watermark_seed % 100) % 100

        current_us = original_ts.dt.microsecond.values.astype(np.int64)
        lower_us = current_us % 10000
        new_us = target_upper * 10000 + lower_us

        base_ts = original_ts.dt.floor('s')
        watermarked_ts = base_ts + pd.to_timedelta(new_us, unit='us')
        df.loc[mask, col] = watermarked_ts

def _apply_numeric_watermark(df: pd.DataFrame, numeric_cols: list, row_anchors: np.ndarray, watermark: str, watermark_seed: int):
    watermark_hash_int = int(watermark[:8], 16)

    for col in numeric_cols:
        if col not in df.columns:
            continue
        mask = df[col].notna()
        if not mask.any():
            continue

        col_min = float(df.loc[mask, col].min())
        col_max = float(df.loc[mask, col].max())
        col_range = max(abs(col_max - col_min), 1.0)
        noise_band = col_range * NOISE_BAND_FRACTION

        row_anchors_masked = row_anchors[mask.values]
        combined_seeds = ((row_anchors_masked ^ watermark_hash_int) % (2**32)).astype(np.uint32)
        watermark_bits = np.array(
            [(int(s) ^ watermark_hash_int) & 1 for s in combined_seeds], dtype=np.float64,
        )
        directions = np.where(watermark_bits == 1, 1.0, -1.0)
        magnitudes = np.array(
            [abs(hash(int(s))) % 1000 / 1000.0 for s in combined_seeds], dtype=np.float64,
        )
        perturbations = directions * magnitudes * noise_band

        original_values = df.loc[mask, col].astype(np.float64).values
        watermarked_values = original_values + perturbations
        watermarked_values = np.clip(watermarked_values, col_min, col_max)

        if pd.api.types.is_integer_dtype(df[col]):
            df.loc[mask, col] = np.round(watermarked_values).astype(df[col].dtype)
        else:
            df.loc[mask, col] = watermarked_values

def _apply_categorical_watermark(df: pd.DataFrame, categorical_cols: list, watermark: str):
    if not categorical_cols:
        return

    grouping_col = categorical_cols[0]
    watermark_hash_int = int(watermark[:8], 16)

    for group_val in df[grouping_col].unique():
        if pd.isna(group_val):
            continue

        group_mask = df[grouping_col] == group_val
        group_indices = df[group_mask].index.tolist()

        if len(group_indices) < 2:
            continue

        group_hash = hash(str(group_val)) % (2**32)
        fingerprint_bit = (watermark_hash_int ^ group_hash) & 1

        group_df = df.loc[group_indices].copy()
        group_df = group_df.sort_index(ascending=(fingerprint_bit == 0))
        df.loc[group_indices] = group_df.values

def apply_watermark_to_dataframe(df: pd.DataFrame, watermark: str, is_trial: bool = False, anchor_columns: list = None) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    anchor_columns = _prepare_anchor_columns(df, anchor_columns)
    watermark, watermark_seed, watermark_bytes = _prepare_watermark_data(watermark)

    df_for_anchor = df.drop(columns=['_watermark_id']) if '_watermark_id' in df.columns else df
    row_anchors = compute_row_anchors_batch(df_for_anchor, anchor_columns)

    timestamp_cols = detect_timestamp_columns(df)

    if is_trial:
        df['_watermark_id'] = (row_anchors % 1000000).astype(np.int64)

    for col in timestamp_cols:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col])

    _apply_timestamp_watermark(df, timestamp_cols, row_anchors, watermark_bytes, watermark_seed)

    numeric_cols = [col for col in df.columns
                   if col != '_watermark_id'
                   and col not in timestamp_cols
                   and (pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_float_dtype(df[col]))]

    categorical_cols = [col for col in df.columns
                       if col != '_watermark_id'
                       and col not in timestamp_cols
                       and col not in numeric_cols
                       and (pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_categorical_dtype(df[col]))]

    _apply_categorical_watermark(df, categorical_cols, watermark)

    pseudorows_df = generate_pseudorows(df, watermark, num_pseudorows=max(1, len(df) // PSEUDOROW_RATIO))
    if not pseudorows_df.empty:
        df = pd.concat([df, pseudorows_df], ignore_index=True)

    return df
