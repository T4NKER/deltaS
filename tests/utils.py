import requests
import pandas as pd
import hashlib
import numpy as np
import base64
from src.seller.watermarking import generate_watermark, compute_row_anchor
from src.utils.encryption import generate_key_pair, decrypt_token as _decrypt_token
from src.utils.metadata_signing import get_seller_metadata_signing_public_key_pem

def decrypt_token(encrypted_token: str, private_key_b64: str) -> str:
    return _decrypt_token(encrypted_token, private_key_b64)

def decrypt_marketplace_profile(profile_data: dict, private_key_b64: str) -> tuple[dict, str]:
    assert "encryptedBearerToken" in profile_data, "Marketplace profile must contain encryptedBearerToken"
    assert "bearerToken" not in profile_data, "Marketplace must not expose plaintext bearerToken"

    decrypted_token = decrypt_token(profile_data["encryptedBearerToken"], private_key_b64)
    standard_profile = dict(profile_data)
    del standard_profile["encryptedBearerToken"]
    standard_profile["bearerToken"] = decrypted_token
    return standard_profile, decrypted_token

def detect_timestamp_columns(df: pd.DataFrame) -> list:
    timestamp_cols = []
    excluded_patterns = ['id', 'key', 'uuid', 'guid', 'hash', 'token', 'code', 'ref']
    time_like_patterns = ['timestamp', 'time', 'date', 'ts', 'at', 'created', 'updated', 'modified', 'event']

    for col in df.columns:
        if col == '_watermark_id':
            continue

        col_lower = col.lower()
        if any(pattern in col_lower for pattern in excluded_patterns):
            continue

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            timestamp_cols.append(col)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            if any(pattern in col_lower for pattern in time_like_patterns):
                try:
                    sample_vals = df[col].dropna().head(10)
                    if len(sample_vals) > 0:
                        parse_success = 0
                        for val in sample_vals:
                            if isinstance(val, str):
                                if 'T' in val or ('-' in val[:10] and len(val) > 10):
                                    try:
                                        pd.to_datetime(val)
                                        parse_success += 1
                                    except:
                                        pass
                        if parse_success >= len(sample_vals) * 0.9:
                            timestamp_cols.append(col)
                except:
                    pass
    return timestamp_cols

def check_watermark(df: pd.DataFrame, seller_id: int, dataset_id: int, verbose: bool = True, anchor_columns: list = None) -> dict:
    expected_watermark = generate_watermark(seller_id, dataset_id)
    watermark_bytes = [int(expected_watermark[i:i+2], 16) for i in range(0, min(16, len(expected_watermark)), 2)]
    watermark_seed = int(expected_watermark[:8], 16)

    has_watermark_column = '_watermark_id' in df.columns
    timestamp_cols = detect_timestamp_columns(df)

    numeric_cols = [col for col in df.columns
                   if col != '_watermark_id'
                   and col not in timestamp_cols
                   and (pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_float_dtype(df[col]))]

    if not timestamp_cols and not has_watermark_column and not numeric_cols:
        return {
            "found": False,
            "reason": "No timestamp columns, watermark column, or numeric columns found",
            "watermark": expected_watermark,
            "timestamp_cols": [],
            "has_watermark_column": False,
            "numeric_cols": []
        }

    df_for_anchor = df.drop(columns=['_watermark_id']) if '_watermark_id' in df.columns else df

    anchor_columns_missing = False
    if anchor_columns:
        available_anchor_cols = [col for col in anchor_columns if col in df_for_anchor.columns]
        if not available_anchor_cols:
            anchor_columns_missing = True
            if verbose:
                missing = [col for col in anchor_columns if col not in df_for_anchor.columns]
                print(f"  [WARN] WARNING: None of the anchor columns are available in DataFrame: {missing}")
                print(f"     Available columns: {list(df_for_anchor.columns)}")
                print(f"     Cannot verify watermark without anchor columns.")
            anchor_columns = None
        elif len(available_anchor_cols) != len(anchor_columns):
            if verbose:
                missing = [col for col in anchor_columns if col not in df_for_anchor.columns]
                print(f"  Note: Some anchor columns missing from DataFrame: {missing}. Using available: {available_anchor_cols}")
            anchor_columns = available_anchor_cols
    else:
        if verbose:
            print(f"  Note: No anchor columns provided. Will use all available columns for row anchoring.")

    watermark_column_matches = 0
    watermark_column_checked = 0
    watermark_column_samples = []

    timestamp_matches = 0
    timestamp_checked = 0
    timestamp_samples = []

    numeric_matches = 0
    numeric_checked = 0
    numeric_samples = []

    row_count = len(df)
    MIN_SAMPLE_SIZE = min(20, max(5, row_count // 4))
    MIN_MATCH_COUNT = max(3, MIN_SAMPLE_SIZE // 4)
    WATERMARK_COL_THRESHOLD = 0.5
    TIMESTAMP_THRESHOLD = 0.15

    if has_watermark_column:
        for idx, row in df.iterrows():
            try:
                watermark_id = row['_watermark_id']
                if pd.isna(watermark_id):
                    continue

                row_for_anchor = row.drop('_watermark_id')
                row_anchor = compute_row_anchor(row_for_anchor, df_for_anchor.dtypes, anchor_columns)
                expected_watermark_id = row_anchor % 1000000

                watermark_column_checked += 1
                diff = abs(watermark_id - expected_watermark_id)
                if diff < 10:
                    watermark_column_matches += 1
                    if verbose and watermark_column_checked <= 5:
                        watermark_column_samples.append(f"Row {idx}, _watermark_id: {watermark_id} (expected: {expected_watermark_id}, diff: {diff})")
            except Exception as e:
                if verbose and watermark_column_checked < 3:
                    watermark_column_samples.append(f"Row {idx}, _watermark_id: Error - {e}")

    rows_checked = set()

    for idx, row in df.iterrows():
        if idx in rows_checked:
            continue

        row_for_anchor = row.drop('_watermark_id') if '_watermark_id' in row.index else row
        try:
            row_anchor = compute_row_anchor(row_for_anchor, df_for_anchor.dtypes, anchor_columns)
            anchor_byte_idx = int(row_anchor % len(watermark_bytes))
            expected_byte = watermark_bytes[anchor_byte_idx]
            expected_upper = (expected_byte + watermark_seed % 100) % 100
        except Exception as e:
            if verbose and len(rows_checked) < 3:
                timestamp_samples.append(f"Row {idx}: Error computing row anchor - {e}")
            continue

        row_matched = False
        row_checked = False
        checked_cols = []

        for col in timestamp_cols:
            try:
                ts = pd.to_datetime(row[col])
                if pd.isna(ts):
                    continue

                microseconds = ts.microsecond
                actual_upper = microseconds // 10000
                checked_cols.append(col)

                diff = abs(actual_upper - expected_upper)
                if diff <= 1 or diff >= 99:
                    row_matched = True
                    if verbose and timestamp_matches < 5:
                        timestamp_samples.append(f"Row {idx}, {col}: upper2={actual_upper} (expected: {expected_upper}, diff: {diff})")
                    break
            except Exception as e:
                if verbose and len(rows_checked) < 3:
                    timestamp_samples.append(f"Row {idx}, {col}: Error - {e}")
                pass

        if checked_cols:
            timestamp_checked += 1
            rows_checked.add(idx)
            if row_matched:
                timestamp_matches += 1
            elif verbose and timestamp_checked <= 5:
                first_col = checked_cols[0]
                ts = pd.to_datetime(row[first_col])
                actual_upper = ts.microsecond // 10000
                timestamp_samples.append(f"Row {idx}, {first_col}: upper2={actual_upper} (expected: {expected_upper}, diff: {abs(actual_upper - expected_upper)}) [MISMATCH]")

    watermark_hash_int = int(expected_watermark[:8], 16)

    _LEGACY_NUMERIC_CHECK_DISABLED = True

    for idx, row in df.iterrows():
        if _LEGACY_NUMERIC_CHECK_DISABLED:
            break
        if idx in rows_checked:
            continue

        row_for_anchor = row.drop('_watermark_id') if '_watermark_id' in row.index else row
        try:
            row_anchor = compute_row_anchor(row_for_anchor, df_for_anchor.dtypes, anchor_columns)
        except Exception as e:
            if verbose and numeric_checked < 3:
                numeric_samples.append(f"Row {idx}: Error computing row anchor - {e}")
            continue

        row_matched = False
        checked_numeric_cols = []

        for col in numeric_cols:
            try:
                if col not in row.index or pd.isna(row[col]):
                    continue

                checked_numeric_cols.append(col)

                if pd.api.types.is_integer_dtype(df[col]):
                    expected_slot = (row_anchor ^ watermark_hash_int) % 10
                    actual_value = int(row[col])
                    actual_slot = actual_value % 10

                    if actual_slot == expected_slot:
                        row_matched = True
                        if verbose and numeric_matches < 5:
                            numeric_samples.append(f"Row {idx}, {col}: LSB={actual_slot} (expected: {expected_slot})")
                        break
                elif pd.api.types.is_float_dtype(df[col]):
                    original_val = float(row[col])
                    original_str = f"{original_val:.10f}"
                    if '.' in original_str:
                        parts = original_str.split('.')
                        if len(parts[1]) > 5:
                            decimal_part = parts[1]
                            bit_at_5th = int(decimal_part[5]) if len(decimal_part) > 5 else None
                            expected_bit = (watermark_hash_int >> (idx % 32)) & 1

                            if bit_at_5th is not None and bit_at_5th == expected_bit:
                                row_matched = True
                                if verbose and numeric_matches < 5:
                                    numeric_samples.append(f"Row {idx}, {col}: 5th decimal bit={bit_at_5th} (expected: {expected_bit})")
                                break
            except Exception as e:
                if verbose and numeric_checked < 3:
                    numeric_samples.append(f"Row {idx}, {col}: Error - {e}")
                pass

        if checked_numeric_cols:
            numeric_checked += 1
            rows_checked.add(idx)
            if row_matched:
                numeric_matches += 1
            elif verbose and numeric_checked <= 5:
                first_col = checked_numeric_cols[0]
                numeric_samples.append(f"Row {idx}, {first_col}: [MISMATCH]")

    watermark_column_rate = (watermark_column_matches / watermark_column_checked * 100) if watermark_column_checked > 0 else 0.0
    timestamp_rate = (timestamp_matches / timestamp_checked * 100) if timestamp_checked > 0 else 0.0
    numeric_rate = (numeric_matches / numeric_checked * 100) if numeric_checked > 0 else 0.0

    watermark_column_found = (
        watermark_column_checked >= MIN_SAMPLE_SIZE and
        watermark_column_matches >= MIN_MATCH_COUNT and
        watermark_column_rate >= WATERMARK_COL_THRESHOLD * 100
    )

    timestamp_found = (
        timestamp_checked >= MIN_SAMPLE_SIZE and
        timestamp_matches >= MIN_MATCH_COUNT and
        timestamp_rate >= TIMESTAMP_THRESHOLD * 100
    )

    NUMERIC_THRESHOLD = 0.15
    numeric_found = (
        numeric_checked >= MIN_SAMPLE_SIZE and
        numeric_matches >= MIN_MATCH_COUNT and
        numeric_rate >= NUMERIC_THRESHOLD * 100
    )

    found = watermark_column_found or timestamp_found or numeric_found

    result = {
        "found": found,
        "watermark": expected_watermark,
        "timestamp_cols": timestamp_cols,
        "has_watermark_column": has_watermark_column,
        "watermark_column": {
            "matches": watermark_column_matches,
            "checked": watermark_column_checked,
            "match_rate": watermark_column_rate,
            "found": watermark_column_found,
            "samples": watermark_column_samples[:5] if verbose else []
        },
        "timestamp": {
            "matches": timestamp_matches,
            "checked": timestamp_checked,
            "match_rate": timestamp_rate,
            "found": timestamp_found,
            "samples": timestamp_samples[:5] if verbose else []
        },
        "numeric": {
            "matches": numeric_matches,
            "checked": numeric_checked,
            "match_rate": numeric_rate,
            "found": numeric_found,
            "samples": numeric_samples[:5] if verbose else []
        }
    }

    if not found:
        if watermark_column_checked < MIN_SAMPLE_SIZE and timestamp_checked < MIN_SAMPLE_SIZE and numeric_checked < MIN_SAMPLE_SIZE:
            result["reason"] = f"Insufficient sample size (need {MIN_SAMPLE_SIZE}, got watermark_col={watermark_column_checked}, timestamp={timestamp_checked}, numeric={numeric_checked})"
        elif watermark_column_matches < MIN_MATCH_COUNT and timestamp_matches < MIN_MATCH_COUNT and numeric_matches < MIN_MATCH_COUNT:
            result["reason"] = f"Insufficient matches (need {MIN_MATCH_COUNT}, got watermark_col={watermark_column_matches}, timestamp={timestamp_matches}, numeric={numeric_matches})"
        elif not watermark_column_found and not timestamp_found and not numeric_found:
            result["reason"] = f"Match rates below threshold (watermark_col={watermark_column_rate:.1f}%/{WATERMARK_COL_THRESHOLD*100:.0f}%, timestamp={timestamp_rate:.1f}%/{TIMESTAMP_THRESHOLD*100:.0f}%, numeric={numeric_rate:.1f}%/{NUMERIC_THRESHOLD*100:.0f}%)"

    if anchor_columns_missing:
        result["found"] = False
        result["reason"] = "Cannot verify watermark: required anchor columns are missing from the DataFrame."
        result["warning"] = "Anchor columns are required for reliable watermark verification."

    return result

def extract_list_items(response_obj):
    if isinstance(response_obj, list):
        return response_obj
    elif hasattr(response_obj, 'shares'):
        return response_obj.shares
    elif hasattr(response_obj, 'schemas'):
        return response_obj.schemas
    elif hasattr(response_obj, 'tables'):
        return response_obj.tables
    else:
        return []

def api_post(url: str, json_data: dict, headers: dict = None, expected_status: int = 200) -> dict:
    response = requests.post(url, json=json_data, headers=headers)
    assert response.status_code == expected_status, f"POST {url} failed: {response.text}"
    return response.json()

def api_get(url: str, headers: dict = None, expected_status: int = 200) -> dict:
    response = requests.get(url, headers=headers)
    assert response.status_code == expected_status, f"GET {url} failed: {response.text}"
    return response.json()

def api_delete(url: str, headers: dict = None, expected_status: int = 204) -> None:
    response = requests.delete(url, headers=headers)
    assert response.status_code == expected_status, f"DELETE {url} failed: {response.text}"

def register_user_public_key(marketplace_url: str, headers: dict, public_key_pem: str) -> dict:
    response = requests.put(
        f"{marketplace_url}/me/public-key",
        json={"public_key": public_key_pem},
        headers=headers
    )
    assert response.status_code == 200, f"Failed to register public key: {response.text}"
    return response.json()

def register_buyer_public_key(marketplace_url: str, buyer_headers: dict) -> dict:
    key_pair = generate_key_pair()
    public_key_pem = base64.b64decode(key_pair['public_key']).decode('utf-8')
    register_user_public_key(marketplace_url, buyer_headers, public_key_pem)

    return {
        'public_key': public_key_pem,
        'private_key': base64.b64decode(key_pair['private_key']).decode('utf-8'),
        'public_key_b64': key_pair['public_key'],
        'private_key_b64': key_pair['private_key']
    }

def register_seller_metadata_public_key(marketplace_url: str, seller_headers: dict) -> dict:
    public_key_pem = get_seller_metadata_signing_public_key_pem()
    return register_user_public_key(marketplace_url, seller_headers, public_key_pem)
