import requests
import pandas as pd
import hashlib
import numpy as np
from src.seller.watermarking import generate_watermark, compute_row_anchor

def detect_timestamp_columns(df: pd.DataFrame) -> list:
    timestamp_cols = []
    for col in df.columns:
        if col == '_watermark_id':
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            timestamp_cols.append(col)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
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
                    if parse_success >= len(sample_vals) * 0.7:
                        timestamp_cols.append(col)
            except:
                pass
    return timestamp_cols

def check_watermark(df: pd.DataFrame, buyer_id: int, share_id: int, verbose: bool = True, anchor_columns: list = None) -> dict:
    expected_watermark = generate_watermark(buyer_id, share_id)
    watermark_bytes = [int(expected_watermark[i:i+2], 16) for i in range(0, min(16, len(expected_watermark)), 2)]
    watermark_seed = int(expected_watermark[:8], 16)
    
    has_watermark_column = '_watermark_id' in df.columns
    timestamp_cols = detect_timestamp_columns(df)
    
    if not timestamp_cols and not has_watermark_column:
        return {
            "found": False,
            "reason": "No timestamp columns or watermark column found",
            "watermark": expected_watermark,
            "timestamp_cols": [],
            "has_watermark_column": False
        }
    
    df_for_anchor = df.drop(columns=['_watermark_id']) if '_watermark_id' in df.columns else df
    
    watermark_column_matches = 0
    watermark_column_checked = 0
    watermark_column_samples = []
    
    timestamp_matches = 0
    timestamp_checked = 0
    timestamp_samples = []
    
    row_count = len(df)
    MIN_SAMPLE_SIZE = min(20, max(5, row_count // 4))
    MIN_MATCH_COUNT = max(3, MIN_SAMPLE_SIZE // 4)
    WATERMARK_COL_THRESHOLD = 0.7
    TIMESTAMP_THRESHOLD = 0.5
    
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
    
    for idx, row in df.iterrows():
        for col in timestamp_cols:
            try:
                ts = pd.to_datetime(row[col])
                if pd.isna(ts):
                    continue
                
                row_for_anchor = row.drop('_watermark_id') if '_watermark_id' in row.index else row
                row_anchor = compute_row_anchor(row_for_anchor, df_for_anchor.dtypes, anchor_columns)
                anchor_byte_idx = int(row_anchor % len(watermark_bytes))
                expected_byte = watermark_bytes[anchor_byte_idx]
                expected_microseconds = (expected_byte * 1000 + watermark_seed % 1000) % 1000000
                
                microseconds = ts.microsecond
                timestamp_checked += 1
                
                diff = abs(microseconds - expected_microseconds)
                if diff < 500 or (diff > 999500):
                    timestamp_matches += 1
                    if verbose and timestamp_checked <= 5:
                        timestamp_samples.append(f"Row {idx}, {col}: microseconds={microseconds} (expected: {expected_microseconds}, diff: {diff})")
            except Exception as e:
                if verbose and timestamp_checked < 3:
                    timestamp_samples.append(f"Row {idx}, {col}: Error - {e}")
                pass
    
    watermark_column_rate = (watermark_column_matches / watermark_column_checked * 100) if watermark_column_checked > 0 else 0.0
    timestamp_rate = (timestamp_matches / timestamp_checked * 100) if timestamp_checked > 0 else 0.0
    
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
    
    found = watermark_column_found or timestamp_found
    
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
        }
    }
    
    if not found:
        if watermark_column_checked < MIN_SAMPLE_SIZE and timestamp_checked < MIN_SAMPLE_SIZE:
            result["reason"] = f"Insufficient sample size (need {MIN_SAMPLE_SIZE}, got watermark_col={watermark_column_checked}, timestamp={timestamp_checked})"
        elif watermark_column_matches < MIN_MATCH_COUNT and timestamp_matches < MIN_MATCH_COUNT:
            result["reason"] = f"Insufficient matches (need {MIN_MATCH_COUNT}, got watermark_col={watermark_column_matches}, timestamp={timestamp_matches})"
        elif not watermark_column_found and not timestamp_found:
            result["reason"] = f"Match rates below threshold (watermark_col={watermark_column_rate:.1f}%/{WATERMARK_COL_THRESHOLD*100:.0f}%, timestamp={timestamp_rate:.1f}%/{TIMESTAMP_THRESHOLD*100:.0f}%)"
    
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

