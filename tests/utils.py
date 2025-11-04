import requests
import pandas as pd
from src.seller.watermarking import generate_watermark

def check_watermark(df: pd.DataFrame, buyer_id: int, share_id: int, verbose: bool = True) -> dict:
    expected_watermark = generate_watermark(buyer_id, share_id)
    watermark_bytes = [int(expected_watermark[i:i+2], 16) for i in range(0, min(16, len(expected_watermark)), 2)]
    watermark_seed = int(expected_watermark[:8], 16)
    
    timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower() or 'time' in col.lower()]
    if not timestamp_cols:
        timestamp_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns]']
    
    if not timestamp_cols:
        return {"found": False, "reason": "No timestamp columns found"}
    
    matches = 0
    total_checked = 0
    sample_matches = []
    sample_mismatches = []
    
    for pos, (idx, row) in enumerate(df.iterrows()):
        for col in timestamp_cols:
            try:
                ts = pd.to_datetime(row[col])
                if pd.isna(ts):
                    continue
                
                microseconds = ts.microsecond
                total_checked += 1
                
                if pos == 0:
                    expected_microseconds = watermark_seed % 1000000
                else:
                    expected_byte = watermark_bytes[pos % len(watermark_bytes)]
                    expected_microseconds = (expected_byte * 1000 + watermark_seed % 1000) % 1000000
                
                diff = abs(microseconds - expected_microseconds)
                if diff < 1000 or (diff > 999000):
                    matches += 1
                    if verbose and pos < 5:
                        sample_matches.append(f"Row {pos} (index {idx}), {col}: microseconds={microseconds} (expected: {expected_microseconds}, diff: {diff})")
                elif verbose and pos < 5:
                    sample_mismatches.append(f"Row {pos} (index {idx}), {col}: microseconds={microseconds} (expected: {expected_microseconds}, diff: {diff})")
            except Exception as e:
                if verbose and pos < 3:
                    sample_mismatches.append(f"Row {pos}, {col}: Error - {e}")
                pass
    
    match_rate = (matches / total_checked) * 100 if total_checked > 0 else 0
    
    result = {
        "found": matches > 0,
        "matches": matches,
        "total_checked": total_checked,
        "match_rate": match_rate,
        "watermark": expected_watermark,
        "timestamp_cols": timestamp_cols,
        "sample_matches": sample_matches
    }
    
    if verbose and sample_mismatches:
        result["sample_mismatches"] = sample_mismatches[:5]
    
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

