import hashlib
import hmac
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from src.seller.watermarking import generate_watermark, compute_row_anchor, normalize_value_for_anchor
from src.utils.settings import get_settings

def generate_buyer_fingerprint(buyer_id: int, share_id: int, redundancy: int = 3) -> Dict:
    settings = get_settings()
    secret = settings.get_watermark_secret_bytes()
    if not secret or not isinstance(secret, bytes):
        raise ValueError("Watermark secret must be bytes")
    
    base_message = f"{buyer_id}:{share_id}".encode('utf-8')
    base_hmac = hmac.new(secret, base_message, hashlib.sha256).hexdigest()
    
    fingerprint = {
        "buyer_id": buyer_id,
        "share_id": share_id,
        "base_hash": base_hmac[:32],
        "redundancy": redundancy,
        "components": []
    }
    
    for i in range(redundancy):
        component_message = f"{base_message}:{i}".encode('utf-8')
        component_hash = hmac.new(secret, component_message, hashlib.sha256).hexdigest()
        fingerprint["components"].append({
            "index": i,
            "hash": component_hash[:16],
            "seed": int(component_hash[:8], 16)
        })
    
    return fingerprint

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
    
    if is_trial:
        df['_watermark_id'] = df.apply(
            lambda row: _compute_trial_watermark_id(row, fingerprint, available_anchor_cols),
            axis=1
        )
    
    timestamp_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    
    if timestamp_cols:
        for col in timestamp_cols:
            df[col] = df.apply(
                lambda row: _embed_fingerprint_in_timestamp(row[col], fingerprint, row, available_anchor_cols),
                axis=1
            )
    
    for component in fingerprint["components"]:
        seed = component["seed"]
        np.random.seed(seed)
        
        for col in df.columns:
            if col in ['_watermark_id'] or col in timestamp_cols:
                continue
            
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = _embed_fingerprint_in_numeric(df[col], component, available_anchor_cols, df)
    
    return df

def _compute_trial_watermark_id(row: pd.Series, fingerprint: Dict, anchor_columns: List[str]) -> int:
    row_anchor = compute_row_anchor(row, anchor_columns=anchor_columns)
    base_hash = fingerprint["base_hash"]
    
    combined = f"{base_hash}:{row_anchor}".encode('utf-8')
    watermark_id = int(hashlib.sha256(combined).hexdigest()[:8], 16)
    
    return watermark_id

def _embed_fingerprint_in_timestamp(
    timestamp_value: pd.Timestamp,
    fingerprint: Dict,
    row: pd.Series,
    anchor_columns: List[str]
) -> pd.Timestamp:
    if pd.isna(timestamp_value):
        return timestamp_value
    
    row_anchor = compute_row_anchor(row, anchor_columns=anchor_columns)
    
    base_hash_int = int(fingerprint["base_hash"][:8], 16)
    combined_seed = (base_hash_int + row_anchor) % (2**32)
    
    np.random.seed(combined_seed)
    watermark_bytes = [int(fingerprint["base_hash"][i:i+2], 16) for i in range(0, min(8, len(fingerprint["base_hash"])), 2)]
    watermark_byte = watermark_bytes[row_anchor % len(watermark_bytes)]
    
    target_microseconds = (watermark_byte * 12500 + combined_seed % 10000) % 1000000
    
    current_microseconds = timestamp_value.microsecond
    new_microseconds = (current_microseconds // 10000) * 10000 + target_microseconds
    
    new_timestamp = timestamp_value.replace(microsecond=new_microseconds)
    return new_timestamp

def _embed_fingerprint_in_numeric(
    series: pd.Series,
    component: Dict,
    anchor_columns: List[str],
    full_df: pd.DataFrame
) -> pd.Series:
    seed = component["seed"]
    np.random.seed(seed)
    
    result = series.copy()
    
    for idx in series.index:
        row = full_df.loc[idx]
        row_anchor = compute_row_anchor(row, anchor_columns=anchor_columns)
        
        component_hash_int = int(component["hash"][:8], 16)
        combined_seed = (component_hash_int + row_anchor) % (2**32)
        np.random.seed(combined_seed)
        
        if pd.api.types.is_integer_dtype(series):
            lsb_mod = component_hash_int % 10
            result.iloc[idx] = (int(result.iloc[idx]) // 10) * 10 + lsb_mod
        elif pd.api.types.is_float_dtype(series):
            noise = (component_hash_int % 100) / 10000.0
            result.iloc[idx] = float(result.iloc[idx]) + noise
    
    return result

def verify_fingerprint(
    df: pd.DataFrame,
    fingerprint: Dict,
    anchor_columns: List[str],
    tolerance: float = 0.15
) -> Dict:
    if df.empty:
        return {"found": False, "reason": "DataFrame is empty"}
    
    available_anchor_cols = [col for col in anchor_columns if col in df.columns]
    if not available_anchor_cols:
        return {"found": False, "reason": f"Anchor columns missing: {anchor_columns}"}
    
    results = {
        "found": False,
        "fingerprint": fingerprint,
        "components": [],
        "overall_match_rate": 0.0
    }
    
    timestamp_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    
    if timestamp_cols:
        timestamp_matches = 0
        timestamp_checked = 0
        
        for idx, row in df.iterrows():
            timestamp_checked += 1
            row_anchor = compute_row_anchor(row, anchor_columns=available_anchor_cols)
            
            base_hash_int = int(fingerprint["base_hash"][:8], 16)
            combined_seed = (base_hash_int + row_anchor) % (2**32)
            
            watermark_bytes = [int(fingerprint["base_hash"][i:i+2], 16) for i in range(0, min(8, len(fingerprint["base_hash"])), 2)]
            watermark_byte = watermark_bytes[row_anchor % len(watermark_bytes)]
            expected_microseconds = (watermark_byte * 12500 + combined_seed % 10000) % 1000000
            
            for col in timestamp_cols:
                if pd.notna(row[col]):
                    actual_microseconds = row[col].microsecond
                    diff = abs(actual_microseconds - expected_microseconds)
                    if diff < 1000 or abs(diff - 1000000) < 1000:
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
        
        for idx, row in df.iterrows():
            watermark_id_checked += 1
            row_anchor = compute_row_anchor(row, anchor_columns=available_anchor_cols)
            
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
    
    overall_match_rate = 0.0
    match_count = 0
    
    if "timestamp" in results:
        overall_match_rate += results["timestamp"]["match_rate"]
        match_count += 1
    
    if "watermark_column" in results:
        overall_match_rate += results["watermark_column"]["match_rate"]
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
    attacks: List[str] = ["row_deletion", "projection", "perturbation"]
) -> Dict:
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
        else:
            continue
        
        verification = verify_fingerprint(attacked_df, fingerprint, anchor_columns)
        results["attacks"][attack] = {
            "survived": verification["found"],
            "match_rate": verification["overall_match_rate"]
        }
    
    return results

