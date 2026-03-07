import hashlib
import hmac
import random
import pandas as pd
import pyarrow as pa
import numpy as np
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable
from sqlalchemy.orm import Session
from src.models.database import Dataset, Share
from src.utils.s3_utils import (
    get_delta_storage_options, get_full_s3_path, get_bucket_name
)
from src.utils.settings import get_settings
from src.utils.data_utils import normalize_value_for_anchor, detect_timestamp_columns, parse_anchor_columns

WATERMARK_LENGTH = 16
DEFAULT_WATERMARK = "0000000000000000"
MAX_ANCHOR_COLUMNS = 10
DEFAULT_ANCHOR_COLUMNS = 5
PSEUDOROW_RATIO = 20
MICROSECONDS_PER_SECOND = 1000000
WATERMARK_BYTE_MULTIPLIER = 12500
WATERMARK_SEED_MOD = 10000
FLOAT_PRECISION_PLACE = 5

def get_watermark_secret() -> bytes:
    settings = get_settings()
    return settings.get_watermark_secret_bytes()

def detect_anchor_columns_from_schema(schema: pa.Schema, sensitive_columns: list = None) -> list:
    all_cols = [field.name for field in schema]
    sensitive_columns = sensitive_columns or []
    
    timestamp_like = [col for col in all_cols if 'timestamp' in col.lower() or 'time' in col.lower() or 'date' in col.lower()]
    pii_like = [col for col in all_cols if any(term in col.lower() for term in ['email', 'phone', 'ssn', 'name', 'address', 'ip'])]
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

def generate_watermark(buyer_id: int, share_id: int) -> str:
    secret = get_watermark_secret()
    if not isinstance(secret, bytes):
        raise ValueError("Watermark secret must be bytes")
    message = f"{buyer_id}:{share_id}".encode('utf-8')
    hmac_hash = hmac.new(secret, message, hashlib.sha256).hexdigest()[:WATERMARK_LENGTH]
    return hmac_hash.ljust(WATERMARK_LENGTH, '0')


def compute_row_anchor(row: pd.Series, df_dtypes: pd.Series = None, anchor_columns: list = None) -> int:
    row_filtered = row.drop('_watermark_id') if '_watermark_id' in row.index else row
    
    if anchor_columns:
        available_anchor_cols = [col for col in anchor_columns if col in row_filtered.index]
        if not available_anchor_cols:
            raise ValueError(f"None of the anchor columns are available in row. Requested: {anchor_columns}, Available: {list(row_filtered.index)}")
        cols_to_use = sorted(available_anchor_cols)
    else:
        cols_to_use = sorted(row_filtered.index)
    
    normalized_parts = []
    for col in cols_to_use:
        value = row_filtered[col]
        if df_dtypes is not None and col in df_dtypes.index:
            dtype = df_dtypes[col]
        else:
            dtype = type(value)
        normalized = normalize_value_for_anchor(value, dtype)
        normalized_parts.append(f"{col}:{normalized}")
    
    row_str = '|'.join(normalized_parts)
    row_hash_hex = hashlib.sha256(row_str.encode('utf-8')).hexdigest()[:16]
    row_hash = int(row_hash_hex, 16)
    return row_hash

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
        num_pseudorows = max(1, len(df) // 10)
    
    watermark = (watermark or DEFAULT_WATERMARK)[:WATERMARK_LENGTH].ljust(WATERMARK_LENGTH, '0')
    watermark_seed = int(watermark[:8], 16)
    watermark_bytes = [int(watermark[i:i+2], 16) for i in range(0, WATERMARK_LENGTH, 2)]
    
    random.seed(watermark_seed)
    
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
        
        anchor_bytes = (row_anchors_masked % len(watermark_bytes)).astype(np.int64)
        watermark_byte_values = np.array([watermark_bytes[int(b)] for b in anchor_bytes])
        
        target_microseconds = (watermark_byte_values * WATERMARK_BYTE_MULTIPLIER + watermark_seed % WATERMARK_SEED_MOD) % MICROSECONDS_PER_SECOND
        
        base_ts = original_ts.dt.floor('S')
        watermarked_ts = base_ts + pd.to_timedelta(target_microseconds, unit='us')
        df.loc[mask, col] = watermarked_ts

def _apply_numeric_watermark(df: pd.DataFrame, numeric_cols: list, row_anchors: np.ndarray, watermark: str, watermark_seed: int):
    watermark_hash_int = int(watermark[:8], 16)
    
    for col in numeric_cols:
        if col not in df.columns:
            continue
        
        mask = df[col].notna()
        if not mask.any():
            continue
        
        row_anchors_masked = row_anchors[mask.values]
        
        if pd.api.types.is_integer_dtype(df[col]):
            slot_values = ((row_anchors_masked ^ watermark_hash_int) % 10).astype(np.int64)
            original_values = df.loc[mask, col].astype(np.int64)
            watermarked_values = (original_values // 10) * 10 + slot_values
            df.loc[mask, col] = watermarked_values.astype(df[col].dtype)
        elif pd.api.types.is_float_dtype(df[col]):
            combined_seed = ((row_anchors_masked ^ watermark_hash_int) % (2**32)).astype(np.uint32)
            for i, idx in enumerate(df.loc[mask, col].index):
                np.random.seed(int(combined_seed[i]))
                bit_value = (watermark_hash_int >> (i % 32)) & 1
                original_val = float(df.loc[idx, col])
                original_str = f"{original_val:.10f}"
                if '.' in original_str:
                    parts = original_str.split('.')
                    if len(parts[1]) > FLOAT_PRECISION_PLACE:
                        decimal_part = parts[1]
                        new_decimal = decimal_part[:FLOAT_PRECISION_PLACE] + str(bit_value) + decimal_part[FLOAT_PRECISION_PLACE+1:]
                        new_val = float(parts[0] + '.' + new_decimal)
                        df.loc[idx, col] = new_val

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
    row_anchors = df_for_anchor.apply(lambda r: compute_row_anchor(r, df_for_anchor.dtypes, anchor_columns), axis=1).values.astype(np.uint64)
    
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
    
    _apply_numeric_watermark(df, numeric_cols, row_anchors, watermark, watermark_seed)
    
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

def create_watermarked_table(
    dataset: Dataset,
    buyer_id: int,
    share_id: int,
    db: Session
) -> str:
    bucket_name = get_bucket_name()
    original_table_path = get_full_s3_path(bucket_name, dataset.table_path)
    storage_options = get_delta_storage_options()
    
    watermark = generate_watermark(buyer_id, share_id)
    watermarked_table_path = f"{dataset.table_path}_buyer_{buyer_id}"
    full_watermarked_path = get_full_s3_path(bucket_name, watermarked_table_path)
    
    delta_table = DeltaTable(original_table_path, storage_options=storage_options)
    df = delta_table.to_pandas()
    
    if df.empty:
        raise ValueError("Original table is empty")
    
    share = db.query(Share).filter(Share.id == share_id).first()
    is_trial = share.is_trial if share else False
    anchor_cols = parse_anchor_columns(dataset.anchor_columns) if dataset.anchor_columns else None
    df = apply_watermark_to_dataframe(df, watermark, is_trial=is_trial, anchor_columns=anchor_cols)
    
    table = pa.Table.from_pandas(df)
    write_deltalake(
        full_watermarked_path,
        table,
        mode='overwrite',
        storage_options=storage_options
    )
    
    if share:
        share.watermarked_table_path = watermarked_table_path
        db.commit()
    
    return watermarked_table_path

def update_watermarked_tables(
    dataset: Dataset,
    new_data: pd.DataFrame,
    db: Session
):
    shares = db.query(Share).filter(
        Share.dataset_id == dataset.id
    ).all()
    
    if not shares:
        return
    
    bucket_name = get_bucket_name()
    storage_options = get_delta_storage_options()
    
    for share in shares:
        if not share.watermarked_table_path:
            try:
                create_watermarked_table(
                    dataset=dataset,
                    buyer_id=share.buyer_id,
                    share_id=share.id,
                    db=db
                )
            except Exception as e:
                print(f"Warning: Failed to create watermarked table for share {share.id}: {e}")
                continue
        
        watermark = generate_watermark(share.buyer_id, share.id)
        anchor_cols = parse_anchor_columns(dataset.anchor_columns) if dataset.anchor_columns else None
        watermarked_data = apply_watermark_to_dataframe(new_data, watermark, is_trial=share.is_trial, anchor_columns=anchor_cols)
        
        full_watermarked_path = get_full_s3_path(bucket_name, share.watermarked_table_path)
        table = pa.Table.from_pandas(watermarked_data)
        
        write_deltalake(
            full_watermarked_path,
            table,
            mode='append',
            storage_options=storage_options
        )

