import hashlib
import hmac
import os
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

def get_watermark_secret() -> bytes:
    secret = os.getenv("WATERMARK_SECRET", "default-watermark-secret-change-in-production")
    return secret.encode('utf-8')

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
    
    return anchor_cols[:10]

def generate_watermark(buyer_id: int, share_id: int) -> str:
    secret = get_watermark_secret()
    message = f"{buyer_id}:{share_id}".encode('utf-8')
    hmac_hash = hmac.new(secret, message, hashlib.sha256).hexdigest()[:16]
    return hmac_hash

def normalize_value_for_anchor(value, dtype):
    if pd.isna(value):
        return 'NULL'
    
    if pd.api.types.is_integer_dtype(dtype):
        return str(int(value))
    elif pd.api.types.is_float_dtype(dtype):
        return f"{float(value):.10f}"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        ts = pd.to_datetime(value)
        return ts.strftime('%Y-%m-%dT%H:%M:%S.%f')
    elif pd.api.types.is_bool_dtype(dtype):
        return 'TRUE' if value else 'FALSE'
    else:
        return str(value)

def compute_row_anchor(row: pd.Series, df_dtypes: pd.Series = None, anchor_columns: list = None) -> int:
    row_filtered = row.drop('_watermark_id') if '_watermark_id' in row.index else row
    
    if anchor_columns:
        available_anchor_cols = [col for col in anchor_columns if col in row_filtered.index]
        if len(available_anchor_cols) != len(anchor_columns):
            missing = [col for col in anchor_columns if col not in row_filtered.index]
            raise ValueError(f"Anchor columns missing from row: {missing}")
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

def generate_pseudorows(df: pd.DataFrame, watermark: str, num_pseudorows: int = None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    
    if num_pseudorows is None:
        num_pseudorows = max(1, len(df) // 10)
    
    watermark_seed = int(watermark[:8], 16)
    watermark_bytes = [int(watermark[i:i+2], 16) for i in range(0, min(16, len(watermark)), 2)]
    
    import random
    random.seed(watermark_seed)
    np_random_state = watermark_seed % (2**32)
    
    pseudorows = []
    for i in range(num_pseudorows):
        row = {}
        byte_idx = i % len(watermark_bytes)
        watermark_byte = watermark_bytes[byte_idx]
        
        for col in df.columns:
            col_series = df[col].dropna()
            
            if pd.api.types.is_integer_dtype(df[col]):
                if len(col_series) > 0:
                    min_val = col_series.min()
                    max_val = col_series.max()
                    mean_val = col_series.mean()
                    std_val = col_series.std() if len(col_series) > 1 else abs(max_val - min_val) / 4
                    
                    base_value = int(mean_val + (watermark_byte - 128) * std_val / 50)
                    base_value = max(min_val, min(max_val, base_value))
                    
                    lsb_watermark = watermark_bytes[(i + byte_idx) % len(watermark_bytes)] % 10
                    row[col] = (base_value // 10) * 10 + lsb_watermark
                else:
                    row[col] = watermark_byte * 100 + i
                    
            elif pd.api.types.is_float_dtype(df[col]):
                if len(col_series) > 0:
                    min_val = col_series.min()
                    max_val = col_series.max()
                    mean_val = col_series.mean()
                    std_val = col_series.std() if len(col_series) > 1 else abs(max_val - min_val) / 4
                    
                    base_value = mean_val + (watermark_byte - 128) * std_val / 50
                    base_value = max(min_val, min(max_val, base_value))
                    
                    lsb_watermark = watermark_bytes[(i + byte_idx) % len(watermark_bytes)] % 100
                    row[col] = round(base_value, 2) + (lsb_watermark / 10000.0)
                else:
                    row[col] = (watermark_byte * 100 + i) / 100.0
                    
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                if len(col_series) > 0:
                    min_date = col_series.min()
                    max_date = col_series.max()
                    date_range = (max_date - min_date).days if max_date != min_date else 365
                    
                    days_offset = (watermark_byte * 7 + watermark_seed % 100) % date_range
                    row[col] = min_date + timedelta(days=days_offset)
                else:
                    base_date = datetime(2020, 1, 1)
                    days_offset = watermark_byte * 10 + (watermark_seed % 100) + i
                    row[col] = base_date + timedelta(days=days_offset)
                    
            elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                if len(col_series) > 0:
                    unique_vals = col_series.unique()
                    if len(unique_vals) > 0:
                        first_val = unique_vals[0]
                        if isinstance(first_val, str) and ('T' in first_val or '-' in first_val[:10]):
                            try:
                                pd.to_datetime(first_val)
                                if len(col_series) > 0:
                                    min_date = pd.to_datetime(col_series).min()
                                    max_date = pd.to_datetime(col_series).max()
                                    date_range = (max_date - min_date).days if max_date != min_date else 365
                                    days_offset = (watermark_byte * 7 + watermark_seed % 100) % date_range
                                    row[col] = (min_date + timedelta(days=days_offset)).isoformat()
                                else:
                                    base_date = datetime(2020, 1, 1)
                                    days_offset = watermark_byte * 10 + (watermark_seed % 100) + i
                                    row[col] = (base_date + timedelta(days=days_offset)).isoformat()
                            except:
                                idx = (watermark_byte + i) % len(unique_vals)
                                row[col] = unique_vals[idx]
                        else:
                            idx = (watermark_byte + i) % len(unique_vals)
                            row[col] = unique_vals[idx]
                    else:
                        row[col] = col_series.iloc[0] if len(col_series) > 0 else None
                else:
                    row[col] = None
            else:
                if len(col_series) > 0:
                    row[col] = col_series.iloc[(watermark_byte + i) % len(col_series)]
                else:
                    row[col] = None
        
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

def apply_watermark_to_dataframe(df: pd.DataFrame, watermark: str, is_trial: bool = False, anchor_columns: list = None) -> pd.DataFrame:
    if df.empty:
        return df
    
    df = df.copy()
    
    watermark_seed = int(watermark[:8], 16)
    watermark_bytes = [int(watermark[i:i+2], 16) for i in range(0, min(16, len(watermark)), 2)]
    
    df_for_anchor = df.drop(columns=['_watermark_id']) if '_watermark_id' in df.columns else df
    row_anchors = df_for_anchor.apply(lambda r: compute_row_anchor(r, df_for_anchor.dtypes, anchor_columns), axis=1).values.astype(np.uint64)
    
    timestamp_cols = []
    for col in df.columns:
        if col == '_watermark_id':
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            timestamp_cols.append(col)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            try:
                sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                if sample_val and isinstance(sample_val, str):
                    if 'T' in sample_val or ('-' in sample_val[:10] and len(sample_val) > 10):
                        try:
                            pd.to_datetime(sample_val)
                            timestamp_cols.append(col)
                        except:
                            pass
            except:
                pass
    
    if is_trial:
        df['_watermark_id'] = (row_anchors % 1000000).astype(np.int64)
    
    if not timestamp_cols and not is_trial:
        return df
    
    for col in timestamp_cols:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col])
    
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
        
        target_microseconds = (watermark_byte_values * 1000 + watermark_seed % 1000) % 1000000
        
        base_ts = original_ts.dt.floor('S')
        watermarked_ts = base_ts + pd.to_timedelta(target_microseconds, unit='us')
        df.loc[mask, col] = watermarked_ts
    
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
    
    try:
        delta_table = DeltaTable(original_table_path, storage_options=storage_options)
        df = delta_table.to_pandas()
        
        if df.empty:
            raise Exception("Original table is empty")
        
        try:
            share = db.query(Share).filter(Share.id == share_id).first()
            is_trial = share.is_trial if share else False
            anchor_cols = None
            if dataset.anchor_columns:
                anchor_cols = [col.strip() for col in dataset.anchor_columns.split(',') if col.strip()]
            df = apply_watermark_to_dataframe(df, watermark, is_trial=is_trial, anchor_columns=anchor_cols)
        except Exception as watermark_error:
            print(f"Warning: Watermarking failed: {watermark_error}")
            df = df.copy()
        
        table = pa.Table.from_pandas(df)
        
        write_deltalake(
            full_watermarked_path,
            table,
            mode='overwrite',
            storage_options=storage_options
        )
        
        share = db.query(Share).filter(Share.id == share_id).first()
        if share:
            share.watermarked_table_path = watermarked_table_path
            db.commit()
        
        return watermarked_table_path
    except Exception as e:
        import traceback
        error_detail = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"Error creating watermarked table: {error_detail}")
        raise Exception(f"Failed to create watermarked table: {str(e)}")

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
        try:
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
            anchor_cols = None
            if dataset.anchor_columns:
                anchor_cols = [col.strip() for col in dataset.anchor_columns.split(',') if col.strip()]
            watermarked_data = apply_watermark_to_dataframe(new_data, watermark, is_trial=share.is_trial, anchor_columns=anchor_cols)
            
            full_watermarked_path = get_full_s3_path(
                bucket_name,
                share.watermarked_table_path
            )
            
            table = pa.Table.from_pandas(watermarked_data)
            
            write_deltalake(
                full_watermarked_path,
                table,
                mode='append',
                storage_options=storage_options
            )
        except Exception as e:
            print(f"Warning: Failed to update watermarked table for share {share.id}: {e}")

