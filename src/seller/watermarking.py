import hashlib
import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable
from sqlalchemy.orm import Session
from src.models.database import Dataset, Share
from src.utils.s3_utils import (
    get_delta_storage_options, get_full_s3_path, get_bucket_name
)

def generate_watermark(buyer_id: int, share_id: int) -> str:
    watermark_string = f"{buyer_id}-{share_id}"
    watermark_hash = hashlib.sha256(watermark_string.encode()).hexdigest()[:16]
    return watermark_hash

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

def apply_watermark_to_dataframe(df: pd.DataFrame, watermark: str) -> pd.DataFrame:
    if df.empty:
        return df
    
    df = df.copy()
    
    watermark_seed = int(watermark[:8], 16)
    watermark_bytes = [int(watermark[i:i+2], 16) for i in range(0, min(16, len(watermark)), 2)]
    
    timestamp_cols = []
    for col in df.columns:
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
    
    if not timestamp_cols:
        return df
    
    for col in timestamp_cols:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col])
    
    for pos, (idx, row) in enumerate(df.iterrows()):
        byte_idx = pos % len(watermark_bytes)
        watermark_byte = watermark_bytes[byte_idx]
        
        for col in timestamp_cols:
            try:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    original_ts = row[col]
                    if pd.isna(original_ts):
                        continue
                    
                    if not isinstance(original_ts, pd.Timestamp) and not hasattr(original_ts, 'microsecond'):
                        original_ts = pd.to_datetime(original_ts)
                    
                    if pos == 0:
                        target_microseconds = watermark_seed % 1000000
                    else:
                        expected_byte = watermark_bytes[pos % len(watermark_bytes)]
                        target_microseconds = (expected_byte * 1000 + watermark_seed % 1000) % 1000000
                    
                    base_ts = original_ts.replace(microsecond=0)
                    watermarked_ts = base_ts + timedelta(microseconds=target_microseconds)
                    df.loc[idx, col] = watermarked_ts
                else:
                    original_str = row[col]
                    if pd.isna(original_str) or not isinstance(original_str, str):
                        continue
                    
                    try:
                        original_ts = pd.to_datetime(original_str)
                        if pos == 0:
                            target_microseconds = watermark_seed % 1000000
                        else:
                            expected_byte = watermark_bytes[pos % len(watermark_bytes)]
                            target_microseconds = (expected_byte * 1000 + watermark_seed % 1000) % 1000000
                        
                        base_ts = original_ts.replace(microsecond=0)
                        watermarked_ts = base_ts + timedelta(microseconds=target_microseconds)
                        if '.' in original_str or 'T' in original_str:
                            df.at[idx, col] = watermarked_ts.strftime('%Y-%m-%dT%H:%M:%S.%f')
                        else:
                            df.at[idx, col] = watermarked_ts.strftime('%Y-%m-%dT%H:%M:%S.%f')
                    except:
                        pass
            except Exception as e:
                pass
    
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
            df = apply_watermark_to_dataframe(df, watermark)
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
            watermarked_data = apply_watermark_to_dataframe(new_data, watermark)
            
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

