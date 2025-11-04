import time
import os
import pandas as pd
import pyarrow as pa
from datetime import datetime
from deltalake import write_deltalake
from sqlalchemy.orm import Session
from src.models.database import get_db, Dataset, SessionLocal
from src.seller.pii_detection import analyze_dataset_for_pii
from src.utils.s3_utils import (
    get_s3_client, get_delta_storage_options, get_bucket_name, get_full_s3_path
)

def write_data_continuously(
    dataset_id: int,
    interval_seconds: int = 30,
    num_writes: int = 10,
    db: Session = None
):
    
    if db is None:
        db = SessionLocal()
    
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise ValueError(f"Dataset {dataset_id} not found")
    
    bucket_name = get_bucket_name()
    table_path = get_full_s3_path(bucket_name, dataset.table_path)
    storage_options = get_delta_storage_options()
    
    s3_client = get_s3_client()
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            print(f"Warning: Could not create bucket {bucket_name}: {e}")
    
    print(f"Starting continuous data writer for dataset {dataset_id}")
    print(f"Table path: {table_path}")
    print(f"Writing every {interval_seconds} seconds, {num_writes} times")
    
    for i in range(num_writes):
        data = pd.DataFrame({
            'timestamp': [datetime.utcnow().isoformat()] * 5,
            'value': [i * 10 + j for j in range(5)],
            'category': [f'cat_{j % 3}' for j in range(5)],
            'write_batch': [i] * 5
        })
        
        if i == 0:
            try:
                sensitive_columns, pii_types, risk_score, risk_level = analyze_dataset_for_pii(data)
                if pii_types:
                    print(f"[{datetime.utcnow()}] PII detected: {dict(pii_types)}, Risk: {risk_score:.2f} ({risk_level})")
                    dataset.risk_score = risk_score
                    dataset.risk_level = risk_level
                    dataset.detected_pii_types = ','.join(pii_types.keys()) if pii_types else None
                    dataset.sensitive_columns = ','.join(sensitive_columns.keys()) if sensitive_columns else None
                    dataset.requires_approval = risk_score >= 20
                    db.commit()
            except Exception as e:
                print(f"[{datetime.utcnow()}] Warning: PII analysis failed: {e}")
        
        try:
            table = pa.Table.from_pandas(data)
            
            write_deltalake(
                table_path,
                table,
                mode='append',
                storage_options=storage_options
            )
            print(f"[{datetime.utcnow()}] Write batch {i+1}/{num_writes}: Added {len(data)} rows")
        except Exception as e:
            print(f"[{datetime.utcnow()}] Error writing batch {i+1}: {e}")
            if "does not exist" in str(e).lower() or "no such file" in str(e).lower():
                try:
                    table = pa.Table.from_pandas(data)
                    write_deltalake(
                        table_path,
                        table,
                        mode='overwrite',
                        storage_options=storage_options
                    )
                    print(f"[{datetime.utcnow()}] Created table and wrote batch {i+1}")
                except Exception as e2:
                    print(f"[{datetime.utcnow()}] Error creating table: {e2}")
        
        if i < num_writes - 1:
            time.sleep(interval_seconds)
    
    print(f"Finished writing {num_writes} batches")

