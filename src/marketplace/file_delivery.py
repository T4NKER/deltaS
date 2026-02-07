import os
import json
import hashlib
import tempfile
from datetime import datetime, timedelta
from typing import Optional, Dict
from sqlalchemy.orm import Session
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from src.models.database import Share, Dataset, AuditLog
from src.utils.s3_utils import (
    get_s3_client, get_delta_storage_options, get_full_s3_path, get_bucket_name
)
from src.utils.settings import get_settings

def generate_file_download_link(
    dataset: Dataset,
    share: Share,
    db: Session,
    expiry_hours: int = 24
) -> Dict:
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    storage_options = get_delta_storage_options()
    
    original_table_path = get_full_s3_path(bucket_name, dataset.table_path)
    
    try:
        delta_table = DeltaTable(original_table_path, storage_options=storage_options)
        df = delta_table.to_pandas()
    except Exception as e:
        raise ValueError(f"Failed to read Delta table: {str(e)}")
    
    snapshot_id = f"snapshot_{share.id}_{int(datetime.utcnow().timestamp())}"
    snapshot_key = f"snapshots/{snapshot_id}.parquet"
    snapshot_path = get_full_s3_path(bucket_name, snapshot_key)
    
    table = pa.Table.from_pandas(df)
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        pq.write_table(table, tmp_file.name)
        tmp_file_path = tmp_file.name
        file_size = os.path.getsize(tmp_file_path)
    
    try:
        s3_client.upload_file(tmp_file_path, bucket_name, snapshot_key)
    finally:
        if os.path.exists(tmp_file_path):
            os.unlink(tmp_file_path)
    
    expires_at = datetime.utcnow() + timedelta(hours=expiry_hours)
    
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': snapshot_key},
        ExpiresIn=expiry_hours * 3600
    )
    
    download_token = hashlib.sha256(f"{share.id}:{snapshot_id}:{expires_at.isoformat()}".encode()).hexdigest()[:32]
    
    audit_log = AuditLog(
        share_id=share.id,
        buyer_id=share.buyer_id,
        dataset_id=dataset.id,
        query_type="file_download",
        query_params=json.dumps({"snapshot_id": snapshot_id, "expiry_hours": expiry_hours}),
        rows_returned=len(df),
        bytes_served=file_size,
        client_metadata=json.dumps({"delivery_method": "file_link", "download_token": download_token})
    )
    db.add(audit_log)
    db.commit()
    
    return {
        "download_url": presigned_url,
        "download_token": download_token,
        "expires_at": expires_at.isoformat(),
        "snapshot_id": snapshot_id,
        "file_size_bytes": len(df) * 100,
        "rows": len(df),
        "columns": list(df.columns)
    }

def revoke_file_download_link(snapshot_id: str, db: Session) -> bool:
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    
    snapshot_key = f"snapshots/{snapshot_id}.parquet"
    
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=snapshot_key)
        return True
    except Exception as e:
        print(f"Error revoking file link: {e}")
        return False

def get_file_delivery_metrics(share_id: int, db: Session) -> Dict:
    audit_logs = db.query(AuditLog).filter(
        AuditLog.share_id == share_id,
        AuditLog.query_type == "file_download"
    ).all()
    
    total_bytes = sum(log.bytes_served or 0 for log in audit_logs)
    total_rows = sum(log.rows_returned or 0 for log in audit_logs)
    total_downloads = len(audit_logs)
    
    return {
        "total_downloads": total_downloads,
        "total_bytes_served": total_bytes,
        "total_rows_served": total_rows,
        "average_bytes_per_download": total_bytes / total_downloads if total_downloads > 0 else 0,
        "average_rows_per_download": total_rows / total_downloads if total_downloads > 0 else 0
    }

