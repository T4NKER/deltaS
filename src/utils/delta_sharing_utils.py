from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta
import os
from src.models.database import Share, Dataset
from src.utils.s3_utils import get_full_s3_path
from src.utils.token_utils import verify_token_hash, is_token_expired

SELLER_ID = os.getenv("SELLER_ID", None)
if SELLER_ID and SELLER_ID.strip():
    try:
        SELLER_ID = int(SELLER_ID.strip())
    except (ValueError, AttributeError):
        SELLER_ID = None
else:
    SELLER_ID = None

def extract_token_from_header(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization")
    return authorization.replace("Bearer ", "")

def validate_share_access(share_name: str, share: Share) -> int:
    if not share_name.startswith("share_"):
        raise HTTPException(status_code=404, detail="Share not found")
    share_id = int(share_name.replace("share_", ""))
    if share_id != share.id:
        raise HTTPException(status_code=403, detail="Access denied")
    return share_id

def get_dataset(share: Share, db: Session) -> Dataset:
    dataset = db.query(Dataset).filter(Dataset.id == share.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset

def get_table_path(dataset: Dataset, bucket_name: str, share: Share = None) -> str:
    if share and share.watermarked_table_path:
        return get_full_s3_path(bucket_name, share.watermarked_table_path)
    return f"s3://{bucket_name}/{dataset.table_path}"

def transform_schema_for_timestamp_ntz(schema_obj: dict) -> dict:
    if 'fields' in schema_obj:
        for field in schema_obj['fields']:
            if 'type' in field:
                field_type = field['type']
                if isinstance(field_type, str) and 'timestamp_ntz' in field_type:
                    field['type'] = 'string'
                elif isinstance(field_type, dict):
                    if field_type.get('type') == 'timestamp_ntz':
                        field['type'] = {'type': 'string'}
                    elif 'timestamp_ntz' in str(field_type):
                        field['type'] = {'type': 'string'}
    return schema_obj

def get_presigned_url(s3_client, bucket: str, key: str, endpoint_for_client: str, is_localstack: bool) -> str:
    if is_localstack:
        return f"{endpoint_for_client}/{bucket}/{key}"
    else:
        try:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=3600
            )
            if 'localstack:4566' in presigned_url:
                presigned_url = presigned_url.replace('localstack:4566', 'localhost:4566')
            return presigned_url
        except Exception:
            return f"{endpoint_for_client}/{bucket}/{key}"

def cleanup_old_watermarked_files(s3_client, bucket: str, prefix: str, max_age_hours: int = 1):
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].startswith(prefix) and obj['Key'].endswith('.parquet'):
                        if obj['LastModified'].replace(tzinfo=None) < cutoff_time:
                            try:
                                s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
                            except Exception:
                                pass
    except Exception:
        pass

def get_share_from_token(token: str, db: Session) -> Share:
    shares = db.query(Share).filter(Share.revoked == False).all()
    
    matching_share = None
    for share in shares:
        if share.token and share.token == token:
            matching_share = share
            break
        elif share.token_hash and verify_token_hash(token, share.token_hash):
            matching_share = share
            break
    
    if not matching_share:
        raise HTTPException(status_code=401, detail="Invalid share token")
    
    share = matching_share
    
    if SELLER_ID is not None and share.seller_id != SELLER_ID:
        raise HTTPException(status_code=403, detail="This server only serves shares for its configured seller")
    
    if share.revoked:
        raise HTTPException(status_code=401, detail="Share has been revoked")
    
    if is_token_expired(share.expires_at):
        raise HTTPException(status_code=401, detail="Share token expired")
    
    if share.is_trial and is_token_expired(share.trial_expires_at):
        raise HTTPException(status_code=401, detail="Trial access expired")
    
    if share.approval_status != "approved":
        raise HTTPException(status_code=403, detail=f"Share is {share.approval_status}, not approved")
    
    return share

