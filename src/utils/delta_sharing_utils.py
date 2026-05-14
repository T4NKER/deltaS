from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from datetime import timedelta
from jose import jwt, JWTError
import os
import json
from src.seller.database import SellerShare, SellerAuditLog
from src.utils.token_utils import is_token_expired, hash_token
from src.utils.settings import get_settings
from src.utils.time_utils import utcnow

def _parse_seller_id() -> Optional[int]:
    seller_id_str = os.getenv("SELLER_ID")
    if not seller_id_str or not seller_id_str.strip():
        return None
    try:
        return int(seller_id_str.strip())
    except (ValueError, AttributeError):
        return None

SELLER_ID = _parse_seller_id()

def extract_token_from_header(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization")
    return authorization.replace("Bearer ", "")

def validate_share_access(share_name: str, share) -> int:
    if not share_name.startswith("share_"):
        raise HTTPException(status_code=404, detail="Share not found")
    expected_id = int(share_name.replace("share_", ""))
    actual_id = share.share_id if hasattr(share, 'share_id') else share.id
    if expected_id != actual_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return expected_id

EXPECTED_SCHEMA_NAME = "default"

def get_expected_table_name(share) -> str:
    if getattr(share, "table_path", None):
        return share.table_path.rstrip("/").split("/")[-1]
    share_id = share.share_id if hasattr(share, "share_id") else share.id
    return f"table_{share_id}"

def validate_schema_and_table(share, schema_name: str, table_name: str) -> None:
    if schema_name != EXPECTED_SCHEMA_NAME:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema_name}' not found in share",
        )
    expected_table = get_expected_table_name(share)
    if table_name != expected_table:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found in share/{schema_name}",
        )

def get_table_path_from_share(share, bucket_name: str) -> str:
    return f"s3://{bucket_name}/{share.table_path}"

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

def cleanup_old_watermarked_files(s3_client, bucket: str, prefix: str, max_age_hours: int = 1):
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        cutoff_time = utcnow() - timedelta(hours=max_age_hours)
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].startswith(prefix) and obj['Key'].endswith('.parquet'):
                        if obj['LastModified'].replace(tzinfo=None) < cutoff_time:
                            s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
    except Exception:
        pass

def get_share_from_token(token: str, db: Session) -> SellerShare:
    computed_hash = hash_token(token)
    matching_share = db.query(SellerShare).filter(SellerShare.token_hash == computed_hash).first()

    if not matching_share:
        raise HTTPException(status_code=401, detail="Invalid share token")

    return assert_share_is_active(matching_share, db, log_denied=True, mark_used=True)

def assert_share_is_active(share, db: Session, log_denied: bool = False, mark_used: bool = True):
    if SELLER_ID is not None and share.seller_id != SELLER_ID:
        raise HTTPException(status_code=403, detail="This server only serves shares for its configured seller")

    if share.revoked:
        if log_denied:
            from src.utils.audit_chain import seal_audit_entry
            denied_entry = SellerAuditLog(
                buyer_id=share.buyer_id,
                dataset_id=share.dataset_id,
                share_id=share.share_id if hasattr(share, 'share_id') else share.id,
                client_metadata=json.dumps({"denied_reason": "revoked"})
            )
            seal_audit_entry(db, denied_entry)
            db.add(denied_entry)
            db.commit()
        raise HTTPException(status_code=403, detail="Share has been revoked")

    if is_token_expired(share.expires_at):
        raise HTTPException(status_code=401, detail="Share token expired")

    if share.is_trial and is_token_expired(share.trial_expires_at):
        raise HTTPException(status_code=401, detail="Trial access expired")

    if share.approval_status != "approved":
        raise HTTPException(status_code=403, detail=f"Share is {share.approval_status}, not approved")

    if mark_used:
        share.last_used_at = utcnow()
        db.commit()

    return share

OBJECT_DELIVERY_TOKEN_EXPIRY_SECONDS = 3600

def create_delivery_token(
    share_id: int,
    key: str,
    kind: str = "query_file",
    expires_in: int = OBJECT_DELIVERY_TOKEN_EXPIRY_SECONDS,
) -> str:
    settings = get_settings()
    payload = {
        "share_id": int(share_id),
        "key": str(key),
        "kind": str(kind),
        "exp": utcnow() + timedelta(seconds=max(1, expires_in)),
    }
    return jwt.encode(payload, settings.DELIVERY_TOKEN_SECRET_KEY, algorithm=settings.SELLER_JWT_ALGORITHM)

def decode_delivery_token(token: str) -> dict:
    settings = get_settings()
    try:
        return jwt.decode(token, settings.DELIVERY_TOKEN_SECRET_KEY, algorithms=[settings.SELLER_JWT_ALGORITHM])
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid or expired delivery token: {exc}")

def build_delivery_url(base_url: str, token: str) -> str:
    from urllib.parse import quote
    return f"{base_url.rstrip('/')}/delivery/{quote(token, safe='')}/data.parquet"

def get_delivery_base_url(request=None, preferred_base_url: str = None) -> str:
    if preferred_base_url:
        return preferred_base_url.rstrip('/')
    if request is not None:
        return str(request.base_url).rstrip('/')
    for env_name in ("SELLER_PUBLIC_BASE_URL", "SELLER_SERVER_URL", "DELTA_SHARING_SERVER_URL"):
        value = os.getenv(env_name)
        if value:
            return value.rstrip('/')
    raise HTTPException(status_code=500, detail="Unable to determine delivery base URL")
