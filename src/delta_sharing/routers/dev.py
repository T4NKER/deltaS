from fastapi import APIRouter, HTTPException, Header, Request
from deltalake import write_deltalake
import pyarrow as pa
import pandas as pd

from src.utils.s3_utils import get_s3_client, get_delta_storage_options, get_full_s3_path, get_bucket_name
from src.utils.delta_sharing_utils import extract_token_from_header
from src.utils.settings import get_settings
from src.utils.logging_setup import get_logger
from src.delta_sharing.auth import decode_seller_token

logger = get_logger(__name__)


def _require_dev_mode_and_seller_auth(authorization: str):
    settings = get_settings()
    if not getattr(settings, "ALLOW_INSECURE_DEFAULTS", False):
        raise HTTPException(status_code=404, detail="Not Found")
    token = extract_token_from_header(authorization)
    payload = decode_seller_token(token)
    if payload.get("role") not in ("seller", "admin"):
        raise HTTPException(status_code=403, detail="Seller token required")


router = APIRouter()


@router.post("/seller/seed-test-data")
async def seed_test_data(request: Request, authorization: str = Header(None)):
    _require_dev_mode_and_seller_auth(authorization)

    body = await request.json()
    table_path = body.get("table_path")
    data = body.get("data")

    if not table_path or not data:
        raise HTTPException(status_code=400, detail="table_path and data are required")

    try:
        df = pd.DataFrame(data)
        bucket_name = get_bucket_name()
        full_path = get_full_s3_path(bucket_name, table_path)
        storage_options = get_delta_storage_options()
        table = pa.Table.from_pandas(df)
        write_deltalake(full_path, table, mode='overwrite', storage_options=storage_options)
        return {"status": "success", "rows": len(df), "table_path": table_path}
    except Exception as e:
        logger.error(f"[seed-test-data] unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Failed to write test data")

@router.post("/seller/ensure-bucket")
async def ensure_bucket(authorization: str = Header(None)):
    _require_dev_mode_and_seller_auth(authorization)

    try:
        s3_client = get_s3_client()
        bucket_name = get_bucket_name()
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            s3_client.create_bucket(Bucket=bucket_name)
        return {"status": "success", "bucket": bucket_name}
    except Exception as e:
        logger.error(f"[ensure-bucket] unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Failed to ensure bucket")
