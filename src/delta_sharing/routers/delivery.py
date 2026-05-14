from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from src.seller.database import SellerShare, get_seller_db
from src.utils.delta_sharing_utils import assert_share_is_active, decode_delivery_token
from src.utils.s3_utils import get_bucket_name, get_s3_client

router = APIRouter()


@router.get("/delivery/{token}/data.parquet")
async def download_delivery_object(token: str, db: Session = Depends(get_seller_db)):
    payload = decode_delivery_token(token)
    share_id = payload.get("share_id")
    key = payload.get("key")
    kind = payload.get("kind")

    if not share_id or not key:
        raise HTTPException(status_code=400, detail="Delivery token missing share_id or key")

    share = db.query(SellerShare).filter(SellerShare.share_id == int(share_id)).first()
    if not share:
        raise HTTPException(status_code=404, detail="Share not found")

    assert_share_is_active(share, db, log_denied=True, mark_used=True)

    if kind != "query_file":
        raise HTTPException(status_code=403, detail="Unsupported delivery token kind")
    if ".." in key or key.startswith("/"):
        raise HTTPException(status_code=403, detail="Delivery key contains illegal path traversal")

    share_table_path = (share.table_path or "").rstrip("/")
    if not share_table_path or not (key == share_table_path or key.startswith(share_table_path + "/")):
        raise HTTPException(status_code=403, detail="Delivery key is not scoped to this share")

    try:
        response = get_s3_client().get_object(Bucket=get_bucket_name(), Key=key)
        headers = {"Content-Type": "application/octet-stream"}
        if response.get("ContentLength") is not None:
            headers["Content-Length"] = str(response["ContentLength"])
        return StreamingResponse(
            response["Body"].iter_chunks(),
            media_type="application/octet-stream",
            headers=headers,
        )
    except HTTPException:
        raise
    except Exception as e:
        message = str(e)
        if "NoSuchKey" in message or "not exist" in message.lower():
            raise HTTPException(status_code=404, detail="Requested object is no longer available")
        raise HTTPException(status_code=500, detail=f"Failed to deliver object: {message}")
