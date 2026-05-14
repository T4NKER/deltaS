from fastapi import APIRouter, Depends, HTTPException, Header, Request
from sqlalchemy.orm import Session
import hashlib
import hmac
import traceback
import pandas as pd

from src.seller.database import get_seller_db, SellerShare
from src.seller.publish import publish_dataset_metadata
from src.seller.synthetic_data import generate_synthetic_data
from src.seller.fingerprinting import generate_buyer_fingerprint, verify_fingerprint
from src.seller.schemas import (
    PublishMetadataRequest, SyntheticDataRequest,
    SellerAuthTokenRequest, SellerAuthTokenResponse,
    EncryptTokenRequest, EncryptTokenResponse,
    RegisterShareRequest, RegisterShareResponse,
    RevokeShareRequest, RevokeShareResponse,
)
from src.utils.encryption import encrypt_token
from src.utils.token_utils import generate_share_token, hash_token
from src.utils.delta_sharing_utils import extract_token_from_header
from src.utils.data_utils import parse_anchor_columns
from src.utils.marketplace_jwt import JWTError as MarketplaceJWTError, decode_marketplace_jwt
from src.utils.time_utils import utcnow
from src.utils.logging_setup import get_logger
from src.delta_sharing.auth import create_seller_token, decode_seller_token

logger = get_logger(__name__)


router = APIRouter()


def _get_seller_id_from_header(authorization: str, action: str) -> int:
    token = extract_token_from_header(authorization)
    try:
        payload = decode_seller_token(token)
        if payload.get("role") != "seller":
            raise HTTPException(status_code=403, detail=f"Only sellers can {action}")
        return int(payload.get("sub"))
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Authentication failed: %s", e)
        raise HTTPException(status_code=401, detail="Invalid authentication")


@router.post("/seller/publish-metadata")
async def publish_metadata(
    request_data: PublishMetadataRequest,
    authorization: str = Header(None),
    db: Session = Depends(get_seller_db)
):
    user_id = _get_seller_id_from_header(authorization, "publish metadata")
    anchor_cols_list = parse_anchor_columns(request_data.anchor_columns) if request_data.anchor_columns else None

    try:
        metadata = publish_dataset_metadata(
            table_path=request_data.table_path,
            seller_id=user_id,
            name=request_data.name,
            description=request_data.description,
            license_name=request_data.license_name,
            license_terms=request_data.license_terms,
            anchor_columns=anchor_cols_list
        )
        return metadata
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to publish metadata: {str(e)}"
        )

@router.post("/seller/generate-synthetic")
async def generate_synthetic_dataset(
    request: SyntheticDataRequest,
    authorization: str = Header(None),
    db: Session = Depends(get_seller_db)
):
    _get_seller_id_from_header(authorization, "generate synthetic data")
    try:
        synthetic_df, metadata = generate_synthetic_data(
            table_path=request.table_path,
            output_table_path=request.output_table_path,
            num_rows=request.num_rows,
            dp_epsilon=request.dp_epsilon,
            preserve_statistics=request.preserve_statistics,
            seed=request.seed
        )

        return {
            "status": "success",
            "output_table_path": request.output_table_path,
            "num_rows": len(synthetic_df),
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to generate synthetic data: {str(e)}"
        )

@router.post("/seller/encrypt-token", response_model=EncryptTokenResponse)
async def encrypt_share_token(
    request: EncryptTokenRequest,
    authorization: str = Header(None),
    db: Session = Depends(get_seller_db)
):
    user_id = _get_seller_id_from_header(authorization, "encrypt tokens")

    share = db.query(SellerShare).filter(SellerShare.share_id == request.share_id).first()
    if not share:
        raise HTTPException(status_code=404, detail=f"Share not found: share_id={request.share_id}")

    if share.seller_id != user_id:
        raise HTTPException(status_code=403, detail="You can only encrypt tokens for your own shares")

    if request.buyer_id and share.buyer_id and share.buyer_id != request.buyer_id:
        raise HTTPException(status_code=403, detail="Buyer ID mismatch - potential impersonation attempt")

    if share.buyer_public_key_hash:
        presented_hash = hashlib.sha256(request.buyer_public_key.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(presented_hash, share.buyer_public_key_hash):
            raise HTTPException(
                status_code=403,
                detail="buyer_public_key does not match the key registered with this share",
            )

    try:
        share_token = generate_share_token()
        token_hash = hash_token(share_token)
        encrypted_token = encrypt_token(share_token, request.buyer_public_key)
    except Exception as e:
        logger.error("[encrypt-token] unexpected error: %s: %s", type(e).__name__, e)
        logger.info(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to encrypt token")

    share.token_hash = token_hash
    db.commit()

    return EncryptTokenResponse(
        encrypted_token=encrypted_token,
        token_hash=token_hash,
    )

@router.post("/seller/detect-fingerprint")
async def detect_fingerprint(
    request: Request,
    authorization: str = Header(None),
    db: Session = Depends(get_seller_db)
):
    user_id = _get_seller_id_from_header(authorization, "detect fingerprints")

    body = await request.json()
    suspicious_data = body.get("data")
    anchor_columns = body.get("anchor_columns", [])

    if not suspicious_data:
        raise HTTPException(status_code=400, detail="data field required")

    try:
        df = pd.DataFrame(suspicious_data)
    except Exception as e:
        logger.warning("Invalid data format: %s", e)
        raise HTTPException(status_code=400, detail="Invalid data format")

    shares = db.query(SellerShare).filter(
        SellerShare.seller_id == user_id,
        SellerShare.revoked == False,
        SellerShare.approval_status == "approved"
    ).all()

    matches = []
    for share in shares:
        try:
            fingerprint = generate_buyer_fingerprint(share.buyer_id, share.share_id)

            share_anchor_columns = anchor_columns
            if not share_anchor_columns and share.anchor_columns:
                share_anchor_columns = parse_anchor_columns(share.anchor_columns)

            if share_anchor_columns:
                result = verify_fingerprint(df, fingerprint, share_anchor_columns)
                if result.get("found"):
                    matches.append({
                        "share_id": share.share_id,
                        "buyer_id": share.buyer_id,
                        "match_rate": result.get("overall_match_rate", 0.0),
                        "details": result
                    })
        except Exception as e:
            continue

    return {
        "matches_found": len(matches) > 0,
        "matches": matches
    }

@router.post("/seller/auth-token", response_model=SellerAuthTokenResponse)
async def generate_seller_auth_token(
    request: SellerAuthTokenRequest,
    db: Session = Depends(get_seller_db)
):
    seller_id = request.seller_id
    try:
        payload = decode_marketplace_jwt(request.marketplace_token)
        role = payload.get("role", "")

        if role not in ("seller", "admin", "marketplace"):
            raise HTTPException(status_code=403, detail="Only sellers can obtain seller tokens")

        sub = payload.get("sub")
        if role in ("seller", "admin"):
            try:
                user_id = int(sub)
            except (TypeError, ValueError):
                raise HTTPException(status_code=401, detail="Invalid sub claim for user role")
            if seller_id is None:
                seller_id = user_id
            if role == "seller" and int(seller_id) != user_id:
                raise HTTPException(status_code=403, detail="Cannot obtain token for another seller")
        else:
            if payload.get("aud") != "seller-auth":
                raise HTTPException(status_code=403, detail="Marketplace token has invalid audience")
            if seller_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="seller_id required when brokering with marketplace service token",
                )

        seller_token = create_seller_token({"sub": str(seller_id), "role": "seller"})
        return SellerAuthTokenResponse(seller_token=seller_token, seller_id=seller_id)
    except MarketplaceJWTError:
        raise HTTPException(status_code=401, detail="Invalid marketplace token")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to generate seller token: %s", e)
        raise HTTPException(status_code=500, detail="Failed to generate seller token")

@router.post("/seller/register-share", response_model=RegisterShareResponse)
async def register_share(
    request: RegisterShareRequest,
    authorization: str = Header(None),
    db: Session = Depends(get_seller_db),
):
    caller_seller_id = _get_seller_id_from_header(authorization, "register shares")

    if int(request.seller_id) != caller_seller_id:
        raise HTTPException(
            status_code=403,
            detail="seller_id in body must match the authenticated caller",
        )

    existing = db.query(SellerShare).filter(SellerShare.share_id == request.share_id).first()
    if existing and existing.seller_id != caller_seller_id:
        raise HTTPException(
            status_code=403,
            detail="Share already exists under a different seller and cannot be overwritten",
        )

    body = request.model_dump(exclude_none=False)
    if existing:
        for key, value in body.items():
            if hasattr(existing, key):
                setattr(existing, key, value)
    else:
        db.add(SellerShare(**{k: v for k, v in body.items() if hasattr(SellerShare, k)}))
    db.commit()
    return RegisterShareResponse(status="success", share_id=request.share_id)

@router.post("/seller/revoke-share", response_model=RevokeShareResponse)
async def revoke_share_seller(
    request: RevokeShareRequest,
    authorization: str = Header(None),
    db: Session = Depends(get_seller_db),
):
    caller_seller_id = _get_seller_id_from_header(authorization, "revoke shares")

    share = db.query(SellerShare).filter(SellerShare.share_id == request.share_id).first()
    if share is None:
        return RevokeShareResponse(status="success", share_id=request.share_id)

    if share.seller_id != caller_seller_id:
        raise HTTPException(
            status_code=403,
            detail="Cannot revoke share belonging to another seller",
        )

    share.revoked = True
    share.revoked_at = utcnow()
    db.commit()
    return RevokeShareResponse(status="success", share_id=request.share_id)
