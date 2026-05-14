import hashlib
import traceback
from datetime import timedelta
from typing import Optional

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.marketplace.auth import (
    create_access_token, get_current_buyer, get_current_seller, get_current_user,
)
from src.marketplace.profile import generate_delta_sharing_profile, generate_profile_json
from src.marketplace.schemas import (
    ApprovalResponse, ProfileListItem, ProfileResponse, PurchaseRequest,
    PurchaseResponse, RejectionResponse, ShareResponse, TokenRotationResponse,
    TrialRequest, TrialResponse, UsageLogResponse,
)
from src.marketplace.services import (
    call_seller_encrypt_token, get_seller_auth_headers, log_audit_event,
    register_share_with_seller, revoke_share_on_seller,
)
from src.models.database import AuditLog, Dataset, Purchase, Share, User, get_db
from src.utils.logging_setup import get_logger
from src.utils.secrets_utils import generate_nonce
from src.utils.settings import get_settings
from src.utils.time_utils import utcnow
from src.utils.token_utils import should_rotate_token

logger = get_logger(__name__)
router = APIRouter()


@router.post("/purchase/{dataset_id}", response_model=PurchaseResponse)
async def purchase_dataset(
    dataset_id: int,
    purchase_request: PurchaseRequest,
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db),
):
    try:
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        if not dataset.is_public and dataset.seller_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Dataset is not public and you are not the seller",
            )

        if dataset.license_terms and not purchase_request.accept_license:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="License acceptance is required before purchase",
            )

        existing_share = db.query(Share).filter(
            Share.dataset_id == dataset_id, Share.buyer_id == current_user.id,
        ).first()
        if existing_share:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You already have access to this dataset",
            )

        settings = get_settings()
        expires_at = utcnow() + timedelta(days=settings.TOKEN_EXPIRY_DAYS)
        approval_status = "pending" if dataset.requires_approval else "approved"

        if approval_status == "approved" and not current_user.public_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Buyer must register a public key before purchasing datasets. Use PUT /me/public-key.",
            )

        share = Share(
            dataset_id=dataset_id,
            seller_id=dataset.seller_id,
            buyer_id=current_user.id,
            encrypted_token=None,
            token_hash=None,
            expires_at=expires_at,
            approval_status=approval_status,
            watermark_nonce=generate_nonce(),
        )
        db.add(share)
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You already have access to this dataset",
            )

        license_accepted_at = utcnow() if dataset.license_terms and purchase_request.accept_license else None
        accepted_license_hash = (
            hashlib.sha256(dataset.license_terms.encode("utf-8")).hexdigest()
            if dataset.license_terms else None
        )
        purchase = Purchase(
            buyer_id=current_user.id,
            dataset_id=dataset_id,
            share_id=share.id,
            amount=dataset.price,
            license_accepted_at=license_accepted_at,
            accepted_license_name=dataset.license_name,
            accepted_license_terms_hash=accepted_license_hash,
        )
        db.add(purchase)

        seller = db.query(User).filter(User.id == dataset.seller_id).first()
        seller_server_url = seller.delta_sharing_server_url if seller else None

        encrypted_token = None
        if approval_status == "approved" and seller and seller.delta_sharing_server_url:
            try:
                marketplace_token = create_access_token({"sub": str(current_user.id)})
                seller_headers = get_seller_auth_headers(seller.delta_sharing_server_url, marketplace_token, seller.id)

                register_share_with_seller(
                    seller.delta_sharing_server_url, seller_headers, share, dataset,
                    buyer_public_key=current_user.public_key,
                )
                encrypt_data = call_seller_encrypt_token(
                    seller.delta_sharing_server_url, seller_headers, share.id,
                    current_user.public_key, current_user.id,
                )
                share.encrypted_token = encrypt_data["encrypted_token"]
                share.token_hash = encrypt_data["token_hash"]

                try:
                    profile = generate_delta_sharing_profile(share, seller)
                    share.profile_json = generate_profile_json(profile)
                    share.profile_generated_at = utcnow()
                except Exception as e:
                    logger.warning("Failed to generate profile on purchase: %s", e)

                encrypted_token = share.encrypted_token
            except requests.exceptions.RequestException as e:
                db.rollback()
                logger.warning("Seller service unreachable during purchase: %s", e)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Seller service is unreachable; purchase aborted: {e}",
                )

        db.commit()
        db.refresh(share)
        db.refresh(purchase)

        log_audit_event(db, "share_purchased", current_user.id, share,
                        extra={"dataset_id": dataset_id, "approval_status": approval_status})

        return PurchaseResponse(
            id=purchase.id,
            buyer_id=purchase.buyer_id,
            dataset_id=purchase.dataset_id,
            share_id=purchase.share_id,
            amount=purchase.amount,
            created_at=purchase.created_at,
            encrypted_token=encrypted_token,
            approval_status=share.approval_status,
            seller_server_url=seller_server_url,
            license_accepted_at=purchase.license_accepted_at,
            accepted_license_name=purchase.accepted_license_name,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("ERROR in purchase_dataset: %s: %s\n%s", type(e).__name__, e, traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Purchase failed: {str(e)}",
        )


@router.post("/datasets/{dataset_id}/trial", response_model=TrialResponse)
async def request_trial(
    dataset_id: int,
    trial_request: TrialRequest,
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db),
):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    if not dataset.is_public and dataset.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Dataset is not public and you are not the seller",
        )

    existing_trial = db.query(Share).filter(
        Share.dataset_id == dataset_id,
        Share.buyer_id == current_user.id,
        Share.is_trial == True,
        Share.revoked == False,
    ).first()
    if existing_trial and existing_trial.trial_expires_at and existing_trial.trial_expires_at > utcnow():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You already have an active trial for this dataset",
        )

    if not current_user.public_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Buyer must register a public key before requesting a trial",
        )

    row_limit = min(trial_request.row_limit or 100, 1000)
    expires_at = utcnow() + timedelta(days=trial_request.days_valid or 7)

    share = Share(
        dataset_id=dataset_id,
        seller_id=dataset.seller_id,
        buyer_id=current_user.id,
        encrypted_token=None,
        token_hash=None,
        expires_at=expires_at,
        approval_status="approved",
        is_trial=True,
        trial_row_limit=row_limit,
        trial_expires_at=expires_at,
        watermark_nonce=generate_nonce(),
    )
    db.add(share)
    db.commit()
    db.refresh(share)

    seller = db.query(User).filter(User.id == dataset.seller_id).first()
    if seller and seller.delta_sharing_server_url:
        try:
            marketplace_token = create_access_token({"sub": str(current_user.id)})
            seller_headers = get_seller_auth_headers(seller.delta_sharing_server_url, marketplace_token, seller.id)
            register_share_with_seller(
                seller.delta_sharing_server_url, seller_headers, share, dataset,
                buyer_public_key=current_user.public_key,
            )
            encrypt_data = call_seller_encrypt_token(
                seller.delta_sharing_server_url, seller_headers, share.id,
                current_user.public_key, current_user.id,
            )
            share.encrypted_token = encrypt_data["encrypted_token"]
            share.token_hash = encrypt_data["token_hash"]
            profile = generate_delta_sharing_profile(share, seller)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = utcnow()
            db.commit()
        except requests.exceptions.RequestException as e:
            db.delete(share)
            db.commit()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to encrypt token via seller server: {str(e)}",
            )
        except Exception as e:
            logger.warning("Failed to generate profile for trial: %s", e)

    seller_server_url = seller.delta_sharing_server_url if seller else None
    return TrialResponse(
        id=share.id,
        buyer_id=share.buyer_id,
        dataset_id=share.dataset_id,
        share_id=share.id,
        encrypted_token=share.encrypted_token,
        approval_status=share.approval_status,
        seller_server_url=seller_server_url,
        is_trial=True,
        trial_row_limit=row_limit,
        trial_expires_at=expires_at,
    )


@router.get("/my-shares", response_model=list[ShareResponse])
async def get_my_shares(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    shares = db.query(Share).filter(
        or_(Share.seller_id == current_user.id, Share.buyer_id == current_user.id),
    ).all()
    return [
        ShareResponse(
            id=s.id, dataset_id=s.dataset_id, dataset_name=s.dataset.name,
            seller_id=s.seller_id, buyer_id=s.buyer_id,
            encrypted_token=s.encrypted_token, created_at=s.created_at,
            expires_at=s.expires_at, approval_status=s.approval_status,
            revoked=s.revoked, revoked_at=s.revoked_at,
        )
        for s in shares
    ]


@router.post("/shares/{share_id}/rotate-token", response_model=TokenRotationResponse, status_code=status.HTTP_200_OK)
async def rotate_share_token(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Share not found")
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only rotate tokens for your own shares",
        )
    if share.revoked:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot rotate token for revoked share",
        )

    settings = get_settings()
    rotation_recommended = should_rotate_token(
        share.created_at, share.last_used_at,
        settings.TOKEN_ROTATION_DAYS, settings.TOKEN_INACTIVITY_DAYS,
    )

    buyer = db.query(User).filter(User.id == share.buyer_id).first()
    if not buyer or not buyer.public_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Buyer must have a registered public key for token rotation",
        )

    seller = db.query(User).filter(User.id == share.seller_id).first()
    if seller and seller.delta_sharing_server_url:
        try:
            marketplace_token = create_access_token({"sub": str(current_user.id)})
            seller_headers = get_seller_auth_headers(seller.delta_sharing_server_url, marketplace_token, seller.id)
            encrypt_data = call_seller_encrypt_token(
                seller.delta_sharing_server_url, seller_headers, share_id, buyer.public_key, buyer.id,
            )
            share.encrypted_token = encrypt_data["encrypted_token"]
            share.token_hash = encrypt_data["token_hash"]
            share.token_rotated_at = utcnow()
            profile = generate_delta_sharing_profile(share, seller)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = utcnow()
            db.commit()
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to encrypt new token via seller server: {str(e)}",
            )
        except Exception as e:
            logger.warning("Failed to regenerate profile after token rotation: %s", e)

    return TokenRotationResponse(
        status="success",
        message="Token rotated successfully",
        share_id=share_id,
        rotation_recommended=rotation_recommended,
    )


@router.delete("/shares/{share_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_share(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Share not found")
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only revoke your own shares",
        )

    share.revoked = True
    share.revoked_at = utcnow()
    share.profile_json = None

    seller = db.query(User).filter(User.id == share.seller_id).first()
    if seller and seller.delta_sharing_server_url:
        try:
            marketplace_token = create_access_token({"sub": str(current_user.id)})
            seller_headers = get_seller_auth_headers(seller.delta_sharing_server_url, marketplace_token, seller.id)
            revoke_share_on_seller(seller.delta_sharing_server_url, seller_headers, share.id)
        except requests.exceptions.RequestException as e:
            logger.warning("Failed to sync revocation with seller: %s", e)
        except Exception as e:
            logger.error("Error during revocation sync: %s", e)

    log_audit_event(db, "share_revoked", current_user.id, share)

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("ERROR in revoke_share: %s\n%s", e, traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to revoke share: {str(e)}",
        )
    return None


@router.post("/shares/{share_id}/approve", response_model=ApprovalResponse, status_code=status.HTTP_200_OK)
async def approve_share(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Share not found")
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only approve your own shares",
        )
    if share.approval_status != "pending":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Share cannot be approved: current status is '{share.approval_status}', expected 'pending'",
        )

    share.approval_status = "approved"

    buyer = db.query(User).filter(User.id == share.buyer_id).first()
    if not buyer or not buyer.public_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Buyer must register a public key before share can be approved",
        )

    seller = db.query(User).filter(User.id == share.seller_id).first()
    if seller and seller.delta_sharing_server_url:
        try:
            marketplace_token = create_access_token({"sub": str(current_user.id)})
            seller_headers = get_seller_auth_headers(seller.delta_sharing_server_url, marketplace_token, seller.id)
            dataset = db.query(Dataset).filter(Dataset.id == share.dataset_id).first()
            if dataset:
                register_share_with_seller(
                    seller.delta_sharing_server_url, seller_headers, share, dataset,
                    buyer_public_key=buyer.public_key,
                )
            encrypt_data = call_seller_encrypt_token(
                seller.delta_sharing_server_url, seller_headers, share_id, buyer.public_key, buyer.id,
            )
            share.encrypted_token = encrypt_data["encrypted_token"]
            share.token_hash = encrypt_data["token_hash"]
            profile = generate_delta_sharing_profile(share, seller)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = utcnow()
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to encrypt token via seller server: {str(e)}",
            )
        except Exception as e:
            logger.warning("Failed to generate profile on approval: %s", e)

    db.commit()
    log_audit_event(db, "share_approved", current_user.id, share)

    return ApprovalResponse(
        status="success",
        message="Share approved",
        share_id=share_id,
        approval_status=share.approval_status,
        profile_generated=share.profile_json is not None,
    )


@router.post("/shares/{share_id}/reject", response_model=RejectionResponse, status_code=status.HTTP_200_OK)
async def reject_share(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Share not found")
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only reject your own shares",
        )
    if share.approval_status != "pending":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Share cannot be rejected: current status is '{share.approval_status}', expected 'pending'",
        )

    share.approval_status = "rejected"
    db.commit()
    log_audit_event(db, "share_rejected", current_user.id, share)

    return RejectionResponse(
        status="success",
        message="Share rejected",
        share_id=share_id,
        approval_status=share.approval_status,
    )


@router.get("/shares/{share_id}/profile", response_model=ProfileResponse)
async def get_share_profile(
    share_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Share not found")
    if share.buyer_id != current_user.id and share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only access profiles for your own shares",
        )

    if not share.profile_json:
        if share.approval_status != "approved":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Profile not available. Share status: {share.approval_status}",
            )
        seller = db.query(User).filter(User.id == share.seller_id).first()
        if not seller or not seller.delta_sharing_server_url:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Seller server URL not configured",
            )
        buyer = db.query(User).filter(User.id == share.buyer_id).first()
        if not buyer or not buyer.public_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Buyer must register a public key before profile can be generated",
            )
        try:
            if not share.encrypted_token:
                marketplace_token = create_access_token({"sub": str(current_user.id)})
                seller_headers = get_seller_auth_headers(seller.delta_sharing_server_url, marketplace_token, seller.id)
                dataset = db.query(Dataset).filter(Dataset.id == share.dataset_id).first()
                if dataset:
                    register_share_with_seller(
                        seller.delta_sharing_server_url, seller_headers, share, dataset,
                        buyer_public_key=buyer.public_key,
                    )
                encrypt_data = call_seller_encrypt_token(
                    seller.delta_sharing_server_url, seller_headers, share_id, buyer.public_key, buyer.id,
                )
                share.encrypted_token = encrypt_data["encrypted_token"]
                share.token_hash = encrypt_data["token_hash"]
            profile = generate_delta_sharing_profile(share, seller)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = utcnow()
            db.commit()
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to encrypt token via seller server: {str(e)}",
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to generate profile: {str(e)}",
            )

    return ProfileResponse(
        share_id=share.id,
        profile_json=share.profile_json,
        generated_at=share.profile_generated_at or share.created_at,
    )


@router.get("/my-profiles", response_model=list[ProfileListItem])
async def get_my_profiles(
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db),
):
    shares = db.query(Share).filter(
        Share.buyer_id == current_user.id,
        Share.approval_status == "approved",
        Share.revoked == False,
    ).all()
    return [
        ProfileListItem(
            share_id=s.id,
            dataset_id=s.dataset_id,
            dataset_name=s.dataset.name,
            profile_json=s.profile_json,
            generated_at=s.profile_generated_at or s.created_at,
            expires_at=s.expires_at,
        )
        for s in shares if s.profile_json
    ]


@router.get("/usage-logs", response_model=list[UsageLogResponse])
async def get_usage_logs(
    dataset_id: Optional[int] = None,
    share_id: Optional[int] = None,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    query = db.query(AuditLog).join(Share).filter(Share.seller_id == current_user.id)
    if dataset_id:
        query = query.filter(AuditLog.dataset_id == dataset_id)
    if share_id:
        query = query.filter(AuditLog.share_id == share_id)
    logs = query.order_by(AuditLog.query_time.desc()).limit(100).all()
    return [
        UsageLogResponse(
            id=log.id, buyer_id=log.buyer_id, dataset_id=log.dataset_id,
            share_id=log.share_id, query_time=log.query_time,
            columns_requested=log.columns_requested,
            row_count_returned=log.row_count_returned,
            query_limit=log.query_limit, ip_address=log.ip_address,
        )
        for log in logs
    ]
