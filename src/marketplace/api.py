from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import or_
from typing import Optional
from datetime import datetime, timedelta
import secrets
import hashlib
import json
import requests
import os
import traceback
from src.models.database import (
    init_db, User, Dataset, Share, Purchase, AuditLog, get_db
)
from src.marketplace.auth import (
    get_password_hash, verify_password, create_access_token,
    get_current_user, get_current_seller, get_current_buyer
)
from src.marketplace.schemas import (
    UserRegister, UserLogin, Token, UserResponse,
    DatasetCreate, DatasetResponse, PurchaseResponse, TrialRequest, TrialResponse, ProfileResponse, FileDownloadRequest,
    DeltaSharingServerUrlRequest, ShareResponse, TokenRotationResponse, ApprovalResponse, RejectionResponse,
    ProfileListItem, UsageLogResponse, FileDownloadResponse, FileDownloadRevokeResponse
)
from src.seller.publish import validate_metadata_signature
from src.marketplace.schemas import DatasetMetadataBundle
from src.utils.token_utils import generate_share_token, hash_token, should_rotate_token, is_token_expired
from src.utils.settings import get_settings
from src.seller.profile_generator import generate_delta_sharing_profile, generate_profile_json
from src.marketplace.file_delivery import generate_file_download_link, revoke_file_download_link, get_file_delivery_metrics

app = FastAPI(title="Delta Sharing Marketplace API")

@app.on_event("startup")
async def startup_event():
    init_db()

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.email == user_data.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    if user_data.role not in ["buyer", "seller"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'buyer' or 'seller'"
        )
    
    hashed_password = get_password_hash(user_data.password)
    user = User(
        email=user_data.email,
        hashed_password=hashed_password,
        role=user_data.role
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

@app.post("/login", response_model=Token)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == credentials.email).first()
    if not user or not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=30)
    access_token = create_access_token(
        data={"sub": str(user.id)}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    return current_user

@app.put("/me/delta-sharing-server-url")
async def update_delta_sharing_server_url(
    request: DeltaSharingServerUrlRequest,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    current_user.delta_sharing_server_url = request.server_url
    db.commit()
    db.refresh(current_user)
    return {"delta_sharing_server_url": current_user.delta_sharing_server_url}

@app.get("/datasets", response_model=list[DatasetResponse])
async def list_datasets(
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db)
):
    if current_user.role == "seller":
        datasets = db.query(Dataset).filter(Dataset.seller_id == current_user.id).all()
    else:
        datasets = db.query(Dataset).filter(Dataset.is_public == True).all()
    return datasets

@app.post("/datasets", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    dataset_data: DatasetCreate,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    if dataset_data.metadata_bundle:
        metadata = dataset_data.metadata_bundle.dict()
        
        if not validate_metadata_signature(metadata, current_user.id):
            raise HTTPException(
                status_code=400,
                detail="Invalid metadata signature. Metadata bundle must be signed by the seller."
            )
        
        if metadata.get("seller_id") != current_user.id:
            raise HTTPException(
                status_code=400,
                detail="Metadata bundle seller_id does not match authenticated seller."
            )
        
        anchor_columns = ','.join(metadata.get("anchor_columns", []))
        pii_analysis = metadata.get("pii_analysis", {})
        risk_score = pii_analysis.get("risk_score", 0.0)
        risk_level = pii_analysis.get("risk_level", "low")
        sensitive_columns = json.dumps(pii_analysis.get("sensitive_columns", {}))
        detected_pii_types = ','.join(pii_analysis.get("pii_types", {}).keys())
        
        dataset = Dataset(
            name=metadata.get("name") or dataset_data.name,
            description=metadata.get("description") or dataset_data.description,
            table_path=metadata.get("table_path") or dataset_data.table_path,
            price=dataset_data.price,
            is_public=dataset_data.is_public,
            seller_id=current_user.id,
            anchor_columns=anchor_columns,
            risk_score=risk_score,
            risk_level=risk_level,
            sensitive_columns=sensitive_columns,
            detected_pii_types=detected_pii_types,
            requires_approval=risk_score >= 20.0
        )
    else:
        anchor_columns = dataset_data.anchor_columns if dataset_data.anchor_columns and dataset_data.anchor_columns.strip() else None
        
        if not anchor_columns:
            raise HTTPException(
                status_code=400,
                detail="metadata_bundle is required. Please provide a signed metadata bundle from the seller publish endpoint."
            )
        
        dataset = Dataset(
            name=dataset_data.name,
            description=dataset_data.description,
            table_path=dataset_data.table_path,
            price=dataset_data.price,
            is_public=dataset_data.is_public,
            seller_id=current_user.id,
            anchor_columns=anchor_columns
        )
    
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset

@app.post("/purchase/{dataset_id}", response_model=PurchaseResponse)
async def purchase_dataset(
    dataset_id: int,
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db)
):
    try:
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dataset not found"
            )
        
        if not dataset.is_public and dataset.seller_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Dataset is not public and you are not the seller"
            )
        
        existing_share = db.query(Share).filter(
            Share.dataset_id == dataset_id,
            Share.buyer_id == current_user.id
        ).first()
        
        if existing_share:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You already have access to this dataset"
            )
        
        share_token = generate_share_token()
        token_hash = hash_token(share_token)
        settings = get_settings()
        expires_at = datetime.utcnow() + timedelta(days=settings.TOKEN_EXPIRY_DAYS)
        
        approval_status = "pending" if dataset.requires_approval else "approved"
        
        share = Share(
            dataset_id=dataset_id,
            seller_id=dataset.seller_id,
            buyer_id=current_user.id,
            token=share_token,
            token_hash=token_hash,
            expires_at=expires_at,
            approval_status=approval_status
        )
        db.add(share)
        db.commit()
        db.refresh(share)
        
        
        purchase = Purchase(
            buyer_id=current_user.id,
            dataset_id=dataset_id,
            share_id=share.id,
            amount=dataset.price
        )
        db.add(purchase)
        db.commit()
        db.refresh(purchase)
        
        approval_status_value = getattr(share, 'approval_status', None) or approval_status
        
        seller = db.query(User).filter(User.id == dataset.seller_id).first()
        seller_server_url = seller.delta_sharing_server_url if seller else None
        
        response_data = {
            "id": purchase.id,
            "buyer_id": purchase.buyer_id,
            "dataset_id": purchase.dataset_id,
            "share_id": purchase.share_id,
            "amount": purchase.amount,
            "created_at": purchase.created_at,
            "share_token": share_token,
            "approval_status": approval_status_value,
            "seller_server_url": seller_server_url
        }
        
        return PurchaseResponse(**response_data)
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"ERROR in purchase_dataset: {error_detail}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Purchase failed: {str(e)}"
        )

@app.post("/datasets/{dataset_id}/trial", response_model=TrialResponse)
async def request_trial(
    dataset_id: int,
    trial_request: TrialRequest,
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db)
):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    if not dataset.is_public and dataset.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Dataset is not public and you are not the seller"
        )
    
    existing_trial = db.query(Share).filter(
        Share.dataset_id == dataset_id,
        Share.buyer_id == current_user.id,
        Share.is_trial == True,
        Share.revoked == False
    ).first()
    
    if existing_trial:
        if existing_trial.trial_expires_at and existing_trial.trial_expires_at > datetime.utcnow():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You already have an active trial for this dataset"
            )
    
    share_token = generate_share_token()
    token_hash = hash_token(share_token)
    row_limit = min(trial_request.row_limit or 100, 1000)
    expires_at = datetime.utcnow() + timedelta(days=trial_request.days_valid or 7)
    
    share = Share(
        dataset_id=dataset_id,
        seller_id=dataset.seller_id,
        buyer_id=current_user.id,
        token=share_token,
        token_hash=token_hash,
        expires_at=expires_at,
        approval_status="approved",
        is_trial=True,
        trial_row_limit=row_limit,
        trial_expires_at=expires_at
    )
    db.add(share)
    db.commit()
    db.refresh(share)
    
    seller = db.query(User).filter(User.id == dataset.seller_id).first()
    seller_server_url = seller.delta_sharing_server_url if seller else None
    
    return TrialResponse(
        id=share.id,
        buyer_id=share.buyer_id,
        dataset_id=share.dataset_id,
        share_id=share.id,
        share_token=share_token,
        approval_status=share.approval_status,
        seller_server_url=seller_server_url,
        is_trial=True,
        trial_row_limit=row_limit,
        trial_expires_at=expires_at
    )

@app.get("/my-datasets", response_model=list[DatasetResponse])
async def get_my_datasets(
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    datasets = db.query(Dataset).filter(Dataset.seller_id == current_user.id).all()
    return datasets

@app.get("/my-shares", response_model=list[ShareResponse])
async def get_my_shares(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    shares = db.query(Share).filter(
        or_(Share.seller_id == current_user.id, Share.buyer_id == current_user.id)
    ).all()
    return [
        ShareResponse(
            id=share.id,
            dataset_id=share.dataset_id,
            dataset_name=share.dataset.name,
            seller_id=share.seller_id,
            buyer_id=share.buyer_id,
            token=share.token if share.token else "[REDACTED]",
            created_at=share.created_at,
            expires_at=share.expires_at,
            approval_status=share.approval_status,
            revoked=share.revoked,
            revoked_at=share.revoked_at
        )
        for share in shares
    ]

@app.post("/shares/{share_id}/rotate-token", response_model=TokenRotationResponse, status_code=status.HTTP_200_OK)
async def rotate_share_token(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only rotate tokens for your own shares"
        )
    
    if share.revoked:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot rotate token for revoked share"
        )
    
    new_token = generate_share_token()
    new_token_hash = hash_token(new_token)
    
    share.token = new_token
    share.token_hash = new_token_hash
    share.token_rotated_at = datetime.utcnow()
    
    seller = db.query(User).filter(User.id == share.seller_id).first()
    if seller and seller.delta_sharing_server_url and share.approval_status == "approved":
        try:
            profile = generate_delta_sharing_profile(share, seller, new_token)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = datetime.utcnow()
        except Exception as e:
            print(f"Warning: Failed to regenerate profile after token rotation: {e}")
    
    db.commit()
    
    return TokenRotationResponse(
        status="success",
        message="Token rotated successfully",
        share_id=share_id,
        new_token=new_token
    )

@app.delete("/shares/{share_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_share(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only revoke your own shares"
        )
    
    share.revoked = True
    share.revoked_at = datetime.utcnow()
    share.profile_json = None
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        import traceback
        error_detail = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"ERROR in revoke_share: {error_detail}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to revoke share: {str(e)}"
        )
    
    return None

@app.post("/shares/{share_id}/approve", response_model=ApprovalResponse, status_code=status.HTTP_200_OK)
async def approve_share(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only approve your own shares"
        )
    
    share.approval_status = "approved"
    
    seller = db.query(User).filter(User.id == share.seller_id).first()
    if seller and seller.delta_sharing_server_url:
        try:
            if not share.token:
                share_token = generate_share_token()
                share.token = share_token
                share.token_hash = hash_token(share_token)
            
            profile = generate_delta_sharing_profile(share, seller, share.token)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = datetime.utcnow()
        except Exception as e:
            print(f"Warning: Failed to generate profile on approval: {e}")
    
    db.commit()
    
    return ApprovalResponse(
        status="success",
        message="Share approved",
        share_id=share_id,
        approval_status=share.approval_status,
        profile_generated=share.profile_json is not None
    )

@app.post("/shares/{share_id}/reject", response_model=RejectionResponse, status_code=status.HTTP_200_OK)
async def reject_share(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only reject your own shares"
        )
    
    share.approval_status = "rejected"
    db.commit()
    
    return RejectionResponse(
        status="success",
        message="Share rejected",
        share_id=share_id,
        approval_status=share.approval_status
    )

@app.get("/shares/{share_id}/profile", response_model=ProfileResponse)
async def get_share_profile(
    share_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.buyer_id != current_user.id and share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only access profiles for your own shares"
        )
    
    if not share.profile_json:
        if share.approval_status != "approved":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Profile not available. Share status: {share.approval_status}"
            )
        
        seller = db.query(User).filter(User.id == share.seller_id).first()
        if not seller or not seller.delta_sharing_server_url:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Seller server URL not configured"
            )
        
        try:
            if not share.token:
                share_token = generate_share_token()
                share.token = share_token
                share.token_hash = hash_token(share_token)
            
            profile = generate_delta_sharing_profile(share, seller, share.token)
            share.profile_json = generate_profile_json(profile)
            share.profile_generated_at = datetime.utcnow()
            db.commit()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to generate profile: {str(e)}"
            )
    
    return ProfileResponse(
        share_id=share.id,
        profile_json=share.profile_json,
        generated_at=share.profile_generated_at or share.created_at
    )

@app.get("/my-profiles", response_model=list[ProfileListItem])
async def get_my_profiles(
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db)
):
    shares = db.query(Share).filter(
        Share.buyer_id == current_user.id,
        Share.approval_status == "approved",
        Share.revoked == False
    ).all()
    
    profiles = []
    for share in shares:
        if share.profile_json:
            profiles.append(ProfileListItem(
                share_id=share.id,
                dataset_id=share.dataset_id,
                dataset_name=share.dataset.name,
                profile_json=share.profile_json,
                generated_at=share.profile_generated_at or share.created_at,
                expires_at=share.expires_at
            ))
    
    return profiles

@app.get("/usage-logs", response_model=list[UsageLogResponse])
async def get_usage_logs(
    dataset_id: Optional[int] = None,
    share_id: Optional[int] = None,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    query = db.query(AuditLog).join(Share).filter(Share.seller_id == current_user.id)
    
    if dataset_id:
        query = query.filter(AuditLog.dataset_id == dataset_id)
    
    if share_id:
        query = query.filter(AuditLog.share_id == share_id)
    
    logs = query.order_by(AuditLog.query_time.desc()).limit(100).all()
    
    return [
        UsageLogResponse(
            id=log.id,
            buyer_id=log.buyer_id,
            dataset_id=log.dataset_id,
            share_id=log.share_id,
            query_time=log.query_time,
            columns_requested=log.columns_requested,
            row_count_returned=log.row_count_returned,
            query_limit=log.query_limit,
            ip_address=log.ip_address
        )
        for log in logs
    ]

@app.post("/shares/{share_id}/file-download", response_model=FileDownloadResponse)
async def request_file_download(
    share_id: int,
    request: FileDownloadRequest,
    current_user: User = Depends(get_current_buyer),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.buyer_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only request file downloads for your own shares"
        )
    
    if share.approval_status != "approved":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Share must be approved. Current status: {share.approval_status}"
        )
    
    if share.revoked:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Share has been revoked"
        )
    
    dataset = db.query(Dataset).filter(Dataset.id == share.dataset_id).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    try:
        download_info = generate_file_download_link(dataset, share, db, request.expiry_hours)
        return FileDownloadResponse(**download_info)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate file download link: {str(e)}"
        )

@app.delete("/shares/{share_id}/file-download/{snapshot_id}", response_model=FileDownloadRevokeResponse)
async def revoke_file_download(
    share_id: int,
    snapshot_id: str,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only revoke file downloads for your own shares"
        )
    
    success = revoke_file_download_link(snapshot_id, db)
    
    if success:
        return FileDownloadRevokeResponse(
            status="success",
            message=f"File download link {snapshot_id} revoked"
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke file download link"
        )

@app.get("/shares/{share_id}/file-delivery-metrics")
async def get_file_delivery_metrics_endpoint(
    share_id: int,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    share = db.query(Share).filter(Share.id == share_id).first()
    if not share:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Share not found"
        )
    
    if share.seller_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only view metrics for your own shares"
        )
    
    metrics = get_file_delivery_metrics(share_id, db)
    return metrics

