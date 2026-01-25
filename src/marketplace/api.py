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
from deltalake import DeltaTable
from src.models.database import (
    init_db, User, Dataset, Share, Purchase, AuditLog, get_db
)
from src.marketplace.auth import (
    get_password_hash, verify_password, create_access_token,
    get_current_user, get_current_seller, get_current_buyer
)
from src.marketplace.schemas import (
    UserRegister, UserLogin, Token, UserResponse,
    DatasetCreate, DatasetResponse, PurchaseResponse, TrialRequest, TrialResponse
)
from src.seller.watermarking import detect_anchor_columns_from_schema
from src.utils.s3_utils import get_delta_storage_options, get_full_s3_path, get_bucket_name

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
    server_url: str,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db)
):
    current_user.delta_sharing_server_url = server_url
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
    anchor_columns = dataset_data.anchor_columns if dataset_data.anchor_columns and dataset_data.anchor_columns.strip() else None
    
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
    db.flush()
    
    if not dataset.anchor_columns:
        try:
            bucket_name = get_bucket_name()
            table_path = get_full_s3_path(bucket_name, dataset_data.table_path)
            storage_options = get_delta_storage_options()
            delta_table = DeltaTable(table_path, storage_options=storage_options)
            arrow_dataset = delta_table.to_pyarrow_dataset()
            schema = arrow_dataset.schema
            
            sensitive_cols = []
            if dataset.sensitive_columns:
                try:
                    sensitive_cols = json.loads(dataset.sensitive_columns) if isinstance(dataset.sensitive_columns, str) else dataset.sensitive_columns
                except:
                    pass
            
            anchor_cols = detect_anchor_columns_from_schema(schema, sensitive_columns=sensitive_cols)
            if not anchor_cols:
                raise ValueError("Could not detect suitable anchor columns from table schema")
            dataset.anchor_columns = ','.join(anchor_cols)
        except Exception as e:
            db.rollback()
            raise HTTPException(
                status_code=400,
                detail=f"Failed to configure anchor columns for dataset. Table may not exist or be accessible. Error: {str(e)}"
            )
    
    if not dataset.anchor_columns:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail="Dataset anchor_columns must be configured. Please ensure the table exists and is accessible."
        )
    
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
        
        token_string = secrets.token_urlsafe(32)
        checksum = hashlib.sha256(token_string.encode()).hexdigest()[:8]
        share_token = f"{token_string}-{checksum}"
        
        approval_status = "pending" if dataset.requires_approval else "approved"
        
        share = Share(
            dataset_id=dataset_id,
            seller_id=dataset.seller_id,
            buyer_id=current_user.id,
            token=share_token,
            expires_at=datetime.utcnow() + timedelta(days=365),
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
    
    token_string = secrets.token_urlsafe(32)
    checksum = hashlib.sha256(token_string.encode()).hexdigest()[:8]
    share_token = f"{token_string}-{checksum}"
    
    row_limit = min(trial_request.row_limit or 100, 1000)
    expires_at = datetime.utcnow() + timedelta(days=trial_request.days_valid or 7)
    
    share = Share(
        dataset_id=dataset_id,
        seller_id=dataset.seller_id,
        buyer_id=current_user.id,
        token=share_token,
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

@app.get("/my-shares")
async def get_my_shares(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    shares = db.query(Share).filter(
        or_(Share.seller_id == current_user.id, Share.buyer_id == current_user.id)
    ).all()
    return [
        {
            "id": share.id,
            "dataset_id": share.dataset_id,
            "dataset_name": share.dataset.name,
            "seller_id": share.seller_id,
            "buyer_id": share.buyer_id,
            "token": share.token,
            "created_at": share.created_at,
            "expires_at": share.expires_at,
            "approval_status": share.approval_status,
            "revoked": share.revoked,
            "revoked_at": share.revoked_at
        }
        for share in shares
    ]

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
    db.commit()
    
    return None

@app.post("/shares/{share_id}/approve", status_code=status.HTTP_200_OK)
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
    db.commit()
    
    return {
        "status": "success",
        "message": "Share approved",
        "share_id": share_id,
        "approval_status": share.approval_status
    }

@app.post("/shares/{share_id}/reject", status_code=status.HTTP_200_OK)
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
    
    return {
        "status": "success",
        "message": "Share rejected",
        "share_id": share_id,
        "approval_status": share.approval_status
    }

@app.get("/usage-logs")
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
        {
            "id": log.id,
            "buyer_id": log.buyer_id,
            "dataset_id": log.dataset_id,
            "share_id": log.share_id,
            "query_time": log.query_time,
            "columns_requested": log.columns_requested,
            "row_count_returned": log.row_count_returned,
            "query_limit": log.query_limit,
            "ip_address": log.ip_address
        }
        for log in logs
    ]

