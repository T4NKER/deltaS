from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from src.marketplace.auth import (
    create_access_token, get_current_seller, get_current_user,
    get_password_hash, verify_password,
)
from src.marketplace.schemas import (
    BuyerPublicKeyResponse, DeltaSharingServerUrlRequest, DeltaSharingServerUrlResponse,
    PublicKeyRegistrationRequest, PublicKeyRegistrationResponse,
    Token, UserLogin, UserRegister, UserResponse,
)
from src.marketplace.services import validate_seller_server_url
from src.models.database import Share, User, get_db
from src.utils.encryption import validate_public_key

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == user_data.email).first():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    if user_data.role not in ["buyer", "seller"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Role must be 'buyer' or 'seller'")

    user = User(
        email=user_data.email,
        hashed_password=get_password_hash(user_data.password),
        role=user_data.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.post("/login", response_model=Token)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == credentials.email).first()
    if not user or not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": str(user.id), "role": user.role},
        expires_delta=timedelta(minutes=30),
    )
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    return current_user


@router.put("/me/delta-sharing-server-url", response_model=DeltaSharingServerUrlResponse)
async def update_delta_sharing_server_url(
    request: DeltaSharingServerUrlRequest,
    current_user: User = Depends(get_current_seller),
    db: Session = Depends(get_db),
):
    validated = validate_seller_server_url(request.server_url)
    current_user.delta_sharing_server_url = validated
    db.commit()
    db.refresh(current_user)
    return DeltaSharingServerUrlResponse(delta_sharing_server_url=current_user.delta_sharing_server_url)


@router.put("/me/public-key", response_model=PublicKeyRegistrationResponse)
async def register_public_key(
    request: PublicKeyRegistrationRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not validate_public_key(request.public_key):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid public key format")
    current_user.public_key = request.public_key
    db.commit()
    return PublicKeyRegistrationResponse(
        status="success",
        message="Public key registered successfully",
        public_key=current_user.public_key,
    )


@router.get("/shares/{share_id}/buyer-public-key", response_model=BuyerPublicKeyResponse)
async def get_buyer_public_key(
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
            detail="You can only access buyer public key for your own shares",
        )
    buyer = db.query(User).filter(User.id == share.buyer_id).first()
    if not buyer or not buyer.public_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Buyer has not registered a public key",
        )
    return BuyerPublicKeyResponse(public_key=buyer.public_key)
