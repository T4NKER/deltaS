from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class UserRegister(BaseModel):
    email: EmailStr
    password: str
    role: str = "buyer"

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class UserResponse(BaseModel):
    id: int
    email: str
    role: str
    created_at: datetime
    
    class Config:
        from_attributes = True

class DatasetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    table_path: str
    price: float = 0.0
    is_public: bool = False

class DatasetResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    table_path: str
    price: float
    is_public: bool
    seller_id: int
    created_at: datetime
    risk_score: Optional[float]
    risk_level: Optional[str]
    
    class Config:
        from_attributes = True

class PurchaseResponse(BaseModel):
    id: int
    buyer_id: int
    dataset_id: int
    share_id: int
    amount: float
    created_at: datetime
    share_token: str
    approval_status: str
    seller_server_url: Optional[str] = None
    
    class Config:
        from_attributes = True

class TrialRequest(BaseModel):
    row_limit: Optional[int] = 100
    days_valid: Optional[int] = 7

class TrialResponse(BaseModel):
    id: int
    buyer_id: int
    dataset_id: int
    share_id: int
    share_token: str
    approval_status: str
    seller_server_url: Optional[str] = None
    is_trial: bool
    trial_row_limit: Optional[int]
    trial_expires_at: Optional[datetime]
    
    class Config:
        from_attributes = True


