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

class DatasetMetadataBundle(BaseModel):
    version: str
    seller_id: int
    name: str
    description: Optional[str] = None
    table_path: str
    schema: dict
    anchor_columns: list
    pii_analysis: dict
    sample_row_count: Optional[int] = None
    total_row_count: Optional[int] = None
    published_at: str
    signature: str

class DatasetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    table_path: str
    price: float = 0.0
    is_public: bool = False
    anchor_columns: Optional[str] = None
    metadata_bundle: Optional[DatasetMetadataBundle] = None

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

class ProfileResponse(BaseModel):
    share_id: int
    profile_json: str
    generated_at: datetime
    
    class Config:
        from_attributes = True

class PublishMetadataRequest(BaseModel):
    table_path: str
    name: str
    description: Optional[str] = None
    anchor_columns: Optional[str] = None

class FileDownloadRequest(BaseModel):
    expiry_hours: int = 24

class SyntheticDataRequest(BaseModel):
    table_path: str
    output_table_path: str
    num_rows: int
    dp_epsilon: Optional[float] = None
    preserve_statistics: bool = True
    seed: Optional[int] = None

class DeltaSharingServerUrlRequest(BaseModel):
    server_url: str

class ShareResponse(BaseModel):
    id: int
    dataset_id: int
    dataset_name: str
    seller_id: int
    buyer_id: int
    token: str
    created_at: datetime
    expires_at: Optional[datetime]
    approval_status: str
    revoked: bool
    revoked_at: Optional[datetime]

class TokenRotationResponse(BaseModel):
    status: str
    message: str
    share_id: int
    new_token: str

class ApprovalResponse(BaseModel):
    status: str
    message: str
    share_id: int
    approval_status: str
    profile_generated: bool

class RejectionResponse(BaseModel):
    status: str
    message: str
    share_id: int
    approval_status: str

class ProfileListItem(BaseModel):
    share_id: int
    dataset_id: int
    dataset_name: str
    profile_json: str
    generated_at: datetime
    expires_at: Optional[datetime]

class UsageLogResponse(BaseModel):
    id: int
    buyer_id: int
    dataset_id: int
    share_id: int
    query_time: datetime
    columns_requested: Optional[str]
    row_count_returned: Optional[int]
    query_limit: Optional[int]
    ip_address: Optional[str]

class FileDownloadResponse(BaseModel):
    download_url: str
    download_token: str
    expires_at: str
    snapshot_id: str
    file_size_bytes: int
    rows: int
    columns: list

class FileDownloadRevokeResponse(BaseModel):
    status: str
    message: str
