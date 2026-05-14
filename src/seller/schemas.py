from pydantic import BaseModel
from typing import Optional, List


class PublishMetadataRequest(BaseModel):
    table_path: str
    name: str
    description: Optional[str] = None
    license_name: Optional[str] = None
    license_terms: Optional[str] = None
    anchor_columns: Optional[str] = None


class SyntheticDataRequest(BaseModel):
    table_path: str
    output_table_path: str
    num_rows: int
    dp_epsilon: Optional[float] = None
    preserve_statistics: bool = True
    seed: Optional[int] = None


class SellerAuthTokenRequest(BaseModel):
    marketplace_token: str
    seller_id: Optional[int] = None


class SellerAuthTokenResponse(BaseModel):
    seller_token: str
    seller_id: int


class EncryptTokenRequest(BaseModel):
    share_id: int
    buyer_public_key: str
    buyer_id: Optional[int] = None


class EncryptTokenResponse(BaseModel):
    encrypted_token: str
    token_hash: str


class RegisterShareRequest(BaseModel):
    share_id: int
    dataset_id: int
    seller_id: int
    buyer_id: int
    token_hash: Optional[str] = None
    approval_status: Optional[str] = None
    revoked: bool = False
    expires_at: Optional[str] = None
    is_trial: bool = False
    trial_row_limit: Optional[int] = None
    trial_expires_at: Optional[str] = None
    watermark_nonce: Optional[str] = None
    table_path: str
    anchor_columns: Optional[str] = None
    buyer_public_key_hash: Optional[str] = None


class RegisterShareResponse(BaseModel):
    status: str
    share_id: int


class RevokeShareRequest(BaseModel):
    share_id: int


class RevokeShareResponse(BaseModel):
    status: str
    share_id: int


class SeedTestDataRequest(BaseModel):
    table_path: str
    data: List[dict]


class SeedTestDataResponse(BaseModel):
    status: str
    rows: int
    table_path: str


class EnsureBucketResponse(BaseModel):
    status: str
    bucket: str


class DetectFingerprintRequest(BaseModel):
    table_path: str
    candidate_buyer_ids: List[int]
    share_ids: Optional[List[int]] = None
    sample_size: Optional[int] = None


class DetectFingerprintResponse(BaseModel):
    matches: List[dict]
    best_match: Optional[dict] = None
