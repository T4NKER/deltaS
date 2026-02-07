import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Optional
from src.utils.settings import get_settings

def hash_token(token: str) -> str:
    settings = get_settings()
    salt = settings.get_token_salt_bytes()
    return hmac.new(salt, token.encode('utf-8'), hashlib.sha256).hexdigest()

def generate_share_token() -> str:
    token_string = secrets.token_urlsafe(32)
    checksum = hashlib.sha256(token_string.encode()).hexdigest()[:8]
    return f"{token_string}-{checksum}"

def verify_token_hash(token: str, stored_hash: str) -> bool:
    computed_hash = hash_token(token)
    return hmac.compare_digest(computed_hash, stored_hash)

def should_rotate_token(created_at: datetime, rotation_days: int = 90) -> bool:
    if not rotation_days or rotation_days <= 0:
        return False
    age = datetime.utcnow() - created_at
    return age.days >= rotation_days

def is_token_expired(expires_at: Optional[datetime]) -> bool:
    if not expires_at:
        return False
    return datetime.utcnow() > expires_at

