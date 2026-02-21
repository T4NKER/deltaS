import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Optional
from src.utils.settings import get_settings

def hash_token(token: str) -> str:
    settings = get_settings()
    salt = settings.get_token_salt_bytes()
    if not salt or not isinstance(salt, bytes):
        raise ValueError("Token salt must be bytes")
    return hmac.new(salt, token.encode('utf-8'), hashlib.sha256).hexdigest()

def generate_share_token() -> str:
    token_string = secrets.token_urlsafe(32)
    checksum = hashlib.sha256(token_string.encode()).hexdigest()[:8]
    return f"{token_string}-{checksum}"

def verify_token_hash(token: str, stored_hash: str) -> bool:
    computed_hash = hash_token(token)
    return hmac.compare_digest(computed_hash, stored_hash)

def should_rotate_token(created_at: datetime, last_used_at: Optional[datetime] = None, rotation_days: int = 90, inactivity_days: int = 30) -> bool:
    from src.utils.settings import get_settings
    settings = get_settings()
    
    rotation_days = settings.TOKEN_ROTATION_DAYS if hasattr(settings, 'TOKEN_ROTATION_DAYS') else rotation_days
    inactivity_days = settings.TOKEN_INACTIVITY_DAYS if hasattr(settings, 'TOKEN_INACTIVITY_DAYS') else inactivity_days
    
    if not rotation_days or rotation_days <= 0:
        return False
    
    now = datetime.utcnow()
    age = now - created_at
    
    if age.days >= rotation_days:
        return True
    
    if last_used_at and inactivity_days > 0:
        inactivity = now - last_used_at
        if inactivity.days >= inactivity_days:
            return True
    
    return False

def is_token_expired(expires_at: Optional[datetime]) -> bool:
    if not expires_at:
        return False
    return datetime.utcnow() > expires_at

