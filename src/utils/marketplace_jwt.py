from datetime import timedelta
from typing import Any, Optional

from jose import JWTError, jwt

from src.utils.settings import get_settings
from src.utils.time_utils import utcnow

def _normalise_pem(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.replace("\\n", "\n").strip()

def _marketplace_signing_config() -> tuple[str, Any]:
    settings = get_settings()
    private_key = _normalise_pem(settings.MARKETPLACE_JWT_PRIVATE_KEY)
    if not private_key:
        raise ValueError("MARKETPLACE_JWT_PRIVATE_KEY must be configured")
    return "RS256", private_key

def _marketplace_verification_config() -> tuple[str, Any]:
    settings = get_settings()
    public_key = _normalise_pem(settings.MARKETPLACE_JWT_PUBLIC_KEY)
    if not public_key:
        raise ValueError("MARKETPLACE_JWT_PUBLIC_KEY must be configured")
    return "RS256", public_key

def encode_marketplace_jwt(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    settings = get_settings()
    algorithm, signing_key = _marketplace_signing_config()
    to_encode = data.copy()
    if expires_delta:
        expire = utcnow() + expires_delta
    else:
        expire = utcnow() + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, signing_key, algorithm=algorithm)

def decode_marketplace_jwt(
    token: str,
    *,
    audience: Optional[str] = None,
    require_audience: bool = False,
) -> dict:
    algorithm, verification_key = _marketplace_verification_config()
    options = {"verify_aud": require_audience or audience is not None}
    kwargs = {"algorithms": [algorithm], "options": options}
    if audience is not None:
        kwargs["audience"] = audience
    return jwt.decode(token, verification_key, **kwargs)

__all__ = ["JWTError", "decode_marketplace_jwt", "encode_marketplace_jwt"]
