from datetime import timedelta

from jose import jwt

from src.utils.settings import get_settings
from src.utils.time_utils import utcnow


def create_seller_token(data: dict) -> str:
    settings = get_settings()
    to_encode = data.copy()
    to_encode["exp"] = utcnow() + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    return jwt.encode(to_encode, settings.SELLER_JWT_SECRET_KEY, algorithm=settings.SELLER_JWT_ALGORITHM)


def decode_seller_token(token: str) -> dict:
    settings = get_settings()
    return jwt.decode(token, settings.SELLER_JWT_SECRET_KEY, algorithms=[settings.SELLER_JWT_ALGORITHM])
