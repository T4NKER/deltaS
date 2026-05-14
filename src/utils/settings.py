import os
from typing import Optional
from functools import lru_cache

class Settings:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://deltasharing:deltasharing123@localhost:5433/marketplace")

    S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
    S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "test")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "test-delta-bucket")
    S3_REGION: str = os.getenv("S3_REGION", "us-east-1")

    WATERMARK_SECRET: str = os.getenv("WATERMARK_SECRET", "default-watermark-secret-change-in-production")
    TOKEN_SIGNING_SECRET: str = os.getenv("TOKEN_SIGNING_SECRET", "default-token-secret-change-in-production")
    TOKEN_SALT: str = os.getenv("TOKEN_SALT", "default-token-salt-change-in-production")

    MARKETPLACE_URL: str = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
    DELTA_SHARING_SERVER_URL: str = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "default-jwt-secret-change-in-production")
    SELLER_JWT_SECRET_KEY: str = os.getenv("SELLER_JWT_SECRET_KEY", "default-seller-jwt-secret-change-in-production")
    DELIVERY_TOKEN_SECRET_KEY: str = os.getenv("DELIVERY_TOKEN_SECRET_KEY", "default-delivery-token-secret-change-in-production")
    SELLER_METADATA_SIGNING_PRIVATE_KEY: Optional[str] = os.getenv("SELLER_METADATA_SIGNING_PRIVATE_KEY")
    SELLER_METADATA_SIGNING_PUBLIC_KEY: Optional[str] = os.getenv("SELLER_METADATA_SIGNING_PUBLIC_KEY")
    MARKETPLACE_JWT_PRIVATE_KEY: Optional[str] = os.getenv("MARKETPLACE_JWT_PRIVATE_KEY")
    MARKETPLACE_JWT_PUBLIC_KEY: Optional[str] = os.getenv("MARKETPLACE_JWT_PUBLIC_KEY")
    SELLER_JWT_ALGORITHM: str = os.getenv("SELLER_JWT_ALGORITHM", os.getenv("JWT_ALGORITHM", "HS256"))
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))

    ALLOW_INSECURE_DEFAULTS: bool = os.getenv("ALLOW_INSECURE_DEFAULTS", "false").lower() == "true"

    def __init__(self):
        self._validate_secrets()

    def _validate_secrets(self):
        insecure_defaults = []
        is_seller_service = bool(os.getenv("SELLER_DATABASE_URL"))
        is_marketplace_service = bool(os.getenv("DATABASE_URL")) and not is_seller_service

        if is_seller_service:
            if self.WATERMARK_SECRET == "default-watermark-secret-change-in-production":
                insecure_defaults.append("WATERMARK_SECRET")
            if self.TOKEN_SALT == "default-token-salt-change-in-production":
                insecure_defaults.append("TOKEN_SALT")
            if self.SELLER_JWT_SECRET_KEY == "default-seller-jwt-secret-change-in-production":
                insecure_defaults.append("SELLER_JWT_SECRET_KEY")
            if self.DELIVERY_TOKEN_SECRET_KEY == "default-delivery-token-secret-change-in-production":
                insecure_defaults.append("DELIVERY_TOKEN_SECRET_KEY")
            if not self.MARKETPLACE_JWT_PUBLIC_KEY:
                raise ValueError("MARKETPLACE_JWT_PUBLIC_KEY must be set for seller service.")

        if is_marketplace_service:
            if not self.MARKETPLACE_JWT_PRIVATE_KEY or not self.MARKETPLACE_JWT_PUBLIC_KEY:
                raise ValueError(
                    "MARKETPLACE_JWT_PRIVATE_KEY and MARKETPLACE_JWT_PUBLIC_KEY must be set for marketplace service."
                )

        if insecure_defaults and not self.ALLOW_INSECURE_DEFAULTS:
            raise ValueError(
                f"Insecure default secrets detected: {', '.join(insecure_defaults)}. "
                "Set these environment variables or set ALLOW_INSECURE_DEFAULTS=true for development only."
            )

    TOKEN_ROTATION_ENABLED: bool = os.getenv("TOKEN_ROTATION_ENABLED", "false").lower() == "true"
    TOKEN_EXPIRY_DAYS: int = int(os.getenv("TOKEN_EXPIRY_DAYS", "180"))
    TOKEN_ROTATION_DAYS: int = int(os.getenv("TOKEN_ROTATION_DAYS", "90"))
    TOKEN_INACTIVITY_DAYS: int = int(os.getenv("TOKEN_INACTIVITY_DAYS", "30"))

    SELLER_MAX_QUERY_ROWS: int = int(os.getenv("SELLER_MAX_QUERY_ROWS", "10000000"))

    @classmethod
    def get_watermark_secret_bytes(cls) -> bytes:
        return cls.WATERMARK_SECRET.encode('utf-8')

    @classmethod
    def get_token_signing_secret_bytes(cls) -> bytes:
        return cls.TOKEN_SIGNING_SECRET.encode('utf-8')

    @classmethod
    def get_token_salt_bytes(cls) -> bytes:
        return cls.TOKEN_SALT.encode('utf-8')

@lru_cache()
def get_settings() -> Settings:
    return Settings()
