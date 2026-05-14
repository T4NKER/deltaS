import base64
import json
from typing import Any, Dict, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from src.utils.settings import get_settings


def _normalise_pem(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.replace("\\n", "\n").strip()

def _canonicalise_metadata(metadata_dict: Dict[str, Any]) -> bytes:
    metadata_json = json.dumps(metadata_dict, sort_keys=True, separators=(",", ":"), default=str)
    return metadata_json.encode("utf-8")

def get_seller_metadata_signing_private_key_pem() -> str:
    settings = get_settings()
    configured = _normalise_pem(settings.SELLER_METADATA_SIGNING_PRIVATE_KEY)
    if configured:
        return configured
    raise ValueError(
        "SELLER_METADATA_SIGNING_PRIVATE_KEY must be configured. "
        "Run scripts/generate_dev_secrets.sh to create a development keypair."
    )

def get_seller_metadata_signing_public_key_pem() -> str:
    settings = get_settings()
    configured = _normalise_pem(settings.SELLER_METADATA_SIGNING_PUBLIC_KEY)
    if configured:
        return configured
    private_key = serialization.load_pem_private_key(
        get_seller_metadata_signing_private_key_pem().encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    public_key = private_key.public_key()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return public_pem.decode("utf-8")

def sign_metadata_payload(metadata_dict: Dict[str, Any]) -> str:
    private_key = serialization.load_pem_private_key(
        get_seller_metadata_signing_private_key_pem().encode("utf-8"),
        password=None,
        backend=default_backend(),
    )
    signature = private_key.sign(
        _canonicalise_metadata(metadata_dict),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")

def verify_metadata_signature(metadata_dict: Dict[str, Any], signature_b64: str, public_key_pem: str) -> bool:
    try:
        public_key = serialization.load_pem_public_key(
            public_key_pem.encode("utf-8"),
            backend=default_backend(),
        )
        public_key.verify(
            base64.b64decode(signature_b64.encode("utf-8")),
            _canonicalise_metadata(metadata_dict),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return True
    except Exception:
        return False


def validate_metadata_signature(metadata_dict: Dict[str, Any], public_key_pem: Optional[str]) -> bool:
    if "signature" not in metadata_dict or not public_key_pem:
        return False
    provided_signature = metadata_dict.pop("signature")
    is_valid = verify_metadata_signature(metadata_dict, provided_signature, public_key_pem)
    metadata_dict["signature"] = provided_signature
    return is_valid
