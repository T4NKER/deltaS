import base64
import os
from datetime import datetime

import pytest

os.environ.setdefault("ALLOW_INSECURE_DEFAULTS", "true")

from src.buyer.cli import prepare_standard_delta_profile
from src.models.database import Share, User
from src.marketplace.profile import generate_delta_sharing_profile
from src.utils.encryption import encrypt_token, generate_key_pair
from src.utils.token_utils import generate_share_token, hash_token, verify_token_hash

def _buyer_keys():
    key_pair = generate_key_pair()
    return {
        "public_key": base64.b64decode(key_pair["public_key"]).decode("utf-8"),
        "private_key_b64": key_pair["private_key"],
    }

def _approved_share(encrypted_token=None):
    return Share(
        id=42,
        dataset_id=10,
        seller_id=1,
        buyer_id=2,
        approval_status="approved",
        encrypted_token=encrypted_token,
        expires_at=datetime(2026, 5, 1, 12, 0, 0),
    )

def _seller():
    return User(id=1, role="seller", delta_sharing_server_url="http://seller:8080")

def test_profile_generation_is_encrypted_token_only():
    profile = generate_delta_sharing_profile(
        _approved_share(encrypted_token="ciphertext"),
        _seller(),
    )

    assert profile["encryptedBearerToken"] == "ciphertext"
    assert "bearerToken" not in profile
    assert profile["endpoint"] == "http://seller:8080"

def test_profile_generation_rejects_legacy_plaintext_token_fallback():
    with pytest.raises(ValueError, match="encrypted_token"):
        generate_delta_sharing_profile(_approved_share(encrypted_token=None), _seller())

def test_buyer_cli_exports_standard_profile_by_decrypting_encrypted_token():
    keys = _buyer_keys()
    encrypted = encrypt_token("share-secret", keys["public_key"])
    marketplace_profile = {
        "shareCredentialsVersion": 1,
        "endpoint": "http://seller:8080",
        "encryptedBearerToken": encrypted,
        "expirationTime": None,
    }

    standard_profile = prepare_standard_delta_profile(
        marketplace_profile,
        keys["private_key_b64"],
    )

    assert standard_profile["bearerToken"] == "share-secret"
    assert "encryptedBearerToken" not in standard_profile

def test_buyer_cli_requires_private_key_for_encrypted_profile():
    with pytest.raises(ValueError, match="encryptedBearerToken"):
        prepare_standard_delta_profile(
            {
                "shareCredentialsVersion": 1,
                "endpoint": "http://seller:8080",
                "encryptedBearerToken": "ciphertext",
            },
            private_key_b64=None,
        )

def test_share_tokens_are_unique_and_verified_only_by_hmac_hash():
    token_a = generate_share_token()
    token_b = generate_share_token()

    assert token_a != token_b
    assert len(token_a) >= 40

    token_hash = hash_token(token_a)
    assert token_hash != token_a
    assert verify_token_hash(token_a, token_hash)
    assert not verify_token_hash(token_b, token_hash)
