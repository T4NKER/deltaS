import json
from typing import Dict, Any
from src.models.database import Share, User


def generate_delta_sharing_profile(
    share: Share,
    seller: User
) -> Dict[str, Any]:
    if not seller.delta_sharing_server_url:
        raise ValueError("Seller must have delta_sharing_server_url configured")

    if share.approval_status != "approved":
        raise ValueError("Cannot generate profile for unapproved share")

    if share.revoked:
        raise ValueError("Cannot generate profile for revoked share")

    if not share.encrypted_token:
        raise ValueError("Share must have encrypted_token to generate profile")

    return {
        "shareCredentialsVersion": 1,
        "endpoint": seller.delta_sharing_server_url,
        "encryptedBearerToken": share.encrypted_token,
        "expirationTime": share.expires_at.isoformat() if share.expires_at else None
    }


def generate_profile_json(profile: Dict[str, Any]) -> str:
    return json.dumps(profile, indent=2)
