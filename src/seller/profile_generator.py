import json
from typing import Dict, Any, Optional
from datetime import datetime
from src.models.database import Share, User
from src.utils.token_utils import generate_share_token, hash_token
from src.utils.settings import get_settings

def generate_delta_sharing_profile(
    share: Share,
    seller: User,
    token: Optional[str] = None
) -> Dict[str, Any]:
    if not seller.delta_sharing_server_url:
        raise ValueError("Seller must have delta_sharing_server_url configured")
    
    if share.approval_status != "approved":
        raise ValueError("Cannot generate profile for unapproved share")
    
    if share.revoked:
        raise ValueError("Cannot generate profile for revoked share")
    
    if not token:
        if not share.token:
            raise ValueError("Share must have a token to generate profile")
        token = share.token
    
    profile = {
        "shareCredentialsVersion": 1,
        "endpoint": seller.delta_sharing_server_url,
        "bearerToken": token,
        "expirationTime": share.expires_at.isoformat() if share.expires_at else None
    }
    
    return profile

def generate_profile_json(profile: Dict[str, Any]) -> str:
    return json.dumps(profile, indent=2)

