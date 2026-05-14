import hashlib
import json
import os
from typing import Optional

import requests
from fastapi import HTTPException, Request
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from src.marketplace.auth import create_access_token
from src.models.database import AuditLog
from src.utils.audit_chain import seal_audit_entry
from src.utils.logging_setup import get_logger
from src.utils.settings import get_settings

logger = get_logger(__name__)

HTTP_REQUEST_TIMEOUT = 10


def fix_seller_url_for_docker(seller_url: str) -> str:
    if os.getenv("DOCKER_ENV") == "true" and "localhost" in seller_url:
        return seller_url.replace("localhost", "seller")
    if os.getenv("DOCKER_ENV") != "true" and "seller:" in seller_url:
        return seller_url.replace("seller:", "localhost:").replace("seller/", "localhost/")
    return seller_url


def get_seller_auth_headers(seller_url: str, _marketplace_token: str, seller_id: int) -> dict:
    seller_url = fix_seller_url_for_docker(seller_url.rstrip('/'))
    service_token = create_access_token({"sub": "marketplace", "role": "marketplace", "aud": "seller-auth"})
    auth_response = requests.post(
        f"{seller_url}/seller/auth-token",
        json={"marketplace_token": service_token, "seller_id": seller_id},
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    auth_response.raise_for_status()
    seller_token = auth_response.json()["seller_token"]
    return {"Authorization": f"Bearer {seller_token}"}


def call_seller_encrypt_token(
    seller_url: str, seller_headers: dict, share_id: int,
    buyer_public_key: str, buyer_id: int,
) -> dict:
    seller_url = fix_seller_url_for_docker(seller_url.rstrip('/'))
    response = requests.post(
        f"{seller_url}/seller/encrypt-token",
        json={"share_id": share_id, "buyer_public_key": buyer_public_key, "buyer_id": buyer_id},
        headers=seller_headers,
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def register_share_with_seller(
    seller_url: str, seller_headers: dict, share, dataset,
    buyer_public_key: Optional[str] = None,
):
    seller_url = fix_seller_url_for_docker(seller_url.rstrip('/'))
    body = {
        "share_id": share.id,
        "dataset_id": share.dataset_id,
        "seller_id": share.seller_id,
        "buyer_id": share.buyer_id,
        "token_hash": share.token_hash,
        "approval_status": share.approval_status,
        "revoked": share.revoked or False,
        "expires_at": share.expires_at.isoformat() if share.expires_at else None,
        "is_trial": share.is_trial or False,
        "trial_row_limit": share.trial_row_limit,
        "trial_expires_at": share.trial_expires_at.isoformat() if share.trial_expires_at else None,
        "watermark_nonce": share.watermark_nonce,
        "table_path": dataset.table_path,
        "anchor_columns": dataset.anchor_columns,
    }
    if buyer_public_key:
        body["buyer_public_key_hash"] = hashlib.sha256(buyer_public_key.encode("utf-8")).hexdigest()
    resp = requests.post(
        f"{seller_url}/seller/register-share",
        json=body, headers=seller_headers, timeout=HTTP_REQUEST_TIMEOUT,
    )
    if resp.status_code != 200:
        logger.warning("register-share failed: %s %s", resp.status_code, resp.text)
    resp.raise_for_status()


def revoke_share_on_seller(seller_url: str, seller_headers: dict, share_id: int):
    seller_url = fix_seller_url_for_docker(seller_url.rstrip('/'))
    try:
        requests.post(
            f"{seller_url}/seller/revoke-share",
            json={"share_id": share_id}, headers=seller_headers,
            timeout=HTTP_REQUEST_TIMEOUT,
        ).raise_for_status()
    except Exception as e:
        logger.warning("Failed to revoke share on seller: %s", e)


def log_audit_event(db: Session, event_type: str, actor_id: int, share, extra: dict = None):
    try:
        metadata = json.dumps(extra) if extra else None
        entry = AuditLog(
            event_type=event_type,
            actor_id=actor_id,
            buyer_id=share.buyer_id,
            dataset_id=share.dataset_id,
            share_id=share.id,
            client_metadata=metadata,
        )
        seal_audit_entry(db, entry)
        db.add(entry)
        db.commit()
    except Exception as e:
        logger.warning("Failed to write audit log (%s): %s", event_type, e)


def validate_seller_server_url(url: str) -> str:
    from urllib.parse import urlparse
    import ipaddress
    import socket

    if not url or not isinstance(url, str):
        raise HTTPException(status_code=400, detail="server_url is required")
    try:
        parsed = urlparse(url)
    except Exception:
        raise HTTPException(status_code=400, detail="server_url is not a valid URL")

    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="server_url must use http:// or https://")
    if not parsed.hostname:
        raise HTTPException(status_code=400, detail="server_url must include a hostname")
    if parsed.username or parsed.password:
        raise HTTPException(status_code=400, detail="server_url must not contain userinfo")
    if parsed.fragment:
        raise HTTPException(status_code=400, detail="server_url must not contain a fragment")

    settings = get_settings()
    allow_insecure = getattr(settings, "ALLOW_INSECURE_DEFAULTS", False)

    if parsed.scheme != "https" and not allow_insecure:
        raise HTTPException(
            status_code=400,
            detail="server_url must use https:// (set ALLOW_INSECURE_DEFAULTS=true only in dev).",
        )

    host = parsed.hostname.lower()
    if host in {"metadata", "metadata.google.internal"}:
        raise HTTPException(status_code=400, detail="server_url hostname is not permitted")

    try:
        resolved = socket.gethostbyname(host)
        addr = ipaddress.ip_address(resolved)
    except (socket.gaierror, ValueError):
        if not allow_insecure:
            raise HTTPException(
                status_code=400,
                detail="server_url hostname could not be resolved",
            )
        return url

    if addr.is_link_local:
        raise HTTPException(
            status_code=400,
            detail="server_url resolves to a blocked (link-local/metadata) address",
        )
    if not allow_insecure and (addr.is_private or addr.is_loopback or addr.is_reserved or addr.is_multicast):
        raise HTTPException(
            status_code=400,
            detail="server_url resolves to a non-public address",
        )
    return url


class CSRFMiddleware(BaseHTTPMiddleware):
    MUTATING_METHODS = {"POST", "PUT", "DELETE", "PATCH"}

    async def dispatch(self, request: Request, call_next):
        if request.method in self.MUTATING_METHODS:
            content_type = request.headers.get("content-type", "")
            has_auth = "authorization" in {k.lower() for k in request.headers.keys()}
            if not has_auth and not content_type.startswith("application/json"):
                return JSONResponse(
                    status_code=415,
                    content={"detail": "Content-Type must be application/json for state-changing requests"},
                )
        return await call_next(request)
