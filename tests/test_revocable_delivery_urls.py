import json
import os
import time

import requests

from tests.test_e2e_delta_sharing import (
    MARKETPLACE_URL,
    DELTA_SHARING_SERVER_URL,
    LOCALSTACK_HEALTH_URL,
    _create_dataset_via_publish,
    _set_seller_server_url,
)
from tests.utils import api_post, api_get, api_delete, register_buyer_public_key, decrypt_marketplace_profile

def _ensure_services_available():
    response = requests.get(LOCALSTACK_HEALTH_URL, timeout=2)
    assert response.status_code == 200, f"LocalStack unavailable: {response.text}"
    response = requests.get(f"{DELTA_SHARING_SERVER_URL}/health", timeout=2)
    assert response.status_code == 200, f"Seller unavailable: {response.text}"

def _create_active_share():
    _ensure_services_available()

    seller_email = f"seller_revocable_{int(time.time() * 1000)}@test.com"
    buyer_email = f"buyer_revocable_{int(time.time() * 1000)}@test.com"
    password = "testpass123"

    seller_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": seller_email,
        "password": password,
        "role": "seller"
    }, expected_status=201)
    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)

    seller_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": seller_email,
        "password": password,
    })
    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": buyer_email,
        "password": password,
    })
    seller_headers = {"Authorization": f"Bearer {seller_login['access_token']}"}
    buyer_headers = {"Authorization": f"Bearer {buyer_login['access_token']}"}

    dataset_data, _ = _create_dataset_via_publish(
        seller_headers,
        name="revocation_url_test",
        description="Delivery URL revocation test dataset",
        table_path=f"revocation_url_table_{int(time.time() * 1000)}",
        price=0.0,
        is_public=True,
        anchor_columns="category,write_batch",
        seller_id=seller_data["id"],
    )

    _set_seller_server_url(seller_headers)
    buyer_keys = register_buyer_public_key(MARKETPLACE_URL, buyer_headers)

    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_data['id']}", {"accept_license": True}, headers=buyer_headers)
    share_id = purchase_data["share_id"]
    if purchase_data["approval_status"] == "pending":
        api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)

    profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
    profile_data = json.loads(profile_resp["profile_json"])
    _, bearer_token = decrypt_marketplace_profile(profile_data, buyer_keys["private_key_b64"])

    endpoint = profile_data["endpoint"].rstrip("/")
    table_name = dataset_data["table_path"]
    return seller_headers, buyer_headers, share_id, endpoint, bearer_token, table_name

def test_query_delivery_url_is_blocked_after_share_revocation():
    seller_headers, buyer_headers, share_id, endpoint, bearer_token, table_name = _create_active_share()

    response = requests.post(
        f"{endpoint}/shares/share_{share_id}/schemas/default/tables/{table_name}/query",
        json={},
        headers={"Authorization": f"Bearer {bearer_token}"},
        timeout=30,
    )
    assert response.status_code == 200, response.text

    lines = [json.loads(line) for line in response.text.splitlines() if line.strip()]
    file_url = next(item["file"]["url"] for item in lines if "file" in item)

    before = requests.get(file_url, timeout=30)
    assert before.status_code == 200, before.text[:200]

    api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)

    after = requests.get(file_url, timeout=30)
    assert after.status_code in (401, 403), after.text[:200]
