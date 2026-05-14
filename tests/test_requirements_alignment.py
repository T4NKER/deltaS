import json
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from tests.test_e2e_delta_sharing import (
    MARKETPLACE_URL,
    _create_dataset_via_publish,
    _set_seller_server_url,
)
from tests.utils import (
    api_get,
    api_post,
    decrypt_marketplace_profile,
    register_buyer_public_key,
)

def _register_user(role: str, prefix: str) -> tuple[dict, dict]:
    email = f"{prefix}_{int(time.time() * 1000)}@test.com"
    password = "testpass123"
    user = api_post(
        f"{MARKETPLACE_URL}/register",
        {"email": email, "password": password, "role": role},
        expected_status=201,
    )
    login = api_post(f"{MARKETPLACE_URL}/login", {"email": email, "password": password})
    return user, {"Authorization": f"Bearer {login['access_token']}"}

def _create_public_dataset(seller: dict, seller_headers: dict, name: str, description: str) -> dict:
    dataset, _ = _create_dataset_via_publish(
        seller_headers,
        name=name,
        description=description,
        table_path=f"{name.lower().replace(' ', '_')}_{int(time.time() * 1000)}",
        price=0.0,
        is_public=True,
        anchor_columns="category,write_batch",
        seller_id=seller["id"],
    )
    return dataset

def test_catalog_metadata_search_and_filtering():
    seller, seller_headers = _register_user("seller", "seller_catalog")
    buyer, buyer_headers = _register_user("buyer", "buyer_catalog")
    _set_seller_server_url(seller_headers)

    weather = _create_public_dataset(
        seller,
        seller_headers,
        "Weather Telemetry",
        "Hourly weather sensor observations",
    )
    finance = _create_public_dataset(
        seller,
        seller_headers,
        "Finance Metrics",
        "Quarterly revenue and cost metrics",
    )

    search_results = api_get(f"{MARKETPLACE_URL}/datasets?q=weather", headers=buyer_headers)
    result_ids = {item["id"] for item in search_results}

    assert weather["id"] in result_ids
    assert finance["id"] not in result_ids

    seller_results = api_get(
        f"{MARKETPLACE_URL}/datasets?seller_id={seller['id']}",
        headers=buyer_headers,
    )
    assert {weather["id"], finance["id"]}.issubset({item["id"] for item in seller_results})

    low_risk_results = api_get(f"{MARKETPLACE_URL}/datasets?risk_level=low", headers=buyer_headers)
    assert weather["id"] in {item["id"] for item in low_risk_results}

def test_buyer_can_poll_share_status_after_purchase():
    seller, seller_headers = _register_user("seller", "seller_status")
    buyer, buyer_headers = _register_user("buyer", "buyer_status")
    _set_seller_server_url(seller_headers)
    buyer_keys = register_buyer_public_key(MARKETPLACE_URL, buyer_headers)
    dataset = _create_public_dataset(
        seller,
        seller_headers,
        "Status Poll Dataset",
        "Dataset for request status polling",
    )

    purchase = api_post(
        f"{MARKETPLACE_URL}/purchase/{dataset['id']}",
        {"accept_license": True},
        headers=buyer_headers,
    )

    shares = api_get(f"{MARKETPLACE_URL}/my-shares", headers=buyer_headers)
    matching = [share for share in shares if share["id"] == purchase["share_id"]]

    assert len(matching) == 1
    assert matching[0]["approval_status"] == purchase["approval_status"]
    assert matching[0]["revoked"] is False
    assert matching[0]["encrypted_token"]

    profile = api_get(f"{MARKETPLACE_URL}/shares/{purchase['share_id']}/profile", headers=buyer_headers)
    profile_data = json.loads(profile["profile_json"])
    decrypt_marketplace_profile(profile_data, buyer_keys["private_key_b64"])

def test_multiple_buyers_receive_independent_shares_and_tokens():
    seller, seller_headers = _register_user("seller", "seller_multi")
    buyer_a, buyer_a_headers = _register_user("buyer", "buyer_multi_a")
    buyer_b, buyer_b_headers = _register_user("buyer", "buyer_multi_b")
    _set_seller_server_url(seller_headers)
    buyer_a_keys = register_buyer_public_key(MARKETPLACE_URL, buyer_a_headers)
    buyer_b_keys = register_buyer_public_key(MARKETPLACE_URL, buyer_b_headers)
    dataset = _create_public_dataset(
        seller,
        seller_headers,
        "Multi Buyer Dataset",
        "Dataset shared to multiple buyers",
    )

    def purchase(headers):
        return api_post(
            f"{MARKETPLACE_URL}/purchase/{dataset['id']}",
            {"accept_license": True},
            headers=headers,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(purchase, buyer_a_headers)
        future_b = executor.submit(purchase, buyer_b_headers)
        purchase_a = future_a.result()
        purchase_b = future_b.result()

    assert purchase_a["share_id"] != purchase_b["share_id"]
    assert purchase_a["encrypted_token"] != purchase_b["encrypted_token"]

    profile_a = json.loads(api_get(
        f"{MARKETPLACE_URL}/shares/{purchase_a['share_id']}/profile",
        headers=buyer_a_headers,
    )["profile_json"])
    profile_b = json.loads(api_get(
        f"{MARKETPLACE_URL}/shares/{purchase_b['share_id']}/profile",
        headers=buyer_b_headers,
    )["profile_json"])

    _, token_a = decrypt_marketplace_profile(profile_a, buyer_a_keys["private_key_b64"])
    _, token_b = decrypt_marketplace_profile(profile_b, buyer_b_keys["private_key_b64"])

    assert token_a != token_b

def test_marketplace_catalog_and_share_status_survive_seller_unavailability():
    seller, seller_headers = _register_user("seller", "seller_down")
    buyer, buyer_headers = _register_user("buyer", "buyer_down")
    _set_seller_server_url(seller_headers)
    register_buyer_public_key(MARKETPLACE_URL, buyer_headers)
    dataset = _create_public_dataset(
        seller,
        seller_headers,
        "Seller Down Dataset",
        "Dataset used to verify marketplace metadata availability",
    )
    purchase = api_post(
        f"{MARKETPLACE_URL}/purchase/{dataset['id']}",
        {"accept_license": True},
        headers=buyer_headers,
    )

    response = requests.put(
        f"{MARKETPLACE_URL}/me/delta-sharing-server-url",
        json={"server_url": "http://seller-unavailable:9999"},
        headers=seller_headers,
        timeout=10,
    )
    assert response.status_code == 200, response.text

    datasets = api_get(f"{MARKETPLACE_URL}/datasets?q=Seller%20Down", headers=buyer_headers)
    shares = api_get(f"{MARKETPLACE_URL}/my-shares", headers=buyer_headers)

    assert dataset["id"] in {item["id"] for item in datasets}
    assert purchase["share_id"] in {item["id"] for item in shares}
