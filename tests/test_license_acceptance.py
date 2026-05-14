import time

from tests.test_e2e_delta_sharing import _create_dataset_via_publish, _set_seller_server_url, MARKETPLACE_URL
from tests.utils import api_post, register_buyer_public_key

def test_purchase_requires_license_acceptance():
    seller_email = f"seller_license_{int(time.time() * 1000)}@test.com"
    buyer_email = f"buyer_license_{int(time.time() * 1000)}@test.com"
    password = "testpass123"

    seller_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": seller_email,
        "password": password,
        "role": "seller"
    }, expected_status=201)
    api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)

    seller_login = api_post(f"{MARKETPLACE_URL}/login", {"email": seller_email, "password": password})
    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {"email": buyer_email, "password": password})
    seller_headers = {"Authorization": f"Bearer {seller_login['access_token']}"}
    buyer_headers = {"Authorization": f"Bearer {buyer_login['access_token']}"}

    _set_seller_server_url(seller_headers)
    register_buyer_public_key(MARKETPLACE_URL, buyer_headers)

    dataset_data, _ = _create_dataset_via_publish(
        seller_headers,
        name="licensed_dataset",
        description="Dataset requiring license acceptance",
        table_path=f"licensed_table_{int(time.time() * 1000)}",
        price=5.0,
        is_public=True,
        anchor_columns="category,write_batch",
        license_name="CC-BY-4.0",
        license_terms="Attribution required for all downstream use",
        seller_id=seller_data["id"],
    )

    api_post(
        f"{MARKETPLACE_URL}/purchase/{dataset_data['id']}",
        {"accept_license": False},
        headers=buyer_headers,
        expected_status=400,
    )

    purchase = api_post(
        f"{MARKETPLACE_URL}/purchase/{dataset_data['id']}",
        {"accept_license": True},
        headers=buyer_headers,
    )

    assert purchase["accepted_license_name"] == "CC-BY-4.0"
    assert purchase["license_accepted_at"] is not None
