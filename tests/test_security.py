import os
import time
import requests

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def test_csrf_blocks_non_json_post():
    response = requests.post(
        f"{MARKETPLACE_URL}/register",
        data="not json",
        headers={"Content-Type": "text/plain"},
        timeout=5,
    )
    assert response.status_code == 415, f"Expected 415, got {response.status_code}: {response.text}"
    print("[OK] CSRF blocks non-JSON POST without auth header")

def test_csrf_allows_json_post():
    response = requests.post(
        f"{MARKETPLACE_URL}/register",
        json={"email": f"csrf_test_{int(time.time())}@test.com", "password": "test123", "role": "buyer"},
        timeout=5,
    )
    assert response.status_code == 201, f"Expected 201, got {response.status_code}: {response.text}"
    print("[OK] CSRF allows JSON POST")

def test_jwt_separation_marketplace_token_rejected_by_seller():
    email = f"jwt_test_{int(time.time())}@test.com"
    requests.post(f"{MARKETPLACE_URL}/register", json={"email": email, "password": "test123", "role": "seller"}, timeout=5)
    login = requests.post(f"{MARKETPLACE_URL}/login", json={"email": email, "password": "test123"}, timeout=5)
    marketplace_token = login.json()["access_token"]

    response = requests.post(
        f"{DELTA_SHARING_SERVER_URL}/seller/publish-metadata",
        json={"table_path": "test", "name": "test"},
        headers={"Authorization": f"Bearer {marketplace_token}"},
        timeout=5,
    )
    assert response.status_code == 401, f"Expected 401, got {response.status_code}: {response.text}"
    assert "Signature verification failed" in response.text or "Invalid" in response.text
    print("[OK] Marketplace JWT rejected by seller endpoint")

def test_jwt_separation_auth_token_exchange_works():
    email = f"jwt_exch_{int(time.time())}@test.com"
    seller_data = requests.post(f"{MARKETPLACE_URL}/register", json={"email": email, "password": "test123", "role": "seller"}, timeout=5).json()
    login = requests.post(f"{MARKETPLACE_URL}/login", json={"email": email, "password": "test123"}, timeout=5)
    marketplace_token = login.json()["access_token"]

    exchange = requests.post(
        f"{DELTA_SHARING_SERVER_URL}/seller/auth-token",
        json={"marketplace_token": marketplace_token, "seller_id": seller_data["id"]},
        timeout=5,
    )
    assert exchange.status_code == 200, f"Expected 200, got {exchange.status_code}: {exchange.text}"
    seller_token = exchange.json()["seller_token"]
    assert seller_token and len(seller_token) > 20
    print("[OK] Auth-token exchange produces valid seller JWT")
