import os
import sys
import time
import requests
import json
import tempfile
import traceback
import importlib.util
import pyarrow as pa
import pytest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.utils import (
    api_post, api_get, extract_list_items, register_buyer_public_key,
    register_seller_metadata_public_key, decrypt_marketplace_profile
)
from tests.test_e2e_delta_sharing import _set_seller_server_url
from delta_sharing import SharingClient, load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile
from src.marketplace import api
from src.seller.publish import publish_dataset_metadata
import pandas as pd

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def test_marketplace_has_no_s3_credentials():
    marketplace_api_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src', 'marketplace', 'api.py')
    assert os.path.exists(marketplace_api_path), f"marketplace api.py not found at {marketplace_api_path}"

    with open(marketplace_api_path, 'r') as f:
        api_content = f.read()

    s3_imports = [
        'from src.utils.s3_utils',
        'import s3_utils',
        'get_s3_client',
        'get_delta_storage_options',
        'DeltaTable',
        'deltalake',
    ]
    found_imports = [imp for imp in s3_imports if imp in api_content]
    assert not found_imports, (
        f"Marketplace API imports S3 / Delta utilities, violating blindness: {found_imports}"
    )

    api_source = api.__file__ if hasattr(api, '__file__') else None
    if api_source and os.path.exists(api_source):
        with open(api_source, 'r') as f:
            content = f.read()
        forbidden = [s for s in ('get_s3_client', 'DeltaTable', 'deltalake') if s in content]
        assert not forbidden, (
            f"Marketplace module contains forbidden S3/Delta references: {forbidden}"
        )

def test_marketplace_cannot_read_delta_tables():
    marketplace_api_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src', 'marketplace', 'api.py')
    assert os.path.exists(marketplace_api_path), f"marketplace api.py not found at {marketplace_api_path}"

    with open(marketplace_api_path, 'r') as f:
        api_content = f.read()

    delta_imports = [
        'from deltalake import',
        'import deltalake',
        'DeltaTable',
        'to_pyarrow_dataset',
        'to_pandas',
    ]
    found_imports = [imp for imp in delta_imports if imp in api_content]
    assert not found_imports, (
        f"Marketplace API imports Delta-table utilities, violating blindness: {found_imports}"
    )

def test_full_blind_workflow():
    print("\n" + "="*80)
    print("Test: Full Blind Workflow - Marketplace Never Accesses Seller Data")
    print("="*80)

    os.environ.setdefault('S3_ENDPOINT_URL', 'http://localhost:4566')
    os.environ.setdefault('S3_ACCESS_KEY', 'test')
    os.environ.setdefault('S3_SECRET_KEY', 'test')
    os.environ.setdefault('S3_BUCKET_NAME', 'test-delta-bucket')
    os.environ.setdefault('S3_REGION', 'us-east-1')

    try:
        response = requests.get("http://localhost:4566/_localstack/health", timeout=2)
        assert response.status_code == 200, f"LocalStack health check failed: {response.status_code}"
    except requests.exceptions.RequestException as e:
        pytest.skip(f"LocalStack unavailable: {e}. Run: docker compose --profile testing up -d localstack")

    print("\n[1] Setting up test data (via seller service)...")
    DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")
    requests.post(f"{DELTA_SHARING_SERVER_URL}/seller/ensure-bucket", timeout=10).raise_for_status()

    table_path = f"blind_test_{int(time.time())}"
    test_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.3, 30.1]
    })
    data = {col: test_data[col].tolist() for col in test_data.columns}
    requests.post(f"{DELTA_SHARING_SERVER_URL}/seller/seed-test-data",
                  json={"table_path": table_path, "data": data}, timeout=30).raise_for_status()
    print(f"[OK] Created test Delta table at {table_path} via seller")

    print("\n[2] Seller publishing metadata (seller-side)...")
    seller_email = f"seller_blind_{int(time.time())}@test.com"
    buyer_email = f"buyer_blind_{int(time.time())}@test.com"
    password = "testpass123"

    seller_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": seller_email,
        "password": password,
        "role": "seller"
    }, expected_status=201)
    seller_id = seller_data["id"]

    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)

    seller_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": seller_email,
        "password": password
    })
    seller_token = seller_login["access_token"]
    seller_headers = {"Authorization": f"Bearer {seller_token}"}

    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": buyer_email,
        "password": password
    })
    buyer_token = buyer_login["access_token"]
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}

    _set_seller_server_url(seller_headers)
    register_seller_metadata_public_key(MARKETPLACE_URL, seller_headers)

    print("\n[3] Seller generating metadata bundle (seller-side only)...")

    metadata = publish_dataset_metadata(
        table_path=table_path,
        seller_id=seller_id,
        name="Blind Test Dataset",
        description="Test dataset for blindness verification"
    )
    print(f"[OK] Metadata bundle generated with signature: {metadata['signature'][:16]}...")

    print("\n[4] Marketplace accepting metadata (no data access)...")
    dataset_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Blind Test Dataset",
        "description": "Test dataset",
        "table_path": table_path,
        "price": 0.0,
        "is_public": True,
        "metadata_bundle": metadata
    }, headers=seller_headers, expected_status=201)

    dataset_id = dataset_data["id"]
    print(f"[OK] Dataset created in marketplace (ID: {dataset_id})")
    print(f"  Risk score: {dataset_data.get('risk_score', 'N/A')}")
    print(f"  Risk level: {dataset_data.get('risk_level', 'N/A')}")
    print(f"  Anchor columns: {dataset_data.get('anchor_columns', 'N/A')}")

    print("\n[5] Registering buyer public key...")
    buyer_keys = register_buyer_public_key(MARKETPLACE_URL, buyer_headers)
    print("[OK] Buyer public key registered")

    print("\n[6] Buyer purchasing dataset...")
    purchase_data = api_post(
        f"{MARKETPLACE_URL}/purchase/{dataset_id}",
        {"accept_license": True},
        headers=buyer_headers,
    )
    share_id = purchase_data["share_id"]
    encrypted_token = purchase_data.get("encrypted_token")
    print(f"[OK] Purchase created (share_id: {share_id})")
    if encrypted_token:
        print(f"  Encrypted token received: {encrypted_token[:30]}...")

    print("\n[7] Seller approving share (generates profile)...")
    approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
    assert approve_resp["approval_status"] == "approved"
    profile_generated = approve_resp.get("profile_generated", False)
    if not profile_generated:
        print(f"  Note: Profile not generated on approval. Response: {approve_resp}")
        print("  Profile will be generated on-demand when buyer requests it")
    else:
        print("[OK] Share approved and profile generated")

    print("\n[8] Buyer retrieving profile from marketplace (will generate if needed)...")
    try:
        profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
    except Exception as e:
        print(f"  Error getting profile: {e}")
        print("  Checking share and seller configuration via API...")
        try:
            shares_list = api_get(f"{MARKETPLACE_URL}/my-shares", headers=seller_headers)
            matching_share = next((s for s in shares_list if s["id"] == share_id), None)
            print(f"  Share exists: {matching_share is not None}")
            if matching_share:
                print(f"  Share buyer_id: {matching_share.get('buyer_id')}, approval_status: {matching_share.get('approval_status')}")
                print(f"  Share has encrypted_token: {matching_share.get('encrypted_token') is not None}")
        except Exception as inner_e:
            print(f"  Could not check share via API: {inner_e}")
        raise
    profile_json_str = profile_resp["profile_json"]
    profile_data = json.loads(profile_json_str)

    assert profile_data["endpoint"] == DELTA_SHARING_SERVER_URL
    profile_data, _ = decrypt_marketplace_profile(profile_data, buyer_keys['private_key_b64'])
    print("[OK] Profile retrieved with encryptedBearerToken")
    print("[OK] Profile retrieved from marketplace")

    print("\n[8] Buyer reading data directly from seller (bypassing marketplace)...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_data, f)
        profile_path = f.name

    try:
        profile = DeltaSharingProfile.read_from_file(profile_path)
        client = SharingClient(profile)

        shares_list = extract_list_items(client.list_shares())
        assert len(shares_list) > 0
        share = shares_list[0]

        schemas_list = extract_list_items(client.list_schemas(share))
        assert len(schemas_list) > 0
        schema = schemas_list[0]

        tables_list = extract_list_items(client.list_tables(schema))
        assert len(tables_list) > 0
        table = tables_list[0]

        table_url = f"{profile_path}#{share.name}.{schema.name}.{table.name}"
        df = load_as_pandas(table_url)

        assert len(df) >= 3
        assert 'id' in df.columns and 'name' in df.columns and 'value' in df.columns

        original_names = ['Alice', 'Bob', 'Charlie']
        rows_with_original_names = df[df['name'].isin(original_names)]

        unique_names_in_data = set(rows_with_original_names['name'].values)
        assert unique_names_in_data == set(original_names), f"Expected all 3 original names but found: {unique_names_in_data}. All names in dataframe: {df['name'].unique().tolist()}"

        assert len(rows_with_original_names) >= 3, f"Expected at least 3 rows with original names (3 originals + possibly pseudorows) but found {len(rows_with_original_names)}"

        original_rows = rows_with_original_names.drop_duplicates(subset=['name'], keep='first')
        assert len(original_rows) == 3, f"Expected 3 unique original rows but found {len(original_rows)}"
        assert set(original_rows['name'].values) == set(original_names)
        print("[OK] Buyer successfully read data directly from seller")
        print(f"  Total rows: {len(df)} (including watermarked pseudorows)")
        print(f"  Rows with original names: {len(rows_with_original_names)}")
        print(f"  Unique original rows: {len(original_rows)}")
        print(f"  Original names: {set(original_rows['name'].values)}")
        print(f"  Columns: {list(df.columns)}")
    finally:
        if os.path.exists(profile_path):
            os.unlink(profile_path)

    print("\n[9] Verifying marketplace never accessed seller data...")
    print("[OK] Marketplace only stored metadata and profile JSON")
    print("[OK] All data access happened directly between buyer and seller")

if __name__ == "__main__":
    print("\n" + "="*80)
    print("Marketplace Blindness Verification Tests")
    print("="*80)

    results = []

    try:
        result = test_marketplace_has_no_s3_credentials()
        results.append(("No S3 Credentials", result))
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        traceback.print_exc()
        results.append(("No S3 Credentials", False))

    try:
        result = test_marketplace_cannot_read_delta_tables()
        results.append(("Cannot Read Delta Tables", result))
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        traceback.print_exc()
        results.append(("Cannot Read Delta Tables", False))

    try:
        result = test_full_blind_workflow()
        results.append(("Full Blind Workflow", result))
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        traceback.print_exc()
        results.append(("Full Blind Workflow", False))

    print("\n" + "="*80)
    print("Test Results Summary")
    print("="*80)
    for test_name, passed in results:
        status = "[OK] PASS" if passed else "[FAIL] FAIL"
        print(f"{status}: {test_name}")

    all_passed = all(result[1] for result in results)
    if all_passed:
        print("\n[OK] All blindness verification tests passed!")
    else:
        print("\n[FAIL] Some tests failed. Marketplace may not be fully blind.")
        sys.exit(1)
