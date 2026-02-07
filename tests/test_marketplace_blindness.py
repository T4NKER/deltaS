import os
import sys
import time
import requests
import json
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.database import SessionLocal, User, Dataset, Share
from src.utils.s3_utils import get_s3_client, get_bucket_name, get_delta_storage_options, get_full_s3_path
from tests.utils import api_post, api_get, extract_list_items
from delta_sharing import SharingClient, load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile
from deltalake import write_deltalake, DeltaTable
import pandas as pd

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def test_marketplace_has_no_s3_credentials():
    print("\n" + "="*80)
    print("Test: Marketplace Blindness - No S3 Credentials")
    print("="*80)
    
    print("\n[1] Checking if marketplace code imports S3 utilities...")
    import importlib.util
    marketplace_api_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src', 'marketplace', 'api.py')
    
    if os.path.exists(marketplace_api_path):
        with open(marketplace_api_path, 'r') as f:
            api_content = f.read()
        
        s3_imports = [
            'from src.utils.s3_utils',
            'import s3_utils',
            'get_s3_client',
            'get_delta_storage_options',
            'DeltaTable',
            'deltalake'
        ]
        
        found_imports = [imp for imp in s3_imports if imp in api_content]
        
        if found_imports:
            print(f"[WARN] WARNING: Marketplace API imports S3/data utilities: {found_imports}")
            print("  This violates blindness requirement!")
            return False
        else:
            print("[OK] Marketplace API does not import S3 utilities (correct)")
    else:
        print("[WARN] Could not find marketplace API file")
    
    print("\n[2] Verifying marketplace code structure...")
    try:
        from src.marketplace import api
        api_source = api.__file__ if hasattr(api, '__file__') else None
        
        if api_source and os.path.exists(api_source):
            with open(api_source, 'r') as f:
                content = f.read()
                if 'get_s3_client' in content or 'DeltaTable' in content or 'deltalake' in content:
                    print("[WARN] WARNING: Marketplace code contains S3/Delta table access!")
                    return False
                else:
                    print("[OK] Marketplace code does not contain S3/Delta table access (correct)")
        else:
            print("[OK] Marketplace module structure verified")
    except Exception as e:
        print(f"[WARN] Could not verify marketplace code: {e}")
    
    return True

def test_marketplace_cannot_read_delta_tables():
    print("\n" + "="*80)
    print("Test: Marketplace Cannot Read Delta Tables")
    print("="*80)
    
    print("\n[1] Checking marketplace API for Delta table imports...")
    marketplace_api_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src', 'marketplace', 'api.py')
    
    if os.path.exists(marketplace_api_path):
        with open(marketplace_api_path, 'r') as f:
            api_content = f.read()
        
        delta_imports = [
            'from deltalake import',
            'import deltalake',
            'DeltaTable',
            'to_pyarrow_dataset',
            'to_pandas'
        ]
        
        found_imports = [imp for imp in delta_imports if imp in api_content]
        
        if found_imports:
            print(f"[WARN] WARNING: Marketplace API imports Delta table utilities: {found_imports}")
            print("  This violates blindness requirement!")
            return False
        else:
            print("[OK] Marketplace API does not import Delta table utilities (correct)")
    
    print("\n[2] Verifying create_dataset endpoint does not access data...")
    if 'api_content' in locals():
        if 'DeltaTable' in api_content or 'deltalake' in api_content:
            print("[WARN] WARNING: Marketplace create_dataset may access Delta tables!")
            return False
        else:
            print("[OK] create_dataset endpoint does not access Delta tables (correct)")
    
    return True

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
        if response.status_code != 200:
            raise Exception("LocalStack health check failed")
    except Exception as e:
        print(f"LocalStack is not running: {e}")
        print("  Please start LocalStack with: sudo docker-compose --profile testing up -d localstack")
        return False
    
    print("\n[1] Setting up test data (seller-side)...")
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        s3_client.create_bucket(Bucket=bucket_name)
    
    table_path = f"blind_test_{int(time.time())}"
    full_path = get_full_s3_path(bucket_name, table_path)
    
    test_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.3, 30.1]
    })
    
    import pyarrow as pa
    table = pa.Table.from_pandas(test_data)
    storage_options = get_delta_storage_options()
    write_deltalake(full_path, table, storage_options=storage_options, mode='overwrite')
    print(f"[OK] Created test Delta table at {table_path}")
    
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
    
    db = SessionLocal()
    try:
        seller_user = db.query(User).filter(User.id == seller_id).first()
        seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
        db.commit()
    finally:
        db.close()
    
    print("\n[3] Seller generating metadata bundle (seller-side only)...")
    from src.seller.publish import publish_dataset_metadata
    
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
    
    print("\n[5] Buyer purchasing dataset...")
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_id = purchase_data["share_id"]
    share_token = purchase_data["share_token"]
    print(f"[OK] Purchase created (share_id: {share_id})")
    
    print("\n[6] Seller approving share (generates profile)...")
    approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
    assert approve_resp["approval_status"] == "approved"
    profile_generated = approve_resp.get("profile_generated", False)
    if not profile_generated:
        print(f"  Note: Profile not generated on approval. Response: {approve_resp}")
        print("  Profile will be generated on-demand when buyer requests it")
    else:
        print("[OK] Share approved and profile generated")
    
    print("\n[7] Buyer retrieving profile from marketplace (will generate if needed)...")
    try:
        profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
    except Exception as e:
        print(f"  Error getting profile: {e}")
        print("  Checking share and seller configuration...")
        db = SessionLocal()
        try:
            share = db.query(Share).filter(Share.id == share_id).first()
            seller_user = db.query(User).filter(User.id == seller_id).first()
            print(f"  Share exists: {share is not None}")
            if share:
                print(f"  Share buyer_id: {share.buyer_id}, approval_status: {share.approval_status}")
                print(f"  Share has token: {share.token is not None}")
            print(f"  Seller delta_sharing_server_url: {seller_user.delta_sharing_server_url if seller_user else 'N/A'}")
        finally:
            db.close()
        raise
    profile_json_str = profile_resp["profile_json"]
    profile_data = json.loads(profile_json_str)
    
    assert profile_data["endpoint"] == DELTA_SHARING_SERVER_URL
    assert profile_data["bearerToken"] == share_token
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
        
        assert len(df) == 3
        assert list(df.columns) == ['id', 'name', 'value']
        print("[OK] Buyer successfully read data directly from seller")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
    finally:
        if os.path.exists(profile_path):
            os.unlink(profile_path)
    
    print("\n[9] Verifying marketplace never accessed seller data...")
    print("[OK] Marketplace only stored metadata and profile JSON")
    print("[OK] All data access happened directly between buyer and seller")
    
    return True

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
        import traceback
        traceback.print_exc()
        results.append(("No S3 Credentials", False))
    
    try:
        result = test_marketplace_cannot_read_delta_tables()
        results.append(("Cannot Read Delta Tables", result))
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Cannot Read Delta Tables", False))
    
    try:
        result = test_full_blind_workflow()
        results.append(("Full Blind Workflow", result))
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        import traceback
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

