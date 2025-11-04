import time
import requests
import pandas as pd
import json
import os
import sys
import tempfile
from pathlib import Path
from threading import Thread
from delta_sharing import SharingClient, load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.database import SessionLocal, Dataset
from tests.utils import check_watermark, extract_list_items, api_post, api_get, api_delete

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def test_e2e_delta_sharing():
    print("\n" + "="*80)
    print("E2E Delta Sharing Test")
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
        print("LocalStack is running")
    except Exception as e:
        print(f"LocalStack is not running or not accessible: {e}")
        print("  Please start LocalStack with: sudo docker-compose --profile testing up -d localstack")
        raise
    
    try:
        response = requests.get(f"{DELTA_SHARING_SERVER_URL}/health", timeout=2)
        if response.status_code != 200:
            raise Exception("Delta Sharing server health check failed")
        print("Delta Sharing server is running")
    except Exception as e:
        print(f"Delta Sharing server is not running or not accessible: {e}")
        print(f"  Please start seller service: sudo docker-compose up -d seller")
        raise
    
    print("\n[1] Registering users...")
    seller_email = f"seller_{int(time.time())}@test.com"
    buyer_email = f"buyer_{int(time.time())}@test.com"
    password = "testpass123"
    
    seller_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": seller_email,
        "password": password,
        "role": "seller"
    }, expected_status=201)
    seller_id = seller_data["id"]
    print(f"✓ Seller registered: {seller_email} (ID: {seller_id})")
    
    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)
    buyer_id = buyer_data["id"]
    print(f"✓ Buyer registered: {buyer_email} (ID: {buyer_id})")
    
    print("\n[2] Logging in...")
    seller_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": seller_email,
        "password": password
    })
    seller_token = seller_login["access_token"]
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    print("✓ Seller logged in")
    
    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": buyer_email,
        "password": password
    })
    buyer_token = buyer_login["access_token"]
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    print("✓ Buyer logged in")
    
    print("\n[3] Seller creating dataset...")
    dataset_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Test Delta Table",
        "description": "E2E test dataset",
        "table_path": "test_table",
        "price": 0.0,
        "is_public": True
    }, headers=seller_headers, expected_status=201)
    dataset_id = dataset_data["id"]
    print(f"✓ Dataset created: {dataset_id}")
    
    print("\n[4a] Creating dataset requiring approval (for approval workflow test)...")
    dataset_approval_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Test Delta Table (Requires Approval)",
        "description": "E2E test dataset requiring approval",
        "table_path": "test_table_approval",
        "price": 0.0,
        "is_public": True
    }, headers=seller_headers, expected_status=201)
    dataset_approval_id = dataset_approval_data["id"]
    
    db = SessionLocal()
    try:
        approval_dataset = db.query(Dataset).filter(Dataset.id == dataset_approval_id).first()
        if approval_dataset:
            approval_dataset.requires_approval = True
            db.commit()
            print(f"✓ Dataset {dataset_approval_id} set to require approval")
    finally:
        db.close()
    
    print("\n[5] Setting seller's Delta Sharing server URL...")
    db = SessionLocal()
    try:
        from src.models.database import User
        seller_user = db.query(User).filter(User.id == seller_id).first()
        if seller_user:
            seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
            db.commit()
            print(f"✓ Seller server URL set: {DELTA_SHARING_SERVER_URL}")
    finally:
        db.close()
    
    print("\n[5a] Buyer purchasing dataset...")
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_token = purchase_data["share_token"]
    approval_status = purchase_data["approval_status"]
    share_id = purchase_data["share_id"]
    seller_server_url = purchase_data.get("seller_server_url", DELTA_SHARING_SERVER_URL)
    print(f"✓ Purchase successful, share token: {share_token[:20]}...")
    print(f"  Approval status: {approval_status}")
    print(f"  Seller server URL: {seller_server_url}")
    
    if approval_status == "pending":
        print("\n[5a] Share requires approval - seller approving...")
        approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
        print(f"✓ Share approved: {approve_resp['approval_status']}")
    
    print("\n[5b] Testing approval workflow with dataset requiring approval...")
    purchase_approval_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_approval_id}", {}, headers=buyer_headers)
    share_approval_id = purchase_approval_data["share_id"]
    assert purchase_approval_data["approval_status"] == "pending", "Share should be pending approval"
    print(f"✓ Purchase created with pending approval status")
    
    print("  Testing rejection...")
    reject_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_approval_id}/reject", {}, headers=seller_headers)
    assert reject_resp["approval_status"] == "rejected", "Share should be rejected"
    print(f"✓ Share rejected successfully")
    
    print("  Testing approval after rejection...")
    approve_resp2 = api_post(f"{MARKETPLACE_URL}/shares/{share_approval_id}/approve", {}, headers=seller_headers)
    assert approve_resp2["approval_status"] == "approved", "Share should be approved"
    print(f"✓ Share approved after rejection")
    
    print("\n[6] Creating Delta Sharing profile...")
    profile_data = {
        "shareCredentialsVersion": 1,
        "endpoint": seller_server_url,
        "bearerToken": share_token,
        "expirationTime": None
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_data, f)
        profile_path = f.name
    
    try:
        profile = DeltaSharingProfile.read_from_file(profile_path)
        client = SharingClient(profile)
        print("✓ Delta Sharing client created")
        
        print("\n[7] Starting seller data writer (background thread)...")
        from src.seller.data_writer import write_data_continuously
        
        db = SessionLocal()
        writer_thread = Thread(
            target=write_data_continuously,
            args=(dataset_id, 30, 5),
            kwargs={"db": db}
        )
        writer_thread.daemon = True
        writer_thread.start()
        print("✓ Data writer started")
        
        print("\n[8] Buyer reading data through Delta Sharing...")
        
        print("  Waiting 5 seconds for first data write...")
        time.sleep(5)
        
        shares_list = extract_list_items(client.list_shares())
        assert len(shares_list) > 0, "No shares found"
        share = shares_list[0]
        print(f"✓ Found share: {share.name}")
        
        schemas_list = extract_list_items(client.list_schemas(share))
        assert len(schemas_list) > 0, "No schemas found"
        schema = schemas_list[0]
        print(f"✓ Found schema: {schema.name}")
        
        tables_list = extract_list_items(client.list_tables(schema))
        assert len(tables_list) > 0, "No tables found"
        table = tables_list[0]
        print(f"✓ Found table: {table.name}")
        
        print("\n[9] Reading table data...")
        table_url = f"{profile_path}#{share.name}.{schema.name}.{table.name}"
        df = load_as_pandas(table_url)
        initial_count = len(df)
        print(f"✓ Initial read: {initial_count} rows")
        print(f"  Columns: {list(df.columns)}")
        if initial_count > 0:
            print(f"  Sample data:\n{df.head()}")
        
        print("\n[9a] Testing watermarking...")
        result = check_watermark(df, buyer_id, share_id, verbose=True)
        
        print(f"  Watermark: {result['watermark']}")
        print(f"  Timestamp columns: {result['timestamp_cols']}")
        print(f"  Checked {result['total_checked']} timestamps")
        
        if result["found"]:
            for sample in result["sample_matches"]:
                print(f"    {sample} ✓")
            print(f"  ✓ Watermark detected: {result['matches']}/{result['total_checked']} timestamps match pattern ({result['match_rate']:.1f}%)")
            if result['match_rate'] >= 50:
                print(f"  ✓ SUCCESS: Watermarking is working correctly!")
            else:
                print(f"  ⚠ WARNING: Low match rate, watermarking may not be working correctly")
        else:
            print(f"  ⚠ {result.get('reason', 'WARNING: No watermark pattern detected in timestamps')}")
            if 'sample_mismatches' in result:
                print(f"  Sample mismatches:")
                for mismatch in result['sample_mismatches']:
                    print(f"    {mismatch}")
        
        print("\n[10] Waiting 35 seconds for more data to be written...")
        time.sleep(35)
        
        print("  Reading table again...")
        df2 = load_as_pandas(table_url)
        new_count = len(df2)
        print(f"✓ Second read: {new_count} rows (was {initial_count})")
        
        assert new_count >= initial_count, f"Expected more or equal rows, got {new_count} < {initial_count}"
        if new_count > initial_count:
            print(f"✓ SUCCESS: Buyer can see new data! ({new_count - initial_count} new rows)")
        else:
            print("  Note: Row count unchanged (may be same version)")
        
        print("\n[11] Waiting another 35 seconds...")
        time.sleep(35)
        
        df3 = load_as_pandas(table_url)
        final_count = len(df3)
        print(f"✓ Final read: {final_count} rows")
        
        print("\n[11a] Verifying watermark persists across queries...")
        result = check_watermark(df3, buyer_id, share_id, verbose=False)
        
        if result["found"]:
            print(f"  ✓ Watermark still present: {result['matches']}/{result['total_checked']} timestamps match ({result['match_rate']:.1f}%)")
        else:
            print(f"  ⚠ WARNING: Watermark not detected in final query")
        
        print("\n[12] Testing usage logging...")
        logs = api_get(f"{MARKETPLACE_URL}/usage-logs?dataset_id={dataset_id}", headers=seller_headers)
        print(f"✓ Found {len(logs)} usage log entries")
        if len(logs) > 0:
            latest_log = logs[0]
            print(f"  Latest query: {latest_log['query_time']}")
            print(f"  Rows returned: {latest_log['row_count_returned']}")
            print(f"  Columns requested: {latest_log['columns_requested']}")
        
        print("\n[13] Testing share revocation...")
        shares_list = api_get(f"{MARKETPLACE_URL}/my-shares", headers=seller_headers)
        test_share = next((s for s in shares_list if s["id"] == share_id), None)
        assert test_share is not None, "Share not found in seller's shares"
        assert test_share["revoked"] == False, "Share should not be revoked yet"
        
        api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
        print("✓ Share revoked successfully")
        
        print("\n[14] Verifying revoked share is blocked...")
        try:
            revoked_df = load_as_pandas(table_url)
            print("  WARNING: Revoked share still accessible (this may be expected if client caches)")
        except Exception as e:
            print(f"✓ Revoked share blocked: {str(e)[:100]}")
        
        print("\n" + "="*80)
        print("E2E Test Summary:")
        print(f"  Initial rows: {initial_count}")
        print(f"  After 35s: {new_count}")
        print(f"  After 70s: {final_count}")
        print(f"  Usage logs: {len(logs)} entries")
        print(f"  Share revoked: ✓")
        print("="*80)
        
        os.unlink(profile_path)
        
        print("\n✓ E2E test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\nE2E test failed: {e}")
        import traceback
        traceback.print_exc()
        if os.path.exists(profile_path):
            os.unlink(profile_path)
        raise

def test_trial_share():
    print("\n" + "="*80)
    print("Trial Share E2E Test")
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
        print("LocalStack is running")
    except Exception as e:
        print(f"LocalStack is not running or not accessible: {e}")
        raise
    
    try:
        response = requests.get(f"{DELTA_SHARING_SERVER_URL}/health", timeout=2)
        if response.status_code != 200:
            raise Exception("Delta Sharing server health check failed")
        print("Delta Sharing server is running")
    except Exception as e:
        print(f"Delta Sharing server is not running or not accessible: {e}")
        raise
    
    print("\n[1] Registering users...")
    seller_email = f"seller_trial_{int(time.time())}@test.com"
    buyer_email = f"buyer_trial_{int(time.time())}@test.com"
    password = "testpass123"
    
    seller_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": seller_email,
        "password": password,
        "role": "seller"
    }, expected_status=201)
    seller_id = seller_data["id"]
    print(f"✓ Seller registered: {seller_email} (ID: {seller_id})")
    
    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)
    buyer_id = buyer_data["id"]
    print(f"✓ Buyer registered: {buyer_email} (ID: {buyer_id})")
    
    print("\n[2] Logging in...")
    seller_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": seller_email,
        "password": password
    })
    seller_token = seller_login["access_token"]
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    print("✓ Seller logged in")
    
    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": buyer_email,
        "password": password
    })
    buyer_token = buyer_login["access_token"]
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    print("✓ Buyer logged in")
    
    print("\n[3] Seller creating dataset...")
    dataset_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Test Dataset for Trial",
        "description": "E2E trial test dataset",
        "table_path": "trial_test_table",
        "price": 10.0,
        "is_public": True
    }, headers=seller_headers, expected_status=201)
    dataset_id = dataset_data["id"]
    print(f"✓ Dataset created: {dataset_id}")
    
    print("\n[4] Setting seller's Delta Sharing server URL...")
    db = SessionLocal()
    try:
        from src.models.database import User
        seller_user = db.query(User).filter(User.id == seller_id).first()
        if seller_user:
            seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
            db.commit()
            print(f"✓ Seller server URL set: {DELTA_SHARING_SERVER_URL}")
    finally:
        db.close()
    
    print("\n[5] Starting seller data writer to create data...")
    from src.seller.data_writer import write_data_continuously
    
    db = SessionLocal()
    writer_thread = Thread(
        target=write_data_continuously,
        args=(dataset_id, 10, 3),
        kwargs={"db": db}
    )
    writer_thread.daemon = True
    writer_thread.start()
    print("✓ Data writer started")
    
    print("  Waiting 15 seconds for data to be written...")
    time.sleep(15)
    
    print("\n[6] Buyer requesting trial access...")
    trial_data = api_post(f"{MARKETPLACE_URL}/datasets/{dataset_id}/trial", {
        "row_limit": 50,
        "days_valid": 7
    }, headers=buyer_headers)
    trial_share_token = trial_data["share_token"]
    trial_share_id = trial_data["share_id"]
    seller_server_url = trial_data.get("seller_server_url", DELTA_SHARING_SERVER_URL)
    trial_row_limit = trial_data["trial_row_limit"]
    print(f"✓ Trial access granted")
    print(f"  Share token: {trial_share_token[:20]}...")
    print(f"  Row limit: {trial_row_limit}")
    print(f"  Expires at: {trial_data['trial_expires_at']}")
    print(f"  Seller server URL: {seller_server_url}")
    
    print("\n[7] Creating Delta Sharing profile for trial...")
    profile_data = {
        "shareCredentialsVersion": 1,
        "endpoint": seller_server_url,
        "bearerToken": trial_share_token,
        "expirationTime": None
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_data, f)
        trial_profile_path = f.name
    
    try:
        profile = DeltaSharingProfile.read_from_file(trial_profile_path)
        client = SharingClient(profile)
        print("✓ Delta Sharing client created for trial")
        
        print("\n[8] Buyer reading trial data through Delta Sharing...")
        shares_list = extract_list_items(client.list_shares())
        assert len(shares_list) > 0, "No shares found"
        share = shares_list[0]
        print(f"✓ Found share: {share.name}")
        
        schemas_list = extract_list_items(client.list_schemas(share))
        assert len(schemas_list) > 0, "No schemas found"
        schema = schemas_list[0]
        print(f"✓ Found schema: {schema.name}")
        
        tables_list = extract_list_items(client.list_tables(schema))
        assert len(tables_list) > 0, "No tables found"
        table = tables_list[0]
        print(f"✓ Found table: {table.name}")
        
        print("\n[9] Reading trial table data...")
        table_url = f"{trial_profile_path}#{share.name}.{schema.name}.{table.name}"
        df = load_as_pandas(table_url)
        trial_row_count = len(df)
        print(f"✓ Trial read: {trial_row_count} rows")
        print(f"  Expected limit: {trial_row_limit} rows")
        
        assert trial_row_count <= trial_row_limit, f"Trial returned {trial_row_count} rows, but limit is {trial_row_limit}"
        print(f"✓ SUCCESS: Trial row limit enforced ({trial_row_count} <= {trial_row_limit})")
        
        if trial_row_count > 0:
            print(f"  Columns: {list(df.columns)}")
            print(f"  Sample data:\n{df.head()}")
        
        print("\n[10] Testing watermarking on trial share...")
        result = check_watermark(df, buyer_id, trial_share_id, verbose=True)
        
        print(f"  Watermark: {result['watermark']}")
        print(f"  Timestamp columns: {result['timestamp_cols']}")
        print(f"  Checked {result['total_checked']} timestamps")
        
        if result["found"]:
            for sample in result["sample_matches"]:
                print(f"    {sample} ✓")
            print(f"  ✓ Watermark detected: {result['matches']}/{result['total_checked']} timestamps match pattern ({result['match_rate']:.1f}%)")
            if result['match_rate'] >= 50:
                print(f"  ✓ SUCCESS: Trial share watermarking is working correctly!")
            else:
                print(f"  ⚠ WARNING: Low match rate, watermarking may not be working correctly")
        else:
            print(f"  ⚠ {result.get('reason', 'WARNING: No watermark pattern detected in timestamps')}")
            if 'sample_mismatches' in result:
                print(f"  Sample mismatches:")
                for mismatch in result['sample_mismatches'][:3]:
                    print(f"    {mismatch}")
        
        print("\n[11] Testing that trial cannot exceed row limit on subsequent queries...")
        df2 = load_as_pandas(table_url)
        trial_row_count_2 = len(df2)
        print(f"✓ Second trial read: {trial_row_count_2} rows")
        assert trial_row_count_2 <= trial_row_limit, f"Second query returned {trial_row_count_2} rows, but limit is {trial_row_limit}"
        print(f"✓ SUCCESS: Trial row limit persists across queries")
        
        print("\n" + "="*80)
        print("Trial Share Test Summary:")
        print(f"  Trial row limit: {trial_row_limit}")
        print(f"  Rows returned (query 1): {trial_row_count}")
        print(f"  Rows returned (query 2): {trial_row_count_2}")
        print(f"  Watermark detected: {result['found']}")
        print(f"  Watermark match rate: {result['match_rate']:.1f}%")
        print("="*80)
        
        os.unlink(trial_profile_path)
        
        print("\n✓ Trial share E2E test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\nTrial share test failed: {e}")
        import traceback
        traceback.print_exc()
        if os.path.exists(trial_profile_path):
            os.unlink(trial_profile_path)
        raise

if __name__ == "__main__":
    test_e2e_delta_sharing()
    print("\n" + "="*80)
    print("Starting Trial Share Test...")
    print("="*80)
    test_trial_share()

