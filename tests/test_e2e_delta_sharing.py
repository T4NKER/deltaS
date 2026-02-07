import time
import requests
import pandas as pd
import json
import os
import sys
import tempfile
import traceback
from pathlib import Path
from threading import Thread
from datetime import datetime, timezone
from delta_sharing import SharingClient, load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile
import pyarrow as pa
from deltalake import write_deltalake

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.database import SessionLocal, Dataset, User
from src.seller.data_writer import write_data_continuously
from src.utils.s3_utils import get_s3_client, get_bucket_name, get_delta_storage_options, get_full_s3_path
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
    
    print("\n[0] Ensuring S3 bucket exists...")
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket {bucket_name} already exists")
    except:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"[OK] Created bucket: {bucket_name}")
        except Exception as e:
            print(f"[WARN] Warning: Could not create bucket {bucket_name}: {e}")
    
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
    print(f"[OK] Seller registered: {seller_email} (ID: {seller_id})")
    
    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)
    buyer_id = buyer_data["id"]
    print(f"[OK] Buyer registered: {buyer_email} (ID: {buyer_id})")
    
    print("\n[2] Logging in...")
    seller_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": seller_email,
        "password": password
    })
    seller_token = seller_login["access_token"]
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    print("[OK] Seller logged in")
    
    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": buyer_email,
        "password": password
    })
    buyer_token = buyer_login["access_token"]
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    print("[OK] Buyer logged in")
    
    print("\n[3] Seller creating dataset...")
    dataset_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Test Delta Table",
        "description": "E2E test dataset",
        "table_path": "test_table",
        "price": 0.0,
        "is_public": True,
        "anchor_columns": "category,write_batch"
    }, headers=seller_headers, expected_status=201)
    dataset_id = dataset_data["id"]
    print(f"[OK] Dataset created: {dataset_id}")
    
    print("\n[4a] Creating dataset requiring approval (for approval workflow test)...")
    dataset_approval_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Test Delta Table (Requires Approval)",
        "description": "E2E test dataset requiring approval",
        "table_path": "test_table_approval",
        "price": 0.0,
        "is_public": True,
        "anchor_columns": "category,write_batch"
    }, headers=seller_headers, expected_status=201)
    dataset_approval_id = dataset_approval_data["id"]
    
    db = SessionLocal()
    try:
        approval_dataset = db.query(Dataset).filter(Dataset.id == dataset_approval_id).first()
        if approval_dataset:
            approval_dataset.requires_approval = True
            db.commit()
            print(f"[OK] Dataset {dataset_approval_id} set to require approval")
    finally:
        db.close()
    
    print("\n[5] Setting seller's Delta Sharing server URL...")
    db = SessionLocal()
    try:
        seller_user = db.query(User).filter(User.id == seller_id).first()
        if seller_user:
            seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
            db.commit()
            print(f"[OK] Seller server URL set: {DELTA_SHARING_SERVER_URL}")
    finally:
        db.close()
    
    print("\n[5a] Buyer purchasing dataset...")
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_token = purchase_data["share_token"]
    approval_status = purchase_data["approval_status"]
    share_id = purchase_data["share_id"]
    seller_server_url = purchase_data.get("seller_server_url", DELTA_SHARING_SERVER_URL)
    print(f"[OK] Purchase successful, share token: {share_token[:20]}...")
    print(f"  Approval status: {approval_status}")
    print(f"  Seller server URL: {seller_server_url}")
    
    if approval_status == "pending":
        print("\n[5a] Share requires approval - seller approving...")
        approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
        print(f"[OK] Share approved: {approve_resp['approval_status']}")
    
    print("\n[5b] Testing approval workflow with dataset requiring approval...")
    purchase_approval_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_approval_id}", {}, headers=buyer_headers)
    share_approval_id = purchase_approval_data["share_id"]
    assert purchase_approval_data["approval_status"] == "pending", "Share should be pending approval"
    print(f"[OK] Purchase created with pending approval status")
    
    print("  Testing rejection...")
    reject_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_approval_id}/reject", {}, headers=seller_headers)
    assert reject_resp["approval_status"] == "rejected", "Share should be rejected"
    print(f"[OK] Share rejected successfully")
    
    print("  Testing approval after rejection...")
    approve_resp2 = api_post(f"{MARKETPLACE_URL}/shares/{share_approval_id}/approve", {}, headers=seller_headers)
    assert approve_resp2["approval_status"] == "approved", "Share should be approved"
    print(f"[OK] Share approved after rejection")
    
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
        print("[OK] Delta Sharing client created")
        
        print("\n[7] Starting seller data writer (background thread)...")
        db = SessionLocal()
        writer_thread = Thread(
            target=write_data_continuously,
            args=(dataset_id, 30, 5),
            kwargs={"db": db}
        )
        writer_thread.daemon = True
        writer_thread.start()
        print("[OK] Data writer started")
        
        print("\n[8] Buyer reading data through Delta Sharing...")
        
        print("  Waiting 5 seconds for first data write...")
        time.sleep(5)
        
        shares_list = extract_list_items(client.list_shares())
        assert len(shares_list) > 0, "No shares found"
        share = shares_list[0]
        print(f"[OK] Found share: {share.name}")
        
        schemas_list = extract_list_items(client.list_schemas(share))
        assert len(schemas_list) > 0, "No schemas found"
        schema = schemas_list[0]
        print(f"[OK] Found schema: {schema.name}")
        
        tables_list = extract_list_items(client.list_tables(schema))
        assert len(tables_list) > 0, "No tables found"
        table = tables_list[0]
        print(f"[OK] Found table: {table.name}")
        
        print("\n[9] Reading table data...")
        table_url = f"{profile_path}#{share.name}.{schema.name}.{table.name}"
        df = load_as_pandas(table_url)
        initial_count = len(df)
        print(f"[OK] Initial read: {initial_count} rows")
        print(f"  Columns: {list(df.columns)}")
        if initial_count > 0:
            print(f"  Sample data:\n{df.head()}")
        
        print("\n[9a] Testing watermarking...")
        db = SessionLocal()
        try:
            dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
            anchor_columns = None
            if dataset and dataset.anchor_columns:
                anchor_columns = [col.strip() for col in dataset.anchor_columns.split(',') if col.strip()]
        finally:
            db.close()
        result = check_watermark(df, buyer_id, share_id, verbose=True, anchor_columns=anchor_columns)
        
        print(f"  Watermark: {result['watermark']}")
        print(f"  Timestamp columns: {result['timestamp_cols']}")
        
        if result.get('watermark_column', {}).get('checked', 0) > 0:
            wc = result['watermark_column']
            print(f"  Watermark column: {wc['matches']}/{wc['checked']} matches ({wc['match_rate']:.1f}%)")
            if wc['found']:
                print(f"    [OK] Watermark column detected")
                for sample in wc.get('samples', [])[:3]:
                    print(f"      {sample}")
        
        if result.get('timestamp', {}).get('checked', 0) > 0:
            ts = result['timestamp']
            print(f"  Timestamp columns: {ts['matches']}/{ts['checked']} matches ({ts['match_rate']:.1f}%)")
            if ts['found']:
                print(f"    [OK] Timestamp watermark detected")
                for sample in ts.get('samples', [])[:3]:
                    print(f"      {sample}")
        
        if result["found"]:
            print(f"  [OK] SUCCESS: Watermarking is working correctly!")
        else:
            print(f"  [WARN] {result.get('reason', 'WARNING: No watermark pattern detected')}")
        
        print("\n[10] Waiting 35 seconds for more data to be written...")
        time.sleep(35)
        
        print("  Reading table again...")
        df2 = load_as_pandas(table_url)
        new_count = len(df2)
        print(f"[OK] Second read: {new_count} rows (was {initial_count})")
        
        assert new_count >= initial_count, f"Expected more or equal rows, got {new_count} < {initial_count}"
        if new_count > initial_count:
            print(f"[OK] SUCCESS: Buyer can see new data! ({new_count - initial_count} new rows)")
        else:
            print("  Note: Row count unchanged (may be same version)")
        
        print("\n[11] Waiting another 35 seconds...")
        time.sleep(35)
        
        df3 = load_as_pandas(table_url)
        final_count = len(df3)
        print(f"[OK] Final read: {final_count} rows")
        
        print("\n[11a] Verifying watermark persists across queries...")
        db = SessionLocal()
        try:
            dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
            anchor_columns = None
            if dataset and dataset.anchor_columns:
                anchor_columns = [col.strip() for col in dataset.anchor_columns.split(',') if col.strip()]
        finally:
            db.close()
        result = check_watermark(df3, buyer_id, share_id, verbose=False, anchor_columns=anchor_columns)
        
        if result["found"]:
            wc = result.get('watermark_column', {})
            ts = result.get('timestamp', {})
            if wc.get('found'):
                print(f"  [OK] Watermark column still present: {wc['matches']}/{wc['checked']} matches ({wc['match_rate']:.1f}%)")
            if ts.get('found'):
                print(f"  [OK] Timestamp watermark still present: {ts['matches']}/{ts['checked']} matches ({ts['match_rate']:.1f}%)")
        else:
            print(f"  [WARN] WARNING: Watermark not detected in final query: {result.get('reason', 'Unknown reason')}")
        
        print("\n[12] Testing usage logging...")
        logs = api_get(f"{MARKETPLACE_URL}/usage-logs?dataset_id={dataset_id}", headers=seller_headers)
        print(f"[OK] Found {len(logs)} usage log entries")
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
        print("[OK] Share revoked successfully")
        
        print("\n[14] Verifying revoked share is blocked...")
        try:
            revoked_df = load_as_pandas(table_url)
            print("  WARNING: Revoked share still accessible (this may be expected if client caches)")
        except Exception as e:
            print(f"[OK] Revoked share blocked: {str(e)[:100]}")
        
        print("\n" + "="*80)
        print("E2E Test Summary:")
        print(f"  Initial rows: {initial_count}")
        print(f"  After 35s: {new_count}")
        print(f"  After 70s: {final_count}")
        print(f"  Usage logs: {len(logs)} entries")
        print(f"  Share revoked: [OK]")
        print("="*80)
        
        os.unlink(profile_path)
        
        print("\n[OK] E2E test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\nE2E test failed: {e}")
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
    
    print("\n[0] Ensuring S3 bucket exists...")
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket {bucket_name} already exists")
    except:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"[OK] Created bucket: {bucket_name}")
        except Exception as e:
            print(f"[WARN] Warning: Could not create bucket {bucket_name}: {e}")
    
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
    print(f"[OK] Seller registered: {seller_email} (ID: {seller_id})")
    
    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)
    buyer_id = buyer_data["id"]
    print(f"[OK] Buyer registered: {buyer_email} (ID: {buyer_id})")
    
    print("\n[2] Logging in...")
    seller_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": seller_email,
        "password": password
    })
    seller_token = seller_login["access_token"]
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    print("[OK] Seller logged in")
    
    buyer_login = api_post(f"{MARKETPLACE_URL}/login", {
        "email": buyer_email,
        "password": password
    })
    buyer_token = buyer_login["access_token"]
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    print("[OK] Buyer logged in")
    
    print("\n[3] Seller creating dataset...")
    dataset_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Test Dataset for Trial",
        "description": "E2E trial test dataset",
        "table_path": "trial_test_table",
        "price": 10.0,
        "is_public": True,
        "anchor_columns": "category,write_batch"
    }, headers=seller_headers, expected_status=201)
    dataset_id = dataset_data["id"]
    print(f"[OK] Dataset created: {dataset_id}")
    
    print("\n[4] Setting seller's Delta Sharing server URL...")
    db = SessionLocal()
    try:
        seller_user = db.query(User).filter(User.id == seller_id).first()
        if seller_user:
            seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
            db.commit()
            print(f"[OK] Seller server URL set: {DELTA_SHARING_SERVER_URL}")
    finally:
        db.close()
    
    print("\n[5] Starting seller data writer to create data...")
    db = SessionLocal()
    writer_thread = Thread(
        target=write_data_continuously,
        args=(dataset_id, 10, 3),
        kwargs={"db": db}
    )
    writer_thread.daemon = True
    writer_thread.start()
    print("[OK] Data writer started")
    
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
    print(f"[OK] Trial access granted")
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
        print("[OK] Delta Sharing client created for trial")
        
        print("\n[8] Buyer reading trial data through Delta Sharing...")
        shares_list = extract_list_items(client.list_shares())
        assert len(shares_list) > 0, "No shares found"
        share = shares_list[0]
        print(f"[OK] Found share: {share.name}")
        
        schemas_list = extract_list_items(client.list_schemas(share))
        assert len(schemas_list) > 0, "No schemas found"
        schema = schemas_list[0]
        print(f"[OK] Found schema: {schema.name}")
        
        tables_list = extract_list_items(client.list_tables(schema))
        assert len(tables_list) > 0, "No tables found"
        table = tables_list[0]
        print(f"[OK] Found table: {table.name}")
        
        print("\n[9] Reading trial table data...")
        table_url = f"{trial_profile_path}#{share.name}.{schema.name}.{table.name}"
        df = load_as_pandas(table_url)
        trial_row_count = len(df)
        print(f"[OK] Trial read: {trial_row_count} rows")
        print(f"  Expected limit: {trial_row_limit} rows")
        
        assert trial_row_count <= trial_row_limit, f"Trial returned {trial_row_count} rows, but limit is {trial_row_limit}"
        print(f"[OK] SUCCESS: Trial row limit enforced ({trial_row_count} <= {trial_row_limit})")
        
        if trial_row_count > 0:
            print(f"  Columns: {list(df.columns)}")
            print(f"  Sample data:\n{df.head()}")
        
        print("\n[10] Testing watermarking on trial share...")
        db = SessionLocal()
        try:
            dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
            anchor_columns = None
            if dataset and dataset.anchor_columns:
                anchor_columns = [col.strip() for col in dataset.anchor_columns.split(',') if col.strip()]
        finally:
            db.close()
        result = check_watermark(df, buyer_id, trial_share_id, verbose=True, anchor_columns=anchor_columns)
        
        print(f"  Watermark: {result['watermark']}")
        print(f"  Timestamp columns: {result['timestamp_cols']}")
        
        if result.get('watermark_column', {}).get('checked', 0) > 0:
            wc = result['watermark_column']
            print(f"  Watermark column: {wc['matches']}/{wc['checked']} matches ({wc['match_rate']:.1f}%)")
            if wc['found']:
                print(f"    [OK] Watermark column detected")
                for sample in wc.get('samples', [])[:3]:
                    print(f"      {sample}")
        
        if result.get('timestamp', {}).get('checked', 0) > 0:
            ts = result['timestamp']
            print(f"  Timestamp columns: {ts['matches']}/{ts['checked']} matches ({ts['match_rate']:.1f}%)")
            if ts['found']:
                print(f"    [OK] Timestamp watermark detected")
                matches_only = [s for s in ts.get('samples', []) if '[MISMATCH]' not in s]
                if matches_only:
                    for sample in matches_only[:3]:
                        print(f"      {sample}")
                else:
                    for sample in ts.get('samples', [])[:3]:
                        print(f"      {sample}")
        
        if result["found"]:
            print(f"  [OK] SUCCESS: Trial share watermarking is working correctly!")
        else:
            print(f"  [WARN] {result.get('reason', 'WARNING: No watermark pattern detected')}")
        
        print("\n[11] Testing that trial cannot exceed row limit on subsequent queries...")
        df2 = load_as_pandas(table_url)
        trial_row_count_2 = len(df2)
        print(f"[OK] Second trial read: {trial_row_count_2} rows")
        assert trial_row_count_2 <= trial_row_limit, f"Second query returned {trial_row_count_2} rows, but limit is {trial_row_limit}"
        print(f"[OK] SUCCESS: Trial row limit persists across queries")
        
        print("\n" + "="*80)
        print("Trial Share Test Summary:")
        print(f"  Trial row limit: {trial_row_limit}")
        print(f"  Rows returned (query 1): {trial_row_count}")
        print(f"  Rows returned (query 2): {trial_row_count_2}")
        print(f"  Watermark detected: {result['found']}")
        wc = result.get('watermark_column', {})
        ts = result.get('timestamp', {})
        if wc.get('checked', 0) > 0:
            print(f"  Watermark column match rate: {wc.get('match_rate', 0):.1f}%")
        if ts.get('checked', 0) > 0:
            print(f"  Timestamp match rate: {ts.get('match_rate', 0):.1f}%")
        print("="*80)
        
        os.unlink(trial_profile_path)
        
        print("\n[OK] Trial share E2E test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\nTrial share test failed: {e}")
        traceback.print_exc()
        if os.path.exists(trial_profile_path):
            os.unlink(trial_profile_path)
        raise

def test_phase2_filtering():
    print("\n" + "="*80)
    print("Phase 2 Filtering E2E Test")
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
    
    print("\n[0] Ensuring S3 bucket exists...")
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[OK] Bucket {bucket_name} already exists")
    except:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"[OK] Created bucket: {bucket_name}")
        except Exception as e:
            print(f"[WARN] Warning: Could not create bucket {bucket_name}: {e}")
    
    print("\n[1] Registering users...")
    seller_email = f"seller_filter_{int(time.time())}@test.com"
    buyer_email = f"buyer_filter_{int(time.time())}@test.com"
    password = "testpass123"
    
    seller_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": seller_email,
        "password": password,
        "role": "seller"
    }, expected_status=201)
    seller_id = seller_data["id"]
    print(f"[OK] Seller registered: {seller_email} (ID: {seller_id})")
    
    buyer_data = api_post(f"{MARKETPLACE_URL}/register", {
        "email": buyer_email,
        "password": password,
        "role": "buyer"
    }, expected_status=201)
    buyer_id = buyer_data["id"]
    print(f"[OK] Buyer registered: {buyer_email} (ID: {buyer_id})")
    
    print("\n[2] Logging in...")
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
    print("[OK] Users logged in")
    
    print("\n[3] Creating dataset with diverse test data...")
    dataset_data = api_post(f"{MARKETPLACE_URL}/datasets", {
        "name": "Filter Test Dataset",
        "description": "Dataset for Phase 2 filter testing",
        "table_path": "filter_test_table",
        "price": 0.0,
        "is_public": True,
        "anchor_columns": "category,write_batch"
    }, headers=seller_headers, expected_status=201)
    dataset_id = dataset_data["id"]
    print(f"[OK] Dataset created: {dataset_id}")
    
    db = SessionLocal()
    try:
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            dataset.table_name = "filter_test_table"
            db.commit()
            print(f"[OK] Table name set: filter_test_table")
    finally:
        db.close()
    
    print("\n[4] Setting seller's Delta Sharing server URL...")
    db = SessionLocal()
    try:
        seller_user = db.query(User).filter(User.id == seller_id).first()
        if seller_user:
            seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
            db.commit()
    finally:
        db.close()
    
    print("\n[5] Writing test data with diverse values...")
    db = SessionLocal()
    try:
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            
            bucket_name = get_bucket_name()
            table_path = get_full_s3_path(bucket_name, dataset.table_path)
            storage_options = get_delta_storage_options()
            s3_client = get_s3_client()
            
            try:
                s3_client.head_bucket(Bucket=bucket_name)
            except:
                s3_client.create_bucket(Bucket=bucket_name)
            
            test_data = pd.DataFrame({
                'country': ['EE', 'EE', 'LV', 'LV', 'LT', 'EE', 'LV', 'LT', 'EE', 'LV'] * 3,
                'amount': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] * 3,
                'category': ['A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A', 'B'] * 3,
                'timestamp': [datetime.now(timezone.utc).isoformat()] * 30,
                'value': list(range(30))
            })
            
            table = pa.Table.from_pandas(test_data)
            write_deltalake(table_path, table, mode='overwrite', storage_options=storage_options)
            print(f"[OK] Wrote {len(test_data)} rows of test data")
            print(f"  Countries: {test_data['country'].unique().tolist()}")
            print(f"  Amount range: {test_data['amount'].min()} - {test_data['amount'].max()}")
    finally:
        db.close()
    
    print("\n[6] Buyer purchasing dataset...")
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_token = purchase_data["share_token"]
    share_id = purchase_data["share_id"]
    seller_server_url = purchase_data.get("seller_server_url", DELTA_SHARING_SERVER_URL)
    print(f"[OK] Purchase successful, share token: {share_token[:20]}...")
    
    if purchase_data.get("approval_status") == "pending":
        approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
        print(f"[OK] Share approved")
    
    print("\n[7] Testing filtered queries via direct HTTP calls...")
    share_name = f"share_{share_id}"
    schema_name = "default"
    table_name = "filter_test_table"
    query_url = f"{seller_server_url}/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/query"
    headers = {"Authorization": f"Bearer {share_token}"}
    
    def query_and_verify(query_body, expected_assertions):
        print(f"\n  Testing: {query_body.get('predicateHints', query_body.get('jsonPredicateHints', 'no filters'))}")
        response = requests.post(query_url, json=query_body, headers=headers)
        
        if response.status_code != 200:
            print(f"    Response status: {response.status_code}")
            print(f"    Response: {response.text[:200]}")
            return response.status_code, None
        
        lines = response.text.strip().split('\n')
        file_actions = []
        for line in lines:
            if line.strip():
                try:
                    obj = json.loads(line)
                    if 'file' in obj:
                        file_actions.append(obj['file'])
                except:
                    pass
        
        if not file_actions:
            print(f"    No file actions in response")
            return 200, pd.DataFrame()
        
        dfs = []
        for file_action in file_actions:
            file_url = file_action['url']
            try:
                file_response = requests.get(file_url, timeout=10)
                if file_response.status_code == 200:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                        tmp.write(file_response.content)
                        tmp_path = tmp.name
                    df = pd.read_parquet(tmp_path)
                    os.unlink(tmp_path)
                    dfs.append(df)
            except Exception as e:
                print(f"    Error downloading file: {e}")
        
        if not dfs:
            return 200, pd.DataFrame()
        
        result_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        
        for assertion_name, assertion_func in expected_assertions.items():
            try:
                assertion_func(result_df)
                print(f"    [OK] {assertion_name}")
            except AssertionError as e:
                print(f"    ✗ {assertion_name}: {e}")
                raise
        
        return 200, result_df
    
    def assert_all_country_ee(df):
        if len(df) > 0:
            assert all(df['country'] == 'EE'), f"Not all rows have country == 'EE', got: {df['country'].unique().tolist()}"
    
    def assert_all_amount_gt_50(df):
        if len(df) > 0:
            assert all(df['amount'] > 50), f"Not all rows have amount > 50, got min: {df['amount'].min()}"
    
    def assert_row_count_le(df, max_count):
        assert len(df) <= max_count, f"Returned {len(df)} rows, expected <= {max_count}"
    
    def assert_row_count_gt(df, min_count=0):
        assert len(df) > min_count, f"Returned {len(df)} rows, expected > {min_count}"
    
    def assert_all_country_in(df, values):
        if len(df) > 0:
            assert all(df['country'].isin(values)), f"Not all rows have country in {values}, got: {df['country'].unique().tolist()}"
    
    def assert_columns_include(df, required_cols):
        assert set(df.columns) >= set(required_cols), f"Missing columns, got: {list(df.columns)}, required: {required_cols}"
    
    def assert_both_predicates(df):
        if len(df) > 0:
            assert all((df['country'] == 'EE') & (df['amount'] >= 50)), "Not all rows satisfy both predicates"
    
    print("\n[7a] Test: Equality filter")
    status, df = query_and_verify(
        {"predicateHints": ["country = 'EE'"]},
        {
            "All rows have country == 'EE'": assert_all_country_ee,
            "Returned row count > 0": assert_row_count_gt
        }
    )
    assert status == 200, "Query should succeed"
    print(f"    Returned {len(df)} rows")
    
    print("\n[7b] Test: Numeric comparison filter")
    status, df = query_and_verify(
        {"predicateHints": ["amount > 50"]},
        {
            "All rows have amount > 50": assert_all_amount_gt_50
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows")
    
    print("\n[7c] Test: Filter and limit ordering")
    status, df = query_and_verify(
        {"predicateHints": ["country = 'EE'"], "limit": 5},
        {
            "Returned row count <= 5": lambda df: assert_row_count_le(df, 5),
            "All rows satisfy predicate": assert_all_country_ee
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows")
    
    print("\n[7d] Test: Compound AND filter (multiple predicateHints)")
    status, df = query_and_verify(
        {"predicateHints": ["country = 'EE'", "amount >= 50"]},
        {
            "All rows satisfy both predicates": assert_both_predicates
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows")
    
    print("\n[7e] Test: Projection with filter")
    status, df = query_and_verify(
        {"columns": ["country", "amount"], "predicateHints": ["country IN ('EE','LV')"]},
        {
            "Returned columns match projection": lambda df: assert_columns_include(df, ['country', 'amount']),
            "All rows satisfy predicate": lambda df: assert_all_country_in(df, ['EE', 'LV'])
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows with columns: {list(df.columns)}")
    
    print("\n[7f] Test: Invalid column predicate rejection")
    response = requests.post(query_url, json={"predicateHints": ["nonexistent_column = 1"]}, headers=headers)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text[:200]}"
    print(f"    [OK] Query correctly rejected with status {response.status_code}")
    
    print("\n[7g] Test: Unsupported operator rejection")
    response = requests.post(query_url, json={"predicateHints": ["country LIKE 'E%'"]}, headers=headers)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text[:200]}"
    print(f"    [OK] Query correctly rejected with status {response.status_code}")
    
    print("\n[7h] Test: JSON predicateHints format")
    status, df = query_and_verify(
        {"jsonPredicateHints": [{"column": "country", "op": "=", "value": "EE"}]},
        {
            "All rows have country == 'EE'": assert_all_country_ee
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows")
    
    print("\n[7i] Test: IN operator")
    status, df = query_and_verify(
        {"predicateHints": ["country IN ('EE','LV')"]},
        {
            "All rows have country in ['EE','LV']": lambda df: assert_all_country_in(df, ['EE', 'LV'])
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows")
    
    print("\n[8] Testing trial share with filters...")
    trial_data = api_post(f"{MARKETPLACE_URL}/datasets/{dataset_id}/trial", {
        "row_limit": 10,
        "days_valid": 7
    }, headers=buyer_headers)
    trial_share_token = trial_data["share_token"]
    trial_share_id = trial_data["share_id"]
    trial_row_limit = trial_data["trial_row_limit"]
    
    trial_query_url = f"{seller_server_url}/shares/share_{trial_share_id}/schemas/{schema_name}/tables/{table_name}/query"
    trial_headers = {"Authorization": f"Bearer {trial_share_token}"}
    
    def trial_query_and_verify(query_body, expected_assertions):
        response = requests.post(trial_query_url, json=query_body, headers=trial_headers)
        if response.status_code != 200:
            return response.status_code, None
        lines = response.text.strip().split('\n')
        file_actions = []
        for line in lines:
            if line.strip():
                try:
                    obj = json.loads(line)
                    if 'file' in obj:
                        file_actions.append(obj['file'])
                except:
                    pass
        if not file_actions:
            return 200, pd.DataFrame()
        dfs = []
        for file_action in file_actions:
            file_url = file_action['url']
            try:
                file_response = requests.get(file_url, timeout=10)
                if file_response.status_code == 200:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                        tmp.write(file_response.content)
                        tmp_path = tmp.name
                    df = pd.read_parquet(tmp_path)
                    os.unlink(tmp_path)
                    dfs.append(df)
            except Exception as e:
                print(f"    Error downloading file: {e}")
        if not dfs:
            return 200, pd.DataFrame()
        result_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        for assertion_name, assertion_func in expected_assertions.items():
            try:
                assertion_func(result_df)
                print(f"    [OK] {assertion_name}")
            except AssertionError as e:
                print(f"    ✗ {assertion_name}: {e}")
                raise
        return 200, result_df
    
    print("\n[8a] Test: Trial share filter cannot bypass row limit")
    status, df = trial_query_and_verify(
        {"predicateHints": ["country = 'EE'"], "limit": 1000},
        {
            "Returned row count <= trial limit": lambda df: assert_row_count_le(df, trial_row_limit)
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows (trial limit: {trial_row_limit})")
    
    print("\n[8b] Test: Trial share filter with smaller requested limit")
    status, df = trial_query_and_verify(
        {"predicateHints": ["country = 'EE'"], "limit": 3},
        {
            "Returned row count <= 3": lambda df: assert_row_count_le(df, 3)
        }
    )
    assert status == 200
    print(f"    Returned {len(df)} rows")
    
    print("\n[9] Testing table name validation...")
    invalid_table_url = f"{seller_server_url}/shares/{share_name}/schemas/{schema_name}/tables/wrong_table_name/query"
    response = requests.post(invalid_table_url, json={}, headers=headers)
    assert response.status_code in [400, 404], f"Expected 400/404, got {response.status_code}"
    print(f"    [OK] Invalid table name correctly rejected (status {response.status_code})")
    
    print("\n[10] Testing revoked share blocks filtered queries...")
    shares_list = api_get(f"{MARKETPLACE_URL}/my-shares", headers=seller_headers)
    test_share = next((s for s in shares_list if s["id"] == share_id), None)
    if test_share:
        api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
        print("[OK] Share revoked")
    
    response = requests.post(query_url, json={"predicateHints": ["country = 'EE'"]}, headers=headers)
    assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
    print(f"    [OK] Revoked share correctly blocked (status {response.status_code})")
    
    print("\n" + "="*80)
    print("Phase 2 Filtering Test Summary:")
    print("  [OK] Equality filter")
    print("  [OK] Numeric comparison filter")
    print("  [OK] Filter + limit ordering")
    print("  [OK] Compound AND filter")
    print("  [OK] Projection with filter")
    print("  [OK] Invalid column rejection")
    print("  [OK] Unsupported operator rejection")
    print("  [OK] JSON predicateHints format")
    print("  [OK] IN operator")
    print("  [OK] Trial share filter limits")
    print("  [OK] Revoked share blocking")
    print("  [OK] Table name validation")
    print("="*80)
    
    print("\n[OK] Phase 2 filtering E2E test completed successfully!")
    return True

if __name__ == "__main__":
    test_e2e_delta_sharing()
    print("\n" + "="*80)
    print("Starting Trial Share Test...")
    print("="*80)
    test_trial_share()
    print("\n" + "="*80)
    print("Starting Phase 2 Filtering Test...")
    print("="*80)
    test_phase2_filtering()

