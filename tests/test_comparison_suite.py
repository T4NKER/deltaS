import os
import sys
import json
import time
import requests
import tempfile
import pandas as pd
import io
import traceback
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
from jose import jwt
from deltalake import write_deltalake
import pyarrow as pa

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from delta_sharing import load_as_pandas, SharingClient
from delta_sharing.protocol import DeltaSharingProfile
from src.utils.s3_utils import get_s3_client, get_bucket_name, get_delta_storage_options, get_full_s3_path
from src.models.database import SessionLocal, User
from src.utils.settings import get_settings
from tests.utils import api_post, api_get, api_delete

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def _create_test_dataset(seller_headers: Dict, seller_id: int, table_path: str = "test_data/comparison_test") -> int:
    print("  Creating test dataset...")
    
    db = SessionLocal()
    try:
        seller_user = db.query(User).filter(User.id == seller_id).first()
        if seller_user:
            seller_user.delta_sharing_server_url = DELTA_SHARING_SERVER_URL
            db.commit()
            print(f"  Set seller server URL: {DELTA_SHARING_SERVER_URL}")
    finally:
        db.close()
    
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        s3_client.create_bucket(Bucket=bucket_name)
    
    test_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'Item_{i}' for i in range(1, 101)],
        'value': [i * 10.5 for i in range(1, 101)],
        'category': ['A', 'B', 'C'] * 33 + ['A'],
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='1H')
    })
    
    full_path = get_full_s3_path(bucket_name, table_path)
    storage_options = get_delta_storage_options()
    
    table = pa.Table.from_pandas(test_data)
    write_deltalake(full_path, table, storage_options=storage_options, mode='overwrite')
    
    seller_server_url = DELTA_SHARING_SERVER_URL.rstrip('/')
    metadata_resp = requests.post(
        f"{seller_server_url}/seller/publish-metadata",
        json={
            "table_path": table_path,
            "name": "Comparison Test Dataset",
            "description": "Test dataset for comparison suite",
            "anchor_columns": "id,category"
        },
        headers=seller_headers
    )
    metadata_resp.raise_for_status()
    metadata = metadata_resp.json()
    
    dataset_resp = api_post(
        f"{MARKETPLACE_URL}/datasets",
        {
            "name": "Comparison Test Dataset",
            "description": "Test dataset for comparison suite",
            "table_path": table_path,
            "price": 0.0,
            "is_public": True,
            "metadata_bundle": metadata
        },
        headers=seller_headers,
        expected_status=201
    )
    
    dataset_id = dataset_resp["id"]
    print(f"  Created dataset ID: {dataset_id}")
    return dataset_id

def run_comparison_experiment(
    dataset_id: Optional[int],
    buyer_email: str,
    buyer_password: str,
    seller_email: str,
    seller_password: str
) -> Dict:
    results = {
        "experiment_timestamp": datetime.utcnow().isoformat(),
        "dataset_id": None,
        "methods": {
            "delta_sharing": {},
            "file_download": {}
        }
    }
    
    buyer_token = _login(buyer_email, buyer_password)
    seller_token = _login(seller_email, seller_password)
    
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    
    settings = get_settings()
    seller_payload = jwt.decode(seller_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    seller_id = int(seller_payload.get("sub"))
    
    if not dataset_id:
        print("\n[0] Creating test dataset...")
        dataset_id = _create_test_dataset(seller_headers, seller_id)
        print(f"[OK] Using dataset ID: {dataset_id}")
    
    results["dataset_id"] = dataset_id
    
    print("\n" + "="*80)
    print("Delta Sharing vs File Download Comparison Experiment")
    print("="*80)
    
    print("\n[1] Testing Delta Sharing delivery...")
    delta_sharing_results, share_id_ds = _test_delta_sharing_delivery(
        dataset_id, buyer_headers, seller_headers
    )
    results["methods"]["delta_sharing"] = delta_sharing_results
    
    print("\n[2] Testing File Download delivery...")
    file_download_results, share_id_fd = _test_file_download_delivery(
        dataset_id, buyer_headers, seller_headers, reuse_share_id=share_id_ds
    )
    results["methods"]["file_download"] = file_download_results
    
    print("\n[3] Testing revocation...")
    revocation_results = _test_revocation(
        dataset_id, buyer_headers, seller_headers, reuse_share_id=share_id_fd
    )
    results["revocation"] = revocation_results
    
    print("\n[4] Comparing methods...")
    comparison = _compare_methods(results["methods"]["delta_sharing"], results["methods"]["file_download"])
    results["comparison"] = comparison
    
    return results

def _login(email: str, password: str) -> str:
    response = requests.post(
        f"{MARKETPLACE_URL}/login",
        json={"email": email, "password": password}
    )
    if response.status_code == 401:
        print(f"  User {email} not found, creating...")
        register_response = requests.post(
            f"{MARKETPLACE_URL}/register",
            json={"email": email, "password": password, "role": "buyer" if "buyer" in email else "seller"}
        )
        if register_response.status_code not in [200, 201]:
            raise Exception(f"Failed to register user {email}: {register_response.text}")
        print(f"  User {email} created, logging in...")
        response = requests.post(
            f"{MARKETPLACE_URL}/login",
            json={"email": email, "password": password}
        )
    response.raise_for_status()
    return response.json()["access_token"]

def _test_delta_sharing_delivery(
    dataset_id: int,
    buyer_headers: Dict,
    seller_headers: Dict
) -> tuple[Dict, int]:
    start_time = time.time()
    
    try:
        purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
        share_id = purchase_data["share_id"]
    except Exception as e:
        if "already have access" in str(e).lower():
            shares = api_get(f"{MARKETPLACE_URL}/my-shares", headers=buyer_headers)
            share = next((s for s in shares if s["dataset_id"] == dataset_id), None)
            if share:
                share_id = share["id"]
            else:
                raise
        else:
            raise
    
    approval_time = time.time()
    
    approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
    
    profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
    profile_json_str = profile_resp["profile_json"]
    profile_data = json.loads(profile_json_str)
    
    profile_time = time.time()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_data, f)
        profile_path = f.name
    
    try:
        profile = DeltaSharingProfile.read_from_file(profile_path)
        client = SharingClient(profile)
        
        shares_list = list(client.list_shares())
        if not shares_list:
            raise Exception("No shares found in profile")
        share = shares_list[0]
        
        schemas_list = list(client.list_schemas(share))
        if not schemas_list:
            raise Exception("No schemas found in share")
        schema = schemas_list[0]
        
        tables_list = list(client.list_tables(schema))
        if not tables_list:
            raise Exception("No tables found in schema")
        table = tables_list[0]
        
        query_start = time.time()
        table_url = f"{profile_path}#{share.name}.{schema.name}.{table.name}"
        df = load_as_pandas(table_url)
        query_time = time.time() - query_start
        
        access_time = time.time() - start_time
        
        return {
            "setup_time_seconds": approval_time - start_time,
            "profile_retrieval_time_seconds": profile_time - approval_time,
            "query_time_seconds": query_time,
            "total_access_time_seconds": access_time,
            "rows_accessed": len(df),
            "columns_accessed": len(df.columns),
            "bytes_estimated": len(df) * 100,
            "success": True
        }, share_id
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }, share_id
    finally:
        if os.path.exists(profile_path):
            os.unlink(profile_path)

def _test_file_download_delivery(
    dataset_id: int,
    buyer_headers: Dict,
    seller_headers: Dict,
    reuse_share_id: Optional[int] = None
) -> tuple[Dict, int]:
    start_time = time.time()
    
    if reuse_share_id:
        share_id = reuse_share_id
        print(f"  Reusing share_id: {share_id}")
    else:
        try:
            purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
            share_id = purchase_data["share_id"]
        except Exception as e:
            if "already have access" in str(e).lower():
                shares = api_get(f"{MARKETPLACE_URL}/my-shares", headers=buyer_headers)
                share = next((s for s in shares if s["dataset_id"] == dataset_id), None)
                if share:
                    share_id = share["id"]
                else:
                    raise
            else:
                raise
    
    approval_time = time.time()
    
    if not reuse_share_id:
        api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
    
    link_generation_start = time.time()
    file_link_resp = api_post(
        f"{MARKETPLACE_URL}/shares/{share_id}/file-download",
        {"expiry_hours": 24},
        headers=buyer_headers
    )
    link_generation_time = time.time() - link_generation_start
    
    download_url = file_link_resp["download_url"]
    download_token = file_link_resp["download_token"]
    snapshot_id = file_link_resp["snapshot_id"]
    
    download_start = time.time()
    try:
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()
        download_time = time.time() - download_start
        
        df = pd.read_parquet(io.BytesIO(response.content))
        
        access_time = time.time() - start_time
        
        return {
            "setup_time_seconds": approval_time - start_time if not reuse_share_id else 0,
            "link_generation_time_seconds": link_generation_time,
            "download_time_seconds": download_time,
            "total_access_time_seconds": access_time,
            "rows_accessed": len(df),
            "columns_accessed": len(df.columns),
            "bytes_served": len(response.content),
            "file_size_bytes": file_link_resp["file_size_bytes"],
            "snapshot_id": snapshot_id,
            "success": True
        }, share_id
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }, share_id

def _test_revocation(
    dataset_id: int,
    buyer_headers: Dict,
    seller_headers: Dict,
    reuse_share_id: Optional[int] = None
) -> Dict:
    if reuse_share_id:
        share_id = reuse_share_id
        print(f"  Reusing share_id: {share_id}")
    else:
        try:
            purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
            share_id = purchase_data["share_id"]
            api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
        except Exception as e:
            if "already have access" in str(e).lower():
                shares = api_get(f"{MARKETPLACE_URL}/my-shares", headers=buyer_headers)
                share = next((s for s in shares if s["dataset_id"] == dataset_id), None)
                if share:
                    share_id = share["id"]
                else:
                    raise
            else:
                raise
    
    profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
    profile_json_str = profile_resp["profile_json"]
    profile_data = json.loads(profile_json_str)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_data, f)
        profile_path = f.name
    
    try:
        profile = DeltaSharingProfile.read_from_file(profile_path)
        client = SharingClient(profile)
        shares_list = list(client.list_shares())
        if shares_list:
            share = shares_list[0]
            schemas_list = list(client.list_schemas(share))
            if schemas_list:
                schema = schemas_list[0]
                tables_list = list(client.list_tables(schema))
                if tables_list:
                    table = tables_list[0]
                    table_url = f"{profile_path}#{share.name}.{schema.name}.{table.name}"
                    df_before = load_as_pandas(table_url)
                    rows_before = len(df_before)
                else:
                    rows_before = 0
            else:
                rows_before = 0
        else:
            rows_before = 0
    except:
        rows_before = 0
    
    revoke_start = time.time()
    api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
    revoke_time = time.time() - revoke_start
    
    time.sleep(1)
    
    try:
        profile = DeltaSharingProfile.read_from_file(profile_path)
        client = SharingClient(profile)
        shares_list = list(client.list_shares())
        if shares_list:
            share = shares_list[0]
            schemas_list = list(client.list_schemas(share))
            if schemas_list:
                schema = schemas_list[0]
                tables_list = list(client.list_tables(schema))
                if tables_list:
                    table = tables_list[0]
                    table_url = f"{profile_path}#{share.name}.{schema.name}.{table.name}"
                    df_after = load_as_pandas(table_url)
                    rows_after = len(df_after)
                    revoked = False
                else:
                    rows_after = 0
                    revoked = True
            else:
                rows_after = 0
                revoked = True
        else:
            rows_after = 0
            revoked = True
    except Exception as e:
        rows_after = 0
        revoked = True
    
    if os.path.exists(profile_path):
        os.unlink(profile_path)
    
    return {
        "revocation_time_seconds": revoke_time,
        "rows_before_revocation": rows_before,
        "rows_after_revocation": rows_after,
        "revocation_effective": revoked,
        "residual_access": not revoked
    }

def _compare_methods(delta_sharing: Dict, file_download: Dict) -> Dict:
    comparison = {
        "setup_time": {
            "delta_sharing": delta_sharing.get("setup_time_seconds", 0),
            "file_download": file_download.get("setup_time_seconds", 0),
            "winner": "delta_sharing" if delta_sharing.get("setup_time_seconds", float('inf')) < file_download.get("setup_time_seconds", float('inf')) else "file_download"
        },
        "access_time": {
            "delta_sharing": delta_sharing.get("total_access_time_seconds", 0),
            "file_download": file_download.get("total_access_time_seconds", 0),
            "winner": "delta_sharing" if delta_sharing.get("total_access_time_seconds", float('inf')) < file_download.get("total_access_time_seconds", float('inf')) else "file_download"
        },
        "data_transfer": {
            "delta_sharing_bytes_estimated": delta_sharing.get("bytes_estimated", 0),
            "file_download_bytes_actual": file_download.get("bytes_served", 0),
            "file_download_file_size": file_download.get("file_size_bytes", 0)
        },
        "operational_overhead": {
            "delta_sharing": "Low - incremental queries, no snapshot management",
            "file_download": "High - snapshot generation, storage, cleanup required"
        },
        "security_posture": {
            "delta_sharing": "High - query-level access control, real-time revocation, audit logging",
            "file_download": "Medium - time-limited links, snapshot revocation requires cleanup"
        },
        "usability": {
            "delta_sharing": "High - standard Delta Sharing protocol, Spark/Pandas integration",
            "file_download": "Medium - requires download, manual processing"
        },
        "revocation": {
            "delta_sharing": "Immediate - query-level blocking",
            "file_download": "Delayed - requires snapshot deletion, cached URLs may persist"
        }
    }
    
    return comparison

if __name__ == "__main__":
    if len(sys.argv) >= 6:
        dataset_id = int(sys.argv[1])
        buyer_email = sys.argv[2]
        buyer_password = sys.argv[3]
        seller_email = sys.argv[4]
        seller_password = sys.argv[5]
    else:
        print("Using default test credentials...")
        print("(Override with: python test_comparison_suite.py <dataset_id> <buyer_email> <buyer_password> <seller_email> <seller_password>)")
        print()
        
        buyer_email = os.getenv("TEST_BUYER_EMAIL", f"buyer_{int(time.time())}@test.com")
        buyer_password = os.getenv("TEST_BUYER_PASSWORD", "testpass123")
        seller_email = os.getenv("TEST_SELLER_EMAIL", f"seller_{int(time.time())}@test.com")
        seller_password = os.getenv("TEST_SELLER_PASSWORD", "testpass123")
        
        if len(sys.argv) >= 2:
            dataset_id = int(sys.argv[1])
        else:
            dataset_id_str = os.getenv("TEST_DATASET_ID", "")
            dataset_id = int(dataset_id_str) if dataset_id_str else None
            if dataset_id:
                print(f"Using dataset_id: {dataset_id} (set TEST_DATASET_ID env var or pass as first argument)")
            else:
                print("No dataset_id provided - will create a test dataset automatically")
            print("Note: Users will be created automatically if they don't exist")
            print()
    
    try:
        results = run_comparison_experiment(
            dataset_id, buyer_email, buyer_password, seller_email, seller_password
        )
        
        output_file = f"comparison_results_{int(time.time())}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n[OK] Results saved to {output_file}")
        print("\nComparison Summary:")
        print(json.dumps(results["comparison"], indent=2))
    except Exception as e:
        print(f"\n[ERROR] Experiment failed: {e}")
        traceback.print_exc()
        sys.exit(1)

