import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, List
from delta_sharing import load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile
import tempfile
from tests.utils import api_post, api_get

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def run_comparison_experiment(
    dataset_id: int,
    buyer_email: str,
    buyer_password: str,
    seller_email: str,
    seller_password: str
) -> Dict:
    results = {
        "experiment_timestamp": datetime.utcnow().isoformat(),
        "dataset_id": dataset_id,
        "methods": {
            "delta_sharing": {},
            "file_link": {}
        }
    }
    
    buyer_token = _login(buyer_email, buyer_password)
    seller_token = _login(seller_email, seller_password)
    
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    
    print("\n" + "="*80)
    print("Comparison Experiment: Delta Sharing vs File Link Delivery")
    print("="*80)
    
    print("\n[1] Testing Delta Sharing delivery...")
    delta_sharing_results = _test_delta_sharing_delivery(
        dataset_id, buyer_headers, seller_headers
    )
    results["methods"]["delta_sharing"] = delta_sharing_results
    
    print("\n[2] Testing File Link delivery...")
    file_link_results = _test_file_link_delivery(
        dataset_id, buyer_headers, seller_headers
    )
    results["methods"]["file_link"] = file_link_results
    
    print("\n[3] Testing revocation...")
    revocation_results = _test_revocation(
        dataset_id, buyer_headers, seller_headers
    )
    results["revocation"] = revocation_results
    
    print("\n[4] Comparing metrics...")
    comparison = _compare_methods(results["methods"]["delta_sharing"], results["methods"]["file_link"])
    results["comparison"] = comparison
    
    return results

def _login(email: str, password: str) -> str:
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
) -> Dict:
    start_time = time.time()
    
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_id = purchase_data["share_id"]
    
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
        query_start = time.time()
        df = load_as_pandas(f"{profile_path}#share1.schema1.table1")
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
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        if os.path.exists(profile_path):
            os.unlink(profile_path)

def _test_file_link_delivery(
    dataset_id: int,
    buyer_headers: Dict,
    seller_headers: Dict
) -> Dict:
    start_time = time.time()
    
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_id = purchase_data["share_id"]
    
    approval_time = time.time()
    
    approve_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
    
    try:
        file_link_resp = api_post(
            f"{MARKETPLACE_URL}/shares/{share_id}/file-download",
            {"expiry_hours": 24},
            headers=buyer_headers
        )
        
        download_url = file_link_resp["download_url"]
        download_token = file_link_resp["download_token"]
        
        link_time = time.time()
        
        download_start = time.time()
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()
        download_time = time.time() - download_start
        
        import pandas as pd
        import io
        df = pd.read_parquet(io.BytesIO(response.content))
        
        access_time = time.time() - start_time
        
        return {
            "setup_time_seconds": approval_time - start_time,
            "link_generation_time_seconds": link_time - approval_time,
            "download_time_seconds": download_time,
            "total_access_time_seconds": access_time,
            "rows_accessed": len(df),
            "columns_accessed": len(df.columns),
            "bytes_served": len(response.content),
            "success": True
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def _test_revocation(
    dataset_id: int,
    buyer_headers: Dict,
    seller_headers: Dict
) -> Dict:
    purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
    share_id = purchase_data["share_id"]
    
    api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
    
    profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
    profile_json_str = profile_resp["profile_json"]
    profile_data = json.loads(profile_json_str)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(profile_data, f)
        profile_path = f.name
    
    try:
        df_before = load_as_pandas(f"{profile_path}#share1.schema1.table1")
        rows_before = len(df_before)
    except:
        rows_before = 0
    
    revoke_start = time.time()
    revoke_resp = api_post(f"{MARKETPLACE_URL}/shares/{share_id}/revoke", {}, headers=seller_headers)
    revoke_time = time.time() - revoke_start
    
    time.sleep(1)
    
    try:
        df_after = load_as_pandas(f"{profile_path}#share1.schema1.table1")
        rows_after = len(df_after)
        revoked = False
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

def _compare_methods(delta_sharing: Dict, file_link: Dict) -> Dict:
    comparison = {
        "setup_time": {
            "delta_sharing": delta_sharing.get("setup_time_seconds", 0),
            "file_link": file_link.get("setup_time_seconds", 0),
            "winner": "delta_sharing" if delta_sharing.get("setup_time_seconds", float('inf')) < file_link.get("setup_time_seconds", float('inf')) else "file_link"
        },
        "access_time": {
            "delta_sharing": delta_sharing.get("total_access_time_seconds", 0),
            "file_link": file_link.get("total_access_time_seconds", 0),
            "winner": "delta_sharing" if delta_sharing.get("total_access_time_seconds", float('inf')) < file_link.get("total_access_time_seconds", float('inf')) else "file_link"
        },
        "operational_overhead": {
            "delta_sharing": "Low - incremental queries, no snapshot management",
            "file_link": "High - snapshot generation, storage, cleanup required"
        },
        "security_posture": {
            "delta_sharing": "High - query-level access control, real-time revocation, audit logging",
            "file_link": "Medium - time-limited links, snapshot revocation requires cleanup"
        },
        "usability": {
            "delta_sharing": "High - standard Delta Sharing protocol, Spark/Pandas integration",
            "file_link": "Medium - requires download, manual processing"
        }
    }
    
    return comparison

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 6:
        print("Usage: python test_comparison_suite.py <dataset_id> <buyer_email> <buyer_password> <seller_email> <seller_password>")
        sys.exit(1)
    
    dataset_id = int(sys.argv[1])
    buyer_email = sys.argv[2]
    buyer_password = sys.argv[3]
    seller_email = sys.argv[4]
    seller_password = sys.argv[5]
    
    results = run_comparison_experiment(
        dataset_id, buyer_email, buyer_password, seller_email, seller_password
    )
    
    output_file = f"comparison_results_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n[OK] Results saved to {output_file}")
    print("\nComparison Summary:")
    print(json.dumps(results["comparison"], indent=2))

