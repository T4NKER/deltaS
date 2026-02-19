import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from delta_sharing import load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile
import tempfile
from tests.utils import api_post, api_get, api_delete

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def run_revocation_ttl_experiments(
    dataset_id: int,
    buyer_email: str,
    buyer_password: str,
    seller_email: str,
    seller_password: str,
    num_iterations: int = 5
) -> Dict:
    results = {
        "experiment_timestamp": datetime.utcnow().isoformat(),
        "dataset_id": dataset_id,
        "num_iterations": num_iterations,
        "experiments": {}
    }
    
    buyer_token = _login(buyer_email, buyer_password)
    seller_token = _login(seller_email, seller_password)
    
    buyer_headers = {"Authorization": f"Bearer {buyer_token}"}
    seller_headers = {"Authorization": f"Bearer {seller_token}"}
    
    print("\n" + "="*80)
    print("Revocation and TTL Experiments")
    print("="*80)
    
    print("\n[1] Testing API revocation latency...")
    api_latency_results = _test_api_revocation_latency(
        dataset_id, buyer_headers, seller_headers, num_iterations
    )
    results["experiments"]["api_revocation_latency"] = api_latency_results
    
    print("\n[2] Testing residual access window...")
    residual_access_results = _test_residual_access_window(
        dataset_id, buyer_headers, seller_headers
    )
    results["experiments"]["residual_access_window"] = residual_access_results
    
    print("\n[3] Testing revocation effectiveness over time...")
    effectiveness_results = _test_revocation_effectiveness(
        dataset_id, buyer_headers, seller_headers
    )
    results["experiments"]["revocation_effectiveness"] = effectiveness_results
    
    print("\n[4] Testing concurrent access during revocation...")
    concurrent_results = _test_concurrent_access_during_revocation(
        dataset_id, buyer_headers, seller_headers
    )
    results["experiments"]["concurrent_access"] = concurrent_results
    
    print("\n[5] Generating summary metrics...")
    summary = _generate_summary_metrics(results["experiments"])
    results["summary"] = summary
    
    return results

def _login(email: str, password: str) -> str:
    response = requests.post(
        f"{MARKETPLACE_URL}/login",
        json={"email": email, "password": password}
    )
    response.raise_for_status()
    return response.json()["access_token"]

def _test_api_revocation_latency(
    dataset_id: int,
    buyer_headers: Dict,
    seller_headers: Dict,
    num_iterations: int
) -> Dict:
    latencies = []
    api_call_times = []
    
    for i in range(num_iterations):
        purchase_data = api_post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", {}, headers=buyer_headers)
        share_id = purchase_data["share_id"]
        
        api_post(f"{MARKETPLACE_URL}/shares/{share_id}/approve", {}, headers=seller_headers)
        
        api_call_start = time.time()
        api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
        api_call_end = time.time()
        api_call_time = api_call_end - api_call_start
        api_call_times.append(api_call_time)
        
        verification_start = time.time()
        try:
            profile_resp = api_get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=buyer_headers)
            verification_end = time.time()
            verification_time = verification_end - verification_start
            
            if profile_resp.get("profile_json"):
                latency = verification_time
                latencies.append(latency)
                print(f"  Iteration {i+1}: API call {api_call_time*1000:.2f}ms, verification {verification_time*1000:.2f}ms")
            else:
                latency = verification_time
                latencies.append(latency)
                print(f"  Iteration {i+1}: API call {api_call_time*1000:.2f}ms, verification {verification_time*1000:.2f}ms (denied)")
        except Exception as e:
            verification_end = time.time()
            verification_time = verification_end - verification_start
            latencies.append(verification_time)
            print(f"  Iteration {i+1}: API call {api_call_time*1000:.2f}ms, verification {verification_time*1000:.2f}ms (error: {str(e)[:50]})")
    
    return {
        "api_call_times_ms": [t * 1000 for t in api_call_times],
        "verification_times_ms": [t * 1000 for t in latencies],
        "mean_api_call_time_ms": sum(api_call_times) / len(api_call_times) * 1000 if api_call_times else 0,
        "mean_verification_time_ms": sum(latencies) / len(latencies) * 1000 if latencies else 0,
        "min_api_call_time_ms": min(api_call_times) * 1000 if api_call_times else 0,
        "max_api_call_time_ms": max(api_call_times) * 1000 if api_call_times else 0,
        "min_verification_time_ms": min(latencies) * 1000 if latencies else 0,
        "max_verification_time_ms": max(latencies) * 1000 if latencies else 0,
        "p95_api_call_time_ms": sorted(api_call_times)[int(len(api_call_times) * 0.95)] * 1000 if api_call_times else 0,
        "p95_verification_time_ms": sorted(latencies)[int(len(latencies) * 0.95)] * 1000 if latencies else 0
    }

def _test_residual_access_window(
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
        print(f"  Before revocation: {rows_before} rows accessible")
    except Exception as e:
        print(f"  Before revocation: Error accessing data - {str(e)[:100]}")
        rows_before = 0
    
    revoke_start = time.time()
    api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
    revoke_end = time.time()
    revocation_time = revoke_end - revoke_start
    
    test_delays = [0.1, 0.5, 1.0, 2.0, 5.0]
    residual_access_results = []
    
    for delay in test_delays:
        time.sleep(delay)
        test_start = time.time()
        try:
            df_after = load_as_pandas(f"{profile_path}#share1.schema1.table1")
            rows_after = len(df_after)
            access_successful = True
            test_time = time.time() - test_start
            print(f"  After {delay}s delay: {rows_after} rows accessible (access time: {test_time*1000:.2f}ms)")
        except Exception as e:
            access_successful = False
            test_time = time.time() - test_start
            error_msg = str(e)[:100]
            print(f"  After {delay}s delay: Access denied (error time: {test_time*1000:.2f}ms) - {error_msg}")
        
        residual_access_results.append({
            "delay_seconds": delay,
            "access_successful": access_successful,
            "test_time_ms": test_time * 1000,
            "rows_accessible": rows_after if access_successful else 0
        })
    
    if os.path.exists(profile_path):
        os.unlink(profile_path)
    
    first_failure_delay = next(
        (r["delay_seconds"] for r in residual_access_results if not r["access_successful"]),
        None
    )
    
    return {
        "revocation_api_time_ms": revocation_time * 1000,
        "rows_before_revocation": rows_before,
        "residual_access_tests": residual_access_results,
        "first_failure_delay_seconds": first_failure_delay,
        "residual_access_window_seconds": first_failure_delay if first_failure_delay else max(test_delays)
    }

def _test_revocation_effectiveness(
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
        print(f"  Before revocation: {rows_before} rows accessible")
    except Exception as e:
        print(f"  Before revocation: Error - {str(e)[:100]}")
        rows_before = 0
    
    api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
    
    effectiveness_tests = []
    test_intervals = [0.1, 0.5, 1.0, 2.0, 5.0]
    last_interval = 0.0
    
    for interval in test_intervals:
        sleep_time = interval - last_interval
        if sleep_time > 0:
            time.sleep(sleep_time)
        last_interval = interval
        
        test_start = time.time()
        try:
            df = load_as_pandas(f"{profile_path}#share1.schema1.table1")
            rows = len(df)
            access_successful = True
            test_time = time.time() - test_start
        except Exception as e:
            access_successful = False
            rows = 0
            test_time = time.time() - test_start
            error_msg = str(e)[:100]
        
        effectiveness_tests.append({
            "time_since_revocation_seconds": interval,
            "access_successful": access_successful,
            "test_time_ms": test_time * 1000,
            "rows_accessible": rows
        })
        
        if not access_successful:
            print(f"  At {interval}s: Access denied (test time: {test_time*1000:.2f}ms)")
        else:
            print(f"  At {interval}s: {rows} rows accessible (test time: {test_time*1000:.2f}ms)")
    
    if os.path.exists(profile_path):
        os.unlink(profile_path)
    
    first_failure = next((t for t in effectiveness_tests if not t["access_successful"]), None)
    
    return {
        "rows_before_revocation": rows_before,
        "post_revocation_tests": effectiveness_tests,
        "first_failure_time_seconds": first_failure["time_since_revocation_seconds"] if first_failure else None,
        "revocation_effective": first_failure is not None
    }

def _test_concurrent_access_during_revocation(
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
    
    concurrent_results = []
    
    def attempt_access():
        try:
            start = time.time()
            df = load_as_pandas(f"{profile_path}#share1.schema1.table1")
            elapsed = time.time() - start
            return {"success": True, "rows": len(df), "time_ms": elapsed * 1000}
        except Exception as e:
            elapsed = time.time() - start
            return {"success": False, "rows": 0, "time_ms": elapsed * 1000, "error": str(e)[:100]}
    
    print("  Testing concurrent access during revocation...")
    
    access_before = attempt_access()
    concurrent_results.append({"phase": "before_revocation", **access_before})
    print(f"    Before revocation: {access_before['rows']} rows, {access_before['time_ms']:.2f}ms")
    
    revoke_start = time.time()
    api_delete(f"{MARKETPLACE_URL}/shares/{share_id}", headers=seller_headers)
    revoke_time = time.time() - revoke_start
    
    time.sleep(0.1)
    
    access_during = attempt_access()
    concurrent_results.append({"phase": "during_revocation", **access_during})
    print(f"    During revocation: {access_during['rows']} rows, {access_during['time_ms']:.2f}ms")
    
    time.sleep(0.5)
    
    access_after = attempt_access()
    concurrent_results.append({"phase": "after_revocation", **access_after})
    print(f"    After revocation: {access_after['rows']} rows, {access_after['time_ms']:.2f}ms")
    
    if os.path.exists(profile_path):
        os.unlink(profile_path)
    
    return {
        "revocation_time_ms": revoke_time * 1000,
        "concurrent_access_results": concurrent_results,
        "revocation_blocks_access": not access_after["success"]
    }

def _generate_summary_metrics(experiments: Dict) -> Dict:
    api_latency = experiments.get("api_revocation_latency", {})
    residual = experiments.get("residual_access_window", {})
    effectiveness = experiments.get("revocation_effectiveness", {})
    concurrent = experiments.get("concurrent_access", {})
    
    return {
        "api_revocation_performance": {
            "mean_api_call_time_ms": api_latency.get("mean_api_call_time_ms", 0),
            "mean_verification_time_ms": api_latency.get("mean_verification_time_ms", 0),
            "p95_api_call_time_ms": api_latency.get("p95_api_call_time_ms", 0),
            "p95_verification_time_ms": api_latency.get("p95_verification_time_ms", 0)
        },
        "residual_access_characteristics": {
            "residual_access_window_seconds": residual.get("residual_access_window_seconds", 0),
            "first_failure_delay_seconds": residual.get("first_failure_delay_seconds", 0),
            "revocation_api_time_ms": residual.get("revocation_api_time_ms", 0)
        },
        "revocation_effectiveness": {
            "revocation_blocks_access": effectiveness.get("revocation_effective", False),
            "post_revocation_access_successful": effectiveness.get("post_revocation_test", {}).get("access_successful", True)
        },
        "concurrent_access_behavior": {
            "revocation_blocks_concurrent_access": concurrent.get("revocation_blocks_access", False),
            "revocation_time_ms": concurrent.get("revocation_time_ms", 0)
        },
        "overall_assessment": {
            "revocation_latency_acceptable": api_latency.get("mean_api_call_time_ms", 0) < 1000,
            "residual_access_minimal": residual.get("residual_access_window_seconds", 0) < 2.0,
            "revocation_effective": effectiveness.get("revocation_effective", False) and concurrent.get("revocation_blocks_access", False)
        }
    }

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 6:
        print("Usage: python test_revocation_ttl_experiments.py <dataset_id> <buyer_email> <buyer_password> <seller_email> <seller_password> [num_iterations]")
        sys.exit(1)
    
    dataset_id = int(sys.argv[1])
    buyer_email = sys.argv[2]
    buyer_password = sys.argv[3]
    seller_email = sys.argv[4]
    seller_password = sys.argv[5]
    num_iterations = int(sys.argv[6]) if len(sys.argv) > 6 else 5
    
    results = run_revocation_ttl_experiments(
        dataset_id, buyer_email, buyer_password, seller_email, seller_password, num_iterations
    )
    
    output_file = f"revocation_ttl_results_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n[OK] Results saved to {output_file}")
    print("\nSummary Metrics:")
    print(json.dumps(results["summary"], indent=2))

