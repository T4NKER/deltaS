import os
import sys
import argparse
import json
import tempfile
import requests
from pathlib import Path
from typing import Optional
from delta_sharing import SharingClient, load_as_pandas
from delta_sharing.protocol import DeltaSharingProfile

MARKETPLACE_URL = os.getenv("MARKETPLACE_URL", "http://localhost:8000")
DELTA_SHARING_SERVER_URL = os.getenv("DELTA_SHARING_SERVER_URL", "http://localhost:8080")

def register_user(email: str, password: str, role: str = "buyer") -> dict:
    response = requests.post(
        f"{MARKETPLACE_URL}/register",
        json={"email": email, "password": password, "role": role}
    )
    response.raise_for_status()
    return response.json()

def login(email: str, password: str) -> str:
    response = requests.post(
        f"{MARKETPLACE_URL}/login",
        json={"email": email, "password": password}
    )
    response.raise_for_status()
    return response.json()["access_token"]

def list_datasets(token: str) -> list:
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{MARKETPLACE_URL}/datasets", headers=headers)
    response.raise_for_status()
    return response.json()

def purchase_dataset(dataset_id: int, token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(f"{MARKETPLACE_URL}/purchase/{dataset_id}", headers=headers)
    response.raise_for_status()
    return response.json()

def request_trial(dataset_id: int, token: str, row_limit: int = 100, days_valid: int = 7) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        f"{MARKETPLACE_URL}/datasets/{dataset_id}/trial",
        json={"row_limit": row_limit, "days_valid": days_valid},
        headers=headers
    )
    response.raise_for_status()
    return response.json()

def get_profile(share_id: int, token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{MARKETPLACE_URL}/shares/{share_id}/profile", headers=headers)
    response.raise_for_status()
    return response.json()

def save_profile(profile_data: dict, output_path: str):
    profile_json = json.loads(profile_data["profile_json"])
    with open(output_path, 'w') as f:
        json.dump(profile_json, f, indent=2)
    print(f"Profile saved to {output_path}")

def query_table(profile_path: str, share_name: str, schema_name: str, table_name: str, limit: Optional[int] = None) -> dict:
    table_url = f"{profile_path}#{share_name}.{schema_name}.{table_name}"
    
    try:
        df = load_as_pandas(table_url, limit=limit)
        return {
            "success": True,
            "rows": len(df),
            "columns": list(df.columns),
            "data": df.head(10).to_dict('records') if len(df) > 0 else []
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def verify_watermark(profile_path: str, share_name: str, schema_name: str, table_name: str, buyer_id: int, share_id: int, anchor_columns: Optional[list] = None):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from tests.utils import check_watermark, extract_list_items
    
    table_url = f"{profile_path}#{share_name}.{schema_name}.{table_name}"
    
    try:
        df = load_as_pandas(table_url)
        result = check_watermark(df, buyer_id, share_id, verbose=True, anchor_columns=anchor_columns)
        return result
    except Exception as e:
        return {"found": False, "error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Delta Sharing Buyer CLI")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    register_parser = subparsers.add_parser('register', help='Register a new user')
    register_parser.add_argument('--email', required=True, help='User email')
    register_parser.add_argument('--password', required=True, help='User password')
    register_parser.add_argument('--role', default='buyer', choices=['buyer', 'seller'], help='User role')
    
    login_parser = subparsers.add_parser('login', help='Login and get token')
    login_parser.add_argument('--email', required=True, help='User email')
    login_parser.add_argument('--password', required=True, help='User password')
    login_parser.add_argument('--save-token', help='Save token to file')
    
    list_parser = subparsers.add_parser('list', help='List available datasets')
    list_parser.add_argument('--token', help='Auth token (or use DELTA_SHARING_TOKEN env var)')
    
    purchase_parser = subparsers.add_parser('purchase', help='Purchase a dataset')
    purchase_parser.add_argument('--dataset-id', type=int, required=True, help='Dataset ID')
    purchase_parser.add_argument('--token', help='Auth token (or use DELTA_SHARING_TOKEN env var)')
    
    trial_parser = subparsers.add_parser('trial', help='Request trial access')
    trial_parser.add_argument('--dataset-id', type=int, required=True, help='Dataset ID')
    trial_parser.add_argument('--row-limit', type=int, default=100, help='Row limit for trial')
    trial_parser.add_argument('--days-valid', type=int, default=7, help='Trial validity in days')
    trial_parser.add_argument('--token', help='Auth token (or use DELTA_SHARING_TOKEN env var)')
    
    profile_parser = subparsers.add_parser('profile', help='Get and save profile')
    profile_parser.add_argument('--share-id', type=int, required=True, help='Share ID')
    profile_parser.add_argument('--output', required=True, help='Output profile file path')
    profile_parser.add_argument('--token', help='Auth token (or use DELTA_SHARING_TOKEN env var)')
    
    query_parser = subparsers.add_parser('query', help='Query a shared table')
    query_parser.add_argument('--profile', required=True, help='Profile file path')
    query_parser.add_argument('--share', required=True, help='Share name')
    query_parser.add_argument('--schema', required=True, help='Schema name')
    query_parser.add_argument('--table', required=True, help='Table name')
    query_parser.add_argument('--limit', type=int, help='Row limit')
    
    verify_parser = subparsers.add_parser('verify', help='Verify watermark in data')
    verify_parser.add_argument('--profile', required=True, help='Profile file path')
    verify_parser.add_argument('--share', required=True, help='Share name')
    verify_parser.add_argument('--schema', required=True, help='Schema name')
    verify_parser.add_argument('--table', required=True, help='Table name')
    verify_parser.add_argument('--buyer-id', type=int, required=True, help='Buyer ID')
    verify_parser.add_argument('--share-id', type=int, required=True, help='Share ID')
    verify_parser.add_argument('--anchor-columns', help='Comma-separated anchor columns')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    token = args.token or os.getenv('DELTA_SHARING_TOKEN')
    
    try:
        if args.command == 'register':
            result = register_user(args.email, args.password, args.role)
            print(f"User registered: {result['email']} (ID: {result['id']})")
        
        elif args.command == 'login':
            token = login(args.email, args.password)
            print(f"Login successful")
            print(f"Token: {token}")
            if args.save_token:
                with open(args.save_token, 'w') as f:
                    f.write(token)
                print(f"Token saved to {args.save_token}")
        
        elif args.command == 'list':
            if not token:
                print("Error: Token required. Use --token or set DELTA_SHARING_TOKEN env var")
                return
            datasets = list_datasets(token)
            print(f"\nAvailable datasets ({len(datasets)}):")
            for ds in datasets:
                print(f"  ID: {ds['id']}, Name: {ds['name']}, Price: ${ds['price']:.2f}")
                if ds.get('risk_level'):
                    print(f"    Risk: {ds['risk_level']} ({ds.get('risk_score', 0):.1f})")
        
        elif args.command == 'purchase':
            if not token:
                print("Error: Token required. Use --token or set DELTA_SHARING_TOKEN env var")
                return
            result = purchase_dataset(args.dataset_id, token)
            print(f"Purchase created")
            print(f"  Share ID: {result['share_id']}")
            print(f"  Approval Status: {result['approval_status']}")
            if result.get('share_token'):
                print(f"  Share Token: {result['share_token'][:20]}...")
        
        elif args.command == 'trial':
            if not token:
                print("Error: Token required. Use --token or set DELTA_SHARING_TOKEN env var")
                return
            result = request_trial(args.dataset_id, token, args.row_limit, args.days_valid)
            print(f"Trial access granted")
            print(f"  Share ID: {result['share_id']}")
            print(f"  Row Limit: {result['trial_row_limit']}")
            print(f"  Expires: {result['trial_expires_at']}")
            if result.get('share_token'):
                print(f"  Share Token: {result['share_token'][:20]}...")
        
        elif args.command == 'profile':
            if not token:
                print("Error: Token required. Use --token or set DELTA_SHARING_TOKEN env var")
                return
            result = get_profile(args.share_id, token)
            save_profile(result, args.output)
        
        elif args.command == 'query':
            result = query_table(args.profile, args.share, args.schema, args.table, args.limit)
            if result['success']:
                print(f"Query successful")
                print(f"  Rows: {result['rows']}")
                print(f"  Columns: {result['columns']}")
                if result['data']:
                    print(f"\n  Sample data (first {min(5, len(result['data']))} rows):")
                    for i, row in enumerate(result['data'][:5]):
                        print(f"    Row {i+1}: {row}")
            else:
                print(f"Query failed: {result['error']}")
        
        elif args.command == 'verify':
            anchor_cols = None
            if args.anchor_columns:
                anchor_cols = [col.strip() for col in args.anchor_columns.split(',')]
            result = verify_watermark(
                args.profile, args.share, args.schema, args.table,
                args.buyer_id, args.share_id, anchor_cols
            )
            if result.get('found'):
                print("Watermark detected")
                if result.get('watermark_column', {}).get('found'):
                    wc = result['watermark_column']
                    print(f"  Watermark column: {wc['matches']}/{wc['checked']} matches ({wc['match_rate']:.1f}%)")
                if result.get('timestamp', {}).get('found'):
                    ts = result['timestamp']
                    print(f"  Timestamp watermark: {ts['matches']}/{ts['checked']} matches ({ts['match_rate']:.1f}%)")
            else:
                print(f"Watermark not detected: {result.get('reason', 'Unknown')}")
    
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if hasattr(e.response, 'text'):
            print(f"  Response: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

