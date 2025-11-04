import os
import boto3
from botocore.config import Config

def fix_endpoint_url_for_docker(endpoint_url: str) -> str:
    if not endpoint_url:
        return endpoint_url
    if os.getenv('DOCKER_ENV') == 'true' and ('localhost' in endpoint_url or '127.0.0.1' in endpoint_url):
        endpoint_url = endpoint_url.replace('localhost', 'localstack').replace('127.0.0.1', 'localstack')
    return endpoint_url.rstrip('/')

def fix_endpoint_url_for_client(endpoint_url: str) -> str:
    if not endpoint_url:
        return endpoint_url
    return endpoint_url.replace('localstack', 'localhost')

def get_s3_client():
    endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
    access_key = os.getenv('S3_ACCESS_KEY', 'test')
    secret_key = os.getenv('S3_SECRET_KEY', 'test')
    region = os.getenv('S3_REGION', 'us-east-1')
    
    endpoint_url = fix_endpoint_url_for_docker(endpoint_url) if endpoint_url else None
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name=region
    )

def get_delta_storage_options() -> dict:
    endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
    access_key = os.getenv('S3_ACCESS_KEY', 'test')
    secret_key = os.getenv('S3_SECRET_KEY', 'test')
    region = os.getenv('S3_REGION', 'us-east-1')
    
    endpoint_url = fix_endpoint_url_for_docker(endpoint_url) if endpoint_url else None
    
    return {
        'AWS_ACCESS_KEY_ID': access_key,
        'AWS_SECRET_ACCESS_KEY': secret_key,
        'AWS_ENDPOINT_URL': endpoint_url,
        'AWS_REGION': region,
        'AWS_ALLOW_HTTP': 'true',
    }

def get_bucket_name() -> str:
    bucket_name = os.getenv('S3_BUCKET_NAME', 'test-delta-bucket')
    if not bucket_name:
        bucket_name = 'test-delta-bucket'
    return bucket_name

def get_full_s3_path(bucket: str, path: str) -> str:
    return f"s3://{bucket}/{path}"

