from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional, Tuple
import json
import io
import os
import traceback
from datetime import datetime, timedelta
from deltalake import DeltaTable
from urllib.parse import urlparse
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pandas as pd
import tempfile
import uuid
from deltalake import write_deltalake

from src.models.database import get_db, AuditLog
from src.seller.watermarking import generate_watermark, apply_watermark_to_dataframe
from src.utils.s3_utils import (
    get_s3_client, get_delta_storage_options,
    fix_endpoint_url_for_client, get_full_s3_path, get_bucket_name
)
from src.utils.delta_sharing_utils import (
    extract_token_from_header, validate_share_access, get_dataset,
    get_table_path, transform_schema_for_timestamp_ntz, get_presigned_url,
    cleanup_old_watermarked_files, get_share_from_token
)

app = FastAPI(title="Delta Sharing Server")

@app.get("/shares")
async def list_shares(
    maxResults: Optional[int] = None,
    pageToken: Optional[str] = None,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    token = extract_token_from_header(authorization)
    share = get_share_from_token(token, db)
    
    return {
        "items": [{"name": f"share_{share.id}"}],
        "nextPageToken": None
    }

@app.get("/shares/{share_name}/schemas")
async def list_schemas(
    share_name: str,
    maxResults: Optional[int] = None,
    pageToken: Optional[str] = None,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    token = extract_token_from_header(authorization)
    share = get_share_from_token(token, db)
    validate_share_access(share_name, share)
    
    return {
        "items": [{"name": "default", "share": share_name}],
        "nextPageToken": None
    }

@app.get("/shares/{share_name}/schemas/{schema_name}/tables")
async def list_tables(
    share_name: str,
    schema_name: str,
    maxResults: Optional[int] = None,
    pageToken: Optional[str] = None,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    token = extract_token_from_header(authorization)
    share = get_share_from_token(token, db)
    validate_share_access(share_name, share)
    dataset = get_dataset(share, db)
    
    return {
        "items": [{"name": dataset.table_path, "share": share_name, "schema": schema_name}],
        "nextPageToken": None
    }

@app.get("/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/metadata")
async def get_table_metadata(
    share_name: str,
    schema_name: str,
    table_name: str,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    token = extract_token_from_header(authorization)
    share = get_share_from_token(token, db)
    validate_share_access(share_name, share)
    dataset = get_dataset(share, db)
    bucket_name = get_bucket_name()
    table_path = get_table_path(dataset, bucket_name, share)
    
    try:
        storage_options = get_delta_storage_options()
        delta_table = DeltaTable(table_path, storage_options=storage_options)
        
        metadata = delta_table.metadata()
        schema = delta_table.schema()
        
        protocol_json = json.dumps({"protocol": {"minReaderVersion": 1}})
        metadata_json = json.dumps({
            "metaData": {
                "id": metadata.id if hasattr(metadata, 'id') else table_name,
                "format": {"provider": "parquet"},
                "schemaString": schema.to_json(),
                "partitionColumns": metadata.partition_columns if hasattr(metadata, 'partition_columns') else []
            }
        })
        
        content = f"{protocol_json}\n{metadata_json}\n"
        
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/x-ndjson",
            headers={"delta-table-version": str(delta_table.version())}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading Delta table: {str(e)}")

@app.get("/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/version")
async def get_table_version(
    share_name: str,
    schema_name: str,
    table_name: str,
    startingTimestamp: Optional[str] = None,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    token = extract_token_from_header(authorization)
    share = get_share_from_token(token, db)
    validate_share_access(share_name, share)
    dataset = get_dataset(share, db)
    bucket_name = get_bucket_name()
    table_path = get_table_path(dataset, bucket_name, share)
    
    try:
        storage_options = get_delta_storage_options()
        delta_table = DeltaTable(table_path, storage_options=storage_options)
        
        return StreamingResponse(
            io.BytesIO(b""),
            media_type="application/json",
            headers={"delta-table-version": str(delta_table.version())}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading Delta table version: {str(e)}")

@app.post("/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/query")
async def query_table(
    share_name: str,
    schema_name: str,
    table_name: str,
    request: Request,
    authorization: str = Header(None),
    db: Session = Depends(get_db)
):
    try:
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    except:
        body = {}
    
    token = extract_token_from_header(authorization)
    share = get_share_from_token(token, db)
    validate_share_access(share_name, share)
    dataset = get_dataset(share, db)
    bucket_name = get_bucket_name()
    
    table_path = get_table_path(dataset, bucket_name, share)
    table_prefix = dataset.table_path.rstrip('/')
    
    watermark = generate_watermark(share.buyer_id, share.id)
    
    try:
        s3_client = get_s3_client()
        bucket = bucket_name
        
        endpoint_url = os.getenv('S3_ENDPOINT_URL', '')
        endpoint_for_client = fix_endpoint_url_for_client(endpoint_url).rstrip('/')
        is_localstack = 'localhost:4566' in endpoint_for_client or 'localstack:4566' in endpoint_for_client or '127.0.0.1:4566' in endpoint_for_client

        watermarked_prefix = f"{table_prefix}/_watermarked_{share.id}_"
        cleanup_old_watermarked_files(s3_client, bucket, watermarked_prefix)

        storage_options = get_delta_storage_options()
        delta_table = DeltaTable(table_path, storage_options=storage_options)
        
        df = delta_table.to_pandas()
        
        if share.is_trial and share.trial_row_limit:
            df = df.head(share.trial_row_limit).copy()
        
        watermarked_df = apply_watermark_to_dataframe(df, watermark)
        
        temp_key = f"{table_prefix}/_watermarked_{share.id}_{uuid.uuid4().hex[:8]}.parquet"
        
        watermarked_table = pa.Table.from_pandas(watermarked_df, preserve_index=False)
        
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                tmp_path = tmp_file.name
            
            pq.write_table(watermarked_table, tmp_path)
            
            extra_args = {}
            if is_localstack:
                extra_args['ContentType'] = 'application/octet-stream'
            
            s3_client.upload_file(tmp_path, bucket, temp_key, ExtraArgs=extra_args)
            
            max_retries = 3
            for i in range(max_retries):
                try:
                    s3_client.head_object(Bucket=bucket, Key=temp_key)
                    break
                except Exception as e:
                    if i < max_retries - 1:
                        import time
                        time.sleep(0.2)
                    else:
                        raise Exception(f"File not accessible after upload: {e}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        files_list = [get_full_s3_path(bucket, temp_key)]
        
        use_fallback = False
        schema_string = None
        try:
            metadata = delta_table.metadata()
            schema = delta_table.schema()
            table_version = delta_table.version()
            try:
                schema_json_str = schema.to_json()
                schema_obj = json.loads(schema_json_str)
                schema_obj = transform_schema_for_timestamp_ntz(schema_obj)
                schema_string = json.dumps(schema_obj)
            except Exception as schema_error:
                print(f"Warning: Could not transform schema from DeltaTable: {schema_error}")
                schema_string = '{"type":"struct","fields":[]}'
        except Exception as e:
            use_fallback = True
            print(f"DeltaTable failed, using fallback: {str(e)}")
            
            log_prefix = f"{table_prefix}/_delta_log/"
            log_files = []
            try:
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=log_prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['Key'].endswith('.json'):
                                log_files.append(obj['Key'])
            except Exception as list_error:
                raise HTTPException(status_code=500, detail=f"Error listing Delta log files: {str(list_error)}")
            
            if not log_files:
                raise HTTPException(status_code=404, detail="No Delta log files found")
            
            log_files.sort()
            
            schema_json = '{}'
            metadata_id = table_name
            partition_columns = []
            
            for log_file in log_files:
                try:
                    response = s3_client.get_object(Bucket=bucket, Key=log_file)
                    log_content = response['Body'].read().decode('utf-8')
                    for line in log_content.strip().split('\n'):
                        if not line.strip():
                            continue
                        try:
                            log_entry = json.loads(line)
                            if 'metaData' in log_entry:
                                metadata_dict = log_entry['metaData']
                                schema_json = metadata_dict.get('schemaString', '{}')
                                metadata_id = metadata_dict.get('id', table_name)
                                partition_columns = metadata_dict.get('partitionColumns', [])
                        except json.JSONDecodeError:
                            continue
                except Exception as read_error:
                    continue
            
            if schema_json != '{}':
                try:
                    schema_obj = json.loads(schema_json) if isinstance(schema_json, str) else schema_json
                    schema_obj = transform_schema_for_timestamp_ntz(schema_obj)
                    schema_string = json.dumps(schema_obj)
                except Exception as schema_error:
                    print(f"Warning: Could not transform schema: {schema_error}")
                    schema_string = schema_json
            else:
                schema_string = '{"type":"struct","fields":[]}'
            
            if use_fallback:
                files_list = []
                for log_file in log_files:
                    try:
                        response = s3_client.get_object(Bucket=bucket, Key=log_file)
                        log_content = response['Body'].read().decode('utf-8')
                        for line in log_content.strip().split('\n'):
                            if not line.strip():
                                continue
                            try:
                                log_entry = json.loads(line)
                                if 'add' in log_entry:
                                    file_path = log_entry['add']['path']
                                    if not file_path.startswith(table_prefix):
                                        full_path = f"{table_prefix}/{file_path}" if table_prefix else file_path
                                    else:
                                        full_path = file_path
                                    files_list.append(get_full_s3_path(bucket, full_path))
                            except json.JSONDecodeError:
                                continue
                    except Exception as parse_error:
                        continue
            
            class MockMetadata:
                def __init__(self, table_id, part_cols):
                    self.id = table_id
                    self.partition_columns = part_cols
            metadata = MockMetadata(metadata_id, partition_columns)
            table_version = len(log_files)
        
        lines = []
        
        lines.append(json.dumps({"protocol": {"minReaderVersion": 1}}))
        
        if schema_string is None:
            schema_string = '{"type":"struct","fields":[]}'
        
        lines.append(json.dumps({
            "metaData": {
                "id": metadata.id if hasattr(metadata, 'id') else table_name,
                "format": {"provider": "parquet"},
                "schemaString": schema_string,
                "partitionColumns": metadata.partition_columns if hasattr(metadata, 'partition_columns') else []
            }
        }))
        
        for file_path in files_list:
            if file_path.startswith(f"s3://{bucket}/"):
                key = file_path.replace(f"s3://{bucket}/", "")
            elif file_path.startswith("/"):
                key = file_path.lstrip("/")
            else:
                key = file_path
            
            if not key.startswith(table_prefix) and table_prefix:
                key = f"{table_prefix}/{key}" if not key.startswith("/") else f"{table_prefix}{key}"
            
            presigned_url = get_presigned_url(s3_client, bucket, key, endpoint_for_client, is_localstack)
            
            try:
                response = s3_client.head_object(Bucket=bucket, Key=key)
                file_size = response.get('ContentLength', 0)
            except:
                file_size = 0
            
            file_action = {
                "file": {
                    "url": presigned_url,
                    "id": key,
                    "partitionValues": {},
                    "size": file_size,
                    "version": table_version
                }
            }
            lines.append(json.dumps(file_action))
        
        content = "\n".join(lines) + "\n"
        
        try:
            columns_requested = None
            query_limit = None
            if body:
                if "columns" in body:
                    columns_requested = ",".join(body["columns"]) if isinstance(body["columns"], list) else str(body["columns"])
                if "limit" in body:
                    query_limit = body["limit"]
            
            client_ip = request.client.host if request.client else None
            
            row_count = len(files_list)
            
            audit_log = AuditLog(
                buyer_id=share.buyer_id,
                dataset_id=share.dataset_id,
                share_id=share.id,
                columns_requested=columns_requested,
                row_count_returned=row_count,
                query_limit=query_limit,
                ip_address=client_ip
            )
            db.add(audit_log)
            db.commit()
        except Exception as e:
            print(f"Warning: Failed to log query: {e}")
        
        return StreamingResponse(
            io.BytesIO(content.encode()),
            media_type="application/x-ndjson",
            headers={"delta-table-version": str(table_version)}
        )
    except HTTPException:
        raise
    except Exception as e:
        error_detail = str(e) if str(e) else repr(e)
        raise HTTPException(status_code=500, detail=f"Error querying Delta table: {error_detail}")

@app.post("/shares/prepare")
async def prepare_share(
    request: Request,
    db: Session = Depends(get_db)
):
    return {"status": "success", "message": "Watermarking is applied at query time"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

