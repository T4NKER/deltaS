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
from src.utils.predicate_parser import parse_query_predicates
from src.utils.predicate_parser import parse_query_predicates

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
    
    table_name = dataset.table_name if dataset.table_name else dataset.name
    
    return {
        "items": [{"name": table_name, "share": share_name, "schema": schema_name}],
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
    
    expected_table_name = dataset.table_name if dataset.table_name else dataset.name
    if table_name != expected_table_name:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
    
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
    
    expected_table_name = dataset.table_name if dataset.table_name else dataset.name
    if table_name != expected_table_name:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
    
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
    
    expected_table_name = dataset.table_name if dataset.table_name else dataset.name
    if table_name != expected_table_name:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
    
    bucket_name = get_bucket_name()
    
    table_path = get_table_path(dataset, bucket_name, share)
    table_prefix = dataset.table_path.rstrip('/')
    
    requested_columns = body.get("columns")
    requested_limit = body.get("limit")
    
    effective_limit = None
    if share.is_trial and share.trial_row_limit:
        if requested_limit:
            effective_limit = min(requested_limit, share.trial_row_limit)
        else:
            effective_limit = share.trial_row_limit
    elif requested_limit:
        effective_limit = requested_limit
    
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
        
        arrow_dataset = delta_table.to_pyarrow_dataset()
        original_schema = arrow_dataset.schema
        
        schema_col_names = [field.name for field in original_schema]
        
        if not dataset.anchor_columns:
            raise HTTPException(status_code=500, detail="Dataset anchor_columns not configured. Anchor columns must be set at dataset creation time.")
        
        anchor_columns_list = [col.strip() for col in dataset.anchor_columns.split(',') if col.strip()]
        anchor_columns_list = [col for col in anchor_columns_list if col in schema_col_names]
        if not anchor_columns_list:
            raise HTTPException(status_code=500, detail="Configured anchor columns not found in table schema")
        
        filter_expr, parsed_predicates = parse_query_predicates(body, original_schema)
        
        requested_columns_set = set()
        if requested_columns:
            if isinstance(requested_columns, list):
                requested_columns_set = set(requested_columns)
            elif isinstance(requested_columns, str):
                requested_columns_set = set([col.strip() for col in requested_columns.split(',')])
        
        columns_to_read = list(requested_columns_set) if requested_columns_set else schema_col_names.copy()
        
        for anchor_col in anchor_columns_list:
            if anchor_col not in columns_to_read:
                columns_to_read.append(anchor_col)
        
        columns_to_read = list(set(columns_to_read))
        
        missing_columns = [col for col in columns_to_read if col not in schema_col_names]
        if missing_columns:
            raise HTTPException(status_code=400, detail=f"Columns not found: {', '.join(missing_columns)}")
        
        missing_anchor_cols = [col for col in anchor_columns_list if col not in columns_to_read]
        if missing_anchor_cols:
            raise HTTPException(status_code=500, detail=f"Anchor columns missing from projection: {', '.join(missing_anchor_cols)}")
        
        schema_fields_for_scan = [field for field in original_schema if field.name in columns_to_read]
        if not schema_fields_for_scan:
            raise HTTPException(status_code=500, detail=f"No valid columns to read. Requested: {columns_to_read}, Available: {schema_col_names}")
        
        projected_schema_for_scan = pa.schema(schema_fields_for_scan)
        
        try:
            scanner_kwargs = {}
            if columns_to_read:
                scanner_kwargs['columns'] = columns_to_read
            if filter_expr is not None:
                scanner_kwargs['filter'] = filter_expr
            
            scanner = arrow_dataset.scanner(**scanner_kwargs)
        except HTTPException:
            raise
        except Exception as e:
            error_msg = str(e) if str(e) else repr(e)
            raise HTTPException(status_code=500, detail=f"Failed to create scanner: {error_msg}")
        
        record_batches = []
        rows_scanned = 0
        
        for batch in scanner.to_batches():
            if effective_limit and rows_scanned >= effective_limit:
                break
            
            batch_size = len(batch)
            if effective_limit:
                remaining = effective_limit - rows_scanned
                if remaining < batch_size:
                    batch = batch.slice(0, remaining)
            
            record_batches.append(batch)
            rows_scanned += len(batch)
            
            if effective_limit and rows_scanned >= effective_limit:
                break
        
        if not record_batches:
            result_table = pa.Table.from_arrays([[] for _ in projected_schema_for_scan], schema=projected_schema_for_scan)
        else:
            result_table = pa.Table.from_batches(record_batches)
        
        if len(result_table.schema) == 0:
            raise HTTPException(status_code=500, detail="Query returned table with empty schema. This may indicate a column projection issue.")
        
        df = result_table.to_pandas()
        
        if df.empty and len(df.columns) == 0 and len(record_batches) > 0:
            first_batch = record_batches[0]
            if len(first_batch.schema) > 0:
                df = first_batch.to_pandas()
        
        if len(df.columns) == 0:
            raise HTTPException(status_code=500, detail=f"Query returned DataFrame with no columns. Schema had {len(result_table.schema)} fields, record_batches: {len(record_batches)}")
        
        watermarked_df = apply_watermark_to_dataframe(df, watermark, is_trial=share.is_trial, anchor_columns=anchor_columns_list)
        
        if requested_columns_set:
            columns_to_return = list(requested_columns_set)
            
            for anchor_col in anchor_columns_list:
                if anchor_col not in requested_columns_set and anchor_col in columns_to_return:
                    columns_to_return.remove(anchor_col)
            
            if '_watermark_id' in watermarked_df.columns and share.is_trial:
                if '_watermark_id' not in requested_columns_set and '_watermark_id' in columns_to_return:
                    columns_to_return.remove('_watermark_id')
            
            if columns_to_return:
                available_cols = [col for col in columns_to_return if col in watermarked_df.columns]
                if available_cols:
                    watermarked_df = watermarked_df[available_cols]
                else:
                    raise HTTPException(status_code=400, detail="None of the requested columns are available in the result")
        
        actual_rows_returned = len(watermarked_df)
        columns_returned = list(watermarked_df.columns)
        
        temp_key = f"{table_prefix}/_watermarked_{share.id}_{uuid.uuid4().hex[:8]}.parquet"
        
        watermarked_table = pa.Table.from_pandas(watermarked_df, preserve_index=False)
        projected_schema = watermarked_table.schema
        
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
        
        try:
            metadata = delta_table.metadata()
            table_version = delta_table.version()
        except Exception as e:
            print(f"Warning: Could not get metadata from DeltaTable: {e}")
            metadata = None
            table_version = 0
        
        try:
            if len(watermarked_df.columns) == 0:
                raise ValueError(f"Cannot generate schema: watermarked_df has no columns. Original df had {len(df.columns)} columns: {list(df.columns)}")
            
            final_columns = list(watermarked_df.columns)
            
            delta_schema = delta_table.schema()
            full_schema_json_str = delta_schema.to_json()
            full_schema_obj = json.loads(full_schema_json_str)
            
            if 'fields' not in full_schema_obj:
                raise ValueError(f"Delta schema JSON missing 'fields' key: {full_schema_obj}")
            
            original_field_names = {field.get('name') for field in full_schema_obj.get('fields', [])}
            
            columns_in_original_schema = [col for col in final_columns if col in original_field_names]
            
            if not columns_in_original_schema:
                raise ValueError(f"No columns from watermarked_df found in original schema. watermarked_df columns: {list(watermarked_df.columns)}, original schema fields: {list(original_field_names)}")
            
            filtered_fields = [field for field in full_schema_obj['fields'] if field.get('name') in columns_in_original_schema]
            
            if not filtered_fields:
                raise ValueError(f"No matching schema fields found. columns_in_original_schema: {columns_in_original_schema}, available fields: {[f.get('name') for f in full_schema_obj.get('fields', [])]}")
            
            schema_obj = {
                "type": "struct",
                "fields": filtered_fields
            }
            
            schema_obj = transform_schema_for_timestamp_ntz(schema_obj)
            schema_string = json.dumps(schema_obj)
            
            if schema_string == '{"type":"struct","fields":[]}':
                raise ValueError(f"Generated empty schema. watermarked_df columns: {list(watermarked_df.columns)}")
        except Exception as e:
            error_msg = f"Failed to generate schema: {str(e)}. watermarked_df shape: {watermarked_df.shape}, columns: {list(watermarked_df.columns)}"
            print(f"ERROR: {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        lines = []
        
        lines.append(json.dumps({"protocol": {"minReaderVersion": 1}}))
        
        if schema_string is None:
            schema_string = '{"type":"struct","fields":[]}'
        
        metadata_id = metadata.id if metadata and hasattr(metadata, 'id') else table_name
        partition_columns = metadata.partition_columns if metadata and hasattr(metadata, 'partition_columns') else []
        
        lines.append(json.dumps({
            "metaData": {
                "id": metadata_id,
                "format": {"provider": "parquet"},
                "schemaString": schema_string,
                "partitionColumns": partition_columns
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
            if requested_columns:
                if isinstance(requested_columns, list):
                    columns_requested = ",".join(requested_columns)
                else:
                    columns_requested = str(requested_columns)
            
            predicates_requested = None
            predicates_applied = None
            predicates_applied_count = None
            
            if parsed_predicates:
                predicates_applied_count = len(parsed_predicates)
                if "jsonPredicateHints" in body:
                    predicates_requested = json.dumps(body["jsonPredicateHints"])
                    predicates_applied = json.dumps([{"column": p.column, "op": p.op, "value": p.value, "values": p.values} for p in parsed_predicates])
                elif "predicateHints" in body:
                    predicate_hints = body["predicateHints"]
                    if isinstance(predicate_hints, list):
                        predicates_requested = "\n".join(predicate_hints)
                    else:
                        predicates_requested = str(predicate_hints)
                    predicates_applied = "\n".join([f"{p.column} {p.op} {p.value if p.value is not None else p.values}" for p in parsed_predicates])
            
            client_ip = request.client.host if request.client else None
            
            audit_log = AuditLog(
                buyer_id=share.buyer_id,
                dataset_id=share.dataset_id,
                share_id=share.id,
                columns_requested=columns_requested,
                columns_returned=','.join(columns_returned) if columns_returned else None,
                row_count_returned=actual_rows_returned,
                query_limit=effective_limit,
                predicates_requested=predicates_requested,
                predicates_applied=predicates_applied,
                predicates_applied_count=predicates_applied_count,
                anchor_columns_used=','.join(anchor_columns_list),
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

