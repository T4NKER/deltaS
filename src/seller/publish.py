import json
import hashlib
import hmac
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from src.seller.pii_detection import analyze_dataset_for_pii
from src.seller.watermarking import detect_anchor_columns_from_schema
from src.utils.s3_utils import get_delta_storage_options, get_full_s3_path, get_bucket_name
from src.utils.settings import get_settings

def generate_metadata_signature(metadata_dict: Dict[str, Any], seller_id: int) -> str:
    settings = get_settings()
    secret = settings.get_token_signing_secret_bytes()
    
    metadata_json = json.dumps(metadata_dict, sort_keys=True, default=str)
    signature = hmac.new(secret, metadata_json.encode('utf-8'), hashlib.sha256).hexdigest()
    return signature

def publish_dataset_metadata(
    table_path: str,
    seller_id: int,
    name: str,
    description: Optional[str] = None,
    anchor_columns: Optional[list] = None
) -> Dict[str, Any]:
    settings = get_settings()
    bucket_name = get_bucket_name()
    full_table_path = get_full_s3_path(bucket_name, table_path)
    storage_options = get_delta_storage_options()
    
    delta_table = DeltaTable(full_table_path, storage_options=storage_options)
    arrow_dataset = delta_table.to_pyarrow_dataset()
    schema = arrow_dataset.schema
    
    schema_fields = []
    for field in schema:
        field_dict = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable
        }
        schema_fields.append(field_dict)
    
    sample_df = delta_table.to_pandas().head(100)
    
    sensitive_columns_dict, pii_types_dict, risk_score, risk_level = analyze_dataset_for_pii(sample_df)
    
    if anchor_columns is None:
        sensitive_cols_list = list(sensitive_columns_dict.keys())
        anchor_columns = detect_anchor_columns_from_schema(schema, sensitive_columns=sensitive_cols_list)
    
    if not anchor_columns:
        raise ValueError("Could not detect suitable anchor columns. Please specify anchor_columns explicitly.")
    
    metadata = {
        "version": "1.0",
        "seller_id": seller_id,
        "name": name,
        "description": description,
        "table_path": table_path,
        "schema": {
            "fields": schema_fields,
            "metadata": schema.metadata if schema.metadata else {}
        },
        "anchor_columns": anchor_columns,
        "pii_analysis": {
            "sensitive_columns": sensitive_columns_dict,
            "pii_types": dict(pii_types_dict),
            "risk_score": float(risk_score),
            "risk_level": risk_level
        },
        "sample_row_count": len(sample_df),
        "total_row_count": None,
        "published_at": datetime.utcnow().isoformat()
    }
    
    signature = generate_metadata_signature(metadata, seller_id)
    metadata["signature"] = signature
    
    return metadata

def validate_metadata_signature(metadata_dict: Dict[str, Any], seller_id: int) -> bool:
    if "signature" not in metadata_dict:
        return False
    
    provided_signature = metadata_dict.pop("signature")
    computed_signature = generate_metadata_signature(metadata_dict, seller_id)
    metadata_dict["signature"] = provided_signature
    
    return hmac.compare_digest(provided_signature, computed_signature)

