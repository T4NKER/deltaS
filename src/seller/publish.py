from typing import Dict, Any, Optional
from datetime import datetime, timezone
import pyarrow as pa
from deltalake import DeltaTable
from src.seller.pii_detection import analyze_dataset_for_pii, assess_privacy_risk
from src.seller.watermarking import detect_anchor_columns_from_schema
from src.utils.s3_utils import get_delta_storage_options, get_full_s3_path, get_bucket_name
from src.utils.data_utils import convert_to_native_types
from src.utils.metadata_signing import sign_metadata_payload

def generate_metadata_signature(metadata_dict: Dict[str, Any]) -> str:
    return sign_metadata_payload(metadata_dict)

def publish_dataset_metadata(
    table_path: str,
    seller_id: int,
    name: str,
    description: Optional[str] = None,
    license_name: Optional[str] = None,
    license_terms: Optional[str] = None,
    anchor_columns: Optional[list] = None
) -> Dict[str, Any]:
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

    try:
        scanner = arrow_dataset.scanner()
        batches = []
        total_rows = 0
        for batch in scanner.to_batches():
            batches.append(batch)
            total_rows += len(batch)
            if total_rows >= 100:
                break
        if not batches:
            raise ValueError("No data found in table")
        sample_table = pa.Table.from_batches(batches)
        if len(sample_table) > 100:
            sample_table = sample_table.slice(0, 100)
        sample_df = sample_table.to_pandas()
    except Exception as e:
        raise ValueError(f"Failed to read sample data from table: {str(e)}")

    sensitive_columns_dict, pii_types_dict, risk_score, risk_level = analyze_dataset_for_pii(sample_df)
    privacy_assessment = assess_privacy_risk(sensitive_columns_dict, pii_types_dict, risk_score)

    if anchor_columns is None:
        sensitive_cols_list = list(sensitive_columns_dict.keys())
        anchor_columns = detect_anchor_columns_from_schema(schema, sensitive_columns=sensitive_cols_list)

    if not anchor_columns:
        raise ValueError("Could not detect suitable anchor columns. Please specify anchor_columns explicitly.")

    pii_types_converted = {str(k): int(v) for k, v in pii_types_dict.items()}
    sensitive_columns_converted = {str(k): [str(item) for item in v] if isinstance(v, list) else str(v) for k, v in sensitive_columns_dict.items()}

    metadata = {
        "version": "1.0",
        "seller_id": int(seller_id),
        "name": str(name),
        "description": str(description) if description else None,
        "table_path": str(table_path),
        "license_name": str(license_name) if license_name else None,
        "license_terms": str(license_terms) if license_terms else None,
        "schema": {
            "fields": schema_fields,
            "metadata": dict(schema.metadata) if schema.metadata else {}
        },
        "anchor_columns": [str(col) for col in anchor_columns],
        "pii_analysis": {
            "sensitive_columns": sensitive_columns_converted,
            "pii_types": pii_types_converted,
            "risk_score": float(risk_score),
            "risk_level": str(risk_level)
        },
        "privacy_assessment": privacy_assessment,
        "sample_row_count": int(len(sample_df)),
        "total_row_count": None,
        "published_at": datetime.now(timezone.utc).isoformat()
    }

    metadata = convert_to_native_types(metadata)

    signature = generate_metadata_signature(metadata)
    metadata["signature"] = signature

    return metadata
