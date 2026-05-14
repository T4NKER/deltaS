import pandas as pd
import numpy as np
from typing import Any, List, Optional

def convert_to_native_types(obj: Any) -> Any:
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {k: convert_to_native_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_native_types(item) for item in obj]
    elif isinstance(obj, set):
        return {convert_to_native_types(item) for item in obj}
    else:
        return obj

def normalize_value_for_anchor(value, dtype):
    if pd.isna(value):
        return 'NULL'

    if pd.api.types.is_integer_dtype(dtype):
        return str(int(value))
    elif pd.api.types.is_float_dtype(dtype):
        return f"{float(value):.10f}"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        ts = pd.to_datetime(value)
        return ts.strftime('%Y-%m-%dT%H:%M:%S.%f')
    elif pd.api.types.is_bool_dtype(dtype):
        return 'TRUE' if value else 'FALSE'
    else:
        return str(value)

def detect_timestamp_columns(df: pd.DataFrame) -> List[str]:
    timestamp_cols = []
    for col in df.columns:
        if col == '_watermark_id':
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            timestamp_cols.append(col)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            try:
                dropped = df[col].dropna()
                sample_val = dropped.iloc[0] if not dropped.empty else None
                if sample_val and isinstance(sample_val, str):
                    if 'T' in sample_val or ('-' in sample_val[:10] and len(sample_val) > 10):
                        try:
                            pd.to_datetime(sample_val)
                            timestamp_cols.append(col)
                        except Exception:
                            pass
            except Exception:
                pass
    return timestamp_cols

def parse_anchor_columns(anchor_columns_str: Optional[str]) -> List[str]:
    if not anchor_columns_str:
        return []
    return [col.strip() for col in anchor_columns_str.split(',') if col.strip()]

