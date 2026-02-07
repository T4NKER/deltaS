import pandas as pd
import numpy as np
import pyarrow as pa
from typing import Dict, List, Optional, Tuple
from deltalake import write_deltalake, DeltaTable
from src.utils.s3_utils import get_delta_storage_options, get_full_s3_path, get_bucket_name
from src.seller.pii_detection import analyze_dataset_for_pii

def generate_synthetic_data(
    original_table_path: str,
    output_table_path: str,
    num_rows: int,
    dp_epsilon: Optional[float] = None,
    preserve_statistics: bool = True,
    seed: Optional[int] = None
) -> Tuple[pd.DataFrame, Dict]:
    storage_options = get_delta_storage_options()
    full_original_path = get_full_s3_path(get_bucket_name(), original_table_path)
    
    try:
        delta_table = DeltaTable(full_original_path, storage_options=storage_options)
        original_df = delta_table.to_pandas()
        schema = delta_table.schema()
    except Exception as e:
        raise ValueError(f"Failed to read original table: {str(e)}")
    
    if original_df.empty:
        raise ValueError("Original table is empty, cannot generate synthetic data")
    
    metadata = {
        "synthetic": True,
        "dp_enabled": dp_epsilon is not None,
        "dp_epsilon": dp_epsilon,
        "num_rows": num_rows,
        "original_rows": len(original_df),
        "preserve_statistics": preserve_statistics
    }
    
    if seed is not None:
        np.random.seed(seed)
        metadata["seed"] = seed
    
    synthetic_df = pd.DataFrame()
    
    for field in schema:
        col_name = field.name
        col_type = str(field.type)
        original_series = original_df[col_name]
        
        if dp_epsilon is not None:
            synthetic_col = _generate_dp_column(original_series, col_type, num_rows, dp_epsilon)
        else:
            synthetic_col = _generate_simple_synthetic_column(original_series, col_type, num_rows, preserve_statistics)
        
        synthetic_df[col_name] = synthetic_col
    
    full_output_path = get_full_s3_path(get_bucket_name(), output_table_path)
    table = pa.Table.from_pandas(synthetic_df)
    
    write_deltalake(
        full_output_path,
        table,
        mode='overwrite',
        storage_options=storage_options
    )
    
    pii_analysis = analyze_dataset_for_pii(synthetic_df)
    metadata["pii_analysis"] = {
        "sensitive_columns": pii_analysis[0],
        "pii_types": dict(pii_analysis[1]),
        "risk_score": float(pii_analysis[2]),
        "risk_level": pii_analysis[3]
    }
    
    return synthetic_df, metadata

def _generate_simple_synthetic_column(series: pd.Series, col_type: str, num_rows: int, preserve_statistics: bool) -> pd.Series:
    non_null_series = series.dropna()
    
    if len(non_null_series) == 0:
        return pd.Series([None] * num_rows)
    
    if pd.api.types.is_integer_dtype(series):
        if preserve_statistics:
            min_val = int(non_null_series.min())
            max_val = int(non_null_series.max())
            mean_val = int(non_null_series.mean())
            std_val = int(non_null_series.std()) if len(non_null_series) > 1 else 1
            
            synthetic = np.random.normal(mean_val, std_val, num_rows).astype(int)
            synthetic = np.clip(synthetic, min_val, max_val)
        else:
            unique_vals = non_null_series.unique()
            synthetic = np.random.choice(unique_vals, num_rows, replace=True)
        
        return pd.Series(synthetic)
    
    elif pd.api.types.is_float_dtype(series):
        if preserve_statistics:
            min_val = float(non_null_series.min())
            max_val = float(non_null_series.max())
            mean_val = float(non_null_series.mean())
            std_val = float(non_null_series.std()) if len(non_null_series) > 1 else (max_val - min_val) / 4
            
            synthetic = np.random.normal(mean_val, std_val, num_rows)
            synthetic = np.clip(synthetic, min_val, max_val)
        else:
            unique_vals = non_null_series.unique()
            synthetic = np.random.choice(unique_vals, num_rows, replace=True)
        
        return pd.Series(synthetic)
    
    elif pd.api.types.is_datetime64_any_dtype(series):
        if preserve_statistics:
            min_date = non_null_series.min()
            max_date = non_null_series.max()
            date_range = (max_date - min_date).total_seconds()
            
            random_seconds = np.random.uniform(0, date_range, num_rows)
            synthetic = min_date + pd.to_timedelta(random_seconds, unit='s')
        else:
            unique_vals = non_null_series.unique()
            synthetic = np.random.choice(unique_vals, num_rows, replace=True)
        
        return pd.Series(synthetic)
    
    elif pd.api.types.is_bool_dtype(series):
        true_ratio = non_null_series.sum() / len(non_null_series)
        synthetic = np.random.random(num_rows) < true_ratio
        return pd.Series(synthetic)
    
    elif pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        value_counts = non_null_series.value_counts()
        probs = value_counts.values / value_counts.values.sum()
        synthetic = np.random.choice(value_counts.index, num_rows, p=probs, replace=True)
        return pd.Series(synthetic)
    
    else:
        unique_vals = non_null_series.unique()
        synthetic = np.random.choice(unique_vals, num_rows, replace=True)
        return pd.Series(synthetic)

def _generate_dp_column(series: pd.Series, col_type: str, num_rows: int, epsilon: float) -> pd.Series:
    non_null_series = series.dropna()
    
    if len(non_null_series) == 0:
        return pd.Series([None] * num_rows)
    
    sensitivity = 1.0
    noise_scale = sensitivity / epsilon if epsilon > 0 else float('inf')
    
    if pd.api.types.is_integer_dtype(series):
        mean_val = float(non_null_series.mean())
        std_val = float(non_null_series.std()) if len(non_null_series) > 1 else 1.0
        
        laplace_noise = np.random.laplace(0, noise_scale, num_rows)
        synthetic = (mean_val + laplace_noise).astype(int)
        
        min_val = int(non_null_series.min())
        max_val = int(non_null_series.max())
        synthetic = np.clip(synthetic, min_val, max_val)
        
        return pd.Series(synthetic)
    
    elif pd.api.types.is_float_dtype(series):
        mean_val = float(non_null_series.mean())
        std_val = float(non_null_series.std()) if len(non_null_series) > 1 else 1.0
        
        laplace_noise = np.random.laplace(0, noise_scale, num_rows)
        synthetic = mean_val + laplace_noise
        
        min_val = float(non_null_series.min())
        max_val = float(non_null_series.max())
        synthetic = np.clip(synthetic, min_val, max_val)
        
        return pd.Series(synthetic)
    
    elif pd.api.types.is_datetime64_any_dtype(series):
        mean_timestamp = non_null_series.astype('int64').mean()
        laplace_noise = np.random.laplace(0, noise_scale * 86400000000000, num_rows)
        synthetic_timestamps = (mean_timestamp + laplace_noise).astype('int64')
        synthetic = pd.to_datetime(synthetic_timestamps)
        
        return pd.Series(synthetic)
    
    else:
        return _generate_simple_synthetic_column(series, col_type, num_rows, preserve_statistics=True)

