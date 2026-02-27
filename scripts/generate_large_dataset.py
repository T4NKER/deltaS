#!/usr/bin/env python3
import sys
import os
import argparse
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
os.environ.setdefault('PYTHONPATH', str(project_root))

import pandas as pd
import numpy as np
import pyarrow as pa
from deltalake import write_deltalake
from datetime import datetime, timedelta
from src.utils.s3_utils import get_delta_storage_options, get_full_s3_path, get_bucket_name

def estimate_rows_for_size(target_size_gb: float, num_columns: int = 10, avg_col_size: int = 100) -> int:
    avg_row_size_bytes = num_columns * avg_col_size
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    estimated_rows = int(target_size_bytes / avg_row_size_bytes)
    return estimated_rows

def generate_large_dataset(
    table_path: str,
    target_size_gb: float = 3.0,
    num_columns: int = 10,
    chunk_size: int = 100000,
    seed: int = 42
):
    np.random.seed(seed)
    
    bucket_name = get_bucket_name()
    full_table_path = get_full_s3_path(bucket_name, table_path)
    storage_options = get_delta_storage_options()
    
    estimated_rows = estimate_rows_for_size(target_size_gb, num_columns)
    num_chunks = (estimated_rows + chunk_size - 1) // chunk_size
    
    print(f"Generating large dataset:")
    print(f"  Target size: {target_size_gb} GB")
    print(f"  Estimated rows: {estimated_rows:,}")
    print(f"  Chunk size: {chunk_size:,} rows")
    print(f"  Number of chunks: {num_chunks}")
    print(f"  Table path: {full_table_path}")
    print()
    
    columns = {
        'id': lambda n: range(n),
        'timestamp': lambda n: [datetime.utcnow() - timedelta(seconds=i) for i in range(n)],
        'value': lambda n: np.random.uniform(0, 1000, n),
        'category': lambda n: np.random.choice(['A', 'B', 'C', 'D', 'E'], n),
        'country': lambda n: np.random.choice(['EE', 'LV', 'LT', 'FI', 'SE'], n),
        'amount': lambda n: np.random.uniform(10, 10000, n),
        'status': lambda n: np.random.choice(['active', 'inactive', 'pending'], n),
        'score': lambda n: np.random.normal(50, 15, n),
        'description': lambda n: [f"Item_{i}_description" for i in range(n)],
        'metadata': lambda n: [f"{{'key': 'value_{i}'}}" for i in range(n)],
    }
    
    column_names = list(columns.keys())[:num_columns]
    
    total_rows_written = 0
    start_time = datetime.utcnow()
    
    for chunk_idx in range(num_chunks):
        rows_in_chunk = min(chunk_size, estimated_rows - total_rows_written)
        if rows_in_chunk <= 0:
            break
        
        chunk_data = {}
        for col_name in column_names:
            chunk_data[col_name] = columns[col_name](rows_in_chunk)
        
        df = pd.DataFrame(chunk_data)
        table = pa.Table.from_pandas(df)
        
        mode = 'overwrite' if chunk_idx == 0 else 'append'
        write_deltalake(
            full_table_path,
            table,
            mode=mode,
            storage_options=storage_options
        )
        
        total_rows_written += rows_in_chunk
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        rate = total_rows_written / elapsed if elapsed > 0 else 0
        
        print(f"Chunk {chunk_idx + 1}/{num_chunks}: Wrote {rows_in_chunk:,} rows "
              f"(Total: {total_rows_written:,}, Rate: {rate:,.0f} rows/sec)")
    
    elapsed_total = (datetime.utcnow() - start_time).total_seconds()
    print()
    print(f"Completed!")
    print(f"  Total rows written: {total_rows_written:,}")
    print(f"  Total time: {elapsed_total:.2f} seconds")
    print(f"  Average rate: {total_rows_written / elapsed_total:,.0f} rows/sec")
    print(f"  Table path: {full_table_path}")
    
    return total_rows_written

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a large Delta Lake dataset for testing")
    parser.add_argument("table_path", help="S3 table path (e.g., 'large_test_dataset')")
    parser.add_argument("--size-gb", type=float, default=3.0, help="Target dataset size in GB (default: 3.0)")
    parser.add_argument("--columns", type=int, default=10, help="Number of columns (default: 10)")
    parser.add_argument("--chunk-size", type=int, default=100000, help="Rows per chunk (default: 100000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    
    args = parser.parse_args()
    
    try:
        generate_large_dataset(
            table_path=args.table_path,
            target_size_gb=args.size_gb,
            num_columns=args.columns,
            chunk_size=args.chunk_size,
            seed=args.seed
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

