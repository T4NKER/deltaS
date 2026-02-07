import os
import sys
from pyspark.sql import SparkSession
from delta.sharing import DeltaSharingClient

def read_delta_sharing_table(profile_path: str, share_name: str, schema_name: str, table_name: str):
    spark = SparkSession.builder \
        .appName("DeltaSharingReader") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    client = DeltaSharingClient(profile_path)
    
    table_path = f"{share_name}.{schema_name}.{table_name}"
    
    print(f"Reading table: {table_path}")
    df = client.load_table(table_path)
    
    print(f"Table loaded successfully")
    print(f"  Rows: {df.count()}")
    print(f"  Columns: {df.columns}")
    print(f"\n  Schema:")
    df.printSchema()
    
    print(f"\n  Sample data (first 10 rows):")
    df.show(10, truncate=False)
    
    print(f"\n  Row count: {df.count()}")
    
    spark.stop()
    return df

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python example_pyspark.py <profile_path> <share_name> <schema_name> <table_name>")
        sys.exit(1)
    
    profile_path = sys.argv[1]
    share_name = sys.argv[2]
    schema_name = sys.argv[3]
    table_name = sys.argv[4]
    
    read_delta_sharing_table(profile_path, share_name, schema_name, table_name)

