import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
os.environ.setdefault('PYTHONPATH', str(project_root))

from src.models.database import engine, Base
from sqlalchemy import text

def migrate_database():
    print("Migrating database to add privacy features, approval workflows, and usage logging...")
    
    with engine.connect() as conn:
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS risk_score FLOAT DEFAULT 0.0;
            """))
            print("Added risk_score column")
        except Exception as e:
            print(f"risk_score column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS risk_level VARCHAR DEFAULT 'low';
            """))
            print("Added risk_level column")
        except Exception as e:
            print(f"risk_level column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS detected_pii_types TEXT;
            """))
            print("Added detected_pii_types column")
        except Exception as e:
            print(f"detected_pii_types column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS sensitive_columns TEXT;
            """))
            print("Added sensitive_columns column")
        except Exception as e:
            print(f"sensitive_columns column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS requires_approval BOOLEAN DEFAULT FALSE;
            """))
            print("Added requires_approval column")
        except Exception as e:
            print(f"requires_approval column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS approval_status VARCHAR DEFAULT 'pending';
            """))
            print("Added approval_status column to shares")
        except Exception as e:
            print(f"approval_status column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS revoked BOOLEAN DEFAULT FALSE;
            """))
            print("Added revoked column to shares")
        except Exception as e:
            print(f"revoked column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMP;
            """))
            print("Added revoked_at column to shares")
        except Exception as e:
            print(f"revoked_at column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS watermarked_table_path VARCHAR;
            """))
            print("Added watermarked_table_path column to shares")
        except Exception as e:
            print(f"watermarked_table_path column: {e}")
        
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id SERIAL PRIMARY KEY,
                    buyer_id INTEGER NOT NULL REFERENCES users(id),
                    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
                    share_id INTEGER NOT NULL REFERENCES shares(id),
                    query_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    columns_requested TEXT,
                    row_count_returned INTEGER DEFAULT 0,
                    query_limit INTEGER,
                    predicates_requested TEXT,
                    predicates_applied TEXT,
                    predicates_applied_count INTEGER,
                    ip_address VARCHAR
                );
            """))
            print("Created audit_logs table")
        except Exception as e:
            print(f"audit_logs table: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE audit_logs 
                ADD COLUMN IF NOT EXISTS predicates_requested TEXT;
            """))
            print("Added predicates_requested column to audit_logs")
        except Exception as e:
            print(f"predicates_requested column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE audit_logs 
                ADD COLUMN IF NOT EXISTS predicates_applied TEXT;
            """))
            print("Added predicates_applied column to audit_logs")
        except Exception as e:
            print(f"predicates_applied column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE audit_logs 
                ADD COLUMN IF NOT EXISTS predicates_applied_count INTEGER;
            """))
            print("Added predicates_applied_count column to audit_logs")
        except Exception as e:
            print(f"predicates_applied_count column: {e}")
        
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_rate_limits (
                    id SERIAL PRIMARY KEY,
                    buyer_id INTEGER NOT NULL REFERENCES users(id),
                    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
                    query_count INTEGER DEFAULT 0,
                    window_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    window_type VARCHAR DEFAULT 'hour'
                );
            """))
            print("Created query_rate_limits table")
        except Exception as e:
            print(f"query_rate_limits table: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS delta_sharing_server_url VARCHAR;
            """))
            print("Added delta_sharing_server_url column to users")
        except Exception as e:
            print(f"delta_sharing_server_url column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                DROP COLUMN IF EXISTS s3_endpoint_id;
            """))
            print("Removed s3_endpoint_id column from datasets")
        except Exception as e:
            print(f"s3_endpoint_id column removal: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS is_trial BOOLEAN DEFAULT FALSE;
            """))
            print("Added is_trial column to shares")
        except Exception as e:
            print(f"is_trial column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS trial_row_limit INTEGER;
            """))
            print("Added trial_row_limit column to shares")
        except Exception as e:
            print(f"trial_row_limit column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE shares 
                ADD COLUMN IF NOT EXISTS trial_expires_at TIMESTAMP;
            """))
            print("Added trial_expires_at column to shares")
        except Exception as e:
            print(f"trial_expires_at column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS table_name VARCHAR;
            """))
            print("Added table_name column to datasets")
            conn.execute(text("""
                UPDATE datasets 
                SET table_name = name 
                WHERE table_name IS NULL;
            """))
            print("Populated table_name with name values for existing datasets")
        except Exception as e:
            print(f"table_name column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS anchor_columns TEXT;
            """))
            print("Added anchor_columns column to datasets")
        except Exception as e:
            print(f"anchor_columns column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE audit_logs 
                ADD COLUMN IF NOT EXISTS anchor_columns_used TEXT;
            """))
            print("Added anchor_columns_used column to audit_logs")
        except Exception as e:
            print(f"anchor_columns_used column: {e}")
        
        try:
            conn.execute(text("""
                ALTER TABLE audit_logs 
                ADD COLUMN IF NOT EXISTS columns_returned TEXT;
            """))
            print("Added columns_returned column to audit_logs")
        except Exception as e:
            print(f"columns_returned column: {e}")
        
        conn.commit()
        print("\nDatabase migration completed!")
        print("Note: If columns already exist, you may see errors above. This is normal.")
        print("Note: S3 credentials are now seller-side (environment variables), not in marketplace database.")

if __name__ == "__main__":
    migrate_database()

