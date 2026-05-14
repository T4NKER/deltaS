import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from src.utils.time_utils import utcnow

SELLER_DATABASE_URL = os.getenv(
    "SELLER_DATABASE_URL",
    os.getenv("DATABASE_URL", "postgresql://deltasharing:deltasharing123@localhost:5433/seller_db")
)

seller_engine = create_engine(SELLER_DATABASE_URL)
SellerSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=seller_engine)
SellerBase = declarative_base()

class SellerShare(SellerBase):
    __tablename__ = "seller_shares"

    id = Column(Integer, primary_key=True)
    share_id = Column(Integer, unique=True, index=True, nullable=False)
    dataset_id = Column(Integer, nullable=False)
    seller_id = Column(Integer, nullable=False)
    buyer_id = Column(Integer, nullable=False)
    token_hash = Column(String, index=True)
    approval_status = Column(String, default="pending")
    revoked = Column(Boolean, default=False)
    revoked_at = Column(DateTime)
    expires_at = Column(DateTime)
    is_trial = Column(Boolean, default=False)
    trial_row_limit = Column(Integer)
    trial_expires_at = Column(DateTime)
    last_used_at = Column(DateTime)
    watermark_nonce = Column(String)
    table_path = Column(String, nullable=False)
    anchor_columns = Column(Text)
    buyer_public_key_hash = Column(String)
    created_at = Column(DateTime, default=utcnow)

class SellerAuditLog(SellerBase):
    __tablename__ = "seller_audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    buyer_id = Column(Integer)
    dataset_id = Column(Integer)
    share_id = Column(Integer)
    query_time = Column(DateTime, default=utcnow)
    columns_requested = Column(Text)
    columns_returned = Column(Text)
    row_count_returned = Column(Integer, default=0)
    query_limit = Column(Integer)
    predicates_requested = Column(Text)
    predicates_applied = Column(Text)
    predicates_applied_count = Column(Integer)
    anchor_columns_used = Column(Text)
    ip_address = Column(String)
    bytes_served = Column(Integer)
    client_metadata = Column(Text)
    prev_hash = Column(String(64))
    entry_hash = Column(String(64))

def get_seller_db():
    db = SellerSessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_seller_db():
    SellerBase.metadata.create_all(bind=seller_engine)
    _ensure_audit_chain_columns()

def _ensure_audit_chain_columns():
    from sqlalchemy import text, inspect
    required = {
        "seller_audit_logs": [
            ("prev_hash", "VARCHAR(64)"),
            ("entry_hash", "VARCHAR(64)"),
        ],
        "seller_shares": [
            ("watermark_nonce", "VARCHAR"),
            ("buyer_public_key_hash", "VARCHAR"),
        ],
    }
    try:
        inspector = inspect(seller_engine)
        existing_tables = set(inspector.get_table_names())
        for table_name, cols in required.items():
            if table_name not in existing_tables:
                continue
            existing_cols = {c["name"] for c in inspector.get_columns(table_name)}
            missing = [(n, t) for (n, t) in cols if n not in existing_cols]
            if not missing:
                continue
            with seller_engine.connect() as conn:
                for col_name, col_type in missing:
                    conn.execute(text(
                        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                    ))
                conn.commit()
    except Exception as e:
        print(f"[seller_db] warning: could not ensure seller table columns: {e}")
