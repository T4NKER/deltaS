import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from src.utils.time_utils import utcnow

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://deltasharing:deltasharing123@localhost:5433/marketplace")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role = Column(String, default="buyer")
    created_at = Column(DateTime, default=utcnow)
    delta_sharing_server_url = Column(String)
    public_key = Column(Text)

    datasets = relationship("Dataset", back_populates="seller")
    shares_as_seller = relationship("Share", foreign_keys="Share.seller_id", back_populates="seller")
    shares_as_buyer = relationship("Share", foreign_keys="Share.buyer_id", back_populates="buyer")
    purchases = relationship("Purchase", back_populates="buyer")

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    table_name = Column(String)
    table_path = Column(String, nullable=False)
    price = Column(Float, default=0.0)
    is_public = Column(Boolean, default=False)
    seller_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=utcnow)
    risk_score = Column(Float, default=0.0)
    risk_level = Column(String, default="low")
    detected_pii_types = Column(Text)
    sensitive_columns = Column(Text)
    requires_approval = Column(Boolean, default=False)
    anchor_columns = Column(Text)
    license_name = Column(String)
    license_terms = Column(Text)
    privacy_status = Column(String)
    privacy_summary = Column(Text)

    seller = relationship("User", back_populates="datasets")
    shares = relationship("Share", back_populates="dataset")
    purchases = relationship("Purchase", back_populates="dataset")

class Share(Base):
    __tablename__ = "shares"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    seller_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    buyer_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    encrypted_token = Column(Text, nullable=True)
    token_hash = Column(String, unique=True, index=True, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    expires_at = Column(DateTime)
    approval_status = Column(String, default="pending")
    revoked = Column(Boolean, default=False)
    revoked_at = Column(DateTime)
    is_trial = Column(Boolean, default=False)
    trial_row_limit = Column(Integer)
    trial_expires_at = Column(DateTime)
    token_rotated_at = Column(DateTime)
    last_used_at = Column(DateTime)
    watermark_nonce = Column(String)
    profile_json = Column(Text)
    profile_generated_at = Column(DateTime)

    dataset = relationship("Dataset", back_populates="shares")
    seller = relationship("User", foreign_keys=[seller_id], back_populates="shares_as_seller")
    buyer = relationship("User", foreign_keys=[buyer_id], back_populates="shares_as_buyer")

    __table_args__ = (
        UniqueConstraint("dataset_id", "buyer_id", name="uq_shares_dataset_buyer"),
    )

class Purchase(Base):
    __tablename__ = "purchases"

    id = Column(Integer, primary_key=True, index=True)
    buyer_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    share_id = Column(Integer, ForeignKey("shares.id"), nullable=False)
    amount = Column(Float, nullable=False)
    created_at = Column(DateTime, default=utcnow)
    license_accepted_at = Column(DateTime)
    accepted_license_name = Column(String)
    accepted_license_terms_hash = Column(String)

    buyer = relationship("User", back_populates="purchases")
    dataset = relationship("Dataset", back_populates="purchases")
    share = relationship("Share")

class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String, default="query")
    actor_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    buyer_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=True)
    share_id = Column(Integer, ForeignKey("shares.id"), nullable=True)
    query_time = Column(DateTime, default=utcnow)
    columns_requested = Column(Text)
    row_count_returned = Column(Integer, default=0)
    query_limit = Column(Integer)
    predicates_requested = Column(Text)
    predicates_applied = Column(Text)
    predicates_applied_count = Column(Integer)
    anchor_columns_used = Column(Text)
    columns_returned = Column(Text)
    ip_address = Column(String)
    bytes_served = Column(Integer)
    client_metadata = Column(Text)
    prev_hash = Column(String(64))
    entry_hash = Column(String(64))

    buyer = relationship("User", foreign_keys=[buyer_id])
    actor = relationship("User", foreign_keys=[actor_id])
    dataset = relationship("Dataset")
    share = relationship("Share")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_unique_constraints()


def _ensure_unique_constraints():
    from sqlalchemy import text, inspect
    try:
        inspector = inspect(engine)
        if "shares" not in inspector.get_table_names():
            return
        existing = {
            uq.get("name") for uq in inspector.get_unique_constraints("shares")
        }
        if "uq_shares_dataset_buyer" in existing:
            return
        with engine.connect() as conn:
            conn.execute(text(
                "ALTER TABLE shares "
                "ADD CONSTRAINT uq_shares_dataset_buyer "
                "UNIQUE (dataset_id, buyer_id)"
            ))
            conn.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            "Could not ensure UNIQUE(dataset_id, buyer_id) on shares: %s", e
        )
