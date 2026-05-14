import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("ALLOW_INSECURE_DEFAULTS", "true")
os.environ.setdefault("WATERMARK_SECRET", "dev-watermark-secret")
os.environ.setdefault("TOKEN_SIGNING_SECRET", "dev-token-signing-secret")
os.environ.setdefault("TOKEN_SALT", "dev-token-salt")
os.environ.setdefault("JWT_SECRET_KEY", "dev-jwt-secret")
os.environ.setdefault("SELLER_JWT_SECRET_KEY", "dev-seller-jwt-secret")

import json

from sqlalchemy import Column, Integer, String, Text, DateTime, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

from src.utils.audit_chain import (
    GENESIS_HASH,
    compute_entry_hash,
    extract_hashable_fields,
    seal_audit_entry,
    verify_chain,
    verify_table_chain,
)
from src.utils.time_utils import utcnow

_Base = declarative_base()

class _TestAuditLog(_Base):
    __tablename__ = "test_audit_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String)
    actor_id = Column(Integer)
    payload = Column(Text)
    created_at = Column(DateTime, default=utcnow)
    prev_hash = Column(String(64))
    entry_hash = Column(String(64))

def _make_session():
    engine = create_engine("sqlite:///:memory:")
    _Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def _seal_and_add(db, **kwargs):
    entry = _TestAuditLog(**kwargs)
    seal_audit_entry(db, entry)
    db.add(entry)
    db.commit()
    return entry

def test_genesis_entry_prev_hash_is_all_zeros():
    db = _make_session()
    e = _seal_and_add(db, event_type="created", actor_id=1, payload=json.dumps({"k": 1}))
    assert e.prev_hash == GENESIS_HASH
    assert e.entry_hash is not None
    assert len(e.entry_hash) == 64

def test_compute_entry_hash_deterministic():
    fields = {"event_type": "x", "actor_id": 42, "payload": "y"}
    h1 = compute_entry_hash(GENESIS_HASH, fields)
    h2 = compute_entry_hash(GENESIS_HASH, fields)
    assert h1 == h2
    assert compute_entry_hash("a" * 64, fields) != h1

def test_100_entries_chain_is_valid():
    db = _make_session()
    for i in range(100):
        _seal_and_add(db, event_type=f"evt_{i}", actor_id=i, payload=json.dumps({"i": i}))

    result = verify_table_chain(db, _TestAuditLog)
    assert result["valid"] is True, result["reason"]
    assert result["checked"] == 100

def test_tampering_middle_entry_detected():
    db = _make_session()
    ids = []
    for i in range(10):
        e = _seal_and_add(db, event_type=f"evt_{i}", actor_id=i, payload=json.dumps({"i": i}))
        ids.append(e.id)

    middle = db.query(_TestAuditLog).filter(_TestAuditLog.id == ids[4]).one()
    middle.payload = "TAMPERED"
    db.commit()

    result = verify_table_chain(db, _TestAuditLog)
    assert result["valid"] is False
    assert result["first_broken_id"] == ids[4]

def test_tampering_prev_hash_detected():
    db = _make_session()
    ids = []
    for i in range(5):
        e = _seal_and_add(db, event_type=f"evt_{i}", actor_id=i, payload=json.dumps({"i": i}))
        ids.append(e.id)

    victim = db.query(_TestAuditLog).filter(_TestAuditLog.id == ids[2]).one()
    victim.prev_hash = "f" * 64
    db.commit()

    result = verify_table_chain(db, _TestAuditLog)
    assert result["valid"] is False
    assert result["first_broken_id"] == ids[2]

def test_deleting_middle_entry_detected():
    db = _make_session()
    ids = []
    for i in range(5):
        e = _seal_and_add(db, event_type=f"evt_{i}", actor_id=i, payload=json.dumps({"i": i}))
        ids.append(e.id)

    db.query(_TestAuditLog).filter(_TestAuditLog.id == ids[2]).delete()
    db.commit()

    result = verify_table_chain(db, _TestAuditLog)
    assert result["valid"] is False
    assert result["first_broken_id"] == ids[3]

def test_empty_chain_is_trivially_valid():
    db = _make_session()
    result = verify_table_chain(db, _TestAuditLog)
    assert result["valid"] is True
    assert result["checked"] == 0

def test_extract_hashable_fields_excludes_hash_columns():
    db = _make_session()
    e = _seal_and_add(db, event_type="x", actor_id=1, payload="{}")
    fields = extract_hashable_fields(e)
    assert "id" not in fields
    assert "prev_hash" not in fields
    assert "entry_hash" not in fields
    assert "event_type" in fields
    assert "actor_id" in fields

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
