from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Iterable

GENESIS_HASH = "0" * 64

def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)

def compute_entry_hash(prev_hash: str, fields: dict[str, Any]) -> str:
    canonical = {k: _serialize_value(v) for k, v in fields.items()}
    payload = prev_hash + "|" + json.dumps(canonical, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def get_last_entry_hash(db, model_class) -> str:
    last = db.query(model_class).order_by(model_class.id.desc()).first()
    if last is None or not last.entry_hash:
        return GENESIS_HASH
    return last.entry_hash

_EXCLUDED_COLUMNS = {"id", "prev_hash", "entry_hash"}

def extract_hashable_fields(instance) -> dict[str, Any]:
    mapper = instance.__mapper__
    result = {}
    for col in mapper.columns:
        if col.key in _EXCLUDED_COLUMNS:
            continue
        result[col.key] = getattr(instance, col.key, None)
    return result

def _apply_column_defaults(instance) -> None:
    mapper = instance.__mapper__
    for col in mapper.columns:
        if col.key in _EXCLUDED_COLUMNS:
            continue
        current = getattr(instance, col.key, None)
        if current is not None:
            continue
        default = col.default
        if default is None:
            continue
        arg = getattr(default, "arg", None)
        if arg is None:
            continue
        if callable(arg):
            try:
                value = arg(None)
            except TypeError:
                value = arg()
        else:
            value = arg
        setattr(instance, col.key, value)

def _acquire_chain_lock(db, table_name: str) -> None:
    try:
        from sqlalchemy import text
        lock_key = abs(hash(table_name)) % (2**31)
        db.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": lock_key})
    except Exception:
        pass


def seal_audit_entry(db, instance) -> None:
    _apply_column_defaults(instance)
    _acquire_chain_lock(db, type(instance).__tablename__)
    prev = get_last_entry_hash(db, type(instance))
    fields = extract_hashable_fields(instance)
    instance.prev_hash = prev
    instance.entry_hash = compute_entry_hash(prev, fields)

def verify_chain(entries: Iterable) -> dict:
    expected_prev = GENESIS_HASH
    checked = 0
    for entry in entries:
        checked += 1
        if entry.prev_hash != expected_prev:
            return {
                "valid": False,
                "checked": checked,
                "first_broken_id": getattr(entry, "id", None),
                "reason": f"prev_hash mismatch at id={entry.id}: "
                          f"expected {expected_prev[:16]}..., got "
                          f"{(entry.prev_hash or '')[:16]}...",
            }
        fields = extract_hashable_fields(entry)
        recomputed = compute_entry_hash(entry.prev_hash, fields)
        if recomputed != entry.entry_hash:
            return {
                "valid": False,
                "checked": checked,
                "first_broken_id": getattr(entry, "id", None),
                "reason": f"entry_hash mismatch at id={entry.id}: "
                          f"content has been modified or hash is corrupt",
            }
        expected_prev = entry.entry_hash
    return {
        "valid": True,
        "checked": checked,
        "first_broken_id": None,
        "reason": None,
    }

def verify_table_chain(db, model_class) -> dict:
    entries = db.query(model_class).order_by(model_class.id.asc()).all()
    return verify_chain(entries)
