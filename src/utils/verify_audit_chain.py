from __future__ import annotations

import argparse
import sys

def _verify_marketplace() -> dict:
    from src.models.database import SessionLocal, AuditLog
    from src.utils.audit_chain import verify_table_chain

    db = SessionLocal()
    try:
        return verify_table_chain(db, AuditLog)
    finally:
        db.close()

def _verify_seller() -> dict:
    from src.seller.database import SellerSessionLocal, SellerAuditLog
    from src.utils.audit_chain import verify_table_chain

    db = SellerSessionLocal()
    try:
        return verify_table_chain(db, SellerAuditLog)
    finally:
        db.close()

def _print_result(scope: str, table: str, result: dict) -> int:
    if result["valid"]:
        print(f"[{scope}] {table}: VALID  (checked {result['checked']} entries)")
        return 0
    print(
        f"[{scope}] {table}: BROKEN at id={result['first_broken_id']} "
        f"(checked {result['checked']} entries)"
    )
    print(f"   reason: {result['reason']}")
    return 1

def main() -> None:
    p = argparse.ArgumentParser(description="Verify audit-log hash chains.")
    p.add_argument("--marketplace", action="store_true", help="Verify marketplace AuditLog only")
    p.add_argument("--seller", action="store_true", help="Verify seller SellerAuditLog only")
    args = p.parse_args()

    both = not (args.marketplace or args.seller)
    exit_code = 0

    if args.marketplace or both:
        try:
            res = _verify_marketplace()
            exit_code |= _print_result("marketplace", "audit_logs", res)
        except Exception as e:
            print(f"[marketplace] audit_logs: ERROR - {e}")
            exit_code |= 1

    if args.seller or both:
        try:
            res = _verify_seller()
            exit_code |= _print_result("seller", "seller_audit_logs", res)
        except Exception as e:
            print(f"[seller] seller_audit_logs: ERROR - {e}")
            exit_code |= 1

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
