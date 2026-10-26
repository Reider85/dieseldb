#!/usr/bin/env python3
"""Deterministic fixture generator for AbstractDieselTest (TESTS-9).

Emits src/test/resources/fixtures/*.csv replicating the exact row formulas
that used to live in AbstractDieselTest.setupCommonTables(). BASE_DATE is a
FIXED constant (not LocalDate.now()) so the committed CSVs stay byte-identical
whenever the script is re-run -- required by the TESTS-9 acceptance criteria.

Formulas (must stay in sync with the SQL path in BulkLoadEquivalenceTest):
  date        = BASE_DATE - i days (ISO yyyy-MM-dd)
  AGE         = 18 + (i % 82)
  BALANCE     = BigDecimal(100 + (i % 9000)) scale 2
  ACTIVE      = TRUE when i % 2 == 0 else FALSE
  PRECISION   = (i % 100) / 10.0, rendered like Java %f (6 decimals)
  AMOUNT      = BigDecimal(50 + (i % 500)) scale 2

Run: python scripts/generate-fixtures.py   (idempotent, byte-identical output)
"""
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

BASE_DATE = date(2026, 1, 1)
ROW_COUNT = 100
OUT_DIR = Path(__file__).resolve().parent.parent / "src" / "test" / "resources" / "fixtures"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    users, profiles, transactions, details = [], [], [], []
    for i in range(1, ROW_COUNT + 1):
        d = (BASE_DATE - timedelta(days=i)).isoformat()
        balance = Decimal(100 + (i % 9000)).quantize(Decimal("0.00"))
        amount = Decimal(50 + (i % 500)).quantize(Decimal("0.00"))
        age = 18 + (i % 82)
        active = "TRUE" if i % 2 == 0 else "FALSE"
        precision = "%.6f" % ((i % 100) / 10.0)
        users.append("%d,CODE%d,User%d,%d,%s,%s,%s,%s" % (i, i, i, age, balance, d, active, precision))
        profiles.append("%d,%d,%d,Profile%d,PCODE%d,Non%d,%s" % (i, i, age, i, i, i, d))
        transactions.append("%d,%d,%s,%s" % (i, i, d, amount))
        details.append("%d,%d,CODE%d,User%d,%d,Info%d,%s" % (i, i, i, i, age, i, balance))
    files = {
        "users.csv": ("ID,USER_CODE,NAME,AGE,BALANCE,DATE_FIELD,ACTIVE,PRECISION", users),
        "profiles.csv": ("PROFILE_ID,USER_ID,PROFILE_AGE,PROFILE_NAME,PROFILE_CODE,NON_INDEXED,PROFILE_DATE", profiles),
        "transactions.csv": ("TRANS_ID,USER_ID,TRANS_DATE,AMOUNT", transactions),
        "user_details.csv": ("DETAIL_ID,USER_ID,USER_CODE,NAME,AGE,INFO,BALANCE", details),
    }
    for name, (header, lines) in files.items():
        path = OUT_DIR / name
        path.write_text("\n".join([header] + lines) + "\n", encoding="utf-8", newline="\n")
        print("wrote %s (%d rows)" % (path, len(lines)))


if __name__ == "__main__":
    main()
