"""Process downloaded SBP Excel archives + MUFAP CSVs into sovereign_curve table.

Reads: /mnt/e/psxdata/sbp_rates/archives/*.xlsx + /mnt/e/psxdata/rates/{pkrv,pkisrv}/
Writes: sovereign_curve table in psx.sqlite

Usage:
    python -m pakfindata.sources.sbp_rates_processor process
    python -m pakfindata.sources.sbp_rates_processor status
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger("sbp_rates_processor")
PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

SBP_ROOT = DATA_ROOT / "sbp_rates"
RATES_DIR = DATA_ROOT / "rates"
DB_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")

TENOR_DAYS = {
    "O/N": 1, "1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122,
    "6M": 182, "9M": 274, "12M": 365,
    "2Y": 730, "3Y": 1095, "4Y": 1460, "5Y": 1825,
    "6Y": 2190, "7Y": 2555, "8Y": 2920, "9Y": 3285, "10Y": 3650,
    "15Y": 5475, "20Y": 7300, "25Y": 9125, "30Y": 10950,
}

UPSERT_SQL = """INSERT OR REPLACE INTO sovereign_curve
    (date, source, tenor, days, yield_pct, bid, offer)
    VALUES (?, ?, ?, ?, ?, ?, ?)"""


def _get_con():
    con = sqlite3.connect(str(DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def create_table():
    """Create the unified sovereign yield curve table."""
    con = _get_con()
    con.execute("""
        CREATE TABLE IF NOT EXISTS sovereign_curve (
            date       TEXT    NOT NULL,
            source     TEXT    NOT NULL,
            tenor      TEXT    NOT NULL,
            days       INTEGER NOT NULL,
            yield_pct  REAL    NOT NULL,
            bid        REAL,
            offer      REAL,
            PRIMARY KEY (date, source, tenor)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_sc_date ON sovereign_curve (date)")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sc_source_date ON sovereign_curve (source, date)"
    )
    con.commit()
    con.close()


# ═══════════════════════════════════════════════════════════════════════════════
# SBP T-BILL ARCHIVE (tb.xlsx → New Detailed Format sheet)
# ═══════════════════════════════════════════════════════════════════════════════

def process_tbill_archive() -> int:
    """Parse tb.xlsx → sovereign_curve rows for MTB cutoff yields."""
    xlsx_path = SBP_ROOT / "archives" / "tb.xlsx"
    if not xlsx_path.exists():
        print("  tb.xlsx not found — run sbp_rates_downloader first")
        return 0

    print(f"  Reading {xlsx_path.name} (New Detailed Format)...", end=" ", flush=True)

    df = pd.read_excel(xlsx_path, sheet_name="New Detailed Format", header=None)

    # Find the header row (contains "Auction Date" and "Cut-off Yield")
    header_idx = None
    for i, row in df.iterrows():
        vals = [str(v).strip() for v in row.values if pd.notna(v)]
        joined = " ".join(vals).lower()
        if "auction" in joined and "tenure" in joined:
            header_idx = i
            break

    if header_idx is None:
        print("FAIL — could not find header row")
        return 0

    # Columns: Auction Number, Tenure, ISIN, Auction Date, Issue Date, Maturity Date,
    # Target Amount, ... , Cut-off Yield, ...
    # Col indices (0-based from the New Detailed Format):
    # 0=Auction#, 1=Tenure, 3=Auction Date, 14=Cut-off Price, 15=Cut-off Yield
    tenure_col = 1
    date_col = 3
    yield_col = 15  # "Cut-off Yield" (decimal, e.g. 0.0996 = 9.96%)

    tenure_map = {
        "3-month": "3M", "6-month": "6M", "12-month": "12M",
        "3-Month": "3M", "6-Month": "6M", "12-Month": "12M",
        "3-MONTH": "3M", "6-MONTH": "6M", "12-MONTH": "12M",
    }

    con = _get_con()
    rows_data = []

    for i in range(header_idx + 2, len(df)):  # +2 to skip header + sub-header
        row = df.iloc[i]
        tenure_raw = str(row.iloc[tenure_col]).strip() if pd.notna(row.iloc[tenure_col]) else ""
        date_raw = row.iloc[date_col]
        yield_raw = row.iloc[yield_col]

        tenor = tenure_map.get(tenure_raw)
        if not tenor:
            continue

        if pd.isna(date_raw) or pd.isna(yield_raw):
            continue

        try:
            date_str = pd.Timestamp(date_raw).strftime("%Y-%m-%d")
        except Exception:
            continue

        try:
            raw_val = float(yield_raw)
        except (ValueError, TypeError):
            continue

        # SBP stores yields as decimal fractions (0.0996 = 9.96%)
        if raw_val < 1:
            yield_pct = raw_val * 100
        else:
            yield_pct = raw_val  # already in percent

        if yield_pct < 0.5 or yield_pct > 30:
            continue

        days = TENOR_DAYS.get(tenor, 0)
        rows_data.append((date_str, "MTB", tenor, days, round(yield_pct, 4), None, None))

    if rows_data:
        con.executemany(UPSERT_SQL, rows_data)
        con.commit()

    con.close()
    print(f"OK ({len(rows_data)} rows)")
    return len(rows_data)


# ═══════════════════════════════════════════════════════════════════════════════
# SBP PIB ARCHIVE (Pakinvestbonds.xlsx → New Format sheet)
# ═══════════════════════════════════════════════════════════════════════════════

def process_pib_archive() -> int:
    """Parse Pakinvestbonds.xlsx → sovereign_curve rows for PIB cutoff yields."""
    xlsx_path = SBP_ROOT / "archives" / "Pakinvestbonds.xlsx"
    if not xlsx_path.exists():
        print("  Pakinvestbonds.xlsx not found")
        return 0

    print(f"  Reading {xlsx_path.name} (New Format)...", end=" ", flush=True)

    df = pd.read_excel(xlsx_path, sheet_name="New Format", header=None)

    # Columns: Auction Type, ISIN, Tenor, Issue Date, Maturity Date, Auction Date,
    # Settlement Date, Target Amount, ..., Cut-off Price(18), Cut-off Yield(19)
    # Find header row
    header_idx = None
    for i, row in df.iterrows():
        vals = [str(v).strip().lower() for v in row.values if pd.notna(v)]
        joined = " ".join(vals)
        if "tenor" in joined and "auction" in joined:
            header_idx = i
            break

    if header_idx is None:
        print("FAIL — could not find header row")
        return 0

    tenor_col = 2   # "Tenor" column
    date_col = 5    # "Auction Date"
    yield_col = 19  # "Cut-off Yield"

    tenure_map = {
        "02-year": "2Y", "03-year": "3Y", "05-year": "5Y",
        "10-year": "10Y", "15-year": "15Y", "20-year": "20Y", "30-year": "30Y",
        "02-Year": "2Y", "03-Year": "3Y", "05-Year": "5Y",
        "10-Year": "10Y", "15-Year": "15Y", "20-Year": "20Y", "30-Year": "30Y",
    }

    con = _get_con()
    rows_data = []

    for i in range(header_idx + 2, len(df)):
        row = df.iloc[i]
        tenor_raw = str(row.iloc[tenor_col]).strip() if pd.notna(row.iloc[tenor_col]) else ""
        date_raw = row.iloc[date_col]
        yield_raw = row.iloc[yield_col]

        tenor = tenure_map.get(tenor_raw)
        if not tenor:
            continue

        if pd.isna(date_raw) or pd.isna(yield_raw):
            continue

        try:
            date_str = pd.Timestamp(date_raw).strftime("%Y-%m-%d")
        except Exception:
            continue

        try:
            yield_pct = float(yield_raw)
        except (ValueError, TypeError):
            continue

        # SBP PIB yields are stored as decimals (0.125 = 12.5%)
        if yield_pct < 1:
            yield_pct *= 100

        if yield_pct < 1 or yield_pct > 30:
            continue

        days = TENOR_DAYS.get(tenor, 0)
        rows_data.append((date_str, "PIB", tenor, days, round(yield_pct, 4), None, None))

    if rows_data:
        con.executemany(UPSERT_SQL, rows_data)
        con.commit()

    con.close()
    print(f"OK ({len(rows_data)} rows)")
    return len(rows_data)


# ═══════════════════════════════════════════════════════════════════════════════
# MUFAP PKRV/PKISRV — copy from existing pkrv_daily / pkisrv_daily tables
# ═══════════════════════════════════════════════════════════════════════════════

_MONTHS_TO_TENOR = {
    1: "1M", 2: "2M", 3: "3M", 4: "4M", 6: "6M", 9: "9M",
    12: "12M", 24: "2Y", 36: "3Y", 48: "4Y", 60: "5Y",
    72: "6Y", 84: "7Y", 96: "8Y", 108: "9Y", 120: "10Y",
    180: "15Y", 240: "20Y", 300: "25Y", 360: "30Y",
}


def process_pkrv_from_db() -> int:
    """Copy pkrv_daily → sovereign_curve (source='PKRV')."""
    print("  Copying pkrv_daily → sovereign_curve...", end=" ", flush=True)
    con = _get_con()

    rows = con.execute(
        "SELECT date, tenor_months, yield_pct FROM pkrv_daily"
    ).fetchall()

    data = []
    for date, months, yield_pct in rows:
        tenor = _MONTHS_TO_TENOR.get(months)
        if not tenor:
            continue
        days = TENOR_DAYS.get(tenor, 0)
        data.append((date, "PKRV", tenor, days, yield_pct, None, None))

    if data:
        con.executemany(UPSERT_SQL, data)
        con.commit()

    con.close()
    print(f"OK ({len(data)} rows)")
    return len(data)


def process_pkisrv_from_db() -> int:
    """Copy pkisrv_daily → sovereign_curve (source='PKISRV')."""
    print("  Copying pkisrv_daily → sovereign_curve...", end=" ", flush=True)
    con = _get_con()

    rows = con.execute(
        "SELECT date, tenor, yield_pct FROM pkisrv_daily"
    ).fetchall()

    data = []
    for date, tenor, yield_pct in rows:
        days = TENOR_DAYS.get(tenor, 0)
        if days == 0:
            # Try mapping "1Y" → "12M" etc.
            if tenor == "1Y":
                tenor = "12M"
                days = 365
        data.append((date, "PKISRV", tenor, days, yield_pct, None, None))

    if data:
        con.executemany(UPSERT_SQL, data)
        con.commit()

    con.close()
    print(f"OK ({len(data)} rows)")
    return len(data)


# ═══════════════════════════════════════════════════════════════════════════════
# KIBOR from existing kibor_daily table
# ═══════════════════════════════════════════════════════════════════════════════

def process_kibor_from_db() -> int:
    """Copy kibor_daily → sovereign_curve (source='KIBOR')."""
    print("  Copying kibor_daily → sovereign_curve...", end=" ", flush=True)
    con = _get_con()

    rows = con.execute(
        "SELECT date, tenor, bid, offer FROM kibor_daily"
    ).fetchall()

    data = []
    for date, tenor, bid, offer in rows:
        days = TENOR_DAYS.get(tenor, 0)
        mid = (bid + offer) / 2 if bid and offer else (offer or bid or 0)
        if mid <= 0:
            continue
        data.append((date, "KIBOR", tenor, days, round(mid, 4), bid, offer))

    if data:
        con.executemany(UPSERT_SQL, data)
        con.commit()

    con.close()
    print(f"OK ({len(data)} rows)")
    return len(data)


# ═══════════════════════════════════════════════════════════════════════════════
# SBP SNAPSHOT (live rates from page_snapshot.json)
# ═══════════════════════════════════════════════════════════════════════════════

def process_snapshot() -> int:
    """Load page_snapshot.json into sovereign_curve."""
    snap_path = SBP_ROOT / "page_snapshot.json"
    if not snap_path.exists():
        return 0

    data = json.loads(snap_path.read_text())
    date_str = datetime.now(PKT).strftime("%Y-%m-%d")

    con = _get_con()
    rows_data = []

    if data.get("policy_rate"):
        rows_data.append(
            (date_str, "POLICY", "O/N", 1, data["policy_rate"], None, None)
        )

    for tenor, rates in data.get("kibor", {}).items():
        days = TENOR_DAYS.get(tenor, 0)
        mid = rates.get("offer", 0)
        rows_data.append(
            (date_str, "KIBOR", tenor, days, mid, rates.get("bid"), rates.get("offer"))
        )

    for tenor, yield_pct in data.get("mtb_cutoffs", {}).items():
        days = TENOR_DAYS.get(tenor, 0)
        rows_data.append((date_str, "MTB", tenor, days, yield_pct, None, None))

    for tenor, yield_pct in data.get("pib_cutoffs", {}).items():
        days = TENOR_DAYS.get(tenor, 0)
        rows_data.append((date_str, "PIB", tenor, days, yield_pct, None, None))

    if rows_data:
        con.executemany(UPSERT_SQL, rows_data)
        con.commit()

    con.close()
    return len(rows_data)


# ═══════════════════════════════════════════════════════════════════════════════
# PROCESS ALL
# ═══════════════════════════════════════════════════════════════════════════════

def process_all():
    """Run all processors into sovereign_curve table."""
    create_table()

    print("\n-- SBP Archives --")
    process_tbill_archive()
    process_pib_archive()

    print("\n-- SBP Live Snapshot --")
    n = process_snapshot()
    print(f"  {n} rates from page snapshot")

    print("\n-- Existing DB tables --")
    process_pkrv_from_db()
    process_pkisrv_from_db()
    process_kibor_from_db()

    # Summary
    con = _get_con()
    total = con.execute("SELECT COUNT(*) FROM sovereign_curve").fetchone()[0]
    sources = con.execute(
        "SELECT source, COUNT(*), MIN(date), MAX(date) FROM sovereign_curve GROUP BY source"
    ).fetchall()
    dates = con.execute(
        "SELECT MIN(date), MAX(date), COUNT(DISTINCT date) FROM sovereign_curve"
    ).fetchone()
    con.close()

    print(f"\n-- Summary --")
    print(f"  Total rows: {total:,}")
    print(f"  Date range: {dates[0]} to {dates[1]} ({dates[2]} dates)")
    for src, cnt, mn, mx in sources:
        print(f"    {src:10s}: {cnt:>8,} rows  ({mn} to {mx})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SBP/MUFAP Rates Processor")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("process", help="Process all downloaded files into DB")
    sub.add_parser("status", help="Show DB status")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if args.command == "process":
        process_all()
    elif args.command == "status":
        con = _get_con()
        try:
            total = con.execute("SELECT COUNT(*) FROM sovereign_curve").fetchone()[0]
            print(f"sovereign_curve: {total:,} rows")
            for row in con.execute(
                "SELECT source, COUNT(*), MIN(date), MAX(date) "
                "FROM sovereign_curve GROUP BY source"
            ):
                print(f"  {row[0]:10s}: {row[1]:>8,} rows  ({row[2]} to {row[3]})")
        except Exception:
            print("sovereign_curve table not found — run 'process' first")
        con.close()
    else:
        parser.print_help()
