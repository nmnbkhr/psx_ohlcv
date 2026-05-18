#!/usr/bin/env python3
"""Backfill data_freshness catalog for every known dataset.

One-shot, idempotent. Records latest_date + row_count + status='ok' for
every dataset that has a known source table + date column. Skips missing
tables with a warning. Reflects current DB state into the catalog so
sub-wave 2.3 can rewire UI freshness reads against it.

Run:
    python scripts/backfill_data_catalog.py

Re-run is safe — `update_catalog` upserts by dataset_id.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from typing import NamedTuple

from pakfindata.config import get_db_path
from pakfindata.db.catalog import update_catalog
from pakfindata.db.safe_writer import safe_writer

# Date columns SHOULD store YYYY-MM-DD. Pre-existing data pollution in some
# tables (forex_kerb 'TBILL', konia_daily 'ZUMA', pib_auctions 'ZUMA',
# instrument_membership 'MUFAP', regular_market_current 'WTL') means
# MAX(date_col) returns garbage. Catalog should signal status='partial' for
# those datasets so downstream UI can show a warning instead of trusting
# the garbage value.
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


class Dataset(NamedTuple):
    dataset_id: str       # PK in data_freshness.domain
    source_table: str
    date_column: str      # column to MAX over (or "" for date-less tables)
    source: str           # upstream provider tag
    display_name: str
    ts_substr: bool = False  # True if date_column is a TEXT ts like "2026-05-18 13:45:21"
    unix_epoch: bool = False  # True if date_column is an INTEGER unix seconds


# Canonical dataset registry. Keeps existing migration-v1 seeded dataset_ids
# where they map cleanly to avoid duplicate concepts; adds table-name-based
# dataset_ids for everything else.
DATASETS: list[Dataset] = [
    # ── Pre-seeded 15 (existing logical names from migration v1) ──
    Dataset("equity_eod",       "eod_ohlcv",                "date",            "psx_dps",         "Equity EOD"),
    Dataset("intraday",         "intraday_bars",            "date",            "psx_api",         "Intraday Bars"),
    Dataset("indices",          "psx_indices",              "index_date",      "psx_dps",         "PSX Indices"),
    Dataset("mutual_funds",     "mutual_fund_nav",          "date",            "mufap",           "Mutual Funds"),
    Dataset("treasury",         "tbill_auctions",           "auction_date",    "sbp",             "T-Bill Auctions"),
    Dataset("pib",              "pib_auctions",             "auction_date",    "sbp",             "PIB Auctions"),
    Dataset("kibor",            "kibor_daily",              "date",            "sbp_easydata",    "KIBOR Rates"),
    Dataset("fx_interbank",     "sbp_fx_interbank",         "date",            "sbp_easydata",    "FX Interbank"),
    Dataset("fx_kerb",          "forex_kerb",               "date",            "forex_pk",        "FX Kerb"),
    Dataset("yield_curve",      "pkrv_daily",               "date",            "mufap",           "Yield Curve PKRV"),
    Dataset("sukuk",            "sukuk_quotes",             "quote_date",      "sbp",             "Sukuk Quotes"),
    Dataset("etf",              "etf_nav",                  "date",            "psx_dps",         "ETF NAV"),
    Dataset("commodities",      "commodity_prices",         "date",            "pmex",            "Commodities"),
    Dataset("company_profile",  "company_profile",          "updated_at",      "psx_dps",         "Company Profiles"),
    Dataset("announcements",    "corporate_announcements",  "announcement_date","psx_dps",        "Announcements"),
    # ── Additional datasets surfaced in plan + audit ──
    Dataset("konia",                    "konia_daily",              "date",            "sbp",                "KONIA O/N Rate"),
    Dataset("sbp_policy_rates",         "sbp_policy_rates",         "rate_date",       "sbp_easydata",       "SBP Policy Rate"),
    Dataset("gis_auctions",             "gis_auctions",             "auction_date",    "sbp",                "GIS Sukuk Auctions"),
    Dataset("sovereign_curve",          "sovereign_curve",          "date",            "computed",           "Sovereign Yield Curve"),
    Dataset("benchmark_snapshot",       "sbp_benchmark_snapshot",   "date",            "sbp_bond_market",    "SBP Benchmark Snapshot"),
    Dataset("instrument_membership",    "instrument_membership",    "effective_date",  "psx_dps",            "Index Membership"),
    Dataset("tick_data",                "tick_data",                "timestamp",       "psx_api",            "Tick Data", False, True),
    Dataset("intraday_daily_summary",   "intraday_daily_summary",   "date",            "computed",           "Intraday Daily Summary"),
    Dataset("regular_market_current",   "regular_market_current",   "ts",              "psx_api",            "Regular Market Current", True),
    Dataset("regular_market_snapshots", "regular_market_snapshots", "ts",              "psx_api",            "Regular Market Snapshots", True),
    Dataset("post_close_turnover",      "post_close_turnover",      "date",            "psx_dps",            "Post-Close Turnover"),
    Dataset("pkisrv",                   "pkisrv_daily",             "date",            "mufap",              "PKISRV Yield Curve"),
    Dataset("pkfrv",                    "pkfrv_daily",              "date",            "mufap",              "PKFRV Pricing"),
    Dataset("eod_symbol_summary",       "eod_symbol_summary",       "date",            "computed",           "EOD Symbol Summary"),
    Dataset("eod_market_summary",       "eod_market_summary",       "date",            "computed",           "EOD Market Summary"),
    Dataset("eod_sector_summary",       "eod_sector_summary",       "date",            "computed",           "EOD Sector Summary"),
]


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,)
    ).fetchone()
    return row is not None


def _query_freshness(
    con: sqlite3.Connection, ds: Dataset
) -> tuple[str | None, int]:
    """Return (latest_date, row_count) for the dataset's source table."""
    if ds.unix_epoch:
        # INTEGER unix seconds — convert via SQLite's unixepoch modifier
        sql = f"SELECT date(MAX({ds.date_column}), 'unixepoch'), COUNT(*) FROM {ds.source_table}"
    elif ds.ts_substr:
        # ts columns like "2026-05-18 13:45:21" — extract date prefix
        sql = f"SELECT SUBSTR(MAX({ds.date_column}), 1, 10), COUNT(*) FROM {ds.source_table}"
    else:
        sql = f"SELECT MAX({ds.date_column}), COUNT(*) FROM {ds.source_table}"
    latest, count = con.execute(sql).fetchone()
    return latest, int(count or 0)


def main() -> int:
    db_path = get_db_path()
    print(f"Backfilling data_freshness catalog at {db_path}\n")

    # Open a read connection first to inspect tables + compute freshness.
    # Cache results, then do a single safe_writer pass for the writes.
    read_con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    read_con.row_factory = sqlite3.Row

    results: list[tuple[Dataset, str | None, int, str, str | None]] = []
    # tuples: (dataset, latest_date, row_count, status, note)

    missing: list[Dataset] = []

    try:
        for ds in DATASETS:
            if not _table_exists(read_con, ds.source_table):
                missing.append(ds)
                continue
            try:
                latest, count = _query_freshness(read_con, ds)
                status = "ok"
                note = None
                if count == 0:
                    note = "empty table"
                elif latest is not None and not _DATE_RE.match(str(latest)):
                    # MAX returned a non-date string — pre-existing pollution
                    # in the source table. Surface as partial so UI shows
                    # warning instead of displaying garbage.
                    status = "partial"
                    note = f"date column contains non-date values (MAX={latest!r})"
                results.append((ds, latest, count, status, note))
            except sqlite3.OperationalError as e:
                # Column mismatch or similar — log + record as failed
                results.append((ds, None, 0, "failed", f"query error: {e}"))
    finally:
        read_con.close()

    # Single safe_writer block for all writes
    with safe_writer() as con:
        for ds, latest, count, status, note in results:
            update_catalog(
                con,
                ds.dataset_id,
                latest_date=latest,
                row_count=count,
                status=status,
                source=ds.source,
                notes=note,
                source_table=ds.source_table,
                display_name=ds.display_name,
                date_column=ds.date_column,
            )

    # Report
    print(f"{'dataset_id':<30}{'latest_date':<14}{'rows':>10}  status   source")
    print("-" * 78)
    for ds, latest, count, status, note in sorted(results, key=lambda r: r[0].dataset_id):
        latest_display = latest or "—"
        note_display = f"  ({note})" if note else ""
        print(f"{ds.dataset_id:<30}{latest_display:<14}{count:>10}  {status:<8} {ds.source}{note_display}")

    print()
    if missing:
        print(f"Skipped (table missing): {len(missing)}")
        for ds in missing:
            print(f"  - {ds.dataset_id} (table: {ds.source_table})")

    print(f"\nTotal written: {len(results)}")
    print(f"Total skipped: {len(missing)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
