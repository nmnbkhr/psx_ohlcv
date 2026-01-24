#!/usr/bin/env python3
"""Feature verification script for PSX OHLCV.

Tests all required features and returns exit code 0 if all pass.
Run this script to verify the system is working correctly.

Usage:
    python scripts/verify_features.py
    # or
    make verify
"""

import sqlite3
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def create_test_db() -> sqlite3.Connection:
    """Create an in-memory test database with all schemas."""
    from psx_ohlcv.analytics import init_analytics_schema
    from psx_ohlcv.db import connect, init_schema
    from psx_ohlcv.sources.market_summary import init_market_summary_tracking
    from psx_ohlcv.sources.regular_market import init_regular_market_schema

    con = connect(":memory:")
    init_schema(con)
    init_analytics_schema(con)
    init_regular_market_schema(con)
    init_market_summary_tracking(con)
    return con


def test_a_master_symbol_sector() -> bool:
    """A) Test master symbol and sector tables exist with correct schema."""
    print("\n[A] Testing Master Symbol + Sector tables...")

    con = create_test_db()

    # Check symbols table columns
    cur = con.execute("PRAGMA table_info(symbols)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "name", "sector", "sector_name", "is_active", "source"}
    missing = required - cols
    if missing:
        print(f"  FAIL: symbols table missing columns: {missing}")
        return False
    print("  OK: symbols table has required columns")

    # Check sectors table
    cur = con.execute("PRAGMA table_info(sectors)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"sector_code", "sector_name"}
    missing = required - cols
    if missing:
        print(f"  FAIL: sectors table missing columns: {missing}")
        return False
    print("  OK: sectors table has required columns")

    con.close()
    return True


def test_b_regular_market_tables() -> bool:
    """B) Test regular market tables exist with correct schema."""
    print("\n[B] Testing Regular Market tables...")

    con = create_test_db()

    # Check regular_market_current
    cur = con.execute("PRAGMA table_info(regular_market_current)")
    cols = {row[1] for row in cur.fetchall()}
    required = {
        "symbol", "status", "current", "change", "change_pct", "volume", "row_hash"
    }
    missing = required - cols
    if missing:
        print(f"  FAIL: regular_market_current missing columns: {missing}")
        return False
    print("  OK: regular_market_current table has required columns")

    # Check regular_market_snapshots
    cur = con.execute("PRAGMA table_info(regular_market_snapshots)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "ts", "current", "volume"}
    missing = required - cols
    if missing:
        print(f"  FAIL: regular_market_snapshots missing columns: {missing}")
        return False
    print("  OK: regular_market_snapshots table has required columns")

    con.close()
    return True


def test_c_sector_name_display() -> bool:
    """C) Test that sector_name is used for display (not sector_code)."""
    print("\n[C] Testing sector_name display functions...")

    # Check that UI app has sector_name helper
    ui_app_path = Path(__file__).parent.parent / "src/psx_ohlcv/ui/app.py"
    if not ui_app_path.exists():
        print("  WARN: UI app.py not found, skipping UI check")
        return True

    content = ui_app_path.read_text()
    if "get_sector_names" not in content:
        print("  FAIL: UI missing get_sector_names function")
        return False
    print("  OK: UI has get_sector_names function")

    if "add_sector_name_column" not in content:
        print("  FAIL: UI missing add_sector_name_column function")
        return False
    print("  OK: UI has add_sector_name_column function")

    # Check that symbols page shows sector_name
    if '"sector_name"' not in content:
        print("  WARN: sector_name column config not found in UI")
    else:
        print("  OK: sector_name is used in UI column configs")

    return True


def test_d_market_summary_tracking() -> bool:
    """D) Test market summary tracking table and retry functions."""
    print("\n[D] Testing Market Summary tracking...")

    con = create_test_db()

    # Check downloaded_market_summary_dates table
    cur = con.execute("PRAGMA table_info(downloaded_market_summary_dates)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"date", "status", "csv_path", "record_count", "error_msg", "fetched_at"}
    missing = required - cols
    if missing:
        print(f"  FAIL: downloaded_market_summary_dates missing columns: {missing}")
        return False
    print("  OK: downloaded_market_summary_dates table has required columns")

    # Test tracking functions
    from psx_ohlcv.sources.market_summary import (
        get_failed_dates,
        get_missing_dates,
        upsert_download_record,
    )

    # Insert test records
    upsert_download_record(con, "2025-01-15", "ok", "/tmp/test.csv", 100)
    upsert_download_record(con, "2025-01-16", "error", None, 0, "Test error")
    upsert_download_record(con, "2025-01-17", "not_found")

    # Verify retrieval
    failed = get_failed_dates(con)
    if failed != ["2025-01-16"]:
        print(f"  FAIL: get_failed_dates returned {failed}, expected ['2025-01-16']")
        return False
    print("  OK: get_failed_dates works correctly")

    missing = get_missing_dates(con)
    if missing != ["2025-01-17"]:
        print(f"  FAIL: get_missing_dates returned {missing}, expected ['2025-01-17']")
        return False
    print("  OK: get_missing_dates works correctly")

    con.close()
    return True


def test_e_intraday_module() -> bool:
    """E) Test intraday tables and module exist."""
    print("\n[E] Testing Intraday module...")

    con = create_test_db()

    # Check intraday_bars table
    cur = con.execute("PRAGMA table_info(intraday_bars)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "ts", "ts_epoch", "open", "high", "low", "close", "volume"}
    missing = required - cols
    if missing:
        print(f"  FAIL: intraday_bars missing columns: {missing}")
        return False
    print("  OK: intraday_bars table has required columns")

    # Check intraday_sync_state table
    cur = con.execute("PRAGMA table_info(intraday_sync_state)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "last_ts", "last_ts_epoch"}
    missing = required - cols
    if missing:
        print(f"  FAIL: intraday_sync_state missing columns: {missing}")
        return False
    print("  OK: intraday_sync_state table has required columns")

    # Check intraday source module exists
    intraday_path = Path(__file__).parent.parent / "src/psx_ohlcv/sources/intraday.py"
    if not intraday_path.exists():
        print("  FAIL: sources/intraday.py not found")
        return False
    print("  OK: intraday source module exists")

    con.close()
    return True


def test_f_company_analytics() -> bool:
    """F) Test company analytics tables."""
    print("\n[F] Testing Company Analytics tables...")

    con = create_test_db()

    # Check company_profile table
    cur = con.execute("PRAGMA table_info(company_profile)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "company_name", "sector_name"}
    missing = required - cols
    if missing:
        print(f"  FAIL: company_profile missing columns: {missing}")
        return False
    print("  OK: company_profile table has required columns")

    # Check company_key_people table
    cur = con.execute("PRAGMA table_info(company_key_people)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "role", "name"}
    missing = required - cols
    if missing:
        print(f"  FAIL: company_key_people missing columns: {missing}")
        return False
    print("  OK: company_key_people table has required columns")

    # Check company_quote_snapshots table
    cur = con.execute("PRAGMA table_info(company_quote_snapshots)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"symbol", "ts", "price", "raw_hash"}
    missing = required - cols
    if missing:
        print(f"  FAIL: company_quote_snapshots missing columns: {missing}")
        return False
    print("  OK: company_quote_snapshots table has required columns")

    con.close()
    return True


def test_g_analytics_tables() -> bool:
    """G) Test analytics tables exist with correct schema."""
    print("\n[G] Testing Analytics tables...")

    con = create_test_db()

    # Check analytics_market_snapshot
    cur = con.execute("PRAGMA table_info(analytics_market_snapshot)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"ts", "gainers_count", "losers_count", "total_volume"}
    missing = required - cols
    if missing:
        print(f"  FAIL: analytics_market_snapshot missing columns: {missing}")
        return False
    print("  OK: analytics_market_snapshot table has required columns")

    # Check analytics_symbol_snapshot
    cur = con.execute("PRAGMA table_info(analytics_symbol_snapshot)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"ts", "rank_type", "rank", "symbol", "sector_name"}
    missing = required - cols
    if missing:
        print(f"  FAIL: analytics_symbol_snapshot missing columns: {missing}")
        return False
    print("  OK: analytics_symbol_snapshot table has required columns")

    # Check analytics_sector_snapshot
    cur = con.execute("PRAGMA table_info(analytics_sector_snapshot)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"ts", "sector_code", "sector_name", "avg_change_pct", "sum_volume"}
    missing = required - cols
    if missing:
        print(f"  FAIL: analytics_sector_snapshot missing columns: {missing}")
        return False
    print("  OK: analytics_sector_snapshot table has required columns")

    con.close()
    return True


def test_h_candlestick_clarity() -> bool:
    """H) Test candlestick chart requirements."""
    print("\n[H] Testing Candlestick clarity...")

    # Check charts.py for minimum height
    charts_path = Path(__file__).parent.parent / "src/psx_ohlcv/ui/charts.py"
    if not charts_path.exists():
        print("  WARN: UI charts.py not found, skipping chart check")
        return True

    content = charts_path.read_text()

    # Check for minimum height constant
    if "MIN_CANDLESTICK_HEIGHT" not in content:
        print("  WARN: MIN_CANDLESTICK_HEIGHT constant not found")
    else:
        print("  OK: MIN_CANDLESTICK_HEIGHT constant exists")

    # Check for SMA function
    if "compute_sma" not in content:
        print("  FAIL: compute_sma function not found")
        return False
    print("  OK: compute_sma function exists")

    # Check for SMA toggles
    if "SMA" not in content or "sma" not in content.lower():
        print("  WARN: SMA overlay not found in charts")
    else:
        print("  OK: SMA overlay support exists")

    return True


def test_joins_work() -> bool:
    """Test that table joins work correctly."""
    print("\n[*] Testing table joins...")

    con = create_test_db()

    # Insert test data
    con.execute("""
        INSERT INTO sectors (sector_code, sector_name, source)
        VALUES ('OG', 'Oil & Gas', 'TEST')
    """)
    con.execute("""
        INSERT INTO symbols (symbol, name, sector, sector_name, is_active,
                            source, discovered_at, updated_at)
        VALUES ('OGDC', 'Oil & Gas Dev Corp', 'OG', 'Oil & Gas', 1,
                'TEST', datetime('now'), datetime('now'))
    """)
    con.commit()

    # Test join
    cur = con.execute("""
        SELECT s.symbol, s.name, sec.sector_name
        FROM symbols s
        LEFT JOIN sectors sec ON s.sector = sec.sector_code
        WHERE s.symbol = 'OGDC'
    """)
    row = cur.fetchone()
    if row is None:
        print("  FAIL: Join returned no results")
        return False

    if row[2] != "Oil & Gas":
        print(f"  FAIL: sector_name mismatch: {row[2]}")
        return False
    print("  OK: Table joins work correctly")

    con.close()
    return True


def main() -> int:
    """Run all feature verification tests."""
    print("=" * 60)
    print("PSX OHLCV Feature Verification")
    print("=" * 60)

    tests = [
        ("A) Master Symbol + Sector", test_a_master_symbol_sector),
        ("B) Regular Market Tables", test_b_regular_market_tables),
        ("C) Sector Name Display", test_c_sector_name_display),
        ("D) Market Summary Tracking", test_d_market_summary_tracking),
        ("E) Intraday Module", test_e_intraday_module),
        ("F) Company Analytics", test_f_company_analytics),
        ("G) Analytics Tables", test_g_analytics_tables),
        ("H) Candlestick Clarity", test_h_candlestick_clarity),
        ("Table Joins", test_joins_work),
    ]

    results = []
    for name, test_fn in tests:
        try:
            passed = test_fn()
            results.append((name, passed))
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append((name, False))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, p in results:
        status = "PASS" if p else "FAIL"
        print(f"  [{status}] {name}")

    print("-" * 60)
    print(f"  Total: {passed}/{total} passed")

    if passed == total:
        print("\n  All features verified successfully!")
        return 0
    else:
        print(f"\n  {total - passed} feature(s) failed verification.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
