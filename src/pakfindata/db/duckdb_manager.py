"""
DuckDB manager for pakfindata analytics.

Hybrid architecture:
  - DuckDB: large tables, analytics, JSONL queries
  - SQLite: reference data, configs, small tables (unchanged)

DuckDB file: /mnt/e/psxdata/pakfindata.duckdb
"""

import duckdb
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
SQLITE_PSX_PATH = Path("/mnt/e/psxdata/psx.sqlite")
SQLITE_TICK_PATH = Path("/mnt/e/psxdata/tick_bars.db")
JSONL_CLOUD_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
JSONL_LOCAL_DIR = Path("/mnt/e/psxdata/tick_logs")
INTRADAY_DIR = Path("/mnt/e/psxdata/intraday")

PKT = timezone(timedelta(hours=5))


# ═══════════════════════════════════════════════════════
# CONNECTION MANAGEMENT
# ═══════════════════════════════════════════════════════

def get_duck() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection with optimal settings."""
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET temp_directory = '/tmp/duckdb_tmp'")
    return con


def get_duck_readonly() -> duckdb.DuckDBPyConnection:
    """Read-only connection for Streamlit pages (safe for concurrent access)."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


# ═══════════════════════════════════════════════════════
# SCHEMA CREATION
# ═══════════════════════════════════════════════════════

def init_duckdb():
    """Create DuckDB tables matching actual SQLite schemas. Run once."""
    con = get_duck()

    # ── 5-Second OHLCV Bars (from tick_bars.db ohlcv_5s) ──
    # Actual schema: symbol TEXT, market TEXT, ts TEXT, o/h/l/c REAL, v INT, trades INT
    con.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_5s (
            symbol VARCHAR NOT NULL,
            market VARCHAR NOT NULL,
            ts VARCHAR NOT NULL,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT DEFAULT 0,
            trades INTEGER DEFAULT 0,
            UNIQUE(symbol, market, ts)
        )
    """)

    # ── Index 5-Second Bars (from tick_bars.db index_ohlcv_5s) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_ohlcv_5s (
            symbol VARCHAR NOT NULL,
            ts VARCHAR NOT NULL,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT DEFAULT 0,
            turnover DOUBLE DEFAULT 0,
            UNIQUE(symbol, ts)
        )
    """)

    # ── Index Raw Ticks (from tick_bars.db index_raw_ticks) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_raw_ticks (
            symbol VARCHAR NOT NULL,
            ts DOUBLE,
            value DOUBLE,
            change DOUBLE DEFAULT 0,
            change_pct DOUBLE DEFAULT 0,
            volume BIGINT DEFAULT 0,
            turnover DOUBLE DEFAULT 0,
            UNIQUE(symbol, ts, value)
        )
    """)

    # ── Daily OHLCV (from psx.sqlite eod_ohlcv) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS eod_ohlcv (
            symbol VARCHAR NOT NULL,
            date VARCHAR NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            prev_close DOUBLE,
            sector_code VARCHAR,
            company_name VARCHAR,
            ingested_at VARCHAR NOT NULL,
            source VARCHAR,
            processname VARCHAR,
            turnover DOUBLE,
            PRIMARY KEY (symbol, date)
        )
    """)

    # ── Intraday Bars (from psx.sqlite intraday_bars) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS intraday_bars (
            symbol VARCHAR NOT NULL,
            ts VARCHAR NOT NULL,
            ts_epoch INTEGER NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            interval VARCHAR NOT NULL DEFAULT 'int',
            ingested_at VARCHAR NOT NULL,
            operation VARCHAR NOT NULL DEFAULT 'insert',
            process_ts VARCHAR DEFAULT '',
            PRIMARY KEY (symbol, ts, close)
        )
    """)

    # ── Tick Logs (from psx.sqlite tick_logs) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS tick_logs (
            symbol VARCHAR NOT NULL,
            market VARCHAR NOT NULL,
            timestamp DOUBLE NOT NULL,
            _ts VARCHAR NOT NULL,
            price DOUBLE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            volume BIGINT DEFAULT 0,
            value DOUBLE DEFAULT 0,
            trades INTEGER DEFAULT 0,
            bid DOUBLE DEFAULT 0,
            ask DOUBLE DEFAULT 0,
            bid_vol BIGINT DEFAULT 0,
            ask_vol BIGINT DEFAULT 0,
            prev_close DOUBLE,
            source_file VARCHAR,
            ingested_at VARCHAR NOT NULL,
            PRIMARY KEY (symbol, market, timestamp, price)
        )
    """)

    # ── PSX EOD (from psx.sqlite psx_eod — older EOD data) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS psx_eod (
            symbol VARCHAR NOT NULL,
            timestamp BIGINT,
            open DOUBLE,
            close DOUBLE,
            volume BIGINT,
            source VARCHAR
        )
    """)

    con.close()
    print("DuckDB schema created at", DUCKDB_PATH)


# ═══════════════════════════════════════════════════════
# DATA MIGRATION — SQLite -> DuckDB
# ═══════════════════════════════════════════════════════

def migrate_from_sqlite():
    """One-time migration of large tables from SQLite to DuckDB."""
    con = get_duck()
    con.execute("INSTALL sqlite; LOAD sqlite;")

    print("=" * 50)
    print("  MIGRATING SQLite -> DuckDB")
    print("=" * 50)

    # ── From tick_bars.db ──
    print("\ntick_bars.db:")
    for tbl in ("ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"):
        _migrate_table(con, str(SQLITE_TICK_PATH), tbl, tbl)

    # ── From psx.sqlite ──
    print("\npsx.sqlite:")
    for tbl in ("eod_ohlcv", "intraday_bars", "tick_logs"):
        _migrate_table(con, str(SQLITE_PSX_PATH), tbl, tbl)

    # psx_eod — check actual schema first
    _migrate_table_safe(con, str(SQLITE_PSX_PATH), "psx_eod", "psx_eod")

    con.close()

    size_mb = DUCKDB_PATH.stat().st_size / 1024 / 1024
    print(f"\nMigration complete. DuckDB: {size_mb:.0f} MB")


def _migrate_table(con, source_db: str, source_table: str, target_table: str):
    """Migrate one table from SQLite to DuckDB using INSERT SELECT."""
    alias = f"src_{target_table}"
    try:
        existing = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
        if existing > 0:
            print(f"  {target_table}: {existing:,} rows already present — skipping")
            return

        con.execute(f"ATTACH '{source_db}' AS {alias} (TYPE sqlite, READ_ONLY)")

        count = con.execute(f"SELECT COUNT(*) FROM {alias}.{source_table}").fetchone()[0]
        print(f"  {target_table}: migrating {count:,} rows...", end=" ", flush=True)

        con.execute(f"INSERT INTO {target_table} SELECT * FROM {alias}.{source_table}")
        con.execute(f"DETACH {alias}")
        print("done")

    except Exception as e:
        print(f"  {target_table}: ERROR — {e}")
        try:
            con.execute(f"DETACH {alias}")
        except Exception:
            pass


def _migrate_table_safe(con, source_db: str, source_table: str, target_table: str):
    """Migrate with column-name matching (for tables where schema may differ)."""
    alias = f"src_{target_table}"
    try:
        existing = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
        if existing > 0:
            print(f"  {target_table}: {existing:,} rows already present — skipping")
            return

        con.execute(f"ATTACH '{source_db}' AS {alias} (TYPE sqlite, READ_ONLY)")

        # Get target columns from DuckDB information_schema
        target_cols = [r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{target_table}' AND table_schema = 'main'"
        ).fetchall()]

        # Get source columns via LIMIT 0 trick
        sample = con.execute(f"SELECT * FROM {alias}.{source_table} LIMIT 0").description
        src_cols = [col[0] for col in sample]

        # Use intersection
        common = [c for c in target_cols if c in src_cols]
        if not common:
            print(f"  {target_table}: no common columns — skipping")
            con.execute(f"DETACH {alias}")
            return

        col_list = ", ".join(f'"{c}"' for c in common)
        count = con.execute(f"SELECT COUNT(*) FROM {alias}.{source_table}").fetchone()[0]
        print(f"  {target_table}: migrating {count:,} rows ({len(common)} cols)...", end=" ", flush=True)

        con.execute(f"INSERT INTO {target_table} ({col_list}) SELECT {col_list} FROM {alias}.{source_table}")
        con.execute(f"DETACH {alias}")
        print("done")

    except Exception as e:
        print(f"  {target_table}: ERROR — {e}")
        try:
            con.execute(f"DETACH {alias}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
# INCREMENTAL SYNC — SQLite -> DuckDB (new rows only)
# ═══════════════════════════════════════════════════════

def sync_incremental():
    """Sync new rows from SQLite into DuckDB (ohlcv_5s, index tables, intraday_bars, tick_logs).

    Only inserts rows not already present. Safe to run repeatedly.
    """
    con = get_duck()
    con.execute("INSTALL sqlite; LOAD sqlite;")

    print("=" * 50)
    print("  INCREMENTAL SYNC — SQLite -> DuckDB")
    print("=" * 50)

    # ── tick_bars.db: ohlcv_5s, index_ohlcv_5s, index_raw_ticks ──
    if SQLITE_TICK_PATH.exists():
        print(f"\ntick_bars.db ({SQLITE_TICK_PATH.stat().st_size / 1024 / 1024:.0f} MB):")
        for tbl in ("ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"):
            _sync_table_incremental(con, str(SQLITE_TICK_PATH), tbl, tbl)

    # ── psx.sqlite: intraday_bars, tick_logs ──
    if SQLITE_PSX_PATH.exists():
        print(f"\npsx.sqlite ({SQLITE_PSX_PATH.stat().st_size / 1024 / 1024:.0f} MB):")
        for tbl in ("intraday_bars", "tick_logs"):
            _sync_table_incremental(con, str(SQLITE_PSX_PATH), tbl, tbl)

    con.close()
    print("\nSync complete.")


def _sync_table_incremental(con, source_db: str, source_table: str, target_table: str):
    """Sync new rows from SQLite table into DuckDB using ANTI JOIN on timestamp columns."""
    alias = f"inc_{target_table}"
    try:
        con.execute(f"ATTACH '{source_db}' AS {alias} (TYPE sqlite, READ_ONLY)")

        src_count = con.execute(f"SELECT COUNT(*) FROM {alias}.{source_table}").fetchone()[0]
        dst_count = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]

        if src_count <= dst_count:
            print(f"  {target_table}: DuckDB({dst_count:,}) >= SQLite({src_count:,}) — up to date")
            con.execute(f"DETACH {alias}")
            return

        # Find the max timestamp-like column in DuckDB to only insert newer rows
        # Use INSERT OR IGNORE to skip duplicates
        new_rows = src_count - dst_count
        print(f"  {target_table}: SQLite({src_count:,}) - DuckDB({dst_count:,}) = ~{new_rows:,} new rows...", end=" ", flush=True)

        con.execute(f"""
            INSERT OR IGNORE INTO {target_table}
            SELECT * FROM {alias}.{source_table}
        """)

        final_count = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
        added = final_count - dst_count
        print(f"+{added:,} rows")

        con.execute(f"DETACH {alias}")

    except Exception as e:
        print(f"  {target_table}: ERROR — {e}")
        try:
            con.execute(f"DETACH {alias}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
# FAST SYNC — /tmp/ copy pattern (USB too slow for DuckDB sqlite_scanner)
# ═══════════════════════════════════════════════════════

_TMP_SYNC_DIR = Path("/tmp/pfsync")


def sync_sqlite_to_duckdb(
    sqlite_path: str,
    duckdb_path: str,
    tables: list[str],
    where_clause: str = "",
) -> dict:
    """Fast SQLite → DuckDB sync via /tmp/ copy.

    1. Copy both files to /tmp/ (local FS, bypasses slow USB)
    2. DuckDB ATTACH SQLite + bulk INSERT OR IGNORE
    3. Copy result back to original path

    Returns: {table: rows_added}
    """
    import shutil

    _TMP_SYNC_DIR.mkdir(exist_ok=True)
    tmp_sqlite = _TMP_SYNC_DIR / "source.db"
    tmp_duckdb = _TMP_SYNC_DIR / "target.duckdb"

    # Step 1: copy to /tmp/
    shutil.copy2(sqlite_path, tmp_sqlite)
    shutil.copy2(duckdb_path, tmp_duckdb)

    # Step 2: attach + bulk INSERT
    con = duckdb.connect(str(tmp_duckdb))
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{tmp_sqlite}' AS src (TYPE SQLITE, READ_ONLY)")

    results = {}
    for table in tables:
        before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
        col_list = ", ".join(cols)
        sql = f"INSERT OR IGNORE INTO {table} SELECT {col_list} FROM src.{table}"
        if where_clause:
            sql += f" {where_clause}"
        con.execute(sql)
        after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        results[table] = after - before

    con.execute("DETACH src")
    con.close()

    # Step 3: copy back
    shutil.copy2(tmp_duckdb, duckdb_path)

    # Cleanup
    tmp_sqlite.unlink(missing_ok=True)
    tmp_duckdb.unlink(missing_ok=True)
    for wal in _TMP_SYNC_DIR.glob("*.wal"):
        wal.unlink(missing_ok=True)

    return results


# ═══════════════════════════════════════════════════════
# JSONL DIRECT QUERIES (no import needed)
# ═══════════════════════════════════════════════════════

def query_tick_jsonl(date_str: str, symbol: str = None,
                     columns: str = "*") -> pd.DataFrame:
    """Query cloud JSONL tick files directly with DuckDB. 50-100x faster than json.loads()."""
    # Cloud files use ticks_YYYY-MM-DD.jsonl naming
    cloud_path = JSONL_CLOUD_DIR / f"ticks_{date_str}.jsonl"
    local_path = JSONL_LOCAL_DIR / f"ticks_{date_str}.jsonl"

    # Also try without prefix
    if not cloud_path.exists():
        cloud_path = JSONL_CLOUD_DIR / f"{date_str}.jsonl"
    if not local_path.exists():
        local_path = JSONL_LOCAL_DIR / f"{date_str}.jsonl"

    path = cloud_path if cloud_path.exists() else local_path
    if not path.exists():
        return pd.DataFrame()

    where = f"WHERE symbol = '{symbol}'" if symbol else ""

    con = duckdb.connect()  # in-memory for read-only
    try:
        df = con.execute(f"""
            SELECT {columns}
            FROM read_json_auto('{path}',
                 format='newline_delimited',
                 maximum_object_size=10485760)
            {where}
            ORDER BY timestamp
        """).df()
    except Exception:
        df = pd.DataFrame()
    con.close()
    return df


def query_tick_jsonl_range(start_date: str, end_date: str,
                           symbol: str = None) -> pd.DataFrame:
    """Query JSONL files across a date range using glob."""
    cloud_pattern = str(JSONL_CLOUD_DIR / "ticks_*.jsonl")

    where_parts = []
    if symbol:
        where_parts.append(f"symbol = '{symbol}'")
    where_parts.append(f"timestamp >= {_date_to_epoch(start_date)}")
    where_parts.append(f"timestamp <= {_date_to_epoch(end_date, end=True)}")
    where = "WHERE " + " AND ".join(where_parts)

    con = duckdb.connect()
    try:
        df = con.execute(f"""
            SELECT *
            FROM read_json_auto('{cloud_pattern}',
                 format='newline_delimited',
                 maximum_object_size=10485760)
            {where}
            ORDER BY timestamp
        """).df()
    except Exception:
        df = pd.DataFrame()
    con.close()
    return df


def query_csv_files(pattern: str, where: str = "") -> pd.DataFrame:
    """Query CSV files directly with DuckDB."""
    con = duckdb.connect()
    try:
        df = con.execute(f"""
            SELECT * FROM read_csv_auto('{pattern}')
            {where}
        """).df()
    except Exception:
        df = pd.DataFrame()
    con.close()
    return df


def _date_to_epoch(date_str: str, end: bool = False) -> float:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=PKT)
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.timestamp()


# ═══════════════════════════════════════════════════════
# ANALYTICS HELPERS
# ═══════════════════════════════════════════════════════

def symbol_daily_stats(symbol: str, days: int = 30) -> pd.DataFrame:
    con = get_duck_readonly()
    df = con.execute(f"""
        SELECT date, open, high, low, close, volume
        FROM eod_ohlcv
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT ?
    """, [symbol, days]).df()
    con.close()
    return df


def batch_scan_all_symbols(date_str: str) -> pd.DataFrame:
    """Scan all symbols at once — single query instead of 500 separate ones."""
    con = get_duck_readonly()
    df = con.execute("""
        SELECT
            symbol,
            COUNT(*) as ticks,
            MIN(c) as low,
            MAX(c) as high,
            FIRST(o) as open,
            LAST(c) as close,
            MAX(v) as volume,
            (LAST(c) - FIRST(o)) / NULLIF(FIRST(o), 0) * 100 as change_pct
        FROM ohlcv_5s
        WHERE ts LIKE ? || '%'
        GROUP BY symbol
        ORDER BY ticks DESC
    """, [date_str]).df()
    con.close()
    return df


def spread_analysis_from_jsonl(date_str: str, symbol: str) -> pd.DataFrame:
    return query_tick_jsonl(
        date_str, symbol,
        columns="""
            timestamp, price, bid, ask,
            (ask - bid) as spread,
            CASE WHEN (bid + ask) > 0
                 THEN (ask - bid) / ((bid + ask) / 2) * 100
                 ELSE 0 END as spread_pct,
            volume
        """
    )


def vwap_from_jsonl(date_str: str, symbol: str) -> pd.DataFrame:
    return query_tick_jsonl(
        date_str, symbol,
        columns="""
            timestamp, price, volume,
            SUM(price * volume) OVER (ORDER BY timestamp) /
            NULLIF(SUM(volume) OVER (ORDER BY timestamp), 0) as vwap
        """
    )


# ═══════════════════════════════════════════════════════
# STATUS
# ═══════════════════════════════════════════════════════

def status():
    print("=" * 50)
    print("  DuckDB STATUS")
    print("=" * 50)

    if not DUCKDB_PATH.exists():
        print("  pakfindata.duckdb does not exist — run init first")
        return

    size_mb = DUCKDB_PATH.stat().st_size / 1024 / 1024
    print(f"  File: {DUCKDB_PATH}")
    print(f"  Size: {size_mb:.0f} MB")

    con = get_duck_readonly()

    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()

    print(f"\n  Tables:")
    for (tbl,) in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"    {tbl:30s} {count:>12,} rows")

    # JSONL files
    jsonl_files = sorted(JSONL_CLOUD_DIR.glob("*.jsonl")) if JSONL_CLOUD_DIR.exists() else []
    print(f"\n  JSONL files (cloud): {len(jsonl_files)}")
    if jsonl_files:
        print(f"    Range: {jsonl_files[0].stem} -> {jsonl_files[-1].stem}")
        total_size = sum(f.stat().st_size for f in jsonl_files)
        print(f"    Total: {total_size / 1024 / 1024:.0f} MB")

    con.close()

    # Comparison
    print(f"\n  Comparison:")
    if SQLITE_PSX_PATH.exists():
        print(f"    psx.sqlite:        {SQLITE_PSX_PATH.stat().st_size / 1024 / 1024:.0f} MB")
    if SQLITE_TICK_PATH.exists():
        print(f"    tick_bars.db:      {SQLITE_TICK_PATH.stat().st_size / 1024 / 1024:.0f} MB")
    print(f"    pakfindata.duckdb: {size_mb:.0f} MB")


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    import time

    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"

    if cmd == "init":
        init_duckdb()
    elif cmd == "migrate":
        init_duckdb()
        migrate_from_sqlite()
    elif cmd == "sync":
        sync_incremental()
    elif cmd == "status":
        status()
    elif cmd == "test":
        print("Testing DuckDB query speed...\n")

        # Test 1: DuckDB GROUP BY
        t0 = time.time()
        con = get_duck_readonly()
        df = con.execute("SELECT symbol, COUNT(*) as cnt FROM ohlcv_5s GROUP BY symbol ORDER BY cnt DESC").df()
        con.close()
        t1 = time.time()
        print(f"  DuckDB GROUP BY all symbols (ohlcv_5s): {t1-t0:.3f}s -> {len(df)} symbols")

        # Test 2: Same on SQLite
        t0 = time.time()
        scon = sqlite3.connect(str(SQLITE_TICK_PATH))
        sdf = pd.read_sql("SELECT symbol, COUNT(*) as cnt FROM ohlcv_5s GROUP BY symbol ORDER BY cnt DESC", scon)
        scon.close()
        t1 = time.time()
        print(f"  SQLite  GROUP BY all symbols (ohlcv_5s): {t1-t0:.3f}s -> {len(sdf)} symbols")

        # Test 3: JSONL direct query
        jsonl_files = sorted(JSONL_CLOUD_DIR.glob("*.jsonl")) if JSONL_CLOUD_DIR.exists() else []
        if jsonl_files:
            date_str = jsonl_files[-1].stem.replace("ticks_", "")
            t0 = time.time()
            jdf = query_tick_jsonl(date_str, "HUBC")
            t1 = time.time()
            print(f"  JSONL query (HUBC, 1 day):               {t1-t0:.3f}s -> {len(jdf)} ticks")

        # Test 4: DuckDB eod_ohlcv scan
        t0 = time.time()
        con = get_duck_readonly()
        df = con.execute("SELECT symbol, COUNT(*) FROM eod_ohlcv GROUP BY symbol").df()
        con.close()
        t1 = time.time()
        print(f"  DuckDB GROUP BY eod_ohlcv:               {t1-t0:.3f}s -> {len(df)} symbols")
    else:
        print("Usage: python -m pakfindata.db.duckdb_manager [init|migrate|sync|status|test]")
