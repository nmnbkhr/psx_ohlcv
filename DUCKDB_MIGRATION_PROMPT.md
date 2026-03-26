# Claude Code Prompt: DuckDB + SQLite Hybrid Migration

## Context

pakfindata (~/pakfindata/) is a Streamlit multi-page app for Pakistan Stock Exchange analytics.
Currently uses SQLite exclusively — two databases:
- `psx.sqlite` (~2.7 GB) — EOD data, fund NAVs, tick_logs, reference tables
- `tick_bars.db` (~367 MB) — 5-second OHLCV bars, index ticks

**Problem:** SQLite is row-oriented — analytics on millions of rows are painfully slow.
COUNT(*), GROUP BY, JOINs across large tables take 30-60 seconds.

**Solution:** Add DuckDB alongside SQLite. Not a full replacement — a hybrid:
- **SQLite** → keeps reference data, small tables, fund NAVs, signal configs (works fine)
- **DuckDB** → handles all analytics: tick data, bars, scanning, aggregations
- **JSONL direct** → DuckDB queries cloud tick logs without import

## Step 1: Full Audit — What Lives Where

```bash
# All tables in psx.sqlite
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
"

# Row counts (quick probe — cap at 100K)
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table'"); do
    count=$(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM (SELECT 1 FROM [$tbl] LIMIT 100001)" 2>/dev/null)
    echo "$tbl: $count"
done

# All tables in tick_bars.db
sqlite3 /mnt/e/psxdata/tick_bars.db "
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
"

for tbl in $(sqlite3 /mnt/e/psxdata/tick_bars.db "SELECT name FROM sqlite_master WHERE type='table'"); do
    count=$(sqlite3 /mnt/e/psxdata/tick_bars.db "SELECT COUNT(*) FROM (SELECT 1 FROM [$tbl] LIMIT 100001)" 2>/dev/null)
    echo "$tbl: $count"
done

# Schema of large tables
sqlite3 /mnt/e/psxdata/psx.sqlite ".schema daily_ohlcv"
sqlite3 /mnt/e/psxdata/psx.sqlite ".schema tick_logs"
sqlite3 /mnt/e/psxdata/psx.sqlite ".schema intraday_bars"
sqlite3 /mnt/e/psxdata/psx.sqlite ".schema mutual_fund_nav"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema ohlcv_5s"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema index_ohlcv_5s"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema index_raw_ticks"

# Check JSONL files (DuckDB will query these directly)
ls -lh /mnt/e/psxdata/tick_logs_cloud/ | head -10
head -1 /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | head -1

# Check all Python files that use sqlite3 or connect to DB
grep -rn "sqlite3\|\.connect\|psx\.sqlite\|tick_bars" ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | grep -v ".pyc" | sort -t: -k1,1 | uniq
```

**STOP — read ALL output. Map every table to its category before proceeding.**

## Step 2: Install DuckDB

```bash
cd ~/pakfindata
source .venv/bin/activate  # or: conda activate psx
pip install duckdb
python -c "import duckdb; print(f'DuckDB {duckdb.__version__} installed')"
```

## Step 3: Classification — What Goes Where

After reading Step 1 output, classify each table:

### DuckDB (analytics — large tables, aggregation-heavy)

| Table | Source DB | Rows | Why DuckDB |
|-------|----------|------|-----------|
| `ohlcv_5s` | tick_bars.db | 2.1M | Time-series aggregations, resampling |
| `index_ohlcv_5s` | tick_bars.db | large | Index analytics |
| `index_raw_ticks` | tick_bars.db | large | Index intraday |
| `daily_ohlcv` | psx.sqlite | large | 5-year EOD analytics, screening |
| `intraday_bars` | psx.sqlite | 2.6M | Intraday analytics |
| `tick_logs` | psx.sqlite | 2.0M | Signal scoring, dashboard |
| Cloud JSONL | files | unlimited | Query directly — no import |

### SQLite (keep as-is — small tables, config, reference)

| Table | Why stays in SQLite |
|-------|-------------------|
| `mutual_fund_nav` | Fund explorer reads it, works fine |
| `mutual_funds` | Small reference table |
| `fund_risk_metrics` | Small |
| `companies` | Reference data |
| `sectors` | Reference data |
| `signal_configs` | User settings |
| `kibor_daily` | Small time series |
| Any table < 10K rows | Not worth migrating |

### Direct JSONL Query (no import needed)

| Source | Path | Query with |
|--------|------|-----------|
| Cloud tick logs | `/mnt/e/psxdata/tick_logs_cloud/*.jsonl` | `duckdb.sql("SELECT ... FROM read_json_auto('...')")` |
| DPS tick CSVs | `/mnt/e/psxdata/intraday/dps_ticks_*.csv` | `duckdb.sql("SELECT ... FROM read_csv_auto('...')")` |
| PSXT kline CSVs | `/mnt/e/psxdata/intraday/psxt_*.csv` | `duckdb.sql("SELECT ... FROM read_csv_auto('...')")` |

## Step 4: Create DuckDB Database

Create `src/pakfindata/db/duckdb_manager.py`:

```python
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
    
    # Performance settings
    con.execute("SET threads TO 4")          # Use all cores
    con.execute("SET memory_limit = '4GB'")  # Cap RAM usage
    con.execute("SET temp_directory = '/tmp/duckdb_tmp'")
    
    # Install and load extensions
    con.execute("INSTALL json")
    con.execute("LOAD json")
    
    return con


def get_duck_readonly() -> duckdb.DuckDBPyConnection:
    """Read-only connection for Streamlit pages (safe for concurrent access)."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    return con


# ═══════════════════════════════════════════════════════
# SCHEMA CREATION
# ═══════════════════════════════════════════════════════

def init_duckdb():
    """Create DuckDB tables. Run once."""
    con = get_duck()
    
    # ── 5-Second OHLCV Bars ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_5s (
            symbol VARCHAR NOT NULL,
            ts DOUBLE NOT NULL,        -- epoch seconds
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            trades INTEGER,
            date DATE,
            PRIMARY KEY (symbol, ts)
        )
    """)
    
    # ── Index 5-Second Bars ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_ohlcv_5s (
            symbol VARCHAR NOT NULL,
            ts DOUBLE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            date DATE,
            PRIMARY KEY (symbol, ts)
        )
    """)
    
    # ── Index Raw Ticks ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_raw_ticks (
            symbol VARCHAR NOT NULL,
            ts DOUBLE NOT NULL,
            value DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            volume BIGINT,
            date DATE,
            PRIMARY KEY (symbol, ts)
        )
    """)
    
    # ── Daily OHLCV (5-year EOD) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_ohlcv (
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (symbol, date)
        )
    """)
    
    # ── Intraday Bars ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS intraday_bars (
            symbol VARCHAR NOT NULL,
            timestamp BIGINT NOT NULL,
            price DOUBLE,
            volume BIGINT,
            date DATE,
            PRIMARY KEY (symbol, timestamp)
        )
    """)
    
    # ── Tick Logs (for signal dashboard) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS tick_logs (
            symbol VARCHAR NOT NULL,
            timestamp DOUBLE NOT NULL,
            price DOUBLE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            change DOUBLE,
            change_pct DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            bid_vol BIGINT,
            ask_vol BIGINT,
            trades INTEGER,
            value DOUBLE,
            previous_close DOUBLE,
            market VARCHAR,
            ts_iso VARCHAR,
            date DATE
        )
    """)
    
    # ── Market Snapshots (from DPS market-watch poller) ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            symbol VARCHAR NOT NULL,
            timestamp DOUBLE NOT NULL,
            date DATE NOT NULL,
            poll INTEGER,
            ldcp DOUBLE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            current DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            volume BIGINT,
            sector VARCHAR,
            indices VARCHAR,
            PRIMARY KEY (symbol, timestamp)
        )
    """)
    
    con.close()
    print("✅ DuckDB schema created")


# ═══════════════════════════════════════════════════════
# DATA MIGRATION — SQLite → DuckDB
# ═══════════════════════════════════════════════════════

def migrate_from_sqlite():
    """One-time migration of large tables from SQLite to DuckDB."""
    con = get_duck()
    
    print("═══════════════════════════════════════")
    print("  MIGRATING SQLite → DuckDB")
    print("═══════════════════════════════════════")
    
    # ── From tick_bars.db ──
    print("\n📦 tick_bars.db:")
    
    migrate_table(
        con,
        source_db=str(SQLITE_TICK_PATH),
        source_table="ohlcv_5s",
        target_table="ohlcv_5s",
        label="5-second bars"
    )
    
    migrate_table(
        con,
        source_db=str(SQLITE_TICK_PATH),
        source_table="index_ohlcv_5s",
        target_table="index_ohlcv_5s",
        label="Index 5-second bars"
    )
    
    migrate_table(
        con,
        source_db=str(SQLITE_TICK_PATH),
        source_table="index_raw_ticks",
        target_table="index_raw_ticks",
        label="Index raw ticks"
    )
    
    # ── From psx.sqlite ──
    print("\n📦 psx.sqlite:")
    
    migrate_table(
        con,
        source_db=str(SQLITE_PSX_PATH),
        source_table="daily_ohlcv",
        target_table="daily_ohlcv",
        label="Daily OHLCV (5-year)"
    )
    
    migrate_table(
        con,
        source_db=str(SQLITE_PSX_PATH),
        source_table="intraday_bars",
        target_table="intraday_bars",
        label="Intraday bars"
    )
    
    migrate_table(
        con,
        source_db=str(SQLITE_PSX_PATH),
        source_table="tick_logs",
        target_table="tick_logs",
        label="Tick logs"
    )
    
    con.close()
    
    # Report
    print(f"\n✅ Migration complete")
    print(f"   DuckDB: {DUCKDB_PATH}")
    print(f"   Size: {DUCKDB_PATH.stat().st_size / 1024 / 1024:.0f} MB")


def migrate_table(con, source_db: str, source_table: str, 
                  target_table: str, label: str):
    """Migrate one table from SQLite to DuckDB."""
    try:
        # Attach SQLite as a source
        con.execute(f"INSTALL sqlite; LOAD sqlite;")
        
        # Check if target already has data
        existing = con.execute(f"""
            SELECT COUNT(*) FROM {target_table}
        """).fetchone()[0]
        
        if existing > 0:
            print(f"  ⏭️ {label}: {existing:,} rows already in DuckDB — skipping")
            return
        
        # Read from SQLite and insert
        con.execute(f"""
            ATTACH '{source_db}' AS src (TYPE sqlite, READ_ONLY);
        """)
        
        # Check source table exists and get count
        tables = [r[0] for r in con.execute("""
            SELECT name FROM src.sqlite_master WHERE type='table'
        """).fetchall()]
        
        if source_table not in tables:
            print(f"  ⚠️ {label}: table '{source_table}' not found in source — skipping")
            con.execute("DETACH src")
            return
        
        count = con.execute(f"SELECT COUNT(*) FROM src.{source_table}").fetchone()[0]
        print(f"  📥 {label}: migrating {count:,} rows...", end=" ", flush=True)
        
        # Insert — DuckDB handles schema mapping
        con.execute(f"""
            INSERT INTO {target_table} 
            SELECT * FROM src.{source_table}
        """)
        
        con.execute("DETACH src")
        print(f"✅")
        
    except Exception as e:
        print(f"  ❌ {label}: {e}")
        try:
            con.execute("DETACH src")
        except:
            pass


# ═══════════════════════════════════════════════════════
# JSONL DIRECT QUERIES (no import needed)
# ═══════════════════════════════════════════════════════

def query_tick_jsonl(date_str: str, symbol: str = None, 
                     columns: str = "*") -> pd.DataFrame:
    """
    Query cloud JSONL tick files directly with DuckDB.
    No import, no ETL — DuckDB reads JSONL natively.
    
    50-100x faster than Python json.loads() line-by-line.
    """
    cloud_path = JSONL_CLOUD_DIR / f"{date_str}.jsonl"
    local_path = JSONL_LOCAL_DIR / f"{date_str}.jsonl"
    
    path = cloud_path if cloud_path.exists() else local_path
    if not path.exists():
        return pd.DataFrame()
    
    where = f"WHERE symbol = '{symbol}'" if symbol else ""
    
    con = duckdb.connect()  # in-memory for read-only queries
    df = con.execute(f"""
        SELECT {columns}
        FROM read_json_auto('{path}',
             format='newline_delimited',
             maximum_object_size=10485760)
        {where}
        ORDER BY timestamp
    """).df()
    con.close()
    
    return df


def query_tick_jsonl_range(start_date: str, end_date: str, 
                           symbol: str = None) -> pd.DataFrame:
    """Query JSONL files across a date range using glob."""
    # DuckDB can glob multiple files
    cloud_pattern = str(JSONL_CLOUD_DIR / "*.jsonl")
    
    where_parts = []
    if symbol:
        where_parts.append(f"symbol = '{symbol}'")
    where_parts.append(f"timestamp >= {_date_to_epoch(start_date)}")
    where_parts.append(f"timestamp <= {_date_to_epoch(end_date, end=True)}")
    
    where = "WHERE " + " AND ".join(where_parts)
    
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT *
        FROM read_json_auto('{cloud_pattern}',
             format='newline_delimited',
             maximum_object_size=10485760)
        {where}
        ORDER BY timestamp
    """).df()
    con.close()
    
    return df


def query_csv_files(pattern: str, where: str = "") -> pd.DataFrame:
    """Query CSV files directly with DuckDB."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT * FROM read_csv_auto('{pattern}')
        {where}
    """).df()
    con.close()
    return df


def _date_to_epoch(date_str: str, end: bool = False) -> float:
    """Convert date string to epoch for filtering."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=PKT)
    if end:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.timestamp()


# ═══════════════════════════════════════════════════════
# ANALYTICS HELPERS (use DuckDB for speed)
# ═══════════════════════════════════════════════════════

def symbol_daily_stats(symbol: str, days: int = 30) -> pd.DataFrame:
    """Fast daily stats for a symbol from DuckDB."""
    con = get_duck_readonly()
    df = con.execute(f"""
        SELECT date, open, high, low, close, volume
        FROM daily_ohlcv
        WHERE symbol = '{symbol}'
        ORDER BY date DESC
        LIMIT {days}
    """).df()
    con.close()
    return df


def batch_scan_all_symbols(date_str: str) -> pd.DataFrame:
    """
    Scan all symbols at once — single query instead of 500 separate ones.
    This is where DuckDB shines vs SQLite.
    """
    con = get_duck_readonly()
    df = con.execute(f"""
        SELECT 
            symbol,
            COUNT(*) as ticks,
            MIN(close) as low,
            MAX(close) as high,
            FIRST(open) as open,
            LAST(close) as close,
            MAX(volume) as volume,
            (LAST(close) - FIRST(open)) / FIRST(open) * 100 as change_pct
        FROM ohlcv_5s
        WHERE date = '{date_str}'
        GROUP BY symbol
        ORDER BY ticks DESC
    """).df()
    con.close()
    return df


def spread_analysis_from_jsonl(date_str: str, symbol: str) -> pd.DataFrame:
    """Compute bid-ask spread directly from JSONL — no DB needed."""
    return query_tick_jsonl(
        date_str, symbol,
        columns="""
            timestamp,
            price,
            bid,
            ask,
            (ask - bid) as spread,
            CASE WHEN (bid + ask) > 0 
                 THEN (ask - bid) / ((bid + ask) / 2) * 100 
                 ELSE 0 END as spread_pct,
            volume,
            "bidVol" as bid_vol,
            "askVol" as ask_vol
        """
    )


def vwap_from_jsonl(date_str: str, symbol: str) -> pd.DataFrame:
    """Compute running VWAP directly from JSONL."""
    return query_tick_jsonl(
        date_str, symbol,
        columns="""
            timestamp,
            price,
            volume,
            SUM(price * volume) OVER (ORDER BY timestamp) / 
            NULLIF(SUM(volume) OVER (ORDER BY timestamp), 0) as vwap
        """
    )


# ═══════════════════════════════════════════════════════
# STATUS
# ═══════════════════════════════════════════════════════

def status():
    """Print DuckDB status report."""
    print("═══════════════════════════════════════")
    print("  DuckDB STATUS")
    print("═══════════════════════════════════════")
    
    if not DUCKDB_PATH.exists():
        print("  ❌ pakfindata.duckdb does not exist — run init_duckdb() first")
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
    
    # JSONL files available
    jsonl_files = sorted(JSONL_CLOUD_DIR.glob("*.jsonl")) if JSONL_CLOUD_DIR.exists() else []
    print(f"\n  JSONL files (cloud): {len(jsonl_files)}")
    if jsonl_files:
        print(f"    Range: {jsonl_files[0].stem} → {jsonl_files[-1].stem}")
        total_size = sum(f.stat().st_size for f in jsonl_files)
        print(f"    Total: {total_size / 1024 / 1024:.0f} MB")
    
    con.close()
    
    # Compare with SQLite
    print(f"\n  Comparison:")
    print(f"    psx.sqlite:       {SQLITE_PSX_PATH.stat().st_size / 1024 / 1024:.0f} MB")
    print(f"    tick_bars.db:     {SQLITE_TICK_PATH.stat().st_size / 1024 / 1024:.0f} MB")
    print(f"    pakfindata.duckdb: {size_mb:.0f} MB")


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    
    if cmd == "init":
        init_duckdb()
    elif cmd == "migrate":
        init_duckdb()
        migrate_from_sqlite()
    elif cmd == "status":
        status()
    elif cmd == "test":
        # Quick performance test
        print("Testing DuckDB query speed...")
        import time
        
        # Test 1: DuckDB table query
        t0 = time.time()
        con = get_duck_readonly()
        df = con.execute("SELECT symbol, COUNT(*) FROM ohlcv_5s GROUP BY symbol").df()
        con.close()
        t1 = time.time()
        print(f"  GROUP BY all symbols (ohlcv_5s): {t1-t0:.3f}s → {len(df)} symbols")
        
        # Test 2: JSONL direct query
        jsonl_files = sorted(JSONL_CLOUD_DIR.glob("*.jsonl"))
        if jsonl_files:
            t0 = time.time()
            df = query_tick_jsonl(jsonl_files[-1].stem, "HUBC")
            t1 = time.time()
            print(f"  JSONL query (HUBC, 1 day): {t1-t0:.3f}s → {len(df)} ticks")
        
        # Test 3: Compare with SQLite
        t0 = time.time()
        scon = sqlite3.connect(str(SQLITE_TICK_PATH))
        df = pd.read_sql("SELECT symbol, COUNT(*) FROM ohlcv_5s GROUP BY symbol", scon)
        scon.close()
        t1 = time.time()
        print(f"  Same query on SQLite: {t1-t0:.3f}s → {len(df)} symbols")
    else:
        print("Usage: python -m pakfindata.db.duckdb_manager [init|migrate|status|test]")
```

## Step 5: Create Streamlit connection helper

Create `src/pakfindata/db/connections.py`:

```python
"""
Database connection factory — routes queries to the right engine.

Usage in Streamlit pages:
    from pakfindata.db.connections import duck, sqlite, jsonl
    
    # Fast analytics (DuckDB)
    df = duck("SELECT * FROM daily_ohlcv WHERE symbol = ?", ["HUBC"])
    
    # Reference data (SQLite — unchanged)
    df = sqlite("SELECT * FROM companies WHERE sector = ?", ["Banking"])
    
    # Tick data from cloud (DuckDB reads JSONL directly)
    df = jsonl("2026-03-18", symbol="HUBC")
"""

import duckdb
import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
SQLITE_PATH = Path("/mnt/e/psxdata/psx.sqlite")
TICK_DB_PATH = Path("/mnt/e/psxdata/tick_bars.db")
JSONL_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")


@st.cache_resource
def _duck_con():
    """Shared DuckDB read-only connection for Streamlit."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def duck(query: str, params: list = None) -> pd.DataFrame:
    """Run analytics query on DuckDB."""
    con = _duck_con()
    if params:
        return con.execute(query, params).df()
    return con.execute(query).df()


def sqlite(query: str, params: list = None, db: str = "psx") -> pd.DataFrame:
    """Run query on SQLite (reference data, configs)."""
    path = SQLITE_PATH if db == "psx" else TICK_DB_PATH
    con = sqlite3.connect(str(path), timeout=10)
    con.row_factory = sqlite3.Row
    df = pd.read_sql_query(query, con, params=params)
    con.close()
    return df


def jsonl(date_str: str, symbol: str = None, 
          columns: str = "*", where_extra: str = "") -> pd.DataFrame:
    """Query cloud JSONL tick files directly via DuckDB."""
    path = JSONL_DIR / f"{date_str}.jsonl"
    if not path.exists():
        # Try local
        path = Path(f"/mnt/e/psxdata/tick_logs/{date_str}.jsonl")
    if not path.exists():
        return pd.DataFrame()
    
    where_parts = []
    if symbol:
        where_parts.append(f"symbol = '{symbol}'")
    if where_extra:
        where_parts.append(where_extra)
    
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    
    con = duckdb.connect()
    try:
        df = con.execute(f"""
            SELECT {columns}
            FROM read_json_auto('{path}', 
                 format='newline_delimited',
                 maximum_object_size=10485760)
            {where}
            ORDER BY timestamp
        """).df()
    except Exception as e:
        df = pd.DataFrame()
    con.close()
    return df
```

## Step 6: Run Migration

```bash
cd ~/pakfindata
source .venv/bin/activate  # or conda activate psx
export PYTHONPATH=~/pakfindata/src

# Step 1: Create DuckDB schema
python -m pakfindata.db.duckdb_manager init

# Step 2: Migrate data from SQLite
python -m pakfindata.db.duckdb_manager migrate

# Step 3: Check status
python -m pakfindata.db.duckdb_manager status

# Step 4: Run performance test
python -m pakfindata.db.duckdb_manager test
```

## Step 7: Gradual Page Migration

**DO NOT migrate all pages at once.** Start with one page, verify, then next.

### Migration pattern for each page:

```python
# BEFORE (SQLite — slow):
import sqlite3
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")
df = pd.read_sql("SELECT * FROM daily_ohlcv WHERE symbol = 'HUBC'", con)
con.close()

# AFTER (DuckDB — fast):
from pakfindata.db.connections import duck
df = duck("SELECT * FROM daily_ohlcv WHERE symbol = ?", ["HUBC"])

# OR for tick data (JSONL direct — no DB at all):
from pakfindata.db.connections import jsonl
df = jsonl("2026-03-18", symbol="HUBC")
```

### Migration order (by impact):

```
1. tick_analytics.py    — heaviest queries, biggest speed gain
2. signal_score.py      — batch scanning 500 symbols
3. signal_dashboard.py  — reads tick_logs
4. intraday.py          — reads intraday_bars + index ticks
5. live_market.py       — if it queries large tables
6. Other pages          — as needed
```

For each page:
1. Find all `sqlite3.connect` or `pd.read_sql` calls
2. Classify: analytics query → `duck()`, reference query → `sqlite()`, tick query → `jsonl()`
3. Replace
4. Test page
5. Commit

## Step 8: Verify

```bash
# Compare query speeds
python -m pakfindata.db.duckdb_manager test

# Run the Streamlit app
cd ~/pakfindata
streamlit run src/pakfindata/ui/app.py

# Test each migrated page:
# - Tick Analytics → select date + symbol → should load in <1s
# - Signal Scanner → batch scan → should complete in <5s
# - Signal Dashboard → should load in <2s

# Check DuckDB file size (should be much smaller than SQLite)
ls -lh /mnt/e/psxdata/pakfindata.duckdb
ls -lh /mnt/e/psxdata/psx.sqlite
ls -lh /mnt/e/psxdata/tick_bars.db
```

## IMPORTANT NOTES

1. **DuckDB + SQLite coexist** — don't delete SQLite files. Pages not yet 
   migrated still read from SQLite. Migration is gradual.

2. **DuckDB columnar compression** — expect 5-10x smaller files:
   - psx.sqlite 2.7GB → DuckDB ~300-500MB
   - tick_bars.db 367MB → DuckDB ~50-80MB

3. **DuckDB reads JSONL natively** — the `read_json_auto()` function is 
   50-100x faster than Python's `json.loads()` line-by-line. No import step.

4. **Thread safety** — DuckDB supports multiple read-only connections.
   Use `read_only=True` for Streamlit pages. Only one writer at a time.

5. **Streamlit caching** — use `@st.cache_resource` for DuckDB connections,
   `@st.cache_data(ttl=300)` for query results.

6. **No schema changes needed for SQLite** — reference tables, fund NAVs,
   configs all stay in SQLite untouched.

7. **Teradata familiar syntax** — DuckDB supports:
   - `QUALIFY ROW_NUMBER() OVER (...)`
   - `GROUP BY ROLLUP / CUBE / GROUPING SETS`
   - Window functions: `LAG`, `LEAD`, `FIRST_VALUE`, `NTH_VALUE`
   - `SAMPLE` for random sampling
   - `EXCLUDE` in window frames
   These all work exactly like Teradata.

8. **Fallback** — if DuckDB file doesn't exist or query fails, fall back 
   to SQLite. No page should crash because of migration.
