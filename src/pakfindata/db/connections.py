"""
Database connection factory — routes queries to the right engine.

Usage in Streamlit pages:
    from pakfindata.db.connections import duck, sqlite_query, jsonl

    # Fast analytics (DuckDB)
    df = duck("SELECT * FROM eod_ohlcv WHERE symbol = ?", ["HUBC"])

    # Reference data (SQLite — unchanged)
    df = sqlite_query("SELECT * FROM companies WHERE sector = ?", ["Banking"])

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
JSONL_CLOUD_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
JSONL_LOCAL_DIR = Path("/mnt/e/psxdata/tick_logs")


@st.cache_resource
def _duck_con():
    """Shared DuckDB read-only connection for Streamlit."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def duck(query: str, params: list = None) -> pd.DataFrame:
    """Run analytics query on DuckDB. Falls back to empty DataFrame on error."""
    try:
        con = _duck_con()
        if params:
            return con.execute(query, params).df()
        return con.execute(query).df()
    except Exception:
        return pd.DataFrame()


def duck_fetchone(query: str, params: list = None):
    """Run a DuckDB query and return first row as tuple, or None."""
    try:
        con = _duck_con()
        if params:
            return con.execute(query, params).fetchone()
        return con.execute(query).fetchone()
    except Exception:
        return None


def sqlite_query(query: str, params: list = None, db: str = "psx") -> pd.DataFrame:
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
    # Try ticks_YYYY-MM-DD.jsonl naming first
    for prefix in ("ticks_", ""):
        for base_dir in (JSONL_CLOUD_DIR, JSONL_LOCAL_DIR):
            path = base_dir / f"{prefix}{date_str}.jsonl"
            if path.exists():
                break
        else:
            continue
        break
    else:
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
    except Exception:
        df = pd.DataFrame()
    con.close()
    return df


JSONL_DIRS = [
    JSONL_CLOUD_DIR,
    Path.home() / "psxdata" / "tick_logs",
    JSONL_LOCAL_DIR,
]


def find_jsonl(date_str: str) -> Path | None:
    """Find JSONL file for a date — checks all locations + naming patterns."""
    for base_dir in JSONL_DIRS:
        if not base_dir.exists():
            continue
        for pattern in (f"ticks_{date_str}.jsonl", f"{date_str}.jsonl"):
            path = base_dir / pattern
            if path.exists() and path.stat().st_size > 100:
                return path
    return None


def get_available_tick_dates() -> list[str]:
    """Get all dates that have tick data (DuckDB + JSONL). Newest first."""
    dates = set()
    # From DuckDB
    try:
        con = _duck_con()
        rows = con.execute(
            "SELECT DISTINCT SUBSTR(_ts, 1, 10) AS d FROM tick_logs ORDER BY d DESC"
        ).fetchall()
        dates.update(r[0] for r in rows if r[0])
    except Exception:
        pass
    # From JSONL files
    for d in JSONL_DIRS:
        if not d.exists():
            continue
        for f in d.glob("ticks_*.jsonl"):
            if "deduped" in f.name:
                continue
            stem = f.stem.replace("ticks_", "")
            if len(stem) == 10 and stem[4] == "-":
                dates.add(stem)
    return sorted(dates, reverse=True)


def load_ticks(symbol: str, date_str: str = None,
               columns: str = "*", limit: int = None) -> pd.DataFrame:
    """Universal tick data loader — 4-level fallback chain.

    1. DuckDB tick_logs table (fastest)
    2. Cloud JSONL via read_json_auto
    3. Local JSONL via read_json_auto
    4. Most recent date that HAS data

    Always returns a DataFrame. Never raises.
    """
    from datetime import datetime, timedelta, timezone
    PKT = timezone(timedelta(hours=5))

    if date_str is None:
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")

    limit_clause = f"LIMIT {limit}" if limit else ""

    # Attempt 1: DuckDB tick_logs
    try:
        con = _duck_con()
        df = con.execute(f"""
            SELECT {columns} FROM tick_logs
            WHERE symbol = ? AND SUBSTR(_ts, 1, 10) = ?
            ORDER BY timestamp {limit_clause}
        """, [symbol, date_str]).df()
        if not df.empty:
            df["_source"] = "duckdb"
            return df
    except Exception:
        pass

    # Attempt 2: JSONL via DuckDB read_json_auto
    jsonl_path = find_jsonl(date_str)
    if jsonl_path:
        try:
            _con = duckdb.connect()
            df = _con.execute(f"""
                SELECT {columns} FROM read_json_auto('{jsonl_path}',
                     format='newline_delimited', maximum_object_size=10485760)
                WHERE symbol = '{symbol}'
                ORDER BY timestamp {limit_clause}
            """).df()
            _con.close()
            if not df.empty:
                df["_source"] = f"jsonl:{jsonl_path.name}"
                return df
        except Exception:
            pass

    # Attempt 3: Fallback to most recent date with data
    today = datetime.now(PKT).strftime("%Y-%m-%d")
    if date_str == today:
        try:
            con = _duck_con()
            row = con.execute(
                "SELECT MAX(SUBSTR(_ts, 1, 10)) FROM tick_logs WHERE symbol = ?",
                [symbol],
            ).fetchone()
            if row and row[0] and row[0] != date_str:
                fb = row[0]
                df = con.execute(f"""
                    SELECT {columns} FROM tick_logs
                    WHERE symbol = ? AND SUBSTR(_ts, 1, 10) = ?
                    ORDER BY timestamp {limit_clause}
                """, [symbol, fb]).df()
                if not df.empty:
                    df["_source"] = f"duckdb:fallback:{fb}"
                    df["_fallback_date"] = fb
                    return df
        except Exception:
            pass

    return pd.DataFrame()


def has_duckdb() -> bool:
    """Check if DuckDB file exists and is usable."""
    return DUCKDB_PATH.exists()


# ═══════════════════════════════════════════════════════
# WRITE HELPERS (DuckDB primary + SQLite backup)
# ═══════════════════════════════════════════════════════

def duck_write() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection for writes. One writer at a time — close promptly."""
    return duckdb.connect(str(DUCKDB_PATH))


def duck_insert(table: str, df: pd.DataFrame) -> int:
    """Insert a DataFrame into DuckDB table. Ignores primary key conflicts."""
    if df is None or df.empty:
        return 0
    con = duck_write()
    try:
        con.register("_temp_df", df)
        con.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM _temp_df")
        con.unregister("_temp_df")
        count = len(df)
        con.close()
        return count
    except Exception as e:
        con.close()
        raise e


def duck_insert_columns(table: str, df: pd.DataFrame, columns: list[str]) -> int:
    """Insert a DataFrame into specific columns of a DuckDB table."""
    if df is None or df.empty:
        return 0
    con = duck_write()
    try:
        con.register("_temp_df", df)
        col_list = ", ".join(f'"{c}"' for c in columns)
        con.execute(f"INSERT OR IGNORE INTO {table} ({col_list}) SELECT {col_list} FROM _temp_df")
        con.unregister("_temp_df")
        count = len(df)
        con.close()
        return count
    except Exception as e:
        con.close()
        raise e


def duck_execute(sql: str, params: list = None):
    """Execute a write SQL statement on DuckDB."""
    con = duck_write()
    try:
        if params:
            con.execute(sql, params)
        else:
            con.execute(sql)
        con.close()
    except Exception as e:
        con.close()
        raise e
