"""
Database connections — single entry point for all DB access.

analytics_con() → in-memory DuckDB with views over Parquet + SQLite
sqlite_con()    → direct SQLite for reference data writes
duck()          → convenience wrapper for analytics queries

THERE IS NO .duckdb FILE. All analytics go through Parquet.
"""

import logging
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger("connections")

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

PARQUET_ROOT = DATA_ROOT / "parquet"
SQLITE_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")
TICK_DB_PATH = Path("/home/smnb/psxdata_rescue/tick_bars.db")
JSONL_DIR = DATA_ROOT / "tick_logs_cloud"


_cached_con: duckdb.DuckDBPyConnection | None = None


class _UnclosableConnection:
    """Wrapper that ignores .close() calls on the cached analytics connection."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def close(self):
        pass  # ignored — connection is cached

    def __getattr__(self, name):
        return getattr(self._con, name)


def _build_analytics_con() -> duckdb.DuckDBPyConnection:
    """Build in-memory DuckDB with views over Parquet + SQLite. Called once."""
    con = duckdb.connect(":memory:")

    # ── Parquet views (analytics tables) ──
    parquet_tables = [
        "ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks",
        "eod_ohlcv", "intraday_bars", "tick_logs", "psx_eod",
        "raw_ticks",
    ]
    created = set()

    for table in parquet_tables:
        table_dir = PARQUET_ROOT / table
        if not table_dir.exists():
            continue
        glob_path = str(table_dir / "*.parquet")
        try:
            con.execute(f"""
                CREATE VIEW {table} AS
                SELECT * FROM read_parquet('{glob_path}',
                    union_by_name=true)
            """)
            created.add(table)
        except Exception as e:
            logger.warning("Parquet view %s failed: %s", table, e)

    # ── SQLite ATTACH (reference tables + fallbacks) ──
    try:
        con.execute("INSTALL sqlite; LOAD sqlite;")
    except Exception:
        pass

    if SQLITE_PATH.exists():
        try:
            con.execute(f"ATTACH '{SQLITE_PATH}' AS sq (TYPE SQLITE, READ_ONLY)")
            # Create views for all SQLite tables not already covered by Parquet
            # Skip tables with partial indexes — they crash DuckDB's SQLite scanner
            sq_tables = set()
            _skip_tables = set()
            try:
                sq_tables = {r[0] for r in con.execute(
                    "SHOW TABLES FROM sq"
                ).fetchall()}
            except Exception:
                pass
            try:
                import sqlite3 as _sq3
                _tmp = _sq3.connect(str(SQLITE_PATH), timeout=5)
                for row in _tmp.execute(
                    "SELECT DISTINCT tbl_name FROM sqlite_master "
                    "WHERE type='index' AND sql LIKE '%WHERE%'"
                ).fetchall():
                    _skip_tables.add(row[0])
                _tmp.close()
            except Exception:
                pass

            for ref in sq_tables:
                if ref not in created and ref not in _skip_tables:
                    try:
                        con.execute(f'CREATE VIEW "{ref}" AS SELECT * FROM sq."{ref}"')
                        created.add(ref)
                    except Exception:
                        pass

            # Fallback: big tables from SQLite if Parquet missing
            for big in ["eod_ohlcv", "intraday_bars", "tick_logs", "psx_eod"]:
                if big not in created and big in sq_tables:
                    try:
                        con.execute(f'CREATE VIEW "{big}" AS SELECT * FROM sq."{big}"')
                        created.add(big)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("SQLite attach failed: %s", e)

    # ── tick_bars.db fallback ──
    if TICK_DB_PATH.exists():
        try:
            con.execute(f"ATTACH '{TICK_DB_PATH}' AS tb (TYPE SQLITE, READ_ONLY)")
            for tick_table in ["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"]:
                if tick_table not in created:
                    try:
                        con.execute(f'CREATE VIEW "{tick_table}" AS SELECT * FROM tb."{tick_table}"')
                    except Exception:
                        pass
        except Exception as e:
            logger.debug("tick_bars.db: %s", e)

    return con


def analytics_con() -> duckdb.DuckDBPyConnection:
    """Cached in-memory DuckDB with views over Parquet + SQLite.

    Built once, reused across calls. Safe because:
      - In-memory (no file locks)
      - Read-only (SQLite ATTACHed as READ_ONLY)
      - Parquet files are append-only (new dates = new files)

    This replaces _duck_con(), get_duck(), get_duck_readonly(),
    and every direct duckdb.connect(pakfindata.duckdb).
    """
    global _cached_con
    if _cached_con is not None:
        try:
            _cached_con.execute("SELECT 1")
            return _UnclosableConnection(_cached_con)
        except Exception:
            _cached_con = None
    _cached_con = _build_analytics_con()
    return _UnclosableConnection(_cached_con)


def refresh_analytics():
    """Force rebuild of the analytics connection (e.g. after Parquet export)."""
    global _cached_con
    if _cached_con is not None:
        try:
            _cached_con.close()
        except Exception:
            pass
    _cached_con = None


def duck(query: str, params=None) -> pd.DataFrame:
    """Run analytics query, return DataFrame. Auto-opens and closes connection."""
    con = analytics_con()
    try:
        if params:
            return con.execute(query, params).df()
        return con.execute(query).df()
    except Exception as e:
        logger.error("Query failed: %s\n%s", e, query[:200])
        return pd.DataFrame()
    finally:
        con.close()


def duck_fetchone(query: str, params=None):
    """Run analytics query, return single row tuple or None."""
    con = analytics_con()
    try:
        if params:
            return con.execute(query, params).fetchone()
        return con.execute(query).fetchone()
    except Exception as e:
        logger.error("Query failed: %s", e)
        return None
    finally:
        con.close()


def has_duckdb() -> bool:
    """Check if analytics data is available (Parquet or SQLite fallback)."""
    try:
        con = analytics_con()
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        con.close()
        return len(tables) > 0
    except Exception:
        return False


def sqlite_con(db: str = "psx") -> sqlite3.Connection:
    """SQLite connection for writes. Caller must close."""
    path = SQLITE_PATH if db == "psx" else TICK_DB_PATH
    con = sqlite3.connect(str(path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def sqlite_query(query: str, params=None, db: str = "psx") -> pd.DataFrame:
    """Read-only SQLite query."""
    con = sqlite_con(db)
    try:
        return pd.read_sql_query(query, con, params=params)
    finally:
        con.close()


def jsonl(date_str: str, symbol: str = None,
          columns: str = "*", where_extra: str = "") -> pd.DataFrame:
    """Query JSONL tick files via in-memory DuckDB."""
    path = JSONL_DIR / f"ticks_{date_str}.jsonl"
    if not path.exists():
        path = JSONL_DIR / f"{date_str}.jsonl"
    if not path.exists():
        path = DATA_ROOT / "tick_logs" / f"ticks_{date_str}.jsonl"
    if not path.exists():
        path = DATA_ROOT / "tick_logs" / f"{date_str}.jsonl"
    if not path.exists():
        return pd.DataFrame()

    where_parts = []
    if symbol:
        where_parts.append(f"symbol = '{symbol}'")
    if where_extra:
        where_parts.append(where_extra)
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    con = duckdb.connect(":memory:")
    try:
        return con.execute(f"""
            SELECT {columns}
            FROM read_json_auto('{path}',
                 format='newline_delimited',
                 maximum_object_size=10485760)
            {where}
            ORDER BY timestamp
        """).df()
    except Exception:
        return pd.DataFrame()
    finally:
        con.close()


# Backwards-compat aliases for callers that import these names.
# They all route through analytics_con() now — no .duckdb file involved.
DUCKDB_PATH = PARQUET_ROOT  # some modules reference this; point to parquet dir
JSONL_CLOUD_DIR = JSONL_DIR
JSONL_LOCAL_DIR = DATA_ROOT / "tick_logs"


def _duck_con():
    """Backwards compat — returns analytics_con() instead of cached singleton."""
    return analytics_con()


def duck_write():
    """Backwards compat — no-op, returns analytics_con(). Writes go to SQLite now."""
    return analytics_con()


def duck_insert(table: str, df: pd.DataFrame) -> int:
    """Backwards compat — no-op. DuckDB writes removed; data flows through SQLite."""
    return 0


def duck_insert_columns(table: str, df: pd.DataFrame, columns: list) -> int:
    """Backwards compat — no-op. DuckDB writes removed; data flows through SQLite."""
    return 0


def duck_execute(sql: str, params=None):
    """Backwards compat — no-op for writes."""
    pass


def get_available_tick_dates() -> list[str]:
    """Get all dates that have tick data (Parquet + JSONL). Newest first."""
    dates = set()
    # From Parquet tick_logs
    tl_dir = PARQUET_ROOT / "tick_logs"
    if tl_dir.exists():
        for f in tl_dir.glob("*.parquet"):
            stem = f.stem
            if len(stem) == 10 and stem[4] == "-":
                dates.add(stem)
    # From JSONL files
    for d in (JSONL_DIR, DATA_ROOT / "tick_logs"):
        if not d.exists():
            continue
        for f in d.glob("ticks_*.jsonl"):
            if "deduped" in f.name:
                continue
            stem = f.stem.replace("ticks_", "")
            if len(stem) == 10 and stem[4] == "-":
                dates.add(stem)
    return sorted(dates, reverse=True)


def find_jsonl(date_str: str) -> Path | None:
    """Find JSONL file for a date."""
    for base_dir in (JSONL_DIR, DATA_ROOT / "tick_logs"):
        if not base_dir.exists():
            continue
        for pattern in (f"ticks_{date_str}.jsonl", f"{date_str}.jsonl"):
            path = base_dir / pattern
            if path.exists() and path.stat().st_size > 100:
                return path
    return None


def load_ticks(symbol: str, date_str: str = None,
               columns: str = "*", limit: int = None) -> pd.DataFrame:
    """Universal tick data loader — Parquet primary, JSONL fallback."""
    from datetime import datetime, timedelta, timezone
    PKT = timezone(timedelta(hours=5))

    if date_str is None:
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")

    limit_clause = f"LIMIT {limit}" if limit else ""

    # Attempt 1: Parquet tick_logs
    try:
        con = analytics_con()
        df = con.execute(f"""
            SELECT {columns} FROM tick_logs
            WHERE symbol = ? AND date = ?
            ORDER BY timestamp {limit_clause}
        """, [symbol, date_str]).df()
        con.close()
        if not df.empty:
            df["_source"] = "parquet"
            return df
    except Exception:
        pass

    # Attempt 2: JSONL
    jsonl_path = find_jsonl(date_str)
    if jsonl_path:
        try:
            _con = duckdb.connect(":memory:")
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

    return pd.DataFrame()
