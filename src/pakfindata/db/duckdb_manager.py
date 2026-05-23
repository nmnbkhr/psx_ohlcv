"""DuckDB Manager — DEPRECATED. Use connections.py directly.

All functions re-export from connections.py for backward compatibility.
The pakfindata.duckdb file no longer exists. Data lives in Parquet.
"""

from pathlib import Path

from pakfindata.db.connections import (
    analytics_con, duck, duck_fetchone, has_duckdb,
    sqlite_con, sqlite_query, jsonl,
    PARQUET_ROOT, JSONL_DIR,
)

# Legacy path constants — some modules import these
DUCKDB_PATH = PARQUET_ROOT  # no longer a .duckdb file
SQLITE_PSX_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")
SQLITE_TICK_PATH = Path("/home/smnb/psxdata_rescue/tick_bars.db")
JSONL_CLOUD_DIR = JSONL_DIR
JSONL_LOCAL_DIR = Path("/mnt/e/psxdata/tick_logs")
INTRADAY_DIR = Path("/mnt/e/psxdata/intraday")


# Legacy aliases
def get_duck():
    """DEPRECATED -> analytics_con()"""
    return analytics_con()


def get_duck_readonly():
    """DEPRECATED -> analytics_con()"""
    return analytics_con()


def init_duckdb():
    """DEPRECATED — no-op. Schema lives in Parquet files."""
    pass


def migrate_from_sqlite():
    """DEPRECATED — use parquet_store.export_all() instead."""
    from pakfindata.db.parquet_store import export_all
    return export_all()


def sync_incremental():
    """DEPRECATED — use parquet_store.export_today() instead."""
    from pakfindata.db.parquet_store import export_today
    return export_today()


def sync_sqlite_to_duckdb(sqlite_path: str, duckdb_path: str,
                           tables: list, where_clause: str = "") -> dict:
    """DEPRECATED — exports to Parquet instead of DuckDB."""
    from pakfindata.db.parquet_store import export_table
    results = {}
    for table in tables:
        try:
            rows = export_table(table)
            results[table] = rows
        except Exception as e:
            results[table] = 0
    return results


def status():
    """Show Parquet store status."""
    from pakfindata.db.parquet_store import status as pq_status
    info = pq_status()
    print("=" * 50)
    print("  Parquet Store STATUS (replaces DuckDB)")
    print("=" * 50)
    for t, d in info.items():
        print(f"  {t:<20} {d['files']:>5} files  {d['size_mb']:>7.1f} MB")


def query_tick_jsonl(date_str: str, symbol: str = None,
                     columns: str = "*") -> "pd.DataFrame":
    """Query JSONL tick files — delegates to connections.jsonl()."""
    return jsonl(date_str, symbol=symbol, columns=columns)


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "status":
        status()
    elif cmd in ("init", "migrate", "sync"):
        print(f"'{cmd}' is deprecated. Use: python -m pakfindata.db.parquet_store export-today")
    else:
        print("Usage: python -m pakfindata.db.duckdb_manager [status]")
