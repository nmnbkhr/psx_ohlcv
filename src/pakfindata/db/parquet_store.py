"""
Parquet Store — exports SQLite data to daily Parquet files.

Run nightly after market close:
    python -m pakfindata.db.parquet_store export-today
    python -m pakfindata.db.parquet_store export-today --date 2026-04-08

Full re-export:
    python -m pakfindata.db.parquet_store export-all
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger("parquet_store")

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

PARQUET_ROOT = DATA_ROOT / "parquet"
SQLITE_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")
TICK_DB_PATH = Path("/home/smnb/psxdata_rescue/tick_bars.db")
PKT = timezone(timedelta(hours=5))

# Source mapping: parquet_table → (sqlite_db, source_table, date_column, date_type)
# date_type: "date" = already YYYY-MM-DD, "ts_str" = string timestamp prefix
SOURCES = {
    "ohlcv_5s":        (TICK_DB_PATH, "ohlcv_5s",        "ts",   "ts_str"),
    "index_ohlcv_5s":  (TICK_DB_PATH, "index_ohlcv_5s",  "ts",   "ts_str"),
    "index_raw_ticks": (TICK_DB_PATH, "index_raw_ticks",  "ts",   "unix"),
    "raw_ticks":       (TICK_DB_PATH, "raw_ticks",        "ts",   "unix"),
    "eod_ohlcv":       (SQLITE_PATH,  "eod_ohlcv",       "date", "date"),
    "intraday_bars":   (SQLITE_PATH,  "intraday_bars",    "date", "date"),
    "tick_logs":       (SQLITE_PATH,  "tick_logs",        "source_file", "source_file"),
    "psx_eod":         (SQLITE_PATH,  "psx_eod",         None,   "none"),
}


def export_table(table: str, date_str: str = None) -> int:
    """Export one table to Parquet. Returns row count."""
    if table not in SOURCES:
        raise ValueError(f"Unknown table: {table}")

    db_path, src_table, date_col, date_type = SOURCES[table]
    out_dir = PARQUET_ROOT / table
    out_dir.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")

    try:
        if date_type == "date":
            if date_str:
                df = pd.read_sql_query(
                    f"SELECT * FROM [{src_table}] WHERE [{date_col}] = ?",
                    con, params=[date_str])
            else:
                df = pd.read_sql_query(f"SELECT * FROM [{src_table}]", con)

        elif date_type == "ts_str":
            if date_str:
                df = pd.read_sql_query(
                    f"SELECT * FROM [{src_table}] WHERE SUBSTR(CAST({date_col} AS TEXT), 1, 10) = ?",
                    con, params=[date_str])
            else:
                df = pd.read_sql_query(f"SELECT * FROM [{src_table}]", con)

        elif date_type == "unix":
            if date_str:
                df = pd.read_sql_query(
                    f"SELECT * FROM [{src_table}] WHERE DATE(datetime({date_col}, 'unixepoch', '+5 hours')) = ?",
                    con, params=[date_str])
            else:
                df = pd.read_sql_query(f"SELECT * FROM [{src_table}]", con)

        elif date_type == "source_file":
            # tick_logs: derive date from source_file column (ticks_YYYY-MM-DD.jsonl)
            if date_str:
                df = pd.read_sql_query(
                    f"SELECT * FROM [{src_table}] WHERE source_file = ?",
                    con, params=[f"ticks_{date_str}.jsonl"])
                if not df.empty:
                    df["date"] = date_str
            else:
                df = pd.read_sql_query(f"SELECT * FROM [{src_table}]", con)
                if not df.empty and "source_file" in df.columns:
                    df["date"] = df["source_file"].str.extract(r"ticks_(\d{4}-\d{2}-\d{2})")

        elif date_type == "none":
            df = pd.read_sql_query(f"SELECT * FROM [{src_table}]", con)

        else:
            df = pd.DataFrame()

        if df.empty:
            return 0

        if date_str:
            out_path = out_dir / f"{date_str}.parquet"
            df.to_parquet(out_path, index=False)
        else:
            if "date" in df.columns:
                for dt, group in df.groupby("date"):
                    if not dt or str(dt) == "None" or len(str(dt)) < 8:
                        continue
                    (out_dir / f"{dt}.parquet").unlink(missing_ok=True)
                    group.to_parquet(out_dir / f"{dt}.parquet", index=False)
            elif date_type == "ts_str" and date_col in df.columns:
                df["_date"] = df[date_col].astype(str).str[:10]
                for dt, group in df.groupby("_date"):
                    if not dt or len(str(dt)) < 8:
                        continue
                    group = group.drop(columns=["_date"])
                    (out_dir / f"{dt}.parquet").unlink(missing_ok=True)
                    group.to_parquet(out_dir / f"{dt}.parquet", index=False)
            else:
                df.to_parquet(out_dir / "all.parquet", index=False)

        return len(df)
    finally:
        con.close()


def export_today(date_str: str = None) -> dict:
    """Nightly: export today's data for all tables."""
    if not date_str:
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")
    results = {}
    for table in SOURCES:
        try:
            rows = export_table(table, date_str)
            results[table] = rows
            if rows > 0:
                print(f"  {table}: {rows:,} rows for {date_str}")
        except Exception as e:
            results[table] = f"ERROR: {e}"
            print(f"  {table}: {e}")
    return results


def export_all() -> dict:
    """Full re-export — all tables, all dates."""
    results = {}
    for table in SOURCES:
        try:
            rows = export_table(table)
            results[table] = rows
            print(f"  {table}: {rows:,} rows")
        except Exception as e:
            results[table] = f"ERROR: {e}"
            print(f"  {table}: {e}")
    return results


def sync_missing(tables: list[str] | None = None) -> dict:
    """Export missing parquet files — fills gaps between manifest and disk.

    Compares dates in date_manifest (what SQLite has) vs parquet files on disk.
    Exports only the missing dates. Fast — no full table scans.
    """
    from pakfindata.db.date_manifest import get_dates

    if tables is None:
        tables = [t for t in SOURCES if SOURCES[t][3] != "none"]

    results = {}
    for table in tables:
        if table not in SOURCES:
            continue
        db_dates = set(get_dates(table))
        pq_dir = PARQUET_ROOT / table
        pq_dates = set(f.stem for f in pq_dir.glob("*.parquet") if f.stem != "all") if pq_dir.exists() else set()
        missing = sorted(db_dates - pq_dates)

        if not missing:
            results[table] = {"missing": 0, "exported": 0}
            continue

        exported = 0
        for d in missing:
            try:
                rows = export_table(table, d)
                if rows > 0:
                    exported += 1
                    print(f"  {table}/{d}: {rows:,} rows")
            except Exception as e:
                print(f"  {table}/{d}: ERROR {e}")

        results[table] = {"missing": len(missing), "exported": exported}

    return results


def status() -> dict:
    info = {}
    for table in SOURCES:
        d = PARQUET_ROOT / table
        if not d.exists():
            info[table] = {"files": 0, "size_mb": 0}
            continue
        files = list(d.rglob("*.parquet"))
        size = sum(f.stat().st_size for f in files)
        dates = sorted(f.stem for f in files if f.stem != "all")
        info[table] = {
            "files": len(files), "size_mb": round(size / 1048576, 1),
            "oldest": dates[0] if dates else None,
            "newest": dates[-1] if dates else None,
        }
    return info


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["export-all", "export-today", "sync-missing", "status"])
    parser.add_argument("--date", default=None)
    parser.add_argument("--table", default=None)
    args = parser.parse_args()

    if args.command == "export-all":
        if args.table:
            print(f"Exporting {args.table}...")
            rows = export_table(args.table)
            print(f"Done: {rows:,} rows")
        else:
            print("Full export — all tables...")
            export_all()
    elif args.command == "export-today":
        export_today(args.date)
    elif args.command == "sync-missing":
        tables = [args.table] if args.table else None
        print("Syncing missing parquet files...")
        results = sync_missing(tables)
        for t, r in results.items():
            if r["missing"]:
                print(f"  {t}: {r['exported']}/{r['missing']} exported")
            else:
                print(f"  {t}: up to date")
    elif args.command == "status":
        info = status()
        print(f"\n{'Table':<20} {'Files':>6} {'Size':>8} {'Oldest':>12} {'Newest':>12}")
        print("-" * 60)
        for t, d in info.items():
            print(f"{t:<20} {d['files']:>6} {d['size_mb']:>6.1f}MB "
                  f"{d.get('oldest') or '-':>12} {d.get('newest') or '-':>12}")
