"""Database maintenance utilities for SQLite.

Provides VACUUM, ANALYZE, integrity checks, stats, and hot backup.

Usage as CLI:
    python -m psx_ohlcv.db.maintenance --vacuum --analyze --stats
    python -m psx_ohlcv.db.maintenance --backup
    python -m psx_ohlcv.db.maintenance --integrity
"""

import argparse
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from psx_ohlcv.config import get_db_path


def vacuum_database(con: sqlite3.Connection) -> None:
    """Run VACUUM to reclaim space and defragment the database."""
    con.execute("VACUUM")


def analyze_database(con: sqlite3.Connection) -> None:
    """Run ANALYZE to update query planner statistics."""
    con.execute("ANALYZE")


def get_db_stats(con: sqlite3.Connection) -> dict:
    """Return database statistics.

    Returns:
        Dict with file_size_mb, table_counts, index_count,
        wal_file_size_mb, free_page_count.
    """
    db_path = con.execute("PRAGMA database_list").fetchone()[2]

    # File size
    file_size_mb = 0.0
    if db_path and os.path.exists(db_path):
        file_size_mb = os.path.getsize(db_path) / (1024 * 1024)

    # WAL file size
    wal_path = db_path + "-wal" if db_path else ""
    wal_size_mb = 0.0
    if wal_path and os.path.exists(wal_path):
        wal_size_mb = os.path.getsize(wal_path) / (1024 * 1024)

    # Table row counts
    tables = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_counts = {}
    for row in tables:
        table_name = row[0]
        try:
            count = con.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
            table_counts[table_name] = count
        except sqlite3.Error:
            table_counts[table_name] = -1

    # Index count
    index_count = con.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"
    ).fetchone()[0]

    # Free page count
    free_pages = con.execute("PRAGMA freelist_count").fetchone()[0]

    return {
        "file_size_mb": file_size_mb,
        "wal_file_size_mb": wal_size_mb,
        "table_counts": table_counts,
        "index_count": index_count,
        "free_page_count": free_pages,
    }


def check_integrity(con: sqlite3.Connection) -> tuple[bool, str]:
    """Run PRAGMA integrity_check.

    Returns:
        Tuple of (is_ok, message).
    """
    result = con.execute("PRAGMA integrity_check").fetchone()[0]
    return result == "ok", result


def backup_database(con: sqlite3.Connection, backup_path: str | Path | None = None) -> str:
    """Hot backup using the sqlite3 backup API.

    Args:
        con: Source database connection.
        backup_path: Destination path. Defaults to
            /mnt/e/psxdata/backups/psx_YYYYMMDD.sqlite

    Returns:
        Path to the backup file.
    """
    if backup_path is None:
        backup_dir = Path(get_db_path()).parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d")
        backup_path = backup_dir / f"psx_{timestamp}.sqlite"

    backup_path = str(backup_path)
    dst = sqlite3.connect(backup_path)
    try:
        con.backup(dst)
    finally:
        dst.close()

    return backup_path


def main():
    """CLI entry point for database maintenance."""
    parser = argparse.ArgumentParser(description="PSX OHLCV Database Maintenance")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM to reclaim space")
    parser.add_argument("--analyze", action="store_true", help="Run ANALYZE for query planner")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--integrity", action="store_true", help="Run integrity check")
    parser.add_argument("--backup", action="store_true", help="Create hot backup")
    parser.add_argument("--backup-path", type=str, default=None, help="Custom backup path")
    args = parser.parse_args()

    if not any([args.vacuum, args.analyze, args.stats, args.integrity, args.backup]):
        parser.print_help()
        return

    from psx_ohlcv.db.connection import connect
    con = connect()

    if args.stats:
        stats = get_db_stats(con)
        print(f"DB size: {stats['file_size_mb']:.1f} MB")
        print(f"WAL size: {stats['wal_file_size_mb']:.1f} MB")
        print(f"Indexes: {stats['index_count']}")
        print(f"Free pages: {stats['free_page_count']}")
        print(f"Tables: {len(stats['table_counts'])}")
        for table, count in sorted(stats["table_counts"].items(), key=lambda x: -x[1])[:15]:
            print(f"  {table}: {count:,} rows")

    if args.integrity:
        ok, msg = check_integrity(con)
        print(f"Integrity: {'PASS' if ok else 'FAIL'} — {msg}")

    if args.analyze:
        print("Running ANALYZE...")
        analyze_database(con)
        print("ANALYZE complete.")

    if args.vacuum:
        print("Running VACUUM...")
        vacuum_database(con)
        print("VACUUM complete.")

    if args.backup:
        print("Creating backup...")
        path = backup_database(con, args.backup_path)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"Backup saved: {path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
