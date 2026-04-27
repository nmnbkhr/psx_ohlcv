"""Migrate tick_logs to v2 — tighten PK to catch retransmits.

Old PK: (symbol, market, timestamp, price)
New PK: (symbol, market, timestamp, price, volume, trades)

All columns unchanged. Indexes preserved. No consumer queries affected.

Durability:
  - Creates tick_logs_v2, copies with INSERT OR IGNORE (drops retransmits)
  - Original kept as tick_logs_backup_YYYYMMDD
  - Idempotent (detects if already migrated)

Usage:
    python scripts/migrate_tick_logs_v2.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pakfindata.settings import get_settings  # noqa: E402

DEFAULT_DB = get_settings().db_path
BATCH_SIZE = 100_000

V2_SCHEMA = """
CREATE TABLE IF NOT EXISTS tick_logs_v2 (
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL,
    timestamp    REAL NOT NULL,
    _ts          TEXT NOT NULL,
    price        REAL,
    open         REAL,
    high         REAL,
    low          REAL,
    change       REAL,
    change_pct   REAL,
    volume       INTEGER DEFAULT 0,
    value        REAL DEFAULT 0,
    trades       INTEGER DEFAULT 0,
    bid          REAL DEFAULT 0,
    ask          REAL DEFAULT 0,
    bid_vol      INTEGER DEFAULT 0,
    ask_vol      INTEGER DEFAULT 0,
    prev_close   REAL,
    source_file  TEXT,
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, timestamp, price, volume, trades)
);
"""

V2_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_ts_date_v2    ON tick_logs_v2(_ts, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_sym_market_v2 ON tick_logs_v2(symbol, market, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_market_ts_v2  ON tick_logs_v2(market, _ts)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_source_v2     ON tick_logs_v2(source_file)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_epoch_v2      ON tick_logs_v2(timestamp)",
]

V2_INDEX_NAMES = [
    "idx_tick_logs_ts_date_v2", "idx_tick_logs_sym_market_v2",
    "idx_tick_logs_market_ts_v2", "idx_tick_logs_source_v2",
    "idx_tick_logs_epoch_v2",
]

CANONICAL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_ts_date    ON tick_logs(_ts, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_sym_market ON tick_logs(symbol, market, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_market_ts  ON tick_logs(market, _ts)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_source     ON tick_logs(source_file)",
    "CREATE INDEX IF NOT EXISTS idx_tick_logs_epoch      ON tick_logs(timestamp)",
]


def already_migrated(con: sqlite3.Connection) -> bool:
    """Detect if PK already includes (volume, trades)."""
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='tick_logs'"
    ).fetchone()
    if not row:
        return False
    sql = row[0] or ""
    # Look for the wider PK
    return "price, volume, trades" in sql.replace("  ", " ").replace("\n", " ")


def apply_pragmas(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-524288")
    con.execute("PRAGMA busy_timeout=60000")


def migrate(db_path: str, dry_run: bool = False) -> int:
    db = Path(db_path)
    if not db.exists():
        print(f"[ERROR] DB not found: {db_path}")
        return 1

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    apply_pragmas(con)

    # Check tick_logs exists
    exists = con.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='tick_logs'"
    ).fetchone()[0]
    if not exists:
        print("[SKIP] tick_logs table does not exist")
        con.close()
        return 2

    if already_migrated(con):
        print("[SKIP] tick_logs already on v2 schema (PK includes volume, trades)")
        con.close()
        return 2

    total = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
    print(f"[INFO] Source tick_logs: {total:,} rows")

    if dry_run:
        # Check how many would collapse with tighter PK
        unique_new = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT symbol, market, timestamp, price, volume, trades
                FROM tick_logs
            )
        """).fetchone()[0]
        print(f"[DRY-RUN] unique under new PK: {unique_new:,}")
        print(f"[DRY-RUN] retransmits to drop: {total - unique_new}")
        con.close()
        return 0

    # Create v2
    print("[STEP 1/3] Creating tick_logs_v2...")
    con.execute("DROP TABLE IF EXISTS tick_logs_v2")
    con.executescript(V2_SCHEMA)

    # Copy — INSERT OR IGNORE silently drops retransmits (same PK)
    print("[STEP 2/3] Copying with OR IGNORE (drops retransmits)...")
    t0 = time.time()
    con.execute("""
        INSERT OR IGNORE INTO tick_logs_v2
        SELECT * FROM tick_logs
    """)
    con.commit()

    v2_count = con.execute("SELECT COUNT(*) FROM tick_logs_v2").fetchone()[0]
    dropped = total - v2_count
    elapsed = time.time() - t0
    print(f"  Copied {v2_count:,} rows, dropped {dropped:,} retransmits ({elapsed:.1f}s)")

    if v2_count < total * 0.99:
        print(f"[WARN] Dropped {dropped/total*100:.2f}% — higher than expected. Investigate before swap.")
        # But continue — user explicitly asked for this tightening

    # Indexes
    print("[STEP 3/3] Building indexes and swapping tables...")
    for idx in V2_INDEXES:
        con.execute(idx)
    con.commit()

    backup_name = f"tick_logs_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    old_indexes = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='tick_logs' AND sql IS NOT NULL"
    )]

    con.execute("BEGIN")
    try:
        for idx_name in old_indexes:
            con.execute(f"DROP INDEX IF EXISTS {idx_name}")
        con.execute(f"ALTER TABLE tick_logs RENAME TO {backup_name}")
        con.execute("ALTER TABLE tick_logs_v2 RENAME TO tick_logs")
        # Drop _v2 suffix indexes (they moved with the table) then create canonical names
        for idx_name in V2_INDEX_NAMES:
            con.execute(f"DROP INDEX IF EXISTS {idx_name}")
        # Use individual execute calls (not executescript, which auto-commits)
        for idx_sql in CANONICAL_INDEXES:
            con.execute(idx_sql)
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"[ERROR] Swap failed: {e}")
        con.close()
        return 1

    print(f"\n[SUCCESS] tick_logs migrated to v2 (tighter PK)")
    print(f"  Backup: {backup_name}  (drop when verified)")
    con.close()
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    return migrate(args.db, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
