"""
Migrate intraday_bars — add `date` column, clean schema, rebuild indexes.

Strategy:
  1. Create intraday_bars_v2 with date column + clean PK
  2. Copy data with date extracted from ts (synchronous=OFF for speed)
  3. Drop old table, rename v2 → intraday_bars
  4. Create indexes on the new table

Run:
    python -m pakfindata.db.migrate_intraday          # full migration
    python -m pakfindata.db.migrate_intraday status    # check current state
"""

import sqlite3
import time
import sys
from pathlib import Path

DB_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")

V2_SCHEMA = """
CREATE TABLE IF NOT EXISTS intraday_bars_v2 (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    ts          TEXT NOT NULL,
    ts_epoch    INTEGER NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      REAL,
    PRIMARY KEY (symbol, ts_epoch)
)
"""


def status():
    """Check migration state."""
    con = sqlite3.connect(str(DB_PATH))
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'intraday_bars%'"
    ).fetchall()]
    print(f"Tables: {tables}")

    for tbl in tables:
        cols = [r[1] for r in con.execute(f"PRAGMA table_info({tbl})").fetchall()]
        count = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        has_date = "date" in cols
        print(f"  {tbl}: {count:,} rows, columns={cols}, has_date={has_date}")

        indexes = con.execute(
            f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{tbl}'"
        ).fetchall()
        print(f"    indexes: {[r[0] for r in indexes]}")

    con.close()


def migrate():
    """Run the full migration."""
    con = sqlite3.connect(str(DB_PATH), timeout=60)

    # Check if already migrated
    cols = [r[1] for r in con.execute("PRAGMA table_info(intraday_bars)").fetchall()]
    if "date" in cols:
        pk = con.execute(
            "SELECT sql FROM sqlite_master WHERE name='intraday_bars'"
        ).fetchone()[0]
        if "ts_epoch" in pk and "date TEXT" in pk:
            print("Already migrated — intraday_bars has date column with correct PK.")
            con.close()
            return

    # Check if v2 already exists (partial migration)
    v2_exists = con.execute(
        "SELECT 1 FROM sqlite_master WHERE name='intraday_bars_v2'"
    ).fetchone()

    old_count = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0]
    print(f"Source: intraday_bars — {old_count:,} rows")

    if not v2_exists:
        print("\nStep 1: Creating intraday_bars_v2...")
        con.executescript(V2_SCHEMA)
        con.commit()

        print("Step 2: Copying data — keep max(volume) per (symbol, ts_epoch)...")
        con.execute("PRAGMA synchronous=OFF")
        con.execute("PRAGMA cache_size=-256000")  # 256 MB cache

        t0 = time.time()
        con.execute("""
            INSERT INTO intraday_bars_v2
                (symbol, date, ts, ts_epoch, open, high, low, close, volume)
            SELECT
                symbol,
                SUBSTR(ts, 1, 10),
                ts,
                ts_epoch,
                open, high, low, close, volume
            FROM intraday_bars
            WHERE rowid IN (
                SELECT rowid FROM intraday_bars ib1
                WHERE volume = (
                    SELECT MAX(volume) FROM intraday_bars ib2
                    WHERE ib2.symbol = ib1.symbol AND ib2.ts_epoch = ib1.ts_epoch
                )
                GROUP BY symbol, ts_epoch
            )
        """)
        con.commit()
        t1 = time.time()

        v2_count = con.execute("SELECT COUNT(*) FROM intraday_bars_v2").fetchone()[0]
        dupes = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0] - v2_count
        print(f"  Copied {v2_count:,} rows in {t1-t0:.1f}s ({dupes:,} sub-second dupes removed)")

        con.execute("PRAGMA synchronous=NORMAL")
    else:
        v2_count = con.execute("SELECT COUNT(*) FROM intraday_bars_v2").fetchone()[0]
        print(f"  intraday_bars_v2 exists with {v2_count:,} rows (resuming)")

    print("\nStep 3: Creating indexes on v2...")
    t0 = time.time()
    con.execute("PRAGMA synchronous=OFF")
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_ib2_date ON intraday_bars_v2(date)",
        "CREATE INDEX IF NOT EXISTS idx_ib2_ts ON intraday_bars_v2(ts)",
        "CREATE INDEX IF NOT EXISTS idx_ib2_symbol ON intraday_bars_v2(symbol, date)",
        "CREATE INDEX IF NOT EXISTS idx_ib2_epoch ON intraday_bars_v2(ts_epoch)",
    ]:
        print(f"  {idx_sql.split('idx_ib2_')[1].split(' ON')[0]}...", end="", flush=True)
        con.execute(idx_sql)
        con.commit()
        print(" OK")
    con.execute("PRAGMA synchronous=NORMAL")
    t1 = time.time()
    print(f"  Indexes created in {t1-t0:.1f}s")

    print("\nStep 4: Swapping tables...")
    con.execute("ALTER TABLE intraday_bars RENAME TO intraday_bars_old")
    con.execute("ALTER TABLE intraday_bars_v2 RENAME TO intraday_bars")
    con.commit()
    print("  intraday_bars_old ← old | intraday_bars ← v2")

    # Rename indexes to match new table name
    # SQLite auto-updates index references on RENAME, but names stay idx_ib2_*
    # That's fine — they work correctly.

    print("\nStep 5: Verify...")
    new_count = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0]
    new_cols = [r[1] for r in con.execute("PRAGMA table_info(intraday_bars)").fetchall()]
    print(f"  intraday_bars: {new_count:,} rows, columns={new_cols}")

    # Quick sanity check
    sample = con.execute(
        "SELECT symbol, date, ts, ts_epoch FROM intraday_bars LIMIT 3"
    ).fetchall()
    for r in sample:
        print(f"    {r}")

    print(f"\nDone! Old table kept as intraday_bars_old ({old_count:,} rows).")
    print("Drop it when ready: DROP TABLE intraday_bars_old")

    con.execute("PRAGMA optimize")
    con.close()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "migrate"
    if cmd == "status":
        status()
    elif cmd == "migrate":
        migrate()
    else:
        print("Usage: python -m pakfindata.db.migrate_intraday [migrate|status]")
