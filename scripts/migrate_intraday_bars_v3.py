"""Migrate intraday_bars to v3 schema.

Changes:
  - Add: market, interval, value, trade_count, vwap, source, ingested_at
  - New PK: (symbol, market, ts_epoch, interval)
  - Backfill market from symbol-pattern inference

Durability safeguards:
  - WAL mode + large temp_store for fast bulk copy
  - Creates intraday_bars_v3 as new table, copies in batches, swaps atomically
  - Original kept as intraday_bars_backup_YYYYMMDD
  - Idempotent: detects if already migrated
  - Verifies row counts before swap

Usage:
    python scripts/migrate_intraday_bars_v3.py [--dry-run]
    python scripts/migrate_intraday_bars_v3.py --db /path/to/psx.sqlite
    # default --db is settings.db_path (env: PSX_DB_PATH)
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from pakfindata.db.market_inference import infer_market  # noqa: E402
from pakfindata.settings import get_settings  # noqa: E402

DEFAULT_DB = get_settings().db_path
BATCH_SIZE = 50_000

V3_SCHEMA = """
CREATE TABLE IF NOT EXISTS intraday_bars_v3 (
    symbol       TEXT NOT NULL,
    market       TEXT NOT NULL DEFAULT 'REG',
    date         TEXT NOT NULL,
    ts           TEXT NOT NULL,
    ts_epoch     INTEGER NOT NULL,
    interval     TEXT NOT NULL DEFAULT '1s',
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       REAL DEFAULT 0,
    value        REAL DEFAULT 0,
    trade_count  INTEGER DEFAULT 0,
    vwap         REAL,
    source       TEXT DEFAULT 'legacy',
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, market, ts_epoch, interval)
);
"""

# Indexes are created during the copy with _v3 suffix to avoid name collision
# with legacy indexes. After table swap, these are renamed to canonical names.
V3_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ib_date_symbol_v3      ON intraday_bars_v3(date, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_ib_symbol_ts_v3        ON intraday_bars_v3(symbol, ts_epoch)",
    "CREATE INDEX IF NOT EXISTS idx_ib_market_date_v3      ON intraday_bars_v3(market, date)",
    "CREATE INDEX IF NOT EXISTS idx_ib_ts_epoch_v3         ON intraday_bars_v3(ts_epoch)",
    "CREATE INDEX IF NOT EXISTS idx_ib_symbol_interval_v3  ON intraday_bars_v3(symbol, interval, ts_epoch)",
]

# After swap, drop _v3 suffix indexes and create canonical ones.
CANONICAL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ib_date_symbol     ON intraday_bars(date, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_ib_symbol_ts       ON intraday_bars(symbol, ts_epoch)",
    "CREATE INDEX IF NOT EXISTS idx_ib_market_date     ON intraday_bars(market, date)",
    "CREATE INDEX IF NOT EXISTS idx_intraday_bars_ts_epoch ON intraday_bars(ts_epoch)",
    "CREATE INDEX IF NOT EXISTS idx_ib_symbol_interval ON intraday_bars(symbol, interval, ts_epoch)",
]
V3_INDEX_NAMES = [
    "idx_ib_date_symbol_v3", "idx_ib_symbol_ts_v3", "idx_ib_market_date_v3",
    "idx_ib_ts_epoch_v3", "idx_ib_symbol_interval_v3",
]


def is_v3_schema(con: sqlite3.Connection) -> bool:
    """Check if intraday_bars is already on v3 schema."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(intraday_bars)")}
    return {"market", "interval", "trade_count"}.issubset(cols)


def apply_pragmas(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-524288")  # 512MB
    con.execute("PRAGMA busy_timeout=60000")


def build_market_cache(con: sqlite3.Connection) -> dict[str, str]:
    """Pre-compute market for every distinct symbol (one-time cost)."""
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM intraday_bars")]
    return {s: infer_market(s) for s in syms}


def migrate(db_path: str, dry_run: bool = False) -> int:
    """Run migration. Returns exit code: 0=success, 1=error, 2=already migrated."""
    db = Path(db_path)
    if not db.exists():
        print(f"[ERROR] DB not found: {db_path}")
        return 1

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    apply_pragmas(con)

    # Idempotency check
    if is_v3_schema(con):
        print("[SKIP] intraday_bars already on v3 schema (has market, interval, trade_count)")
        con.close()
        return 2

    # Row count for progress
    total = con.execute("SELECT COUNT(*) FROM intraday_bars").fetchone()[0]
    print(f"[INFO] Source intraday_bars: {total:,} rows")

    if total == 0:
        print("[INFO] Empty source — creating v3 schema directly")
        if dry_run:
            print("[DRY-RUN] would create intraday_bars with v3 schema")
            con.close()
            return 0
        con.execute("DROP TABLE intraday_bars")
        con.executescript(V3_SCHEMA.replace("intraday_bars_v3", "intraday_bars"))
        for idx in V3_INDEXES:
            con.execute(idx.replace("intraday_bars_v3", "intraday_bars"))
        con.commit()
        con.close()
        return 0

    # 1. Build market cache
    print("[STEP 1/5] Building market inference cache...")
    t0 = time.time()
    market_cache = build_market_cache(con)
    print(f"  {len(market_cache)} symbols classified ({time.time()-t0:.1f}s)")
    dist: dict[str, int] = {}
    for m in market_cache.values():
        dist[m] = dist.get(m, 0) + 1
    for mkt, cnt in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"    {mkt}: {cnt}")

    if dry_run:
        print("[DRY-RUN] stopping before schema changes")
        con.close()
        return 0

    # 2. Create v3 table (drop any stale one first)
    print("[STEP 2/5] Creating intraday_bars_v3 table...")
    con.execute("DROP TABLE IF EXISTS intraday_bars_v3")
    con.executescript(V3_SCHEMA)
    con.commit()

    # 3. Bulk copy with aggregation — collapse duplicate (symbol, ts_epoch)
    # into proper 1s OHLC bars (handles legacy tick-level storage).
    print(f"[STEP 3/5] Copying rows in batches of {BATCH_SIZE:,} (with OHLC aggregation)...")
    t0 = time.time()
    copied = 0
    insert_sql = """
        INSERT INTO intraday_bars_v3
            (symbol, market, date, ts, ts_epoch, interval,
             open, high, low, close, volume, trade_count, source)
        VALUES (?, ?, ?, ?, ?, '1s', ?, ?, ?, ?, ?, ?, 'legacy')
    """

    # Read ordered by (symbol, ts_epoch, rowid) so open/close are deterministic
    cur = con.execute(
        "SELECT symbol, date, ts, ts_epoch, open, high, low, close, volume "
        "FROM intraday_bars ORDER BY symbol, ts_epoch, rowid"
    )

    # Aggregate duplicate (symbol, ts_epoch) into one bar
    def flush_bar(bar: dict | None, batch: list[tuple]) -> None:
        if bar is None:
            return
        batch.append((
            bar["symbol"],
            market_cache.get(bar["symbol"], "REG"),
            bar["date"], bar["ts"], bar["ts_epoch"],
            bar["o"], bar["h"], bar["l"], bar["c"],
            bar["volume"], bar["trade_count"],
        ))

    current: dict | None = None
    batch: list[tuple] = []
    con.execute("BEGIN")
    try:
        for row in cur:
            sym, ts_epoch = row["symbol"], row["ts_epoch"]
            close = row["close"] if row["close"] is not None else 0
            vol = row["volume"] if row["volume"] is not None else 0

            if current is None or current["symbol"] != sym or current["ts_epoch"] != ts_epoch:
                # Flush previous bar
                flush_bar(current, batch)
                # Start new bar — first row's OHLC seed from (open/high/low/close)
                current = {
                    "symbol": sym, "ts_epoch": ts_epoch,
                    "date": row["date"], "ts": row["ts"],
                    "o": row["open"] if row["open"] is not None else close,
                    "h": row["high"] if row["high"] is not None else close,
                    "l": row["low"]  if row["low"]  is not None else close,
                    "c": close,
                    "volume": vol,
                    "trade_count": 1,
                }
            else:
                # Same-second duplicate — extend the bar
                if row["high"] is not None:
                    current["h"] = max(current["h"], row["high"])
                if row["low"] is not None:
                    current["l"] = min(current["l"], row["low"])
                current["h"] = max(current["h"], close)
                current["l"] = min(current["l"], close)
                current["c"] = close                # last row wins by rowid order
                current["volume"] = max(current["volume"], vol)  # use max (cumulative daily)
                current["trade_count"] += 1

            if len(batch) >= BATCH_SIZE:
                con.executemany(insert_sql, batch)
                copied += len(batch)
                batch.clear()
                if copied % (BATCH_SIZE * 5) == 0:
                    rate = copied / (time.time() - t0)
                    print(f"    {copied:,} bars written — {rate:,.0f} bars/s")

        # Flush final bar + batch
        flush_bar(current, batch)
        if batch:
            con.executemany(insert_sql, batch)
            copied += len(batch)
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"[ERROR] Copy failed: {e}")
        con.close()
        return 1

    elapsed = time.time() - t0
    print(f"  Copied {copied:,} rows in {elapsed:.1f}s ({copied/elapsed:,.0f} rows/s)")

    # 4. Create indexes on v3 (faster after bulk load)
    print("[STEP 4/5] Building indexes...")
    t0 = time.time()
    for idx in V3_INDEXES:
        con.execute(idx)
    con.commit()
    print(f"  Indexes built ({time.time()-t0:.1f}s)")

    # 5. Verify and swap
    print("[STEP 5/5] Verifying row counts...")
    v3_count = con.execute("SELECT COUNT(*) FROM intraday_bars_v3").fetchone()[0]
    # v3 has bar semantics: duplicate (symbol, ts_epoch) in legacy collapsed into one bar.
    # Expected: v3_count == distinct (symbol, ts_epoch) in legacy.
    legacy_unique = con.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM intraday_bars GROUP BY symbol, ts_epoch)"
    ).fetchone()[0]
    collapsed = total - legacy_unique
    print(f"  legacy total: {total:,}, legacy unique (sym, ts_epoch): {legacy_unique:,}")
    print(f"  v3: {v3_count:,}  (collapsed {collapsed:,} same-second dupes into bars)")
    if v3_count != legacy_unique:
        print(f"[ERROR] v3 count ({v3_count}) != legacy unique keys ({legacy_unique}). Aborting swap.")
        con.close()
        return 1

    # Atomic swap inside a single transaction
    backup_name = f"intraday_bars_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"  Swapping: intraday_bars -> {backup_name}, intraday_bars_v3 -> intraday_bars")

    # Indexes from old table need renaming to avoid conflicts on swap
    old_indexes = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='intraday_bars' AND sql IS NOT NULL"
    )]
    con.execute("BEGIN")
    try:
        # Drop legacy indexes first (frees up names like idx_ib_date_symbol)
        for idx_name in old_indexes:
            con.execute(f"DROP INDEX IF EXISTS {idx_name}")
        # Rename tables
        con.execute(f"ALTER TABLE intraday_bars RENAME TO {backup_name}")
        con.execute("ALTER TABLE intraday_bars_v3 RENAME TO intraday_bars")
        # Drop _v3 suffix indexes (they moved with the table) then recreate
        # with canonical names on the new intraday_bars
        for idx_name in V3_INDEX_NAMES:
            con.execute(f"DROP INDEX IF EXISTS {idx_name}")
        for idx_sql in CANONICAL_INDEXES:
            con.execute(idx_sql)
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"[ERROR] Swap failed: {e}")
        con.close()
        return 1

    print(f"\n[SUCCESS] Migration complete")
    print(f"  Backup kept as: {backup_name}")
    print(f"  Drop backup when verified: DROP TABLE {backup_name};")
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
