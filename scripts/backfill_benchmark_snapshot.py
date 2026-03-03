"""Backfill sbp_benchmark_snapshot from existing historical tables.

Populates the benchmark snapshot table with historical data from:
  - kibor_daily       → kibor_3m_bid/offer, kibor_6m_bid/offer, kibor_12m_bid/offer
  - tbill_auctions    → mtb_3m, mtb_6m, mtb_12m (carry-forward between auctions)
  - pib_auctions      → pib_2y, pib_3y, pib_5y, pib_10y, pib_15y (carry-forward)
  - policy rate history → policy_rate (hardcoded from SBP official records)

Uses INSERT OR IGNORE — never overwrites existing scraped snapshots.

Usage:
    python scripts/backfill_benchmark_snapshot.py [--db PATH] [--dry-run]
"""

import argparse
import sqlite3
from datetime import date, timedelta


# ── SBP Policy Rate History ─────────────────────────────────────────────────
# Sources: SBP official (sbp.org.pk/m_policy/), Trading Economics,
#          Business Recorder, Dawn, Express Tribune
# Each entry: (effective_date, rate)
# Rate stays in effect until the next change date.

POLICY_RATE_HISTORY = [
    ("2019-02-01", 10.25),
    ("2019-04-01", 10.75),
    ("2019-05-21", 12.25),
    ("2019-07-17", 13.25),
    ("2020-03-17", 12.50),
    ("2020-03-24", 11.00),
    ("2020-04-16", 9.00),
    ("2020-05-15", 8.00),
    ("2020-06-25", 7.00),
    ("2021-09-20", 7.25),
    ("2021-11-19", 8.75),
    ("2021-12-14", 9.75),
    ("2022-04-07", 12.25),
    ("2022-05-23", 13.75),
    ("2022-07-07", 15.00),
    ("2023-01-23", 17.00),
    ("2023-03-02", 20.00),
    ("2023-04-04", 21.00),
    ("2023-06-27", 22.00),
    ("2024-06-11", 20.50),
    ("2024-07-29", 19.50),
    ("2024-09-13", 17.50),
    ("2024-11-05", 15.00),
    ("2024-12-17", 13.00),
    ("2025-01-28", 12.00),
    ("2025-05-06", 11.00),
    ("2025-12-16", 10.50),
]


def _get_policy_rate_for_date(d: str) -> float | None:
    """Return the SBP policy rate in effect on a given date string (YYYY-MM-DD)."""
    rate = None
    for eff_date, r in POLICY_RATE_HISTORY:
        if d >= eff_date:
            rate = r
        else:
            break
    return rate


def _generate_date_range(start: str, end: str) -> list[str]:
    """Generate all dates (YYYY-MM-DD strings) from start to end inclusive."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    dates = []
    cur = s
    while cur <= e:
        dates.append(cur.isoformat())
        cur += timedelta(days=1)
    return dates


def backfill(db_path: str, dry_run: bool = False):
    """Run the full backfill."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")

    # Ensure table exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS sbp_benchmark_snapshot (
            date TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            scraped_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (date, metric)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_benchmark_date
            ON sbp_benchmark_snapshot(date)
    """)

    total_inserted = 0
    rows_to_insert = []

    # ── 1. KIBOR from kibor_daily ────────────────────────────────────────
    print("── Step 1: KIBOR rates from kibor_daily ──")

    # Map kibor_daily tenors to benchmark metric names
    # Note: older data uses "1Y", newer uses "12M" — both map to kibor_12m_*
    kibor_tenor_map = {
        "1W":  ("kibor_1w_bid", "kibor_1w_offer"),
        "1M":  ("kibor_1m_bid", "kibor_1m_offer"),
        "3M":  ("kibor_3m_bid", "kibor_3m_offer"),
        "6M":  ("kibor_6m_bid", "kibor_6m_offer"),
        "12M": ("kibor_12m_bid", "kibor_12m_offer"),
        "9M":  ("kibor_9m_bid", "kibor_9m_offer"),
        "1Y":  ("kibor_12m_bid", "kibor_12m_offer"),  # alias
    }

    kibor_rows = con.execute(
        "SELECT date, tenor, bid, offer FROM kibor_daily "
        "WHERE tenor IN ('1W', '1M', '3M', '6M', '9M', '12M', '1Y') "
        "ORDER BY date"
    ).fetchall()

    kibor_count = 0
    for row_date, tenor, bid, offer in kibor_rows:
        metrics = kibor_tenor_map.get(tenor)
        if not metrics:
            continue
        bid_metric, offer_metric = metrics
        if bid is not None:
            rows_to_insert.append((row_date, bid_metric, bid))
            kibor_count += 1
        if offer is not None:
            rows_to_insert.append((row_date, offer_metric, offer))
            kibor_count += 1

    print(f"   KIBOR: {kibor_count} metric-rows prepared")

    # ── 2. T-Bill auctions (carry-forward) ───────────────────────────────
    print("── Step 2: T-Bill auction cutoffs (carry-forward) ──")

    tbill_tenor_map = {"3M": "mtb_3m", "6M": "mtb_6m", "12M": "mtb_12m"}

    # Get all auction dates and cutoffs, ordered
    tbill_rows = con.execute(
        "SELECT auction_date, tenor, cutoff_yield FROM tbill_auctions "
        "WHERE cutoff_yield IS NOT NULL AND tenor IN ('3M', '6M', '12M') "
        "ORDER BY auction_date"
    ).fetchall()

    # Build timeline: for each tenor, list of (date, yield)
    tbill_by_tenor: dict[str, list[tuple[str, float]]] = {}
    for adate, tenor, cyield in tbill_rows:
        tbill_by_tenor.setdefault(tenor, []).append((adate, cyield))

    # Use KIBOR date range as the anchor — all series align to this window
    date_range_end = date.today().isoformat()
    date_range_start = kibor_rows[0][0] if kibor_rows else "2024-01-10"

    tbill_count = 0
    if date_range_start:
        all_dates = _generate_date_range(date_range_start, date_range_end)

        for tenor, auctions in tbill_by_tenor.items():
            metric = tbill_tenor_map.get(tenor)
            if not metric:
                continue

            # Carry-forward: for each date, use the most recent auction yield
            auction_idx = 0
            current_yield = None

            for d in all_dates:
                # Advance to the latest auction on or before this date
                while (auction_idx < len(auctions)
                       and auctions[auction_idx][0] <= d):
                    current_yield = auctions[auction_idx][1]
                    auction_idx += 1

                if current_yield is not None:
                    rows_to_insert.append((d, metric, current_yield))
                    tbill_count += 1

    print(f"   T-Bills: {tbill_count} metric-rows prepared (carry-forward)")

    # ── 3. PIB auctions (carry-forward) ──────────────────────────────────
    print("── Step 3: PIB auction cutoffs (carry-forward) ──")

    pib_tenor_map = {
        "2Y": "pib_2y", "3Y": "pib_3y", "5Y": "pib_5y",
        "10Y": "pib_10y", "15Y": "pib_15y",
    }

    pib_rows = con.execute(
        "SELECT auction_date, tenor, cutoff_yield FROM pib_auctions "
        "WHERE cutoff_yield IS NOT NULL "
        "AND tenor IN ('2Y', '3Y', '5Y', '10Y', '15Y') "
        "ORDER BY auction_date"
    ).fetchall()

    # Build timeline per tenor
    pib_by_tenor: dict[str, list[tuple[str, float]]] = {}
    for adate, tenor, cyield in pib_rows:
        pib_by_tenor.setdefault(tenor, []).append((adate, cyield))

    # Use same KIBOR-anchored date range so all series align on charts
    pib_count = 0
    if date_range_start:
        pib_dates = _generate_date_range(date_range_start, date_range_end)

        for tenor, auctions in pib_by_tenor.items():
            metric = pib_tenor_map.get(tenor)
            if not metric:
                continue

            auction_idx = 0
            current_yield = None

            for d in pib_dates:
                while (auction_idx < len(auctions)
                       and auctions[auction_idx][0] <= d):
                    current_yield = auctions[auction_idx][1]
                    auction_idx += 1

                if current_yield is not None:
                    rows_to_insert.append((d, metric, current_yield))
                    pib_count += 1

    print(f"   PIBs: {pib_count} metric-rows prepared (carry-forward)")

    # ── 4. Policy rate (carry-forward from hardcoded history) ────────────
    print("── Step 4: SBP Policy Rate history ──")

    # Same KIBOR-anchored date range
    policy_dates = _generate_date_range(date_range_start, date_range_end)
    policy_count = 0

    for d in policy_dates:
        rate = _get_policy_rate_for_date(d)
        if rate is not None:
            rows_to_insert.append((d, "policy_rate", rate))
            policy_count += 1

    print(f"   Policy rate: {policy_count} metric-rows prepared")

    # ── INSERT ───────────────────────────────────────────────────────────
    print(f"\n── Total rows to insert: {len(rows_to_insert)} ──")

    if dry_run:
        print("DRY RUN — no changes made.")
        # Show summary by metric
        from collections import Counter
        metric_counts = Counter(r[1] for r in rows_to_insert)
        for m, c in sorted(metric_counts.items()):
            print(f"   {m}: {c}")
        con.close()
        return

    print("Inserting with INSERT OR IGNORE (preserving existing data)...")

    con.execute("BEGIN")
    cursor = con.executemany(
        "INSERT OR IGNORE INTO sbp_benchmark_snapshot (date, metric, value, scraped_at) "
        "VALUES (?, ?, ?, datetime('now', 'localtime'))",
        rows_to_insert,
    )
    total_inserted = cursor.rowcount
    con.commit()

    # Get actual count — rowcount on executemany may not be accurate for IGNORE
    after_count = con.execute(
        "SELECT COUNT(*) FROM sbp_benchmark_snapshot"
    ).fetchone()[0]

    date_range = con.execute(
        "SELECT MIN(date), MAX(date) FROM sbp_benchmark_snapshot"
    ).fetchone()

    unique_dates = con.execute(
        "SELECT COUNT(DISTINCT date) FROM sbp_benchmark_snapshot"
    ).fetchone()[0]

    unique_metrics = con.execute(
        "SELECT COUNT(DISTINCT metric) FROM sbp_benchmark_snapshot"
    ).fetchone()[0]

    print(f"\n── Results ──")
    print(f"   Total rows now: {after_count}")
    print(f"   Date range: {date_range[0]} → {date_range[1]}")
    print(f"   Unique dates: {unique_dates}")
    print(f"   Unique metrics: {unique_metrics}")

    # Per-metric summary
    print("\n── Rows per metric ──")
    metric_summary = con.execute(
        "SELECT metric, COUNT(*), MIN(date), MAX(date) "
        "FROM sbp_benchmark_snapshot GROUP BY metric ORDER BY metric"
    ).fetchall()
    for metric, cnt, mn, mx in metric_summary:
        print(f"   {metric}: {cnt} rows ({mn} → {mx})")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill sbp_benchmark_snapshot")
    parser.add_argument("--db", help="Path to SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    if args.db:
        db = args.db
    else:
        from pakfindata.config import get_db_path
        db = str(get_db_path())

    print(f"Database: {db}")
    backfill(db, dry_run=args.dry_run)
