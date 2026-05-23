"""Fixed income instruments repository (bonds, T-bills, sukuk)."""

from __future__ import annotations

import sqlite3

import pandas as pd


# =============================================================================
# FX (Foreign Exchange) Functions
# =============================================================================


def upsert_fx_pair(con: sqlite3.Connection, pair_data: dict) -> bool:
    """
    Insert or update an FX pair.

    Args:
        con: Database connection
        pair_data: Dict with pair, base_currency, quote_currency, source, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO fx_pairs (
                pair, base_currency, quote_currency, source, description,
                is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(pair) DO UPDATE SET
                base_currency = excluded.base_currency,
                quote_currency = excluded.quote_currency,
                source = excluded.source,
                description = excluded.description,
                is_active = excluded.is_active,
                updated_at = datetime('now')
        """, (
            pair_data.get("pair"),
            pair_data.get("base_currency"),
            pair_data.get("quote_currency"),
            pair_data.get("source", "MANUAL"),
            pair_data.get("description"),
            pair_data.get("is_active", 1),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fx_pairs(
    con: sqlite3.Connection,
    active_only: bool = True
) -> list[dict]:
    """
    Get all FX pairs.

    Args:
        con: Database connection
        active_only: If True, only return active pairs

    Returns:
        List of pair dicts
    """
    query = "SELECT * FROM fx_pairs"
    if active_only:
        query += " WHERE is_active = 1"
    query += " ORDER BY pair"

    try:
        cur = con.execute(query)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fx_pair(con: sqlite3.Connection, pair: str) -> dict | None:
    """Get a single FX pair by name."""
    try:
        cur = con.execute("SELECT * FROM fx_pairs WHERE pair = ?", (pair,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fx_ohlcv(
    con: sqlite3.Connection,
    pair: str,
    df: "pd.DataFrame"
) -> int:
    """
    Upsert FX OHLCV data.

    Args:
        con: Database connection
        pair: FX pair (e.g., "USD/PKR")
        df: DataFrame with date, open, high, low, close, volume columns

    Returns:
        Number of rows upserted
    """
    if df.empty:
        return 0

    rows = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT INTO fx_ohlcv (pair, date, open, high, low, close, volume, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(pair, date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    ingested_at = datetime('now')
            """, (
                pair,
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
            ))
            rows += 1
        except Exception:
            pass

    con.commit()
    return rows


def get_fx_ohlcv(
    con: sqlite3.Connection,
    pair: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None
) -> "pd.DataFrame":
    """
    Get FX OHLCV data.

    Args:
        con: Database connection
        pair: FX pair
        start_date: Start date filter
        end_date: End date filter
        limit: Max rows to return

    Returns:
        DataFrame with OHLCV data
    """
    import pandas as pd

    query = "SELECT * FROM fx_ohlcv WHERE pair = ?"
    params = [pair]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_fx_latest_date(con: sqlite3.Connection, pair: str) -> str | None:
    """Get the latest date for an FX pair."""
    try:
        cur = con.execute(
            "SELECT MAX(date) FROM fx_ohlcv WHERE pair = ?",
            (pair,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def get_fx_latest_rate(con: sqlite3.Connection, pair: str) -> dict | None:
    """Get the latest FX rate for a pair."""
    try:
        cur = con.execute("""
            SELECT * FROM fx_ohlcv
            WHERE pair = ?
            ORDER BY date DESC
            LIMIT 1
        """, (pair,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fx_adjusted_metric(con: sqlite3.Connection, metric: dict) -> bool:
    """
    Insert or update an FX-adjusted metric.

    Args:
        con: Database connection
        metric: Dict with as_of_date, symbol, fx_pair, equity_return, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO fx_adjusted_metrics (
                as_of_date, symbol, fx_pair, equity_return, fx_return,
                fx_adjusted_return, period, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(as_of_date, symbol, fx_pair, period) DO UPDATE SET
                equity_return = excluded.equity_return,
                fx_return = excluded.fx_return,
                fx_adjusted_return = excluded.fx_adjusted_return,
                computed_at = datetime('now')
        """, (
            metric.get("as_of_date"),
            metric.get("symbol"),
            metric.get("fx_pair"),
            metric.get("equity_return"),
            metric.get("fx_return"),
            metric.get("fx_adjusted_return"),
            metric.get("period", "1M"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fx_adjusted_metrics(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    symbol: str | None = None,
    fx_pair: str | None = None,
    period: str | None = None,
    limit: int = 50
) -> list[dict]:
    """
    Get FX-adjusted metrics.

    Args:
        con: Database connection
        as_of_date: Filter by date
        symbol: Filter by symbol
        fx_pair: Filter by FX pair
        period: Filter by period
        limit: Max results

    Returns:
        List of metric dicts
    """
    query = "SELECT * FROM fx_adjusted_metrics WHERE 1=1"
    params = []

    if as_of_date:
        query += " AND as_of_date = ?"
        params.append(as_of_date)
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if fx_pair:
        query += " AND fx_pair = ?"
        params.append(fx_pair)
    if period:
        query += " AND period = ?"
        params.append(period)

    query += " ORDER BY as_of_date DESC, fx_adjusted_return DESC LIMIT ?"
    params.append(limit)

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def record_fx_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    pairs: list[str]
) -> bool:
    """Record the start of an FX sync run."""
    try:
        con.execute("""
            INSERT INTO fx_sync_runs (run_id, started_at, pairs_synced, status)
            VALUES (?, datetime('now'), ?, 'running')
        """, (run_id, ",".join(pairs)))
        con.commit()
        return True
    except Exception:
        return False


def update_fx_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update an FX sync run."""
    try:
        con.execute("""
            UPDATE fx_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_fx_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent FX sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM fx_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


# =============================================================================
# Mutual Fund CRUD Functions (MUFAP Integration)
# =============================================================================


def upsert_mutual_fund(con: sqlite3.Connection, fund_data: dict) -> bool:
    """
    Insert or update a mutual fund.

    Args:
        con: Database connection
        fund_data: Dict with fund_id, symbol, fund_name, amc_code, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO mutual_funds (
                fund_id, symbol, fund_name, amc_code, amc_name,
                fund_type, category, is_shariah, launch_date,
                expense_ratio, management_fee,
                mufap_fund_id, mufap_int_id, mufap_amc_id,
                front_load, back_load, risk_profile, benchmark,
                rating, trustee, fund_manager,
                is_active, source,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, datetime('now'), datetime('now')
            )
            ON CONFLICT(fund_id) DO UPDATE SET
                symbol = excluded.symbol,
                fund_name = excluded.fund_name,
                amc_code = excluded.amc_code,
                amc_name = excluded.amc_name,
                fund_type = excluded.fund_type,
                category = excluded.category,
                is_shariah = excluded.is_shariah,
                launch_date = excluded.launch_date,
                expense_ratio = excluded.expense_ratio,
                management_fee = excluded.management_fee,
                mufap_fund_id = COALESCE(excluded.mufap_fund_id, mutual_funds.mufap_fund_id),
                mufap_int_id = COALESCE(excluded.mufap_int_id, mutual_funds.mufap_int_id),
                mufap_amc_id = COALESCE(excluded.mufap_amc_id, mutual_funds.mufap_amc_id),
                front_load = COALESCE(excluded.front_load, mutual_funds.front_load),
                back_load = COALESCE(excluded.back_load, mutual_funds.back_load),
                risk_profile = COALESCE(excluded.risk_profile, mutual_funds.risk_profile),
                benchmark = COALESCE(excluded.benchmark, mutual_funds.benchmark),
                rating = COALESCE(excluded.rating, mutual_funds.rating),
                trustee = COALESCE(excluded.trustee, mutual_funds.trustee),
                fund_manager = COALESCE(excluded.fund_manager, mutual_funds.fund_manager),
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = datetime('now')
        """, (
            fund_data.get("fund_id"),
            fund_data.get("symbol"),
            fund_data.get("fund_name"),
            fund_data.get("amc_code"),
            fund_data.get("amc_name"),
            fund_data.get("fund_type", "OPEN_END"),
            fund_data.get("category"),
            fund_data.get("is_shariah", 0),
            fund_data.get("launch_date"),
            fund_data.get("expense_ratio"),
            fund_data.get("management_fee"),
            fund_data.get("mufap_fund_id"),
            fund_data.get("mufap_int_id"),
            fund_data.get("mufap_amc_id"),
            fund_data.get("front_load"),
            fund_data.get("back_load"),
            fund_data.get("risk_profile"),
            fund_data.get("benchmark"),
            fund_data.get("rating"),
            fund_data.get("trustee"),
            fund_data.get("fund_manager"),
            fund_data.get("is_active", 1),
            fund_data.get("source", "MUFAP"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_mutual_funds(
    con: sqlite3.Connection,
    category: str | None = None,
    fund_type: str | None = None,
    is_shariah: bool | None = None,
    active_only: bool = True,
    search: str | None = None,
) -> list[dict]:
    """
    Get mutual funds with optional filters.

    Args:
        con: Database connection
        category: Filter by category (e.g., 'Equity', 'Money Market')
        fund_type: Filter by fund type ('OPEN_END', 'VPS', 'ETF')
        is_shariah: Filter by Shariah compliance
        active_only: If True, only return active funds
        search: Search term for fund name or symbol

    Returns:
        List of fund dicts
    """
    query = "SELECT * FROM mutual_funds WHERE 1=1"
    params = []

    if category:
        query += " AND category = ?"
        params.append(category)
    if fund_type:
        query += " AND fund_type = ?"
        params.append(fund_type)
    if is_shariah is not None:
        query += " AND is_shariah = ?"
        params.append(1 if is_shariah else 0)
    if active_only:
        query += " AND is_active = 1"
    if search:
        query += " AND (fund_name LIKE ? OR symbol LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY category, fund_name"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_mutual_fund(con: sqlite3.Connection, fund_id: str) -> dict | None:
    """Get a single mutual fund by fund_id."""
    try:
        cur = con.execute("SELECT * FROM mutual_funds WHERE fund_id = ?", (fund_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_mutual_fund_by_symbol(con: sqlite3.Connection, symbol: str) -> dict | None:
    """Get a single mutual fund by symbol."""
    try:
        cur = con.execute("SELECT * FROM mutual_funds WHERE symbol = ?", (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_mf_nav(
    con: sqlite3.Connection,
    fund_id: str,
    df: "pd.DataFrame"
) -> int:
    """
    Upsert mutual fund NAV data.

    Args:
        con: Database connection
        fund_id: Mutual fund ID
        df: DataFrame with date, nav, offer_price, redemption_price, aum, nav_change_pct

    Returns:
        Number of rows upserted
    """
    if df.empty:
        return 0

    rows = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT INTO mutual_fund_nav (
                    fund_id, date, nav, offer_price, redemption_price,
                    aum, nav_change_pct, source, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(fund_id, date) DO UPDATE SET
                    nav = excluded.nav,
                    offer_price = excluded.offer_price,
                    redemption_price = excluded.redemption_price,
                    aum = excluded.aum,
                    nav_change_pct = excluded.nav_change_pct,
                    source = excluded.source,
                    ingested_at = datetime('now')
            """, (
                fund_id,
                row.get("date"),
                row.get("nav"),
                row.get("offer_price"),
                row.get("redemption_price"),
                row.get("aum"),
                row.get("nav_change_pct"),
                row.get("source", "MUFAP"),
            ))
            rows += 1
        except Exception:
            pass

    con.commit()
    return rows


def _safe_float_fi(v) -> float | None:
    """Safely convert to float (local helper to avoid importing from sources)."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def parse_nav_history_to_tuples(
    fund_id: str,
    nav_history: list[dict],
) -> list[tuple]:
    """Convert raw MUFAP nav_history records to upsert tuples.

    Args:
        fund_id: Internal fund_id (e.g., "MUFAP:123").
        nav_history: List of dicts from MUFAP API Table1.

    Returns:
        List of (fund_id, date, nav, offer_price, redemption_price,
                 aum, nav_change_pct, source).
    """
    rows = []
    for rec in nav_history:
        date_str = rec.get("entryDate") or rec.get("CalDate")
        nav_val = rec.get("netval")
        if not date_str or nav_val is None:
            continue
        date_clean = str(date_str)[:10]
        try:
            nav_float = float(nav_val)
        except (ValueError, TypeError):
            continue

        offer = _safe_float_fi(rec.get("offer_price") or rec.get("OfferPrice"))
        redemp = _safe_float_fi(
            rec.get("repurchase_price") or rec.get("RedemptionPrice")
        )

        rows.append((
            fund_id,
            date_clean,
            nav_float,
            offer or nav_float,
            redemp or nav_float,
            None,     # aum
            None,     # nav_change_pct
            "MUFAP",
        ))
    return rows


def upsert_mf_nav_batch(
    con: sqlite3.Connection,
    fund_id: str,
    rows: list[tuple],
) -> int:
    """Batch upsert mutual fund NAV data using executemany.

    Args:
        con: Database connection.
        fund_id: Mutual fund ID (for logging only; fund_id is in each tuple).
        rows: List of tuples from parse_nav_history_to_tuples().

    Returns:
        Number of rows submitted.
    """
    if not rows:
        return 0

    con.executemany(
        """INSERT INTO mutual_fund_nav (
            fund_id, date, nav, offer_price, redemption_price,
            aum, nav_change_pct, source, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(fund_id, date) DO UPDATE SET
            nav = excluded.nav,
            offer_price = excluded.offer_price,
            redemption_price = excluded.redemption_price,
            aum = excluded.aum,
            nav_change_pct = excluded.nav_change_pct,
            source = excluded.source,
            ingested_at = datetime('now')
        """,
        rows,
    )
    con.commit()
    return len(rows)


def get_mf_nav(
    con: sqlite3.Connection,
    fund_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None
) -> "pd.DataFrame":
    """
    Get mutual fund NAV data.

    Args:
        con: Database connection
        fund_id: Mutual fund ID
        start_date: Start date filter
        end_date: End date filter
        limit: Max rows to return

    Returns:
        DataFrame with NAV data
    """
    import pandas as pd

    query = "SELECT * FROM mutual_fund_nav WHERE fund_id = ?"
    params = [fund_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_mf_latest_date(con: sqlite3.Connection, fund_id: str) -> str | None:
    """Get the latest NAV date for a mutual fund."""
    try:
        cur = con.execute(
            "SELECT MAX(date) FROM mutual_fund_nav WHERE fund_id = ?",
            (fund_id,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def get_mf_latest_nav(con: sqlite3.Connection, fund_id: str) -> dict | None:
    """Get the latest NAV for a mutual fund."""
    try:
        cur = con.execute("""
            SELECT * FROM mutual_fund_nav
            WHERE fund_id = ?
            ORDER BY date DESC
            LIMIT 1
        """, (fund_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def record_mf_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    funds_total: int = 0
) -> bool:
    """Record the start of a mutual fund sync run."""
    try:
        con.execute("""
            INSERT INTO mutual_fund_sync_runs (
                run_id, started_at, sync_type, status, funds_total
            ) VALUES (?, datetime('now'), ?, 'running', ?)
        """, (run_id, sync_type, funds_total))
        con.commit()
        return True
    except Exception:
        return False


def update_mf_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    funds_ok: int = 0,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update a mutual fund sync run."""
    try:
        con.execute("""
            UPDATE mutual_fund_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                funds_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, funds_ok, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_mf_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent mutual fund sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM mutual_fund_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_mf_data_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of mutual fund data in database.

    Returns:
        Dict with fund counts, date ranges, category breakdown, etc.
    """
    summary = {
        "total_funds": 0,
        "active_funds": 0,
        "funds_with_nav": 0,
        "total_nav_rows": 0,
        "categories": {},
        "fund_types": {},
        "latest_nav_date": None,
        "earliest_nav_date": None,
    }

    try:
        # Total and active funds
        cur = con.execute("SELECT COUNT(*) FROM mutual_funds")
        summary["total_funds"] = cur.fetchone()[0]

        cur = con.execute("SELECT COUNT(*) FROM mutual_funds WHERE is_active = 1")
        summary["active_funds"] = cur.fetchone()[0]

        # Funds with NAV data
        cur = con.execute("""
            SELECT COUNT(DISTINCT fund_id) FROM mutual_fund_nav
        """)
        summary["funds_with_nav"] = cur.fetchone()[0]

        # Total NAV rows
        cur = con.execute("SELECT COUNT(*) FROM mutual_fund_nav")
        summary["total_nav_rows"] = cur.fetchone()[0]

        # Category breakdown
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM mutual_funds
            WHERE is_active = 1
            GROUP BY category
            ORDER BY count DESC
        """)
        summary["categories"] = {row[0]: row[1] for row in cur.fetchall()}

        # Fund type breakdown
        cur = con.execute("""
            SELECT fund_type, COUNT(*) as count
            FROM mutual_funds
            WHERE is_active = 1
            GROUP BY fund_type
            ORDER BY count DESC
        """)
        summary["fund_types"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute("SELECT MAX(date), MIN(date) FROM mutual_fund_nav")
        row = cur.fetchone()
        if row:
            summary["latest_nav_date"] = row[0]
            summary["earliest_nav_date"] = row[1]

    except Exception:
        pass

    return summary


# =============================================================================
# Fund Performance Functions (MUFAP tab=1 daily return snapshots)
# =============================================================================


def _ensure_mutual_funds_columns(con: sqlite3.Connection) -> None:
    """Add missing columns to mutual_funds table (safe migration)."""
    existing = {row[1] for row in con.execute("PRAGMA table_info(mutual_funds)").fetchall()}
    migrations = [
        ("aum", "REAL"),
        ("sector", "TEXT"),
    ]
    for col, dtype in migrations:
        if col not in existing:
            try:
                con.execute(f"ALTER TABLE mutual_funds ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    con.commit()


def init_fund_performance_schema(con: sqlite3.Connection) -> None:
    """Create fund_performance table and indexes if they don't exist."""
    _ensure_mutual_funds_columns(con)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fund_performance (
            fund_name TEXT NOT NULL,
            fund_id TEXT,
            sector TEXT,
            category TEXT,
            rating TEXT,
            benchmark TEXT,
            validity_date TEXT NOT NULL,
            nav REAL,
            return_ytd REAL,
            return_mtd REAL,
            return_1d REAL,
            return_15d REAL,
            return_30d REAL,
            return_90d REAL,
            return_180d REAL,
            return_270d REAL,
            return_365d REAL,
            return_2y REAL,
            return_3y REAL,
            scraped_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (fund_name, validity_date)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_fund_perf_date ON fund_performance(validity_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fund_perf_category ON fund_performance(category)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fund_perf_sector ON fund_performance(sector)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fund_perf_name_date ON fund_performance(fund_name, validity_date DESC)")

    # Migration: add source column if missing (mufap=official, computed=NAV-derived)
    existing_cols = {row[1] for row in con.execute("PRAGMA table_info(fund_performance)").fetchall()}
    if "source" not in existing_cols:
        con.execute("ALTER TABLE fund_performance ADD COLUMN source TEXT NOT NULL DEFAULT 'mufap'")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fund_perf_source ON fund_performance(source)")

    # Summary tables — pre-computed latest row per fund for fast UI queries
    con.execute("""
        CREATE TABLE IF NOT EXISTS fund_performance_latest (
            fund_name TEXT PRIMARY KEY,
            fund_id TEXT,
            sector TEXT,
            category TEXT,
            rating TEXT,
            validity_date TEXT,
            nav REAL,
            return_ytd REAL,
            return_mtd REAL,
            return_1d REAL,
            return_15d REAL,
            return_30d REAL,
            return_90d REAL,
            return_180d REAL,
            return_270d REAL,
            return_365d REAL,
            return_2y REAL,
            return_3y REAL,
            refreshed_at TEXT DEFAULT (datetime('now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fund_nav_latest (
            fund_id TEXT PRIMARY KEY,
            nav REAL,
            date TEXT,
            refreshed_at TEXT DEFAULT (datetime('now'))
        )
    """)

    con.commit()


def refresh_fund_performance_latest(con: sqlite3.Connection) -> int:
    """Rebuild fund_performance_latest from fund_performance (~2-3s)."""
    con.execute("DELETE FROM fund_performance_latest")
    con.execute("""
        INSERT INTO fund_performance_latest
            (fund_name, fund_id, sector, category, rating, validity_date, nav,
             return_ytd, return_mtd, return_1d, return_15d, return_30d,
             return_90d, return_180d, return_270d, return_365d,
             return_2y, return_3y)
        SELECT fp.fund_name, fp.fund_id, fp.sector, fp.category, fp.rating,
               fp.validity_date, fp.nav,
               fp.return_ytd, fp.return_mtd, fp.return_1d, fp.return_15d,
               fp.return_30d, fp.return_90d, fp.return_180d, fp.return_270d,
               fp.return_365d, fp.return_2y, fp.return_3y
        FROM fund_performance fp
        INNER JOIN (
            SELECT fund_name, MAX(validity_date) as max_date
            FROM fund_performance GROUP BY fund_name
        ) latest ON fp.fund_name = latest.fund_name
                 AND fp.validity_date = latest.max_date
    """)
    con.commit()
    return con.execute("SELECT COUNT(*) FROM fund_performance_latest").fetchone()[0]


def refresh_fund_nav_latest(con: sqlite3.Connection) -> int:
    """Incrementally update fund_nav_latest — only process new NAVs.

    Finds the oldest 'latest date' across all funds already in the summary,
    then only scans mutual_fund_nav rows >= that date.  This turns a 1.9M-row
    scan into a tiny delta scan (typically a few thousand rows).
    On first run (empty table), falls back to a full rebuild.
    """
    # Check if summary table already has data
    row = con.execute(
        "SELECT MIN(date) AS earliest, COUNT(*) AS cnt FROM fund_nav_latest"
    ).fetchone()

    if row and row[0] and row[1] > 0:
        # Incremental: only look at NAVs on or after the earliest known latest date.
        # This is conservative — covers funds whose latest might have changed.
        cutoff = row[0]
        con.execute("""
            INSERT OR REPLACE INTO fund_nav_latest (fund_id, nav, date, refreshed_at)
            SELECT n.fund_id, n.nav, n.date, datetime('now')
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MAX(date) AS max_date
                FROM mutual_fund_nav
                WHERE date >= ?
                GROUP BY fund_id
            ) m ON n.fund_id = m.fund_id AND n.date = m.max_date
            LEFT JOIN fund_nav_latest ex ON ex.fund_id = n.fund_id
            WHERE ex.fund_id IS NULL OR n.date > ex.date
        """, (cutoff,))
    else:
        # First run: full rebuild
        con.execute("""
            INSERT OR REPLACE INTO fund_nav_latest (fund_id, nav, date, refreshed_at)
            SELECT n.fund_id, n.nav, n.date, datetime('now')
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MAX(date) AS max_date
                FROM mutual_fund_nav GROUP BY fund_id
            ) m ON n.fund_id = m.fund_id AND n.date = m.max_date
        """)

    con.commit()
    return con.execute("SELECT COUNT(*) FROM fund_nav_latest").fetchone()[0]


def refresh_fund_summary_tables(con: sqlite3.Connection) -> dict:
    """Refresh both pre-computed summary tables. Call after sync operations."""
    perf_count = refresh_fund_performance_latest(con)
    nav_count = refresh_fund_nav_latest(con)
    return {"perf_latest": perf_count, "nav_latest": nav_count}


def upsert_fund_performance(con: sqlite3.Connection, records: list[dict]) -> int:
    """Bulk upsert fund performance records. Returns count of rows upserted.

    Priority: 'mufap' source always wins over 'computed' source.
    String comparison 'mufap' >= 'computed' gates the ON CONFLICT UPDATE.
    """
    if not records:
        return 0
    init_fund_performance_schema(con)
    count = 0
    for rec in records:
        try:
            con.execute("""
                INSERT INTO fund_performance (
                    fund_name, fund_id, sector, category, rating, benchmark,
                    validity_date, nav,
                    return_ytd, return_mtd, return_1d, return_15d,
                    return_30d, return_90d, return_180d, return_270d,
                    return_365d, return_2y, return_3y, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fund_name, validity_date) DO UPDATE SET
                    fund_id = excluded.fund_id,
                    sector = excluded.sector,
                    category = excluded.category,
                    rating = excluded.rating,
                    benchmark = excluded.benchmark,
                    nav = excluded.nav,
                    return_ytd = excluded.return_ytd,
                    return_mtd = excluded.return_mtd,
                    return_1d = excluded.return_1d,
                    return_15d = excluded.return_15d,
                    return_30d = excluded.return_30d,
                    return_90d = excluded.return_90d,
                    return_180d = excluded.return_180d,
                    return_270d = excluded.return_270d,
                    return_365d = excluded.return_365d,
                    return_2y = excluded.return_2y,
                    return_3y = excluded.return_3y,
                    source = excluded.source,
                    scraped_at = datetime('now')
                WHERE excluded.source >= fund_performance.source
            """, (
                rec.get("fund_name"),
                rec.get("fund_id"),
                rec.get("sector"),
                rec.get("category"),
                rec.get("rating"),
                rec.get("benchmark"),
                rec.get("validity_date"),
                rec.get("nav"),
                rec.get("return_ytd"),
                rec.get("return_mtd"),
                rec.get("return_1d"),
                rec.get("return_15d"),
                rec.get("return_30d"),
                rec.get("return_90d"),
                rec.get("return_180d"),
                rec.get("return_270d"),
                rec.get("return_365d"),
                rec.get("return_2y"),
                rec.get("return_3y"),
                rec.get("source", "mufap"),
            ))
            count += 1
        except Exception:
            continue
    con.commit()
    return count


def get_fund_performance(
    con: sqlite3.Connection,
    date: str | None = None,
    category: str | None = None,
    sector: str | None = None,
) -> pd.DataFrame:
    """Get fund performance data with optional filters."""
    init_fund_performance_schema(con)
    query = "SELECT * FROM fund_performance WHERE 1=1"
    params: list = []
    if date:
        query += " AND validity_date = ?"
        params.append(date)
    else:
        query += """ AND validity_date = (
            SELECT MAX(fp2.validity_date) FROM fund_performance fp2
            WHERE fp2.fund_name = fund_performance.fund_name
        )"""
    if category:
        query += " AND category = ?"
        params.append(category)
    if sector:
        query += " AND sector = ?"
        params.append(sector)
    query += " ORDER BY fund_name"
    return pd.read_sql_query(query, con, params=params)


def get_top_performers(
    con: sqlite3.Connection,
    period: str = "return_ytd",
    n: int = 20,
    category: str | None = None,
    sector: str | None = None,
) -> pd.DataFrame:
    """Get top N performing funds by a specific return period."""
    init_fund_performance_schema(con)
    valid_periods = {
        "return_ytd", "return_mtd", "return_1d", "return_15d",
        "return_30d", "return_90d", "return_180d", "return_270d",
        "return_365d", "return_2y", "return_3y",
    }
    if period not in valid_periods:
        period = "return_ytd"

    query = f"""
        SELECT fund_name, category, sector, nav, rating, {period}
        FROM fund_performance fp
        WHERE validity_date = (
            SELECT MAX(fp2.validity_date) FROM fund_performance fp2
            WHERE fp2.fund_name = fp.fund_name
        )
          AND {period} IS NOT NULL
    """
    params: list = []
    if category:
        query += " AND category = ?"
        params.append(category)
    if sector:
        query += " AND sector = ?"
        params.append(sector)
    query += f" ORDER BY {period} DESC LIMIT ?"
    params.append(n)
    return pd.read_sql_query(query, con, params=params)


def get_fund_returns(
    con: sqlite3.Connection,
    fund_name: str,
    start_date: str | None = None,
) -> pd.DataFrame:
    """Get time-series of a fund's returns across snapshot dates."""
    init_fund_performance_schema(con)
    query = "SELECT * FROM fund_performance WHERE fund_name = ?"
    params: list = [fund_name]
    if start_date:
        query += " AND validity_date >= ?"
        params.append(start_date)
    query += " ORDER BY validity_date"
    return pd.read_sql_query(query, con, params=params)


def get_category_summary(
    con: sqlite3.Connection,
    date: str | None = None,
) -> pd.DataFrame:
    """Get average returns per category (defaults to latest snapshot per fund)."""
    init_fund_performance_schema(con)
    if date:
        date_filter = "WHERE fp.validity_date = ?"
        params: list = [date]
    else:
        date_filter = """WHERE fp.validity_date = (
            SELECT MAX(fp2.validity_date) FROM fund_performance fp2
            WHERE fp2.fund_name = fp.fund_name
        )"""
        params = []
    query = f"""
        SELECT category, sector,
               COUNT(*) as fund_count,
               ROUND(AVG(return_ytd), 2) as avg_ytd,
               ROUND(AVG(return_30d), 2) as avg_30d,
               ROUND(AVG(return_90d), 2) as avg_90d,
               ROUND(AVG(return_365d), 2) as avg_1y,
               ROUND(MIN(return_ytd), 2) as worst_ytd,
               ROUND(MAX(return_ytd), 2) as best_ytd
        FROM fund_performance fp
        {date_filter}
        GROUP BY category, sector
        ORDER BY avg_ytd DESC
    """
    return pd.read_sql_query(query, con, params=params)


def get_vps_funds(
    con: sqlite3.Connection,
    date: str | None = None,
) -> pd.DataFrame:
    """Get VPS pension fund performance data."""
    init_fund_performance_schema(con)
    if date:
        date_filter = "AND fp.validity_date = ?"
        params: list = [date]
    else:
        date_filter = """AND fp.validity_date = (
            SELECT MAX(fp2.validity_date) FROM fund_performance fp2
            WHERE fp2.fund_name = fp.fund_name
        )"""
        params = []
    query = f"""
        SELECT fund_name, category, nav, rating,
               return_ytd, return_30d, return_90d,
               return_365d, return_2y, return_3y
        FROM fund_performance fp
        WHERE (sector LIKE '%VPS%' OR category LIKE 'VPS%')
          {date_filter}
        ORDER BY category, return_ytd DESC
    """
    return pd.read_sql_query(query, con, params=params)


def get_fund_performance_status(con: sqlite3.Connection) -> dict:
    """Get summary stats for the fund_performance table."""
    init_fund_performance_schema(con)
    try:
        total = con.execute("SELECT COUNT(*) FROM fund_performance").fetchone()[0]
        dates = con.execute("SELECT MIN(validity_date), MAX(validity_date), COUNT(DISTINCT validity_date) FROM fund_performance").fetchone()
        cats = con.execute("SELECT COUNT(DISTINCT category) FROM fund_performance").fetchone()[0]
        return {
            "total_rows": total,
            "earliest_date": dates[0],
            "latest_date": dates[1],
            "snapshot_days": dates[2],
            "categories": cats,
        }
    except Exception:
        return {"total_rows": 0}


# =============================================================================
# Bonds Functions
# =============================================================================


def upsert_bond(con: sqlite3.Connection, bond_data: dict) -> bool:
    """
    Upsert a bond into the bonds_master table.

    Args:
        con: Database connection
        bond_data: Dict with bond fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO bonds_master (
                bond_id, isin, symbol, issuer, bond_type, is_islamic,
                face_value, coupon_rate, coupon_frequency, issue_date,
                maturity_date, day_count, currency, is_active, source,
                notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      datetime('now'), datetime('now'))
            ON CONFLICT(bond_id) DO UPDATE SET
                isin = excluded.isin,
                symbol = excluded.symbol,
                issuer = excluded.issuer,
                bond_type = excluded.bond_type,
                is_islamic = excluded.is_islamic,
                face_value = excluded.face_value,
                coupon_rate = excluded.coupon_rate,
                coupon_frequency = excluded.coupon_frequency,
                issue_date = excluded.issue_date,
                maturity_date = excluded.maturity_date,
                day_count = excluded.day_count,
                currency = excluded.currency,
                is_active = excluded.is_active,
                source = excluded.source,
                notes = excluded.notes,
                updated_at = datetime('now')
        """, (
            bond_data.get("bond_id"),
            bond_data.get("isin"),
            bond_data.get("symbol"),
            bond_data.get("issuer"),
            bond_data.get("bond_type"),
            bond_data.get("is_islamic", 0),
            bond_data.get("face_value", 100),
            bond_data.get("coupon_rate"),
            bond_data.get("coupon_frequency", 2),
            bond_data.get("issue_date"),
            bond_data.get("maturity_date"),
            bond_data.get("day_count", "ACT/ACT"),
            bond_data.get("currency", "PKR"),
            bond_data.get("is_active", 1),
            bond_data.get("source", "MANUAL"),
            bond_data.get("notes"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_bonds(
    con: sqlite3.Connection,
    bond_type: str | None = None,
    issuer: str | None = None,
    is_islamic: bool | None = None,
    active_only: bool = True,
) -> list[dict]:
    """
    Get bonds with optional filters.

    Args:
        con: Database connection
        bond_type: Filter by bond type
        issuer: Filter by issuer
        is_islamic: Filter by Islamic/conventional
        active_only: Only return active bonds

    Returns:
        List of bond dicts
    """
    query = "SELECT * FROM bonds_master WHERE 1=1"
    params = []

    if active_only:
        query += " AND is_active = 1"
    if bond_type:
        query += " AND bond_type = ?"
        params.append(bond_type)
    if issuer:
        query += " AND issuer = ?"
        params.append(issuer)
    if is_islamic is not None:
        query += " AND is_islamic = ?"
        params.append(1 if is_islamic else 0)

    query += " ORDER BY maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_bond(con: sqlite3.Connection, bond_id: str) -> dict | None:
    """Get a single bond by ID."""
    try:
        cur = con.execute(
            "SELECT * FROM bonds_master WHERE bond_id = ?",
            (bond_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_bond_by_symbol(con: sqlite3.Connection, symbol: str) -> dict | None:
    """Get a bond by symbol."""
    try:
        cur = con.execute(
            "SELECT * FROM bonds_master WHERE symbol = ?",
            (symbol,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_bond_quote(con: sqlite3.Connection, quote_data: dict) -> bool:
    """
    Upsert a bond quote.

    Args:
        con: Database connection
        quote_data: Dict with bond_id, date, price/yield fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO bond_quotes (
                bond_id, date, price, dirty_price, ytm,
                bid_yield, ask_yield, bid_price, ask_price,
                volume, source, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(bond_id, date) DO UPDATE SET
                price = excluded.price,
                dirty_price = excluded.dirty_price,
                ytm = excluded.ytm,
                bid_yield = excluded.bid_yield,
                ask_yield = excluded.ask_yield,
                bid_price = excluded.bid_price,
                ask_price = excluded.ask_price,
                volume = excluded.volume,
                source = excluded.source,
                ingested_at = datetime('now')
        """, (
            quote_data.get("bond_id"),
            quote_data.get("date"),
            quote_data.get("price"),
            quote_data.get("dirty_price"),
            quote_data.get("ytm"),
            quote_data.get("bid_yield"),
            quote_data.get("ask_yield"),
            quote_data.get("bid_price"),
            quote_data.get("ask_price"),
            quote_data.get("volume"),
            quote_data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def upsert_bond_quotes_batch(
    con: sqlite3.Connection,
    quotes: list[dict]
) -> int:
    """
    Upsert multiple bond quotes.

    Args:
        con: Database connection
        quotes: List of quote dicts

    Returns:
        Number of rows upserted
    """
    count = 0
    for quote in quotes:
        if upsert_bond_quote(con, quote):
            count += 1
    return count


def get_bond_quotes(
    con: sqlite3.Connection,
    bond_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
) -> "pd.DataFrame":
    """
    Get bond quotes.

    Args:
        con: Database connection
        bond_id: Bond ID
        start_date: Start date filter
        end_date: End date filter
        limit: Max rows

    Returns:
        DataFrame with quotes
    """
    import pandas as pd

    query = "SELECT * FROM bond_quotes WHERE bond_id = ?"
    params = [bond_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_bond_latest_quote(con: sqlite3.Connection, bond_id: str) -> dict | None:
    """Get the latest quote for a bond."""
    try:
        cur = con.execute("""
            SELECT * FROM bond_quotes
            WHERE bond_id = ?
            ORDER BY date DESC
            LIMIT 1
        """, (bond_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_all_latest_quotes(
    con: sqlite3.Connection,
    bond_type: str | None = None,
) -> list[dict]:
    """Get latest quotes for all bonds with optional type filter."""
    query = """
        SELECT bq.*, bm.symbol, bm.issuer, bm.bond_type, bm.coupon_rate,
               bm.maturity_date, bm.is_islamic
        FROM bond_quotes bq
        JOIN bonds_master bm ON bq.bond_id = bm.bond_id
        WHERE bq.date = (
            SELECT MAX(date) FROM bond_quotes WHERE bond_id = bq.bond_id
        )
        AND bm.is_active = 1
    """
    params = []

    if bond_type:
        query += " AND bm.bond_type = ?"
        params.append(bond_type)

    query += " ORDER BY bm.maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def upsert_yield_curve_point(con: sqlite3.Connection, point_data: dict) -> bool:
    """
    Upsert a yield curve point.

    Args:
        con: Database connection
        point_data: Dict with curve_date, tenor_months, yield_rate, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO yield_curve_points (
                curve_date, tenor_months, yield_rate, bond_type,
                interpolation, computed_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(curve_date, tenor_months, bond_type) DO UPDATE SET
                yield_rate = excluded.yield_rate,
                interpolation = excluded.interpolation,
                computed_at = datetime('now')
        """, (
            point_data.get("curve_date"),
            point_data.get("tenor_months"),
            point_data.get("yield_rate"),
            point_data.get("bond_type", "PIB"),
            point_data.get("interpolation", "LINEAR"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_yield_curve(
    con: sqlite3.Connection,
    curve_date: str,
    bond_type: str = "PIB",
) -> list[dict]:
    """
    Get yield curve points for a date.

    Args:
        con: Database connection
        curve_date: Date for the curve
        bond_type: Bond type filter

    Returns:
        List of curve points sorted by tenor
    """
    try:
        cur = con.execute("""
            SELECT * FROM yield_curve_points
            WHERE curve_date = ? AND bond_type = ?
            ORDER BY tenor_months ASC
        """, (curve_date, bond_type))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_yield_curve(
    con: sqlite3.Connection,
    bond_type: str = "PIB",
) -> tuple[str | None, list[dict]]:
    """
    Get the most recent yield curve.

    Args:
        con: Database connection
        bond_type: Bond type filter

    Returns:
        Tuple of (curve_date, list of points)
    """
    try:
        # Get latest date
        cur = con.execute("""
            SELECT MAX(curve_date) FROM yield_curve_points
            WHERE bond_type = ?
        """, (bond_type,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None, []

        curve_date = row[0]
        points = get_yield_curve(con, curve_date, bond_type)
        return curve_date, points
    except Exception:
        return None, []


def upsert_bond_analytics(con: sqlite3.Connection, analytics: dict) -> bool:
    """
    Upsert bond analytics snapshot.

    Args:
        con: Database connection
        analytics: Dict with analytics fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO bond_analytics_snapshots (
                bond_id, as_of_date, price, ytm, duration,
                modified_duration, convexity, accrued_interest,
                spread_to_benchmark, days_to_maturity, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(bond_id, as_of_date) DO UPDATE SET
                price = excluded.price,
                ytm = excluded.ytm,
                duration = excluded.duration,
                modified_duration = excluded.modified_duration,
                convexity = excluded.convexity,
                accrued_interest = excluded.accrued_interest,
                spread_to_benchmark = excluded.spread_to_benchmark,
                days_to_maturity = excluded.days_to_maturity,
                computed_at = datetime('now')
        """, (
            analytics.get("bond_id"),
            analytics.get("as_of_date"),
            analytics.get("price"),
            analytics.get("ytm"),
            analytics.get("duration"),
            analytics.get("modified_duration"),
            analytics.get("convexity"),
            analytics.get("accrued_interest"),
            analytics.get("spread_to_benchmark"),
            analytics.get("days_to_maturity"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_bond_analytics(
    con: sqlite3.Connection,
    bond_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """Get bond analytics snapshot."""
    try:
        if as_of_date:
            cur = con.execute("""
                SELECT * FROM bond_analytics_snapshots
                WHERE bond_id = ? AND as_of_date = ?
            """, (bond_id, as_of_date))
        else:
            cur = con.execute("""
                SELECT * FROM bond_analytics_snapshots
                WHERE bond_id = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (bond_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def record_bond_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    items_total: int = 0
) -> bool:
    """Record the start of a bond sync run."""
    try:
        con.execute("""
            INSERT INTO bond_sync_runs (
                run_id, started_at, sync_type, status, items_total
            ) VALUES (?, datetime('now'), ?, 'running', ?)
        """, (run_id, sync_type, items_total))
        con.commit()
        return True
    except Exception:
        return False


def update_bond_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    items_ok: int = 0,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update a bond sync run."""
    try:
        con.execute("""
            UPDATE bond_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                items_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, items_ok, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_bond_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent bond sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM bond_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_bond_data_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of bond data in database.

    Returns:
        Dict with bond counts, date ranges, type breakdown, etc.
    """
    summary = {
        "total_bonds": 0,
        "active_bonds": 0,
        "bonds_with_quotes": 0,
        "total_quote_rows": 0,
        "bond_types": {},
        "issuers": {},
        "islamic_count": 0,
        "latest_quote_date": None,
        "earliest_quote_date": None,
        "yield_curve_dates": 0,
    }

    try:
        # Total and active bonds
        cur = con.execute("SELECT COUNT(*) FROM bonds_master")
        summary["total_bonds"] = cur.fetchone()[0]

        cur = con.execute("SELECT COUNT(*) FROM bonds_master WHERE is_active = 1")
        summary["active_bonds"] = cur.fetchone()[0]

        # Islamic bonds
        cur = con.execute(
            "SELECT COUNT(*) FROM bonds_master WHERE is_islamic = 1 AND is_active = 1"
        )
        summary["islamic_count"] = cur.fetchone()[0]

        # Bonds with quotes
        cur = con.execute("SELECT COUNT(DISTINCT bond_id) FROM bond_quotes")
        summary["bonds_with_quotes"] = cur.fetchone()[0]

        # Total quote rows
        cur = con.execute("SELECT COUNT(*) FROM bond_quotes")
        summary["total_quote_rows"] = cur.fetchone()[0]

        # Bond type breakdown
        cur = con.execute("""
            SELECT bond_type, COUNT(*) as count
            FROM bonds_master
            WHERE is_active = 1
            GROUP BY bond_type
            ORDER BY count DESC
        """)
        summary["bond_types"] = {row[0]: row[1] for row in cur.fetchall()}

        # Issuer breakdown
        cur = con.execute("""
            SELECT issuer, COUNT(*) as count
            FROM bonds_master
            WHERE is_active = 1
            GROUP BY issuer
            ORDER BY count DESC
        """)
        summary["issuers"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute("SELECT MAX(date), MIN(date) FROM bond_quotes")
        row = cur.fetchone()
        if row:
            summary["latest_quote_date"] = row[0]
            summary["earliest_quote_date"] = row[1]

        # Yield curve dates
        cur = con.execute("SELECT COUNT(DISTINCT curve_date) FROM yield_curve_points")
        summary["yield_curve_dates"] = cur.fetchone()[0]

    except Exception:
        pass

    return summary


# =============================================================================
# Sukuk/Debt Market Functions
# =============================================================================


def upsert_sukuk(con: sqlite3.Connection, sukuk_data: dict) -> bool:
    """
    Upsert a sukuk into the sukuk_master table.

    Args:
        con: Database connection
        sukuk_data: Dict with sukuk fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO sukuk_master (
                instrument_id, issuer, name, category, currency,
                issue_date, maturity_date, coupon_rate, coupon_frequency,
                face_value, issue_size, shariah_compliant, is_active,
                source, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      datetime('now'))
            ON CONFLICT(instrument_id) DO UPDATE SET
                issuer = excluded.issuer,
                name = excluded.name,
                category = excluded.category,
                currency = excluded.currency,
                issue_date = excluded.issue_date,
                maturity_date = excluded.maturity_date,
                coupon_rate = excluded.coupon_rate,
                coupon_frequency = excluded.coupon_frequency,
                face_value = excluded.face_value,
                issue_size = excluded.issue_size,
                shariah_compliant = excluded.shariah_compliant,
                is_active = excluded.is_active,
                source = excluded.source,
                notes = excluded.notes
        """, (
            sukuk_data.get("instrument_id"),
            sukuk_data.get("issuer"),
            sukuk_data.get("name"),
            sukuk_data.get("category"),
            sukuk_data.get("currency", "PKR"),
            sukuk_data.get("issue_date"),
            sukuk_data.get("maturity_date"),
            sukuk_data.get("coupon_rate"),
            sukuk_data.get("coupon_frequency"),
            sukuk_data.get("face_value", 100.0),
            sukuk_data.get("issue_size"),
            sukuk_data.get("shariah_compliant", 1),
            sukuk_data.get("is_active", 1),
            sukuk_data.get("source", "MANUAL"),
            sukuk_data.get("notes"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_list(
    con: sqlite3.Connection,
    category: str | None = None,
    issuer: str | None = None,
    shariah_only: bool = False,
    active_only: bool = True,
) -> list[dict]:
    """
    Get sukuk instruments with optional filters.

    Args:
        con: Database connection
        category: Filter by category (GOP_SUKUK, PIB, T-BILL, etc.)
        issuer: Filter by issuer
        shariah_only: Only return Shariah-compliant instruments
        active_only: Only return active instruments

    Returns:
        List of sukuk dicts
    """
    query = "SELECT * FROM sukuk_master WHERE 1=1"
    params = []

    if active_only:
        query += " AND is_active = 1"
    if category:
        query += " AND category = ?"
        params.append(category)
    if issuer:
        query += " AND issuer = ?"
        params.append(issuer)
    if shariah_only:
        query += " AND shariah_compliant = 1"

    query += " ORDER BY maturity_date ASC"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_sukuk(con: sqlite3.Connection, instrument_id: str) -> dict | None:
    """Get a single sukuk by instrument ID."""
    try:
        cur = con.execute(
            "SELECT * FROM sukuk_master WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sukuk_quote(con: sqlite3.Connection, quote_data: dict) -> bool:
    """
    Upsert a sukuk quote.

    Args:
        con: Database connection
        quote_data: Dict with quote fields

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT INTO sukuk_quotes (
                instrument_id, quote_date, clean_price, dirty_price,
                yield_to_maturity, bid_yield, ask_yield, volume,
                source, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(instrument_id, quote_date) DO UPDATE SET
                clean_price = excluded.clean_price,
                dirty_price = excluded.dirty_price,
                yield_to_maturity = excluded.yield_to_maturity,
                bid_yield = excluded.bid_yield,
                ask_yield = excluded.ask_yield,
                volume = excluded.volume,
                source = excluded.source,
                ingested_at = datetime('now')
        """, (
            quote_data.get("instrument_id"),
            quote_data.get("quote_date"),
            quote_data.get("clean_price"),
            quote_data.get("dirty_price"),
            quote_data.get("yield_to_maturity"),
            quote_data.get("bid_yield"),
            quote_data.get("ask_yield"),
            quote_data.get("volume"),
            quote_data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_quotes(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
) -> "pd.DataFrame":
    """Get sukuk quotes as DataFrame."""
    import pandas as pd

    query = "SELECT * FROM sukuk_quotes WHERE instrument_id = ?"
    params = [instrument_id]

    if start_date:
        query += " AND quote_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND quote_date <= ?"
        params.append(end_date)

    query += " ORDER BY quote_date DESC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def get_sukuk_latest_quote(
    con: sqlite3.Connection,
    instrument_id: str
) -> dict | None:
    """Get the latest quote for a sukuk."""
    try:
        cur = con.execute("""
            SELECT * FROM sukuk_quotes
            WHERE instrument_id = ?
            ORDER BY quote_date DESC
            LIMIT 1
        """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sukuk_yield_curve_point(
    con: sqlite3.Connection,
    point_data: dict
) -> bool:
    """Upsert a yield curve point."""
    try:
        con.execute("""
            INSERT INTO sukuk_yield_curve (
                curve_name, curve_date, tenor_days, yield_rate,
                source, computed_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(curve_name, curve_date, tenor_days) DO UPDATE SET
                yield_rate = excluded.yield_rate,
                source = excluded.source,
                computed_at = datetime('now')
        """, (
            point_data.get("curve_name"),
            point_data.get("curve_date"),
            point_data.get("tenor_days"),
            point_data.get("yield_rate"),
            point_data.get("source", "SBP"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_yield_curve(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str,
) -> list[dict]:
    """Get yield curve points for a specific curve and date."""
    try:
        cur = con.execute("""
            SELECT * FROM sukuk_yield_curve
            WHERE curve_name = ? AND curve_date = ?
            ORDER BY tenor_days ASC
        """, (curve_name, curve_date))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_sukuk_latest_yield_curve(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str | None = None,
) -> list[dict]:
    """
    Get yield curve for a curve name.

    Args:
        con: Database connection
        curve_name: Name of the curve (e.g., 'GOP_SUKUK')
        curve_date: Specific date (None = latest available)

    Returns:
        List of curve point dicts
    """
    try:
        if curve_date is None:
            # Get latest date
            cur = con.execute("""
                SELECT MAX(curve_date) FROM sukuk_yield_curve
                WHERE curve_name = ?
            """, (curve_name,))
            row = cur.fetchone()
            if not row or not row[0]:
                return []
            curve_date = row[0]

        points = get_sukuk_yield_curve(con, curve_name, curve_date)
        return points
    except Exception:
        return []


def get_available_curve_dates(
    con: sqlite3.Connection,
    curve_name: str,
    limit: int = 30,
) -> list[str]:
    """Get available curve dates for a curve name."""
    try:
        cur = con.execute("""
            SELECT DISTINCT curve_date FROM sukuk_yield_curve
            WHERE curve_name = ?
            ORDER BY curve_date DESC
            LIMIT ?
        """, (curve_name, limit))
        return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def upsert_sukuk_analytics(con: sqlite3.Connection, analytics: dict) -> bool:
    """Upsert sukuk analytics snapshot."""
    try:
        con.execute("""
            INSERT INTO sukuk_analytics_snapshots (
                instrument_id, as_of_date, price, ytm,
                macaulay_duration, modified_duration, convexity,
                accrued_interest, days_to_maturity, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(instrument_id, as_of_date) DO UPDATE SET
                price = excluded.price,
                ytm = excluded.ytm,
                macaulay_duration = excluded.macaulay_duration,
                modified_duration = excluded.modified_duration,
                convexity = excluded.convexity,
                accrued_interest = excluded.accrued_interest,
                days_to_maturity = excluded.days_to_maturity,
                computed_at = datetime('now')
        """, (
            analytics.get("instrument_id"),
            analytics.get("as_of_date"),
            analytics.get("price"),
            analytics.get("ytm"),
            analytics.get("macaulay_duration"),
            analytics.get("modified_duration"),
            analytics.get("convexity"),
            analytics.get("accrued_interest"),
            analytics.get("days_to_maturity"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_analytics(
    con: sqlite3.Connection,
    instrument_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """Get sukuk analytics snapshot."""
    try:
        if as_of_date:
            cur = con.execute("""
                SELECT * FROM sukuk_analytics_snapshots
                WHERE instrument_id = ? AND as_of_date = ?
            """, (instrument_id, as_of_date))
        else:
            cur = con.execute("""
                SELECT * FROM sukuk_analytics_snapshots
                WHERE instrument_id = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sbp_document(con: sqlite3.Connection, doc_data: dict) -> bool:
    """Upsert an SBP primary market document."""
    try:
        con.execute("""
            INSERT INTO sbp_primary_market_docs (
                doc_id, category, title, doc_date, url,
                local_path, file_size, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(doc_id) DO UPDATE SET
                category = excluded.category,
                title = excluded.title,
                doc_date = excluded.doc_date,
                url = excluded.url,
                local_path = excluded.local_path,
                file_size = excluded.file_size,
                fetched_at = datetime('now')
        """, (
            doc_data.get("doc_id"),
            doc_data.get("category"),
            doc_data.get("title"),
            doc_data.get("doc_date"),
            doc_data.get("url"),
            doc_data.get("local_path"),
            doc_data.get("file_size"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sbp_documents(
    con: sqlite3.Connection,
    category: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Get SBP primary market documents."""
    query = "SELECT * FROM sbp_primary_market_docs WHERE 1=1"
    params = []

    if category:
        query += " AND category = ?"
        params.append(category)

    query += " ORDER BY doc_date DESC, fetched_at DESC"
    query += f" LIMIT {limit}"

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def record_sukuk_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    items_total: int = 0
) -> bool:
    """Record the start of a sukuk sync run."""
    try:
        con.execute("""
            INSERT INTO sukuk_sync_runs (
                run_id, started_at, sync_type, status, items_total
            ) VALUES (?, datetime('now'), ?, 'running', ?)
        """, (run_id, sync_type, items_total))
        con.commit()
        return True
    except Exception:
        return False


def update_sukuk_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    items_ok: int = 0,
    rows_upserted: int = 0,
    error: str | None = None
) -> bool:
    """Update a sukuk sync run."""
    try:
        con.execute("""
            UPDATE sukuk_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                items_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, items_ok, rows_upserted, error, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_sukuk_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent sukuk sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM sukuk_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_sukuk_data_summary(con: sqlite3.Connection) -> dict:
    """
    Get summary of sukuk data in database.

    Returns:
        Dict with sukuk counts, date ranges, category breakdown, etc.
    """
    summary = {
        "total_instruments": 0,
        "active_instruments": 0,
        "shariah_compliant": 0,
        "instruments_with_quotes": 0,
        "total_quote_rows": 0,
        "categories": {},
        "issuers": {},
        "latest_quote_date": None,
        "earliest_quote_date": None,
        "yield_curves": 0,
        "sbp_documents": 0,
    }

    try:
        # Total and active instruments
        cur = con.execute("SELECT COUNT(*) FROM sukuk_master")
        summary["total_instruments"] = cur.fetchone()[0]

        cur = con.execute(
            "SELECT COUNT(*) FROM sukuk_master WHERE is_active = 1"
        )
        summary["active_instruments"] = cur.fetchone()[0]

        # Shariah compliant
        cur = con.execute(
            "SELECT COUNT(*) FROM sukuk_master WHERE shariah_compliant = 1"
        )
        summary["shariah_compliant"] = cur.fetchone()[0]

        # Instruments with quotes
        cur = con.execute(
            "SELECT COUNT(DISTINCT instrument_id) FROM sukuk_quotes"
        )
        summary["instruments_with_quotes"] = cur.fetchone()[0]

        # Total quote rows
        cur = con.execute("SELECT COUNT(*) FROM sukuk_quotes")
        summary["total_quote_rows"] = cur.fetchone()[0]

        # Category breakdown
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM sukuk_master
            WHERE is_active = 1
            GROUP BY category
            ORDER BY count DESC
        """)
        summary["categories"] = {row[0]: row[1] for row in cur.fetchall()}

        # Issuer breakdown
        cur = con.execute("""
            SELECT issuer, COUNT(*) as count
            FROM sukuk_master
            WHERE is_active = 1
            GROUP BY issuer
            ORDER BY count DESC
        """)
        summary["issuers"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute(
            "SELECT MAX(quote_date), MIN(quote_date) FROM sukuk_quotes"
        )
        row = cur.fetchone()
        if row:
            summary["latest_quote_date"] = row[0]
            summary["earliest_quote_date"] = row[1]

        # Yield curve count
        cur = con.execute(
            "SELECT COUNT(DISTINCT curve_name || curve_date) FROM sukuk_yield_curve"
        )
        summary["yield_curves"] = cur.fetchone()[0]

        # SBP documents
        cur = con.execute("SELECT COUNT(*) FROM sbp_primary_market_docs")
        summary["sbp_documents"] = cur.fetchone()[0]

    except Exception:
        pass

    return summary


# =============================================================================
# Fixed Income Generic CRUD Functions
# =============================================================================


def upsert_fi_instrument(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a fixed income instrument."""
    try:
        con.execute("""
            INSERT INTO fi_instruments (
                instrument_id, isin, issuer, name, category, currency,
                issue_date, maturity_date, coupon_rate, coupon_frequency,
                day_count, face_value, shariah_compliant, is_active, source,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(instrument_id) DO UPDATE SET
                isin = excluded.isin,
                issuer = excluded.issuer,
                name = excluded.name,
                category = excluded.category,
                currency = excluded.currency,
                issue_date = excluded.issue_date,
                maturity_date = excluded.maturity_date,
                coupon_rate = excluded.coupon_rate,
                coupon_frequency = excluded.coupon_frequency,
                day_count = excluded.day_count,
                face_value = excluded.face_value,
                shariah_compliant = excluded.shariah_compliant,
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = datetime('now')
        """, (
            data.get("instrument_id"),
            data.get("isin"),
            data.get("issuer", "GOVT_OF_PAKISTAN"),
            data.get("name"),
            data.get("category"),
            data.get("currency", "PKR"),
            data.get("issue_date"),
            data.get("maturity_date"),
            data.get("coupon_rate"),
            data.get("coupon_frequency"),
            data.get("day_count", "ACT/365"),
            data.get("face_value", 100.0),
            1 if data.get("shariah_compliant") else 0,
            1 if data.get("is_active", True) else 0,
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_instruments(
    con: sqlite3.Connection,
    category: str | None = None,
    active_only: bool = True,
    issuer: str | None = None,
) -> list[dict]:
    """Get fixed income instruments with optional filters."""
    try:
        query = "SELECT * FROM fi_instruments WHERE 1=1"
        params = []

        if active_only:
            query += " AND is_active = 1"

        if category:
            query += " AND category = ?"
            params.append(category)

        if issuer:
            query += " AND issuer LIKE ?"
            params.append(f"%{issuer}%")

        query += " ORDER BY maturity_date ASC"

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_instrument(con: sqlite3.Connection, instrument_id: str) -> dict | None:
    """Get a single fixed income instrument by ID."""
    try:
        cur = con.execute(
            "SELECT * FROM fi_instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fi_quote(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a fixed income quote."""
    try:
        con.execute("""
            INSERT INTO fi_quotes (
                instrument_id, quote_date, clean_price, ytm, bid, ask,
                volume, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, quote_date) DO UPDATE SET
                clean_price = excluded.clean_price,
                ytm = excluded.ytm,
                bid = excluded.bid,
                ask = excluded.ask,
                volume = excluded.volume,
                source = excluded.source,
                ingested_at = datetime('now')
        """, (
            data.get("instrument_id"),
            data.get("quote_date"),
            data.get("clean_price"),
            data.get("ytm"),
            data.get("bid"),
            data.get("ask"),
            data.get("volume"),
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_quotes(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 365,
) -> list[dict]:
    """Get quotes for an instrument."""
    try:
        query = "SELECT * FROM fi_quotes WHERE instrument_id = ?"
        params = [instrument_id]

        if start_date:
            query += " AND quote_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND quote_date <= ?"
            params.append(end_date)

        query += " ORDER BY quote_date DESC LIMIT ?"
        params.append(limit)

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_latest_quote(
    con: sqlite3.Connection,
    instrument_id: str,
) -> dict | None:
    """Get latest quote for an instrument."""
    try:
        cur = con.execute("""
            SELECT * FROM fi_quotes
            WHERE instrument_id = ?
            ORDER BY quote_date DESC
            LIMIT 1
        """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_fi_curve_point(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a yield curve point."""
    try:
        con.execute("""
            INSERT INTO fi_curves (curve_name, curve_date, tenor_days, rate, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(curve_name, curve_date, tenor_days) DO UPDATE SET
                rate = excluded.rate,
                source = excluded.source
        """, (
            data.get("curve_name"),
            data.get("curve_date"),
            data.get("tenor_days"),
            data.get("rate"),
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_curve(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str | None = None,
) -> list[dict]:
    """
    Get yield curve points.

    Args:
        con: Database connection
        curve_name: Name of curve (PKR_MTB, PKR_PIB, etc.)
        curve_date: Specific date (None = latest)

    Returns:
        List of curve points sorted by tenor
    """
    try:
        if curve_date is None:
            # Get latest date for this curve
            cur = con.execute("""
                SELECT MAX(curve_date) FROM fi_curves
                WHERE curve_name = ?
            """, (curve_name,))
            row = cur.fetchone()
            if not row or not row[0]:
                return []
            curve_date = row[0]

        cur = con.execute("""
            SELECT * FROM fi_curves
            WHERE curve_name = ? AND curve_date = ?
            ORDER BY tenor_days ASC
        """, (curve_name, curve_date))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_curve_dates(
    con: sqlite3.Connection,
    curve_name: str | None = None,
    limit: int = 30,
) -> list:
    """
    Get available curve dates.

    If curve_name is provided, returns list of date strings for that curve.
    If curve_name is None, returns list of dicts with curve summaries.
    """
    try:
        if curve_name:
            cur = con.execute("""
                SELECT DISTINCT curve_date FROM fi_curves
                WHERE curve_name = ?
                ORDER BY curve_date DESC
                LIMIT ?
            """, (curve_name, limit))
            return [row[0] for row in cur.fetchall()]
        else:
            # Return summary of all curves
            cur = con.execute("""
                SELECT curve_name,
                       MAX(curve_date) as latest_date,
                       COUNT(*) as count
                FROM fi_curves
                GROUP BY curve_name
                ORDER BY curve_name
            """)
            return [
                {"curve_name": row[0], "latest_date": row[1], "count": row[2]}
                for row in cur.fetchall()
            ]
    except Exception:
        return []


def upsert_fi_analytics(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert fixed income analytics snapshot."""
    try:
        con.execute("""
            INSERT INTO fi_analytics (
                instrument_id, as_of_date, price, ytm,
                macaulay_duration, modified_duration, convexity, pvbp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, as_of_date) DO UPDATE SET
                price = excluded.price,
                ytm = excluded.ytm,
                macaulay_duration = excluded.macaulay_duration,
                modified_duration = excluded.modified_duration,
                convexity = excluded.convexity,
                pvbp = excluded.pvbp
        """, (
            data.get("instrument_id"),
            data.get("as_of_date"),
            data.get("price"),
            data.get("ytm"),
            data.get("macaulay_duration"),
            data.get("modified_duration"),
            data.get("convexity"),
            data.get("pvbp"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_analytics(
    con: sqlite3.Connection,
    instrument_id: str,
    as_of_date: str | None = None,
) -> dict | None:
    """Get analytics for an instrument."""
    try:
        if as_of_date:
            cur = con.execute("""
                SELECT * FROM fi_analytics
                WHERE instrument_id = ? AND as_of_date = ?
            """, (instrument_id, as_of_date))
        else:
            cur = con.execute("""
                SELECT * FROM fi_analytics
                WHERE instrument_id = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """, (instrument_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def upsert_sbp_pma_doc(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert an SBP PMA document record."""
    try:
        con.execute("""
            INSERT INTO sbp_pma_docs (
                doc_id, category, title, doc_date, url, local_path, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
                category = excluded.category,
                title = excluded.title,
                doc_date = excluded.doc_date,
                url = excluded.url,
                local_path = excluded.local_path,
                fetched_at = datetime('now')
        """, (
            data.get("doc_id"),
            data.get("category"),
            data.get("title"),
            data.get("doc_date"),
            data.get("url"),
            data.get("local_path"),
            data.get("source", "SBP_PMA"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sbp_pma_docs(
    con: sqlite3.Connection,
    category: str | None = None,
    doc_type: str | None = None,
    since: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Get SBP PMA documents with optional filters."""
    try:
        query = "SELECT * FROM sbp_pma_docs WHERE 1=1"
        params = []

        if category:
            query += " AND category = ?"
            params.append(category)

        if doc_type:
            query += " AND doc_type = ?"
            params.append(doc_type)

        if since:
            query += " AND doc_date >= ?"
            params.append(since)

        query += " ORDER BY doc_date DESC, fetched_at DESC LIMIT ?"
        params.append(limit)

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def upsert_fi_event(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert a fixed income event."""
    try:
        con.execute("""
            INSERT INTO fi_events (
                event_id, category, event_date, label, notes, source_doc_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                category = excluded.category,
                event_date = excluded.event_date,
                label = excluded.label,
                notes = excluded.notes,
                source_doc_id = excluded.source_doc_id
        """, (
            data.get("event_id"),
            data.get("category"),
            data.get("event_date"),
            data.get("label"),
            data.get("notes"),
            data.get("source_doc_id"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def record_fi_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    sync_type: str,
    items_total: int = 0,
) -> bool:
    """Record start of a fixed income sync run."""
    try:
        con.execute("""
            INSERT INTO fi_sync_runs (run_id, started_at, sync_type, items_total)
            VALUES (?, datetime('now'), ?, ?)
        """, (run_id, sync_type, items_total))
        con.commit()
        return True
    except Exception:
        return False


def update_fi_sync_run(
    con: sqlite3.Connection,
    run_id: str,
    status: str,
    items_ok: int = 0,
    rows_upserted: int = 0,
    error_message: str | None = None,
) -> bool:
    """Update a fixed income sync run."""
    try:
        con.execute("""
            UPDATE fi_sync_runs SET
                ended_at = datetime('now'),
                status = ?,
                items_ok = ?,
                rows_upserted = ?,
                error_message = ?
            WHERE run_id = ?
        """, (status, items_ok, rows_upserted, error_message, run_id))
        con.commit()
        return True
    except Exception:
        return False


def get_fi_sync_runs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent fixed income sync runs."""
    try:
        cur = con.execute("""
            SELECT * FROM fi_sync_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_fi_data_summary(con: sqlite3.Connection) -> dict:
    """Get summary of fixed income data in database."""
    summary = {
        "total_instruments": 0,
        "active_instruments": 0,
        "instruments_with_quotes": 0,
        "total_quote_rows": 0,
        "categories": {},
        "latest_quote_date": None,
        "earliest_quote_date": None,
        "curve_count": 0,
        "sbp_doc_count": 0,
    }

    try:
        # Total and active instruments
        cur = con.execute("SELECT COUNT(*) FROM fi_instruments")
        summary["total_instruments"] = cur.fetchone()[0]

        cur = con.execute(
            "SELECT COUNT(*) FROM fi_instruments WHERE is_active = 1"
        )
        summary["active_instruments"] = cur.fetchone()[0]

        # Instruments with quotes
        cur = con.execute(
            "SELECT COUNT(DISTINCT instrument_id) FROM fi_quotes"
        )
        summary["instruments_with_quotes"] = cur.fetchone()[0]

        # Total quote rows
        cur = con.execute("SELECT COUNT(*) FROM fi_quotes")
        summary["total_quote_rows"] = cur.fetchone()[0]

        # Category breakdown
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM fi_instruments
            WHERE is_active = 1
            GROUP BY category
            ORDER BY count DESC
        """)
        summary["categories"] = {row[0]: row[1] for row in cur.fetchall()}

        # Date range
        cur = con.execute(
            "SELECT MAX(quote_date), MIN(quote_date) FROM fi_quotes"
        )
        row = cur.fetchone()
        if row:
            summary["latest_quote_date"] = row[0]
            summary["earliest_quote_date"] = row[1]

        # Curve count
        cur = con.execute(
            "SELECT COUNT(DISTINCT curve_name || curve_date) FROM fi_curves"
        )
        summary["curve_count"] = cur.fetchone()[0]

        # SBP documents
        cur = con.execute("SELECT COUNT(*) FROM sbp_pma_docs")
        summary["sbp_doc_count"] = cur.fetchone()[0]

    except Exception:
        pass

    return summary


# =============================================================================
# SBP Policy Rates and KIBOR CRUD Functions
# =============================================================================


def seed_sbp_policy_rates(con: sqlite3.Connection) -> int:
    """Seed historical SBP policy rate decisions (effective dates).

    Source: SBP Monetary Policy Statements archive.
    Rates are stored as percentages (e.g. 22.0 = 22%).
    Only inserts missing dates — existing rows are untouched.
    Returns number of rows inserted.
    """
    # (effective_date, policy_rate_pct)
    # Complete history of SBP policy rate changes
    _HISTORY = [
        # 2004-2008: Tightening cycle
        ("2004-11-15", 7.50),
        ("2005-04-15", 9.00),
        ("2006-07-29", 9.50),
        ("2008-05-22", 10.50),
        ("2008-07-31", 12.00),
        ("2008-08-13", 13.00),
        ("2008-11-13", 15.00),
        # 2009: Easing
        ("2009-04-21", 14.00),
        ("2009-08-14", 13.00),
        ("2009-11-24", 12.50),
        # 2010-2011: Tightening
        ("2010-07-30", 13.00),
        ("2010-09-29", 13.50),
        ("2010-11-30", 14.00),
        # 2011-2012: Easing
        ("2011-07-30", 13.50),
        ("2011-10-08", 12.00),
        ("2012-04-13", 12.00),
        ("2012-08-13", 10.50),
        ("2012-10-08", 10.00),
        ("2012-12-14", 9.50),
        # 2013-2014
        ("2013-06-22", 9.00),
        ("2013-09-14", 9.50),
        ("2013-11-18", 10.00),
        # 2014-2015: Easing
        ("2014-11-15", 10.00),
        ("2015-01-24", 9.50),
        ("2015-03-24", 8.00),
        ("2015-05-23", 7.00),
        ("2015-09-14", 6.50),
        ("2016-05-21", 6.25),
        ("2016-09-24", 5.75),
        # 2018-2019: Aggressive tightening
        ("2018-01-27", 6.00),
        ("2018-05-26", 6.50),
        ("2018-07-14", 7.50),
        ("2018-10-01", 8.50),
        ("2018-12-01", 10.00),
        ("2019-01-31", 10.25),
        ("2019-04-02", 10.75),
        ("2019-05-21", 12.25),
        ("2019-07-17", 13.25),
        # 2020: COVID easing
        ("2020-03-17", 12.50),
        ("2020-03-24", 11.00),
        ("2020-04-16", 9.00),
        ("2020-06-25", 7.00),
        # 2021-2023: Tightening
        ("2021-09-20", 7.25),
        ("2021-11-19", 8.75),
        ("2021-12-14", 9.75),
        ("2022-01-24", 9.75),
        ("2022-04-07", 12.25),
        ("2022-05-23", 13.75),
        ("2022-07-07", 15.00),
        ("2022-11-25", 16.00),
        ("2023-03-02", 20.00),
        ("2023-06-12", 21.00),
        ("2023-06-26", 22.00),
        # 2024-2025: Easing cycle
        ("2024-06-10", 20.50),
        ("2024-07-29", 19.50),
        ("2024-09-12", 17.50),
        ("2024-10-11", 17.50),
        ("2024-11-04", 15.00),
        ("2024-12-16", 13.00),
        ("2025-01-27", 12.00),
        ("2025-03-10", 12.00),
        ("2025-04-14", 12.00),
        ("2025-06-02", 11.00),
        ("2025-07-07", 11.00),
        ("2025-09-01", 10.50),
        ("2025-11-03", 10.50),
        ("2026-01-30", 10.50),
    ]

    inserted = 0
    for rate_date, rate in _HISTORY:
        try:
            con.execute(
                """INSERT OR IGNORE INTO sbp_policy_rates
                   (rate_date, policy_rate, source)
                   VALUES (?, ?, 'SEED')""",
                (rate_date, rate),
            )
            inserted += con.total_changes  # approximate
        except Exception:
            pass
    con.commit()

    # Return actual row count
    count = con.execute("SELECT COUNT(*) FROM sbp_policy_rates").fetchone()[0]
    return count


def upsert_policy_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert SBP policy rate data."""
    try:
        con.execute("""
            INSERT INTO sbp_policy_rates (
                rate_date, policy_rate, ceiling_rate, floor_rate,
                overnight_repo_rate, source
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(rate_date) DO UPDATE SET
                policy_rate = excluded.policy_rate,
                ceiling_rate = excluded.ceiling_rate,
                floor_rate = excluded.floor_rate,
                overnight_repo_rate = excluded.overnight_repo_rate,
                source = excluded.source
        """, (
            data.get("rate_date"),
            data.get("policy_rate"),
            data.get("ceiling_rate"),
            data.get("floor_rate"),
            data.get("overnight_repo_rate"),
            data.get("source", "SBP_MSM"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_latest_policy_rate(con: sqlite3.Connection) -> dict | None:
    """Get the latest SBP policy rate."""
    try:
        cur = con.execute("""
            SELECT * FROM sbp_policy_rates
            ORDER BY rate_date DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_policy_rates(
    con: sqlite3.Connection,
    since: str | None = None,
    limit: int = 30,
) -> list[dict]:
    """Get SBP policy rate history."""
    try:
        if since:
            cur = con.execute("""
                SELECT * FROM sbp_policy_rates
                WHERE rate_date >= ?
                ORDER BY rate_date DESC
                LIMIT ?
            """, (since, limit))
        else:
            cur = con.execute("""
                SELECT * FROM sbp_policy_rates
                ORDER BY rate_date DESC
                LIMIT ?
            """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def upsert_kibor_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Upsert KIBOR rate data."""
    try:
        con.execute("""
            INSERT INTO kibor_rates (
                rate_date, tenor_months, bid, offer, source
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(rate_date, tenor_months) DO UPDATE SET
                bid = excluded.bid,
                offer = excluded.offer,
                source = excluded.source
        """, (
            data.get("rate_date"),
            data.get("tenor_months"),
            data.get("bid"),
            data.get("offer"),
            data.get("source", "SBP_MSM"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_kibor_rates(
    con: sqlite3.Connection,
    rate_date: str | None = None,
    tenor_months: int | None = None,
    limit: int = 30,
) -> list[dict]:
    """Get KIBOR rates with optional filters."""
    try:
        query = "SELECT * FROM kibor_rates WHERE 1=1"
        params = []

        if rate_date:
            query += " AND rate_date = ?"
            params.append(rate_date)

        if tenor_months:
            query += " AND tenor_months = ?"
            params.append(tenor_months)

        query += " ORDER BY rate_date DESC, tenor_months ASC LIMIT ?"
        params.append(limit)

        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_kibor_rates(con: sqlite3.Connection) -> list[dict]:
    """Get the latest KIBOR rates for all tenors."""
    try:
        cur = con.execute("""
            SELECT * FROM kibor_rates
            WHERE rate_date = (SELECT MAX(rate_date) FROM kibor_rates)
            ORDER BY tenor_months ASC
        """)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
