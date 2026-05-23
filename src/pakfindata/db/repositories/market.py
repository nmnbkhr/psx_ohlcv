"""Market-wide data repository (indices, summaries, breadth)."""

import sqlite3


# =============================================================================
# Index Functions
# =============================================================================


def upsert_index_data(con: sqlite3.Connection, index_data: dict) -> bool:
    """
    Insert or update index data.

    Args:
        con: Database connection
        index_data: Dict with index_code, index_date, value, etc.

    Returns:
        True if successful
    """
    try:
        con.execute("""
            INSERT OR REPLACE INTO psx_indices (
                index_code, index_date, index_time,
                value, change, change_pct,
                open, high, low, volume,
                previous_close,
                ytd_change_pct, one_year_change_pct,
                week_52_low, week_52_high,
                trades, market_cap, turnover,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            index_data.get("index_code"),
            index_data.get("index_date"),
            index_data.get("index_time"),
            index_data.get("value"),
            index_data.get("change"),
            index_data.get("change_pct"),
            index_data.get("open"),
            index_data.get("high"),
            index_data.get("low"),
            index_data.get("volume"),
            index_data.get("previous_close"),
            index_data.get("ytd_change_pct"),
            index_data.get("one_year_change_pct"),
            index_data.get("week_52_low"),
            index_data.get("week_52_high"),
            index_data.get("trades"),
            index_data.get("market_cap"),
            index_data.get("turnover"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_latest_index(con: sqlite3.Connection, index_code: str = "KSE100") -> dict | None:
    """
    Get the latest data for any index.

    Args:
        con: Database connection
        index_code: Index code (KSE100, KSE30, KMI30, etc.)

    Returns:
        Dict with index data or None
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_indices
            WHERE index_code = ?
            ORDER BY index_date DESC, index_time DESC
            LIMIT 1
        """, (index_code,))
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_index_history(
    con: sqlite3.Connection,
    index_code: str = "KSE100",
    days: int = 30
) -> list[dict]:
    """
    Get index history for a specified number of days.

    Args:
        con: Database connection
        index_code: Index code
        days: Number of days of history

    Returns:
        List of dicts with daily index values
    """
    try:
        cur = con.execute("""
            SELECT DISTINCT index_date, value, change, change_pct, high, low, volume
            FROM psx_indices
            WHERE index_code = ?
            ORDER BY index_date DESC
            LIMIT ?
        """, (index_code, days))
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_index_history_range(
    con: sqlite3.Connection,
    index_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 5000,
) -> list[dict]:
    """Return one index's daily history within an explicit date range.

    Complements :func:`get_index_history`, which takes ``days=N``; the
    API contract surfaces ``from``/``to`` so callers can pin a window.

    Args:
        con: Database connection
        index_code: Index code (case-sensitive; caller normalizes)
        start_date: Inclusive lower bound (YYYY-MM-DD); None = unbounded
        end_date: Inclusive upper bound (YYYY-MM-DD); None = unbounded
        limit: Safety cap on rows returned

    Returns:
        List of dicts, newest first; empty list on error or no data.
    """
    sql = (
        "SELECT DISTINCT index_date, value, change, change_pct, "
        "high, low, volume "
        "FROM psx_indices WHERE index_code = ?"
    )
    params: list = [index_code]
    if start_date:
        sql += " AND index_date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND index_date <= ?"
        params.append(end_date)
    sql += " ORDER BY index_date DESC LIMIT ?"
    params.append(limit)
    try:
        cur = con.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_all_latest_indices(con: sqlite3.Connection) -> list[dict]:
    """
    Get latest data for all indices.

    Returns:
        List of dicts with index data, one row per index code.
    """
    # Window-function "latest per group" — robust to the case where the
    # day's MAX(index_time) doesn't fall on the index's MAX(index_date).
    # The previous composite-key IN-clause silently dropped any index
    # whose latest-time row wasn't on its latest-date.
    try:
        cur = con.execute("""
            SELECT * FROM (
                SELECT pi.*, ROW_NUMBER() OVER (
                    PARTITION BY index_code
                    ORDER BY index_date DESC, index_time DESC
                ) AS _rn
                FROM psx_indices pi
            ) WHERE _rn = 1
            ORDER BY
                CASE index_code
                    WHEN 'KSE100' THEN 1
                    WHEN 'KSE30' THEN 2
                    WHEN 'KMI30' THEN 3
                    ELSE 4
                END,
                index_code
        """)
        return [{k: v for k, v in dict(row).items() if k != "_rn"}
                for row in cur.fetchall()]
    except Exception:
        return []


def get_latest_kse100(con: sqlite3.Connection) -> dict | None:
    """
    Get the latest KSE-100 index data.

    Returns:
        Dict with index data or None if not available
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_indices
            WHERE index_code = 'KSE100'
            ORDER BY index_date DESC, index_time DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


# =============================================================================
# Market Stats Functions
# =============================================================================


def get_latest_market_stats(con: sqlite3.Connection) -> dict | None:
    """
    Get the latest market stats (trading segments).

    Returns:
        Dict with segment data or None
    """
    try:
        cur = con.execute("""
            SELECT * FROM psx_market_stats
            ORDER BY stat_date DESC, stat_time DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


# =============================================================================
# Yield Curve Functions
# =============================================================================


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
