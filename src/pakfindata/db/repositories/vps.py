"""VPS (Voluntary Pension System) query functions.

VPS funds are stored in the existing mutual_funds / mutual_fund_nav tables
with fund_type = 'VPS'. This module provides VPS-specific query helpers.
"""

import sqlite3

import pandas as pd

__all__ = [
    "get_vps_funds",
    "get_vps_nav_history",
    "compare_vps_performance",
    "get_vps_summary",
]


def get_vps_funds(con: sqlite3.Connection) -> pd.DataFrame:
    """Get all VPS pension funds."""
    return pd.read_sql_query(
        """SELECT fund_id, symbol, fund_name, amc_name, category,
                  is_shariah, launch_date, is_active
           FROM mutual_funds
           WHERE fund_type = 'VPS'
           ORDER BY fund_name""",
        con,
    )


def get_vps_nav_history(
    con: sqlite3.Connection,
    fund_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get NAV history for a VPS fund.

    Args:
        fund_id: VPS fund ID (e.g. 'MUFAP:ABL-VPS-EQ').
        start_date: Start date filter.
        end_date: End date filter.
    """
    query = "SELECT * FROM mutual_fund_nav WHERE fund_id = ?"
    params: list = [fund_id]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"
    return pd.read_sql_query(query, con, params=params)


def compare_vps_performance(
    con: sqlite3.Connection,
    days: int = 365,
) -> pd.DataFrame:
    """Compare VPS fund performance over a period.

    Returns DataFrame with fund_id, fund_name, latest_nav, return_pct.
    """
    return pd.read_sql_query(
        """
        WITH vps_funds AS (
            SELECT fund_id, fund_name FROM mutual_funds WHERE fund_type = 'VPS'
        ),
        latest_nav AS (
            SELECT n.fund_id, n.nav, n.date
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MAX(date) as max_date
                FROM mutual_fund_nav
                GROUP BY fund_id
            ) ln ON n.fund_id = ln.fund_id AND n.date = ln.max_date
        ),
        old_nav AS (
            SELECT n.fund_id, n.nav, n.date
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MIN(date) as min_date
                FROM mutual_fund_nav
                WHERE date >= date('now', ? || ' days')
                GROUP BY fund_id
            ) od ON n.fund_id = od.fund_id AND n.date = od.min_date
        )
        SELECT f.fund_id, f.fund_name,
               l.nav as latest_nav, l.date as latest_date,
               o.nav as old_nav, o.date as old_date,
               ROUND((l.nav - o.nav) / o.nav * 100, 2) as return_pct
        FROM vps_funds f
        INNER JOIN latest_nav l ON f.fund_id = l.fund_id
        LEFT JOIN old_nav o ON f.fund_id = o.fund_id
        ORDER BY return_pct DESC
        """,
        con,
        params=(f"-{days}",),
    )


def get_vps_summary(con: sqlite3.Connection) -> dict:
    """Get VPS data summary statistics."""
    funds_row = con.execute(
        "SELECT COUNT(*) as cnt FROM mutual_funds WHERE fund_type = 'VPS'"
    ).fetchone()
    nav_row = con.execute(
        """SELECT COUNT(*) as cnt, MIN(date) as min_date, MAX(date) as max_date
           FROM mutual_fund_nav n
           INNER JOIN mutual_funds f ON n.fund_id = f.fund_id
           WHERE f.fund_type = 'VPS'"""
    ).fetchone()
    return {
        "total_funds": funds_row["cnt"],
        "total_nav_records": nav_row["cnt"],
        "earliest_date": nav_row["min_date"],
        "latest_date": nav_row["max_date"],
    }
