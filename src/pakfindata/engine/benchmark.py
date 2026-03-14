"""
Benchmark Data Resolution for Fund Analytics.

Resolves benchmark names to NAV/index time series from DB.

Usage:
    from pakfindata.engine.benchmark import get_benchmark_nav

    kse100 = get_benchmark_nav(con, "KSE-100", start_date, end_date)
"""

from __future__ import annotations

from datetime import date

import pandas as pd


# Mapping from benchmark names to DB query strategies
_BENCHMARK_MAP = {
    # Index-based benchmarks (from psx_indices table)
    "KSE-100": {"table": "psx_indices", "symbol": "KSE100", "col": "value", "date_col": "index_date"},
    "KSE100": {"table": "psx_indices", "symbol": "KSE100", "col": "value", "date_col": "index_date"},
    "KSE-30": {"table": "psx_indices", "symbol": "KSE30", "col": "value", "date_col": "index_date"},
    "KSE30": {"table": "psx_indices", "symbol": "KSE30", "col": "value", "date_col": "index_date"},
    "KMI-30": {"table": "psx_indices", "symbol": "KMI30", "col": "value", "date_col": "index_date"},
    "KMI30": {"table": "psx_indices", "symbol": "KMI30", "col": "value", "date_col": "index_date"},
    "ALLSHR": {"table": "psx_indices", "symbol": "ALLSHR", "col": "value", "date_col": "index_date"},
}

# Fund-based proxies (from mutual_fund_nav)
_FUND_PROXIES = {
    "KSE-100": ["NIT Islamic Equity Fund", "NIT Index Fund"],
    "KMI-30": ["Meezan Islamic Fund", "Al Meezan Mutual Fund"],
}


def get_benchmark_nav(
    con,
    benchmark: str,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> pd.Series:
    """Resolve benchmark name to NAV/index time series.

    Tries index data first, then falls back to fund proxy NAV.

    Args:
        con: SQLite connection.
        benchmark: Benchmark name (e.g., "KSE-100", "KMI-30").
        start_date: Optional start date filter.
        end_date: Optional end date filter.

    Returns:
        pd.Series with DatetimeIndex and float values, sorted by date.
        Empty Series if no data found.
    """
    # Normalize benchmark name
    bm = benchmark.upper().replace(" ", "")

    # Try index table first
    spec = _BENCHMARK_MAP.get(bm) or _BENCHMARK_MAP.get(benchmark)
    if spec:
        series = _query_index(con, spec, start_date, end_date)
        if not series.empty:
            return series

    # Fallback: try fund proxy
    proxy_names = _FUND_PROXIES.get(benchmark, [])
    for fund_name in proxy_names:
        series = _query_fund_nav(con, fund_name, start_date, end_date)
        if not series.empty:
            return series

    return pd.Series(dtype=float)


def _query_index(con, spec: dict, start_date, end_date) -> pd.Series:
    """Query index table for benchmark values."""
    try:
        where = [f"symbol = '{spec['symbol']}'", f"{spec['col']} > 0"]
        if start_date:
            where.append(f"{spec['date_col']} >= '{start_date}'")
        if end_date:
            where.append(f"{spec['date_col']} <= '{end_date}'")

        q = f"""
            SELECT {spec['date_col']} as date, {spec['col']} as value
            FROM {spec['table']}
            WHERE {' AND '.join(where)}
            ORDER BY {spec['date_col']}
        """
        df = pd.read_sql_query(q, con)
        if df.empty:
            return pd.Series(dtype=float)
        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")["value"]
    except Exception:
        return pd.Series(dtype=float)


def _query_fund_nav(con, fund_name: str, start_date, end_date) -> pd.Series:
    """Query mutual_fund_nav for proxy benchmark NAV."""
    try:
        # Find fund_id from fund_name
        row = con.execute(
            "SELECT fund_id FROM mutual_funds WHERE fund_name LIKE ? LIMIT 1",
            (f"%{fund_name}%",),
        ).fetchone()
        if not row:
            return pd.Series(dtype=float)

        fund_id = row[0] if isinstance(row, tuple) else row["fund_id"]
        where = [f"fund_id = '{fund_id}'"]
        if start_date:
            where.append(f"date >= '{start_date}'")
        if end_date:
            where.append(f"date <= '{end_date}'")

        q = f"""
            SELECT date, nav as value
            FROM mutual_fund_nav
            WHERE {' AND '.join(where)} AND nav > 0
            ORDER BY date
        """
        df = pd.read_sql_query(q, con)
        if df.empty:
            return pd.Series(dtype=float)
        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")["value"]
    except Exception:
        return pd.Series(dtype=float)


def get_risk_free_rate(con, rate_name: str = "KIBOR-6M") -> float:
    """Get current risk-free rate from DB or return default.

    Args:
        con: SQLite connection.
        rate_name: Rate identifier.

    Returns:
        Annualized rate as decimal (e.g., 0.1208 for 12.08%).
    """
    try:
        # Try SBP rates table
        row = con.execute("""
            SELECT rate FROM sbp_rates
            WHERE name LIKE '%KIBOR%6%'
            ORDER BY date DESC LIMIT 1
        """).fetchone()
        if row:
            val = row[0] if isinstance(row, tuple) else row["rate"]
            return float(val) / 100 if val > 1 else float(val)
    except Exception:
        pass

    # Default KIBOR 6M
    return 0.1208
