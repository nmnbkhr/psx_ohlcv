"""
Mutual Fund Analytics module for Phase 2.5.

This module provides mutual fund analytics:
- NAV return calculations (1W, 1M, 3M, 6M, 1Y)
- Category performance comparison
- Volatility and risk metrics
- Sharpe ratio calculation

All analytics are READ-ONLY and for informational purposes only.
No investment recommendations or trading signals.
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from .db import (
    get_mf_latest_nav,
    get_mf_nav,
    get_mutual_fund,
    get_mutual_funds,
)


def compute_nav_returns(df: pd.DataFrame, periods: list[int] | None = None) -> dict:
    """
    Compute NAV returns for various periods.

    Args:
        df: DataFrame with 'date' and 'nav' columns, sorted by date DESC
        periods: List of periods in trading days (default: [5, 21, 63, 126, 252])
            5 = ~1 week, 21 = ~1 month, 63 = ~3 months, 126 = ~6 months, 252 = ~1 year

    Returns:
        Dict with period keys and return values (as decimals)
    """
    if df.empty or "nav" not in df.columns:
        return {}

    if periods is None:
        periods = [5, 21, 63, 126, 252]  # 1W, 1M, 3M, 6M, 1Y

    # Ensure sorted by date descending
    df = df.sort_values("date", ascending=False).reset_index(drop=True)

    results = {}
    period_labels = {5: "1W", 21: "1M", 63: "3M", 126: "6M", 252: "1Y"}

    current_nav = df["nav"].iloc[0] if len(df) > 0 else None

    if current_nav is None or current_nav == 0:
        return {}

    for period in periods:
        label = period_labels.get(period, f"{period}D")
        key = f"return_{label}"

        if len(df) > period:
            past_nav = df["nav"].iloc[period]
            if past_nav and past_nav != 0:
                ret = (current_nav - past_nav) / past_nav
                results[key] = round(ret, 6)

    return results


def compute_nav_volatility(df: pd.DataFrame, windows: list[int] | None = None) -> dict:
    """
    Compute NAV volatility (annualized standard deviation of returns).

    Args:
        df: DataFrame with 'date' and 'nav' columns
        windows: List of rolling windows in days (default: [21, 63])

    Returns:
        Dict with window keys and volatility values (as decimals)
    """
    if df.empty or "nav" not in df.columns or len(df) < 5:
        return {}

    if windows is None:
        windows = [21, 63]  # 1M, 3M

    # Sort by date ascending for proper return calculation
    df = df.sort_values("date", ascending=True).copy()

    # Calculate daily returns
    df["return"] = df["nav"].pct_change()

    results = {}
    window_labels = {21: "1M", 63: "3M", 126: "6M", 252: "1Y"}

    for window in windows:
        label = window_labels.get(window, f"{window}D")
        key = f"vol_{label}"

        if len(df) >= window:
            recent_returns = df["return"].tail(window).dropna()
            if len(recent_returns) >= window // 2:
                std = recent_returns.std()
                # Annualize: multiply by sqrt(252)
                annualized_vol = std * (252 ** 0.5)
                results[key] = round(annualized_vol, 6)

    return results


def compute_sharpe_ratio(
    df: pd.DataFrame,
    risk_free_rate: float = 0.15,
    period_days: int = 252,
) -> float | None:
    """
    Compute Sharpe ratio.

    Args:
        df: DataFrame with NAV data
        risk_free_rate: Annual risk-free rate (default: 15% KIBOR approx)
        period_days: Period for calculation (default: 252 = 1 year)

    Returns:
        Sharpe ratio or None if insufficient data
    """
    if df.empty or "nav" not in df.columns or len(df) < period_days // 2:
        return None

    # Sort by date ascending
    df = df.sort_values("date", ascending=True).copy()

    # Calculate daily returns
    df["return"] = df["nav"].pct_change()

    # Get recent returns
    recent_returns = df["return"].tail(period_days).dropna()

    if len(recent_returns) < period_days // 2:
        return None

    # Annualized return
    mean_daily_return = recent_returns.mean()
    annualized_return = mean_daily_return * 252

    # Annualized volatility
    daily_vol = recent_returns.std()
    annualized_vol = daily_vol * (252 ** 0.5)

    if annualized_vol == 0:
        return None

    # Sharpe ratio
    sharpe = (annualized_return - risk_free_rate) / annualized_vol

    return round(sharpe, 4)


def compute_max_drawdown(df: pd.DataFrame) -> dict:
    """
    Compute maximum drawdown from NAV series.

    Args:
        df: DataFrame with 'date' and 'nav' columns

    Returns:
        Dict with max_drawdown, drawdown_start, drawdown_end
    """
    if df.empty or "nav" not in df.columns or len(df) < 5:
        return {}

    # Sort by date ascending
    df = df.sort_values("date", ascending=True).reset_index(drop=True)

    # Calculate running maximum
    df["running_max"] = df["nav"].cummax()

    # Calculate drawdown
    df["drawdown"] = (df["nav"] - df["running_max"]) / df["running_max"]

    # Find max drawdown
    max_dd_idx = df["drawdown"].idxmin()
    max_dd = df["drawdown"].iloc[max_dd_idx]

    if max_dd == 0:
        return {"max_drawdown": 0.0}

    # Find drawdown start (peak before trough)
    peak_idx = df.loc[:max_dd_idx, "nav"].idxmax()
    peak_date = df.loc[peak_idx, "date"]
    trough_date = df.loc[max_dd_idx, "date"]

    return {
        "max_drawdown": round(max_dd, 6),
        "drawdown_start": peak_date,
        "drawdown_end": trough_date,
    }


def get_mf_analytics(
    con: sqlite3.Connection,
    fund_id: str,
) -> dict:
    """
    Get comprehensive analytics for a mutual fund.

    Args:
        con: Database connection
        fund_id: Mutual fund ID

    Returns:
        Dict with returns, volatility, Sharpe ratio, drawdown, etc.
    """
    # Get fund info
    fund = get_mutual_fund(con, fund_id)
    if not fund:
        return {"fund_id": fund_id, "error": "fund_not_found"}

    # Get NAV data
    df = get_mf_nav(con, fund_id, limit=300)

    if df.empty:
        return {
            "fund_id": fund_id,
            "fund_name": fund.get("fund_name"),
            "category": fund.get("category"),
            "error": "no_nav_data",
        }

    analytics = {
        "fund_id": fund_id,
        "symbol": fund.get("symbol"),
        "fund_name": fund.get("fund_name"),
        "category": fund.get("category"),
        "amc_name": fund.get("amc_name"),
        "is_shariah": fund.get("is_shariah"),
        "expense_ratio": fund.get("expense_ratio"),
    }

    # Latest NAV
    latest = get_mf_latest_nav(con, fund_id)
    if latest:
        analytics["latest_date"] = latest.get("date")
        analytics["latest_nav"] = latest.get("nav")
        analytics["latest_aum"] = latest.get("aum")

    # Returns
    returns = compute_nav_returns(df)
    analytics.update(returns)

    # Volatility
    volatility = compute_nav_volatility(df)
    analytics.update(volatility)

    # Sharpe ratio
    sharpe = compute_sharpe_ratio(df)
    if sharpe is not None:
        analytics["sharpe_ratio"] = sharpe

    # Max drawdown
    drawdown = compute_max_drawdown(df)
    analytics.update(drawdown)

    return analytics


def get_category_performance(
    con: sqlite3.Connection,
    category: str,
    period: str = "1M",
    top_n: int = 10,
) -> list[dict]:
    """
    Get performance leaderboard for a category.

    Args:
        con: Database connection
        category: Category to analyze
        period: Return period (1W, 1M, 3M, 6M, 1Y)
        top_n: Number of top performers to return

    Returns:
        List of fund performance dicts sorted by return
    """
    # Get funds in category
    funds = get_mutual_funds(con, category=category, active_only=True)

    if not funds:
        return []

    period_days = {"1W": 5, "1M": 21, "3M": 63, "6M": 126, "1Y": 252}
    days = period_days.get(period, 21)

    results = []

    for fund in funds:
        fund_id = fund["fund_id"]
        df = get_mf_nav(con, fund_id, limit=days + 10)

        if df.empty:
            continue

        # Calculate return
        returns = compute_nav_returns(df, [days])
        ret_key = f"return_{period}"
        ret_value = returns.get(ret_key)

        if ret_value is not None:
            latest = get_mf_latest_nav(con, fund_id)

            results.append({
                "fund_id": fund_id,
                "symbol": fund.get("symbol"),
                "fund_name": fund.get("fund_name"),
                "amc_name": fund.get("amc_name"),
                "category": category,
                "is_shariah": fund.get("is_shariah"),
                f"return_{period}": ret_value,
                "return_pct": round(ret_value * 100, 2),
                "latest_nav": latest.get("nav") if latest else None,
                "latest_date": latest.get("date") if latest else None,
            })

    # Sort by return descending
    results.sort(key=lambda x: x.get(f"return_{period}") or -999, reverse=True)

    # Add rank
    for i, r in enumerate(results):
        r["rank"] = i + 1
        r["category_total"] = len(results)

    return results[:top_n]


def get_category_summary(
    con: sqlite3.Connection,
    category: str,
    period: str = "1M",
) -> dict:
    """
    Get summary statistics for a category.

    Args:
        con: Database connection
        category: Category to analyze
        period: Return period

    Returns:
        Dict with category stats
    """
    performance = get_category_performance(con, category, period, top_n=100)

    if not performance:
        return {"category": category, "error": "no_data"}

    ret_key = f"return_{period}"
    returns = [p.get(ret_key) for p in performance if p.get(ret_key) is not None]

    if not returns:
        return {"category": category, "error": "no_returns"}

    return {
        "category": category,
        "period": period,
        "fund_count": len(performance),
        "avg_return": round(sum(returns) / len(returns), 6),
        "avg_return_pct": round((sum(returns) / len(returns)) * 100, 2),
        "max_return": round(max(returns), 6),
        "max_return_pct": round(max(returns) * 100, 2),
        "min_return": round(min(returns), 6),
        "min_return_pct": round(min(returns) * 100, 2),
        "best_fund": performance[0]["fund_name"] if performance else None,
        "best_fund_symbol": performance[0]["symbol"] if performance else None,
        "worst_fund": performance[-1]["fund_name"] if performance else None,
        "worst_fund_symbol": performance[-1]["symbol"] if performance else None,
    }


def compare_funds(
    con: sqlite3.Connection,
    fund_ids: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    base: float = 100.0,
) -> pd.DataFrame:
    """
    Compare multiple funds' normalized performance.

    Args:
        con: Database connection
        fund_ids: List of fund IDs to compare
        start_date: Start date
        end_date: End date
        base: Base value for normalization (default: 100)

    Returns:
        DataFrame with date index and columns for each fund
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    dfs = []
    for fund_id in fund_ids:
        df = get_mf_nav(con, fund_id, start_date=start_date, end_date=end_date)
        if not df.empty:
            df = df.sort_values("date").set_index("date")
            first_nav = df["nav"].iloc[0]
            if first_nav and first_nav != 0:
                # Get fund symbol for column name
                fund = get_mutual_fund(con, fund_id)
                col_name = fund.get("symbol", fund_id) if fund else fund_id
                df[col_name] = (df["nav"] / first_nav) * base
                dfs.append(df[[col_name]])

    if not dfs:
        return pd.DataFrame()

    result = dfs[0]
    for df in dfs[1:]:
        result = result.join(df, how="outer")

    return result.sort_index()


def get_aum_trends(
    con: sqlite3.Connection,
    fund_id: str | None = None,
    category: str | None = None,
    days: int = 365,
) -> pd.DataFrame:
    """
    Get AUM trends over time.

    Args:
        con: Database connection
        fund_id: Specific fund ID, or None for industry total
        category: Filter by category
        days: Number of days to include

    Returns:
        DataFrame with date and AUM columns
    """
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    if fund_id:
        # Single fund AUM
        df = get_mf_nav(con, fund_id, start_date=start_date)
        if df.empty:
            return pd.DataFrame()
        return df[["date", "aum"]].dropna().sort_values("date")

    # Aggregate AUM by date
    query = """
        SELECT
            date,
            SUM(aum) as total_aum,
            COUNT(DISTINCT fund_id) as fund_count
        FROM mutual_fund_nav
        WHERE date >= ?
        AND aum IS NOT NULL
    """
    params = [start_date]

    if category:
        query += """
            AND fund_id IN (
                SELECT fund_id FROM mutual_funds
                WHERE category = ? AND is_active = 1
            )
        """
        params.append(category)

    query += " GROUP BY date ORDER BY date"

    try:
        df = pd.read_sql_query(query, con, params=params)
        return df
    except Exception:
        return pd.DataFrame()


def get_fund_comparison_table(
    con: sqlite3.Connection,
    fund_ids: list[str],
) -> list[dict]:
    """
    Get comparison table for multiple funds.

    Args:
        con: Database connection
        fund_ids: List of fund IDs to compare

    Returns:
        List of fund analytics dicts
    """
    results = []

    for fund_id in fund_ids:
        analytics = get_mf_analytics(con, fund_id)
        if "error" not in analytics:
            results.append({
                "fund_id": analytics.get("fund_id"),
                "symbol": analytics.get("symbol"),
                "fund_name": analytics.get("fund_name"),
                "category": analytics.get("category"),
                "is_shariah": analytics.get("is_shariah"),
                "latest_nav": analytics.get("latest_nav"),
                "return_1W": analytics.get("return_1W"),
                "return_1M": analytics.get("return_1M"),
                "return_3M": analytics.get("return_3M"),
                "return_6M": analytics.get("return_6M"),
                "return_1Y": analytics.get("return_1Y"),
                "vol_1M": analytics.get("vol_1M"),
                "vol_3M": analytics.get("vol_3M"),
                "sharpe_ratio": analytics.get("sharpe_ratio"),
                "max_drawdown": analytics.get("max_drawdown"),
                "expense_ratio": analytics.get("expense_ratio"),
            })

    return results
