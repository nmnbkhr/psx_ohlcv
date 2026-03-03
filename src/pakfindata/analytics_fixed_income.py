"""
Fixed Income Analytics module for Phase 3.

This module provides bond math calculations:
- YTM (Yield to Maturity) solver using bisection method
- Macaulay Duration
- Modified Duration
- Convexity
- PVBP (Price Value of a Basis Point)
- Yield curve analytics

All calculations use ACT/365 day count convention.
All analytics are READ-ONLY and for informational purposes only.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from .db import (
    get_fi_analytics,
    get_fi_curve,
    get_fi_instrument,
    get_fi_instruments,
    get_fi_latest_quote,
    upsert_fi_analytics,
)

# Day count convention: ACT/365
DAYS_PER_YEAR = 365.0


@dataclass
class BondCashFlow:
    """Represents a single cash flow from a bond."""

    date: str  # YYYY-MM-DD
    days_from_settlement: int
    years_from_settlement: float
    coupon: float
    principal: float
    total: float


def days_between(date1: str, date2: str) -> int:
    """
    Calculate days between two dates.

    Args:
        date1: Start date (YYYY-MM-DD)
        date2: End date (YYYY-MM-DD)

    Returns:
        Number of days (positive if date2 > date1)
    """
    d1 = datetime.strptime(date1, "%Y-%m-%d")
    d2 = datetime.strptime(date2, "%Y-%m-%d")
    return (d2 - d1).days


def years_between(date1: str, date2: str) -> float:
    """
    Calculate years between two dates using ACT/365.

    Args:
        date1: Start date (YYYY-MM-DD)
        date2: End date (YYYY-MM-DD)

    Returns:
        Years as float (ACT/365 convention)
    """
    return days_between(date1, date2) / DAYS_PER_YEAR


def generate_cash_flows(
    settlement_date: str,
    maturity_date: str,
    coupon_rate: float,
    face_value: float = 100.0,
    frequency: int = 2,  # Semi-annual default for PIBs
) -> list[BondCashFlow]:
    """
    Generate cash flows for a bond.

    Args:
        settlement_date: Settlement date (YYYY-MM-DD)
        maturity_date: Maturity date (YYYY-MM-DD)
        coupon_rate: Annual coupon rate as decimal (e.g., 0.155 for 15.5%)
        face_value: Face value of bond (default 100)
        frequency: Coupon frequency per year (1=annual, 2=semi-annual)

    Returns:
        List of BondCashFlow objects
    """
    cash_flows = []

    settle = datetime.strptime(settlement_date, "%Y-%m-%d")
    maturity = datetime.strptime(maturity_date, "%Y-%m-%d")

    if maturity <= settle:
        return cash_flows

    # Calculate coupon payment per period
    coupon_per_period = (coupon_rate * face_value) / frequency

    # Generate coupon dates working backward from maturity
    coupon_dates = []
    current = maturity
    months_per_period = 12 // frequency

    while current > settle:
        coupon_dates.append(current)
        # Move back by one period
        month = current.month - months_per_period
        year = current.year
        if month <= 0:
            month += 12
            year -= 1
        try:
            current = current.replace(year=year, month=month)
        except ValueError:
            # Handle month-end issues (e.g., Jan 31 -> Feb 28)
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            new_day = min(current.day, last_day)
            current = current.replace(year=year, month=month, day=new_day)

    # Reverse to get chronological order
    coupon_dates.reverse()

    # Generate cash flows
    for i, cf_date in enumerate(coupon_dates):
        is_maturity = (cf_date == maturity)
        days = (cf_date - settle).days
        years = days / DAYS_PER_YEAR

        principal = face_value if is_maturity else 0.0
        coupon = coupon_per_period

        cash_flows.append(BondCashFlow(
            date=cf_date.strftime("%Y-%m-%d"),
            days_from_settlement=days,
            years_from_settlement=years,
            coupon=coupon,
            principal=principal,
            total=coupon + principal,
        ))

    return cash_flows


def bond_price_from_ytm(
    cash_flows: list[BondCashFlow],
    ytm: float,
) -> float:
    """
    Calculate bond price given YTM.

    Args:
        cash_flows: List of cash flows
        ytm: Yield to maturity as decimal

    Returns:
        Present value (price) of bond
    """
    if not cash_flows or ytm < -0.99:  # Prevent division issues
        return 0.0

    pv = 0.0
    for cf in cash_flows:
        discount_factor = 1 / ((1 + ytm) ** cf.years_from_settlement)
        pv += cf.total * discount_factor

    return pv


def solve_ytm(
    cash_flows: list[BondCashFlow],
    dirty_price: float,
    tolerance: float = 1e-8,
    max_iterations: int = 100,
) -> float | None:
    """
    Solve for YTM using bisection method.

    Args:
        cash_flows: List of cash flows
        dirty_price: Current dirty price of bond
        tolerance: Convergence tolerance
        max_iterations: Maximum iterations

    Returns:
        YTM as decimal, or None if no solution found
    """
    if not cash_flows or dirty_price <= 0:
        return None

    # Initial bounds for bisection
    ytm_low = -0.5  # -50%
    ytm_high = 1.0   # 100%

    # Verify bounds bracket the solution
    pv_low = bond_price_from_ytm(cash_flows, ytm_low)
    pv_high = bond_price_from_ytm(cash_flows, ytm_high)

    if (pv_low - dirty_price) * (pv_high - dirty_price) > 0:
        # Solution not bracketed, expand search
        ytm_low = -0.9
        ytm_high = 2.0
        pv_low = bond_price_from_ytm(cash_flows, ytm_low)
        pv_high = bond_price_from_ytm(cash_flows, ytm_high)

        if (pv_low - dirty_price) * (pv_high - dirty_price) > 0:
            return None

    # Bisection iteration
    for _ in range(max_iterations):
        ytm_mid = (ytm_low + ytm_high) / 2
        pv_mid = bond_price_from_ytm(cash_flows, ytm_mid)

        if abs(pv_mid - dirty_price) < tolerance:
            return ytm_mid

        if (pv_mid - dirty_price) * (pv_low - dirty_price) < 0:
            ytm_high = ytm_mid
        else:
            ytm_low = ytm_mid

    # Return best estimate
    return (ytm_low + ytm_high) / 2


def compute_macaulay_duration(
    cash_flows: list[BondCashFlow],
    ytm: float,
) -> float | None:
    """
    Calculate Macaulay duration.

    Macaulay duration is the weighted average time to receive cash flows,
    where weights are present values.

    Args:
        cash_flows: List of cash flows
        ytm: Yield to maturity as decimal

    Returns:
        Macaulay duration in years, or None
    """
    if not cash_flows or ytm <= -1:
        return None

    pv_total = 0.0
    weighted_time = 0.0

    for cf in cash_flows:
        discount_factor = 1 / ((1 + ytm) ** cf.years_from_settlement)
        pv = cf.total * discount_factor
        pv_total += pv
        weighted_time += pv * cf.years_from_settlement

    if pv_total <= 0:
        return None

    return weighted_time / pv_total


def compute_modified_duration(
    macaulay_duration: float,
    ytm: float,
    frequency: int = 2,
) -> float | None:
    """
    Calculate modified duration.

    Modified duration measures price sensitivity to yield changes.

    Args:
        macaulay_duration: Macaulay duration in years
        ytm: Yield to maturity as decimal
        frequency: Coupon frequency per year

    Returns:
        Modified duration, or None
    """
    if macaulay_duration is None or ytm <= -1:
        return None

    return macaulay_duration / (1 + ytm / frequency)


def compute_convexity(
    cash_flows: list[BondCashFlow],
    ytm: float,
) -> float | None:
    """
    Calculate bond convexity.

    Convexity measures the curvature of price-yield relationship.

    Args:
        cash_flows: List of cash flows
        ytm: Yield to maturity as decimal

    Returns:
        Convexity, or None
    """
    if not cash_flows or ytm <= -1:
        return None

    pv_total = 0.0
    convexity_sum = 0.0

    for cf in cash_flows:
        t = cf.years_from_settlement
        discount_factor = 1 / ((1 + ytm) ** t)
        pv = cf.total * discount_factor
        pv_total += pv
        # Convexity formula: sum of t*(t+1)*PV(cf) / (1+y)^2
        convexity_sum += t * (t + 1) * pv

    if pv_total <= 0:
        return None

    return convexity_sum / (pv_total * (1 + ytm) ** 2)


def compute_pvbp(
    cash_flows: list[BondCashFlow],
    ytm: float,
    face_value: float = 100.0,
) -> float | None:
    """
    Calculate PVBP (Price Value of a Basis Point).

    PVBP is the change in price for a 1 basis point change in yield.

    Args:
        cash_flows: List of cash flows
        ytm: Yield to maturity as decimal
        face_value: Face value for normalization

    Returns:
        PVBP per 100 face value, or None
    """
    if not cash_flows:
        return None

    bp = 0.0001  # 1 basis point

    price_up = bond_price_from_ytm(cash_flows, ytm + bp)
    price_down = bond_price_from_ytm(cash_flows, ytm - bp)

    # PVBP is average of up/down moves (to account for convexity)
    return abs(price_down - price_up) / 2


def compute_bond_analytics(
    settlement_date: str,
    maturity_date: str,
    coupon_rate: float,
    dirty_price: float,
    face_value: float = 100.0,
    frequency: int = 2,
) -> dict:
    """
    Compute all bond analytics.

    Args:
        settlement_date: Settlement date (YYYY-MM-DD)
        maturity_date: Maturity date (YYYY-MM-DD)
        coupon_rate: Annual coupon rate as decimal
        dirty_price: Dirty price of bond
        face_value: Face value (default 100)
        frequency: Coupon frequency (default 2 for semi-annual)

    Returns:
        Dict with all analytics (ytm, duration, convexity, pvbp)
    """
    result = {
        "settlement_date": settlement_date,
        "maturity_date": maturity_date,
        "coupon_rate": coupon_rate,
        "dirty_price": dirty_price,
        "face_value": face_value,
        "frequency": frequency,
    }

    # Generate cash flows
    cash_flows = generate_cash_flows(
        settlement_date=settlement_date,
        maturity_date=maturity_date,
        coupon_rate=coupon_rate,
        face_value=face_value,
        frequency=frequency,
    )

    if not cash_flows:
        result["error"] = "no_cash_flows"
        return result

    result["num_cash_flows"] = len(cash_flows)

    # Years to maturity
    ytm_years = years_between(settlement_date, maturity_date)
    result["years_to_maturity"] = round(ytm_years, 4)

    # Solve for YTM
    ytm = solve_ytm(cash_flows, dirty_price)
    if ytm is not None:
        result["ytm"] = round(ytm, 6)
        result["ytm_pct"] = round(ytm * 100, 4)

        # Macaulay duration
        mac_dur = compute_macaulay_duration(cash_flows, ytm)
        if mac_dur is not None:
            result["macaulay_duration"] = round(mac_dur, 4)

            # Modified duration
            mod_dur = compute_modified_duration(mac_dur, ytm, frequency)
            if mod_dur is not None:
                result["modified_duration"] = round(mod_dur, 4)

        # Convexity
        convexity = compute_convexity(cash_flows, ytm)
        if convexity is not None:
            result["convexity"] = round(convexity, 4)

        # PVBP
        pvbp = compute_pvbp(cash_flows, ytm, face_value)
        if pvbp is not None:
            result["pvbp"] = round(pvbp, 6)

    return result


def compute_analytics_for_instrument(
    con: sqlite3.Connection,
    isin: str,
    as_of_date: str | None = None,
) -> dict | None:
    """
    Compute analytics for a fixed income instrument.

    Args:
        con: Database connection
        isin: Instrument ISIN
        as_of_date: Date for analytics, or None for latest

    Returns:
        Analytics dict, or None if insufficient data
    """
    # Get instrument details
    instrument = get_fi_instrument(con, isin)
    if not instrument:
        return None

    # Get latest quote
    quote = get_fi_latest_quote(con, isin)
    if not quote:
        return None

    # Extract required fields
    maturity_date = instrument.get("maturity_date")
    coupon_rate = instrument.get("coupon_rate", 0.0)
    frequency = instrument.get("coupon_frequency", 2)

    settlement_date = as_of_date or quote.get("date")
    dirty_price = quote.get("dirty_price") or quote.get("clean_price")

    if not all([maturity_date, settlement_date, dirty_price]):
        return None

    # Compute analytics
    analytics = compute_bond_analytics(
        settlement_date=settlement_date,
        maturity_date=maturity_date,
        coupon_rate=coupon_rate or 0.0,
        dirty_price=dirty_price,
        frequency=frequency or 2,
    )

    # Add instrument info
    analytics["isin"] = isin
    analytics["symbol"] = instrument.get("symbol")
    analytics["category"] = instrument.get("category")
    analytics["quote_date"] = quote.get("date")
    analytics["quoted_yield"] = quote.get("yield_to_maturity")

    return analytics


def compute_and_store_analytics(
    con: sqlite3.Connection,
    isins: list[str] | None = None,
    as_of_date: str | None = None,
) -> dict:
    """
    Compute and store analytics for instruments.

    Args:
        con: Database connection
        isins: List of ISINs, or None for all active
        as_of_date: Date for analytics, or None for today

    Returns:
        Summary dict with counts
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    if isins is None:
        instruments = get_fi_instruments(con, category=None, active_only=True)
        isins = [i.get("isin") for i in instruments if i.get("isin")]

    stored = 0
    failed = 0
    errors = []

    for isin in isins:
        try:
            analytics = compute_analytics_for_instrument(con, isin, as_of_date)
            if analytics and "ytm" in analytics:
                record = {
                    "isin": isin,
                    "as_of_date": as_of_date,
                    "ytm": analytics.get("ytm"),
                    "macaulay_duration": analytics.get("macaulay_duration"),
                    "modified_duration": analytics.get("modified_duration"),
                    "convexity": analytics.get("convexity"),
                    "pvbp": analytics.get("pvbp"),
                    "spread_to_benchmark": None,  # Would need benchmark curve
                }
                if upsert_fi_analytics(con, record):
                    stored += 1
                else:
                    failed += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            errors.append(f"{isin}: {e!s}")

    return {
        "success": failed == 0,
        "stored": stored,
        "failed": failed,
        "total": len(isins),
        "errors": errors[:10],
    }


def get_yield_curve_analytics(
    con: sqlite3.Connection,
    curve_name: str,
    curve_date: str | None = None,
) -> dict:
    """
    Get yield curve with analytics.

    Args:
        con: Database connection
        curve_name: Curve name (e.g., "PKR_MTB")
        curve_date: Specific date, or None for latest

    Returns:
        Dict with curve points and analytics
    """
    curve_points = get_fi_curve(con, curve_name, curve_date=curve_date)

    if not curve_points:
        return {"curve_name": curve_name, "error": "no_data"}

    # Convert to DataFrame for analytics
    df = pd.DataFrame(curve_points)

    # Normalize column names (DB uses tenor_days/rate, we want tenor_months/yield_value)
    if "tenor_days" in df.columns and "tenor_months" not in df.columns:
        df["tenor_months"] = df["tenor_days"] / 30  # Approximate conversion
    if "rate" in df.columns and "yield_value" not in df.columns:
        df["yield_value"] = df["rate"]

    # Add normalized columns to curve_points for display
    normalized_points = []
    for p in curve_points:
        np = dict(p)
        if "tenor_days" in np and "tenor_months" not in np:
            np["tenor_months"] = np["tenor_days"] / 30
        if "rate" in np and "yield_value" not in np:
            np["yield_value"] = np["rate"]
        normalized_points.append(np)

    result = {
        "curve_name": curve_name,
        "curve_date": curve_points[0].get("curve_date") if curve_points else None,
        "num_points": len(curve_points),
        "points": normalized_points,
    }

    if len(df) >= 2 and "tenor_months" in df.columns and "yield_value" in df.columns:
        # Calculate curve steepness (long end - short end)
        df_sorted = df.sort_values("tenor_months")
        short_yield = df_sorted.iloc[0]["yield_value"]
        long_yield = df_sorted.iloc[-1]["yield_value"]

        result["short_tenor"] = int(df_sorted.iloc[0]["tenor_months"])
        result["long_tenor"] = int(df_sorted.iloc[-1]["tenor_months"])
        result["short_yield"] = short_yield
        result["long_yield"] = long_yield
        if short_yield and long_yield:
            result["steepness"] = round(long_yield - short_yield, 4)
        else:
            result["steepness"] = None

        # Curve shape
        if result["steepness"]:
            if result["steepness"] > 0.5:
                result["shape"] = "steep"
            elif result["steepness"] > 0:
                result["shape"] = "normal"
            elif result["steepness"] > -0.5:
                result["shape"] = "flat"
            else:
                result["shape"] = "inverted"

    return result


def compare_yield_curves(
    con: sqlite3.Connection,
    curve_name: str,
    date1: str,
    date2: str,
) -> dict:
    """
    Compare yield curves across two dates.

    Args:
        con: Database connection
        curve_name: Curve name
        date1: First date
        date2: Second date

    Returns:
        Comparison dict with changes by tenor
    """
    curve1 = get_fi_curve(con, curve_name, curve_date=date1)
    curve2 = get_fi_curve(con, curve_name, curve_date=date2)

    if not curve1 or not curve2:
        return {"error": "insufficient_data"}

    # Create DataFrames and normalize column names
    df1 = pd.DataFrame(curve1)
    df2 = pd.DataFrame(curve2)

    # Normalize column names (DB uses tenor_days/rate)
    for df in [df1, df2]:
        if "tenor_days" in df.columns:
            df["tenor_months"] = df["tenor_days"] / 30
        if "rate" in df.columns:
            df["yield_value"] = df["rate"]

    df1 = df1.set_index("tenor_months")
    df2 = df2.set_index("tenor_months")

    # Merge on tenor
    comparison = []
    tenors = set(df1.index) | set(df2.index)

    for tenor in sorted(tenors):
        point = {"tenor_months": int(tenor)}

        if tenor in df1.index:
            point["yield_date1"] = df1.loc[tenor, "yield_value"]
        if tenor in df2.index:
            point["yield_date2"] = df2.loc[tenor, "yield_value"]

        if "yield_date1" in point and "yield_date2" in point:
            change = point["yield_date2"] - point["yield_date1"]
            point["change"] = round(change, 4)
            point["change_bps"] = round(change * 100, 1)

        comparison.append(point)

    return {
        "curve_name": curve_name,
        "date1": date1,
        "date2": date2,
        "comparison": comparison,
    }


def get_instruments_by_yield(
    con: sqlite3.Connection,
    category: str | None = None,
    min_yield: float | None = None,
    max_yield: float | None = None,
    sort_by: str = "yield",
    limit: int = 50,
) -> list[dict]:
    """
    Get instruments filtered and sorted by yield.

    Args:
        con: Database connection
        category: Filter by category
        min_yield: Minimum yield filter
        max_yield: Maximum yield filter
        sort_by: Sort field ("yield", "duration", "maturity")
        limit: Maximum results

    Returns:
        List of instruments with analytics
    """
    instruments = get_fi_instruments(con, category=category, active_only=True)

    results = []
    for inst in instruments:
        isin = inst.get("isin")
        quote = get_fi_latest_quote(con, isin)

        if not quote:
            continue

        ytm = quote.get("yield_to_maturity")

        # Apply yield filters
        if min_yield is not None and (ytm is None or ytm < min_yield):
            continue
        if max_yield is not None and (ytm is None or ytm > max_yield):
            continue

        # Get stored analytics
        analytics = get_fi_analytics(con, isin)

        result = {
            **inst,
            "quote_date": quote.get("date"),
            "clean_price": quote.get("clean_price"),
            "dirty_price": quote.get("dirty_price"),
            "yield_to_maturity": ytm,
        }

        if analytics:
            result["macaulay_duration"] = analytics.get("macaulay_duration")
            result["modified_duration"] = analytics.get("modified_duration")
            result["convexity"] = analytics.get("convexity")

        results.append(result)

    # Sort results
    sort_keys = {
        "yield": lambda x: x.get("yield_to_maturity") or 0,
        "duration": lambda x: x.get("modified_duration") or 0,
        "maturity": lambda x: x.get("maturity_date") or "",
    }
    sort_fn = sort_keys.get(sort_by, sort_keys["yield"])
    results.sort(key=sort_fn, reverse=(sort_by == "yield"))

    return results[:limit]
