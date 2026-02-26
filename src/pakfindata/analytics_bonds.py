"""
Bond Analytics module for Phase 3.

This module provides bond analytics:
- YTM calculation (yield to maturity)
- Duration (Macaulay and Modified)
- Convexity
- Accrued interest
- Yield curve construction and interpolation

All analytics are READ-ONLY and for informational purposes only.
No investment recommendations or trading signals.
"""

import math
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from .db import (
    get_all_latest_quotes,
    get_bond,
    get_bond_latest_quote,
    get_bonds,
    get_latest_yield_curve,
    get_yield_curve,
    upsert_bond_analytics,
    upsert_yield_curve_point,
)


def calculate_accrued_interest(
    coupon_rate: float,
    face_value: float,
    last_coupon_date: str,
    settlement_date: str,
    coupon_frequency: int = 2,
    day_count: str = "ACT/ACT",
) -> float:
    """
    Calculate accrued interest.

    Args:
        coupon_rate: Annual coupon rate (decimal)
        face_value: Face value of bond
        last_coupon_date: Date of last coupon payment (YYYY-MM-DD)
        settlement_date: Settlement date (YYYY-MM-DD)
        coupon_frequency: Payments per year
        day_count: Day count convention

    Returns:
        Accrued interest
    """
    if coupon_rate is None or coupon_rate == 0 or coupon_frequency == 0:
        return 0.0

    last_dt = datetime.strptime(last_coupon_date, "%Y-%m-%d")
    settle_dt = datetime.strptime(settlement_date, "%Y-%m-%d")

    days_accrued = (settle_dt - last_dt).days

    # Days in coupon period (simplified)
    if day_count == "30/360":
        days_in_period = 180 if coupon_frequency == 2 else 360 // coupon_frequency
    else:  # ACT/ACT, ACT/360
        days_in_period = 365 // coupon_frequency

    coupon_payment = coupon_rate * face_value / coupon_frequency
    accrued = coupon_payment * (days_accrued / days_in_period)

    return round(accrued, 4)


def calculate_bond_price(
    ytm: float,
    coupon_rate: float | None,
    face_value: float,
    periods_remaining: int,
    coupon_frequency: int = 2,
) -> float:
    """
    Calculate bond clean price given YTM.

    Args:
        ytm: Yield to maturity (annual, decimal)
        coupon_rate: Annual coupon rate (decimal), None for zero-coupon
        face_value: Face value
        periods_remaining: Number of coupon periods remaining
        coupon_frequency: Payments per year

    Returns:
        Clean price
    """
    if periods_remaining <= 0:
        return face_value

    period_ytm = ytm / coupon_frequency

    if period_ytm == 0:
        # Special case: zero yield
        if coupon_rate is None or coupon_rate == 0:
            return face_value
        coupon_payment = coupon_rate * face_value / coupon_frequency
        return face_value + coupon_payment * periods_remaining

    # Zero-coupon bond
    if coupon_rate is None or coupon_rate == 0:
        return face_value / ((1 + period_ytm) ** periods_remaining)

    # Coupon bond
    coupon_payment = coupon_rate * face_value / coupon_frequency

    # PV of coupons
    pv_factor = (1 - (1 + period_ytm) ** (-periods_remaining)) / period_ytm
    pv_coupons = coupon_payment * pv_factor

    # PV of principal
    pv_principal = face_value / ((1 + period_ytm) ** periods_remaining)

    return pv_coupons + pv_principal


def calculate_ytm(
    price: float,
    coupon_rate: float | None,
    face_value: float,
    periods_remaining: int,
    coupon_frequency: int = 2,
    tolerance: float = 1e-8,
    max_iterations: int = 100,
) -> float | None:
    """
    Calculate yield to maturity using Newton-Raphson method.

    Args:
        price: Current clean price
        coupon_rate: Annual coupon rate (decimal)
        face_value: Face value
        periods_remaining: Number of coupon periods remaining
        coupon_frequency: Payments per year
        tolerance: Convergence tolerance
        max_iterations: Maximum iterations

    Returns:
        YTM as annual decimal, or None if not converged
    """
    if periods_remaining <= 0 or price <= 0:
        return None

    # Zero-coupon bond: direct calculation
    if coupon_rate is None or coupon_rate == 0:
        if price >= face_value:
            return 0.0
        period_ytm = (face_value / price) ** (1 / periods_remaining) - 1
        return period_ytm * coupon_frequency

    # Initial guess based on current yield
    coupon_payment = coupon_rate * face_value / coupon_frequency
    current_yield = (coupon_payment * coupon_frequency) / price
    ytm_guess = current_yield

    for _ in range(max_iterations):
        # Calculate price at current guess
        calc_price = calculate_bond_price(
            ytm_guess, coupon_rate, face_value, periods_remaining, coupon_frequency
        )

        # Calculate derivative (using numerical approximation)
        delta = 0.0001
        price_up = calculate_bond_price(
            ytm_guess + delta, coupon_rate, face_value,
            periods_remaining, coupon_frequency
        )
        derivative = (price_up - calc_price) / delta

        if abs(derivative) < 1e-10:
            break

        # Newton-Raphson update
        error = calc_price - price
        ytm_guess = ytm_guess - error / derivative

        # Keep within reasonable bounds
        ytm_guess = max(0.0001, min(1.0, ytm_guess))

        if abs(error) < tolerance:
            return round(ytm_guess, 8)

    return round(ytm_guess, 8)


def calculate_macaulay_duration(
    ytm: float,
    coupon_rate: float | None,
    face_value: float,
    periods_remaining: int,
    coupon_frequency: int = 2,
) -> float:
    """
    Calculate Macaulay duration in years.

    Args:
        ytm: Yield to maturity (annual, decimal)
        coupon_rate: Annual coupon rate (decimal)
        face_value: Face value
        periods_remaining: Number of coupon periods remaining
        coupon_frequency: Payments per year

    Returns:
        Macaulay duration in years
    """
    if periods_remaining <= 0:
        return 0.0

    period_ytm = ytm / coupon_frequency
    if period_ytm <= 0:
        period_ytm = 0.0001

    # Zero-coupon bond
    if coupon_rate is None or coupon_rate == 0:
        return periods_remaining / coupon_frequency

    coupon_payment = coupon_rate * face_value / coupon_frequency
    price = calculate_bond_price(
        ytm, coupon_rate, face_value, periods_remaining, coupon_frequency
    )

    if price == 0:
        return 0.0

    # Weighted average time
    weighted_time = 0.0
    for t in range(1, periods_remaining + 1):
        cf = coupon_payment
        if t == periods_remaining:
            cf += face_value
        pv = cf / ((1 + period_ytm) ** t)
        weighted_time += t * pv

    duration_periods = weighted_time / price
    duration_years = duration_periods / coupon_frequency

    return round(duration_years, 4)


def calculate_modified_duration(
    macaulay_duration: float,
    ytm: float,
    coupon_frequency: int = 2,
) -> float:
    """
    Calculate modified duration.

    Args:
        macaulay_duration: Macaulay duration in years
        ytm: Yield to maturity (annual, decimal)
        coupon_frequency: Payments per year

    Returns:
        Modified duration
    """
    period_ytm = ytm / coupon_frequency
    mod_duration = macaulay_duration / (1 + period_ytm)
    return round(mod_duration, 4)


def calculate_convexity(
    ytm: float,
    coupon_rate: float | None,
    face_value: float,
    periods_remaining: int,
    coupon_frequency: int = 2,
) -> float:
    """
    Calculate bond convexity.

    Args:
        ytm: Yield to maturity (annual, decimal)
        coupon_rate: Annual coupon rate (decimal)
        face_value: Face value
        periods_remaining: Number of coupon periods remaining
        coupon_frequency: Payments per year

    Returns:
        Convexity
    """
    if periods_remaining <= 0:
        return 0.0

    period_ytm = ytm / coupon_frequency
    if period_ytm <= 0:
        period_ytm = 0.0001

    price = calculate_bond_price(
        ytm, coupon_rate, face_value, periods_remaining, coupon_frequency
    )

    if price == 0:
        return 0.0

    # Zero-coupon bond
    if coupon_rate is None or coupon_rate == 0:
        n = periods_remaining
        convexity = (n * (n + 1)) / ((1 + period_ytm) ** 2)
        return round(convexity / (coupon_frequency ** 2), 4)

    coupon_payment = coupon_rate * face_value / coupon_frequency

    # Sum of t*(t+1)*PV(CF)
    convex_sum = 0.0
    for t in range(1, periods_remaining + 1):
        cf = coupon_payment
        if t == periods_remaining:
            cf += face_value
        pv = cf / ((1 + period_ytm) ** t)
        convex_sum += t * (t + 1) * pv

    convexity = convex_sum / (price * (1 + period_ytm) ** 2)
    convexity_years = convexity / (coupon_frequency ** 2)

    return round(convexity_years, 4)


def get_periods_remaining(
    maturity_date: str,
    settlement_date: str | None = None,
    coupon_frequency: int = 2,
) -> int:
    """
    Calculate number of coupon periods remaining.

    Args:
        maturity_date: Bond maturity date (YYYY-MM-DD)
        settlement_date: Settlement date (default: today)
        coupon_frequency: Payments per year

    Returns:
        Number of periods remaining
    """
    if settlement_date is None:
        settle_dt = datetime.now()
    else:
        settle_dt = datetime.strptime(settlement_date, "%Y-%m-%d")

    mat_dt = datetime.strptime(maturity_date, "%Y-%m-%d")

    days_to_maturity = (mat_dt - settle_dt).days
    years_to_maturity = days_to_maturity / 365.25

    periods = int(math.ceil(years_to_maturity * coupon_frequency))
    return max(0, periods)


def get_bond_full_analytics(
    con: sqlite3.Connection,
    bond_id: str,
    as_of_date: str | None = None,
) -> dict:
    """
    Get comprehensive analytics for a bond.

    Args:
        con: Database connection
        bond_id: Bond ID
        as_of_date: Date for calculations (default: latest quote)

    Returns:
        Dict with all analytics
    """
    # Get bond info
    bond = get_bond(con, bond_id)
    if not bond:
        return {"bond_id": bond_id, "error": "bond_not_found"}

    # Get latest quote
    quote = get_bond_latest_quote(con, bond_id)
    if not quote:
        return {
            "bond_id": bond_id,
            "symbol": bond.get("symbol"),
            "error": "no_quote_data",
        }

    if as_of_date is None:
        as_of_date = quote.get("date")

    # Extract bond details
    coupon_rate = bond.get("coupon_rate")
    face_value = bond.get("face_value", 100)
    coupon_frequency = bond.get("coupon_frequency", 2)
    maturity_date = bond.get("maturity_date")

    # Get price and YTM
    price = quote.get("price")
    ytm = quote.get("ytm")

    # Calculate periods remaining
    periods = get_periods_remaining(maturity_date, as_of_date, coupon_frequency)
    days_to_mat = (
        datetime.strptime(maturity_date, "%Y-%m-%d") -
        datetime.strptime(as_of_date, "%Y-%m-%d")
    ).days

    # If no YTM in quote, calculate it
    if ytm is None and price is not None:
        ytm = calculate_ytm(
            price, coupon_rate, face_value, periods, coupon_frequency
        )

    # If no price, calculate from YTM
    if price is None and ytm is not None:
        price = calculate_bond_price(
            ytm, coupon_rate, face_value, periods, coupon_frequency
        )

    analytics = {
        "bond_id": bond_id,
        "symbol": bond.get("symbol"),
        "issuer": bond.get("issuer"),
        "bond_type": bond.get("bond_type"),
        "is_islamic": bond.get("is_islamic"),
        "coupon_rate": coupon_rate,
        "face_value": face_value,
        "maturity_date": maturity_date,
        "as_of_date": as_of_date,
        "price": round(price, 4) if price else None,
        "ytm": round(ytm, 6) if ytm else None,
        "days_to_maturity": days_to_mat,
        "periods_remaining": periods,
    }

    # Calculate duration and convexity if we have YTM
    if ytm is not None and periods > 0:
        mac_duration = calculate_macaulay_duration(
            ytm, coupon_rate, face_value, periods, coupon_frequency
        )
        mod_duration = calculate_modified_duration(mac_duration, ytm, coupon_frequency)
        convexity = calculate_convexity(
            ytm, coupon_rate, face_value, periods, coupon_frequency
        )

        analytics["duration"] = mac_duration
        analytics["modified_duration"] = mod_duration
        analytics["convexity"] = convexity

    # Calculate accrued interest (simplified - use settlement as last coupon)
    if coupon_rate and coupon_frequency:
        # Estimate last coupon date
        months_per_period = 12 // coupon_frequency
        mat_dt = datetime.strptime(maturity_date, "%Y-%m-%d")
        as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d")

        # Find last coupon date before settlement
        coupon_dt = mat_dt
        while coupon_dt > as_of_dt:
            coupon_dt = coupon_dt - timedelta(days=months_per_period * 30)

        if coupon_dt < as_of_dt:
            accrued = calculate_accrued_interest(
                coupon_rate, face_value,
                coupon_dt.strftime("%Y-%m-%d"),
                as_of_date,
                coupon_frequency,
                bond.get("day_count", "ACT/ACT"),
            )
            analytics["accrued_interest"] = accrued
            if price:
                analytics["dirty_price"] = round(price + accrued, 4)

    return analytics


def compute_and_store_analytics(
    con: sqlite3.Connection,
    bond_ids: list[str] | None = None,
    as_of_date: str | None = None,
) -> dict:
    """
    Compute and store analytics for bonds.

    Args:
        con: Database connection
        bond_ids: List of bond IDs (None = all active)
        as_of_date: Date for calculations

    Returns:
        Summary dict
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    # Get bonds
    if bond_ids is None:
        bonds = get_bonds(con, active_only=True)
        bond_ids = [b["bond_id"] for b in bonds]

    stored = 0
    failed = 0

    for bond_id in bond_ids:
        analytics = get_bond_full_analytics(con, bond_id, as_of_date)

        if "error" in analytics:
            failed += 1
            continue

        # Store in analytics table
        record = {
            "bond_id": bond_id,
            "as_of_date": as_of_date,
            "price": analytics.get("price"),
            "ytm": analytics.get("ytm"),
            "duration": analytics.get("duration"),
            "modified_duration": analytics.get("modified_duration"),
            "convexity": analytics.get("convexity"),
            "accrued_interest": analytics.get("accrued_interest"),
            "days_to_maturity": analytics.get("days_to_maturity"),
        }

        if upsert_bond_analytics(con, record):
            stored += 1
        else:
            failed += 1

    return {
        "success": True,
        "stored": stored,
        "failed": failed,
        "as_of_date": as_of_date,
    }


def build_yield_curve(
    con: sqlite3.Connection,
    curve_date: str | None = None,
    bond_type: str = "PIB",
    interpolation: str = "LINEAR",
) -> list[dict]:
    """
    Build yield curve from bond quotes.

    Args:
        con: Database connection
        curve_date: Date for curve (default: latest)
        bond_type: Bond type filter
        interpolation: Interpolation method

    Returns:
        List of yield curve points
    """
    if curve_date is None:
        curve_date = datetime.now().strftime("%Y-%m-%d")

    # Get latest quotes for bond type
    quotes = get_all_latest_quotes(con, bond_type)

    if not quotes:
        return []

    # Build points from quotes
    points = []
    for quote in quotes:
        ytm = quote.get("ytm")
        maturity = quote.get("maturity_date")

        if ytm is None or maturity is None:
            continue

        # Calculate tenor in months
        mat_dt = datetime.strptime(maturity, "%Y-%m-%d")
        curve_dt = datetime.strptime(curve_date, "%Y-%m-%d")
        days = (mat_dt - curve_dt).days
        tenor_months = max(1, int(days / 30))

        points.append({
            "curve_date": curve_date,
            "tenor_months": tenor_months,
            "yield_rate": ytm,
            "bond_type": bond_type,
            "interpolation": interpolation,
        })

    # Sort by tenor
    points.sort(key=lambda x: x["tenor_months"])

    # Store points
    for point in points:
        upsert_yield_curve_point(con, point)

    return points


def interpolate_yield(
    curve_points: list[dict],
    tenor_months: int,
    method: str = "LINEAR",
) -> float | None:
    """
    Interpolate yield for a given tenor.

    Args:
        curve_points: List of yield curve points
        tenor_months: Target tenor in months
        method: Interpolation method

    Returns:
        Interpolated yield rate
    """
    if not curve_points:
        return None

    # Sort by tenor
    points = sorted(curve_points, key=lambda x: x["tenor_months"])

    # Extract tenors and yields
    tenors = [p["tenor_months"] for p in points]
    yields = [p["yield_rate"] for p in points]

    # Check bounds
    if tenor_months <= tenors[0]:
        return yields[0]
    if tenor_months >= tenors[-1]:
        return yields[-1]

    # Find bracketing points
    for i in range(len(tenors) - 1):
        if tenors[i] <= tenor_months <= tenors[i + 1]:
            if method == "LINEAR":
                # Linear interpolation
                t1, t2 = tenors[i], tenors[i + 1]
                y1, y2 = yields[i], yields[i + 1]
                weight = (tenor_months - t1) / (t2 - t1)
                return y1 + weight * (y2 - y1)

    return None


def get_bond_screener_data(
    con: sqlite3.Connection,
    bond_type: str | None = None,
    issuer: str | None = None,
    is_islamic: bool | None = None,
    min_ytm: float | None = None,
    max_duration: float | None = None,
) -> list[dict]:
    """
    Get bonds for screener with analytics.

    Args:
        con: Database connection
        bond_type: Filter by type
        issuer: Filter by issuer
        is_islamic: Filter by Islamic
        min_ytm: Minimum YTM filter
        max_duration: Maximum duration filter

    Returns:
        List of bonds with analytics
    """
    # Get bonds with filters
    bonds = get_bonds(con, bond_type=bond_type, issuer=issuer, is_islamic=is_islamic)

    results = []
    for bond in bonds:
        # Get analytics
        analytics = get_bond_full_analytics(con, bond["bond_id"])

        if "error" in analytics:
            continue

        # Apply filters
        ytm = analytics.get("ytm")
        duration = analytics.get("modified_duration")

        if min_ytm is not None and (ytm is None or ytm < min_ytm):
            continue
        if max_duration is not None and (duration is None or duration > max_duration):
            continue

        results.append(analytics)

    # Sort by YTM descending
    results.sort(key=lambda x: x.get("ytm") or 0, reverse=True)

    return results


def compare_bonds(
    con: sqlite3.Connection,
    bond_ids: list[str],
) -> list[dict]:
    """
    Compare multiple bonds.

    Args:
        con: Database connection
        bond_ids: List of bond IDs

    Returns:
        List of analytics for comparison
    """
    results = []
    for bond_id in bond_ids:
        analytics = get_bond_full_analytics(con, bond_id)
        if "error" not in analytics:
            results.append(analytics)
    return results


def get_yield_curve_chart_data(
    con: sqlite3.Connection,
    curve_date: str | None = None,
    bond_type: str = "PIB",
) -> pd.DataFrame:
    """
    Get yield curve data formatted for charting.

    Args:
        con: Database connection
        curve_date: Date for curve
        bond_type: Bond type

    Returns:
        DataFrame with tenor and yield columns
    """
    if curve_date:
        points = get_yield_curve(con, curve_date, bond_type)
    else:
        curve_date, points = get_latest_yield_curve(con, bond_type)

    if not points:
        return pd.DataFrame()

    df = pd.DataFrame(points)

    # Add tenor labels
    tenor_labels = {
        3: "3M", 6: "6M", 12: "1Y", 24: "2Y",
        36: "3Y", 60: "5Y", 84: "7Y", 120: "10Y",
    }
    df["tenor_label"] = df["tenor_months"].apply(
        lambda x: tenor_labels.get(x, f"{x}M")
    )

    return df[["tenor_months", "tenor_label", "yield_rate", "curve_date"]]
