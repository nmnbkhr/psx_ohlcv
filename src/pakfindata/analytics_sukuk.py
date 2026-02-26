"""
Sukuk Analytics Module for Phase 3.

This module provides fixed income analytics for sukuk and debt instruments:
- Yield to Maturity (YTM) calculation
- Macaulay Duration
- Modified Duration
- Convexity
- Accrued Interest
- Yield curve interpolation

All calculations are for educational/analytical purposes only.
Not investment advice.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import (
    connect,
    get_sukuk,
    get_sukuk_analytics,
    get_sukuk_latest_quote,
    get_sukuk_latest_yield_curve,
    get_sukuk_list,
    init_schema,
    upsert_sukuk_analytics,
)


@dataclass
class SukukAnalyticsResult:
    """Result of sukuk analytics calculation."""

    instrument_id: str
    calc_date: str
    clean_price: float | None = None
    dirty_price: float | None = None
    yield_to_maturity: float | None = None
    macaulay_duration: float | None = None
    modified_duration: float | None = None
    convexity: float | None = None
    accrued_interest: float | None = None
    days_to_maturity: int | None = None
    current_yield: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for database storage."""
        return {
            "instrument_id": self.instrument_id,
            "calc_date": self.calc_date,
            "clean_price": self.clean_price,
            "dirty_price": self.dirty_price,
            "yield_to_maturity": self.yield_to_maturity,
            "macaulay_duration": self.macaulay_duration,
            "modified_duration": self.modified_duration,
            "convexity": self.convexity,
            "accrued_interest": self.accrued_interest,
            "days_to_maturity": self.days_to_maturity,
            "current_yield": self.current_yield,
        }


def calculate_ytm(
    price: float,
    face_value: float,
    coupon_rate: float,
    years_to_maturity: float,
    frequency: int = 2,
    max_iterations: int = 100,
    tolerance: float = 0.0001,
) -> float | None:
    """
    Calculate Yield to Maturity using Newton-Raphson method.

    Args:
        price: Current bond price (dirty price)
        face_value: Face/par value of bond
        coupon_rate: Annual coupon rate (as percentage, e.g., 15.0)
        years_to_maturity: Years until maturity
        frequency: Coupon payments per year (2 = semi-annual)
        max_iterations: Maximum solver iterations
        tolerance: Convergence tolerance

    Returns:
        YTM as percentage (e.g., 15.5) or None if calculation fails
    """
    if price <= 0 or face_value <= 0 or years_to_maturity <= 0:
        return None

    if frequency == 0:
        # Zero coupon bond
        return _calculate_zero_coupon_ytm(price, face_value, years_to_maturity)

    coupon_payment = (coupon_rate / 100) * face_value / frequency
    n_periods = int(years_to_maturity * frequency)

    if n_periods <= 0:
        return None

    # Initial guess based on current yield
    ytm = (coupon_payment * frequency / price) * 100

    for _ in range(max_iterations):
        # Calculate present value and derivative
        pv = 0.0
        dpv = 0.0
        r = ytm / (100 * frequency)

        for t in range(1, n_periods + 1):
            discount = (1 + r) ** t
            pv += coupon_payment / discount
            dpv -= t * coupon_payment / (discount * (1 + r))

        # Add face value at maturity
        discount = (1 + r) ** n_periods
        pv += face_value / discount
        dpv -= n_periods * face_value / (discount * (1 + r))

        # Newton-Raphson step
        f = pv - price
        if abs(dpv) < 1e-10:
            break

        ytm_new = ytm - (f / dpv) * frequency * 100

        if abs(ytm_new - ytm) < tolerance:
            return round(ytm_new, 4)

        ytm = ytm_new

    return round(ytm, 4) if ytm > 0 else None


def _calculate_zero_coupon_ytm(
    price: float,
    face_value: float,
    years_to_maturity: float,
) -> float | None:
    """Calculate YTM for zero-coupon bond."""
    if price <= 0 or face_value <= 0 or years_to_maturity <= 0:
        return None

    ytm = ((face_value / price) ** (1 / years_to_maturity) - 1) * 100
    return round(ytm, 4)


def calculate_macaulay_duration(
    ytm: float,
    coupon_rate: float,
    years_to_maturity: float,
    frequency: int = 2,
    face_value: float = 100.0,
) -> float | None:
    """
    Calculate Macaulay Duration.

    Args:
        ytm: Yield to maturity (as percentage)
        coupon_rate: Annual coupon rate (as percentage)
        years_to_maturity: Years until maturity
        frequency: Coupon payments per year
        face_value: Face value

    Returns:
        Macaulay duration in years or None
    """
    if ytm <= 0 or years_to_maturity <= 0:
        return None

    if frequency == 0:
        # Zero coupon bond: duration equals maturity
        return years_to_maturity

    r = ytm / (100 * frequency)
    c = coupon_rate / (100 * frequency)
    n = int(years_to_maturity * frequency)

    if n <= 0:
        return None

    # Calculate weighted present values
    pv_total = 0.0
    weighted_pv = 0.0
    coupon = c * face_value

    for t in range(1, n + 1):
        discount = (1 + r) ** t
        pv = coupon / discount
        pv_total += pv
        weighted_pv += (t / frequency) * pv

    # Face value at maturity
    discount = (1 + r) ** n
    pv_face = face_value / discount
    pv_total += pv_face
    weighted_pv += years_to_maturity * pv_face

    if pv_total <= 0:
        return None

    duration = weighted_pv / pv_total
    return round(duration, 4)


def calculate_modified_duration(
    macaulay_duration: float,
    ytm: float,
    frequency: int = 2,
) -> float | None:
    """
    Calculate Modified Duration from Macaulay Duration.

    Args:
        macaulay_duration: Macaulay duration in years
        ytm: Yield to maturity (as percentage)
        frequency: Coupon payments per year

    Returns:
        Modified duration or None
    """
    if macaulay_duration is None or ytm <= 0:
        return None

    r = ytm / 100
    if frequency == 0:
        # Zero-coupon: modified duration = macaulay / (1 + r)
        mod_duration = macaulay_duration / (1 + r)
    else:
        mod_duration = macaulay_duration / (1 + r / frequency)
    return round(mod_duration, 4)


def calculate_convexity(
    ytm: float,
    coupon_rate: float,
    years_to_maturity: float,
    frequency: int = 2,
    face_value: float = 100.0,
) -> float | None:
    """
    Calculate bond convexity.

    Args:
        ytm: Yield to maturity (as percentage)
        coupon_rate: Annual coupon rate (as percentage)
        years_to_maturity: Years until maturity
        frequency: Coupon payments per year
        face_value: Face value

    Returns:
        Convexity or None
    """
    if ytm <= 0 or years_to_maturity <= 0:
        return None

    if frequency == 0:
        # Zero-coupon: convexity = n*(n+1) / (1+r)^2
        r = ytm / 100
        return round(years_to_maturity * (years_to_maturity + 1) / (1 + r) ** 2, 4)

    r = ytm / (100 * frequency)
    c = coupon_rate / (100 * frequency)
    n = int(years_to_maturity * frequency)

    if n <= 0:
        return None

    pv_total = 0.0
    convexity_sum = 0.0
    coupon = c * face_value

    for t in range(1, n + 1):
        discount = (1 + r) ** t
        pv = coupon / discount
        pv_total += pv
        convexity_sum += (t * (t + 1)) * pv / ((1 + r) ** 2)

    # Face value at maturity
    discount = (1 + r) ** n
    pv_face = face_value / discount
    pv_total += pv_face
    convexity_sum += (n * (n + 1)) * pv_face / ((1 + r) ** 2)

    if pv_total <= 0:
        return None

    convexity = convexity_sum / (pv_total * frequency * frequency)
    return round(convexity, 4)


def calculate_accrued_interest(
    coupon_rate: float,
    face_value: float,
    days_since_last_coupon: int,
    days_in_coupon_period: int = 182,
) -> float:
    """
    Calculate accrued interest.

    Args:
        coupon_rate: Annual coupon rate (as percentage)
        face_value: Face value
        days_since_last_coupon: Days since last coupon payment
        days_in_coupon_period: Days in coupon period (default: 182 for semi-annual)

    Returns:
        Accrued interest amount
    """
    if days_in_coupon_period <= 0:
        return 0.0

    semi_annual_coupon = (coupon_rate / 100) * face_value / 2
    accrued = semi_annual_coupon * (days_since_last_coupon / days_in_coupon_period)
    return round(accrued, 4)


def calculate_current_yield(
    coupon_rate: float,
    price: float,
    face_value: float = 100.0,
) -> float | None:
    """
    Calculate current yield.

    Args:
        coupon_rate: Annual coupon rate (as percentage)
        price: Current price
        face_value: Face value

    Returns:
        Current yield as percentage or None
    """
    if price <= 0:
        return None

    annual_coupon = (coupon_rate / 100) * face_value
    current_yield = (annual_coupon / price) * 100
    return round(current_yield, 4)


def compute_sukuk_analytics(
    sukuk: dict[str, Any],
    quote: dict[str, Any] | None = None,
    calc_date: str | None = None,
) -> SukukAnalyticsResult:
    """
    Compute full analytics for a sukuk instrument.

    Args:
        sukuk: Sukuk master data dict
        quote: Optional quote data (uses latest if None)
        calc_date: Calculation date (default: today)

    Returns:
        SukukAnalyticsResult
    """
    if calc_date is None:
        calc_date = datetime.now().date().isoformat()

    instrument_id = sukuk["instrument_id"]
    result = SukukAnalyticsResult(
        instrument_id=instrument_id,
        calc_date=calc_date,
    )

    # Get maturity info
    maturity_date = sukuk.get("maturity_date")
    if not maturity_date:
        return result

    try:
        maturity = datetime.strptime(maturity_date, "%Y-%m-%d").date()
        calc = datetime.strptime(calc_date, "%Y-%m-%d").date()
        days_to_maturity = (maturity - calc).days
        years_to_maturity = days_to_maturity / 365.0
    except Exception:
        return result

    if days_to_maturity <= 0:
        return result

    result.days_to_maturity = days_to_maturity

    # Get pricing info
    # PSX prices are quoted per Rs.100 par, so always use 100 for analytics
    face_value = 100.0
    coupon_rate = sukuk.get("coupon_rate") or 0.0
    frequency = sukuk.get("coupon_frequency", 2)

    if quote:
        clean_price = quote.get("clean_price")
        dirty_price = quote.get("dirty_price")
        ytm = quote.get("yield_to_maturity")
    else:
        clean_price = None
        dirty_price = None
        ytm = None

    result.clean_price = clean_price
    result.dirty_price = dirty_price

    # Calculate YTM if we have price but no YTM
    price_for_ytm = dirty_price or clean_price
    if price_for_ytm and not ytm:
        ytm = calculate_ytm(
            price=price_for_ytm,
            face_value=face_value,
            coupon_rate=coupon_rate,
            years_to_maturity=years_to_maturity,
            frequency=frequency,
        )

    result.yield_to_maturity = ytm

    # Calculate duration and convexity if we have YTM
    if ytm:
        result.macaulay_duration = calculate_macaulay_duration(
            ytm=ytm,
            coupon_rate=coupon_rate,
            years_to_maturity=years_to_maturity,
            frequency=frequency,
            face_value=face_value,
        )

        if result.macaulay_duration:
            result.modified_duration = calculate_modified_duration(
                macaulay_duration=result.macaulay_duration,
                ytm=ytm,
                frequency=frequency,
            )

        result.convexity = calculate_convexity(
            ytm=ytm,
            coupon_rate=coupon_rate,
            years_to_maturity=years_to_maturity,
            frequency=frequency,
            face_value=face_value,
        )

    # Current yield
    if clean_price and coupon_rate:
        result.current_yield = calculate_current_yield(
            coupon_rate=coupon_rate,
            price=clean_price,
            face_value=face_value,
        )

    return result


def get_sukuk_analytics_full(
    instrument_id: str,
    db_path: Path | str | None = None,
    calc_date: str | None = None,
) -> dict[str, Any]:
    """
    Get full analytics for a sukuk instrument from database.

    Args:
        instrument_id: Sukuk instrument ID
        db_path: Database path
        calc_date: Calculation date

    Returns:
        Dict with sukuk info and analytics
    """
    con = connect(db_path)
    init_schema(con)

    sukuk = get_sukuk(con, instrument_id)
    if not sukuk:
        con.close()
        return {"error": f"Sukuk not found: {instrument_id}"}

    quote = get_sukuk_latest_quote(con, instrument_id)
    analytics = compute_sukuk_analytics(sukuk, quote, calc_date)

    # Store analytics snapshot
    upsert_sukuk_analytics(con, analytics.to_dict())

    con.close()

    return {
        "sukuk": sukuk,
        "quote": quote,
        "analytics": analytics.to_dict(),
    }


def compute_and_store_analytics(
    db_path: Path | str | None = None,
    instrument_ids: list[str] | None = None,
    calc_date: str | None = None,
) -> dict[str, Any]:
    """
    Compute and store analytics for multiple sukuk.

    Args:
        db_path: Database path
        instrument_ids: List of instruments (None = all)
        calc_date: Calculation date

    Returns:
        Summary dict
    """
    con = connect(db_path)
    init_schema(con)

    sukuk_list = get_sukuk_list(con, active_only=True)
    if instrument_ids:
        sukuk_list = [s for s in sukuk_list if s["instrument_id"] in instrument_ids]

    computed = 0
    failed = 0

    for sukuk in sukuk_list:
        quote = get_sukuk_latest_quote(con, sukuk["instrument_id"])
        analytics = compute_sukuk_analytics(sukuk, quote, calc_date)

        if analytics.yield_to_maturity is not None:
            if upsert_sukuk_analytics(con, analytics.to_dict()):
                computed += 1
            else:
                failed += 1
        else:
            failed += 1

    con.close()

    return {
        "success": True,
        "computed": computed,
        "failed": failed,
        "total": len(sukuk_list),
    }


def get_analytics_by_category(
    category: str | None = None,
    db_path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """
    Get analytics summary grouped by category.

    Args:
        category: Optional filter by category
        db_path: Database path

    Returns:
        List of analytics results
    """
    con = connect(db_path)
    init_schema(con)

    sukuk_list = get_sukuk_list(con, active_only=True, category=category)
    results = []

    for sukuk in sukuk_list:
        quote = get_sukuk_latest_quote(con, sukuk["instrument_id"])
        analytics = compute_sukuk_analytics(sukuk, quote)

        results.append({
            "instrument_id": sukuk["instrument_id"],
            "name": sukuk.get("name"),
            "category": sukuk.get("category"),
            "maturity_date": sukuk.get("maturity_date"),
            "coupon_rate": sukuk.get("coupon_rate"),
            "shariah_compliant": sukuk.get("shariah_compliant"),
            "clean_price": analytics.clean_price,
            "ytm": analytics.yield_to_maturity,
            "duration": analytics.modified_duration,
            "convexity": analytics.convexity,
            "days_to_maturity": analytics.days_to_maturity,
        })

    con.close()

    # Sort by YTM descending
    results.sort(key=lambda x: x.get("ytm") or 0, reverse=True)

    return results


def interpolate_yield_curve(
    curve_points: list[dict],
    target_tenor_days: int,
) -> float | None:
    """
    Interpolate yield for a specific tenor from curve points.

    Uses linear interpolation between adjacent points.

    Args:
        curve_points: List of curve points with tenor_days and yield_rate
        target_tenor_days: Target tenor in days

    Returns:
        Interpolated yield rate or None
    """
    if not curve_points:
        return None

    # Sort by tenor
    points = sorted(curve_points, key=lambda x: x.get("tenor_days", 0))

    # Find surrounding points
    lower = None
    upper = None

    for point in points:
        tenor = point.get("tenor_days", 0)
        if tenor <= target_tenor_days:
            lower = point
        if tenor >= target_tenor_days and upper is None:
            upper = point
            break

    # Exact match
    if lower and lower.get("tenor_days") == target_tenor_days:
        return lower.get("yield_rate")

    # Extrapolation not supported
    if lower is None or upper is None:
        return None

    # Linear interpolation
    x0 = lower["tenor_days"]
    x1 = upper["tenor_days"]
    y0 = lower["yield_rate"]
    y1 = upper["yield_rate"]

    if x1 == x0:
        return y0

    slope = (y1 - y0) / (x1 - x0)
    interpolated = y0 + slope * (target_tenor_days - x0)

    return round(interpolated, 4)


def get_yield_curve_data(
    curve_name: str = "GOP_SUKUK",
    curve_date: str | None = None,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Get yield curve data for charting.

    Args:
        curve_name: Name of the curve
        curve_date: Specific date (default: latest)
        db_path: Database path

    Returns:
        Dict with curve points and metadata
    """
    con = connect(db_path)
    init_schema(con)

    curve = get_sukuk_latest_yield_curve(con, curve_name, curve_date)
    con.close()

    if not curve:
        return {
            "curve_name": curve_name,
            "curve_date": curve_date,
            "points": [],
            "error": "No curve data found",
        }

    # Format for charting
    points = []
    for point in curve:
        tenor_days = point.get("tenor_days", 0)
        tenor_label = _tenor_days_to_label(tenor_days)
        points.append({
            "tenor_days": tenor_days,
            "tenor_label": tenor_label,
            "yield_rate": point.get("yield_rate"),
        })

    # Sort by tenor
    points.sort(key=lambda x: x["tenor_days"])

    return {
        "curve_name": curve_name,
        "curve_date": curve[0].get("curve_date") if curve else None,
        "points": points,
    }


def _tenor_days_to_label(days: int) -> str:
    """Convert tenor days to human-readable label."""
    if days <= 30:
        return f"{days}D"
    elif days <= 90:
        return f"{days // 30}M"
    elif days < 365:
        months = round(days / 30)
        return f"{months}M"
    else:
        years = round(days / 365, 1)
        if years == int(years):
            return f"{int(years)}Y"
        return f"{years}Y"


def compare_sukuk(
    instrument_ids: list[str],
    db_path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """
    Compare multiple sukuk instruments.

    Args:
        instrument_ids: List of instrument IDs to compare
        db_path: Database path

    Returns:
        List of comparison data
    """
    con = connect(db_path)
    init_schema(con)

    results = []

    for instrument_id in instrument_ids:
        sukuk = get_sukuk(con, instrument_id)
        if not sukuk:
            continue

        quote = get_sukuk_latest_quote(con, instrument_id)
        analytics = compute_sukuk_analytics(sukuk, quote)

        results.append({
            "instrument_id": instrument_id,
            "name": sukuk.get("name"),
            "category": sukuk.get("category"),
            "issuer": sukuk.get("issuer"),
            "maturity_date": sukuk.get("maturity_date"),
            "coupon_rate": sukuk.get("coupon_rate"),
            "shariah_compliant": sukuk.get("shariah_compliant"),
            "clean_price": analytics.clean_price,
            "ytm": analytics.yield_to_maturity,
            "macaulay_duration": analytics.macaulay_duration,
            "modified_duration": analytics.modified_duration,
            "convexity": analytics.convexity,
            "current_yield": analytics.current_yield,
            "days_to_maturity": analytics.days_to_maturity,
        })

    con.close()

    return results


def get_historical_analytics(
    instrument_id: str,
    db_path: Path | str | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """
    Get historical analytics snapshots for a sukuk.

    Args:
        instrument_id: Sukuk instrument ID
        db_path: Database path
        limit: Maximum records to return

    Returns:
        List of historical analytics
    """
    con = connect(db_path)
    init_schema(con)

    analytics = get_sukuk_analytics(con, instrument_id, limit=limit)
    con.close()

    return analytics
