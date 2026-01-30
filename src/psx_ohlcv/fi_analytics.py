"""
Fixed Income Analytics Module.

Bloomberg/Reuters-style analytics for Pakistani debt securities:
- Yield calculations (YTM, Discount Yield, Current Yield)
- Duration & Risk (Macaulay, Modified, DV01, Convexity)
- Price Analytics (Clean, Dirty, Accrued Interest)
- Spread Analytics (G-Spread, Z-Spread)
- Curve Analytics (Govt Curve, Sukuk Curve)

Naming conventions follow industry standards:
- YTM: Yield to Maturity
- DY: Discount Yield (money market)
- CY: Current Yield
- BEY: Bond Equivalent Yield
- DUR: Macaulay Duration
- MDUR: Modified Duration
- DV01: Dollar Value of 01 (price change per 1bp yield move)
- CNVX: Convexity
- AI: Accrued Interest
- GVT_SPRD: Government Spread

All data is READ-ONLY and for informational purposes only.
"""

import logging
import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

# Day count conventions
DAY_COUNT_ACT_ACT = "ACT/ACT"
DAY_COUNT_ACT_365 = "ACT/365"
DAY_COUNT_30_360 = "30/360"

# Coupon frequencies
FREQ_ANNUAL = 1
FREQ_SEMI_ANNUAL = 2
FREQ_QUARTERLY = 4
FREQ_MONTHLY = 12
FREQ_ZERO = 0  # Zero coupon / discount

# Security type classifications (Bloomberg-style)
SECURITY_TYPES = {
    "MTB": "Market Treasury Bill",
    "PIB": "Pakistan Investment Bond",
    "GIS": "Government Ijarah Sukuk",
    "FRR": "Fixed Rate Rental Sukuk",
    "VRR": "Variable Rate Rental Sukuk",
    "FRN": "Floating Rate Note",
    "TFC": "Term Finance Certificate",
    "SUKUK": "Corporate Sukuk",
}


@dataclass
class YieldMetrics:
    """Yield analytics for a fixed income security."""

    ytm: float | None = None  # Yield to Maturity (annualized)
    discount_yield: float | None = None  # Discount Yield (T-Bills)
    current_yield: float | None = None  # Current Yield (coupon/price)
    bey: float | None = None  # Bond Equivalent Yield
    simple_yield: float | None = None  # Simple yield (no compounding)
    spread_to_govt: float | None = None  # Spread vs govt benchmark


@dataclass
class DurationMetrics:
    """Duration and risk analytics."""

    macaulay_dur: float | None = None  # Macaulay Duration (years)
    modified_dur: float | None = None  # Modified Duration
    dv01: float | None = None  # Dollar Value of 01bp (per 100 face)
    convexity: float | None = None  # Convexity
    effective_dur: float | None = None  # Effective Duration (for callables)


@dataclass
class PriceMetrics:
    """Price analytics."""

    clean_price: float | None = None  # Clean price (excluding AI)
    dirty_price: float | None = None  # Dirty/Full price (including AI)
    accrued_interest: float | None = None  # Accrued interest
    theoretical_price: float | None = None  # DCF-based fair value
    price_change_1d: float | None = None  # 1-day price change %
    price_change_1w: float | None = None  # 1-week price change %
    price_change_1m: float | None = None  # 1-month price change %


@dataclass
class SecurityAnalytics:
    """Complete analytics for a fixed income security."""

    symbol: str
    name: str | None = None
    security_type: str | None = None
    category: str | None = None

    # Key dates
    issue_date: str | None = None
    maturity_date: str | None = None
    settlement_date: str | None = None

    # Basic info
    face_value: float = 5000.0
    coupon_rate: float = 0.0  # Annual coupon rate (decimal)
    coupon_frequency: int = FREQ_SEMI_ANNUAL
    day_count: str = DAY_COUNT_ACT_ACT

    # Calculated metrics
    days_to_maturity: int | None = None
    years_to_maturity: float | None = None

    # Analytics
    yield_metrics: YieldMetrics | None = None
    duration_metrics: DurationMetrics | None = None
    price_metrics: PriceMetrics | None = None

    # Market data
    last_price: float | None = None
    last_volume: int | None = None
    avg_daily_volume: float | None = None


def days_between(date1: str | date, date2: str | date) -> int:
    """Calculate days between two dates."""
    if isinstance(date1, str):
        date1 = datetime.strptime(date1, "%Y-%m-%d").date()
    if isinstance(date2, str):
        date2 = datetime.strptime(date2, "%Y-%m-%d").date()
    return (date2 - date1).days


def year_fraction(
    date1: str | date,
    date2: str | date,
    day_count: str = DAY_COUNT_ACT_365,
) -> float:
    """Calculate year fraction between two dates."""
    days = days_between(date1, date2)

    if day_count == DAY_COUNT_ACT_365:
        return days / 365.0
    elif day_count == DAY_COUNT_ACT_ACT:
        return days / 365.25
    elif day_count == DAY_COUNT_30_360:
        return days / 360.0
    else:
        return days / 365.0


def calculate_discount_yield(
    face_value: float,
    price: float,
    days_to_maturity: int,
) -> float:
    """
    Calculate discount yield for T-Bills (money market convention).

    DY = ((Face - Price) / Face) * (360 / Days)

    Args:
        face_value: Face/par value
        price: Current market price
        days_to_maturity: Days until maturity

    Returns:
        Discount yield as decimal (e.g., 0.12 for 12%)
    """
    if days_to_maturity <= 0 or face_value <= 0:
        return 0.0

    discount = face_value - price
    return (discount / face_value) * (360 / days_to_maturity)


def calculate_bond_equivalent_yield(
    face_value: float,
    price: float,
    days_to_maturity: int,
) -> float:
    """
    Calculate Bond Equivalent Yield (BEY) for T-Bills.

    BEY = ((Face - Price) / Price) * (365 / Days)

    This is comparable to bond yields.

    Args:
        face_value: Face/par value
        price: Current market price
        days_to_maturity: Days until maturity

    Returns:
        BEY as decimal
    """
    if days_to_maturity <= 0 or price <= 0:
        return 0.0

    discount = face_value - price
    return (discount / price) * (365 / days_to_maturity)


def calculate_current_yield(
    coupon_rate: float,
    price: float,
    face_value: float = 100.0,
) -> float:
    """
    Calculate current yield.

    CY = (Annual Coupon / Price) * 100

    Args:
        coupon_rate: Annual coupon rate (decimal)
        price: Current market price (per 100 face)
        face_value: Face value for scaling

    Returns:
        Current yield as decimal
    """
    if price <= 0:
        return 0.0

    annual_coupon = coupon_rate * face_value
    return annual_coupon / price


def calculate_ytm(
    price: float,
    face_value: float,
    coupon_rate: float,
    years_to_maturity: float,
    frequency: int = FREQ_SEMI_ANNUAL,
    tolerance: float = 1e-6,
    max_iterations: int = 100,
) -> float | None:
    """
    Calculate Yield to Maturity using Newton-Raphson method.

    Args:
        price: Current market price
        face_value: Face/par value
        coupon_rate: Annual coupon rate (decimal)
        years_to_maturity: Years until maturity
        frequency: Coupon payments per year
        tolerance: Convergence tolerance
        max_iterations: Maximum iterations

    Returns:
        YTM as decimal, or None if doesn't converge
    """
    if years_to_maturity <= 0 or price <= 0:
        return None

    # Handle zero coupon bonds
    if coupon_rate == 0 or frequency == 0:
        # Simple discount bond YTM
        try:
            ytm = (face_value / price) ** (1 / years_to_maturity) - 1
            return ytm
        except (ValueError, ZeroDivisionError):
            return None

    # Periodic coupon payment
    coupon = (coupon_rate * face_value) / frequency
    n_periods = int(years_to_maturity * frequency)

    if n_periods <= 0:
        return None

    # Initial guess based on current yield
    ytm = coupon_rate

    for _ in range(max_iterations):
        # Calculate price from current YTM guess
        r = ytm / frequency
        if r <= -1:
            r = 0.001

        try:
            pv_coupons = coupon * (1 - (1 + r) ** (-n_periods)) / r
            pv_face = face_value / ((1 + r) ** n_periods)
            calc_price = pv_coupons + pv_face

            # Price difference
            diff = calc_price - price

            if abs(diff) < tolerance:
                return ytm

            # Derivative for Newton-Raphson
            d_pv_coupons = -coupon * n_periods * (1 + r) ** (-n_periods - 1) / r
            d_pv_coupons += coupon * (1 - (1 + r) ** (-n_periods)) / (r * r)
            d_pv_face = -n_periods * face_value / ((1 + r) ** (n_periods + 1))
            derivative = (d_pv_coupons + d_pv_face) / frequency

            if abs(derivative) < 1e-10:
                break

            ytm = ytm - diff / derivative

        except (ValueError, ZeroDivisionError, OverflowError):
            break

    return ytm if abs(diff) < tolerance * 100 else None


def calculate_macaulay_duration(
    price: float,
    face_value: float,
    coupon_rate: float,
    years_to_maturity: float,
    ytm: float,
    frequency: int = FREQ_SEMI_ANNUAL,
) -> float | None:
    """
    Calculate Macaulay Duration.

    Duration = Sum(t * PV(CF_t)) / Price

    Args:
        price: Current market price
        face_value: Face/par value
        coupon_rate: Annual coupon rate (decimal)
        years_to_maturity: Years until maturity
        ytm: Yield to maturity (decimal)
        frequency: Coupon payments per year

    Returns:
        Macaulay duration in years
    """
    if years_to_maturity <= 0 or price <= 0:
        return None

    # Zero coupon bond
    if coupon_rate == 0 or frequency == 0:
        return years_to_maturity

    coupon = (coupon_rate * face_value) / frequency
    n_periods = int(years_to_maturity * frequency)
    r = ytm / frequency

    if r <= -1:
        return None

    weighted_sum = 0.0

    try:
        for t in range(1, n_periods + 1):
            period_years = t / frequency
            pv_cf = coupon / ((1 + r) ** t)
            weighted_sum += period_years * pv_cf

        # Add final principal
        pv_principal = face_value / ((1 + r) ** n_periods)
        weighted_sum += years_to_maturity * pv_principal

        return weighted_sum / price

    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def calculate_modified_duration(
    macaulay_duration: float,
    ytm: float,
    frequency: int = FREQ_SEMI_ANNUAL,
) -> float:
    """
    Calculate Modified Duration.

    ModDur = MacDur / (1 + YTM/frequency)

    Args:
        macaulay_duration: Macaulay duration in years
        ytm: Yield to maturity (decimal)
        frequency: Coupon payments per year

    Returns:
        Modified duration
    """
    if frequency == 0:
        frequency = 1
    return macaulay_duration / (1 + ytm / frequency)


def calculate_dv01(
    modified_duration: float,
    price: float,
    face_value: float = 100.0,
) -> float:
    """
    Calculate DV01 (Dollar Value of 01bp).

    DV01 = ModDur * Price * 0.0001

    This is the price change for a 1bp move in yield.

    Args:
        modified_duration: Modified duration
        price: Current price (per 100 face)
        face_value: Face value for scaling

    Returns:
        DV01 per 100 face value
    """
    return modified_duration * (price / 100) * 0.0001 * face_value


def calculate_convexity(
    price: float,
    face_value: float,
    coupon_rate: float,
    years_to_maturity: float,
    ytm: float,
    frequency: int = FREQ_SEMI_ANNUAL,
) -> float | None:
    """
    Calculate bond convexity.

    Convexity = Sum(t*(t+1) * PV(CF_t)) / (Price * (1+y)^2)

    Args:
        price: Current market price
        face_value: Face/par value
        coupon_rate: Annual coupon rate (decimal)
        years_to_maturity: Years until maturity
        ytm: Yield to maturity (decimal)
        frequency: Coupon payments per year

    Returns:
        Convexity
    """
    if years_to_maturity <= 0 or price <= 0:
        return None

    if coupon_rate == 0 or frequency == 0:
        # Zero coupon convexity
        return years_to_maturity * (years_to_maturity + 1) / ((1 + ytm) ** 2)

    coupon = (coupon_rate * face_value) / frequency
    n_periods = int(years_to_maturity * frequency)
    r = ytm / frequency

    if r <= -1:
        return None

    weighted_sum = 0.0

    try:
        for t in range(1, n_periods + 1):
            pv_cf = coupon / ((1 + r) ** t)
            t_years = t / frequency
            weighted_sum += t_years * (t_years + 1 / frequency) * pv_cf

        # Add principal
        pv_principal = face_value / ((1 + r) ** n_periods)
        t_years = years_to_maturity
        weighted_sum += t_years * (t_years + 1 / frequency) * pv_principal

        return weighted_sum / (price * (1 + r) ** 2)

    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def calculate_accrued_interest(
    face_value: float,
    coupon_rate: float,
    last_coupon_date: str | date,
    settlement_date: str | date | None = None,
    frequency: int = FREQ_SEMI_ANNUAL,
    day_count: str = DAY_COUNT_ACT_ACT,
) -> float:
    """
    Calculate accrued interest.

    AI = (Coupon / Frequency) * (Days since last coupon / Days in period)

    Args:
        face_value: Face/par value
        coupon_rate: Annual coupon rate (decimal)
        last_coupon_date: Last coupon payment date
        settlement_date: Settlement date (default: today)
        frequency: Coupon payments per year
        day_count: Day count convention

    Returns:
        Accrued interest
    """
    if coupon_rate == 0 or frequency == 0:
        return 0.0

    if settlement_date is None:
        settlement_date = date.today()

    days_accrued = days_between(last_coupon_date, settlement_date)

    if day_count == DAY_COUNT_30_360:
        days_in_period = 360 / frequency
    else:
        days_in_period = 365 / frequency

    coupon_payment = (coupon_rate * face_value) / frequency
    accrued = coupon_payment * (days_accrued / days_in_period)

    return max(0, accrued)


def normalize_price(
    price: float,
    face_value: float,
) -> float:
    """
    Normalize price to per-100 basis for yield calculations.

    PSX debt prices are quoted per face value, so we need to
    convert to per-100 for standard bond math.

    Args:
        price: Market price
        face_value: Face value of the security

    Returns:
        Price per 100 face value
    """
    if face_value <= 0 or price <= 0:
        return price
    return (price / face_value) * 100


def analyze_security(
    symbol: str,
    name: str | None,
    security_type: str | None,
    face_value: float,
    coupon_rate: float,
    maturity_date: str,
    price: float | None,
    issue_date: str | None = None,
    prev_coupon_date: str | None = None,
    frequency: int = FREQ_SEMI_ANNUAL,
    day_count: str = DAY_COUNT_ACT_ACT,
    settlement_date: str | None = None,
    price_is_per_100: bool = True,
) -> SecurityAnalytics:
    """
    Perform comprehensive analysis of a fixed income security.

    Args:
        symbol: Security symbol
        name: Security name
        security_type: Type (T-Bill, PIB, GIS, TFC, etc.)
        face_value: Face/par value
        coupon_rate: Annual coupon rate (decimal, e.g., 0.12 for 12%)
        maturity_date: Maturity date (YYYY-MM-DD)
        price: Current market price
        issue_date: Issue date
        prev_coupon_date: Previous coupon date (for AI calculation)
        frequency: Coupon payments per year
        day_count: Day count convention
        settlement_date: Settlement date (default: today)

    Returns:
        SecurityAnalytics with all calculated metrics
    """
    if settlement_date is None:
        settlement_date = date.today().strftime("%Y-%m-%d")

    analytics = SecurityAnalytics(
        symbol=symbol,
        name=name,
        security_type=security_type,
        face_value=face_value,
        coupon_rate=coupon_rate,
        coupon_frequency=frequency,
        day_count=day_count,
        issue_date=issue_date,
        maturity_date=maturity_date,
        settlement_date=settlement_date,
    )

    # Calculate time to maturity
    try:
        analytics.days_to_maturity = days_between(settlement_date, maturity_date)
        analytics.years_to_maturity = year_fraction(
            settlement_date, maturity_date, day_count
        )
    except (ValueError, TypeError):
        return analytics

    if analytics.days_to_maturity <= 0:
        return analytics

    # Initialize metrics
    yield_metrics = YieldMetrics()
    duration_metrics = DurationMetrics()
    price_metrics = PriceMetrics()

    if price is not None and price > 0:
        analytics.last_price = price
        price_metrics.clean_price = price

        # Normalize to per-100 basis for bond math
        # PSX prices are typically quoted as percentage of face (e.g., 98.42)
        if price_is_per_100:
            price_per_100 = price
            face_per_100 = 100.0
        else:
            # Price is absolute, normalize it
            price_per_100 = (price / face_value) * 100
            face_per_100 = 100.0

        # Yield calculations
        is_tbill = security_type in ("T-Bill", "MTB") or coupon_rate == 0

        if is_tbill:
            # T-Bill / Discount instrument yields
            # For T-Bills: discount from face (100) to price
            yield_metrics.discount_yield = calculate_discount_yield(
                face_per_100, price_per_100, analytics.days_to_maturity
            )
            yield_metrics.bey = calculate_bond_equivalent_yield(
                face_per_100, price_per_100, analytics.days_to_maturity
            )
            yield_metrics.ytm = yield_metrics.bey
        else:
            # Coupon bond yields
            yield_metrics.current_yield = calculate_current_yield(
                coupon_rate, price_per_100, face_per_100
            )
            yield_metrics.ytm = calculate_ytm(
                price_per_100,
                face_per_100,
                coupon_rate,
                analytics.years_to_maturity,
                frequency,
            )

        # Duration calculations (if we have YTM)
        ytm = yield_metrics.ytm
        if ytm is not None and ytm > -1 and abs(ytm) < 1:  # Sanity check on YTM
            duration_metrics.macaulay_dur = calculate_macaulay_duration(
                price_per_100,
                face_per_100,
                coupon_rate,
                analytics.years_to_maturity,
                ytm,
                frequency,
            )

            if duration_metrics.macaulay_dur is not None:
                duration_metrics.modified_dur = calculate_modified_duration(
                    duration_metrics.macaulay_dur, ytm, frequency
                )
                duration_metrics.dv01 = calculate_dv01(
                    duration_metrics.modified_dur, price_per_100, face_per_100
                )

            duration_metrics.convexity = calculate_convexity(
                price_per_100,
                face_per_100,
                coupon_rate,
                analytics.years_to_maturity,
                ytm,
                frequency,
            )

        # Accrued interest (calculated on actual face value, then normalized)
        if prev_coupon_date and coupon_rate > 0:
            # Calculate AI per 100 face
            ai_per_100 = calculate_accrued_interest(
                face_per_100,
                coupon_rate,
                prev_coupon_date,
                settlement_date,
                frequency,
                day_count,
            )
            price_metrics.accrued_interest = ai_per_100
            if ai_per_100:
                price_metrics.dirty_price = price_per_100 + ai_per_100

    analytics.yield_metrics = yield_metrics
    analytics.duration_metrics = duration_metrics
    analytics.price_metrics = price_metrics

    return analytics


def calculate_curve_point(
    securities: list[dict],
    tenor_years: float,
) -> float | None:
    """
    Interpolate yield for a specific tenor from a list of securities.

    Args:
        securities: List of security dicts with 'years_to_maturity' and 'ytm'
        tenor_years: Target tenor in years

    Returns:
        Interpolated yield, or None if not enough data
    """
    # Sort by maturity
    sorted_secs = sorted(
        [s for s in securities if s.get("years_to_maturity") and s.get("ytm")],
        key=lambda x: x["years_to_maturity"],
    )

    if len(sorted_secs) < 2:
        return None

    # Find bracketing points
    for i in range(len(sorted_secs) - 1):
        t1 = sorted_secs[i]["years_to_maturity"]
        t2 = sorted_secs[i + 1]["years_to_maturity"]

        if t1 <= tenor_years <= t2:
            y1 = sorted_secs[i]["ytm"]
            y2 = sorted_secs[i + 1]["ytm"]

            # Linear interpolation
            weight = (tenor_years - t1) / (t2 - t1) if t2 != t1 else 0
            return y1 + weight * (y2 - y1)

    # Extrapolate if outside range
    if tenor_years < sorted_secs[0]["years_to_maturity"]:
        return sorted_secs[0]["ytm"]
    else:
        return sorted_secs[-1]["ytm"]


def build_yield_curve(
    securities: list[dict],
    curve_name: str = "PKR_GOVT",
    tenors: list[float] | None = None,
) -> dict:
    """
    Build a yield curve from a list of securities.

    Args:
        securities: List of security dicts with analytics
        curve_name: Curve identifier
        tenors: Specific tenors to calculate (default: standard tenors)

    Returns:
        Dict with curve points and metadata
    """
    if tenors is None:
        tenors = [0.25, 0.5, 1, 2, 3, 5, 7, 10, 15, 20, 30]

    curve_points = []
    for tenor in tenors:
        ytm = calculate_curve_point(securities, tenor)
        if ytm is not None:
            curve_points.append({
                "tenor": tenor,
                "yield": ytm,
                "tenor_label": _tenor_label(tenor),
            })

    # Calculate curve metrics
    curve_2y = next((p["yield"] for p in curve_points if p["tenor"] == 2), None)
    curve_10y = next((p["yield"] for p in curve_points if p["tenor"] == 10), None)

    steepness_2s10s = None
    if curve_2y is not None and curve_10y is not None:
        steepness_2s10s = (curve_10y - curve_2y) * 10000  # In basis points

    return {
        "curve_name": curve_name,
        "curve_date": date.today().strftime("%Y-%m-%d"),
        "points": curve_points,
        "steepness_2s10s": steepness_2s10s,
        "security_count": len(securities),
    }


def _tenor_label(years: float) -> str:
    """Convert tenor in years to label (e.g., '3M', '2Y')."""
    if years < 1:
        months = int(years * 12)
        return f"{months}M"
    elif years == int(years):
        return f"{int(years)}Y"
    else:
        return f"{years}Y"


# Market-wide analytics
def calculate_market_summary(securities: list[SecurityAnalytics]) -> dict:
    """
    Calculate market-wide summary statistics.

    Args:
        securities: List of SecurityAnalytics objects

    Returns:
        Dict with market summary metrics
    """
    total_outstanding = 0.0
    weighted_yield_sum = 0.0
    weighted_duration_sum = 0.0
    total_volume = 0

    by_type = {}
    by_issuer = {}

    for sec in securities:
        # Skip securities without price data
        if not sec.last_price:
            continue

        # Estimate outstanding value (face * volume as proxy)
        outstanding = sec.face_value * (sec.last_volume or 0)
        total_outstanding += outstanding

        # Weighted averages
        if sec.yield_metrics and sec.yield_metrics.ytm:
            weighted_yield_sum += sec.yield_metrics.ytm * outstanding

        if sec.duration_metrics and sec.duration_metrics.modified_dur:
            weighted_duration_sum += sec.duration_metrics.modified_dur * outstanding

        if sec.last_volume:
            total_volume += sec.last_volume

        # By type
        sec_type = sec.security_type or "Other"
        if sec_type not in by_type:
            by_type[sec_type] = {"count": 0, "volume": 0}
        by_type[sec_type]["count"] += 1
        by_type[sec_type]["volume"] += sec.last_volume or 0

    # Calculate weighted averages
    weighted_avg_yield = None
    weighted_avg_duration = None

    if total_outstanding > 0:
        weighted_avg_yield = weighted_yield_sum / total_outstanding
        weighted_avg_duration = weighted_duration_sum / total_outstanding

    return {
        "total_outstanding": total_outstanding,
        "total_volume": total_volume,
        "security_count": len(securities),
        "weighted_avg_yield": weighted_avg_yield,
        "weighted_avg_duration": weighted_avg_duration,
        "by_type": by_type,
        "as_of_date": date.today().strftime("%Y-%m-%d"),
    }
