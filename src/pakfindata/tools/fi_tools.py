"""Fixed income tools for agentic AI.

Wraps existing fixed income functions as callable tools for AI agents.
These tools provide access to sukuk, bonds, yield curves, and
fixed income analytics for the Pakistan debt market.
"""

from datetime import datetime, timedelta
from typing import Any

from ..db import (
    get_connection,
    get_sukuk_list,
    get_sukuk,
    get_sukuk_latest_quote,
    get_sukuk_yield_curve,
    get_sukuk_latest_yield_curve,
    get_sukuk_data_summary,
    get_bonds,
    get_bond,
    get_bond_latest_quote,
    get_yield_curve,
    get_bond_data_summary,
)
from .registry import Tool, ToolCategory, ToolRegistry


# =============================================================================
# Sukuk Tools
# =============================================================================


def get_sukuk_instruments(
    category: str | None = None,
    shariah_only: bool = False,
    active_only: bool = True,
) -> dict[str, Any]:
    """Get list of sukuk instruments.

    Args:
        category: Filter by category (e.g., "GOP", "CORPORATE")
        shariah_only: Only return shariah-compliant instruments
        active_only: Only return active instruments

    Returns:
        Dict with sukuk list and summary
    """
    con = get_connection()

    sukuk_list = get_sukuk_list(con, active_only=active_only)

    # Apply filters
    if category:
        sukuk_list = [s for s in sukuk_list if s.get("category") == category.upper()]
    if shariah_only:
        sukuk_list = [s for s in sukuk_list if s.get("shariah_compliant")]

    # Get summary stats
    categories = {}
    for s in sukuk_list:
        cat = s.get("category", "OTHER")
        categories[cat] = categories.get(cat, 0) + 1

    return {
        "count": len(sukuk_list),
        "categories": categories,
        "instruments": [
            {
                "instrument_id": s.get("instrument_id"),
                "name": s.get("name"),
                "category": s.get("category"),
                "issuer": s.get("issuer"),
                "issue_date": s.get("issue_date"),
                "maturity_date": s.get("maturity_date"),
                "coupon_rate": s.get("coupon_rate"),
                "face_value": s.get("face_value"),
                "shariah_compliant": s.get("shariah_compliant"),
            }
            for s in sukuk_list[:20]  # Limit to 20
        ],
        "showing": min(len(sukuk_list), 20),
    }


ToolRegistry.register(
    Tool(
        name="get_sukuk_instruments",
        description="Get list of sukuk (Islamic bonds) instruments available in the Pakistan market. Can filter by category (GOP for government, CORPORATE for corporate) and shariah compliance.",
        function=get_sukuk_instruments,
        category=ToolCategory.FIXED_INCOME,
        parameters={
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Filter by category (e.g., 'GOP' for government sukuk, 'CORPORATE' for corporate)",
                    "enum": ["GOP", "CORPORATE", "PSE"],
                },
                "shariah_only": {
                    "type": "boolean",
                    "description": "Only return shariah-compliant instruments (default: false)",
                    "default": False,
                },
                "active_only": {
                    "type": "boolean",
                    "description": "Only return active (non-matured) instruments (default: true)",
                    "default": True,
                },
            },
        },
        returns_description="Dict with sukuk instruments list and category breakdown",
    )
)


def get_sukuk_quote(instrument_id: str) -> dict[str, Any]:
    """Get latest quote for a sukuk instrument.

    Args:
        instrument_id: Sukuk instrument ID

    Returns:
        Dict with quote data and instrument details
    """
    con = get_connection()

    # Get instrument details
    sukuk = get_sukuk(con, instrument_id)
    if not sukuk:
        return {
            "instrument_id": instrument_id,
            "error": f"Sukuk instrument not found: {instrument_id}",
        }

    # Get latest quote
    quote = get_sukuk_latest_quote(con, instrument_id)

    result = {
        "instrument_id": instrument_id,
        "name": sukuk.get("name"),
        "issuer": sukuk.get("issuer"),
        "category": sukuk.get("category"),
        "maturity_date": sukuk.get("maturity_date"),
        "coupon_rate": sukuk.get("coupon_rate"),
        "face_value": sukuk.get("face_value"),
    }

    if quote:
        result["quote"] = {
            "quote_date": quote.get("quote_date"),
            "clean_price": quote.get("clean_price"),
            "dirty_price": quote.get("dirty_price"),
            "ytm": quote.get("ytm"),
            "yield_spread": quote.get("yield_spread"),
            "duration": quote.get("duration"),
            "convexity": quote.get("convexity"),
        }
    else:
        result["quote"] = None
        result["note"] = "No quote data available"

    return result


ToolRegistry.register(
    Tool(
        name="get_sukuk_quote",
        description="Get latest quote and yield data for a specific sukuk instrument. Includes price, YTM (yield to maturity), duration, and convexity.",
        function=get_sukuk_quote,
        category=ToolCategory.FIXED_INCOME,
        parameters={
            "type": "object",
            "properties": {
                "instrument_id": {
                    "type": "string",
                    "description": "Sukuk instrument ID",
                },
            },
            "required": ["instrument_id"],
        },
        returns_description="Dict with sukuk details and latest quote",
    )
)


# =============================================================================
# Bond Tools
# =============================================================================


def get_bond_list(
    bond_type: str | None = None,
    include_islamic: bool = True,
    active_only: bool = True,
) -> dict[str, Any]:
    """Get list of bond instruments.

    Args:
        bond_type: Filter by type (e.g., "PIB", "TBILL", "TFC")
        include_islamic: Include Islamic sukuk bonds
        active_only: Only return active (non-matured) bonds

    Returns:
        Dict with bond list and summary
    """
    con = get_connection()

    bonds = get_bonds(con, active_only=active_only)

    # Apply filters
    if bond_type:
        bonds = [b for b in bonds if b.get("bond_type") == bond_type.upper()]
    if not include_islamic:
        bonds = [b for b in bonds if not b.get("is_islamic")]

    # Get summary by type
    types = {}
    for b in bonds:
        bt = b.get("bond_type", "OTHER")
        types[bt] = types.get(bt, 0) + 1

    return {
        "count": len(bonds),
        "bond_types": types,
        "bonds": [
            {
                "bond_id": b.get("bond_id"),
                "symbol": b.get("symbol"),
                "name": b.get("name"),
                "bond_type": b.get("bond_type"),
                "issuer": b.get("issuer"),
                "issue_date": b.get("issue_date"),
                "maturity_date": b.get("maturity_date"),
                "coupon_rate": b.get("coupon_rate"),
                "is_islamic": b.get("is_islamic"),
            }
            for b in bonds[:20]  # Limit to 20
        ],
        "showing": min(len(bonds), 20),
    }


ToolRegistry.register(
    Tool(
        name="get_bond_list",
        description="Get list of bond instruments including PIBs (Pakistan Investment Bonds), T-Bills, TFCs (Term Finance Certificates), and corporate bonds.",
        function=get_bond_list,
        category=ToolCategory.FIXED_INCOME,
        parameters={
            "type": "object",
            "properties": {
                "bond_type": {
                    "type": "string",
                    "description": "Filter by bond type",
                    "enum": ["PIB", "TBILL", "TFC", "SUKUK", "CORPORATE"],
                },
                "include_islamic": {
                    "type": "boolean",
                    "description": "Include Islamic/shariah-compliant bonds (default: true)",
                    "default": True,
                },
                "active_only": {
                    "type": "boolean",
                    "description": "Only return active (non-matured) bonds (default: true)",
                    "default": True,
                },
            },
        },
        returns_description="Dict with bond list and type breakdown",
    )
)


def get_bond_quote(bond_id: str) -> dict[str, Any]:
    """Get latest quote for a bond.

    Args:
        bond_id: Bond ID

    Returns:
        Dict with quote data and bond details
    """
    con = get_connection()

    # Get bond details
    bond = get_bond(con, bond_id)
    if not bond:
        return {
            "bond_id": bond_id,
            "error": f"Bond not found: {bond_id}",
        }

    # Get latest quote
    quote = get_bond_latest_quote(con, bond_id)

    result = {
        "bond_id": bond_id,
        "symbol": bond.get("symbol"),
        "name": bond.get("name"),
        "bond_type": bond.get("bond_type"),
        "issuer": bond.get("issuer"),
        "maturity_date": bond.get("maturity_date"),
        "coupon_rate": bond.get("coupon_rate"),
        "face_value": bond.get("face_value"),
        "is_islamic": bond.get("is_islamic"),
    }

    if quote:
        result["quote"] = {
            "quote_date": quote.get("quote_date"),
            "clean_price": quote.get("clean_price"),
            "dirty_price": quote.get("dirty_price"),
            "ytm": quote.get("ytm"),
            "duration": quote.get("duration"),
            "convexity": quote.get("convexity"),
            "spread_to_benchmark": quote.get("spread_to_benchmark"),
        }
    else:
        result["quote"] = None
        result["note"] = "No quote data available"

    return result


ToolRegistry.register(
    Tool(
        name="get_bond_quote",
        description="Get latest quote and yield data for a specific bond. Includes clean/dirty price, YTM, duration, convexity, and spread.",
        function=get_bond_quote,
        category=ToolCategory.FIXED_INCOME,
        parameters={
            "type": "object",
            "properties": {
                "bond_id": {
                    "type": "string",
                    "description": "Bond ID",
                },
            },
            "required": ["bond_id"],
        },
        returns_description="Dict with bond details and latest quote",
    )
)


# =============================================================================
# Yield Curve Tools
# =============================================================================


def get_yield_curve_data(
    curve_name: str = "PIB",
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """Get yield curve data for a specific curve.

    Args:
        curve_name: Curve name (e.g., "PIB", "TBILL", "GOP_SUKUK", "KIBOR")
        as_of_date: Date for the curve (default: latest available)

    Returns:
        Dict with yield curve points
    """
    con = get_connection()

    if as_of_date:
        # Get curve for specific date
        curve_points = get_yield_curve(con, curve_name, as_of_date)
    else:
        # Get latest curve
        curve_points = get_sukuk_latest_yield_curve(con, curve_name)

    if not curve_points:
        return {
            "curve_name": curve_name,
            "error": f"No yield curve data found for {curve_name}",
        }

    # Sort by tenor
    curve_points = sorted(curve_points, key=lambda x: x.get("tenor_years", 0))

    # Extract curve date
    curve_date = curve_points[0].get("curve_date") if curve_points else None

    return {
        "curve_name": curve_name,
        "curve_date": curve_date,
        "points_count": len(curve_points),
        "points": [
            {
                "tenor": p.get("tenor"),
                "tenor_years": p.get("tenor_years"),
                "yield_pct": p.get("yield_pct"),
                "yield_type": p.get("yield_type"),
            }
            for p in curve_points
        ],
        "short_end": curve_points[0].get("yield_pct") if curve_points else None,
        "long_end": curve_points[-1].get("yield_pct") if curve_points else None,
        "curve_shape": _determine_curve_shape(curve_points) if curve_points else None,
    }


def _determine_curve_shape(points: list[dict]) -> str:
    """Determine the shape of the yield curve."""
    if len(points) < 2:
        return "insufficient_data"

    yields = [p.get("yield_pct", 0) for p in points]
    short_yield = yields[0]
    long_yield = yields[-1]

    if long_yield > short_yield + 0.5:
        return "normal (upward sloping)"
    elif short_yield > long_yield + 0.5:
        return "inverted (downward sloping)"
    else:
        return "flat"


ToolRegistry.register(
    Tool(
        name="get_yield_curve_data",
        description="Get yield curve data for Pakistan government securities (PIBs, T-Bills) or sukuk. Shows yields at different maturities and curve shape (normal, inverted, flat).",
        function=get_yield_curve_data,
        category=ToolCategory.FIXED_INCOME,
        parameters={
            "type": "object",
            "properties": {
                "curve_name": {
                    "type": "string",
                    "description": "Yield curve name (default: PIB)",
                    "enum": ["PIB", "TBILL", "GOP_SUKUK", "KIBOR", "CORPORATE"],
                    "default": "PIB",
                },
                "as_of_date": {
                    "type": "string",
                    "description": "Date for the curve (YYYY-MM-DD format). If not provided, returns latest curve.",
                },
            },
        },
        returns_description="Dict with yield curve points and shape analysis",
    )
)


# =============================================================================
# Fixed Income Analytics Tools
# =============================================================================


def compute_bond_ytm(
    clean_price: float,
    face_value: float,
    coupon_rate: float,
    years_to_maturity: float,
    frequency: int = 2,
) -> dict[str, Any]:
    """Compute yield to maturity for a bond.

    Args:
        clean_price: Current clean price
        face_value: Bond face value
        coupon_rate: Annual coupon rate (as percentage, e.g., 10.5)
        years_to_maturity: Years until maturity
        frequency: Coupon payment frequency per year (1=annual, 2=semi-annual)

    Returns:
        Dict with YTM and related metrics
    """
    if clean_price <= 0 or face_value <= 0 or years_to_maturity <= 0:
        return {"error": "Invalid input parameters"}

    # Convert coupon rate to decimal
    coupon_decimal = coupon_rate / 100
    annual_coupon = face_value * coupon_decimal
    periodic_coupon = annual_coupon / frequency

    # Number of periods
    n_periods = int(years_to_maturity * frequency)

    # Simple YTM approximation using the formula:
    # YTM ≈ (C + (F - P) / n) / ((F + P) / 2)
    # Where C = annual coupon, F = face value, P = price, n = years

    annual_capital_gain = (face_value - clean_price) / years_to_maturity
    avg_price = (face_value + clean_price) / 2
    ytm_approx = (annual_coupon + annual_capital_gain) / avg_price * 100

    # Current yield
    current_yield = (annual_coupon / clean_price) * 100

    # Macaulay Duration approximation
    # D = (1 + y) / y - (1 + y + n(c - y)) / (c((1 + y)^n - 1) + y)
    # Simplified: D ≈ (1 - 1/(1+y)^n) / y + n / (1+y)^n for approximate duration
    y = ytm_approx / 100 / frequency
    if y > 0:
        pv_factor = 1 / ((1 + y) ** n_periods)
        duration_approx = ((1 - pv_factor) / y + n_periods * pv_factor) / frequency
    else:
        duration_approx = years_to_maturity

    # Modified duration
    mod_duration = duration_approx / (1 + ytm_approx / 100 / frequency)

    # Convexity approximation
    convexity = duration_approx * (duration_approx + 1) / (1 + ytm_approx / 100) ** 2

    return {
        "ytm_pct": round(ytm_approx, 4),
        "current_yield_pct": round(current_yield, 4),
        "macaulay_duration": round(duration_approx, 4),
        "modified_duration": round(mod_duration, 4),
        "convexity": round(convexity, 4),
        "inputs": {
            "clean_price": clean_price,
            "face_value": face_value,
            "coupon_rate": coupon_rate,
            "years_to_maturity": years_to_maturity,
            "frequency": frequency,
        },
        "interpretation": {
            "ytm": f"Expected annual return if held to maturity: {round(ytm_approx, 2)}%",
            "duration": f"Price sensitivity: {round(mod_duration, 2)} years modified duration",
            "convexity": f"Curvature: {round(convexity, 2)} (higher = more protection from rate rises)",
        },
    }


ToolRegistry.register(
    Tool(
        name="compute_bond_ytm",
        description="Compute yield to maturity (YTM), duration, and convexity for a bond given its price and terms. Use this for bond valuation and interest rate risk analysis.",
        function=compute_bond_ytm,
        category=ToolCategory.FIXED_INCOME,
        parameters={
            "type": "object",
            "properties": {
                "clean_price": {
                    "type": "number",
                    "description": "Current clean price of the bond",
                },
                "face_value": {
                    "type": "number",
                    "description": "Face/par value of the bond (typically 100 or 1000)",
                },
                "coupon_rate": {
                    "type": "number",
                    "description": "Annual coupon rate as percentage (e.g., 10.5 for 10.5%)",
                },
                "years_to_maturity": {
                    "type": "number",
                    "description": "Years until bond maturity",
                },
                "frequency": {
                    "type": "integer",
                    "description": "Coupon payment frequency (1=annual, 2=semi-annual, 4=quarterly)",
                    "default": 2,
                    "enum": [1, 2, 4],
                },
            },
            "required": ["clean_price", "face_value", "coupon_rate", "years_to_maturity"],
        },
        returns_description="Dict with YTM, duration, convexity, and interpretations",
    )
)


def get_sbp_rates() -> dict[str, Any]:
    """Get current SBP policy rates and key benchmarks.

    Returns:
        Dict with SBP policy rate, KIBOR rates, and other benchmarks
    """
    con = get_connection()

    # Get KIBOR curve (interbank rates)
    kibor_curve = get_sukuk_latest_yield_curve(con, "KIBOR")

    # Get T-Bill rates (short-term government)
    tbill_curve = get_sukuk_latest_yield_curve(con, "TBILL")

    # Get PIB rates (long-term government)
    pib_curve = get_sukuk_latest_yield_curve(con, "PIB")

    result = {
        "as_of": datetime.now().strftime("%Y-%m-%d"),
        "kibor": {},
        "tbill": {},
        "pib": {},
    }

    # Extract KIBOR rates by tenor
    if kibor_curve:
        for point in kibor_curve:
            tenor = point.get("tenor", "")
            if "3M" in tenor or tenor == "3 Month":
                result["kibor"]["3m"] = point.get("yield_pct")
            elif "6M" in tenor or tenor == "6 Month":
                result["kibor"]["6m"] = point.get("yield_pct")
            elif "12M" in tenor or tenor == "12 Month" or tenor == "1Y":
                result["kibor"]["12m"] = point.get("yield_pct")

    # Extract T-Bill rates
    if tbill_curve:
        for point in tbill_curve:
            tenor = point.get("tenor", "")
            if "3M" in tenor:
                result["tbill"]["3m"] = point.get("yield_pct")
            elif "6M" in tenor:
                result["tbill"]["6m"] = point.get("yield_pct")
            elif "12M" in tenor or "1Y" in tenor:
                result["tbill"]["12m"] = point.get("yield_pct")

    # Extract PIB rates
    if pib_curve:
        for point in pib_curve:
            tenor = point.get("tenor", "")
            tenor_years = point.get("tenor_years", 0)
            if tenor_years == 3 or "3Y" in tenor:
                result["pib"]["3y"] = point.get("yield_pct")
            elif tenor_years == 5 or "5Y" in tenor:
                result["pib"]["5y"] = point.get("yield_pct")
            elif tenor_years == 10 or "10Y" in tenor:
                result["pib"]["10y"] = point.get("yield_pct")

    # Add summary
    result["summary"] = {
        "short_term_benchmark": result["kibor"].get("3m") or result["tbill"].get("3m"),
        "medium_term_benchmark": result["pib"].get("3y"),
        "long_term_benchmark": result["pib"].get("10y"),
    }

    return result


ToolRegistry.register(
    Tool(
        name="get_sbp_rates",
        description="Get current SBP (State Bank of Pakistan) policy rates and key benchmark rates including KIBOR, T-Bill yields, and PIB yields at various tenors.",
        function=get_sbp_rates,
        category=ToolCategory.FIXED_INCOME,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with KIBOR, T-Bill, and PIB rates by tenor",
    )
)


# =============================================================================
# Fixed Income Data Summary
# =============================================================================


def get_fixed_income_summary() -> dict[str, Any]:
    """Get summary of all fixed income data in the database.

    Returns:
        Dict with counts and status for sukuk, bonds, and yield curves
    """
    con = get_connection()

    # Get sukuk summary
    try:
        sukuk_summary = get_sukuk_data_summary(con)
    except Exception:
        sukuk_summary = {"total": 0}

    # Get bond summary
    try:
        bond_summary = get_bond_data_summary(con)
    except Exception:
        bond_summary = {"total": 0}

    return {
        "sukuk": {
            "total_instruments": sukuk_summary.get("total_sukuk", 0),
            "active_instruments": sukuk_summary.get("active_sukuk", 0),
            "total_quotes": sukuk_summary.get("total_quotes", 0),
            "categories": sukuk_summary.get("categories", {}),
        },
        "bonds": {
            "total_instruments": bond_summary.get("total_bonds", 0),
            "active_instruments": bond_summary.get("active_bonds", 0),
            "total_quotes": bond_summary.get("total_quotes", 0),
            "types": bond_summary.get("types", {}),
        },
        "data_available": (
            sukuk_summary.get("total_sukuk", 0) > 0 or
            bond_summary.get("total_bonds", 0) > 0
        ),
    }


ToolRegistry.register(
    Tool(
        name="get_fixed_income_summary",
        description="Get summary of all fixed income data in the database including sukuk, bonds, and yield curve availability.",
        function=get_fixed_income_summary,
        category=ToolCategory.FIXED_INCOME,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with fixed income data summary",
    )
)
