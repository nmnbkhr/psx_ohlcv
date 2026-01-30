"""Market data tools for agentic AI.

Wraps existing market data functions as callable tools for AI agents.
These tools provide access to stock prices, market analytics, and sector data.
"""

from datetime import datetime, timedelta
from typing import Any

from ..db import get_connection
from ..query import (
    get_latest_close,
    get_ohlcv_range,
    get_ohlcv_stats,
    get_ohlcv_symbol_stats,
    get_symbols_list,
    get_company_profile,
    get_company_latest_quote,
)
from ..analytics import (
    get_latest_market_analytics,
    get_top_list,
    get_sector_leaderboard,
    get_current_market_with_sectors,
)
from .registry import Tool, ToolCategory, ToolRegistry


# =============================================================================
# Stock Price Tools
# =============================================================================


def get_stock_price(symbol: str, days: int = 30) -> dict[str, Any]:
    """Fetch historical OHLCV data for a PSX symbol.

    Args:
        symbol: PSX stock symbol (e.g., "HBL", "OGDC")
        days: Number of days of history (default: 30)

    Returns:
        Dict with price data including latest price, period stats, and history
    """
    con = get_connection()

    # Get date range
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Fetch OHLCV data
    df = get_ohlcv_range(con, symbol.upper(), start_date=start_date, end_date=end_date)

    if df.empty:
        return {
            "symbol": symbol.upper(),
            "error": f"No data found for {symbol}",
            "data_points": 0,
        }

    # Calculate stats
    latest = df.iloc[-1]
    first = df.iloc[0]
    period_return = (
        ((latest["close"] / first["close"]) - 1) * 100
        if first["close"] and first["close"] > 0
        else None
    )

    # Get company profile for name
    profile = get_company_profile(con, symbol.upper())
    company_name = profile.get("company_name") if profile else None

    return {
        "symbol": symbol.upper(),
        "company_name": company_name,
        "latest_date": str(latest["date"]),
        "latest_close": float(latest["close"]) if latest["close"] else None,
        "latest_open": float(latest["open"]) if latest["open"] else None,
        "latest_high": float(latest["high"]) if latest["high"] else None,
        "latest_low": float(latest["low"]) if latest["low"] else None,
        "latest_volume": int(latest["volume"]) if latest["volume"] else None,
        "period_high": float(df["high"].max()) if not df["high"].isna().all() else None,
        "period_low": float(df["low"].min()) if not df["low"].isna().all() else None,
        "period_return_pct": round(period_return, 2) if period_return else None,
        "avg_volume": int(df["volume"].mean()) if not df["volume"].isna().all() else None,
        "data_points": len(df),
        "start_date": str(df.iloc[0]["date"]),
        "history": df.tail(5).to_dict("records"),  # Last 5 days
    }


ToolRegistry.register(
    Tool(
        name="get_stock_price",
        description="Fetch historical OHLCV (Open, High, Low, Close, Volume) data for a PSX stock symbol. Use this to get price history, recent performance, and trading data for any stock listed on Pakistan Stock Exchange.",
        function=get_stock_price,
        category=ToolCategory.MARKET_DATA,
        parameters={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., HBL, OGDC, ENGRO, MCB, UBL)",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days of history to fetch (default: 30)",
                    "default": 30,
                },
            },
            "required": ["symbol"],
        },
        returns_description="Dict with latest price, period high/low, return percentage, and recent history",
    )
)


# =============================================================================
# Market Overview Tools
# =============================================================================


def get_market_overview() -> dict[str, Any]:
    """Get current market overview including breadth and top movers.

    Returns:
        Dict with market breadth, top gainers, losers, and volume leaders
    """
    con = get_connection()

    # Get market analytics
    analytics = get_latest_market_analytics(con)

    # Get top lists
    gainers = get_top_list(con, "gainers", limit=5)
    losers = get_top_list(con, "losers", limit=5)
    volume = get_top_list(con, "volume", limit=5)

    result = {
        "as_of": analytics.get("ts") if analytics else None,
        "market_breadth": {
            "gainers_count": analytics.get("gainers_count", 0) if analytics else 0,
            "losers_count": analytics.get("losers_count", 0) if analytics else 0,
            "unchanged_count": analytics.get("unchanged_count", 0) if analytics else 0,
            "total_symbols": analytics.get("total_symbols", 0) if analytics else 0,
            "total_volume": analytics.get("total_volume", 0) if analytics else 0,
        },
        "top_gainer": analytics.get("top_gainer_symbol") if analytics else None,
        "top_loser": analytics.get("top_loser_symbol") if analytics else None,
        "top_gainers": gainers.to_dict("records") if not gainers.empty else [],
        "top_losers": losers.to_dict("records") if not losers.empty else [],
        "top_volume": volume.to_dict("records") if not volume.empty else [],
    }

    return result


ToolRegistry.register(
    Tool(
        name="get_market_overview",
        description="Get current PSX market overview including market breadth (gainers vs losers count), top gaining stocks, top losing stocks, and highest volume stocks. Use this for a quick market summary.",
        function=get_market_overview,
        category=ToolCategory.MARKET_DATA,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with market breadth counts, top gainers, losers, and volume leaders",
    )
)


# =============================================================================
# Sector Tools
# =============================================================================


def get_sector_performance(sector_name: str | None = None) -> dict[str, Any]:
    """Get sector performance data.

    Args:
        sector_name: Optional sector name to filter (partial match).
                    If None, returns all sectors.

    Returns:
        Dict with sector performance data
    """
    con = get_connection()

    # Get sector leaderboard
    df = get_sector_leaderboard(con, sort_by="avg_change_pct", ascending=False)

    if df.empty:
        return {"sectors": [], "count": 0, "error": "No sector data available"}

    # Filter by sector name if provided
    if sector_name:
        sector_lower = sector_name.lower()
        df = df[df["sector_name"].str.lower().str.contains(sector_lower, na=False)]

    sectors = df.to_dict("records")

    return {
        "as_of": df.iloc[0]["ts"] if not df.empty else None,
        "sectors": sectors,
        "count": len(sectors),
        "top_sector": sectors[0]["sector_name"] if sectors else None,
        "worst_sector": sectors[-1]["sector_name"] if sectors else None,
    }


ToolRegistry.register(
    Tool(
        name="get_sector_performance",
        description="Get sector-wise performance data for PSX. Shows average change percentage, total volume, and top symbol for each sector. Can filter by sector name (e.g., 'banking', 'cement', 'oil').",
        function=get_sector_performance,
        category=ToolCategory.MARKET_DATA,
        parameters={
            "type": "object",
            "properties": {
                "sector_name": {
                    "type": "string",
                    "description": "Optional sector name to filter (e.g., 'banking', 'cement', 'oil & gas'). Partial match supported.",
                },
            },
        },
        returns_description="Dict with sector performance data including avg change, volume, and top performers",
    )
)


# =============================================================================
# Symbol Search Tools
# =============================================================================


def search_symbols(query: str) -> dict[str, Any]:
    """Search for symbols by name or symbol code.

    Args:
        query: Search query (matches symbol code or company name)

    Returns:
        Dict with matching symbols
    """
    con = get_connection()

    # Get all symbols
    symbols = get_symbols_list(con, is_active_only=True)

    # Search in current market data which has names
    df = get_current_market_with_sectors(con)

    if df.empty:
        return {"matches": [], "count": 0, "query": query}

    # Filter by query
    query_lower = query.lower()
    matches = df[
        df["symbol"].str.lower().str.contains(query_lower, na=False)
        | df["company_name"].str.lower().str.contains(query_lower, na=False)
    ]

    # Select relevant columns
    result_df = matches[
        ["symbol", "company_name", "sector_name", "current", "change_pct", "volume"]
    ].head(20)

    return {
        "query": query,
        "matches": result_df.to_dict("records"),
        "count": len(result_df),
    }


ToolRegistry.register(
    Tool(
        name="search_symbols",
        description="Search for PSX stock symbols by symbol code or company name. Use this when user mentions a partial name or wants to find a specific stock.",
        function=search_symbols,
        category=ToolCategory.MARKET_DATA,
        parameters={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query - can be partial symbol code or company name",
                },
            },
            "required": ["query"],
        },
        returns_description="Dict with matching symbols including current price and change",
    )
)


# =============================================================================
# Data Freshness Tools
# =============================================================================


def check_data_freshness() -> dict[str, Any]:
    """Check when market data was last updated.

    Returns:
        Dict with data freshness information
    """
    con = get_connection()

    # Get OHLCV stats
    stats = get_ohlcv_stats(con)

    # Get latest analytics timestamp
    analytics = get_latest_market_analytics(con)

    # Determine if data is stale
    latest_date = stats.get("max_date")
    is_stale = False
    days_old = None

    if latest_date:
        latest = datetime.strptime(latest_date, "%Y-%m-%d")
        today = datetime.now()
        days_old = (today - latest).days

        # Data is stale if more than 1 day old (accounting for weekends)
        # On Monday, data from Friday (3 days old) is not stale
        weekday = today.weekday()
        if weekday == 0:  # Monday
            is_stale = days_old > 3
        elif weekday == 6:  # Sunday
            is_stale = days_old > 2
        else:
            is_stale = days_old > 1

    return {
        "eod_data": {
            "latest_date": stats.get("max_date"),
            "oldest_date": stats.get("min_date"),
            "total_rows": stats.get("total_rows", 0),
            "symbols_count": stats.get("unique_symbols", 0),
        },
        "analytics": {
            "latest_snapshot": analytics.get("ts") if analytics else None,
            "computed_at": analytics.get("computed_at") if analytics else None,
        },
        "is_stale": is_stale,
        "days_old": days_old,
        "recommendation": (
            "Data is current"
            if not is_stale
            else f"Data is {days_old} days old. Consider running a sync."
        ),
    }


ToolRegistry.register(
    Tool(
        name="check_data_freshness",
        description="Check when market data was last updated and whether it's stale. Use this to determine if a data sync is needed.",
        function=check_data_freshness,
        category=ToolCategory.MARKET_DATA,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with data freshness info and recommendation",
    )
)


# =============================================================================
# Company Profile Tools
# =============================================================================


def get_company_info(symbol: str) -> dict[str, Any]:
    """Get comprehensive company information.

    Args:
        symbol: PSX stock symbol

    Returns:
        Dict with company profile and latest quote
    """
    con = get_connection()
    symbol = symbol.upper()

    # Get profile
    profile = get_company_profile(con, symbol)

    # Get latest quote
    quote = get_company_latest_quote(con, symbol)

    # Get price data
    price_data = get_stock_price(symbol, days=30)

    if not profile and not quote:
        return {
            "symbol": symbol,
            "error": f"No company data found for {symbol}",
        }

    result = {
        "symbol": symbol,
        "company_name": profile.get("company_name") if profile else None,
        "sector": profile.get("sector_name") if profile else None,
        "business_description": profile.get("business_description") if profile else None,
        "website": profile.get("website") if profile else None,
        "fiscal_year_end": profile.get("fiscal_year_end") if profile else None,
    }

    if quote:
        result["quote"] = {
            "price": quote.get("price"),
            "change": quote.get("change"),
            "change_pct": quote.get("change_pct"),
            "volume": quote.get("volume"),
            "day_range": f"{quote.get('day_range_low')} - {quote.get('day_range_high')}",
            "52_week_range": f"{quote.get('wk52_low')} - {quote.get('wk52_high')}",
            "as_of": quote.get("as_of"),
        }

    if price_data and "error" not in price_data:
        result["performance"] = {
            "30d_return_pct": price_data.get("period_return_pct"),
            "30d_high": price_data.get("period_high"),
            "30d_low": price_data.get("period_low"),
            "avg_volume": price_data.get("avg_volume"),
        }

    return result


ToolRegistry.register(
    Tool(
        name="get_company_info",
        description="Get comprehensive company information including profile, sector, latest quote, and 30-day performance. Use this for detailed company analysis.",
        function=get_company_info,
        category=ToolCategory.COMPANY,
        parameters={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., HBL, OGDC)",
                },
            },
            "required": ["symbol"],
        },
        returns_description="Dict with company profile, quote, and performance data",
    )
)


# =============================================================================
# Compare Stocks Tool
# =============================================================================


def compare_stocks(symbols: list[str]) -> dict[str, Any]:
    """Compare multiple stocks side by side.

    Args:
        symbols: List of stock symbols to compare (2-5 symbols)

    Returns:
        Dict with comparison data
    """
    if len(symbols) < 2:
        return {"error": "Need at least 2 symbols to compare"}
    if len(symbols) > 5:
        symbols = symbols[:5]  # Limit to 5

    comparisons = []
    for symbol in symbols:
        data = get_stock_price(symbol, days=30)
        if "error" not in data:
            comparisons.append({
                "symbol": data["symbol"],
                "company_name": data.get("company_name"),
                "latest_close": data.get("latest_close"),
                "30d_return_pct": data.get("period_return_pct"),
                "30d_high": data.get("period_high"),
                "30d_low": data.get("period_low"),
                "avg_volume": data.get("avg_volume"),
            })
        else:
            comparisons.append({
                "symbol": symbol.upper(),
                "error": data.get("error"),
            })

    # Sort by 30d return
    valid_comparisons = [c for c in comparisons if "error" not in c]
    valid_comparisons.sort(
        key=lambda x: x.get("30d_return_pct") or -999, reverse=True
    )

    return {
        "symbols_compared": len(comparisons),
        "comparison": comparisons,
        "best_performer": valid_comparisons[0]["symbol"] if valid_comparisons else None,
        "worst_performer": valid_comparisons[-1]["symbol"] if valid_comparisons else None,
    }


ToolRegistry.register(
    Tool(
        name="compare_stocks",
        description="Compare multiple PSX stocks side by side on price, 30-day return, and volume. Use this when user wants to compare different stocks.",
        function=compare_stocks,
        category=ToolCategory.ANALYTICS,
        parameters={
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of stock symbols to compare (2-5 symbols)",
                    "minItems": 2,
                    "maxItems": 5,
                },
            },
            "required": ["symbols"],
        },
        returns_description="Dict with side-by-side comparison of stocks",
    )
)
