"""Sync tools for agentic AI.

Wraps existing sync functions as callable tools for AI agents.
These tools enable data synchronization operations for market data,
FX rates, and mutual funds.
"""

from datetime import datetime
from typing import Any

from ..db import get_connection
from ..sync import sync_all, SyncSummary
from ..sync_fx import sync_fx_pairs, get_fx_data_summary, FXSyncSummary
from ..sync_mufap import sync_mutual_funds, get_data_summary as get_mf_data_summary
from .registry import Tool, ToolCategory, ToolRegistry


# =============================================================================
# Market Data Sync Tools
# =============================================================================


def sync_market_data(
    symbols: list[str] | None = None,
    refresh_symbols: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    """Sync EOD market data from PSX.

    Args:
        symbols: Specific symbols to sync (None = all active symbols)
        refresh_symbols: Whether to refresh the symbols list first
        limit: Limit number of symbols to sync (for testing)

    Returns:
        Dict with sync results including counts and any errors
    """
    try:
        result: SyncSummary = sync_all(
            refresh_symbols=refresh_symbols,
            limit_symbols=limit,
            symbols_list=symbols,
        )

        return {
            "status": "completed",
            "run_id": result.run_id,
            "symbols_total": result.symbols_total,
            "symbols_ok": result.symbols_ok,
            "symbols_failed": result.symbols_failed,
            "rows_upserted": result.rows_upserted,
            "indices_synced": result.indices_synced,
            "errors": [
                {"symbol": f["symbol"], "error": f["error_message"]}
                for f in result.failures[:5]
            ] if result.failures else [],
            "synced_at": datetime.now().isoformat(),
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
        }


ToolRegistry.register(
    Tool(
        name="sync_market_data",
        description="Synchronize EOD (End of Day) market data from Pakistan Stock Exchange. Use this when user asks to update data, refresh prices, or sync market information. Can sync all symbols or specific ones.",
        function=sync_market_data,
        category=ToolCategory.SYNC,
        parameters={
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of specific symbols to sync. If not provided, syncs all active symbols.",
                },
                "refresh_symbols": {
                    "type": "boolean",
                    "description": "Whether to refresh the symbols list from PSX first (default: false)",
                    "default": False,
                },
                "limit": {
                    "type": "integer",
                    "description": "Optional limit on number of symbols to sync",
                },
            },
        },
        requires_confirmation=True,
        returns_description="Dict with sync status, counts, and any errors",
    )
)


# =============================================================================
# FX Sync Tools
# =============================================================================


def sync_fx_rates(
    pairs: list[str] | None = None,
    incremental: bool = True,
) -> dict[str, Any]:
    """Sync FX rate data.

    Args:
        pairs: Specific pairs to sync (e.g., ["USD/PKR", "EUR/PKR"])
        incremental: Only fetch new data since last sync

    Returns:
        Dict with sync results
    """
    try:
        result: FXSyncSummary = sync_fx_pairs(
            pairs=pairs,
            incremental=incremental,
        )

        return {
            "status": "completed" if result.failed == 0 else "partial",
            "total_pairs": result.total,
            "pairs_ok": result.ok,
            "pairs_failed": result.failed,
            "no_data": result.no_data,
            "rows_upserted": result.rows_upserted,
            "errors": [
                {"pair": pair, "error": error}
                for pair, error in result.errors[:5]
            ] if result.errors else [],
            "synced_at": datetime.now().isoformat(),
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
        }


ToolRegistry.register(
    Tool(
        name="sync_fx_rates",
        description="Synchronize FX (foreign exchange) rate data for currency pairs like USD/PKR, EUR/PKR, etc. Use this when user asks to update FX rates or currency data.",
        function=sync_fx_rates,
        category=ToolCategory.SYNC,
        parameters={
            "type": "object",
            "properties": {
                "pairs": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of FX pairs to sync (e.g., ['USD/PKR', 'EUR/PKR']). If not provided, syncs all active pairs.",
                },
                "incremental": {
                    "type": "boolean",
                    "description": "Only fetch new data since last sync (default: true)",
                    "default": True,
                },
            },
        },
        requires_confirmation=True,
        returns_description="Dict with sync status and counts",
    )
)


def get_fx_status() -> dict[str, Any]:
    """Get FX data summary and status.

    Returns:
        Dict with FX pairs info, row counts, and latest dates
    """
    try:
        summary = get_fx_data_summary()
        return {
            "status": "ok",
            "total_pairs": summary.get("total_pairs", 0),
            "active_pairs": summary.get("active_pairs", 0),
            "pairs": summary.get("pairs", []),
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }


ToolRegistry.register(
    Tool(
        name="get_fx_status",
        description="Get status and summary of FX (foreign exchange) data in the database. Shows available pairs, row counts, and latest dates.",
        function=get_fx_status,
        category=ToolCategory.SYNC,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with FX data summary",
    )
)


# =============================================================================
# Mutual Fund Sync Tools
# =============================================================================


def sync_mutual_fund_nav(
    fund_ids: list[str] | None = None,
    category: str | None = None,
    incremental: bool = True,
) -> dict[str, Any]:
    """Sync mutual fund NAV data from MUFAP.

    Args:
        fund_ids: Specific fund IDs to sync
        category: Filter by category (e.g., "EQUITY", "INCOME", "MONEY_MARKET")
        incremental: Only fetch new data since last sync

    Returns:
        Dict with sync results
    """
    try:
        from ..sync_mufap import sync_mutual_funds, MufapSyncSummary

        result: MufapSyncSummary = sync_mutual_funds(
            fund_ids=fund_ids,
            category=category,
            incremental=incremental,
        )

        return {
            "status": "completed" if result.failed == 0 else "partial",
            "total_funds": result.total,
            "funds_ok": result.ok,
            "funds_failed": result.failed,
            "no_data": result.no_data,
            "rows_upserted": result.rows_upserted,
            "errors": [
                {"fund_id": fid, "error": error}
                for fid, error in result.errors[:5]
            ] if result.errors else [],
            "synced_at": datetime.now().isoformat(),
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
        }


ToolRegistry.register(
    Tool(
        name="sync_mutual_fund_nav",
        description="Synchronize mutual fund NAV (Net Asset Value) data from MUFAP. Use this when user asks to update mutual fund prices or NAV data.",
        function=sync_mutual_fund_nav,
        category=ToolCategory.SYNC,
        parameters={
            "type": "object",
            "properties": {
                "fund_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of fund IDs to sync. If not provided, syncs all active funds.",
                },
                "category": {
                    "type": "string",
                    "description": "Filter by category (e.g., 'EQUITY', 'INCOME', 'MONEY_MARKET')",
                    "enum": ["EQUITY", "INCOME", "MONEY_MARKET", "BALANCED", "ASSET_ALLOCATION", "CAPITAL_PROTECTED", "FUND_OF_FUNDS", "SHARIAH_EQUITY", "SHARIAH_INCOME", "VPS"],
                },
                "incremental": {
                    "type": "boolean",
                    "description": "Only fetch new data since last sync (default: true)",
                    "default": True,
                },
            },
        },
        requires_confirmation=True,
        returns_description="Dict with sync status and counts",
    )
)


def get_mutual_fund_status() -> dict[str, Any]:
    """Get mutual fund data summary and status.

    Returns:
        Dict with fund info, counts by category, and latest dates
    """
    try:
        summary = get_mf_data_summary()
        return {
            "status": "ok",
            "total_funds": summary.get("total_funds", 0),
            "active_funds": summary.get("active_funds", 0),
            "categories": summary.get("categories", {}),
            "nav_date_range": {
                "min": summary.get("min_nav_date"),
                "max": summary.get("max_nav_date"),
            },
            "total_nav_records": summary.get("total_nav_records", 0),
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }


ToolRegistry.register(
    Tool(
        name="get_mutual_fund_status",
        description="Get status and summary of mutual fund data in the database. Shows fund counts by category, date ranges, and total records.",
        function=get_mutual_fund_status,
        category=ToolCategory.SYNC,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with mutual fund data summary",
    )
)


# =============================================================================
# Sync Status Tools
# =============================================================================


def get_sync_status() -> dict[str, Any]:
    """Get overall data sync status across all sources.

    Returns:
        Dict with sync status for market data, FX, and mutual funds
    """
    from ..db import get_connection
    from ..query import get_ohlcv_stats

    con = get_connection()

    # Get OHLCV stats
    try:
        ohlcv_stats = get_ohlcv_stats(con)
    except Exception:
        ohlcv_stats = {}

    # Get FX summary
    try:
        fx_summary = get_fx_data_summary()
    except Exception:
        fx_summary = {"total_pairs": 0, "active_pairs": 0}

    # Get MF summary
    try:
        mf_summary = get_mf_data_summary()
    except Exception:
        mf_summary = {"total_funds": 0, "active_funds": 0}

    # Determine staleness for OHLCV
    is_stale = False
    days_old = None
    latest_date = ohlcv_stats.get("max_date")

    if latest_date:
        latest = datetime.strptime(latest_date, "%Y-%m-%d")
        today = datetime.now()
        days_old = (today - latest).days

        weekday = today.weekday()
        if weekday == 0:  # Monday
            is_stale = days_old > 3
        elif weekday == 6:  # Sunday
            is_stale = days_old > 2
        else:
            is_stale = days_old > 1

    return {
        "market_data": {
            "latest_date": ohlcv_stats.get("max_date"),
            "total_rows": ohlcv_stats.get("total_rows", 0),
            "symbols_count": ohlcv_stats.get("unique_symbols", 0),
            "is_stale": is_stale,
            "days_old": days_old,
        },
        "fx_data": {
            "total_pairs": fx_summary.get("total_pairs", 0),
            "active_pairs": fx_summary.get("active_pairs", 0),
        },
        "mutual_funds": {
            "total_funds": mf_summary.get("total_funds", 0),
            "active_funds": mf_summary.get("active_funds", 0),
        },
        "recommendation": (
            "All data sources are current"
            if not is_stale
            else f"Market data is {days_old} days old. Consider running sync_market_data."
        ),
        "checked_at": datetime.now().isoformat(),
    }


ToolRegistry.register(
    Tool(
        name="get_sync_status",
        description="Get overall data synchronization status across all sources (market data, FX, mutual funds). Use this to check data freshness and determine if syncs are needed.",
        function=get_sync_status,
        category=ToolCategory.SYNC,
        parameters={"type": "object", "properties": {}},
        returns_description="Dict with sync status for all data sources",
    )
)
