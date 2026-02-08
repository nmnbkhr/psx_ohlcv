"""PSX OHLCV MCP Server.

Exposes Pakistan Stock Exchange data as AI-callable tools via the
Model Context Protocol (MCP). Uses stdio transport for Claude Code integration.
"""

import json
import os
import sqlite3

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

server = Server("psx-ohlcv")

DB_PATH = os.environ.get("PSX_DB_PATH", "/mnt/e/psxdata/psx.sqlite")


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _json_response(data) -> list[types.TextContent]:
    """Wrap data as a JSON TextContent list."""
    return [types.TextContent(type="text", text=json.dumps(data, indent=2, default=str))]


# ─── TOOL DEFINITIONS ──────────────────────────────────────────────

EQUITY_TOOLS = [
    types.Tool(
        name="get_eod",
        description=(
            "Get EOD OHLCV price data for a PSX stock symbol. "
            "Returns date, open, high, low, close, volume."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "PSX stock symbol (e.g., OGDC, HBL, MCB)",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date YYYY-MM-DD (optional)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date YYYY-MM-DD (optional)",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows (default 100)",
                    "default": 100,
                },
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="search_symbols",
        description=(
            "Search PSX symbols by name or code. "
            "Returns matching symbols with sector and status."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search term (symbol code or company name)",
                },
                "sector": {
                    "type": "string",
                    "description": "Filter by sector (optional)",
                },
                "active_only": {"type": "boolean", "default": True},
            },
            "required": ["query"],
        },
    ),
    types.Tool(
        name="get_company_profile",
        description=(
            "Get company profile including sector, market cap, P/E, EPS, "
            "dividend yield, 52-week range, and more."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "PSX stock symbol"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="get_market_snapshot",
        description=(
            "Get current market snapshot: all PSX indices with latest value, "
            "change, volume, and 52-week range."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="get_top_movers",
        description="Get top gainers and losers by price change percentage.",
        inputSchema={
            "type": "object",
            "properties": {
                "n": {
                    "type": "integer",
                    "default": 10,
                    "description": "Number of stocks per list",
                },
                "direction": {
                    "type": "string",
                    "enum": ["gainers", "losers", "both"],
                    "default": "both",
                },
            },
        },
    ),
]


# ─── FIXED INCOME TOOL DEFINITIONS ─────────────────────────────────

FIXED_INCOME_TOOLS = [
    types.Tool(
        name="get_sukuk",
        description="List sukuk (Islamic bonds) with master data and latest quotes.",
        inputSchema={
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Filter by category (e.g., GOP_SUKUK)",
                },
            },
        },
    ),
    types.Tool(
        name="get_yield_curve",
        description=(
            "Get yield curve data (PKRV or yield_curve_points). "
            "Returns tenor and yield for a given date."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "curve_type": {
                    "type": "string",
                    "enum": ["pkrv", "pib", "all"],
                    "default": "pkrv",
                    "description": "Curve type: pkrv, pib, or all",
                },
                "date": {
                    "type": "string",
                    "description": "Date YYYY-MM-DD (default: latest available)",
                },
            },
        },
    ),
    types.Tool(
        name="get_tbill_auctions",
        description="Get T-Bill auction results with yields, amounts, and tenors.",
        inputSchema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
                "tenor": {"type": "string", "description": "Filter by tenor (e.g., 3M, 6M, 12M)"},
            },
        },
    ),
    types.Tool(
        name="get_pib_auctions",
        description="Get PIB (Pakistan Investment Bond) auction results.",
        inputSchema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
            },
        },
    ),
    types.Tool(
        name="get_gis_auctions",
        description="Get GIS (Government Ijarah Sukuk) auction results.",
        inputSchema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
            },
        },
    ),
    types.Tool(
        name="get_latest_yields",
        description=(
            "Get latest yields across all fixed income instruments: "
            "PKRV curve, T-Bill, PIB, KIBOR, KONIA, and SBP policy rate."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
]


# ─── TOOL REGISTRATION ─────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return EQUITY_TOOLS + FIXED_INCOME_TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    con = get_db()
    try:
        handler = _HANDLERS.get(name)
        if handler:
            return handler(con, arguments)
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
    finally:
        con.close()


# ─── EQUITY HANDLERS ───────────────────────────────────────────────

def _handle_get_eod(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    symbol = args["symbol"].upper()
    limit = args.get("limit", 100)
    query = "SELECT date, open, high, low, close, volume FROM eod_ohlcv WHERE symbol = ?"
    params: list = [symbol]
    if args.get("start_date"):
        query += " AND date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        query += " AND date <= ?"
        params.append(args["end_date"])
    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in con.execute(query, params).fetchall()]
    return _json_response({"symbol": symbol, "count": len(rows), "data": rows})


def _handle_search_symbols(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    query_text = args["query"].upper()
    active_only = args.get("active_only", True)

    sql = """SELECT symbol, name, sector, sector_name, is_active
             FROM symbols
             WHERE (UPPER(symbol) LIKE ? OR UPPER(name) LIKE ?)"""
    params: list = [f"%{query_text}%", f"%{query_text}%"]

    if args.get("sector"):
        sql += " AND (UPPER(sector) LIKE ? OR UPPER(sector_name) LIKE ?)"
        params.extend([f"%{args['sector'].upper()}%"] * 2)
    if active_only:
        sql += " AND is_active = 1"

    sql += " ORDER BY symbol LIMIT 50"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"query": args["query"], "count": len(rows), "results": rows})


def _handle_get_company_profile(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    symbol = args["symbol"].upper()

    # Get fundamentals (latest snapshot with price, market cap, PE, etc.)
    fund = con.execute(
        """SELECT symbol, company_name, sector_name, price, change, change_pct,
                  open, high, low, volume, ldcp, market_cap, pe_ratio,
                  total_shares, free_float_shares, free_float_pct,
                  wk52_low, wk52_high, ytd_change_pct, one_year_change_pct,
                  business_description, website, fiscal_year_end
           FROM company_fundamentals WHERE symbol = ?""",
        (symbol,),
    ).fetchone()

    if fund:
        result = dict(fund)
    else:
        # Fallback: build profile from symbols + latest EOD
        sym = con.execute(
            "SELECT symbol, name, sector, sector_name FROM symbols WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if not sym:
            return _json_response({"error": f"Symbol {symbol} not found"})
        result = dict(sym)

        # Add latest EOD price data
        eod = con.execute(
            """SELECT date, open, high, low, close, volume
               FROM eod_ohlcv WHERE symbol = ? ORDER BY date DESC LIMIT 1""",
            (symbol,),
        ).fetchone()
        if eod:
            result.update(dict(eod))

        # Add company_profile data if available
        prof = con.execute(
            """SELECT company_name, business_description, website, fiscal_year_end
               FROM company_profile WHERE symbol = ?""",
            (symbol,),
        ).fetchone()
        if prof:
            result.update({k: v for k, v in dict(prof).items() if v is not None})

    # Append recent payouts
    payouts = con.execute(
        """SELECT ex_date, payout_type, amount, fiscal_year
           FROM company_payouts WHERE symbol = ?
           ORDER BY ex_date DESC LIMIT 5""",
        (symbol,),
    ).fetchall()
    result["recent_payouts"] = [dict(p) for p in payouts]

    return _json_response(result)


def _handle_get_market_snapshot(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    # Latest indices
    indices = con.execute(
        """SELECT i.*
           FROM psx_indices i
           INNER JOIN (
               SELECT index_code, MAX(index_date) as max_date
               FROM psx_indices GROUP BY index_code
           ) latest ON i.index_code = latest.index_code AND i.index_date = latest.max_date
           ORDER BY i.index_code"""
    ).fetchall()

    # Latest EOD date summary
    latest_date_row = con.execute(
        "SELECT MAX(date) as latest_date, COUNT(DISTINCT symbol) as symbols FROM eod_ohlcv"
    ).fetchone()

    # Trading stats for latest date
    latest_date = latest_date_row["latest_date"] if latest_date_row else None
    stats = {}
    if latest_date:
        row = con.execute(
            """SELECT COUNT(*) as traded, SUM(volume) as total_volume
               FROM eod_ohlcv WHERE date = ?""",
            (latest_date,),
        ).fetchone()
        stats = dict(row) if row else {}

    return _json_response({
        "indices": [dict(i) for i in indices],
        "latest_eod_date": latest_date,
        "total_symbols": latest_date_row["symbols"] if latest_date_row else 0,
        "trading_stats": stats,
    })


def _handle_get_top_movers(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    n = args.get("n", 10)
    direction = args.get("direction", "both")

    # Find the latest two trading dates
    dates = con.execute(
        "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date DESC LIMIT 2"
    ).fetchall()
    if len(dates) < 2:
        return _json_response({"error": "Need at least 2 trading days of data"})

    latest_date = dates[0]["date"]
    prev_date = dates[1]["date"]
    result: dict = {"date": latest_date, "prev_date": prev_date}

    # Self-join today vs previous day to compute change
    base_sql = """
        SELECT t.symbol, t.close, p.close as prev_close, t.volume,
               ROUND((t.close - p.close) / p.close * 100, 2) as change_pct
        FROM eod_ohlcv t
        INNER JOIN eod_ohlcv p ON t.symbol = p.symbol AND p.date = ?
        WHERE t.date = ? AND p.close > 0 AND t.close > 0
    """

    if direction in ("gainers", "both"):
        rows = con.execute(
            base_sql + " ORDER BY change_pct DESC LIMIT ?",
            (prev_date, latest_date, n),
        ).fetchall()
        result["gainers"] = [dict(r) for r in rows]

    if direction in ("losers", "both"):
        rows = con.execute(
            base_sql + " ORDER BY change_pct ASC LIMIT ?",
            (prev_date, latest_date, n),
        ).fetchall()
        result["losers"] = [dict(r) for r in rows]

    return _json_response(result)


# ─── FIXED INCOME HANDLERS ─────────────────────────────────────────

def _handle_get_sukuk(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    sql = """SELECT m.*, q.quote_date, q.clean_price, q.dirty_price,
                    q.yield_to_maturity, q.bid_yield, q.ask_yield
             FROM sukuk_master m
             LEFT JOIN sukuk_quotes q ON m.instrument_id = q.instrument_id
               AND q.quote_date = (
                   SELECT MAX(quote_date) FROM sukuk_quotes WHERE instrument_id = m.instrument_id
               )
             WHERE 1=1"""
    params: list = []
    if args.get("category"):
        sql += " AND UPPER(m.category) LIKE ?"
        params.append(f"%{args['category'].upper()}%")
    sql += " ORDER BY m.name"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


def _handle_get_yield_curve(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    curve_type = args.get("curve_type", "pkrv")
    date = args.get("date")

    if curve_type == "pkrv":
        if not date:
            row = con.execute("SELECT MAX(date) as d FROM pkrv_daily").fetchone()
            date = row["d"] if row else None
        if not date:
            return _json_response({"error": "No PKRV data available"})
        rows = con.execute(
            "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
            (date,),
        ).fetchall()
        return _json_response({"curve_type": "pkrv", "date": date, "points": [dict(r) for r in rows]})
    else:
        # yield_curve_points table
        bond_filter = "PIB" if curve_type == "pib" else "%"
        if not date:
            row = con.execute(
                "SELECT MAX(curve_date) as d FROM yield_curve_points WHERE bond_type LIKE ?",
                (bond_filter,),
            ).fetchone()
            date = row["d"] if row else None
        if not date:
            return _json_response({"error": f"No yield curve data for {curve_type}"})
        rows = con.execute(
            """SELECT tenor_months, yield_rate, bond_type
               FROM yield_curve_points
               WHERE curve_date = ? AND bond_type LIKE ?
               ORDER BY tenor_months""",
            (date, bond_filter),
        ).fetchall()
        return _json_response({
            "curve_type": curve_type, "date": date, "points": [dict(r) for r in rows],
        })


def _handle_get_tbill_auctions(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    sql = "SELECT * FROM tbill_auctions WHERE 1=1"
    params: list = []
    if args.get("start_date"):
        sql += " AND auction_date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        sql += " AND auction_date <= ?"
        params.append(args["end_date"])
    if args.get("tenor"):
        sql += " AND UPPER(tenor) = ?"
        params.append(args["tenor"].upper())
    sql += " ORDER BY auction_date DESC LIMIT 50"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


def _handle_get_pib_auctions(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    sql = "SELECT * FROM pib_auctions WHERE 1=1"
    params: list = []
    if args.get("start_date"):
        sql += " AND auction_date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        sql += " AND auction_date <= ?"
        params.append(args["end_date"])
    sql += " ORDER BY auction_date DESC LIMIT 50"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


def _handle_get_gis_auctions(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    sql = "SELECT * FROM gis_auctions WHERE 1=1"
    params: list = []
    if args.get("start_date"):
        sql += " AND auction_date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        sql += " AND auction_date <= ?"
        params.append(args["end_date"])
    sql += " ORDER BY auction_date DESC LIMIT 50"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


def _handle_get_latest_yields(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    result: dict = {}

    # SBP Policy Rate
    row = con.execute(
        "SELECT * FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    result["policy_rate"] = dict(row) if row else None

    # KIBOR (all tenors, latest date)
    kibor_date = con.execute("SELECT MAX(date) as d FROM kibor_daily").fetchone()
    if kibor_date and kibor_date["d"]:
        rows = con.execute(
            "SELECT tenor, bid, offer FROM kibor_daily WHERE date = ? ORDER BY tenor",
            (kibor_date["d"],),
        ).fetchall()
        result["kibor"] = {"date": kibor_date["d"], "rates": [dict(r) for r in rows]}

    # KONIA
    row = con.execute(
        "SELECT * FROM konia_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    result["konia"] = dict(row) if row else None

    # PKRV curve (latest)
    pkrv_date = con.execute("SELECT MAX(date) as d FROM pkrv_daily").fetchone()
    if pkrv_date and pkrv_date["d"]:
        rows = con.execute(
            "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
            (pkrv_date["d"],),
        ).fetchall()
        result["pkrv"] = {"date": pkrv_date["d"], "curve": [dict(r) for r in rows]}

    # Latest T-Bill auction yield
    row = con.execute(
        "SELECT * FROM tbill_auctions ORDER BY auction_date DESC LIMIT 1"
    ).fetchone()
    result["latest_tbill_auction"] = dict(row) if row else None

    # Latest PIB auction
    row = con.execute(
        "SELECT * FROM pib_auctions ORDER BY auction_date DESC LIMIT 1"
    ).fetchone()
    result["latest_pib_auction"] = dict(row) if row else None

    return _json_response(result)


# ─── HANDLER DISPATCH ──────────────────────────────────────────────

_HANDLERS = {
    # Equity
    "get_eod": _handle_get_eod,
    "search_symbols": _handle_search_symbols,
    "get_company_profile": _handle_get_company_profile,
    "get_market_snapshot": _handle_get_market_snapshot,
    "get_top_movers": _handle_get_top_movers,
    # Fixed income
    "get_sukuk": _handle_get_sukuk,
    "get_yield_curve": _handle_get_yield_curve,
    "get_tbill_auctions": _handle_get_tbill_auctions,
    "get_pib_auctions": _handle_get_pib_auctions,
    "get_gis_auctions": _handle_get_gis_auctions,
    "get_latest_yields": _handle_get_latest_yields,
}


# ─── ENTRY POINT ───────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
