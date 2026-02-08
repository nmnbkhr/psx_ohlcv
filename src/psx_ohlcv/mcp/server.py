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


# ─── FUND + FX + RATES TOOL DEFINITIONS ────────────────────────────

FUND_FX_TOOLS = [
    types.Tool(
        name="get_mutual_funds",
        description="List mutual funds with optional filters. Returns fund details and latest NAV.",
        inputSchema={
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Filter by category (Equity, Money Market, Income, etc.)"},
                "shariah": {"type": "boolean", "description": "Filter Shariah-compliant only"},
                "amc": {"type": "string", "description": "Filter by AMC name"},
            },
        },
    ),
    types.Tool(
        name="get_fund_nav_history",
        description="Get NAV time series for a mutual fund.",
        inputSchema={
            "type": "object",
            "properties": {
                "fund_id": {"type": "string", "description": "Fund ID (e.g., MUFAP:ABL-ISF)"},
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD"},
                "limit": {"type": "integer", "default": 100},
            },
            "required": ["fund_id"],
        },
    ),
    types.Tool(
        name="get_fund_rankings",
        description="Top performing mutual funds over a period.",
        inputSchema={
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 365, "description": "Period in days"},
                "category": {"type": "string", "description": "Filter by category"},
                "n": {"type": "integer", "default": 20, "description": "Number of results"},
            },
        },
    ),
    types.Tool(
        name="get_etf_data",
        description="Get ETF detail including NAV, market price, and premium/discount.",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "ETF symbol"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="get_etf_list",
        description="Get all listed ETFs with latest data.",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="get_fx_rates",
        description="Get latest FX rates from interbank, open market, or kerb sources.",
        inputSchema={
            "type": "object",
            "properties": {
                "currency": {"type": "string", "description": "Currency code (e.g., USD, EUR, GBP)"},
                "source": {
                    "type": "string",
                    "enum": ["interbank", "open_market", "kerb", "all"],
                    "default": "all",
                    "description": "FX rate source",
                },
            },
        },
    ),
    types.Tool(
        name="get_fx_history",
        description="Get historical FX rates for a currency pair.",
        inputSchema={
            "type": "object",
            "properties": {
                "currency": {"type": "string", "description": "Currency code"},
                "source": {
                    "type": "string",
                    "enum": ["interbank", "open_market", "kerb"],
                    "default": "interbank",
                },
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
            },
            "required": ["currency"],
        },
    ),
    types.Tool(
        name="get_fx_spread",
        description="Compare FX rates across all sources for a currency.",
        inputSchema={
            "type": "object",
            "properties": {
                "currency": {"type": "string", "description": "Currency code (default USD)", "default": "USD"},
            },
        },
    ),
    types.Tool(
        name="get_kibor",
        description="Get KIBOR rates for all tenors on a given date.",
        inputSchema={
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "Date YYYY-MM-DD (default: latest)"},
            },
        },
    ),
    types.Tool(
        name="get_policy_rate",
        description="Get current SBP policy rate and rate history.",
        inputSchema={
            "type": "object",
            "properties": {
                "history": {"type": "boolean", "default": False, "description": "Include rate change history"},
            },
        },
    ),
    types.Tool(
        name="get_konia",
        description="Get KONIA (Karachi Overnight Index Average) rate history.",
        inputSchema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "limit": {"type": "integer", "default": 30},
            },
        },
    ),
]


# ─── ANALYTICS + SYSTEM TOOL DEFINITIONS ───────────────────────────

ANALYTICS_SYSTEM_TOOLS = [
    types.Tool(
        name="screen_stocks",
        description="Stock screener with multiple filters: market cap, P/E, dividend yield, sector.",
        inputSchema={
            "type": "object",
            "properties": {
                "min_market_cap": {"type": "number", "description": "Min market cap in millions"},
                "max_pe": {"type": "number", "description": "Maximum P/E ratio"},
                "min_div_yield": {"type": "number", "description": "Min annual dividend yield %"},
                "sector": {"type": "string", "description": "Filter by sector name"},
                "shariah": {"type": "boolean", "description": "Shariah-compliant only"},
                "limit": {"type": "integer", "default": 50},
            },
        },
    ),
    types.Tool(
        name="compare_securities",
        description="Side-by-side comparison of multiple stocks.",
        inputSchema={
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of PSX stock symbols to compare",
                },
            },
            "required": ["symbols"],
        },
    ),
    types.Tool(
        name="calculate_returns",
        description="Calculate returns for a stock over multiple periods (1D, 1W, 1M, 3M, 6M, 1Y, YTD).",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "PSX stock symbol"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="get_sector_performance",
        description="Sector-wise average returns and volume for the latest trading day.",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="get_correlation",
        description="Price correlation between two stocks over a period.",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol1": {"type": "string"},
                "symbol2": {"type": "string"},
                "days": {"type": "integer", "default": 90, "description": "Period in days"},
            },
            "required": ["symbol1", "symbol2"],
        },
    ),
    types.Tool(
        name="get_data_freshness",
        description="Status of all data domains — row counts, latest dates, staleness.",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="get_coverage_summary",
        description="Summary of data coverage: symbols, funds, bonds, FX pairs, etc.",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="run_sql",
        description="Execute a read-only SQL query against the database. Only SELECT queries allowed.",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL SELECT query"},
                "limit": {"type": "integer", "default": 100, "description": "Max rows"},
            },
            "required": ["query"],
        },
    ),
]


# ─── TOOL REGISTRATION ─────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return EQUITY_TOOLS + FIXED_INCOME_TOOLS + FUND_FX_TOOLS + ANALYTICS_SYSTEM_TOOLS


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


# ─── FUND + FX + RATES HANDLERS ────────────────────────────────────

def _handle_get_mutual_funds(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    sql = """SELECT f.*, ln.nav as latest_nav, ln.date as nav_date
             FROM mutual_funds f
             LEFT JOIN (
                 SELECT n.fund_id, n.nav, n.date
                 FROM mutual_fund_nav n
                 INNER JOIN (
                     SELECT fund_id, MAX(date) as max_date
                     FROM mutual_fund_nav GROUP BY fund_id
                 ) mx ON n.fund_id = mx.fund_id AND n.date = mx.max_date
             ) ln ON f.fund_id = ln.fund_id
             WHERE f.fund_type != 'VPS'"""
    params: list = []
    if args.get("category"):
        sql += " AND UPPER(f.category) LIKE ?"
        params.append(f"%{args['category'].upper()}%")
    if args.get("shariah"):
        sql += " AND f.is_shariah = 1"
    if args.get("amc"):
        sql += " AND UPPER(f.amc_name) LIKE ?"
        params.append(f"%{args['amc'].upper()}%")
    sql += " ORDER BY f.fund_name LIMIT 100"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


def _handle_get_fund_nav_history(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    fund_id = args["fund_id"]
    limit = args.get("limit", 100)
    sql = "SELECT * FROM mutual_fund_nav WHERE fund_id = ?"
    params: list = [fund_id]
    if args.get("start_date"):
        sql += " AND date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        sql += " AND date <= ?"
        params.append(args["end_date"])
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"fund_id": fund_id, "count": len(rows), "data": rows})


def _handle_get_fund_rankings(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    days = args.get("days", 365)
    n = args.get("n", 20)
    sql = """
        WITH latest_nav AS (
            SELECT n.fund_id, n.nav, n.date
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MAX(date) as max_date FROM mutual_fund_nav GROUP BY fund_id
            ) mx ON n.fund_id = mx.fund_id AND n.date = mx.max_date
        ),
        old_nav AS (
            SELECT n.fund_id, n.nav, n.date
            FROM mutual_fund_nav n
            INNER JOIN (
                SELECT fund_id, MIN(date) as min_date
                FROM mutual_fund_nav WHERE date >= date('now', ? || ' days')
                GROUP BY fund_id
            ) mn ON n.fund_id = mn.fund_id AND n.date = mn.min_date
        )
        SELECT f.fund_id, f.fund_name, f.category, f.amc_name,
               l.nav as latest_nav, l.date as latest_date,
               o.nav as old_nav, o.date as old_date,
               ROUND((l.nav - o.nav) / o.nav * 100, 2) as return_pct
        FROM mutual_funds f
        INNER JOIN latest_nav l ON f.fund_id = l.fund_id
        INNER JOIN old_nav o ON f.fund_id = o.fund_id
        WHERE o.nav > 0
    """
    params: list = [f"-{days}"]
    if args.get("category"):
        sql += " AND UPPER(f.category) LIKE ?"
        params.append(f"%{args['category'].upper()}%")
    sql += " ORDER BY return_pct DESC LIMIT ?"
    params.append(n)
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"period_days": days, "count": len(rows), "rankings": rows})


def _handle_get_etf_data(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    symbol = args["symbol"].upper()
    master = con.execute("SELECT * FROM etf_master WHERE symbol = ?", (symbol,)).fetchone()
    if not master:
        return _json_response({"error": f"ETF {symbol} not found"})
    result = dict(master)
    nav = con.execute(
        "SELECT * FROM etf_nav WHERE symbol = ? ORDER BY date DESC LIMIT 1", (symbol,)
    ).fetchone()
    if nav:
        result.update({"latest_" + k: v for k, v in dict(nav).items() if k != "symbol"})
    return _json_response(result)


def _handle_get_etf_list(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    sql = """SELECT m.*, n.date as nav_date, n.nav, n.market_price,
                    n.premium_discount, n.aum_millions
             FROM etf_master m
             LEFT JOIN etf_nav n ON m.symbol = n.symbol
               AND n.date = (SELECT MAX(date) FROM etf_nav WHERE symbol = m.symbol)
             ORDER BY m.symbol"""
    rows = [dict(r) for r in con.execute(sql).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


_FX_TABLES = {
    "interbank": "sbp_fx_interbank",
    "open_market": "sbp_fx_open_market",
    "kerb": "forex_kerb",
}


def _handle_get_fx_rates(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    currency = args.get("currency", "").upper()
    source = args.get("source", "all")
    result: dict = {}

    sources = _FX_TABLES if source == "all" else {source: _FX_TABLES.get(source, "")}
    for src_name, table in sources.items():
        if not table:
            continue
        params: list = []
        # Subquery to get latest date per currency
        inner = f"SELECT currency, MAX(date) as max_date FROM {table}"
        if currency:
            inner += " WHERE UPPER(currency) = ?"
            params.append(currency)
        inner += " GROUP BY currency"

        sql = f"""SELECT t.* FROM {table} t
                  INNER JOIN ({inner}) mx
                  ON t.currency = mx.currency AND t.date = mx.max_date
                  ORDER BY t.currency"""
        rows = [dict(r) for r in con.execute(sql, params).fetchall()]
        result[src_name] = rows

    return _json_response(result)


def _handle_get_fx_history(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    currency = args["currency"].upper()
    source = args.get("source", "interbank")
    table = _FX_TABLES.get(source, "sbp_fx_interbank")

    sql = f"SELECT * FROM {table} WHERE UPPER(currency) = ?"
    params: list = [currency]
    if args.get("start_date"):
        sql += " AND date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        sql += " AND date <= ?"
        params.append(args["end_date"])
    sql += " ORDER BY date DESC LIMIT 100"
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"currency": currency, "source": source, "count": len(rows), "data": rows})


def _handle_get_fx_spread(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    currency = args.get("currency", "USD").upper()
    result: dict = {"currency": currency}

    for src_name, table in _FX_TABLES.items():
        row = con.execute(
            f"""SELECT date, buying, selling,
                       ROUND(selling - buying, 4) as spread
                FROM {table}
                WHERE UPPER(currency) = ?
                ORDER BY date DESC LIMIT 1""",
            (currency,),
        ).fetchone()
        result[src_name] = dict(row) if row else None

    return _json_response(result)


def _handle_get_kibor(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    date = args.get("date")
    if not date:
        row = con.execute("SELECT MAX(date) as d FROM kibor_daily").fetchone()
        date = row["d"] if row else None
    if not date:
        return _json_response({"error": "No KIBOR data available"})
    rows = con.execute(
        "SELECT tenor, bid, offer FROM kibor_daily WHERE date = ? ORDER BY tenor",
        (date,),
    ).fetchall()
    return _json_response({"date": date, "rates": [dict(r) for r in rows]})


def _handle_get_policy_rate(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    include_history = args.get("history", False)
    latest = con.execute(
        "SELECT * FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    result: dict = {"current": dict(latest) if latest else None}

    if include_history:
        rows = con.execute(
            "SELECT * FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 20"
        ).fetchall()
        result["history"] = [dict(r) for r in rows]

    return _json_response(result)


def _handle_get_konia(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    limit = args.get("limit", 30)
    sql = "SELECT * FROM konia_daily WHERE 1=1"
    params: list = []
    if args.get("start_date"):
        sql += " AND date >= ?"
        params.append(args["start_date"])
    if args.get("end_date"):
        sql += " AND date <= ?"
        params.append(args["end_date"])
    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]
    return _json_response({"count": len(rows), "data": rows})


# ─── ANALYTICS + SYSTEM HANDLERS ───────────────────────────────────

def _handle_screen_stocks(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    limit = args.get("limit", 50)

    # Use latest EOD date for price data; join with symbols for sector
    latest = con.execute("SELECT MAX(date) as d FROM eod_ohlcv").fetchone()
    if not latest or not latest["d"]:
        return _json_response({"error": "No EOD data"})
    date = latest["d"]

    # Get previous date for change calculation
    prev = con.execute(
        "SELECT MAX(date) as d FROM eod_ohlcv WHERE date < ?", (date,)
    ).fetchone()
    prev_date = prev["d"] if prev else None

    sql = """
        SELECT e.symbol, s.name, s.sector_name, e.close as price, e.volume,
               s.outstanding_shares,
               CASE WHEN s.outstanding_shares > 0
                    THEN ROUND(e.close * s.outstanding_shares / 1e6, 2) END as market_cap_m
    """
    if prev_date:
        sql += """,
               ROUND((e.close - p.close) / p.close * 100, 2) as change_pct"""

    sql += """
        FROM eod_ohlcv e
        INNER JOIN symbols s ON e.symbol = s.symbol AND s.is_active = 1
    """
    if prev_date:
        sql += " LEFT JOIN eod_ohlcv p ON e.symbol = p.symbol AND p.date = ?"

    sql += " WHERE e.date = ? AND e.close > 0"
    params: list = []
    if prev_date:
        params.append(prev_date)
    params.append(date)

    if args.get("min_market_cap") and args["min_market_cap"] > 0:
        sql += " AND s.outstanding_shares > 0 AND (e.close * s.outstanding_shares / 1e6) >= ?"
        params.append(args["min_market_cap"])
    if args.get("sector"):
        sql += " AND UPPER(s.sector_name) LIKE ?"
        params.append(f"%{args['sector'].upper()}%")

    sql += " ORDER BY market_cap_m DESC NULLS LAST LIMIT ?"
    params.append(limit)

    rows = [dict(r) for r in con.execute(sql, params).fetchall()]

    # Post-filter for max_pe and min_div_yield if company_fundamentals available
    # (These require fundamentals data which may not exist for all)
    if args.get("max_pe") is not None or args.get("min_div_yield") is not None:
        filtered = []
        for row in rows:
            fund = con.execute(
                "SELECT pe_ratio FROM company_fundamentals WHERE symbol = ?",
                (row["symbol"],),
            ).fetchone()
            pe = fund["pe_ratio"] if fund else None
            if args.get("max_pe") is not None and (pe is None or pe > args["max_pe"]):
                continue
            # Dividend yield from payouts in last year
            if args.get("min_div_yield") is not None:
                div_sum = con.execute(
                    """SELECT COALESCE(SUM(amount), 0) as total
                       FROM company_payouts
                       WHERE symbol = ? AND payout_type = 'cash'
                         AND ex_date >= date('now', '-365 days')""",
                    (row["symbol"],),
                ).fetchone()
                div_yield = (div_sum["total"] / row["price"] * 100) if div_sum and row["price"] > 0 else 0
                if div_yield < args["min_div_yield"]:
                    continue
                row["div_yield"] = round(div_yield, 2)
            if pe is not None:
                row["pe_ratio"] = pe
            filtered.append(row)
        rows = filtered

    return _json_response({"date": date, "count": len(rows), "data": rows})


def _handle_compare_securities(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    symbols = [s.upper() for s in args["symbols"]]

    latest = con.execute("SELECT MAX(date) as d FROM eod_ohlcv").fetchone()
    if not latest or not latest["d"]:
        return _json_response({"error": "No EOD data"})
    date = latest["d"]

    prev = con.execute(
        "SELECT MAX(date) as d FROM eod_ohlcv WHERE date < ?", (date,)
    ).fetchone()
    prev_date = prev["d"] if prev else None

    result = []
    for sym in symbols:
        eod = con.execute(
            "SELECT * FROM eod_ohlcv WHERE symbol = ? AND date = ?", (sym, date)
        ).fetchone()
        info = con.execute(
            "SELECT name, sector_name, outstanding_shares FROM symbols WHERE symbol = ?", (sym,)
        ).fetchone()

        entry: dict = {"symbol": sym}
        if info:
            entry.update(dict(info))
        if eod:
            entry.update({"close": eod["close"], "volume": eod["volume"], "date": date})
            if info and info["outstanding_shares"]:
                entry["market_cap_m"] = round(eod["close"] * info["outstanding_shares"] / 1e6, 2)
            if prev_date:
                prev_eod = con.execute(
                    "SELECT close FROM eod_ohlcv WHERE symbol = ? AND date = ?", (sym, prev_date)
                ).fetchone()
                if prev_eod and prev_eod["close"] > 0:
                    entry["change_pct"] = round(
                        (eod["close"] - prev_eod["close"]) / prev_eod["close"] * 100, 2
                    )
        result.append(entry)

    return _json_response({"date": date, "securities": result})


def _handle_calculate_returns(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    symbol = args["symbol"].upper()

    latest = con.execute(
        "SELECT date, close FROM eod_ohlcv WHERE symbol = ? ORDER BY date DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    if not latest:
        return _json_response({"error": f"No data for {symbol}"})

    current_price = latest["close"]
    current_date = latest["date"]

    # Define periods
    periods = {
        "1D": "-1 days", "1W": "-7 days", "1M": "-1 months",
        "3M": "-3 months", "6M": "-6 months", "1Y": "-1 years",
    }
    returns: dict = {"symbol": symbol, "price": current_price, "date": current_date}

    for label, offset in periods.items():
        target_date = con.execute(f"SELECT date(?, ?)", (current_date, offset)).fetchone()[0]
        row = con.execute(
            """SELECT close FROM eod_ohlcv
               WHERE symbol = ? AND date <= ? ORDER BY date DESC LIMIT 1""",
            (symbol, target_date),
        ).fetchone()
        if row and row["close"] > 0:
            returns[label] = round((current_price - row["close"]) / row["close"] * 100, 2)

    # YTD
    year_start = current_date[:4] + "-01-01"
    row = con.execute(
        """SELECT close FROM eod_ohlcv
           WHERE symbol = ? AND date >= ? ORDER BY date ASC LIMIT 1""",
        (symbol, year_start),
    ).fetchone()
    if row and row["close"] > 0:
        returns["YTD"] = round((current_price - row["close"]) / row["close"] * 100, 2)

    return _json_response(returns)


def _handle_get_sector_performance(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    # Latest two trading dates
    dates = con.execute(
        "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date DESC LIMIT 2"
    ).fetchall()
    if len(dates) < 2:
        return _json_response({"error": "Need at least 2 trading days"})

    latest_date = dates[0]["date"]
    prev_date = dates[1]["date"]

    rows = con.execute(
        """SELECT s.sector_name,
                  COUNT(*) as stocks,
                  SUM(t.volume) as total_volume,
                  ROUND(AVG((t.close - p.close) / p.close * 100), 2) as avg_change_pct,
                  ROUND(MAX((t.close - p.close) / p.close * 100), 2) as best_change_pct,
                  ROUND(MIN((t.close - p.close) / p.close * 100), 2) as worst_change_pct
           FROM eod_ohlcv t
           INNER JOIN eod_ohlcv p ON t.symbol = p.symbol AND p.date = ?
           INNER JOIN symbols s ON t.symbol = s.symbol
           WHERE t.date = ? AND p.close > 0 AND t.close > 0
             AND s.sector_name IS NOT NULL
           GROUP BY s.sector_name
           ORDER BY avg_change_pct DESC""",
        (prev_date, latest_date),
    ).fetchall()

    return _json_response({
        "date": latest_date,
        "prev_date": prev_date,
        "sectors": [dict(r) for r in rows],
    })


def _handle_get_correlation(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    s1 = args["symbol1"].upper()
    s2 = args["symbol2"].upper()
    days = args.get("days", 90)

    cutoff = con.execute(
        f"SELECT date('now', ? || ' days')", (f"-{days}",)
    ).fetchone()[0]

    rows = con.execute(
        """SELECT a.date, a.close as price1, b.close as price2
           FROM eod_ohlcv a
           INNER JOIN eod_ohlcv b ON a.date = b.date
           WHERE a.symbol = ? AND b.symbol = ? AND a.date >= ?
             AND a.close > 0 AND b.close > 0
           ORDER BY a.date""",
        (s1, s2, cutoff),
    ).fetchall()

    if len(rows) < 5:
        return _json_response({
            "error": f"Insufficient overlapping data ({len(rows)} days) for {s1}/{s2}",
        })

    # Compute Pearson correlation of daily returns
    prices1 = [r["price1"] for r in rows]
    prices2 = [r["price2"] for r in rows]
    returns1 = [(prices1[i] - prices1[i - 1]) / prices1[i - 1] for i in range(1, len(prices1))]
    returns2 = [(prices2[i] - prices2[i - 1]) / prices2[i - 1] for i in range(1, len(prices2))]

    n = len(returns1)
    mean1 = sum(returns1) / n
    mean2 = sum(returns2) / n
    cov = sum((r1 - mean1) * (r2 - mean2) for r1, r2 in zip(returns1, returns2)) / n
    std1 = (sum((r - mean1) ** 2 for r in returns1) / n) ** 0.5
    std2 = (sum((r - mean2) ** 2 for r in returns2) / n) ** 0.5
    correlation = round(cov / (std1 * std2), 4) if std1 > 0 and std2 > 0 else 0

    return _json_response({
        "symbol1": s1, "symbol2": s2,
        "period_days": days, "overlapping_days": len(rows),
        "correlation": correlation,
    })


# Data freshness domain config: (table, date_column)
_FRESHNESS_DOMAINS = [
    ("eod_ohlcv", "date"), ("symbols", "updated_at"),
    ("company_fundamentals", "updated_at"), ("company_profile", "updated_at"),
    ("psx_indices", "index_date"), ("mutual_funds", "updated_at"),
    ("mutual_fund_nav", "date"), ("etf_master", "inception_date"),
    ("etf_nav", "date"), ("sukuk_master", "created_at"),
    ("pkrv_daily", "date"), ("tbill_auctions", "auction_date"),
    ("pib_auctions", "auction_date"), ("gis_auctions", "auction_date"),
    ("kibor_daily", "date"), ("konia_daily", "date"),
    ("sbp_policy_rates", "rate_date"),
    ("sbp_fx_interbank", "date"), ("forex_kerb", "date"),
    ("ipo_listings", "listing_date"),
    ("company_payouts", "ex_date"),
]


def _handle_get_data_freshness(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    domains = []
    for table, date_col in _FRESHNESS_DOMAINS:
        try:
            row = con.execute(
                f"SELECT COUNT(*) as rows, MAX({date_col}) as latest FROM {table}"
            ).fetchone()
            domains.append({
                "table": table, "rows": row["rows"], "latest": row["latest"],
            })
        except Exception:
            domains.append({"table": table, "rows": 0, "latest": None, "error": "table missing"})

    # DB stats
    db_size = con.execute("PRAGMA page_count").fetchone()[0] * con.execute("PRAGMA page_size").fetchone()[0]
    table_count = con.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
    ).fetchone()[0]

    return _json_response({
        "domains": domains,
        "db_size_mb": round(db_size / 1e6, 1),
        "table_count": table_count,
    })


def _handle_get_coverage_summary(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    result: dict = {}
    queries = {
        "active_symbols": "SELECT COUNT(*) FROM symbols WHERE is_active = 1",
        "total_symbols": "SELECT COUNT(*) FROM symbols",
        "eod_records": "SELECT COUNT(*) FROM eod_ohlcv",
        "eod_date_range": "SELECT MIN(date) || ' to ' || MAX(date) FROM eod_ohlcv",
        "mutual_funds": "SELECT COUNT(*) FROM mutual_funds",
        "fund_nav_records": "SELECT COUNT(*) FROM mutual_fund_nav",
        "etfs": "SELECT COUNT(*) FROM etf_master",
        "sukuk": "SELECT COUNT(*) FROM sukuk_master",
        "ipo_listings": "SELECT COUNT(*) FROM ipo_listings",
        "pkrv_points": "SELECT COUNT(*) FROM pkrv_daily",
        "tbill_auctions": "SELECT COUNT(*) FROM tbill_auctions",
        "pib_auctions": "SELECT COUNT(*) FROM pib_auctions",
        "company_profiles": "SELECT COUNT(*) FROM company_profile",
    }
    for key, sql in queries.items():
        try:
            row = con.execute(sql).fetchone()
            result[key] = row[0]
        except Exception:
            result[key] = 0

    return _json_response(result)


_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "REPLACE", "ATTACH", "DETACH"}


def _handle_run_sql(con: sqlite3.Connection, args: dict) -> list[types.TextContent]:
    query = args["query"].strip()
    limit = args.get("limit", 100)

    # Security: reject write queries
    first_word = query.split()[0].upper() if query.split() else ""
    if first_word in _WRITE_KEYWORDS:
        return _json_response({"error": f"Write queries not allowed. Only SELECT is permitted."})

    # Extra safety: reject semicolons that could chain statements
    if ";" in query.rstrip(";"):
        return _json_response({"error": "Multiple statements not allowed."})

    try:
        # Add LIMIT if not present
        upper_q = query.upper()
        if "LIMIT" not in upper_q:
            query = query.rstrip(";") + f" LIMIT {limit}"
        rows = [dict(r) for r in con.execute(query).fetchall()]
        return _json_response({"count": len(rows), "data": rows})
    except Exception as e:
        return _json_response({"error": str(e)})


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
    # Fund + FX + Rates
    "get_mutual_funds": _handle_get_mutual_funds,
    "get_fund_nav_history": _handle_get_fund_nav_history,
    "get_fund_rankings": _handle_get_fund_rankings,
    "get_etf_data": _handle_get_etf_data,
    "get_etf_list": _handle_get_etf_list,
    "get_fx_rates": _handle_get_fx_rates,
    "get_fx_history": _handle_get_fx_history,
    "get_fx_spread": _handle_get_fx_spread,
    "get_kibor": _handle_get_kibor,
    "get_policy_rate": _handle_get_policy_rate,
    "get_konia": _handle_get_konia,
    # Analytics + System
    "screen_stocks": _handle_screen_stocks,
    "compare_securities": _handle_compare_securities,
    "calculate_returns": _handle_calculate_returns,
    "get_sector_performance": _handle_get_sector_performance,
    "get_correlation": _handle_get_correlation,
    "get_data_freshness": _handle_get_data_freshness,
    "get_coverage_summary": _handle_get_coverage_summary,
    "run_sql": _handle_run_sql,
}


# ─── RESOURCES ─────────────────────────────────────────────────────

@server.list_resources()
async def list_resources() -> list[types.Resource]:
    return [
        types.Resource(
            uri="psx://schema",
            name="Database Schema",
            description="Complete SQLite schema for all PSX data tables",
            mimeType="text/plain",
        ),
        types.Resource(
            uri="psx://symbols",
            name="Symbol Master List",
            description="All PSX stock symbols with name, sector, and status",
            mimeType="text/plain",
        ),
        types.Resource(
            uri="psx://data-dictionary",
            name="Data Dictionary",
            description="Column descriptions for key tables",
            mimeType="text/plain",
        ),
        types.Resource(
            uri="psx://trading-calendar",
            name="Trading Calendar",
            description="PSX trading days available in the database",
            mimeType="text/plain",
        ),
    ]


@server.read_resource()
async def read_resource(uri: str) -> str:
    con = get_db()
    try:
        if uri == "psx://schema":
            tables = con.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            return "\n\n".join(row[0] for row in tables if row[0])

        elif uri == "psx://symbols":
            rows = con.execute(
                "SELECT symbol, name, sector_name, is_active FROM symbols ORDER BY symbol"
            ).fetchall()
            lines = ["symbol | name | sector | active"]
            lines.extend(
                f"{r['symbol']} | {r['name']} | {r['sector_name']} | {r['is_active']}"
                for r in rows
            )
            return "\n".join(lines)

        elif uri == "psx://data-dictionary":
            return _DATA_DICTIONARY

        elif uri == "psx://trading-calendar":
            rows = con.execute(
                "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date DESC LIMIT 500"
            ).fetchall()
            lines = [f"Trading days in database ({len(rows)} most recent):"]
            lines.extend(r["date"] for r in rows)
            return "\n".join(lines)

        return f"Unknown resource: {uri}"
    finally:
        con.close()


_DATA_DICTIONARY = """\
# PSX OHLCV Data Dictionary

## eod_ohlcv — End of Day OHLCV
- symbol: PSX stock ticker (e.g., OGDC, HBL)
- date: Trading date (YYYY-MM-DD)
- open/high/low/close: Price in PKR
- volume: Number of shares traded
- prev_close: Previous day close (often NULL — use self-join instead)
- sector_code: Sector code from market summary
- source: Data origin (market_summary, closing_rates_pdf, per_symbol_api)

## symbols — Master symbol list
- symbol: PSX stock ticker (PRIMARY KEY)
- name: Full company name
- sector/sector_name: Sector code and description
- outstanding_shares: Total shares outstanding
- is_active: 1 = currently trading

## company_fundamentals — Latest company metrics
- symbol: PSX ticker
- price/change/change_pct: Latest price data
- market_cap: Market capitalization (thousands PKR)
- pe_ratio: Price to earnings ratio
- total_shares/free_float_shares/free_float_pct: Float data
- wk52_low/wk52_high: 52-week trading range
- ytd_change_pct/one_year_change_pct: Performance metrics

## psx_indices — Index data
- index_code: KSE100, KSE30, KMI30, ALLSHR
- value: Current index value
- change/change_pct: Daily change

## mutual_funds — Fund master data
- fund_id: Unique identifier (e.g., MUFAP:ABL-ISF)
- fund_type: OPEN_END, VPS, ETF
- category: Equity, Money Market, Income, etc.
- is_shariah: 1 = Shariah-compliant

## mutual_fund_nav — Fund NAV time series
- fund_id + date: Composite key
- nav: Net Asset Value per unit
- offer_price/redemption_price: Entry/exit prices

## pkrv_daily — PKRV yield curve
- date + tenor_months: Composite key
- yield_pct: Yield percentage

## tbill_auctions / pib_auctions — Treasury auctions
- auction_date + tenor: Key fields
- cutoff_yield/cutoff_price: Auction results
- amount_accepted_billions: Volume

## kibor_daily — Interbank offered rate
- date + tenor: Key fields
- bid/offer: Rate percentages

## company_payouts — Dividend/bonus history
- symbol + ex_date + payout_type: Composite key
- amount: Cash dividend per share or bonus percentage
"""


# ─── PROMPTS ───────────────────────────────────────────────────────

@server.list_prompts()
async def list_prompts() -> list[types.Prompt]:
    return [
        types.Prompt(
            name="daily_market_brief",
            description="Generate a comprehensive morning market brief for Pakistan's financial markets",
            arguments=[],
        ),
        types.Prompt(
            name="stock_deep_dive",
            description="Comprehensive single-stock analysis",
            arguments=[
                types.PromptArgument(
                    name="symbol", description="PSX stock symbol", required=True
                ),
            ],
        ),
        types.Prompt(
            name="portfolio_review",
            description="Multi-stock portfolio analysis",
            arguments=[
                types.PromptArgument(
                    name="symbols",
                    description="Comma-separated PSX stock symbols",
                    required=True,
                ),
            ],
        ),
        types.Prompt(
            name="sector_rotation",
            description="Cross-sector comparison and rotation analysis",
            arguments=[],
        ),
        types.Prompt(
            name="yield_curve_analysis",
            description="Fixed income landscape and yield curve analysis",
            arguments=[],
        ),
        types.Prompt(
            name="fx_outlook",
            description="Currency market overview — PKR rates across sources",
            arguments=[],
        ),
    ]


@server.get_prompt()
async def get_prompt(name: str, arguments: dict | None = None) -> types.GetPromptResult:
    arguments = arguments or {}

    if name == "daily_market_brief":
        return types.GetPromptResult(
            messages=[types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text="""\
Execute these tools and synthesize into a professional market brief:

1. get_market_snapshot() — Get today's market data
2. get_top_movers(n=10, direction="both") — Top gainers and losers
3. get_sector_performance() — Sector rotation
4. get_fx_rates(currency="USD") — USD/PKR rate
5. get_latest_yields() — Treasury yields
6. get_kibor() — Interbank rates

Format the output as a professional market brief with:
- Market headline (1 line)
- Index performance (KSE-100, KSE-30, KMI-30)
- Top movers table
- Sector highlights
- Fixed income snapshot
- Currency update
"""),
            )],
        )

    elif name == "stock_deep_dive":
        symbol = arguments.get("symbol", "OGDC")
        return types.GetPromptResult(
            messages=[types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text=f"""\
Perform a comprehensive analysis of {symbol}:

1. get_company_profile(symbol="{symbol}") — Company overview
2. get_eod(symbol="{symbol}", limit=30) — Last 30 trading days
3. calculate_returns(symbol="{symbol}") — Multi-period returns
4. get_correlation(symbol1="{symbol}", symbol2="KSE100", days=180) — Market correlation

Provide:
- Company overview and sector positioning
- Price action and volume analysis
- Technical levels (support/resistance from data)
- Return profile vs market
- Key risks and catalysts
"""),
            )],
        )

    elif name == "portfolio_review":
        symbols_str = arguments.get("symbols", "OGDC,HBL,LUCK")
        symbols = [s.strip() for s in symbols_str.split(",")]
        tool_calls = "\n".join(
            f'{i+1}. calculate_returns(symbol="{s}")' for i, s in enumerate(symbols)
        )
        return types.GetPromptResult(
            messages=[types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text=f"""\
Analyze this portfolio: {', '.join(symbols)}

1. compare_securities(symbols={json.dumps(symbols)})
{tool_calls}
{len(symbols)+2}. get_sector_performance()

Provide:
- Portfolio composition and sector exposure
- Individual stock performance
- Correlation between holdings
- Portfolio-level returns
- Rebalancing suggestions
"""),
            )],
        )

    elif name == "sector_rotation":
        return types.GetPromptResult(
            messages=[types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text="""\
Analyze sector rotation in Pakistan's equity market:

1. get_sector_performance() — Current sector returns
2. get_market_snapshot() — Overall market context
3. screen_stocks(limit=10) — Top stocks by market cap

Provide:
- Sector ranking by performance
- Leading and lagging sectors
- Volume analysis by sector
- Sector rotation signals
- Top picks from leading sectors
"""),
            )],
        )

    elif name == "yield_curve_analysis":
        return types.GetPromptResult(
            messages=[types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text="""\
Analyze Pakistan's fixed income landscape:

1. get_yield_curve(curve_type="pkrv") — Current PKRV yield curve
2. get_latest_yields() — All yield benchmarks
3. get_tbill_auctions() — Recent T-Bill auctions
4. get_pib_auctions() — Recent PIB auctions
5. get_policy_rate(history=true) — SBP policy rate trajectory

Provide:
- Yield curve shape analysis (normal/inverted/flat)
- Key rate levels across tenors
- Monetary policy direction
- Auction demand trends
- Rate outlook
"""),
            )],
        )

    elif name == "fx_outlook":
        return types.GetPromptResult(
            messages=[types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text="""\
Analyze Pakistan's currency market:

1. get_fx_rates(source="all") — All FX sources
2. get_fx_spread(currency="USD") — USD spread across sources
3. get_fx_spread(currency="EUR") — EUR spread
4. get_fx_spread(currency="GBP") — GBP spread
5. get_policy_rate() — SBP policy rate context

Provide:
- USD/PKR rate across interbank, open market, and kerb
- Spread analysis (interbank vs kerb premium)
- Major currency pair rates
- Policy rate context for currency outlook
"""),
            )],
        )

    return types.GetPromptResult(
        messages=[types.PromptMessage(
            role="user",
            content=types.TextContent(type="text", text=f"Unknown prompt: {name}"),
        )],
    )


# ─── ENTRY POINT ───────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
