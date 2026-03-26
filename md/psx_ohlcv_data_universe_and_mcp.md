# PSX OHLCV v3.0 — Complete Pakistan Financial Data Universe + MCP Architecture

## Vision
Transform PSX OHLCV from a stock data app into the definitive Pakistan financial research platform — covering every investable asset class with historical + real-time data, exposed via MCP for AI-powered analysis.

---

## 1. COMPLETE DATA UNIVERSE MAP

### What You ALREADY Have (Audit of v2.0.0)

| # | Asset Class | Tables Exist | Scraper Exists | Data Populated | Source |
|---|-------------|:---:|:---:|:---:|---|
| 1 | **Equities — EOD OHLCV** | ✅ eod_ohlcv | ✅ | ✅ ~540 symbols | dps.psx.com.pk/timeseries/eod/{SYM} |
| 2 | **Equities — Intraday** | ✅ intraday_bars | ✅ | ✅ | dps.psx.com.pk/timeseries/int/{SYM} |
| 3 | **Company Profiles** | ✅ company_fundamentals | ✅ | ✅ | dps.psx.com.pk/company/{SYM} |
| 4 | **Company Quotes** | ✅ company_quote_snapshots | ✅ | ✅ | dps.psx.com.pk/company/{SYM} |
| 5 | **Market Indices** | ✅ psx_indices | ✅ | ✅ | dps.psx.com.pk/indices |
| 6 | **Regular Market** | ✅ regular_market_snapshots | ✅ | ✅ | dps.psx.com.pk/market-watch |
| 7 | **Sector Summary** | ✅ (via queries) | ✅ | ✅ | dps.psx.com.pk/sector-summary |
| 8 | **Corporate Announcements** | ✅ corporate_announcements | ✅ | Partial | psx.com.pk announcements |
| 9 | **Financial Announcements** | ✅ financial_announcements | ✅ | Partial | psx.com.pk/announcement/financial |
| 10 | **Instruments (Indices)** | ✅ instruments, ohlcv_instruments | ✅ | ✅ | dps.psx.com.pk |
| 11 | **Bonds (Corporate)** | ✅ bonds_master, bond_quotes, bond_analytics | ✅ | ✅ | dps.psx.com.pk/debt/{SYM} |
| 12 | **Sukuk** | ✅ sukuk_master, sukuk_quotes, sukuk_yield_curve | ✅ | ✅ | dps.psx.com.pk + SBP |
| 13 | **FX Pairs** | ✅ fx_pairs, fx_ohlcv | ✅ | Partial | Multiple sources |
| 14 | **Mutual Funds (MUFAP)** | ✅ mutual_funds, mutual_fund_nav | ✅ | ✅ | mufap.com.pk |
| 15 | **SBP Policy Rates** | ✅ sbp_policy_rates | ✅ | ✅ | sbp.org.pk |
| 16 | **KIBOR Rates** | ✅ kibor_rates | ✅ | ✅ | sbp.org.pk |
| 17 | **FI Instruments** | ✅ fi_instruments | ✅ | Partial | PSX fixed income |

**Verdict: You already have 17 data domains with tables and scrapers. The foundation is massive.**

---

### What's MISSING or INCOMPLETE

| # | Data Domain | Status | Source | Priority | Effort |
|---|-------------|--------|--------|----------|--------|
| 18 | **ETFs (PSX-listed)** | Tables: NO, Scraper: NO | dps.psx.com.pk/etf/{SYM} | 🔴 HIGH | 1 day |
| 19 | **REITs (PSX-listed)** | Tables: NO, Scraper: NO | dps.psx.com.pk (treated as equity) | 🔴 HIGH | 0.5 day |
| 20 | **T-Bill Auction Results** | Tables: NO, Scraper: PARTIAL (URL exists) | sbp.org.pk/ecodata/auction-tbills.pdf | 🔴 HIGH | 1 day |
| 21 | **PIB Auction Results** | Tables: NO, Scraper: NO | sbp.org.pk/ecodata/pib-auction.asp | 🔴 HIGH | 1 day |
| 22 | **Govt Ijara Sukuk Auctions** | Tables: NO, Scraper: NO | sbp.org.pk (Govt Islamic bonds) | 🟡 MED | 0.5 day |
| 23 | **PKRV Yield Curve** | Tables: NO, Scraper: NO | sbp.org.pk PKRV rates | 🔴 HIGH | 0.5 day |
| 24 | **KONIA Rates** | Tables: NO, Scraper: NO | sbp.org.pk KONIA | 🟡 MED | 0.5 day |
| 25 | **SBP FX Interbank** | Tables: NO, Scraper: NO | sbp.org.pk/ecodata/rates/ | 🔴 HIGH | 0.5 day |
| 26 | **SBP FX Open Market** | Tables: NO, Scraper: NO | sbp.org.pk/ecodata/rates/ECAP/ | 🔴 HIGH | 0.5 day |
| 27 | **Forex Open Market (kerb)** | Tables: NO, Scraper: NO | forex.pk, hamariweb | 🟡 MED | 0.5 day |
| 28 | **VPS (Voluntary Pension)** | Tables: NO, Scraper: NO | mufap.com.pk VPS section | 🟡 MED | 1 day |
| 29 | **Secondary Bond Trading** | Tables: NO, Scraper: NO | gsp.sbp.org.pk trading data | 🟡 MED | 1 day |
| 30 | **Commodity Prices (Gold/Silver/Crude)** | Tables: NO, Scraper: NO | forex.pk / international APIs | 🟢 LOW | 0.5 day |
| 31 | **IPO Calendar / Listing Status** | Tables: NO, Scraper: NO | dps.psx.com.pk/listings | 🟡 MED | 0.5 day |
| 32 | **Dividend/Corporate Actions History** | PARTIAL in announcements | dps.psx.com.pk company pages | 🟡 MED | 1 day |
| 33 | **Insider Trading / Shareholding** | Tables: NO, Scraper: NO | SECP filings | 🟢 LOW | 1 day |
| 34 | **Economic Indicators (CPI, GDP)** | Tables: NO, Scraper: NO | sbp.org.pk/ecodata/ | 🟢 LOW | 1 day |

---

## 2. DATA SOURCE DEEP DIVE — Every Endpoint

### A. PSX Data Portal (dps.psx.com.pk) — FREE, No Auth

| Endpoint | Format | Freq | What You Get |
|----------|--------|------|-------------|
| `/timeseries/eod/{SYM}` | JSON | EOD | Full OHLCV history (already scraped) |
| `/timeseries/int/{SYM}` | JSON | Live | Intraday bars (already scraped) |
| `/company/{SYM}` | HTML+JSON | EOD | Profile, fundamentals, quote (already scraped) |
| `/market-watch` | JSON | Live | All symbols real-time quotes (already scraped) |
| `/indices` | JSON | Live | All index values (already scraped) |
| `/sector-summary` | JSON | Live | Sector-wise aggregates (already scraped) |
| `/etf/{SYM}` | HTML+JSON | EOD | **NEW — ETF NAV, basket, benchmark** |
| `/debt/{SYM}` | HTML+JSON | EOD | Bond/sukuk detail (already scraped) |
| `/listings` | HTML | Weekly | **NEW — IPO calendar, listing status** |
| `/announcements/secp` | HTML | Daily | SECP notices |
| `/announcements/companies` | HTML | Daily | Corporate announcements (partial) |
| `/download/closing_rates/{DATE}.pdf` | PDF | EOD | Official closing prices PDF |
| `/download/mkt_summary/{DATE}.Z` | Compressed | EOD | Full market summary file |
| `/download/text/listed_cmp.lst.Z` | Compressed | Weekly | Listed companies master file |

### B. SBP (sbp.org.pk) — FREE, No Auth

| Endpoint | Format | Freq | What You Get |
|----------|--------|------|-------------|
| `/ecodata/auction-tbills.pdf` | PDF | Bi-weekly | **NEW — T-Bill auction results** |
| `/ecodata/t-bills.asp` | HTML | Bi-weekly | **T-Bill data (partial scraper exists)** |
| `/ecodata/auction-results.asp` | HTML | Bi-weekly | **Auction results page** |
| `/ecodata/rates/WAR/WAR-Current.asp` | HTML | Daily | **NEW — Weighted Avg Rates (interbank FX)** |
| `/ecodata/rates/ECAP/ECAP-Current.asp` | HTML | Daily | **NEW — Open market exchange rates** |
| `/ecodata/rates/KIBOR/` | HTML | Daily | KIBOR rates (already scraped) |
| gsp.sbp.org.pk `/auction_results` | HTML | Bi-weekly | **NEW — PIB/Sukuk auction results** |
| gsp.sbp.org.pk PKRV rates | HTML | Daily | **NEW — PKRV yield curve** |
| gsp.sbp.org.pk KONIA rates | HTML | Daily | **NEW — KONIA overnight rates** |
| gsp.sbp.org.pk secondary market | HTML | Daily | **NEW — Secondary bond trading volumes** |

### C. MUFAP (mufap.com.pk) — FREE, No Auth

| Endpoint | Format | Freq | What You Get |
|----------|--------|------|-------------|
| `/Industry/IndustryStatDaily?tab=3` | HTML/JSON | Daily | NAV + Sales Load (already scraped) |
| `/Industry/IndustryStatDaily?tab=1` | HTML/JSON | Daily | Performance Summary |
| `/FundProfile/FundDirectory` | HTML | Static | **NEW — Complete fund directory** |
| VPS section | HTML | Daily | **NEW — Voluntary Pension fund NAVs** |
| old.mufap.com.pk/nav-report.php | HTML | Daily | Legacy NAV data (more complete) |

### D. Third-Party FREE APIs

| Source | Endpoint | Format | What You Get | Limits |
|--------|----------|--------|-------------|--------|
| **forex.pk** | `/open_market_rates.asp` | HTML | Open market / kerb FX rates | Scrape |
| **Open Exchange Rates** | `openexchangerates.org/api/latest.json` | JSON | 170+ currencies vs USD | 1000/mo free |
| **ExchangeRate-API** | `v6.exchangerate-api.com/v6/{KEY}/latest/PKR` | JSON | PKR cross-rates | 1500/mo free |
| **Yahoo Finance** | `.KA` suffix for PSX | JSON | EOD OHLCV (backup source) | Rate limited |

### E. Paid APIs (If Needed Later)

| Source | Coverage | Price | When Needed |
|--------|----------|-------|-------------|
| **EODHD** | PSX EOD + fundamentals | $30/mo | If DPS goes down or blocks |
| **Alpha Vantage** | Global stocks, FX, crypto | Free tier (25/day) | Global comparisons |
| **Twelve Data** | Real-time + historical | Free tier (800/day) | Real-time FX |
| **PSX Official Data Feed** | Level 1/2 real-time | Contact PSX | Production SaaS product |

---

## 3. ETF + REIT DETAIL (What PSX Has)

### PSX-Listed ETFs (as of Feb 2026)

| Symbol | Name | Benchmark | AMC |
|--------|------|-----------|-----|
| MZNPETF | Meezan Pakistan ETF | KMI-30 (Shariah) | Al Meezan |
| NBPGETF | NBP Growth ETF | KSE-30 | NBP Funds |
| NITGETF | NIT Growth ETF | KSE-30 | NIT |
| UBLPETF | UBL Pakistan ETF | KSE-100 | UBL Fund Managers |
| MIIETF | Mahaana Islamic Index ETF | KMI-30 | Mahaana Wealth |

ETFs trade like stocks on PSX. DPS endpoint: `dps.psx.com.pk/etf/{SYMBOL}`
They already have EOD OHLCV via `/timeseries/eod/{SYM}` — but ETF-specific data (NAV, basket composition, iNAV) needs separate scraping.

### PSX-Listed REITs

| Symbol | Name | Type |
|--------|------|------|
| DLREIT | Dolmen City REIT | Rental REIT |
| ARREIT | Arif Habib REIT | Developmental REIT |
| SREIT | Signature Residency REIT | Rental REIT |
| + more | New listings in 2025-26 | Various |

REITs trade as equities — already captured in EOD OHLCV. Need REIT-specific data: NAV, rental yield, occupancy, property details from SECP filings.

---

## 4. MCP SERVER ARCHITECTURE

### What Is MCP (Model Context Protocol)

MCP lets AI assistants (Claude, GPT, etc.) call your application's tools directly. Instead of the user copy-pasting data, the AI calls `get_eod("OGDC", "2025-01-01", "2025-12-31")` and gets structured data back.

**For PSX OHLCV, an MCP server means:**
- Claude Code can query your database directly
- AI agents can do financial research autonomously
- Your FastAPI endpoints become AI-accessible tools
- Natural language → structured financial data pipeline

### Existing PSX MCP Server (by ahad-raza24)

There's already a basic PSX MCP server on GitHub that hits 3 DPS endpoints:
- `/market-watch` → market data
- `/timeseries/int/{SYM}` → intraday
- `/timeseries/eod/{SYM}` → EOD

**Your version should be 10x more comprehensive** because you have 17+ data domains in your SQLite database, not just 3 DPS endpoints.

### PSX OHLCV MCP Server Design

```
┌────────────────────────────────────────────┐
│          MCP CLIENT (Claude Code,          │
│          Claude Desktop, AI Agents)        │
└──────────────┬─────────────────────────────┘
               │ MCP Protocol (stdio or HTTP)
               ▼
┌────────────────────────────────────────────┐
│        pakfindata MCP Server                │
│        (src/pakfindata/mcp_server.py)       │
│                                            │
│  TOOLS (callable by AI):                   │
│  ├── Equities                              │
│  │   ├── get_eod(symbol, start, end)       │
│  │   ├── get_intraday(symbol)              │
│  │   ├── get_company_profile(symbol)       │
│  │   ├── get_market_snapshot()             │
│  │   ├── get_sector_summary()              │
│  │   └── search_symbols(query)             │
│  ├── Fixed Income                          │
│  │   ├── get_bonds(filters)                │
│  │   ├── get_sukuk(filters)                │
│  │   ├── get_yield_curve(type, date)       │
│  │   ├── get_tbill_auctions(start, end)    │
│  │   └── get_pib_auctions(start, end)      │
│  ├── Funds                                 │
│  │   ├── get_mutual_funds(category, amc)   │
│  │   ├── get_fund_nav_history(fund_id)     │
│  │   ├── get_etf_data(symbol)              │
│  │   └── get_fund_rankings(period)         │
│  ├── FX & Rates                            │
│  │   ├── get_fx_rates(pair, source)        │
│  │   ├── get_kibor_rates(date)             │
│  │   ├── get_policy_rate_history()         │
│  │   ├── get_pkrv_curve(date)              │
│  │   └── get_open_market_fx()              │
│  ├── Indices & Market                      │
│  │   ├── get_index_values(index)           │
│  │   ├── get_market_breadth()              │
│  │   ├── get_top_gainers(n)                │
│  │   └── get_top_losers(n)                 │
│  ├── Analytics                             │
│  │   ├── calculate_returns(symbol, period) │
│  │   ├── compare_securities(symbols)       │
│  │   ├── get_correlation_matrix(symbols)   │
│  │   └── screen_stocks(filters)            │
│  └── System                                │
│      ├── get_data_freshness()              │
│      ├── get_coverage_summary()            │
│      └── trigger_sync(data_type)           │
│                                            │
│  RESOURCES (context for AI):               │
│  ├── Database schema                       │
│  ├── Data dictionary                       │
│  ├── PSX trading calendar                  │
│  └── Symbol master list                    │
│                                            │
│  PROMPTS (templates for AI):               │
│  ├── daily_market_report                   │
│  ├── stock_analysis                        │
│  ├── portfolio_review                      │
│  └── sector_comparison                     │
└──────────────┬─────────────────────────────┘
               │ Direct DB access
               ▼
┌────────────────────────────────────────────┐
│     /mnt/e/psxdata/psx.sqlite              │
│     (50+ tables, all data domains)         │
└────────────────────────────────────────────┘
```

### MCP Server Implementation

```python
# src/pakfindata/mcp_server.py
from mcp.server import Server
from mcp.types import Tool, Resource, TextContent
import json

server = Server("psx-ohlcv")

# ─── TOOLS ──────────────────────────────────

@server.tool()
async def get_eod(symbol: str, start_date: str = None, end_date: str = None) -> str:
    """Get EOD OHLCV data for a PSX symbol. Returns JSON array of {date, open, high, low, close, volume}."""
    from pakfindata.db import connect, get_eod_ohlcv
    con = connect()
    df = get_eod_ohlcv(con, symbol, start_date, end_date)
    return df.to_json(orient="records", date_format="iso")

@server.tool()
async def get_market_snapshot() -> str:
    """Get current market snapshot: all symbols with last price, change, volume."""
    ...

@server.tool()
async def get_mutual_funds(category: str = None, shariah: bool = None) -> str:
    """Get mutual fund list with latest NAV, returns, AUM. Filter by category or shariah compliance."""
    ...

@server.tool()
async def get_yield_curve(curve_type: str = "pkrv", date: str = None) -> str:
    """Get yield curve data. Types: pkrv, tbill, pib, sukuk. Returns tenors and yields."""
    ...

@server.tool()
async def screen_stocks(
    min_market_cap: float = None,
    max_pe: float = None,
    min_dividend_yield: float = None,
    sector: str = None,
    shariah_compliant: bool = None,
) -> str:
    """Screen PSX stocks by fundamental criteria. Returns matching symbols with key metrics."""
    ...

# ─── RESOURCES ──────────────────────────────

@server.resource("psx://schema")
async def get_schema() -> str:
    """Complete database schema for reference."""
    ...

@server.resource("psx://symbols")
async def get_symbols() -> str:
    """Master list of all PSX symbols with sector, status."""
    ...

@server.resource("psx://calendar")
async def get_calendar() -> str:
    """PSX trading calendar: holidays, half-days, settlement dates."""
    ...

# ─── PROMPTS ────────────────────────────────

@server.prompt()
async def daily_market_report() -> str:
    """Generate a comprehensive daily market report template."""
    return """Analyze today's PSX market:
    1. Call get_market_snapshot() for current prices
    2. Call get_index_values("KSE100") for index performance
    3. Call get_top_gainers(10) and get_top_losers(10)
    4. Call get_sector_summary() for sector rotation
    5. Call get_fx_rates("USDPKR") for currency impact
    6. Synthesize into a professional market report"""
```

### MCP Config for Claude Desktop / Claude Code

```json
// claude_desktop_config.json
{
  "mcpServers": {
    "psx-ohlcv": {
      "command": "python",
      "args": ["-m", "pakfindata.mcp_server"],
      "env": {
        "PSX_DB_PATH": "/mnt/e/psxdata/psx.sqlite"
      }
    }
  }
}
```

```toml
# .claude/config.toml (for Claude Code)
[mcp_servers.psx-ohlcv]
command = "python"
args = ["-m", "pakfindata.mcp_server"]

[mcp_servers.psx-ohlcv.env]
PSX_DB_PATH = "/mnt/e/psxdata/psx.sqlite"
```

---

## 5. IMPLEMENTATION PLAN — Claude Code Prompts

### PHASE A: Fill Data Gaps (New Scrapers) — 5 days

| Prompt | Task | New Tables | Source |
|--------|------|------------|--------|
| A.1 | ETF scraper + tables | etf_master, etf_nav, etf_basket | dps.psx.com.pk/etf/{SYM} |
| A.2 | REIT metadata enrichment | reit_properties (extend company_fundamentals) | SECP + DPS |
| A.3 | T-Bill auction scraper | tbill_auctions, tbill_auction_details | sbp.org.pk/ecodata/ |
| A.4 | PIB auction scraper | pib_auctions, pib_auction_details | gsp.sbp.org.pk |
| A.5 | Govt Ijara Sukuk auctions | gis_auctions | gsp.sbp.org.pk |
| A.6 | PKRV yield curve scraper | pkrv_daily_curve | gsp.sbp.org.pk PKRV |
| A.7 | KONIA rates scraper | konia_daily_rates | gsp.sbp.org.pk KONIA |
| A.8 | SBP FX rates (interbank + open market) | sbp_fx_interbank, sbp_fx_open_market | sbp.org.pk/ecodata/rates/ |
| A.9 | Forex.pk open market rates | forex_open_market | forex.pk scrape |
| A.10 | VPS (pension funds) via MUFAP | vps_funds, vps_nav (extend mutual_fund tables) | mufap.com.pk |
| A.11 | IPO calendar / listing status | ipo_listings | dps.psx.com.pk/listings |
| A.12 | Dividend history extraction | dividend_history | dps.psx.com.pk company pages |
| A.13 | Commit + verify all new scrapers | — | — |

### PHASE B: Build MCP Server — 2 days

| Prompt | Task |
|--------|------|
| B.1 | Install mcp SDK, create base server with 5 equity tools |
| B.2 | Add fixed income tools (bonds, sukuk, yield curves, auctions) |
| B.3 | Add funds tools (mutual funds, ETFs, VPS, rankings) |
| B.4 | Add FX & rates tools (all FX sources, KIBOR, KONIA, policy rate) |
| B.5 | Add analytics tools (screening, correlation, returns calculation) |
| B.6 | Add resources (schema, symbols, calendar) + prompts (daily report, analysis) |
| B.7 | Claude Desktop config + Claude Code config + test all tools |
| B.8 | Commit |

### PHASE C: Real-Time + Automation — 1 day

| Prompt | Task |
|--------|------|
| C.1 | Integrate ALL new scrapers into async sync pipeline |
| C.2 | Update cron: daily EOD + weekly SBP auctions + daily FX |
| C.3 | Add MCP tool: trigger_sync for on-demand refresh |
| C.4 | Data freshness dashboard: all 30+ data domains |

### Total: ~8 days of Claude Code sessions

---

## 6. NEW DATABASE TABLES (Schema for Phase A)

```sql
-- ═══════════════════════════════════
-- ETFs
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS etf_master (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    amc TEXT,
    benchmark_index TEXT,
    inception_date TEXT,
    expense_ratio REAL,
    shariah_compliant INTEGER DEFAULT 0,
    nav_frequency TEXT DEFAULT 'daily',
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS etf_nav (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    nav REAL,
    inav REAL,          -- indicative NAV (intraday)
    market_price REAL,  -- actual trading price
    premium_discount REAL, -- (market_price - nav) / nav * 100
    aum REAL,           -- assets under management (Rs millions)
    PRIMARY KEY (symbol, date),
    FOREIGN KEY (symbol) REFERENCES etf_master(symbol)
);

CREATE TABLE IF NOT EXISTS etf_basket (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    constituent_symbol TEXT NOT NULL,
    weight REAL,
    shares INTEGER,
    PRIMARY KEY (symbol, date, constituent_symbol)
);

-- ═══════════════════════════════════
-- T-BILL AUCTIONS (SBP)
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS tbill_auctions (
    auction_date TEXT NOT NULL,
    settlement_date TEXT NOT NULL,
    tenor TEXT NOT NULL,          -- '1M', '3M', '6M', '12M'
    target_amount REAL,           -- Rs billions
    bids_received REAL,
    amount_accepted REAL,
    cutoff_yield REAL,            -- annualized %
    cutoff_price REAL,
    weighted_avg_yield REAL,
    non_competitive_amount REAL,
    maturity_date TEXT,
    PRIMARY KEY (auction_date, tenor)
);

-- ═══════════════════════════════════
-- PIB AUCTIONS (SBP)
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS pib_auctions (
    auction_date TEXT NOT NULL,
    settlement_date TEXT NOT NULL,
    tenor TEXT NOT NULL,          -- '3Y', '5Y', '10Y', '15Y', '20Y', '30Y'
    pib_type TEXT NOT NULL,       -- 'Fixed', 'Floating-SA', 'Floating-Q', 'Floating-FR'
    target_amount REAL,
    bids_received REAL,
    amount_accepted REAL,
    cutoff_yield REAL,
    cutoff_price REAL,
    coupon_rate REAL,
    maturity_date TEXT,
    PRIMARY KEY (auction_date, tenor, pib_type)
);

-- ═══════════════════════════════════
-- GOVT IJARA SUKUK AUCTIONS
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS gis_auctions (
    auction_date TEXT NOT NULL,
    settlement_date TEXT NOT NULL,
    gis_type TEXT NOT NULL,       -- 'Fixed', 'Variable'
    tenor TEXT,
    target_amount REAL,
    bids_received REAL,
    amount_accepted REAL,
    cutoff_rental_rate REAL,
    maturity_date TEXT,
    PRIMARY KEY (auction_date, gis_type)
);

-- ═══════════════════════════════════
-- PKRV YIELD CURVE (Daily)
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS pkrv_daily_curve (
    date TEXT NOT NULL,
    tenor_months INTEGER NOT NULL,  -- 1, 3, 6, 12, 24, 36, 60, 120, 180, 240, 360
    yield REAL NOT NULL,
    PRIMARY KEY (date, tenor_months)
);

-- ═══════════════════════════════════
-- KONIA RATES (Daily Overnight Rate)
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS konia_daily_rates (
    date TEXT PRIMARY KEY,
    konia_rate REAL NOT NULL,
    volume REAL,                    -- Rs billions traded
    high REAL,
    low REAL,
    number_of_trades INTEGER
);

-- ═══════════════════════════════════
-- SBP FX RATES
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS sbp_fx_interbank (
    date TEXT NOT NULL,
    currency TEXT NOT NULL,         -- 'USD', 'EUR', 'GBP', 'SAR', etc.
    buying REAL,
    selling REAL,
    mid REAL,
    PRIMARY KEY (date, currency)
);

CREATE TABLE IF NOT EXISTS sbp_fx_open_market (
    date TEXT NOT NULL,
    currency TEXT NOT NULL,
    buying REAL,
    selling REAL,
    PRIMARY KEY (date, currency)
);

-- ═══════════════════════════════════
-- OPEN MARKET FX (kerb / forex dealers)
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS forex_open_market (
    date TEXT NOT NULL,
    currency TEXT NOT NULL,
    buying REAL,
    selling REAL,
    source TEXT DEFAULT 'forex.pk',
    ts TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, currency, source)
);

-- ═══════════════════════════════════
-- IPO / LISTING STATUS
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS ipo_listings (
    symbol TEXT NOT NULL,
    company_name TEXT,
    board TEXT,                     -- 'Main Board', 'GEM'
    status TEXT,                    -- 'Upcoming', 'Active', 'Closed', 'Listed'
    offer_price REAL,
    shares_offered INTEGER,
    subscription_open TEXT,
    subscription_close TEXT,
    listing_date TEXT,
    ipo_type TEXT,                  -- 'IPO', 'OFS', 'Book Building'
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, subscription_open)
);

-- ═══════════════════════════════════
-- DIVIDEND HISTORY
-- ═══════════════════════════════════

CREATE TABLE IF NOT EXISTS dividend_history (
    symbol TEXT NOT NULL,
    announcement_date TEXT,
    ex_date TEXT,
    payment_date TEXT,
    dividend_type TEXT,             -- 'Cash', 'Stock', 'Bonus', 'Right'
    amount REAL,                    -- Rs per share (for cash) or % (for bonus/right)
    year_end TEXT,
    PRIMARY KEY (symbol, announcement_date, dividend_type)
);

-- Indexes for all new tables
CREATE INDEX IF NOT EXISTS idx_etf_nav_date ON etf_nav(date);
CREATE INDEX IF NOT EXISTS idx_tbill_date ON tbill_auctions(auction_date);
CREATE INDEX IF NOT EXISTS idx_pib_date ON pib_auctions(auction_date);
CREATE INDEX IF NOT EXISTS idx_pkrv_date ON pkrv_daily_curve(date);
CREATE INDEX IF NOT EXISTS idx_sbp_fx_ib_date ON sbp_fx_interbank(date);
CREATE INDEX IF NOT EXISTS idx_sbp_fx_om_date ON sbp_fx_open_market(date);
CREATE INDEX IF NOT EXISTS idx_forex_om_date ON forex_open_market(date);
CREATE INDEX IF NOT EXISTS idx_dividend_symbol ON dividend_history(symbol);
```

---

## 7. DATA REFRESH SCHEDULE

| Data Domain | Frequency | Time (PKT) | Method |
|-------------|-----------|------------|--------|
| Equities EOD | Daily Mon-Fri | 18:30 | Async fetcher (existing) |
| Intraday | During market hours | Every 5 min | Async fetcher |
| Market watch | During market hours | Every 1 min | Async fetcher |
| Indices | Daily Mon-Fri | 18:30 | With EOD sync |
| ETF NAV | Daily Mon-Fri | 19:00 | New scraper |
| Mutual Fund NAV | Daily Mon-Fri | 20:00 | MUFAP sync (existing) |
| T-Bill auctions | Bi-weekly (Wed) | 19:00 | SBP scraper |
| PIB auctions | Monthly | 19:00 | SBP scraper |
| PKRV curve | Daily Mon-Fri | 18:00 | SBP scraper |
| KIBOR | Daily Mon-Fri | 12:00 | SBP scraper (existing) |
| KONIA | Daily Mon-Fri | 18:00 | SBP scraper |
| SBP FX rates | Daily Mon-Fri | 17:00 | SBP scraper |
| Forex open market | 3x daily | 10:00, 14:00, 17:00 | forex.pk scraper |
| Bonds/Sukuk | Daily Mon-Fri | 18:30 | With EOD sync (existing) |
| Company profiles | Weekly (Fri) | 20:00 | Deep scrape (existing) |
| Dividends | Daily | 19:00 | Announcements scraper |
| IPO calendar | Weekly | 20:00 | DPS scraper |

### Cron Schedule

```bash
# ═══ MARKET HOURS (Mon-Fri) ═══
# Intraday + market watch during trading hours (9:30-15:30 PKT = 4:30-10:30 UTC)
*/5 4-10 * * 1-5  ~/pakfindata/scripts/sync_intraday.sh

# ═══ POST-MARKET (Mon-Fri) ═══
# FX rates (SBP publishes ~16:00 PKT)
0 12 * * 1-5  ~/pakfindata/scripts/sync_fx.sh

# EOD + indices + bonds (after market close)
30 13 * * 1-5  ~/pakfindata/scripts/daily_sync.sh

# ETF NAV + dividends + IPO
0 14 * * 1-5  ~/pakfindata/scripts/sync_etf_div.sh

# PKRV + KONIA
0 13 * * 1-5  ~/pakfindata/scripts/sync_rates.sh

# Mutual fund NAV (MUFAP publishes late)
0 15 * * 1-5  ~/pakfindata/scripts/sync_mufap.sh

# ═══ WEEKLY ═══
# Company profiles deep scrape (Friday after close)
0 16 * * 5  ~/pakfindata/scripts/deep_scrape.sh

# IPO calendar update
0 15 * * 5  ~/pakfindata/scripts/sync_ipo.sh

# ═══ BI-WEEKLY ═══
# T-Bill auctions (Wednesday after auction)
0 14 * * 3  ~/pakfindata/scripts/sync_tbill_auction.sh

# ═══ MONTHLY ═══
# PIB auctions
0 14 15 * *  ~/pakfindata/scripts/sync_pib_auction.sh

# DB maintenance (vacuum + analyze)
0 3 1 * *  ~/pakfindata/scripts/maintenance.sh
```

---

## 8. WHAT THIS GIVES YOU

### Before (v2.0.0)
- Stock prices + company data
- Basic bonds/sukuk
- KIBOR + policy rates
- Mutual fund NAVs

### After (v3.0.0)
**Every investable asset class in Pakistan:**
- 540+ equities with EOD + intraday
- 5+ ETFs with NAV, basket, premium/discount tracking
- 5+ REITs with property-level data
- 300+ mutual funds + VPS pension funds
- T-Bills (1M, 3M, 6M, 12M) auction history
- PIBs (3Y to 30Y, Fixed + Floating) auction history
- Govt Ijara Sukuk auctions
- Corporate bonds + sukuk
- Full PKRV yield curve (daily)
- KONIA overnight rates
- KIBOR term rates
- SBP interbank FX (official)
- SBP open market FX
- Kerb market FX (forex dealers)
- SBP policy rate history
- IPO calendar + listing pipeline
- Dividend history for all companies
- Corporate actions + announcements

**All exposed via MCP** — Claude can query any of this with natural language:
- "What's the yield curve shape today vs 3 months ago?"
- "Show me all Shariah-compliant funds that beat KMI-30 this year"
- "Compare T-Bill rates with KIBOR — is there an arbitrage?"
- "Which stocks have the highest dividend yield in the banking sector?"
- "What happened to OGDC after the last ex-dividend date?"

**All stored locally** on `/mnt/e/psxdata/psx.sqlite` — zero subscription costs, full data ownership, offline access.
