# PSX OHLCV v3.0 — Complete Production Plan
## Pakistan Financial Data Research Platform + MCP/AI Architecture

---

# PART 1: OBJECTIVES

## 1.1 What We're Building

Transform PSX OHLCV from a stock-only tool into Pakistan's most comprehensive financial data research platform, covering every investable asset class with:

- **Date-wise persistent storage** — every data point stored with its date, queryable historically
- **Bulk operations** — sync all 540+ symbols, all funds, all rates in one command  
- **Single-symbol lookup** — user picks one symbol/fund/rate and sees full detail
- **MCP server** — Claude/AI agents can query the entire database as tools
- **Automated ops** — cron-based daily sync, zero manual intervention

## 1.2 Constraints (Your Machine)

| Constraint | Value | Impact |
|------------|-------|--------|
| OS | WSL2 Ubuntu on Windows | Cron works, but `/mnt/e/` has NTFS limitations |
| RAM | 32GB | Plenty for SQLite + async scraping |
| GPU | RTX 4080 | Not needed for data platform (useful for AI later) |
| Storage | External drive `/mnt/e/psxdata/` | All data lives here, WAL mode may need testing on NTFS |
| Database | SQLite only — no Docker/Redis/PostgreSQL | Single-file DB, WAL for concurrent reads |
| APIs | Free sources only — no paid subscriptions | PSX DPS, SBP, MUFAP, forex.pk — all free |
| Dev tool | Claude Code | All prompts designed for copy-paste into Claude Code |
| Python | Existing venv with project installed | pip install new deps with --break-system-packages |

## 1.3 Success Criteria

| # | Criterion | Measurable |
|---|-----------|------------|
| 1 | All 34 data domains have tables + scrapers + data | `python -m psx_ohlcv status` shows all green |
| 2 | Historical backfill: T-Bills since 2020, FX since 2020 | `SELECT MIN(date) FROM tbill_auctions` → 2020 |
| 3 | Bulk sync completes <10 min for all domains | Timed via cron logs |
| 4 | Single-symbol query returns <1s | `get_eod("OGDC")` response time |
| 5 | MCP server exposes 25+ tools, works in Claude Code | `mcp list-tools` shows all |
| 6 | 15 Streamlit pages (existing) + 5 new pages work | UI starts without errors |
| 7 | 350+ tests pass (existing 342 + new) | `pytest tests/ -q` all green |
| 8 | Cron runs daily unattended for 7 days | Check logs after 1 week |

---

# PART 2: MCP vs AGENTIC AI — ANALYSIS & RECOMMENDATION

## 2.1 What Is MCP (Model Context Protocol)

MCP is a **tool interface standard**. Your app exposes structured tools (functions), and any MCP-compatible AI client (Claude Desktop, Claude Code, VS Code Copilot, etc.) can call them.

```
User → Claude → "What's OGDC's price?" 
                 → Claude calls MCP tool: get_eod("OGDC")
                 → Your SQLite returns data
                 → Claude formats the answer
```

**MCP is passive** — it waits for the AI to call it. It doesn't think, plan, or decide.

**What MCP gives you:**
- Any AI can query your 50+ tables with natural language
- Zero UI development needed — Claude IS the UI
- Structured, typed tool interface (input schema, output schema)
- Works with Claude Code (your dev tool), Claude Desktop, and 3rd party clients
- Resources (schema docs, symbol lists) give AI context about your data

**What MCP does NOT give you:**
- No autonomous decision-making
- No multi-step workflows (unless the AI client does it)
- No scheduling, alerting, or monitoring
- No data processing pipeline orchestration

## 2.2 What Is Agentic AI

Agentic AI means the AI **plans, decides, and executes** multi-step workflows autonomously.

```
User → "Give me a daily morning market brief"
Agent autonomously:
  1. Checks market status (open/closed)
  2. Fetches yesterday's EOD data
  3. Fetches FX rates, T-Bill yields, KIBOR
  4. Compares sector performance
  5. Identifies unusual volume/price moves
  6. Generates narrative report
  7. Saves to file / sends notification
```

**Agentic is active** — it plans and executes without step-by-step user guidance.

**What agentic gives you:**
- Autonomous research workflows
- Multi-data-source correlation
- Proactive alerts ("OGDC dropped 5% — here's why")
- Complex analysis that requires 10+ data queries chained together
- Portfolio monitoring, trade signal generation

**What agentic costs you:**
- More complex architecture
- LLM API costs for each agent run (unless using local models)
- Error handling for autonomous decisions
- Need to define guardrails (what can the agent do without asking?)

## 2.3 Comparison Table

| Dimension | MCP Only | Agentic Only | Hybrid |
|-----------|----------|--------------|--------|
| **User interaction** | User asks, AI calls tools | Agent acts autonomously | Both modes available |
| **Complexity** | Low — just expose tools | High — planning, memory, recovery | Medium |
| **Cost** | Zero extra LLM calls | LLM call per agent step | Controlled |
| **Your use case** | "Show me OGDC data" | "Monitor my portfolio daily" | Best of both |
| **Dev effort** | 2 days | 5+ days | 3 days |
| **Maintenance** | Minimal | Need error recovery, retries | Moderate |
| **Flexibility** | High — any AI client works | Locked to your agent framework | High |

## 2.4 MY RECOMMENDATION: Hybrid (MCP-First + Lightweight Agent)

**Why hybrid:**

1. **MCP is your foundation** — expose all 25+ data tools. This alone gives you 80% of the value. Claude Code becomes a research terminal for Pakistan's entire financial market.

2. **Lightweight agent for operational tasks** — don't build a full autonomous agent framework. Instead, build **prompt chains** (pre-defined multi-tool sequences) that run inside MCP:

```python
# MCP prompt (not a full agent — a scripted multi-tool chain)
@server.prompt("daily_brief")
async def daily_brief():
    """Pre-defined chain: fetch all data, format report."""
    return """Execute these tools in order and synthesize:
    1. get_market_snapshot()          — today's market
    2. get_index_values("KSE100")    — index level
    3. get_top_gainers(10)           — winners
    4. get_top_losers(10)            — losers  
    5. get_fx_rates("USDPKR")       — currency
    6. get_latest_tbill_yields()     — risk-free rate
    7. get_kibor_rates()             — interbank rate
    
    Format as a professional morning market brief."""
```

This is **not a full agent** — it's a scripted recipe. The AI still executes it, but the plan is pre-defined. You get:
- Repeatable, predictable results
- No runaway agent costs
- Still feels like autonomous analysis to the user
- Easy to add new recipes

3. **True agentic layer later** — when you have the data platform stable and MCP working, you can add a real agent layer on top that does portfolio monitoring, alert generation, and autonomous research. But that's v4.0.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│                   USER INTERFACES                    │
│                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ Streamlit│  │ Claude   │  │ Claude Code      │  │
│  │ Dashboard│  │ Desktop  │  │ (your dev tool)  │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────────────┘  │
│       │              │             │                 │
│       │         MCP Protocol  MCP Protocol           │
│       │              │             │                 │
└───────┼──────────────┼─────────────┼─────────────────┘
        │              │             │
        ▼              ▼             ▼
┌─────────────────────────────────────────────────────┐
│              APPLICATION LAYER                       │
│                                                     │
│  ┌──────────────┐  ┌────────────────────────────┐   │
│  │ FastAPI       │  │ MCP Server                 │   │
│  │ (REST + WS)   │  │ (25+ tools, 5+ resources, │   │
│  │ 39 endpoints  │  │  10+ prompt chains)        │   │
│  └──────┬───────┘  └──────────┬─────────────────┘   │
│         │                     │                      │
│         ▼                     ▼                      │
│  ┌────────────────────────────────────────────────┐  │
│  │          Data Access Layer (DAL)               │  │
│  │  - db.py repository modules (12 existing)      │  │
│  │  - New: etf_repo, treasury_repo, fx_repo, etc. │  │
│  │  - Connection pooling, WAL mode, 64MB cache    │  │
│  └───────────────────┬────────────────────────────┘  │
│                      │                               │
└──────────────────────┼───────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│              DATA LAYER                              │
│                                                     │
│  /mnt/e/psxdata/psx.sqlite                          │
│  ├── 70+ tables (existing + new)                    │
│  ├── WAL mode for concurrent access                 │
│  ├── Indexes on all date + symbol columns           │
│  └── Backups to /mnt/e/psxdata/backups/             │
│                                                     │
│  ┌────────────────────────────────────────────────┐  │
│  │          Scraper Layer                         │  │
│  │  - AsyncPSXFetcher (equities, 540+ symbols)   │  │
│  │  - SBPScraper (rates, auctions, FX)           │  │
│  │  - MUFAPScraper (mutual funds, VPS)           │  │
│  │  - ETFScraper (ETF NAV, baskets)              │  │
│  │  - FXScraper (open market, kerb rates)        │  │
│  │  Triggered by: CLI, Cron, MCP tool            │  │
│  └────────────────────────────────────────────────┘  │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

# PART 3: DESIGN — DATA PERSISTENCE MODEL

## 3.1 Persistence Principle

**Every data point is date-stamped and immutable once stored.** This gives you:
- Full historical research capability
- Time-series analysis across any date range  
- Audit trail of when data was collected
- No data loss from re-syncs

### Persistence Patterns

| Pattern | Use Case | Table Design | Example |
|---------|----------|-------------|---------|
| **Date-keyed timeseries** | EOD prices, NAV, FX rates | `PRIMARY KEY (symbol, date)` | eod_ohlcv, etf_nav, fx_rates |
| **Timestamp-keyed snapshots** | Intraday, live quotes | `PRIMARY KEY (symbol, ts)` | intraday_bars, market_snapshots |
| **Event-keyed records** | Auctions, dividends, IPOs | `PRIMARY KEY (date, type)` | tbill_auctions, dividend_history |
| **Curve-keyed data** | Yield curves, rate terms | `PRIMARY KEY (date, tenor)` | pkrv_curve, kibor_rates |
| **Master + detail** | Funds, bonds, ETFs | Master table + history table | mutual_funds + mutual_fund_nav |

### User Query Patterns

```
BULK:     "Sync all symbols"     → Loop all 540+ symbols, upsert EOD
          "Get all fund NAVs"    → Fetch from MUFAP, upsert 300+ funds
          
SINGLE:   "Show OGDC history"   → SELECT * FROM eod_ohlcv WHERE symbol='OGDC'
          "ETF MZNPETF detail"   → JOIN etf_master + etf_nav + etf_basket
          
RANGE:    "KSE-100 last 3 months" → WHERE date >= date('now','-3 months')
          "T-Bill yields 2024"     → WHERE auction_date BETWEEN '2024-01-01' AND '2024-12-31'
          
CROSS:    "Compare KIBOR vs T-Bill" → JOIN kibor_rates ON date = tbill_auctions.auction_date
          "FX impact on KSE-100"     → Correlate fx_rates with psx_indices
```

## 3.2 Module Organization (Extending v2.0.0)

```
src/psx_ohlcv/
├── db/                        # Existing 12 modules
│   ├── eod.py                 # ✅ EXISTS
│   ├── intraday.py            # ✅ EXISTS
│   ├── company.py             # ✅ EXISTS
│   ├── bonds.py               # ✅ EXISTS
│   ├── sukuk.py               # ✅ EXISTS
│   ├── mutual_funds.py        # ✅ EXISTS
│   ├── fx.py                  # ✅ EXISTS (partial)
│   ├── indices.py             # ✅ EXISTS
│   ├── announcements.py       # ✅ EXISTS
│   ├── rates.py               # ✅ EXISTS (KIBOR, policy)
│   ├── etf.py                 # 🆕 NEW — ETF master, NAV, baskets
│   ├── treasury.py            # 🆕 NEW — T-Bills, PIBs, GIS auctions
│   ├── yield_curves.py        # 🆕 NEW — PKRV, KONIA curves
│   ├── fx_extended.py         # 🆕 NEW — SBP interbank, open market, kerb
│   ├── dividends.py           # 🆕 NEW — Dividend history
│   └── ipo.py                 # 🆕 NEW — IPO calendar
│
├── sources/                   # Scrapers
│   ├── eod.py                 # ✅ EXISTS
│   ├── async_fetcher.py       # ✅ EXISTS
│   ├── intraday.py            # ✅ EXISTS
│   ├── market_watch.py        # ✅ EXISTS
│   ├── mufap.py               # ✅ EXISTS
│   ├── sbp.py                 # ✅ EXISTS (partial)
│   ├── company.py             # ✅ EXISTS
│   ├── etf_scraper.py         # 🆕 NEW
│   ├── sbp_treasury.py        # 🆕 NEW — T-Bill + PIB auction parser
│   ├── sbp_rates.py           # 🆕 NEW — PKRV, KONIA, FX rates
│   ├── forex_scraper.py       # 🆕 NEW — forex.pk open market
│   ├── dividend_scraper.py    # 🆕 NEW
│   └── ipo_scraper.py         # 🆕 NEW
│
├── mcp/                       # 🆕 NEW — MCP Server
│   ├── __init__.py
│   ├── server.py              # Main MCP server
│   ├── tools/
│   │   ├── equity_tools.py    # get_eod, get_intraday, search_symbols
│   │   ├── fi_tools.py        # get_bonds, get_sukuk, get_yield_curve
│   │   ├── fund_tools.py      # get_mutual_funds, get_etf, get_rankings
│   │   ├── rates_tools.py     # get_fx, get_kibor, get_policy_rate
│   │   ├── market_tools.py    # get_indices, get_breadth, top_movers
│   │   ├── analytics_tools.py # screen_stocks, compare, correlate
│   │   └── system_tools.py    # get_freshness, trigger_sync
│   ├── resources/
│   │   ├── schema.py          # DB schema as context
│   │   └── symbols.py         # Symbol master list
│   └── prompts/
│       ├── daily_brief.py     # Morning market report chain
│       ├── stock_analysis.py  # Single-stock deep dive chain
│       └── portfolio.py       # Portfolio review chain
│
├── api/                       # Existing FastAPI
│   ├── main.py               # ✅ EXISTS
│   ├── routers/
│   │   ├── eod.py            # ✅ EXISTS
│   │   ├── tasks.py          # ✅ EXISTS
│   │   ├── treasury.py       # 🆕 NEW
│   │   ├── funds.py          # 🆕 NEW
│   │   ├── rates.py          # 🆕 NEW
│   │   └── market.py         # 🆕 NEW
│
├── ui/pages/                  # Existing Streamlit
│   ├── dashboard.py          # ✅ EXISTS
│   ├── ... (15 existing)     # ✅ EXISTS
│   ├── treasury_dashboard.py # 🆕 NEW — T-Bill/PIB/Yield curves
│   ├── fx_dashboard.py       # 🆕 NEW — All FX rates comparison
│   ├── fund_explorer.py      # 🆕 NEW — Mutual funds + ETFs + VPS
│   ├── data_quality.py       # 🆕 NEW — Coverage + freshness
│   └── research_terminal.py  # 🆕 NEW — Free-form SQL + charts
│
└── scripts/                   # Cron scripts
    ├── daily_sync.sh         # ✅ EXISTS (extend)
    ├── sync_treasury.sh      # 🆕 NEW
    ├── sync_rates.sh         # 🆕 NEW
    ├── sync_fx.sh            # 🆕 NEW
    ├── sync_etf.sh           # 🆕 NEW
    └── maintenance.sh        # 🆕 NEW
```

---

# PART 4: IMPLEMENTATION — Claude Code Prompts

## Phase 1: New Data Scrapers + Tables (12 prompts, ~5 days)

### Prompt 1.1 — ETF Scraper + Tables
```
I'm extending psx_ohlcv (v2.0.0, just tagged) to cover all Pakistan financial data.

TASK: Build ETF data collection.

PSX has 5 listed ETFs: MZNPETF, NBPGETF, NITGETF, UBLPETF, MIIETF
ETF detail page: https://dps.psx.com.pk/etf/{SYMBOL}

Step 1 — Create src/psx_ohlcv/db/etf.py with:

  CREATE TABLE IF NOT EXISTS etf_master (
      symbol TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      amc TEXT,
      benchmark_index TEXT,
      inception_date TEXT,
      expense_ratio REAL,
      shariah_compliant INTEGER DEFAULT 0,
      updated_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS etf_nav (
      symbol TEXT NOT NULL,
      date TEXT NOT NULL,
      nav REAL,
      market_price REAL,
      premium_discount REAL,
      aum_millions REAL,
      PRIMARY KEY (symbol, date)
  );

  CREATE INDEX IF NOT EXISTS idx_etf_nav_date ON etf_nav(date);
  CREATE INDEX IF NOT EXISTS idx_etf_nav_symbol ON etf_nav(symbol);

  Functions:
  - init_etf_schema(con)
  - upsert_etf_master(con, data: dict) -> bool
  - upsert_etf_nav(con, symbol, date, nav, market_price, aum) -> bool
  - get_etf_list(con) -> list[dict]
  - get_etf_nav_history(con, symbol, start_date=None, end_date=None) -> pd.DataFrame
  - get_etf_detail(con, symbol) -> dict  # master + latest NAV

Step 2 — Create src/psx_ohlcv/sources/etf_scraper.py:

  Scrape https://dps.psx.com.pk/etf/{SYMBOL} for each ETF.
  Extract: name, AMC, benchmark, NAV, market price, AUM, expense ratio.
  Note: ETFs also have EOD OHLCV via /timeseries/eod/{SYM} which is already scraped.
  This scraper adds ETF-specific metadata.
  
  class ETFScraper:
      ETF_SYMBOLS = ["MZNPETF", "NBPGETF", "NITGETF", "UBLPETF", "MIIETF"]
      
      def scrape_etf(self, symbol: str) -> dict:
          """Scrape single ETF detail page."""
      
      def sync_all_etfs(self, con: sqlite3.Connection) -> dict:
          """Scrape all ETFs, upsert to DB. Returns {ok: N, failed: N}"""

Step 3 — Add CLI command:
  In cli.py, add: psxsync etf sync
  And: psxsync etf list
  And: psxsync etf show MZNPETF

Step 4 — Add init_etf_schema(con) call to the main init_schema function.

Step 5 — Test:
  python -m psx_ohlcv etf sync --db /mnt/e/psxdata/psx.sqlite
  python -m psx_ohlcv etf list --db /mnt/e/psxdata/psx.sqlite
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT * FROM etf_master;"
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT * FROM etf_nav ORDER BY date DESC LIMIT 10;"

Step 6 — Write tests/test_etf.py:
  - test_init_etf_schema_creates_tables
  - test_upsert_etf_master
  - test_upsert_etf_nav
  - test_get_etf_nav_history_date_range
  - test_get_etf_detail_returns_combined
  Run: pytest tests/test_etf.py -v

Commit:
  git add -A
  git commit -m "feat: ETF data collection — master, NAV, scraper, CLI

  - db/etf.py: etf_master + etf_nav tables with CRUD
  - sources/etf_scraper.py: scrapes dps.psx.com.pk/etf/{SYM}
  - CLI: psxsync etf sync|list|show
  - 5 ETFs: MZNPETF, NBPGETF, NITGETF, UBLPETF, MIIETF
  - Tests: test_etf.py"
```

### Prompt 1.2 — T-Bill Auction Scraper
```
TASK: Build T-Bill auction data collection from SBP.

SBP publishes T-Bill auction results at:
  - https://www.sbp.org.pk/ecodata/t-bills.asp (HTML table with historical data)
  - https://www.sbp.org.pk/ecodata/auction-tbills.pdf (latest auction PDF)

Your codebase already has:
  SBP_AUCTION_URL = "https://www.sbp.org.pk/ecodata/auction-results.asp"
  SBP_TBILL_URL = "https://www.sbp.org.pk/ecodata/t-bills.asp"

Step 1 — Create src/psx_ohlcv/db/treasury.py:

  CREATE TABLE IF NOT EXISTS tbill_auctions (
      auction_date TEXT NOT NULL,
      tenor TEXT NOT NULL,
      target_amount_billions REAL,
      bids_received_billions REAL,
      amount_accepted_billions REAL,
      cutoff_yield REAL,
      cutoff_price REAL,
      weighted_avg_yield REAL,
      maturity_date TEXT,
      settlement_date TEXT,
      scraped_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (auction_date, tenor)
  );

  CREATE INDEX IF NOT EXISTS idx_tbill_date ON tbill_auctions(auction_date);
  CREATE INDEX IF NOT EXISTS idx_tbill_tenor ON tbill_auctions(tenor);

  CREATE TABLE IF NOT EXISTS pib_auctions (
      auction_date TEXT NOT NULL,
      tenor TEXT NOT NULL,
      pib_type TEXT NOT NULL DEFAULT 'Fixed',
      target_amount_billions REAL,
      bids_received_billions REAL,
      amount_accepted_billions REAL,
      cutoff_yield REAL,
      cutoff_price REAL,
      coupon_rate REAL,
      maturity_date TEXT,
      scraped_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (auction_date, tenor, pib_type)
  );

  CREATE INDEX IF NOT EXISTS idx_pib_date ON pib_auctions(auction_date);

  Functions:
  - init_treasury_schema(con)
  - upsert_tbill_auction(con, data: dict) -> bool
  - upsert_pib_auction(con, data: dict) -> bool
  - get_tbill_auctions(con, start_date=None, end_date=None, tenor=None) -> pd.DataFrame
  - get_pib_auctions(con, start_date=None, end_date=None) -> pd.DataFrame
  - get_latest_tbill_yields(con) -> dict  # latest cutoff yields for all tenors
  - get_yield_trend(con, tenor: str, n_auctions: int = 20) -> pd.DataFrame

Step 2 — Create src/psx_ohlcv/sources/sbp_treasury.py:

  class SBPTreasuryScraper:
      TBILL_URL = "https://www.sbp.org.pk/ecodata/t-bills.asp"
      
      def scrape_tbill_page(self) -> list[dict]:
          """Parse the HTML table at t-bills.asp. Returns list of auction records.
          Each row has: auction_date, tenor (3M/6M/12M), target, accepted, yield, price."""
      
      def sync_tbills(self, con: sqlite3.Connection) -> dict:
          """Scrape and upsert all T-Bill auction data."""

  Important: The SBP page has an HTML table with historical auction results.
  Parse with BeautifulSoup. Handle:
  - Multiple tenors per auction date (1M, 3M, 6M, 12M in same row or separate)
  - Rs values in billions (may have commas)
  - Yield as percentage (e.g., "10.4800")
  - Date format: varies (DD-Mon-YYYY or other SBP formats)

Step 3 — CLI:
  psxsync treasury tbill-sync  # scrape and store T-Bill auctions
  psxsync treasury tbill-list  # show recent auctions
  psxsync treasury tbill-latest  # show latest yields for all tenors

Step 4 — Test with real data:
  python -m psx_ohlcv treasury tbill-sync --db /mnt/e/psxdata/psx.sqlite
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT auction_date, tenor, cutoff_yield FROM tbill_auctions ORDER BY auction_date DESC LIMIT 20;"

Step 5 — Tests:
  tests/test_treasury.py:
  - test_parse_tbill_html (use a saved HTML fixture)
  - test_upsert_tbill_auction
  - test_get_latest_tbill_yields
  - test_get_yield_trend

Commit: "feat: T-Bill auction data from SBP"
```

### Prompt 1.3 — PIB Auction Scraper
```
TASK: Build PIB (Pakistan Investment Bond) auction data collection.

Source: https://gsp.sbp.org.pk/ — Government Securities Portal
This portal has auction results for:
  - PIB Fixed Coupon (3Y, 5Y, 10Y, 15Y, 20Y, 30Y)
  - PIB Floating Semi-Annual
  - PIB Floating Quarterly
  - PIB Floating Fortnightly Reset
  - Govt Ijara Sukuk (Fixed + Variable rental)

Step 1 — Extend src/psx_ohlcv/db/treasury.py (already created in 1.2):
  Add: upsert_pib_auction, get_pib_auctions, get_latest_pib_yields
  The pib_auctions table was already created in 1.2.

  Also add:
  CREATE TABLE IF NOT EXISTS gis_auctions (
      auction_date TEXT NOT NULL,
      gis_type TEXT NOT NULL,
      tenor TEXT,
      target_amount_billions REAL,
      amount_accepted_billions REAL,
      cutoff_rental_rate REAL,
      maturity_date TEXT,
      scraped_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (auction_date, gis_type)
  );

Step 2 — Create src/psx_ohlcv/sources/sbp_gsp.py:

  class GSPScraper:
      BASE_URL = "https://gsp.sbp.org.pk"
      
      def scrape_pib_auctions(self) -> list[dict]:
          """Scrape PIB auction results from GSP portal."""
      
      def scrape_gis_auctions(self) -> list[dict]:
          """Scrape Govt Ijara Sukuk auction results."""
      
      def sync_all(self, con) -> dict:
          """Sync PIB + GIS auctions."""

  NOTE: gsp.sbp.org.pk may require session handling or have a different
  HTML structure. First fetch the page and inspect the response.
  If the portal requires login/session, scrape from the public 
  sbp.org.pk/ecodata/ pages instead which have summary data.

Step 3 — CLI:
  psxsync treasury pib-sync
  psxsync treasury gis-sync

Step 4 — Test with real data.
Commit: "feat: PIB + Govt Ijara Sukuk auction data from SBP GSP"
```

### Prompt 1.4 — PKRV Yield Curve + KONIA Rates
```
TASK: Build daily yield curve and overnight rate data.

PKRV (Pakistan Revaluation Rate) — the daily yield curve used by banks for 
bond valuation. Published by Financial Markets Association (FMA) via SBP.

KONIA (Karachi Overnight New Index Average) — the overnight interbank rate.

Step 1 — Create src/psx_ohlcv/db/yield_curves.py:

  CREATE TABLE IF NOT EXISTS pkrv_daily (
      date TEXT NOT NULL,
      tenor_months INTEGER NOT NULL,
      yield_pct REAL NOT NULL,
      PRIMARY KEY (date, tenor_months)
  );
  -- Standard tenors: 1, 3, 6, 12, 24, 36, 60, 84, 120, 180, 240, 360

  CREATE TABLE IF NOT EXISTS konia_daily (
      date TEXT PRIMARY KEY,
      rate_pct REAL NOT NULL,
      volume_billions REAL,
      high REAL,
      low REAL
  );

  Functions:
  - get_pkrv_curve(con, date=None) -> pd.DataFrame  # if date=None, latest
  - get_pkrv_history(con, tenor_months, start_date, end_date) -> pd.DataFrame
  - get_konia_history(con, start_date=None, end_date=None) -> pd.DataFrame
  - compare_curves(con, date1, date2) -> pd.DataFrame  # side-by-side comparison

Step 2 — Create src/psx_ohlcv/sources/sbp_rates.py:

  class SBPRatesScraper:
      PKRV_URL = "https://gsp.sbp.org.pk/"  # or specific PKRV page
      KONIA_URL = "https://gsp.sbp.org.pk/"  # KONIA section
      
      def scrape_pkrv(self, date: str = None) -> list[dict]:
          """Scrape PKRV yield curve for a date. Returns [{tenor_months, yield_pct}]"""
      
      def scrape_konia(self, date: str = None) -> dict:
          """Scrape KONIA rate for a date."""

  FALLBACK: If gsp.sbp.org.pk is hard to parse, use:
  - sbp.org.pk PKRV published data
  - Or compute yield curve from T-Bill + PIB auction cutoff yields (interpolated)

Step 3 — CLI:
  psxsync rates pkrv-sync
  psxsync rates konia-sync
  psxsync rates curve --date 2026-02-07

Commit: "feat: PKRV yield curve + KONIA overnight rate"
```

### Prompt 1.5 — SBP FX Rates (Interbank + Open Market)
```
TASK: Build comprehensive FX rate collection from SBP.

Sources:
  - Interbank (WAR): https://www.sbp.org.pk/ecodata/rates/WAR/WAR-Current.asp
  - Open Market (ECAP): https://www.sbp.org.pk/ecodata/rates/ECAP/ECAP-Current.asp

These pages show daily exchange rates for major currencies vs PKR.

Step 1 — Create src/psx_ohlcv/db/fx_extended.py:

  CREATE TABLE IF NOT EXISTS sbp_fx_interbank (
      date TEXT NOT NULL,
      currency TEXT NOT NULL,
      buying REAL,
      selling REAL,
      mid REAL GENERATED ALWAYS AS ((buying + selling) / 2.0) STORED,
      PRIMARY KEY (date, currency)
  );

  CREATE TABLE IF NOT EXISTS sbp_fx_open_market (
      date TEXT NOT NULL,
      currency TEXT NOT NULL,
      buying REAL,
      selling REAL,
      PRIMARY KEY (date, currency)
  );

  CREATE TABLE IF NOT EXISTS forex_kerb (
      date TEXT NOT NULL,
      ts TEXT NOT NULL,
      currency TEXT NOT NULL,
      buying REAL,
      selling REAL,
      source TEXT DEFAULT 'forex.pk',
      PRIMARY KEY (date, currency, source, ts)
  );

  CREATE INDEX IF NOT EXISTS idx_sbp_fx_ib_date ON sbp_fx_interbank(date);
  CREATE INDEX IF NOT EXISTS idx_sbp_fx_om_date ON sbp_fx_open_market(date);
  CREATE INDEX IF NOT EXISTS idx_forex_kerb_date ON forex_kerb(date);

  Key currencies: USD, EUR, GBP, SAR, AED, JPY, CNY, CAD, AUD, CHF, KWD, QAR, OMR, BHD

  Functions:
  - get_fx_rate(con, currency, source='interbank', date=None) -> dict
  - get_fx_history(con, currency, source, start_date, end_date) -> pd.DataFrame
  - get_all_fx_latest(con, source='interbank') -> pd.DataFrame
  - get_fx_spread(con, currency, date=None) -> dict  # interbank vs open market vs kerb

Step 2 — Create src/psx_ohlcv/sources/sbp_fx.py:

  class SBPFXScraper:
      WAR_URL = "https://www.sbp.org.pk/ecodata/rates/WAR/WAR-Current.asp"
      ECAP_URL = "https://www.sbp.org.pk/ecodata/rates/ECAP/ECAP-Current.asp"
      
      def scrape_interbank(self) -> list[dict]:
      def scrape_open_market(self) -> list[dict]:
      def sync_all(self, con) -> dict:

Step 3 — Create src/psx_ohlcv/sources/forex_scraper.py:

  class ForexPKScraper:
      URL = "https://www.forex.pk/open_market_rates.asp"
      
      def scrape_open_market(self) -> list[dict]:
          """Scrape forex.pk for kerb/dealer rates."""

Step 4 — CLI:
  psxsync fx sbp-sync    # interbank + open market from SBP
  psxsync fx kerb-sync   # kerb rates from forex.pk
  psxsync fx latest      # show all latest rates from all sources
  psxsync fx spread USD  # show interbank vs open market vs kerb spread

Commit: "feat: comprehensive FX rates — SBP interbank, open market, kerb"
```

### Prompt 1.6 — Dividend History Scraper
```
TASK: Build historical dividend data extraction.

Source: dps.psx.com.pk/company/{SYMBOL} — the company page already scraped
has a "Payouts" section with dividend history.

Your existing company scraper (sources/company.py) already hits this page.
You also have a company_payouts table.

Step 1 — Check if company_payouts already has what we need:
  sqlite3 /mnt/e/psxdata/psx.sqlite ".schema company_payouts"
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT * FROM company_payouts LIMIT 5;"

If company_payouts already has dividend history → just build query functions:
  - get_dividend_history(con, symbol) -> pd.DataFrame
  - get_dividend_yield(con, symbol, years=5) -> float
  - get_ex_dividend_dates(con, symbol) -> list[str]
  - get_highest_dividend_stocks(con, n=20) -> pd.DataFrame

If company_payouts is missing dividend detail → create dividend_history table
and extend company scraper to extract payout section.

Step 2 — Add query functions to db/dividends.py (or extend db/company.py).

Step 3 — CLI:
  psxsync dividends show OGDC
  psxsync dividends top --n 20
  psxsync dividends yield OGDC --years 5

Commit: "feat: dividend history queries + CLI"
```

### Prompt 1.7 — IPO Calendar + Listing Status
```
TASK: Build IPO/listing pipeline tracker.

Source: https://dps.psx.com.pk/listings — listing status page

Step 1 — Create src/psx_ohlcv/db/ipo.py:

  CREATE TABLE IF NOT EXISTS ipo_listings (
      symbol TEXT NOT NULL,
      company_name TEXT,
      board TEXT,
      status TEXT,
      offer_price REAL,
      shares_offered INTEGER,
      subscription_open TEXT,
      subscription_close TEXT,
      listing_date TEXT,
      ipo_type TEXT,
      prospectus_url TEXT,
      updated_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (symbol, COALESCE(subscription_open, listing_date, updated_at))
  );

Step 2 — Create src/psx_ohlcv/sources/ipo_scraper.py:
  Scrape https://dps.psx.com.pk/listings
  Extract: symbol, company name, board, listing status, dates

Step 3 — CLI:
  psxsync ipo sync
  psxsync ipo list --status upcoming

Commit: "feat: IPO calendar + listing status tracker"
```

### Prompt 1.8 — VPS (Voluntary Pension) Extension
```
TASK: Extend mutual fund collection to include VPS pension funds.

MUFAP website has VPS funds under the same structure as mutual funds.
Your existing MUFAP scraper already fetches fund data.

Step 1 — Check if VPS funds are already captured:
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT DISTINCT fund_type FROM mutual_funds;"
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM mutual_funds WHERE fund_type LIKE '%pension%' OR fund_type LIKE '%VPS%';"

If already there → just add query functions for VPS filtering.
If missing → extend MUFAP scraper to hit the VPS tab:
  https://www.mufap.com.pk/Industry/IndustryStatDaily (check if VPS funds appear)
  https://old.mufap.com.pk/nav-report.php (has VPS section)

Step 2 — Add functions:
  - get_vps_funds(con) -> pd.DataFrame
  - get_vps_nav_history(con, fund_id, start_date, end_date) -> pd.DataFrame
  - compare_vps_allocations(con) -> pd.DataFrame  # equity vs debt vs money market sub-funds

Commit: "feat: VPS pension fund data via MUFAP"
```

### Prompt 1.9 — Integrate All New Schemas + Master Init
```
TASK: Wire all new schemas into the master init_schema.

Step 1 — In db/__init__.py or wherever init_schema lives:
  Import and call all new init functions:
    init_etf_schema(con)
    init_treasury_schema(con)
    init_yield_curve_schema(con)
    init_fx_extended_schema(con)
    init_ipo_schema(con)

Step 2 — Verify all tables exist:
  python -c "
  from psx_ohlcv.db import connect, init_schema
  con = connect('/mnt/e/psxdata/psx.sqlite')
  init_schema(con)
  tables = con.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()
  print(f'Total tables: {len(tables)}')
  for t in tables:
      count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
      print(f'  {t[0]}: {count} rows')
  "

Step 3 — Run ALL new scrapers:
  python -m psx_ohlcv etf sync
  python -m psx_ohlcv treasury tbill-sync
  python -m psx_ohlcv treasury pib-sync
  python -m psx_ohlcv rates pkrv-sync
  python -m psx_ohlcv rates konia-sync
  python -m psx_ohlcv fx sbp-sync
  python -m psx_ohlcv fx kerb-sync
  python -m psx_ohlcv ipo sync

Step 4 — Run ALL tests:
  pytest tests/ -x -q --tb=short

Commit: "feat: master schema integration — all new tables initialized"
```

### Prompt 1.10 — Unified Sync Command + Status Dashboard
```
TASK: Create a unified sync command and data status report.

Step 1 — Add unified sync:
  psxsync sync-all
  
  This runs ALL data scrapers in order:
  1. Symbols refresh (if needed)
  2. EOD OHLCV (async, all symbols)
  3. ETF NAV + metadata
  4. Market watch snapshot
  5. SBP FX rates (interbank + open market)
  6. Kerb FX rates
  7. KIBOR rates
  8. T-Bill auctions (check if new auction since last sync)
  9. PIB auctions (check if new)
  10. PKRV curve
  11. KONIA rate
  12. Mutual fund NAVs (MUFAP)
  13. Company announcements
  14. IPO listing status

  Each step: try/except, log result, continue on failure.
  Print summary at end: X of 14 sync steps succeeded.

Step 2 — Add status command:
  psxsync status
  
  Output:
  ┌──────────────────────────┬──────────┬───────────────┬────────────┐
  │ Data Domain              │ Rows     │ Latest Date   │ Status     │
  ├──────────────────────────┼──────────┼───────────────┼────────────┤
  │ EOD OHLCV                │ 1,234,567│ 2026-02-07    │ ✅ Fresh   │
  │ Intraday Bars            │   89,012 │ 2026-02-07    │ ✅ Fresh   │
  │ ETF NAV                  │      125 │ 2026-02-07    │ ✅ Fresh   │
  │ T-Bill Auctions          │      240 │ 2026-02-05    │ ✅ Fresh   │
  │ PIB Auctions             │       48 │ 2026-01-15    │ ⚠️ 23 days │
  │ PKRV Curve               │    3,600 │ 2026-02-07    │ ✅ Fresh   │
  │ KONIA                    │      730 │ 2026-02-07    │ ✅ Fresh   │
  │ SBP FX Interbank         │   10,950 │ 2026-02-07    │ ✅ Fresh   │
  │ SBP FX Open Market       │   10,950 │ 2026-02-07    │ ✅ Fresh   │
  │ Kerb FX                  │    7,300 │ 2026-02-07    │ ✅ Fresh   │
  │ KIBOR                    │      730 │ 2026-02-07    │ ✅ Fresh   │
  │ SBP Policy Rate          │       24 │ 2025-12-16    │ ✅ OK      │
  │ Mutual Funds             │      300 │ 2026-02-06    │ ⚠️ 1 day   │
  │ Bonds                    │       45 │ 2026-02-07    │ ✅ Fresh   │
  │ Sukuk                    │       12 │ 2026-02-07    │ ✅ Fresh   │
  │ IPO Calendar             │        8 │ 2026-02-01    │ ✅ OK      │
  │ Company Profiles         │      540 │ 2026-01-31    │ ⚠️ Weekly  │
  │ Corporate Announcements  │    2,500 │ 2026-02-06    │ ✅ Fresh   │
  └──────────────────────────┴──────────┴───────────────┴────────────┘
  
  DB Size: 245 MB | WAL Size: 12 MB | Tables: 72 | Indexes: 94

Commit: "feat: unified sync-all command + data status dashboard"
```

---

## Phase 2: MCP Server (8 prompts, ~2 days)

### Prompt 2.1 — MCP Server Scaffold + Equity Tools
```
TASK: Build the PSX OHLCV MCP server.

This server exposes your entire SQLite database as AI-callable tools.
It uses the official MCP Python SDK.

Step 1 — Install MCP SDK:
  pip install mcp --break-system-packages

Step 2 — Create src/psx_ohlcv/mcp/__init__.py (empty)

Step 3 — Create src/psx_ohlcv/mcp/server.py:

  from mcp.server import Server
  from mcp.server.stdio import stdio_server
  import mcp.types as types
  import json
  import os
  import sqlite3

  # The server
  server = Server("psx-ohlcv")
  
  DB_PATH = os.environ.get("PSX_DB_PATH", "/mnt/e/psxdata/psx.sqlite")
  
  def get_db():
      con = sqlite3.connect(DB_PATH)
      con.row_factory = sqlite3.Row
      return con

  # ─── EQUITY TOOLS ─────────────────────────

  @server.list_tools()
  async def list_tools() -> list[types.Tool]:
      return [
          types.Tool(
              name="get_eod",
              description="Get EOD OHLCV price data for a PSX stock symbol. Returns date, open, high, low, close, volume.",
              inputSchema={
                  "type": "object",
                  "properties": {
                      "symbol": {"type": "string", "description": "PSX stock symbol (e.g., OGDC, HBL, MCB)"},
                      "start_date": {"type": "string", "description": "Start date YYYY-MM-DD (optional)"},
                      "end_date": {"type": "string", "description": "End date YYYY-MM-DD (optional)"},
                      "limit": {"type": "integer", "description": "Max rows (default 100)", "default": 100}
                  },
                  "required": ["symbol"]
              }
          ),
          types.Tool(
              name="search_symbols",
              description="Search PSX symbols by name or code. Returns matching symbols with sector and status.",
              inputSchema={
                  "type": "object",
                  "properties": {
                      "query": {"type": "string", "description": "Search term (symbol code or company name)"},
                      "sector": {"type": "string", "description": "Filter by sector (optional)"},
                      "active_only": {"type": "boolean", "default": True}
                  },
                  "required": ["query"]
              }
          ),
          types.Tool(
              name="get_company_profile",
              description="Get company profile including sector, market cap, P/E, EPS, dividend yield, etc.",
              inputSchema={
                  "type": "object", 
                  "properties": {
                      "symbol": {"type": "string"}
                  },
                  "required": ["symbol"]
              }
          ),
          types.Tool(
              name="get_market_snapshot",
              description="Get current market snapshot: all actively trading symbols with latest price, change, volume.",
              inputSchema={"type": "object", "properties": {}}
          ),
          types.Tool(
              name="get_top_movers",
              description="Get top gainers and losers by price change percentage.",
              inputSchema={
                  "type": "object",
                  "properties": {
                      "n": {"type": "integer", "default": 10, "description": "Number of stocks per list"},
                      "direction": {"type": "string", "enum": ["gainers", "losers", "both"], "default": "both"}
                  }
              }
          ),
          # ... more tools added in subsequent prompts
      ]

  @server.call_tool()
  async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
      con = get_db()
      try:
          if name == "get_eod":
              return _handle_get_eod(con, arguments)
          elif name == "search_symbols":
              return _handle_search_symbols(con, arguments)
          elif name == "get_company_profile":
              return _handle_get_company_profile(con, arguments)
          elif name == "get_market_snapshot":
              return _handle_get_market_snapshot(con, arguments)
          elif name == "get_top_movers":
              return _handle_get_top_movers(con, arguments)
          else:
              return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
      finally:
          con.close()

  def _handle_get_eod(con, args):
      symbol = args["symbol"].upper()
      limit = args.get("limit", 100)
      query = "SELECT date, open, high, low, close, volume FROM eod_ohlcv WHERE symbol = ?"
      params = [symbol]
      if args.get("start_date"):
          query += " AND date >= ?"
          params.append(args["start_date"])
      if args.get("end_date"):
          query += " AND date <= ?"
          params.append(args["end_date"])
      query += f" ORDER BY date DESC LIMIT {limit}"
      rows = [dict(r) for r in con.execute(query, params).fetchall()]
      return [types.TextContent(type="text", text=json.dumps(rows, indent=2))]

  # ... implement other handlers similarly

  # ─── ENTRY POINT ──────────────────────────

  async def main():
      async with stdio_server() as (read, write):
          await server.run(read, write, server.create_initialization_options())

  if __name__ == "__main__":
      import asyncio
      asyncio.run(main())

Step 4 — Create src/psx_ohlcv/mcp/__main__.py:
  from .server import main
  import asyncio
  asyncio.run(main())

Step 5 — Test locally:
  # Test that server starts
  echo '{"jsonrpc": "2.0", "method": "tools/list", "id": 1}' | \
    PSX_DB_PATH=/mnt/e/psxdata/psx.sqlite python -m psx_ohlcv.mcp

Step 6 — Create Claude Code config:
  Create ~/psx_ohlcv/.claude/config.toml:
  
  [mcp_servers.psx-ohlcv]
  command = "python"
  args = ["-m", "psx_ohlcv.mcp"]
  
  [mcp_servers.psx-ohlcv.env]
  PSX_DB_PATH = "/mnt/e/psxdata/psx.sqlite"

Commit: "feat: MCP server — equity tools (get_eod, search, profile, snapshot, movers)"
```

### Prompt 2.2 — Fixed Income MCP Tools
```
TASK: Add fixed income tools to the MCP server.

Add these tools to server.py:

1. get_bonds(filters) — List corporate bonds with latest quotes
2. get_sukuk(filters) — List sukuk with latest data
3. get_yield_curve(curve_type, date) — Get PKRV, T-Bill, or PIB yield curve
4. get_tbill_auctions(start_date, end_date, tenor) — T-Bill auction history
5. get_pib_auctions(start_date, end_date) — PIB auction history
6. get_latest_yields() — Latest yields across all fixed income instruments

Each tool:
  - Add to list_tools() with proper inputSchema
  - Add handler in call_tool()
  - Query from the appropriate tables
  - Return JSON

Test each tool works:
  echo '{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{"name":"get_yield_curve","arguments":{"curve_type":"pkrv"}}}' | \
    PSX_DB_PATH=/mnt/e/psxdata/psx.sqlite python -m psx_ohlcv.mcp

Commit: "feat: MCP fixed income tools — bonds, sukuk, yield curves, auctions"
```

### Prompt 2.3 — Fund + FX + Rates MCP Tools
```
TASK: Add fund and FX tools to MCP server.

Fund tools:
1. get_mutual_funds(category, shariah, amc) — Fund list with latest NAV
2. get_fund_nav_history(fund_id, start, end) — NAV time series
3. get_fund_rankings(period, category) — Top performers
4. get_etf_data(symbol) — ETF detail + NAV + premium/discount
5. get_etf_list() — All listed ETFs

FX + Rates tools:
6. get_fx_rates(currency, source) — Latest FX rate from interbank/open market/kerb
7. get_fx_history(currency, source, start, end) — Historical FX
8. get_fx_spread(currency) — Compare all sources
9. get_kibor(date) — KIBOR rates for all tenors
10. get_policy_rate() — Current SBP policy rate + history
11. get_konia(start, end) — KONIA overnight rate history

Commit: "feat: MCP fund + FX + rates tools"
```

### Prompt 2.4 — Analytics + System MCP Tools
```
TASK: Add analytics and system tools to MCP server.

Analytics tools:
1. screen_stocks(min_market_cap, max_pe, min_div_yield, sector, shariah)
   — Stock screener with multiple filters
2. compare_securities(symbols: list) — Side-by-side comparison
3. calculate_returns(symbol, period) — 1D, 1W, 1M, 3M, 6M, 1Y, YTD returns
4. get_sector_performance() — Sector-wise returns and rotation
5. get_correlation(symbol1, symbol2, period) — Price correlation

System tools:
6. get_data_freshness() — Status of all data domains (same as psxsync status)
7. get_coverage_summary() — How many symbols, funds, etc.
8. trigger_sync(data_type) — On-demand sync (equities|fx|funds|treasury|all)
9. run_sql(query) — Execute read-only SQL query against the database
   IMPORTANT: Only SELECT queries allowed. Reject INSERT/UPDATE/DELETE/DROP.

Commit: "feat: MCP analytics + system tools"
```

### Prompt 2.5 — MCP Resources + Prompts
```
TASK: Add resources and prompt templates to MCP server.

Resources (static context for AI):
1. psx://schema — Complete database schema (all CREATE TABLE statements)
2. psx://symbols — Full symbol list with name, sector, status
3. psx://data-dictionary — Column descriptions for key tables
4. psx://trading-calendar — PSX trading days, holidays

  @server.list_resources()
  async def list_resources() -> list[types.Resource]:
      return [
          types.Resource(uri="psx://schema", name="Database Schema", ...),
          types.Resource(uri="psx://symbols", name="Symbol Master List", ...),
      ]
  
  @server.read_resource()
  async def read_resource(uri: str) -> str:
      if uri == "psx://schema":
          # Read schema from DB
          con = get_db()
          tables = con.execute("SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
          return "\n\n".join(row[0] for row in tables if row[0])

Prompts (pre-defined analysis chains):
1. daily_market_brief — Fetches all key data, formats morning report
2. stock_deep_dive — Comprehensive single-stock analysis
3. portfolio_review — Multi-stock portfolio analysis
4. sector_rotation — Cross-sector comparison
5. yield_curve_analysis — Fixed income landscape
6. fx_outlook — Currency market overview

  @server.list_prompts()
  async def list_prompts() -> list[types.Prompt]:
      return [
          types.Prompt(
              name="daily_market_brief",
              description="Generate a comprehensive morning market brief for Pakistan's financial markets",
              arguments=[]
          ),
          ...
      ]

  @server.get_prompt()
  async def get_prompt(name: str, arguments: dict) -> types.GetPromptResult:
      if name == "daily_market_brief":
          return types.GetPromptResult(
              messages=[types.PromptMessage(
                  role="user",
                  content=types.TextContent(type="text", text="""
                      Execute these tools and synthesize into a professional market brief:
                      
                      1. get_market_snapshot() — Get today's market data
                      2. get_top_movers(n=10, direction="both") — Top gainers and losers
                      3. get_sector_performance() — Sector rotation
                      4. get_fx_rates("USD") — USD/PKR rate
                      5. get_latest_yields() — Treasury yields
                      6. get_kibor() — Interbank rates
                      
                      Format the output as a professional market brief with:
                      - Market headline (1 line)
                      - Index performance (KSE-100, KSE-30, KMI-30)
                      - Top movers table
                      - Sector highlights
                      - Fixed income snapshot
                      - Currency update
                      - Key events/announcements
                  """)
              )]
          )

Commit: "feat: MCP resources (schema, symbols) + prompt templates (5 analysis chains)"
```

### Prompt 2.6 — MCP Integration Test
```
TASK: Full integration test of MCP server with Claude Code.

Step 1 — Verify MCP server starts:
  PSX_DB_PATH=/mnt/e/psxdata/psx.sqlite python -m psx_ohlcv.mcp &
  # Should not crash

Step 2 — List all tools:
  echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | \
    PSX_DB_PATH=/mnt/e/psxdata/psx.sqlite python -m psx_ohlcv.mcp
  
  # Should show 25+ tools

Step 3 — Test each tool category:
  # Equity
  echo '{"jsonrpc":"2.0","method":"tools/call","id":1,"params":{"name":"get_eod","arguments":{"symbol":"OGDC","limit":5}}}' | python -m psx_ohlcv.mcp
  
  # Fixed Income
  echo '{"jsonrpc":"2.0","method":"tools/call","id":2,"params":{"name":"get_latest_yields","arguments":{}}}' | python -m psx_ohlcv.mcp
  
  # Funds
  echo '{"jsonrpc":"2.0","method":"tools/call","id":3,"params":{"name":"get_mutual_funds","arguments":{"category":"Equity"}}}' | python -m psx_ohlcv.mcp
  
  # FX
  echo '{"jsonrpc":"2.0","method":"tools/call","id":4,"params":{"name":"get_fx_rates","arguments":{"currency":"USD"}}}' | python -m psx_ohlcv.mcp
  
  # Analytics
  echo '{"jsonrpc":"2.0","method":"tools/call","id":5,"params":{"name":"screen_stocks","arguments":{"min_dividend_yield":5}}}' | python -m psx_ohlcv.mcp
  
  # System
  echo '{"jsonrpc":"2.0","method":"tools/call","id":6,"params":{"name":"get_data_freshness","arguments":{}}}' | python -m psx_ohlcv.mcp

Step 4 — Claude Code integration:
  Ensure ~/psx_ohlcv/.claude/config.toml has the MCP server configured.
  Start Claude Code in the project directory.
  Ask: "What tools do you have for PSX data?"
  Ask: "Show me OGDC's last 10 trading days"
  Ask: "What's the current yield curve?"
  Ask: "Run the daily market brief"

Step 5 — Write tests/test_mcp.py:
  - test_server_starts
  - test_list_tools_returns_all
  - test_get_eod_valid_symbol
  - test_get_eod_invalid_symbol_returns_empty
  - test_search_symbols
  - test_screen_stocks_filters
  - test_run_sql_rejects_write_queries

  pytest tests/test_mcp.py -v

Commit: "test: MCP server integration tests — all tools verified"
```

---

## Phase 3: New UI Pages + API Routes (5 prompts, ~2 days)

### Prompt 3.1 — Treasury Dashboard (Streamlit)
```
TASK: Create a Treasury Market dashboard page.

Create src/psx_ohlcv/ui/pages/treasury_dashboard.py

Sections:
1. YIELD CURVE CHART (plotly)
   - Current PKRV curve (line chart, tenor on x-axis, yield on y-axis)
   - Overlay: curve from 1 month ago, 3 months ago
   - User can pick custom comparison dates

2. T-BILL AUCTION TABLE
   - Latest 10 auctions with tenor, yield, amount accepted
   - Yield trend chart for each tenor (3M, 6M, 12M over time)

3. PIB AUCTION TABLE
   - Latest auctions with tenor, coupon, yield

4. RATE COMPARISON
   - st.metric: Policy Rate, KIBOR 3M, T-Bill 3M, KONIA
   - Shows change from previous value

5. RATE HISTORY CHART
   - Multi-line chart: Policy Rate vs KIBOR vs T-Bill 3M vs KONIA over 2 years

Add to app.py navigation menu.

Commit: "feat: Treasury Market dashboard page"
```

### Prompt 3.2 — FX Dashboard
```
TASK: Create FX rates comparison dashboard.

Create src/psx_ohlcv/ui/pages/fx_dashboard.py

Sections:
1. RATE CARDS — For USD, EUR, GBP, SAR, AED:
   - SBP Interbank: buying/selling
   - SBP Open Market: buying/selling  
   - Kerb Rate: buying/selling
   - Spread indicator (kerb premium over interbank)

2. HISTORY CHART — USD/PKR from all 3 sources over time (plotly line chart)
   - User selects currency and date range

3. ALL CURRENCIES TABLE
   - Current rates for all 14 currencies from interbank
   - Sortable by buying rate

4. SPREAD ANALYSIS
   - Bar chart showing interbank vs kerb spread for each currency

Commit: "feat: FX rates comparison dashboard"
```

### Prompt 3.3 — Fund Explorer
```
TASK: Create mutual fund + ETF explorer page.

Create src/psx_ohlcv/ui/pages/fund_explorer.py

Sections:
1. FUND FILTER SIDEBAR
   - Category: Equity, Income, Money Market, Islamic, Balanced, etc.
   - AMC: dropdown of all AMCs
   - Shariah: Yes/No/All
   - Type: Open-end / ETF / VPS

2. FUND TABLE
   - Name, AMC, Category, Latest NAV, 1M Return, 3M Return, YTD, AUM
   - Sortable by any column
   - Click a fund → detail view

3. FUND DETAIL VIEW (expandable)
   - NAV history chart (plotly)
   - Return comparison vs benchmark
   - Risk metrics (volatility, Sharpe approximation)

4. ETF SECTION
   - All 5 ETFs with NAV vs market price
   - Premium/discount indicator
   - iNAV tracking chart

5. TOP PERFORMERS
   - Best/worst funds by 1M, 3M, YTD return
   - Category-wise rankings

Commit: "feat: mutual fund + ETF explorer page"
```

### Prompt 3.4 — Data Quality Dashboard
```
TASK: Create data quality monitoring page.

Create src/psx_ohlcv/ui/pages/data_quality.py

This reuses the same logic as psxsync status but in a visual dashboard.

Sections:
1. FRESHNESS TABLE — all 18 data domains with:
   - Latest date, rows, days old, status badge (✅⚠️🔴)

2. COVERAGE HEATMAP — calendar view showing:
   - Green = data exists for that day
   - Red = missing (expected trading day but no data)
   - User selects: EOD, Intraday, FX, etc.

3. DB STATS
   - Total DB size, WAL size, table count, index count
   - Top 10 largest tables by row count

4. QUICK ACTIONS
   - Button: Sync All (calls psxsync sync-all)
   - Button: Vacuum DB
   - Button: Backup DB
   - Button: Run Analyze

Commit: "feat: data quality dashboard page"
```

### Prompt 3.5 — Research Terminal
```
TASK: Create a SQL research terminal page.

Create src/psx_ohlcv/ui/pages/research_terminal.py

This is for power users who want to run custom SQL queries.

Sections:
1. SQL EDITOR
   - st.text_area for SQL input
   - Syntax highlighting (via streamlit-ace or similar)
   - Only SELECT queries allowed (validate before executing)

2. RESULTS TABLE
   - Display query results as sortable dataframe
   - Download as CSV button

3. SAVED QUERIES
   - Pre-built query templates:
     "T-Bill yield spread (3M vs 12M)" 
     "FX interbank vs kerb premium"
     "Top dividend yield stocks"
     "Sector PE comparison"
     "Monthly KSE-100 returns"
     "Fund NAV correlation with KSE-100"
   - User clicks → query loads in editor

4. SCHEMA BROWSER
   - Sidebar: list all tables
   - Click table → shows columns and types
   - Shows row count

Commit: "feat: SQL research terminal page"
```

---

## Phase 4: Automation + Testing + Deploy (5 prompts, ~1 day)

### Prompt 4.1 — Cron Setup for All Data Domains
```
TASK: Set up comprehensive cron scheduling.

Create all sync scripts in ~/psx_ohlcv/scripts/:

scripts/sync_all.sh — Master sync (calls everything)
scripts/sync_eod.sh — EOD + indices only
scripts/sync_rates.sh — KIBOR, KONIA, PKRV, policy rate
scripts/sync_fx.sh — SBP FX + kerb FX
scripts/sync_treasury.sh — T-Bill + PIB auctions
scripts/sync_etf.sh — ETF data
scripts/sync_mufap.sh — Mutual funds + VPS
scripts/maintenance.sh — Vacuum + analyze + backup

Each script:
  - Sets correct working directory
  - Logs to /mnt/e/psxdata/logs/
  - Uses error handling (set -e with trap)
  - Records start/end time

Crontab (all times in UTC, PKT = UTC+5):
  # Daily Mon-Fri
  30 12 * * 1-5 ~/psx_ohlcv/scripts/sync_rates.sh    # 17:30 PKT
  0 13 * * 1-5  ~/psx_ohlcv/scripts/sync_fx.sh        # 18:00 PKT
  30 13 * * 1-5 ~/psx_ohlcv/scripts/sync_eod.sh       # 18:30 PKT
  0 14 * * 1-5  ~/psx_ohlcv/scripts/sync_etf.sh       # 19:00 PKT
  0 15 * * 1-5  ~/psx_ohlcv/scripts/sync_mufap.sh     # 20:00 PKT
  
  # Weekly (Friday)
  0 16 * * 5    ~/psx_ohlcv/scripts/sync_treasury.sh  # 21:00 PKT Fri
  
  # Monthly (1st)
  0 22 1 * *    ~/psx_ohlcv/scripts/maintenance.sh     # 03:00 PKT

Install crontab:
  crontab -e
  (paste the schedule)

Verify:
  crontab -l | grep psx

Test manually:
  bash scripts/sync_all.sh
  cat /mnt/e/psxdata/logs/sync_all_$(date +%Y%m%d).log

Commit: "ops: comprehensive cron scheduling for all data domains"
```

### Prompt 4.2 — Test Suite for All New Modules
```
TASK: Write tests for all new data modules.

Create tests for:
  tests/test_etf.py — ETF schema, CRUD, scraper parse
  tests/test_treasury.py — T-Bill/PIB schema, CRUD, HTML parse
  tests/test_yield_curves.py — PKRV, KONIA schema and queries
  tests/test_fx_extended.py — SBP FX, kerb FX schema and queries
  tests/test_ipo.py — IPO schema and CRUD
  tests/test_mcp.py — MCP server tool dispatch

Each test file should:
  - Use in-memory SQLite (":memory:")
  - Test schema creation
  - Test upsert with sample data
  - Test query with date ranges
  - Test edge cases (empty results, duplicate dates)

Run full suite:
  pytest tests/ -x -q --tb=short
  # Must show: all passed

Commit: "test: comprehensive test suite for all new modules"
```

### Prompt 4.3 — FastAPI Routes for New Data
```
TASK: Add FastAPI routes for new data domains.

Create src/psx_ohlcv/api/routers/treasury.py:
  GET /api/treasury/tbills — T-Bill auctions with filters
  GET /api/treasury/pibs — PIB auctions
  GET /api/treasury/yields — Latest yields across all instruments
  GET /api/treasury/curve/{type} — Yield curve (pkrv, tbill, pib)

Create src/psx_ohlcv/api/routers/funds.py:
  GET /api/funds/mutual — Mutual funds list with filters
  GET /api/funds/mutual/{fund_id}/nav — NAV history
  GET /api/funds/etf — ETF list
  GET /api/funds/etf/{symbol} — ETF detail

Create src/psx_ohlcv/api/routers/rates.py:
  GET /api/rates/fx — FX rates (params: currency, source)
  GET /api/rates/fx/history — FX history
  GET /api/rates/kibor — KIBOR
  GET /api/rates/konia — KONIA
  GET /api/rates/policy — Policy rate

Create src/psx_ohlcv/api/routers/market.py:
  GET /api/market/snapshot — Full market snapshot
  GET /api/market/indices — All indices
  GET /api/market/movers — Top gainers/losers
  GET /api/market/sectors — Sector summary

Register all in api/main.py:
  app.include_router(treasury.router, prefix="/api/treasury", tags=["Treasury"])
  app.include_router(funds.router, prefix="/api/funds", tags=["Funds"])
  app.include_router(rates.router, prefix="/api/rates", tags=["Rates"])
  app.include_router(market.router, prefix="/api/market", tags=["Market"])

Verify:
  uvicorn psx_ohlcv.api.main:app --port 8000 &
  curl http://localhost:8000/api/treasury/yields | python -m json.tool
  curl http://localhost:8000/api/rates/fx?currency=USD | python -m json.tool
  curl http://localhost:8000/docs  # Swagger should show all new routes
  kill %1

Commit: "feat: FastAPI routes for treasury, funds, rates, market"
```

### Prompt 4.4 — Final Integration + Tag v3.0.0
```
TASK: Final verification and release.

Step 1 — Run ALL tests:
  pytest tests/ -x -q --tb=short
  # Must be ALL passed

Step 2 — Run status check:
  python -m psx_ohlcv status --db /mnt/e/psxdata/psx.sqlite
  # All data domains should show data

Step 3 — Start Streamlit, verify all pages load:
  streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -10

Step 4 — Start FastAPI, verify docs:
  uvicorn psx_ohlcv.api.main:app --port 8000 &
  curl -s http://localhost:8000/docs | head -5
  kill %1

Step 5 — MCP server, verify tools:
  echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | \
    PSX_DB_PATH=/mnt/e/psxdata/psx.sqlite python -m psx_ohlcv.mcp 2>/dev/null | \
    python -c "import sys,json; d=json.load(sys.stdin); print(f'Tools: {len(d[\"result\"][\"tools\"])}')"

Step 6 — DB stats:
  sqlite3 /mnt/e/psxdata/psx.sqlite "
    SELECT 
      (SELECT COUNT(*) FROM sqlite_master WHERE type='table') as tables,
      (SELECT COUNT(*) FROM sqlite_master WHERE type='index') as indexes;
  "
  ls -lh /mnt/e/psxdata/psx.sqlite

Step 7 — Tag:
  git log --oneline HEAD~20..HEAD
  
  git tag -a v3.0.0 -m "v3.0.0: Pakistan Financial Data Research Platform

  DATA COVERAGE:
  - 540+ equities (EOD + intraday)
  - 5 ETFs (NAV, basket, premium/discount)
  - 300+ mutual funds (NAV history, rankings)
  - T-Bill auctions (1M/3M/6M/12M yields)
  - PIB auctions (3Y-30Y, fixed + floating)
  - Govt Ijara Sukuk auctions
  - PKRV yield curve (daily)
  - KONIA overnight rate
  - KIBOR term rates
  - SBP policy rate history
  - SBP FX interbank + open market
  - Kerb market FX rates (forex dealers)
  - Corporate bonds + sukuk
  - IPO calendar + listing status
  - Dividend history

  ARCHITECTURE:
  - MCP server with 25+ tools for AI integration
  - FastAPI with 50+ REST endpoints
  - Streamlit with 20 pages
  - Automated cron for all data domains
  - SQLite on /mnt/e/psxdata/ — zero infrastructure"
  
  git push origin dev --tags

DONE.
```

---

# PART 5: POST-DEPLOY — What Comes After v3.0.0

| Track | Feature | When |
|-------|---------|------|
| **v3.1** | Historical backfill — T-Bills since 2020, PKRV since 2020, FX since 2020 | Week after v3.0 |
| **v3.2** | Agentic layer — autonomous portfolio monitoring + alerts | 2 weeks after |
| **v3.3** | AI-powered daily morning brief (auto-generated PDF) | 3 weeks after |
| **v4.0** | Web deployment — FastAPI + React frontend on VPS/cloud | 1 month after |
| **v4.1** | Mobile app or PWA | 2 months after |
| **v4.2** | Multi-user auth + portfolio tracking | 3 months after |

---

# QUICK REFERENCE — Execution Order

```
PHASE 1: Data Scrapers (5 days, prompts 1.1–1.10)
  Day 1: ETF (1.1) + T-Bill (1.2)
  Day 2: PIB (1.3) + PKRV/KONIA (1.4)
  Day 3: FX rates (1.5) + Dividends (1.6)
  Day 4: IPO (1.7) + VPS (1.8) + Master init (1.9)
  Day 5: Unified sync + status (1.10)

PHASE 2: MCP Server (2 days, prompts 2.1–2.6)
  Day 6: Server scaffold + equity tools (2.1) + FI tools (2.2)
  Day 7: Fund/FX tools (2.3) + Analytics (2.4) + Resources (2.5) + Test (2.6)

PHASE 3: UI + API (2 days, prompts 3.1–3.5)
  Day 8: Treasury dashboard (3.1) + FX dashboard (3.2)
  Day 9: Fund explorer (3.3) + Data quality (3.4) + Research terminal (3.5)

PHASE 4: Automation + Deploy (1 day, prompts 4.1–4.4)
  Day 10: Cron (4.1) + Tests (4.2) + API routes (4.3) + Tag v3.0.0 (4.4)

TOTAL: 10 days × 3-4 hours per day = 30-40 Claude Code hours
```
