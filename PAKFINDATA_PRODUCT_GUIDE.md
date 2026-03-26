# pakfindata — Product Guide & Business Overview

## Bloomberg Terminal-Style Analytics for Pakistan Stock Exchange

**Built by Godaitec (godai.tech) | Karachi, Pakistan**

---

## What is pakfindata?

pakfindata is a **quantitative analytics terminal** for the Pakistan Stock Exchange (PSX). It provides institutional-grade market intelligence, tick-level microstructure analysis, and AI-powered signal scoring — capabilities previously available only through Bloomberg Terminal (at $24,000/year) or Reuters Eikon.

**Think of it as:** Bloomberg Terminal meets Pakistan — built for local markets, local data sources, and local regulatory context.

---

## Target Audience

| Audience | Value Proposition |
|----------|-------------------|
| **Brokerage firms** | Real-time market monitoring, client reporting, trade execution insights |
| **Fund managers** | Signal scoring, portfolio analytics, sector rotation intelligence |
| **Proprietary traders** | Tick-level microstructure, VPIN toxicity, volume profiles |
| **Research analysts** | Multi-factor screening, fundamental + technical synthesis |
| **Banking ALM desks** | Yield curves, PIB auctions, T-bill rates, KIBOR monitoring |
| **Corporate treasurers** | FX rates, interbank monitoring, debt market intelligence |
| **Regulators (SECP/SBP)** | Market surveillance, breadth analytics, anomaly detection |
| **Fintech startups** | API-ready data layer for building trading apps |

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Frontend | Streamlit (Python) | Interactive dark-themed Bloomberg UI |
| Primary DB | DuckDB | Fast analytics (104x faster than SQLite) |
| Reference DB | SQLite | Configs, reference data |
| Tick Storage | JSONL files | Raw tick data from cloud |
| Cloud | Oracle Cloud (Jeddah) | Real-time tick collection 24/5 |
| Charts | Plotly + TradingView Lightweight Charts | Professional financial charts |
| AI | OpenAI GPT-4o / Ollama | Market commentary engine |
| Theme | Dark (#0B0E11), Gold (#C8A96E), JetBrains Mono | Professional terminal aesthetic |

---

## Pages — Complete Feature Guide

### SECTION 1: MARKET OVERVIEW

---

### 1. Market Dashboard
**URL:** `/` (home page)

**What it shows:**
- KSE-100 index value with daily change (e.g., 153,966.36 ▲ +1,225.99)
- High/Low/Volume/YTD performance bar
- Macro ticker: SBP rate (10.5%), KIBOR 3M (11.23%), T-Bill (11.50%), PKRV 10Y (11.24%), USD/PKR (279.36)
- Data freshness badges: Equity EOD, Intraday Ticks, PSX Indices, Mutual Funds, Treasury Auctions, PIB Auctions, KIBOR Rates — each showing days since last sync
- Market Breadth donut: Gainers (green) / Losers (red) / Unchanged (gray) with net breadth (e.g., "Bullish +117")
- Top Gainers / Top Losers with horizontal bar charts
- Volume Leaders table (symbol, close, volume, change%)
- 52-Week Range: stocks near 52-week highs and lows
- Sector Performance table: sector, stock count, average change%, volume, top symbol
- Sync & Data Management panel (collapsible)

**Business value:** Single-screen market intelligence. A portfolio manager opens this once at 9:30 AM and sees everything — index, breadth, leaders, sectors, rates — without switching between 5 different websites.

**Audience:** Everyone — the landing page for all users.

---

### 2. Market Pulse
**URL:** `/market-pulse`

**What it shows:**
- Market breadth bar (277 Adv / 140 Dec / 67 Unch) with volume and value totals
- Return distribution histogram (how many stocks moved by what %)
- Top Gainers table (symbol, close, prev, change%, volume)
- Top Losers table (same format)
- Volume Leaders (symbol, close, volume, change%)
- Value Leaders / Turnover (symbol, close, vol, value in PKR, change%)

**Business value:** End-of-day market summary. Research analysts use this to write daily market reports in 5 minutes instead of 30.

**Audience:** Research analysts, portfolio managers, market commentators.

---

### 3. Index Monitor
**URL:** `/index-monitor`

**What it shows:**
- All 18 PSX indices: KSE-100, KSE-30, KMI-30, All Share, Banking (BKTI), Oil & Gas (OGTI), Islamic indices, and specialty indices
- Each with: Last value, 1D%, 1W%, 1M%, 3M% change, and 30-day volume
- Market summary: total gainers/losers across indices, average 1M return
- Index Detail section: select any index → see historical chart + performance table

**Business value:** Multi-index comparison in one view. Fund managers compare sector indices to spot rotation (e.g., Banking Index down 17.71% 1M while Pharma up — capital rotating to defensive).

**Audience:** Index fund managers, sector rotation traders, macro analysts.

---

### SECTION 2: EQUITIES

---

### 4. Market Summary (Download Manager)
**URL:** `/market-summary`

**What it shows:**
- Download statistics: 1,321 tracked files, 1,187 OK, 133 missing, 765,458 total rows
- Date range: 2020-01-01 to present
- Single Day download: select date → Download (fetches market summary CSV from PSX DPS)
- Date Range download: bulk backfill any period
- Sync to EOD Database: imports downloaded CSVs into eod_ohlcv table
- DuckDB sync: separate buttons for SQLite → DuckDB migration
- Retry Failed: re-download any 404 files

**Business value:** Automated data pipeline. What used to require manual CSV downloads from PSX DPS website every day is now one-click with full historical backfill.

**Audience:** Data engineers, quant researchers, system administrators.

---

### 5. Stock Screener
**URL:** `/stock-screener`

**What it shows:**
- Filter by: Sector (dropdown), Min/Max P/E, Min Market Cap (M), Min Avg Volume
- Results table: Symbol, Name, Sector, Price, P/E, Market Cap, Free Float %
- Export CSV button
- Currently shows 47 matches (from live market watch data)

**Business value:** Find tradeable opportunities instantly. "Show me all banking stocks with P/E below 5 and daily volume above 1M" — answered in 1 second.

**Audience:** Stock pickers, value investors, fundamental analysts.

---

### 6. Company Profile
**URL:** `/company-profile`

**What it shows:**
- Select symbol → full company information
- Business description, key people, contact details
- Fundamental data: P/E, dividend yield, market cap, book value
- Historical price chart with SMA overlays

**Business value:** Due diligence research. Everything about a company in one place — no need to visit PSX website, annual reports, and corporate filings separately.

**Audience:** Research analysts, investors, compliance teams.

---

### 7. Sector Analysis
**URL:** `/sector-analysis`

**What it shows:**
- Treemap visualization sized by KSE-100 index weights
- Toggle between volume-weighted and index-weight views
- Sector breakdown: how much weight each sector has in the index
- Index weights sourced from PSX constituent_data XLS files

**Business value:** Understand market structure at a glance. Which sectors dominate? Where is the money concentrated? Critical for passive replication and benchmark tracking.

**Audience:** Index fund managers, sector strategists, asset allocators.

---

### 8. Symbol Financials
**URL:** `/symbol-financials`

**What it shows:**
- Select symbol → financial statements
- Revenue, profit, EPS trends
- Key ratios over time

**Business value:** Fundamental analysis without downloading annual reports.

**Audience:** Fundamental analysts, credit analysts.

---

### 9. Factors
**URL:** `/factors`

**What it shows:**
- Multi-factor analysis: momentum, value, quality, volatility factors
- Factor returns and exposures

**Business value:** Quantitative factor investing for PSX — which factors drive returns in Pakistan's market?

**Audience:** Quant analysts, factor-based portfolio managers.

---

### 10. Intraday Trading Terminal
**URL:** `/intraday`

**What it shows:**
- Dashboard tab: market-wide intraday stats (if data synced)
- Charts tab: individual symbol intraday OHLCV charts
- Market Pulse tab: intraday breadth
- Volume tab: intraday volume analysis
- Movers tab: real-time top movers
- Index tab: KSE-100 intraday overlay
- Dedup tab: data quality tools
- Sync tab: Bulk Intraday Sync (all 622 symbols), DuckDB sync buttons

**Business value:** The trading floor in a browser. Intraday price, volume, and breadth analytics for every symbol — updated with each DPS API sync.

**Audience:** Day traders, proprietary desks, market makers.

---

### 11. Live Ticker
**URL:** `/live-ticker`

**What it shows:**
- Real-time streaming market watch from PSX DPS API
- Price, change, volume for all symbols
- Auto-refreshing

**Business value:** Live market feed without PSX terminal subscription.

**Audience:** Active traders, brokers, market monitors.

---

### 12. Futures & Odd Lot
**URL:** `/futures-odd-lot`

**What it shows:**
- Deliverable Futures Contracts (DFC): open interest (REAL from XLS), basis analysis, volume
- Odd Lot market: small lot trading activity
- Rollover tracker: contract month transitions
- OI buildup/unwind matrix: where institutional money is flowing

**Business value:** Derivatives intelligence. Futures basis shows market expectations, OI buildup reveals institutional positioning. Only terminal in Pakistan providing real OI analytics.

**Audience:** Derivatives traders, institutional desks, prop traders.

---

### 13. Post Close
**URL:** `/post-close`

**What it shows:**
- Post-close session data
- Final settlement prices
- Any after-hours trades

**Business value:** Complete end-of-day picture including post-close adjustments.

**Audience:** Clearing operations, settlement teams, risk managers.

---

### SECTION 3: FIXED INCOME

---

### 14. Rates Overview
**URL:** `/rates-overview`

**What it shows:**
- SBP policy rate history
- KIBOR rates (1W, 1M, 3M, 6M, 9M, 12M) from SBP EasyData
- T-Bill cut-off yields
- PIB benchmark rates
- All with historical charts and trend analysis

**Business value:** Pakistan's interest rate environment at a glance. Treasury desks need this every morning to price instruments.

**Audience:** Fixed income traders, treasury desks, ALM teams, corporate treasurers.

---

### 15. Yield Curves
**URL:** `/yield-curves`

**What it shows:**
- PKRV yield curve (1M to 30Y)
- Historical curve movements
- Curve shape analysis (steepening/flattening)

**Business value:** Yield curve analysis for bond pricing and macro forecasting. A flattening curve signals economic slowdown — critical for fixed income portfolio positioning.

**Audience:** Bond traders, fixed income portfolio managers, macro analysts.

---

### 16. Treasury Auctions
**URL:** `/treasury-auctions`

**What it shows:**
- T-Bill auction results (3M, 6M, 12M)
- PIB auction results (3Y, 5Y, 10Y, 20Y, 30Y)
- Cut-off yields, bid-to-cover ratios, amounts raised
- Historical comparison

**Business value:** Government debt market intelligence. Banks and mutual funds need auction data to plan their portfolios and bidding strategies.

**Audience:** Treasury operations, mutual fund managers, government debt traders.

---

### 17. Bond Market / Debt Terminal
**URL:** `/bond-market` / `/debt-terminal`

**What it shows:**
- Secondary market bond trading data
- GDS (Government Debt Securities) and CDS (Corporate Debt Securities)
- Benchmark Monitor: PKRV rates for all tenors
- Trading volumes and yields

**Business value:** Fixed income secondary market monitoring. Where is volume? Which tenors are active? What are the clearing prices?

**Audience:** Debt capital markets, bond traders, credit analysts.

---

### SECTION 4: FX & RATES

---

### 18. Currency Dashboard
**URL:** `/currency-dashboard`

**What it shows:**
- USD/PKR, EUR/PKR, GBP/PKR rates
- Interbank vs open market spreads
- Historical FX trends

**Business value:** FX monitoring for trade finance, remittances, and hedging decisions.

**Audience:** Corporate treasurers, FX dealers, trade finance teams, overseas Pakistanis.

---

### 19. FX Dashboard / Interbank vs Open / Rate History
**URL:** `/fx-dashboard` / `/interbank-vs-open` / `/rate-history`

**What it shows:**
- Detailed FX rate comparisons
- NBP TT Buying/Selling rates
- Interbank vs kerb market spread
- Historical rate movements

**Business value:** Understanding PKR dynamics. Spread between interbank and open market signals currency pressure.

**Audience:** FX traders, economists, remittance companies.

---

### SECTION 5: COMMODITIES

---

### 20. Commodities
**URL:** `/commodities`

**What it shows:**
- Gold, silver, crude oil prices
- Local commodity prices in PKR

**Business value:** Cross-asset context for equity analysts. Rising oil affects fertilizer costs, gold affects jewelry sector.

**Audience:** Multi-asset analysts, commodity traders.

---

### 21. PMEX
**URL:** `/pmex`

**What it shows:**
- Pakistan Mercantile Exchange data
- OHLC for PMEX contracts
- Margin requirements

**Business value:** Commodity derivatives intelligence for Pakistan market.

**Audience:** Commodity futures traders, agricultural hedgers.

---

### SECTION 6: FUNDS

---

### 22. Fund Explorer / VPS Pension / Top Performers / Fund Analytics / ETFs
**URL:** `/fund-explorer` / `/vps-pension` / `/top-performers` / `/fund-analytics` / `/etfs`

**What it shows:**
- Mutual fund NAV tracking from MUFAP
- Performance comparison across fund categories
- VPS (Voluntary Pension Scheme) analysis
- ETF tracking
- Risk metrics: Sharpe ratio, max drawdown, volatility

**Business value:** Complete mutual fund analytics. Which funds outperform? What's the risk-adjusted return? MUFAP data parsed and analyzed automatically.

**Audience:** Fund distributors, wealth managers, retail investors, pension fund selectors.

---

### SECTION 7: RESEARCH (Quantitative Analytics)

---

### 23. Research
**URL:** `/research`

**What it shows:**
- Research tools and analysis framework
- Cross-referencing multiple data sources

**Business value:** Unified research workspace.

**Audience:** Research analysts.

---

### 24. Signal Analysis (Composite Signal Scanner)
**URL:** `/signal-analysis`

**What it shows:**
- **Batch Scanner:** Scan all 500+ symbols → ranked by composite score (1-100)
- **Single Symbol Deep Dive** (3 layers):
  - **Layer 1 — Macro Regime:** Hurst exponent (trending/mean-reverting), annualized volatility, SMA-200 position, circuit breaker history. Score: X/33
  - **Layer 2 — Intraday Anchor:** VWAP distance, POC (Point of Control), Value Area range, ER (earnings risk) status. VWAP bands + volume profile chart. Score: X/33
  - **Layer 3 — Execution DNA:** Tick count, Buy/Sell split, CVD slope, Order Flow Imbalance, VPIN toxicity, block trades. CVD chart + order flow heatmap. Score: X/33
- **Composite Score:** All three layers combined (e.g., "80 — Strong Buy Setup")
- **Intelligence Brief:** Cross-data summary (price action, microstructure, derivatives, institutional)
- **Quant Analyst Commentary:** AI-generated analysis (OpenAI GPT-4o)
- **Methodology section:** Full explanation of scoring algorithm

**Business value:** This is the crown jewel. A unified signal score combining macro, intraday, and execution data — something that doesn't exist anywhere else for PSX. A prop trader can scan 500 symbols in seconds and drill into any signal.

**Audience:** Quantitative traders, prop desks, systematic strategy developers, portfolio managers.

---

### 25. Microstructure Analytics
**URL:** `/microstructure`

**What it shows:**
- **VPIN Toxicity Monitor:** Gauge showing order flow toxicity (0-1 scale, green/yellow/red)
- VPIN statistics: current, mean, max, std dev
- **Volume Bucket Flow:** Buy vs Sell volume per bucket with VPIN overlay
- **Trade Size Distribution:** histogram of trade sizes (institutional vs retail detection)
- **Order Flow Imbalance:** per-minute buy/sell pressure
- **Bid-Ask Spread Analysis:** spread distribution and time-series
- **Tick Table:** raw tick-by-tick data with bid/ask

**Business value:** Market microstructure intelligence previously available only to exchanges and HFT firms. VPIN detects informed trading before price moves. Trade size distribution reveals institutional activity.

**Audience:** Algorithmic traders, market surveillance teams, exchange compliance, academic researchers.

---

### 26. Tick Analytics Terminal
**URL:** `/tick-analytics`

**What it shows:**
- **Overview:** Market-wide metrics from tick data — total ticks (483K+), symbols traded (485), total volume, turnover, total trades, tick rate (1,215/min). Breadth & Sentiment: A/D ratio, breadth %. Microstructure: median spread, P90 spread, cross-sec vol, top-10 concentration.
- **Daily KPI History:** Table of per-day market metrics
- **Intraday Analytics:** Per-symbol tick analysis with OHLCV charts
- **Sync:** JSONL → DuckDB import, tick_bars.db → DuckDB sync, Full Nightly Sync button

**Business value:** Quant-grade market analytics from tick data. How liquid is the market? How concentrated? What's the spread environment? Answers questions that daily OHLCV data cannot.

**Audience:** Quantitative researchers, market makers, exchange surveillance.

---

### 27. Tick Replay
**URL:** `/tick-replay`

**What it shows:**
- Select date + symbol → Play/Pause tick-by-tick replay at 60fps
- Speed controls: 0.1x to 500x
- TradingView Lightweight Charts in client-side JavaScript (zero Streamlit rerenders)
- Live stats: price, change, high, low, volume, trades, VWAP
- Order book: bid, ask, spread, imbalance
- Timeline scrubber: drag to any point in the trading day
- Last 5 trades: real-time tick log with buy/sell coloring

**Business value:** Study any trade, any day, at any speed. A compliance officer investigating a suspicious trade can replay the exact market conditions. A trader studies their execution quality by replaying their fills.

**Audience:** Compliance officers, trade surveillance, execution analysts, trading coaches.

---

### 28. Quant Lab
**URL:** `/quant-lab`

**What it shows:**
- Multi-timeframe analysis using PSX Terminal klines (1m, 5m, 15m, 1h, 1d, 1w)
- Backtesting workspace
- Factor analysis tools

**Business value:** Quantitative research sandbox for developing and testing trading strategies on PSX data.

**Audience:** Quant researchers, algorithmic strategy developers.

---

### 29. Macro Cycles
**URL:** `/macro-cycles`

**What it shows:**
- SBP EasyData integration: CPI, money supply, balance of payments, GDP
- Interest rate cycle analysis
- Macro regime identification (expansion/contraction)
- KIBOR vs equity return correlations

**Business value:** Macro-financial linkages. When SBP cuts rates, which sectors benefit? Historical cycle analysis answers this.

**Audience:** Macro economists, fund managers, strategic planners.

---

### 30. Sector Breadth
**URL:** `/sector-breadth`

**What it shows:**
- Per-sector advance/decline analysis
- Sector rotation heatmaps
- Breadth divergence signals (market up but breadth narrowing = warning)

**Business value:** Early warning system. Narrow breadth signals market vulnerability even when headline index looks strong.

**Audience:** Technical analysts, risk managers, portfolio constructors.

---

### 31. Market Research
**URL:** `/market-research`

**What it shows:**
- AI-powered research synthesis
- Cross-referencing market data with news and announcements

**Business value:** Automated research assistant for generating market notes.

**Audience:** Research analysts, strategists.

---

### SECTION 8: ADMIN

---

### 32. Data Status
**URL:** `/data-status`

**What it shows:**
- All data tables: row counts, date ranges, last update timestamps
- Database file sizes
- Data completeness checks

**Business value:** System monitoring. "Is my data fresh?" answered instantly.

**Audience:** System administrators, data engineers.

---

### 33. Sync Center
**URL:** `/sync-center`

**What it shows:**
- Central hub for all data sync operations
- DPS downloads, DuckDB migration, JSONL imports
- Bulk operations for backfill

**Business value:** One-stop data management.

**Audience:** System administrators.

---

### 34. Schema Explorer
**URL:** `/schema-explorer`

**What it shows:**
- Browse all database tables, columns, types
- Row counts and sample data
- Useful for debugging and data exploration

**Business value:** Database documentation that's always up to date.

**Audience:** Developers, data engineers.

---

## Data Sources

| Source | Data | Frequency |
|--------|------|-----------|
| PSX DPS (Official) | EOD OHLCV, market watch, intraday ticks | Real-time / Daily |
| PSX WebSocket | Live tick stream via cloud VM | Real-time (09:14–17:30 PKT) |
| PSX Downloads | Market summary, OI, circuit limits, index weights, VAR margins | Daily |
| SBP EasyData | 195 datasets: KIBOR, CPI, FX, money supply, BoP, GDP | Daily / Monthly |
| MUFAP | Mutual fund NAV data | Daily |
| PSX Terminal | Supplementary klines (1m–1w), fundamentals | On-demand |
| PMEX | Commodity futures OHLC, margins | Daily |
| NBP | Exchange rates (TT Buying/Selling) | Daily |

---

## Competitive Positioning

| Feature | Bloomberg | Reuters | PSX Website | pakfindata |
|---------|-----------|---------|-------------|------------|
| Annual cost | $24,000+ | $15,000+ | Free | Self-hosted |
| PSX tick data | ✅ | ✅ | ❌ | ✅ |
| Real OI analytics | ✅ | ✅ | ❌ | ✅ |
| VPIN toxicity | ✅ | ❌ | ❌ | ✅ |
| Composite signals | ❌ | ❌ | ❌ | ✅ |
| SBP macro integration | ❌ | Partial | ❌ | ✅ |
| Tick replay 60fps | ❌ | ❌ | ❌ | ✅ |
| Local language NLP | ❌ | ❌ | ❌ | Planned |
| Open Interest analytics | ✅ | ✅ | ❌ | ✅ |
| DuckDB 104x speed | N/A | N/A | N/A | ✅ |

---

## Business Model Options

| Model | Description | Revenue |
|-------|-------------|---------|
| **B2B SaaS** | Hosted terminal for brokers/funds — monthly subscription | PKR 50K–500K/month |
| **Data licensing** | Sell processed PSX data (tick, OI, signals) via API | Per-query or flat rate |
| **White-label** | Custom-branded terminal for brokerage firms | Project + monthly fee |
| **Consulting** | Analytics consulting using pakfindata as platform | Hourly/project |
| **Enterprise** | Full deployment for banks/institutions with customization | PKR 5M+ annual |

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Total pages | 34+ |
| Symbols covered | 539 (full PSX universe) |
| Historical EOD | 598,000+ daily bars (5+ years) |
| Tick data | 4.6M+ tick logs in DuckDB |
| Intraday bars | 3.3M+ (DPS timeseries) |
| SBP macro series | 626 series, 227K observations |
| DuckDB query speed | 104x faster than SQLite |
| Tick replay | 60fps client-side (TradingView charts) |
| Cloud uptime | Mon-Fri 09:14–17:30 PKT automatic |
| Data freshness | 15-minute auto-sync during market hours |

---

## Built by Godaitec

**Godaitec (godai.tech)** — Technology consulting firm based in Karachi, Pakistan. Specializing in banking systems, quantitative finance platforms, and data engineering.

**Contact:** info@godai.tech

---

*pakfindata — Where Pakistan's markets meet institutional-grade analytics.*
