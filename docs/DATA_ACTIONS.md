# PSX OHLCV Explorer - Page-by-Page User Guide

This guide explains what to do on each page to view and refresh data.

---

## Page Navigation Quick Reference

| Page | First Visit Action | Refresh Action | Auto-Refresh |
|------|-------------------|----------------|--------------|
| Dashboard | None (auto-loads) | Wait 60s or reload page | Yes (60s) |
| Candlestick | Select symbol | Change symbol/date range | No |
| Intraday | Enter symbol + click Fetch | Click "Fetch Intraday" | Yes (60s) |
| Regular Market | Click "Fetch Market Data" | Click "Fetch Market Data" | No |
| Company Analytics | Search symbol | Click "Refresh" button | No |
| Data Acquisition | Enter symbol + click Scrape | Click "Scrape" again | No |
| Factor Analysis | Adjust sliders | Adjust sliders | No |
| AI Insights | Select mode + click Generate | Click "Generate" again | No |
| History | Select date range | Change date range | No |
| Market Summary | Set dates + click Download | Click "Retry Failed" | No |
| Symbols | None (auto-loads) | Reload page | No |
| Instruments | Select type filter | Change filter | No |
| Rankings | Click "Refresh Rankings" | Click "Refresh Rankings" | No |
| FX Overview | Select currency pair | Change pair selection | No |
| FX Impact | Click "Compute" | Click "Compute" again | No |
| Mutual Funds | Click "Seed" first time | Click "Sync NAVs" | No |
| Fund Analytics | Select category | Change category/period | No |
| Bonds Screener | Click "Initialize" first | Click "Generate Samples" | No |
| Yield Curve | Select type + date | Change type/date | No |
| Sukuk Screener | Click "Seed" first time | Click "Generate Samples" | No |
| Sukuk Yield Curve | Select curve + date | Change curve/date | No |
| SBP Archive | Click "Create Samples" | Click "Re-index" | No |
| Sync Monitor | Click "Run Full Sync" | Click "Run Full Sync" | No |
| Schema | None (static) | N/A | No |
| Settings | None (static) | N/A | No |

---

## Detailed Page Instructions

### 1. Dashboard (📊)

**When You First Open:**
- Data loads automatically from database
- Shows market KPIs, breadth, top movers

**To Refresh Dashboard Display:**
- **Browser reload:** Press F5 or click browser refresh
- **Auto-refresh:** Every 60 seconds if background service is running

**To Update Underlying Data (Dashboard shows stale data):**

| Data Type | Go To | Action |
|-----------|-------|--------|
| EOD Prices | Sync Monitor | Click "Run Full Sync" |
| Live Market | Regular Market | Click "Fetch Market Data" |
| Intraday | Intraday Trend | Click "Fetch Intraday Data" |

**Dashboard Data Sources:**
- **KSE-100 Index:** From `eod_ohlcv` table (updated via Sync Monitor)
- **Market Breadth:** From `regular_market_current` (updated via Regular Market page)
- **Top Movers:** From `regular_market_current` (updated via Regular Market page)
- **Data Freshness Badge:** Checks `eod_ohlcv` for latest sync date

**Refresh Workflow:**
```
1. Check Dashboard → See "Data is X days old" badge
2. If stale (orange/red badge):
   - Go to Sync Monitor → Click "Run Full Sync"
   - Wait for sync to complete
   - Return to Dashboard → Data is now fresh
3. For live intraday data:
   - Go to Regular Market → Click "Fetch Market Data"
   - Return to Dashboard → See updated movers
```

**What You See:**
- KSE-100 Index value and change
- Market breadth (gainers/losers/unchanged)
- Top 5 gainers and losers
- Data freshness indicator (green/orange/red badge)

**No buttons on Dashboard** - It's a read-only summary page. Update data from other pages.

---

### 2. Candlestick Explorer (📈)

**When You First Open:**
1. Select a symbol from the **"Select Symbol"** dropdown
2. Choose a date range: **1M, 3M, 6M, 1Y, or All**
3. Optionally toggle **"Show SMA"** for moving averages

**To Refresh Data:**
- Change the symbol selection
- Change the date range
- Data reloads automatically on selection change

**What You See:**
- Candlestick chart with OHLC prices
- Volume bars below
- SMA(20) and SMA(50) overlays (if enabled)

---

### 3. Intraday Trend (⏱)

**When You First Open:**
1. Enter a symbol in the **"Symbol"** text input (e.g., `HBLPSX`)
2. Click **"Fetch Intraday Data"** button
3. Wait for data to load (may take a few seconds)

**To Refresh Data:**
- Click **"Fetch Intraday Data"** button again
- Or wait for auto-refresh (every 60 seconds if service running)

**Options:**
- **"Incremental sync"** checkbox: Only fetch new data since last sync (faster)

**What You See:**
- 1-minute price bars
- High-Low range shading
- Volume bars

---

### 4. Regular Market (📊)

**When You First Open:**
1. Click **"Fetch Market Data"** button (primary blue button)
2. Wait for scrape to complete (shows progress)

**To Refresh Data:**
- Click **"Fetch Market Data"** button again

**What You See:**
- Live market watch data
- Current prices, changes, volumes
- Change tracking between snapshots

**Note:** This scrapes the PSX website - use responsibly.

---

### 5. Company Analytics (🏢)

**When You First Open:**
1. Enter a symbol in **"Search Symbol"** input (e.g., `HBL`)
2. Press Enter to load company data

**To Refresh Data:**
- Click **"Refresh"** button to deep scrape latest data
- Warning: Deep scrape takes 30+ seconds

**What You See:**
- Company profile and key people
- Quote history and trading sessions
- Financial statements and ratios
- Corporate announcements
- Dividend/bonus history

---

### 6. Data Acquisition (📥)

**Single Symbol Scrape:**
1. Enter symbol in **"Symbol"** input
2. Optionally check **"Save raw HTML"**
3. Click **"Scrape"** button

**Batch Scrape Multiple Symbols:**
1. Enter symbols in text area (one per line)
2. Set delay between requests (3-5 seconds recommended)
3. Click **"Start Batch Scrape"** button
4. Monitor progress bar

**To Refresh:**
- Run scrape again for updated data

---

### 7. Factor Analysis (📊)

**When You First Open:**
- Data loads automatically from existing company snapshots

**To Adjust Rankings:**
1. Move the weight sliders:
   - **Value Weight** (0.0 - 1.0)
   - **Momentum Weight** (0.0 - 1.0)
   - **Quality Weight** (0.0 - 1.0)
   - **Volatility Weight** (0.0 - 1.0)
2. Rankings recalculate instantly

**What You See:**
- Multi-factor stock rankings
- Factor correlation heatmap
- Sector exposure analysis

**Note:** Requires company data from Company Analytics or Data Acquisition.

---

### 8. AI Insights (🤖)

**When You First Open:**
1. Select **Analysis Mode**:
   - **Company**: Analysis of a specific company
   - **Intraday**: Analysis of intraday patterns
   - **Market**: Overall market analysis
   - **History**: Historical trend analysis
2. Enter symbol (if Company/Intraday mode)
3. Click **"Generate Insights"** button
4. Wait for LLM response (may take 10-30 seconds)

**To Refresh:**
- Click **"Generate Insights"** again

**Requirements:**
- `OPENAI_API_KEY` environment variable must be set
- Token usage shown (costs money)

---

### 9. History (📚)

**When You First Open:**
1. Select **Start Date** and **End Date**
2. Data loads automatically

**To View Different Data:**
- Change the date range
- Select a specific symbol for detailed view

**What You See:**
- Daily market aggregates
- Gainers/losers/unchanged counts
- Symbol-specific OHLCV charts

---

### 10. Market Summary (📥)

**When You First Open:**
1. Set **Start Date** and **End Date** for download range
2. Click **"Download"** button
3. Monitor progress as files download

**To Retry Failed Downloads:**
- Click **"Retry Failed"** button

**What You See:**
- Download statistics (OK, 404 Missing, Failed)
- Download history table
- Missing dates list

**Note:** Downloads daily market summary files from PSX.

---

### 11. Symbols (🧵)

**When You First Open:**
- Symbol list loads automatically

**To Filter:**
- Toggle **"Active symbols only"** checkbox
- Use **Search** input to find specific symbols

**What You See:**
- Complete symbol master list
- Symbol, name, sector, shares outstanding
- Active/inactive status

**No refresh needed** - Master data is static.

---

### 12. Instruments (📦) - Phase 1

**When You First Open:**
1. Select **Type** filter: All, ETF, REIT, or INDEX
2. Toggle **"Active only"** if needed

**To Refresh:**
- Change filter selections

**What You See:**
- ETF, REIT, and Index listings
- Performance metrics (1M/3M returns, volatility)

---

### 13. Rankings (🏆) - Phase 1

**When You First Open:**
1. Select instrument types using **multi-select**
2. Set **Top N** slider
3. Click **"Refresh Rankings"** button

**To Refresh:**
- Click **"Refresh Rankings"** button again

**What You See:**
- Performance rankings by 1M, 3M, 6M, 1Y returns
- Volatility metrics
- Ranked instrument list

---

### 14. FX Overview (🌍) - Phase 2

**When You First Open:**
1. Select **Currency Pair** from dropdown (USD/PKR, EUR/PKR, etc.)
2. Data loads automatically

**To View Different Pair:**
- Change the pair selection

**What You See:**
- Current exchange rate
- 1W, 1M, 3M returns
- Trend indicator (UP/DOWN)
- Volatility metrics

---

### 15. FX Impact (📊) - Phase 2

**When You First Open:**
1. Select **Currency Pair**
2. Select **Period** (1W, 1M, 3M)
3. Set **Top N stocks** slider
4. Click **"Compute"** button

**To Refresh:**
- Click **"Compute"** button again

**What You See:**
- FX-adjusted equity returns
- FX context (rate, return, volatility)
- Performance table

---

### 16. Mutual Funds (🏦) - Phase 2.5

**First Time Setup:**
1. Click **"Seed Funds"** button to initialize fund definitions
2. Click **"Sync NAVs"** button to fetch NAV history

**To Browse Funds:**
1. Select **Category** filter (Equity, Income, Money Market, etc.)
2. Select **Type** filter (Open-End, Closed-End)
3. Toggle **"Shariah compliant only"** if needed

**To Refresh:**
- Click **"Sync NAVs"** button

**What You See:**
- Fund directory with filters
- Fund details and NAV history
- AMC breakdown

---

### 17. Fund Analytics (📊) - Phase 2.5

**When You First Open:**
1. Select **Category** from dropdown
2. Select **Period** (1W, 1M, 3M, 6M, 1Y)
3. Set **Top N** slider

**To Refresh:**
- Change category or period selection

**What You See:**
- Category performance comparison
- Top funds ranking
- Fund comparison charts

---

### 18. Bonds Screener (🧾) - Phase 3

**First Time Setup:**
1. Click **"Initialize"** button to seed bond definitions
2. Click **"Generate Samples"** button to create sample quotes

**To Browse Bonds:**
1. Select **Type** filter (PIB, T-Bill, Sukuk, etc.)
2. Select **Issuer** filter
3. Toggle **"Islamic bonds only"** if needed
4. Set **Min YTM %** if filtering by yield

**To Refresh:**
- Click **"Generate Samples"** for new quotes

**What You See:**
- Bond universe with analytics
- YTM, Duration, Convexity
- Spread analysis

---

### 19. Yield Curve (📉) - Phase 3

**When You First Open:**
1. Select **Curve Type** (PIB, T-Bill, Sukuk, ALL)
2. Select **Date** (or use "Latest")
3. If curve doesn't exist, click **"Build Curve"**

**To View Different Curve:**
- Change type or date selection

**What You See:**
- Yield curve chart (Yield % vs Tenor)
- Term structure data table
- Interpolated values

---

### 20. Sukuk Screener (🕌) - Phase 3

**First Time Setup:**
1. Click **"Seed"** button to initialize sukuk definitions
2. Click **"Generate Samples"** button to create sample quotes

**To Browse Sukuk:**
1. Select **Category** filter (GOP_SUKUK, PIB, etc.)
2. Toggle **"Shariah compliant only"** if needed

**To Refresh:**
- Click **"Generate Samples"** for new quotes

**What You See:**
- Sukuk universe with analytics
- Category-based metrics

---

### 21. Sukuk Yield Curve (📈) - Phase 3

**When You First Open:**
1. Select **Curve** name (GOP_SUKUK, PIB, TBILL)
2. Select **Date** (or use "Latest")

**To Generate Sample Curves:**
- Click **"Generate Sample Curves"** button

**To View Different Curve:**
- Change curve or date selection

**What You See:**
- Sukuk yield curve chart
- Tenor labels and yield percentages

---

### 22. SBP Archive (🏛️) - Phase 3

**First Time Setup:**
1. Click **"Create Samples"** button to generate sample documents
2. Click **"Re-index"** button to index documents

**To Browse Documents:**
1. Select **Document Type** filter
2. Select **Instrument Type** filter

**To Refresh:**
- Click **"Re-index"** button after adding new documents

**What You See:**
- SBP primary market documents
- Auction results, notifications
- Official SBP source URLs

---

### 23. Sync Monitor (🔄)

**To Run Full Data Sync (Background):**

1. Optionally check **"Refresh symbols before sync"**
2. Optionally check **"Incremental mode"** (recommended - faster)
3. Click **"▶️ Run Full Sync"** button
4. Sync starts in background - you can navigate to other pages

**While Sync is Running:**

- Progress bar shows current status
- Current symbol being synced is displayed
- Click **"🛑 Stop Sync"** to cancel if needed
- Page auto-refreshes every 2 seconds to show progress

**After Sync Completes:**

- Last sync status shown (Success/Partial/Error/Cancelled)
- Metrics: Symbols OK, Failed, Rows synced
- Timestamp of completion
- Any error messages if sync failed

**To Sync Intraday Data:**

1. Select symbols from multi-select
2. Click **"Bulk Intraday Sync"** button

**What You See:**

- Real-time sync progress
- Symbols OK, failed, skipped
- Historical sync runs

**Note:** Full sync runs in background (5-15 minutes). You can continue using other pages while sync runs.

---

### 24. Schema (📋)

**When You First Open:**
- Documentation loads automatically

**No actions needed** - Read-only reference page.

**What You See:**
- Database table overview
- Glossary of market terms
- SQL creation scripts
- Table row counts

---

### 25. Settings (⚙️)

**When You First Open:**
- Settings display automatically

**No actions needed** - Read-only configuration display.

**What You See:**
- Database path
- Sync configuration
- Logging settings
- Export directory

---

## Data Flow Summary

### Getting Started (New Installation)

**Step 1: Initialize Master Data**
```
1. Go to Sync Monitor
2. Check "Refresh symbol list first"
3. Click "Run Full Sync"
```

**Step 2: Initialize Phase 2-3 Data (Optional)**
```
1. Mutual Funds → Click "Seed Funds" then "Sync NAVs"
2. Bonds Screener → Click "Initialize" then "Generate Samples"
3. Sukuk Screener → Click "Seed" then "Generate Samples"
```

**Step 3: Get Company Data**
```
1. Go to Data Acquisition
2. Enter symbols (one per line)
3. Click "Start Batch Scrape"
```

### Daily Operations

| Task | Page | Action |
|------|------|--------|
| Update EOD prices | Sync Monitor | Click "Run Full Sync" |
| Get live market data | Regular Market | Click "Fetch Market Data" |
| Update intraday | Intraday Trend | Click "Fetch Intraday" |
| Refresh company | Company Analytics | Click "Refresh" |
| Update fund NAVs | Mutual Funds | Click "Sync NAVs" |

### Automated via CLI

```bash
# Daily EOD sync (can be scheduled via cron)
pfsync sync --all

# Single symbol update
pfsync sync HBLPSX

# Intraday sync
pfsync intraday HBLPSX
```

---

## Troubleshooting

### "No data available"
1. Check if you've run initial sync
2. Try clicking the refresh/fetch button
3. Check Sync Monitor for sync status

### "Data is stale"
1. Go to Sync Monitor
2. Click "Run Full Sync"
3. Check Dashboard for freshness badge

### "Button not responding"
1. Check if another operation is in progress (look for spinner)
2. Wait for current operation to complete
3. Reload page if stuck

### "AI Insights not working"
1. Check `OPENAI_API_KEY` environment variable
2. Ensure you have API credits
3. Check token estimate before generating

---

## Button Reference by Type

### Primary Data Fetch Buttons (Blue)
- **"Fetch Market Data"** - Regular Market
- **"Fetch Intraday Data"** - Intraday Trend
- **"Run Full Sync"** - Sync Monitor
- **"Generate Insights"** - AI Insights
- **"Download"** - Market Summary

### Refresh/Update Buttons
- **"Refresh"** - Company Analytics
- **"Refresh Rankings"** - Rankings
- **"Compute"** - FX Impact
- **"Retry Failed"** - Market Summary
- **"Re-index"** - SBP Archive

### Initialization Buttons (One-Time)
- **"Seed Funds"** - Mutual Funds
- **"Initialize"** - Bonds Screener
- **"Seed"** - Sukuk Screener
- **"Create Samples"** - SBP Archive

### Sample Data Generators
- **"Generate Samples"** - Bonds Screener, Sukuk Screener
- **"Generate Sample Curves"** - Sukuk Yield Curve
- **"Sync NAVs"** - Mutual Funds

### Scrape Buttons
- **"Scrape"** - Data Acquisition (single)
- **"Start Batch Scrape"** - Data Acquisition (multiple)

---

*Last Updated: 2026-01-29*
