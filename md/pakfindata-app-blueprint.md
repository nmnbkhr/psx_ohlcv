# PakFinData — App Architecture & UX Blueprint

## The Problem

You've built 15+ data pipelines, 10+ tables, multiple sync commands — but the app grew
organically. Data is there but the **user journey** isn't designed. A financial professional
opening this app should immediately know: where am I, what can I see, what can I do.

---

## INFORMATION ARCHITECTURE

### The 5 Pillars of Pakistan Financial Data

Everything in your app falls into one of these:

```
┌─────────────────────────────────────────────────────────────┐
│                     PakFinData                               │
├──────────┬──────────┬──────────┬──────────┬────────────────┤
│ EQUITIES │  FIXED   │  FUNDS   │    FX    │   MACRO /      │
│          │  INCOME  │          │          │   OVERVIEW     │
├──────────┼──────────┼──────────┼──────────┼────────────────┤
│ PSX EOD  │ Yield    │ Mutual   │ Interbank│ Dashboard      │
│ Intraday │ Curves   │ Funds    │ Open Mkt │ Market Pulse   │
│ Company  │ Auctions │ VPS      │ Kerb     │ Data Status    │
│ Sectors  │ KIBOR    │ ETFs     │ Crosses  │ Admin / Sync   │
│ Indices  │ Bond OTC │ Perf     │ History  │ Schema         │
│ Screener │ Benchmark│ Rankings │          │                │
└──────────┴──────────┴──────────┴──────────┴────────────────┘
```

---

## PAGE STRUCTURE (Navigation)

### Sidebar Navigation — Clean, Grouped

```
📊 MARKET OVERVIEW
   ├── Dashboard                  (the home page — everything at a glance)
   └── Market Pulse               (what moved today, alerts, notable events)

📈 EQUITIES
   ├── Market Summary             (KSE-100, indices, top movers, breadth)
   ├── Stock Screener             (filter by sector, P/E, market cap, etc.)
   ├── Company Profile            (single stock deep dive)
   ├── Sector Analysis            (sector rotation, relative performance)
   └── Live Ticker                (WebSocket feed if available)

💰 FIXED INCOME
   ├── Rates Overview             (KIBOR, KONIA, policy rate — the big picture)
   ├── Yield Curves               (PKRV, PKISRV, PKFRV — interactive)
   ├── Treasury Auctions          (T-Bill, PIB, GIS — results + history)
   ├── Bond Market (OTC)          (SMTV daily volumes + yields)
   └── Benchmark Monitor          (SBP benchmark snapshot + history)

🏦 FUNDS
   ├── Fund Explorer              (browse all 1,190 funds, filter, compare)
   ├── VPS Pension                (dedicated VPS view by AMC)
   ├── Top Performers             (rankings by period, category)
   ├── Fund Analytics             (single fund deep dive + NAV chart)
   └── ETFs                       (ETF-specific view)

💱 FX & RATES
   ├── Currency Dashboard         (PKR vs USD, EUR, GBP, AED, SAR, CNY)
   ├── Interbank vs Open Market   (spread visualization)
   └── Rate History               (interactive charts, any pair)

⚙️ ADMIN
   ├── Data Status                (freshness of every table, row counts, gaps)
   ├── Sync Center                (run syncs, see logs, schedule)
   └── Schema Explorer            (browse DB structure, download)
```

**Total: 19 pages across 5 sections + admin**

---

## PAGE DESIGNS

### 1. DASHBOARD (Home Page) — "What happened today"

This is what a portfolio manager sees at 9am. Everything important, nothing extra.

```
┌─────────────────────────────────────────────────────────────────┐
│  PakFinData Dashboard                        📅 Feb 27, 2026   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ │
│  │ KSE-100 │ │ KSE-30  │ │ SBP Rate│ │KIBOR 3M │ │ USD/PKR │ │
│  │ 82,450  │ │ 35,120  │ │ 10.50%  │ │ 10.58%  │ │ 278.50  │ │
│  │ ▲ +1.2% │ │ ▲ +0.9% │ │ — 0.0%  │ │ ▼-0.02% │ │ ▼-0.15  │ │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ │
│                                                                 │
│  ┌────────────────────────────┐  ┌────────────────────────────┐ │
│  │ EQUITY MOVERS              │  │ FIXED INCOME               │ │
│  │                            │  │                            │ │
│  │ Top Gainers    Top Losers  │  │ Today's Auction: T-Bill    │ │
│  │ LUCK +4.2%    ENGRO -2.1% │  │ Cutoff: 10.28% (3M)       │ │
│  │ HBL  +3.8%    PPL   -1.8% │  │                            │ │
│  │ MCB  +2.9%    OGDC  -1.5% │  │ OTC Volume: PKR 928B      │ │
│  │                            │  │ (MTB: 650B, PIB: 200B)    │ │
│  │ Volume: 485M shares        │  │                            │ │
│  │ Value: PKR 22.3B           │  │ PKRV 10Y: 11.24%          │ │
│  └────────────────────────────┘  └────────────────────────────┘ │
│                                                                 │
│  ┌────────────────────────────┐  ┌────────────────────────────┐ │
│  │ FUNDS                      │  │ FX RATES                   │ │
│  │                            │  │                            │ │
│  │ Best Fund Today:           │  │ USD/PKR  278.50  ▼-0.15   │ │
│  │ JS Growth +2.4% (Equity)   │  │ EUR/PKR  301.20  ▲+0.40   │ │
│  │                            │  │ GBP/PKR  352.80  ▲+0.25   │ │
│  │ Category Avg Returns:      │  │ AED/PKR   75.85  ▼-0.05   │ │
│  │ Equity: +18.2% YTD         │  │ SAR/PKR   74.25  ▼-0.03   │ │
│  │ Income: +11.5% YTD         │  │                            │ │
│  │ Money Mkt: +8.9% YTD       │  │ Spread (IB vs OM): 1.25   │ │
│  └────────────────────────────┘  └────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ DATA FRESHNESS                                  [Sync All] │ │
│  │ EOD: ✅ Today  NAV: ✅ Today  KIBOR: ✅ Today              │ │
│  │ PKRV: ⚠️ 2 days ago  FX: ✅ Today  SMTV: ✅ Today         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**KEY DESIGN PRINCIPLE:** Dashboard shows LATEST values + change from previous.
Every number is clickable — takes you to the detailed page.
Data freshness bar at bottom — one glance tells you if anything is stale.

---

### 2. RATES OVERVIEW — "The Fixed Income Command Center"

```
┌─────────────────────────────────────────────────────────────────┐
│  Rates Overview                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  SBP POLICY RATE                                                │
│  ┌──────────────────────────────────────────────────────┐       │
│  │           10.50%           Unchanged since Jan 2026  │       │
│  │  ████████████████████████████████░░░░░░░░░░░░░░░░░░  │       │
│  │  (Range: 5.75% — 22.00% over last 10 years)         │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                 │
│  MONEY MARKET RATES                                             │
│  ┌──────────┬──────────┬──────────┬──────────┐                 │
│  │          │   Bid    │  Offer   │  Change  │                 │
│  │ KIBOR 1W │  10.20%  │  10.45%  │  -0.02   │                 │
│  │ KIBOR 1M │  10.25%  │  10.50%  │  -0.01   │                 │
│  │ KIBOR 3M │  10.33%  │  10.58%  │  -0.02   │                 │
│  │ KIBOR 6M │  10.35%  │  10.60%  │  -0.01   │                 │
│  │ KIBOR 1Y │  10.37%  │  10.87%  │  +0.05   │                 │
│  │ KONIA    │  10.15%  │    —     │  -0.03   │                 │
│  └──────────┴──────────┴──────────┴──────────┘                 │
│                                                                 │
│  GOVERNMENT SECURITIES — Latest Auction Cutoffs                 │
│  ┌──────────┬──────────┬──────────┐                            │
│  │   MTB    │  Cutoff  │  Date    │                            │
│  │   3M     │  10.29%  │  Feb 26  │                            │
│  │   6M     │  10.44%  │  Feb 26  │                            │
│  │  12M     │  10.60%  │  Feb 26  │                            │
│  ├──────────┼──────────┼──────────┤                            │
│  │   PIB    │  Cutoff  │  Date    │                            │
│  │   2Y     │  10.34%  │  Feb 19  │                            │
│  │   3Y     │  10.25%  │  Feb 19  │                            │
│  │   5Y     │  10.75%  │  Feb 12  │                            │
│  │  10Y     │  11.24%  │  Feb 12  │                            │
│  │  15Y     │  11.50%  │  Jan 29  │                            │
│  └──────────┴──────────┴──────────┘                            │
│                                                                 │
│  [📊 Yield Curves]  [📋 Auction History]  [📈 Rate Trends]     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### 3. FUND EXPLORER — "Bloomberg for Pakistani Mutual Funds"

```
┌─────────────────────────────────────────────────────────────────┐
│  Fund Explorer                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐             │
│  │1,190    │ │ 519     │ │ +15.2%  │ │ PKR 2.1T│             │
│  │Funds    │ │Active   │ │Avg YTD  │ │Total AUM│             │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘             │
│                                                                 │
│  FILTERS:                                                       │
│  [Category ▼] [AMC ▼] [Rating ▼] [Min Return ___] [Shariah ☐] │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Fund Name        │ AMC    │ Category  │ NAV  │1M  │YTD  │   │
│  │──────────────────┼────────┼───────────┼──────┼────┼─────│   │
│  │ JS Growth Fund   │ JSIL   │ Equity    │52.31 │+2.4│+18.2│   │
│  │ ABL Income Fund  │ ABL    │ Income    │11.85 │+0.9│+11.5│   │
│  │ HBL Money Market │ HBL    │ Money Mkt │10.12 │+0.7│ +8.9│   │
│  │ Meezan Equity    │ Meezan │ SC Equity │45.67 │+2.1│+17.8│   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Click any fund → Fund Analytics deep dive                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## ACTION BUTTONS — Where and What

### Principle: Actions belong where data is consumed

**DON'T** have a giant "Sync Everything" page with 20 buttons.
**DO** have contextual sync buttons on each page.

### Button Placement Map

```
PAGE                    ACTIONS (top-right of each section)
────────────────────    ──────────────────────────────────────────

Dashboard               [🔄 Sync All]  (runs full daily sync)
                        Data freshness indicators are clickable → jump to stale source

Market Summary          [🔄 Sync EOD]  (pfsync eod sync)
                        [📥 Export CSV]

Stock Screener          [🔄 Refresh]
                        [📥 Export Results]

Company Profile         [🔄 Update Financials]  (single company)
                        [📊 Compare]  (add to comparison)

Rates Overview          [🔄 Sync Rates]  (KIBOR + KONIA + policy rate)
                        No export needed — rates are on-screen

Yield Curves            [🔄 Sync PKRV]
                        [📅 Date Picker]  (historical curve as-of-date)
                        [📥 Export Curve Data]

Treasury Auctions       [🔄 Sync Auctions]  (T-Bill + PIB + GIS)
                        [📅 Date Range]
                        [📥 Export]

Bond Market (OTC)       [🔄 Sync SMTV]  (today's PDF)
                        [📅 Date Range]

Benchmark Monitor       [🔄 Sync Benchmark]
                        [📅 Date Range]  (historical snapshots)

Fund Explorer           [🔄 Sync Funds]  (NAV + performance + expense)
                        [📅 As-of Date]  (view historical snapshot)
                        [📥 Export Fund List]
                        [🔍 Compare]  (multi-fund comparison mode)

VPS Pension             [🔄 Sync Funds]
                        [📊 AMC Comparison]

Top Performers          [🔄 Refresh Rankings]
                        [📅 Period: YTD|1Y|3Y]
                        [📥 Export]

Fund Analytics          [🔄 Update NAV]  (single fund)
                        [📥 Export NAV History]
                        [📊 Add to Compare]

ETFs                    [🔄 Sync ETFs]

Currency Dashboard      [🔄 Sync FX]
                        [📅 Date Range]

Data Status             [🔄 Sync All]  [🔄 Sync Selected]
                        [🗑️ Clear Cache]
                        Per-table: [🔄] individual sync buttons

Sync Center             [▶️ Run Now]  per sync job
                        [📋 View Log]
                        [⏰ Schedule]  (cron setup helper)

Schema Explorer         [📥 Download Schema SQL]
                        [📥 Download Full DB Backup]
```

---

## SYNC CENTER (Admin Page) — The Command Center

This replaces scattered sync buttons for power users:

```
┌─────────────────────────────────────────────────────────────────┐
│  Sync Center                                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DAILY SYNC STATUS                              [▶️ Run All]    │
│  ┌────────────────────┬──────────┬─────────┬──────────────┐    │
│  │ Pipeline           │ Last Run │ Status  │ Action       │    │
│  │────────────────────┼──────────┼─────────┼──────────────│    │
│  │ EOD Prices         │ Today    │ ✅ 582  │ [▶️] [📋]    │    │
│  │ Mutual Fund NAVs   │ Today    │ ✅ 1190 │ [▶️] [📋]    │    │
│  │ Fund Performance   │ Today    │ ✅ 519  │ [▶️] [📋]    │    │
│  │ KIBOR Daily        │ Today    │ ✅ OK   │ [▶️] [📋]    │    │
│  │ PKRV Curve         │ 2 days   │ ⚠️ Stale│ [▶️] [📋]    │    │
│  │ FX Rates           │ Today    │ ✅ OK   │ [▶️] [📋]    │    │
│  │ Bond Trading SMTV  │ Today    │ ✅ OK   │ [▶️] [📋]    │    │
│  │ Benchmark Snapshot  │ Today    │ ✅ OK   │ [▶️] [📋]    │    │
│  │ Expense Ratios     │ 5 days   │ ℹ️ OK   │ [▶️] [📋]    │    │
│  └────────────────────┴──────────┴─────────┴──────────────┘    │
│                                                                 │
│  BACKFILL JOBS                                                  │
│  ┌────────────────────┬──────────────────┬──────────────┐      │
│  │ Job                │ Coverage         │ Action       │      │
│  │────────────────────┼──────────────────┼──────────────│      │
│  │ NAV History        │ 1996 → Today     │ [▶️ Fill Gaps]│      │
│  │ Fund Returns       │ 2024 → Today     │ [▶️ Backfill] │      │
│  │ PKRV History       │ 21 days ⚠️       │ [▶️ Backfill] │      │
│  │ KONIA History      │ 22 days ⚠️       │ [▶️ Backfill] │      │
│  │ Policy Rate History│ 1 row ⚠️         │ [▶️ Backfill] │      │
│  │ Auction History    │ 2000 → Today ✅  │ [▶️ Update]   │      │
│  └────────────────────┴──────────────────┴──────────────┘      │
│                                                                 │
│  CRON SCHEDULE (recommended)                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 4:30 PM PKT  │ pfsync eod sync          (after market)  │   │
│  │ 5:00 PM PKT  │ pfsync funds sync        (NAVs)          │   │
│  │ 5:00 PM PKT  │ pfsync funds performance (MUFAP tab=1)   │   │
│  │ 5:30 PM PKT  │ pfsync rates sync        (KIBOR/KONIA)   │   │
│  │ 6:30 PM PKT  │ pfsync bonds smtv-sync   (after SBP pub) │   │
│  │ 6:30 PM PKT  │ pfsync bonds benchmark   (SBP snapshot)  │   │
│  │ 7:00 PM PKT  │ pfsync fx sync           (FX rates)      │   │
│  │ Weekly (Fri)  │ pfsync funds expense     (expense ratios) │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  LAST SYNC LOG                                    [Full Logs]   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 17:05 ✅ Fund NAVs: 1,190 funds synced (4.2s)           │   │
│  │ 17:05 ✅ Fund Performance: 519 funds (2.1s)              │   │
│  │ 16:32 ✅ EOD Prices: 582 symbols (8.3s)                  │   │
│  │ 16:30 ⚠️ PKRV: timeout — will retry                     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## DATA STATUS PAGE — "Health Check at a Glance"

```
┌─────────────────────────────────────────────────────────────────┐
│  Data Status                                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Table              │ Rows      │ Earliest  │ Latest    │    │
│  │────────────────────┼───────────┼───────────┼───────────│    │
│  │ EQUITIES                                               │    │
│  │ eod_ohlcv          │ 2,450,000 │ 2015-01-05│ 2026-02-27│    │
│  │ company_info       │ 582       │     —     │ 2026-02-27│    │
│  │                                                        │    │
│  │ FIXED INCOME                                           │    │
│  │ kibor_daily        │ 2,664     │ 2024-01-02│ 2026-02-27│    │
│  │ konia_daily        │ 22        │ 2026-01-15│ 2026-02-27│ ⚠️│
│  │ pkrv_daily         │ 21        │ 2026-01-20│ 2026-02-27│ ⚠️│
│  │ tbill_auctions     │ 155       │ 2024-06-05│ 2026-02-26│    │
│  │ pib_auctions       │ 935       │ 2000-12-13│ 2026-02-19│    │
│  │ gis_auctions       │ 66        │ 2010-11-17│ 2023-12-20│    │
│  │ sbp_policy_rates   │ 1         │ 2026-01-27│ 2026-01-27│ ⚠️│
│  │ sbp_bond_trading   │ 0         │     —     │     —     │ 🔴│
│  │ sbp_benchmark      │ 0         │     —     │     —     │ 🔴│
│  │                                                        │    │
│  │ FUNDS                                                  │    │
│  │ mutual_funds       │ 1,190     │     —     │ 2026-02-27│    │
│  │ mutual_fund_nav    │ 1,900,000 │ 1996-02-01│ 2026-02-27│    │
│  │ fund_performance   │ 519       │ 2026-02-27│ 2026-02-27│    │
│  │                                                        │    │
│  │ FX                                                     │    │
│  │ fx_rates           │ 15,000    │ 2020-01-02│ 2026-02-27│    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  Legend: ✅ Fresh  ⚠️ Thin/Stale  🔴 Empty (needs first sync)   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## UX PRINCIPLES FOR THE WHOLE APP

### 1. Numbers Before Charts
Show the current rate/price/return as a BIG number first. Chart below.
Financial professionals want the number, not a decorative line chart.

### 2. Change is King
Every number shows its change: ▲/▼, green/red, vs yesterday/last week/last month.
Static numbers are meaningless in finance — delta is everything.

### 3. Freshness is Trust
Always show when data was last updated. Stale data kills trust.
A small "Last updated: 5:02 PM" under every section.

### 4. Drill-Down, Not Dump
Dashboard → Category → Individual item.
Don't show 1,190 funds on one page. Show 4 category cards, then filter.

### 5. Export Everything
Every table, every chart → [📥 Export CSV]. Financial users paste into Excel constantly.

### 6. Dark Theme
Financial terminals are dark. Your app should be dark. Use:
- Background: #0E1117 (Streamlit dark default)
- Green: #00D26A (positive returns)
- Red: #FF4B4B (negative returns)
- Accent: #4DA8DA (links, selected items)
- Muted: #6C757D (secondary text)

### 7. No Clutter
- No emoji in data tables
- No "Welcome to PakFinData!" headers
- No decorative separators
- Use st.metric() for key numbers — it handles delta natively
- Minimal sidebar — just navigation, no controls

### 8. Contextual Actions
Sync buttons live ON the page that shows the data, not in a separate admin page.
The Sync Center exists for power users who want to run everything at once.

### 9. Loading States
Every section that queries the DB shows a spinner.
Every sync button shows progress and result count.

### 10. Mobile Responsive
Streamlit is already responsive, but:
- Use st.columns() that collapse well (2-col on mobile, 4 on desktop)
- Metric cards in a row (they stack automatically)
- Tables with horizontal scroll for many columns

---

## WHAT TO BUILD / RESTRUCTURE

### Phase 1 — Dashboard + Navigation (immediate)
- Build the Dashboard home page with metric cards for each pillar
- Set up the sidebar navigation with the 5-group structure
- Add data freshness bar to dashboard

### Phase 2 — Consolidate Existing Pages
- Merge scattered fund pages into the Fund Explorer structure
- Merge rate-related pages into Fixed Income group
- Ensure every page has: header metrics, filters, data table, export

### Phase 3 — Add Missing Pages
- Rates Overview (the command center view above)
- Bond Market OTC (after SMTV scraper is built)
- Benchmark Monitor
- Sync Center (admin)
- Data Status (admin)

### Phase 4 — Polish
- Consistent color coding across all pages
- Every number shows delta
- Export buttons on every data table
- Loading states and error handling

---

## STREAMLIT IMPLEMENTATION NOTES

### Navigation Pattern
```python
# In app.py — use st.navigation (Streamlit 1.36+)
pages = {
    "Market Overview": [
        st.Page("pages/dashboard.py", title="Dashboard", icon="📊"),
        st.Page("pages/market_pulse.py", title="Market Pulse", icon="📈"),
    ],
    "Equities": [
        st.Page("pages/market_summary.py", title="Market Summary", icon="📈"),
        st.Page("pages/screener.py", title="Stock Screener", icon="🔍"),
        st.Page("pages/company.py", title="Company Profile", icon="🏢"),
        st.Page("pages/sectors.py", title="Sector Analysis", icon="📊"),
    ],
    "Fixed Income": [
        st.Page("pages/rates_overview.py", title="Rates Overview", icon="💰"),
        st.Page("pages/yield_curves.py", title="Yield Curves", icon="📉"),
        st.Page("pages/auctions.py", title="Treasury Auctions", icon="📋"),
        st.Page("pages/bond_market.py", title="Bond Market", icon="🏦"),
        st.Page("pages/benchmark.py", title="Benchmark Monitor", icon="📊"),
    ],
    "Funds": [
        st.Page("pages/fund_explorer.py", title="Fund Explorer", icon="🏦"),
        st.Page("pages/vps.py", title="VPS Pension", icon="🏦"),
        st.Page("pages/top_performers.py", title="Top Performers", icon="🏆"),
        st.Page("pages/fund_analytics.py", title="Fund Analytics", icon="📊"),
        st.Page("pages/etfs.py", title="ETFs", icon="📈"),
    ],
    "FX & Rates": [
        st.Page("pages/fx_dashboard.py", title="Currency Dashboard", icon="💱"),
    ],
    "Admin": [
        st.Page("pages/data_status.py", title="Data Status", icon="📊"),
        st.Page("pages/sync_center.py", title="Sync Center", icon="⚙️"),
        st.Page("pages/schema.py", title="Schema Explorer", icon="🗄️"),
    ],
}
pg = st.navigation(pages)
pg.run()
```

### Sync Button Pattern (reusable component)
```python
def sync_button(label: str, sync_func, key: str):
    """
    Reusable sync button with progress feedback.
    Place in top-right of any section.
    """
    col1, col2 = st.columns([8, 2])
    with col2:
        if st.button(f"🔄 {label}", key=key):
            with st.spinner(f"Syncing {label}..."):
                try:
                    result = sync_func()
                    st.success(f"✅ {result}")
                except Exception as e:
                    st.error(f"❌ {e}")
```

### Metric Card Pattern
```python
def metric_row(metrics: list[tuple]):
    """
    Display a row of st.metric cards.
    metrics = [("KSE-100", "82,450", "+1.2%"), ...]
    """
    cols = st.columns(len(metrics))
    for col, (label, value, delta) in zip(cols, metrics):
        col.metric(label, value, delta)
```

### Data Freshness Component
```python
def data_freshness_bar(con):
    """
    Show freshness of key data sources in a compact bar.
    """
    sources = [
        ("EOD", "SELECT MAX(date) FROM eod_ohlcv"),
        ("NAV", "SELECT MAX(nav_date) FROM mutual_fund_nav"),
        ("KIBOR", "SELECT MAX(date) FROM kibor_daily"),
        ("PKRV", "SELECT MAX(date) FROM pkrv_daily"),
        ("FX", "SELECT MAX(date) FROM fx_rates"),
        ("SMTV", "SELECT MAX(date) FROM sbp_bond_trading_daily"),
    ]
    cols = st.columns(len(sources))
    today = date.today().isoformat()
    for col, (name, query) in zip(cols, sources):
        try:
            latest = con.execute(query).fetchone()[0]
            if latest == today:
                col.markdown(f"**{name}**: ✅")
            elif latest and (date.today() - date.fromisoformat(latest)).days <= 2:
                col.markdown(f"**{name}**: ⚠️ {latest}")
            else:
                col.markdown(f"**{name}**: 🔴 {latest or 'empty'}")
        except:
            col.markdown(f"**{name}**: 🔴")
```

---

## CLI COMMAND SUMMARY (for reference)

```
pfsync eod sync              # Daily EOD prices
pfsync eod backfill          # Historical EOD

pfsync funds sync            # Fund NAVs
pfsync funds performance     # MUFAP tab=1 returns
pfsync funds expense         # MUFAP tab=5 expense ratios
pfsync funds resync-categories  # Fix all fund categories
pfsync funds backfill-returns   # Compute historical returns from NAV

pfsync rates sync            # KIBOR + KONIA + policy rate
pfsync rates pkrv            # PKRV yield curve

pfsync bonds smtv-sync       # SBP SMTV PDF
pfsync bonds benchmark-sync  # SBP benchmark snapshot
pfsync bonds smtv-backfill   # Historical SMTV from archives

pfsync auctions sync         # T-Bill + PIB + GIS auctions

pfsync fx sync               # FX rates

pfsync status                # Show all data freshness
pfsync sync-all              # Run everything in order
```
