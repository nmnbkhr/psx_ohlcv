# Claude Code Prompt: MUFAP Fund Explorer — Audit, Enhance, Complete

## Context

I'm working on `psx_ohlcv` — a Pakistan financial data platform (Streamlit app).
- DB: `/mnt/e/psxdata/psx.sqlite`
- Project: `~/psx_ohlcv/` (dev branch)
- The app has a "Fund Explorer" page in the Streamlit UI that shows mutual fund data
- Data source: MUFAP (mufap.com.pk) — the official industry body for mutual funds in Pakistan
- The fund explorer currently has LIMITED information — some categories may be missing
  (e.g., Commodity funds, VPS pension funds, dedicated equity, gold sub-funds, etc.)

## PROBLEM STATEMENT

The Fund Explorer page is incomplete. Possible issues:
1. Missing fund categories — MUFAP has 25+ categories, we may only be scraping a subset
2. Limited fund metadata — missing AUM, expense ratio, benchmark, risk profile, rating
3. Missing performance data — MUFAP publishes daily returns (YTD, MTD, 1D through 3Y)
4. No VPS pension fund section
5. No fund comparison or ranking features
6. Scraper may not be capturing all tabs from MUFAP (Performance, NAVs, Payouts, Expense Ratios)

---

## PHASE 1 — DEEP AUDIT (INVESTIGATE EVERYTHING FIRST)

### Step 1 — Audit current fund-related tables

```bash
# Find ALL fund/mutual/mufap related tables
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' 
AND (name LIKE '%fund%' OR name LIKE '%mutual%' OR name LIKE '%mufap%' 
     OR name LIKE '%nav%' OR name LIKE '%vps%' OR name LIKE '%pension%'
     OR name LIKE '%etf%' OR name LIKE '%amc%')
ORDER BY name;
"

# For EACH fund table — show schema, row count, date range, sample data
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%fund%' OR name LIKE '%mutual%' OR name LIKE '%mufap%' OR name LIKE '%nav%' OR name LIKE '%vps%' OR name LIKE '%pension%' OR name LIKE '%etf%' OR name LIKE '%amc%');"); do
  echo "════════════════════════════════════════"
  echo "TABLE: $tbl"
  echo "════════════════════════════════════════"
  sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl"
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) as total_rows FROM $tbl;"
  # Try common date column names
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT MIN(date) as earliest, MAX(date) as latest FROM $tbl;" 2>/dev/null
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT MIN(nav_date) as earliest, MAX(nav_date) as latest FROM $tbl;" 2>/dev/null
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT MIN(validity_date) as earliest, MAX(validity_date) as latest FROM $tbl;" 2>/dev/null
  echo "--- Sample rows (latest 5) ---"
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT * FROM $tbl ORDER BY rowid DESC LIMIT 5;"
  echo ""
done
```

### Step 2 — Check what fund CATEGORIES currently exist

```bash
# What categories/types does our DB have?
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT category, COUNT(*) as fund_count 
FROM mutual_funds 
GROUP BY category ORDER BY fund_count DESC;
" 2>/dev/null || echo "Table 'mutual_funds' not found, trying alternatives..."

# Try other possible column names
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT fund_type, COUNT(*) as cnt FROM mutual_funds GROUP BY fund_type ORDER BY cnt DESC;
" 2>/dev/null

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT fund_category, COUNT(*) as cnt FROM mutual_funds GROUP BY fund_category ORDER BY cnt DESC;
" 2>/dev/null

# Check what sectors/types exist
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT sector, COUNT(*) as cnt FROM mutual_funds GROUP BY sector ORDER BY cnt DESC;
" 2>/dev/null

# Check for AMC coverage
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT amc, COUNT(*) as fund_count FROM mutual_funds GROUP BY amc ORDER BY fund_count DESC;
" 2>/dev/null || sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT amc_name, COUNT(*) as fund_count FROM mutual_funds GROUP BY amc_name ORDER BY fund_count DESC;
" 2>/dev/null
```

### Step 3 — Compare against MUFAP's FULL category list

MUFAP has these SECP-approved fund categories (from their Fund Directory):

**Open-End Funds (Conventional):**
- Money Market
- Income
- Aggressive Fixed Income
- Asset Allocation
- Balanced
- Equity
- Fund of Funds
- Capital Protected
- Fixed Rate / Return
- Commodity

**Open-End Funds (Shariah Compliant):**
- Shariah Compliant Money Market
- Shariah Compliant Income
- Shariah Compliant Aggressive Fixed Income
- Shariah Compliant Asset Allocation
- Shariah Compliant Balanced
- Shariah Compliant Equity
- Shariah Compliant Fund of Funds
- Shariah Compliant Fund of Funds - CPPI
- Shariah Compliant Capital Protected
- Shariah Compliant Fixed Rate / Return
- Shariah Compliant Dedicated Equity
- Shariah Compliant Commodity

**Dedicated Equity Funds**

**Voluntary Pension Scheme (VPS):**
- VPS-Equity / VPS-Shariah Compliant Equity
- VPS-Debt / VPS-Shariah Compliant Debt
- VPS-Money Market / VPS-Shariah Compliant Money Market
- VPS-Commodity (Gold sub-fund)

**Employer Pension Funds**

```bash
# Check which of these categories we're MISSING
# (Compare the list above with what Step 2 shows)
echo "=== EXPECTED vs ACTUAL CATEGORIES ==="
echo "Check output of Step 2 against the full MUFAP list above."
echo "Key categories likely missing: Commodity, VPS-*, Dedicated Equity, Employer Pension"
```

### Step 4 — Audit the existing MUFAP scraper

```bash
# Find the scraper file(s)
find ~/psx_ohlcv/src/ -name "*.py" | xargs grep -l "mufap\|MUFAP\|mutual.*fund\|IndustryStatDaily\|FundDirectory" 2>/dev/null

# Show the full scraper source
echo "=== MUFAP Scraper Source ==="
for f in $(find ~/psx_ohlcv/src/ -name "*.py" | xargs grep -l "mufap\|MUFAP" 2>/dev/null); do
  echo "--- FILE: $f ---"
  cat "$f"
  echo ""
done

# Check what MUFAP URLs are being scraped
grep -rn "mufap.com\|MUFAP.*URL\|IndustryStatDaily\|FundDirectory\|FundDetail\|FundProfile" ~/psx_ohlcv/src/ 2>/dev/null
```

### Step 5 — Audit the Fund Explorer UI page

```bash
# Find the fund explorer Streamlit page
find ~/psx_ohlcv/src/ -name "*.py" | xargs grep -l "fund.*explorer\|Fund.*Explorer\|mutual.*fund.*page\|fund_explorer" 2>/dev/null

# Show the full page source
echo "=== Fund Explorer Page Source ==="
for f in $(find ~/psx_ohlcv/src/psx_ohlcv/ui/pages/ -name "*fund*" -o -name "*mutual*"); do
  echo "--- FILE: $f ---"
  cat "$f"
  echo ""
done

# Check what DB queries the fund page makes
grep -rn "mutual_fund\|fund_nav\|fund_performance\|fund_category\|SELECT.*FROM.*fund" ~/psx_ohlcv/src/psx_ohlcv/ui/pages/ 2>/dev/null
```

### Step 6 — Check fund-related DB query functions

```bash
# Find all fund query functions in db/ layer
grep -rn "def.*fund\|def.*nav\|def.*mufap\|def.*mutual" ~/psx_ohlcv/src/psx_ohlcv/db/ 2>/dev/null

# Show the fund DB module
for f in $(find ~/psx_ohlcv/src/psx_ohlcv/db/ -name "*fund*" -o -name "*mutual*" -o -name "*mufap*" -o -name "*nav*"); do
  echo "--- FILE: $f ---"
  cat "$f"
  echo ""
done
```

### Step 7 — Test the current scraper

```bash
# Check if fund sync CLI command exists
grep -rn "fund.*sync\|mufap.*sync\|nav.*sync\|fund_sync" ~/psx_ohlcv/src/psx_ohlcv/cli/ 2>/dev/null

# Try running it
python -m psx_ohlcv fund sync 2>&1 | head -20 || \
python -m psx_ohlcv mufap sync 2>&1 | head -20 || \
python -m psx_ohlcv --help 2>&1 | grep -i fund
```

**STOP HERE — Show me ALL output from Steps 1-7 before proceeding.**

---

## PHASE 2 — FIX THE SCRAPER (based on Phase 1 findings)

### What MUFAP provides (5 daily data pages we should scrape):

**Tab 1: Performance Summary** (`/Industry/IndustryStatDaily?tab=1`)
```
HTML table with: Sector, Category, Fund Name (with link to FundID), Rating, 
Benchmark, Validity Date, NAV, YTD, MTD, 1D, 15D, 30D, 90D, 180D, 270D, 365D, 2Y, 3Y returns
```
This is the RICHEST single page — has performance data for ALL funds across ALL categories.

**Tab 3: NAVs and Sale Loads** (`/Industry/IndustryStatDaily?tab=3`)
```
HTML table with: Fund Name, NAV Date, NAV, Offer Price, Redemption Price,
Front-End Load %, Back-End Load %
```

**Tab 4: Payouts** (`/Industry/IndustryStatDaily?tab=4`)
```
Dividend/payout announcements by fund
```

**Tab 5: Expense Ratios** (`/Industry/IndustryStatDaily?tab=5`)
```
Expense ratio for each fund (annual management fee percentage)
```

**Fund Directory** (`/FundProfile/FundDirectory`)
```
Complete fund listing with: AMC, NAV, Offer Price, Category, Risk Profile, Sector (Open-End/VPS/Employer/Dedicated)
FundID for each fund (e.g., FundID=12768)
```

**Fund Detail** (`/FundProfile/FundDetail?FundID={ID}`)
```
Individual fund page with: full profile, NAV history, benchmark comparison,
category, risk profile, AMC, launch date, AUM, investment policy, etc.
```

### Step 8 — Enhance scraper to capture ALL data

Based on Phase 1 findings, extend the MUFAP scraper to:

1. **Scrape Performance Summary (tab=1)** — this single page gives us returns for ALL funds:
   - Parse the HTML table
   - Extract: sector, category, fund_name, fund_id (from href), rating, benchmark, 
     validity_date, nav, ytd, mtd, 1d, 15d, 30d, 90d, 180d, 270d, 365d, 2y, 3y
   - This table has ALL categories including Commodity, VPS, Dedicated Equity, etc.

2. **Scrape NAVs + Loads (tab=3)** — pricing data:
   - nav, offer_price, redemption_price, front_load_pct, back_load_pct

3. **Scrape Expense Ratios (tab=5)** — cost data:
   - expense_ratio_pct per fund

4. **Scrape Fund Directory** — master fund list with metadata:
   - amc_name, category, risk_profile, sector (Open-End/VPS/Dedicated/Employer)
   - fund_id for linking

### Step 9 — Update DB schema (extend, don't replace)

Based on what Phase 1 reveals, ADD missing columns or tables. Common gaps:

```sql
-- If mutual_funds table exists but lacks columns, ALTER TABLE:
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS rating TEXT;
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS benchmark TEXT;
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS risk_profile TEXT;
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS sector TEXT;          -- 'Open-End Funds', 'VPS', 'Dedicated Equity', 'Employer Pension'
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS expense_ratio REAL;
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS front_load REAL;
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS back_load REAL;
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS fund_id INTEGER;      -- MUFAP FundID for detail page link
ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS aum REAL;             -- AUM in PKR millions

-- Performance returns table (daily snapshot of returns across all periods)
CREATE TABLE IF NOT EXISTS fund_performance (
    fund_name TEXT NOT NULL,
    fund_id INTEGER,
    validity_date TEXT NOT NULL,
    nav REAL,
    return_ytd REAL,
    return_mtd REAL,
    return_1d REAL,
    return_15d REAL,
    return_30d REAL,
    return_90d REAL,
    return_180d REAL,
    return_270d REAL,
    return_365d REAL,
    return_2y REAL,
    return_3y REAL,
    PRIMARY KEY (fund_name, validity_date)
);

CREATE INDEX IF NOT EXISTS idx_fund_perf_date ON fund_performance(validity_date);
CREATE INDEX IF NOT EXISTS idx_fund_perf_id ON fund_performance(fund_id);
```

NOTE: Check Phase 1 output first — if tables already have these columns, skip the ALTER.
If there's already a performance table, extend it rather than creating a duplicate.

### Step 10 — Update the Fund Explorer UI page

Rebuild the Fund Explorer with these sections:

**A. Category Filter Sidebar:**
```
- Sector: Open-End | VPS | Dedicated Equity | Employer Pension | All
- Category: [Full dropdown of 25+ MUFAP categories]
  Including: Commodity, Shariah Compliant Commodity, VPS-Commodity (Gold),
  Aggressive Fixed Income, Capital Protected, Fixed Rate/Return, etc.
- AMC: [All AMCs dropdown]
- Shariah: Conventional | Shariah Compliant | All
- Risk Profile: Very Low | Low | Medium | High | All
- Search: Free text search by fund name
```

**B. Fund Table (main content):**
```
Columns: Fund Name | AMC | Category | NAV | 1M Return | 3M Return | YTD | 1Y | Rating | AUM
- Sortable by any column
- Color-coded returns (green positive, red negative)
- Click fund name → expander with detail view
- Show total fund count matching filters
```

**C. Category Summaries (tabs or cards at top):**
```
For each major category show:
- Number of funds
- Average YTD return
- Best/worst performer
- Total AUM
Cards for: Equity | Income | Money Market | Balanced | Islamic | Commodity | VPS
```

**D. Top Performers Section:**
```
- Top 10 funds by: YTD | 1Y | 3Y return
- Filter by category
- Sortable
```

**E. Fund Detail Expander (when clicking a fund):**
```
- Full metadata: AMC, category, benchmark, risk profile, rating
- NAV chart (if NAV history exists in DB)
- Return bars: visual bar chart of 1D/15D/30D/90D/180D/1Y/2Y/3Y
- Loads: front-end, back-end, expense ratio
- Link to MUFAP fund page
```

**F. VPS Pension Section (separate tab or section):**
```
- Group by AMC → show equity/debt/money market sub-funds side by side
- Compare: Which AMC's pension fund performed best?
- Gold/Commodity sub-fund section (if data exists)
```

### Step 11 — CLI commands

```
psxsync funds sync              # Sync all MUFAP data (performance + NAVs + directory)
psxsync funds performance       # Sync performance tab only
psxsync funds directory         # Sync fund directory/metadata
psxsync funds status            # Show: X funds, Y categories, latest date, coverage
psxsync funds list --category "Commodity"    # List funds by category
psxsync funds top --period ytd --n 10        # Top performers
```

---

## PHASE 3 — VERIFY

```bash
# Step 12 — Run the enhanced sync
python -m psx_ohlcv funds sync 2>&1

# Step 13 — Verify data completeness
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT sector, category, COUNT(*) as funds, 
       AVG(return_ytd) as avg_ytd,
       MIN(return_ytd) as worst_ytd,
       MAX(return_ytd) as best_ytd
FROM fund_performance fp
JOIN mutual_funds mf ON fp.fund_name = mf.fund_name
WHERE fp.validity_date = (SELECT MAX(validity_date) FROM fund_performance)
GROUP BY sector, category
ORDER BY sector, category;
"

# Step 14 — Check all MUFAP categories are captured
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT DISTINCT category, COUNT(*) as cnt 
FROM mutual_funds 
GROUP BY category ORDER BY cnt DESC;
"
echo "---"
echo "Expected categories: ~25+ (Money Market, Income, Equity, Balanced,"
echo "  Asset Allocation, Commodity, Fund of Funds, Capital Protected,"
echo "  Fixed Rate/Return, Aggressive Fixed Income, Dedicated Equity,"
echo "  + Shariah versions of each, + VPS sub-funds)"

# Step 15 — Verify Commodity funds specifically
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT fund_name, category, nav, return_ytd 
FROM fund_performance fp
JOIN mutual_funds mf ON fp.fund_name = mf.fund_name
WHERE mf.category LIKE '%Commodity%' OR mf.category LIKE '%Gold%'
ORDER BY return_ytd DESC;
"

# Step 16 — Verify VPS pension funds
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT fund_name, category, nav, return_ytd
FROM fund_performance fp
JOIN mutual_funds mf ON fp.fund_name = mf.fund_name
WHERE mf.sector LIKE '%VPS%' OR mf.sector LIKE '%Pension%'
ORDER BY category, return_ytd DESC;
"

# Step 17 — Start Streamlit and verify UI
streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5

# Step 18 — Run tests
pytest tests/ -x -q --tb=short -k "fund or mufap or nav" 2>&1 | tail -10
```

---

## CRITICAL RULES

1. **INVESTIGATE FIRST** — Phase 1 output determines everything. Do NOT assume table names, column names, or what exists.
2. **EXTEND, DON'T REPLACE** — The existing MUFAP scraper works for what it does. Add to it, don't rewrite from scratch.
3. **Performance Summary (tab=1) is the KEY page** — One scrape gives ALL funds with ALL return periods. If you can only scrape ONE page, scrape this one.
4. **Category completeness is the main gap** — The user specifically mentioned "commodity" is missing. Ensure ALL 25+ MUFAP categories appear, including: Commodity, Shariah Compliant Commodity, VPS-Commodity (Gold), Aggressive Fixed Income, Dedicated Equity, Capital Protected, Fixed Rate/Return.
5. **Handle N/A values** — MUFAP shows "N/A" for funds without certain return periods. Store as NULL, not string.
6. **Fund IDs matter** — Extract FundID from the MUFAP links (e.g., FundDetail?FundID=12768). This enables linking to individual fund detail pages later.
7. **Match existing code patterns** — Use the same session management, error handling, DB access patterns as existing scrapers.
8. **Dark theme** — UI must match the app's existing dark theme.
9. **st.navigation() page isolation** — Single render function per page, no import side effects.
10. **Empty state handling** — If a category has no funds (e.g., Commodity category returns 0 results), show an info message, not an error.

## GIT

```bash
git add -A
git commit -m "feat: enhanced MUFAP fund explorer — full category coverage + performance data

  SCRAPER:
  - Scrape MUFAP Performance Summary (tab=1) — all funds with YTD through 3Y returns
  - Scrape NAVs + Sale Loads (tab=3) — pricing with front/back loads
  - Scrape Expense Ratios (tab=5)
  - Scrape Fund Directory — full metadata with FundIDs
  - Coverage: 25+ SECP categories including Commodity, VPS, Dedicated Equity
  
  DB:
  - fund_performance table with daily return snapshots
  - Extended mutual_funds with rating, benchmark, risk_profile, expense_ratio, AUM
  
  UI:
  - Rebuilt Fund Explorer with category filters, performance tables, VPS section
  - Top performers rankings, fund comparison, return visualizations
  - Full category sidebar including Commodity, Islamic, VPS sub-funds
  
  CLI: psxsync funds sync / performance / directory / status / top"

git push origin dev
```
