# Claude Code Prompt: MUFAP Fund Explorer — Phase 2 Fix (Post-Audit)

## Context

Working on `psx_ohlcv` — Pakistan financial data platform (Streamlit app).
- DB: `/mnt/e/psxdata/psx.sqlite`  
- Project: `~/psx_ohlcv/` (dev branch)
- Python 3.11.14 at `/opt/miniconda/envs/psx/bin`

## Audit Summary (Phase 1 COMPLETE — do NOT re-investigate)

### What EXISTS and works:
- `mutual_funds` table: 1,190 funds, 25 columns (metadata)
- `mutual_fund_nav` table: 1.9M rows back to 1996
- Scraper (`mufap.py`): fetches Tab 1, Tab 3, API profiles, async batch NAV
- Fund Explorer UI: 3 tabs (Mutual Funds, ETFs, Top Performers)
- Funds UI page: directory, analytics, bulk sync
- CLI: 6 subcommands (seed, sync, show, list, rankings, status)
- `fetch_daily_performance_html()` ALREADY returns 519 funds with 36 categories including ALL missing ones

### What's BROKEN/MISSING (this prompt fixes ALL of these):

| # | Gap | Fix |
|---|-----|-----|
| 1 | `fund_performance` table doesn't exist | Create it — store tab=1 returns (YTD through 3Y) |
| 2 | `aum` column missing from `mutual_funds` | ALTER TABLE to add it |
| 3 | Only 17 of 36 MUFAP categories in DB | Fix `map_mufap_category()` — stop collapsing categories |
| 4 | Missing categories: Aggressive Fixed Income, SC Asset Allocation, VPS-Debt, VPS-Equity, VPS-Commodities/Gold, SC Balanced, SC FoF-CPPI, Dedicated Equity, Capital Protected-Income | Category mapping fix solves this |
| 5 | VPS section missing in UI | Add VPS tab/section — 88 VPS funds exist in MUFAP |
| 6 | Top Performers uses NAV-computed returns, not MUFAP official | Switch to `fund_performance` table data |
| 7 | Expense ratios (tab=5) not stored | Add scrape + store |
| 8 | Tab=1 raw suffix "(Annualized Return)" / "(Absolute Return)" in category names | Clean during mapping |

### CRITICAL FINDING:
`fetch_daily_performance_html()` already works and returns 519 funds with all 36 categories.
The data pipeline exists — it just needs a destination table and the category mapper needs to stop collapsing.

---

## STEP 1 — Fix Category Mapping

Find `map_mufap_category()` in the scraper and fix it:

```bash
grep -n "map_mufap_category\|category_map\|CATEGORY_MAP" ~/psx_ohlcv/src/psx_ohlcv/sources/mufap.py
```

**Current behavior (BROKEN):** Collapses categories like "Aggressive Fixed Income" → "Income", "Shariah Compliant Dedicated Equity" → "Equity", etc.

**New behavior:** Preserve MUFAP's actual categories. Clean only the suffix noise:

```python
def map_mufap_category(raw_category: str) -> str:
    """
    Clean MUFAP category name — preserve granularity, remove display suffixes.
    
    Raw examples from tab=1:
      "Money Market (Annualized Return )" → "Money Market"
      "Equity (Absolute Return )" → "Equity"  
      "Shariah Compliant Commodity (Absolute Return )" → "Shariah Compliant Commodity"
      "VPS-Shariah Compliant Equity (Absolute Return )" → "VPS-Shariah Compliant Equity"
      "Aggressive Fixed Income (Annualized Return )" → "Aggressive Fixed Income"
    
    DO NOT collapse categories. "Aggressive Fixed Income" stays as-is, NOT mapped to "Income".
    """
    import re
    # Strip the "(Annualized Return )" or "(Absolute Return )" suffix
    cleaned = re.sub(r'\s*\((?:Annualized|Absolute)\s+Return\s*\)\s*$', '', raw_category).strip()
    return cleaned
```

This should yield the full 36 categories in DB instead of 17.

**Implementation:**
1. Find the existing mapping function
2. Replace it with the above logic
3. Check if the old mapping is used in sync/seed flows — update all call sites
4. Do NOT change historical data — the fix is forward-looking
5. Consider a one-time re-sync to populate correct categories for existing funds

---

## STEP 2 — Create `fund_performance` Table

```sql
CREATE TABLE IF NOT EXISTS fund_performance (
    fund_name TEXT NOT NULL,
    fund_id INTEGER,                -- MUFAP FundID (from href in tab=1)
    sector TEXT,                    -- 'Open-End Funds', 'Voluntary Pension Scheme (VPS)', 'Dedicated Equity Funds', 'Employer Pension Funds'
    category TEXT,                  -- cleaned MUFAP category (36 values)
    rating TEXT,                    -- e.g., 'AA+(f)', 'AAA(f)'
    benchmark TEXT,
    validity_date TEXT NOT NULL,    -- MUFAP's reported date
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
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (fund_name, validity_date)
);

CREATE INDEX IF NOT EXISTS idx_fund_perf_date ON fund_performance(validity_date);
CREATE INDEX IF NOT EXISTS idx_fund_perf_category ON fund_performance(category);
CREATE INDEX IF NOT EXISTS idx_fund_perf_sector ON fund_performance(sector);
CREATE INDEX IF NOT EXISTS idx_fund_perf_id ON fund_performance(fund_id);
```

**Query functions needed** (add to existing fund DB module):
- `upsert_fund_performance(con, records: list[dict]) -> int` — bulk upsert
- `get_fund_performance(con, date=None, category=None, sector=None) -> pd.DataFrame`
- `get_top_performers(con, period='return_ytd', n=10, category=None) -> pd.DataFrame`
- `get_fund_returns(con, fund_name, start_date=None) -> pd.DataFrame` — time series of a fund's returns
- `get_category_summary(con, date=None) -> pd.DataFrame` — avg return per category
- `get_vps_funds(con, date=None) -> pd.DataFrame` — VPS sector funds only

**Handle N/A:** MUFAP shows "N/A" for funds without certain return periods. Parse as NULL/None, NOT string "N/A".

---

## STEP 3 — ALTER `mutual_funds` Table

```bash
# Check current schema first
sqlite3 /mnt/e/psxdata/psx.sqlite ".schema mutual_funds"
```

Add missing columns (only if they don't exist — check schema output first):

```sql
ALTER TABLE mutual_funds ADD COLUMN aum REAL;              -- AUM in PKR millions
ALTER TABLE mutual_funds ADD COLUMN expense_ratio REAL;    -- from tab=5
ALTER TABLE mutual_funds ADD COLUMN front_load REAL;       -- from tab=3
ALTER TABLE mutual_funds ADD COLUMN back_load REAL;        -- from tab=3
ALTER TABLE mutual_funds ADD COLUMN rating TEXT;            -- from tab=1
ALTER TABLE mutual_funds ADD COLUMN benchmark TEXT;         -- from tab=1
ALTER TABLE mutual_funds ADD COLUMN risk_profile TEXT;      -- from Fund Directory
ALTER TABLE mutual_funds ADD COLUMN fund_id INTEGER;        -- MUFAP FundID
ALTER TABLE mutual_funds ADD COLUMN sector TEXT;            -- 'Open-End', 'VPS', etc.
```

**IMPORTANT:** Use `ALTER TABLE ... ADD COLUMN` with try/except or check `PRAGMA table_info(mutual_funds)` first — SQLite will error if column already exists.

---

## STEP 4 — Wire `fetch_daily_performance_html()` to Persistence

The function already exists and returns the data. Find it:

```bash
grep -n "fetch_daily_performance\|daily_performance" ~/psx_ohlcv/src/psx_ohlcv/sources/mufap.py
```

Then wire it into the sync flow:

```python
def sync_performance(self, con) -> dict:
    """
    Fetch MUFAP Performance Summary (tab=1) and persist to fund_performance table.
    Also updates mutual_funds with rating, benchmark, sector from this data.
    """
    # 1. Call existing fetch_daily_performance_html()
    # 2. Parse HTML → list of dicts with cleaned categories
    # 3. Upsert into fund_performance table
    # 4. Update mutual_funds metadata (rating, benchmark, sector, category)
    # 5. Return stats: {funds_synced: N, categories: N, date: "..."}
```

Also add expense ratio sync:

```python
def sync_expense_ratios(self, con) -> dict:
    """
    Fetch MUFAP Expense Ratios (tab=5) and update mutual_funds.expense_ratio.
    """
    # 1. Fetch https://www.mufap.com.pk/Industry/IndustryStatDaily?tab=5
    # 2. Parse HTML table
    # 3. UPDATE mutual_funds SET expense_ratio = ? WHERE fund_name = ?
```

---

## STEP 5 — Enhance Fund Explorer UI

### 5A — Add VPS Tab

The Fund Explorer currently has 3 tabs: Mutual Funds, ETFs, Top Performers.
Add a 4th tab: **VPS Pension Funds**.

```python
# In the fund explorer page, add tab:
tab_mf, tab_etf, tab_vps, tab_top = st.tabs([
    "📊 Mutual Funds", "📈 ETFs", "🏦 VPS Pension", "🏆 Top Performers"
])
```

**VPS Tab content:**
- Filter by AMC
- Group VPS funds by AMC → show equity/debt/money market/gold sub-funds side by side
- Table: Fund Name | Category | NAV | YTD | 1Y | 3Y
- Comparison: "Which AMC's pension equity fund performs best?"
- Highlight gold/commodity sub-funds if they exist

### 5B — Fix Top Performers Tab

Currently uses NAV-computed returns. Switch to MUFAP's official returns from `fund_performance`:

```python
# BEFORE (broken — computing from NAV):
# returns = compute_returns_from_nav(...)

# AFTER (correct — using MUFAP's official returns):
df = get_top_performers(con, period=selected_period, n=20, category=selected_category)
```

Add period selector: YTD | MTD | 1M | 3M | 6M | 1Y | 2Y | 3Y
Add category filter dropdown (all 36 categories)

### 5C — Fix Category Filter in Mutual Funds Tab

Replace the current category dropdown with the full 36 categories.
Group categories logically:

```python
category_groups = {
    "Conventional": ["Money Market", "Income", "Aggressive Fixed Income", "Equity", 
                     "Balanced", "Asset Allocation", "Fund of Funds", 
                     "Capital Protected", "Fixed Rate / Return", "Commodity"],
    "Shariah Compliant": ["Shariah Compliant Money Market", "Shariah Compliant Income", 
                          "Shariah Compliant Equity", "Shariah Compliant Balanced",
                          "Shariah Compliant Asset Allocation", "Shariah Compliant Fund of Funds",
                          "Shariah Compliant Commodity", "Shariah Compliant Capital Protected",
                          "Shariah Compliant Dedicated Equity", "Shariah Compliant Fixed Rate / Return",
                          "Shariah Compliant Aggressive Fixed Income",
                          "Shariah Compliant Fund of Funds - CPPI"],
    "VPS": ["VPS-Equity", "VPS-Debt", "VPS-Money Market", 
            "VPS-Shariah Compliant Equity", "VPS-Shariah Compliant Debt",
            "VPS-Shariah Compliant Money Market", "VPS-Commodities"],
    "Other": ["Dedicated Equity", "Capital Protected - Income"]
}
```

### 5D — Add Category Summary Cards

At the top of Mutual Funds tab, show summary metric cards:

```python
# For each major category group: fund count, avg YTD, best performer
cols = st.columns(4)
cols[0].metric("Equity Funds", f"{equity_count}", f"Avg YTD: {avg_equity_ytd:.1f}%")
cols[1].metric("Income Funds", f"{income_count}", f"Avg YTD: {avg_income_ytd:.1f}%")
cols[2].metric("Islamic Funds", f"{islamic_count}", f"Avg YTD: {avg_islamic_ytd:.1f}%")
cols[3].metric("VPS Pension", f"{vps_count}", f"Avg YTD: {avg_vps_ytd:.1f}%")
```

### 5E — Add Performance Columns to Fund Table

Currently shows: Name, AMC, Category, NAV, etc.
Add from `fund_performance`: 1M Return, 3M Return, YTD, 1Y

Color-code returns: green for positive, red for negative (use Streamlit column_config).

---

## STEP 6 — Update CLI

Add to existing fund CLI subcommands:

```
psxsync funds performance     # Sync MUFAP Performance Summary (tab=1) → fund_performance table
psxsync funds expense         # Sync expense ratios (tab=5) → mutual_funds.expense_ratio
```

Modify existing `psxsync funds sync` to also call `sync_performance()` and `sync_expense_ratios()`.

---

## STEP 7 — One-Time Category Re-sync

After fixing `map_mufap_category()`, do a one-time re-sync to fix categories for existing 1,190 funds:

```python
# In sync flow or as a migration:
# 1. Fetch fresh Fund Directory from MUFAP
# 2. Update mutual_funds.category with corrected mapping for all existing funds
# 3. Also populate: sector, risk_profile, fund_id from directory data
```

This ensures the 1,190 existing funds get proper categories, not just new syncs.

---

## STEP 8 — Verify

```bash
# 8A — Run performance sync
python -m psx_ohlcv funds performance 2>&1

# 8B — Check fund_performance table
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT category, COUNT(*) as funds, 
       ROUND(AVG(return_ytd),2) as avg_ytd,
       ROUND(MIN(return_ytd),2) as worst,
       ROUND(MAX(return_ytd),2) as best
FROM fund_performance
WHERE validity_date = (SELECT MAX(validity_date) FROM fund_performance)
GROUP BY category ORDER BY avg_ytd DESC;
"

# 8C — Verify all 36 categories present
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT COUNT(DISTINCT category) as unique_categories FROM fund_performance;
"
# Expected: ~36 (or close, depending on active funds)

# 8D — Check Commodity funds
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT fund_name, category, nav, return_ytd, return_1y
FROM fund_performance 
WHERE category LIKE '%Commodity%' OR category LIKE '%Gold%'
AND validity_date = (SELECT MAX(validity_date) FROM fund_performance);
"

# 8E — Check VPS funds
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT fund_name, category, nav, return_ytd
FROM fund_performance 
WHERE category LIKE 'VPS%'
AND validity_date = (SELECT MAX(validity_date) FROM fund_performance)
ORDER BY category, return_ytd DESC;
"

# 8F — Verify mutual_funds categories updated
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT category, COUNT(*) FROM mutual_funds GROUP BY category ORDER BY COUNT(*) DESC;
"
# Expected: ~25+ categories (was 17 before fix)

# 8G — Verify expense ratios populated
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT COUNT(*) as with_expense FROM mutual_funds WHERE expense_ratio IS NOT NULL;
"

# 8H — Start UI and verify
streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5

# 8I — Tests
pytest tests/ -x -q --tb=short -k "fund or mufap" 2>&1 | tail -10
```

---

## CRITICAL RULES

1. **`fetch_daily_performance_html()` already works** — do NOT rewrite the fetcher. Just add persistence.
2. **Fix the category mapper, don't add a new one** — find `map_mufap_category()`, replace its logic in-place.
3. **ALTER TABLE safely** — check `PRAGMA table_info()` before adding columns. SQLite errors on duplicate ADD COLUMN.
4. **N/A → NULL** — MUFAP shows "N/A" for missing returns. Store as NULL, not string.
5. **Match existing patterns** — use the same upsert style, logging, error handling as the existing fund sync.
6. **Dark theme** — all new UI matches existing app theme.
7. **Page isolation** — st.navigation() pattern, single render function per page file.
8. **Don't break existing NAV sync** — the 1.9M row NAV table and its sync flow must remain untouched.
9. **Performance data is a DAILY SNAPSHOT** — store each day's returns separately (PK: fund_name + validity_date). Don't overwrite — accumulate. This enables "what were the top performers last month" queries.

## GIT

```bash
git add -A
git commit -m "feat: MUFAP fund explorer — full 36-category coverage + performance persistence

  FIXES:
  - Category mapper: preserve all 36 MUFAP categories (was collapsing to 17)
  - Categories now include: Commodity, VPS-Gold, Aggressive FI, Dedicated Equity, etc.
  
  NEW:
  - fund_performance table: daily return snapshots (YTD through 3Y) for 519 funds
  - Expense ratio sync from MUFAP tab=5
  - VPS Pension tab in Fund Explorer UI
  - Top Performers now uses MUFAP official returns
  - Category summary cards + full category filter
  - mutual_funds extended: aum, expense_ratio, rating, benchmark, risk_profile, fund_id
  
  CLI: psxsync funds performance / expense"

git push origin dev
```
