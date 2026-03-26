# Claude Code Prompt: MUFAP Fund Explorer — Audit Current State

## DO NOT CHANGE ANY CODE. THIS IS AN AUDIT ONLY.

Read everything, report findings, suggest what's needed. No edits.

## Step 1: Find all fund-related files

```bash
echo "=== Fund/MUFAP related files ==="
find ~/psx_ohlcv -name "*.py" -not -path "*/.venv/*" -not -path "*__pycache__*" | \
  xargs grep -l "mufap\|MUFAP\|mutual.*fund\|fund.*nav\|fund_explorer\|FundProfile\|FundDirectory\|IndustryStatDaily" 2>/dev/null

echo ""
echo "=== Fund UI pages ==="
find ~/psx_ohlcv -name "*fund*" -not -path "*/.venv/*" -not -path "*__pycache__*"
```

## Step 2: Check DB schema — what fund tables exist and what columns

```python
import sqlite3

con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")

# All fund/nav/mufap tables
tables = con.execute("""
    SELECT name FROM sqlite_master 
    WHERE type='table' 
    AND (name LIKE '%fund%' OR name LIKE '%nav%' OR name LIKE '%mufap%' 
         OR name LIKE '%mutual%' OR name LIKE '%amc%')
""").fetchall()

print("=== FUND-RELATED TABLES ===")
for t in tables:
    name = t[0]
    count = con.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
    cols = con.execute(f"PRAGMA table_info([{name}])").fetchall()
    print(f"\n📋 {name}: {count:,} rows")
    for c in cols:
        print(f"   {c[1]} ({c[2]})")

# Sample data from each table
print("\n=== SAMPLE DATA ===")
for t in tables:
    name = t[0]
    print(f"\n--- {name} (first 3 rows) ---")
    rows = con.execute(f"SELECT * FROM [{name}] LIMIT 3").fetchall()
    cols = [c[1] for c in con.execute(f"PRAGMA table_info([{name}])").fetchall()]
    for row in rows:
        print(dict(zip(cols, row)))

con.close()
```

## Step 3: Check what MUFAP data is being scraped

Read ALL scraper files found in Step 1. For each, report:
- What URL(s) does it scrape?
- What data does it extract? (fields, categories)
- How does it store data? (which table, insert/upsert logic)
- Does it do incremental sync or full re-scrape?
- What MUFAP tabs does it cover? (Tab 1: Performance, Tab 2: NAV, Tab 3: AUM, Tab 4: Returns, Tab 5: Expense)

## Step 4: Check the Fund Explorer UI page

Read the full fund explorer Streamlit page. Report:
- What data does it display?
- What filters are available? (category, AMC, date range)
- What charts/visualizations exist?
- What analytics are calculated? (returns, volatility, Sharpe, etc.)
- What's missing compared to the target analytics below?

## Step 5: Check fund categories coverage

```python
import sqlite3

con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")

# Find the main fund table (could be mutual_funds, funds, fund_nav, etc.)
# Check distinct categories/types
for tbl in [t[0] for t in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
    cols = [c[1] for c in con.execute(f"PRAGMA table_info([{tbl}])").fetchall()]
    cat_cols = [c for c in cols if any(k in c.lower() for k in ['category', 'type', 'sector', 'class', 'fund_type'])]
    if cat_cols and any(k in tbl.lower() for k in ['fund', 'nav', 'mufap', 'mutual']):
        for cc in cat_cols:
            print(f"\n📊 {tbl}.{cc} — distinct values:")
            vals = con.execute(f"SELECT DISTINCT [{cc}], COUNT(*) FROM [{tbl}] GROUP BY [{cc}] ORDER BY COUNT(*) DESC").fetchall()
            for v in vals:
                print(f"   {v[0]}: {v[1]:,}")

con.close()
```

## Step 6: Identify the NAV history depth

```python
import sqlite3

con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")

# Find NAV history table and check date range
for tbl in [t[0] for t in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
    cols = [c[1] for c in con.execute(f"PRAGMA table_info([{tbl}])").fetchall()]
    if any('nav' in c.lower() for c in cols) and any('date' in c.lower() for c in cols):
        date_col = [c for c in cols if 'date' in c.lower()][0]
        nav_col = [c for c in cols if 'nav' in c.lower()][0]
        r = con.execute(f"SELECT MIN([{date_col}]), MAX([{date_col}]), COUNT(DISTINCT [{date_col}]) FROM [{tbl}]").fetchone()
        fund_count = con.execute(f"SELECT COUNT(DISTINCT fund_id) FROM [{tbl}]").fetchone()[0] if 'fund_id' in cols else 'N/A'
        print(f"\n📅 {tbl}:")
        print(f"   Date range: {r[0]} → {r[1]}")
        print(f"   Trading days: {r[2]:,}")
        print(f"   Unique funds: {fund_count}")

con.close()
```

## Step 7: Check what FMR (Fund Manager Report) data exists

```python
import sqlite3

con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")

# Check for FMR-related data: expense ratio, AUM, benchmark, rating
for tbl in [t[0] for t in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
    cols = [c[1] for c in con.execute(f"PRAGMA table_info([{tbl}])").fetchall()]
    fmr_cols = [c for c in cols if any(k in c.lower() for k in [
        'expense', 'aum', 'benchmark', 'rating', 'risk', 'sharpe', 
        'beta', 'stdev', 'std_dev', 'volatility', 'pacra', 'vis',
        'management_fee', 'front_load', 'back_load', 'min_invest'
    ])]
    if fmr_cols and any(k in tbl.lower() for k in ['fund', 'nav', 'mufap', 'mutual', 'fmr', 'performance']):
        print(f"\n📊 {tbl} has FMR columns: {fmr_cols}")
        for fc in fmr_cols[:5]:
            sample = con.execute(f"SELECT DISTINCT [{fc}] FROM [{tbl}] WHERE [{fc}] IS NOT NULL LIMIT 5").fetchall()
            print(f"   {fc} samples: {[s[0] for s in sample]}")

con.close()
```

---

## OUTPUT FORMAT

After running all steps, produce a report with this EXACT structure:

```
═══════════════════════════════════════════════════════
  MUFAP FUND EXPLORER — AUDIT REPORT
═══════════════════════════════════════════════════════

1. FILES FOUND
   - Scraper: [list files]
   - UI Page: [list files]  
   - DB Layer: [list files]

2. DATABASE TABLES
   [table name] → [row count] rows, [column count] cols
   Key columns: [list]

3. DATA COVERAGE
   ✅ Available:
   - [what data exists]
   
   ❌ Missing:
   - [what data is NOT in the DB]

4. FUND CATEGORIES
   ✅ Present: [list categories found]
   ❌ Missing: [compare against full MUFAP list below]

5. ANALYTICS CURRENTLY AVAILABLE
   ✅ [what calculations the UI already does]
   ❌ [what it doesn't do]

6. GAP ANALYSIS — What needs to be ADDED
   
   A. SCRAPER ENHANCEMENTS NEEDED:
      - [what new data to scrape]
   
   B. NEW DB TABLES/COLUMNS NEEDED:
      - [schema changes]
   
   C. NEW ANALYTICS TO BUILD:
      - [calculations]
   
   D. NEW UI FEATURES TO ADD:
      - [pages/components]

═══════════════════════════════════════════════════════
```

## TARGET ANALYTICS (what we want to eventually have)

Compare current state against this target list:

### Quantitative Metrics
- [ ] Standard Deviation (SD) — rolling 30/90/180/365 day
- [ ] Sharpe Ratio — using KIBOR as risk-free rate
- [ ] Beta — relative to KSE-100 (equity funds) or benchmark
- [ ] Alpha — excess return over benchmark
- [ ] Max Drawdown — worst peak-to-trough decline
- [ ] Sortino Ratio — downside deviation only
- [ ] Information Ratio — active return / tracking error
- [ ] Treynor Ratio — excess return / beta
- [ ] R-squared — correlation with benchmark

### Performance Metrics  
- [ ] Returns: 1D, 7D, 15D, 30D, 90D, 180D, 365D, 2Y, 3Y, 5Y, Since Inception
- [ ] Rolling returns (1Y rolling, annualized)
- [ ] Calendar year returns
- [ ] NAV growth chart (line chart, multi-fund overlay)
- [ ] Drawdown chart

### Fund Metadata
- [ ] AUM (Assets Under Management) — current + historical
- [ ] Expense Ratio / Management Fee
- [ ] Front-end / Back-end Load
- [ ] Minimum Investment
- [ ] PACRA / VIS Rating
- [ ] Risk Category (Low/Medium/High)
- [ ] Benchmark name
- [ ] Fund Manager name
- [ ] Inception date
- [ ] Fund Type / Category (all 25+ MUFAP categories)

### Fund Categories (full MUFAP list)
- [ ] Money Market
- [ ] Income / Bond  
- [ ] Government Securities
- [ ] Equity
- [ ] Balanced / Asset Allocation
- [ ] Capital Protected
- [ ] Commodity (Gold, etc.)
- [ ] Fund of Funds
- [ ] Index Tracker
- [ ] VPS Equity Sub-Fund
- [ ] VPS Debt Sub-Fund
- [ ] VPS Money Market Sub-Fund
- [ ] Dedicated Equity (CEF)
- [ ] Islamic variants of all above

### Comparison Features
- [ ] Side-by-side fund comparison (2-4 funds)
- [ ] Category ranking table (sorted by Sharpe, return, etc.)
- [ ] AMC-level aggregation (total AUM, avg performance)
- [ ] Sector allocation breakdown (if available)
- [ ] Risk-return scatter plot (SD vs Return)

DO NOT BUILD ANY OF THIS YET. Just report what exists vs what's missing.
