# Claude Code Prompt: Fix Fund Metadata Gaps — Re-scrape 630 Funds

## Context

The `mutual_funds` table in `/mnt/e/psxdata/psx.sqlite` has 1,205 funds but ~630 
have NULL values for critical metadata: sector, front_load, back_load, risk_profile, 
benchmark, trustee, fund_manager, aum, mufap_fund_id, mufap_int_id.

The scraper is at `src/pakfindata/sources/mufap.py`.
It scrapes from mufap.com.pk (FundProfile/FundDirectory and FundDetail pages).

## Task

Fix the metadata gaps by re-scraping fund details for funds with missing data.

### Step 1: Assess the damage

```python
import sqlite3

con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")

print("=== NULL COUNTS PER COLUMN ===")
cols_to_check = [
    'sector', 'front_load', 'back_load', 'risk_profile', 
    'benchmark', 'trustee', 'fund_manager', 'aum',
    'mufap_fund_id', 'mufap_int_id', 'mufap_amc_id',
    'expense_ratio', 'management_fee', 'launch_date'
]
for col in cols_to_check:
    try:
        n = con.execute(f"SELECT COUNT(*) FROM mutual_funds WHERE [{col}] IS NULL").fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM mutual_funds").fetchone()[0]
        print(f"  {col}: {n}/{total} NULL ({n/total*100:.0f}%)")
    except:
        print(f"  {col}: column not found")

print("\n=== FUNDS WITH MOST NULLS ===")
# Show 10 funds with most missing fields
rows = con.execute("""
    SELECT fund_id, fund_name, category,
        (CASE WHEN sector IS NULL THEN 1 ELSE 0 END +
         CASE WHEN front_load IS NULL THEN 1 ELSE 0 END +
         CASE WHEN back_load IS NULL THEN 1 ELSE 0 END +
         CASE WHEN risk_profile IS NULL THEN 1 ELSE 0 END +
         CASE WHEN benchmark IS NULL THEN 1 ELSE 0 END +
         CASE WHEN trustee IS NULL THEN 1 ELSE 0 END +
         CASE WHEN fund_manager IS NULL THEN 1 ELSE 0 END +
         CASE WHEN aum IS NULL THEN 1 ELSE 0 END) as null_count
    FROM mutual_funds
    ORDER BY null_count DESC
    LIMIT 10
""").fetchall()
for r in rows:
    print(f"  {r[0]} | {r[1][:40]:40s} | {r[2]:30s} | {r[3]} nulls")

con.close()
```

### Step 2: Understand the existing scraper

Read `src/pakfindata/sources/mufap.py` completely. Find:
1. How it scrapes FundDirectory (list of all funds)
2. How it scrapes FundDetail (individual fund metadata)
3. Which fields it extracts vs which it skips
4. Whether it uses mufap_fund_id or mufap_int_id to fetch details
5. How it stores data (INSERT vs UPSERT)

Also read the MUFAP FundProfile API patterns. The app already knows these URLs:
- `https://www.mufap.com.pk/FundProfile/FundDirectory` — all funds
- `https://www.mufap.com.pk/FundProfile/FundDetail?FundID={id}` — single fund
- `https://www.mufap.com.pk/Industry/IndustryStatDaily` — performance data

### Step 3: Build a targeted metadata re-scraper

Create or modify a function that:

1. Queries mutual_funds for all funds where sector IS NULL OR benchmark IS NULL 
   OR risk_profile IS NULL (the most impactful missing fields)
2. For each fund, tries to fetch metadata from MUFAP:
   - First try: FundDetail API with mufap_fund_id (if available)
   - Second try: FundDetail API with fund_id
   - Third try: Match by fund_name in IndustryStatDaily data (already in fund_performance table)
3. Extracts: sector, benchmark, risk_profile, front_load, back_load, 
   trustee, fund_manager, management_fee, expense_ratio, aum, launch_date
4. UPDATE (not INSERT) the mutual_funds row — only set columns that were NULL

### Step 4: Cross-reference from fund_performance table

The `fund_performance` table has 491,517 rows with sector, category, rating, benchmark 
for many funds. Use this as a FALLBACK source for funds where MUFAP scraping fails:

```python
# Fill sector from fund_performance where mutual_funds.sector IS NULL
UPDATE mutual_funds 
SET sector = (
    SELECT DISTINCT fp.sector 
    FROM fund_performance fp 
    WHERE fp.fund_id = mutual_funds.fund_id 
    AND fp.sector IS NOT NULL 
    LIMIT 1
)
WHERE sector IS NULL 
AND fund_id IN (SELECT DISTINCT fund_id FROM fund_performance WHERE sector IS NOT NULL)
```

Same pattern for: benchmark, rating, category (if different/missing).

### Step 5: Add CLI command

```bash
# Re-scrape metadata for all funds with gaps
python -m pakfindata.sources.mufap --fix-metadata

# Re-scrape specific fund
python -m pakfindata.sources.mufap --fix-metadata --fund-id 12768

# Just fill from fund_performance table (no web scraping)
python -m pakfindata.sources.mufap --fill-from-performance
```

### Step 6: Respect rate limits

- MUFAP is a small site — don't hammer it
- Add 1-2 second delay between FundDetail requests
- Use existing DrissionPage/session if the scraper already has one
- Print progress: `[42/630] Updating ABL Cash Fund — sector: Money Market, benchmark: KIBOR`
- Skip funds that return 404 or empty data
- Summary at end: `✅ Updated 485 funds, 145 still incomplete (no MUFAP page found)`

## VERIFY

```bash
# Run fill-from-performance first (instant, no web)
python -m pakfindata.sources.mufap --fill-from-performance

# Check improvement
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for col in ['sector','benchmark','risk_profile','front_load','trustee','fund_manager','aum']:
    n = con.execute(f'SELECT COUNT(*) FROM mutual_funds WHERE [{col}] IS NULL').fetchone()[0]
    print(f'  {col}: {n} still NULL')
con.close()
"

# Then run web re-scrape for remaining gaps
python -m pakfindata.sources.mufap --fix-metadata
```
