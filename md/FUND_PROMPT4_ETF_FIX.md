# Claude Code Prompt: Fix ETF NAV Pipeline

## Context

The `etf_nav` table has 45 rows with `nav=NULL` for all of them. The scraper 
only fetches market_price from PSX DPS API but never populates NAV.

**The NAV data already exists** in `mutual_fund_nav` — MUFAP has 8 ETFs in 
`mutual_funds` table with category "Exchange Traded Fund" or 
"Shariah Compliant Exchange Traded Fund". Their daily NAV is already being 
scraped and stored.

The fix is to cross-reference `etf_nav.market_price` with `mutual_fund_nav.nav` 
for matching ETFs.

## Step 1: Map ETF symbols to MUFAP fund_ids

```python
import sqlite3
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")

# ETFs in etf_nav table (from PSX)
print("=== ETF_NAV symbols ===")
for row in con.execute("SELECT DISTINCT symbol FROM etf_nav"):
    print(f"  {row[0]}")

# ETFs in mutual_funds table (from MUFAP)
print("\n=== MUFAP ETF funds ===")
for row in con.execute("""
    SELECT fund_id, symbol, fund_name, category 
    FROM mutual_funds 
    WHERE category LIKE '%Exchange Traded%' OR fund_type = 'ETF'
"""):
    print(f"  {row[0]} | {row[1]:15s} | {row[2]:40s} | {row[3]}")

# Check NAV data available for these ETFs
print("\n=== NAV data for ETFs ===")
for row in con.execute("""
    SELECT mf.fund_id, mf.symbol, COUNT(n.nav) as nav_count, 
           MIN(n.date) as first, MAX(n.date) as last
    FROM mutual_funds mf
    JOIN mutual_fund_nav n ON n.fund_id = mf.fund_id
    WHERE mf.category LIKE '%Exchange Traded%' OR mf.fund_type = 'ETF'
    GROUP BY mf.fund_id
"""):
    print(f"  {row[0]} | {row[1]:15s} | {row[2]:,} NAVs | {row[3]} → {row[4]}")

con.close()
```

Run this FIRST. Show the output. Then proceed.

## Step 2: Create symbol mapping

The PSX symbol (in etf_nav) and MUFAP symbol (in mutual_funds) may differ slightly.
For example: PSX has "ACIETF", MUFAP might have "ACI-ETF" or "ACIETF".

Build a mapping dict by:
1. Exact match on symbol
2. Fuzzy match (strip hyphens, lowercase compare)
3. Manual mapping for any that don't match

Store this mapping somewhere reusable (a dict in the ETF scraper or a small table).

## Step 3: Fix the scraper — populate NAV from mutual_fund_nav

In the ETF scraper (`etf_scraper.py` or wherever `scrape_etf()` lives):

**Option A (recommended — no web scraping needed):**

After fetching market_price from PSX DPS API, look up the NAV from mutual_fund_nav:

```python
def _get_etf_nav_from_mufap(self, symbol: str, date: str) -> float | None:
    """Cross-reference ETF NAV from MUFAP mutual_fund_nav table."""
    # Use the symbol → fund_id mapping from Step 2
    fund_id = ETF_SYMBOL_MAP.get(symbol)
    if not fund_id:
        return None
    
    row = self.con.execute(
        "SELECT nav FROM mutual_fund_nav WHERE fund_id = ? AND date = ?",
        (fund_id, date)
    ).fetchone()
    
    return row[0] if row else None
```

Then in scrape_etf():
```python
data["nav"] = self._get_etf_nav_from_mufap(symbol, date)
if data["nav"] and data["market_price"]:
    data["premium_discount"] = ((data["market_price"] / data["nav"]) - 1) * 100
```

## Step 4: Backfill existing etf_nav rows

Run a one-time backfill to populate NAV for all existing 45 rows:

```python
def backfill_etf_nav(con):
    """Fill NULL nav values in etf_nav from mutual_fund_nav."""
    updated = 0
    rows = con.execute(
        "SELECT rowid, symbol, date FROM etf_nav WHERE nav IS NULL"
    ).fetchall()
    
    for rowid, symbol, date in rows:
        fund_id = ETF_SYMBOL_MAP.get(symbol)
        if not fund_id:
            continue
        nav_row = con.execute(
            "SELECT nav FROM mutual_fund_nav WHERE fund_id = ? AND date = ?",
            (fund_id, date)
        ).fetchone()
        if nav_row and nav_row[0]:
            premium = ((con.execute(
                "SELECT market_price FROM etf_nav WHERE rowid = ?", (rowid,)
            ).fetchone()[0] or 0) / nav_row[0] - 1) * 100 if nav_row[0] else None
            
            con.execute(
                "UPDATE etf_nav SET nav = ?, premium_discount = ? WHERE rowid = ?",
                (nav_row[0], premium, rowid)
            )
            updated += 1
    
    con.commit()
    print(f"✅ Backfilled NAV for {updated}/{len(rows)} ETF records")
```

Add CLI: `python -m pakfindata.sources.etf_scraper --backfill-nav`

## Step 5: Update the ETF UI tab

In fund_explorer.py, the ETF tab should now show:
- Symbol | Market Price | NAV | Premium/Discount %
- Color premium/discount: green if discount (buy opportunity), red if premium
- Historical premium/discount chart for each ETF

## VERIFY

```bash
# Run backfill
python -m pakfindata.sources.etf_scraper --backfill-nav

# Check results
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
total = con.execute('SELECT COUNT(*) FROM etf_nav').fetchone()[0]
with_nav = con.execute('SELECT COUNT(*) FROM etf_nav WHERE nav IS NOT NULL').fetchone()[0]
print(f'ETF NAV: {with_nav}/{total} rows have NAV data')
print()
for row in con.execute('SELECT symbol, date, market_price, nav, premium_discount FROM etf_nav LIMIT 10'):
    print(f'  {row[0]:10s} | {row[1]} | Mkt: {row[2]} | NAV: {row[3]} | P/D: {row[4]}')
con.close()
"
```
