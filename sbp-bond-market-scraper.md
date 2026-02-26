# Claude Code Prompt: SBP Bond Market Data — Scraper + Backfill + FI Page Integration

## Context

I'm working on `psx_ohlcv` — a Pakistan financial data platform.
- DB: `/mnt/e/psxdata/psx.sqlite`
- Project: `~/psx_ohlcv/` (dev branch)
- v3.0.0 is complete — already has populated tables from prior work
- The FI page enhancement prompt is running separately — this prompt adds the BOND TRADING data layer

### ALREADY BUILT & POPULATED (do NOT recreate):
| Table              | Rows  | Date Range                | Notes                          |
|--------------------|-------|---------------------------|--------------------------------|
| `tbill_auctions`   | 155   | Jun 2024 — Feb 2026       | SBP t-bills.asp scraper works  |
| `pib_auctions`     | 935   | Dec 2000 — Feb 2026       | 25 years of history            |
| `gis_auctions`     | 66    | Nov 2010 — Dec 2023       | GIS Sukuk auction data         |
| `kibor_daily`      | 2,664 | Jan 2024 — Feb 2026       | ✅ Correct source of truth     |
| `konia_daily`      | 22    | limited                   | Needs more backfill sources    |
| `pkrv_daily`       | 21    | limited                   | Needs more backfill sources    |
| `sbp_policy_rates` | 1     | current only              | Needs historical backfill      |
| `kibor_rates`      | legacy| DO NOT USE — broken offer  | 3M offer shows 0.03 (wrong)   |

### WHAT THIS PROMPT BUILDS (NEW):
1. **SBP Outright Secondary Market Trading Volume** — daily OTC bond trade data (the SMTV PDF)
2. **SBP Benchmark Rate Snapshot** — daily scrape of policy rate, KIBOR, MTB/PIB cutoffs from msm.asp
3. **Archive backfill** — historical secondary market data from SecMarBankArc.asp / SecMarNonBankArc.asp
4. **Deeper PKRV backfill** — from InvestPak or FMA if available (current 21 rows is thin)
5. **KONIA backfill** — same, need more history

Pakistan's bond market is 99% OTC interbank — NOT on PSX exchange. The real bond
quote data comes from SBP, not PSX. This prompt builds that data pipeline.

---

## PHASE 1 — INVESTIGATE EXISTING STATE + GAPS

### Step 1 — Verify existing tables and identify data gaps

```bash
# Confirm existing tables and their current state
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT 'tbill_auctions' as tbl, COUNT(*) as rows, MIN(auction_date) as earliest, MAX(auction_date) as latest FROM tbill_auctions
UNION ALL
SELECT 'pib_auctions', COUNT(*), MIN(auction_date), MAX(auction_date) FROM pib_auctions
UNION ALL
SELECT 'gis_auctions', COUNT(*), MIN(auction_date), MAX(auction_date) FROM gis_auctions
UNION ALL
SELECT 'kibor_daily', COUNT(*), MIN(date), MAX(date) FROM kibor_daily
UNION ALL
SELECT 'konia_daily', COUNT(*), MIN(date), MAX(date) FROM konia_daily
UNION ALL
SELECT 'pkrv_daily', COUNT(*), MIN(date), MAX(date) FROM pkrv_daily
UNION ALL
SELECT 'sbp_policy_rates', COUNT(*), MIN(date), MAX(date) FROM sbp_policy_rates;
"

# Check if any NEW tables from this prompt already exist
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' 
AND (name LIKE '%bond_trading%' OR name LIKE '%benchmark%' OR name LIKE '%smtv%')
ORDER BY name;
"

# Show PKRV data (only 21 rows — need to understand the gap)
sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT * FROM pkrv_daily ORDER BY date DESC LIMIT 5;"

# Show KONIA data
sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT * FROM konia_daily ORDER BY date DESC LIMIT 5;"

# Check existing scraper source files
find ~/psx_ohlcv/src/ -name "*.py" | xargs grep -l "sbp\|pkrv\|konia\|treasury\|smtv\|outright\|secondary.*market\|benchmark" 2>/dev/null

# Show existing CLI structure
grep -rn "add_command\|@click\|group\|subparser\|def .*sync\|def .*backfill" ~/psx_ohlcv/src/psx_ohlcv/cli/ 2>/dev/null | head -30
```

### Step 2 — Probe SBP archive endpoints for backfill

```bash
# Test SBP secondary market archive pages
curl -s "https://www.sbp.org.pk/DFMD/SecMarBankArc.asp" 2>/dev/null | head -200
curl -s "https://www.sbp.org.pk/DFMD/SecMarNonBankArc.asp" 2>/dev/null | head -200

# Test the daily Outright SMTV PDF (current day)
curl -sI "https://www.sbp.org.pk/ecodata/Outright-SMTV.pdf" 2>/dev/null | head -10

# Test historical SMTV PDFs — try date-based URLs
curl -sI "https://www.sbp.org.pk/ecodata/Outright-SMTV-24-Feb-26.pdf" 2>/dev/null | head -5
curl -sI "https://www.sbp.org.pk/ecodata/Outright-SMTV-2026-02-24.pdf" 2>/dev/null | head -5

# Test InvestPak PKRV endpoint
curl -s "https://investpak.sbp.org.pk/rates_detail/pkrv" 2>/dev/null | head -200
curl -s "https://investpak.sbp.org.pk/live_quotes" 2>/dev/null | head -200

# Test FMA market data (PKRV source)
curl -s "https://fma.com.pk/market-data/" 2>/dev/null | head -200

# Test SBP daily secondary market yields
curl -s "https://www.sbp.org.pk/DFMD/msm.asp" 2>/dev/null | grep -i "yield\|rate\|kibor\|mtb\|pib" | head -30
```

**STOP — Show me ALL output from Steps 1-3 before proceeding.**

---

## PHASE 2 — BUILD SECONDARY BOND MARKET DATA

### Step 4 — DB schema for bond trading data

Create or extend the appropriate DB module (use existing patterns):

```sql
-- Daily OTC bond trading volume from SBP
-- Source: sbp.org.pk/ecodata/Outright-SMTV.pdf (daily PDF)
-- Also: sbp.org.pk/DFMD/SecMarBankArc.asp (interbank archive)
-- Also: sbp.org.pk/DFMD/SecMarNonBankArc.asp (bank-to-nonbank archive)

CREATE TABLE IF NOT EXISTS sbp_bond_trading_daily (
    date TEXT NOT NULL,
    security_type TEXT NOT NULL,      -- 'MTB', 'PIB_FIXED', 'PIB_FLOAT', 'GIS_VRR', 'GIS_FRR', 'GIS_DIS', 'PIB_DIS'
    maturity_year INTEGER,            -- e.g. 2026, 2027, 2030
    tenor_bucket TEXT,                -- for MTBs: '0-14D', '15-91D', '92-182D', '183-364D'
    segment TEXT NOT NULL,            -- 'interbank', 'bank_nonbank_purchase', 'bank_nonbank_sale'
    face_amount REAL,                 -- PKR millions
    realized_amount REAL,             -- PKR millions
    yield_min REAL,                   -- %
    yield_max REAL,                   -- %
    yield_weighted_avg REAL,          -- %
    PRIMARY KEY (date, security_type, maturity_year, segment)
);

-- Daily aggregate totals
CREATE TABLE IF NOT EXISTS sbp_bond_trading_summary (
    date TEXT NOT NULL,
    segment TEXT NOT NULL,            -- 'interbank', 'bank_nonbank', 'grand_total'
    total_face_amount REAL,           -- PKR millions
    total_realized_amount REAL,       -- PKR millions
    PRIMARY KEY (date, segment)
);

-- SBP secondary market yields (from msm.asp sidebar data)
-- These are the current benchmark yields displayed on SBP website
CREATE TABLE IF NOT EXISTS sbp_benchmark_snapshot (
    date TEXT NOT NULL,
    metric TEXT NOT NULL,             -- 'policy_rate', 'overnight_repo', 'kibor_3m_bid', 'kibor_3m_offer',
                                     -- 'kibor_6m_bid', 'kibor_6m_offer', 'kibor_12m_bid', 'kibor_12m_offer',
                                     -- 'mtb_1m', 'mtb_3m', 'mtb_6m', 'mtb_12m',
                                     -- 'pib_2y', 'pib_3y', 'pib_5y', 'pib_10y', 'pib_15y'
    value REAL NOT NULL,              -- rate/yield %
    PRIMARY KEY (date, metric)
);

CREATE INDEX IF NOT EXISTS idx_bond_trading_date ON sbp_bond_trading_daily(date);
CREATE INDEX IF NOT EXISTS idx_bond_trading_type ON sbp_bond_trading_daily(security_type);
CREATE INDEX IF NOT EXISTS idx_benchmark_date ON sbp_benchmark_snapshot(date);
```

Query functions needed:
- `get_bond_trading(con, date=None, security_type=None, segment=None) -> pd.DataFrame`
- `get_trading_volume_trend(con, n_days=30, security_type=None) -> pd.DataFrame`
- `get_benchmark_snapshot(con, date=None) -> dict`
- `get_benchmark_history(con, metric, start_date=None, end_date=None) -> pd.DataFrame`

### Step 5 — SBP Outright SMTV PDF scraper

Create `src/psx_ohlcv/sources/sbp_bond_market.py`:

```python
"""
Scraper for SBP secondary bond market data.

PRIMARY SOURCE: Outright Secondary Market Trading Volume PDF
URL: https://www.sbp.org.pk/ecodata/Outright-SMTV.pdf
Published: Daily (by ~6pm PKT on trading days)
Contains: All OTC bond trades for the day — MTBs, PIBs, GIS — with
          face value, realized value, min/max/weighted-avg yields,
          broken down by interbank + bank-to-nonbank.

ARCHIVE SOURCES for backfill:
- https://www.sbp.org.pk/DFMD/SecMarBankArc.asp (interbank trades archive)
- https://www.sbp.org.pk/DFMD/SecMarNonBankArc.asp (bank-to-nonbank archive)
These archive pages have links to historical PDFs/data.

BENCHMARK SOURCE:
- https://www.sbp.org.pk/DFMD/msm.asp (Money & Secondary Markets page)
  Sidebar has: SBP policy rate, KIBOR, MTB cutoffs, PIB cutoffs, overnight repo
  This is scraped daily as a snapshot of current benchmark rates.

APPROACH for the daily SMTV PDF:
1. Download PDF from sbp.org.pk/ecodata/Outright-SMTV.pdf
2. Extract text using pdfplumber (pip install pdfplumber)
3. Parse the tabular structure:
   - Page 1: Purchase Interbank Market (top) + Banks Outright Sales to Non Banks (bottom)
   - Page 2: Banks Outright Purchases from Non Banks + Grand Total
4. Each section has rows like:
   Security Type | Maturity | Face Amount | Realized Amount | Min Yield | Max Yield | Wtd Avg Yield
5. Extract date from header: "February 25, 2026"

The PDF structure (from actual extraction):
- "1. Market Treasury Bills-(MTB)" with sub-buckets (A) Upto 14 Days, (B) 15-91 Days, etc.
- "2. Variable Rental Rate Ijara Sukuk-(GISVRR)" with maturity years
- "3. Fixed Rental Rate GoP Ijara Sukuk-(GISFRR)"
- "4. Fixed-Rate Pakistan Investment Bond-(PIB)"
- "5. Floating-rate PIBs (Half-yearly Coupon Reset)-(PFL)"
- "Discounted Pakistan Investment Bond-(PIB DIS)"
- "Discounted Ijara Sukuk-(GIS DIS)"
- Subtotal per section, Grand Total at bottom

For BACKFILL:
- Check if the archive pages (SecMarBankArc.asp, SecMarNonBankArc.asp) link to 
  individual daily PDFs or aggregate data
- If daily PDFs: iterate dates and download/parse each
- If aggregate tables: parse the HTML tables directly
- Start with most recent 6 months, extend backward as needed
"""

import requests
import pdfplumber
import pandas as pd
import re
import io
import logging
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

class SBPBondMarketScraper:
    SMTV_URL = "https://www.sbp.org.pk/ecodata/Outright-SMTV.pdf"
    MSM_URL = "https://www.sbp.org.pk/DFMD/msm.asp"
    BANK_ARCHIVE_URL = "https://www.sbp.org.pk/DFMD/SecMarBankArc.asp"
    NONBANK_ARCHIVE_URL = "https://www.sbp.org.pk/DFMD/SecMarNonBankArc.asp"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
    
    def scrape_daily_smtv(self) -> dict:
        """
        Download and parse today's Outright SMTV PDF.
        Returns: {
            'date': '2026-02-25',
            'trades': [
                {'security_type': 'MTB', 'tenor_bucket': '15-91D', 'maturity_year': None,
                 'segment': 'interbank', 'face_amount': 78623.03, 'realized_amount': 78093.62,
                 'yield_min': 10.31, 'yield_max': 10.60, 'yield_weighted_avg': 10.55},
                ...
            ],
            'totals': {
                'interbank': {'face': 245844.47, 'realized': 252957.66},
                'bank_nonbank': {'face': 682814.35, 'realized': ...},
                'grand_total': {'face': 928658.82, 'realized': ...}
            }
        }
        """
        # Download PDF
        # Extract with pdfplumber
        # Parse each page/section
        pass
    
    def scrape_benchmark_snapshot(self) -> dict:
        """
        Scrape current benchmark rates from sbp.org.pk/DFMD/msm.asp sidebar.
        Returns: {
            'date': '2026-02-26',
            'policy_rate': 10.50,
            'overnight_repo': 10.65,
            'kibor_3m_bid': 10.33, 'kibor_3m_offer': 10.58,
            'kibor_6m_bid': 10.35, 'kibor_6m_offer': 10.60,
            'kibor_12m_bid': 10.37, 'kibor_12m_offer': 10.87,
            'mtb_1m': 10.1482, 'mtb_3m': 10.2853, 'mtb_6m': 10.4437, 'mtb_12m': 10.5996,
            'pib_2y': 10.3380, 'pib_3y': 10.2489, 'pib_5y': 10.7500,
            'pib_10y': 11.2390, 'pib_15y': 11.4998,
        }
        """
        pass
    
    def scrape_archive_interbank(self) -> pd.DataFrame:
        """
        Scrape historical interbank secondary market trading data.
        Source: sbp.org.pk/DFMD/SecMarBankArc.asp
        First inspect what format the archive provides (HTML tables vs PDF links).
        """
        pass
    
    def scrape_archive_nonbank(self) -> pd.DataFrame:
        """
        Scrape historical bank-to-nonbank secondary market trading data.
        Source: sbp.org.pk/DFMD/SecMarNonBankArc.asp
        """
        pass
    
    def backfill_smtv(self, start_date: date, end_date: date = None) -> int:
        """
        Attempt to backfill daily SMTV data from archive sources.
        Try these approaches:
        1. Check if SMTV PDF has date-parameterized URLs
        2. Use archive pages for historical aggregate data
        3. Parse InvestPak historical data if available
        Returns number of days successfully backfilled.
        """
        pass
```

### Step 6 — InvestPak live quotes scraper (bonus data source)

```python
"""
InvestPak has live indicative quotes at:
https://investpak.sbp.org.pk/live_quotes

Also has:
- PKRV rates: https://investpak.sbp.org.pk/rates_detail/pkrv
- PFL revaluation: https://investpak.sbp.org.pk/rates_detail/pfl_revaluation
- Sukuk rates: https://investpak.sbp.org.pk/rates_detail/sukuk
- KIBOR: https://investpak.sbp.org.pk/rates_detail/kibor
- Auction results: https://investpak.sbp.org.pk/auction_results
- Historical auctions: https://investpak.sbp.org.pk/auctions_historical_data/pages

Check if these return JSON APIs or need HTML parsing.
If PKRV/PFL/Sukuk data here is BETTER or MORE COMPLETE than what we already
have from our existing scraper, add it as a supplementary source.
"""
```

### Step 6.5 — PKRV + KONIA Deep Backfill (PRIORITY — current data is thin)

`pkrv_daily` has only 21 rows and `konia_daily` has 22 rows. These need deep backfill.

**PKRV backfill sources (try in order):**

1. **FMA Pakistan (fma.com.pk/market-data/)** — FMA/FMAP is the ORIGINAL publisher of PKRV.
   They may have downloadable historical data or an API. Probe the page first.
   ```bash
   curl -s "https://fma.com.pk/market-data/" | head -300
   # Look for download links, CSV exports, date pickers, AJAX endpoints
   ```

2. **InvestPak PKRV page (investpak.sbp.org.pk/rates_detail/pkrv)** — SBP republishes PKRV.
   Check if it has a date picker or historical data:
   ```bash
   curl -s "https://investpak.sbp.org.pk/rates_detail/pkrv" | head -300
   # Look for AJAX calls, date parameters, /api/ endpoints
   ```

3. **MUFAP (mufap.com.pk)** — Your existing MUFAP scraper might already fetch PKRV.
   Check if there's PKRV data in the existing codebase or tables:
   ```bash
   sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%mufap%';"
   grep -rn "pkrv\|PKRV\|revaluation" ~/psx_ohlcv/src/psx_ohlcv/sources/mufap* 2>/dev/null
   ```

4. **SBP EasyData (easydata.sbp.org.pk)** — SBP's statistical portal.
   Has downloadable time series for monetary/financial data:
   ```bash
   curl -s "https://easydata.sbp.org.pk/apex/f?p=10:211" 2>/dev/null | head -200
   ```

5. **Existing scraper in psx_ohlcv** — Check what's already scraping PKRV:
   ```bash
   grep -rn "pkrv\|PKRV" ~/psx_ohlcv/src/ 2>/dev/null
   # Find the scraper that populated the 21 rows — extend it for backfill
   ```

**KONIA backfill sources:**

1. SBP EasyData — KONIA time series may be downloadable
2. FMA market data page — may have KONIA history
3. SBP msm.asp sidebar has current overnight repo rate — but not historical

**For both PKRV and KONIA:**
- The existing `pkrv_daily` and `konia_daily` table schemas are already correct — just need more data
- Use existing upsert functions to add historical records
- Target: at least 1 year of daily data (250+ trading days)
- If no API/download found, note it and move on — we can manually source later

### Step 7 — CLI commands

```
psxsync bonds smtv-sync         # Download & parse today's SMTV PDF
psxsync bonds smtv-backfill     # Backfill from archives
psxsync bonds benchmark-sync    # Scrape SBP benchmark snapshot
psxsync bonds status            # Show bond data coverage

# Example output of status:
# Bond Trading Data:
#   Daily SMTV: 45 days (2026-01-02 to 2026-02-26)
#   Interbank archive: 250 rows
#   Benchmark snapshots: 45 days
#   Latest SMTV: Feb 25 — Grand Total PKR 928.7B
#   Latest 10Y PIB yield: 11.24%
```

### Step 8 — Cron integration

Add to the existing cron/sync system:
```
# Daily bond data (after market, ~6pm PKT = 1pm UTC)
0 13 * * 1-5  python -m psx_ohlcv bonds smtv-sync
0 13 * * 1-5  python -m psx_ohlcv bonds benchmark-sync
```

---

## PHASE 3 — INTEGRATE INTO FI PAGE

The FI page enhancement prompt is running separately. This data should
appear on the FI page automatically IF you follow these conventions:

### Step 9 — Add bond trading sections to FI page

Add to the existing FI Overview page (or create a sub-page):

**Section: OTC Bond Market (from SBP)**
```
- Daily trading volume bar chart: face value by security type (MTB, PIB, GIS)
- Interbank vs bank-to-nonbank split (stacked or side-by-side)
- Volume trend over last 30 days
- Grand total comparison (today vs yesterday vs last week)
```

**Section: Benchmark Rates Dashboard**
```
- Current SBP policy rate (prominent display)
- KIBOR panel: 3M/6M/12M bid/offer
- MTB cutoff yields: 1M/3M/6M/12M
- Fixed PIB cutoffs: 2Y/3Y/5Y/10Y/15Y
- Rate changes from previous snapshot (colored arrows)
- Historical trend chart for any selected benchmark
```

**Section: Yield Curve Comparison**
```
- Overlay PKRV curve (from existing data) with auction cutoff yields
- Show spread: PKRV 10Y vs PIB 10Y auction cutoff
- This is a trader's key view — where the curve IS vs where auctions cleared
```

### Step 10 — Wire data to existing REST API / MCP if applicable

```
If the project has a FastAPI or MCP server, add endpoints:
- GET /api/bonds/trading?date=2026-02-25
- GET /api/bonds/benchmark?date=2026-02-25
- GET /api/bonds/volume-trend?days=30
- MCP tool: get_bond_trading_volume(date)
- MCP tool: get_benchmark_rates(date)
```

---

## PHASE 4 — VERIFY

```bash
# Step 11 — Test SMTV scraper
python -m psx_ohlcv bonds smtv-sync 2>&1

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT security_type, segment, 
       SUM(face_amount) as total_face_m,
       AVG(yield_weighted_avg) as avg_yield
FROM sbp_bond_trading_daily 
WHERE date = (SELECT MAX(date) FROM sbp_bond_trading_daily)
GROUP BY security_type, segment;
"

# Step 12 — Test benchmark scraper
python -m psx_ohlcv bonds benchmark-sync 2>&1

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT * FROM sbp_benchmark_snapshot 
WHERE date = (SELECT MAX(date) FROM sbp_benchmark_snapshot)
ORDER BY metric;
"

# Step 13 — Test backfill (try archives)
python -m psx_ohlcv bonds smtv-backfill 2>&1

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT date, SUM(face_amount) as total_face 
FROM sbp_bond_trading_daily 
GROUP BY date ORDER BY date DESC LIMIT 10;
"

# Step 14 — Verify FI page loads bond data
streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5

# Step 15 — Tests
pytest tests/ -x -q --tb=short 2>&1 | tail -10
```

---

## CRITICAL RULES

1. **INVESTIGATE FIRST** — Phase 1 tells you what exists. Do NOT duplicate existing tables or scrapers.
2. **USE EXISTING PATTERNS** — The project already has working scrapers (sbp_treasury.py, sbp_rates.py, sbp_msm.py, sbp_gsp.py etc). Match their code style, error handling, session management, and DB access patterns EXACTLY. Add new scrapers as sibling files, not replacements.
3. **CHECK BEFORE CREATING** — Before creating any new file, `ls` the target directory. Before creating any table, check `sqlite_master`. The tables listed in "ALREADY BUILT" section are CONFIRMED populated — do not recreate or alter them.
4. **pdfplumber for SMTV PDF** — `pip install pdfplumber` if not installed. The PDF has structured tables.
5. **Handle network failures gracefully** — SBP website can be slow/down. Retry with backoff.
6. **The SMTV PDF changes daily** — Same URL, new content. Save the raw PDF to `data/smtv/` with date in filename for archival.
7. **Archive pages may need exploration** — The SecMarBankArc.asp and SecMarNonBankArc.asp pages may link to individual PDFs, Excel files, or have inline HTML tables. Inspect first, then code.
8. **InvestPak may require login** — Check if the rate detail pages work without auth. If login required, skip and note it.
9. **Don't break existing scrapers** — This is purely additive. Existing rate/treasury data stays as-is.
10. **PKRV backfill is high priority** — 21 rows is not usable. Find the existing PKRV scraper and extend it, or find a new backfill source. The yield curve is THE key view for the FI page.

## GIT

```bash
git add -A
git commit -m "feat: SBP bond market data — SMTV scraper, benchmark snapshot, archive backfill

  DATA SOURCES:
  - SBP Outright Secondary Market Trading Volume (daily PDF)
  - SBP benchmark rates (policy rate, KIBOR, MTB/PIB cutoffs)
  - SBP secondary market archives (interbank + nonbank)
  
  TABLES:
  - sbp_bond_trading_daily: per-security OTC trade volumes + yields
  - sbp_bond_trading_summary: daily aggregate totals
  - sbp_benchmark_snapshot: daily benchmark rate snapshots
  
  CLI: psxsync bonds smtv-sync / benchmark-sync / smtv-backfill / status"

git push origin dev
```
