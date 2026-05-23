# Claude Code Prompt: SBP Bond Market Data — Scraper + Backfill + FI Page Integration

## Context

Working on `pakfindata` — Pakistan financial data platform (Streamlit app).
- DB: `/mnt/e/psxdata/psx.sqlite`
- Project: `~/pakfindata/` (dev branch)
- v3.7.0 is current — already has populated tables from prior work
- The FI page enhancement prompt is running separately — this prompt adds the BOND TRADING data layer
- All imports use `from pakfindata.xxx import yyy`
- CLI command is `pfsync`

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
| `fund_performance` | new   | from MUFAP Phase 2        | 519 funds, daily returns       |
| `mutual_funds`     | 1,190 | full coverage             | 36 MUFAP categories now        |
| `mutual_fund_nav`  | 1.9M  | 1996 — present            | NAV history                    |

### WHAT THIS PROMPT BUILDS (NEW):
1. **SBP Outright Secondary Market Trading Volume** — daily OTC bond trade data (the SMTV PDF)
2. **SBP Benchmark Rate Snapshot** — daily scrape of policy rate, KIBOR, MTB/PIB cutoffs from msm.asp
3. **Archive backfill** — historical secondary market data from SecMarBankArc.asp / SecMarNonBankArc.asp
4. **Deeper PKRV backfill** — from InvestPak or FMA if available (current 21 rows is thin)
5. **KONIA backfill** — same, need more history

Pakistan's bond market is 99% OTC interbank — NOT on PSX exchange. The real bond
quote data comes from SBP, not PSX. This prompt builds that data pipeline.

---

## PHASE 1 — INVESTIGATE EXISTING STATE

### Step 1 — Check what bond/rate tables already exist and have data

```bash
# All rate/yield/bond related tables
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' 
AND (name LIKE '%pkrv%' OR name LIKE '%isrv%' OR name LIKE '%frv%' 
     OR name LIKE '%konia%' OR name LIKE '%kibor%' OR name LIKE '%yield%' 
     OR name LIKE '%bond%' OR name LIKE '%debt%' OR name LIKE '%sukuk%' 
     OR name LIKE '%gis%' OR name LIKE '%pib%' OR name LIKE '%tbill%'
     OR name LIKE '%treasury%' OR name LIKE '%secondary%' OR name LIKE '%smtv%'
     OR name LIKE '%trading_vol%')
ORDER BY name;
"

# For each, show schema + count + date range
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%pkrv%' OR name LIKE '%isrv%' OR name LIKE '%frv%' OR name LIKE '%konia%' OR name LIKE '%kibor%' OR name LIKE '%yield%' OR name LIKE '%bond%' OR name LIKE '%debt%' OR name LIKE '%sukuk%' OR name LIKE '%gis%' OR name LIKE '%pib%' OR name LIKE '%tbill%' OR name LIKE '%treasury%' OR name LIKE '%secondary%');"); do
  echo "=== $tbl ==="
  sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl"
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) as rows FROM $tbl;"
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT MIN(date) as earliest, MAX(date) as latest FROM $tbl;" 2>/dev/null
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT * FROM $tbl ORDER BY rowid DESC LIMIT 3;"
  echo ""
done
```

### Step 2 — Check existing scrapers for rates/bonds

```bash
# Find all existing source files related to rates, bonds, SBP, treasury
find ~/pakfindata/src/ -name "*.py" | xargs grep -l "sbp\|pkrv\|konia\|treasury\|bond\|secondary.*market\|smtv\|outright" 2>/dev/null

# Show existing scraper classes
grep -rn "class.*Scraper\|class.*Source\|def scrape\|def sync" ~/pakfindata/src/pakfindata/sources/ 2>/dev/null | head -30

# Check CLI commands already available
pfsync --help 2>/dev/null | head -30
grep -rn "add_command\|@click\|@app.command\|subparser" ~/pakfindata/src/pakfindata/cli/ ~/pakfindata/src/pakfindata/__main__.py 2>/dev/null | head -30
```

### Step 3 — Check what the SBP archive pages return

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

Create `src/pakfindata/sources/sbp_bond_market.py`:

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
            "User-Agent": "pakfindata/3.7.0"
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

PRIORITY: Use InvestPak to backfill pkrv_daily (currently only 21 rows) 
and konia_daily (only 22 rows). These need MUCH deeper history.
"""
```

### Step 7 — CLI commands

```
pfsync bonds smtv-sync         # Download & parse today's SMTV PDF
pfsync bonds smtv-backfill     # Backfill from archives
pfsync bonds benchmark-sync    # Scrape SBP benchmark snapshot
pfsync bonds status            # Show bond data coverage

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
0 13 * * 1-5  python -m pakfindata bonds smtv-sync
0 13 * * 1-5  python -m pakfindata bonds benchmark-sync
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
pfsync bonds smtv-sync 2>&1

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT security_type, segment, 
       SUM(face_amount) as total_face_m,
       AVG(yield_weighted_avg) as avg_yield
FROM sbp_bond_trading_daily 
WHERE date = (SELECT MAX(date) FROM sbp_bond_trading_daily)
GROUP BY security_type, segment;
"

# Step 12 — Test benchmark scraper
pfsync bonds benchmark-sync 2>&1

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT * FROM sbp_benchmark_snapshot 
WHERE date = (SELECT MAX(date) FROM sbp_benchmark_snapshot)
ORDER BY metric;
"

# Step 13 — Test backfill (try archives)
pfsync bonds smtv-backfill 2>&1

sqlite3 -header /mnt/e/psxdata/psx.sqlite "
SELECT date, SUM(face_amount) as total_face 
FROM sbp_bond_trading_daily 
GROUP BY date ORDER BY date DESC LIMIT 10;
"

# Step 14 — Verify FI page loads bond data
streamlit run src/pakfindata/ui/app.py --server.headless true 2>&1 | head -5

# Step 15 — Tests
pytest tests/ -x -q --tb=short 2>&1 | tail -10
```

---

## CRITICAL RULES

1. **INVESTIGATE FIRST** — Phase 1 tells you what exists. Do NOT duplicate existing tables.
2. **Use existing patterns** — Match the project's existing code style for scrapers, DB modules, CLI commands.
3. **pdfplumber for SMTV PDF** — `pip install pdfplumber` if not installed. The PDF has structured tables.
4. **Handle network failures gracefully** — SBP website can be slow/down. Retry with backoff.
5. **The SMTV PDF changes daily** — Same URL, new content. Save the raw PDF to `data/smtv/` with date in filename for archival.
6. **Archive pages may need exploration** — The SecMarBankArc.asp and SecMarNonBankArc.asp pages may link to individual PDFs, Excel files, or have inline HTML tables. Inspect first, then code.
7. **InvestPak may require login** — Check if the rate detail pages work without auth. If login required, skip and note it.
8. **Don't break existing PKRV/KIBOR scrapers** — This is additive. Existing rate data stays as-is.
9. **All imports use `pakfindata`** — `from pakfindata.xxx import yyy`. CLI is `pfsync`.
10. **PKRV/KONIA backfill is high priority** — 21 and 22 rows respectively is very thin. If InvestPak or FMA provides history, backfill aggressively.

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
  
  CLI: pfsync bonds smtv-sync / benchmark-sync / smtv-backfill / status"

git push origin dev
```
