# Claude Code Prompt: Enhance Bond/FI Page with Real Data

## Context

I'm working on `pakfindata` — a Pakistan financial data platform with Streamlit UI.
- DB: `/mnt/e/psxdata/psx.sqlite`
- Project: `~/pakfindata/` (dev branch)
- The app already has PKRV, PKISRV, PKFRV rate tables stored in SQLite
- There is a Streamlit FI Overview page that currently has fake/placeholder bond data
- The app uses `st.navigation()` for page routing (recently fixed)

## PHASE 1 — INVESTIGATE (show me ALL output before making changes)

### Step 1 — Check existing rate tables and their data

```bash
sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%pkrv%' OR name LIKE '%isrv%' OR name LIKE '%frv%' OR name LIKE '%konia%' OR name LIKE '%yield%' OR name LIKE '%bond%' OR name LIKE '%debt%' OR name LIKE '%sukuk%' OR name LIKE '%gis%' OR name LIKE '%pib%' OR name LIKE '%tbill%' ORDER BY name;"

# For each table found, show schema + row count + sample data:
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%pkrv%' OR name LIKE '%isrv%' OR name LIKE '%frv%' OR name LIKE '%konia%' OR name LIKE '%yield%' OR name LIKE '%bond%' OR name LIKE '%debt%' OR name LIKE '%sukuk%' OR name LIKE '%gis%' OR name LIKE '%pib%' OR name LIKE '%tbill%');"); do
  echo "=== TABLE: $tbl ==="
  sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl"
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) as rows FROM $tbl;"
  echo "--- Latest 5 rows ---"
  sqlite3 -header /mnt/e/psxdata/psx.sqlite "SELECT * FROM $tbl ORDER BY rowid DESC LIMIT 5;"
  echo "--- Date range ---"
  sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT MIN(date) as earliest, MAX(date) as latest FROM $tbl;" 2>/dev/null || echo "(no date column)"
  echo ""
done
```

### Step 2 — Check the current FI/Bond page(s)

```bash
# Find all FI/bond related UI files
find ~/pakfindata/src/pakfindata/ui/ -name "*fi*" -o -name "*fixed*" -o -name "*bond*" -o -name "*debt*" -o -name "*income*" | sort

# Show the main FI page content
cat $(find ~/pakfindata/src/pakfindata/ui/pages/ -name "*fi*" -o -name "*fixed*" -o -name "*bond*" -o -name "*debt*" | head -1)

# Check app.py for how FI pages are registered
grep -n "fi_\|fixed\|bond\|debt\|income\|FI\|Fixed Income" ~/pakfindata/src/pakfindata/ui/app.py
```

### Step 3 — Check existing DB query functions for rates

```bash
# Find existing rate/yield/bond query functions
grep -rn "def.*pkrv\|def.*isrv\|def.*frv\|def.*konia\|def.*yield\|def.*bond\|def.*debt\|def.*sukuk" ~/pakfindata/src/pakfindata/db/ ~/pakfindata/src/pakfindata/sources/ 2>/dev/null

# Check if there are existing rate scrapers
find ~/pakfindata/src/pakfindata/sources/ -name "*rate*" -o -name "*sbp*" -o -name "*mufap*" -o -name "*pkrv*" | sort
cat $(find ~/pakfindata/src/pakfindata/sources/ -name "*rate*" -o -name "*sbp*" | head -1) 2>/dev/null | head -80

# Check if there's a bond scraper from PSX debt market
grep -rn "debt-market\|dps.psx.com.pk/debt\|debt_market" ~/pakfindata/src/ 2>/dev/null
```

### Step 4 — Check what data the current FI page uses (real vs fake)

```bash
# Look for hardcoded/fake data in FI pages
grep -n "sample\|fake\|dummy\|placeholder\|mock\|hardcode\|example.*data\|demo" $(find ~/pakfindata/src/pakfindata/ui/pages/ -name "*fi*" -o -name "*bond*" -o -name "*debt*") 2>/dev/null

# Look for DataFrame creation with fake data
grep -n "pd.DataFrame\|DataFrame(" $(find ~/pakfindata/src/pakfindata/ui/pages/ -name "*fi*" -o -name "*bond*" -o -name "*debt*") 2>/dev/null
```

**STOP HERE. Show me all output from Steps 1-4 before proceeding.**

---

## PHASE 2 — SCRAPE REAL BOND LISTINGS FROM PSX

The PSX Data Portal at `dps.psx.com.pk/debt-market` has real bond listings across 4 tabs:
- GoP Ijarah Sukuk
- Public Debt Securities  
- Privately Debt Securities
- Government Debt Securities

Each bond has: Security Code, Security Name, Face Value, Listing Date, Issue Date, 
Issue Size, Maturity Date, Coupon/Rental Rate, Previous/Next Coupon Date, Outstanding Days, Remaining Years.

Individual bond pages are at: `dps.psx.com.pk/debt/{SECURITY_CODE}` (e.g., `/debt/P01GIS040226`)

### Step 5 — Create bond listings scraper

Create `src/pakfindata/sources/psx_debt.py`:

```python
"""
Scraper for PSX Fixed Income Securities from dps.psx.com.pk/debt-market

The page is client-side rendered. Try these approaches in order:
1. Check if there's a JSON API backing the page (inspect for XHR/fetch patterns 
   similar to /market-watch endpoint)
2. If no JSON API, use requests + BeautifulSoup to parse the HTML table
3. The page has 4 tabs — each tab likely hits a different API or has data 
   embedded in a <script> tag

Security categories:
- gis: GoP Ijarah Sukuk (Islamic govt bonds)
- pub: Public Debt Securities (TFCs, corporate bonds)
- pri: Privately Placed Debt Securities  
- gov: Government Debt Securities (PIBs)
"""

import requests
import pandas as pd
from datetime import datetime
import re
import json
import logging

logger = logging.getLogger(__name__)

class PSXDebtScraper:
    BASE_URL = "https://dps.psx.com.pk"
    DEBT_URL = f"{BASE_URL}/debt-market"
    
    CATEGORIES = {
        "gis": "GoP Ijarah Sukuk",
        "pub": "Public Debt Securities",
        "pri": "Privately Debt Securities", 
        "gov": "Government Debt Securities",
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/html",
            "Referer": self.DEBT_URL,
        })
    
    def scrape_all(self) -> pd.DataFrame:
        """
        Scrape all bond listings from all 4 categories.
        
        First try: fetch the HTML page and look for embedded JSON data in 
        <script> tags (PSX portal often embeds data as JSON in the page).
        
        Second try: look for API endpoint patterns like:
        - /api/debt-market?category=gis
        - /debt-market/data
        - /api/securities?type=debt
        
        Third try: parse HTML tables directly with BeautifulSoup.
        
        Returns DataFrame with columns:
        security_code, security_name, category, face_value, listing_date,
        issue_date, issue_size, maturity_date, coupon_rate, prev_coupon_date,
        next_coupon_date, outstanding_days, remaining_years
        """
        # Implementation: try JSON first, fall back to HTML parsing
        pass
    
    def scrape_bond_detail(self, security_code: str) -> dict:
        """Scrape individual bond details from /debt/{security_code}"""
        pass
```

### Step 6 — Create bond listings DB schema + functions

Create or update `src/pakfindata/db/bonds.py`:

```sql
CREATE TABLE IF NOT EXISTS psx_debt_securities (
    security_code TEXT PRIMARY KEY,
    security_name TEXT NOT NULL,
    category TEXT NOT NULL,          -- 'gis', 'pub', 'pri', 'gov'
    face_value REAL,
    listing_date TEXT,
    issue_date TEXT,
    issue_size REAL,                 -- in billions
    maturity_date TEXT,
    coupon_rate REAL,                -- percentage
    prev_coupon_date TEXT,
    next_coupon_date TEXT,
    outstanding_days INTEGER,
    remaining_years REAL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_debt_category ON psx_debt_securities(category);
CREATE INDEX IF NOT EXISTS idx_debt_maturity ON psx_debt_securities(maturity_date);
```

Functions needed:
- `init_bond_schema(con)`
- `upsert_bonds(con, df: pd.DataFrame) -> int`
- `get_bonds(con, category=None) -> pd.DataFrame`
- `get_bond_detail(con, security_code) -> dict`
- `get_bonds_by_maturity_range(con, start_date, end_date) -> pd.DataFrame`

### Step 7 — CLI command

Add to CLI:
```
pfsync bonds sync       # scrape all 4 categories from PSX debt market
pfsync bonds list       # show bond count by category
pfsync bonds detail P01GIS040226  # show individual bond
```

Test the scraper:
```bash
python -m pakfindata bonds sync
sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT category, COUNT(*) FROM psx_debt_securities GROUP BY category;"
```

---

## PHASE 3 — ENHANCE THE STREAMLIT FI PAGE

Replace ALL fake/placeholder data with real data from the database.

### Step 8 — Rebuild the FI Overview page

The FI page should have these sections, ALL powered by real data:

**Section 1: PKRV Yield Curve (interactive chart)**
```
- Read latest PKRV curve from pkrv_daily_curve (or whatever the table is named)
- Plotly line chart: X = tenor (1M, 3M, 6M, 1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 15Y, 20Y, 30Y)
                     Y = yield %
- Add comparison: toggle to overlay previous day, previous week, previous month curve
- Show curve shift (bps change) in a small table below
- Color: use the app's dark theme colors
```

**Section 2: PKISRV + PKFRV Curves (if data exists)**
```
- If pkisrv (Islamic revaluation) and pkfrv (floating rate) tables exist:
  - Show them as additional tabs or overlaid on same chart
  - Show spread between conventional PKRV and Islamic PKISRV
```

**Section 3: Rate History Charts**
```
- PKRV history for key tenors (3M, 1Y, 5Y, 10Y) over last 6 months
- KONIA overnight rate history (if table exists)
- KIBOR term rates history (if table exists)
- Interactive date range selector
```

**Section 4: Bond Listings (from psx_debt_securities)**
```
- Tabs: "GoP Ijarah Sukuk" | "Public Debt" | "Private Debt" | "Govt Debt"
- Each tab shows a searchable/sortable dataframe:
  Security Code | Name | Face Value | Coupon Rate | Maturity | Issue Size | Remaining Years
- Add filters: maturity range, coupon range
- Highlight: bonds maturing within 3 months (yellow), within 1 month (red)
- Click on a bond code → expand to show full details
```

**Section 5: T-Bill + PIB Auction Results (if tables exist)**
```
- Latest T-Bill auction: cut-off yields for 3M, 6M, 12M
- Latest PIB auction: cut-off yields by tenor
- Historical auction yield chart
- Bid-to-cover ratio trend
```

**Section 6: Market Summary Metrics (top of page)**
```
- Total securities listed: COUNT from psx_debt_securities
- By category breakdown
- Latest PKRV 10Y rate
- PKRV 10Y change from yesterday
- KONIA rate (if available)
- SBP policy rate (if available in any table)
```

### Step 9 — Add sync button on the page

```python
# At top of FI page, add refresh capability:
col1, col2, col3 = st.columns([6, 1, 1])
with col2:
    if st.button("🔄 Sync Rates"):
        # Call the PKRV/KONIA scraper
        # from pakfindata.sources.sbp_rates import SBPRatesScraper
        # ... sync and show success/fail
        pass
with col3:
    if st.button("🔄 Sync Bonds"):
        # Call the PSX debt scraper
        # from pakfindata.sources.psx_debt import PSXDebtScraper
        # ... sync and show success/fail
        pass
```

### Step 10 — Remove ALL fake/placeholder data

```
Search the entire FI page file for:
- Hardcoded DataFrames with sample data
- Random number generation (np.random, random.randint, etc.)
- Static lists of bond names/codes that aren't from DB
- Any "placeholder", "sample", "demo", "fake" data

Replace EVERY instance with a real DB query.

If a table is empty (no data synced yet), show:
  st.info("No data available. Click 'Sync Rates' to fetch latest data.")
NOT fake data.
```

---

## PHASE 4 — VERIFY

```bash
# Step 11 — Run the bond scraper
python -m pakfindata bonds sync 2>&1

# Check data populated
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
  SELECT category, COUNT(*) as count FROM psx_debt_securities GROUP BY category;
"
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
  SELECT security_code, security_name, coupon_rate, maturity_date, remaining_years 
  FROM psx_debt_securities 
  ORDER BY remaining_years 
  LIMIT 10;
"

# Check rate data
sqlite3 -header /mnt/e/psxdata/psx.sqlite "
  SELECT * FROM pkrv_daily_curve WHERE date = (SELECT MAX(date) FROM pkrv_daily_curve) ORDER BY tenor_months;
" 2>/dev/null || echo "PKRV table name may differ — check Step 1 output"

# Step 12 — Start Streamlit and verify
streamlit run src/pakfindata/ui/app.py --server.headless true 2>&1 | head -5

# Step 13 — Run tests
pytest tests/ -x -q --tb=short 2>&1 | tail -10
```

---

## CRITICAL RULES

1. **INVESTIGATE FIRST** — Phase 1 output tells you what tables exist, what data is there, and what the current page looks like. Do NOT assume table names.
2. **Use EXISTING table names** — The tables might be named `pkrv_daily_curve` or `pkrv_daily` or something else. Use what you find.
3. **Use EXISTING query functions** — If `db/yield_curves.py` already has `get_pkrv_curve()`, use it. Don't duplicate.
4. **Use EXISTING scrapers** — If `sources/sbp_rates.py` already has PKRV scraper, don't recreate. Extend if needed.
5. **NO fake data anywhere** — If no data exists, show an info message, not placeholder rows.
6. **Match the app's dark theme** — Use existing Plotly theme/colors from other pages.
7. **st.navigation() page isolation** — The FI page must be a single render function. No side effects at module level.
8. **Handle empty tables gracefully** — `if df.empty: st.info("No data. Sync first.")`

Commit after each phase:
- Phase 2: `git commit -m "feat: PSX debt securities scraper + bond listings table"`
- Phase 3: `git commit -m "feat: FI page with real PKRV curves, bond listings, auction data"`
- Phase 4: `git commit -m "fix: verify all FI data flows end-to-end"`

Final: `git push origin dev`
