# Prompt 5.1 -- FCY Instrument Support + Global Reference Rates (SOFR/SONIA/EUSTR/TONA)

## Context
You are working on the PSX OHLCV project at `~/pakfindata/`.
Project structure: `src/pakfindata/` with sub-packages:
- `db/repositories/` -- all repository modules live here (NOT directly in `db/`)
- `db/connection.py` -- provides `connect()` function (NOT `get_db()`)
- `sources/` -- scrapers
- `collectors/` -- data collectors
- `api/routers/` -- FastAPI route modules
- `ui/page_views/` -- Streamlit page modules (NOT `ui/pages/`)

Database: SQLite at `/mnt/e/psxdata/psx.sqlite` (accessed via `connect()` from `db/connection.py`).

CRITICAL CODEBASE CONVENTIONS:
- Database connection: use `connect()` from `db.connection`, NEVER `get_db()`
- Repository files go in `db/repositories/`, NOT `db/`
- UI page files go in `ui/page_views/`, NOT `ui/pages/`
- CLI uses argparse with hierarchical subparsers, NOT Click
- Two KIBOR tables exist: `kibor_daily` (tenor TEXT: '1W','1M','3M','6M','12M') and `kibor_rates` (tenor_months INTEGER). Use `kibor_daily` -- it is the actively-used one.

We already have: KIBOR rates, SBP policy rates, FX rates (interbank + open market + kerb), bonds, sukuk, fi_instruments tables.

Pakistan's LIBOR transition is complete (June 30, 2023). All FCY-denominated instruments (USD bonds, Eurobonds, FCY sukuk, export refinance) now reference SOFR, SONIA, EUSTR, or TONA instead of LIBOR. We need to:
1. Add global reference rate tracking
2. Add FCY denomination metadata to existing FI instrument tables
3. Build scrapers for NY Fed SOFR API (primary) + stubs for BoE/ECB/BoJ
4. Add API routes and CLI commands
5. Add a Streamlit page for rate visualization

## SESSION STATE
Update `.claude_session_state.md`:
```
Current Phase: 5.1 -- FCY + Global Reference Rates
Status: IN PROGRESS
Branch: feat/fcy-global-rates
```

## GIT
```bash
cd ~/pakfindata
git checkout dev  # or main, whichever is your integration branch
git pull
git checkout -b feat/fcy-global-rates
```

---

## Step 1 -- Database: `src/pakfindata/db/repositories/global_rates.py`

Create a new repository module. Follow the exact same pattern as the existing repo modules in `db/repositories/` (e.g., the KIBOR/policy rate module).

### Tables

```sql
CREATE TABLE IF NOT EXISTS global_reference_rates (
    date TEXT NOT NULL,
    rate_name TEXT NOT NULL,       -- 'SOFR', 'SOFR_AVG_30D', 'SOFR_AVG_90D', 'SOFR_INDEX',
                                   -- 'SONIA', 'EUSTR', 'TONA'
    currency TEXT NOT NULL,        -- 'USD', 'GBP', 'EUR', 'JPY'
    tenor TEXT NOT NULL DEFAULT 'ON',  -- 'ON', '1M', '3M', '6M', '12M'
    rate REAL NOT NULL,            -- annualized percentage (e.g. 5.33 for 5.33%)
    volume REAL,                   -- transaction volume in billions (SOFR has this)
    percentile_25 REAL,            -- 25th percentile (SOFR)
    percentile_75 REAL,            -- 75th percentile (SOFR)
    source TEXT NOT NULL DEFAULT 'nyfed',  -- 'nyfed', 'boe', 'ecb', 'boj'
    fetched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, rate_name, tenor)
);

CREATE INDEX IF NOT EXISTS idx_grr_date ON global_reference_rates(date);
CREATE INDEX IF NOT EXISTS idx_grr_rate_name ON global_reference_rates(rate_name);
CREATE INDEX IF NOT EXISTS idx_grr_currency ON global_reference_rates(currency);

-- Term SOFR (CME) -- these are forward-looking rates used in loan contracts
CREATE TABLE IF NOT EXISTS term_reference_rates (
    date TEXT NOT NULL,
    rate_name TEXT NOT NULL,        -- 'TERM_SOFR', 'TERM_SONIA'
    tenor TEXT NOT NULL,            -- '1M', '3M', '6M', '12M'
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'cme',
    PRIMARY KEY (date, rate_name, tenor)
);

CREATE INDEX IF NOT EXISTS idx_trr_date ON term_reference_rates(date);

-- SOFR-KIBOR spread tracking (key for FX swap pricing in Pakistan)
-- NOTE: Uses kibor_daily (TEXT tenors like '3M','6M'), NOT kibor_rates (INTEGER tenor_months)
CREATE VIEW IF NOT EXISTS v_sofr_kibor_spread AS
SELECT 
    k.date,
    k.tenor,
    k.bid AS kibor_bid,
    k.offer AS kibor_offer,
    g.rate AS sofr_rate,
    ROUND(k.offer - g.rate, 4) AS spread_over_sofr,
    f.selling AS usdpkr
FROM kibor_daily k
LEFT JOIN global_reference_rates g 
    ON g.date = k.date AND g.rate_name = 'SOFR' AND g.tenor = 'ON'
LEFT JOIN sbp_fx_interbank f
    ON f.date = k.date AND f.currency = 'USD'
WHERE k.tenor IN ('1W', '1M', '3M', '6M', '12M')
ORDER BY k.date DESC, k.tenor;
```

### Repository functions

```python
# In src/pakfindata/db/repositories/global_rates.py

def ensure_tables(con): ...  # CREATE TABLE IF NOT EXISTS for both tables + view

def upsert_global_rate(con, date, rate_name, currency, tenor, rate, 
                        volume=None, pct25=None, pct75=None, source='nyfed'): ...

def upsert_term_rate(con, date, rate_name, tenor, currency, rate, source='cme'): ...

def get_latest_rate(con, rate_name='SOFR', tenor='ON') -> dict | None: ...

def get_rate_history(con, rate_name='SOFR', tenor='ON', 
                     start_date=None, end_date=None, limit=365) -> pd.DataFrame: ...

def get_all_latest_rates(con) -> pd.DataFrame:
    """Return latest value of every rate_name/tenor combination."""

def get_sofr_kibor_spread(con, start_date=None, end_date=None) -> pd.DataFrame:
    """Query the v_sofr_kibor_spread view."""

def get_rate_comparison(con, date=None) -> dict:
    """Return a dict with SOFR, SONIA, EUSTR, TONA, KIBOR, policy_rate for a given date.
    Used for dashboard display."""
```

Register in `db/__init__.py` (or `db/repositories/__init__.py` -- follow the existing pattern for how other repos are exported):
```python
from .repositories.global_rates import (
    ensure_tables as ensure_global_rates_tables,
    upsert_global_rate, upsert_term_rate,
    get_latest_rate, get_rate_history,
    get_all_latest_rates, get_sofr_kibor_spread,
    get_rate_comparison
)
```

---

## Step 2 -- Alter Existing FI Tables for FCY Denomination

Run ALTER TABLE on existing tables to add FCY metadata. Do this in a migration function.

```python
# In src/pakfindata/db/repositories/global_rates.py -- add to ensure_tables()

def _migrate_fi_fcy_columns(con):
    """Add FCY columns to existing FI tables if not present."""
    cursor = con.cursor()
    
    # fi_instruments
    for col, default in [
        ('denomination_currency', "'PKR'"),
        ('reference_rate', "NULL"),       # 'KIBOR', 'SOFR', 'FIXED', 'EUSTR'
        ('spread_bps', "NULL"),           # basis points over reference rate
        ('coupon_frequency', "NULL"),     # 'Q', 'SA', 'A' (quarterly, semi-annual, annual)
    ]:
        try:
            cursor.execute(f"ALTER TABLE fi_instruments ADD COLUMN {col} TEXT DEFAULT {default}")
        except Exception:
            pass  # column already exists
    
    # bonds_master
    for col, default in [
        ('denomination_currency', "'PKR'"),
        ('reference_rate', "NULL"),
        ('spread_bps', "NULL"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE bonds_master ADD COLUMN {col} TEXT DEFAULT {default}")
        except Exception:
            pass
    
    # sukuk_master
    for col, default in [
        ('denomination_currency', "'PKR'"),
        ('reference_rate', "NULL"),
        ('spread_bps', "NULL"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE sukuk_master ADD COLUMN {col} TEXT DEFAULT {default}")
        except Exception:
            pass
    
    con.commit()
```

---

## Step 3 -- Scraper: `src/pakfindata/sources/global_rates_scraper.py`

### NY Fed SOFR API (Primary -- this MUST work)

```
Endpoint: https://markets.newyorkfed.org/api/rates/sofr/last/100.json
Response format:
{
  "refRates": [
    {
      "effectiveDate": "2025-05-28",
      "type": "SOFR",
      "percentRate": 4.37,
      "volumeInBillions": 2106.86,
      "percentPercentile25": 4.35,
      "percentPercentile75": 4.38
    },
    ...
  ]
}

Other endpoints:
- SOFR averages: https://markets.newyorkfed.org/api/rates/sofr/last/100.json  (includes SOFRINDEX, AVG30, AVG90, AVG180)
- All secured rates: https://markets.newyorkfed.org/api/rates/secured/last/100.json
- Specific date range: https://markets.newyorkfed.org/api/rates/sofr/search.json?startDate=2024-01-01&endDate=2024-12-31

The NY Fed API is UNAUTHENTICATED, no API key needed, returns clean JSON.
Rate limit: be polite, 1 request per second.
```

### Class Structure

```python
import requests
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class GlobalRatesScraper:
    """Scraper for global alternative reference rates (post-LIBOR)."""
    
    NYFED_BASE = "https://markets.newyorkfed.org/api/rates"
    BOE_BASE = "https://www.bankofengland.co.uk/boeapps/database"
    
    RATE_CONFIGS = {
        'SOFR': {
            'url': f"{NYFED_BASE}/sofr/last/{{count}}.json",
            'source': 'nyfed',
            'currency': 'USD',
            'parser': '_parse_nyfed_sofr',
        },
        'EFFR': {  # Effective Federal Funds Rate -- useful context
            'url': f"{NYFED_BASE}/effr/last/{{count}}.json",
            'source': 'nyfed',
            'currency': 'USD',
            'parser': '_parse_nyfed_effr',
        },
    }
    
    def __init__(self, session=None):
        self.session = session or requests.Session()
        self.session.headers.update({
            'User-Agent': 'PSX-OHLCV-Research/3.0',
            'Accept': 'application/json'
        })
    
    def scrape_sofr(self, count=100) -> list[dict]:
        """Fetch last N days of SOFR from NY Fed."""
        url = f"{self.NYFED_BASE}/sofr/last/{count}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for item in data.get('refRates', []):
            results.append({
                'date': item['effectiveDate'],
                'rate_name': item.get('type', 'SOFR'),
                'currency': 'USD',
                'tenor': 'ON',
                'rate': float(item['percentRate']),
                'volume': float(item.get('volumeInBillions', 0)) if item.get('volumeInBillions') else None,
                'percentile_25': float(item.get('percentPercentile25', 0)) if item.get('percentPercentile25') else None,
                'percentile_75': float(item.get('percentPercentile75', 0)) if item.get('percentPercentile75') else None,
                'source': 'nyfed',
            })
        
        logger.info(f"Scraped {len(results)} SOFR rates from NY Fed")
        return results
    
    def scrape_sofr_averages(self, count=100) -> list[dict]:
        """Fetch SOFR averages (30-day, 90-day, 180-day) and SOFR Index."""
        url = f"{self.NYFED_BASE}/secured/last/{count}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for item in data.get('refRates', []):
            rtype = item.get('type', '')
            if rtype in ('SOFR', 'SOFRINDEX', 'SOFRAI', 'SOFR30DAVG', 'SOFR90DAVG', 'SOFR180DAVG'):
                tenor_map = {
                    'SOFR': 'ON',
                    'SOFRINDEX': 'INDEX',
                    'SOFR30DAVG': '30D_AVG',
                    'SOFR90DAVG': '90D_AVG',
                    'SOFR180DAVG': '180D_AVG',
                }
                results.append({
                    'date': item['effectiveDate'],
                    'rate_name': 'SOFR' if rtype == 'SOFR' else f'SOFR_{rtype.replace("SOFR", "").replace("DAVG", "D_AVG")}',
                    'currency': 'USD',
                    'tenor': tenor_map.get(rtype, rtype),
                    'rate': float(item['percentRate']),
                    'volume': float(item.get('volumeInBillions', 0)) if item.get('volumeInBillions') else None,
                    'percentile_25': float(item.get('percentPercentile25')) if item.get('percentPercentile25') else None,
                    'percentile_75': float(item.get('percentPercentile75')) if item.get('percentPercentile75') else None,
                    'source': 'nyfed',
                })
        
        logger.info(f"Scraped {len(results)} SOFR average rates from NY Fed")
        return results

    def scrape_effr(self, count=100) -> list[dict]:
        """Fetch Effective Federal Funds Rate -- context for SOFR."""
        url = f"{self.NYFED_BASE}/effr/last/{count}.json"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for item in data.get('refRates', []):
            results.append({
                'date': item['effectiveDate'],
                'rate_name': 'EFFR',
                'currency': 'USD',
                'tenor': 'ON',
                'rate': float(item['percentRate']),
                'volume': float(item.get('volumeInBillions', 0)) if item.get('volumeInBillions') else None,
                'percentile_25': float(item.get('percentPercentile25')) if item.get('percentPercentile25') else None,
                'percentile_75': float(item.get('percentPercentile75')) if item.get('percentPercentile75') else None,
                'source': 'nyfed',
            })
        
        logger.info(f"Scraped {len(results)} EFFR rates from NY Fed")
        return results
    
    def scrape_sonia_stub(self) -> list[dict]:
        """STUB: Bank of England SONIA -- implement when needed.
        
        Source: https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?Travel=NIxAZxI1x&FromSeries=1&ToSeries=50&DAession=DA012023&VPD=Y&VFD=N
        Alternative: https://api.bankofengland.co.uk/mfsd/api/v1/dataset/IUDSNOA/data?startdate=2024-01-01
        
        The BoE API returns XML. Parse with:
          resp = requests.get(url)
          from xml.etree import ElementTree
          root = ElementTree.fromstring(resp.text)
        """
        logger.warning("SONIA scraper not yet implemented -- stub only")
        return []
    
    def scrape_eustr_stub(self) -> list[dict]:
        """STUB: ECB EUSTR -- implement when needed.
        
        Source: https://data.ecb.europa.eu/data/datasets/EST/EST.B.EU000A2X2A25.WT
        API: https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25.WT?format=csvdata
        """
        logger.warning("EUSTR scraper not yet implemented -- stub only")
        return []
    
    def scrape_tona_stub(self) -> list[dict]:
        """STUB: BoJ TONA -- implement when needed.
        
        Source: https://www3.boj.or.jp/market/en/stat/of_m.htm
        """
        logger.warning("TONA scraper not yet implemented -- stub only")
        return []
    
    def sync_all(self, con) -> dict:
        """Sync all available rates into database."""
        from pakfindata.db.repositories.global_rates import ensure_tables, upsert_global_rate
        
        ensure_tables(con)
        stats = {}
        
        # SOFR (primary)
        try:
            sofr_data = self.scrape_sofr(count=100)
            for row in sofr_data:
                upsert_global_rate(con, **row)
            stats['SOFR'] = len(sofr_data)
        except Exception as e:
            logger.error(f"SOFR sync failed: {e}")
            stats['SOFR'] = f"ERROR: {e}"
        
        time.sleep(1)  # polite rate limit
        
        # SOFR Averages
        try:
            avg_data = self.scrape_sofr_averages(count=100)
            for row in avg_data:
                upsert_global_rate(con, **row)
            stats['SOFR_AVG'] = len(avg_data)
        except Exception as e:
            logger.error(f"SOFR averages sync failed: {e}")
            stats['SOFR_AVG'] = f"ERROR: {e}"
        
        time.sleep(1)
        
        # EFFR
        try:
            effr_data = self.scrape_effr(count=100)
            for row in effr_data:
                upsert_global_rate(con, **row)
            stats['EFFR'] = len(effr_data)
        except Exception as e:
            logger.error(f"EFFR sync failed: {e}")
            stats['EFFR'] = f"ERROR: {e}"
        
        con.commit()
        logger.info(f"Global rates sync complete: {stats}")
        return stats
```

---

## Step 4 -- CLI Commands

Add to existing CLI (follow the existing argparse subparser pattern in `cli.py`):

```python
# Register 'globalrates' as a subparser under the main parser.
# Follow the EXACT same pattern as other subcommands in cli.py.

def register_globalrates_subparser(subparsers):
    """Register globalrates subcommand group."""
    gr_parser = subparsers.add_parser('globalrates', help='Global reference rates (SOFR, SONIA, EUSTR, TONA)')
    gr_sub = gr_parser.add_subparsers(dest='globalrates_cmd')

    # globalrates sync
    sync_p = gr_sub.add_parser('sync', help='Sync SOFR + EFFR from NY Fed')
    sync_p.add_argument('--count', type=int, default=100, help='Number of days to fetch')
    sync_p.set_defaults(func=cmd_globalrates_sync)

    # globalrates latest
    latest_p = gr_sub.add_parser('latest', help='Show latest global reference rates')
    latest_p.set_defaults(func=cmd_globalrates_latest)

    # globalrates spread
    spread_p = gr_sub.add_parser('spread', help='Show SOFR-KIBOR spread history')
    spread_p.add_argument('--days', type=int, default=30, help='Number of days of history')
    spread_p.set_defaults(func=cmd_globalrates_spread)

    # globalrates history
    hist_p = gr_sub.add_parser('history', help='Show rate history for a specific rate')
    hist_p.add_argument('rate_name', nargs='?', default='SOFR', help='Rate name (default: SOFR)')
    hist_p.add_argument('--days', type=int, default=30, help='Number of days')
    hist_p.set_defaults(func=cmd_globalrates_history)


def cmd_globalrates_sync(args):
    from pakfindata.sources.global_rates_scraper import GlobalRatesScraper
    from pakfindata.db.connection import connect
    from pakfindata.db.repositories.global_rates import ensure_tables

    con = connect()
    ensure_tables(con)
    scraper = GlobalRatesScraper()
    stats = scraper.sync_all(con)
    con.close()
    print(f"Sync complete: {stats}")


def cmd_globalrates_latest(args):
    from pakfindata.db.connection import connect
    from pakfindata.db.repositories.global_rates import get_all_latest_rates

    con = connect()
    df = get_all_latest_rates(con)
    con.close()
    if df.empty:
        print("No rates found. Run: pfsync globalrates sync")
    else:
        print(df.to_string(index=False))


def cmd_globalrates_spread(args):
    from pakfindata.db.connection import connect
    from pakfindata.db.repositories.global_rates import get_sofr_kibor_spread
    from datetime import datetime, timedelta

    con = connect()
    start = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    df = get_sofr_kibor_spread(con, start_date=start)
    con.close()
    if df.empty:
        print("No spread data. Ensure both KIBOR and SOFR are synced.")
    else:
        print(df.to_string(index=False))


def cmd_globalrates_history(args):
    from pakfindata.db.connection import connect
    from pakfindata.db.repositories.global_rates import get_rate_history
    from datetime import datetime, timedelta

    con = connect()
    start = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    df = get_rate_history(con, rate_name=args.rate_name.upper(), start_date=start)
    con.close()
    if df.empty:
        print(f"No data for {args.rate_name}")
    else:
        print(df.to_string(index=False))
```

IMPORTANT: Wire `register_globalrates_subparser(subparsers)` into the main CLI setup
wherever other subparser groups are registered. Look at how existing groups like
`rates`, `fx`, `bonds` etc. are registered and follow that exact pattern.

---

## Step 5 -- FastAPI Routes: `src/pakfindata/api/routers/global_rates.py`

Follow the same pattern as existing routers (e.g., `rates.py` router). Use `connect()` from `db.connection`.

```python
from fastapi import APIRouter, Query
from typing import Optional
from pakfindata.db.connection import connect
from pakfindata.db.repositories import global_rates as gr_repo

router = APIRouter()

@router.get("/latest")
def get_latest(rate_name: Optional[str] = None):
    """Get latest values of all global reference rates."""
    con = connect()
    df = gr_repo.get_all_latest_rates(con)
    con.close()
    # ... return as dict/list matching existing router patterns

@router.get("/sofr")
def get_sofr(days: int = Query(30, ge=1, le=1000)):
    """Get SOFR history."""

@router.get("/effr")
def get_effr(days: int = Query(30, ge=1, le=1000)):
    """Get EFFR history."""

@router.get("/spread/sofr-kibor")
def get_sofr_kibor_spread(days: int = Query(30, ge=1, le=365)):
    """Get SOFR vs KIBOR spread for FX swap pricing analysis."""

@router.get("/comparison")
def get_rate_comparison(date: Optional[str] = None):
    """Compare all rates (SOFR, KIBOR, policy, SONIA, etc.) for a given date."""
```

Register in `api/main.py`:
```python
from .routers import global_rates
app.include_router(global_rates.router, prefix="/api/global-rates", tags=["Global Reference Rates"])
```

---

## Step 6 -- Streamlit Page: `src/pakfindata/ui/page_views/global_rates.py`

Add a new page view (or tab inside existing rates page). Follow the exact same pattern
as other files in `ui/page_views/`. Wire it into the main `ui/app.py` navigation.

### Tab 1: Rate Dashboard
- Show latest SOFR, EFFR, KIBOR, policy rate, SONIA (if available), EUSTR (if available) in a summary table
- Color code: green if rate decreased vs previous, red if increased
- Show date of last update for each

### Tab 2: SOFR History
- Line chart of SOFR over time (use plotly or altair, match your existing charting library)
- Optional: overlay EFFR on same chart
- Show volume bars below (SOFR transaction volume)

### Tab 3: SOFR-KIBOR Spread
- Dual-axis chart: SOFR line + KIBOR line on left axis, spread on right axis
- Table below with: date, KIBOR (offer), SOFR, spread (bps), USD/PKR rate
- This is the KEY analytics view for FX swap traders

### Tab 4: FCY Instrument Browser
- Query fi_instruments, bonds_master, sukuk_master WHERE denomination_currency != 'PKR'
- Show: symbol, name, currency, reference_rate, spread_bps, maturity
- If no FCY instruments exist yet, show a note explaining they can be tagged via admin

---

## Step 7 -- Cron / Scheduler Integration

Add to your existing crontab or scheduler config:

```bash
# NY Fed publishes SOFR at ~8:00 AM ET (next business day)
# That's 6:00 PM PKT -- run at 7:00 PM PKT to be safe
0 19 * * 1-5  cd ~/pakfindata && python -m pakfindata.cli globalrates sync >> /tmp/global_rates_sync.log 2>&1
```

If you use a Python scheduler instead, add to the existing schedule config.

---

## VERIFY

Run these after implementation:

```bash
# 1. Tables created
python -c "
from pakfindata.db.connection import connect
from pakfindata.db.repositories.global_rates import ensure_tables
con = connect()
ensure_tables(con)
cur = con.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%global%' OR name LIKE '%term_%'\")
print('Tables:', [r[0] for r in cur.fetchall()])
cur = con.execute(\"SELECT name FROM sqlite_master WHERE type='view' AND name LIKE '%sofr%'\")
print('Views:', [r[0] for r in cur.fetchall()])
con.close()
print('DB OK')
"

# 2. FI table migration (FCY columns added)
python -c "
from pakfindata.db.connection import connect
con = connect()
cur = con.execute('PRAGMA table_info(fi_instruments)')
cols = [r[1] for r in cur.fetchall()]
assert 'denomination_currency' in cols, 'Missing denomination_currency'
assert 'reference_rate' in cols, 'Missing reference_rate'
assert 'spread_bps' in cols, 'Missing spread_bps'
con.close()
print('FI migration OK')
"

# 3. Scraper works (requires internet)
python -c "
from pakfindata.sources.global_rates_scraper import GlobalRatesScraper
s = GlobalRatesScraper()
data = s.scrape_sofr(count=5)
assert len(data) > 0, 'No SOFR data returned'
print(f'SOFR scraper OK: {len(data)} rates, latest: {data[0][\"date\"]} = {data[0][\"rate\"]}%')
"

# 4. Full sync
pfsync globalrates sync --count 30

# 5. CLI works
pfsync globalrates latest
pfsync globalrates spread --days 7
pfsync globalrates history SOFR --days 7

# 6. API routes
uvicorn pakfindata.api.main:app --port 8000 &
sleep 2
curl -s http://localhost:8000/api/global-rates/latest | python -m json.tool
curl -s http://localhost:8000/api/global-rates/sofr?days=5 | python -m json.tool
curl -s http://localhost:8000/api/global-rates/spread/sofr-kibor?days=7 | python -m json.tool
curl -s http://localhost:8000/docs | grep -c "global-rates"
kill %1

# 7. Streamlit page loads
streamlit run src/pakfindata/ui/app.py --server.headless true 2>&1 | head -5
```

All 7 checks must pass. If any fail, fix before proceeding.

---

## COMMIT

```bash
git add -A
git commit -m "feat: global reference rates (SOFR/EFFR) + FCY instrument support

- New: global_reference_rates + term_reference_rates tables
- New: v_sofr_kibor_spread view for FX swap pricing
- New: NY Fed SOFR/EFFR scraper (with SONIA/EUSTR/TONA stubs)
- New: FCY denomination columns on fi_instruments, bonds_master, sukuk_master
- New: CLI commands (pfsync globalrates sync/latest/spread/history)
- New: FastAPI routes /api/global-rates/*
- New: Streamlit global rates page with spread analytics
- Ref: SBP BPRD LIBOR transition circulars, post-June 2023"
```

Update `.claude_session_state.md`:
```
Current Phase: 5.1 -- FCY + Global Reference Rates
Status: COMPLETE
Branch: feat/fcy-global-rates
Next: Merge to dev, then Prompt 5.2 (if any)
```
