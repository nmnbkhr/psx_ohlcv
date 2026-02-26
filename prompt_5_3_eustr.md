# Prompt 5.3 -- EUSTR Scraper (European Central Bank)

## Context
You are working on the PSX OHLCV project at `~/psx_ohlcv/`.

CODEBASE CONVENTIONS (MUST FOLLOW):
- Database connection: `connect()` from `db.connection`, NEVER `get_db()`
- Repository files: `db/repositories/`
- UI page files: `ui/page_views/`
- CLI: argparse with hierarchical subparsers, NOT Click
- DB path: `/mnt/e/psxdata/psx.sqlite`

COMPLETED:
- 5.1: global_reference_rates table, GlobalRatesScraper class, SOFR/EFFR working
- 5.2: SONIA scraper working (BoE CSV API)
- `scrape_eustr_stub()` exists as empty stub returning []

## TASK
Replace the `scrape_eustr_stub()` in `sources/global_rates_scraper.py` with a working
EUSTR scraper using the ECB Data Portal API.

## SESSION STATE
Update `.claude_session_state.md`:
```
Current Phase: 5.3 -- EUSTR Scraper
Status: IN PROGRESS
Branch: feat/fcy-global-rates (continue on same branch)
```

## ECB Data Portal API Details

The ECB provides EUSTR (Euro Short-Term Rate) via their Statistical Data Warehouse API.

### Primary endpoint (CSV format -- RECOMMENDED):
```
https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25.WT?format=csvdata&startPeriod=2024-01-01&endPeriod=2025-12-31
```

Parameters:
- `format=csvdata` -- returns CSV
- `startPeriod=YYYY-MM-DD` -- start date
- `endPeriod=YYYY-MM-DD` -- end date
- Dataset path: `EST/B.EU000A2X2A25.WT`
  - `EST` = EUSTR dataset
  - `B` = business frequency
  - `EU000A2X2A25` = EUSTR ISIN
  - `WT` = weighted trimmed mean (the main rate)

Response format (CSV with headers):
```
KEY,FREQ,REF_AREA,CURRENCY,PROVIDER_FM_ID,DATA_TYPE_FM,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK,OBS_COM
EST.B.EU000A2X2A25.WT,B,U2,EUR,4F,WT,2024-01-02,3.906,,F,,
EST.B.EU000A2X2A25.WT,B,U2,EUR,4F,WT,2024-01-03,3.907,,F,,
...
```

Key columns:
- `TIME_PERIOD` -- date in YYYY-MM-DD format
- `OBS_VALUE` -- rate as percentage (e.g., 3.906 = 3.906%)

Notes:
- UNAUTHENTICATED, no API key needed
- Returns clean CSV with standard headers
- Dates are already in ISO format (no parsing needed)
- ECB also provides volume data in a separate series (not needed for now)
- Response times are generally fast (2-5 seconds)

### Alternative endpoint (JSON -- fallback):
```
https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25.WT?format=jsondata&startPeriod=2024-01-01
```

Use CSV as primary. JSON as fallback only if CSV parsing fails.

## Implementation

### Step 1 -- Replace stub in `sources/global_rates_scraper.py`

Find the existing `scrape_eustr_stub()` method and replace it with:

```python
def scrape_eustr(self, days=100) -> list[dict]:
    """Fetch EUSTR rate from European Central Bank.
    
    Uses the ECB Statistical Data Warehouse API.
    Dataset: EST (Euro Short-Term Rate)
    Series: B.EU000A2X2A25.WT (weighted trimmed mean)
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    url = (
        "https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25.WT"
        f"?format=csvdata&startPeriod={start_str}&endPeriod={end_str}"
    )
    
    resp = self.session.get(url, timeout=30)
    resp.raise_for_status()
    
    results = []
    lines = resp.text.strip().split('\n')
    
    if len(lines) < 2:
        logger.warning("EUSTR API returned no data rows")
        return results
    
    # Parse CSV header to find column indices
    header = lines[0].split(',')
    try:
        date_idx = header.index('TIME_PERIOD')
        value_idx = header.index('OBS_VALUE')
    except ValueError:
        logger.error(f"EUSTR CSV missing expected columns. Headers: {header}")
        return results
    
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split(',')
        if len(parts) <= max(date_idx, value_idx):
            continue
        
        date_str = parts[date_idx].strip()
        rate_str = parts[value_idx].strip()
        
        if not rate_str:
            continue
        
        try:
            rate = float(rate_str)
        except (ValueError, TypeError):
            continue
        
        # ECB dates are already YYYY-MM-DD
        results.append({
            'date': date_str,
            'rate_name': 'EUSTR',
            'currency': 'EUR',
            'tenor': 'ON',
            'rate': rate,
            'volume': None,
            'percentile_25': None,
            'percentile_75': None,
            'source': 'ecb',
        })
    
    logger.info(f"Scraped {len(results)} EUSTR rates from ECB")
    return results
```

### Step 2 -- Update sync_all() in same file

Add EUSTR after the SONIA block:

```python
        time.sleep(1)
        
        # EUSTR
        try:
            eustr_data = self.scrape_eustr(days=150)
            for row in eustr_data:
                upsert_global_rate(con, **row)
            stats['EUSTR'] = len(eustr_data)
        except Exception as e:
            logger.error(f"EUSTR sync failed: {e}")
            stats['EUSTR'] = f"ERROR: {e}"
```

### Step 3 -- No other changes needed

The CLI, API routes, and Streamlit page all work generically from the
`global_reference_rates` table. EUSTR will automatically appear in:
- `psxsync globalrates latest`
- `GET /api/global-rates/latest`
- Streamlit Rate Dashboard tab

## VERIFY

```bash
# 1. Scraper works (requires internet)
python -c "
from psx_ohlcv.sources.global_rates_scraper import GlobalRatesScraper
s = GlobalRatesScraper()
data = s.scrape_eustr(days=10)
assert len(data) > 0, 'No EUSTR data returned'
print(f'EUSTR scraper OK: {len(data)} rates')
for d in data[:3]:
    print(f'  {d[\"date\"]}: {d[\"rate\"]}%')
"

# 2. Full sync includes EUSTR
psxsync globalrates sync --count 30
# Should show EUSTR: N in stats

# 3. Latest rates show all 4 sources
psxsync globalrates latest
# Should show: SOFR, EFFR, SONIA, EUSTR rows

# 4. API returns EUSTR
uvicorn psx_ohlcv.api.main:app --port 8000 &
sleep 2
curl -s http://localhost:8000/api/global-rates/latest | python -m json.tool | grep -A2 EUSTR
kill %1

# 5. History works
psxsync globalrates history EUSTR --days 10
```

## COMMIT

```bash
git add -A
git commit -m "feat: EUSTR scraper -- ECB Statistical Data Warehouse API

- Replaced scrape_eustr_stub() with working ECB CSV API scraper
- Added EUSTR to sync_all() pipeline
- Dataset: EST/B.EU000A2X2A25.WT (weighted trimmed mean)
- Verified: rate data flowing into global_reference_rates table
- Source: data-api.ecb.europa.eu"
```

Update `.claude_session_state.md`:
```
Current Phase: 5.3 -- EUSTR Scraper
Status: COMPLETE
Next: Prompt 5.4 (TONA) or skip if JPY not needed
```
