# Prompt 5.2 -- SONIA Scraper (Bank of England)

## Context
You are working on the PSX OHLCV project at `~/psx_ohlcv/`.

CODEBASE CONVENTIONS (MUST FOLLOW):
- Database connection: `connect()` from `db.connection`, NEVER `get_db()`
- Repository files: `db/repositories/`
- UI page files: `ui/page_views/`
- CLI: argparse with hierarchical subparsers, NOT Click
- DB path: `/mnt/e/psxdata/psx.sqlite`

COMPLETED IN 5.1:
- `db/repositories/global_rates.py` -- tables, view, repo functions all exist
- `sources/global_rates_scraper.py` -- GlobalRatesScraper class with SOFR/EFFR working
- `scrape_sonia_stub()` exists as empty stub returning []
- `global_reference_rates` table ready to accept SONIA data
- Correct NY Fed URL pattern: `/api/rates/secured/sofr/last/{count}.json`

## TASK
Replace the `scrape_sonia_stub()` in `sources/global_rates_scraper.py` with a working
SONIA scraper using the Bank of England Statistical Interactive Database API.

## SESSION STATE
Update `.claude_session_state.md`:
```
Current Phase: 5.2 -- SONIA Scraper
Status: IN PROGRESS
Branch: feat/fcy-global-rates (continue on same branch)
```

## Bank of England API Details

The BoE has a public statistical data API. SONIA is series code `IUDSNOA`.

### Primary endpoint (CSV format -- RECOMMENDED):
```
https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/2024&Dateto=31/Dec/2025&SeriesCodes=IUDSNOA&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N
```

Parameters:
- `csv.x=yes` -- return CSV format
- `Datefrom=DD/Mon/YYYY` -- start date (e.g., 01/Jan/2024)
- `Dateto=DD/Mon/YYYY` -- end date
- `SeriesCodes=IUDSNOA` -- SONIA overnight rate
- `CSVF=TN` -- CSV format with tab/newline
- `UsingCodes=Y` -- use series codes as headers
- `VPD=Y` -- include dates

Response format (CSV):
```
DATE,IUDSNOA
02 Jan 2024,5.1930
03 Jan 2024,5.1930
04 Jan 2024,5.1930
...
```

Notes:
- The API is UNAUTHENTICATED, no key needed
- Date format in response is "DD Mon YYYY" (e.g., "02 Jan 2024")
- Rate is in percentage (e.g., 5.1930 means 5.193%)
- No data on weekends/holidays
- The API can be slow (5-10 second response times) -- use timeout=30
- User-Agent header recommended

### Additional SONIA series (optional, add if time permits):
- `IUDSNOA` -- SONIA rate (primary)
- `IUDSOIA` -- SONIA Index
- `IUDSOIACA` -- SONIA Compounded Index

## Implementation

### Step 1 -- Replace stub in `sources/global_rates_scraper.py`

Find the existing `scrape_sonia_stub()` method and replace it with:

```python
def scrape_sonia(self, days=100) -> list[dict]:
    """Fetch SONIA rate from Bank of England.
    
    Uses the BoE Statistical Interactive Database CSV API.
    Series: IUDSNOA (Sterling Overnight Index Average)
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # BoE date format: DD/Mon/YYYY
    date_from = start_date.strftime('%d/%b/%Y')
    date_to = end_date.strftime('%d/%b/%Y')
    
    url = (
        "https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp"
        f"?csv.x=yes&Datefrom={date_from}&Dateto={date_to}"
        "&SeriesCodes=IUDSNOA&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N"
    )
    
    resp = self.session.get(url, timeout=30)
    resp.raise_for_status()
    
    results = []
    lines = resp.text.strip().split('\n')
    
    # Skip header line
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split(',')
        if len(parts) < 2:
            continue
        
        date_str = parts[0].strip()
        rate_str = parts[1].strip()
        
        # Skip if rate is empty or non-numeric
        try:
            rate = float(rate_str)
        except (ValueError, TypeError):
            continue
        
        # Parse BoE date format "DD Mon YYYY" -> "YYYY-MM-DD"
        try:
            dt = datetime.strptime(date_str, '%d %b %Y')
            iso_date = dt.strftime('%Y-%m-%d')
        except ValueError:
            logger.warning(f"Unparseable SONIA date: {date_str}")
            continue
        
        results.append({
            'date': iso_date,
            'rate_name': 'SONIA',
            'currency': 'GBP',
            'tenor': 'ON',
            'rate': rate,
            'volume': None,       # BoE doesn't provide volume in this endpoint
            'percentile_25': None,
            'percentile_75': None,
            'source': 'boe',
        })
    
    logger.info(f"Scraped {len(results)} SONIA rates from Bank of England")
    return results
```

### Step 2 -- Update sync_all() in same file

Add SONIA to the `sync_all()` method, after the EFFR block:

```python
        time.sleep(1)
        
        # SONIA
        try:
            sonia_data = self.scrape_sonia(days=150)  # ~100 business days
            for row in sonia_data:
                upsert_global_rate(con, **row)
            stats['SONIA'] = len(sonia_data)
        except Exception as e:
            logger.error(f"SONIA sync failed: {e}")
            stats['SONIA'] = f"ERROR: {e}"
```

### Step 3 -- Add SONIA to CLI 'sync' output

No CLI changes needed -- `psxsync globalrates sync` already calls `sync_all()` which
will now include SONIA. Just verify it shows in the output.

### Step 4 -- Update Streamlit page

In `ui/page_views/global_rates.py`, Tab 1 (Rate Dashboard):
- SONIA should now appear in the rates table alongside SOFR/EFFR/KIBOR
- No code change needed if the dashboard queries `get_all_latest_rates()` generically

If the dashboard has hardcoded rate names, add 'SONIA' to the list.

## VERIFY

```bash
# 1. Scraper works (requires internet)
python -c "
from psx_ohlcv.sources.global_rates_scraper import GlobalRatesScraper
s = GlobalRatesScraper()
data = s.scrape_sonia(days=10)
assert len(data) > 0, 'No SONIA data returned'
print(f'SONIA scraper OK: {len(data)} rates')
for d in data[:3]:
    print(f'  {d[\"date\"]}: {d[\"rate\"]}%')
"

# 2. Full sync includes SONIA
psxsync globalrates sync --count 30
# Should show SONIA: N in the stats output

# 3. Latest rates show SONIA
psxsync globalrates latest
# Should include a SONIA row

# 4. API returns SONIA
uvicorn psx_ohlcv.api.main:app --port 8000 &
sleep 2
curl -s http://localhost:8000/api/global-rates/latest | python -m json.tool | grep -A2 SONIA
kill %1

# 5. History works
psxsync globalrates history SONIA --days 10
```

## COMMIT

```bash
git add -A
git commit -m "feat: SONIA scraper -- Bank of England CSV API

- Replaced scrape_sonia_stub() with working BoE IUDSNOA series scraper
- Added SONIA to sync_all() pipeline
- Verified: rate data flowing into global_reference_rates table
- Source: bankofengland.co.uk Statistical Interactive Database"
```

Update `.claude_session_state.md`:
```
Current Phase: 5.2 -- SONIA Scraper
Status: COMPLETE
Next: Prompt 5.3 (EUSTR)
```
