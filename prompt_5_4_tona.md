# Prompt 5.4 -- TONA Scraper (Bank of Japan)

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
- 5.3: EUSTR scraper working (ECB CSV API)
- `scrape_tona_stub()` exists as empty stub returning []

## TASK
Replace the `scrape_tona_stub()` in `sources/global_rates_scraper.py` with a working
TONA scraper using the Bank of Japan data.

NOTE: TONA is lower priority than SOFR/SONIA/EUSTR for Pakistan markets. JPY-denominated
instruments are rare in PSX. This is for completeness. If the BoJ source proves too
fragile or unreliable, it is acceptable to implement a simpler version.

## SESSION STATE
Update `.claude_session_state.md`:
```
Current Phase: 5.4 -- TONA Scraper
Status: IN PROGRESS
Branch: feat/fcy-global-rates (continue on same branch)
```

## Bank of Japan Data Sources

The BoJ publishes TONA (Tokyo Overnight Average Rate) but does NOT have a clean REST API
like the NY Fed or ECB. There are two approaches:

### Option A -- BoJ Time-Series Data Search (RECOMMENDED)
```
https://www.stat-search.boj.or.jp/ssi/mtshtml/fm08_m_1.html
```
This is an HTML page. However, the BoJ also provides a CSV download endpoint:

```
https://www.stat-search.boj.or.jp/ssi/cgi-bin/famecgi2?cgi=$nme_a000&lng=e&dtf=2&date1=20240101&date2=20251231&code=FM08/FM0802
```

Parameters:
- `lng=e` -- English
- `dtf=2` -- CSV download format
- `date1=YYYYMMDD` -- start date
- `date2=YYYYMMDD` -- end date  
- `code=FM08/FM0802` -- TONA (Call Rate, Uncollateralized Overnight)

The CSV response has a non-standard format with metadata lines at the top.
You need to skip header lines until you find the actual data rows.

Typical response:
```
"Call Rate (Uncollateralized Overnight)"
"Source: Bank of Japan"
...
"Date","FM0802"
"2024/01/04","0.0"
"2024/01/05","0.0"
...
"2025/01/14","0.228"
```

### Option B -- Scrape the HTML table (FALLBACK)
```
https://www3.boj.or.jp/market/en/stat/of_m.htm
```
This shows monthly averages. Less precise but always available.

### Option C -- Use a TONA proxy from FRED (SIMPLEST)
The Federal Reserve Economic Data (FRED) API mirrors TONA:
```
https://api.stlouisfed.org/fred/series/observations?series_id=IRST&api_key=DEMO_KEY&file_type=json&observation_start=2024-01-01
```
- `IRST` is the FRED series ID for Japan uncollateralized overnight rate
- Requires a free API key from FRED (or use DEMO_KEY for testing)
- Clean JSON response
- BUT: data is delayed by 1-2 days vs BoJ direct

RECOMMENDATION: Try Option A first. If the BoJ CSV endpoint is unreliable or the
format is too fragile, fall back to Option C (FRED). Document whichever works.

## Implementation

### Step 1 -- Replace stub in `sources/global_rates_scraper.py`

Find the existing `scrape_tona_stub()` method and replace with:

```python
def scrape_tona(self, days=100) -> list[dict]:
    """Fetch TONA rate from Bank of Japan.
    
    Uses the BoJ Statistical Time-Series CSV endpoint.
    Series: FM08/FM0802 (Call Rate, Uncollateralized Overnight)
    
    Fallback: If BoJ endpoint fails, returns empty list.
    TONA is lower priority -- JPY instruments are rare on PSX.
    """
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    date1 = start_date.strftime('%Y%m%d')
    date2 = end_date.strftime('%Y%m%d')
    
    url = (
        "https://www.stat-search.boj.or.jp/ssi/cgi-bin/famecgi2"
        f"?cgi=$nme_a000&lng=e&dtf=2&date1={date1}&date2={date2}"
        "&code=FM08/FM0802"
    )
    
    try:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"BoJ TONA endpoint failed: {e}")
        return []
    
    results = []
    lines = resp.text.strip().split('\n')
    
    # BoJ CSV has metadata header lines before data
    # Find the line that starts with "Date" or contains date-like patterns
    data_started = False
    
    for line in lines:
        line = line.strip().strip('"')
        if not line:
            continue
        
        # Detect header row
        if 'Date' in line or 'date' in line:
            data_started = True
            continue
        
        if not data_started:
            continue
        
        # Clean and split -- BoJ uses quoted CSV
        parts = [p.strip().strip('"') for p in line.split(',')]
        if len(parts) < 2:
            continue
        
        date_str = parts[0].strip()
        rate_str = parts[1].strip()
        
        if not rate_str or rate_str == '-':
            continue
        
        try:
            rate = float(rate_str)
        except (ValueError, TypeError):
            continue
        
        # Parse BoJ date format "YYYY/MM/DD" -> "YYYY-MM-DD"
        try:
            if '/' in date_str:
                dt = datetime.strptime(date_str, '%Y/%m/%d')
            elif '-' in date_str:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                continue
            iso_date = dt.strftime('%Y-%m-%d')
        except ValueError:
            logger.warning(f"Unparseable TONA date: {date_str}")
            continue
        
        results.append({
            'date': iso_date,
            'rate_name': 'TONA',
            'currency': 'JPY',
            'tenor': 'ON',
            'rate': rate,
            'volume': None,
            'percentile_25': None,
            'percentile_75': None,
            'source': 'boj',
        })
    
    logger.info(f"Scraped {len(results)} TONA rates from Bank of Japan")
    return results
```

### Step 2 -- Update sync_all() in same file

Add TONA after the EUSTR block:

```python
        time.sleep(1)
        
        # TONA (lower priority -- JPY rare on PSX)
        try:
            tona_data = self.scrape_tona(days=150)
            for row in tona_data:
                upsert_global_rate(con, **row)
            stats['TONA'] = len(tona_data)
        except Exception as e:
            logger.warning(f"TONA sync failed (non-critical): {e}")
            stats['TONA'] = f"SKIPPED: {e}"
```

Note: Use `logger.warning` not `logger.error` for TONA failures -- it is non-critical.

### Step 3 -- Update cron comment

In the crontab entry (or wherever the schedule is documented), update the comment:

```bash
# Syncs: SOFR+EFFR (NY Fed), SONIA (BoE), EUSTR (ECB), TONA (BoJ)
0 19 * * 1-5  cd ~/psx_ohlcv && python -m psx_ohlcv.cli globalrates sync >> /tmp/global_rates_sync.log 2>&1
```

## VERIFY

```bash
# 1. Scraper works (requires internet -- BoJ may be slow)
python -c "
from psx_ohlcv.sources.global_rates_scraper import GlobalRatesScraper
s = GlobalRatesScraper()
data = s.scrape_tona(days=30)
if len(data) > 0:
    print(f'TONA scraper OK: {len(data)} rates')
    for d in data[:3]:
        print(f'  {d[\"date\"]}: {d[\"rate\"]}%')
else:
    print('TONA: No data returned (BoJ endpoint may be unreliable)')
    print('This is acceptable -- TONA is low priority for PSX')
"

# 2. Full sync includes all rates
psxsync globalrates sync --count 30
# Should show SOFR, EFFR, SONIA, EUSTR, TONA in stats

# 3. Latest rates show all sources
psxsync globalrates latest
# Should show rates from 4+ central banks

# 4. Rate comparison
python -c "
from psx_ohlcv.db.connection import connect
from psx_ohlcv.db.repositories.global_rates import get_rate_comparison
con = connect()
comp = get_rate_comparison(con)
con.close()
for k, v in comp.items():
    print(f'{k}: {v}')
"
```

NOTE: If the BoJ endpoint returns no data or errors, that is ACCEPTABLE.
The scraper should gracefully return [] and sync_all should log a warning.
Do NOT fail the entire sync pipeline over TONA.

## COMMIT

```bash
git add -A
git commit -m "feat: TONA scraper -- Bank of Japan CSV endpoint

- Replaced scrape_tona_stub() with BoJ time-series CSV scraper
- Series: FM08/FM0802 (Call Rate, Uncollateralized Overnight)
- Added TONA to sync_all() with non-critical error handling
- All 4 global ARRs now active: SOFR, SONIA, EUSTR, TONA
- Source: stat-search.boj.or.jp"
```

Update `.claude_session_state.md`:
```
Current Phase: 5.4 -- TONA Scraper
Status: COMPLETE
Next: Merge feat/fcy-global-rates to dev, tag release
```

---

## POST-COMPLETION: Merge and Tag

After all 4 prompts (5.1-5.4) are complete:

```bash
git checkout dev
git merge feat/fcy-global-rates --no-ff -m "merge: FCY instruments + global reference rates (SOFR/SONIA/EUSTR/TONA)"
git tag -a v3.1.0 -m "v3.1.0: Global reference rates + FCY support"
git push origin dev --tags
```

Final verification:
```bash
psxsync globalrates sync --count 100
psxsync globalrates latest
psxsync globalrates spread --days 30
```

This should show a complete rate dashboard:
- SOFR (USD) -- NY Fed
- EFFR (USD) -- NY Fed
- SONIA (GBP) -- Bank of England
- EUSTR (EUR) -- ECB
- TONA (JPY) -- Bank of Japan
- KIBOR (PKR) -- already existed
- Policy Rate (PKR) -- already existed
