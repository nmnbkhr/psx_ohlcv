# Claude Code Prompt: SBP EasyData API Scraper

## Context

SBP EasyData (`easydata.sbp.org.pk`) provides 18,000+ economic/financial variables 
via REST API. Free registration, API key valid 90 days.

**API Base:** `https://easydata.sbp.org.pk/api/v1`

**Three endpoints:**
```
GET /series/{series_key}/data?api_key=KEY&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&format=json
GET /series/{series_key}/meta?api_key=KEY&format=json
GET /dataset/{dataset_code}/meta?api_key=KEY&format=json
```

**Rate limits:** 250/hour, 2,000/day. API key expires every 90 days.

**Response format (series data):**
```json
{
  "columns": ["Dataset Name", "Series Key", "Series Name", "Observation Date", 
               "Observation Value", "Unit", "Observation Status", "Status Comments"],
  "rows": [
    ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0010", 
     "Total Cash inflow of Workers' remittances", "2022-07-31", 
     "2523.753816", "Million USD", "Normal", ""]
  ]
}
```

**7 Subject Areas:**
1. External Sector (exchange rates, remittances, BoP, FDI, forex reserves)
2. Interest Rates (KIBOR, policy rate, bank rates)
3. Monetary and Financial Sector (money supply, credit, banking)
4. Pakistan's Debt Profile (domestic, external debt)
5. Public Finance (revenue, expenditure, fiscal deficit)
6. Real Sector (CPI, WPI, GDP, industrial production)
7. Social Sector Developments (education, health, demographics)

**Data Sources:** SBP, PBS, FBR, PSX, Ministry of Finance, NEPRA, and others.

**Frequencies:** Daily, Weekly, Monthly, Quarterly, Half-yearly, Annual.

**Storage:** `/mnt/e/psxdata/sbp_easydata/` (separate from pakfindata DB)

## Step 1: Setup

```bash
mkdir -p ~/pakfindata/src/pakfindata/sources
mkdir -p /mnt/e/psxdata/sbp_easydata/{raw,datasets,series}
```

## Step 2: Create the scraper

Create `src/pakfindata/sources/sbp_easydata.py`:

```python
"""
SBP EasyData API Scraper — downloads macro/financial data from State Bank of Pakistan.

18,000+ variables: KIBOR, exchange rates, CPI, money supply, debt, remittances, etc.

API: https://easydata.sbp.org.pk/api/v1
Auth: API key (90-day expiry, 250 req/hour, 2000 req/day)

Usage:
  python -m pakfindata.sources.sbp_easydata discover          # Find all datasets + series
  python -m pakfindata.sources.sbp_easydata fetch-priority     # Download priority datasets
  python -m pakfindata.sources.sbp_easydata fetch-all          # Download everything
  python -m pakfindata.sources.sbp_easydata fetch-series KEY   # Download one series
  python -m pakfindata.sources.sbp_easydata update             # Incremental update (new data only)
  python -m pakfindata.sources.sbp_easydata status             # Show coverage
"""

import requests
import json
import csv
import time
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

API_BASE = "https://easydata.sbp.org.pk/api/v1"

# ⚠️ UPDATE THIS — generate at: My Data Basket → My Account → Generate API Key
API_KEY = "YOUR_API_KEY_HERE"

OUTPUT_DIR = Path("/mnt/e/psxdata/sbp_easydata")
RAW_DIR = OUTPUT_DIR / "raw"          # Raw JSON responses
DATASETS_DIR = OUTPUT_DIR / "datasets"  # Dataset metadata
SERIES_DIR = OUTPUT_DIR / "series"      # Series data CSV/JSON
CATALOG_FILE = OUTPUT_DIR / "catalog.json"  # Master catalog of all series

# Rate limiting
REQUESTS_PER_HOUR = 240   # Stay under 250 limit
REQUESTS_PER_DAY = 1900   # Stay under 2000 limit
DELAY_BETWEEN_REQUESTS = 3600 / REQUESTS_PER_HOUR  # ~15 seconds

# Ensure dirs exist
for d in [OUTPUT_DIR, RAW_DIR, DATASETS_DIR, SERIES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

session = requests.Session()
session.headers.update({"Accept": "application/json"})
session.verify = False  # SBP cert sometimes has issues

_request_count = 0
_hour_start = time.time()


# ═══════════════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════════════

def rate_limit():
    """Respect API rate limits."""
    global _request_count, _hour_start
    
    _request_count += 1
    
    # Reset hourly counter
    if time.time() - _hour_start > 3600:
        _request_count = 0
        _hour_start = time.time()
    
    # Pause if approaching hourly limit
    if _request_count >= REQUESTS_PER_HOUR:
        wait = 3600 - (time.time() - _hour_start) + 10
        if wait > 0:
            print(f"  ⏳ Rate limit — waiting {wait:.0f}s...")
            time.sleep(wait)
        _request_count = 0
        _hour_start = time.time()
    
    time.sleep(DELAY_BETWEEN_REQUESTS)


# ═══════════════════════════════════════════════════════
# API CALLS
# ═══════════════════════════════════════════════════════

def api_get(endpoint: str, params: dict = None) -> dict | None:
    """Make authenticated API request."""
    if params is None:
        params = {}
    params["api_key"] = API_KEY
    
    url = f"{API_BASE}/{endpoint}"
    
    try:
        rate_limit()
        r = session.get(url, params=params, timeout=30)
        
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            print(f"  ⏳ Rate limited — waiting 60s...")
            time.sleep(60)
            return api_get(endpoint, params)  # Retry
        else:
            print(f"  ❌ HTTP {r.status_code}: {url}")
            return None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None


def get_series_data(series_key: str, start_date: str = None, 
                    end_date: str = None) -> dict | None:
    """Get time-series data for a series."""
    params = {"format": "json"}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    
    return api_get(f"series/{series_key}/data", params)


def get_series_meta(series_key: str) -> dict | None:
    """Get metadata for a series."""
    return api_get(f"series/{series_key}/meta")


def get_dataset_meta(dataset_code: str) -> dict | None:
    """Get metadata for a dataset (lists all series in it)."""
    return api_get(f"dataset/{dataset_code}/meta")


# ═══════════════════════════════════════════════════════
# KNOWN DATASET CODES
# ═══════════════════════════════════════════════════════

# Priority datasets for pakfindata (most relevant to PSX analytics)
PRIORITY_DATASETS = {
    # Interest Rates
    "TS_GP_IR_KIBOR_D": "KIBOR Daily",
    "TS_GP_IR_KIBOR_W": "KIBOR Weekly",
    "TS_GP_IR_PRATE_D": "SBP Policy Rate Daily",
    "TS_GP_IR_DSCR_M": "Discount Rate Monthly",
    "TS_GP_IR_WATLR_M": "Weighted Average Lending Rate",
    "TS_GP_IR_WATDR_M": "Weighted Average Deposit Rate",
    
    # Exchange Rates
    "TS_GP_ER_FAERPKR_M": "Exchange Rates Monthly (PKR)",
    "TS_GP_ER_FAERPKR_D": "Exchange Rates Daily (PKR)",
    "TS_GP_ER_REER_M": "Real Effective Exchange Rate",
    
    # External Sector
    "TS_GP_BOP_WR_M": "Workers Remittances Monthly",
    "TS_GP_BOP_FPI_M": "Foreign Portfolio Investment",
    "TS_GP_BOP_BPMM_M": "Balance of Payments Monthly",
    "TS_GP_ES_FCD_M": "Foreign Currency Deposits",
    "TS_GP_ES_GFER_M": "Gold & Forex Reserves",
    
    # Monetary
    "TS_GP_MFS_MSPALLB_M": "Money Supply Monthly",
    "TS_GP_MFS_BCREDIT_M": "Bank Credit Monthly",
    
    # Real Sector
    "TS_GP_RS_CPI_M": "CPI Monthly",
    "TS_GP_RS_WPI_M": "WPI Monthly",
    "TS_GP_RS_SPI_W": "Sensitive Price Index Weekly",
    
    # Debt
    "TS_GP_DP_DDEBT_M": "Domestic Debt Monthly",
    "TS_GP_DP_EDEBT_Q": "External Debt Quarterly",
    
    # Public Finance
    "TS_GP_PF_FBR_M": "FBR Revenue Monthly",
}

# All known dataset code patterns (discovered from URL patterns)
# The full list needs to be discovered via the portal
ALL_DATASET_PREFIXES = [
    "TS_GP_IR_",    # Interest Rates
    "TS_GP_ER_",    # Exchange Rates
    "TS_GP_BOP_",   # Balance of Payments
    "TS_GP_ES_",    # External Sector
    "TS_GP_MFS_",   # Monetary & Financial
    "TS_GP_RS_",    # Real Sector
    "TS_GP_DP_",    # Debt Profile
    "TS_GP_PF_",    # Public Finance
    "TS_GP_SS_",    # Social Sector
]


# ═══════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════

def cmd_discover():
    """
    Discover all available datasets and their series.
    
    Strategy: Try known dataset codes, extract series keys from metadata,
    build a master catalog.
    """
    print("═══════════════════════════════════════")
    print("  SBP EASYDATA — DISCOVER ALL DATASETS")
    print("═══════════════════════════════════════")
    
    catalog = {"datasets": {}, "series": {}, "discovered_at": datetime.now().isoformat()}
    
    # First try priority datasets
    for code, name in PRIORITY_DATASETS.items():
        print(f"\n📂 {code} — {name}")
        meta = get_dataset_meta(code)
        
        if meta:
            # Save raw metadata
            with open(DATASETS_DIR / f"{code}_meta.json", "w") as f:
                json.dump(meta, f, indent=2)
            
            # Extract series keys
            series_keys = extract_series_keys(meta)
            catalog["datasets"][code] = {
                "name": name,
                "series_count": len(series_keys),
                "series_keys": series_keys,
            }
            print(f"  Found {len(series_keys)} series")
            
            for sk in series_keys:
                catalog["series"][sk] = {"dataset": code, "dataset_name": name}
        else:
            print(f"  ⚠️ Not found or empty")
    
    # Save catalog
    with open(CATALOG_FILE, "w") as f:
        json.dump(catalog, f, indent=2)
    
    total_series = len(catalog["series"])
    total_datasets = len(catalog["datasets"])
    print(f"\n✅ Discovered: {total_datasets} datasets, {total_series} series")
    print(f"   Catalog: {CATALOG_FILE}")


def extract_series_keys(meta: dict) -> list:
    """Extract series keys from dataset metadata response."""
    keys = []
    
    # The metadata response structure may vary
    # Try common patterns:
    if isinstance(meta, dict):
        if "rows" in meta:
            for row in meta["rows"]:
                # Series key is typically the second column
                if len(row) >= 2:
                    keys.append(row[1] if isinstance(row[1], str) else str(row[1]))
        elif "series" in meta:
            if isinstance(meta["series"], list):
                keys = meta["series"]
            elif isinstance(meta["series"], dict):
                keys = list(meta["series"].keys())
        elif "data" in meta:
            if isinstance(meta["data"], list):
                for item in meta["data"]:
                    if isinstance(item, dict) and "series_key" in item:
                        keys.append(item["series_key"])
    
    return keys


def cmd_fetch_priority():
    """Download all priority datasets with full history."""
    print("═══════════════════════════════════════")
    print("  SBP EASYDATA — FETCH PRIORITY DATA")
    print("═══════════════════════════════════════")
    
    # Load catalog
    if not CATALOG_FILE.exists():
        print("  ⚠️ No catalog found — running discover first...")
        cmd_discover()
    
    with open(CATALOG_FILE) as f:
        catalog = json.load(f)
    
    total_series = 0
    total_obs = 0
    
    for code, info in catalog.get("datasets", {}).items():
        if code not in PRIORITY_DATASETS:
            continue
        
        print(f"\n📂 {code} — {info.get('name', '')}")
        
        for series_key in info.get("series_keys", []):
            data = get_series_data(
                series_key,
                start_date="1947-01-01",  # Get all history
                end_date=datetime.now().strftime("%Y-%m-%d")
            )
            
            if data and data.get("rows"):
                # Save as JSON
                fp = SERIES_DIR / f"{series_key.replace('.', '_')}.json"
                with open(fp, "w") as f:
                    json.dump(data, f, indent=2)
                
                # Also save as CSV
                fp_csv = SERIES_DIR / f"{series_key.replace('.', '_')}.csv"
                with open(fp_csv, "w", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(data.get("columns", []))
                    w.writerows(data.get("rows", []))
                
                obs = len(data["rows"])
                total_obs += obs
                total_series += 1
                print(f"  ✅ {series_key}: {obs} observations")
            else:
                print(f"  ⚠️ {series_key}: no data")
    
    print(f"\n✅ Total: {total_series} series, {total_obs:,} observations")


def cmd_fetch_series(series_key: str, start_date: str = None):
    """Download a single series."""
    print(f"📥 Fetching: {series_key}")
    
    if not start_date:
        start_date = "1947-01-01"
    
    data = get_series_data(
        series_key,
        start_date=start_date,
        end_date=datetime.now().strftime("%Y-%m-%d")
    )
    
    if data and data.get("rows"):
        # Save JSON
        fp = SERIES_DIR / f"{series_key.replace('.', '_')}.json"
        with open(fp, "w") as f:
            json.dump(data, f, indent=2)
        
        # Save CSV
        fp_csv = SERIES_DIR / f"{series_key.replace('.', '_')}.csv"
        with open(fp_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(data.get("columns", []))
            w.writerows(data.get("rows", []))
        
        print(f"  ✅ {len(data['rows'])} observations → {fp_csv}")
        
        # Show sample
        if data["rows"]:
            first = data["rows"][0]
            last = data["rows"][-1]
            print(f"  Range: {first[3]} → {last[3]}")
            print(f"  Latest: {last[4]} {last[5]}")
    else:
        print(f"  ❌ No data returned")


def cmd_update():
    """Incremental update — fetch only new observations since last download."""
    print("═══════════════════════════════════════")
    print("  SBP EASYDATA — INCREMENTAL UPDATE")
    print("═══════════════════════════════════════")
    
    if not CATALOG_FILE.exists():
        print("  ⚠️ No catalog — run 'discover' first")
        return
    
    with open(CATALOG_FILE) as f:
        catalog = json.load(f)
    
    updated = 0
    
    for series_key in catalog.get("series", {}):
        # Find last date in existing data
        fp = SERIES_DIR / f"{series_key.replace('.', '_')}.json"
        last_date = None
        
        if fp.exists():
            with open(fp) as f:
                existing = json.load(f)
            if existing.get("rows"):
                # Last observation date
                dates = [row[3] for row in existing["rows"] if row[3]]
                if dates:
                    last_date = max(dates)
        
        if not last_date:
            # No existing data — fetch all
            start_date = "1947-01-01"
        else:
            # Fetch from day after last observation
            start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Skip if already up to date (last_date is today or yesterday)
        today = datetime.now().strftime("%Y-%m-%d")
        if last_date and last_date >= (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"):
            continue
        
        data = get_series_data(series_key, start_date=start_date)
        
        if data and data.get("rows"):
            # Merge with existing
            if fp.exists():
                with open(fp) as f:
                    existing = json.load(f)
                
                # Deduplicate by date
                existing_dates = set(row[3] for row in existing.get("rows", []))
                new_rows = [row for row in data["rows"] if row[3] not in existing_dates]
                
                if new_rows:
                    existing["rows"].extend(new_rows)
                    existing["rows"].sort(key=lambda x: x[3])
                    
                    with open(fp, "w") as f:
                        json.dump(existing, f, indent=2)
                    
                    updated += 1
                    print(f"  📥 {series_key}: +{len(new_rows)} new observations")
            else:
                with open(fp, "w") as f:
                    json.dump(data, f, indent=2)
                updated += 1
                print(f"  📥 {series_key}: {len(data['rows'])} observations (new)")
    
    print(f"\n✅ Updated: {updated} series")


def cmd_status():
    """Show download coverage."""
    print("═══════════════════════════════════════")
    print("  SBP EASYDATA — STATUS")
    print("═══════════════════════════════════════")
    
    # Catalog
    if CATALOG_FILE.exists():
        with open(CATALOG_FILE) as f:
            catalog = json.load(f)
        print(f"\n  📋 Catalog:")
        print(f"     Datasets: {len(catalog.get('datasets', {}))}")
        print(f"     Series:   {len(catalog.get('series', {}))}")
        print(f"     Updated:  {catalog.get('discovered_at', 'unknown')}")
    else:
        print("  ❌ No catalog — run 'discover' first")
    
    # Downloaded files
    json_files = list(SERIES_DIR.glob("*.json"))
    csv_files = list(SERIES_DIR.glob("*.csv"))
    
    if json_files:
        total_obs = 0
        latest_dates = []
        
        for fp in json_files:
            try:
                with open(fp) as f:
                    data = json.load(f)
                rows = data.get("rows", [])
                total_obs += len(rows)
                if rows:
                    dates = [r[3] for r in rows if r[3]]
                    if dates:
                        latest_dates.append(max(dates))
            except:
                pass
        
        total_size = sum(f.stat().st_size for f in json_files) + sum(f.stat().st_size for f in csv_files)
        
        print(f"\n  📊 Downloaded Data:")
        print(f"     Series files: {len(json_files)} JSON + {len(csv_files)} CSV")
        print(f"     Observations: {total_obs:,}")
        print(f"     Size:         {total_size / 1024 / 1024:.1f} MB")
        if latest_dates:
            print(f"     Latest date:  {max(latest_dates)}")
    else:
        print("\n  📊 No data downloaded yet")
    
    # API key check
    if API_KEY == "YOUR_API_KEY_HERE":
        print("\n  ⚠️ API_KEY not set! Update it in sbp_easydata.py")
    else:
        print(f"\n  🔑 API Key: {API_KEY[:8]}...{API_KEY[-4:]}")
    
    print(f"\n  📁 Output: {OUTPUT_DIR}")


# ═══════════════════════════════════════════════════════
# QUICK HELPERS (for use in other modules)
# ═══════════════════════════════════════════════════════

def get_kibor(tenor: str = "6M", start_date: str = "2020-01-01") -> list:
    """Quick helper: get KIBOR rate history."""
    # KIBOR series keys follow pattern: TS_GP_IR_KIBOR_D.KIBOR_{tenor}
    series_key = f"TS_GP_IR_KIBOR_D.KIBOR_{tenor}"
    data = get_series_data(series_key, start_date=start_date)
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


def get_exchange_rate(currency: str = "USD", start_date: str = "2020-01-01") -> list:
    """Quick helper: get PKR exchange rate history."""
    # Exchange rate series key pattern varies — discover via catalog
    data = get_series_data(
        f"TS_GP_ER_FAERPKR_D.{currency}PKR",
        start_date=start_date
    )
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


def get_cpi(start_date: str = "2020-01-01") -> list:
    """Quick helper: get CPI inflation history."""
    data = get_series_data(
        "TS_GP_RS_CPI_M.CPI_GENERAL",
        start_date=start_date
    )
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


def get_policy_rate(start_date: str = "2020-01-01") -> list:
    """Quick helper: get SBP policy rate history."""
    data = get_series_data(
        "TS_GP_IR_PRATE_D.PRATE",
        start_date=start_date
    )
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    
    parser = argparse.ArgumentParser(description="SBP EasyData API Scraper")
    parser.add_argument("command", 
                        choices=["discover", "fetch-priority", "fetch-all", 
                                 "fetch-series", "update", "status"])
    parser.add_argument("args", nargs="*", help="Series key (for fetch-series)")
    parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD")
    
    args = parser.parse_args()
    
    if API_KEY == "YOUR_API_KEY_HERE":
        print("❌ Set your API key first!")
        print("   Edit: src/pakfindata/sources/sbp_easydata.py")
        print("   Line: API_KEY = 'YOUR_KEY_HERE'")
        print("   Get key from: easydata.sbp.org.pk → My Data Basket → My Account → Generate API Key")
        sys.exit(1)
    
    if args.command == "discover":
        cmd_discover()
    elif args.command == "fetch-priority":
        cmd_fetch_priority()
    elif args.command == "fetch-all":
        # Discover first, then fetch all series
        cmd_discover()
        # Then fetch all discovered series
        if CATALOG_FILE.exists():
            with open(CATALOG_FILE) as f:
                catalog = json.load(f)
            for series_key in catalog.get("series", {}):
                cmd_fetch_series(series_key)
    elif args.command == "fetch-series":
        if not args.args:
            print("Usage: fetch-series TS_GP_BOP_WR_M.WR0010")
            sys.exit(1)
        cmd_fetch_series(args.args[0], start_date=args.start_date)
    elif args.command == "update":
        cmd_update()
    elif args.command == "status":
        cmd_status()
```

## Step 3: Set your API key

```bash
# Edit the file and replace YOUR_API_KEY_HERE with your actual key
# Get key from: easydata.sbp.org.pk → My Data Basket → My Account → Generate API Key
```

## Step 4: Test with one series first

```bash
cd ~/pakfindata
source .venv/bin/activate
export PYTHONPATH=~/pakfindata/src

# Quick test — fetch Workers' Remittances
python -m pakfindata.sources.sbp_easydata fetch-series TS_GP_BOP_WR_M.WR0010

# Check output
cat /mnt/e/psxdata/sbp_easydata/series/TS_GP_BOP_WR_M_WR0010.json | python3 -m json.tool | head -20
```

## Step 5: Discover all datasets

```bash
# Discover — tries all known dataset codes, builds catalog
python -m pakfindata.sources.sbp_easydata discover

# Check catalog
cat /mnt/e/psxdata/sbp_easydata/catalog.json | python3 -m json.tool | head -40
```

## Step 6: Download priority data

```bash
# Download the ~25 most important datasets with full history
python -m pakfindata.sources.sbp_easydata fetch-priority

# This takes ~30 minutes (rate limited at 250 req/hour)
# Progress shown for each series
```

## Step 7: Check status

```bash
python -m pakfindata.sources.sbp_easydata status
```

## Step 8: Daily update (incremental)

```bash
# Only fetches new observations since last download
python -m pakfindata.sources.sbp_easydata update
```

## Step 9: Integration with pakfindata

These helpers can be used directly from any Streamlit page or engine:

```python
from pakfindata.sources.sbp_easydata import get_kibor, get_exchange_rate, get_cpi, get_policy_rate

# Get KIBOR 6-month rate
kibor = get_kibor("6M", start_date="2024-01-01")
# Returns: [("2024-01-02", 22.09), ("2024-01-03", 22.08), ...]

# Get USD/PKR rate
usdpkr = get_exchange_rate("USD", start_date="2024-01-01")

# Get CPI inflation
cpi = get_cpi(start_date="2020-01-01")

# Get SBP policy rate
rate = get_policy_rate()
```

Future integration:
- `macro_regime.py` → feed CPI, policy rate, money supply
- `kibor_daily` table → replace static data with SBP API live
- Banking profitability engine → lending/deposit rates from SBP
- NEXUS geopolitical → forex reserves, remittances as risk indicators

## IMPORTANT NOTES

1. **API key expires every 90 days** — set a calendar reminder to regenerate.

2. **Rate limits are strict** — 250/hour, 2000/day. The scraper enforces 
   ~15 second delay between requests. Full fetch of all 18K variables 
   would take multiple days. Start with priority datasets.

3. **Series keys must be exact** — the key format is `{dataset_code}.{series_id}`.
   Example: `TS_GP_BOP_WR_M.WR0010`. Discover via dataset metadata.

4. **verify=False** — SBP's SSL certificate sometimes has issues. 
   The scraper disables SSL verification. This is fine for data download.

5. **Dataset codes are GUESSED** — the priority list may have incorrect codes.
   The `discover` command tests each one. Update based on what works.

6. **Data goes back decades** — USD/PKR from 1957, remittances from 1972, 
   KIBOR from 2008. Use `start_date="1947-01-01"` to get everything.

7. **Store separately** — all data goes to `/mnt/e/psxdata/sbp_easydata/`.
   Not integrated into DuckDB yet — review data quality first.
