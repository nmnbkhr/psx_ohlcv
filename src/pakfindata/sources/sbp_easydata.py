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
API_KEY ="6353A150564FE7E3E4C5BC070ED4333A7A1BDA96"
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

# Priority datasets — verified working codes from easydata.sbp.org.pk
PRIORITY_DATASETS = {
    # Interest Rates
    "TS_GP_BAM_SIRKIBOR_D": "KIBOR Daily",
    "TS_GP_IR_SIRPR_AH": "SBP Policy Rate (Ad-hoc)",
    "TS_GP_BAM_WALDR_M": "Weighted Avg Lending/Deposit Rates Monthly",

    # Exchange Rates
    "TS_GP_ER_FAERPKR_M": "Exchange Rates Monthly (PKR)",
    "TS_GP_ER_FAERUSD_M": "Exchange Rates Monthly (USD)",
    "TS_GP_ER_FMEERPKR_M": "Open Market Exchange Rates (PKR)",
    "TS_GP_ER_REERNEER_M": "REER/NEER Indices",
    "TS_GP_ES_FADERPKR_M": "Daily Average Exchange Rates (PKR)",

    # External Sector / Balance of Payments
    "TS_GP_BOP_WR_M": "Workers Remittances Monthly",
    "TS_GP_BOP_BPM6SUM_M": "Balance of Payments Monthly (BPM6)",
    "TS_GP_BOP_BPM6SUM_Q": "Balance of Payments Quarterly (BPM6)",
    "TS_GP_ES_PKBOPSTND_M": "BoP Standard Presentation Monthly",
    "TS_GP_BOP_FCD_M": "Foreign Currency Deposits Monthly",
    "TS_GP_EXT_PAKRES_M": "Gold & Forex Reserves Monthly",
    "TS_GP_ES_KSORDA_M": "Roshan Digital Account Monthly",

    # Foreign Investment
    "TS_GP_FI_SUMFIPK_M": "Foreign Investment Summary Monthly",
    "TS_GP_BOP_FDIISIC4_M": "FDI by Sector (ISIC-IV) Monthly",
    "TS_GP_FI_REPATFI_M": "Profit/Dividend Repatriation Monthly",

    # Monetary & Financial
    "TS_GP_MFS_MSPALLB_M": "Money Supply Monthly",
    "TS_GP_MFS_PSD_Q": "Payment Systems Quarterly",

    # Prices
    "TS_GP_PT_CPI_M": "CPI Monthly",

    # Real Sector
    "TS_GP_RLS_EMPLSM_M": "Employment in LSM Monthly",
    "TS_GP_RLS_ELECGEN_M": "Electricity Generation Monthly",
    "TS_GP_RLS_POLSALE_M": "POL Sales Monthly",

    # Banking
    "TS_GP_BS_NDAND_HY": "Deposit Accounts & Depositors (Half-yearly)",
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

KIBOR_TENORS = {
    "1W": "1KIBOR1W", "2W": "2KIBOR2W", "1M": "KIBOR0010",
    "3M": "KIBOR0020", "6M": "KIBOR0030", "9M": "6KIBOR9M",
    "1Y": "7KIBOR12M", "2Y": "8KIBOR2Y", "3Y": "9KIBOR3Y",
}


def get_kibor(tenor: str = "6M", start_date: str = "2020-01-01") -> list:
    """Quick helper: get KIBOR rate history."""
    code = KIBOR_TENORS.get(tenor, "KIBOR0030")
    series_key = f"TS_GP_BAM_SIRKIBOR_D.{code}"
    data = get_series_data(series_key, start_date=start_date)
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


def get_exchange_rate(currency: str = "USD", start_date: str = "2020-01-01") -> list:
    """Quick helper: get PKR exchange rate history (monthly)."""
    # Load catalog to find the right series key for the currency
    if CATALOG_FILE.exists():
        with open(CATALOG_FILE) as f:
            catalog = json.load(f)
        for sk in catalog.get("series", {}):
            if sk.startswith("TS_GP_ER_FAERPKR_M.") and currency.lower() in sk.lower():
                data = get_series_data(sk, start_date=start_date)
                if data and data.get("rows"):
                    return [(row[3], float(row[4])) for row in data["rows"]]
    return []


def get_cpi(start_date: str = "2020-01-01") -> list:
    """Quick helper: get National CPI inflation (YoY)."""
    data = get_series_data(
        "TS_GP_PT_CPI_M.P00011516",
        start_date=start_date
    )
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


def get_policy_rate(start_date: str = "2020-01-01") -> list:
    """Quick helper: get SBP policy (target) rate."""
    data = get_series_data(
        "TS_GP_IR_SIRPR_AH.SBPOL0030",
        start_date=start_date
    )
    if data and data.get("rows"):
        return [(row[3], float(row[4])) for row in data["rows"]]
    return []


# ═══════════════════════════════════════════════════════
# READER — Load downloaded EasyData CSVs into DataFrames
# ═══════════════════════════════════════════════════════

def read_series(series_key: str) -> list[dict]:
    """Read a single EasyData series CSV. Returns list of {date, value} dicts."""
    # File naming: dots in key replaced with underscores
    fname = series_key.replace(".", "_")
    csv_path = SERIES_DIR / f"{fname}.csv"
    if not csv_path.exists():
        return []
    rows = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            val = row.get("Observation Value", "")
            if val == "" or val is None:
                continue
            try:
                rows.append({
                    "date": row["Observation Date"],
                    "value": float(val),
                    "series": series_key,
                    "name": row.get("Series Name", ""),
                })
            except (ValueError, KeyError):
                continue
    return rows


def read_dataset_series(dataset_code: str) -> dict[str, list[dict]]:
    """Read all series for a dataset. Returns {series_key: [{date, value, ...}]}."""
    cat = _load_catalog()
    if not cat:
        return {}
    ds = cat.get("datasets", {}).get(dataset_code, {})
    result = {}
    for sk in ds.get("series_keys", []):
        data = read_series(sk)
        if data:
            result[sk] = data
    return result


def _load_catalog() -> dict:
    if CATALOG_FILE.exists():
        return json.load(open(CATALOG_FILE))
    return {}


# ═══════════════════════════════════════════════════════
# DB SYNC — Replace web scrapers with EasyData CSV reads
# ═══════════════════════════════════════════════════════

# KIBOR tenor mapping: EasyData series key suffix → DB tenor name
_KIBOR_MAP = {
    "1KIBOR1W": ("1W", "offer"), "2KIBOR2W": ("2W", "offer"),
    "3KIBOR1M": ("1M", "offer"), "4KIBOR3M": ("3M", "offer"),
    "5KIBOR6M": ("6M", "offer"), "6KIBOR9M": ("9M", "offer"),
    "7KIBOR12M": ("12M", "offer"), "8KIBOR2Y": ("2Y", "offer"),
    "9KIBOR3Y": ("3Y", "offer"),
    "10KIBID1W": ("1W", "bid"), "11KIBID2W": ("2W", "bid"),
    "12KIBID1M": ("1M", "bid"), "13KIBID3M": ("3M", "bid"),
    "14KIBID6M": ("6M", "bid"), "15KIBID9M": ("9M", "bid"),
    "16KIBID12M": ("12M", "bid"), "17KIBID2Y": ("2Y", "bid"),
    "18KIBID3Y": ("3Y", "bid"),
}

# FX currency mapping: EasyData series suffix → ISO currency code
_FX_MAP = {
    "E00010": "AUD", "E00020": "BHD", "E00030": "CAD", "E00040": "CNY",
    "E00050": "DKK", "E00060": "HKD", "E00070": "JPY", "E00080": "KWD",
    "E00090": "MYR", "E00100": "NZD", "E00110": "NOK", "E00120": "OMR",
    "E00130": "QAR", "E00140": "SGD", "E00150": "SEK", "E00160": "CHF",
    "E00170": "SAR", "E00180": "THB", "E00190": "TRY", "E00200": "AED",
    "E00210": "GBP", "E00220": "USD", "E00230": "EUR", "E00240": "SDR",
}


def sync_kibor_to_db(con, since: str = "") -> dict:
    """Load KIBOR/KIBID from EasyData CSVs into kibor_daily table."""
    import sqlite3
    con.execute("""CREATE TABLE IF NOT EXISTS kibor_daily (
        date TEXT, tenor TEXT, bid REAL, offer REAL, scraped_at TEXT,
        PRIMARY KEY (date, tenor)
    )""")

    all_series = read_dataset_series("TS_GP_BAM_SIRKIBOR_D")
    inserted = 0
    for sk, rows in all_series.items():
        suffix = sk.split(".")[-1] if "." in sk else sk.rsplit("_", 1)[-1]
        if suffix not in _KIBOR_MAP:
            continue
        tenor, side = _KIBOR_MAP[suffix]
        col = side  # 'bid' or 'offer'

        for r in rows:
            if since and r["date"] < since:
                continue
            try:
                con.execute(f"""
                    INSERT INTO kibor_daily (date, tenor, {col}, scraped_at)
                    VALUES (?, ?, ?, 'easydata')
                    ON CONFLICT(date, tenor) DO UPDATE SET {col}=excluded.{col}, scraped_at='easydata'
                """, (r["date"], tenor, r["value"]))
                inserted += 1
            except sqlite3.IntegrityError:
                pass
    con.commit()
    return {"kibor_rows": inserted}


def sync_fx_to_db(con, since: str = "") -> dict:
    """Load FX avg rates from EasyData CSVs into sbp_fx_interbank table."""
    import sqlite3
    con.execute("""CREATE TABLE IF NOT EXISTS sbp_fx_interbank (
        date TEXT, currency TEXT, buying REAL, selling REAL, mid REAL, scraped_at TEXT,
        PRIMARY KEY (date, currency)
    )""")

    all_series = read_dataset_series("TS_GP_ER_FAERPKR_M")
    inserted = 0
    for sk, rows in all_series.items():
        suffix = sk.split(".")[-1] if "." in sk else sk.rsplit("_", 1)[-1]
        currency = _FX_MAP.get(suffix)
        if not currency:
            continue

        for r in rows:
            if since and r["date"] < since:
                continue
            mid = r["value"]
            try:
                con.execute("""
                    INSERT INTO sbp_fx_interbank (date, currency, buying, selling, mid, scraped_at)
                    VALUES (?, ?, ?, ?, ?, 'easydata')
                    ON CONFLICT(date, currency) DO UPDATE SET
                        mid=excluded.mid, scraped_at='easydata'
                """, (r["date"], currency, mid, mid, mid))
                inserted += 1
            except sqlite3.IntegrityError:
                pass
    con.commit()
    return {"fx_rows": inserted}


def sync_policy_rate_to_db(con, since: str = "") -> dict:
    """Load SBP policy rates from EasyData CSVs into sbp_policy_rates table."""
    import sqlite3
    con.execute("""CREATE TABLE IF NOT EXISTS sbp_policy_rates (
        rate_date TEXT PRIMARY KEY, policy_rate REAL, ceiling_rate REAL,
        floor_rate REAL, overnight_repo_rate REAL, source TEXT, ingested_at TEXT
    )""")

    # SBPOL0010 = Reverse Repo, SBPOL0020 = Repo (ceiling), SBPOL0030 = Policy Rate
    series_map = {
        "TS_GP_IR_SIRPR_AH.SBPOL0010": "floor_rate",      # Reverse Repo
        "TS_GP_IR_SIRPR_AH.SBPOL0020": "ceiling_rate",     # Repo
        "TS_GP_IR_SIRPR_AH.SBPOL0030": "policy_rate",      # Policy Rate
    }

    inserted = 0
    for sk, col in series_map.items():
        rows = read_series(sk)
        for r in rows:
            if since and r["date"] < since:
                continue
            try:
                con.execute(f"""
                    INSERT INTO sbp_policy_rates (rate_date, {col}, source, ingested_at)
                    VALUES (?, ?, 'easydata', datetime('now'))
                    ON CONFLICT(rate_date) DO UPDATE SET {col}=excluded.{col}, source='easydata'
                """, (r["date"], r["value"]))
                inserted += 1
            except sqlite3.IntegrityError:
                pass
    con.commit()
    return {"policy_rows": inserted}


def sync_all_to_db(con) -> dict:
    """Sync all available EasyData series to local DB tables."""
    results = {}
    results.update(sync_kibor_to_db(con))
    results.update(sync_fx_to_db(con))
    results.update(sync_policy_rate_to_db(con))
    return results


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    parser = argparse.ArgumentParser(description="SBP EasyData API Scraper")
    parser.add_argument("command",
                        choices=["discover", "fetch-priority", "fetch-all",
                                 "fetch-series", "update", "status", "sync-db"])
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
    elif args.command == "sync-db":
        import sqlite3
        db_path = args.args[0] if args.args else "/mnt/e/psxdata/psx.sqlite"
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        r = sync_all_to_db(con)
        print(json.dumps(r, indent=2))
        con.close()
