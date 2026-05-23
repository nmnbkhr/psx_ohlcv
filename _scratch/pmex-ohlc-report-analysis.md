# PMEX OHLC Report — Official Free Commodity OHLCV + Settlement + FX Rate

## 🏆 THE SINGLE BEST FREE SOURCE FOR PAKISTAN COMMODITY DATA

**URL**: `https://mportal.pmex.com.pk/mt5bonew/Home/OHLCReport`
**Access**: PUBLIC — No login, no API key, no registration
**Cost**: FREE
**Data**: Official PMEX exchange OHLCV + Settlement Price + FX Rate

---

## Page Structure

### Form Inputs
- **From Date** (required) — Start date
- **To Date** (required) — End date
- **Max range**: 3 months per request
- **"Show" button** — Loads data into HTML table
- **"Download Report" button** — Exports to file (likely Excel/CSV)

### Table Schema

| Column | Description | Value |
|--------|-------------|-------|
| **Trading Date** | Settlement date | e.g., 2025-02-28 |
| **Symbol** | PMEX contract symbol | e.g., GOLD-1OZ-MAR26 |
| **Open** | Opening price (PKR) | Official exchange open |
| **High** | Day's high (PKR) | True intraday high |
| **Low** | Day's low (PKR) | True intraday low |
| **Close** | Closing price (PKR) | Last trade or closing session |
| **Traded Volume** | Contracts traded | Real exchange volume |
| **Settlement Price** | Official daily settlement (PKR) | VWAP/consensus price |
| **FX Rate** | USD/PKR rate used for settlement | SBP notified rate |

### Why This Is The Best Source

| Feature | PMEX OHLC Report | yfinance | khistocks | FRED |
|---------|------------------|----------|-----------|------|
| Official exchange data | ✅ | ❌ (derived) | ✅ (via BR) | ❌ (World Bank) |
| OHLCV complete | ✅ | ✅ | ❌ (Open/Close only) | ❌ (Close only) |
| Settlement Price | ✅ | ❌ | ❌ | ❌ |
| FX Rate included | ✅ | ❌ | ❌ | ❌ |
| PKR native | ✅ | ❌ (USD) | ✅ | ❌ (USD) |
| All PMEX products | ✅ (~100+) | ~30 tickers | ~10 products | ~20 series |
| True volume | ✅ | ❌ (estimated) | ❌ | ❌ |
| Download button | ✅ | via API | ❌ (scrape) | via API |
| No registration | ✅ | ✅ | ✅ | ❌ (free key) |
| Daily frequency | ✅ | ✅ | ✅ | ❌ (monthly) |

---

## Products Available (All PMEX-Listed)

Based on PMEX market watch categories:

### Metals (~20+ contracts)
- Gold: 1oz, 100oz, 1 Tola, 50 Tola, 100 Tola, Kilo, 100g, MiniGold 10g, Micro Oz, Milli Oz, JPY Gold
- Silver: 10oz, 100oz
- Platinum, Palladium, Copper

### Energy (~5+ contracts)
- Crude Oil WTI (100bbl, 10bbl)
- Brent Crude (10bbl)
- Natural Gas (various lots)

### Agriculture — International (~10+ contracts)
- Cotton #2 (ICE-referenced)
- Corn, Soybean, Coffee, Cocoa, Sugar, Wheat, Rice, Palm Oil

### Agriculture — Local (Physical Delivery)
- Local Wheat, Rice, Sugar (PKR, physical delivery)

### Physical Gold (Deliverable)
- 1 Tola, 10g gold bars — actual physical delivery from PMEX vault

### Financials
- KIBOR futures
- USD/PKR futures, EUR/PKR

### EWR (Electronic Warehouse Receipts)
- Agriculture warehouse-based contracts

### Indices
- PMEX commodity indices

---

## Implementation for pakfindata

### Approach 1: requests + form POST (Preferred — Lightweight)

The page is ASP.NET MVC. The form likely POSTs to the same URL or an API action.

```python
"""
pakfindata/commodities/fetcher_pmex_ohlc.py
Official PMEX OHLC Report fetcher — FREE, no login, no API key
"""

import requests
import pandas as pd
from io import StringIO, BytesIO
from datetime import date, timedelta
from typing import Optional

OHLC_URL = "https://mportal.pmex.com.pk/mt5bonew/Home/OHLCReport"

# Common ASP.NET MVC action patterns to try:
POSSIBLE_POST_URLS = [
    "https://mportal.pmex.com.pk/mt5bonew/Home/OHLCReport",          # Same URL POST
    "https://mportal.pmex.com.pk/mt5bonew/Home/GetOHLCData",         # AJAX data load
    "https://mportal.pmex.com.pk/mt5bonew/Home/ExportOHLCReport",    # Download action
    "https://mportal.pmex.com.pk/mt5bonew/Home/DownloadOHLCReport",  # Download action alt
    "https://mportal.pmex.com.pk/mt5bonew/Home/ExportToExcel",       # Excel export
]

# Date format patterns to try (ASP.NET typically uses MM/dd/yyyy or yyyy-MM-dd)
DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]

def fetch_ohlc_report(
    from_date: date,
    to_date: date,
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """
    Fetch PMEX OHLC report for a date range (max 3 months).
    
    Returns DataFrame with columns:
    trading_date, symbol, open, high, low, close, traded_volume, settlement_price, fx_rate
    """
    s = session or requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": OHLC_URL,
    })
    
    # First GET the page to capture any anti-forgery tokens
    page = s.get(OHLC_URL)
    
    # Try to extract __RequestVerificationToken if present (ASP.NET anti-forgery)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page.text, "html.parser")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    
    # Build form data — try different date format patterns
    for date_fmt in DATE_FORMATS:
        form_data = {
            "FromDate": from_date.strftime(date_fmt),
            "ToDate": to_date.strftime(date_fmt),
        }
        if token_input:
            form_data["__RequestVerificationToken"] = token_input["value"]
        
        # Try POST to same URL first
        resp = s.post(OHLC_URL, data=form_data)
        if resp.status_code == 200 and "Trading Date" in resp.text:
            return _parse_html_table(resp.text)
    
    return pd.DataFrame()


def download_ohlc_report(
    from_date: date,
    to_date: date,
    output_path: str = None,
    session: Optional[requests.Session] = None
) -> pd.DataFrame:
    """
    Download PMEX OHLC report file (Excel/CSV).
    Tries common ASP.NET MVC download action patterns.
    """
    s = session or requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": OHLC_URL,
    })
    
    # GET page first for session/tokens
    s.get(OHLC_URL)
    
    for date_fmt in DATE_FORMATS:
        form_data = {
            "FromDate": from_date.strftime(date_fmt),
            "ToDate": to_date.strftime(date_fmt),
        }
        
        for url in POSSIBLE_POST_URLS:
            try:
                resp = s.post(url, data=form_data)
                content_type = resp.headers.get("Content-Type", "")
                
                # Check if response is a file download
                if any(ct in content_type for ct in [
                    "application/vnd.openxmlformats",  # xlsx
                    "application/vnd.ms-excel",         # xls
                    "text/csv",                         # csv
                    "application/octet-stream",         # generic binary
                ]):
                    # It's a file download!
                    if "csv" in content_type:
                        df = pd.read_csv(BytesIO(resp.content))
                    else:
                        df = pd.read_excel(BytesIO(resp.content))
                    
                    if output_path:
                        with open(output_path, "wb") as f:
                            f.write(resp.content)
                    
                    return df
            except Exception:
                continue
    
    return pd.DataFrame()


def _parse_html_table(html: str) -> pd.DataFrame:
    """Parse OHLC data from HTML table response."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    
    # Find the data table
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.text.strip() for th in table.find_all("th")]
        if "Trading Date" in headers and "Settlement Price" in headers:
            rows = []
            for tr in table.find_all("tr")[1:]:
                cells = [td.text.strip() for td in tr.find_all("td")]
                if cells and len(cells) >= 9:
                    rows.append(cells)
            
            df = pd.DataFrame(rows, columns=[
                "trading_date", "symbol", "open", "high", "low", 
                "close", "traded_volume", "settlement_price", "fx_rate"
            ])
            
            # Convert types
            for col in ["open", "high", "low", "close", "settlement_price", "fx_rate"]:
                df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")
            df["traded_volume"] = pd.to_numeric(
                df["traded_volume"].str.replace(",", ""), errors="coerce"
            )
            df["trading_date"] = pd.to_datetime(df["trading_date"])
            
            return df
    
    return pd.DataFrame()


def fetch_full_history(
    start_date: date = date(2020, 1, 1),
    end_date: date = None,
) -> pd.DataFrame:
    """
    Fetch PMEX OHLC data from start_date to end_date.
    Automatically chunks into 3-month windows.
    """
    if end_date is None:
        end_date = date.today()
    
    session = requests.Session()
    all_data = []
    current_start = start_date
    
    while current_start < end_date:
        # 3-month chunk (90 days to be safe)
        current_end = min(current_start + timedelta(days=89), end_date)
        
        print(f"Fetching {current_start} to {current_end}...")
        df = fetch_ohlc_report(current_start, current_end, session=session)
        
        if not df.empty:
            all_data.append(df)
            print(f"  → Got {len(df)} rows")
        else:
            print(f"  → No data")
        
        current_start = current_end + timedelta(days=1)
        
        # Be polite
        import time
        time.sleep(2)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()
```

### Approach 2: Selenium (Fallback if requests doesn't work)

```python
def selenium_fetch_ohlc(from_date: date, to_date: date) -> pd.DataFrame:
    """Selenium fallback for PMEX OHLC Report."""
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(OHLC_URL)
        
        # Fill in date fields
        from_input = driver.find_element(By.NAME, "FromDate")  # or By.ID
        to_input = driver.find_element(By.NAME, "ToDate")
        
        from_input.clear()
        from_input.send_keys(from_date.strftime("%m/%d/%Y"))  # try format
        to_input.clear()
        to_input.send_keys(to_date.strftime("%m/%d/%Y"))
        
        # Click Show button
        show_btn = driver.find_element(By.XPATH, "//button[contains(text(),'Show')]")
        show_btn.click()
        
        # Wait for table to populate
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.XPATH, "//table//td[contains(text(),'GOLD')]")
            )
        )
        
        # Parse table
        from bs4 import BeautifulSoup
        return _parse_html_table(driver.page_source)
    
    finally:
        driver.quit()
```

---

## Historical Backfill Strategy

### Chunked 3-Month Window Approach

To build a complete historical database:

```
Year 2020: 4 requests (Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec)
Year 2021: 4 requests
Year 2022: 4 requests
Year 2023: 4 requests
Year 2024: 4 requests
Year 2025: 4 requests
Year 2026 (partial): 1 request
─────────────────────
Total: ~25 requests for 6 years of daily data
```

At ~2 sec delay between requests = **~50 seconds** for complete backfill.
Expected data volume: ~100 symbols × ~250 trading days × 6 years = **~150,000 rows**.

### Incremental Daily Sync

After backfill, daily sync fetches just the last day:
```python
# Daily cron job
today = date.today()
yesterday = today - timedelta(days=1)
df = fetch_ohlc_report(yesterday, today)
# Upsert to commodity_eod table
```

---

## Database Integration

### Schema Addition for pakfindata

```sql
-- New table specifically for PMEX official settlement data
CREATE TABLE IF NOT EXISTS pmex_ohlc (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    traded_volume INTEGER,
    settlement_price REAL,      -- Official PMEX settlement
    fx_rate REAL,               -- USD/PKR rate used
    source TEXT DEFAULT 'pmex_portal',
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trading_date, symbol)
);

CREATE INDEX idx_pmex_ohlc_symbol ON pmex_ohlc(symbol);
CREATE INDEX idx_pmex_ohlc_date ON pmex_ohlc(trading_date);

-- View: PMEX prices with USD equivalent
CREATE VIEW pmex_ohlc_with_usd AS
SELECT 
    trading_date,
    symbol,
    open,
    high,
    low,
    close,
    settlement_price,
    fx_rate,
    CASE WHEN fx_rate > 0 THEN settlement_price / fx_rate ELSE NULL END AS settlement_usd,
    traded_volume
FROM pmex_ohlc;
```

### CLI Commands

```bash
# Backfill all PMEX OHLC history
pakfin pmex backfill --from 2020-01-01

# Daily sync
pakfin pmex sync

# Show specific commodity
pakfin pmex show --symbol "GOLD-1OZ" --days 30

# List all available symbols
pakfin pmex symbols

# Export to CSV
pakfin pmex export --from 2025-01-01 --to 2025-12-31 --output pmex_2025.csv
```

---

## Updated Source Priority (FINAL)

With this discovery, the source hierarchy is now:

```
TIER 0: PMEX OHLC Report     — Official OHLCV + Settlement + FX Rate (FREE)  ★★★
  └── https://mportal.pmex.com.pk/mt5bonew/Home/OHLCReport
  └── All PMEX commodities, daily, PKR native
  └── 3-month chunks, ~25 requests for full backfill
  └── NO login, NO API key, NO registration

TIER 1: yfinance             — 30 international daily OHLCV tickers (FREE)
  └── Fills non-PMEX commodities (Lumber, some ETFs)

TIER 2: FRED API              — 20+ monthly series (FREE key)
  └── Gap-fill for commodities not on PMEX or yfinance

TIER 3: World Bank Pink Sheet — 70+ monthly (FREE download)
  └── Long-term historical backfill since 1960

TIER 4: khistocks.com         — 7 daily PKR feeds (FREE, Selenium)
  4a. Karachi Bullion (Sarafa Bazaar gold PKR/Tola)
  4b. Lahore Akbari Mandi (30+ food staples — UNIQUE)
  4c. LME Metals (Cash + 3-month — daily)
  4d. Karachi Cotton, International Bullion, FX rates

TIER 5: GoldPriceZ API        — PKR/Tola gold (FREE key)
TIER 6: SBP direct             — WAR exchange rates (FREE)
TIER 7: PMEX MT5 Demo          — Intraday/tick data if needed (FREE)
TIER 8: PMEX mwatchnew         — Real-time snapshot (FREE, Selenium)
```

### What PMEX OHLC Report Does NOT Cover
(Still need other sources for these):
- Lahore Akbari Mandi wholesale food prices → khistocks.com
- Sarafa Bazaar physical gold PKR/Tola → khistocks.com 
- LME base metals daily (non-PMEX) → khistocks.com
- Commodities not listed on PMEX (Lumber, Coal, Iron Ore) → yfinance/FRED
- Monthly macro commodity indices → World Bank
- Historical data before PMEX inception (2007) → yfinance/FRED/World Bank

---

## Critical DevTools Requirement

**Before coding the fetcher, open this page in Chrome DevTools to discover:**

1. **Network tab → XHR filter → Click "Show" button**
   - Find the actual POST endpoint URL
   - Find the form parameter names and date format
   - Find the Content-Type of the response

2. **Network tab → Click "Download Report" button**
   - Find the download endpoint URL
   - Find the file format (Excel/CSV)
   - Find any additional parameters

3. **Elements tab → Inspect the form**
   - Find input field names/IDs
   - Find any hidden fields (__RequestVerificationToken, etc.)
   - Find the "Show" and "Download" button selectors

4. **Console tab → Check for any JavaScript errors**
   - The page uses jQuery + ASP.NET MVC
   - AJAX calls might be visible in console

Document these findings in fetcher_pmex_ohlc.py config section before deployment.
