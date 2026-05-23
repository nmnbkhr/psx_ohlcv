# khistocks.com Deep Dive — Pakistan Commodity Data Goldmine

## Overview

**khistocks.com** is Business Recorder's (brecorder.com) free market data portal. It is Pakistan's most comprehensive single-source for domestic commodity prices in PKR.

**Key Finding**: 7 commodity data feeds covering PMEX futures, Sarafa Bazaar bullion, international bullion OHLC, KCA cotton, Lahore wholesale grain/food, and LME base metals — all FREE, all in one place.

**Technical**: All data is loaded via **AJAX/JavaScript** — tables in static HTML are empty. Scraping requires **Selenium** (or intercepting XHR endpoints via browser DevTools).

---

## 7 Commodity Data Feeds

### 1. PMEX Live Prices
**URL**: `https://www.khistocks.com/commodity/pmex-live.html`
**Data**: Real-time streaming PMEX commodity futures
**Update**: Live during PMEX trading hours (10:00–23:30 PKT)
**Products**: Gold, Silver, Crude Oil, Brent, Natural Gas, Cotton, Wheat, Palm Oil, financials
**Currency**: PKR
**Schema**: Likely bid/ask/last/volume/change (standard live ticker format)
**Use case**: Real-time PMEX monitoring (replaces dportal.pmex.com.pk scraping)

### 2. PMEX Commodity Prices (Historical/Archives)
**URL**: `https://www.khistocks.com/commodity/pmex-commodity-prices.html`
**Source**: Pakistan Mercantile Exchange (PMEX)

| Column | Description |
|--------|-------------|
| Dt | Row index |
| Name | Commodity name (e.g., "Gold 1oz", "Crude Oil 100bbl") |
| Date | Trade date |
| Quotation | PKR quote unit |
| Open | Opening price (PKR) |
| Close | Closing price (PKR) |

**Note**: "Price quotes reflect trades from 05:00 am previous trading day to 02:00 am current trading day"
**Dropdown**: "All Commodities" selector — can filter by individual commodity
**Chart**: Built-in charting tab with chart + table toggle
**Currency**: PKR
**Use case**: Historical PMEX settlement prices — **DAILY PKR OHLC for gold, oil, cotton etc.**

### 3. Karachi Bullion Rates (Sarafa Bazaar)
**URL**: `https://www.khistocks.com/commodity/karachi-bullion-rates.html`
**Source**: Karachi Sarafa Bazaar Association

| Column | Description |
|--------|-------------|
| dt | Row index |
| Instrument | Gold 24K, Gold 22K, Silver, etc. |
| Date | Rate date |
| Rate | PKR per tola (or PKR per 10 grams for silver) |

**Currency**: PKR per Tola (Pakistan native unit)
**Products**: Gold (24K, 22K, 21K, 18K), Silver
**Chart**: Built-in charting
**Use case**: **Daily PKR/Tola gold rates** — the actual Sarafa Bazaar benchmark. This is the physical gold price that Pakistani jewellers and consumers see.

### 4. International Bullion Rates
**URL**: `https://www.khistocks.com/commodity/gold-rates.html`
**Source**: International bullion markets

| Column | Description |
|--------|-------------|
| dt | Row index |
| Instrument | Gold, Silver, Platinum, Palladium |
| Date | Rate date |
| Opening | USD opening price |
| Closing | USD closing price |
| High | USD high |
| Low | USD low |
| Netchange | USD net change |
| Change (%) | Percentage change |

**Currency**: USD (international)
**Products**: Gold, Silver, Platinum, Palladium
**Use case**: **Full OHLC for international precious metals** — cross-reference with yfinance GC=F/SI=F. Can validate yfinance data quality.

### 5. Karachi Cotton Rates
**URL**: `https://www.khistocks.com/commodity/karachi-cotton-rates.html`
**Source**: Karachi cotton market (likely KCA or equivalent)

**Schema**: [Dt, Name, Date, Rate] (inferred from similar pages)
**Currency**: PKR per maund (37.32 kg)
**Products**: Cotton varieties (Base Grade, various staple lengths)
**Use case**: **KCA-equivalent cotton spot rates in PKR/maund** — critical because KCA building is sealed since Dec 2025. This may be an alternative source that aggregates cotton market data.

### 6. Lahore Akbari Mandi (Wholesale Grain/Food Market)
**URL**: `https://www.khistocks.com/commodity/lahore-akbari-mandi.html`
**Source**: Lahore Akbari Mandi wholesale market

| Column | Description |
|--------|-------------|
| Dt | Row index |
| Name | Commodity name (wheat, rice, sugar, ghee, tea, etc.) |
| Date | Rate date |
| High | PKR high price |
| Low | PKR low price |

**Currency**: PKR per 100kg (except Tea=1kg, Ghee=16kg)
**Dropdown**: "All Commodities" selector for filtering
**Products**: Wheat (various grades), Rice (IRRI-6, Basmati, Broken), Sugar (refined, raw), Ghee/Cooking Oil, Lentils (Daal Masoor, Chana, Moong), Tea, Maize/Corn, Mustard, Onions, Potatoes, and more
**Use case**: **MASSIVE for Pakistan context** — this is the actual wholesale food commodity market. Daily high/low for 30+ domestic food staples in PKR. No other free source has this data.

### 7. London Metal Exchange (LME)
**URL**: `https://www.khistocks.com/commodity/london-metal-exchange.html`
**Source**: London Metal Exchange

| Column | Description |
|--------|-------------|
| dt | Row index (metal name) |
| Date | Rate date |
| Cash Buyer | Cash settlement buyer price (USD/tonne) |
| Cash Seller | Cash settlement seller price (USD/tonne) |
| 3 Month Buyer | 3-month futures buyer price (USD/tonne) |
| 3 Month Seller | 3-month futures seller price (USD/tonne) |

**Currency**: USD per metric tonne
**Products**: Copper, Aluminum, Nickel, Zinc, Lead, Tin (the 6 core LME metals)
**Chart options**: Cash Buyer, Cash Seller, 3-month Buyer, 3-month Seller
**Use case**: **FREE daily LME reference prices with bid/ask spread** — fills the Zinc, Nickel, Lead, Tin gap that yfinance can't cover with daily data.

---

## Bonus: Non-Commodity Data Also Available

The site also has these free data feeds (not commodity but relevant for pakfindata):

| Page | URL | Data |
|------|-----|------|
| Interbank USD Rate | /currency/interbank-rates.html | Daily interbank USD/PKR |
| Open Market Rates | /currency/kerb-rates.html | Kerb/open market FX rates |
| SBP Exchange Rates | /currency/exchange-rates.html | Official SBP rates for 20+ currencies |
| NBP TT Rates | /currency/nbp-rates.html | National Bank telegraphic transfer rates |
| KIBOR Rates | /market-data/kibor-rates.html | Karachi Interbank Offered Rate |
| PKRV Rates | /market-data/pkrv-rates.html | Pakistan Revaluation Rates (bond) |
| TFC Rates | /market-data/tfc-rates.html | Term Finance Certificate rates |

---

## Technical Implementation for pakfindata

### Scraping Strategy

All pages use the same template: static HTML with empty `<table>` elements, populated via JavaScript AJAX calls. Two approaches:

**Approach A: Selenium (Guaranteed)**
```python
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd

def scrape_khistocks_table(url: str, table_id: str = None) -> pd.DataFrame:
    """Scrape any khistocks.com commodity page."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    
    # Wait for AJAX data to load
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr td"))
    )
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    # Parse the data table (skip navigation tables)
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.text.strip() for th in table.find_all("th")]
        if "Date" in headers and ("Rate" in headers or "Close" in headers or "High" in headers):
            rows = []
            for tr in table.find_all("tr")[1:]:  # skip header
                cells = [td.text.strip() for td in tr.find_all("td")]
                if cells:
                    rows.append(cells)
            return pd.DataFrame(rows, columns=headers)
    
    return pd.DataFrame()
```

**Approach B: Intercept XHR Endpoints (Better, if discovered)**
Use browser DevTools (Network tab) on each page to find the JSON API endpoints that populate the tables. These would be something like:
```
https://www.khistocks.com/api/commodity/pmex-prices?commodity=all
https://www.khistocks.com/api/commodity/bullion-rates
https://www.khistocks.com/api/commodity/akbari-mandi?commodity=all
https://www.khistocks.com/api/commodity/lme-rates
```

**RECOMMENDED**: Open each page in Chrome DevTools → Network tab → XHR filter → reload the page → copy the endpoint URLs. This gives you direct JSON access without Selenium overhead.

### New Fetcher File

```
src/pakfindata/commodities/fetcher_khistocks.py
```

```python
"""
pakfindata/commodities/fetcher_khistocks.py
Scraper for khistocks.com (Business Recorder) — 7 Pakistan commodity feeds
FREE, no API key, daily data in PKR
"""

PAGES = {
    "pmex_live": {
        "url": "https://www.khistocks.com/commodity/pmex-live.html",
        "desc": "PMEX real-time futures",
        "currency": "PKR",
        "frequency": "live",
    },
    "pmex_historical": {
        "url": "https://www.khistocks.com/commodity/pmex-commodity-prices.html",
        "desc": "PMEX daily settlement (Open/Close)",
        "currency": "PKR",
        "frequency": "daily",
        "columns": ["Name", "Date", "Quotation", "Open", "Close"],
    },
    "karachi_bullion": {
        "url": "https://www.khistocks.com/commodity/karachi-bullion-rates.html",
        "desc": "Sarafa Bazaar gold/silver PKR/Tola",
        "currency": "PKR/tola",
        "frequency": "daily",
        "columns": ["Instrument", "Date", "Rate"],
    },
    "intl_bullion": {
        "url": "https://www.khistocks.com/commodity/gold-rates.html",
        "desc": "International gold/silver OHLC",
        "currency": "USD",
        "frequency": "daily",
        "columns": ["Instrument", "Date", "Opening", "Closing", "High", "Low", "Netchange", "Change (%)"],
    },
    "karachi_cotton": {
        "url": "https://www.khistocks.com/commodity/karachi-cotton-rates.html",
        "desc": "Karachi cotton spot rates PKR/maund",
        "currency": "PKR/maund",
        "frequency": "daily",
    },
    "lahore_mandi": {
        "url": "https://www.khistocks.com/commodity/lahore-akbari-mandi.html",
        "desc": "Wholesale grain/food prices (30+ items)",
        "currency": "PKR/100kg",
        "frequency": "daily",
        "columns": ["Name", "Date", "High", "Low"],
        "notes": "PKR per 100kg except Tea (1kg) and Ghee (16kg)",
    },
    "lme": {
        "url": "https://www.khistocks.com/commodity/london-metal-exchange.html",
        "desc": "LME base metals (Cu, Al, Ni, Zn, Pb, Sn)",
        "currency": "USD/tonne",
        "frequency": "daily",
        "columns": ["Date", "Cash Buyer", "Cash Seller", "3Month Buyer", "3Month Seller"],
    },
}

# Currency pages (bonus — also free from khistocks)
CURRENCY_PAGES = {
    "interbank_usd": "https://www.khistocks.com/currency/interbank-rates.html",
    "open_market": "https://www.khistocks.com/currency/kerb-rates.html",
    "sbp_rates": "https://www.khistocks.com/currency/exchange-rates.html",
    "nbp_tt": "https://www.khistocks.com/currency/nbp-rates.html",
}

FIXED_INCOME_PAGES = {
    "kibor": "https://www.khistocks.com/market-data/kibor-rates.html",
    "pkrv": "https://www.khistocks.com/market-data/pkrv-rates.html",
    "tfc": "https://www.khistocks.com/market-data/tfc-rates.html",
}
```

---

## What khistocks.com Fills (Gap Analysis)

| Gap in yfinance | khistocks.com Source | Data Quality |
|------------------|---------------------|--------------|
| Gold PKR/Tola (Sarafa) | Karachi Bullion Rates | ✅ Daily, official Sarafa |
| Silver PKR/Tola | Karachi Bullion Rates | ✅ Daily |
| Cotton PKR/Maund | Karachi Cotton Rates | ✅ Daily (if KCA resumes via BR) |
| Wheat PKR/100kg | Lahore Akbari Mandi | ✅ Daily wholesale |
| Rice (IRRI-6, Basmati) PKR | Lahore Akbari Mandi | ✅ Daily wholesale |
| Sugar PKR/100kg | Lahore Akbari Mandi | ✅ Daily wholesale |
| Ghee/Cooking Oil PKR | Lahore Akbari Mandi | ✅ Daily wholesale |
| Lentils/Daal PKR | Lahore Akbari Mandi | ✅ Daily wholesale |
| Zinc USD/tonne (daily) | LME page | ✅ Cash + 3-month |
| Nickel USD/tonne (daily) | LME page | ✅ Cash + 3-month |
| Lead USD/tonne (daily) | LME page | ✅ Cash + 3-month |
| Tin USD/tonne (daily) | LME page | ✅ Cash + 3-month |
| PMEX Gold PKR | PMEX Historical | ✅ Daily Open/Close |
| PMEX Crude Oil PKR | PMEX Historical | ✅ Daily Open/Close |
| PMEX Cotton PKR | PMEX Historical | ✅ Daily Open/Close |
| PMEX Natural Gas PKR | PMEX Historical | ✅ Daily Open/Close |
| Intl Gold/Silver OHLC | International Bullion | ✅ Daily OHLC + change |
| USD/PKR Interbank | Currency pages | ✅ Daily official |
| SBP Exchange Rates | Currency pages | ✅ Multi-currency |
| KIBOR Rates | Fixed income pages | ✅ Daily benchmark |

---

## Impact on Claude Code Prompt

khistocks.com should be added as **TIER 4a** (highest-priority scraper) in the zero-cost architecture because:

1. **It's a ONE STOP SHOP** — replaces 5 separate scraping targets (PMEX dportal, KCA, Sarafa Bazaar, Akbari Mandi, LME) with a single site
2. **Consistent template** — all 7 pages use the same HTML structure, so one Selenium function handles all
3. **Daily PKR data** — the Lahore Akbari Mandi page alone provides 30+ domestic food commodities in PKR that NO API has
4. **LME with bid/ask** — daily LME Cash + 3-month prices for all 6 base metals, filling yfinance gaps
5. **Business Recorder is authoritative** — Pakistan's premier financial newspaper since 1965
6. **Completely free** — no registration, no API key, no rate limit (reasonable scraping)
7. **Currency + fixed income bonus** — interbank rates, SBP rates, KIBOR from same source

### Updated Source Hierarchy for Claude Code Prompt

```
TIER 1: yfinance          — 30 daily OHLCV tickers (FREE, no key)
TIER 2: FRED API           — 20+ monthly series (FREE key, no credit card)
TIER 3: World Bank         — 70+ monthly (FREE, CC BY 4.0, direct download)
TIER 4: khistocks.com      — 7 PKR daily feeds via Selenium (FREE, no key)  ← NEW
  4a. PMEX Historical      — Gold, Oil, Cotton, Gas settlement (PKR Open/Close)
  4b. Karachi Bullion      — Gold PKR/Tola, Silver (Sarafa Bazaar)
  4c. International Bullion— Gold, Silver, Platinum, Palladium OHLC (USD)
  4d. Karachi Cotton       — Cotton PKR/Maund
  4e. Lahore Akbari Mandi  — 30+ food staples PKR/100kg (UNIQUE DATA)
  4f. LME Metals           — Cu, Al, Ni, Zn, Pb, Sn (Cash + 3-month)
  4g. Currency              — Interbank, Open Market, SBP, NBP rates
TIER 5: GoldPriceZ API    — PKR/Tola gold (FREE key)
TIER 6: SBP direct        — WAR exchange rates (FREE, BS4 scrape)
```

### Action Items for Claude Code

1. **First priority**: Open each khistocks.com commodity page in Chrome DevTools → Network tab → reload → find the XHR/Fetch API endpoint URLs that return JSON data. This is the fastest path.
2. **Fallback**: Use Selenium with headless Chrome to render the JS and parse the populated tables with BS4.
3. Add `fetcher_khistocks.py` to the commodity module implementation plan.
4. The Akbari Mandi data is **unique** — no other source (free or paid) provides daily wholesale prices for Pakistani food staples.
