# PMEX Data Portal Deep Dive — pmex.com.pk/downloads + dportal.pmex.com.pk/mwatchnew

## Executive Summary

PMEX provides **3 data access channels** for pakfindata, ranging from real-time live tickers to historical archives. The combination fills most Pakistan commodity gaps with **official exchange data in PKR**.

---

## 1. dportal.pmex.com.pk/mwatchnew (New Market Watch)

### What It Is
Redesigned real-time market watch page (replacing the old `/mwatch`). Built by techstar.io using modern web stack. Shows live bid/ask/OHLCV for all active PMEX contracts.

### Data Schema

| Column | Description |
|--------|-------------|
| Symbol | PMEX contract symbol (e.g., GOLD-1OZ-MAR26, CRUDEOIL-100BBL-APR26) |
| Bid | Current bid price (PKR) |
| Ask | Current ask price (PKR) |
| Open | Day's opening price (PKR) |
| High | Day's high (PKR) |
| Low | Day's low (PKR) |
| Close | Previous close / settlement (PKR) |
| Total Volume | Total contracts traded |
| Change | Net change from close (PKR) |
| Change% | Percentage change |
| Last Price | Last traded price (PKR) |
| Last Volume | Volume of last trade |

### Product Categories (10 categories)

| Category | Likely Products |
|----------|----------------|
| **Indices** | PMEX Commodity Index, KSE-related contracts |
| **Energy** | Natural Gas (various lot sizes) |
| **Metals** | Gold (1oz, 100oz, 1Tola, 50Tola, 100Tola, Kilo, 100g, MiniGold 10g, Milli Oz, JPY Gold), Silver (10oz, 100oz), Platinum, Palladium, Copper |
| **Oil** | Crude Oil WTI (100bbl, 10bbl), Brent Crude |
| **COTS** | Cotton #2 (ICE-referenced, various lot sizes) |
| **International Agriculture** | Corn, Soybean, Rice, Coffee, Cocoa, Sugar, Wheat, Palm Oil |
| **Physical Gold** | Deliverable gold contracts (1 Tola, 10g, etc.) — settled via vault delivery |
| **Local Agriculture** | Local wheat, rice, sugar (PKR, physical delivery, Exclusive of Taxes) |
| **Financials** | KIBOR futures, USD/PKR futures, EUR/PKR |
| **EWR** | Electronic Warehouse Receipts — agricultural warehouse-based contracts |

### Filter Options
- "Active Contracts" vs "All Contracts" toggle
- Active shows only contracts with open interest; All shows every listed contract including expired

### Technical: How Data Loads
- **Real-time streaming** — likely ASP.NET SignalR hub (common for .NET exchanges)
- Tables are empty in static HTML, populated via WebSocket/SSE push
- Trading hours: ~22.5 hours/day (05:00 AM to 03:30 AM next day PKT, Mon-Fri)
- Developed by techstar.io on .NET stack

### Scraping Strategy
```python
# Option A: Selenium (captures rendered snapshot)
# Selenium can capture current state of the table after JS renders

# Option B: SignalR Client (Python) — BETTER for continuous monitoring
# pip install signalrcore
from signalrcore.hub_connection_builder import HubConnectionBuilder

# Need to discover hub URL via browser DevTools:
# Open dportal.pmex.com.pk/mwatchnew → DevTools → Network → WS filter
# Look for: /signalr/negotiate or /mwatchhub or similar
# Then connect:
hub_url = "https://dportal.pmex.com.pk/<hub_path>"  # TBD via DevTools
hub = HubConnectionBuilder()\
    .with_url(hub_url)\
    .build()

hub.on("ReceiveMarketData", lambda data: process(data))
hub.start()
```

### Value for pakfindata
- **Real-time OHLCV for ~100+ PMEX contracts** in PKR
- **True bid/ask spread** data (not available from any other free source)
- **All product categories** in one feed — metals, energy, agri, financials, physical gold
- Can capture **intraday snapshots** by polling periodically

---

## 2. pmex.com.pk/downloads/

### What It Is
The main PMEX website's downloads section. **Returns 403 to automated fetchers** (WAF/bot protection on WordPress site). Based on PMEX site navigation, this page likely contains:

### Probable Content (from nav structure analysis)
The PMEX website nav shows these data sections:

**Downloads Menu Items:**
1. **MT5 Desktop Client** — MetaTrader 5 installer (pmex5setup.exe)
   - URL: `https://download.mql5.com/cdn/web/pakistan.mercantile.exchange/mt5/pmex5setup.exe`
   - FREE download, no account needed for demo
2. **MT5 Mobile** — iOS and Android links
3. **Contract Specifications** — PDFs for all products (already available at `/wp-content/uploads/`)
4. **Demo Trading Guide** — PDF tutorial

### Known PDF Downloads (discovered via search)
All hosted at `pmex.com.pk/wp-content/uploads/2019/09/` or `2025/`:
- PMEX-1-Ounce-Gold-Futures-Contract-Specification.pdf
- PMEX-Micro-Ounce-Gold-Futures-Contract-Specification.pdf
- PMEX-Mini-Gold-Futures-Contract-Specification.pdf
- PMEX-JPY-Gold-Futures-Contract.pdf
- PMEX-Silver-10-Ounces-Futures-Contract.pdf
- PMEX-Silver-100-Ounces-Futures-Contract-Specifications.pdf (updated 2025/08)
- PMEX-Palladium-100-Oz-Contract-Specifications.pdf
- PMEX-Corn-Futures-Contract-Specifications.pdf
- PMEX-Soybean-Futures-Contract-Specifications.pdf (updated 2025/10)
- A-Guide-to-Trading-Commodity-Futures-at-PMEX-1-1.pdf
- Demo-Trading-Guide-2.pdf
- Notification-No-54.-PMEX-launches-New-Portal.pdf

### Value for pakfindata
- Contract specs give exact **lot sizes, units, settlement methods, trading hours**
- Essential for proper PKR conversion: e.g., Gold 1oz settled via CME Comex reference price × SBP USD/PKR rate
- Can scrape all PDF specs to build automated contract metadata

---

## 3. pmex.com.pk "Historical Data" Page

### Discovery
Every PMEX page's navigation includes a **"Historical Data"** link, sitting between "Margins" and "Notifications". This is a distinct section separate from market watch.

### Probable URL
`https://pmex.com.pk/pmex-home/historical-data/` (blocked by 403 to automated fetchers)

### What It Likely Contains
Based on regulatory requirements for futures exchanges and PMEX's SECP obligations:
- **Daily settlement prices** for all active and expired contracts
- **Volume and turnover statistics** by commodity category
- **Open Interest data** historical
- Possibly downloadable as **Excel/CSV** files

### How to Access
- **Manual access**: Visit in browser, check if data is downloadable
- **If JS-rendered**: Use Selenium to render and scrape
- **Alternative**: khistocks.com PMEX Historical page already aggregates this data

---

## 4. MT5 Demo Account — FREE Historical Data Channel

### The Hidden Goldmine
PMEX uses MetaTrader 5 (MT5) as its trading platform. **MT5 Demo accounts are FREE** and provide:

1. **Historical OHLCV data** for all PMEX-listed instruments going back years
2. **Tick data** (every trade, bid/ask change)
3. **1-minute to monthly** bar data
4. **Exportable** via MT5 built-in tools or Python MT5 API

### Setup
```bash
# Step 1: Download MT5 from PMEX
wget https://download.mql5.com/cdn/web/pakistan.mercantile.exchange/mt5/pmex5setup.exe

# Step 2: Install and open
# Step 3: File → Open Account → Search "Pakistan Mercantile Exchange"
# Step 4: Select "Open a demo account"
# Step 5: Fill details → Get login credentials
```

### Python MT5 API (MetaTrader5 package)
```python
# pip install MetaTrader5
import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime

# Initialize MT5
mt5.initialize(login=YOUR_DEMO_LOGIN, server="PakistanMercantileExchange-Server",
               password="YOUR_DEMO_PASSWORD")

# Get all available symbols
symbols = mt5.symbols_get()
for s in symbols:
    print(f"{s.name}: {s.description}, Currency: {s.currency_base}")

# Download historical OHLCV for Gold 1oz
rates = mt5.copy_rates_from(
    "GOLD-1OZ",           # Symbol name (check actual symbol)
    mt5.TIMEFRAME_D1,     # Daily bars
    datetime(2020, 1, 1), # From date
    10000                 # Number of bars
)
df = pd.DataFrame(rates)
df['time'] = pd.to_datetime(df['time'], unit='s')
print(df.head())
# Columns: time, open, high, low, close, tick_volume, spread, real_volume

# Get tick data
ticks = mt5.copy_ticks_from("GOLD-1OZ", datetime(2025, 1, 1), 100000, mt5.COPY_TICKS_ALL)
# Columns: time, bid, ask, last, volume, flags

mt5.shutdown()
```

### PMEX MT5 Symbol Naming Convention
Based on contract specs, symbols likely follow patterns like:
```
GOLD-1OZ-MMMyy        # Gold 1 ounce, month/year
GOLD-100OZ-MMMyy      # Gold 100 ounce
GOLD-1TOLA-MMMyy      # Gold 1 Tola (deliverable)
SILVER-10OZ-MMMyy     # Silver 10 ounce
SILVER-100OZ-MMMyy    # Silver 100 ounce
CRUDEOIL-100BBL-MMMyy # WTI Crude 100 barrel
CRUDEOIL-10BBL-MMMyy  # WTI Crude mini 10 barrel
BRENT-10BBL-MMMyy     # Brent Crude
NATGAS-MMMyy          # Natural Gas
ICOTTON-MMMyy         # ICE Cotton
CORN-MMMyy            # Corn
SOYBEAN-MMMyy         # Soybean
COFFEE-MMMyy          # Coffee
SUGAR-MMMyy           # Sugar
PLATINUM-MMMyy        # Platinum
PALLADIUM-MMMyy       # Palladium
COPPER-MMMyy          # Copper
USDPKR-MMMyy          # USD/PKR futures
KIBOR-MMMyy           # KIBOR futures
MINIGOLD-10G-MMMyy    # Mini Gold 10g (deliverable)
WHEAT-LOCAL-MMMyy     # Local wheat (physical delivery)
RICE-LOCAL-MMMyy      # Local rice (physical delivery)
```

### Advantages of MT5 Channel
| Feature | Advantage |
|---------|-----------|
| Historical depth | Years of daily/intraday data |
| Data quality | Official exchange data, not derived |
| Granularity | 1-min to monthly, plus tick data |
| Bid/Ask | Full spread data available |
| Volume | Real exchange volume (not estimated) |
| Cost | FREE (demo account) |
| Automation | Python MetaTrader5 package for scripting |
| Platform | Windows only (runs in WSL2 via Wine or native Windows) |

### Limitation
- **Windows only**: MT5 Python API requires Windows. On WSL2, you'd need to run it from Windows side and pipe data via file/socket
- **Demo account longevity**: Demo accounts may expire after inactivity (typically 30 days). Re-create as needed
- **Data might be delayed**: Demo accounts may have delayed data vs live
- **Symbol names need discovery**: Actual PMEX symbol strings need to be queried once connected

---

## 5. Combined PMEX Data Architecture for pakfindata

### 3-Channel Strategy

```
┌─────────────────────────────────────────────────────┐
│                    PMEX DATA STACK                   │
├─────────────────────────────────────────────────────┤
│                                                      │
│  Channel 1: MT5 Demo (Python API)                   │
│  ├── Historical OHLCV (daily + intraday)            │
│  ├── All PMEX symbols (100+ contracts)              │
│  ├── Tick data with bid/ask                         │
│  ├── Backfill from 2020+                            │
│  └── Automated via MetaTrader5 Python package       │
│                                                      │
│  Channel 2: dportal.pmex.com.pk/mwatchnew           │
│  ├── Real-time streaming (SignalR)                  │
│  ├── Bid/Ask/OHLCV/Volume/Change                    │
│  ├── 10 product categories                          │
│  ├── Selenium snapshot or SignalR client             │
│  └── Fallback if MT5 unavailable                    │
│                                                      │
│  Channel 3: khistocks.com PMEX Historical           │
│  ├── Daily settlement (Open/Close)                  │
│  ├── Already scraped via khistocks fetcher           │
│  └── Backup/cross-validation source                 │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### Priority Order for Implementation
1. **MT5 Demo** — Richest data, free, scriptable. Set up first on Windows side
2. **mwatchnew Selenium** — Quick daily snapshot, no account needed
3. **khistocks.com** — Already planned, provides PMEX + other Pakistan commodity data

### New File for pakfindata

```python
# src/pakfindata/commodities/fetcher_pmex.py

"""
PMEX Data Fetcher — 3 channels:
1. MT5 API (primary — historical + real-time)
2. dportal.pmex.com.pk/mwatchnew (Selenium fallback)
3. khistocks.com PMEX page (already in fetcher_khistocks.py)
"""

import platform
from typing import Optional
import pandas as pd

# ── Channel 1: MT5 API ──────────────────────────────
def is_mt5_available() -> bool:
    """MT5 only works on Windows."""
    return platform.system() == "Windows"

def mt5_get_symbols() -> list[dict]:
    """List all PMEX symbols from MT5 demo."""
    if not is_mt5_available():
        raise RuntimeError("MT5 requires Windows. Use Selenium fallback.")
    import MetaTrader5 as mt5
    mt5.initialize()
    symbols = mt5.symbols_get()
    result = [{"name": s.name, "description": s.description,
               "currency": s.currency_base, "category": s.path}
              for s in symbols]
    mt5.shutdown()
    return result

def mt5_fetch_daily(symbol: str, from_date, num_bars: int = 5000) -> pd.DataFrame:
    """Fetch daily OHLCV from MT5 demo for a PMEX symbol."""
    if not is_mt5_available():
        raise RuntimeError("MT5 requires Windows.")
    import MetaTrader5 as mt5
    mt5.initialize()
    rates = mt5.copy_rates_from(symbol, mt5.TIMEFRAME_D1, from_date, num_bars)
    mt5.shutdown()
    if rates is None:
        return pd.DataFrame()
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.rename(columns={'time': 'date', 'tick_volume': 'volume'}, inplace=True)
    return df[['date', 'open', 'high', 'low', 'close', 'volume']]

# ── Channel 2: Market Watch Selenium ─────────────────
MWATCH_URL = "https://dportal.pmex.com.pk/mwatchnew"

def selenium_fetch_market_watch() -> pd.DataFrame:
    """Scrape current market watch snapshot via Selenium."""
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    driver.get(MWATCH_URL)

    # Wait for data to load
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr td"))
    )
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    # Parse market watch table
    table = soup.find("table")
    headers = [th.text.strip() for th in table.find_all("th")]
    rows = []
    current_category = None
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        text = [c.text.strip() for c in cells]
        # Category headers are bold rows
        if cells[0].find("strong"):
            current_category = cells[0].text.strip()
            continue
        if text[0]:  # Has symbol
            row = {"category": current_category}
            for i, h in enumerate(headers):
                if i < len(text):
                    row[h] = text[i]
            rows.append(row)

    return pd.DataFrame(rows)
```

---

## 6. PMEX Contract Specifications Summary

From the contract spec PDFs discovered, here are the key trading parameters:

| Product | Lot Size | Quote Unit | Settlement | Reference |
|---------|----------|------------|------------|-----------|
| Gold 1oz | 1 troy oz | USD/oz → PKR | Cash (CME Comex ref) | PMEX-1-Ounce-Gold |
| Gold 100oz | 100 troy oz | USD/oz → PKR | Cash (CME Comex ref) | PMEX-Gold-100oz |
| Gold 1 Tola | 1 tola (11.66g) | PKR/10g | Physical delivery | PMEX-Mini-Gold |
| Gold MiniGold 10g | 10g | PKR/10g | Physical delivery | PMEX-Mini-Gold |
| Gold Micro Oz | milli oz | USD/oz → PKR | Cash | PMEX-Micro-Ounce |
| Gold JPY | milli oz | JPY/oz → PKR | Cash | PMEX-JPY-Gold |
| Silver 10oz | 10 troy oz | USD/oz → PKR | Cash (CME ref) | PMEX-Silver-10oz |
| Silver 100oz | 100 troy oz | USD/oz → PKR | Cash (CME ref) | PMEX-Silver-100oz |
| Palladium 100oz | 100 troy oz | USD/oz → PKR | Cash | PMEX-Palladium |
| Crude Oil | 100 bbl / 10 bbl | USD/bbl → PKR | Cash (NYMEX ref) | TBD |
| Brent Crude | 10 bbl | USD/bbl → PKR | Cash (ICE ref) | TBD |
| Natural Gas | varies | USD/MMBtu → PKR | Cash (NYMEX ref) | TBD |
| Cotton #2 | varies | USc/lb → PKR | Cash (ICE ref) | TBD |
| Corn | varies | USc/bu → PKR | Cash (CBOT ref) | PMEX-Corn |
| Soybean | varies | USc/bu → PKR | Cash (CBOT ref) | PMEX-Soybean |
| KIBOR | notional | % rate | Cash | TBD |
| USD/PKR | lot | PKR/USD | Cash | TBD |

**Key insight**: All international commodities are **PKR-settled using SBP USD/PKR rate**. This means PMEX prices = International Reference Price × SBP Exchange Rate. This is perfect for pakfindata's PKR conversion layer — PMEX prices ARE the official PKR prices.

---

## 7. Action Items for Claude Code Prompt Update

### Must Add to Implementation Plan:

1. **`fetcher_pmex.py`** — New fetcher with MT5 + Selenium dual-channel
2. **MT5 Demo Setup Task** — Claude Code should include MT5 setup script for Windows
3. **Symbol Discovery Task** — First run: connect to MT5 demo → `mt5.symbols_get()` → save all PMEX symbols to `commodity_symbols` table
4. **Historical Backfill Task** — Use MT5 to pull daily OHLCV back to 2020 for all active symbols
5. **mwatchnew Selenium Task** — Daily snapshot capture as fallback
6. **Contract Spec Scraper** — Download all PDFs from `pmex.com.pk/wp-content/uploads/` and parse lot sizes, units

### Priority in Updated Source Hierarchy:

```
TIER 1: yfinance             — 30 daily OHLCV tickers (FREE, no key)
TIER 2: PMEX MT5 Demo        — 100+ contracts, full OHLCV history (FREE)  ← NEW
TIER 3: FRED API              — 20+ monthly series (FREE key)
TIER 4: World Bank Pink Sheet — 70+ monthly (FREE, direct download)
TIER 5: khistocks.com         — 7 daily PKR feeds via Selenium (FREE)
  5a. PMEX Historical          — Settlement prices (cross-validates TIER 2)
  5b. Karachi Bullion           — Sarafa Bazaar gold PKR/Tola
  5c-5g. [other feeds]
TIER 6: PMEX mwatchnew        — Real-time snapshot via Selenium (FREE)  ← NEW
TIER 7: GoldPriceZ API        — PKR/Tola gold (FREE key)
TIER 8: SBP direct             — WAR exchange rates (FREE)
```

### Browser DevTools Requirement
**CRITICAL for Claude Code**: Before implementing fetcher_pmex.py, you MUST:
1. Open `https://dportal.pmex.com.pk/mwatchnew` in Chrome
2. Open DevTools → Network tab → WS filter → Reload
3. Find the SignalR/WebSocket hub URL
4. Find the initial data load XHR endpoint
5. Document these endpoints in fetcher_pmex.py config

Without these endpoints, Selenium is the only option for mwatchnew scraping.
