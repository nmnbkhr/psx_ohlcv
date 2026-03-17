# Commodity Data Sources — Deep Dive for Pakistan Trading App

## Pakistan Relevance Context

Pakistan imports/exports and trades these commodities actively:
- **Metals**: Gold (largest consumer market, Sarafa Bazaar), Silver, Copper (industrial)
- **Energy**: Crude Oil (massive import dependency), LNG/Natural Gas, Coal
- **Agri**: Cotton (#4 global producer), Rice (#4 global exporter — Basmati), Wheat (#8 producer), Sugar, Palm Oil (largest edible oil import)

PMEX (Pakistan Mercantile Exchange) trades futures in Gold, Silver, Crude Oil, Cotton, Natural Gas, and currency pairs — all in PKR.

---

## 1. yfinance — FREE, Zero Dependencies (BEST STARTING POINT)

Already in your stack. `yf.download("GC=F")` returns identical OHLCV DataFrame as equities.

### METALS

| Commodity | Ticker | Exchange | History | Pakistan Relevance |
|-----------|--------|----------|---------|-------------------|
| Gold (front month) | `GC=F` | COMEX | 2000+ | Sarafa Bazaar benchmark, PMEX reference |
| Gold Mini | `MGC=F` | COMEX | 2010+ | Smaller contract |
| Silver | `SI=F` | COMEX | 2000+ | PMEX Silver reference |
| Silver Mini | `SIL=F` | COMEX | 2012+ | |
| Platinum | `PL=F` | NYMEX | 2000+ | |
| Palladium | `PA=F` | NYMEX | 2000+ | |
| Copper | `HG=F` | COMEX | 2000+ | Industrial, wiring, construction |
| Aluminum | `ALI=F` | LME/COMEX | Limited | Packaging, construction |
| Iron Ore | — | — | Not on Yahoo | See Commodities-API |
| Zinc | — | — | Not on Yahoo | See Commodities-API |
| Steel | — | — | Not on Yahoo | See Commodities-API |

### ENERGY

| Commodity | Ticker | Exchange | History | Pakistan Relevance |
|-----------|--------|----------|---------|-------------------|
| Crude Oil WTI | `CL=F` | NYMEX | 2000+ | Pakistan oil import pricing |
| Brent Crude | `BZ=F` | ICE | 2007+ | Closer to Pakistan import benchmark |
| Natural Gas | `NG=F` | NYMEX | 2000+ | SSGC/SNGPL, LNG imports |
| Heating Oil | `HO=F` | NYMEX | 2000+ | Diesel proxy |
| RBOB Gasoline | `RB=F` | NYMEX | 2005+ | Petrol proxy |
| Ethanol | — | — | Limited | |
| Coal | — | — | Not on Yahoo | See Commodities-API |
| Uranium | `UX=F` | — | Limited | PAEC nuclear program |

### AGRICULTURE

| Commodity | Ticker | Exchange | History | Pakistan Relevance |
|-----------|--------|----------|---------|-------------------|
| Cotton #2 | `CT=F` | ICE | 2000+ | **#4 global producer**, textile backbone |
| Rough Rice | `ZR=F` | CBOT | 2000+ | **#4 global exporter** (Basmati) |
| Wheat | `ZW=F` | CBOT | 2000+ | **#8 producer**, staple food |
| Corn | `ZC=F` | CBOT | 2000+ | Feed/poultry industry |
| Sugar #11 | `SB=F` | ICE | 2000+ | Major domestic crop |
| Soybeans | `ZS=F` | CBOT | 2000+ | Edible oil |
| Soybean Oil | `ZL=F` | CBOT | 2000+ | Cooking oil import substitute |
| Soybean Meal | `ZM=F` | CBOT | 2000+ | Animal feed |
| Coffee (Arabica) | `KC=F` | ICE | 2000+ | Import only |
| Cocoa | `CC=F` | ICE | 2000+ | Confectionery import |
| Orange Juice | `OJ=F` | ICE | 2000+ | |
| Oats | `ZO=F` | CBOT | 2000+ | |
| Lumber | `LBS=F` | CME | 2000+ | Construction |
| Live Cattle | `LE=F` | CME | 2000+ | Meat industry proxy |
| Feeder Cattle | `GF=F` | CME | 2000+ | |
| Lean Hogs | `HE=F` | CME | 2000+ | NOT relevant (halal) |
| Palm Oil | — | — | Not on Yahoo | **Largest PK edible oil import** — see Bursa Malaysia |

### FOREX (relevant for PKR conversion)

| Pair | Ticker | Notes |
|------|--------|-------|
| USD/PKR | `PKR=X` | Direct PKR rate |
| EUR/USD | `EURUSD=X` | |
| GBP/USD | `GBPUSD=X` | Remittances corridor |
| USD/SAR | `SAR=X` | Gulf remittances |
| USD/AED | `AED=X` | Gulf remittances |
| USD/CNY | `CNY=X` | CPEC trade |
| DXY (Dollar Index) | `DX-Y.NYB` | Dollar strength |

### COMMODITY ETFs (alternative to futures — no rollover issues)

| ETF | Ticker | Tracks |
|-----|--------|--------|
| SPDR Gold Trust | `GLD` | Gold spot |
| iShares Silver Trust | `SLV` | Silver spot |
| USO Oil Fund | `USO` | WTI Crude |
| US Natural Gas Fund | `UNG` | Natural Gas |
| Invesco DB Agriculture | `DBA` | Broad Agri basket |
| iPath Cotton ETN | `BAL` | Cotton |
| iPath Bloomberg Sugar | `SGG` | Sugar |
| Invesco DB Commodity | `DBC` | Broad commodities |
| GSCI Commodity ETF | `GSG` | Broad benchmark |

### yfinance Usage

```python
import yfinance as yf

# Single commodity
gold = yf.download("GC=F", start="2020-01-01")
# Returns: Date, Open, High, Low, Close, Adj Close, Volume

# Multiple commodities at once
commodities = yf.download(
    ["GC=F", "SI=F", "CL=F", "CT=F", "ZR=F", "ZW=F"],
    start="2020-01-01",
    group_by="ticker"
)

# Commodity ETFs (no rollover issues, better for long-term analytics)
etfs = yf.download(["GLD", "SLV", "USO", "DBA"], start="2015-01-01")
```

**Limitations**: No Palm Oil, Iron Ore, Zinc, Aluminum, Coal, Steel, Lithium, LNG spot. No PKR-denominated prices. Rolling front-month futures have rollover gaps.

---

## 2. PMEX (Pakistan Mercantile Exchange) — PKR Denominated

### Products Traded

**Metals (PKR denominated)**
- Gold: GO1oz, GO10oz, GO100oz, MUGOLD (Micro)
- Silver: SL5kg, SL100oz
- Platinum: PT5oz, PT50oz
- Palladium: PD50oz, PD100oz
- Copper: CU1K, CU25K (lbs)

**Energy (USD & PKR)**
- Crude Oil: CR10, CR100, CR1000 (barrels)
- Brent Oil: BR10, BR100
- Natural Gas: NG1K, NG10K (mmbtu)

**Agriculture**
- Cotton: CO5K, CO50K (lbs)
- Local Wheat, IRRI6 Rice, Sugar (seasonal)
- International: Palm Oil

**Financials**
- USD/PKR futures
- EUR/USD, GBP/USD, USD/JPY
- DowJones, NASDAQ-100, S&P500, JPY Equity indices

### Data Access

PMEX uses MetaTrader 5 (MT5) as trading platform. Data access options:

1. **PMEX Data Portal** (`dportal.pmex.com.pk/mwatch`) — JS-rendered, no API
2. **MT5 Terminal** — Free download, has built-in data export
3. **MT5 Python API** (`MetaTrader5` pip package) — Programmatic access

```python
# MT5 Python integration (requires MT5 terminal running)
import MetaTrader5 as mt5

mt5.initialize()
# Get PMEX Gold rates
rates = mt5.copy_rates_from_pos("GO1oz", mt5.TIMEFRAME_D1, 0, 1000)
# Returns: time, open, high, low, close, tick_volume, spread, real_volume
```

**Constraint**: MT5 must be installed and connected to a PMEX broker account (Abbasi & Company, JS Global, Foundation Securities, etc.). Data is real-time during market hours.

---

## 3. Dedicated Commodity APIs

### Tier A: Best Free Options

#### API-Ninjas Commodity API
- **URL**: `api.api-ninjas.com/v1/commodityprice`
- **Free**: 50,000 req/month (7 rotating commodities/week on free tier)
- **Premium**: $9.99/mo (all commodities)
- **Historical**: `/v1/commoditypricehistorical` — OHLCV format, 1m to 1d intervals
- **Commodities**: Gold, Silver, Platinum, Palladium, Copper, Crude Oil, Brent, Natural Gas, Wheat, Corn, Soybeans, Soybean Oil, Soybean Meal, Cotton, Sugar, Coffee, Cocoa, Rice, Oats, Lumber, Live Cattle, Feeder Cattle, Lean Hogs, Heating Oil, Gasoline
- **Currency**: USD only
- **Best for**: Historical OHLCV data, cheapest premium tier

```python
import requests
url = "https://api.api-ninjas.com/v1/commoditypricehistorical"
params = {"name": "gold", "interval": "1d"}
headers = {"X-Api-Key": "YOUR_KEY"}
r = requests.get(url, params=params, headers=headers)
```

#### Commodities-API
- **URL**: `commodities-api.com`
- **Free**: 100 req/month
- **Paid**: $14.99/mo (10K req), $49.99/mo (50K req)
- **Commodities**: **600+ symbols** including Rice, Wheat, Cotton, Palm Oil, Rubber, Coal, Aluminum, Zinc, Iron Ore, Lumber, Sugar, Coffee, Cocoa + all metals + energy
- **Currency**: 170 currencies **including PKR**
- **Historical**: Daily rates back to 1999 for major commodities
- **Best for**: Broadest coverage, PKR conversion built-in

```python
url = "https://commodities-api.com/api/latest"
params = {
    "access_key": "YOUR_KEY",
    "base": "USD",
    "symbols": "RICE,WHEAT,COTTON,PALM OIL,GOLD,CRUDE OIL"
}
```

#### Metals-API
- **URL**: `metals-api.com`
- **Free**: 100 req/month
- **Paid**: $14.99/mo (10K req)
- **Metals**: Gold, Silver, Platinum, Palladium + LME metals (Copper, Aluminum, Zinc, Lead, Nickel, Tin)
- **Currency**: 170 currencies including PKR
- **Historical**: LBMA Gold back to 1968, Silver 2010+, LME metals 2008+
- **OHLC**: Available on paid plans
- **Best for**: Metals-focused with LME industrial metals

#### CommodityPriceAPI (NEW)
- **URL**: `commoditypriceapi.com`
- **Free**: Limited trial
- **Commodities**: 130+ including all Pakistan-relevant ones
- **Features**: Real-time, Historical OHLC, Time-Series, Fluctuation
- **Currency**: Multi-currency support
- **Best for**: Modern API, good documentation

### Tier B: Specialized Sources

#### GoldPriceZ API (Pakistan Gold Rates)
- **URL**: `goldpricez.com/api`
- **Free**: 30-60 req/hour
- **Unique**: **PKR per Tola** (Pakistan unit) natively
- **Metals**: Gold (24K-10K by karat), Silver
- **Units**: Gram, Ounce, Tola-Pakistan, Tola-India, Masha, Baht, Tael
- **Best for**: Pakistan local gold/silver rates matching Sarafa Bazaar

```python
headers = {"X-API-KEY": "YOUR_KEY"}
url = "https://goldpricez.com/api/rates/currency/pkr/measure/tola-pakistan"
```

#### Metals.Dev
- **URL**: `metals.dev`
- **Free**: 100 req/month
- **Metals**: Precious (LBMA) + Industrial (LME)
- **Currency**: 170 currencies including PKR
- **Historical**: 5+ years
- **Best for**: LBMA/LME official settlement prices

#### GoldAPI.io
- **URL**: `goldapi.io`
- **Free**: 100 req/month
- **Metals**: Gold, Silver, Platinum, Palladium
- **Sources**: FOREX, SAXO, OANDA, IDC
- **Best for**: Real-time spot prices with bid/ask spread

---

## 4. Pakistan-Specific Data Sources

### SBP (State Bank of Pakistan) — FREE
- **Exchange Rates**: `sbp.org.pk/ecodata/rates/`
  - WAR (Weighted Average Rate) — interbank FX
  - ECAP — open market exchange rates
  - KIBOR — interbank lending rates
- **Relevance**: USD/PKR rate needed to convert all USD commodity prices to local

### Pakistan Bureau of Statistics — FREE
- **URL**: `pbs.gov.pk`
- **Data**: Monthly wholesale/retail prices for domestic commodities
- **Includes**: Wheat flour, rice (IRRI-6, Basmati), sugar, cooking oil, ghee, cotton, cement
- **Format**: PDF reports (need scraping)

### PASSCO (Pakistan Agricultural Storage & Services Corporation)
- **Wheat procurement prices** set by government
- **Relevant for**: Local wheat vs international wheat price differential

### Karachi Cotton Association (KCA)
- **URL**: `kcapk.org`
- **Daily**: Local cotton spot rates (PKR per maund)
- **Relevant for**: Pakistan cotton vs ICE Cotton #2 spread

### Pakistan Edible Oil Conference
- **Palm Oil import pricing** aligned with Bursa Malaysia Palm Oil futures
- **BMD Palm Oil ticker on yfinance**: Not available — use Commodities-API

---

## 5. Missing from yfinance — Alternative Sources

These commodities are critical for Pakistan but NOT available on yfinance:

| Commodity | PK Relevance | Best Free Source | Ticker/Symbol |
|-----------|-------------|-----------------|---------------|
| **Palm Oil** | #1 edible oil import | Commodities-API | `PALM OIL` |
| **Iron Ore** | Steel/construction | Commodities-API | `IRON ORE 62%` |
| **Coal** | Power generation | Commodities-API | `COAL` |
| **Zinc** | Galvanizing industry | Metals-API | `LME-XZN` |
| **Aluminum** | Packaging/construction | Commodities-API | `ALUMINUM` |
| **Steel** | Construction boom | Finnworlds | `STEEL` |
| **Lithium** | EV future | Metals-API | — |
| **Tin** | Electronics/solder | Metals-API | `LME-XSN` |
| **Lead** | Batteries | Metals-API | `LME-XPB` |
| **Nickel** | Stainless steel | Metals-API | `LME-XNI` |
| **LNG Spot** | Gas imports | No free API | Manual from GIIGNL |
| **Rubber** | Tires/industry | Commodities-API | `RUBBER` |
| **Urea/DAP** | Fertilizer (Engro, FFC) | Manual from fertilizer assoc | — |

---

## 6. Recommended Architecture for Your Trading App

### Phase 1: Immediate (Zero cost, zero new deps)

```python
# config/commodity_universe.py
COMMODITY_UNIVERSE = {
    "precious_metals": {
        "Gold":      {"yf": "GC=F",  "etf": "GLD",  "pmex": "GO1oz",  "unit": "USD/oz"},
        "Silver":    {"yf": "SI=F",  "etf": "SLV",  "pmex": "SL100oz", "unit": "USD/oz"},
        "Platinum":  {"yf": "PL=F",  "etf": None,   "pmex": "PT5oz",   "unit": "USD/oz"},
        "Palladium": {"yf": "PA=F",  "etf": None,   "pmex": "PD50oz",  "unit": "USD/oz"},
    },
    "base_metals": {
        "Copper":    {"yf": "HG=F",  "etf": None,   "pmex": "CU1K",   "unit": "USD/lb"},
    },
    "energy": {
        "Crude Oil WTI":  {"yf": "CL=F",  "etf": "USO", "pmex": "CR100",  "unit": "USD/bbl"},
        "Brent Crude":    {"yf": "BZ=F",  "etf": None,  "pmex": "BR100",  "unit": "USD/bbl"},
        "Natural Gas":    {"yf": "NG=F",  "etf": "UNG", "pmex": "NG1K",   "unit": "USD/mmbtu"},
        "Heating Oil":    {"yf": "HO=F",  "etf": None,  "pmex": None,     "unit": "USD/gal"},
        "Gasoline":       {"yf": "RB=F",  "etf": None,  "pmex": None,     "unit": "USD/gal"},
    },
    "agriculture": {
        "Cotton":     {"yf": "CT=F",  "etf": "BAL", "pmex": "CO5K",  "unit": "USD/lb"},
        "Rice":       {"yf": "ZR=F",  "etf": None,  "pmex": None,    "unit": "USD/cwt"},
        "Wheat":      {"yf": "ZW=F",  "etf": None,  "pmex": None,    "unit": "USD/bu"},
        "Sugar":      {"yf": "SB=F",  "etf": "SGG", "pmex": None,    "unit": "USD/lb"},
        "Corn":       {"yf": "ZC=F",  "etf": None,  "pmex": None,    "unit": "USD/bu"},
        "Soybeans":   {"yf": "ZS=F",  "etf": None,  "pmex": None,    "unit": "USD/bu"},
        "Soybean Oil":{"yf": "ZL=F",  "etf": None,  "pmex": None,    "unit": "USD/lb"},
        "Coffee":     {"yf": "KC=F",  "etf": None,  "pmex": None,    "unit": "USD/lb"},
        "Cocoa":      {"yf": "CC=F",  "etf": None,  "pmex": None,    "unit": "USD/ton"},
        "Lumber":     {"yf": "LBS=F", "etf": None,  "pmex": None,    "unit": "USD/mbf"},
    },
    "fx": {
        "USD/PKR":    {"yf": "PKR=X"},
        "EUR/USD":    {"yf": "EURUSD=X"},
        "GBP/USD":    {"yf": "GBPUSD=X"},
        "DXY":        {"yf": "DX-Y.NYB"},
    },
}
```

### Phase 2: Add Missing Commodities (API-Ninjas or Commodities-API)

```python
# For Palm Oil, Iron Ore, Coal, LME metals
EXTENDED_UNIVERSE = {
    "industrial_metals": {
        "Aluminum":  {"api": "commodities-api", "symbol": "ALUMINUM"},
        "Zinc":      {"api": "metals-api",      "symbol": "LME-XZN"},
        "Nickel":    {"api": "metals-api",      "symbol": "LME-XNI"},
        "Lead":      {"api": "metals-api",      "symbol": "LME-XPB"},
        "Tin":       {"api": "metals-api",      "symbol": "LME-XSN"},
        "Iron Ore":  {"api": "commodities-api", "symbol": "IRON ORE 62%"},
    },
    "pk_critical_imports": {
        "Palm Oil":  {"api": "commodities-api", "symbol": "PALM OIL"},
        "Coal":      {"api": "commodities-api", "symbol": "COAL"},
        "Rubber":    {"api": "commodities-api", "symbol": "RUBBER"},
    },
}
```

### Phase 3: PKR Localization

```python
# Convert all USD prices to PKR using live or daily FX
import yfinance as yf

pkr_rate = yf.download("PKR=X", period="1d")["Close"].iloc[-1]

# Gold in PKR per Tola
gold_usd_oz = yf.download("GC=F", period="1d")["Close"].iloc[-1]
TOLA_TO_OZ = 0.40125  # Pakistan tola = 0.40125 troy oz
gold_pkr_tola = gold_usd_oz * TOLA_TO_OZ * pkr_rate

# Or use GoldPriceZ API for direct PKR/Tola rates
```

### Phase 4: PMEX Integration (MT5 API)

```python
# Real PKR-denominated futures from Pakistan's exchange
import MetaTrader5 as mt5

mt5.initialize(server="PMEX-Live")
symbols = ["GO1oz", "SL100oz", "CR100", "CO5K", "NG1K"]
for s in symbols:
    rates = mt5.copy_rates_from_pos(s, mt5.TIMEFRAME_D1, 0, 365)
```

---

## 7. API Comparison Matrix

| Feature | yfinance | API-Ninjas | Commodities-API | Metals-API | GoldPriceZ |
|---------|----------|------------|-----------------|------------|------------|
| **Cost** | FREE | FREE/9.99$/mo | FREE/14.99$/mo | FREE/14.99$/mo | FREE |
| **Rate Limit** | ~2K/hr | 50K/mo | 100/mo free | 100/mo free | 30-60/hr |
| **OHLCV** | ✅ Full | ✅ Historical | ✅ OHLC | ✅ OHLC | ❌ Spot only |
| **Precious Metals** | ✅ 4 | ✅ 4 | ✅ 6+ | ✅ 4+LME | ✅ 2 |
| **Base Metals** | Copper only | ❌ | ✅ All LME | ✅ All LME | ❌ |
| **Energy** | ✅ 5 | ✅ 5 | ✅ 6+ | ❌ | ❌ |
| **Agriculture** | ✅ 12+ | ✅ 12+ | ✅ 20+ | ❌ | ❌ |
| **Palm Oil** | ❌ | ❌ | ✅ | ❌ | ❌ |
| **Iron Ore** | ❌ | ❌ | ✅ | ❌ | ❌ |
| **Coal** | ❌ | ❌ | ✅ | ❌ | ❌ |
| **PKR Support** | ✅ FX pair | ❌ USD only | ✅ 170 currencies | ✅ 170 currencies | ✅ Native PKR/Tola |
| **History Depth** | 20+ years | ~1 year | 25+ years | 50+ years (LBMA) | Current only |
| **Intraday** | ✅ 1m-1h | ✅ 1m-4h | ❌ Daily only | ❌ Daily only | ❌ |
| **Python SDK** | ✅ yfinance | requests | requests | requests | requests |

---

## 8. Quick Start — Minimum Viable Commodity Module

```python
"""
commodity_adapter.py — Drop into psx_ohlcv or qp-mono
Fetches OHLCV for all Pakistan-relevant commodities via yfinance
"""
import yfinance as yf
import pandas as pd

PK_COMMODITIES = {
    # Metals
    "GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F",
    "PALLADIUM": "PA=F", "COPPER": "HG=F",
    # Energy
    "CRUDE_WTI": "CL=F", "BRENT": "BZ=F", "NATGAS": "NG=F",
    "HEATING_OIL": "HO=F", "GASOLINE": "RB=F",
    # Agriculture
    "COTTON": "CT=F", "RICE": "ZR=F", "WHEAT": "ZW=F",
    "SUGAR": "SB=F", "CORN": "ZC=F", "SOYBEANS": "ZS=F",
    "SOYBEAN_OIL": "ZL=F", "COFFEE": "KC=F", "COCOA": "CC=F",
    "LUMBER": "LBS=F",
    # FX
    "USD_PKR": "PKR=X", "DXY": "DX-Y.NYB",
}

def fetch_commodity(name: str, start: str = "2020-01-01") -> pd.DataFrame:
    ticker = PK_COMMODITIES.get(name.upper())
    if not ticker:
        raise ValueError(f"Unknown commodity: {name}")
    return yf.download(ticker, start=start, auto_adjust=True)

def fetch_all(start: str = "2020-01-01") -> dict[str, pd.DataFrame]:
    tickers = list(PK_COMMODITIES.values())
    data = yf.download(tickers, start=start, group_by="ticker")
    return {
        name: data[ticker] for name, ticker in PK_COMMODITIES.items()
        if ticker in data.columns.get_level_values(0)
    }

def gold_pkr_tola(start: str = "2020-01-01") -> pd.Series:
    """Gold price in PKR per Tola (Pakistan standard unit)"""
    gold = yf.download("GC=F", start=start)["Close"]
    pkr = yf.download("PKR=X", start=start)["Close"]
    TOLA_OZ = 0.40125  # 1 Pakistan Tola = 0.40125 troy ounces
    aligned = pd.concat([gold, pkr], axis=1, keys=["gold_usd", "pkr_rate"]).dropna()
    return aligned["gold_usd"] * TOLA_OZ * aligned["pkr_rate"]
```

---

## Summary: What to Use When

| Need | Use |
|------|-----|
| Quick OHLCV for 25+ commodities | yfinance (free, in your stack) |
| Palm Oil, Iron Ore, Coal, Zinc, Aluminum | Commodities-API ($14.99/mo) |
| LME settlement prices for base metals | Metals-API ($14.99/mo) |
| Gold/Silver in PKR per Tola | GoldPriceZ (free) |
| PMEX futures in PKR | MT5 Python API (free with broker account) |
| Full historical OHLCV with intraday | API-Ninjas ($9.99/mo) |
| SBP exchange rates for PKR conversion | SBP website (free, scrape) |
