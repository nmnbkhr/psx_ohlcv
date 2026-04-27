# PMEX Market Watch API — Complete Discovery Report

## Summary

The PMEX (Pakistan Mercantile Exchange) Data Portal at `dportal.pmex.com.pk` provides **real-time commodity futures data** via a simple JSON API. One POST endpoint returns all 63 active contracts across 7 categories with bid/ask, OHLC, volume, and change data.

---

## API Endpoint — Confirmed Working

```
POST https://dportal.pmex.com.pk/MWatchNew/Home/GetJSONObject
Content-Type: application/json
Body: {"status": "Active"}
```

**Response:** JSON array of contract objects. Polled automatically by the page every few seconds.

**Alternative status:** `"Inactive"` maps to "All Contracts" dropdown — includes expired contracts (not tested from curl but available via browser).

---

## Response Schema (All Fields)

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `Contract` | string | `"GO100OZ-JU26"` | Contract symbol with expiry code |
| `Category` | string | `"Metals"` | Asset class grouping |
| `Bid` | float | `393390` | Current bid price |
| `Ask` | float | `393490` | Current ask price |
| `Bid_Vol` | null | `null` | Bid volume (not populated) |
| `Ask_Vol` | null | `null` | Ask volume (not populated) |
| `Open` | float | `393105` | Opening price |
| `High` | float | `393925` | Day high |
| `Low` | float | `391490` | Day low |
| `Close` | float | `392590` | Previous close / settlement |
| `Last_Price` | string | `"393830"` | Last traded price (string!) |
| `Last_Vol` | string | `"1"` | Last trade volume (string!) |
| `Total_Vol` | string | `"47"` | Total day volume (string!) |
| `Total_Volume` | int | `0` | Alternative volume field |
| `Change` | float | `-85.25` | Absolute price change |
| `Change_Per` | float | `-0.37` | Percentage change |
| `BidDiff` | int | `0` | Bid change since last poll |
| `AskDiff` | int | `0` | Ask change since last poll |
| `_datetime` | int | `1774663199` | Unix timestamp |
| `State` | null | `null` | Contract state |
| `_date` | null | `null` | Not populated |
| `_time` | null | `null` | Not populated |
| `_Message` | null | `null` | Not populated |
| `SysDate` | null | `null` | Not populated |
| `StatusTime` | null | `null` | Not populated |
| `last_` | int | `0` | Unknown |
| `volume_` | int | `0` | Unknown |
| `rBid` | null | `null` | Not populated |
| `rAsk` | null | `null` | Not populated |
| `BidVolume` | null | `null` | Not populated |
| `AskVolume` | null | `null` | Not populated |

**NOTE:** `Last_Price`, `Last_Vol`, and `Total_Vol` are **strings**, not numbers. Parse accordingly.

---

## All 63 Active Contracts (as of 2026-03-29)

### Indices (6 contracts)
| Contract | Description |
|----------|-------------|
| `2NSDQ100-JU26` | Mini NASDAQ 100 (Jun 2026) |
| `DJ-JU26` | Dow Jones (Jun 2026) |
| `JPYEQTY1-JU26` | JPY Equity Index 1 (Jun 2026) |
| `JPYEQTY5-JU26` | JPY Equity Index 5 (Jun 2026) |
| `NSDQ100-JU26` | NASDAQ 100 (Jun 2026) |
| `SP500-JU26` | S&P 500 (Jun 2026) |

### Energy (2 contracts)
| Contract | Description |
|----------|-------------|
| `NGAS10K-MY26` | Natural Gas 10K (May 2026) |
| `NGAS1K-MY26` | Natural Gas 1K (May 2026) |

### Metals (14 contracts)
| Contract | Description |
|----------|-------------|
| `ALUMINUM1-JU26` | Aluminum 1 lot (Jun 2026) |
| `ALUMINUM5-JU26` | Aluminum 5 lot (Jun 2026) |
| `COPPER-MY26` | Copper (May 2026) |
| `COPPER25K-MY26` | Copper 25K (May 2026) |
| `GO100OZ-JU26` | Gold 100oz (Jun 2026) |
| `GO100OZ-JU26ID` | Gold 100oz Intraday (Jun 2026) |
| `GO10OZ-JU26` | Gold 10oz (Jun 2026) |
| `GO10OZ-JU26ID` | Gold 10oz Intraday (Jun 2026) |
| `GO1OZ-JU26` | Gold 1oz (Jun 2026) |
| `GO1OZ-JU26ID` | Gold 1oz Intraday (Jun 2026) |
| `GOMOZ-JU26` | Gold Mini Oz (Jun 2026) |
| `PALDIUM100-JU26` | Palladium 100 (Jun 2026) |
| `PLATINUM5-JY26` | Platinum 5 (Jul 2026) |
| `PLATINUM50-JY26` | Platinum 50 (Jul 2026) |

### Oil (12 contracts)
| Contract | Description |
|----------|-------------|
| `BRENT10-JU26` | Brent Crude 10bbl (Jun 2026) |
| `BRENT10-MY26` | Brent Crude 10bbl (May 2026) |
| `BRENT100-JU26` | Brent Crude 100bbl (Jun 2026) |
| `BRENT100-MY26` | Brent Crude 100bbl (May 2026) |
| `BRENT1000-JU26` | Brent Crude 1000bbl (Jun 2026) |
| `BRENT1000-MY26` | Brent Crude 1000bbl (May 2026) |
| `CRUDE10-MY26` | WTI Crude 10bbl (May 2026) |
| `CRUDE10-MY26ID` | WTI Crude 10bbl Intraday (May 2026) |
| `CRUDE100-MY26` | WTI Crude 100bbl (May 2026) |
| `CRUDE100-MY26ID` | WTI Crude 100bbl Intraday (May 2026) |
| `CRUDE1000-MY26` | WTI Crude 1000bbl (May 2026) |
| `CRUDE1000-MY26ID` | WTI Crude 1000bbl Intraday (May 2026) |

### COTS — Currency-Commodity (22 contracts)
Cross-currency gold contracts (Contracts on Trading System):
- `GOLDAUDCAD-MY26`, `GOLDAUDJPY-MY26`, `GOLDAUDUSD-MY26`, `GOLDAUDUSD-MY26ID`
- `GOLDCHFJPY-MY26`, `GOLDEURAUD-MY26`, `GOLDEURCAD-MY26`, `GOLDEURCHF-MY26`
- `GOLDEURGBP-MY26`, `GOLDEURJPY-MY26`, `GOLDEURUSD-MY26`, `GOLDEURUSD-MY26ID`
- `GOLDGBPCHF-MY26`, `GOLDGBPJPY-MY26`, `GOLDGBPUSD-MY26`, `GOLDGBPUSD-MY26ID`
- `GOLDUSDCAD-MY26`, `GOLDUSDCAD-MY26ID`, `GOLDUSDCHF-MY26`, `GOLDUSDCHF-MY26ID`
- `GOLDUSDJPY-MY26`, `GOLDUSDJPY-MY26ID`

### Agriculture (5 contracts)
| Contract | Description |
|----------|-------------|
| `ICORN-MY26` | International Corn (May 2026) |
| `ICOTTON-MY26` | International Cotton (May 2026) |
| `ICOTTON50K-MY26` | International Cotton 50K (May 2026) |
| `ISOYBEAN-MY26` | International Soybean (May 2026) |
| `IWHEAT-MY26` | International Wheat (May 2026) |

### Physical Gold (2 contracts)
| Contract | Description |
|----------|-------------|
| `MTOLAGOLD-WED` | Mini Tola Gold (Weekly Wed) |
| `TOLAGOLD-WED` | Tola Gold (Weekly Wed) |

---

## Contract Naming Convention

```
{COMMODITY}{SIZE}-{EXPIRY_CODE}
```

**Expiry codes:**
- `MY26` = May 2026
- `JU26` = June 2026
- `JY26` = July 2026
- `WED` = Weekly Wednesday settlement (physical delivery)

**Suffix `ID`** = Intraday contract (same commodity, same expiry, different margin/settlement rules)

---

## Page Architecture

- **URL (new):** `https://dportal.pmex.com.pk/mwatchnew` — responsive, Bootstrap 5
- **URL (old):** `https://dportal.pmex.com.pk/mwatch` — same API, older layout
- **Tech:** jQuery AJAX POST, polls every ~3-5 seconds
- **Dropdown:** `#marketStatus` select — `Active` (live contracts) or `Inactive` (all contracts including expired)
- **No authentication** required
- **Categories displayed on page but not in API:** Financials, EWR, Local Agriculture — these likely have 0 active contracts currently

---

## What This Means for pakfindata

### Current State
The pakfindata PMEX page (`/pmex`) currently exists and shows commodity data. This API discovery confirms:

1. **Single POST call = all 63 contracts** with bid/ask + OHLCV + volume + change
2. **Real-time polling** — data updates every ~3-5 seconds during market hours
3. **No rate limiting detected** — page itself polls continuously
4. **No auth needed** — fully public API

### Integration Approach

```python
import requests

def fetch_pmex_market_watch(status="Active"):
    """Fetch all PMEX contracts in one call."""
    resp = requests.post(
        "https://dportal.pmex.com.pk/MWatchNew/Home/GetJSONObject",
        json={"status": status},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    
    # Fix string fields
    for item in data:
        item["Last_Price"] = float(item.get("Last_Price") or 0)
        item["Last_Vol"] = int(item.get("Last_Vol") or 0)
        item["Total_Vol"] = int(item.get("Total_Vol") or 0)
    
    return data
```

### Storage
New SQLite table `pmex_market_watch`:
```sql
CREATE TABLE IF NOT EXISTS pmex_market_watch (
    contract TEXT,
    category TEXT,
    bid REAL,
    ask REAL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    last_price REAL,
    total_volume INTEGER,
    change REAL,
    change_pct REAL,
    timestamp INTEGER,
    scraped_at TEXT,
    PRIMARY KEY (contract, scraped_at)
);
```

### Daily Sync
Add to scraper_service schedule:
```
16:30 PKT — fetch PMEX market watch (PMEX closes after PSX)
```

---

## Comparison: PMEX vs PSX Data Sources

| Feature | PSX (DPS) | PMEX |
|---------|-----------|------|
| Asset type | Equities + Index | Commodity futures |
| Bid/Ask | WebSocket only | REST API ✅ |
| OHLCV | REST API | REST API ✅ |
| Contracts | ~487 symbols | 63 contracts |
| Historical | 5yr via `/timeseries/eod` | Current day only (no historical endpoint found) |
| Auth | None | None |
| Real-time | WebSocket | Polling (~3-5s) |

**Key advantage of PMEX API:** Bid/Ask is available via REST — unlike PSX where bid/ask only exists in the WebSocket stream.

**Gap:** No historical PMEX data endpoint found. The API only returns current market state. Historical data would need to be built by daily snapshots.
