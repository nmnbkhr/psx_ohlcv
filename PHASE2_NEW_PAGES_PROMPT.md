# Claude Code Prompt: Phase 2 — New Analytics Pages

## Context

pakfindata (~/pakfindata/) is a Streamlit multi-page app with Bloomberg Terminal 
dark theme (#0B0E11 bg, #C8A96E gold, JetBrains Mono). DB: `/mnt/e/psxdata/psx.sqlite`.

Phase 1 completed: circuit limits in Live Market, index weights in Heatmap, VAR in Scanner.

Phase 2 adds **two new pages** using tick-level JSONL data and downloaded gap files.

**Rules:**
- Additive only — do NOT modify existing pages
- Read existing page structure FIRST (routing, sidebar, theme imports)
- Use pakfindata theme constants
- All math in raw numpy/pandas (no TA libraries)
- Cache aggressively with `@st.cache_data`

## Step 1: Understand existing app structure

```bash
# Page routing
cat ~/pakfindata/src/pakfindata/ui/app.py | head -80

# Existing pages list
ls ~/pakfindata/src/pakfindata/ui/page_views/

# Theme/constants
cat ~/pakfindata/src/pakfindata/ui/theme.py 2>/dev/null || \
grep -rn "0B0E11\|C8A96E\|JetBrains" ~/pakfindata/src/pakfindata/ui/ | head -10

# How pages are registered (sidebar nav)
grep -rn "page_config\|sidebar\|navigation\|page_views" ~/pakfindata/src/pakfindata/ui/app.py

# Check available tick data
ls -lh /mnt/e/psxdata/tick_logs_cloud/ | tail -5
head -1 /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | head -3

# Check downloaded derivatives data
ls /mnt/e/psxdata/downloads/daily/*/futures/ 2>/dev/null | head -5
ls /mnt/e/psxdata/downloads/daily/*/sif/ 2>/dev/null | head -5

# Check existing engine modules
ls ~/pakfindata/src/pakfindata/engine/
```

**STOP — read ALL output. Understand page registration, theme system, and data availability before building.**

---

## Page 1: Microstructure Analytics

### Location
`src/pakfindata/ui/page_views/microstructure.py`

### Data Sources
- **Primary:** Cloud tick logs at `/mnt/e/psxdata/tick_logs_cloud/*.jsonl`
- **Secondary:** Local tick logs at `/mnt/e/psxdata/tick_logs/*.jsonl`
- JSONL format per line:
  ```json
  {
    "symbol": "HUBC", "market": "REG", "price": 188.38,
    "open": 195.1, "high": 197.9, "low": 187.1,
    "change": -6.72, "changePercent": -0.03444,
    "volume": 5978764, "value": 1123456789,
    "trades": 2341, "bid": 188.35, "ask": 188.40,
    "bidVol": 5000, "askVol": 3200,
    "previousClose": 195.1, "timestamp": 1773650988.364,
    "_ts": "2026-03-17T13:49:48.364+05:00"
  }
  ```

### Page Layout

```
┌─────────────────────────────────────────────────────────────┐
│  MICROSTRUCTURE ANALYTICS                    [Date picker]  │
│  Symbol: [HUBC ▼]                            [Load Data]    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐       │
│  │ Spread (avg) │ │ VPIN Score   │ │ Trade Imbal. │       │
│  │   0.12 PKR   │ │    67/100    │ │   +23% buy   │       │
│  └──────────────┘ └──────────────┘ └──────────────┘       │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Bid-Ask Spread (intraday)                 │   │
│  │  ▁▂▃▄▅▆▇█▇▆▅▄▃▂▁▂▃▄▅▆▇▆▅▄▃▂▁                     │   │
│  │  09:30        11:00        13:00        15:00       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────┐  ┌──────────────────────────┐   │
│  │  Volume Profile       │  │  Trade Size Distribution │   │
│  │  (price vs volume)    │  │  (histogram of sizes)    │   │
│  │                       │  │                          │   │
│  │  ████ 188.50          │  │  ▓▓▓▓▓▓ 1-100           │   │
│  │  ██████ 188.40        │  │  ▓▓▓▓ 101-500           │   │
│  │  ████████ 188.30      │  │  ▓▓ 501-1000            │   │
│  │  ██ 188.20            │  │  ▓ 1000+                 │   │
│  └──────────────────────┘  └──────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              VPIN (Volume-Synchronized)              │   │
│  │  ▁▂▃▄▅▆▇█▇▆▅▄▃▂▁▂▃▄▅▆▇▆▅▄▃▂▁                     │   │
│  │  Toxic zone ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ (0.7 threshold) │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Order Flow Imbalance                    │   │
│  │  Buy ████████████████░░░░░░░░ Sell                  │   │
│  │       62%                38%                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Tick-by-Tick Table (last 100 trades)               │   │
│  │  Time      Price   Vol   Bid    Ask    Spread  Side │   │
│  │  13:49:48  188.38  500   188.35 188.40  0.05   SELL │   │
│  │  13:49:47  188.38  50    188.35 188.40  0.05   SELL │   │
│  │  ...                                                │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Analytics to Compute (all raw numpy/pandas)

**1. Bid-Ask Spread Analysis**
```python
# From tick JSONL: bid, ask fields
spread = ask - bid                           # absolute spread
spread_pct = spread / ((bid + ask) / 2) * 100  # relative spread %
# Plot: intraday spread time series (resample to 1-minute avg)
# Metrics: mean spread, median spread, max spread, spread volatility
```

**2. VPIN (Volume-Synchronized Probability of Informed Trading)**
```python
# Already exists in engine/ — check:
grep -rn "vpin\|VPIN" ~/pakfindata/src/pakfindata/engine/ 2>/dev/null
# If exists, import and use. If not, implement:
# 
# Algorithm:
# 1. Classify each tick as buy/sell using tick rule:
#    if price > prev_price → buy
#    if price < prev_price → sell
#    if price == prev_price → same as previous classification
# 2. Create volume buckets of equal size (e.g., 50,000 shares each)
# 3. For each bucket: VPIN = |buy_volume - sell_volume| / bucket_size
# 4. Rolling average over N buckets (e.g., N=50)
# 5. VPIN > 0.7 = toxic (informed trading likely)
```

**3. Volume Profile (Price vs Volume)**
```python
# Group ticks by price level (round to nearest 0.01 or tick size)
# Sum volume at each price level
# Plot horizontal bar chart: price on Y-axis, volume on X-axis
# Highlight POC (Point of Control) = price with max volume
# Highlight Value Area (70% of total volume)
```

**4. Trade Size Distribution**
```python
# From tick JSONL: volume field (per-tick volume)
# Note: volume in JSONL is CUMULATIVE — need to diff:
#   tick_volume = current_volume - previous_volume (per symbol)
#   OR use the DPS ticks which have per-trade volume
# Buckets: 1-100, 101-500, 501-1000, 1001-5000, 5000+
# Histogram + stats: median trade size, % retail (<500) vs institutional (>5000)
```

**5. Order Flow Imbalance**
```python
# Using bid/ask volumes from ticks:
imbalance = (bidVol - askVol) / (bidVol + askVol)  # -1 to +1
# Positive = buying pressure, Negative = selling pressure
# Plot: intraday imbalance time series
# Also: classify trades as buy/sell using Lee-Ready algorithm:
#   if trade_price >= ask → buyer initiated
#   if trade_price <= bid → seller initiated
#   if between → use tick rule
```

**6. Tick-by-Tick Table**
```python
# Show last N trades with:
# Time, Price, Volume, Bid, Ask, Spread, Side (BUY/SELL), Cumulative Vol
# Color: green rows for buys, red for sells
# Side determination: Lee-Ready or simple tick rule
```

### Data Loading

```python
@st.cache_data(ttl=300)
def load_tick_data(date_str: str, symbol: str = None) -> pd.DataFrame:
    """Load JSONL tick data for a given date."""
    # Try cloud first, then local
    cloud_path = Path(f"/mnt/e/psxdata/tick_logs_cloud/{date_str}.jsonl")
    local_path = Path(f"/mnt/e/psxdata/tick_logs/{date_str}.jsonl")
    
    path = cloud_path if cloud_path.exists() else local_path
    if not path.exists():
        return pd.DataFrame()
    
    records = []
    with open(path) as f:
        for line in f:
            try:
                rec = json.loads(line)
                if symbol and rec.get("symbol") != symbol:
                    continue
                records.append(rec)
            except:
                continue
    
    df = pd.DataFrame(records)
    if df.empty:
        return df
    
    # Parse timestamps
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Karachi")
    df = df.sort_values("datetime")
    
    return df
```

### Important Notes for Microstructure Page

1. **JSONL volume is CUMULATIVE per symbol** — you need to diff consecutive 
   ticks to get per-trade volume. Check the `trades` field too.

2. **bid/ask may be 0.0** for some ticks (pre-market, auction periods) — 
   filter these out before computing spreads.

3. **File size is ~215MB/day** — loading entire file is slow. 
   Filter by symbol during read (line-by-line) not after.

4. **Available dates** depend on when cloud collection started. 
   Show a date picker with only available dates.

5. **VPIN may already exist** in `engine/` — check and reuse.

---

## Page 2: Derivatives Analytics

### Location
`src/pakfindata/ui/page_views/derivatives.py`

### Data Sources
- **Futures OI:** `/mnt/e/psxdata/downloads/daily/{date}/futures/`
  - `futures_oi_dfc.xls` — Deliverable Futures Contract OI
  - `futures_oi_csf.xls` — Cash Settled Futures OI
- **SIF OI:** `/mnt/e/psxdata/downloads/daily/{date}/sif/`
  - `sif_open_interest.csv` — Single Stock/Index Futures
- **Futures prices:** From tick JSONL where `market == "FUT"`
- **Spot prices:** From tick JSONL where `market == "REG"` or DPS EOD

### Page Layout

```
┌─────────────────────────────────────────────────────────────┐
│  DERIVATIVES ANALYTICS                       [Date picker]  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐       │
│  │ Total OI     │ │ OI Change    │ │ Put/Call      │       │
│  │ 245.6M shs   │ │ +12.3M (+5%) │ │ N/A (no opts) │       │
│  └──────────────┘ └──────────────┘ └──────────────┘       │
│                                                             │
│  ═══ FUTURES OPEN INTEREST ═══                              │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Top 20 by OI — Deliverable Futures (DFC)           │   │
│  │  Symbol  | OI Contracts | OI Value | OI Δ | Spot  │   │
│  │  HUBC    | 1,234,000    | 232.5M   | +5%  | 188   │   │
│  │  OGDC    | 987,000      | 97.1M    | -3%  | 98    │   │
│  │  ...                                                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  OI vs Price (selected symbol)          [HUBC ▼]    │   │
│  │                                                      │   │
│  │  Price ─────────────────── (left axis)              │   │
│  │  OI    ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ (right axis, bars)       │   │
│  │                                                      │   │
│  │  Date range: last 30 trading days                   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ═══ BASIS ANALYSIS ═══                                     │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Futures Basis (Premium/Discount to Spot)            │   │
│  │                                                      │   │
│  │  Symbol | Spot   | Futures | Basis | Basis% | Days  │   │
│  │  HUBC   | 188.38 | 189.50  | +1.12 | +0.59% | 15   │   │
│  │  OGDC   | 98.50  | 97.80   | -0.70 | -0.71% | 15   │   │
│  │  (premium = bullish positioning, discount = bearish) │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ═══ OI ANALYSIS ═══                                        │
│                                                             │
│  ┌──────────────────────┐  ┌──────────────────────────┐   │
│  │  OI Buildup/Unwind   │  │  OI Concentration        │   │
│  │                       │  │  (top 10 by OI share)    │   │
│  │  ↑OI + ↑Price = Long │  │                          │   │
│  │  ↑OI + ↓Price = Short│  │  HUBC ████████ 15%       │   │
│  │  ↓OI + ↑Price = Cover│  │  OGDC ██████ 12%         │   │
│  │  ↓OI + ↓Price = Liq  │  │  PPL  █████ 10%          │   │
│  └──────────────────────┘  └──────────────────────────┘   │
│                                                             │
│  ═══ ROLLOVER TRACKER ═══                                   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Current month expiry: Mar 27, 2026 (9 days)        │   │
│  │  Next month: Apr 2026                                │   │
│  │                                                      │   │
│  │  Symbol | Current OI | Next OI | Rolled% | Basis    │   │
│  │  HUBC   | 900K       | 334K    | 27%     | +0.59%   │   │
│  │  ...                                                 │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Analytics to Compute

**1. Open Interest Table**
```python
# Load DFC + CSF XLS files
# Parse: symbol, OI (contracts), OI (value PKR)
# Compare with previous day's OI → compute change
# Sort by OI descending
# Show top 20
```

**2. OI vs Price Chart (dual axis)**
```python
# For selected symbol, load last N trading days:
#   - Daily OI from futures files (need backfill of downloads)
#   - Daily close price from psx.sqlite or DPS EOD
# Left axis: price line
# Right axis: OI bars
# Highlight days where OI and price move in same/opposite direction
```

**3. Futures Basis Analysis**
```python
# basis = futures_price - spot_price
# basis_pct = basis / spot_price * 100
# annualized_basis = basis_pct * (365 / days_to_expiry)
#
# Data sources:
#   spot_price: from DPS EOD or tick JSONL (REG market)
#   futures_price: from tick JSONL (FUT market) or DPS EOD
#   days_to_expiry: PSX futures expire last Thursday of month
#
# Interpretation:
#   Premium (basis > 0) = market bullish on this stock
#   Discount (basis < 0) = market bearish
#   High annualized basis = strong conviction
```

**4. OI Buildup/Unwind Matrix**
```python
# Classic 2x2 matrix:
#   Price ↑ + OI ↑ = Long buildup (bullish)
#   Price ↓ + OI ↑ = Short buildup (bearish)  
#   Price ↑ + OI ↓ = Short covering (weak bullish)
#   Price ↓ + OI ↓ = Long liquidation (weak bearish)
#
# For each symbol: classify today's action into one of 4 quadrants
# Show as color-coded table or scatter plot
```

**5. OI Concentration**
```python
# Total OI across all futures symbols
# Each symbol's share of total OI
# Top 10 as horizontal bar chart
# Track changes: which symbols are gaining/losing OI share
```

**6. Rollover Tracker**
```python
# PSX futures expiry: last Thursday of each month
# Calculate days to current month expiry
# For symbols with both current + next month contracts:
#   rolled_pct = next_month_OI / (current_OI + next_OI) * 100
# Show rollover progress
# Highlight symbols that haven't rolled (squeeze risk)
```

### Data Loading

```python
@st.cache_data(ttl=300)
def load_futures_oi(date_str: str) -> pd.DataFrame:
    """Load futures open interest from downloaded XLS files."""
    dfc_path = find_file(f"/mnt/e/psxdata/downloads/daily/{date_str}/futures/", "dfc")
    csf_path = find_file(f"/mnt/e/psxdata/downloads/daily/{date_str}/futures/", "csf")
    
    frames = []
    for path, contract_type in [(dfc_path, "DFC"), (csf_path, "CSF")]:
        if path and path.exists():
            # Parse XLS — format discovered in Phase 1 Step 1
            df = parse_oi_xls(path)
            df["contract_type"] = contract_type
            frames.append(df)
    
    return pd.concat(frames) if frames else pd.DataFrame()


@st.cache_data(ttl=300)
def load_sif_oi(date_str: str) -> pd.DataFrame:
    """Load SIF open interest from downloaded CSV."""
    sif_path = find_file(f"/mnt/e/psxdata/downloads/daily/{date_str}/sif/", "open_interest")
    if sif_path and sif_path.exists():
        return pd.read_csv(sif_path)
    return pd.DataFrame()


def get_expiry_date(year: int, month: int) -> datetime:
    """Calculate PSX futures expiry (last Thursday of month)."""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    dt = datetime(year, month, last_day)
    while dt.weekday() != 3:  # Thursday
        dt -= timedelta(days=1)
    return dt
```

### Important Notes for Derivatives Page

1. **OI data requires backfill** — you need multiple days of downloads 
   to show OI trends. Run the downloads scraper for past 30 days first:
   ```bash
   python -m pakfindata.sources.psx_downloads backfill 2026-02-15 2026-03-18
   ```

2. **XLS parsing** — use `openpyxl` or `xlrd` depending on format.
   Install: `pip install openpyxl xlrd`

3. **Futures symbols** — PSX futures tickers may differ from spot.
   Common pattern: spot=HUBC, future=HUBCF or HUBC-MAR26.
   Check the OI XLS files for exact naming.

4. **No options on PSX** — Pakistan doesn't have listed equity options.
   Skip any put/call related analytics.

5. **CSF (Cash Settled Futures)** were discontinued — the file may be 
   empty or not exist for recent dates. Handle gracefully.

6. **Basis calculation needs matching** — ensure you're comparing 
   the correct futures contract month with spot price.

---

## Page Registration

Add both pages to the Streamlit app navigation:

```bash
# Check how pages are registered
grep -A5 "page_views\|navigation\|sidebar" ~/pakfindata/src/pakfindata/ui/app.py | head -30
```

Add to the page list in the appropriate section:
```python
# Under "Analytics" or "Advanced" section:
"Microstructure": page_views.microstructure,
"Derivatives": page_views.derivatives,
```

## VERIFY

```bash
# Check both pages render without errors
cd ~/pakfindata
streamlit run src/pakfindata/ui/app.py

# Navigate to:
# 1. Microstructure → select HUBC → select today → see spread chart + VPIN
# 2. Derivatives → see OI table → select symbol → see OI vs Price
```
