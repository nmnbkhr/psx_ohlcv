# Claude Code Prompt: Add Live Tick OHLCV Builder to PSX OHLCV App

## Context
This is the `psx_ohlcv` project — a Pakistan Stock Exchange data platform built with Streamlit + SQLite. The app already has pages for candlestick explorer, intraday trend, regular market, etc. under `src/psx_ohlcv/ui/`. The main DB layer is in `src/psx_ohlcv/db.py` (monolith ~8600 lines). The Streamlit entry is `src/psx_ohlcv/ui/app.py` (~11000 lines monolith).

## What to Build
Add a **Live Tick OHLCV Builder** feature — a new Streamlit page + backend collector that:

### Data Source
- Polls `https://dps.psx.com.pk/market-watch` endpoint every 5 seconds
- This endpoint returns JSON array of ALL ~600 listed symbols in one shot
- Each item has fields like: `symbol`, `current` (price), `ldcp` (prev close), `change`, `change_p` (change%), `vol` (cumulative volume), `high`, `low`, `open`, etc.
- Data updates roughly every 3 seconds on the server side

### Core Logic — Incremental OHLCV
1. **First poll**: For each symbol, the first price seen becomes OPEN. Set HIGH=price, LOW=price, CLOSE=price
2. **Each subsequent poll**: 
   - `HIGH = max(existing_HIGH, new_price)`
   - `LOW = min(existing_LOW, new_price)` 
   - `CLOSE = new_price` (always latest)
   - `VOLUME = cumulative_volume` from market-watch
3. **Dedup**: Skip symbols where price AND volume haven't changed since last snapshot
4. **Track tick count** per symbol (how many actual price changes detected)

### Database Changes
Add these tables to the existing SQLite DB (add schema in `db.py` or appropriate location):

```sql
CREATE TABLE IF NOT EXISTS tick_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    price REAL NOT NULL,
    change REAL DEFAULT 0,
    change_pct REAL DEFAULT 0,
    cumulative_volume INTEGER DEFAULT 0,
    mw_high REAL DEFAULT 0,
    mw_low REAL DEFAULT 0,
    mw_open REAL DEFAULT 0,
    UNIQUE(symbol, timestamp, price)
);
CREATE INDEX IF NOT EXISTS idx_tick_symbol_ts ON tick_data(symbol, timestamp);

CREATE TABLE IF NOT EXISTS tick_ohlcv (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER DEFAULT 0,
    tick_count INTEGER DEFAULT 0,
    first_tick_ts INTEGER,
    last_tick_ts INTEGER,
    source TEXT DEFAULT 'tick_collector',
    PRIMARY KEY(symbol, date)
);
```

Add DB functions: `insert_ticks_batch()`, `upsert_tick_ohlcv()`, `get_ticks_for_symbol_today()`, `get_tick_ohlcv_today()`, `get_tick_ohlcv_symbol()`, `promote_tick_ohlcv_to_eod()` (copies tick-built OHLCV into eod_data with source='tick_aggregation' — this solves the fake H/L problem), `cleanup_old_ticks(days=7)`.

### Collector Service
Create `src/psx_ohlcv/collectors/tick_collector.py`:
- `TickCollector` class with `start()` / `stop()` (runs in background daemon thread)
- In-memory state: `_last_snapshot` dict for dedup, `running_ohlcv` dict for live OHLCV, `tick_history` dict for per-symbol tick lists
- `_poll_once()` → fetch market-watch JSON, dedup, update running OHLCV, optionally persist to DB
- Expose: `get_running_ohlcv(symbol=None)`, `get_tick_history(symbol)`, `get_stats()`
- Use `st.session_state` integration OR standalone thread that Streamlit page reads from

### Streamlit Page
Create a new page (either in `app.py` or as `src/psx_ohlcv/ui/pages/live_ohlcv.py` — follow existing pattern):

**Controls Row:**
- Toggle: Auto-Poll (5s) — starts/stops continuous polling
- Button: Poll Once — single manual fetch
- Button: Save OHLCV to DB — persists current running OHLCV to `tick_ohlcv` table
- Button: Promote to EOD — copies tick OHLCV into `eod_data` table (real H/L!)
- Button: Reset — clears session state

**Status Metrics Row:**
- Poll count, Symbols tracked, Last poll time, Collection started time, Total ticks captured

**3 Tabs:**

**Tab 1: Symbol Deep View**
- Symbol dropdown (default to HBL if available, show all tracked symbols)
- Chart type toggle: Line + OHLCV Band / Tick Scatter
- OHLCV KPI cards: OPEN (first tick), HIGH (running max), LOW (running min), CLOSE (latest), VOLUME, TICK COUNT
- **Incremental Build Log Table** — this is the key feature:
  - Columns: Tick#, Time, Price, Action, O, H, L, C, Vol
  - Action shows: "→ SET as OPEN" for first tick, "↑ NEW HIGH (old→new)" when price exceeds HIGH, "↓ NEW LOW (old→new)" when price drops below LOW, "→ CLOSE updated" always
  - Show last 20 rows with expander for full history
- **Live Chart**: Price line with running HIGH/LOW as dotted band overlay, horizontal OPEN reference line, volume bars below (green up / red down)
- Button to save individual symbol's ticks to CSV

**Tab 2: Running OHLCV Table**
- Full table of ALL ~600 symbols with columns: Symbol, Open, High, Low, Close, Chg, Chg%, Spread (H-L), Volume, Ticks, First Time, Last Time
- Sort by dropdown (default: Chg%), order toggle
- Symbol search/filter
- Color code: green for positive change, red for negative
- Sorted by absolute change% descending by default

**Tab 3: Raw Market Watch**
- Show last raw JSON response (first 3 items in expander)
- Full raw DataFrame of last response

### CLI Command
Add to existing CLI (`cli.py`):
```
psxsync collect-ticks --interval 5
```
Starts the tick collector in foreground, Ctrl+C to stop. On stop, auto-saves OHLCV to DB.

### Auto-Poll in Streamlit
When auto-poll is enabled:
- Fetch market-watch
- Process ticks (dedup + update OHLCV)
- Update session state
- `time.sleep(5)` then `st.rerun()`

### Important Notes
- Follow the existing code style and patterns in the project
- Use the existing `fetcher.py` HTTP client if possible, or `requests` directly
- Match the existing Streamlit theming from `themes.py`
- The market-watch endpoint needs no auth
- Raw tick_data table will grow fast (~600 symbols × every 5s = ~7200 rows/min during market hours) — the `cleanup_old_ticks()` function should be called periodically or via CLI
- This feature solves a critical P0 issue: the current EOD data has FAKE high/low values (derived as max/min of open,close). Tick-collected OHLCV gives REAL high/low.

### Verify
```bash
# Schema created
python -c "from psx_ohlcv.db import get_db; db=get_db(); print('tick tables ok')"

# Collector imports
python -c "from psx_ohlcv.collectors.tick_collector import TickCollector; print('collector ok')"

# Streamlit starts without errors
streamlit run src/psx_ohlcv/ui/app.py --server.headless true 2>&1 | head -5

# CLI command registered
psxsync collect-ticks --help
```
