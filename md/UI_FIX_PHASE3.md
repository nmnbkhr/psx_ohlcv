# Claude Code Prompt: UI Fix Phase 3 — Performance

## Context

UI audit found 39 of 50 pages have ZERO caching. Every page re-queries the DB 
on every rerun (which happens on every user interaction in Streamlit). 
Connection pooling doesn't exist — each call creates a new sqlite3 connection.

## Fix 1: Add connection caching

Find `get_connection()` in app.py (or helpers.py). Replace with a cached version:

```python
@st.cache_resource
def get_cached_connection():
    """Singleton DB connection cached across reruns."""
    import sqlite3
    from pakfindata.config import DATA_ROOT
    db_path = DATA_ROOT / "psx.sqlite"
    con = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL") 
    return con
```

Then update all pages that call `get_connection()` to use `get_cached_connection()`.
If pages import get_connection from app or helpers, update the import.

Alternatively, keep `get_connection()` as the function name but make it return 
the cached connection internally.

**IMPORTANT:** SQLite with `check_same_thread=False` is safe for READ-ONLY 
operations (which is what all UI pages do). Write operations should still 
use their own connections.

## Fix 2: Add @st.cache_data to heavy pages

For each page below, find the main data-loading function(s) and wrap with caching.

### Priority pages (heaviest queries):

**fixed_income.py** (2,496 lines, no caching):
```python
@st.cache_data(ttl=300)  # 5 min cache
def load_fi_data():
    # existing query code
```

**fund_explorer.py** (2,038 lines, only 2 cached functions):
Add caching to the remaining uncached data loaders.

**intraday.py** (1,766 lines):
```python
@st.cache_data(ttl=60)  # 1 min — more recent data
def load_intraday_data(symbol, date):
    # existing query
```

**pmex.py** (1,536 lines):
```python
@st.cache_data(ttl=300)
def load_pmex_data():
    # existing query
```

**company_deep.py**:
```python
@st.cache_data(ttl=600)  # 10 min — company data changes slowly
def load_company_data(symbol):
    # existing query
```

**fx_dashboard.py**:
```python
@st.cache_data(ttl=300)
def load_fx_data():
    # existing query
```

**alm_dashboard.py**:
```python
@st.cache_data(ttl=300)
def load_alm_data():
    # existing query
```

**market_summary.py**:
```python
@st.cache_data(ttl=120)  # 2 min
def load_market_summary():
    # existing query
```

### Rules for adding cache:

1. **Only cache data-loading functions** — not rendering functions
2. **TTL guidelines:**
   - Live data pages (live_ticker, live_market): DON'T cache (they need fresh data)
   - Market data (intraday, EOD): 60-120 seconds
   - Reference data (company info, fund metadata): 300-600 seconds
   - Static data (FI instruments, historical): 600+ seconds
3. **Cache key must include all parameters** — if function takes a symbol arg,
   the cache is per-symbol automatically
4. **Don't cache functions that write to DB** — only readers
5. **Add `show_spinner=False`** to avoid UI flicker:
   ```python
   @st.cache_data(ttl=300, show_spinner=False)
   ```

## Fix 3: Reduce live_ticker.py refresh rate

File: `src/pakfindata/ui/page_views/live_ticker.py`

Current: 2-3 second refresh interval (st_autorefresh)
Change to: 5 seconds

```python
st_autorefresh(interval=5000, limit=None, key="ticker_refresh")
```

Also for live_indices.py if it has a similar aggressive refresh.

2-3 seconds causes excessive reruns and DB queries. 5 seconds is still 
responsive enough for market data display.

## Fix 4: Deduplicate app.py ↔ helpers.py

The audit found 600+ duplicated lines between app.py and helpers.py.

1. Read both files and identify identical or near-identical functions
2. Keep the canonical version in helpers.py
3. In app.py, import from helpers.py instead of duplicating
4. Make sure all pages that import from app.py still work (check imports)

```bash
# Find what's imported from app vs helpers
grep -rn "from.*app import\|from.*helpers import" \
  src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | head -20
```

## VERIFY

```bash
# Count cached pages BEFORE
echo "BEFORE — pages with caching:"
grep -rln "cache_data\|cache_resource" src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

# AFTER — should be 20+
echo "AFTER — pages with caching:"
grep -rln "cache_data\|cache_resource" src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

# Check connection caching exists
grep -n "cache_resource\|get_cached_connection" src/pakfindata/ui/app.py src/pakfindata/ui/helpers.py

# Check live_ticker refresh interval
grep -n "autorefresh\|interval" src/pakfindata/ui/page_views/live_ticker.py

# Run app — no import errors
cd ~/psx_ohlcv && python -c "from pakfindata.ui.app import *; print('OK')" 2>&1
```
