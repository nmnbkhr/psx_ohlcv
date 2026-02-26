# Claude Code Prompt: Integrate FX Microservice Client into PSX OHLCV

## Context

I have two independent projects:

1. **PSX OHLCV** — my main Pakistan stock market app (FastAPI + Streamlit + SQLite)
2. **FX Trading Module** — a standalone microservice running on `http://localhost:8100` that provides FX rates, KIBOR, carry trade signals, premium spread, SBP intervention data, and FX-equity regime signals

The FX module is already built and running. I need to integrate it into PSX OHLCV as a **consumer** — PSX OHLCV calls the FX service via HTTP, never crashes if FX is down.

## What to do

### Prerequisite

`sources/fx_client.py` is already in place (copied manually). It's a thin HTTP client (~200 lines) that talks to the FX microservice at `http://localhost:8100`. Every method returns `None` or `{}` on failure — never raises exceptions. Methods: `is_healthy()`, `get_snapshot()`, `get_rates()`, `get_kibor()`, `get_regime()`, `get_signals_report()`, `get_intervention()`.

### Step 0: Add history endpoint to FX microservice (separate project)

The FX service at `/mnt/e/projects/fx-trading-module/` needs a new endpoint so PSX OHLCV can backfill historical rates. 

**File:** `api/service.py` in the FX module — add this endpoint:

```python
@app.get("/rates/history")
async def rates_history(
    from_date: str = Query(..., alias="from", description="Start date YYYY-MM-DD"),
    to_date: str = Query(None, description="End date YYYY-MM-DD, defaults to today"),
):
    """Return all interbank rates between two dates from local SQLite."""
    import sqlite3
    from datetime import date as dt
    
    if to_date is None:
        to_date = dt.today().isoformat()
    
    db_path = "data/fx_data.db"  # adjust to actual path from settings
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    rows = conn.execute("""
        SELECT date, pair as currency, buying, selling
        FROM interbank_rates
        WHERE date >= ? AND date <= ?
        ORDER BY date, pair
    """, (from_date, to_date)).fetchall()
    conn.close()
    
    return {
        "rates": [dict(r) for r in rows],
        "count": len(rows),
        "from": from_date,
        "to": to_date,
    }
```

**Important:** Check the actual table name and column names in the FX service's SQLite. Run `sqlite3 data/fx_data.db ".schema"` to verify. The table might be `rates`, `sbp_rates`, or `interbank_rates` — adjust the query accordingly.

Also add a `_get` helper to `fx_client.py` if it doesn't already have one (it should — it's used internally by other methods). If missing:
```python
def _get(self, path):
    """Raw GET request to FX service."""
    try:
        resp = self.session.get(f"{self.base_url}{path}", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None
```

### Step 1: Add FX data to the main Streamlit dashboard sidebar

**File:** `ui/pages/dashboard.py` (or wherever the main dashboard sidebar is)

Add FX rates and KIBOR to the sidebar so traders see macro context alongside equity data:

```python
from sources.fx_client import FXClient

_fx = FXClient("http://localhost:8100")

# In the sidebar section:
if _fx.is_healthy():
    snap = _fx.get_snapshot()
    if snap:
        rates = snap.get("rates", {})
        kibor = snap.get("kibor", {})
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 💱 FX & Rates")
        
        # USD/PKR with bid/offer
        usd = rates.get("USD/PKR", {})
        if usd:
            mid = (usd.get("buying", 0) + usd.get("selling", 0)) / 2
            st.sidebar.metric("USD/PKR", f"{mid:.2f}", help="SBP Interbank M2M Rate")
        
        # KIBOR 6M
        k6m = kibor.get("6M", {})
        if k6m:
            offer = k6m.get("offer", 0)
            st.sidebar.metric("KIBOR 6M", f"{offer:.2f}%", help="KIBOR 6-Month Offer Rate")
        
        # SBP Policy Rate
        policy = kibor.get("POLICY", {})
        if policy:
            st.sidebar.metric("Policy Rate", f"{policy.get('bid', 0):.1f}%")
else:
    # FX service not running — show nothing, don't crash
    pass
```

### Step 2: Add API endpoint for FX data passthrough

**File:** `api/routers/` — create a new `fx.py` router OR add to existing rates router

```python
from fastapi import APIRouter, HTTPException
from sources.fx_client import FXClient

router = APIRouter(prefix="/fx", tags=["fx"])
_fx = FXClient("http://localhost:8100")

@router.get("/snapshot")
async def fx_snapshot():
    """Get FX snapshot (rates + KIBOR + signals) from FX microservice."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    data = _fx.get_snapshot()
    if not data:
        raise HTTPException(502, "FX service returned empty response")
    return data

@router.get("/regime")
async def fx_regime():
    """Get FX-equity regime signal with sector exposure guide."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    return _fx.get_regime() or {}

@router.get("/intervention")
async def fx_intervention():
    """Get SBP intervention report (FXIM published data + statistical)."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    return _fx.get_intervention() or {}

@router.get("/health")
async def fx_health():
    """Check if FX microservice is reachable."""
    return {"fx_service": "up" if _fx.is_healthy() else "down"}

@router.post("/backfill")
async def fx_backfill(from_date: str = "2024-01-01", to_date: str = None):
    """Adhoc backfill — pull historical FX rates from FX service into local DB.
    Call this once on first setup, or anytime to fill date gaps."""
    if not _fx.is_healthy():
        raise HTTPException(503, "FX service unavailable")
    from sources.fx_sync import backfill_fx_history
    # Get db_conn from your app's DB connection (adjust import as needed)
    from db import get_connection  # or however PSX OHLCV gets its DB conn
    conn = get_connection()
    result = backfill_fx_history(conn, from_date, to_date)
    return result
```

Then register in `api/main.py`:
```python
from api.routers.fx import router as fx_router
app.include_router(fx_router)
```

### Step 3: Create FX Dashboard page

**File:** `ui/pages/fx_dashboard.py` — new Streamlit page

This is a dedicated page that shows all FX data from the microservice. Build it with these sections:

1. **Header row:** USD/PKR, EUR/PKR, GBP/PKR, AED/PKR, SAR/PKR as metric cards
2. **KIBOR table:** All tenors with bid/offer/mid
3. **SBP Intervention:** Latest FXIM data (net USD purchases/sales per month) + stance
4. **Premium Spread:** Interbank vs open market gap (stress indicator)
5. **Carry Trade Signal:** KIBOR vs foreign rate differential
6. **FX-Equity Regime:** Current regime + sector exposure heatmap

Use `_fx.get_snapshot()` for rates/KIBOR, `_fx.get_signals_report()` for signals, `_fx.get_intervention()` for FXIM data, `_fx.get_regime()` for sector exposure.

Always wrap every FX call in:
```python
if _fx.is_healthy():
    data = _fx.get_something()
    if data:
        # render
    else:
        st.warning("FX data temporarily unavailable")
else:
    st.error("FX microservice is not running. Start it: uvicorn api.service:app --port 8100")
```

### Step 4: Persist FX data into existing PSX OHLCV database tables

PSX OHLCV already has FX/rates tables in `db.py`. The FX microservice should feed data into these tables so the equity app has historical FX data locally, even when the FX service is down.

**Table mapping — FX service → existing PSX OHLCV tables:**

```
sbp_fx_interbank (date, currency, buying, selling, mid)
  ← fx_client.get_rates() → interbank M2M rates for all 35 currencies

sbp_fx_open_market (date, currency, buying, selling)  
  ← fx_client.get_rates() → open market rates

forex_open_market (date, currency, buying, selling, source)
  ← fx_client.get_snapshot() → premium spread open market side

kibor_rates (date, tenor, bid, offer, mid)
  ← fx_client.get_kibor() → all KIBOR tenors

policy_rates (date, rate)
  ← fx_client.get_kibor() → SBP policy rate from KIBOR response
```

**File:** Create `sources/fx_sync.py` — a sync job that pulls from FX service and upserts into local DB:

```python
"""Sync FX microservice data into local PSX OHLCV database tables."""

from datetime import date, timedelta
from sources.fx_client import FXClient

_fx = FXClient("http://localhost:8100")


def sync_fx_rates(db_conn):
    """Pull latest FX rates from microservice and store locally.
    Also backfills any missing dates from FX service history.
    """
    if not _fx.is_healthy():
        return {"status": "skipped", "reason": "FX service unavailable"}
    
    today = date.today().isoformat()
    stored = 0
    backfilled = 0
    
    # --- 1. Today's snapshot (daily) ---
    snapshot = _fx.get_snapshot()
    if snapshot and "rates" in snapshot:
        for pair, data in snapshot["rates"].items():
            currency = pair.replace("/PKR", "")
            buying = data.get("buying")
            selling = data.get("selling")
            mid = (buying + selling) / 2 if buying and selling else None
            db_conn.execute("""
                INSERT OR REPLACE INTO sbp_fx_interbank (date, currency, buying, selling, mid)
                VALUES (?, ?, ?, ?, ?)
            """, (today, currency, buying, selling, mid))
            stored += 1
    
    # KIBOR + policy rate
    kibor = snapshot.get("kibor", {}) if snapshot else {}
    for tenor, data in kibor.items():
        if tenor == "POLICY":
            db_conn.execute("""
                INSERT OR REPLACE INTO policy_rates (date, rate)
                VALUES (?, ?)
            """, (today, data.get("bid")))
        else:
            bid = data.get("bid")
            offer = data.get("offer")
            mid = (bid + offer) / 2 if bid and offer else None
            db_conn.execute("""
                INSERT OR REPLACE INTO kibor_rates (date, tenor, bid, offer, mid)
                VALUES (?, ?, ?, ?, ?)
            """, (today, tenor, bid, offer, mid))
    
    # --- 2. Backfill missing dates ---
    backfilled = _backfill_missing_dates(db_conn)
    
    db_conn.commit()
    return {"status": "ok", "rates_stored": stored, "backfilled": backfilled, "date": today}


def _backfill_missing_dates(db_conn):
    """Find date gaps in local DB and fill from FX service history."""
    if not _fx.is_healthy():
        return 0
    
    # Find the earliest and latest date we have locally
    row = db_conn.execute("""
        SELECT MIN(date), MAX(date) FROM sbp_fx_interbank
    """).fetchone()
    
    if not row or not row[0]:
        # Empty table — do full backfill from FX service
        from_date = "2024-01-01"
    else:
        from_date = row[0]
    
    to_date = date.today().isoformat()
    
    # Get history from FX service
    history = _fx._get(f"/rates/history?from={from_date}&to={to_date}")
    if not history or "rates" not in history:
        return 0
    
    backfilled = 0
    for record in history["rates"]:
        dt = record.get("date")
        currency = record.get("currency")
        buying = record.get("buying")
        selling = record.get("selling")
        mid = (buying + selling) / 2 if buying and selling else None
        
        # INSERT OR IGNORE — only fills gaps, doesn't overwrite existing
        result = db_conn.execute("""
            INSERT OR IGNORE INTO sbp_fx_interbank (date, currency, buying, selling, mid)
            VALUES (?, ?, ?, ?, ?)
        """, (dt, currency, buying, selling, mid))
        if result.rowcount > 0:
            backfilled += 1
    
    return backfilled


def backfill_fx_history(db_conn, from_date="2024-01-01", to_date=None):
    """Manual/adhoc backfill — call this directly when needed.
    
    Usage:
        from sources.fx_sync import backfill_fx_history
        backfill_fx_history(db_conn)                          # full backfill
        backfill_fx_history(db_conn, "2025-06-01", "2025-06-30")  # specific range
    """
    if not _fx.is_healthy():
        return {"status": "error", "reason": "FX service unavailable"}
    
    if to_date is None:
        to_date = date.today().isoformat()
    
    history = _fx._get(f"/rates/history?from={from_date}&to={to_date}")
    if not history or "rates" not in history:
        return {"status": "error", "reason": "No history returned from FX service"}
    
    inserted = 0
    updated = 0
    for record in history["rates"]:
        dt = record.get("date")
        currency = record.get("currency")
        buying = record.get("buying")
        selling = record.get("selling")
        mid = (buying + selling) / 2 if buying and selling else None
        
        # Use REPLACE to overwrite — adhoc backfill should fix bad data too
        existing = db_conn.execute("""
            SELECT 1 FROM sbp_fx_interbank WHERE date=? AND currency=?
        """, (dt, currency)).fetchone()
        
        db_conn.execute("""
            INSERT OR REPLACE INTO sbp_fx_interbank (date, currency, buying, selling, mid)
            VALUES (?, ?, ?, ?, ?)
        """, (dt, currency, buying, selling, mid))
        
        if existing:
            updated += 1
        else:
            inserted += 1
    
    db_conn.commit()
    return {"status": "ok", "inserted": inserted, "updated": updated, "from": from_date, "to": to_date}
```

**Important:** Check the actual column names in `db.py` before writing — the schema above is based on what was found in the codebase review. Adjust column names if they differ. Look for the `CREATE TABLE` statements for: `sbp_fx_interbank`, `sbp_fx_open_market`, `forex_open_market`, `kibor_rates`, `policy_rates`.

**Then wire it into the existing scrape scheduler** (wherever daily scrapes are triggered) so FX data syncs alongside equity data:

```python
from sources.fx_sync import sync_fx_rates

# In the daily sync/scrape routine:
result = sync_fx_rates(db_conn)
logger.info(f"FX sync: {result}")
```

## Critical rules

1. **NEVER import anything from the FX module directly** — only use `sources/fx_client.py` HTTP client
2. **NEVER crash if FX service is down** — every call wrapped in `is_healthy()` + null checks
3. **NEVER modify existing PSX OHLCV logic** — FX is purely additive
4. **FXClient is instantiated once per module** as a module-level singleton (it has connection pooling)
5. **`sources/fx_client.py` is already in place** — don't rewrite or modify it
6. **Before writing `fx_sync.py`, inspect `db.py`** — run `grep -A5 "CREATE TABLE.*sbp_fx\|CREATE TABLE.*kibor\|CREATE TABLE.*policy_rates\|CREATE TABLE.*forex_open" db.py` to confirm exact column names and PRIMARY KEYs

## FX Service API reference (running on localhost:8100)

```
GET /health                → {"status": "ok"}
GET /snapshot              → {rates: {}, kibor: {}, signals: {}}
GET /rates/latest          → [{pair, date, source, buying, selling}, ...]
GET /rates/interbank       → interbank rates only
GET /rates/history?from=YYYY-MM-DD&to=YYYY-MM-DD → {rates: [{date, currency, buying, selling}], count}
GET /kibor                 → [{tenor, bid, offer, mid}, ...]
GET /signals/report        → {carry: {}, premium_spread: {}, intervention: {}, regime: {}}
GET /signals/intervention  → {signal: {}, fxim: {history: [...]}, statistical: {}}
GET /signals/regime        → {regime, equity_signal, sector_bias, sector_exposures: {}}
GET /signals/carry         → carry trade analysis
GET /signals/premium       → premium spread (interbank vs open market)
```

## Files to create/modify summary

| Action | File | Project | What |
|--------|------|---------|------|
| MODIFY | `api/service.py` | FX module | Add `/rates/history` endpoint |
| MODIFY | `ui/pages/dashboard.py` | PSX OHLCV | Add FX metrics to sidebar |
| CREATE | `api/routers/fx.py` | PSX OHLCV | FX passthrough + backfill endpoints |
| MODIFY | `api/main.py` | PSX OHLCV | Register FX router |
| CREATE | `ui/pages/fx_dashboard.py` | PSX OHLCV | Dedicated FX dashboard page |
| CREATE | `sources/fx_sync.py` | PSX OHLCV | Sync + backfill FX data into local DB |

## Test

```bash
# 1. Make sure FX service is running
curl http://localhost:8100/health

# 2. Test FX history endpoint (new)
curl "http://localhost:8100/rates/history?from=2025-01-01&to=2025-01-31"

# 3. Test PSX OHLCV FX passthrough
curl http://localhost:8000/fx/health
curl http://localhost:8000/fx/snapshot

# 4. Trigger one-time backfill of all historical rates
curl -X POST "http://localhost:8000/fx/backfill?from_date=2024-01-01"

# 5. Verify data landed in local DB
sqlite3 psx_ohlcv.db "SELECT COUNT(*), MIN(date), MAX(date) FROM sbp_fx_interbank"

# 6. Run existing tests — nothing should break
pytest tests/ --tb=short
```
