# Claude Code Prompt: Integrate FX Microservice Client into PSX OHLCV

## Context

I have two independent projects:

1. **PSX OHLCV** — my main Pakistan stock market app (FastAPI + Streamlit + SQLite)
2. **FX Trading Module** — a standalone microservice running on `http://localhost:8100` that provides FX rates, KIBOR, carry trade signals, premium spread, SBP intervention data, and FX-equity regime signals

The FX module is already built and running. I need to integrate it into PSX OHLCV as a **consumer** — PSX OHLCV calls the FX service via HTTP, never crashes if FX is down.

## What to do

### Prerequisite

`sources/fx_client.py` is already in place (copied manually). It's a thin HTTP client (~200 lines) that talks to the FX microservice at `http://localhost:8100`. Every method returns `None` or `{}` on failure — never raises exceptions. Methods: `is_healthy()`, `get_snapshot()`, `get_rates()`, `get_kibor()`, `get_regime()`, `get_signals_report()`, `get_intervention()`.

### Step 1: Add FX overlay to sector factor analysis

**File:** `qsresearch/sector_analysis.py`

The sector analysis already computes Value, Momentum, Quality, LowVol factor scores per sector. Add an FX exposure dimension using the regime signal from the FX service.

```python
from sources.fx_client import FXClient

_fx = FXClient("http://localhost:8100")

# FX exposure mapping — how each PSX sector responds to PKR moves
# Positive = benefits from weak PKR, Negative = hurt by weak PKR
SECTOR_FX_SENSITIVITY = {
    "Textile Composite": 0.8,
    "Textile Spinning": 0.7,
    "Textile Weaving": 0.7,
    "Technology & Communication": 0.6,   # IT exports
    "Leather & Tanneries": 0.5,
    "Sugar & Allied": 0.3,
    "Fertilizer": 0.1,
    "Oil & Gas Exploration": 0.0,        # USD-denominated revenue offsets
    "Oil & Gas Marketing": -0.3,         # Import costs
    "Cement": -0.2,
    "Chemical": -0.4,                    # Import-dependent
    "Automobile Assembler": -0.6,        # CKD imports
    "Automobile Parts": -0.5,
    "Pharmaceutical": -0.5,              # API imports
    "Engineering": -0.3,
    "Power Generation": -0.4,            # Fuel imports
    "Food & Personal Care": -0.2,
    "Insurance": 0.0,
    "Inv. Banks / Securities": 0.0,
    "Commercial Banks": 0.1,             # FX trading gains
    "Modarabas": 0.0,
    "Miscellaneous": 0.0,
}
```

In the function that builds the sector factor response, add:

```python
def get_sector_factors(self, ...):
    # ... existing factor computation ...
    
    # FX regime overlay (non-blocking)
    fx_regime = None
    if _fx.is_healthy():
        fx_regime = _fx.get_regime()
    
    for sector in sectors:
        name = sector["name"]
        sensitivity = SECTOR_FX_SENSITIVITY.get(name, 0.0)
        sector["fx_sensitivity"] = sensitivity
        
        if fx_regime:
            regime_type = fx_regime.get("regime", "stable")
            # Adjust composite score based on FX regime
            if regime_type == "pkr_weakening" and sensitivity > 0:
                sector["fx_boost"] = round(sensitivity * 0.1, 3)  # Mild tailwind
            elif regime_type == "pkr_weakening" and sensitivity < 0:
                sector["fx_boost"] = round(sensitivity * 0.1, 3)  # Mild headwind
            elif regime_type == "pkr_strengthening" and sensitivity > 0:
                sector["fx_boost"] = round(-sensitivity * 0.1, 3)  # Reverse
            elif regime_type == "pkr_strengthening" and sensitivity < 0:
                sector["fx_boost"] = round(-sensitivity * 0.1, 3)
            else:
                sector["fx_boost"] = 0.0
        else:
            sector["fx_boost"] = None  # FX service unavailable
    
    # Add FX summary to response
    response["fx_regime"] = {
        "available": fx_regime is not None,
        "regime": fx_regime.get("regime") if fx_regime else None,
        "usd_pkr": fx_regime.get("metrics", {}).get("last_close") if fx_regime else None,
    }
    
    return response
```

**Do NOT change:** Existing factor math, Value/Momentum/Quality/LowVol calculations, backend adapters, or existing tests.

### Step 2: Add FX data to the main Streamlit dashboard sidebar

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

### Step 3: Add API endpoint for FX data passthrough

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
```

Then register in `api/main.py`:
```python
from api.routers.fx import router as fx_router
app.include_router(fx_router)
```

### Step 4: Create FX Dashboard page

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

## Critical rules

1. **NEVER import anything from the FX module directly** — only use `sources/fx_client.py` HTTP client
2. **NEVER crash if FX service is down** — every call wrapped in `is_healthy()` + null checks
3. **NEVER modify existing factor math** — FX is an overlay/addition, not a replacement
4. **FXClient is instantiated once per module** as a module-level singleton (it has connection pooling)
5. **`sources/fx_client.py` is already in place** — don't rewrite or modify it

## FX Service API reference (running on localhost:8100)

```
GET /health                → {"status": "ok"}
GET /snapshot              → {rates: {}, kibor: {}, signals: {}}
GET /rates/latest          → [{pair, date, source, buying, selling}, ...]
GET /rates/interbank       → interbank rates only
GET /kibor                 → [{tenor, bid, offer, mid}, ...]
GET /signals/report        → {carry: {}, premium_spread: {}, intervention: {}, regime: {}}
GET /signals/intervention  → {signal: {}, fxim: {history: [...]}, statistical: {}}
GET /signals/regime        → {regime, equity_signal, sector_bias, sector_exposures: {}}
GET /signals/carry         → carry trade analysis
GET /signals/premium       → premium spread (interbank vs open market)
```

## Files to create/modify summary

| Action | File | What |
|--------|------|------|
| MODIFY | `qsresearch/sector_analysis.py` | Add FX sensitivity + regime overlay |
| MODIFY | `ui/pages/dashboard.py` | Add FX metrics to sidebar |
| CREATE | `api/routers/fx.py` | FX passthrough API endpoints |
| MODIFY | `api/main.py` | Register FX router |
| CREATE | `ui/pages/fx_dashboard.py` | Dedicated FX dashboard page |

## Test

```bash
# 1. Make sure FX service is running
curl http://localhost:8100/health

# 2. Test PSX OHLCV FX passthrough
curl http://localhost:8000/fx/health
curl http://localhost:8000/fx/snapshot

# 3. Check sector factors include FX overlay
curl http://localhost:8000/qsresearch/sector-factors

# 4. Run existing tests — nothing should break
pytest tests/ --tb=short
```
