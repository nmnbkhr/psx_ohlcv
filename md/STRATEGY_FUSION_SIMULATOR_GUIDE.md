# Strategy Fusion Simulator — Complete Technical Guide

## What It Is

The Strategy Fusion Simulator is the **capstone feature** of pakfindata. It orchestrates all 14 strategy engines into a single, unified BUY/SELL/HOLD decision with confidence scoring, virtual portfolio tracking, and real-time visualization.

Think of it as a **quantitative trading cockpit** — every strategy runs in parallel, votes are weighted and fused, vetoes are applied, and the result drives a virtual portfolio with stop-loss, take-profit, and position sizing.

---

## Architecture

### The Problem with Streamlit

Streamlit reruns the **entire page** on every interaction. For a real-time simulator this means:
- Every "Compute" click causes a full rerun (1-5 seconds of blank screen)
- Charts flicker and reset
- State gets lost between reruns
- No smooth animations — just snapshots

### The Solution: Embedded HTML Panel + Background API

```
┌─────────────────────────────────────────────────────────┐
│ Streamlit Page (runs ONCE on START)                      │
│                                                          │
│  SIDEBAR (Streamlit)          MAIN AREA (HTML/JS)        │
│  ┌──────────────┐            ┌────────────────────────┐  │
│  │ Symbol: OGDC │            │ EMBEDDED PANEL         │  │
│  │ Capital: 1M  │            │ (simulator_panel.html) │  │
│  │              │            │                        │  │
│  │ ☑ VPIN      │     ┌──────│ Polls every 10s:       │  │
│  │ ☑ OFI       │     │      │  GET /fusion/state     │  │
│  │ ☑ Macro HMM │     │      │  POST /fusion/compute  │  │
│  │ ☐ CVD       │     │      │  GET /fusion/price     │  │
│  │              │     │      │                        │  │
│  │ [START]      │     │      │ Updates at 60fps:      │  │
│  └──────────────┘     │      │  - Decision panel      │  │
│                       │      │  - Signal heatmap      │  │
│  On START:            │      │  - Category bars       │  │
│  Launches Flask ──────┘      │  - Equity curve        │  │
│  on port 8766                │  - Position table      │  │
│  (background thread)         │  - Trade blotter       │  │
│                              └────────────────────────┘  │
└─────────────────────────────────────────────────────────┘

Data Flow:
  Panel JS  ──GET /fusion/price──>  Flask API ──> DuckDB tick_logs
  Panel JS  ──POST /fusion/compute──>  Flask API ──> StrategyFusionEngine
                                                      ├─ vpin_strategy
                                                      ├─ ofi_strategy
                                                      ├─ macro_regime_hmm
                                                      ├─ sector_rotation
                                                      ├─ oi_strategy
                                                      ├─ basis_strategy
                                                      ├─ hawkes_process
                                                      └─ ... (12 engines)
  Flask API  ──JSON response──>  Panel JS ──> DOM update (60fps)
```

### Why This Works

| Approach | Reruns? | Speed | Animation | Price Source |
|----------|---------|-------|-----------|-------------|
| Pure Streamlit | Full rerun every click | 1-5s latency | None | Manual input |
| st_autorefresh | Full rerun every 15s | Jerky | None | DuckDB poll |
| **HTML Panel + Flask API** | **Zero reruns** | **<100ms** | **60fps** | **Auto from DuckDB** |

---

## Components

### 1. Strategy Fusion Engine (`engine/strategy_fusion.py`)

The brain — orchestrates all strategy engines into one decision.

**Strategy Categories and Weights (sum to 100%):**

| Category | Weight | Strategies | Role |
|----------|--------|-----------|------|
| REGIME | 30% | Macro HMM (15%), Sector Rotation (15%) | Market environment |
| FLOW | 30% | VPIN (10%), OFI (8%), CVD (7%), OI Buildup (5%) | Order flow signals |
| STRUCTURE | 20% | Basis Arb (10%), Pairs Trading (10%) | Relative value |
| ALPHA | 15% | ML Predictions (8%), LLM Sentiment (7%) | Predictive signals |
| RESEARCH | 5% | Hawkes (3%), VWAP (2%) | Risk/execution context |

**Decision Logic:**

```
1. Each enabled strategy produces:
   - direction: -1 (SHORT), 0 (NEUTRAL), +1 (LONG)
   - confidence: 0.0 to 1.0
   - signal: human-readable description

2. Weighted vote = direction × confidence × weight

3. Raw score = Σ(weighted votes) / Σ(enabled weights)

4. Decision thresholds:
   score > +0.30  →  STRONG_BUY
   score > +0.15  →  BUY
   -0.15 to +0.15 →  HOLD
   score < -0.15  →  SELL
   score < -0.30  →  STRONG_SELL

5. VETO system:
   - VPIN TOXIC: vetoes ALL buy signals (informed flow detected)
   - Hawkes BURST: halves position size (activity spike)
```

**Strategy Adapters — actual function calls:**

| Strategy | Import | Function | Returns |
|----------|--------|----------|---------|
| vpin | `vpin_strategy` | `compute_live_signal(symbol)` | VPINSignal (signal, confidence, vpin) |
| ofi | `ofi_strategy` | `scan_current_ofi([symbol])` | DataFrame (signal, confidence, ofi) |
| cvd | `cvd_strategy` | `scan_divergences()` | list of dicts (signal, confidence) |
| basis_arb | `basis_strategy` | `scan_basis_signals()` | list of BasisSignal dicts |
| macro_hmm | `macro_regime_hmm` | `get_current_regime()` | dict (regime, probability) |
| sector_rotation | `sector_rotation` | `rank_sectors()` | list of SectorSignal |
| oi_buildup | `oi_strategy` | `scan_oi_signals([symbol])` | DataFrame (state, signal, confidence) |
| pairs_trading | `pairs_trading` | `scan_pair_opportunities()` | DataFrame (direction, zscore) |
| sentiment | `sentiment_strategy` | `score_recent_announcements()` | list of SentimentSignal |
| ml_predictions | `ml_features` | `get_eod_features(symbol)` | DataFrame with target_direction |
| hawkes | `hawkes_process` | `analyze_symbol(symbol, fast=True)` | dict (summary with n_bursts) |
| vwap | `vwap_execution` | — | execution context only |

**Virtual Portfolio:**

- Starting capital: configurable (default 1M PKR)
- Position sizing: |score| × 5% of capital
- Max 10 concurrent positions
- Stop loss: 2% (automatic)
- Take profit: 4% (automatic)
- Tracks: equity curve, realized P&L, unrealized P&L, win rate, trade log

### 2. Flask Micro-API (background thread, port 8766)

Started by Streamlit when you click START. Runs in a daemon thread.

**Endpoints:**

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/fusion/state` | GET | Full fusion state JSON (decision + portfolio + votes) |
| `/fusion/compute` | POST/GET | Trigger fusion computation with latest price |
| `/fusion/price?symbol=X` | GET | Fetch latest tick price from DuckDB |
| `/fusion/toggle` | POST | Enable/disable a strategy at runtime |
| `/health` | GET | Server status check |

**Rate limiting:** Fusion engine runs at most once per 5 seconds (to avoid overloading slow strategies).

**Price source:** `/fusion/price` queries DuckDB:
1. First tries `tick_logs` (most recent tick, sub-second)
2. Falls back to `eod_ohlcv` (end-of-day close)

### 3. Embedded HTML Panel (`simulator_panel.html`)

Self-contained HTML/CSS/JS that renders inside Streamlit via `st.components.v1.html()`. Uses Chart.js for equity curve. No build step needed.

**Panels:**

| Panel | What It Shows |
|-------|-------------|
| FUSION DECISION | STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL + confidence bar |
| STRATEGY SIGNALS | Heatmap grid — green (LONG), red (SHORT), gray (neutral/disabled) |
| CATEGORY BREAKDOWN | Horizontal bars for REGIME/FLOW/STRUCTURE/ALPHA scores |
| PORTFOLIO | Total P&L, win rate, trade count |
| EQUITY CURVE | Chart.js line chart, auto-colors green/red based on P&L sign |
| OPEN POSITIONS | Table: symbol, side, entry, current, P&L, P&L% |
| TRADE BLOTTER | Scrollable log of all trades with timestamps |

**Update cycle:**
1. Every REFRESH_MS (default 10,000ms = 10s):
   - Fetch latest price via GET `/fusion/price`
   - Trigger computation via POST `/fusion/compute`
   - Fetch full state via GET `/fusion/state`
   - Update all DOM elements (smooth transitions via CSS)
2. Status dot: green (connected), yellow (stale), red (disconnected)

### 4. Streamlit Page (`strategy_simulator.py`)

Minimal — only handles sidebar controls and launching the API.

**Sidebar controls:**
- Symbol input
- Capital input
- Refresh interval slider (5/10/15/30 seconds)
- Strategy toggles grouped by category (REGIME/FLOW/STRUCTURE/ALPHA/RESEARCH)
- START button

**On START:**
1. Creates `StrategyFusionEngine` with configured capital and enabled strategies
2. Checks if Flask API is already running on port 8766
3. If not, launches it in a daemon thread
4. Waits 1.5s for server to start
5. Injects API URL into HTML panel template
6. Renders the panel via `st.components.v1.html()`

---

## Strategy Speed Benchmarks

Tested on the actual pakfindata DuckDB (6.8M ticks, 598K EOD bars):

| Strategy | Time | Default |
|----------|------|---------|
| macro_hmm | 0.7s | ON |
| sector_rotation | 0.1s | ON |
| vpin | 0.4s | ON |
| ofi | 0.5s | ON |
| oi_buildup | 0.6s | ON |
| hawkes | 0.5s | OFF |
| basis_arb | 12.9s | ON |
| cvd | 15.0s | OFF (slow) |
| pairs_trading | 11.1s | OFF (slow) |
| sentiment | 29.2s | OFF (slow — calls LLM) |
| ml_predictions | 0.1s | OFF |
| vwap | 0.0s | OFF |

**With fast defaults enabled:** ~2-3 seconds per fusion computation.
**With ALL enabled:** ~70+ seconds (dominated by sentiment LLM calls).

---

## What Changed and Why

### Iteration 1: Pure Streamlit (Manual)
- **Problem:** User had to type price manually and click "Compute" every time
- **Problem:** Full page rerun on every click — charts flickered, state lost
- **Problem:** HTML confidence bar rendered as raw `<div>` tags (broken rendering)

### Iteration 2: Auto-refresh with st_autorefresh
- **Improvement:** Added auto-fetch of price from DuckDB (no manual typing)
- **Improvement:** st_autorefresh triggers rerun every 15s
- **Problem:** Still full page reruns — jerky, not real-time

### Iteration 3: Background Flask API + Embedded HTML Panel (current)
- **Solution:** Flask micro-API runs in background thread (port 8766)
- **Solution:** HTML/JS panel polls API every 10s — zero Streamlit reruns
- **Solution:** Chart.js equity curve updates smoothly
- **Solution:** Signal heatmap, position table, blotter all update via DOM manipulation
- **Result:** Real-time feel, smooth animations, no flicker

---

## How to Use

### Quick Start
```bash
conda activate psx
cd ~/pakfindata
streamlit run src/pakfindata/ui/app.py --server.port 8501
```

1. Click **Strategy Simulator** (first item in sidebar)
2. Set symbol (e.g., OGDC), capital (e.g., 1,000,000)
3. Toggle strategies ON/OFF
4. Click **START SIMULATOR**
5. The panel appears and auto-updates every 10 seconds

### With WebSocket Relay (optional, for sub-second ticks)
```bash
# Terminal 1: Start ws_relay
python -m pakfindata.services.ws_relay

# Terminal 2: Start Streamlit
streamlit run src/pakfindata/ui/app.py --server.port 8501
```

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| flask | 3.1.3 | Background micro-API |
| flask-cors | 6.0.2 | CORS for embedded panel |
| streamlit | 1.53.0 | UI framework |
| Chart.js | 4.4.1 (CDN) | Equity curve chart |
| duckdb | 1.5.0 | Price data |
| All 14 strategy engines | Various | Signal generation |

---

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `engine/strategy_fusion.py` | ~400 | Fusion engine, portfolio, decision logic |
| `ui/page_views/strategy_simulator.py` | ~200 | Streamlit page, Flask launcher |
| `ui/page_views/simulator_panel.html` | ~200 | Real-time HTML/JS panel |

---

## API Reference

### GET /fusion/state
Returns full simulator state:
```json
{
  "timestamp": "2026-03-26 15:30:00",
  "decision": {
    "decision": "BUY",
    "raw_score": 0.23,
    "confidence": 46,
    "votes": [...],
    "regime_score": 0.15,
    "flow_score": 0.31,
    "structure_score": 0.0,
    "alpha_score": 0.0,
    "vetoed": false,
    "suggested_size": 820,
    "suggested_size_pct": 2.3
  },
  "portfolio": {
    "capital": 1000000,
    "cash": 876543,
    "total_pnl": 12450,
    "realized_pnl": 8200,
    "unrealized_pnl": 4250,
    "trade_count": 7,
    "win_count": 5,
    "loss_count": 2,
    "win_rate": 71.4,
    "positions": [...],
    "equity_curve": [...]
  },
  "trade_log": [...]
}
```

### POST /fusion/compute?symbol=OGDC&price=279.0
Triggers a fusion computation. Returns same as /fusion/state.

### GET /fusion/price?symbol=OGDC
Returns: `{"symbol": "OGDC", "price": 279.50}`
