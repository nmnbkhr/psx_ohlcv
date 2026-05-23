# pakfindata — Strategy Layer Audit & Implementation Plan

**Date:** 2026-03-26  
**Repo:** github.com/nmnbkhr/psx_ohlcv  
**Version:** 3.7.0  

---

## Executive Summary

All 13 trading strategy prompts were designed and documented in chat sessions but **NONE have been implemented** in the codebase. The data infrastructure is production-ready (tick_logs, EOD, DuckDB, WebSocket, etc.), but the entire strategy engine layer (`src/pakfindata/engine/`) does not exist.

**What exists:** 13 detailed Claude Code prompts with full engine code, UI pages, and test scripts.  
**What's missing:** The `engine/` directory, strategy UI pages, sidebar STRATEGIES/ADVANCED sections.

---

## Audit Results

### Repository Structure — Current vs Required

```
src/pakfindata/
├── engine/                          ← DOES NOT EXIST (needs creation)
│   ├── __init__.py
│   ├── vpin_strategy.py             ← Strategy 1
│   ├── ofi_strategy.py              ← Strategy 2
│   ├── cvd_strategy.py              ← Strategy 3
│   ├── basis_strategy.py            ← Strategy 4
│   ├── vwap_execution.py            ← Strategy 5
│   ├── macro_regime_hmm.py          ← Strategy 6
│   ├── sector_rotation.py           ← Strategy 7
│   ├── oi_strategy.py               ← Strategy 8
│   ├── pairs_strategy.py            ← Strategy 9
│   ├── sentiment_signals.py         ← Strategy 10
│   ├── orderbook_rl.py              ← Strategy 11 (Research)
│   ├── gnn_stock_graph.py           ← Strategy 12 (Research)
│   ├── ml_features.py               ← ML feature engineering
│   └── ml_model.py                  ← ML model training/prediction
│
├── ui/page_views/
│   ├── strategy_vpin.py             ← DOES NOT EXIST
│   ├── strategy_ofi.py              ← DOES NOT EXIST
│   ├── strategy_cvd.py              ← DOES NOT EXIST
│   ├── strategy_basis.py            ← DOES NOT EXIST
│   ├── strategy_vwap.py             ← DOES NOT EXIST
│   ├── strategy_macro.py            ← DOES NOT EXIST
│   ├── strategy_sector.py           ← DOES NOT EXIST
│   ├── strategy_oi.py               ← DOES NOT EXIST
│   ├── strategy_pairs.py            ← DOES NOT EXIST
│   ├── strategy_sentiment.py        ← DOES NOT EXIST
│   ├── ml_predictions.py            ← DOES NOT EXIST
│   └── (research pages)             ← DOES NOT EXIST
│
└── ui/app.py
    └── Sidebar: NO "STRATEGIES" section
    └── Sidebar: NO "ADVANCED" section
```

### Sidebar Navigation — Missing Sections

Current sidebar has: Dashboard, Markets, Analytics, Fixed Income, FX, Funds, Admin, etc.

**Needs to be added:**

```
STRATEGIES (new section)
  ├── ⚡ VPIN Regime Switch
  ├── 📊 OFI Alpha
  ├── 🔀 CVD Divergence
  ├── 📐 Futures Basis Arb
  ├── 🎯 VWAP Execution
  ├── 🌍 Macro Regimes
  ├── 🔄 Sector Rotation
  ├── 📈 OI Buildup/Unwind
  ├── ⚖️ Pairs Trading
  └── 💬 Sentiment Signals

RESEARCH (new section — or add to existing)
  └── 🤖 ML Predictions

ADVANCED (new section)
  ├── 📖 Order Book RL
  └── 🕸️ GNN Stock Graph
```

---

## Complete Strategy Catalog

### Tier 1 — Microstructure (uses tick_logs data)

| # | Name | Engine File | Data Sources | Dependencies | Effort |
|---|------|-------------|--------------|--------------|--------|
| 1 | **VPIN Regime Switch** | `vpin_strategy.py` | tick_logs, eod_ohlcv | scipy | 1-2 days |
| 2 | **OFI Alpha** | `ofi_strategy.py` | tick_logs | numpy/pandas only | 1-2 days |
| 3 | **CVD Divergence** | `cvd_strategy.py` | tick_logs | numpy/pandas only | 1-2 days |

**Strategy 1 — VPIN Regime Switch**
- Computes VPIN toxicity from tick data using Bulk Volume Classification
- Combines with Hurst exponent (R/S analysis on EOD) for regime detection
- Signal matrix: VPIN state (SAFE/ELEVATED/WARNING/TOXIC/CLEARING) × Hurst regime (TRENDING/MEAN_REVERTING/RANDOM_WALK)
- Key edge: CLEARING signal (VPIN dropping from TOXIC) = informed traders done, entry window
- On PSX, VPIN signals persist for hours (vs microseconds on NYSE)
- 4-tab UI: Live Signal gauge, Backtest equity curve, Symbol Scanner, Methodology

**Strategy 2 — OFI Alpha**
- Order Flow Imbalance from bid/ask/bidVol/askVol on every tick
- Two methods: instantaneous OFI + Cont delta OFI
- 15-min bar aggregation, signal when |OFI| > 0.3
- Entry: next bar in OFI direction. Exit: 3% TP / 2% SL / 4 bars timeout / OFI flip
- Research tab: OFI vs next-bar return scatter with R² (validates edge)
- 4-tab UI: Live Monitor, Backtest, Scanner, Research

**Strategy 3 — CVD Divergence**
- Cumulative Volume Delta = running sum of (buy_vol - sell_vol)
- Tick classification: price >= ask = BUY, price <= bid = SELL
- 4 divergence types: Regular Bullish/Bearish, Hidden Bullish/Bearish
- Swing detection on 5-min resampled CVD with ±15 bar window
- Exit: 4% TP / 2.5% SL / 20 bars max hold
- PSX edge: no dark pools, all volume visible, institutional accumulation takes days

### Tier 2 — Market Structure (uses EOD + derivatives data)

| # | Name | Engine File | Data Sources | Dependencies | Effort |
|---|------|-------------|--------------|--------------|--------|
| 4 | **Futures Basis Arb** | `basis_strategy.py` | eod_ohlcv, futures_contracts | numpy/pandas only | 2 days |
| 5 | **VWAP Execution** | `vwap_execution.py` | ohlcv_5s, eod_ohlcv | numpy/pandas only | 2 days |
| 8 | **OI Buildup/Unwind** | `oi_strategy.py` | eod_ohlcv, futures OI (DFC XLS) | numpy/pandas only | 1-2 days |

**Strategy 4 — Futures Basis Arb**
- Basis = (Futures - Spot) / Spot. Fair basis = KIBOR × (DTE/365)
- Z-score on 20-day rolling excess basis
- Entry at ±2σ, exit at ±0.5σ. Auto-exit 3 days before expiry
- Market-neutral (long one leg, short other). Physical delivery guarantees convergence
- PSX edge: retail traders roll late, basis spikes ±3-5% near expiry

**Strategy 5 — VWAP Execution**
- Volume profile from historical intraday data (POC, Value Area)
- 3 strategies: VWAP (proportional to volume), TWAP (equal), AGGRESSIVE (front-loaded)
- Execution simulator: replay historical day, show fills vs VWAP
- Participation rate capping (default 15%, max 30%)
- 3-tab UI: Volume Profile, Execution Planner, Execution Simulator

**Strategy 8 — OI Buildup/Unwind**
- Classic OI matrix: Price↑+OI↑=Long Buildup (BUY), Price↓+OI↑=Short Buildup (SELL), etc.
- Min streak = 2 days for signal confirmation
- Rollover-aware: cuts confidence 50% in last 5 days before expiry
- Basis confirmation: premium aligns with bullish OI = higher conviction
- Force exit at DTE ≤ 2 to avoid physical delivery risk

### Tier 3 — Cross-Asset & Allocation

| # | Name | Engine File | Data Sources | Dependencies | Effort |
|---|------|-------------|--------------|--------------|--------|
| 6 | **Macro Regime HMM** | `macro_regime_hmm.py` | eod_ohlcv, kibor_daily, sbp_easydata | hmmlearn (optional) | 2-3 days |
| 7 | **Sector Rotation** | `sector_rotation.py` | eod_ohlcv, sector_summary | numpy/pandas only | 1-2 days |

**Strategy 6 — Macro Regime HMM**
- 5 features: KSE-100 momentum, KIBOR 3M, PKR/USD, Brent crude, SBP rate
- 4 hidden regimes: RISK_ON (80% equity), TRANSITION (40%), RISK_OFF (20%), CRISIS (0%)
- HMM (hmmlearn) with rule-based fallback if not installed
- Monthly rebalancing. Transition matrix shows regime persistence
- Should identify 2022-23 as CRISIS, 2025-26 as RISK_ON

**Strategy 7 — Sector Rotation**
- Composite = 60%(1M momentum) + 30%(3M momentum) + 10%(breadth - 50%)
- Monthly rebalance: long top 3 sectors, short bottom 3
- Pakistan rotation playbook hardcoded: rate cuts → banks, stimulus → cement, etc.
- Works WITH Strategy 6: HMM says regime, Rotation says where within regime

### Tier 4 — Statistical & NLP

| # | Name | Engine File | Data Sources | Dependencies | Effort |
|---|------|-------------|--------------|--------------|--------|
| 9 | **Pairs Trading** | `pairs_strategy.py` | eod_ohlcv | statsmodels, pykalman | 2 weeks |
| 10 | **Sentiment LLM** | `sentiment_signals.py` | announcements, GPT-4o/Ollama | anthropic/openai, FinBERT | 2 weeks |

**Strategy 9 — Pairs Trading (Stat Arb)**
- Cointegration testing (Johansen) across all PSX pairs
- Dynamic hedge ratio via Kalman filter
- Known PSX pairs: OGDC/PPL, HBL/UBL, LUCK/DGKC
- Spread z-score entry/exit with half-life estimation

**Strategy 10 — Sentiment Signals (LLM)**
- Announcement → LLM sentiment score (-1 to +1) → merge with technical signal
- 3 scoring methods: GPT-4o API, FinBERT local, Ollama Docker (RTX 4080)
- Combined signal: 40% sentiment + 60% technical. Both agree = 1.3× confidence
- PSX edge: only ~30 companies have analyst coverage, 534 are information deserts

### Tier 5 — ML & Research

| # | Name | Engine File | Data Sources | Dependencies | Effort |
|---|------|-------------|--------------|--------------|--------|
| ML | **ML Predictions** | `ml_features.py` + `ml_model.py` | eod_ohlcv, tick_logs, ohlcv_5s | xgboost, lightgbm, shap | 1 week |
| 11 | **Order Book RL** | `orderbook_rl.py` | tick_logs (bid/ask) | torch, gymnasium, stable-baselines3 | Research |
| 12 | **GNN Stock Graph** | `gnn_stock_graph.py` | company profiles, sector, eod_ohlcv | torch, torch_geometric | Research |

**ML Predictions**
- 40+ features from EOD (SMA, RSI, MACD, Bollinger, Hurst, momentum, vol)
- 8 tick features (VPIN, OFI, spread) when available
- Walk-forward validation only. Target = next-day direction (binary)
- XGBoost default, LightGBM/RF alternatives. GPU: tree_method="gpu_hist"

**Strategy 11 — Order Book RL (Research)**
- Approximate L2 from L1 using power law: vol at level k = best_vol × 0.5^k
- Agent-based sim: NoiseTrader, MomentumTrader, MarketMaker
- RL agent (PPO/DQN): 14-dim state, 7 discrete actions
- Requires PyTorch + gymnasium + stable-baselines3

**Strategy 12 — GNN Stock Graph (Research)**
- 564 nodes (PSX stocks), 4 edge types: sector, supply chain, directors, correlation
- GNN predicts which connections drive future price co-movement
- Requires PyTorch Geometric (torch_geometric, torch_scatter, torch_sparse, torch_cluster)
- NeurIPS/ICML publication-grade research

---

## Dependency Matrix

| Package | Strategies | Already Installed? | Size |
|---------|------------|:--:|------|
| numpy, pandas | ALL | ✅ | — |
| scipy | 1 (VPIN) | Check | ~30 MB |
| statsmodels | 9 (Pairs) | Check | ~50 MB |
| pykalman | 9 (Pairs) | Check | ~5 MB |
| hmmlearn | 6 (Macro HMM) | ❌ | ~5 MB |
| xgboost | ML | ❌ | ~30 MB |
| lightgbm | ML | ❌ | ~20 MB |
| shap | ML | ❌ | ~30 MB |
| anthropic/openai | 10 (Sentiment) | ✅ (agentic extras) | — |
| PyTorch (CUDA) | 11, 12 | ✅ 2.11.0+cu130 | — |
| gymnasium | 11 | ❌ | ~15 MB |
| stable-baselines3 | 11 | ❌ | ~5 MB |
| torch_geometric | 12 | ❌ | ~100 MB |

---

## Implementation Priority

### Phase A — Quick Wins (1 week total, numpy/pandas only)

```
1. Create src/pakfindata/engine/__init__.py
2. Strategy 7: Sector Rotation       ← uses existing sector data, no new deps
3. Strategy 8: OI Buildup/Unwind     ← uses existing DFC/OI data, no new deps
4. Strategy 2: OFI Alpha             ← uses existing tick_logs, no new deps
5. Add STRATEGIES sidebar section to app.py
```

**Why these first:** Zero new dependencies, data already populated, simplest signal logic.

### Phase B — Core Microstructure (1-2 weeks, scipy needed)

```
6. Strategy 1: VPIN Regime Switch     ← needs scipy for R/S analysis
7. Strategy 3: CVD Divergence         ← numpy only but complex logic
8. Strategy 5: VWAP Execution         ← numpy only, needs ohlcv_5s data
```

### Phase C — Market Structure (1-2 weeks, hmmlearn optional)

```
9. Strategy 4: Futures Basis Arb      ← needs futures data verification
10. Strategy 6: Macro Regime HMM      ← needs hmmlearn (has rule-based fallback)
11. ML Predictions                    ← needs xgboost/lightgbm install
```

### Phase D — Advanced (2-4 weeks, external APIs)

```
12. Strategy 9: Pairs Trading         ← needs statsmodels + pykalman
13. Strategy 10: Sentiment LLM        ← needs Ollama Docker or API keys
```

### Phase E — Research (ongoing)

```
14. Strategy 11: Order Book RL        ← needs gymnasium + SB3
15. Strategy 12: GNN Stock Graph      ← needs torch_geometric
```

---

## Data Readiness Check

| Data Source | Table/File | Populated? | Required By |
|-------------|-----------|:--:|------------|
| tick_logs (bid/ask/price/vol) | DuckDB tick_logs | ✅ ~652K/day | 1, 2, 3, 11 |
| EOD OHLCV (5yr daily) | DuckDB eod_ohlcv | ✅ ~598K bars | 1, 4, 6, 7, 9, ML |
| Intraday 5s bars | DuckDB ohlcv_5s | ✅ | 5, ML |
| Futures contracts | SQLite/DuckDB | ✅ (verify cols) | 4, 8 |
| Futures OI (DFC XLS) | Downloads | ✅ | 8 |
| Sector data | SQLite sector_summary | ✅ | 7 |
| KIBOR rates | DuckDB/SQLite | ✅ | 4, 6 |
| SBP EasyData | sbp_easydata/ | ✅ 626 series | 6 |
| Company profiles | SQLite | ✅ | 12 |
| Announcements | SQLite corporate_announcements | ✅ | 10 |
| Company key people | SQLite company_key_people | ✅ | 12 (directors graph) |

---

## Lineage Graph Enhancement

The admin lineage page should add these nodes:

```python
STRATEGY_EDGES = {
    "VPIN Strategy":    {"reads": ["tick_logs", "eod_ohlcv"],         "engine": "engine/vpin_strategy.py"},
    "OFI Alpha":        {"reads": ["tick_logs"],                       "engine": "engine/ofi_strategy.py"},
    "CVD Divergence":   {"reads": ["tick_logs"],                       "engine": "engine/cvd_strategy.py"},
    "Basis Arb":        {"reads": ["eod_ohlcv", "futures_contracts"],  "engine": "engine/basis_strategy.py"},
    "VWAP Execution":   {"reads": ["ohlcv_5s", "eod_ohlcv"],          "engine": "engine/vwap_execution.py"},
    "Macro Regime":     {"reads": ["eod_ohlcv", "kibor_daily", "sbp_easydata"], "engine": "engine/macro_regime_hmm.py"},
    "Sector Rotation":  {"reads": ["eod_ohlcv", "sector_summary"],     "engine": "engine/sector_rotation.py"},
    "OI Buildup":       {"reads": ["eod_ohlcv", "futures_oi"],         "engine": "engine/oi_strategy.py"},
    "Pairs Trading":    {"reads": ["eod_ohlcv"],                       "engine": "engine/pairs_strategy.py"},
    "Sentiment LLM":    {"reads": ["corporate_announcements"],         "engine": "engine/sentiment_signals.py"},
    "ML Predictions":   {"reads": ["eod_ohlcv", "tick_logs", "ohlcv_5s"], "engine": "engine/ml_model.py"},
}
```

New layer in the graph: **Sources → DB Tables → Engines → UI Pages**

---

## Other Discussed Features — Not in Repo

Beyond strategies, these features were discussed in chat but are NOT in the GitHub repo:

| Feature | Discussed | In Repo | Notes |
|---------|:---------:|:-------:|-------|
| `sources/psx_market_data.py` (unified DPS+PSXT backfill) | ✅ | ❌ | Full code written in chat, never committed |
| Microstructure page (VPIN, spread analytics) | ✅ | ❌ | No page file exists |
| Derivatives page (OI, basis, rollover) | ✅ | ❌ | `futures.py` exists but no OI matrix/basis |
| Signal Dashboard (composite scores) | ✅ | ❌ | Referenced as existing but no file found |
| Admin Lineage Graph page (vertex/node/edge) | ✅ | ❌ | User said "added" — may be local only, not pushed |
| DPS market-watch poller (30s snapshots) | ✅ | ❌ | Prompt written, not implemented |
| Circuit limits integration in Live Market | ✅ | ❌ | Data downloaded but not integrated |
| Index weights in Sector Heatmap | ✅ | ❌ | Data available from downloads |
| VAR margins as signal factor | ✅ | ❌ | Downloaded but not used |
| Ollama Docker for sentiment | ✅ | ❌ | Docker compose + code written in chat |
| CSV→JSONL tick converter | ✅ | ❌ | Prompt written for Claude Code |

---

## How to Use This Document

Each strategy prompt was designed to be **fed directly to Claude Code** for implementation. The workflow:

1. Pick a strategy from the priority list above
2. Find the corresponding prompt in the chat history (or recreate from this doc)
3. Feed to Claude Code with: `cd ~/pakfindata && conda activate psx`
4. Claude Code creates the engine file + UI page + adds to sidebar
5. Run the test commands at the end of each prompt to verify
6. Commit: `git add src/pakfindata/engine/ src/pakfindata/ui/page_views/strategy_*.py`

**Start with Phase A** — Strategy 7 (Sector Rotation) is the easiest entry point because it uses existing sector data and requires zero new dependencies.
