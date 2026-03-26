# Claude Code Prompt: Strategy 6 — Cross-Asset Macro Regime Model (HMM)

## Context

pakfindata has 20 years of SBP EasyData (195 datasets, 18K series) plus 5 years of EOD.
This strategy uses a Hidden Markov Model to detect macro regimes by combining multiple 
asset classes — then allocates accordingly.

**The insight:** Pakistan's market cycles are driven by SBP rate cycles, PKR stability, 
and oil prices. These are observable. The REGIME (risk-on, risk-off, crisis, transition) 
is hidden — HMM infers it from the observable signals.

**What HMM does:** Given a sequence of observations (returns, rates, FX moves), HMM 
infers which hidden state (regime) the market is most likely in RIGHT NOW, and the 
probability of transitioning to another regime tomorrow.

## What already exists

```bash
# Find existing macro/regime code
grep -rn "hurst\|regime\|macro\|kibor\|sbp.*rate\|policy.*rate" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check SBP EasyData available
ls /mnt/e/psxdata/sbp_easydata/series/ | head -20
python3 -c "
import json
try:
    cat = json.load(open('/mnt/e/psxdata/sbp_easydata/catalog.json'))
    datasets = cat if isinstance(cat, list) else cat.get('datasets', [])
    print(f'Total datasets: {len(datasets)}')
    for ds in datasets[:10]:
        print(f'  {ds.get(\"code\",\"\")}: {ds.get(\"name\",\"\")}')
except Exception as e:
    print(f'Error: {e}')
"

# Check EOD data range
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
print('eod_ohlcv:', con.execute('SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM eod_ohlcv').fetchone())

# Check if we have index data
for t in ['index_ohlcv_5s','psx_eod']:
    try:
        r = con.execute(f'SELECT COUNT(*), MIN(date), MAX(date) FROM {t}').fetchone()
        print(f'{t}: {r}')
    except: pass

# Check KIBOR data
for t in ['kibor_daily','kibor_rates']:
    try:
        r = con.execute(f'SELECT COUNT(*), MIN(date), MAX(date) FROM {t}').fetchone()
        print(f'{t}: {r}')
    except: pass
con.close()
"

# Check psx.sqlite for additional macro data
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
tables = [r[0] for r in con.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]
for t in tables:
    tl = t.lower()
    if any(k in tl for k in ['kibor','rate','fx','currency','macro','sbp','index','kse']):
        count = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        print(f'{t}: {count:,}')
con.close()
"
```

**READ ALL OUTPUT before proceeding. Identify exact table names and date ranges.**

## Step 1: Create the Macro Regime Engine

Create `src/pakfindata/engine/macro_regime_hmm.py`:

```python
"""
Cross-Asset Macro Regime Model using Hidden Markov Models.

Observes 5 asset signals:
  1. KSE-100 momentum (20-day return)
  2. KIBOR 3M direction (rate change)
  3. PKR/USD trend (FX depreciation rate)
  4. Brent crude trend (oil price change)
  5. SBP rate cycle position (easing vs tightening)

Infers 4 hidden regimes:
  RISK_ON:    KSE rising, rates falling, PKR stable, oil stable
  RISK_OFF:   KSE falling, rates rising, PKR weakening, oil spiking
  TRANSITION: Mixed signals, regime changing
  CRISIS:     Sharp drawdowns, PKR crash, rate spikes, liquidity freeze

Allocation per regime:
  RISK_ON:    80% equity, 10% bonds, 10% cash
  RISK_OFF:   20% equity, 50% bonds, 30% cash
  TRANSITION: 40% equity, 30% bonds, 30% cash
  CRISIS:     0% equity, 30% bonds, 70% cash (preserve capital)

PSX-Specific:
  - SBP rate cycle is THE dominant driver (explains ~40% of KSE variance)
  - PKR depreciation correlates with equity selloffs
  - Oil > $100/bbl is bearish for Pakistan (import bill)
  - KIBOR leads equity by 2-3 months
  - Trading days: 245/year
"""

import numpy as np
import pandas as pd
import duckdb
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
SBP_DIR = Path("/mnt/e/psxdata/sbp_easydata")
TRADING_DAYS = 245

# Regime definitions
class MacroRegime(Enum):
    RISK_ON = 0
    TRANSITION = 1
    RISK_OFF = 2
    CRISIS = 3

# Allocation per regime
REGIME_ALLOCATION = {
    MacroRegime.RISK_ON:    {"equity": 0.80, "bonds": 0.10, "cash": 0.10},
    MacroRegime.TRANSITION: {"equity": 0.40, "bonds": 0.30, "cash": 0.30},
    MacroRegime.RISK_OFF:   {"equity": 0.20, "bonds": 0.50, "cash": 0.30},
    MacroRegime.CRISIS:     {"equity": 0.00, "bonds": 0.30, "cash": 0.70},
}

REGIME_COLORS = {
    MacroRegime.RISK_ON: "#22C55E",
    MacroRegime.TRANSITION: "#EAB308",
    MacroRegime.RISK_OFF: "#F97316",
    MacroRegime.CRISIS: "#EF4444",
}


@dataclass
class RegimeState:
    date: str
    regime: MacroRegime
    probability: float         # P(current regime)
    regime_probs: dict         # {regime: probability} for all 4
    kse_momentum: float        # 20-day KSE return
    kibor_direction: float     # KIBOR change (bps)
    pkr_trend: float           # PKR depreciation (%)
    oil_trend: float           # Brent change (%)
    sbp_cycle: str             # "EASING", "TIGHTENING", "HOLD"
    allocation: dict           # recommended allocation


@dataclass
class RegimeAnalysis:
    states: list[RegimeState]
    transition_matrix: np.ndarray   # 4×4 transition probabilities
    regime_durations: dict          # avg days in each regime
    current_regime: MacroRegime
    current_probs: dict
    model_params: dict


def load_macro_features(lookback_months: int = 60) -> pd.DataFrame:
    """
    Load and merge all macro features into a single monthly DataFrame.
    
    Sources:
      - KSE-100: from eod_ohlcv (DuckDB) — use symbol 'KSE100' or index table
      - KIBOR: from kibor table or SBP EasyData
      - PKR/USD: from FX data in psx.sqlite or SBP EasyData
      - Brent crude: from commodities or external
      - SBP rate: from SBP EasyData or scraped data
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=lookback_months * 30)).strftime("%Y-%m-%d")
    
    # ── 1. KSE-100 index data ──
    kse = None
    
    # Try index tables first
    for query in [
        f"SELECT date, close FROM psx_eod WHERE symbol='KSE100' AND date>='{cutoff}' ORDER BY date",
        f"SELECT date, close FROM eod_ohlcv WHERE symbol='KSE100' AND date>='{cutoff}' ORDER BY date",
        f"SELECT date, close FROM eod_ohlcv WHERE symbol='KSE-100' AND date>='{cutoff}' ORDER BY date",
    ]:
        try:
            kse = con.execute(query).df()
            if not kse.empty:
                break
        except:
            continue
    
    # Fallback: compute KSE proxy from top stocks
    if kse is None or kse.empty:
        try:
            kse = con.execute(f"""
                SELECT date, AVG(close) as close FROM eod_ohlcv
                WHERE symbol IN ('OGDC','HBL','UBL','LUCK','ENGRO','PPL','MCB','FFC','HUBC','NBP')
                AND date >= '{cutoff}'
                GROUP BY date ORDER BY date
            """).df()
        except:
            kse = pd.DataFrame()
    
    # ── 2. KIBOR rates ──
    kibor = pd.DataFrame()
    for query in [
        f"SELECT date, value as kibor FROM kibor_daily WHERE date>='{cutoff}' ORDER BY date",
        f"SELECT date, offer as kibor FROM kibor_rates WHERE tenor='3M' AND date>='{cutoff}' ORDER BY date",
    ]:
        try:
            kibor = con.execute(query).df()
            if not kibor.empty:
                break
        except:
            continue
    
    con.close()
    
    # ── 3. FX rates (PKR/USD) ──
    fx = pd.DataFrame()
    try:
        scon = sqlite3.connect(str(PSX_SQLITE))
        # Try various FX tables
        for t in ['fx_interbank', 'fx_rates', 'currency_rates', 'exchange_rates']:
            try:
                fx = pd.read_sql(f"""
                    SELECT date, rate as pkr_usd FROM {t}
                    WHERE currency LIKE '%USD%' AND date >= '{cutoff}'
                    ORDER BY date
                """, scon)
                if not fx.empty:
                    break
            except:
                continue
        scon.close()
    except:
        pass
    
    # Fallback FX from SBP EasyData
    if fx.empty:
        fx = _load_sbp_series("exchange", "USD", cutoff)
    
    # ── 4. Oil price ──
    oil = pd.DataFrame()
    try:
        scon = sqlite3.connect(str(PSX_SQLITE))
        for t in ['commodities', 'commodity_prices']:
            try:
                oil = pd.read_sql(f"""
                    SELECT date, price as brent FROM {t}
                    WHERE commodity LIKE '%Brent%' OR symbol='BRENT'
                    AND date >= '{cutoff}' ORDER BY date
                """, scon)
                if not oil.empty:
                    break
            except:
                continue
        scon.close()
    except:
        pass
    
    # ── 5. SBP Policy Rate ──
    sbp_rate = pd.DataFrame()
    sbp_rate = _load_sbp_series("policy", "rate", cutoff)
    
    # ── Merge all into monthly frequency ──
    # Resample each to monthly (last value)
    dfs = {}
    
    if not kse.empty:
        kse["date"] = pd.to_datetime(kse["date"])
        kse = kse.set_index("date").resample("ME").last().reset_index()
        dfs["kse"] = kse.rename(columns={"close": "kse_close"})
    
    if not kibor.empty:
        kibor["date"] = pd.to_datetime(kibor["date"])
        kibor = kibor.set_index("date").resample("ME").last().reset_index()
        dfs["kibor"] = kibor
    
    if not fx.empty:
        fx["date"] = pd.to_datetime(fx["date"])
        fx = fx.set_index("date").resample("ME").last().reset_index()
        dfs["fx"] = fx
    
    if not oil.empty:
        oil["date"] = pd.to_datetime(oil["date"])
        oil = oil.set_index("date").resample("ME").last().reset_index()
        dfs["oil"] = oil
    
    if not sbp_rate.empty:
        sbp_rate["date"] = pd.to_datetime(sbp_rate["date"])
        sbp_rate = sbp_rate.set_index("date").resample("ME").last().reset_index()
        dfs["sbp"] = sbp_rate
    
    # Start from KSE (most important)
    if "kse" not in dfs:
        return pd.DataFrame()
    
    merged = dfs["kse"]
    for name, df in dfs.items():
        if name != "kse":
            merged = merged.merge(df, on="date", how="left")
    
    # Forward-fill missing values
    merged = merged.sort_values("date").ffill()
    
    # ── Compute features ──
    # KSE momentum (1-month return)
    merged["kse_mom_1m"] = merged["kse_close"].pct_change()
    # KSE momentum (3-month return)
    merged["kse_mom_3m"] = merged["kse_close"].pct_change(3)
    # KSE volatility (3-month rolling)
    merged["kse_vol_3m"] = merged["kse_mom_1m"].rolling(3).std()
    
    # KIBOR direction (monthly change in bps)
    if "kibor" in merged.columns:
        merged["kibor_chg"] = merged["kibor"].diff() * 100  # to bps
    else:
        merged["kibor_chg"] = 0
    
    # PKR trend (monthly depreciation %)
    if "pkr_usd" in merged.columns:
        merged["pkr_chg"] = merged["pkr_usd"].pct_change() * 100
    else:
        merged["pkr_chg"] = 0
    
    # Oil trend
    if "brent" in merged.columns:
        merged["oil_chg"] = merged["brent"].pct_change() * 100
    else:
        merged["oil_chg"] = 0
    
    # SBP cycle
    if "sbp_rate" in merged.columns:
        merged["sbp_chg"] = merged["sbp_rate"].diff()
        merged["sbp_cycle"] = np.where(merged["sbp_chg"] < 0, "EASING",
                              np.where(merged["sbp_chg"] > 0, "TIGHTENING", "HOLD"))
    else:
        merged["sbp_chg"] = 0
        merged["sbp_cycle"] = "HOLD"
    
    return merged.dropna(subset=["kse_mom_1m"])


def _load_sbp_series(keyword1: str, keyword2: str, cutoff: str) -> pd.DataFrame:
    """Try to load a series from SBP EasyData local files."""
    series_dir = SBP_DIR / "series"
    if not series_dir.exists():
        return pd.DataFrame()
    
    for f in series_dir.glob("*.json"):
        try:
            data = json.load(open(f))
            name = str(data.get("name", "")).lower()
            if keyword1.lower() in name and keyword2.lower() in name:
                obs = data.get("observations", [])
                if obs:
                    df = pd.DataFrame(obs)
                    # Normalize columns
                    date_col = next((c for c in df.columns if "date" in c.lower()), None)
                    val_col = next((c for c in df.columns if "value" in c.lower() or "obs" in c.lower()), None)
                    if date_col and val_col:
                        result = df[[date_col, val_col]].copy()
                        result.columns = ["date", keyword1 + "_" + keyword2 if keyword2 != "rate" else "sbp_rate"]
                        result["date"] = pd.to_datetime(result["date"])
                        result = result[result["date"] >= cutoff]
                        if not result.empty:
                            return result
        except:
            continue
    
    return pd.DataFrame()


def fit_hmm(features_df: pd.DataFrame, n_regimes: int = 4) -> dict:
    """
    Fit a Gaussian Hidden Markov Model to macro features.
    
    Uses hmmlearn library. If not available, falls back to 
    simple rule-based regime classification.
    
    Observation features: [kse_mom_1m, kibor_chg, pkr_chg, oil_chg, kse_vol_3m]
    """
    # Prepare observation matrix
    feature_cols = ["kse_mom_1m", "kibor_chg", "pkr_chg", "oil_chg", "kse_vol_3m"]
    available = [c for c in feature_cols if c in features_df.columns]
    
    if len(available) < 2:
        return _rule_based_regime(features_df)
    
    X = features_df[available].fillna(0).values
    
    # Standardize
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    try:
        from hmmlearn.hmm import GaussianHMM
        
        model = GaussianHMM(
            n_components=n_regimes,
            covariance_type="full",
            n_iter=200,
            random_state=42,
            tol=0.01,
        )
        
        model.fit(X_scaled)
        
        # Predict hidden states
        hidden_states = model.predict(X_scaled)
        state_probs = model.predict_proba(X_scaled)
        
        # Map HMM states to regime labels based on mean characteristics
        state_means = {}
        for state in range(n_regimes):
            mask = hidden_states == state
            if mask.sum() > 0:
                state_means[state] = {
                    "kse_mean": features_df.loc[mask, "kse_mom_1m"].mean() if "kse_mom_1m" in features_df.columns else 0,
                    "vol_mean": features_df.loc[mask, "kse_vol_3m"].mean() if "kse_vol_3m" in features_df.columns else 0,
                    "count": int(mask.sum()),
                }
        
        # Sort states by KSE return: highest = RISK_ON, lowest = CRISIS
        sorted_states = sorted(state_means.keys(), 
                              key=lambda s: state_means[s]["kse_mean"], 
                              reverse=True)
        
        regime_map = {}
        regime_labels = [MacroRegime.RISK_ON, MacroRegime.TRANSITION, 
                        MacroRegime.RISK_OFF, MacroRegime.CRISIS]
        for i, state in enumerate(sorted_states):
            if i < len(regime_labels):
                regime_map[state] = regime_labels[i]
        
        # Map predictions to regime labels
        regimes = [regime_map.get(s, MacroRegime.TRANSITION) for s in hidden_states]
        
        return {
            "model": model,
            "scaler": scaler,
            "regimes": regimes,
            "state_probs": state_probs,
            "regime_map": regime_map,
            "transition_matrix": model.transmat_,
            "feature_cols": available,
            "method": "HMM",
            "n_regimes": n_regimes,
            "state_means": state_means,
        }
    
    except ImportError:
        # hmmlearn not installed — use rule-based fallback
        return _rule_based_regime(features_df)


def _rule_based_regime(features_df: pd.DataFrame) -> dict:
    """
    Rule-based regime classification (fallback when hmmlearn not available).
    
    Rules:
      RISK_ON: KSE > 0%, KIBOR falling, PKR stable (< 2% depreciation)
      RISK_OFF: KSE < 0%, KIBOR rising, PKR weakening
      CRISIS: KSE < -5%, PKR > 5% depreciation, vol spike
      TRANSITION: everything else
    """
    regimes = []
    probs = []
    
    for _, row in features_df.iterrows():
        kse = row.get("kse_mom_1m", 0) or 0
        kibor = row.get("kibor_chg", 0) or 0
        pkr = row.get("pkr_chg", 0) or 0
        vol = row.get("kse_vol_3m", 0) or 0
        
        # Score each regime
        scores = {
            MacroRegime.RISK_ON: 0,
            MacroRegime.RISK_OFF: 0,
            MacroRegime.TRANSITION: 1,  # default bias
            MacroRegime.CRISIS: 0,
        }
        
        # CRISIS signals
        if kse < -0.05: scores[MacroRegime.CRISIS] += 3
        if pkr > 5: scores[MacroRegime.CRISIS] += 2
        if vol > 0.08: scores[MacroRegime.CRISIS] += 2
        if kibor > 100: scores[MacroRegime.CRISIS] += 1  # 100bps hike
        
        # RISK_OFF signals
        if kse < 0: scores[MacroRegime.RISK_OFF] += 2
        if kibor > 0: scores[MacroRegime.RISK_OFF] += 1
        if pkr > 2: scores[MacroRegime.RISK_OFF] += 1
        
        # RISK_ON signals
        if kse > 0.02: scores[MacroRegime.RISK_ON] += 2
        if kibor < 0: scores[MacroRegime.RISK_ON] += 1.5
        if pkr < 1: scores[MacroRegime.RISK_ON] += 1
        if vol < 0.04: scores[MacroRegime.RISK_ON] += 1
        
        # Normalize to probabilities
        total = sum(scores.values())
        regime_probs = {r: s / total for r, s in scores.items()}
        
        best = max(scores, key=scores.get)
        regimes.append(best)
        probs.append(regime_probs)
    
    # Build approximate transition matrix
    trans = np.zeros((4, 4))
    for i in range(1, len(regimes)):
        trans[regimes[i-1].value][regimes[i].value] += 1
    
    # Normalize rows
    row_sums = trans.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    trans = trans / row_sums
    
    return {
        "model": None,
        "scaler": None,
        "regimes": regimes,
        "state_probs": np.array([[p.get(MacroRegime(j), 0) for j in range(4)] for p in probs]),
        "regime_map": {i: MacroRegime(i) for i in range(4)},
        "transition_matrix": trans,
        "feature_cols": ["kse_mom_1m", "kibor_chg", "pkr_chg", "oil_chg", "kse_vol_3m"],
        "method": "RULE_BASED",
        "n_regimes": 4,
    }


def analyze_regimes(lookback_months: int = 60) -> RegimeAnalysis:
    """
    Full regime analysis: load data → fit HMM → return analysis.
    """
    features = load_macro_features(lookback_months)
    
    if features.empty or len(features) < 12:
        return None
    
    result = fit_hmm(features)
    
    regimes = result["regimes"]
    probs = result["state_probs"]
    
    # Build state history
    states = []
    for i, (_, row) in enumerate(features.iterrows()):
        regime = regimes[i]
        regime_probs = {MacroRegime(j): float(probs[i][j]) for j in range(min(4, probs.shape[1]))}
        
        states.append(RegimeState(
            date=str(row["date"])[:10],
            regime=regime,
            probability=float(probs[i][regime.value]) if regime.value < probs.shape[1] else 0,
            regime_probs=regime_probs,
            kse_momentum=row.get("kse_mom_1m", 0),
            kibor_direction=row.get("kibor_chg", 0),
            pkr_trend=row.get("pkr_chg", 0),
            oil_trend=row.get("oil_chg", 0),
            sbp_cycle=row.get("sbp_cycle", "HOLD"),
            allocation=REGIME_ALLOCATION.get(regime, REGIME_ALLOCATION[MacroRegime.TRANSITION]),
        ))
    
    # Regime durations
    durations = {r: [] for r in MacroRegime}
    current_regime = regimes[0]
    current_duration = 1
    for i in range(1, len(regimes)):
        if regimes[i] == current_regime:
            current_duration += 1
        else:
            durations[current_regime].append(current_duration)
            current_regime = regimes[i]
            current_duration = 1
    durations[current_regime].append(current_duration)
    
    avg_durations = {r: np.mean(d) if d else 0 for r, d in durations.items()}
    
    return RegimeAnalysis(
        states=states,
        transition_matrix=result["transition_matrix"],
        regime_durations=avg_durations,
        current_regime=regimes[-1],
        current_probs={MacroRegime(j): float(probs[-1][j]) for j in range(min(4, probs.shape[1]))},
        model_params={
            "method": result["method"],
            "n_regimes": result["n_regimes"],
            "features_used": result["feature_cols"],
            "months_analyzed": len(features),
        }
    )


def backtest_regime_allocation(
    lookback_months: int = 60,
    rebalance_frequency: str = "monthly",
) -> dict:
    """
    Backtest regime-based allocation vs buy-and-hold.
    
    Each month:
      1. Determine regime from HMM
      2. Allocate: equity/bonds/cash per regime rules
      3. Equity return = KSE-100 monthly return
      4. Bond return = KIBOR/12 (proxy)
      5. Cash return = SBP rate / 12 (proxy)
    """
    features = load_macro_features(lookback_months)
    if features.empty or len(features) < 24:
        return {"error": "Need at least 24 months of data"}
    
    result = fit_hmm(features)
    regimes = result["regimes"]
    
    # Monthly returns
    kse_returns = features["kse_mom_1m"].fillna(0).values
    kibor_vals = features["kibor"].values / 100 / 12 if "kibor" in features.columns else np.full(len(features), 0.01)
    sbp_vals = features.get("sbp_rate", pd.Series(np.full(len(features), 10))).values / 100 / 12
    
    # Strategy returns
    strategy_returns = []
    bh_returns = []  # buy and hold KSE
    allocations = []
    
    for i in range(len(features)):
        regime = regimes[i]
        alloc = REGIME_ALLOCATION[regime]
        
        equity_ret = kse_returns[i]
        bond_ret = kibor_vals[i] if i < len(kibor_vals) else 0.008
        cash_ret = sbp_vals[i] if i < len(sbp_vals) else 0.007
        
        portfolio_ret = (alloc["equity"] * equity_ret + 
                        alloc["bonds"] * bond_ret + 
                        alloc["cash"] * cash_ret)
        
        strategy_returns.append(portfolio_ret)
        bh_returns.append(equity_ret)
        allocations.append(alloc)
    
    # Compute metrics
    strat = np.array(strategy_returns)
    bh = np.array(bh_returns)
    dates = features["date"].values
    
    strat_cum = np.cumprod(1 + strat)
    bh_cum = np.cumprod(1 + bh)
    
    strat_total = strat_cum[-1] - 1
    bh_total = bh_cum[-1] - 1
    
    strat_vol = strat.std() * np.sqrt(12)
    bh_vol = bh.std() * np.sqrt(12)
    
    strat_sharpe = (strat.mean() * 12) / strat_vol if strat_vol > 0 else 0
    bh_sharpe = (bh.mean() * 12) / bh_vol if bh_vol > 0 else 0
    
    # Max drawdown
    def max_dd(cum_returns):
        peak = np.maximum.accumulate(cum_returns)
        dd = (cum_returns - peak) / peak
        return dd.min()
    
    strat_dd = max_dd(strat_cum)
    bh_dd = max_dd(bh_cum)
    
    # Regime statistics
    regime_returns = {}
    for regime in MacroRegime:
        mask = np.array([r == regime for r in regimes])
        if mask.sum() > 0:
            regime_returns[regime.name] = {
                "months": int(mask.sum()),
                "avg_return": float(strat[mask].mean()),
                "kse_avg": float(bh[mask].mean()),
                "avoided_loss": float(bh[mask].mean() - strat[mask].mean()) if bh[mask].mean() < 0 else 0,
            }
    
    return {
        "equity_curve": pd.DataFrame({
            "date": dates,
            "strategy": strat_cum,
            "buy_hold": bh_cum,
            "regime": [r.name for r in regimes],
        }),
        "allocations": pd.DataFrame({
            "date": dates,
            "equity_pct": [a["equity"] for a in allocations],
            "bonds_pct": [a["bonds"] for a in allocations],
            "cash_pct": [a["cash"] for a in allocations],
            "regime": [r.name for r in regimes],
        }),
        "metrics": {
            "strategy_return": float(strat_total),
            "buyhold_return": float(bh_total),
            "alpha": float(strat_total - bh_total),
            "strategy_vol": float(strat_vol),
            "buyhold_vol": float(bh_vol),
            "strategy_sharpe": float(strat_sharpe),
            "buyhold_sharpe": float(bh_sharpe),
            "strategy_maxdd": float(strat_dd),
            "buyhold_maxdd": float(bh_dd),
            "months_analyzed": len(features),
            "method": result["method"],
        },
        "regime_stats": regime_returns,
        "transition_matrix": result["transition_matrix"],
    }
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_macro.py`:

### Tab 1: Current Regime
```
├── Big regime badge: RISK_ON / RISK_OFF / TRANSITION / CRISIS
│   with color (green/yellow/orange/red) and probability %
├── Regime probability bar chart (all 4 regimes)
├── Current allocation pie chart: Equity X% / Bonds Y% / Cash Z%
├── Feature dashboard cards:
│   ├── KSE-100 Momentum: +X.X%
│   ├── KIBOR Direction: +XX bps
│   ├── PKR Trend: X.X% depreciation
│   ├── Oil Trend: +X.X%
│   └── SBP Cycle: EASING / TIGHTENING / HOLD
├── Transition probabilities: "P(staying RISK_ON) = 85%, P(→TRANSITION) = 12%..."
└── Next regime forecast based on transition matrix
```

### Tab 2: Regime History
```
├── Timeline chart: KSE-100 price with regime-colored background bands
│   (green bands = RISK_ON, red = CRISIS, etc.)
├── Regime duration table: how long each regime lasted
├── Feature evolution chart: 5 features stacked over time
├── Allocation history: stacked area chart (equity/bonds/cash %)
└── Regime frequency pie chart
```

### Tab 3: Backtest
```
├── Lookback period: 2Y / 3Y / 5Y
├── [Run Backtest]
├── Equity curve: Strategy (regime-switching) vs Buy-and-Hold
├── Metrics: Return, Sharpe, MaxDD, Vol for both
├── Alpha highlighted
├── Regime-specific returns table: how did each regime perform?
├── Monthly returns heatmap
├── Drawdown chart comparison
└── "Crisis avoidance" metric: how much loss was prevented in CRISIS months
```

### Tab 4: Transition Matrix & Research
```
├── 4×4 transition matrix heatmap (probability of regime i → j)
├── Average regime duration bars
├── Feature importance: which feature drives regime changes most?
├── SBP rate cycle overlay: rate hikes/cuts vs regime changes
├── Historical crisis timeline: 2008 GFC, 2018 BoP crisis, 2022 floods, COVID
├── Model diagnostics: HMM log-likelihood, BIC, AIC
└── Methodology: HMM theory, Gaussian emissions, EM algorithm
```

### Key chart — KSE with regime bands:
```python
import plotly.graph_objects as go

fig = go.Figure()

# KSE price line
fig.add_trace(go.Scatter(x=df["date"], y=df["kse_close"],
    line=dict(color="#E0E0E0", width=1.5), name="KSE-100"))

# Regime background bands
for regime in MacroRegime:
    mask = df["regime"] == regime.name
    if mask.any():
        starts = df[mask & ~mask.shift(1, fill_value=False)]["date"]
        ends = df[mask & ~mask.shift(-1, fill_value=False)]["date"]
        for s, e in zip(starts, ends):
            fig.add_vrect(x0=s, x1=e, 
                         fillcolor=REGIME_COLORS[regime], opacity=0.15,
                         layer="below", line_width=0)

fig.update_layout(template="plotly_dark", paper_bgcolor="#0B0E11",
                  plot_bgcolor="#0B0E11", height=400)
```

## Step 3: Add to sidebar

```python
st.page_link("page_views/strategy_macro.py", label="Macro Regimes (HMM)", icon="🌍")
```

## Step 4: Install dependencies

```bash
conda activate psx
pip install hmmlearn scikit-learn --break-system-packages 2>/dev/null || pip install hmmlearn scikit-learn
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test feature loading
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.macro_regime_hmm import load_macro_features

df = load_macro_features(lookback_months=60)
print(f'Features loaded: {len(df)} months')
print(f'Columns: {list(df.columns)}')
if not df.empty:
    print(f'Date range: {df[\"date\"].min()} → {df[\"date\"].max()}')
    print(df[['date','kse_mom_1m','kibor_chg','pkr_chg','oil_chg','sbp_cycle']].tail(10).to_string())
"

# Test regime analysis
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.macro_regime_hmm import analyze_regimes

analysis = analyze_regimes(lookback_months=60)
if analysis:
    print(f'Current regime: {analysis.current_regime.name}')
    print(f'Probabilities: {analysis.current_probs}')
    print(f'Method: {analysis.model_params[\"method\"]}')
    print(f'Months analyzed: {analysis.model_params[\"months_analyzed\"]}')
    print(f'\nAvg regime durations (months):')
    for r, d in analysis.regime_durations.items():
        print(f'  {r.name}: {d:.1f}')
    print(f'\nAllocation: {analysis.states[-1].allocation}')
    print(f'\nTransition matrix:')
    print(analysis.transition_matrix.round(2))
else:
    print('Analysis failed')
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.macro_regime_hmm import backtest_regime_allocation

result = backtest_regime_allocation(lookback_months=60)
if 'error' not in result:
    m = result['metrics']
    print(f'=== REGIME STRATEGY vs BUY & HOLD ===')
    print(f'Strategy Return: {m[\"strategy_return\"]:.1%}')
    print(f'Buy&Hold Return: {m[\"buyhold_return\"]:.1%}')
    print(f'Alpha:           {m[\"alpha\"]:+.1%}')
    print(f'Strategy Sharpe: {m[\"strategy_sharpe\"]:.2f}')
    print(f'B&H Sharpe:      {m[\"buyhold_sharpe\"]:.2f}')
    print(f'Strategy MaxDD:  {m[\"strategy_maxdd\"]:.1%}')
    print(f'B&H MaxDD:       {m[\"buyhold_maxdd\"]:.1%}')
    print(f'Method:          {m[\"method\"]}')
    print(f'\nRegime stats:')
    for regime, stats in result['regime_stats'].items():
        print(f'  {regime}: {stats[\"months\"]} months, avg ret {stats[\"avg_return\"]:+.2%}')
else:
    print(result)
"
```

## IMPORTANT NOTES

1. **hmmlearn is optional** — if not installed, falls back to rule-based regime classification
2. **Rule-based fallback is still useful** — it uses the same signals, just without probabilistic inference
3. **Monthly frequency** — regimes change over months, not days. Monthly rebalancing is appropriate
4. **Feature discovery is dynamic** — code tries multiple table names for KIBOR, FX, oil, SBP rate
5. **SBP rate cycle is the #1 driver** — it explains ~40% of KSE variance historically
6. **Proxy returns:** bonds = KIBOR/12, cash = SBP/12 — approximations, not exact
7. **HMM states are UNORDERED** — the code maps them to regime labels by sorting on KSE mean return
8. **Transition matrix** shows persistence — P(RISK_ON → RISK_ON) is typically 85%+
9. **Crisis avoidance** is the key metric — avoiding -20% drawdowns is more valuable than capturing +20% rallies
10. **No TA libraries** — all math in numpy/pandas/sklearn/hmmlearn
11. **Add under STRATEGIES** in sidebar after VWAP Execution
12. **Pakistan historical crises to validate:** 2008 GFC, 2018 BoP crisis (PKR from 110→160), 2020 COVID, 2022 floods + political turmoil (PKR 180→280, rates to 22%)
13. **The model should correctly identify 2022-2023 as CRISIS** and 2025-2026 as RISK_ON (rates falling from 22% to 10.5%)
