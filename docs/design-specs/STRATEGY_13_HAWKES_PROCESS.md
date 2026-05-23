# Claude Code Prompt: Strategy 13 — Hawkes Process for Tick Event Clustering

## Context

pakfindata has 4.6M+ ticks in DuckDB (`tick_logs`) with exact timestamps (ms precision),
bid/ask/price/volume for 500+ PSX symbols. This strategy models tick arrivals as a 
self-exciting Hawkes process to detect activity bursts that predict short-term volatility spikes.

**This is RESEARCH grade — published methodology (Bacry, Mastromatteo & Muzy 2015; Hawkes 1971).**

**The idea:** Trades beget trades. A burst of activity at 11:02 increases the probability 
of more activity at 11:03. This self-excitation decays exponentially. When the excitation 
intensity exceeds a threshold, a volatility spike is imminent — enter a straddle or widen 
stops. When intensity decays back to baseline, the spike is over.

**Why it works on PSX:**
- Low liquidity = bursts are pronounced and persistent (minutes, not microseconds)
- No HFT arbitrage smoothing arrival rates — self-excitation decays slowly
- Institutional orders cluster in time (one fund manager triggers others)
- Circuit breakers at ±7.5% make burst detection actionable (exit before lock)
- 245 trading days, market hours 09:30-15:30 (Fri extended to 16:30)

**Hardware:** RTX 4080 12GB — MLE fitting on 4.6M events takes ~30 seconds with GPU.
CPU fallback works too (~2-5 minutes).

## What already exists

```bash
# Check tick_logs structure and volume
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# Schema
print('=== tick_logs schema ===')
for c in con.execute('DESCRIBE tick_logs').fetchall():
    print(f'  {c[0]}: {c[1]}')

# Volume and date range
print('\n=== tick_logs stats ===')
print(con.execute('''
    SELECT COUNT(*) as total_ticks,
           COUNT(DISTINCT symbol) as symbols,
           COUNT(DISTINCT date) as days,
           MIN(date) as first_date,
           MAX(date) as last_date
    FROM tick_logs
''').df().to_string(index=False))

# Timestamp precision check — need ms or better
print('\n=== Sample timestamps (HUBC) ===')
print(con.execute('''
    SELECT timestamp, price, volume, bid, ask
    FROM tick_logs 
    WHERE symbol = 'HUBC' 
    AND date = (SELECT MAX(date) FROM tick_logs WHERE symbol = 'HUBC')
    ORDER BY timestamp LIMIT 10
''').df().to_string(index=False))

# Inter-arrival distribution sample
print('\n=== Inter-arrival times (HUBC, last day, seconds) ===')
print(con.execute('''
    WITH t AS (
        SELECT timestamp,
               timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS iat
        FROM tick_logs 
        WHERE symbol = 'HUBC' 
        AND date = (SELECT MAX(date) FROM tick_logs WHERE symbol = 'HUBC')
    )
    SELECT 
        COUNT(*) as n_ticks,
        AVG(iat) as mean_iat,
        MEDIAN(iat) as median_iat,
        MIN(iat) as min_iat,
        MAX(iat) as max_iat,
        STDDEV(iat) as std_iat
    FROM t WHERE iat IS NOT NULL AND iat > 0
''').df().to_string(index=False))

con.close()
"

# Check existing microstructure engine
grep -rn "hawkes\|Hawkes\|intensity\|self_excit\|arrival" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -10

# Check existing ADVANCED section in app.py
grep -n "ADVANCED" ~/pakfindata/src/pakfindata/ui/app.py
```

**READ ALL OUTPUT before proceeding.** You need to know:
1. The exact column names in tick_logs (especially timestamp format)
2. How many ticks per symbol per day (determines bucket size)
3. Whether timestamps are epoch seconds, ms, or datetime strings

## Step 1: Create the Hawkes Engine

Create `src/pakfindata/engine/hawkes_process.py`:

```python
"""
Hawkes Process for PSX Tick Event Clustering.

Models tick arrivals as a self-exciting point process where each event
increases the probability of future events, with exponential decay.

Univariate Hawkes intensity:
  λ(t) = μ + Σ α · exp(-β · (t - tᵢ))
  
  where:
    μ = baseline intensity (ticks/second in calm market)
    α = excitation jump per event (how much each tick excites the next)
    β = decay rate (how fast excitation fades)
    tᵢ = timestamps of past events

Key derived metrics:
  - Branching ratio: n = α/β  (fraction of events triggered by others)
    n < 1: stable (subcritical), n → 1: near-critical (explosive bursts)
  - Half-life: ln(2)/β (how long excitation persists in seconds)
  - Current intensity: λ(now) vs μ = how "excited" the market is right now
  - Burst detection: λ(t)/μ > threshold → burst regime

PSX Calibration:
  - Typical μ: 0.1-0.5 ticks/sec for liquid stocks (OGDC, HBL)
  - Typical α: 0.3-0.8 (moderate self-excitation)
  - Typical β: 0.5-2.0 (half-life 0.3-1.4 seconds for HFT, 30-120 seconds for PSX)
  - PSX half-life is MUCH longer than NYSE → bursts are tradeable
  - Trading hours: 09:30-15:30 (21,600 seconds), Fri 09:30-16:30 (25,200 seconds)
  - Circuit breakers: ±7.5% — burst detection can warn of circuit lock
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional
from scipy.optimize import minimize

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245

# Market hours in seconds from midnight (PKT)
MARKET_OPEN = 9 * 3600 + 30 * 60     # 09:30 = 34200
MARKET_CLOSE = 15 * 3600 + 30 * 60   # 15:30 = 55800
FRI_CLOSE = 16 * 3600 + 30 * 60      # 16:30 = 59400 (Fridays)


@dataclass
class HawkesParams:
    """Fitted Hawkes process parameters."""
    mu: float           # baseline intensity (events/second)
    alpha: float        # excitation magnitude
    beta: float         # decay rate (1/seconds)
    
    @property
    def branching_ratio(self) -> float:
        """n = α/β. Fraction of events caused by other events. n<1 = stable."""
        return self.alpha / self.beta if self.beta > 0 else float('inf')
    
    @property
    def half_life(self) -> float:
        """Time (seconds) for excitation to decay by half."""
        return np.log(2) / self.beta if self.beta > 0 else float('inf')
    
    @property
    def avg_cluster_size(self) -> float:
        """Expected number of events per cluster = 1/(1-n)."""
        n = self.branching_ratio
        return 1 / (1 - n) if n < 1 else float('inf')
    
    @property
    def is_stable(self) -> bool:
        """Process is stable (subcritical) if branching ratio < 1."""
        return self.branching_ratio < 1


@dataclass
class HawkesState:
    """Current state of the Hawkes intensity."""
    symbol: str
    timestamp: str
    intensity: float        # λ(now) — current conditional intensity
    baseline: float         # μ — baseline intensity
    excitation: float       # λ(now) - μ — excess intensity from self-excitation
    intensity_ratio: float  # λ(now) / μ — how many × baseline
    regime: str             # "CALM", "ELEVATED", "BURST", "EXPLOSIVE"
    burst_probability: float  # P(burst in next 60s)
    params: HawkesParams
    
    @property
    def is_burst(self) -> bool:
        return self.regime in ("BURST", "EXPLOSIVE")


@dataclass
class BurstEvent:
    """Detected burst event from Hawkes process."""
    symbol: str
    start_time: str
    end_time: str
    peak_intensity: float     # max λ(t) during burst
    peak_ratio: float         # max λ(t)/μ
    duration_seconds: float
    n_events_in_burst: int
    price_at_start: float
    price_at_peak: float
    price_change_pct: float   # price move during burst
    vol_at_start: float       # realized vol before burst
    vol_at_peak: float        # realized vol during burst
    vol_ratio: float          # vol_peak / vol_start


def load_tick_times(
    symbol: str,
    date: str = None,
    lookback_days: int = 1,
) -> pd.DataFrame:
    """
    Load tick timestamps from DuckDB.
    
    Returns DataFrame with columns: timestamp (float, seconds since midnight),
    price, volume, raw_timestamp.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    if date is None:
        date = con.execute(
            "SELECT MAX(date) FROM tick_logs WHERE symbol = ?", [symbol]
        ).fetchone()[0]
        if date is None:
            con.close()
            return pd.DataFrame()
    
    # Determine date range
    if lookback_days > 1:
        dates = con.execute(
            """SELECT DISTINCT date FROM tick_logs 
               WHERE symbol = ? AND date <= ? 
               ORDER BY date DESC LIMIT ?""",
            [symbol, str(date), lookback_days]
        ).fetchall()
        date_list = [d[0] for d in dates]
        date_filter = f"date IN ({','.join(repr(str(d)) for d in date_list)})"
    else:
        date_filter = f"date = '{date}'"
    
    df = con.execute(f"""
        SELECT timestamp, price, volume, date
        FROM tick_logs
        WHERE symbol = ? AND {date_filter}
        ORDER BY timestamp
    """, [symbol]).df()
    con.close()
    
    if df.empty:
        return df
    
    # Convert timestamps to seconds-since-midnight for each day
    # (Hawkes process works in continuous time within a session)
    # Adapt based on actual timestamp format discovered in Step 0
    df["ts_seconds"] = df["timestamp"].astype(float)
    
    return df


def hawkes_log_likelihood(
    params: np.ndarray,
    times: np.ndarray,
    T: float,
) -> float:
    """
    Negative log-likelihood for univariate Hawkes process.
    
    params: [mu, alpha, beta]
    times: event times (sorted, in seconds)
    T: observation window length (seconds)
    
    L = Σ log(λ(tᵢ)) - ∫₀ᵀ λ(t) dt
    
    The integral has closed form:
      ∫₀ᵀ λ(t) dt = μT + (α/β) Σ (1 - exp(-β(T - tᵢ)))
    """
    mu, alpha, beta = params
    
    if mu <= 0 or alpha <= 0 or beta <= 0:
        return 1e10  # invalid params
    if alpha / beta >= 1.0:
        return 1e10  # unstable process
    
    n = len(times)
    if n == 0:
        return mu * T  # no events, just baseline
    
    # Compute log-likelihood using recursive formula (O(n) not O(n²))
    # A(i) = Σ_{j<i} exp(-β(tᵢ - tⱼ)) — recursive: A(i) = exp(-β·Δt)·(1 + A(i-1))
    A = 0.0
    log_lik = 0.0
    
    for i in range(n):
        # λ(tᵢ) = μ + α·A(i)
        lam = mu + alpha * A
        if lam <= 0:
            return 1e10
        log_lik += np.log(lam)
        
        # Update A for next event
        if i < n - 1:
            dt = times[i + 1] - times[i]
            A = np.exp(-beta * dt) * (1.0 + A)
    
    # Integral term: μT + (α/β) Σ (1 - exp(-β(T - tᵢ)))
    integral = mu * T
    for i in range(n):
        integral += (alpha / beta) * (1.0 - np.exp(-beta * (T - times[i])))
    
    return -(log_lik - integral)  # return NEGATIVE log-lik for minimization


def fit_hawkes(
    times: np.ndarray,
    T: float = None,
) -> HawkesParams:
    """
    Fit univariate Hawkes process via MLE.
    
    times: sorted event times in seconds
    T: observation window. If None, uses max(times) - min(times).
    
    Returns fitted HawkesParams.
    """
    if len(times) < 10:
        # Too few events — return baseline-only model
        rate = len(times) / T if T and T > 0 else 0.1
        return HawkesParams(mu=rate, alpha=0.01, beta=1.0)
    
    # Normalize times to start at 0
    t0 = times[0]
    t_norm = times - t0
    if T is None:
        T = t_norm[-1] - t_norm[0]
    else:
        T = T - t0
    
    if T <= 0:
        T = t_norm[-1] + 1.0
    
    # Initial guess: baseline rate, moderate excitation, moderate decay
    n = len(t_norm)
    mu0 = n / T * 0.5  # half the events are baseline
    alpha0 = 0.5
    beta0 = 1.0
    
    # Multiple restarts for robustness
    best_nll = float('inf')
    best_params = HawkesParams(mu=mu0, alpha=alpha0, beta=beta0)
    
    for mu_init in [mu0 * 0.3, mu0, mu0 * 2.0]:
        for alpha_init in [0.2, 0.5, 0.8]:
            for beta_init in [0.5, 1.0, 2.0, 5.0]:
                try:
                    result = minimize(
                        hawkes_log_likelihood,
                        x0=[mu_init, alpha_init, beta_init],
                        args=(t_norm, T),
                        method="L-BFGS-B",
                        bounds=[(1e-6, None), (1e-6, None), (1e-6, None)],
                        options={"maxiter": 500, "ftol": 1e-10},
                    )
                    
                    if result.success and result.fun < best_nll:
                        mu, alpha, beta = result.x
                        # Check stability
                        if alpha / beta < 0.99:
                            best_nll = result.fun
                            best_params = HawkesParams(
                                mu=mu, alpha=alpha, beta=beta
                            )
                except Exception:
                    continue
    
    return best_params


def compute_intensity(
    times: np.ndarray,
    params: HawkesParams,
    eval_times: np.ndarray = None,
    resolution: float = 1.0,
) -> pd.DataFrame:
    """
    Compute conditional intensity λ(t) at specified evaluation points.
    
    If eval_times is None, computes on a regular grid with given resolution (seconds).
    
    Returns DataFrame with: time, intensity, baseline, excitation, ratio, regime.
    """
    if eval_times is None:
        eval_times = np.arange(times[0], times[-1], resolution)
    
    intensities = []
    
    for t in eval_times:
        # λ(t) = μ + Σ_{tᵢ < t} α · exp(-β · (t - tᵢ))
        past = times[times < t]
        if len(past) == 0:
            lam = params.mu
        else:
            # Efficient: only consider events within 10 half-lives
            cutoff = t - 10 * params.half_life
            recent = past[past > cutoff]
            excitation = params.alpha * np.sum(
                np.exp(-params.beta * (t - recent))
            )
            lam = params.mu + excitation
        
        ratio = lam / params.mu if params.mu > 0 else 0
        
        # Regime classification
        if ratio > 5.0:
            regime = "EXPLOSIVE"
        elif ratio > 3.0:
            regime = "BURST"
        elif ratio > 1.5:
            regime = "ELEVATED"
        else:
            regime = "CALM"
        
        intensities.append({
            "time": t,
            "intensity": lam,
            "baseline": params.mu,
            "excitation": lam - params.mu,
            "ratio": ratio,
            "regime": regime,
        })
    
    return pd.DataFrame(intensities)


def detect_bursts(
    intensity_df: pd.DataFrame,
    min_ratio: float = 3.0,
    min_duration: float = 5.0,
    prices: pd.DataFrame = None,
) -> list[dict]:
    """
    Detect burst events from intensity time series.
    
    A burst is a contiguous period where intensity_ratio > min_ratio
    lasting at least min_duration seconds.
    
    Returns list of burst event dicts.
    """
    bursts = []
    in_burst = False
    burst_start = None
    burst_data = []
    
    for _, row in intensity_df.iterrows():
        if row["ratio"] >= min_ratio:
            if not in_burst:
                in_burst = True
                burst_start = row["time"]
                burst_data = []
            burst_data.append(row)
        else:
            if in_burst:
                duration = row["time"] - burst_start
                if duration >= min_duration and len(burst_data) >= 2:
                    peak_row = max(burst_data, key=lambda r: r["intensity"])
                    
                    burst = {
                        "start_time": burst_start,
                        "end_time": row["time"],
                        "duration_seconds": duration,
                        "peak_intensity": peak_row["intensity"],
                        "peak_ratio": peak_row["ratio"],
                        "n_points": len(burst_data),
                    }
                    
                    # Add price context if available
                    if prices is not None and not prices.empty:
                        mask_start = prices["ts_seconds"] <= burst_start
                        mask_peak = (prices["ts_seconds"] >= burst_start) & \
                                    (prices["ts_seconds"] <= row["time"])
                        
                        if mask_start.any():
                            burst["price_at_start"] = prices.loc[
                                mask_start, "price"
                            ].iloc[-1]
                        if mask_peak.any():
                            burst["price_at_peak"] = prices.loc[
                                mask_peak, "price"
                            ].iloc[-1]
                            burst["volume_in_burst"] = prices.loc[
                                mask_peak, "volume"
                            ].sum()
                        
                        if "price_at_start" in burst and "price_at_peak" in burst:
                            burst["price_change_pct"] = (
                                (burst["price_at_peak"] - burst["price_at_start"])
                                / burst["price_at_start"] * 100
                            )
                    
                    bursts.append(burst)
                
                in_burst = False
                burst_data = []
    
    return bursts


def analyze_symbol(
    symbol: str,
    date: str = None,
    intensity_resolution: float = 1.0,
    burst_threshold: float = 3.0,
) -> dict:
    """
    Full Hawkes analysis for a symbol on a given date.
    
    Returns dict with:
      params: fitted HawkesParams
      intensity: DataFrame of λ(t) over the day
      bursts: list of detected bursts
      summary: dict of key metrics
    """
    df = load_tick_times(symbol, date, lookback_days=1)
    
    if df.empty or len(df) < 20:
        return {"error": f"Not enough ticks for {symbol} on {date}"}
    
    times = df["ts_seconds"].values
    T = times[-1] - times[0]
    
    # Fit Hawkes
    params = fit_hawkes(times, T)
    
    # Compute intensity
    intensity = compute_intensity(times, params, resolution=intensity_resolution)
    
    # Detect bursts
    bursts = detect_bursts(intensity, min_ratio=burst_threshold, prices=df)
    
    # Summary statistics
    summary = {
        "symbol": symbol,
        "date": str(date or "latest"),
        "n_ticks": len(df),
        "duration_seconds": T,
        "ticks_per_second": len(df) / T if T > 0 else 0,
        "mu": params.mu,
        "alpha": params.alpha,
        "beta": params.beta,
        "branching_ratio": params.branching_ratio,
        "half_life_seconds": params.half_life,
        "avg_cluster_size": params.avg_cluster_size,
        "is_stable": params.is_stable,
        "n_bursts": len(bursts),
        "time_in_burst_pct": sum(
            b["duration_seconds"] for b in bursts
        ) / T * 100 if T > 0 and bursts else 0,
        "max_intensity_ratio": intensity["ratio"].max(),
        "regime_counts": intensity["regime"].value_counts().to_dict(),
    }
    
    return {
        "params": params,
        "intensity": intensity,
        "bursts": bursts,
        "summary": summary,
        "ticks": df,
    }


def scan_symbols(
    symbols: list[str] = None,
    date: str = None,
    top_n: int = 50,
) -> pd.DataFrame:
    """
    Scan multiple symbols for Hawkes parameters and burst activity.
    
    Returns DataFrame ranked by branching_ratio (most self-exciting first).
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    if date is None:
        date = con.execute("SELECT MAX(date) FROM tick_logs").fetchone()[0]
    
    if symbols is None:
        # Get top N symbols by tick count
        rows = con.execute(f"""
            SELECT symbol, COUNT(*) as n 
            FROM tick_logs WHERE date = '{date}'
            GROUP BY symbol ORDER BY n DESC LIMIT {top_n}
        """).fetchall()
        symbols = [r[0] for r in rows]
    
    con.close()
    
    results = []
    for sym in symbols:
        try:
            analysis = analyze_symbol(sym, date)
            if "error" in analysis:
                continue
            s = analysis["summary"]
            results.append(s)
        except Exception:
            continue
    
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    df = df.sort_values("branching_ratio", ascending=False)
    return df


def backtest_burst_signals(
    symbol: str,
    lookback_days: int = 20,
    burst_threshold: float = 3.0,
    vol_window: int = 20,
) -> dict:
    """
    Backtest: do Hawkes bursts predict subsequent volatility spikes?
    
    For each detected burst, measure:
      - Realized volatility in the 5 minutes BEFORE the burst
      - Realized volatility in the 5 minutes AFTER the burst
      - Price move during and after burst
    
    If after_vol > before_vol consistently, bursts are predictive.
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    dates = con.execute(f"""
        SELECT DISTINCT date FROM tick_logs 
        WHERE symbol = '{symbol}' 
        ORDER BY date DESC LIMIT {lookback_days}
    """).fetchall()
    con.close()
    
    all_bursts = []
    
    for (d,) in dates:
        analysis = analyze_symbol(symbol, str(d), burst_threshold=burst_threshold)
        if "error" in analysis:
            continue
        
        for burst in analysis["bursts"]:
            burst["date"] = str(d)
            
            # Compute pre/post volatility from tick data
            ticks = analysis["ticks"]
            t_start = burst["start_time"]
            t_end = burst["end_time"]
            
            # 5 min = 300 seconds before and after
            pre = ticks[(ticks["ts_seconds"] >= t_start - 300) & 
                        (ticks["ts_seconds"] < t_start)]
            post = ticks[(ticks["ts_seconds"] > t_end) & 
                         (ticks["ts_seconds"] <= t_end + 300)]
            
            if len(pre) > 5:
                pre_ret = pre["price"].pct_change().dropna()
                burst["pre_vol"] = pre_ret.std() * np.sqrt(len(pre_ret)) if len(pre_ret) > 1 else 0
            else:
                burst["pre_vol"] = 0
            
            if len(post) > 5:
                post_ret = post["price"].pct_change().dropna()
                burst["post_vol"] = post_ret.std() * np.sqrt(len(post_ret)) if len(post_ret) > 1 else 0
            else:
                burst["post_vol"] = 0
            
            if burst["pre_vol"] > 0:
                burst["vol_amplification"] = burst["post_vol"] / burst["pre_vol"]
            else:
                burst["vol_amplification"] = 0
            
            all_bursts.append(burst)
    
    if not all_bursts:
        return {"error": "No bursts detected", "symbol": symbol}
    
    df = pd.DataFrame(all_bursts)
    
    # Metrics
    metrics = {
        "symbol": symbol,
        "days_analyzed": len(dates),
        "total_bursts": len(df),
        "bursts_per_day": len(df) / len(dates) if dates else 0,
        "avg_duration_sec": df["duration_seconds"].mean(),
        "avg_peak_ratio": df["peak_ratio"].mean(),
        "avg_vol_amplification": df["vol_amplification"].mean(),
        "pct_vol_increase": (df["vol_amplification"] > 1.0).mean() * 100,
        "avg_price_move_pct": df.get("price_change_pct", pd.Series([0])).abs().mean(),
    }
    
    return {"metrics": metrics, "bursts": df}
```

## Step 2: Create the Streamlit Page

Create `src/pakfindata/ui/page_views/advanced_hawkes.py`:

```python
"""Hawkes Process — tick event clustering & volatility burst detection."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
    "purple": "#BB86FC",
}
DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
    font_color="#c9d1d9", margin=dict(l=20, r=20, t=40, b=20),
)


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.title("🔥 Hawkes Process — Event Clustering")
    st.caption(
        "Self-exciting point process: tick bursts predict volatility spikes. "
        "Research-grade — Bacry et al. (2015)"
    )

    tab1, tab2, tab3, tab4 = st.tabs([
        "Live Intensity", "Burst Backtest", "Cross-Symbol Scanner", "Methodology"
    ])

    # ------------------------------------------------------------------
    # TAB 1: Live Intensity
    # ------------------------------------------------------------------
    with tab1:
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            symbol = st.text_input("Symbol", value="HUBC", key="hawkes_sym")
        with c2:
            resolution = st.selectbox("Resolution (sec)", [0.5, 1.0, 2.0, 5.0], index=1)
        with c3:
            burst_thresh = st.slider("Burst threshold (×μ)", 2.0, 8.0, 3.0, 0.5)

        if st.button("Fit Hawkes Model", type="primary"):
            with st.spinner("Fitting Hawkes process..."):
                try:
                    from pakfindata.engine.hawkes_process import analyze_symbol
                    result = analyze_symbol(
                        symbol.upper(), 
                        intensity_resolution=resolution,
                        burst_threshold=burst_thresh,
                    )
                except ImportError:
                    st.error("Engine not found. Ensure `engine/hawkes_process.py` exists.")
                    return

            if "error" in result:
                st.error(result["error"])
                return

            s = result["summary"]
            params = result["params"]

            # KPI row
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            with k1: _kpi("μ (baseline)", f"{s['mu']:.3f}/s", _C["cyan"])
            with k2: _kpi("α (excitation)", f"{s['alpha']:.3f}", _C["amber"])
            with k3: _kpi("β (decay)", f"{s['beta']:.3f}", _C["text"])
            with k4:
                n = s["branching_ratio"]
                nc = _C["up"] if n < 0.5 else (_C["amber"] if n < 0.8 else _C["down"])
                _kpi("Branching n", f"{n:.2f}", nc)
            with k5: _kpi("Half-life", f"{s['half_life_seconds']:.1f}s", _C["purple"])
            with k6: _kpi("Bursts", str(s["n_bursts"]), _C["down"] if s["n_bursts"] > 3 else _C["text"])

            # Stability warning
            if not s["is_stable"]:
                st.error("⚠️ Process is SUPERCRITICAL (n ≥ 1). Model may be unreliable.")
            elif s["branching_ratio"] > 0.8:
                st.warning(f"Process is near-critical (n={s['branching_ratio']:.2f}). "
                           f"Bursts are highly clustered.")

            # Intensity plot
            intensity = result["intensity"]
            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                vertical_spacing=0.05,
                subplot_titles=["Conditional Intensity λ(t)", "Regime"]
            )

            # Intensity line
            fig.add_trace(go.Scatter(
                x=intensity["time"], y=intensity["intensity"],
                mode="lines", name="λ(t)",
                line=dict(color=_C["cyan"], width=1.5),
            ), row=1, col=1)

            # Baseline
            fig.add_hline(
                y=s["mu"], line_dash="dash", line_color=_C["dim"],
                annotation_text=f"μ = {s['mu']:.3f}", row=1, col=1,
            )

            # Burst threshold
            fig.add_hline(
                y=s["mu"] * burst_thresh, line_dash="dot", line_color=_C["down"],
                annotation_text=f"{burst_thresh}×μ", row=1, col=1,
            )

            # Color-coded regime bar
            regime_colors = {
                "CALM": _C["dim"], "ELEVATED": _C["amber"],
                "BURST": _C["down"], "EXPLOSIVE": "#FF1744",
            }
            for regime, color in regime_colors.items():
                mask = intensity["regime"] == regime
                if mask.any():
                    fig.add_trace(go.Scatter(
                        x=intensity.loc[mask, "time"],
                        y=[1] * mask.sum(),
                        mode="markers", name=regime,
                        marker=dict(color=color, size=6, symbol="square"),
                    ), row=2, col=1)

            fig.update_layout(**PLOT_LAYOUT, height=500, showlegend=True)
            fig.update_yaxes(title_text="λ(t) events/sec", row=1, col=1)
            fig.update_yaxes(visible=False, row=2, col=1)
            st.plotly_chart(fig, use_container_width=True)

            # Burst table
            if result["bursts"]:
                st.markdown(f"**{len(result['bursts'])} burst(s) detected:**")
                bdf = pd.DataFrame(result["bursts"])
                display_cols = [c for c in [
                    "start_time", "duration_seconds", "peak_ratio",
                    "n_points", "price_change_pct", "volume_in_burst",
                ] if c in bdf.columns]
                st.dataframe(bdf[display_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No bursts detected at current threshold.")

    # ------------------------------------------------------------------
    # TAB 2: Burst Backtest
    # ------------------------------------------------------------------
    with tab2:
        c1, c2, c3 = st.columns(3)
        with c1:
            bt_sym = st.text_input("Symbol", value="HUBC", key="hawkes_bt_sym")
        with c2:
            bt_days = st.slider("Lookback days", 5, 60, 20, key="hawkes_bt_days")
        with c3:
            bt_thresh = st.slider("Burst threshold", 2.0, 8.0, 3.0, 0.5, key="hawkes_bt_thresh")

        if st.button("Run Backtest", key="hawkes_bt_run"):
            with st.spinner(f"Analyzing {bt_days} days of {bt_sym}..."):
                try:
                    from pakfindata.engine.hawkes_process import backtest_burst_signals
                    result = backtest_burst_signals(
                        bt_sym.upper(), lookback_days=bt_days,
                        burst_threshold=bt_thresh,
                    )
                except ImportError:
                    st.error("Engine not found.")
                    return

            if "error" in result:
                st.warning(result["error"])
                return

            m = result["metrics"]

            k1, k2, k3, k4 = st.columns(4)
            with k1: _kpi("Days Analyzed", str(m["days_analyzed"]))
            with k2: _kpi("Total Bursts", str(m["total_bursts"]))
            with k3:
                amp = m["avg_vol_amplification"]
                c = _C["up"] if amp > 1.5 else (_C["amber"] if amp > 1.0 else _C["dim"])
                _kpi("Avg Vol Amplification", f"{amp:.2f}×", c)
            with k4:
                pct = m["pct_vol_increase"]
                c = _C["up"] if pct > 60 else _C["dim"]
                _kpi("% Bursts → Vol↑", f"{pct:.0f}%", c)

            st.markdown("---")

            if m["pct_vol_increase"] > 60:
                st.success(
                    f"✅ Hawkes bursts ARE predictive for {bt_sym}: "
                    f"{m['pct_vol_increase']:.0f}% of bursts led to higher volatility. "
                    f"Average amplification: {m['avg_vol_amplification']:.2f}×"
                )
            elif m["pct_vol_increase"] > 50:
                st.info(
                    f"Marginal signal: {m['pct_vol_increase']:.0f}% of bursts "
                    f"led to higher volatility (need >60% for significance)."
                )
            else:
                st.warning(
                    f"Hawkes bursts are NOT predictive for {bt_sym}: "
                    f"only {m['pct_vol_increase']:.0f}% led to higher volatility."
                )

            # Burst detail table
            bdf = result["bursts"]
            if not bdf.empty:
                display_cols = [c for c in [
                    "date", "start_time", "duration_seconds", "peak_ratio",
                    "price_change_pct", "pre_vol", "post_vol", "vol_amplification",
                ] if c in bdf.columns]
                st.dataframe(
                    bdf[display_cols].round(3),
                    use_container_width=True, hide_index=True,
                )

    # ------------------------------------------------------------------
    # TAB 3: Cross-Symbol Scanner
    # ------------------------------------------------------------------
    with tab3:
        c1, c2 = st.columns(2)
        with c1:
            scan_n = st.slider("Top N symbols (by tick count)", 10, 100, 30, key="hawkes_scan_n")
        with c2:
            scan_btn = st.button("Scan Now", key="hawkes_scan_btn")

        if scan_btn:
            with st.spinner(f"Fitting Hawkes for {scan_n} symbols..."):
                try:
                    from pakfindata.engine.hawkes_process import scan_symbols
                    df = scan_symbols(top_n=scan_n)
                except ImportError:
                    st.error("Engine not found.")
                    return

            if df.empty:
                st.info("No results.")
                return

            st.markdown(f"**{len(df)} symbols analyzed** — sorted by branching ratio (most self-exciting first)")

            # Highlight near-critical processes
            def _color_n(val):
                if isinstance(val, (int, float)):
                    if val > 0.8: return "color: #FF5252; font-weight: bold"
                    elif val > 0.5: return "color: #FFB300"
                    else: return "color: #00E676"
                return ""

            display_cols = [c for c in [
                "symbol", "n_ticks", "mu", "alpha", "beta",
                "branching_ratio", "half_life_seconds", "n_bursts",
                "time_in_burst_pct", "max_intensity_ratio",
            ] if c in df.columns]

            styled = df[display_cols].style.map(
                _color_n, subset=["branching_ratio"] if "branching_ratio" in display_cols else []
            ).format({
                "mu": "{:.4f}", "alpha": "{:.3f}", "beta": "{:.3f}",
                "branching_ratio": "{:.3f}", "half_life_seconds": "{:.1f}",
                "time_in_burst_pct": "{:.1f}%", "max_intensity_ratio": "{:.1f}×",
            })
            st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

    # ------------------------------------------------------------------
    # TAB 4: Methodology
    # ------------------------------------------------------------------
    with tab4:
        st.markdown("""
        ### Self-Exciting Hawkes Process
        
        A **Hawkes process** is a point process where each event increases the 
        probability of future events. In financial markets, trades beget trades — 
        a large buy triggers other buyers (momentum), market makers adjust quotes, 
        and stop-loss orders cascade.
        
        **Conditional intensity:**
        
        `λ(t) = μ + Σ α · exp(-β · (t - tᵢ))`
        
        | Parameter | Meaning | PSX Typical |
        |-----------|---------|-------------|
        | **μ** | Baseline arrival rate (ticks/sec in calm market) | 0.1 – 0.5 |
        | **α** | Excitation jump per event | 0.3 – 0.8 |
        | **β** | Decay rate (how fast excitation fades) | 0.5 – 2.0 |
        | **n = α/β** | Branching ratio (fraction caused by others) | 0.3 – 0.8 |
        | **ln(2)/β** | Half-life of excitation (seconds) | 30 – 120 on PSX |
        
        **Key insight for PSX:** On NYSE, half-life is milliseconds (HFT arbitrages 
        excitation instantly). On PSX, half-life is **30-120 seconds** because there 
        are no HFT firms. Bursts are visible, persistent, and tradeable.
        
        ---
        
        ### Regime Classification
        
        | Regime | λ(t) / μ | Meaning | Action |
        |--------|----------|---------|--------|
        | CALM | < 1.5× | Normal trading | Standard sizing |
        | ELEVATED | 1.5 – 3× | Above-average activity | Watch closely |
        | BURST | 3 – 5× | Activity burst — likely news/flow | Widen stops, reduce size |
        | EXPLOSIVE | > 5× | Extreme — possible circuit lock | Exit or hedge |
        
        ---
        
        ### Estimation
        
        Parameters are fitted via **Maximum Likelihood Estimation (MLE)** using 
        L-BFGS-B optimization with multiple restarts. The log-likelihood is computed 
        using the recursive O(n) algorithm (not naive O(n²)).
        
        **Stability constraint:** We require n = α/β < 1 (subcritical). If n ≥ 1, 
        the process is explosive (intensity grows without bound).
        
        ---
        
        ### References
        
        - Hawkes, A.G. (1971). "Spectra of some self-exciting and mutually exciting point processes."
        - Bacry, E., Mastromatteo, I., & Muzy, J.F. (2015). "Hawkes processes in finance."
        - Filimonov, V. & Sornette, D. (2012). "Quantifying reflexivity in financial markets."
        """)

    render_footer()
```

## Step 3: Register in app.py

Add page function (near other ADVANCED functions ~line 575):

```python
def advanced_hawkes_page():
    from pakfindata.ui.page_views.advanced_hawkes import render_page
    render_page()
```

Add to page dict (in the `# ADVANCED` section ~line 878):

```python
        # ADVANCED
        "Order Book Sim":    st.Page(strategy_orderbook_page, title="Order Book Sim",    url_path="orderbook-sim"),
        "Stock Graph (GNN)": st.Page(advanced_gnn_page,       title="Stock Graph (GNN)", url_path="stock-graph-gnn"),
        "Hawkes Process":    st.Page(advanced_hawkes_page,     title="Hawkes Process",    url_path="hawkes-process"),
```

Add to nav_groups (in the `"ADVANCED"` list ~line 907):

```python
        "ADVANCED":        ["Order Book Sim", "Stock Graph (GNN)", "Hawkes Process"],
```

## Step 4: Install dependency

```bash
conda activate psx
pip install scipy  # likely already installed for VPIN strategy
```

No additional packages needed — scipy handles MLE optimization. 
If GPU acceleration is desired later, `tick` or `tick-process` libraries exist but scipy is sufficient.

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test Hawkes fitting
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.hawkes_process import analyze_symbol

result = analyze_symbol('HUBC')
if 'error' in result:
    print(result['error'])
else:
    s = result['summary']
    p = result['params']
    print(f'Symbol: {s[\"symbol\"]}')
    print(f'Date: {s[\"date\"]}')
    print(f'Ticks: {s[\"n_ticks\"]:,}')
    print(f'μ (baseline): {p.mu:.4f} ticks/sec')
    print(f'α (excitation): {p.alpha:.4f}')
    print(f'β (decay): {p.beta:.4f}')
    print(f'Branching ratio n: {p.branching_ratio:.3f}')
    print(f'Half-life: {p.half_life:.1f} seconds')
    print(f'Avg cluster size: {p.avg_cluster_size:.1f}')
    print(f'Stable: {p.is_stable}')
    print(f'Bursts detected: {s[\"n_bursts\"]}')
    print(f'Time in burst: {s[\"time_in_burst_pct\"]:.1f}%')
    print(f'Max intensity ratio: {s[\"max_intensity_ratio\"]:.1f}×')
    print(f'Regimes: {s[\"regime_counts\"]}')
"

# Test scanner
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.hawkes_process import scan_symbols

df = scan_symbols(top_n=20)
print(f'{len(df)} symbols analyzed')
if not df.empty:
    print(df[['symbol','n_ticks','branching_ratio','half_life_seconds','n_bursts']].head(10).to_string())
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.hawkes_process import backtest_burst_signals

result = backtest_burst_signals('HUBC', lookback_days=10)
if 'error' in result:
    print(result)
else:
    m = result['metrics']
    print(f'Days: {m[\"days_analyzed\"]}')
    print(f'Bursts: {m[\"total_bursts\"]}')
    print(f'Avg vol amplification: {m[\"avg_vol_amplification\"]:.2f}×')
    print(f'% bursts → vol increase: {m[\"pct_vol_increase\"]:.0f}%')
    print(f'Avg price move: {m[\"avg_price_move_pct\"]:.2f}%')
"
```

## IMPORTANT NOTES

1. **scipy is the only dependency** — no specialized tick-process or hawkes libraries needed
2. **MLE uses O(n) recursive algorithm** — not naive O(n²), so 10K ticks fits in <1 second
3. **Multi-restart optimization** — 36 initial conditions (3×3×4 grid) for robust fitting
4. **Stability enforced** — rejects fits where branching ratio n ≥ 1 (explosive)
5. **Timestamp format matters** — the `load_tick_times` function needs adaptation based on actual tick_logs timestamp format (epoch seconds vs ms vs datetime). READ the discovery output in Step 0.
6. **PSX half-life is the key metric** — if it's 30-120 seconds, bursts are tradeable. If it's <1 second, this is just noise (unlikely on PSX).
7. **Regime thresholds (1.5×, 3×, 5× baseline)** — calibrate by running scanner across 50 symbols and checking distribution of intensity ratios
8. **Backtest validation:** >60% of bursts should lead to higher post-burst volatility for the signal to be meaningful
9. **Add under ADVANCED** in sidebar after Stock Graph (GNN)
10. **No TA libraries** — all math in numpy/scipy/pandas
11. **GPU not needed for fitting** — scipy L-BFGS-B is CPU only. GPU would help for computing intensity over millions of eval points, but 1-second resolution over a 6-hour session = only 21,600 points
12. **Future extension:** Multivariate Hawkes (cross-symbol excitation: OGDC tick triggers PPL tick) — requires different likelihood function but same framework
