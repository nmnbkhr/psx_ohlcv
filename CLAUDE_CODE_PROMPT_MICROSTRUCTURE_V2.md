# Claude Code Prompt: Top-Down Microstructure Analysis — Gap Fill

## Context — What Already Exists (DO NOT REBUILD)

The pakfindata (psx_ohlcv) codebase already has strong microstructure building blocks. **Read these files first** before writing any code:

```
EXISTING — read but DO NOT modify:
  engine/microstructure.py          — VPIN (BVC-based), Maker-Taker game theory (EV_make)
  engine/commentary.py              — Rules-based + AI commentary engine
  ui/page_views/microstructure.py   — VPIN toxicity monitor page (gauge, volume buckets, time series)
  ui/page_views/tick_analytics.py   — CVD, Volume Profile, VWAP overlay, Spread & Liquidity,
                                      Volatility estimators (Realized, Garman-Klass, Parkinson),
                                      Order Imbalance, Futures Basis
  services/tick_service.py          — Real-time WebSocket tick collector (memory → JSONL → SQLite)
  db/repositories/tick_logs.py      — Tick log persistence (JSONL → SQLite backfill)
```

**This prompt fills 9 specific gaps.** It creates 2 new files and touches 0 existing files.

---

## Rules — READ THESE FIRST

1. **CREATE `engine/macro_regime.py`** — Layer 1 analytics (entirely missing, the biggest gap)
2. **CREATE `engine/signal_score.py`** — 3-layer scoring framework + unified report dataclasses
3. **CREATE `ui/page_views/signal_dashboard.py`** — Unified 3-layer analysis page
4. **DO NOT modify any existing files** — zero changes to `microstructure.py`, `tick_analytics.py`, `commentary.py`, `app.py`, or anything else
5. **IMPORT from existing modules** — reuse what's already built (see Reuse Map below)
6. **All new math in pandas/numpy** — no `ta-lib`, no `pandas-ta`, no `scipy`. Implement from scratch.
7. **Use the existing DB layer** — import from `db/repositories/` for queries. Match existing query patterns.
8. **Follow existing UI patterns** — study `tick_analytics.py` and `microstructure.py` for CSS, layout, sidebar, chart styling. Match the Bloomberg dark theme already in use.

---

## Reuse Map — What to Import vs What to Build

| Capability | Status | Action |
|-----------|--------|--------|
| VPIN computation | ✅ EXISTS in `engine/microstructure.py` | **IMPORT** — use for execution layer toxicity signal |
| CVD (tick-rule + cumsum) | ✅ EXISTS in `tick_analytics.py` | **EXTRACT** the CVD function into importable form, or replicate the same logic in `signal_score.py` |
| Volume Profile histogram | ✅ EXISTS in `tick_analytics.py` | **REUSE** the logic but add POC + Value Area (70%) calculation on top |
| VWAP overlay | ✅ EXISTS in `tick_analytics.py` | **REUSE** but add ±1σ / ±2σ standard deviation bands |
| Order Imbalance | ✅ EXISTS in `tick_analytics.py` | **REUSE** but normalize to ±1.0 range for scoring |
| Volatility estimators | ✅ EXISTS in `tick_analytics.py` | **REUSE** realized vol for macro layer |
| Futures Basis | ✅ EXISTS in `tick_analytics.py` | **IMPORT** for cross-market analysis |
| Spread & Liquidity | ✅ EXISTS in `tick_analytics.py` | **IMPORT** Amihud ratio as supplementary signal |
| Commentary engine | ✅ EXISTS in `engine/commentary.py` | **IMPORT** — generate narrative interpretation of the signal score |
| Hurst Exponent | ❌ MISSING | **BUILD** in `engine/macro_regime.py` |
| 200-Day SMA + distance | ❌ MISSING | **BUILD** in `engine/macro_regime.py` |
| Annualized Volatility (regime) | ❌ MISSING | **BUILD** in `engine/macro_regime.py` |
| Circuit Breaker detection | ❌ MISSING | **BUILD** in `engine/macro_regime.py` |
| KSE-100 Relative Strength | ❌ MISSING | **BUILD** in `engine/macro_regime.py` |
| Regime classification labels | ❌ MISSING | **BUILD** in `engine/macro_regime.py` |
| VWAP std-dev bands | ❌ MISSING | **BUILD** in `signal_score.py` (wraps existing VWAP) |
| Volume Profile POC + VA | ❌ MISSING | **BUILD** in `signal_score.py` (extends existing profile) |
| Efficiency Ratio spike | ❌ MISSING | **BUILD** in `signal_score.py` |
| Auction period filtering | ❌ MISSING | **BUILD** in `signal_score.py` |
| Lee-Ready with ffill | ❌ MISSING | **BUILD** in `signal_score.py` (different from existing tick-rule) |
| CVD slope for scoring | ❌ MISSING | **BUILD** in `signal_score.py` |
| Block Trade detection | ❌ MISSING | **BUILD** in `signal_score.py` |
| Cross-Market Flow (REG vs FUT) | ❌ MISSING | **BUILD** in `signal_score.py` |
| Session Segmentation OFI | ❌ MISSING | **BUILD** in `signal_score.py` |
| Signal Score (1-100) | ❌ MISSING | **BUILD** in `signal_score.py` |
| Unified analysis page | ❌ MISSING | **BUILD** in `ui/page_views/signal_dashboard.py` |

---

## GAP 1 + GAP 3: `engine/macro_regime.py` — Layer 1 (Build from scratch)

This is the biggest gap — nothing like it exists in the codebase.

```python
"""
engine/macro_regime.py
Layer 1: Macro Regime Detection from Daily OHLCV

Entirely new — no existing code covers these metrics.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, List

PSX_TRADING_DAYS = 245  # PSX trading days per year

@dataclass
class MacroRegime:
    """Complete macro regime assessment for a symbol."""
    symbol: str
    sector: Optional[str] = None
    
    # Core metrics
    ann_volatility: float = 0.0         # Annualized log-return volatility (%)
    hurst_exponent: float = 0.5         # R/S Hurst (0-1)
    regime: str = "UNKNOWN"             # TRENDING / MEAN_REVERTING / RANDOM_WALK
    
    # SMA analysis
    sma_200: float = 0.0
    sma_200_actual_window: int = 0      # May be < 200 if insufficient data
    current_price: float = 0.0
    sma_distance_pct: float = 0.0       # + = above SMA, - = below
    
    # PSX-specific
    circuit_breaker_dates: List[str] = field(default_factory=list)  # Dates with ±7% moves
    fake_hl_warning: bool = False       # True if H/L appear derived
    
    # KSE-100 relative strength
    beta_20d: Optional[float] = None    # 20-day rolling beta vs KSE-100
    alpha_20d: Optional[float] = None   # 20-day rolling alpha vs KSE-100
    
    # Scoring
    score: int = 0                      # 0-33 contribution to total signal
    
    # Raw data for charting
    daily_df: Optional[pd.DataFrame] = None
    hurst_rolling: Optional[pd.Series] = None


def hurst_exponent_rs(series: np.ndarray, max_lag: int = 100) -> float:
    """
    Rescaled Range (R/S) analysis for Hurst Exponent.
    
    H > 0.55 → Persistent / Trending
    0.45 < H < 0.55 → Random walk
    H < 0.45 → Anti-persistent / Mean-reverting
    
    Uses log-returns as input series.
    """
    if len(series) < 20:
        return 0.5  # insufficient data
    
    lags = range(2, min(max_lag, len(series) // 2))
    rs_values = []
    
    for lag in lags:
        chunks = [series[i:i+lag] for i in range(0, len(series) - lag, lag)]
        rs_per_chunk = []
        for chunk in chunks:
            if len(chunk) < lag:
                continue
            mean_c = np.mean(chunk)
            devs = np.cumsum(chunk - mean_c)
            R = np.max(devs) - np.min(devs)
            S = np.std(chunk, ddof=1)
            if S > 1e-10:
                rs_per_chunk.append(R / S)
        if rs_per_chunk:
            rs_values.append((lag, np.mean(rs_per_chunk)))
    
    if len(rs_values) < 5:
        return 0.5
    
    log_lags = np.log([v[0] for v in rs_values])
    log_rs = np.log([v[1] for v in rs_values])
    H = np.polyfit(log_lags, log_rs, 1)[0]
    return float(np.clip(H, 0.0, 1.0))


def classify_regime(hurst: float) -> str:
    """Classify market regime from Hurst exponent."""
    if hurst > 0.55:
        return "TRENDING"
    elif hurst < 0.45:
        return "MEAN_REVERTING"
    else:
        return "RANDOM_WALK"


def detect_circuit_breakers(df: pd.DataFrame, threshold: float = 7.0, lookback: int = 5) -> List[str]:
    """
    Flag dates where daily return exceeded ±threshold%.
    PSX circuit breaker is ±7.5%, we use 7.0% to catch near-locks too.
    
    Args:
        df: DataFrame with 'date' and 'close' columns, sorted ascending
        threshold: Percentage threshold (default 7.0)
        lookback: Number of recent sessions to check (default 5)
    
    Returns:
        List of date strings where circuit breaker was likely hit
    """
    if len(df) < 2:
        return []
    
    df = df.copy()
    df['daily_return_pct'] = df['close'].pct_change() * 100
    recent = df.tail(lookback)
    
    flagged = recent[recent['daily_return_pct'].abs() >= threshold]['date'].tolist()
    return [str(d) for d in flagged]


def detect_fake_hl(df: pd.DataFrame) -> bool:
    """
    Detect if high/low are derived (fake) rather than real OHLC.
    If high == max(open, close) AND low == min(open, close) for >95% of rows,
    the data is likely derived.
    """
    if len(df) < 10:
        return False
    
    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    derived_high = (df['high'] == df[['open', 'close']].max(axis=1))
    derived_low = (df['low'] == df[['open', 'close']].min(axis=1))
    both_derived = (derived_high & derived_low).mean()
    
    return both_derived > 0.95


def compute_relative_strength(symbol_returns: pd.Series, index_returns: pd.Series, window: int = 20):
    """
    Calculate rolling beta and alpha vs KSE-100 index.
    Returns (beta, alpha) for the most recent window.
    """
    # Align on common dates
    aligned = pd.DataFrame({
        'stock': symbol_returns,
        'index': index_returns
    }).dropna()
    
    if len(aligned) < window:
        return None, None
    
    recent = aligned.tail(window)
    cov = recent['stock'].cov(recent['index'])
    var_idx = recent['index'].var()
    
    if var_idx < 1e-10:
        return None, None
    
    beta = cov / var_idx
    alpha = recent['stock'].mean() - beta * recent['index'].mean()
    # Annualize alpha
    alpha_ann = alpha * PSX_TRADING_DAYS * 100  # as percentage
    
    return round(beta, 3), round(alpha_ann, 2)


def compute_macro_regime(
    daily_df: pd.DataFrame,
    symbol: str,
    sector: Optional[str] = None,
    index_df: Optional[pd.DataFrame] = None,
    lookback_years: int = 2
) -> MacroRegime:
    """
    Full Layer 1 analysis. 
    
    Args:
        daily_df: OHLCV DataFrame sorted by date ascending.
                  Columns: date, open, high, low, close, volume
        symbol: Stock symbol
        sector: Sector from symbols table (optional)
        index_df: KSE-100 daily data for relative strength (optional)
        lookback_years: How many years of history to use
    
    Returns:
        MacroRegime dataclass with all metrics + score
    """
    result = MacroRegime(symbol=symbol, sector=sector)
    
    if daily_df.empty or len(daily_df) < 10:
        return result
    
    # Trim to lookback window
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=lookback_years)
    df = daily_df[pd.to_datetime(daily_df['date']) >= cutoff].copy()
    df = df.sort_values('date').reset_index(drop=True)
    
    if len(df) < 10:
        return result
    
    result.daily_df = df
    result.current_price = float(df['close'].iloc[-1])
    
    # --- Annualized Volatility ---
    log_ret = np.log(df['close'] / df['close'].shift(1)).dropna().values
    result.ann_volatility = round(float(np.std(log_ret) * np.sqrt(PSX_TRADING_DAYS) * 100), 2)
    
    # --- Hurst Exponent ---
    result.hurst_exponent = round(hurst_exponent_rs(log_ret), 3)
    result.regime = classify_regime(result.hurst_exponent)
    
    # --- Rolling Hurst (for chart) ---
    if len(log_ret) > 120:
        rolling_h = []
        for i in range(100, len(log_ret)):
            h = hurst_exponent_rs(log_ret[max(0, i-200):i], max_lag=50)
            rolling_h.append(h)
        result.hurst_rolling = pd.Series(rolling_h, index=df.index[101:len(log_ret)+1])
    
    # --- 200-Day SMA ---
    available = min(200, len(df))
    result.sma_200_actual_window = available
    result.sma_200 = round(float(df['close'].rolling(available).mean().iloc[-1]), 2)
    result.sma_distance_pct = round(
        ((result.current_price - result.sma_200) / result.sma_200) * 100, 2
    )
    
    # --- Circuit Breakers ---
    result.circuit_breaker_dates = detect_circuit_breakers(df)
    
    # --- Fake H/L Warning ---
    result.fake_hl_warning = detect_fake_hl(df)
    
    # --- Relative Strength vs KSE-100 ---
    if index_df is not None and not index_df.empty:
        stock_ret = df.set_index('date')['close'].pct_change().dropna()
        idx_ret = index_df.set_index('date')['close'].pct_change().dropna()
        result.beta_20d, result.alpha_20d = compute_relative_strength(stock_ret, idx_ret)
    
    # --- Macro Score (0-33) ---
    score = 0
    
    # Trend direction: +15 if trending UP, +5 if trending DOWN, +3 if random walk
    if result.hurst_exponent > 0.55 and result.sma_distance_pct > 0:
        score += 15
    elif result.hurst_exponent > 0.55 and result.sma_distance_pct < 0:
        score += 5
    elif result.hurst_exponent > 0.45:
        score += 3
    else:
        score += 10  # Mean reverting — opportunity
    
    # Price vs SMA
    if result.sma_distance_pct > 2:
        score += 10
    elif result.sma_distance_pct > -2:
        score += 5
    
    # Volatility regime (moderate is best)
    if 15 < result.ann_volatility < 40:
        score += 8  # Goldilocks
    elif result.ann_volatility <= 15:
        score += 3  # Too quiet
    else:
        score += 2  # Too volatile
    
    result.score = min(score, 33)
    return result
```

---

## GAP 2 + GAP 5 + GAP 6: `engine/signal_score.py` — 3-Layer Framework + Scoring

This is the unifying layer. It imports from existing modules AND from the new `macro_regime.py`.

```python
"""
engine/signal_score.py
Unified 3-Layer Signal Score Framework

Imports from:
  - engine/macro_regime.py (NEW — Layer 1)
  - engine/microstructure.py (EXISTING — VPIN for toxicity context)
  - Replicates/extends logic from tick_analytics.py (Layer 2 & 3)

Creates:
  - IntradayAnchor dataclass (Layer 2)
  - ExecutionDNA dataclass (Layer 3)
  - SignalReport dataclass (combined)
  - compute_signal_score() — the 1-100 composite
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

# Import Layer 1 (NEW)
from engine.macro_regime import MacroRegime, compute_macro_regime

# Try importing existing capabilities — handle gracefully if paths differ
try:
    from engine.microstructure import compute_vpin  # or whatever the function is named
except ImportError:
    compute_vpin = None  # VPIN available but import path may differ


# ============================================================
# LAYER 2: Intraday Anchor
# ============================================================

@dataclass
class IntradayAnchor:
    """Layer 2 results — intraday structure analysis."""
    # VWAP with bands (EXTENDS existing VWAP)
    vwap_df: Optional[pd.DataFrame] = None  # bars + vwap, upper_1, lower_1, upper_2, lower_2
    vwap_distance_std: Optional[float] = None  # current price distance from VWAP in std devs
    
    # Volume Profile (EXTENDS existing — adds POC + Value Area)
    poc_price: Optional[float] = None       # Point of Control
    va_low: Optional[float] = None          # Value Area low
    va_high: Optional[float] = None         # Value Area high
    poc_distance_pct: Optional[float] = None
    profile_data: Optional[Dict] = None     # For charting
    
    # Efficiency Ratio (NEW)
    er_spike_active: bool = False
    er_series: Optional[pd.Series] = None
    
    # Data source tracking
    data_source: str = 'none'  # 'tick_bars', 'intraday_data', 'none'
    bar_count: int = 0
    
    # Scoring
    score: int = 0


def compute_vwap_with_bands(df: pd.DataFrame) -> pd.DataFrame:
    """
    Anchored VWAP with ±1σ and ±2σ bands — session-reset daily.
    
    EXTENDS the existing VWAP in tick_analytics.py by adding std dev bands.
    The existing VWAP is a simple overlay; this adds statistical context.
    
    Args:
        df: DataFrame with columns: datetime, open, high, low, close, volume
            Must be sorted by datetime ascending.
    
    Returns:
        df with added columns: vwap, vwap_upper_1, vwap_lower_1, vwap_upper_2, vwap_lower_2
    """
    df = df.copy()
    df['session_date'] = pd.to_datetime(df['datetime']).dt.date
    
    result_frames = []
    for date, group in df.groupby('session_date'):
        g = group.copy()
        typical = (g['high'] + g['low'] + g['close']) / 3
        cum_tp_vol = (typical * g['volume']).cumsum()
        cum_vol = g['volume'].cumsum().replace(0, np.nan)
        vwap = cum_tp_vol / cum_vol
        
        # Rolling variance for bands
        squared_diff = ((typical - vwap) ** 2 * g['volume']).cumsum()
        vwap_std = np.sqrt(squared_diff / cum_vol)
        
        g['vwap'] = vwap
        g['vwap_upper_1'] = vwap + vwap_std
        g['vwap_lower_1'] = vwap - vwap_std
        g['vwap_upper_2'] = vwap + 2 * vwap_std
        g['vwap_lower_2'] = vwap - 2 * vwap_std
        result_frames.append(g)
    
    return pd.concat(result_frames) if result_frames else df


def compute_volume_profile_poc(df: pd.DataFrame, bins: int = 50, lookback_days: int = 20) -> Dict:
    """
    Volume at Price with Point of Control (POC) and Value Area (70%).
    
    EXTENDS the existing volume profile in tick_analytics.py.
    Existing version shows the histogram; this adds POC + VA computation.
    
    Returns:
        dict with keys: poc, va_low, va_high, profile (array), levels (array)
    """
    if df.empty:
        return {'poc': None, 'va_low': None, 'va_high': None}
    
    recent = df.copy()
    if 'datetime' in recent.columns:
        cutoff = pd.to_datetime(recent['datetime']).max() - pd.Timedelta(days=lookback_days)
        recent = recent[pd.to_datetime(recent['datetime']) >= cutoff]
    
    if recent.empty or len(recent) < 5:
        return {'poc': None, 'va_low': None, 'va_high': None}
    
    price_min, price_max = recent['close'].min(), recent['close'].max()
    if price_max - price_min < 0.01:
        return {'poc': float(price_min), 'va_low': float(price_min), 'va_high': float(price_max)}
    
    edges = np.linspace(price_min, price_max, bins + 1)
    vol_at_price = np.zeros(bins)
    
    for i in range(bins):
        mask = (recent['close'] >= edges[i]) & (recent['close'] < edges[i + 1])
        vol_at_price[i] = recent.loc[mask, 'volume'].sum()
    
    # POC — price with most volume
    poc_idx = int(np.argmax(vol_at_price))
    poc = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)
    
    # Value Area — 70% of total volume, expanding from POC
    total_vol = vol_at_price.sum()
    if total_vol == 0:
        return {'poc': poc, 'va_low': float(price_min), 'va_high': float(price_max)}
    
    sorted_idx = np.argsort(vol_at_price)[::-1]
    cum = 0.0
    va_indices = []
    for idx in sorted_idx:
        cum += vol_at_price[idx]
        va_indices.append(idx)
        if cum >= total_vol * 0.70:
            break
    
    va_low = float(edges[min(va_indices)])
    va_high = float(edges[max(va_indices) + 1])
    
    levels = ((edges[:-1] + edges[1:]) / 2).tolist()
    
    return {
        'poc': poc, 'va_low': va_low, 'va_high': va_high,
        'profile': vol_at_price.tolist(), 'levels': levels
    }


def compute_efficiency_ratio(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Efficiency Ratio = (High - Low) / Volume per bar.
    Spike = ER > 2× rolling mean → large price move on thin volume (institutional sweep).
    
    ENTIRELY NEW — not in any existing module.
    """
    df = df.copy()
    price_range = df['high'] - df['low']
    df['er'] = price_range / df['volume'].replace(0, np.nan)
    df['er_ma'] = df['er'].rolling(window, min_periods=5).mean()
    df['er_spike'] = df['er'] > (2 * df['er_ma'])
    return df


PSX_AUCTION_OPEN = (9, 15, 9, 30)    # Opening auction: 09:15 - 09:30
PSX_AUCTION_CLOSE = (15, 28, 15, 30)  # Closing auction: 15:28 - 15:30

def filter_auction_periods(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove auction periods from intraday data for cleaner VWAP.
    PSX opening auction: 09:15-09:30, closing auction: 15:28-15:30.
    
    ENTIRELY NEW — not in any existing module.
    """
    df = df.copy()
    dt = pd.to_datetime(df['datetime'])
    time_minutes = dt.dt.hour * 60 + dt.dt.minute
    
    opening_start = 9 * 60 + 15
    opening_end = 9 * 60 + 30
    closing_start = 15 * 60 + 28
    closing_end = 15 * 60 + 30
    
    mask = ~(
        ((time_minutes >= opening_start) & (time_minutes < opening_end)) |
        ((time_minutes >= closing_start) & (time_minutes <= closing_end))
    )
    return df[mask].copy()


# ============================================================
# LAYER 3: Execution DNA
# ============================================================

@dataclass
class ExecutionDNA:
    """Layer 3 results — tick-level order flow analysis."""
    has_tick_data: bool = False
    tick_count: int = 0
    days_available: int = 0
    
    # Trade classification (EXTENDS existing tick-rule in tick_analytics.py)
    buy_pct: float = 50.0
    sell_pct: float = 50.0
    
    # CVD (EXISTS in tick_analytics.py — we add slope for scoring)
    cvd_final: float = 0.0
    cvd_slope: float = 0.0          # NEW — linear regression slope of CVD
    cvd_series: Optional[pd.Series] = None
    
    # OFI (EXISTS in tick_analytics.py — we normalize to ±1.0 range)
    ofi_df: Optional[pd.DataFrame] = None
    recent_ofi: float = 0.0         # Last 15 minutes average
    
    # Block trades (ENTIRELY NEW)
    block_trades: Optional[pd.DataFrame] = None
    block_count: int = 0
    block_bias: int = 0              # +1 buy-heavy, -1 sell-heavy, 0 neutral
    
    # Cross-market (ENTIRELY NEW)
    reg_cvd: Optional[float] = None
    fut_cvd: Optional[float] = None
    cross_market_divergence: bool = False
    
    # Session segmentation (ENTIRELY NEW)
    session_ofi: Optional[Dict] = None  # {'pre_open': x, 'morning': x, 'afternoon': x, 'close': x}
    
    # VPIN integration (from EXISTING engine/microstructure.py)
    vpin_value: Optional[float] = None
    vpin_toxicity: Optional[str] = None  # 'LOW', 'MODERATE', 'HIGH', 'TOXIC'
    
    # Scoring
    score: int = 0


def classify_trades_lee_ready(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lee-Ready trade classification using tick rule.
    
    DIFFERENT from existing tick-rule in tick_analytics.py:
    - Adds forward-fill for zero-change ticks (existing may not do this)
    - Returns trade_sign column: +1 = buy, -1 = sell
    
    PSX ticks don't have bid/ask, so we use pure tick rule.
    """
    df = df.copy()
    df['price_diff'] = df['price'].diff()
    df['trade_sign'] = 0
    df.loc[df['price_diff'] > 0, 'trade_sign'] = 1
    df.loc[df['price_diff'] < 0, 'trade_sign'] = -1
    
    # Forward-fill zero-change ticks (the Lee-Ready continuation rule)
    df['trade_sign'] = df['trade_sign'].replace(0, np.nan).ffill().fillna(0).astype(int)
    
    return df


def compute_cvd_with_slope(df: pd.DataFrame) -> tuple:
    """
    Cumulative Volume Delta + linear slope for scoring.
    
    EXTENDS existing CVD in tick_analytics.py by adding:
    - slope calculation (for momentum direction)
    - normalized slope for cross-symbol comparison
    """
    df = df.copy()
    df['signed_vol'] = df['trade_sign'] * df['volume']
    df['cvd'] = df['signed_vol'].cumsum()
    
    cvd_values = df['cvd'].values
    if len(cvd_values) < 10:
        return df, 0.0
    
    # Linear regression slope of CVD
    x = np.arange(len(cvd_values))
    slope = np.polyfit(x, cvd_values, 1)[0]
    
    # Normalize by total volume for cross-symbol comparison
    total_vol = df['volume'].sum()
    if total_vol > 0:
        normalized_slope = slope / (total_vol / len(cvd_values))
    else:
        normalized_slope = 0.0
    
    return df, float(normalized_slope)


def compute_ofi_per_minute(df: pd.DataFrame) -> pd.DataFrame:
    """
    Order Flow Imbalance per minute, normalized to [-1.0, +1.0].
    
    EXTENDS existing Order Imbalance in tick_analytics.py by:
    - Normalizing to [-1, +1] range (existing may use raw counts)
    - Providing per-minute granularity for heatmap display
    """
    df = df.copy()
    df['minute'] = pd.to_datetime(df['datetime']).dt.floor('1min')
    
    buys = df[df['trade_sign'] == 1].groupby('minute')['volume'].sum().rename('buy_vol')
    sells = df[df['trade_sign'] == -1].groupby('minute')['volume'].sum().rename('sell_vol')
    
    ofi = pd.DataFrame({'buy_vol': buys, 'sell_vol': sells}).fillna(0)
    ofi['total'] = ofi['buy_vol'] + ofi['sell_vol']
    ofi['ofi'] = (ofi['buy_vol'] - ofi['sell_vol']) / ofi['total'].replace(0, np.nan)
    ofi['ofi'] = ofi['ofi'].fillna(0).clip(-1.0, 1.0)
    
    return ofi


def detect_block_trades(df: pd.DataFrame, multiplier: float = 5.0) -> pd.DataFrame:
    """
    Flag ticks where volume > multiplier × median tick volume.
    
    ENTIRELY NEW — not in any existing module.
    Block trades suggest institutional activity.
    """
    median_vol = df['volume'].median()
    if median_vol <= 0:
        return pd.DataFrame()
    
    threshold = median_vol * multiplier
    blocks = df[df['volume'] >= threshold].copy()
    return blocks


def compute_session_segmentation(ofi_df: pd.DataFrame) -> Dict[str, float]:
    """
    Break trading day into segments and compute average OFI per segment.
    
    ENTIRELY NEW — PSX-specific session breakdown:
    - Pre-Open: 09:15-09:30
    - Morning:  09:30-12:00
    - Afternoon: 12:00-15:00
    - Close:    15:00-15:30
    """
    if ofi_df.empty:
        return {}
    
    idx = ofi_df.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.to_datetime(idx)
    
    minutes = idx.hour * 60 + idx.minute
    
    segments = {
        'pre_open':  (9*60+15, 9*60+30),
        'morning':   (9*60+30, 12*60),
        'afternoon': (12*60,   15*60),
        'close':     (15*60,   15*60+30),
    }
    
    result = {}
    for name, (start, end) in segments.items():
        mask = (minutes >= start) & (minutes < end)
        segment_ofi = ofi_df.loc[mask, 'ofi'] if 'ofi' in ofi_df.columns else pd.Series(dtype=float)
        result[name] = round(float(segment_ofi.mean()), 3) if len(segment_ofi) > 0 else 0.0
    
    return result


def compute_cross_market_cvd(reg_ticks: pd.DataFrame, fut_ticks: pd.DataFrame) -> Dict:
    """
    Compare CVD between REG (spot) and FUT (futures) markets.
    
    ENTIRELY NEW — uses futures basis from existing tick_analytics.py concept
    but adds CVD divergence detection.
    
    Divergence: FUT CVD rising while REG CVD flat/falling = smart money accumulating.
    """
    result = {'reg_cvd': 0.0, 'fut_cvd': 0.0, 'divergence': False}
    
    if not reg_ticks.empty:
        reg = classify_trades_lee_ready(reg_ticks)
        reg['signed_vol'] = reg['trade_sign'] * reg['volume']
        result['reg_cvd'] = float(reg['signed_vol'].sum())
    
    if not fut_ticks.empty:
        fut = classify_trades_lee_ready(fut_ticks)
        fut['signed_vol'] = fut['trade_sign'] * fut['volume']
        result['fut_cvd'] = float(fut['signed_vol'].sum())
    
    # Divergence detection
    if result['reg_cvd'] != 0 and result['fut_cvd'] != 0:
        # Futures positive while spot negative/flat = smart money buying
        if result['fut_cvd'] > 0 and result['reg_cvd'] <= 0:
            result['divergence'] = True
        # Futures negative while spot positive = smart money selling
        elif result['fut_cvd'] < 0 and result['reg_cvd'] >= 0:
            result['divergence'] = True
    
    return result


# ============================================================
# SIGNAL SCORE COMPOSITE (GAP 6)
# ============================================================

@dataclass
class SignalReport:
    """Unified 3-layer analysis report."""
    symbol: str
    timestamp: str = ""
    
    # Three layers
    macro: Optional[MacroRegime] = None
    intraday: Optional[IntradayAnchor] = None
    execution: Optional[ExecutionDNA] = None
    
    # Composite
    signal_score: int = 0           # 1-100
    interpretation: str = ""        # Human-readable label
    
    # Data availability
    eod_days: int = 0
    intraday_bars: int = 0
    tick_count: int = 0
    market_is_open: bool = False


def compute_signal_score(macro: MacroRegime, intraday: IntradayAnchor, execution: ExecutionDNA) -> int:
    """
    Composite signal score (1-100).
    Each layer contributes 0-33 points.
    
    ENTIRELY NEW — the core gap in the codebase.
    """
    # === MACRO (0-33) — already computed in macro.score ===
    macro_score = macro.score if macro else 0
    
    # === INTRADAY (0-33) ===
    intra_score = 0
    
    if intraday and intraday.data_source != 'none':
        # Price near VWAP
        if intraday.vwap_distance_std is not None:
            d = abs(intraday.vwap_distance_std)
            if d < 0.5:
                intra_score += 15
            elif d < 1.0:
                intra_score += 10
            elif d < 2.0:
                intra_score += 5
        
        # Price near POC
        if intraday.poc_distance_pct is not None:
            d = abs(intraday.poc_distance_pct)
            if d < 1:
                intra_score += 10
            elif d < 2:
                intra_score += 5
        
        # Clean price action (no ER spike)
        intra_score += 2 if intraday.er_spike_active else 8
    else:
        intra_score = 16  # Neutral when no data
    
    # === EXECUTION (0-33) ===
    exec_score = 0
    
    if execution and execution.has_tick_data:
        # CVD momentum
        if execution.cvd_slope > 0.1:
            exec_score += 15
        elif execution.cvd_slope > -0.1:
            exec_score += 5
        
        # Recent OFI
        if execution.recent_ofi > 0.3:
            exec_score += 10
        elif execution.recent_ofi > 0:
            exec_score += 5
        
        # Block trade alignment
        if execution.block_bias > 0:
            exec_score += 8
        elif execution.block_bias == 0:
            exec_score += 4
    else:
        exec_score = 16  # Neutral when no data
    
    total = 1 + macro_score + min(intra_score, 33) + min(exec_score, 33)
    return min(total, 100)


def interpret_score(score: int) -> str:
    """Human-readable interpretation of the signal score."""
    if score >= 86:
        return "Exceptional Confluence"
    elif score >= 71:
        return "Strong Buy Setup"
    elif score >= 51:
        return "Moderate Buy Setup"
    elif score >= 31:
        return "Neutral — Wait"
    else:
        return "Weak — Avoid"
```

---

## GAP 7: `ui/page_views/signal_dashboard.py` — Unified UI Page

**CRITICAL:** Study these existing files FIRST for styling patterns, sidebar structure, chart theming, and CSS:
- `ui/page_views/tick_analytics.py` — most similar page, has charts + metrics
- `ui/page_views/microstructure.py` — VPIN gauge pattern

**Page structure:**

```
┌─ SIDEBAR ──────────────────┐  ┌─ MAIN AREA ────────────────────────────┐
│                             │  │                                          │
│ Symbol [▼ selectbox]        │  │  SIGNAL SCORE GAUGE (big number)         │
│ [🔍 Run Analysis]           │  │  Macro: X/33 | Intra: X/33 | Exec: X/33│
│                             │  │  "Strong Buy Setup"                      │
│ ── Data Availability ──     │  │                                          │
│ ✅ EOD: 1,247 days          │  │  ═══ LAYER 1: MACRO REGIME ═══          │
│ ✅ Intraday: 892 bars       │  │  [4 metric cards: Hurst, Vol, SMA, ...]  │
│ ⚠️ Ticks: No data          │  │  [2-year price chart + SMA-200]          │
│                             │  │  [Rolling Hurst chart]                   │
│ Market: 🔴 LIVE / Closed    │  │                                          │
│                             │  │  ═══ LAYER 2: INTRADAY ANCHOR ═══       │
│ ── Quick Actions ──         │  │  [VWAP + bands chart]                    │
│ ↗ Open VPIN Page            │  │  [Volume Profile with POC + VA]          │
│ ↗ Open Tick Analytics       │  │  [ER spike indicators]                   │
│                             │  │                                          │
│                             │  │  ═══ LAYER 3: EXECUTION DNA ═══         │
│                             │  │  [CVD chart — 3-day cumulative]          │
│                             │  │  [OFI heatmap per minute]                │
│                             │  │  [Block trade markers]                   │
│                             │  │  [Session segmentation OFI]              │
│                             │  │                                          │
│                             │  │  ═══ METHODOLOGY ═══ (st.expander)      │
│                             │  │  [Explains each metric + scoring]        │
└─────────────────────────────┘  └──────────────────────────────────────────┘
```

**Key implementation notes for the page:**

1. **Symbol selector:** Query the `symbols` table for active symbols. Match existing pattern in tick_analytics.py.

2. **Data queries:** Use the EXISTING db/repositories pattern:
   - EOD: Query `eod_data` (or `eod_ohlcv` — check actual table name) from psx.sqlite
   - Intraday: Query `intraday_data` (or `intraday_bars`) from psx.sqlite
   - Ticks: Query `raw_ticks` from tick_bars.db (check `db/repositories/tick_logs.py` for actual table/column names)
   - **IMPORTANT:** Read the existing repository files to get the ACTUAL table names. The gap analysis notes that spec table names may differ from actual schema.

3. **Charts:** All Plotly with dark theme:
   ```python
   layout = dict(
       template='plotly_dark',
       paper_bgcolor='#0B0E11',
       plot_bgcolor='#0B0E11',
       font=dict(family='JetBrains Mono, monospace'),
       xaxis=dict(gridcolor='#1A1D23'),
       yaxis=dict(gridcolor='#1A1D23'),
   )
   ```

4. **Score gauge:** Use `st.metric` or custom HTML with the gold accent `#C8A96E`:
   ```
   Score 0-30:  🔴 color: #E24B4A
   Score 31-50: 🟡 color: #EF9F27
   Score 51-70: 🟢 color: #5DCAA5
   Score 71-85: 💚 color: #1D9E75
   Score 86-100: ⭐ color: #C8A96E
   ```

5. **Cross-links:** Add sidebar buttons that link to existing VPIN and Tick Analytics pages using `st.page_link` or equivalent.

6. **Commentary integration:** If the existing `engine/commentary.py` exposes a function that takes metrics and returns narrative text, call it to generate an AI-powered interpretation of the signal score. If the API is unclear, skip this and use the `interpret_score()` function instead.

---

## GAP 8: Database Reconciliation

**BEFORE writing any queries, READ these files to confirm actual table/column names:**

```bash
# Check actual EOD table
grep -n "CREATE TABLE" db/*.py db/repositories/*.py | grep -i "eod\|daily"

# Check actual intraday table  
grep -n "CREATE TABLE" db/*.py db/repositories/*.py | grep -i "intraday\|bars"

# Check tick tables
grep -n "CREATE TABLE" db/repositories/tick_logs.py
cat db/repositories/tick_logs.py | head -100

# Check tick_bars.db tables
grep -n "CREATE TABLE" services/tick_service.py
```

The gap analysis warns: "The spec's tables (eod_data, raw_ticks, ohlcv_5s) may differ from actual schema." **You MUST reconcile before writing queries.** Common differences to watch for:

| Spec assumes | Might actually be |
|---|---|
| `eod_data` | `eod_ohlcv` or `daily_ohlcv` |
| `intraday_data` | `intraday_bars` |
| `raw_ticks` in tick_bars.db | `tick_logs` or different columns |
| `ohlcv_5s` in tick_bars.db | `tick_ohlcv` or `bars_5s` |

---

## GAP 9: Edge Cases

Implement these defensive checks in the page:

```python
# 1. Symbol not found
if eod_df.empty:
    st.error(f"No EOD data found for {symbol}. Check the Symbols page.")
    st.stop()

# 2. Insufficient EOD data
if len(eod_df) < 200:
    st.warning(f"Only {len(eod_df)} days of data. SMA uses {len(eod_df)}-day window.")

if len(eod_df) < 30:
    st.warning("Insufficient data for Hurst Exponent (need 30+ days).")

# 3. Fake H/L detection
if macro_result.fake_hl_warning:
    st.warning("⚠️ High/Low values appear derived (max/min of Open,Close). "
               "ATR and volatility calculations may be inaccurate.")

# 4. No tick data
if not TICK_DB_PATH.exists():
    st.info("💡 Layer 3 requires tick data. Start collector: `python services/tick_service.py`")

# 5. Market status detection
import datetime
now_pkt = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5)))
is_weekday = now_pkt.weekday() < 5
is_market_hours = (
    datetime.time(9, 15) <= now_pkt.time() <= datetime.time(15, 30)
)
market_open = is_weekday and is_market_hours

if market_open:
    st.sidebar.markdown("🔴 **LIVE** — Market Open")
else:
    st.sidebar.markdown("⚫ Market Closed")
```

---

## Wire into App

**After creating the 3 files**, add the page to the Streamlit app. Check how existing pages are registered in `ui/app.py`:

```bash
grep -n "page_views\|add_page\|navigation\|pages" ui/app.py | head -20
```

Then add `signal_dashboard` following the same pattern. It should appear in the sidebar as **"Signal Analysis"** or **"Top-Down Analysis"** — between the existing VPIN and Tick Analytics pages.

---

## Verification

```bash
# 1. Engine imports cleanly
python -c "from engine.macro_regime import compute_macro_regime; print('macro OK')"
python -c "from engine.signal_score import compute_signal_score, SignalReport; print('score OK')"

# 2. Run for a liquid stock
python -c "
from engine.macro_regime import compute_macro_regime
import sqlite3, pandas as pd

# Adjust path and table name as needed
conn = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
df = pd.read_sql('SELECT * FROM eod_data WHERE symbol=\"HUBC\" ORDER BY date', conn)
print(f'Loaded {len(df)} rows')

result = compute_macro_regime(df, 'HUBC')
print(f'Hurst: {result.hurst_exponent}')
print(f'Regime: {result.regime}')
print(f'Volatility: {result.ann_volatility}%')
print(f'SMA Distance: {result.sma_distance_pct}%')
print(f'Score: {result.score}/33')
"

# 3. Start Streamlit
streamlit run src/psx_ohlcv/ui/app.py
# → Navigate to Signal Analysis page
# → Select HUBC → Run Analysis
```

---

## Summary

This prompt targets exactly the 9 gaps from the gap analysis:

| Gap | File | What's built |
|-----|------|-------------|
| GAP 1: No standalone engine | `engine/macro_regime.py` | Pure computation, no UI |
| GAP 2: No 3-layer architecture | `engine/signal_score.py` | Dataclasses + framework |
| GAP 3: Macro regime (missing) | `engine/macro_regime.py` | Hurst, SMA, vol, circuit breakers, relative strength |
| GAP 4: Intraday (partial) | `engine/signal_score.py` | VWAP bands, POC+VA, ER spike, auction filter |
| GAP 5: Execution (partial) | `engine/signal_score.py` | Lee-Ready ffill, CVD slope, blocks, cross-market, sessions |
| GAP 6: Signal score (missing) | `engine/signal_score.py` | 1-100 composite with interpretation |
| GAP 7: Unified page (missing) | `ui/page_views/signal_dashboard.py` | Full layout spec |
| GAP 8: Dual-DB queries (missing) | All files | Schema reconciliation instructions |
| GAP 9: Edge cases (missing) | `ui/page_views/signal_dashboard.py` | Defensive checks |

**Zero existing files modified. All new capabilities. Maximum reuse of existing building blocks.**
