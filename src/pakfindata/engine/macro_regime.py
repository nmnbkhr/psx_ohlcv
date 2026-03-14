"""Layer 1: Macro Regime Detection from Daily OHLCV.

Entirely new — no existing code covers these metrics.
Implements: Hurst Exponent (R/S), Annualized Volatility, 200-Day SMA,
Circuit Breaker Detection, Fake H/L Warning, KSE-100 Relative Strength.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional

PSX_TRADING_DAYS = 245  # PSX trading days per year


@dataclass
class MacroRegime:
    """Complete macro regime assessment for a symbol."""

    symbol: str
    sector: Optional[str] = None
    sector_name: Optional[str] = None

    # Core metrics
    ann_volatility: float = 0.0  # Annualized log-return volatility (%)
    hurst_exponent: float = 0.5  # R/S Hurst (0-1)
    regime: str = "UNKNOWN"  # TRENDING / MEAN_REVERTING / RANDOM_WALK

    # SMA analysis
    sma_200: float = 0.0
    sma_200_actual_window: int = 0  # May be < 200 if insufficient data
    current_price: float = 0.0
    sma_distance_pct: float = 0.0  # + = above SMA, - = below

    # PSX-specific
    circuit_breaker_dates: list[str] = field(default_factory=list)
    fake_hl_warning: bool = False

    # KSE-100 relative strength
    beta_20d: Optional[float] = None
    alpha_20d: Optional[float] = None

    # Momentum crossover (20/60 SMA)
    momentum_signal: str = "NEUTRAL"  # BULLISH_CROSS / BEARISH_CROSS / BULLISH / BEARISH / NEUTRAL
    momentum_score_adj: int = 0  # ±3

    # Volume confirmation
    vol_confirm_adj: int = 0  # ±3

    # Scoring
    score: int = 0

    # Raw data for charting
    daily_df: Optional[pd.DataFrame] = None
    hurst_rolling: Optional[pd.Series] = None


# ─────────────────────────────────────────────────────────────────────────────
# HURST EXPONENT (R/S METHOD)
# ─────────────────────────────────────────────────────────────────────────────


def hurst_exponent_rs(series: np.ndarray, max_lag: int = 100) -> float:
    """Rescaled Range (R/S) analysis for Hurst Exponent.

    H > 0.55  -> Persistent / Trending
    0.45 < H < 0.55 -> Random walk
    H < 0.45  -> Anti-persistent / Mean-reverting

    Uses log-returns as input series.
    """
    if len(series) < 20:
        return 0.5

    lags = range(2, min(max_lag, len(series) // 2))
    rs_values: list[tuple[int, float]] = []

    for lag in lags:
        chunks = [series[i : i + lag] for i in range(0, len(series) - lag, lag)]
        rs_per_chunk: list[float] = []
        for chunk in chunks:
            if len(chunk) < lag:
                continue
            mean_c = np.mean(chunk)
            devs = np.cumsum(chunk - mean_c)
            R = float(np.max(devs) - np.min(devs))
            S = float(np.std(chunk, ddof=1))
            if S > 1e-10:
                rs_per_chunk.append(R / S)
        if rs_per_chunk:
            rs_values.append((lag, float(np.mean(rs_per_chunk))))

    if len(rs_values) < 5:
        return 0.5

    log_lags = np.log([v[0] for v in rs_values])
    log_rs = np.log([v[1] for v in rs_values])
    H = float(np.polyfit(log_lags, log_rs, 1)[0])
    return float(np.clip(H, 0.0, 1.0))


def classify_regime(hurst: float) -> str:
    """Classify market regime from Hurst exponent."""
    if hurst > 0.55:
        return "TRENDING"
    elif hurst < 0.45:
        return "MEAN_REVERTING"
    return "RANDOM_WALK"


# ─────────────────────────────────────────────────────────────────────────────
# PSX-SPECIFIC DETECTORS
# ─────────────────────────────────────────────────────────────────────────────


def detect_circuit_breakers(
    df: pd.DataFrame, threshold: float = 7.0, lookback: int = 5
) -> list[str]:
    """Flag dates where daily return exceeded +/-threshold%.

    PSX circuit breaker is +/-7.5%, we use 7.0% to catch near-locks too.
    """
    if len(df) < 2:
        return []

    df = df.copy()
    df["daily_return_pct"] = df["close"].pct_change() * 100
    recent = df.tail(lookback)
    flagged = recent[recent["daily_return_pct"].abs() >= threshold]["date"].tolist()
    return [str(d) for d in flagged]


def detect_fake_hl(df: pd.DataFrame) -> bool:
    """Detect if high/low are derived rather than real OHLC.

    If high == max(open, close) AND low == min(open, close) for >95% of rows,
    the data is likely derived.
    """
    if len(df) < 10:
        return False

    clean = df.dropna(subset=["open", "high", "low", "close"])
    if clean.empty:
        return False

    derived_high = clean["high"] == clean[["open", "close"]].max(axis=1)
    derived_low = clean["low"] == clean[["open", "close"]].min(axis=1)
    both_derived = (derived_high & derived_low).mean()
    return bool(both_derived > 0.95)


def compute_relative_strength(
    symbol_returns: pd.Series,
    index_returns: pd.Series,
    window: int = 20,
) -> tuple[float | None, float | None]:
    """Calculate rolling beta and alpha vs KSE-100 index.

    Returns (beta, alpha_annualized_pct) for the most recent window.
    """
    aligned = pd.DataFrame({"stock": symbol_returns, "index": index_returns}).dropna()

    if len(aligned) < window:
        return None, None

    recent = aligned.tail(window)
    cov = recent["stock"].cov(recent["index"])
    var_idx = recent["index"].var()

    if var_idx < 1e-10:
        return None, None

    beta = cov / var_idx
    alpha = recent["stock"].mean() - beta * recent["index"].mean()
    alpha_ann = alpha * PSX_TRADING_DAYS * 100  # annualize as %

    return round(float(beta), 3), round(float(alpha_ann), 2)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────


def compute_macro_regime(
    daily_df: pd.DataFrame,
    symbol: str,
    sector: str | None = None,
    sector_name: str | None = None,
    index_df: pd.DataFrame | None = None,
    lookback_years: int = 2,
) -> MacroRegime:
    """Full Layer 1 analysis.

    Args:
        daily_df: OHLCV DataFrame sorted by date ascending.
                  Columns: date, open, high, low, close, volume
        symbol: Stock symbol
        sector: Sector code from symbols table (optional)
        sector_name: Sector name from symbols table (optional)
        index_df: KSE-100 daily data with 'date' and 'close' columns (optional)
        lookback_years: How many years of history to use

    Returns:
        MacroRegime dataclass with all metrics + score
    """
    result = MacroRegime(symbol=symbol, sector=sector, sector_name=sector_name)

    if daily_df.empty or len(daily_df) < 10:
        return result

    # Trim to lookback window
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=lookback_years)
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= cutoff].sort_values("date").reset_index(drop=True)

    if len(df) < 10:
        return result

    result.daily_df = df
    result.current_price = float(df["close"].iloc[-1])

    # --- Annualized Volatility ---
    log_ret = np.log(df["close"] / df["close"].shift(1)).dropna().values
    if len(log_ret) > 0:
        result.ann_volatility = round(
            float(np.std(log_ret) * np.sqrt(PSX_TRADING_DAYS) * 100), 2
        )

    # --- Hurst Exponent ---
    if len(log_ret) >= 30:
        result.hurst_exponent = round(hurst_exponent_rs(log_ret), 3)
        result.regime = classify_regime(result.hurst_exponent)

        # Rolling Hurst (for chart) — 200-bar window, computed every bar from 100+
        if len(log_ret) > 120:
            rolling_h = []
            for i in range(100, len(log_ret)):
                h = hurst_exponent_rs(log_ret[max(0, i - 200) : i], max_lag=50)
                rolling_h.append(h)
            result.hurst_rolling = pd.Series(
                rolling_h, index=df.index[101 : len(log_ret) + 1]
            )
    else:
        result.regime = "INSUFFICIENT_DATA"

    # --- 200-Day SMA ---
    available = min(200, len(df))
    result.sma_200_actual_window = available
    result.sma_200 = round(float(df["close"].rolling(available).mean().iloc[-1]), 2)
    if result.sma_200 > 0:
        result.sma_distance_pct = round(
            ((result.current_price - result.sma_200) / result.sma_200) * 100, 2
        )

    # --- Circuit Breakers ---
    result.circuit_breaker_dates = detect_circuit_breakers(df)

    # --- Fake H/L Warning ---
    result.fake_hl_warning = detect_fake_hl(df)

    # --- Relative Strength vs KSE-100 ---
    if index_df is not None and not index_df.empty:
        idx = index_df.copy()
        idx["date"] = pd.to_datetime(idx["date"])
        stock_ret = df.set_index("date")["close"].pct_change().dropna()
        idx_ret = idx.set_index("date")["close"].pct_change().dropna()
        result.beta_20d, result.alpha_20d = compute_relative_strength(
            stock_ret, idx_ret
        )

    # --- Momentum Crossover (20/60 SMA) ---
    if len(df) >= 62:
        sma_20 = df["close"].rolling(20).mean()
        sma_60 = df["close"].rolling(60).mean()
        cur_20, prev_20 = float(sma_20.iloc[-1]), float(sma_20.iloc[-2])
        cur_60, prev_60 = float(sma_60.iloc[-1]), float(sma_60.iloc[-2])

        if cur_20 > cur_60 and prev_20 <= prev_60:
            result.momentum_signal = "BULLISH_CROSS"
            result.momentum_score_adj = 3
        elif cur_20 < cur_60 and prev_20 >= prev_60:
            result.momentum_signal = "BEARISH_CROSS"
            result.momentum_score_adj = -3
        elif cur_20 > cur_60:
            result.momentum_signal = "BULLISH"
            result.momentum_score_adj = 1
        else:
            result.momentum_signal = "BEARISH"
            result.momentum_score_adj = -1

    # --- Volume Confirmation ---
    if len(df) >= 40:
        vol_20d = float(df["volume"].tail(20).mean())
        vol_40d = float(df["volume"].tail(40).mean())
        if vol_40d > 0:
            vol_ratio = vol_20d / vol_40d
            if vol_ratio > 1.2 and result.sma_distance_pct > 0:
                result.vol_confirm_adj = 3  # Rising vol + uptrend
            elif vol_ratio < 0.8 and result.sma_distance_pct > 0:
                result.vol_confirm_adj = -2  # Falling vol + uptrend
            elif vol_ratio > 1.2 and result.sma_distance_pct < 0:
                result.vol_confirm_adj = -2  # Rising vol + downtrend

    # --- Macro Score (0-33) ---
    score = 0

    # Trend direction
    if result.hurst_exponent > 0.55 and result.sma_distance_pct > 0:
        score += 15  # Trending UP
    elif result.hurst_exponent > 0.55 and result.sma_distance_pct < 0:
        score += 5  # Trending DOWN
    elif result.hurst_exponent > 0.45:
        score += 3  # Random walk
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

    # Circuit breaker penalty (-2 per hit, max -6)
    if result.circuit_breaker_dates:
        penalty = min(len(result.circuit_breaker_dates) * 2, 6)
        score -= penalty

    # Momentum + volume confirmation adjustments
    score += result.momentum_score_adj
    score += result.vol_confirm_adj

    result.score = max(min(score, 33), 0)
    return result
