"""Unified 3-Layer Signal Score Framework.

Imports from:
  - engine/macro_regime.py (NEW - Layer 1)
  - engine/microstructure.py (EXISTING - VPIN for toxicity context)
  - Replicates/extends logic from tick_analytics.py (Layer 2 & 3)

Creates:
  - IntradayAnchor dataclass (Layer 2)
  - ExecutionDNA dataclass (Layer 3)
  - SignalReport dataclass (combined)
  - compute_signal_score() - the 1-100 composite
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional

from pakfindata.engine.macro_regime import MacroRegime

try:
    from pakfindata.engine.microstructure import compute_vpin
except ImportError:
    compute_vpin = None  # type: ignore[assignment]


# ═════════════════════════════════════════════════════════════════════════════
# LAYER 2: INTRADAY ANCHOR
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class IntradayAnchor:
    """Layer 2 results - intraday structure analysis."""

    # VWAP with bands
    vwap_df: Optional[pd.DataFrame] = None
    vwap_distance_std: Optional[float] = None

    # Volume Profile POC + Value Area
    poc_price: Optional[float] = None
    va_low: Optional[float] = None
    va_high: Optional[float] = None
    poc_distance_pct: Optional[float] = None
    profile_data: Optional[dict] = None

    # Efficiency Ratio
    er_spike_active: bool = False
    er_df: Optional[pd.DataFrame] = None

    # Data source
    data_source: str = "none"  # 'intraday_bars', 'tick_logs', 'none'
    bar_count: int = 0

    # Scoring
    score: int = 0


# ─── PSX Auction Periods ─────────────────────────────────────────────────────

PSX_OPEN_AUCTION = (9 * 60 + 15, 9 * 60 + 30)  # 09:15-09:30
PSX_CLOSE_AUCTION = (15 * 60 + 28, 15 * 60 + 30)  # 15:28-15:30


def filter_auction_periods(df: pd.DataFrame, dt_col: str = "datetime") -> pd.DataFrame:
    """Remove PSX auction periods for cleaner VWAP computation.

    Opening auction: 09:15-09:30, Closing auction: 15:28-15:30.
    """
    df = df.copy()
    dt = pd.to_datetime(df[dt_col])
    minutes = dt.dt.hour * 60 + dt.dt.minute
    mask = ~(
        ((minutes >= PSX_OPEN_AUCTION[0]) & (minutes < PSX_OPEN_AUCTION[1]))
        | ((minutes >= PSX_CLOSE_AUCTION[0]) & (minutes <= PSX_CLOSE_AUCTION[1]))
    )
    return df[mask].copy()


# ─── VWAP with Standard Deviation Bands ──────────────────────────────────────


def compute_vwap_with_bands(
    df: pd.DataFrame, dt_col: str = "datetime"
) -> pd.DataFrame:
    """Anchored VWAP with +/-1sigma and +/-2sigma bands, session-reset daily.

    Args:
        df: DataFrame with columns: datetime/ts, open, high, low, close, volume
            Must be sorted ascending.

    Returns:
        df with added: vwap, vwap_upper_1, vwap_lower_1, vwap_upper_2, vwap_lower_2
    """
    df = df.copy()
    df["session_date"] = pd.to_datetime(df[dt_col]).dt.date

    result_frames: list[pd.DataFrame] = []
    for _date, group in df.groupby("session_date"):
        g = group.copy()
        typical = (g["high"] + g["low"] + g["close"]) / 3
        cum_tp_vol = (typical * g["volume"]).cumsum()
        cum_vol = g["volume"].cumsum().replace(0, np.nan)
        vwap = cum_tp_vol / cum_vol

        # Std-dev bands
        squared_diff = ((typical - vwap) ** 2 * g["volume"]).cumsum()
        vwap_std = np.sqrt(squared_diff / cum_vol)

        g["vwap"] = vwap
        g["vwap_upper_1"] = vwap + vwap_std
        g["vwap_lower_1"] = vwap - vwap_std
        g["vwap_upper_2"] = vwap + 2 * vwap_std
        g["vwap_lower_2"] = vwap - 2 * vwap_std
        result_frames.append(g)

    return pd.concat(result_frames) if result_frames else df


# ─── Volume Profile with POC + Value Area ────────────────────────────────────


def compute_volume_profile_poc(
    df: pd.DataFrame, bins: int = 50, lookback_days: int = 20
) -> dict:
    """Volume at Price with Point of Control (POC) and Value Area (70%).

    Returns dict with: poc, va_low, va_high, profile (list), levels (list)
    """
    empty = {"poc": None, "va_low": None, "va_high": None, "profile": [], "levels": []}

    if df.empty or len(df) < 5:
        return empty

    recent = df.copy()
    if "datetime" in recent.columns:
        cutoff = pd.to_datetime(recent["datetime"]).max() - pd.Timedelta(
            days=lookback_days
        )
        recent = recent[pd.to_datetime(recent["datetime"]) >= cutoff]
    elif "ts" in recent.columns:
        cutoff = pd.to_datetime(recent["ts"]).max() - pd.Timedelta(days=lookback_days)
        recent = recent[pd.to_datetime(recent["ts"]) >= cutoff]

    if recent.empty or len(recent) < 5:
        return empty

    price_min, price_max = float(recent["close"].min()), float(recent["close"].max())
    if price_max - price_min < 0.01:
        return {
            "poc": price_min,
            "va_low": price_min,
            "va_high": price_max,
            "profile": [],
            "levels": [],
        }

    edges = np.linspace(price_min, price_max, bins + 1)
    vol_at_price = np.zeros(bins)

    for i in range(bins):
        mask = (recent["close"] >= edges[i]) & (recent["close"] < edges[i + 1])
        vol_at_price[i] = float(recent.loc[mask, "volume"].sum())

    # POC
    poc_idx = int(np.argmax(vol_at_price))
    poc = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)

    # Value Area (70%)
    total_vol = vol_at_price.sum()
    if total_vol == 0:
        return {
            "poc": poc,
            "va_low": float(price_min),
            "va_high": float(price_max),
            "profile": vol_at_price.tolist(),
            "levels": ((edges[:-1] + edges[1:]) / 2).tolist(),
        }

    sorted_idx = np.argsort(vol_at_price)[::-1]
    cum = 0.0
    va_indices: list[int] = []
    for idx in sorted_idx:
        cum += vol_at_price[idx]
        va_indices.append(int(idx))
        if cum >= total_vol * 0.70:
            break

    va_low = float(edges[min(va_indices)])
    va_high = float(edges[max(va_indices) + 1])

    return {
        "poc": poc,
        "va_low": va_low,
        "va_high": va_high,
        "profile": vol_at_price.tolist(),
        "levels": ((edges[:-1] + edges[1:]) / 2).tolist(),
    }


# ─── Efficiency Ratio ────────────────────────────────────────────────────────


def compute_efficiency_ratio(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """ER = (High - Low) / Volume.

    Spike = ER > 2x rolling mean -> large price move on thin volume.
    """
    df = df.copy()
    df["er"] = (df["high"] - df["low"]) / df["volume"].replace(0, np.nan)
    df["er_ma"] = df["er"].rolling(window, min_periods=5).mean()
    df["er_spike"] = df["er"] > (2 * df["er_ma"])
    return df


# ─── Layer 2 Orchestrator ────────────────────────────────────────────────────


def compute_intraday_anchor(
    intraday_df: pd.DataFrame,
    data_source: str = "intraday_bars",
    dt_col: str = "ts",
) -> IntradayAnchor:
    """Full Layer 2 analysis from intraday bar data.

    Args:
        intraday_df: Intraday bars with columns matching intraday_bars schema
                     (ts, open, high, low, close, volume)
        data_source: Label for data source tracking
        dt_col: Name of datetime column

    Returns:
        IntradayAnchor dataclass with all metrics + score
    """
    result = IntradayAnchor(data_source=data_source)

    if intraday_df.empty or len(intraday_df) < 5:
        result.data_source = "none"
        return result

    df = intraday_df.copy()
    # Normalize datetime column name
    if dt_col != "datetime" and dt_col in df.columns:
        df["datetime"] = pd.to_datetime(df[dt_col])
    elif "datetime" not in df.columns and "ts" in df.columns:
        df["datetime"] = pd.to_datetime(df["ts"])

    result.bar_count = len(df)

    # Filter auction periods for cleaner analysis
    df_clean = filter_auction_periods(df)
    if df_clean.empty:
        df_clean = df  # Fallback if all data is during auction

    # --- VWAP with bands ---
    if all(c in df_clean.columns for c in ["high", "low", "close", "volume"]):
        vwap_df = compute_vwap_with_bands(df_clean)
        result.vwap_df = vwap_df

        # Current distance from VWAP in std devs
        last = vwap_df.iloc[-1]
        if pd.notna(last.get("vwap")) and pd.notna(last.get("vwap_upper_1")):
            vwap_std = last["vwap_upper_1"] - last["vwap"]
            if vwap_std > 0:
                result.vwap_distance_std = round(
                    float((last["close"] - last["vwap"]) / vwap_std), 3
                )

    # --- Volume Profile POC + VA ---
    profile = compute_volume_profile_poc(df_clean)
    result.poc_price = profile.get("poc")
    result.va_low = profile.get("va_low")
    result.va_high = profile.get("va_high")
    result.profile_data = profile

    if result.poc_price and result.poc_price > 0:
        current = float(df["close"].iloc[-1])
        result.poc_distance_pct = round(
            ((current - result.poc_price) / result.poc_price) * 100, 2
        )

    # --- Efficiency Ratio ---
    if all(c in df.columns for c in ["high", "low", "volume"]):
        er_df = compute_efficiency_ratio(df)
        result.er_df = er_df
        result.er_spike_active = bool(er_df["er_spike"].iloc[-1]) if len(er_df) > 0 else False

    # --- Intraday Score (0-33) ---
    score = 0

    # Price near VWAP
    if result.vwap_distance_std is not None:
        d = abs(result.vwap_distance_std)
        if d < 0.5:
            score += 15
        elif d < 1.0:
            score += 10
        elif d < 2.0:
            score += 5

    # Price near POC
    if result.poc_distance_pct is not None:
        d = abs(result.poc_distance_pct)
        if d < 1:
            score += 10
        elif d < 2:
            score += 5

    # Clean price action (no ER spike)
    score += 2 if result.er_spike_active else 8

    result.score = min(score, 33)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# LAYER 3: EXECUTION DNA
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class ExecutionDNA:
    """Layer 3 results - tick-level order flow analysis."""

    has_tick_data: bool = False
    tick_count: int = 0
    days_available: int = 0

    # Trade classification
    buy_pct: float = 50.0
    sell_pct: float = 50.0

    # CVD
    cvd_final: float = 0.0
    cvd_slope: float = 0.0
    cvd_df: Optional[pd.DataFrame] = None

    # OFI
    ofi_df: Optional[pd.DataFrame] = None
    recent_ofi: float = 0.0

    # Block trades
    block_trades: Optional[pd.DataFrame] = None
    block_count: int = 0
    block_bias: int = 0  # +1 buy-heavy, -1 sell-heavy, 0 neutral

    # Cross-market
    reg_cvd: Optional[float] = None
    fut_cvd: Optional[float] = None
    cross_market_divergence: bool = False

    # Session segmentation
    session_ofi: Optional[dict] = None

    # VPIN integration
    vpin_value: Optional[float] = None
    vpin_toxicity: Optional[str] = None

    # Scoring
    score: int = 0


# ─── Lee-Ready Trade Classification ──────────────────────────────────────────


def classify_trades_lee_ready(df: pd.DataFrame) -> pd.DataFrame:
    """Lee-Ready trade classification using tick rule with forward-fill.

    PSX ticks don't have bid/ask, so we use pure tick rule.
    +1 = buyer-initiated, -1 = seller-initiated.
    """
    df = df.copy()
    df["price_diff"] = df["price"].diff()
    df["trade_sign"] = 0
    df.loc[df["price_diff"] > 0, "trade_sign"] = 1
    df.loc[df["price_diff"] < 0, "trade_sign"] = -1

    # Forward-fill zero-change ticks (Lee-Ready continuation rule)
    df["trade_sign"] = df["trade_sign"].replace(0, np.nan).ffill().fillna(0).astype(int)
    return df


# ─── CVD with Slope ──────────────────────────────────────────────────────────


def compute_cvd_with_slope(df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """Cumulative Volume Delta + linear slope for scoring.

    Returns (df_with_cvd, normalized_slope).
    """
    df = df.copy()
    df["signed_vol"] = df["trade_sign"] * df["volume"]
    df["cvd"] = df["signed_vol"].cumsum()

    cvd_values = df["cvd"].values
    if len(cvd_values) < 10:
        return df, 0.0

    x = np.arange(len(cvd_values))
    slope = float(np.polyfit(x, cvd_values, 1)[0])

    total_vol = df["volume"].sum()
    if total_vol > 0:
        normalized_slope = slope / (total_vol / len(cvd_values))
    else:
        normalized_slope = 0.0

    return df, float(normalized_slope)


# ─── OFI per Minute ──────────────────────────────────────────────────────────


def compute_ofi_per_minute(df: pd.DataFrame) -> pd.DataFrame:
    """Order Flow Imbalance per minute, normalized to [-1.0, +1.0]."""
    df = df.copy()
    df["minute"] = pd.to_datetime(df["_ts"]).dt.floor("1min")

    buys = (
        df[df["trade_sign"] == 1].groupby("minute")["volume"].sum().rename("buy_vol")
    )
    sells = (
        df[df["trade_sign"] == -1].groupby("minute")["volume"].sum().rename("sell_vol")
    )

    ofi = pd.DataFrame({"buy_vol": buys, "sell_vol": sells}).fillna(0)
    ofi["total"] = ofi["buy_vol"] + ofi["sell_vol"]
    ofi["ofi"] = (ofi["buy_vol"] - ofi["sell_vol"]) / ofi["total"].replace(0, np.nan)
    ofi["ofi"] = ofi["ofi"].fillna(0).clip(-1.0, 1.0)
    return ofi


# ─── Block Trade Detection ───────────────────────────────────────────────────


def detect_block_trades(df: pd.DataFrame, multiplier: float = 5.0) -> pd.DataFrame:
    """Flag ticks where volume > multiplier x median tick volume."""
    if df.empty:
        return pd.DataFrame()

    median_vol = df["volume"].median()
    if median_vol <= 0:
        return pd.DataFrame()

    threshold = median_vol * multiplier
    return df[df["volume"] >= threshold].copy()


# ─── Session Segmentation OFI ────────────────────────────────────────────────


def compute_session_segmentation(ofi_df: pd.DataFrame) -> dict[str, float]:
    """Break trading day into PSX session segments and compute avg OFI."""
    if ofi_df.empty or "ofi" not in ofi_df.columns:
        return {}

    idx = ofi_df.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.to_datetime(idx)

    minutes = idx.hour * 60 + idx.minute

    segments = {
        "pre_open": (9 * 60 + 15, 9 * 60 + 30),
        "morning": (9 * 60 + 30, 12 * 60),
        "afternoon": (12 * 60, 15 * 60),
        "close": (15 * 60, 15 * 60 + 30),
    }

    result: dict[str, float] = {}
    for name, (start, end) in segments.items():
        mask = (minutes >= start) & (minutes < end)
        seg_ofi = ofi_df.loc[mask, "ofi"]
        result[name] = round(float(seg_ofi.mean()), 3) if len(seg_ofi) > 0 else 0.0
    return result


# ─── Cross-Market CVD ────────────────────────────────────────────────────────


def compute_cross_market_cvd(
    reg_ticks: pd.DataFrame, fut_ticks: pd.DataFrame
) -> dict:
    """Compare CVD between REG (spot) and FUT (futures) markets.

    Divergence: FUT CVD rising while REG flat/falling = smart money signal.
    """
    result: dict = {"reg_cvd": 0.0, "fut_cvd": 0.0, "divergence": False}

    if not reg_ticks.empty:
        reg = classify_trades_lee_ready(reg_ticks)
        reg["signed_vol"] = reg["trade_sign"] * reg["volume"]
        result["reg_cvd"] = float(reg["signed_vol"].sum())

    if not fut_ticks.empty:
        fut = classify_trades_lee_ready(fut_ticks)
        fut["signed_vol"] = fut["trade_sign"] * fut["volume"]
        result["fut_cvd"] = float(fut["signed_vol"].sum())

    # Divergence detection
    if result["reg_cvd"] != 0 and result["fut_cvd"] != 0:
        if result["fut_cvd"] > 0 and result["reg_cvd"] <= 0:
            result["divergence"] = True
        elif result["fut_cvd"] < 0 and result["reg_cvd"] >= 0:
            result["divergence"] = True

    return result


# ─── Recent OFI (auction-filtered) ───────────────────────────────────────────


def _compute_recent_ofi(ofi_df: pd.DataFrame, lookback_minutes: int = 15) -> float:
    """Average OFI over the last N minutes of ACTIVE trading.

    Excludes auction periods and extends lookback if insufficient data.
    Returns 0.0 (neutral) instead of -1.0 on edge cases.
    """
    if ofi_df.empty or "ofi" not in ofi_df.columns:
        return 0.0

    idx = ofi_df.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            idx = pd.to_datetime(idx)
        except Exception:
            # Fallback: last 15 rows unfiltered
            return float(ofi_df["ofi"].tail(lookback_minutes).mean())

    # Filter out auction periods
    minutes = idx.hour * 60 + idx.minute
    active_mask = ~(
        ((minutes >= 9 * 60 + 15) & (minutes < 9 * 60 + 30))
        | ((minutes >= 15 * 60 + 28) & (minutes <= 15 * 60 + 30))
    )
    active_ofi = ofi_df.loc[active_mask]

    if active_ofi.empty:
        return 0.0

    # Last N minutes from active trading
    last_ts = active_ofi.index.max()
    cutoff = last_ts - pd.Timedelta(minutes=lookback_minutes)
    recent = active_ofi.loc[active_ofi.index >= cutoff]

    # Extend lookback if too few data points
    if len(recent) < 5:
        cutoff = last_ts - pd.Timedelta(minutes=30)
        recent = active_ofi.loc[active_ofi.index >= cutoff]

    if recent.empty:
        return 0.0

    return float(recent["ofi"].mean())


# ─── Layer 3 Orchestrator ────────────────────────────────────────────────────


def compute_execution_dna(
    tick_df: pd.DataFrame,
    fut_tick_df: pd.DataFrame | None = None,
) -> ExecutionDNA:
    """Full Layer 3 analysis from tick-level data.

    Args:
        tick_df: Tick logs for REG market with columns from tick_logs table:
                 symbol, market, timestamp, _ts, price, volume, ...
        fut_tick_df: Optional FUT market ticks for cross-market analysis

    Returns:
        ExecutionDNA dataclass with all metrics + score
    """
    result = ExecutionDNA()

    if tick_df.empty or len(tick_df) < 10:
        return result

    result.has_tick_data = True
    result.tick_count = len(tick_df)

    # Unique days
    if "_ts" in tick_df.columns:
        result.days_available = int(
            pd.to_datetime(tick_df["_ts"]).dt.date.nunique()
        )

    # --- Trade Classification ---
    classified = classify_trades_lee_ready(tick_df)

    total_classified = (classified["trade_sign"] != 0).sum()
    if total_classified > 0:
        buy_count = (classified["trade_sign"] == 1).sum()
        result.buy_pct = round(float(buy_count / total_classified * 100), 1)
        result.sell_pct = round(100 - result.buy_pct, 1)

    # --- CVD with slope ---
    cvd_df, slope = compute_cvd_with_slope(classified)
    result.cvd_df = cvd_df
    result.cvd_final = float(cvd_df["cvd"].iloc[-1]) if not cvd_df.empty else 0.0
    result.cvd_slope = round(slope, 4)

    # --- OFI per minute ---
    ofi_df = compute_ofi_per_minute(classified)
    result.ofi_df = ofi_df

    # Recent OFI (last 15 minutes of ACTIVE trading, exclude auctions)
    if not ofi_df.empty:
        result.recent_ofi = round(_compute_recent_ofi(ofi_df), 3)

    # --- Block Trade Detection ---
    blocks = detect_block_trades(classified)
    result.block_trades = blocks
    result.block_count = len(blocks)

    if not blocks.empty:
        buy_blocks = (blocks["trade_sign"] == 1).sum()
        sell_blocks = (blocks["trade_sign"] == -1).sum()
        if buy_blocks > sell_blocks:
            result.block_bias = 1
        elif sell_blocks > buy_blocks:
            result.block_bias = -1

    # --- Session Segmentation ---
    result.session_ofi = compute_session_segmentation(ofi_df)

    # --- Cross-Market ---
    if fut_tick_df is not None and not fut_tick_df.empty:
        cross = compute_cross_market_cvd(tick_df, fut_tick_df)
        result.reg_cvd = cross["reg_cvd"]
        result.fut_cvd = cross["fut_cvd"]
        result.cross_market_divergence = cross["divergence"]

    # --- VPIN (from existing engine) ---
    if compute_vpin is not None and len(classified) >= 50:
        try:
            vpin_input = classified[["price", "volume"]].copy()
            vpin_input.columns = ["close", "volume"]
            vpin_result = compute_vpin(vpin_input)
            result.vpin_value = round(vpin_result.current_vpin, 3)
            if result.vpin_value < 0.4:
                result.vpin_toxicity = "LOW"
            elif result.vpin_value < 0.7:
                result.vpin_toxicity = "MODERATE"
            else:
                result.vpin_toxicity = "TOXIC"
        except Exception:
            pass

    # --- Execution Score (0-33) ---
    score = 0

    # CVD momentum
    if result.cvd_slope > 0.1:
        score += 15
    elif result.cvd_slope > -0.1:
        score += 5

    # Recent OFI
    if result.recent_ofi > 0.3:
        score += 10
    elif result.recent_ofi > 0:
        score += 5

    # Block trade alignment
    if result.block_bias > 0:
        score += 8
    elif result.block_bias == 0:
        score += 4

    result.score = min(score, 33)
    return result


# ═════════════════════════════════════════════════════════════════════════════
# SIGNAL SCORE COMPOSITE
# ═════════════════════════════════════════════════════════════════════════════


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
    signal_score: int = 0
    interpretation: str = ""

    # Data availability
    eod_days: int = 0
    intraday_bars: int = 0
    tick_count: int = 0
    market_is_open: bool = False


def compute_signal_score(
    macro: MacroRegime | None,
    intraday: IntradayAnchor | None,
    execution: ExecutionDNA | None,
) -> int:
    """Composite signal score (1-100). Each layer contributes 0-33 points."""
    macro_score = macro.score if macro else 0

    if intraday and intraday.data_source != "none":
        intra_score = intraday.score
    else:
        intra_score = 16  # Neutral

    if execution and execution.has_tick_data:
        exec_score = execution.score
    else:
        exec_score = 16  # Neutral

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
        return "Neutral - Wait"
    return "Weak - Avoid"


def score_color(score: int) -> str:
    """Color hex for the signal score."""
    if score >= 86:
        return "#C8A96E"  # Gold
    elif score >= 71:
        return "#1D9E75"  # Bright green
    elif score >= 51:
        return "#5DCAA5"  # Green
    elif score >= 31:
        return "#EF9F27"  # Yellow
    return "#E24B4A"  # Red


# ═════════════════════════════════════════════════════════════════════════════
# BATCH SCANNER + ENHANCED SCORING
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class BatchScanResult:
    """Result of scoring a single symbol in batch mode."""

    symbol: str
    sector: str = ""
    sector_name: str = ""
    current_price: float = 0.0
    signal_score: int = 0
    interpretation: str = ""
    macro_score: int = 0
    intraday_score: int = 16
    execution_score: int = 16
    regime: str = "UNKNOWN"
    hurst: float = 0.5
    sma_distance_pct: float = 0.0
    ann_volatility: float = 0.0
    momentum: str = "NEUTRAL"
    vol_confirm: int = 0
    has_intraday: bool = False
    has_ticks: bool = False
    error: str = ""


@dataclass
class ScoringConfig:
    """Configurable scoring weights."""

    macro_weight: float = 0.40
    intraday_weight: float = 0.30
    execution_weight: float = 0.30
    vpin_bonus_max: int = 5
    volume_confirm_max: int = 3
    sector_relative_max: int = 5
    momentum_max: int = 3
    hurst_trending: float = 0.55
    hurst_mean_rev: float = 0.45


DEFAULT_CONFIG = ScoringConfig()


def compute_signal_score_v2(
    macro: MacroRegime | None,
    intraday: IntradayAnchor | None,
    execution: ExecutionDNA | None,
    config: ScoringConfig = DEFAULT_CONFIG,
) -> int:
    """Enhanced composite score with configurable weights."""
    macro_pct = (macro.score / 33) * 100 if macro else 50
    intraday_pct = (
        (intraday.score / 33) * 100
        if intraday and intraday.data_source != "none"
        else 50
    )
    execution_pct = (
        (execution.score / 33) * 100
        if execution and execution.has_tick_data
        else 50
    )

    base = (
        macro_pct * config.macro_weight
        + intraday_pct * config.intraday_weight
        + execution_pct * config.execution_weight
    )

    bonus = 0
    if execution and execution.vpin_value is not None:
        if execution.vpin_value < 0.3:
            bonus += config.vpin_bonus_max
        elif execution.vpin_value > 0.7:
            bonus -= config.vpin_bonus_max

    return int(np.clip(base + bonus, 1, 100))


def adjust_score_sector_relative(
    symbol_score: int,
    sector_avg: float,
    sector_std: float,
) -> int:
    """Adjust signal score relative to sector peers (+5/-3)."""
    if sector_std < 1:
        return symbol_score
    z = (symbol_score - sector_avg) / sector_std
    if z > 1.0:
        return min(symbol_score + 5, 100)
    elif z < -1.0:
        return max(symbol_score - 3, 1)
    return symbol_score


def _compute_momentum(closes: pd.Series) -> tuple[str, int]:
    """20/60 SMA crossover momentum signal. Returns (label, score_adj)."""
    if len(closes) < 62:
        return "NEUTRAL", 0
    sma_20 = closes.rolling(20).mean()
    sma_60 = closes.rolling(60).mean()
    cur_20, prev_20 = float(sma_20.iloc[-1]), float(sma_20.iloc[-2])
    cur_60, prev_60 = float(sma_60.iloc[-1]), float(sma_60.iloc[-2])

    if cur_20 > cur_60 and prev_20 <= prev_60:
        return "BULLISH_CROSS", 3
    elif cur_20 < cur_60 and prev_20 >= prev_60:
        return "BEARISH_CROSS", -3
    elif cur_20 > cur_60:
        return "BULLISH", 1
    else:
        return "BEARISH", -1


def _compute_volume_confirm(df: pd.DataFrame, sma_dist_pct: float) -> int:
    """Volume trend confirmation adjustment (+3/-2)."""
    if len(df) < 40 or "volume" not in df.columns:
        return 0
    vol_20d = float(df["volume"].tail(20).mean())
    vol_40d = float(df["volume"].tail(40).mean())
    if vol_40d <= 0:
        return 0
    ratio = vol_20d / vol_40d
    if ratio > 1.2 and sma_dist_pct > 0:
        return 3
    elif ratio < 0.8 and sma_dist_pct > 0:
        return -2
    elif ratio > 1.2 and sma_dist_pct < 0:
        return -2
    return 0


def batch_score_symbols(
    con,
    symbols: list[str],
    progress_callback=None,
    min_volume: int = 0,
    sector_filter: str = "",
) -> list[BatchScanResult]:
    """Score multiple symbols using bulk EOD loading for performance.

    Args:
        con: Database connection (sqlite3 row_factory or similar)
        symbols: List of symbol strings to score
        progress_callback: Optional callable(current_idx, total) for UI progress
        min_volume: Minimum 20-day average volume filter (0 = no filter)
        sector_filter: Sector name to filter by ("" = all sectors)

    Returns:
        List of BatchScanResult sorted by signal_score descending
    """
    from pakfindata.engine.macro_regime import compute_macro_regime

    results: list[BatchScanResult] = []

    # --- Bulk load sector map ---
    sector_map: dict[str, tuple[str, str]] = {}
    try:
        sector_rows = con.execute(
            "SELECT symbol, sector, sector_name FROM symbols WHERE is_active = 1"
        ).fetchall()
        sector_map = {
            r["symbol"]: (r["sector"] or "", r["sector_name"] or "")
            for r in sector_rows
        }
    except Exception:
        pass

    # --- Apply sector filter ---
    if sector_filter:
        symbols = [
            s for s in symbols
            if sector_map.get(s, ("", ""))[1] == sector_filter
        ]

    if not symbols:
        return results

    # --- Bulk load ALL EOD data in ONE query ---
    cutoff = (pd.Timestamp.now() - pd.DateOffset(years=2)).strftime("%Y-%m-%d")
    placeholders = ",".join(["?"] * len(symbols))

    try:
        all_eod = pd.read_sql_query(
            f"""SELECT symbol, date, open, high, low, close, volume
                FROM eod_ohlcv
                WHERE symbol IN ({placeholders}) AND date >= ?
                ORDER BY symbol, date""",
            con,
            params=symbols + [cutoff],
        )
    except Exception:
        return results

    if all_eod.empty:
        return results

    # --- Pre-filter by minimum volume ---
    if min_volume > 0:
        avg_vol = all_eod.groupby("symbol")["volume"].apply(
            lambda x: x.tail(20).mean()
        )
        liquid = avg_vol[avg_vol >= min_volume].index.tolist()
        all_eod = all_eod[all_eod["symbol"].isin(liquid)]

    grouped = dict(list(all_eod.groupby("symbol")))

    # --- Load KSE-100 index data ---
    index_df = pd.DataFrame()
    for idx_sym in ("KSE100", "KSE-100", "KSEALL"):
        try:
            index_df = pd.read_sql_query(
                "SELECT date, close FROM eod_ohlcv WHERE symbol = ? AND date >= ? ORDER BY date",
                con,
                params=[idx_sym, cutoff],
            )
            if not index_df.empty:
                break
        except Exception:
            continue
    idx = index_df if not index_df.empty else None

    # --- Check intraday/tick availability (bulk) ---
    intraday_syms: set[str] = set()
    tick_syms: set[str] = set()
    try:
        rows = con.execute("SELECT DISTINCT symbol FROM intraday_bars").fetchall()
        intraday_syms = {r["symbol"] for r in rows}
    except Exception:
        pass
    try:
        rows = con.execute("SELECT DISTINCT symbol FROM tick_logs").fetchall()
        tick_syms = {r["symbol"] for r in rows}
    except Exception:
        pass

    # --- Score each symbol ---
    score_symbols = [s for s in symbols if s in grouped]
    total = len(score_symbols)

    for i, sym in enumerate(score_symbols):
        if progress_callback:
            progress_callback(i, total)

        sym_df = grouped[sym]
        if len(sym_df) < 10:
            continue

        try:
            sec_code, sec_name = sector_map.get(sym, ("", ""))
            macro = compute_macro_regime(
                sym_df,
                sym,
                sector=sec_code or None,
                sector_name=sec_name or None,
                index_df=idx,
            )

            # Momentum (20/60 SMA crossover)
            mom_label, mom_adj = _compute_momentum(sym_df["close"])
            # Volume confirmation
            vol_adj = _compute_volume_confirm(sym_df, macro.sma_distance_pct)

            # Composite: macro + neutral L2/L3 + adjustments
            base_score = 1 + macro.score + 16 + 16
            total_score = int(np.clip(base_score + mom_adj + vol_adj, 1, 100))

            results.append(
                BatchScanResult(
                    symbol=sym,
                    sector=sec_code,
                    sector_name=sec_name,
                    current_price=macro.current_price,
                    signal_score=total_score,
                    interpretation=interpret_score(total_score),
                    macro_score=macro.score,
                    regime=macro.regime,
                    hurst=macro.hurst_exponent,
                    sma_distance_pct=macro.sma_distance_pct,
                    ann_volatility=macro.ann_volatility,
                    momentum=mom_label,
                    vol_confirm=vol_adj,
                    has_intraday=sym in intraday_syms,
                    has_ticks=sym in tick_syms,
                )
            )
        except Exception as e:
            results.append(
                BatchScanResult(
                    symbol=sym,
                    error=str(e),
                )
            )

    if progress_callback:
        progress_callback(total, total)

    # --- Sector-relative adjustment ---
    sector_groups: dict[str, list[BatchScanResult]] = {}
    for r in results:
        key = r.sector_name or "Unknown"
        sector_groups.setdefault(key, []).append(r)

    for _sector, group in sector_groups.items():
        if len(group) < 3:
            continue
        scores = [r.signal_score for r in group]
        avg = float(np.mean(scores))
        std = float(np.std(scores))
        for r in group:
            r.signal_score = adjust_score_sector_relative(r.signal_score, avg, std)
            r.interpretation = interpret_score(r.signal_score)

    results.sort(key=lambda r: r.signal_score, reverse=True)
    return results


def batch_results_to_dataframe(results: list[BatchScanResult]) -> pd.DataFrame:
    """Convert batch results to a display-ready DataFrame."""
    records = []
    for r in results:
        records.append(
            {
                "Symbol": r.symbol,
                "Sector": r.sector_name,
                "Price": r.current_price,
                "Score": r.signal_score,
                "Signal": r.interpretation,
                "Macro": r.macro_score,
                "Regime": r.regime,
                "Hurst": r.hurst,
                "SMA %": r.sma_distance_pct,
                "Vol %": r.ann_volatility,
                "Momentum": r.momentum,
                "Intraday": r.has_intraday,
                "Ticks": r.has_ticks,
            }
        )
    return pd.DataFrame(records)
