"""Market Microstructure Engine — VPIN & Maker-Taker Game Theory.

Implements:
  - Volume bucketing (time-bars → volume-bars)
  - Bulk Volume Classification (BVC) for trade signing
  - VPIN (Volume-Synchronized Probability of Informed Trading)
  - Maker-Taker expected-value payoff matrix
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import norm


# ═════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class VPINResult:
    """Result of a VPIN computation."""
    buckets: pd.DataFrame       # volume buckets with V_buy, V_sell, imbalance
    vpin_series: pd.Series      # rolling VPIN values
    current_vpin: float         # latest VPIN reading
    dominant_bucket_size: int   # V used for bucketing


@dataclass
class PayoffResult:
    """Maker-Taker game theory evaluation."""
    vpin: float                 # current toxicity (π)
    half_spread: float          # s — profit for providing liquidity
    adverse_loss: float         # L — adverse selection loss
    ev_make: float              # expected value of making
    market_state: str           # "Normal", "Transitional", "Toxic"
    optimal_strategy: str       # "MAKER: Post Limit" or "TAKER: Cross Spread"


# ═════════════════════════════════════════════════════════════════════════════
# VOLUME BUCKETING
# ═════════════════════════════════════════════════════════════════════════════

def volume_bucket(df: pd.DataFrame, bucket_size: int) -> pd.DataFrame:
    """Slice tick/bar data into equal-volume buckets.

    Parameters
    ----------
    df : DataFrame with columns ['close', 'volume'] (and optionally 'datetime').
    bucket_size : V — the target volume per bucket.

    Returns
    -------
    DataFrame with one row per bucket: bucket_id, close (VWAP or last),
    volume, datetime (end of bucket).
    """
    records: list[dict] = []
    cum_vol = 0
    bucket_id = 0
    bucket_prices: list[float] = []
    bucket_volumes: list[float] = []
    bucket_end = None

    for _, row in df.iterrows():
        remaining = float(row["volume"])
        price = float(row["close"])
        bucket_end = row.get("datetime", row.name)

        while remaining > 0:
            space = bucket_size - cum_vol
            fill = min(remaining, space)
            cum_vol += fill
            remaining -= fill
            bucket_prices.append(price)
            bucket_volumes.append(fill)

            if cum_vol >= bucket_size:
                # Close this bucket — weighted-average price
                bp = np.array(bucket_prices)
                bv = np.array(bucket_volumes)
                vwap = np.average(bp, weights=bv)
                records.append({
                    "bucket_id": bucket_id,
                    "close": vwap,
                    "volume": bucket_size,
                    "datetime": bucket_end,
                })
                bucket_id += 1
                cum_vol = 0
                bucket_prices = []
                bucket_volumes = []

    # Discard partial trailing bucket (standard practice)
    return pd.DataFrame(records)


# ═════════════════════════════════════════════════════════════════════════════
# BULK VOLUME CLASSIFICATION (BVC)
# ═════════════════════════════════════════════════════════════════════════════

def classify_volume(buckets: pd.DataFrame) -> pd.DataFrame:
    """Apply BVC to assign buy/sell volume per bucket.

    Uses Z(ΔP / σ_ΔP) from the standard normal CDF.

    Adds columns: delta_p, z_score, buy_pct, V_buy, V_sell, imbalance.
    """
    df = buckets.copy()
    df["delta_p"] = df["close"].diff()
    # First bucket has no delta — treat as neutral
    df["delta_p"] = df["delta_p"].fillna(0.0)

    sigma = df["delta_p"].std()
    if sigma == 0 or np.isnan(sigma):
        sigma = 1e-8  # prevent division by zero

    df["z_score"] = df["delta_p"] / sigma
    df["buy_pct"] = df["z_score"].apply(norm.cdf)  # Φ(z)
    df["V_buy"] = df["volume"] * df["buy_pct"]
    df["V_sell"] = df["volume"] * (1.0 - df["buy_pct"])
    df["imbalance"] = (df["V_buy"] - df["V_sell"]).abs()
    return df


# ═════════════════════════════════════════════════════════════════════════════
# VPIN CALCULATION
# ═════════════════════════════════════════════════════════════════════════════

def compute_vpin(
    df: pd.DataFrame,
    bucket_size: int | None = None,
    window: int = 50,
) -> VPINResult:
    """End-to-end VPIN computation.

    Parameters
    ----------
    df : DataFrame with ['close', 'volume'] columns (tick or bar data).
    bucket_size : V for volume bucketing.  If None, auto-set to
                  total_volume / 200 (creates ~200 buckets).
    window : n — rolling window of buckets for VPIN.

    Returns
    -------
    VPINResult with buckets DataFrame, rolling VPIN series, and current value.
    """
    if bucket_size is None:
        total_vol = df["volume"].sum()
        bucket_size = max(int(total_vol / 200), 1)

    buckets = volume_bucket(df, bucket_size)
    if len(buckets) < 2:
        # Not enough data for meaningful VPIN
        return VPINResult(
            buckets=buckets,
            vpin_series=pd.Series(dtype=float),
            current_vpin=0.0,
            dominant_bucket_size=bucket_size,
        )

    classified = classify_volume(buckets)

    # Rolling VPIN = Σ|V_buy - V_sell| / (n * V)
    rolling_imbalance = classified["imbalance"].rolling(window=window, min_periods=1).sum()
    denominator = window * bucket_size
    vpin_series = rolling_imbalance / denominator
    vpin_series = vpin_series.clip(0.0, 1.0)

    classified["vpin"] = vpin_series.values

    return VPINResult(
        buckets=classified,
        vpin_series=vpin_series.reset_index(drop=True),
        current_vpin=float(vpin_series.iloc[-1]),
        dominant_bucket_size=bucket_size,
    )


# ═════════════════════════════════════════════════════════════════════════════
# MAKER-TAKER PAYOFF MATRIX
# ═════════════════════════════════════════════════════════════════════════════

def evaluate_payoff(
    vpin: float,
    half_spread: float = 0.5,
    adverse_loss: float = 2.0,
) -> PayoffResult:
    """Evaluate the Maker-Taker game given current toxicity.

    EV_make = (1 - π) * s  -  π * L

    Parameters
    ----------
    vpin : π — probability of informed trading (0..1).
    half_spread : s — profit captured when filling against noise trader.
    adverse_loss : L — loss when an informed trader runs over your limit order.
    """
    ev = (1.0 - vpin) * half_spread - vpin * adverse_loss

    # Strategy determined by EV sign, not VPIN thresholds
    if ev > 0:
        state = "Normal"
        strategy = "MAKER: Post Limit"
    elif ev < 0:
        state = "Toxic"
        strategy = "TAKER: Cross Spread"
    else:
        state = "Transitional"
        strategy = "NEUTRAL: Widen Spread"

    return PayoffResult(
        vpin=vpin,
        half_spread=half_spread,
        adverse_loss=adverse_loss,
        ev_make=ev,
        market_state=state,
        optimal_strategy=strategy,
    )


def build_payoff_table(
    vpin: float,
    half_spread: float = 0.5,
    adverse_loss: float = 2.0,
) -> pd.DataFrame:
    """Build the full payoff matrix table for display.

    Returns a DataFrame with columns:
      Market State | VPIN (π) | EV_make | Optimal Strategy
    Rows for Normal, Transitional, Toxic + a highlighted "Current" row.
    """
    scenarios = [
        ("Normal / Noise",       0.1),
        ("Transitional",         0.5),
        ("Toxic / Informed",     0.9),
        ("▶ Current",            vpin),
    ]
    rows = []
    for label, pi in scenarios:
        ev = (1.0 - pi) * half_spread - pi * adverse_loss
        rows.append({
            "Market State": label,
            "VPIN (π)": round(pi, 3),
            "EV_make": round(ev, 4),
        })
    df = pd.DataFrame(rows)

    # Strategy determined by EV sign (vectorized via np.select)
    conditions = [df["EV_make"] > 0, df["EV_make"] < 0]
    choices = ["MAKER: Post Limit", "TAKER: Cross Spread"]
    df["Optimal Strategy"] = np.select(conditions, choices, default="NEUTRAL: Widen Spread")
    return df


# ═════════════════════════════════════════════════════════════════════════════
# DUMMY DATA GENERATOR (for testing / demo)
# ═════════════════════════════════════════════════════════════════════════════

def generate_dummy_tick_data(
    n_bars: int = 2000,
    base_price: float = 150.0,
    avg_volume: int = 50000,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate realistic dummy intraday bar data.

    Simulates a stock with:
    - Random-walk price with mean-reversion
    - Volume spikes at open/close (U-shaped volume curve)
    - An informed-trading event in the last 20% of bars
    """
    rng = np.random.default_rng(seed)

    # Price: random walk with slight mean-reversion
    returns = rng.normal(0, 0.002, n_bars)
    # Inject an informed-trading burst in the last 20%
    burst_start = int(n_bars * 0.8)
    returns[burst_start:burst_start + 30] += 0.005  # sustained buying pressure

    prices = base_price * np.exp(np.cumsum(returns))

    # Volume: U-shaped intraday pattern + noise
    t = np.linspace(0, 1, n_bars)
    u_shape = 1.5 - 2.0 * t * (1 - t)  # higher at open & close
    volumes = (avg_volume * u_shape * rng.uniform(0.5, 1.5, n_bars)).astype(int)
    # Spike volume during informed burst
    volumes[burst_start:burst_start + 30] = (volumes[burst_start:burst_start + 30] * 3)

    # Timestamps (1-min bars over a trading day)
    timestamps = pd.date_range("2026-03-10 09:30", periods=n_bars, freq="1min")

    return pd.DataFrame({
        "datetime": timestamps[:n_bars],
        "close": prices,
        "volume": volumes,
    })
