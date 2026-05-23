"""Volume Profile Engine — institutional-grade analytics from tick data.

Functions:
    compute_volume_profile  — Price-volume profile with POC and Value Area
    compute_vwap_bands      — VWAP with ±1σ/±2σ bands
    compute_cumulative_delta — Running buy-sell delta
    compute_tpo_profile     — Time Price Opportunity (Market Profile)
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _tick_volume(volumes: pd.Series) -> pd.Series:
    """Convert cumulative volume to per-tick volume."""
    diff = volumes.diff().fillna(0).clip(lower=0)
    return diff


def _classify_trades(prices: pd.Series, bids: pd.Series | None, asks: pd.Series | None) -> pd.Series:
    """Classify trades as buy (+1) or sell (-1) using Lee-Ready algorithm."""
    sides = pd.Series(0, index=prices.index, dtype=int)

    if bids is not None and asks is not None:
        # Lee-Ready: price >= ask → buy, price <= bid → sell
        sides[prices >= asks] = 1
        sides[prices <= bids] = -1

    # Tick rule for remaining (price == prev → same, else direction)
    mask = sides == 0
    price_diff = prices.diff()
    last_side = 0
    for i in mask[mask].index:
        d = price_diff.get(i, 0)
        if d > 0:
            last_side = 1
        elif d < 0:
            last_side = -1
        sides.at[i] = last_side if last_side != 0 else 1

    return sides


def compute_volume_profile(
    df: pd.DataFrame,
    price_col: str = "price",
    volume_col: str = "volume",
    tick_size: float = 0.0,
    value_area_pct: float = 0.70,
) -> dict:
    """Compute volume profile from tick data.

    Returns dict with keys: levels, poc, vah, val, total_volume, buy_volume, sell_volume.
    """
    prices = df[price_col].dropna()
    if prices.empty or len(prices) < 5:
        return {"levels": [], "poc": 0, "vah": 0, "val": 0,
                "total_volume": 0, "buy_volume": 0, "sell_volume": 0}

    # Auto tick size
    if tick_size <= 0:
        price_range = prices.max() - prices.min()
        tick_size = max(0.01, round(price_range / 50, 2))

    # Per-tick volume
    tick_vol = _tick_volume(df[volume_col]) if volume_col in df.columns else pd.Series(1, index=df.index)

    # Classify trades
    bids = df["bid"] if "bid" in df.columns else None
    asks = df["ask"] if "ask" in df.columns else None
    sides = _classify_trades(prices, bids, asks)

    # Round to tick size
    levels = (prices / tick_size).round() * tick_size

    # Aggregate
    agg = pd.DataFrame({"level": levels, "vol": tick_vol, "side": sides})
    profile = agg.groupby("level").agg(
        volume=("vol", "sum"),
        buy_vol=("vol", lambda x: x[agg.loc[x.index, "side"] == 1].sum()),
        sell_vol=("vol", lambda x: x[agg.loc[x.index, "side"] == -1].sum()),
    ).reset_index()
    profile = profile[profile["volume"] > 0].sort_values("level")

    if profile.empty:
        return {"levels": [], "poc": 0, "vah": 0, "val": 0,
                "total_volume": 0, "buy_volume": 0, "sell_volume": 0}

    # POC
    poc_idx = profile["volume"].idxmax()
    poc = profile.loc[poc_idx, "level"]

    # Value Area — expand from POC
    total_vol = profile["volume"].sum()
    target = total_vol * value_area_pct
    profile_sorted = profile.set_index("level").sort_index()
    poc_pos = profile_sorted.index.get_loc(poc)

    cum = profile_sorted.iloc[poc_pos]["volume"]
    lo, hi = poc_pos, poc_pos

    while cum < target and (lo > 0 or hi < len(profile_sorted) - 1):
        up_vol = profile_sorted.iloc[hi + 1]["volume"] if hi < len(profile_sorted) - 1 else 0
        dn_vol = profile_sorted.iloc[lo - 1]["volume"] if lo > 0 else 0

        if up_vol >= dn_vol and hi < len(profile_sorted) - 1:
            hi += 1
            cum += up_vol
        elif lo > 0:
            lo -= 1
            cum += dn_vol
        else:
            hi += 1
            cum += up_vol

    val = profile_sorted.index[lo]
    vah = profile_sorted.index[hi]

    levels_list = [
        {"price": row["level"], "volume": row["volume"],
         "buy_vol": row["buy_vol"], "sell_vol": row["sell_vol"]}
        for _, row in profile.iterrows()
    ]

    return {
        "levels": levels_list,
        "poc": float(poc),
        "vah": float(vah),
        "val": float(val),
        "total_volume": float(total_vol),
        "buy_volume": float(profile["buy_vol"].sum()),
        "sell_volume": float(profile["sell_vol"].sum()),
    }


def compute_vwap_bands(
    df: pd.DataFrame,
    price_col: str = "price",
    volume_col: str = "volume",
    num_std: list[int] | None = None,
) -> dict:
    """Compute running VWAP with standard deviation bands.

    Returns dict with keys: vwap (series), upper_1, lower_1, upper_2, lower_2, final_vwap.
    """
    if num_std is None:
        num_std = [1, 2]

    prices = df[price_col].values.astype(float)
    tick_vol = _tick_volume(df[volume_col]).values if volume_col in df.columns else np.ones(len(prices))

    cum_pv = np.cumsum(prices * tick_vol)
    cum_v = np.cumsum(tick_vol)
    cum_v[cum_v == 0] = 1  # avoid div/0

    vwap = cum_pv / cum_v

    # Running variance
    cum_pv2 = np.cumsum(tick_vol * (prices - vwap) ** 2)
    variance = cum_pv2 / cum_v
    std = np.sqrt(np.maximum(variance, 0))

    ts = df.index if "timestamp" not in df.columns else df["timestamp"].values

    result = {"vwap": vwap, "std": std, "final_vwap": float(vwap[-1]) if len(vwap) else 0}
    for n in num_std:
        result[f"upper_{n}"] = vwap + n * std
        result[f"lower_{n}"] = vwap - n * std

    return result


def compute_cumulative_delta(
    df: pd.DataFrame,
    price_col: str = "price",
    volume_col: str = "volume",
) -> pd.Series:
    """Cumulative delta = running sum of (buy_vol - sell_vol)."""
    prices = df[price_col]
    tick_vol = _tick_volume(df[volume_col]) if volume_col in df.columns else pd.Series(1, index=df.index)

    bids = df["bid"] if "bid" in df.columns else None
    asks = df["ask"] if "ask" in df.columns else None
    sides = _classify_trades(prices, bids, asks)

    delta = (sides * tick_vol).cumsum()
    return delta


def compute_tpo_profile(
    df: pd.DataFrame,
    price_col: str = "price",
    period_minutes: int = 30,
    tick_size: float = 0.0,
) -> dict:
    """Time Price Opportunity (Market Profile).

    Returns dict with keys: periods, profile (price→letters), poc,
    initial_balance_high, initial_balance_low.
    """
    if "timestamp" in df.columns:
        times = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    elif "datetime" in df.columns:
        times = pd.to_datetime(df["datetime"])
    else:
        return {"periods": [], "profile": {}, "poc": 0,
                "initial_balance_high": 0, "initial_balance_low": 0}

    prices = df[price_col].values

    if tick_size <= 0:
        price_range = prices.max() - prices.min()
        tick_size = max(0.01, round(price_range / 40, 2))

    rounded = (np.array(prices) / tick_size).round() * tick_size

    # Assign period letters (A, B, C, ...)
    start = times.min()
    period_idx = ((times - start).dt.total_seconds() / (period_minutes * 60)).astype(int)
    letters = [chr(65 + min(p, 25)) for p in period_idx]  # A-Z

    # Build profile
    profile: dict[float, list[str]] = {}
    for price, letter in zip(rounded, letters):
        key = round(float(price), 4)
        if key not in profile:
            profile[key] = []
        if letter not in profile[key]:
            profile[key].append(letter)

    # POC = price with most letters
    poc = max(profile, key=lambda k: len(profile[k])) if profile else 0

    # Initial balance (first 2 periods: A, B)
    ib_mask = [l in ("A", "B") for l in letters]
    ib_prices = rounded[ib_mask] if any(ib_mask) else rounded[:1]
    ib_high = float(ib_prices.max()) if len(ib_prices) else 0
    ib_low = float(ib_prices.min()) if len(ib_prices) else 0

    unique_periods = sorted(set(letters))

    return {
        "periods": unique_periods,
        "profile": profile,
        "poc": float(poc),
        "initial_balance_high": ib_high,
        "initial_balance_low": ib_low,
    }
