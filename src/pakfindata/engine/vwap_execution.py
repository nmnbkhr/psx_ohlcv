"""
VWAP Execution Optimizer.

Builds historical volume profiles, generates order slicing schedules
to minimize market impact. Three modes: VWAP, TWAP, Aggressive.

Performance: Implementation Shortfall, VWAP Slippage, Participation Rate.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timezone, timedelta
from dataclasses import dataclass, field, asdict

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
SLICE_MINUTES = 15


def _duck_con():
    return analytics_con()


@dataclass
class ExecutionSlice:
    slice_num: int
    time_start: str
    time_end: str
    target_shares: int
    target_pct: float
    hist_volume: float
    participation_rate: float
    limit_price: float
    urgency: str

    def to_dict(self):
        return asdict(self)


@dataclass
class ExecutionPlan:
    symbol: str
    side: str
    total_shares: int
    strategy: str
    slices: list[ExecutionSlice]
    estimated_vwap: float
    estimated_slippage_bps: float
    duration_min: int
    arrival_price: float
    daily_avg_volume: float
    participation_total: float
    warnings: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════
# VOLUME PROFILE
# ═══════════════════════════════════════════════════════

def build_volume_profile(symbol: str, lookback_dates: int = 10, interval_min: int = SLICE_MINUTES) -> pd.DataFrame:
    """Build average intraday volume profile from ohlcv_5s bars."""
    con = _duck_con()
    # Get available dates
    dates = con.execute(
        "SELECT DISTINCT SUBSTR(ts,1,10) AS d FROM ohlcv_5s WHERE symbol=? ORDER BY d DESC LIMIT ?",
        [symbol, lookback_dates],
    ).fetchall()
    if not dates:
        con.close()
        return pd.DataFrame()

    date_list = [d[0] for d in dates]
    placeholders = ",".join(f"'{d}'" for d in date_list)

    df = con.execute(f"""
        SELECT ts, o, h, l, c, v
        FROM ohlcv_5s
        WHERE symbol = '{symbol}' AND SUBSTR(ts,1,10) IN ({placeholders})
        ORDER BY ts
    """).df()
    con.close()

    if df.empty or len(df) < 50:
        return pd.DataFrame()

    # Parse hour:minute from ts string
    df["hour"] = df["ts"].str[11:13].astype(int)
    df["minute"] = df["ts"].str[14:16].astype(int)
    df["bucket"] = df["hour"] * 60 + (df["minute"] // interval_min) * interval_min
    df["date"] = df["ts"].str[:10]

    # Filter market hours (09:30 - 15:30)
    df = df[(df["bucket"] >= 570) & (df["bucket"] < 930)]
    if df.empty:
        return pd.DataFrame()

    # Per-bucket per-day volume, then average across days
    daily_bucket = df.groupby(["date", "bucket"]).agg(
        vol=("v", "sum"), vwap_num=("c", lambda x: (x * df.loc[x.index, "v"]).sum()),
        vwap_den=("v", "sum"), avg_price=("c", "mean"),
        hi=("h", "max"), lo=("l", "min"), bars=("v", "count"),
    ).reset_index()

    profile = daily_bucket.groupby("bucket").agg(
        avg_volume=("vol", "mean"),
        avg_price=("avg_price", "mean"),
        avg_hi=("hi", "mean"),
        avg_lo=("lo", "mean"),
        days=("date", "nunique"),
    ).reset_index()

    total = profile["avg_volume"].sum()
    profile["pct_of_day"] = profile["avg_volume"] / total if total > 0 else 0
    profile["cum_pct"] = profile["pct_of_day"].cumsum()
    profile["spread_bps"] = (profile["avg_hi"] - profile["avg_lo"]) / profile["avg_price"].replace(0, np.nan) * 10000

    # Format time labels
    profile["time_start"] = profile["bucket"].apply(lambda b: f"{b // 60:02d}:{b % 60:02d}")
    profile["time_end"] = profile["bucket"].apply(lambda b: f"{(b + interval_min) // 60:02d}:{(b + interval_min) % 60:02d}")

    return profile


def get_latest_price(symbol: str) -> float:
    """Get latest close price from ohlcv_5s."""
    con = _duck_con()
    r = con.execute("SELECT c FROM ohlcv_5s WHERE symbol=? ORDER BY ts DESC LIMIT 1", [symbol]).fetchone()
    con.close()
    return float(r[0]) if r else 0.0


# ═══════════════════════════════════════════════════════
# EXECUTION PLAN GENERATION
# ═══════════════════════════════════════════════════════

def generate_execution_plan(
    symbol: str,
    side: str,
    total_shares: int,
    strategy: str = "VWAP",
    max_participation: float = 0.15,
    lookback_dates: int = 10,
) -> ExecutionPlan | None:
    """Generate an execution plan for a large order."""
    profile = build_volume_profile(symbol, lookback_dates=lookback_dates)
    if profile.empty:
        return None

    arrival = get_latest_price(symbol)
    if arrival <= 0:
        return None

    daily_avg_vol = float(profile["avg_volume"].sum())
    order_pct_of_daily = total_shares / daily_avg_vol if daily_avg_vol > 0 else 999
    warnings = []

    if order_pct_of_daily > 0.5:
        warnings.append(f"Order is {order_pct_of_daily:.0%} of avg daily volume — high impact risk")
    if order_pct_of_daily > 1.0:
        warnings.append("Order exceeds avg daily volume — consider multi-day execution")

    slices = []
    remaining = total_shares

    for idx, (_, row) in enumerate(profile.iterrows()):
        if remaining <= 0:
            break

        if strategy == "VWAP":
            target_pct = float(row["pct_of_day"])
        elif strategy == "TWAP":
            target_pct = 1.0 / len(profile)
        elif strategy == "AGGRESSIVE":
            # Front-load when spread is tight
            spread = float(row.get("spread_bps", 50))
            weight = 1.0 / max(spread, 1)
            total_weight = sum(1.0 / max(float(r.get("spread_bps", 50)), 1) for _, r in profile.iterrows())
            target_pct = weight / total_weight if total_weight > 0 else 1.0 / len(profile)
        else:
            target_pct = 1.0 / len(profile)

        target = int(total_shares * target_pct)
        hist_vol = float(row["avg_volume"])
        max_shares = int(hist_vol * max_participation)

        actual = min(target, remaining, max_shares) if max_shares > 0 else min(target, remaining)
        if actual <= 0:
            actual = min(100, remaining)  # minimum slice

        participation = actual / hist_vol if hist_vol > 0 else 0
        spread_bps = float(row.get("spread_bps", 0))

        # Limit price: arrival ± expected spread impact
        impact_bps = participation * 10  # rough: 10bps per 100% participation
        if side == "BUY":
            limit = arrival * (1 + impact_bps / 10000)
        else:
            limit = arrival * (1 - impact_bps / 10000)

        urgency = "HIGH" if participation > 0.10 else "MEDIUM" if participation > 0.05 else "LOW"

        slices.append(ExecutionSlice(
            slice_num=idx + 1,
            time_start=str(row["time_start"]),
            time_end=str(row["time_end"]),
            target_shares=actual,
            target_pct=actual / total_shares if total_shares > 0 else 0,
            hist_volume=hist_vol,
            participation_rate=participation,
            limit_price=round(limit, 2),
            urgency=urgency,
        ))
        remaining -= actual

    # Distribute any remaining to last slices
    if remaining > 0 and slices:
        slices[-1] = ExecutionSlice(
            **{**slices[-1].to_dict(), "target_shares": slices[-1].target_shares + remaining,
               "target_pct": (slices[-1].target_shares + remaining) / total_shares}
        )

    est_slippage = order_pct_of_daily * 50  # rough: 50bps per 100% of daily volume
    duration = len(slices) * SLICE_MINUTES

    return ExecutionPlan(
        symbol=symbol, side=side, total_shares=total_shares, strategy=strategy,
        slices=slices, estimated_vwap=arrival, estimated_slippage_bps=round(est_slippage, 1),
        duration_min=duration, arrival_price=arrival, daily_avg_volume=daily_avg_vol,
        participation_total=total_shares / daily_avg_vol if daily_avg_vol > 0 else 0,
        warnings=warnings,
    )


# ═══════════════════════════════════════════════════════
# BACKTEST EXECUTION
# ═══════════════════════════════════════════════════════

def backtest_execution(
    symbol: str,
    side: str,
    total_shares: int,
    date_str: str,
    strategy: str = "VWAP",
    max_participation: float = 0.15,
) -> dict | None:
    """Backtest execution on a specific date using actual 5s bars."""
    con = _duck_con()
    bars = con.execute(
        "SELECT ts, o, h, l, c, v FROM ohlcv_5s "
        "WHERE symbol=? AND SUBSTR(ts,1,10)=? ORDER BY ts",
        [symbol, date_str],
    ).df()
    con.close()

    if bars.empty or len(bars) < 20:
        return None

    bars["hour"] = bars["ts"].str[11:13].astype(int)
    bars["minute"] = bars["ts"].str[14:16].astype(int)
    bars["bucket"] = bars["hour"] * 60 + (bars["minute"] // SLICE_MINUTES) * SLICE_MINUTES
    bars = bars[(bars["bucket"] >= 570) & (bars["bucket"] < 930)]

    if bars.empty:
        return None

    # Market VWAP
    market_vwap = float((bars["c"] * bars["v"]).sum() / bars["v"].sum()) if bars["v"].sum() > 0 else 0
    arrival_price = float(bars["c"].iloc[0])
    total_market_vol = float(bars["v"].sum())

    # Build execution schedule
    plan = generate_execution_plan(symbol, side, total_shares, strategy, max_participation)
    if not plan:
        return None

    # Simulate execution
    exec_prices = []
    exec_shares = []
    bucket_groups = bars.groupby("bucket")

    for sl in plan.slices:
        bucket_min = int(sl.time_start[:2]) * 60 + int(sl.time_start[3:5])
        if bucket_min in bucket_groups.groups:
            bucket_bars = bucket_groups.get_group(bucket_min)
            # Execute at VWAP of this bucket
            bv = bucket_bars["v"].sum()
            if bv > 0:
                bucket_vwap = float((bucket_bars["c"] * bucket_bars["v"]).sum() / bv)
            else:
                bucket_vwap = float(bucket_bars["c"].mean())
            exec_prices.append(bucket_vwap)
            exec_shares.append(sl.target_shares)

    if not exec_prices:
        return None

    # Execution VWAP
    exec_vwap = sum(p * s for p, s in zip(exec_prices, exec_shares)) / sum(exec_shares) if sum(exec_shares) > 0 else 0

    # Metrics
    if side == "BUY":
        impl_shortfall = (exec_vwap - arrival_price) / arrival_price * 10000  # bps
        vwap_slippage = (exec_vwap - market_vwap) / market_vwap * 10000 if market_vwap > 0 else 0
    else:
        impl_shortfall = (arrival_price - exec_vwap) / arrival_price * 10000
        vwap_slippage = (market_vwap - exec_vwap) / market_vwap * 10000 if market_vwap > 0 else 0

    return {
        "symbol": symbol,
        "date": date_str,
        "side": side,
        "strategy": strategy,
        "total_shares": total_shares,
        "exec_vwap": exec_vwap,
        "market_vwap": market_vwap,
        "arrival_price": arrival_price,
        "impl_shortfall_bps": float(impl_shortfall),
        "vwap_slippage_bps": float(vwap_slippage),
        "total_market_vol": total_market_vol,
        "participation": total_shares / total_market_vol if total_market_vol > 0 else 0,
        "slices_executed": len(exec_prices),
        "plan": plan,
    }


def get_available_symbols() -> list[str]:
    """Get symbols with ohlcv_5s data."""
    con = _duck_con()
    rows = con.execute(
        "SELECT DISTINCT symbol FROM ohlcv_5s ORDER BY symbol"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def get_available_dates(symbol: str) -> list[str]:
    """Get dates with ohlcv_5s data for a symbol."""
    con = _duck_con()
    rows = con.execute(
        "SELECT DISTINCT SUBSTR(ts,1,10) AS d FROM ohlcv_5s WHERE symbol=? ORDER BY d DESC",
        [symbol],
    ).fetchall()
    con.close()
    return [r[0] for r in rows]
