"""
VPIN Regime-Switching Strategy Engine.

Combines:
  1. VPIN toxicity (from tick data) — measures informed trading probability
  2. Hurst exponent (from EOD data) — determines trending vs mean-reverting regime
  3. Signal generation — when to enter/exit based on VPIN state transitions

Signal Logic:
  - VPIN > 0.7  -> TOXIC: reduce exposure (informed traders active)
  - VPIN drops from >0.7 to <0.4 -> CLEARING: entry opportunity
  - VPIN < 0.3  -> SAFE: normal trading, use Hurst regime
  - Hurst > 0.55 -> TRENDING: momentum (follow the move)
  - Hurst < 0.45 -> MEAN-REVERTING: fade extremes
  - Hurst 0.45-0.55 -> RANDOM WALK: no edge, reduce size

PSX-Specific:
  - Trading days: 245/year, hours: 09:30-15:30 (Mon-Thu), 09:30-16:30 (Fri)
  - Circuit breakers: +/-7.5%
  - Tick data: DuckDB tick_logs, EOD: eod_ohlcv
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from enum import Enum

from pakfindata.engine.microstructure import compute_vpin as _micro_vpin
from pakfindata.engine.macro_regime import hurst_exponent_rs, classify_regime

PKT = timezone(timedelta(hours=5))
TRADING_DAYS = 245


class VPINState(Enum):
    SAFE = "SAFE"
    ELEVATED = "ELEVATED"
    WARNING = "WARNING"
    TOXIC = "TOXIC"
    CLEARING = "CLEARING"


class HurstRegime(Enum):
    TRENDING = "TRENDING"
    RANDOM_WALK = "RANDOM_WALK"
    MEAN_REVERTING = "MEAN_REVERTING"


@dataclass
class VPINSignal:
    symbol: str
    date: str
    timestamp: str
    vpin: float
    vpin_state: VPINState
    hurst: float
    hurst_regime: HurstRegime
    signal: str          # BUY, SELL, HOLD, EXIT, REDUCE
    confidence: float    # 0-1
    reason: str
    position_size: float  # 0-1

    def to_dict(self) -> dict:
        d = asdict(self)
        d["vpin_state"] = self.vpin_state.value
        d["hurst_regime"] = self.hurst_regime.value
        return d


# ═══════════════════════════════════════════════════════
# VPIN COMPUTATION
# ═══════════════════════════════════════════════════════

def compute_vpin_from_ticks(ticks_df: pd.DataFrame, n_buckets: int = 50) -> pd.DataFrame:
    """Compute VPIN from tick data using bulk volume classification.

    Args:
        ticks_df: must have columns [price, volume] and optionally [timestamp/_ts]
        n_buckets: volume bucket count

    Returns:
        DataFrame with columns [bucket, buy_vol, sell_vol, total_vol, imbalance, vpin, price, timestamp]
    """
    if ticks_df.empty or len(ticks_df) < 20:
        return pd.DataFrame()

    total_vol = ticks_df["volume"].sum()
    if total_vol <= 0:
        return pd.DataFrame()

    bucket_size = total_vol / n_buckets
    if bucket_size <= 0:
        return pd.DataFrame()

    from scipy.stats import norm

    ts_col = "_ts" if "_ts" in ticks_df.columns else "timestamp" if "timestamp" in ticks_df.columns else None

    results = []
    cum_vol = 0
    bucket_start = 0
    bucket_num = 0

    prices = ticks_df["price"].values
    volumes = ticks_df["volume"].values
    timestamps = ticks_df[ts_col].values if ts_col else range(len(ticks_df))

    for i in range(len(ticks_df)):
        cum_vol += volumes[i]
        if cum_vol >= bucket_size or i == len(ticks_df) - 1:
            p_start = prices[bucket_start]
            p_end = prices[i]
            bvol = sum(volumes[bucket_start:i + 1])

            z = ((p_end - p_start) / p_start * 100) if p_start > 0 else 0
            buy_frac = norm.cdf(z)
            buy_vol = bvol * buy_frac
            sell_vol = bvol * (1 - buy_frac)

            results.append({
                "bucket": bucket_num,
                "buy_vol": buy_vol,
                "sell_vol": sell_vol,
                "total_vol": bvol,
                "imbalance": abs(buy_vol - sell_vol),
                "timestamp": timestamps[i],
                "price": p_end,
            })
            bucket_num += 1
            cum_vol = 0
            bucket_start = i + 1

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    window = min(50, len(df))
    df["vpin"] = (
        df["imbalance"].rolling(window, min_periods=1).sum()
        / df["total_vol"].rolling(window, min_periods=1).sum()
    )
    return df


def compute_hurst(prices: pd.Series, window: int = 100) -> float:
    """Compute Hurst exponent using existing R/S implementation."""
    if len(prices) < window:
        return 0.5
    arr = prices.values[-window:]
    returns = np.diff(np.log(arr))
    if len(returns) < 20:
        return 0.5
    return float(np.clip(hurst_exponent_rs(returns, max_lag=min(50, len(returns) // 2)), 0.0, 1.0))


# ═══════════════════════════════════════════════════════
# STATE CLASSIFICATION
# ═══════════════════════════════════════════════════════

def classify_vpin_state(vpin: float, prev_state: VPINState | None = None) -> VPINState:
    if vpin >= 0.7:
        return VPINState.TOXIC
    elif vpin >= 0.5:
        return VPINState.WARNING
    elif vpin >= 0.3:
        if prev_state in (VPINState.TOXIC, VPINState.WARNING):
            return VPINState.CLEARING
        return VPINState.ELEVATED
    else:
        if prev_state in (VPINState.TOXIC, VPINState.WARNING, VPINState.CLEARING):
            return VPINState.CLEARING
        return VPINState.SAFE


def classify_hurst_regime(hurst: float) -> HurstRegime:
    if hurst > 0.55:
        return HurstRegime.TRENDING
    elif hurst < 0.45:
        return HurstRegime.MEAN_REVERTING
    return HurstRegime.RANDOM_WALK


# ═══════════════════════════════════════════════════════
# SIGNAL GENERATION
# ═══════════════════════════════════════════════════════

def generate_signal(
    symbol: str,
    vpin: float,
    vpin_state: VPINState,
    hurst: float,
    hurst_regime: HurstRegime,
    price_vs_sma: float,
    recent_return_5d: float,
    ofi: float = 0.0,
) -> VPINSignal:
    now = datetime.now(PKT)

    signal = "HOLD"
    confidence = 0.0
    reason = ""
    position_size = 0.0

    if vpin_state == VPINState.TOXIC:
        signal, confidence = "EXIT", 0.9
        reason = f"VPIN {vpin:.3f} > 0.7 — toxic flow, informed traders active"
        position_size = 0.0

    elif vpin_state == VPINState.WARNING:
        signal, confidence = "REDUCE", 0.7
        reason = f"VPIN {vpin:.3f} elevated — reduce exposure, tighten stops"
        position_size = 0.25

    elif vpin_state == VPINState.CLEARING:
        if hurst_regime == HurstRegime.TRENDING:
            if price_vs_sma > 0 and recent_return_5d > 0:
                signal, confidence = "BUY", 0.8
                reason = f"VPIN clearing ({vpin:.3f}), trending (H={hurst:.3f}), above SMA — momentum long"
                position_size = 0.7
            elif price_vs_sma < 0 and recent_return_5d < 0:
                signal, confidence = "SELL", 0.8
                reason = f"VPIN clearing ({vpin:.3f}), trending (H={hurst:.3f}), below SMA — momentum short"
                position_size = 0.7
            else:
                signal, confidence = "HOLD", 0.5
                reason = "VPIN clearing but mixed trend signals"
                position_size = 0.3
        elif hurst_regime == HurstRegime.MEAN_REVERTING:
            if recent_return_5d < -0.03:
                signal, confidence = "BUY", 0.75
                reason = f"VPIN clearing ({vpin:.3f}), mean-reverting (H={hurst:.3f}), oversold — fade dip"
                position_size = 0.6
            elif recent_return_5d > 0.03:
                signal, confidence = "SELL", 0.75
                reason = f"VPIN clearing ({vpin:.3f}), mean-reverting (H={hurst:.3f}), overbought — fade rally"
                position_size = 0.6
            else:
                signal, confidence = "HOLD", 0.4
                reason = "VPIN clearing, mean-reverting but no extreme"
                position_size = 0.2
        else:
            signal, confidence = "HOLD", 0.3
            reason = "VPIN clearing but random walk — no edge"
            position_size = 0.15

    elif vpin_state == VPINState.SAFE:
        if hurst_regime == HurstRegime.TRENDING:
            if price_vs_sma > 0.02 and ofi > 0.2:
                signal, confidence = "BUY", 0.6
                reason = f"Safe VPIN ({vpin:.3f}), trending (H={hurst:.3f}), above SMA + positive OFI"
                position_size = 0.5
            elif price_vs_sma < -0.02 and ofi < -0.2:
                signal, confidence = "SELL", 0.6
                reason = f"Safe VPIN ({vpin:.3f}), trending (H={hurst:.3f}), below SMA + negative OFI"
                position_size = 0.5
            else:
                signal, confidence = "HOLD", 0.3
                reason = "Safe, trending but no clear direction"
                position_size = 0.3
        else:
            signal, confidence = "HOLD", 0.2
            reason = "Normal conditions, no strong signal"
            position_size = 0.2
    else:
        signal, confidence = "HOLD", 0.4
        reason = f"VPIN {vpin:.3f} elevated — watching for spike or clearing"
        position_size = 0.3

    return VPINSignal(
        symbol=symbol, date=now.strftime("%Y-%m-%d"), timestamp=now.strftime("%H:%M:%S"),
        vpin=vpin, vpin_state=vpin_state, hurst=hurst, hurst_regime=hurst_regime,
        signal=signal, confidence=confidence, reason=reason, position_size=position_size,
    )


# ═══════════════════════════════════════════════════════
# DATA LOADING HELPERS
# ═══════════════════════════════════════════════════════

def _duck_con():
    from pakfindata.db.connections import analytics_con
    return analytics_con()


def load_ticks(symbol: str, date_str: str) -> pd.DataFrame:
    """Load tick data from DuckDB tick_logs for a symbol+date."""
    con = _duck_con()
    df = con.execute(
        "SELECT price, volume, _ts AS timestamp FROM tick_logs "
        "WHERE symbol = ? AND date = ? ORDER BY timestamp",
        [symbol, date_str],
    ).df()
    con.close()
    return df


def load_eod(symbol: str, limit: int = 300) -> pd.DataFrame:
    """Load EOD OHLCV from DuckDB."""
    con = _duck_con()
    df = con.execute(
        "SELECT date, open, high, low, close, volume FROM eod_ohlcv "
        "WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        [symbol, limit],
    ).df()
    con.close()
    return df.sort_values("date").reset_index(drop=True) if not df.empty else df


def get_tick_dates(symbol: str) -> list[str]:
    """Get available tick dates for a symbol."""
    con = _duck_con()
    rows = con.execute(
        "SELECT DISTINCT date AS d FROM tick_logs "
        "WHERE symbol = ? ORDER BY d DESC", [symbol],
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def get_liquid_symbols(date_str: str, min_ticks: int = 500, top_n: int = 50) -> list[str]:
    """Get most liquid symbols for a date by tick count."""
    con = _duck_con()
    rows = con.execute(
        "SELECT symbol, COUNT(*) AS cnt FROM tick_logs "
        "WHERE date = ? AND market = 'REG' "
        "GROUP BY symbol HAVING cnt >= ? ORDER BY cnt DESC LIMIT ?",
        [date_str, min_ticks, top_n],
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════
# LIVE SIGNAL FOR SINGLE SYMBOL
# ═══════════════════════════════════════════════════════

def compute_live_signal(symbol: str, date_str: str | None = None) -> VPINSignal | None:
    """Compute full VPIN signal for a symbol on a date."""
    if date_str is None:
        dates = get_tick_dates(symbol)
        if not dates:
            return None
        date_str = dates[0]

    # Tick data → VPIN
    ticks = load_ticks(symbol, date_str)
    if len(ticks) < 50:
        return None

    vpin_df = compute_vpin_from_ticks(ticks, n_buckets=50)
    if vpin_df.empty:
        return None
    vpin = float(vpin_df["vpin"].iloc[-1])
    vpin_state = classify_vpin_state(vpin)

    # EOD data → Hurst + context
    eod = load_eod(symbol, limit=250)
    if len(eod) < 100:
        return None

    hurst = compute_hurst(eod["close"], window=100)
    hurst_regime = classify_hurst_regime(hurst)

    sma_200 = eod["close"].tail(200).mean()
    latest_close = eod["close"].iloc[-1]
    price_vs_sma = (latest_close - sma_200) / sma_200 if sma_200 > 0 else 0
    ret_5d = (latest_close / eod["close"].iloc[-6]) - 1 if len(eod) >= 6 else 0

    return generate_signal(
        symbol=symbol, vpin=vpin, vpin_state=vpin_state,
        hurst=hurst, hurst_regime=hurst_regime,
        price_vs_sma=price_vs_sma, recent_return_5d=ret_5d,
    )


# ═══════════════════════════════════════════════════════
# SCANNER — BATCH SIGNALS
# ═══════════════════════════════════════════════════════

def scan_signals(date_str: str | None = None, top_n: int = 50) -> list[dict]:
    """Scan top symbols for VPIN signals."""
    if date_str is None:
        con = _duck_con()
        r = con.execute("SELECT MAX(date) FROM tick_logs").fetchone()
        con.close()
        date_str = r[0] if r and r[0] else None
        if not date_str:
            return []

    symbols = get_liquid_symbols(date_str, min_ticks=200, top_n=top_n)
    results = []
    for sym in symbols:
        sig = compute_live_signal(sym, date_str)
        if sig:
            results.append(sig.to_dict())
    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results


# ═══════════════════════════════════════════════════════
# BACKTEST
# ═══════════════════════════════════════════════════════

def backtest_vpin_strategy(
    symbol: str,
    lookback_days: int = 250,
    initial_capital: float = 1_000_000,
) -> dict:
    """Backtest VPIN regime-switching strategy on historical data."""
    eod = load_eod(symbol, limit=lookback_days + 100)
    if len(eod) < 150:
        return {"error": f"Not enough EOD data for {symbol} ({len(eod)} days)"}

    tick_dates = set(get_tick_dates(symbol))

    capital = initial_capital
    position = 0
    entry_price = 0.0
    equity_curve = []
    trades = []
    prev_state = VPINState.SAFE

    for i in range(100, len(eod)):
        row = eod.iloc[i]
        date_str = str(row["date"])
        close = float(row["close"])

        # Hurst from rolling window
        prices = eod.iloc[max(0, i - 100):i + 1]["close"]
        hurst = compute_hurst(prices, window=100)
        hurst_regime = classify_hurst_regime(hurst)

        # VPIN from tick data if available, otherwise estimate from EOD
        vpin = None
        if date_str in tick_dates:
            try:
                ticks = load_ticks(symbol, date_str)
                if len(ticks) > 50:
                    vpin_df = compute_vpin_from_ticks(ticks, n_buckets=50)
                    if not vpin_df.empty:
                        vpin = float(vpin_df["vpin"].iloc[-1])
            except Exception:
                pass

        if vpin is None:
            # EOD proxy: estimate VPIN from intraday range + volume anomaly
            # High range + volume spike => likely informed trading
            hi, lo = float(row["high"]), float(row["low"])
            intra_range = (hi - lo) / lo if lo > 0 else 0
            avg_range_20 = eod.iloc[max(0, i - 20):i].apply(
                lambda r: (r["high"] - r["low"]) / r["low"] if r["low"] > 0 else 0, axis=1
            ).mean()
            vol_today = float(row["volume"])
            avg_vol_20 = eod.iloc[max(0, i - 20):i]["volume"].mean()
            vol_ratio = vol_today / avg_vol_20 if avg_vol_20 > 0 else 1
            range_ratio = intra_range / avg_range_20 if avg_range_20 > 0 else 1
            # VPIN proxy: combine range and volume anomalies (0.1 - 0.8)
            vpin = float(np.clip(0.15 + 0.25 * (range_ratio - 1) + 0.15 * (vol_ratio - 1), 0.05, 0.85))

        vpin_state = classify_vpin_state(vpin, prev_state)

        sma_200 = eod.iloc[max(0, i - 200):i + 1]["close"].mean()
        price_vs_sma = (close - sma_200) / sma_200 if sma_200 > 0 else 0
        ret_5d = (close / float(eod.iloc[max(0, i - 5)]["close"])) - 1
        # OFI proxy from price direction + volume
        ofi = float(np.sign(close - float(row["open"])) * min(vol_ratio, 3) / 3)

        sig = generate_signal(
            symbol=symbol, vpin=vpin, vpin_state=vpin_state,
            hurst=hurst, hurst_regime=hurst_regime,
            price_vs_sma=price_vs_sma, recent_return_5d=ret_5d, ofi=ofi,
        )

        # Execute
        if sig.signal == "BUY" and position <= 0:
            if position < 0:
                pnl = (entry_price - close) * abs(position)
                capital += pnl
                trades.append({"date": date_str, "action": "COVER", "price": close, "pnl": pnl})
            shares = int((capital * sig.position_size) / close)
            if shares > 0:
                position = shares
                entry_price = close
                trades.append({"date": date_str, "action": "BUY", "price": close, "shares": shares, "reason": sig.reason})

        elif sig.signal == "SELL" and position >= 0:
            if position > 0:
                pnl = (close - entry_price) * position
                capital += pnl
                trades.append({"date": date_str, "action": "SELL", "price": close, "pnl": pnl})
            shares = int((capital * sig.position_size) / close)
            if shares > 0:
                position = -shares
                entry_price = close
                trades.append({"date": date_str, "action": "SHORT", "price": close, "shares": shares, "reason": sig.reason})

        elif sig.signal == "EXIT" and position != 0:
            pnl = ((close - entry_price) * position) if position > 0 else ((entry_price - close) * abs(position))
            capital += pnl
            trades.append({"date": date_str, "action": "EXIT", "price": close, "pnl": pnl, "reason": sig.reason})
            position = 0

        elif sig.signal == "REDUCE" and abs(position) > 0:
            reduce = abs(position) // 2
            if reduce > 0:
                pnl = ((close - entry_price) * reduce) if position > 0 else ((entry_price - close) * reduce)
                capital += pnl
                position = position - reduce if position > 0 else position + reduce
                trades.append({"date": date_str, "action": "REDUCE", "price": close, "pnl": pnl})

        unrealized = 0
        if position > 0:
            unrealized = (close - entry_price) * position
        elif position < 0:
            unrealized = (entry_price - close) * abs(position)

        equity_curve.append({
            "date": date_str, "equity": capital + unrealized, "capital": capital,
            "position": position, "vpin": vpin, "vpin_state": vpin_state.value,
            "hurst": hurst, "hurst_regime": hurst_regime.value,
            "signal": sig.signal, "close": close,
        })
        prev_state = vpin_state

    # Close remaining
    if position != 0:
        fc = float(eod.iloc[-1]["close"])
        pnl = ((fc - entry_price) * position) if position > 0 else ((entry_price - fc) * abs(position))
        capital += pnl

    eq_df = pd.DataFrame(equity_curve)
    eq_ret = eq_df["equity"].pct_change().dropna()
    total_ret = (capital - initial_capital) / initial_capital
    ann_ret = (1 + total_ret) ** (TRADING_DAYS / max(len(eq_df), 1)) - 1
    ann_vol = float(eq_ret.std() * np.sqrt(TRADING_DAYS)) if len(eq_ret) > 0 else 0
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
    max_dd = float((eq_df["equity"] / eq_df["equity"].cummax() - 1).min()) if len(eq_df) > 0 else 0
    bh_ret = (float(eod.iloc[-1]["close"]) / float(eod.iloc[100]["close"])) - 1

    winning = [t for t in trades if t.get("pnl", 0) > 0]
    losing = [t for t in trades if t.get("pnl", 0) < 0]

    return {
        "equity_curve": eq_df,
        "trades": pd.DataFrame(trades) if trades else pd.DataFrame(),
        "metrics": {
            "total_return": total_ret,
            "annualized_return": ann_ret,
            "annualized_volatility": ann_vol,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_dd,
            "total_trades": len(trades),
            "winning_trades": len(winning),
            "losing_trades": len(losing),
            "win_rate": len(winning) / max(1, len(winning) + len(losing)),
            "buy_hold_return": bh_ret,
            "alpha": total_ret - bh_ret,
        },
    }
