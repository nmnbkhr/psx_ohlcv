"""
Order Flow Imbalance (OFI) Alpha Strategy.

OFI measures net buying vs selling pressure at best bid/ask.
When 15-min OFI exceeds threshold, predict next-bar direction.

Academic basis: Cont, Kukanov & Stoikov (2014) — OFI explains ~65% of short-term price changes.
PSX edge: No HFT → OFI signals persist 15-60 minutes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timezone, timedelta
from dataclasses import dataclass, asdict

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
TRADING_DAYS = 245


@dataclass
class OFISignal:
    symbol: str
    date: str
    bar_end: str
    ofi_raw: float
    ofi_normalized: float
    signal: str       # LONG, SHORT, FLAT
    strength: float   # 0-1
    predicted_direction: float
    tick_count: int

    def to_dict(self) -> dict:
        return asdict(self)


def _duck_con():
    return analytics_con()


def load_ticks_for_ofi(symbol: str, date_str: str) -> pd.DataFrame:
    """Load tick data with bid/ask volumes."""
    con = _duck_con()
    df = con.execute(
        "SELECT price, volume, bid, ask, bid_vol, ask_vol, timestamp, _ts "
        "FROM tick_logs WHERE symbol = ? AND date = ? "
        "AND bid_vol > 0 AND ask_vol > 0 ORDER BY timestamp",
        [symbol, date_str],
    ).df()
    con.close()
    return df


def compute_ofi_bars(ticks: pd.DataFrame, bar_minutes: int = 15) -> pd.DataFrame:
    """Compute OFI per time bar from tick data."""
    if ticks.empty or len(ticks) < 10:
        return pd.DataFrame()

    ticks = ticks.copy()

    # Parse timestamp
    if not pd.api.types.is_datetime64_any_dtype(ticks.get("_ts")):
        ticks["_ts"] = pd.to_datetime(ticks["_ts"])
    ticks["ts"] = ticks["_ts"]

    # Per-tick OFI
    bid_vol = ticks["bid_vol"].fillna(0).astype(float)
    ask_vol = ticks["ask_vol"].fillna(0).astype(float)

    ticks["ofi_instant"] = bid_vol - ask_vol
    ticks["total_depth"] = bid_vol + ask_vol

    # Delta-based OFI (Cont et al.)
    ticks["bid_delta"] = bid_vol.diff().fillna(0)
    ticks["ask_delta"] = ask_vol.diff().fillna(0)
    ticks["bid_change"] = ticks["bid"].diff().fillna(0)
    ticks["ask_change"] = ticks["ask"].diff().fillna(0)

    ticks["ofi_delta"] = 0.0
    mask_bid_up = ticks["bid_change"] >= 0
    mask_ask_dn = ticks["ask_change"] <= 0
    ticks.loc[mask_bid_up, "ofi_delta"] += ticks.loc[mask_bid_up, "bid_delta"]
    ticks.loc[mask_ask_dn, "ofi_delta"] -= ticks.loc[mask_ask_dn, "ask_delta"]

    ticks = ticks.set_index("ts")

    bars = ticks.resample(f"{bar_minutes}min").agg({
        "price": ["first", "last", "max", "min"],
        "volume": "sum",
        "ofi_instant": "mean",
        "ofi_delta": "sum",
        "total_depth": "mean",
        "bid": "last",
        "ask": "last",
        "bid_vol": "last",
        "ask_vol": "last",
    }).dropna()

    bars.columns = [
        "open", "close", "high", "low", "volume",
        "ofi_instant_mean", "ofi_delta_sum", "avg_depth",
        "bid", "ask", "bid_vol", "ask_vol",
    ]

    bars["ofi_normalized"] = (bars["ofi_instant_mean"] / bars["avg_depth"].replace(0, np.nan)).fillna(0).clip(-1, 1)
    bars["ofi_delta_norm"] = (bars["ofi_delta_sum"] / bars["volume"].replace(0, np.nan)).fillna(0).clip(-1, 1)
    bars["bar_return"] = bars["close"] / bars["open"] - 1
    bars["next_return"] = bars["bar_return"].shift(-1)
    bars["spread"] = bars["ask"] - bars["bid"]
    bars["spread_bps"] = bars["spread"] / bars["close"] * 10000
    bars["tick_count"] = ticks.resample(f"{bar_minutes}min")["price"].count()

    bars = bars.reset_index().rename(columns={"ts": "bar_time"})
    return bars


def generate_ofi_signals(
    bars: pd.DataFrame,
    long_threshold: float = 0.3,
    short_threshold: float = -0.3,
    min_ticks: int = 20,
) -> list[OFISignal]:
    signals = []
    for _, bar in bars.iterrows():
        ofi = bar["ofi_normalized"]
        tc = int(bar.get("tick_count", 0))

        if tc < min_ticks:
            sig, strength = "FLAT", 0.0
        elif ofi > long_threshold:
            sig = "LONG"
            strength = min(1.0, (ofi - long_threshold) / (1.0 - long_threshold))
        elif ofi < short_threshold:
            sig = "SHORT"
            strength = min(1.0, (short_threshold - ofi) / (1.0 + short_threshold))
        else:
            sig, strength = "FLAT", 0.0

        signals.append(OFISignal(
            symbol="", date=str(bar.get("bar_time", ""))[:10],
            bar_end=str(bar.get("bar_time", "")),
            ofi_raw=float(bar.get("ofi_instant_mean", 0)),
            ofi_normalized=float(ofi), signal=sig, strength=float(strength),
            predicted_direction=float(np.sign(ofi)) if abs(ofi) > 0.1 else 0.0,
            tick_count=tc,
        ))
    return signals


def backtest_ofi_strategy(
    symbol: str,
    bar_minutes: int = 15,
    long_threshold: float = 0.3,
    short_threshold: float = -0.3,
    min_ticks: int = 20,
    stop_loss_pct: float = 0.02,
    take_profit_pct: float = 0.03,
    max_hold_bars: int = 4,
) -> dict:
    """Backtest OFI strategy across all available tick dates."""
    con = _duck_con()
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT date AS d FROM tick_logs "
        "WHERE symbol = ? ORDER BY d", [symbol],
    ).fetchall()]
    con.close()

    if not dates:
        return {"error": f"No tick data for {symbol}"}

    all_trades = []
    all_bars = []

    for date_str in dates:
        ticks = load_ticks_for_ofi(symbol, date_str)
        if len(ticks) < 50:
            continue

        bars = compute_ofi_bars(ticks, bar_minutes=bar_minutes)
        if bars.empty:
            continue

        bars["symbol"] = symbol
        all_bars.append(bars)

        position = None

        for i in range(len(bars) - 1):
            bar = bars.iloc[i]
            next_bar = bars.iloc[i + 1]
            ofi = bar["ofi_normalized"]
            tc = bar.get("tick_count", 0)

            if position is not None:
                bars_held = i - position["entry_bar"]
                if position["direction"] == "LONG":
                    pnl_pct = (next_bar["open"] / position["entry_price"]) - 1
                else:
                    pnl_pct = 1 - (next_bar["open"] / position["entry_price"])

                exit_reason = None
                if pnl_pct >= take_profit_pct:
                    exit_reason = "TAKE_PROFIT"
                elif pnl_pct <= -stop_loss_pct:
                    exit_reason = "STOP_LOSS"
                elif bars_held >= max_hold_bars:
                    exit_reason = "MAX_HOLD"
                elif position["direction"] == "LONG" and ofi < -0.1:
                    exit_reason = "OFI_REVERSAL"
                elif position["direction"] == "SHORT" and ofi > 0.1:
                    exit_reason = "OFI_REVERSAL"

                if exit_reason:
                    all_trades.append({
                        "date": date_str, "symbol": symbol,
                        "direction": position["direction"],
                        "entry_time": position["entry_time"],
                        "entry_price": position["entry_price"],
                        "exit_time": str(next_bar["bar_time"]),
                        "exit_price": float(next_bar["open"]),
                        "pnl_pct": float(pnl_pct),
                        "bars_held": bars_held,
                        "exit_reason": exit_reason,
                        "entry_ofi": position["entry_ofi"],
                    })
                    position = None

            if position is None and tc >= min_ticks:
                if ofi > long_threshold:
                    position = {
                        "entry_price": float(next_bar["open"]), "direction": "LONG",
                        "entry_bar": i + 1, "entry_time": str(next_bar["bar_time"]),
                        "entry_ofi": float(ofi),
                    }
                elif ofi < short_threshold:
                    position = {
                        "entry_price": float(next_bar["open"]), "direction": "SHORT",
                        "entry_bar": i + 1, "entry_time": str(next_bar["bar_time"]),
                        "entry_ofi": float(ofi),
                    }

    bars_df = pd.concat(all_bars, ignore_index=True) if all_bars else pd.DataFrame()

    if not all_trades:
        return {"error": "No trades generated", "bars": bars_df}

    trades_df = pd.DataFrame(all_trades)
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]
    total = len(trades_df)

    gross_profit = winning["pnl_pct"].sum() if len(winning) else 0
    gross_loss = abs(losing["pnl_pct"].sum()) if len(losing) else 1
    trades_df["cum_return"] = (1 + trades_df["pnl_pct"]).cumprod()
    total_return = float(trades_df["cum_return"].iloc[-1] - 1)
    max_dd = float((trades_df["cum_return"] / trades_df["cum_return"].cummax() - 1).min())

    std = trades_df["pnl_pct"].std()
    trades_per_year = total / max(1, len(dates)) * TRADING_DAYS
    sharpe = float((trades_df["pnl_pct"].mean() / std) * np.sqrt(trades_per_year)) if std > 0 else 0

    longs = trades_df[trades_df["direction"] == "LONG"]
    shorts = trades_df[trades_df["direction"] == "SHORT"]

    return {
        "trades": trades_df,
        "bars": bars_df,
        "metrics": {
            "total_trades": total,
            "win_rate": len(winning) / max(1, total),
            "avg_win": float(winning["pnl_pct"].mean()) if len(winning) else 0,
            "avg_loss": float(losing["pnl_pct"].mean()) if len(losing) else 0,
            "profit_factor": gross_profit / gross_loss if gross_loss > 0 else 0,
            "total_return": total_return,
            "max_drawdown": max_dd,
            "sharpe_ratio": sharpe,
            "avg_bars_held": float(trades_df["bars_held"].mean()),
            "exit_reasons": trades_df["exit_reason"].value_counts().to_dict(),
            "long_trades": len(longs),
            "short_trades": len(shorts),
            "long_win_rate": float((longs["pnl_pct"] > 0).mean()) if len(longs) else 0,
            "short_win_rate": float((shorts["pnl_pct"] > 0).mean()) if len(shorts) else 0,
            "dates_tested": len(dates),
        },
    }


def scan_current_ofi(symbols: list[str] | None = None, bar_minutes: int = 15) -> pd.DataFrame:
    """Scan current OFI for multiple symbols on latest date."""
    con = _duck_con()
    latest = con.execute("SELECT MAX(date) FROM tick_logs").fetchone()[0]
    if symbols is None:
        symbols = [r[0] for r in con.execute(
            "SELECT symbol, COUNT(*) AS c FROM tick_logs "
            "WHERE date = ? AND market = 'REG' "
            "GROUP BY symbol ORDER BY c DESC LIMIT 50", [latest],
        ).fetchall()]
    con.close()

    results = []
    for sym in symbols:
        ticks = load_ticks_for_ofi(sym, latest)
        if len(ticks) < 30:
            continue
        bars = compute_ofi_bars(ticks, bar_minutes=bar_minutes)
        if bars.empty:
            continue
        last = bars.iloc[-1]
        ofi = float(last["ofi_normalized"])
        if abs(ofi) > 0.15:
            results.append({
                "symbol": sym, "ofi": ofi, "ofi_abs": abs(ofi),
                "signal": "LONG" if ofi > 0.3 else "SHORT" if ofi < -0.3 else "WEAK",
                "price": float(last["close"]),
                "spread_bps": float(last.get("spread_bps", 0)),
                "tick_count": int(last.get("tick_count", 0)),
                "bar_return": float(last["bar_return"]),
                "date": latest,
            })

    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results).sort_values("ofi_abs", ascending=False).reset_index(drop=True)
