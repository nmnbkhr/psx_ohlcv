"""
Futures Basis Mean-Reversion Strategy.

Trades the spread between PSX futures and spot when it deviates
beyond normal range. Settlement forces convergence — structural edge.

Basis = (Futures - Spot) / Spot × 100 (%)
Fair basis ≈ risk_free_rate × days_to_expiry / 365
Excess basis = Actual - Fair
Signal: |Excess basis| > 2σ → mean-revert
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

SQLITE_PATH = Path("/mnt/e/psxdata/psx.sqlite")
KIBOR_3M = 12.0  # fallback KIBOR 3M rate (%)


def _sqlite_con():
    con = sqlite3.connect(str(SQLITE_PATH), timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    return con


@dataclass
class BasisSignal:
    symbol: str
    date: str
    spot_price: float
    futures_price: float
    basis_pct: float
    basis_zscore: float
    fair_basis: float
    excess_basis: float
    signal: str        # SELL_BASIS, BUY_BASIS, HOLD
    confidence: float
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════

def load_basis_history(base_symbol: str, contract_month: str = "MAR", lookback: int = 250) -> pd.DataFrame:
    """Load spot + futures prices and compute basis time series."""
    con = _sqlite_con()

    # Spot (REG market from eod_ohlcv)
    spot = pd.read_sql_query(
        "SELECT date, close AS spot FROM eod_ohlcv WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        con, params=[base_symbol, lookback],
    )

    # Futures (nearest month from futures_eod)
    fut = pd.read_sql_query(
        """SELECT date, close AS futures, contract_month, volume AS fut_volume
           FROM futures_eod
           WHERE base_symbol = ? AND market_type = 'CONT'
           ORDER BY date DESC LIMIT ?""",
        con, params=[base_symbol, lookback * 3],
    )
    con.close()

    if spot.empty or fut.empty:
        return pd.DataFrame()

    # For each date, pick the nearest-month contract with volume > 0 (or any)
    # Group by date, pick the one with most volume
    fut_best = fut.sort_values(["date", "fut_volume"], ascending=[True, False]).drop_duplicates("date", keep="first")

    df = spot.merge(fut_best[["date", "futures", "contract_month", "fut_volume"]], on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)

    if df.empty or len(df) < 10:
        return pd.DataFrame()

    # Compute basis
    df["basis"] = df["futures"] - df["spot"]
    df["basis_pct"] = (df["basis"] / df["spot"]) * 100

    # Rolling stats for z-score
    window = min(60, len(df) - 1)
    df["basis_mean"] = df["basis_pct"].rolling(window, min_periods=10).mean()
    df["basis_std"] = df["basis_pct"].rolling(window, min_periods=10).std()
    df["basis_zscore"] = (df["basis_pct"] - df["basis_mean"]) / df["basis_std"].replace(0, np.nan)
    df["basis_zscore"] = df["basis_zscore"].fillna(0)

    # Fair basis (KIBOR proxy)
    df["fair_basis"] = KIBOR_3M / 365 * 30 / 100 * 100  # ~1% for 30 days at 12%
    df["excess_basis"] = df["basis_pct"] - df["fair_basis"]

    return df


def get_active_futures_symbols(min_dates: int = 20) -> list[str]:
    """Get base symbols that have both spot and futures data."""
    con = _sqlite_con()
    rows = con.execute("""
        SELECT f.base_symbol, COUNT(DISTINCT f.date) AS fut_dates
        FROM futures_eod f
        INNER JOIN eod_ohlcv e ON f.base_symbol = e.symbol AND f.date = e.date
        WHERE f.market_type = 'CONT' AND f.close > 0 AND e.close > 0
        GROUP BY f.base_symbol
        HAVING fut_dates >= ?
        ORDER BY fut_dates DESC
    """, [min_dates]).fetchall()
    con.close()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════
# SIGNAL GENERATION
# ═══════════════════════════════════════════════════════

def generate_basis_signal(df: pd.DataFrame, symbol: str) -> BasisSignal | None:
    """Generate signal from latest basis data."""
    if df.empty or len(df) < 20:
        return None

    latest = df.iloc[-1]
    z = float(latest["basis_zscore"])
    basis = float(latest["basis_pct"])
    excess = float(latest.get("excess_basis", basis))

    if abs(z) > 2.0:
        if z > 2.0:
            signal = "SELL_BASIS"
            confidence = min(1.0, abs(z) / 4)
            reason = f"Basis {basis:+.2f}% (z={z:+.2f}) — premium too wide, sell futures/buy spot"
        else:
            signal = "BUY_BASIS"
            confidence = min(1.0, abs(z) / 4)
            reason = f"Basis {basis:+.2f}% (z={z:+.2f}) — discount too deep, buy futures/sell spot"
    elif abs(z) > 1.5:
        signal = "HOLD"
        confidence = 0.4
        reason = f"Basis {basis:+.2f}% (z={z:+.2f}) — elevated but not extreme"
    else:
        signal = "HOLD"
        confidence = 0.2
        reason = f"Basis {basis:+.2f}% (z={z:+.2f}) — within normal range"

    return BasisSignal(
        symbol=symbol, date=str(latest["date"]),
        spot_price=float(latest["spot"]), futures_price=float(latest["futures"]),
        basis_pct=basis, basis_zscore=z,
        fair_basis=float(latest.get("fair_basis", 0)),
        excess_basis=excess,
        signal=signal, confidence=confidence, reason=reason,
    )


# ═══════════════════════════════════════════════════════
# BACKTEST
# ═══════════════════════════════════════════════════════

def backtest_basis_strategy(
    symbol: str,
    lookback: int = 500,
    entry_z: float = 2.0,
    exit_z: float = 0.5,
    stop_z: float = 4.0,
) -> dict:
    """Backtest basis mean-reversion."""
    df = load_basis_history(symbol, lookback=lookback)
    if len(df) < 50:
        return {"error": f"Not enough data for {symbol} ({len(df)} rows)"}

    trades = []
    position = None  # {"direction": "SELL_BASIS"/"BUY_BASIS", "entry_date", "entry_basis", "entry_z"}
    equity = [1.0]

    for i in range(30, len(df)):
        row = df.iloc[i]
        z = float(row["basis_zscore"])
        basis = float(row["basis_pct"])
        date = str(row["date"])

        if position is not None:
            # Check exit
            exit_reason = None
            if position["direction"] == "SELL_BASIS" and z < exit_z:
                exit_reason = "MEAN_REVERT"
            elif position["direction"] == "BUY_BASIS" and z > -exit_z:
                exit_reason = "MEAN_REVERT"
            elif abs(z) > stop_z:
                exit_reason = "STOP"

            if exit_reason:
                entry_basis = position["entry_basis"]
                if position["direction"] == "SELL_BASIS":
                    pnl = entry_basis - basis  # sold high, covers low
                else:
                    pnl = basis - entry_basis  # bought low, sells high
                pnl_pct = pnl / 100  # basis is in %, convert to decimal

                trades.append({
                    "entry_date": position["entry_date"],
                    "exit_date": date,
                    "direction": position["direction"],
                    "entry_basis": entry_basis,
                    "exit_basis": basis,
                    "entry_z": position["entry_z"],
                    "exit_z": z,
                    "pnl_pct": pnl_pct,
                    "exit_reason": exit_reason,
                })
                equity.append(equity[-1] * (1 + pnl_pct))
                position = None

        if position is None:
            if z > entry_z:
                position = {"direction": "SELL_BASIS", "entry_date": date, "entry_basis": basis, "entry_z": z}
            elif z < -entry_z:
                position = {"direction": "BUY_BASIS", "entry_date": date, "entry_basis": basis, "entry_z": z}

    if not trades:
        return {"error": "No trades", "basis_df": df}

    trades_df = pd.DataFrame(trades)
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]
    total = len(trades_df)

    eq = np.array(equity)
    total_ret = float(eq[-1] / eq[0] - 1)
    max_dd = float((eq / np.maximum.accumulate(eq) - 1).min())
    std = trades_df["pnl_pct"].std()
    sharpe = float(trades_df["pnl_pct"].mean() / std * np.sqrt(total)) if std > 0 else 0

    return {
        "trades": trades_df,
        "equity": eq.tolist(),
        "basis_df": df,
        "metrics": {
            "total_trades": total,
            "win_rate": len(winning) / max(1, total),
            "avg_win": float(winning["pnl_pct"].mean()) if len(winning) else 0,
            "avg_loss": float(losing["pnl_pct"].mean()) if len(losing) else 0,
            "total_return": total_ret,
            "max_drawdown": max_dd,
            "sharpe_ratio": sharpe,
            "avg_hold_days": float((pd.to_datetime(trades_df["exit_date"]) - pd.to_datetime(trades_df["entry_date"])).dt.days.mean()),
            "profit_factor": float(winning["pnl_pct"].sum() / abs(losing["pnl_pct"].sum())) if len(losing) and losing["pnl_pct"].sum() != 0 else 0,
        },
    }


# ═══════════════════════════════════════════════════════
# SCANNER
# ═══════════════════════════════════════════════════════

def scan_basis_signals(min_dates: int = 50, top_n: int = 50) -> list[dict]:
    """Scan all futures symbols for basis signals."""
    symbols = get_active_futures_symbols(min_dates=min_dates)[:top_n]
    results = []

    for sym in symbols:
        df = load_basis_history(sym, lookback=200)
        if len(df) < 30:
            continue
        sig = generate_basis_signal(df, sym)
        if sig:
            results.append(sig.to_dict())

    results.sort(key=lambda x: abs(x["basis_zscore"]), reverse=True)
    return results
