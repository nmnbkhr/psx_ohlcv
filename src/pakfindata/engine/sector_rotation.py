"""
Sector Rotation Momentum Strategy.

Monthly: long top 3 sectors by 1M momentum, underweight bottom 3.
Computes sector returns from constituent stock EOD data.

PSX sector rotation cycle:
  Rate cuts -> Banking, Cement, Auto
  PKR stable -> Pharma, Tech
  Oil down -> Refineries rally, E&P falls
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict

PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")

# Sector code -> name mapping (PSX codes without leading zero)
SECTOR_NAMES = {
    "801": "Auto Assembler", "802": "Auto Parts", "803": "Cable & Electrical",
    "804": "Cement", "805": "Chemical", "807": "Commercial Banks",
    "808": "Engineering", "809": "Fertilizer", "810": "Food & Personal Care",
    "811": "Glass & Ceramics", "812": "Inv Banks / Securities", "813": "Insurance",
    "815": "Leather", "818": "Modarabas", "819": "Oil & Gas Exploration",
    "820": "Oil & Gas Marketing", "821": "Paper & Board", "822": "Pharma",
    "823": "Power Generation", "824": "Refinery", "825": "Sugar",
    "826": "Synthetic & Rayon", "827": "Technology", "828": "Textile Composite",
    "829": "Textile Spinning", "830": "Textile Weaving", "831": "Tobacco",
    "832": "Transport",
}


def _sqlite_con():
    con = sqlite3.connect(str(PSX_SQLITE), timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    return con


@dataclass
class SectorSignal:
    sector_code: str
    sector_name: str
    return_1m: float
    return_3m: float
    momentum_rank: int
    signal: str       # OVERWEIGHT, NEUTRAL, UNDERWEIGHT
    stocks: int
    avg_volume: float

    def to_dict(self):
        return asdict(self)


# ═══════════════════════════════════════════════════════
# SECTOR RETURNS
# ═══════════════════════════════════════════════════════

def compute_sector_returns(lookback_months: int = 12) -> pd.DataFrame:
    """Compute monthly equal-weighted sector returns from EOD data."""
    import duckdb
    DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")

    if DUCKDB_PATH.exists():
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        cutoff = (datetime.now() - pd.DateOffset(months=lookback_months + 2)).strftime("%Y-%m-%d")
        df = con.execute("""
            SELECT symbol, sector_code, date, close, volume
            FROM eod_ohlcv
            WHERE sector_code IS NOT NULL AND sector_code != ''
              AND close > 0 AND date >= ?
            ORDER BY date
        """, [cutoff]).df()
        con.close()
    else:
        con = _sqlite_con()
        df = pd.read_sql_query("""
            SELECT symbol, sector_code, date, close, volume
            FROM eod_ohlcv
            WHERE sector_code IS NOT NULL AND sector_code != ''
              AND close > 0 AND date >= date('now', ?)
            ORDER BY date
        """, con, params=[f"-{lookback_months + 2} months"])
        con.close()

    if df.empty:
        return pd.DataFrame()

    # Normalize sector codes (remove leading zeros, strip .0)
    df["sector_code"] = df["sector_code"].astype(str).str.replace(".0", "", regex=False).str.lstrip("0")
    df["sector_code"] = df["sector_code"].replace("", "0")

    # Filter to known sectors
    df = df[df["sector_code"].isin(SECTOR_NAMES.keys())]
    if df.empty:
        return pd.DataFrame()

    df["month"] = df["date"].str[:7]

    # Monthly last close per stock
    monthly = df.sort_values("date").groupby(["sector_code", "symbol", "month"]).last().reset_index()
    monthly["stock_return"] = monthly.groupby(["sector_code", "symbol"])["close"].pct_change()

    # Equal-weighted sector return per month
    sector_monthly = monthly.groupby(["sector_code", "month"]).agg(
        sector_return=("stock_return", "mean"),
        stocks=("symbol", "nunique"),
        avg_volume=("volume", "mean"),
    ).reset_index()

    # Pivot to wide format
    pivot = sector_monthly.pivot(index="month", columns="sector_code", values="sector_return")
    pivot = pivot.dropna(how="all").sort_index()

    return pivot


def rank_sectors(lookback_months: int = 12) -> list[SectorSignal]:
    """Rank sectors by 1-month and 3-month momentum."""
    pivot = compute_sector_returns(lookback_months)
    if pivot.empty or len(pivot) < 2:
        return []

    results = []
    for sector_code in pivot.columns:
        series = pivot[sector_code].dropna()
        if len(series) < 2:
            continue

        ret_1m = float(series.iloc[-1]) if len(series) >= 1 else 0
        ret_3m = float(series.iloc[-3:].sum()) if len(series) >= 3 else ret_1m

        results.append({
            "sector_code": sector_code,
            "sector_name": SECTOR_NAMES.get(sector_code, sector_code),
            "return_1m": ret_1m,
            "return_3m": ret_3m,
        })

    if not results:
        return []

    # Rank by 1M return
    results.sort(key=lambda x: x["return_1m"], reverse=True)
    signals = []
    n = len(results)

    for i, r in enumerate(results):
        if i < 3:
            signal = "OVERWEIGHT"
        elif i >= n - 3:
            signal = "UNDERWEIGHT"
        else:
            signal = "NEUTRAL"

        signals.append(SectorSignal(
            sector_code=r["sector_code"],
            sector_name=r["sector_name"],
            return_1m=r["return_1m"],
            return_3m=r["return_3m"],
            momentum_rank=i + 1,
            signal=signal,
            stocks=0,
            avg_volume=0,
        ))

    return signals


# ═══════════════════════════════════════════════════════
# BACKTEST
# ═══════════════════════════════════════════════════════

def backtest_sector_rotation(
    lookback_months: int = 60,
    top_n: int = 3,
    bottom_n: int = 3,
) -> dict:
    """Backtest sector rotation: long top N, short bottom N, monthly rebalance."""
    pivot = compute_sector_returns(lookback_months)
    if pivot.empty or len(pivot) < 6:
        return {"error": f"Not enough data ({len(pivot)} months)"}

    # Forward returns for each month
    equity = [1.0]
    bh_equity = [1.0]
    trades = []

    for i in range(3, len(pivot) - 1):
        # Rank by trailing 1M return
        trailing = pivot.iloc[i]
        valid = trailing.dropna()
        if len(valid) < top_n + bottom_n:
            continue

        ranked = valid.sort_values(ascending=False)
        top_sectors = ranked.index[:top_n].tolist()
        bottom_sectors = ranked.index[-bottom_n:].tolist()

        # Next month returns
        next_returns = pivot.iloc[i + 1]
        month = pivot.index[i + 1]

        # Long top sectors (equal weight)
        long_ret = next_returns[top_sectors].mean() if top_sectors else 0
        # Short bottom sectors (equal weight)
        short_ret = -next_returns[bottom_sectors].mean() if bottom_sectors else 0

        # Combined (50% long, 50% short)
        strategy_ret = 0.5 * long_ret + 0.5 * short_ret
        # Long-only variant
        long_only_ret = long_ret

        # Buy and hold (equal weight all sectors)
        bh_ret = next_returns.mean()

        if not np.isnan(strategy_ret):
            equity.append(equity[-1] * (1 + float(long_only_ret)))
        else:
            equity.append(equity[-1])

        if not np.isnan(bh_ret):
            bh_equity.append(bh_equity[-1] * (1 + float(bh_ret)))
        else:
            bh_equity.append(bh_equity[-1])

        trades.append({
            "month": month,
            "long": [SECTOR_NAMES.get(s, s) for s in top_sectors],
            "short": [SECTOR_NAMES.get(s, s) for s in bottom_sectors],
            "long_ret": float(long_ret) if not np.isnan(long_ret) else 0,
            "short_ret": float(short_ret) if not np.isnan(short_ret) else 0,
            "strategy_ret": float(long_only_ret) if not np.isnan(long_only_ret) else 0,
            "bh_ret": float(bh_ret) if not np.isnan(bh_ret) else 0,
        })

    if not trades:
        return {"error": "No trades generated"}

    trades_df = pd.DataFrame(trades)
    eq = np.array(equity)
    bh = np.array(bh_equity)

    strat_ret = float(eq[-1] / eq[0] - 1)
    bh_ret = float(bh[-1] / bh[0] - 1)
    strat_vol = float(trades_df["strategy_ret"].std() * np.sqrt(12))
    bh_vol = float(trades_df["bh_ret"].std() * np.sqrt(12))
    strat_sharpe = float(trades_df["strategy_ret"].mean() / trades_df["strategy_ret"].std() * np.sqrt(12)) if trades_df["strategy_ret"].std() > 0 else 0
    strat_dd = float((eq / np.maximum.accumulate(eq) - 1).min())
    bh_dd = float((bh / np.maximum.accumulate(bh) - 1).min())

    # Win rate
    wins = (trades_df["strategy_ret"] > trades_df["bh_ret"]).sum()

    return {
        "trades": trades_df,
        "equity": eq.tolist(),
        "bh_equity": bh.tolist(),
        "metrics": {
            "strategy_return": strat_ret,
            "bh_return": bh_ret,
            "alpha": strat_ret - bh_ret,
            "strategy_sharpe": strat_sharpe,
            "strategy_vol": strat_vol,
            "strategy_max_dd": strat_dd,
            "bh_max_dd": bh_dd,
            "months": len(trades_df),
            "win_rate": float(wins / len(trades_df)) if len(trades_df) > 0 else 0,
        },
    }
