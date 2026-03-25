"""
CVD Divergence Trading Strategy.

Detects divergences between price and Cumulative Volume Delta (CVD).
- BEARISH: Price new high + CVD lower high -> distribution -> SELL
- BULLISH: Price new low + CVD higher low -> accumulation -> BUY

Tick classification: price >= ask -> BUY, price <= bid -> SELL, else midpoint rule.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import timezone, timedelta
from dataclasses import dataclass, asdict
from enum import Enum

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")


class DivType(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    HIDDEN_BULLISH = "HIDDEN_BULLISH"
    HIDDEN_BEARISH = "HIDDEN_BEARISH"
    NONE = "NONE"


@dataclass
class CVDDivergence:
    div_type: DivType
    symbol: str
    date: str
    detected_at: str
    price_pivot_1: float
    price_pivot_2: float
    cvd_pivot_1: float
    cvd_pivot_2: float
    signal: str          # BUY, SELL, HOLD
    confidence: float
    reason: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["div_type"] = self.div_type.value
        return d


def _duck_con():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def load_ticks(symbol: str, date_str: str) -> pd.DataFrame:
    con = _duck_con()
    df = con.execute(
        "SELECT price, volume, bid, ask, bid_vol, ask_vol, change, timestamp, _ts "
        "FROM tick_logs WHERE symbol = ? AND SUBSTR(_ts, 1, 10) = ? "
        "ORDER BY timestamp", [symbol, date_str],
    ).df()
    con.close()
    return df


def get_tick_dates(symbol: str) -> list[str]:
    con = _duck_con()
    rows = con.execute(
        "SELECT DISTINCT SUBSTR(_ts, 1, 10) AS d FROM tick_logs "
        "WHERE symbol = ? ORDER BY d DESC", [symbol],
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def get_liquid_symbols(date_str: str, top_n: int = 50) -> list[str]:
    con = _duck_con()
    rows = con.execute(
        "SELECT symbol, COUNT(*) AS c FROM tick_logs "
        "WHERE SUBSTR(_ts, 1, 10) = ? AND market = 'REG' "
        "GROUP BY symbol HAVING c >= 200 ORDER BY c DESC LIMIT ?",
        [date_str, top_n],
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════
# CVD COMPUTATION
# ═══════════════════════════════════════════════════════

def compute_cvd(ticks: pd.DataFrame) -> pd.DataFrame:
    """Compute CVD from tick data. Classifies each tick as buy/sell."""
    if ticks.empty:
        return pd.DataFrame()

    df = ticks.copy()
    price = df["price"].values
    bid = df["bid"].fillna(0).values
    ask = df["ask"].fillna(0).values
    change = df["change"].fillna(0).values

    # Classify: price >= ask -> BUY, price <= bid -> SELL, else midpoint
    direction = np.where(
        (ask > 0) & (price >= ask), 1,
        np.where(
            (bid > 0) & (price <= bid), -1,
            np.where(
                (bid > 0) & (ask > 0), np.where(price >= (bid + ask) / 2, 1, -1),
                np.where(change >= 0, 1, -1)
            )
        )
    )

    df["direction"] = np.where(direction == 1, "BUY", "SELL")
    df["delta"] = direction * df["volume"].values
    df["cvd"] = df["delta"].cumsum()
    df["buy_vol_cum"] = np.where(direction == 1, df["volume"].values, 0).cumsum()
    df["sell_vol_cum"] = np.where(direction == -1, df["volume"].values, 0).cumsum()

    return df


def detect_swings(series: np.ndarray, window: int = 10) -> tuple[list[int], list[int]]:
    """Detect swing highs and lows."""
    highs, lows = [], []
    n = len(series)
    for i in range(window, n - window):
        left = series[max(0, i - window):i]
        right = series[i + 1:min(n, i + window + 1)]
        if series[i] >= np.max(left) and series[i] >= np.max(right):
            highs.append(i)
        if series[i] <= np.min(left) and series[i] <= np.min(right):
            lows.append(i)
    return highs, lows


def detect_divergences(
    cvd_df: pd.DataFrame,
    swing_window: int = 10,
    min_price_move: float = 0.003,
) -> tuple[pd.DataFrame, list[CVDDivergence]]:
    """Detect CVD divergences. Returns (5min bars DataFrame, list of divergences)."""
    if cvd_df.empty or len(cvd_df) < 50:
        return pd.DataFrame(), []

    df = cvd_df.copy()
    if "_ts" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["_ts"]):
        df["_ts"] = pd.to_datetime(df["_ts"])

    ts_col = "_ts" if "_ts" in df.columns else "timestamp"

    # Resample to 5-min bars
    bars = df.set_index(ts_col).resample("5min").agg({
        "price": ["first", "last", "max", "min"],
        "cvd": "last",
        "volume": "sum",
    }).dropna()
    bars.columns = ["open", "close", "high", "low", "cvd", "volume"]
    bars = bars.reset_index().rename(columns={ts_col: "bar_time"})

    if len(bars) < swing_window * 3:
        return bars, []

    price_highs, price_lows = detect_swings(bars["close"].values, swing_window)
    symbol = str(cvd_df.get("symbol", pd.Series([""])).iloc[0]) if "symbol" in cvd_df.columns else ""
    date_str = ""

    divergences = []

    # BEARISH: price higher high + CVD lower high
    for i in range(1, len(price_highs)):
        i1, i2 = price_highs[i - 1], price_highs[i]
        p1, p2 = bars.iloc[i1]["close"], bars.iloc[i2]["close"]
        c1, c2 = bars.iloc[i1]["cvd"], bars.iloc[i2]["cvd"]

        if (p2 - p1) / max(p1, 0.01) > min_price_move and c2 < c1:
            conf = min(1.0, abs(c2 - c1) / max(abs(c1), 1))
            divergences.append(CVDDivergence(
                div_type=DivType.BEARISH, symbol=symbol, date=date_str,
                detected_at=str(bars.iloc[i2]["bar_time"]),
                price_pivot_1=float(p1), price_pivot_2=float(p2),
                cvd_pivot_1=float(c1), cvd_pivot_2=float(c2),
                signal="SELL", confidence=conf,
                reason=f"Price higher high ({p2:.2f}>{p1:.2f}) but CVD declining — distribution",
            ))

    # BULLISH: price lower low + CVD higher low
    for i in range(1, len(price_lows)):
        i1, i2 = price_lows[i - 1], price_lows[i]
        p1, p2 = bars.iloc[i1]["close"], bars.iloc[i2]["close"]
        c1, c2 = bars.iloc[i1]["cvd"], bars.iloc[i2]["cvd"]

        if (p1 - p2) / max(p1, 0.01) > min_price_move and c2 > c1:
            conf = min(1.0, abs(c2 - c1) / max(abs(c1), 1))
            divergences.append(CVDDivergence(
                div_type=DivType.BULLISH, symbol=symbol, date=date_str,
                detected_at=str(bars.iloc[i2]["bar_time"]),
                price_pivot_1=float(p1), price_pivot_2=float(p2),
                cvd_pivot_1=float(c1), cvd_pivot_2=float(c2),
                signal="BUY", confidence=conf,
                reason=f"Price lower low ({p2:.2f}<{p1:.2f}) but CVD rising — accumulation",
            ))

    return bars, divergences


# ═══════════════════════════════════════════════════════
# ANALYSIS FOR A SINGLE SYMBOL+DATE
# ═══════════════════════════════════════════════════════

def analyze_cvd(symbol: str, date_str: str) -> dict | None:
    """Full CVD analysis for a symbol on a date."""
    ticks = load_ticks(symbol, date_str)
    if len(ticks) < 50:
        return None

    cvd_df = compute_cvd(ticks)
    if cvd_df.empty:
        return None

    bars, divs = detect_divergences(cvd_df)

    total_buy = float(cvd_df[cvd_df["direction"] == "BUY"]["volume"].sum())
    total_sell = float(cvd_df[cvd_df["direction"] == "SELL"]["volume"].sum())
    total = total_buy + total_sell

    # CVD slope (linear fit)
    if len(cvd_df) > 10:
        x = np.arange(len(cvd_df))
        slope = float(np.polyfit(x, cvd_df["cvd"].values, 1)[0])
    else:
        slope = 0.0

    return {
        "symbol": symbol,
        "date": date_str,
        "ticks": len(cvd_df),
        "cvd_final": float(cvd_df["cvd"].iloc[-1]),
        "cvd_slope": slope,
        "buy_volume": total_buy,
        "sell_volume": total_sell,
        "buy_sell_ratio": total_buy / total if total > 0 else 0.5,
        "divergences": [d.to_dict() for d in divs],
        "bars": bars,
        "cvd_series": cvd_df[["_ts", "price", "cvd", "volume", "direction"]].copy() if "_ts" in cvd_df.columns else cvd_df[["price", "cvd", "volume", "direction"]].copy(),
    }


# ═══════════════════════════════════════════════════════
# SCANNER
# ═══════════════════════════════════════════════════════

def scan_divergences(date_str: str | None = None, top_n: int = 40) -> list[dict]:
    """Scan top symbols for CVD divergences."""
    if date_str is None:
        con = _duck_con()
        r = con.execute("SELECT MAX(SUBSTR(_ts, 1, 10)) FROM tick_logs").fetchone()
        con.close()
        date_str = r[0] if r and r[0] else None
        if not date_str:
            return []

    symbols = get_liquid_symbols(date_str, top_n=top_n)
    results = []

    for sym in symbols:
        analysis = analyze_cvd(sym, date_str)
        if not analysis:
            continue
        results.append({
            "symbol": sym,
            "date": date_str,
            "ticks": analysis["ticks"],
            "cvd_final": analysis["cvd_final"],
            "cvd_slope": analysis["cvd_slope"],
            "buy_sell_ratio": analysis["buy_sell_ratio"],
            "divergences": len(analysis["divergences"]),
            "div_types": ", ".join(d["div_type"] for d in analysis["divergences"]) or "NONE",
            "signal": analysis["divergences"][-1]["signal"] if analysis["divergences"] else "HOLD",
            "confidence": analysis["divergences"][-1]["confidence"] if analysis["divergences"] else 0,
        })

    results.sort(key=lambda x: x["divergences"], reverse=True)
    return results
