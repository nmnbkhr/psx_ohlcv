"""
Synthetic Depth Heatmap — reconstructs order book depth from L1 tick data.

Input: Tick-level data with bid/ask price and volume from tick_logs or JSONL.
Output: 2D matrices (price x time) for bid/ask/trade intensity.

Techniques:
1. Price-time footprint: how long did bid/ask rest at each price?
2. Volume accumulation: bid/ask size changes at same price level
3. Trade-at-price: executed volume per price level
4. Iceberg detection: constant bid_vol but trades keep executing
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.db.connections import analytics_con, jsonl
except ImportError:
    analytics_con = None
    jsonl = None


@dataclass
class DepthHeatmapData:
    """Output of the heatmap builder."""
    price_levels: list[float]
    time_bins: list[str]
    bid_matrix: list[list[float]]
    ask_matrix: list[list[float]]
    trade_matrix: list[list[float]]
    last_price: float
    bid_price: float
    ask_price: float
    symbol: str
    date: str
    stats: dict


def build_heatmap(
    symbol: str,
    date: str = None,
    price_granularity: float = 0.25,
    time_granularity: int = 60,
    source: str = "auto",
) -> DepthHeatmapData | None:
    """Build a synthetic depth heatmap from L1 tick data.

    Args:
        symbol: Stock symbol (e.g., "OGDC")
        date: Date string YYYY-MM-DD. If None, uses latest.
        price_granularity: Price step for Y-axis binning (PKR)
        time_granularity: Time step for X-axis binning (seconds)
        source: "tick_logs", "jsonl", "ohlcv", "auto"
    """
    ticks = _load_ticks(symbol, date, source)
    if ticks is None or ticks.empty or len(ticks) < 10:
        return None

    has_bid_ask = "bid" in ticks.columns and "ask" in ticks.columns

    # Determine price range from all available price columns
    all_prices = []
    for col in ("price", "bid", "ask"):
        if col in ticks.columns:
            vals = ticks[col].dropna()
            vals = vals[vals > 0]
            all_prices.extend(vals.tolist())

    if not all_prices:
        return None

    # Use P2-P98 to trim outliers that waste canvas space
    p2 = float(np.percentile(all_prices, 2))
    p98 = float(np.percentile(all_prices, 98))
    price_min = p2 - price_granularity * 3
    price_max = p98 + price_granularity * 3
    price_levels = np.arange(price_min, price_max + price_granularity, price_granularity)
    price_levels = np.round(price_levels, 2)
    n_prices = len(price_levels)

    if n_prices > 300:
        price_granularity *= 2
        price_levels = np.arange(price_min, price_max + price_granularity, price_granularity)
        price_levels = np.round(price_levels, 2)
        n_prices = len(price_levels)

    # Time axis from timestamps
    ts_col = "timestamp" if "timestamp" in ticks.columns else "ts"
    raw_ts = ticks[ts_col]
    if raw_ts.dtype in ("float64", "int64"):
        ts_dt = pd.to_datetime(raw_ts, unit="s", utc=True).dt.tz_convert("Asia/Karachi")
    else:
        ts_dt = pd.to_datetime(raw_ts, utc=True).dt.tz_convert("Asia/Karachi")
    ts_seconds = (ts_dt - ts_dt.iloc[0]).dt.total_seconds().values

    total_seconds = ts_seconds[-1] - ts_seconds[0]
    if total_seconds <= 0:
        return None

    n_bins = max(1, int(total_seconds / time_granularity) + 1)

    time_bins = []
    t0 = ts_dt.iloc[0]
    for i in range(n_bins):
        t = t0 + pd.Timedelta(seconds=i * time_granularity)
        time_bins.append(t.strftime("%H:%M"))

    # Initialize matrices
    bid_matrix = np.zeros((n_prices, n_bins))
    ask_matrix = np.zeros((n_prices, n_bins))
    trade_matrix = np.zeros((n_prices, n_bins))

    def price_idx(p):
        idx = int(round((p - price_min) / price_granularity))
        return max(0, min(n_prices - 1, idx))

    def time_idx(t):
        idx = int(t / time_granularity)
        return max(0, min(n_bins - 1, idx))

    # Prepare arrays — convert cumulative volume to per-tick delta
    prices = ticks.get("price", pd.Series(dtype=float)).values
    raw_volumes = ticks.get("volume", pd.Series(1, index=ticks.index)).values
    # tick_logs volume is cumulative daily → compute incremental
    vol_diff = np.diff(raw_volumes, prepend=0)
    volumes = np.where(vol_diff > 0, vol_diff, 1)  # fallback to 1 if not cumulative

    if has_bid_ask:
        bids = ticks["bid"].values
        asks = ticks["ask"].values
        bid_vols = ticks.get("bid_vol", ticks.get("bidVol", pd.Series(1, index=ticks.index))).values
        ask_vols = ticks.get("ask_vol", ticks.get("askVol", pd.Series(1, index=ticks.index))).values
    else:
        bids = asks = bid_vols = ask_vols = None

    for i in range(len(ticks)):
        t_idx = time_idx(ts_seconds[i])

        # Trade at price
        p = prices[i] if i < len(prices) else 0
        v = volumes[i] if i < len(volumes) else 1
        if p > 0 and not np.isnan(p):
            trade_matrix[price_idx(p)][t_idx] += float(v) if v and not np.isnan(v) else 1

        if has_bid_ask:
            b = bids[i]
            a = asks[i]
            if b > 0 and not np.isnan(b):
                bv = bid_vols[i] if bid_vols is not None and not np.isnan(bid_vols[i]) else 1
                bid_matrix[price_idx(b)][t_idx] += float(bv)
            if a > 0 and not np.isnan(a):
                av = ask_vols[i] if ask_vols is not None and not np.isnan(ask_vols[i]) else 1
                ask_matrix[price_idx(a)][t_idx] += float(av)
        elif p > 0 and not np.isnan(p):
            v_half = (float(v) if v and not np.isnan(v) else 1) * 0.5
            bid_matrix[price_idx(p - price_granularity)][t_idx] += v_half
            ask_matrix[price_idx(p + price_granularity)][t_idx] += v_half

    # Normalize to 0-100
    for matrix in [bid_matrix, ask_matrix, trade_matrix]:
        mx = matrix.max()
        if mx > 0:
            matrix *= 100.0 / mx

    # Current state
    last_row = ticks.iloc[-1]
    last_price = float(last_row.get("price", 0))
    bid_price = float(last_row.get("bid", last_price - price_granularity)) if has_bid_ask else last_price
    ask_price = float(last_row.get("ask", last_price + price_granularity)) if has_bid_ask else last_price

    # Stats + support/resistance levels
    bid_vol_by_price = bid_matrix.sum(axis=1)
    ask_vol_by_price = ask_matrix.sum(axis=1)

    top_bid = sorted(range(n_prices), key=lambda i: bid_vol_by_price[i], reverse=True)[:5]
    top_ask = sorted(range(n_prices), key=lambda i: ask_vol_by_price[i], reverse=True)[:5]

    stats = {
        "total_ticks": len(ticks),
        "has_bid_ask": has_bid_ask,
        "price_range": f"{min(all_prices):.2f} - {max(all_prices):.2f}",
        "time_range": f"{time_bins[0]} - {time_bins[-1]}" if time_bins else "",
        "n_price_levels": n_prices,
        "n_time_bins": n_bins,
        "price_granularity": price_granularity,
        "top_bid_levels": [{"price": float(price_levels[i]), "volume": float(bid_vol_by_price[i])}
                           for i in top_bid if bid_vol_by_price[i] > 0],
        "top_ask_levels": [{"price": float(price_levels[i]), "volume": float(ask_vol_by_price[i])}
                           for i in top_ask if ask_vol_by_price[i] > 0],
    }

    return DepthHeatmapData(
        price_levels=[float(p) for p in price_levels],
        time_bins=time_bins,
        bid_matrix=bid_matrix.tolist(),
        ask_matrix=ask_matrix.tolist(),
        trade_matrix=trade_matrix.tolist(),
        last_price=last_price,
        bid_price=bid_price,
        ask_price=ask_price,
        symbol=symbol,
        date=date or "latest",
        stats=stats,
    )


def _load_ticks(symbol: str, date: str = None, source: str = "auto") -> pd.DataFrame | None:
    """Load tick data from available sources.

    Priority: tick_logs (DuckDB, has bid/ask) > JSONL > ohlcv_5s (fallback).
    """
    # Try tick_logs DuckDB table first
    if source in ("tick_logs", "auto"):
        try:
            con = analytics_con()
            if date is None:
                date = str(con.execute(
                    "SELECT MAX(date) FROM tick_logs WHERE symbol = ?", [symbol]
                ).fetchone()[0])

            df = con.execute("""
                SELECT timestamp, price, volume, bid, ask, bid_vol, ask_vol
                FROM tick_logs
                WHERE symbol = ? AND date = ?
                ORDER BY timestamp
            """, [symbol, date]).df()
            con.close()

            if not df.empty:
                return df
        except Exception:
            pass

    # Try JSONL
    if source in ("jsonl", "auto") and jsonl is not None:
        try:
            if date is None:
                from pathlib import Path
                files = sorted(Path("/mnt/e/psxdata/tick_logs_cloud").glob("*.jsonl"))
                if files:
                    date = files[-1].stem.replace("ticks_", "")

            if date:
                df = jsonl(date, symbol=symbol)
                if df is not None and not df.empty:
                    return df
        except Exception:
            pass

    # Fallback: ohlcv_5s
    if source in ("ohlcv", "auto"):
        try:
            con = analytics_con()
            if date is None:
                date = str(con.execute(
                    "SELECT MAX(ts::DATE) FROM ohlcv_5s WHERE symbol = ?", [symbol]
                ).fetchone()[0])

            df = con.execute("""
                SELECT ts as timestamp, c as price, v as volume
                FROM ohlcv_5s
                WHERE symbol = ? AND ts::DATE = ?
                ORDER BY ts
            """, [symbol, date]).df()
            con.close()

            if not df.empty:
                return df
        except Exception:
            pass

    return None


def get_available_dates(symbol: str = "OGDC", limit: int = 30) -> list[str]:
    """Get dates with tick data available for a symbol."""
    try:
        con = analytics_con()
        df = con.execute("""
            SELECT DISTINCT date FROM tick_logs
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
        """, [symbol, limit]).df()
        con.close()
        if not df.empty:
            return [str(d) for d in df["date"].tolist()]
    except Exception:
        pass

    # Fallback to ohlcv_5s dates
    try:
        con = analytics_con()
        df = con.execute("""
            SELECT DISTINCT ts::DATE as date FROM ohlcv_5s
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
        """, [symbol, limit]).df()
        con.close()
        return [str(d) for d in df["date"].tolist()]
    except Exception:
        return []
