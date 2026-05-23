"""
ML Feature Engineering for PSX Price Prediction.

Extracts features from DuckDB tables, computes technical indicators,
and builds training/prediction datasets.

All features are computed in raw numpy/pandas -- NO TA libraries.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
TRADING_DAYS = 245


def get_eod_features(symbol: str, lookback_days: int = 500) -> pd.DataFrame:
    """
    Extract EOD features for a symbol from DuckDB.
    Returns DataFrame with date, OHLCV, and 40+ computed features.
    """
    con = analytics_con()

    df = con.execute("""
        SELECT date, open, high, low, close, volume
        FROM eod_ohlcv
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT ?
    """, [symbol, lookback_days]).df()

    # con is cached singleton — do not close

    if df.empty:
        return df

    df = df.sort_values("date").reset_index(drop=True)

    # -- Price features --
    df["returns"] = df["close"].pct_change()
    df["log_returns"] = np.log(df["close"] / df["close"].shift(1))
    df["range_pct"] = (df["high"] - df["low"]) / df["close"]
    df["body_pct"] = abs(df["close"] - df["open"]) / df["close"]
    df["upper_shadow"] = (df["high"] - df[["close", "open"]].max(axis=1)) / df["close"]
    df["lower_shadow"] = (df[["close", "open"]].min(axis=1) - df["low"]) / df["close"]
    df["gap"] = (df["open"] - df["close"].shift(1)) / df["close"].shift(1)

    # -- Moving averages --
    for window in [5, 10, 20, 50, 100, 200]:
        df[f"sma_{window}"] = df["close"].rolling(window).mean()
        df[f"close_vs_sma_{window}"] = (df["close"] - df[f"sma_{window}"]) / df[f"sma_{window}"]

    # -- EMA --
    for window in [12, 26]:
        df[f"ema_{window}"] = df["close"].ewm(span=window, adjust=False).mean()

    # MACD
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # -- RSI --
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # -- Bollinger Bands --
    bb_sma = df["close"].rolling(20).mean()
    bb_std = df["close"].rolling(20).std()
    df["bb_upper"] = bb_sma + 2 * bb_std
    df["bb_lower"] = bb_sma - 2 * bb_std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / bb_sma
    df["bb_position"] = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

    # -- Volatility --
    for window in [5, 10, 20, 60]:
        df[f"vol_{window}d"] = df["log_returns"].rolling(window).std() * np.sqrt(TRADING_DAYS)

    df["vol_ratio"] = df["vol_5d"] / df["vol_20d"].replace(0, np.nan)

    # -- Volume features --
    df["vol_sma_20"] = df["volume"].rolling(20).mean()
    df["vol_ratio_20"] = df["volume"] / df["vol_sma_20"].replace(0, np.nan)
    df["vol_change"] = df["volume"].pct_change()

    # -- Momentum --
    for period in [1, 5, 10, 20, 60]:
        df[f"mom_{period}d"] = df["close"].pct_change(period)

    # -- Hurst exponent (simplified R/S) --
    def hurst_rs(series, window=100):
        result = np.full(len(series), np.nan)
        for i in range(window, len(series)):
            s = series.iloc[i - window:i].values
            mean_val = np.mean(s)
            deviate = np.cumsum(s - mean_val)
            R = np.max(deviate) - np.min(deviate)
            S = np.std(s, ddof=1)
            if S > 0:
                result[i] = np.log(R / S) / np.log(window)
            else:
                result[i] = 0.5
        return result

    df["hurst"] = hurst_rs(df["log_returns"], window=100)

    # -- Mean reversion signals --
    df["dist_from_52w_high"] = df["close"] / df["high"].rolling(TRADING_DAYS).max() - 1
    df["dist_from_52w_low"] = df["close"] / df["low"].rolling(TRADING_DAYS).min() - 1

    # -- SMA crossover signals --
    df["sma_20_50_cross"] = np.where(df["sma_20"] > df["sma_50"], 1, -1)
    df["sma_50_200_cross"] = np.where(df["sma_50"] > df["sma_200"], 1, -1)

    # -- Day of week --
    df["day_of_week"] = pd.to_datetime(df["date"]).dt.dayofweek

    return df


def get_tick_features(symbol: str, date_str: str) -> dict:
    """
    Extract tick-level features for a symbol on a specific date.
    Returns a dict of features to be merged with EOD data.
    """
    try:
        con = analytics_con()

        source_file = f"ticks_{date_str}.jsonl"
        df = con.execute("""
            SELECT price, volume, bid, ask, bid_vol, ask_vol,
                   change, trades, timestamp
            FROM tick_logs
            WHERE symbol = ? AND source_file = ?
            ORDER BY timestamp
        """, [symbol, source_file]).df()

        # con is cached singleton — do not close

        if df.empty or len(df) < 10:
            return {}

        return _compute_tick_features(df)

    except Exception:
        return {}


def get_tick_features_batch(symbol: str, dates: list[str]) -> dict[str, dict]:
    """Batch compute tick features for a symbol across multiple dates.

    Single query fetches all dates at once, then groups by source_file.
    Returns {date_str: {feature_dict}} for dates that have data.
    """
    try:
        con = analytics_con()

        source_files = [f"ticks_{d}.jsonl" for d in dates]
        df = con.execute("""
            SELECT price, volume, bid, ask, bid_vol, ask_vol,
                   change, trades, timestamp, source_file
            FROM tick_logs
            WHERE symbol = ? AND source_file = ANY(?)
            ORDER BY source_file, timestamp
        """, [symbol, source_files]).df()
        # con is cached singleton — do not close

        if df.empty:
            return {}

        results = {}
        for src_file, group in df.groupby("source_file"):
            date_str = src_file.replace("ticks_", "").replace(".jsonl", "")
            if len(group) >= 10:
                results[date_str] = _compute_tick_features(group)

        return results

    except Exception:
        return {}


def _compute_tick_features(df: pd.DataFrame) -> dict:
    """Compute tick-level features from a DataFrame of ticks."""
    features = {}
    features["tick_count"] = len(df)

    # VPIN (simplified)
    n_buckets = min(50, len(df) // 10)
    if n_buckets > 5:
        bucket_size = len(df) // n_buckets
        buy_vol = 0
        sell_vol = 0
        for i in range(n_buckets):
            chunk = df.iloc[i * bucket_size:(i + 1) * bucket_size]
            price_change = chunk["price"].iloc[-1] - chunk["price"].iloc[0]
            vol = chunk["volume"].iloc[-1] - chunk["volume"].iloc[0] if len(chunk) > 1 else 0
            if vol < 0:
                vol = 0
            if price_change >= 0:
                buy_vol += vol
            else:
                sell_vol += vol
        total = buy_vol + sell_vol
        features["vpin"] = abs(buy_vol - sell_vol) / total if total > 0 else 0

    # Bid-ask spread
    spreads = df["ask"] - df["bid"]
    spreads = spreads[spreads > 0]
    if len(spreads) > 0:
        features["median_spread"] = float(spreads.median())
        features["mean_spread"] = float(spreads.mean())

    # Order flow imbalance
    if "bid_vol" in df.columns and "ask_vol" in df.columns:
        bid_v = df["bid_vol"].dropna()
        ask_v = df["ask_vol"].dropna()
        if len(bid_v) > 0 and len(ask_v) > 0:
            total_bid = bid_v.sum()
            total_ask = ask_v.sum()
            denom = total_bid + total_ask
            features["ofi"] = float((total_bid - total_ask) / denom) if denom > 0 else 0

    # Price volatility intraday
    if len(df) > 1:
        intraday_returns = df["price"].pct_change().dropna()
        features["intraday_vol"] = float(intraday_returns.std())
        features["intraday_skew"] = float(intraday_returns.skew())
        features["intraday_kurt"] = float(intraday_returns.kurtosis())

    return features


def build_dataset(
    symbols: list[str] | None = None,
    lookback_days: int = 500,
    target_horizon: int = 1,
    include_ticks: bool = False,
) -> pd.DataFrame:
    """
    Build ML training dataset for multiple symbols.

    Args:
        symbols: list of symbols (None = top 50 by volume)
        lookback_days: days of history per symbol
        target_horizon: predict N-day forward return
        include_ticks: include tick-level features (slower)

    Returns:
        DataFrame with features + target column
    """
    if symbols is None:
        con = analytics_con()
        symbols = [r[0] for r in con.execute("""
            SELECT symbol FROM eod_ohlcv
            WHERE date >= (SELECT MAX(date) FROM eod_ohlcv)
            GROUP BY symbol
            ORDER BY SUM(volume) DESC
            LIMIT 50
        """).fetchall()]
        # con is cached singleton — do not close

    all_data = []

    for sym in symbols:
        df = get_eod_features(sym, lookback_days)
        if df.empty or len(df) < 100:
            continue

        # Target: N-day forward return
        df["target"] = df["close"].shift(-target_horizon) / df["close"] - 1

        # Binary target: 1 = positive return, 0 = negative
        df["target_direction"] = (df["target"] > 0).astype(int)

        df["symbol"] = sym

        # Add tick features if requested (batch: one query per symbol)
        if include_ticks:
            dates_list = df["date"].astype(str).tolist()
            tick_batch = get_tick_features_batch(sym, dates_list)
            if tick_batch:
                for idx, row in df.iterrows():
                    feats = tick_batch.get(str(row["date"]), {})
                    for k, v in feats.items():
                        df.loc[idx, k] = v

        all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.dropna(subset=["target"])

    return combined


def compute_return_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Add historical move-size stats for expected value computation.

    New columns:
      - hist_up_move_avg: rolling 60d average of positive daily returns (%)
      - hist_dn_move_avg: rolling 60d average of negative daily returns (%, positive number)
      - hist_volatility:  rolling 20d realized volatility (annualized %)
    """
    df = df.copy()
    df = df.sort_values("date").reset_index(drop=True)

    returns = df["close"].pct_change() * 100

    # Rolling average of positive returns (last 60 days)
    pos_ret = returns.where(returns > 0)
    df["hist_up_move_avg"] = pos_ret.rolling(60, min_periods=20).mean()

    # Rolling average of negative returns (absolute value)
    neg_ret = returns.where(returns < 0)
    df["hist_dn_move_avg"] = neg_ret.rolling(60, min_periods=20).mean().abs()

    # Realized volatility (20-day, annualized)
    df["hist_volatility"] = returns.rolling(20, min_periods=10).std() * np.sqrt(245)

    # Forward-fill for last row (today's prediction)
    for col in ["hist_up_move_avg", "hist_dn_move_avg", "hist_volatility"]:
        df[col] = df[col].ffill()

    return df


# Feature columns for ML (exclude date, symbol, target, raw OHLCV)
FEATURE_COLS = [
    "returns", "log_returns", "range_pct", "body_pct", "upper_shadow", "lower_shadow", "gap",
    "close_vs_sma_5", "close_vs_sma_10", "close_vs_sma_20", "close_vs_sma_50",
    "close_vs_sma_100", "close_vs_sma_200",
    "macd", "macd_signal", "macd_hist",
    "rsi_14", "bb_width", "bb_position",
    "vol_5d", "vol_10d", "vol_20d", "vol_60d", "vol_ratio",
    "vol_ratio_20", "vol_change",
    "mom_1d", "mom_5d", "mom_10d", "mom_20d", "mom_60d",
    "hurst", "dist_from_52w_high", "dist_from_52w_low",
    "sma_20_50_cross", "sma_50_200_cross", "day_of_week",
]

TICK_FEATURE_COLS = [
    "tick_count", "vpin", "median_spread", "mean_spread",
    "ofi", "intraday_vol", "intraday_skew", "intraday_kurt",
]
