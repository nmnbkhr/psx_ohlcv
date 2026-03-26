"""
Pairs Trading (Statistical Arbitrage) Engine.

Finds cointegrated stock pairs on PSX using:
  1. Correlation pre-filter (fast)
  2. Engle-Granger cointegration test
  3. Kalman filter dynamic hedge ratio

Trading logic:
  Spread = Price_A - beta * Price_B
  Z-score = (Spread - mu) / sigma
  Entry: |Z| > entry_threshold (default 2.0)
  Exit:  |Z| < exit_threshold (default 0.5)
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from itertools import combinations

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245
TRANSACTION_COST_PCT = 0.005  # 0.5% round-trip

KNOWN_PAIR_CANDIDATES = [
    ("OGDC", "PPL"), ("HBL", "UBL"), ("LUCK", "DGKC"),
    ("ENGRO", "FFC"), ("MCB", "ABL"), ("HUBC", "KAPCO"),
    ("PSO", "SHEL"), ("LUCK", "MLCF"), ("BAHL", "MEBL"),
    ("NBP", "BOP"), ("MARI", "PPL"), ("ATRL", "NRL"), ("MTL", "CHCC"),
]


@dataclass
class PairStats:
    symbol_a: str
    symbol_b: str
    sector: str
    correlation: float
    cointegration_pvalue: float
    is_cointegrated: bool
    hedge_ratio_static: float
    hedge_ratio_kalman: float
    half_life: float
    spread_mean: float
    spread_std: float
    current_zscore: float
    hurst_exponent: float
    lookback_days: int
    adf_statistic: float
    adf_pvalue: float


@dataclass
class PairsSignal:
    symbol_a: str
    symbol_b: str
    date: str
    spread: float
    zscore: float
    hedge_ratio: float
    signal: str
    confidence: float
    position_a: str
    position_b: str
    shares_a: int
    shares_b: int
    reason: str


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_pair_prices(symbol_a: str, symbol_b: str, days: int = 500):
    """Load aligned close prices for a pair from DuckDB."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    df_a = con.execute("""
        SELECT date, close FROM eod_ohlcv
        WHERE symbol = ? AND CAST(date AS DATE) >= ?::DATE ORDER BY date
    """, [symbol_a, cutoff]).df()

    df_b = con.execute("""
        SELECT date, close FROM eod_ohlcv
        WHERE symbol = ? AND CAST(date AS DATE) >= ?::DATE ORDER BY date
    """, [symbol_b, cutoff]).df()

    con.close()

    if df_a.empty or df_b.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.DataFrame()

    df_a = df_a.rename(columns={"close": "price_a"})
    df_b = df_b.rename(columns={"close": "price_b"})

    merged = df_a.merge(df_b, on="date", how="inner").sort_values("date").reset_index(drop=True)

    return merged["price_a"], merged["price_b"], merged


# ---------------------------------------------------------------------------
# Cointegration tests
# ---------------------------------------------------------------------------

def _compute_hurst(returns: np.ndarray, max_lag: int = 50) -> float:
    if len(returns) < max_lag:
        return 0.5
    lags = range(2, min(max_lag, len(returns) // 2))
    tau = []
    for lag in lags:
        tau.append(np.std(np.subtract(returns[lag:], returns[:-lag])))
    if not tau or any(t <= 0 for t in tau):
        return 0.5
    try:
        poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
        return float(poly[0])
    except Exception:
        return 0.5


def test_cointegration(prices_a: pd.Series, prices_b: pd.Series) -> dict:
    """Test cointegration using Engle-Granger method."""
    if len(prices_a) < 60 or len(prices_b) < 60:
        return {"is_cointegrated": False, "error": "Insufficient data"}

    try:
        from statsmodels.tsa.stattools import coint, adfuller

        score, pvalue, _ = coint(prices_a.values, prices_b.values)
        beta = np.polyfit(prices_b.values, prices_a.values, 1)[0]
        spread = prices_a.values - beta * prices_b.values

        adf_result = adfuller(spread, maxlag=20)

        # Half-life of mean reversion
        spread_lag = spread[:-1]
        spread_diff = np.diff(spread)
        if len(spread_lag) > 10:
            beta_mr = np.polyfit(spread_lag, spread_diff, 1)[0]
            half_life = -np.log(2) / beta_mr if beta_mr < 0 else 999
        else:
            half_life = 999

        returns = np.diff(spread) / (np.abs(spread[:-1]) + 1e-10)
        hurst = _compute_hurst(returns)

        return {
            "is_cointegrated": pvalue < 0.05,
            "coint_pvalue": float(pvalue),
            "coint_statistic": float(score),
            "hedge_ratio": float(beta),
            "spread": spread,
            "spread_mean": float(np.mean(spread)),
            "spread_std": float(np.std(spread)),
            "half_life": float(half_life),
            "adf_statistic": float(adf_result[0]),
            "adf_pvalue": float(adf_result[1]),
            "hurst": float(hurst),
        }

    except ImportError:
        return _simple_cointegration(prices_a, prices_b)


def _simple_cointegration(prices_a: pd.Series, prices_b: pd.Series) -> dict:
    """Fallback without statsmodels."""
    corr = prices_a.corr(prices_b)
    beta = np.polyfit(prices_b.values, prices_a.values, 1)[0]
    spread = prices_a.values - beta * prices_b.values

    n = len(spread)
    half = n // 2
    var1 = np.var(spread[:half])
    var2 = np.var(spread[half:])
    var_ratio = var1 / var2 if var2 > 0 else 999
    is_stationary = 0.5 < var_ratio < 2.0

    half_life = 999
    spread_lag = spread[:-1]
    spread_diff = np.diff(spread)
    if len(spread_lag) > 10:
        beta_mr = np.polyfit(spread_lag, spread_diff, 1)[0]
        half_life = -np.log(2) / beta_mr if beta_mr < 0 else 999

    return {
        "is_cointegrated": corr > 0.8 and is_stationary,
        "coint_pvalue": 1.0 - corr,
        "coint_statistic": 0,
        "hedge_ratio": float(beta),
        "spread": spread,
        "spread_mean": float(np.mean(spread)),
        "spread_std": float(np.std(spread)),
        "half_life": float(half_life),
        "adf_statistic": 0,
        "adf_pvalue": 1.0 - corr,
        "hurst": 0.5,
    }


# ---------------------------------------------------------------------------
# Kalman filter hedge ratio
# ---------------------------------------------------------------------------

def kalman_hedge_ratio(prices_a: pd.Series, prices_b: pd.Series) -> pd.Series:
    """Time-varying hedge ratio via Kalman filter."""
    try:
        from pykalman import KalmanFilter

        obs = prices_a.values
        obs_mat = np.expand_dims(prices_b.values, axis=(1, 2))

        kf = KalmanFilter(
            n_dim_obs=1,
            n_dim_state=1,
            initial_state_mean=[np.polyfit(prices_b.values[:60], prices_a.values[:60], 1)[0]],
            initial_state_covariance=np.array([[1.0]]),
            transition_matrices=np.array([[1.0]]),
            observation_matrices=obs_mat,
            observation_covariance=np.array([[1.0]]),
            transition_covariance=np.array([[0.01]]),
        )

        state_means, _ = kf.filter(obs.reshape(-1, 1))
        return pd.Series(state_means.flatten(), index=prices_a.index)

    except ImportError:
        # Fallback: rolling OLS
        window = 60
        ratios = []
        for i in range(len(prices_a)):
            if i < window:
                ratios.append(np.nan)
            else:
                beta = np.polyfit(prices_b.values[i - window:i], prices_a.values[i - window:i], 1)[0]
                ratios.append(beta)
        return pd.Series(ratios, index=prices_a.index)


# ---------------------------------------------------------------------------
# Pair discovery
# ---------------------------------------------------------------------------

def find_cointegrated_pairs(
    min_correlation: float = 0.7,
    max_pvalue: float = 0.05,
    min_half_life: float = 5,
    max_half_life: float = 60,
    min_days: int = 250,
    sector_only: bool = True,
    top_n: int = 20,
) -> list[PairStats]:
    """Scan liquid PSX pairs for cointegration."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=int(min_days * 1.5))).strftime("%Y-%m-%d")

    symbols_df = con.execute(f"""
        SELECT symbol,
               COUNT(*) as days,
               AVG(volume) as avg_vol,
               MAX(sector_code) as sector
        FROM eod_ohlcv
        WHERE CAST(date AS DATE) >= '{cutoff}'::DATE
        GROUP BY symbol
        HAVING COUNT(*) >= {min_days} AND AVG(volume) > 100000
        ORDER BY AVG(volume) DESC
        LIMIT 100
    """).df()

    con.close()

    if symbols_df.empty:
        return []

    symbols = symbols_df["symbol"].tolist()
    sectors = dict(zip(symbols_df["symbol"], symbols_df["sector"].fillna("Unknown")))

    # Generate candidates
    if sector_only and any(s != "Unknown" for s in sectors.values()):
        sector_groups = {}
        for sym, sec in sectors.items():
            sector_groups.setdefault(sec, []).append(sym)
        candidates = []
        for sec, syms in sector_groups.items():
            if len(syms) >= 2:
                candidates.extend(combinations(syms, 2))
    else:
        candidates = list(combinations(symbols[:50], 2))

    # Add known candidates
    for a, b in KNOWN_PAIR_CANDIDATES:
        if a in symbols and b in symbols and (a, b) not in candidates and (b, a) not in candidates:
            candidates.append((a, b))

    results = []

    for sym_a, sym_b in candidates:
        prices_a, prices_b, merged = load_pair_prices(sym_a, sym_b, days=min_days)
        if len(prices_a) < min_days * 0.8:
            continue

        corr = prices_a.corr(prices_b)
        if abs(corr) < min_correlation:
            continue

        coint_result = test_cointegration(prices_a, prices_b)
        if not coint_result.get("is_cointegrated", False):
            continue

        half_life = coint_result.get("half_life", 999)
        if half_life < min_half_life or half_life > max_half_life:
            continue

        kalman_ratios = kalman_hedge_ratio(prices_a, prices_b)
        latest_kalman = (
            kalman_ratios.dropna().iloc[-1]
            if not kalman_ratios.dropna().empty
            else coint_result["hedge_ratio"]
        )

        spread = prices_a.values - latest_kalman * prices_b.values
        window = max(20, int(half_life * 2))
        spread_mean = np.mean(spread[-window:])
        spread_std = np.std(spread[-window:])
        current_z = (spread[-1] - spread_mean) / spread_std if spread_std > 0 else 0

        sector = sectors.get(sym_a, sectors.get(sym_b, "Unknown"))

        results.append(PairStats(
            symbol_a=sym_a, symbol_b=sym_b, sector=sector,
            correlation=corr,
            cointegration_pvalue=coint_result["coint_pvalue"],
            is_cointegrated=True,
            hedge_ratio_static=coint_result["hedge_ratio"],
            hedge_ratio_kalman=float(latest_kalman),
            half_life=half_life,
            spread_mean=spread_mean, spread_std=spread_std,
            current_zscore=float(current_z),
            hurst_exponent=coint_result.get("hurst", 0.5),
            lookback_days=len(prices_a),
            adf_statistic=coint_result.get("adf_statistic", 0),
            adf_pvalue=coint_result.get("adf_pvalue", 1),
        ))

    results.sort(key=lambda x: x.half_life)
    return results[:top_n]


# ---------------------------------------------------------------------------
# Signal generation
# ---------------------------------------------------------------------------

def generate_pairs_signal(
    symbol_a: str, symbol_b: str,
    entry_zscore: float = 2.0, exit_zscore: float = 0.5,
    lookback_days: int = 500, capital: float = 1_000_000,
) -> PairsSignal | None:
    """Generate current trading signal for a pair."""
    prices_a, prices_b, merged = load_pair_prices(symbol_a, symbol_b, lookback_days)
    if merged.empty or len(merged) < 60:
        return None

    kalman_ratios = kalman_hedge_ratio(prices_a, prices_b)
    hedge_ratio = (
        kalman_ratios.dropna().iloc[-1]
        if not kalman_ratios.dropna().empty
        else np.polyfit(prices_b.values, prices_a.values, 1)[0]
    )

    spread = prices_a.values - hedge_ratio * prices_b.values
    coint_result = test_cointegration(prices_a, prices_b)
    half_life = coint_result.get("half_life", 30)
    window = max(20, min(120, int(half_life * 2)))

    spread_mean = np.mean(spread[-window:])
    spread_std = np.std(spread[-window:])
    zscore = (spread[-1] - spread_mean) / spread_std if spread_std > 0 else 0

    price_a = prices_a.iloc[-1]
    price_b = prices_b.iloc[-1]
    date = str(merged["date"].iloc[-1])[:10]

    signal = "HOLD"
    position_a = position_b = ""
    confidence = 0.0
    reason = ""

    if zscore > entry_zscore:
        signal = "SHORT_SPREAD"
        position_a, position_b = "SELL", "BUY"
        confidence = min(1.0, abs(zscore) / 4)
        reason = (f"Z-score {zscore:.2f} > {entry_zscore} -- spread overextended. "
                  f"Sell {symbol_a}, Buy {symbol_b}. Half-life: {half_life:.0f}d.")
    elif zscore < -entry_zscore:
        signal = "LONG_SPREAD"
        position_a, position_b = "BUY", "SELL"
        confidence = min(1.0, abs(zscore) / 4)
        reason = (f"Z-score {zscore:.2f} < -{entry_zscore} -- spread compressed. "
                  f"Buy {symbol_a}, Sell {symbol_b}. Half-life: {half_life:.0f}d.")
    elif abs(zscore) < exit_zscore:
        signal = "EXIT"
        confidence = 0.5
        reason = f"Z-score {zscore:.2f} normalized -- close any open position"
    else:
        signal = "HOLD"
        confidence = 0.2
        reason = f"Z-score {zscore:.2f} between entry/exit thresholds"

    half_capital = capital / 2
    shares_a = int(half_capital / price_a) if price_a > 0 else 0
    shares_b = int(half_capital / (price_b * abs(hedge_ratio))) if price_b > 0 and hedge_ratio != 0 else 0

    return PairsSignal(
        symbol_a=symbol_a, symbol_b=symbol_b, date=date,
        spread=float(spread[-1]), zscore=float(zscore),
        hedge_ratio=float(hedge_ratio), signal=signal,
        confidence=confidence, position_a=position_a,
        position_b=position_b, shares_a=shares_a, shares_b=shares_b,
        reason=reason,
    )


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------

def backtest_pairs_strategy(
    symbol_a: str, symbol_b: str,
    entry_zscore: float = 2.0, exit_zscore: float = 0.5,
    stop_loss_zscore: float = 4.0, max_hold_days: int = 60,
    use_kalman: bool = True, lookback_days: int = 500,
    transaction_cost: float = TRANSACTION_COST_PCT,
) -> dict:
    """Backtest pairs trading strategy."""
    prices_a, prices_b, merged = load_pair_prices(symbol_a, symbol_b, lookback_days)
    if merged.empty or len(merged) < 120:
        return {"error": f"Insufficient data for {symbol_a}/{symbol_b}"}

    if use_kalman:
        hedge_ratios = kalman_hedge_ratio(prices_a, prices_b)
    else:
        static_beta = np.polyfit(prices_b.values[:120], prices_a.values[:120], 1)[0]
        hedge_ratios = pd.Series([static_beta] * len(prices_a), index=prices_a.index)

    spread = prices_a.values - hedge_ratios.values * prices_b.values

    coint_result = test_cointegration(prices_a, prices_b)
    half_life = coint_result.get("half_life", 30)
    window = max(20, min(120, int(half_life * 2)))

    spread_mean = pd.Series(spread).rolling(window, min_periods=20).mean().values
    spread_std = pd.Series(spread).rolling(window, min_periods=20).std().values
    zscore = np.where(spread_std > 0, (spread - spread_mean) / spread_std, 0)

    trades = []
    position = None

    for i in range(window, len(merged)):
        z = zscore[i]
        if np.isnan(z):
            continue

        if position is not None:
            days_held = i - position["entry_idx"]
            spread_change = spread[i] - position["entry_spread"]

            if position["direction"] == "LONG_SPREAD":
                pnl_raw = spread_change
            else:
                pnl_raw = -spread_change

            entry_price_a = prices_a.iloc[position["entry_idx"]]
            pnl_pct = pnl_raw / entry_price_a if entry_price_a > 0 else 0

            exit_reason = None
            if abs(z) < exit_zscore:
                exit_reason = "MEAN_REVERT"
            elif abs(z) > stop_loss_zscore:
                exit_reason = "STOP_LOSS"
            elif days_held >= max_hold_days:
                exit_reason = "MAX_HOLD"

            if exit_reason:
                total_pnl = pnl_pct - transaction_cost
                trades.append({
                    "entry_date": str(merged.iloc[position["entry_idx"]]["date"])[:10],
                    "exit_date": str(merged.iloc[i]["date"])[:10],
                    "direction": position["direction"],
                    "entry_z": position["entry_z"],
                    "exit_z": z,
                    "entry_spread": position["entry_spread"],
                    "exit_spread": spread[i],
                    "pnl_pct": total_pnl,
                    "days_held": days_held,
                    "exit_reason": exit_reason,
                    "hedge_ratio": float(hedge_ratios.iloc[i]),
                })
                position = None

        if position is None:
            if z > entry_zscore:
                position = {
                    "entry_idx": i, "direction": "SHORT_SPREAD",
                    "entry_spread": spread[i], "entry_z": z,
                }
            elif z < -entry_zscore:
                position = {
                    "entry_idx": i, "direction": "LONG_SPREAD",
                    "entry_spread": spread[i], "entry_z": z,
                }

    if not trades:
        return {
            "error": "No trades generated",
            "spread_data": pd.DataFrame({
                "date": merged["date"], "spread": spread, "zscore": zscore,
                "price_a": prices_a.values, "price_b": prices_b.values,
                "hedge_ratio": hedge_ratios.values,
            }),
        }

    trades_df = pd.DataFrame(trades)
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]

    trades_df["cum_return"] = (1 + trades_df["pnl_pct"]).cumprod()
    total_return = trades_df["cum_return"].iloc[-1] - 1
    max_dd = (trades_df["cum_return"] / trades_df["cum_return"].cummax() - 1).min()

    days_span = (pd.to_datetime(trades_df["exit_date"].iloc[-1]) -
                 pd.to_datetime(trades_df["entry_date"].iloc[0])).days
    years = days_span / 365 if days_span > 0 else 1
    ann_return = (1 + total_return) ** (1 / years) - 1
    avg_hold = trades_df["days_held"].mean()
    ann_vol = trades_df["pnl_pct"].std() * np.sqrt(TRADING_DAYS / avg_hold) if avg_hold > 0 else 0
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0

    return {
        "trades": trades_df,
        "spread_data": pd.DataFrame({
            "date": merged["date"], "spread": spread, "zscore": zscore,
            "price_a": prices_a.values, "price_b": prices_b.values,
            "hedge_ratio": hedge_ratios.values,
        }),
        "pair_stats": coint_result,
        "metrics": {
            "total_trades": len(trades_df),
            "win_rate": len(winning) / len(trades_df) if len(trades_df) > 0 else 0,
            "avg_win": float(winning["pnl_pct"].mean()) if len(winning) > 0 else 0,
            "avg_loss": float(losing["pnl_pct"].mean()) if len(losing) > 0 else 0,
            "profit_factor": (
                abs(winning["pnl_pct"].sum() / losing["pnl_pct"].sum())
                if len(losing) > 0 and losing["pnl_pct"].sum() != 0 else 0
            ),
            "total_return": float(total_return),
            "annualized_return": float(ann_return),
            "annualized_vol": float(ann_vol),
            "sharpe_ratio": float(sharpe),
            "max_drawdown": float(max_dd),
            "avg_days_held": float(avg_hold),
            "half_life": coint_result.get("half_life", 0),
            "hurst": coint_result.get("hurst", 0.5),
            "cointegration_pvalue": coint_result.get("coint_pvalue", 1),
            "exit_reasons": trades_df["exit_reason"].value_counts().to_dict(),
            "use_kalman": use_kalman,
            "transaction_cost": transaction_cost,
        },
    }


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def scan_pair_opportunities(entry_zscore: float = 1.5, max_pairs: int = 20) -> pd.DataFrame:
    """Scan cointegrated pairs for current opportunities."""
    pairs = find_cointegrated_pairs(top_n=30)

    results = []
    for pair in pairs:
        if abs(pair.current_zscore) > entry_zscore:
            direction = "SHORT_SPREAD" if pair.current_zscore > 0 else "LONG_SPREAD"
        elif abs(pair.current_zscore) > 1.0:
            direction = "WATCH"
        else:
            continue

        results.append({
            "pair": f"{pair.symbol_a}/{pair.symbol_b}",
            "symbol_a": pair.symbol_a,
            "symbol_b": pair.symbol_b,
            "sector": pair.sector,
            "zscore": pair.current_zscore,
            "direction": direction,
            "half_life": pair.half_life,
            "correlation": pair.correlation,
            "coint_pvalue": pair.cointegration_pvalue,
            "hedge_ratio": pair.hedge_ratio_kalman,
            "hurst": pair.hurst_exponent,
            "spread_std": pair.spread_std,
        })

    if not results:
        return pd.DataFrame()

    return (pd.DataFrame(results)
            .sort_values("zscore", key=abs, ascending=False)
            .head(max_pairs)
            .reset_index(drop=True))
