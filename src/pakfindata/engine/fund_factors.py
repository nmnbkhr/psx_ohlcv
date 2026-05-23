"""
Factor Analysis & Volatility Module for Fund Analytics.

Provides rolling volatility, MA crossover signals, CAPM/multi-factor
regressions, and peer comparison ranking.

Usage:
    from pakfindata.engine.fund_factors import (
        rolling_volatility, nav_ma_signals, single_factor_regression, peer_rank,
    )
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from pakfindata.engine.fund_risk import (
    TRADING_DAYS,
    _daily_rf,
    _log_returns,
    _simple_returns,
)


# ---------------------------------------------------------------------------
# Volatility Tracking
# ---------------------------------------------------------------------------

def rolling_volatility(
    nav_series: pd.Series,
    windows: list[int] | None = None,
) -> pd.DataFrame:
    """Annualized rolling volatility at multiple windows.

    Args:
        nav_series: Daily NAV values.
        windows: Rolling window sizes (default [21, 63, 252]).

    Returns:
        DataFrame with columns vol_21d, vol_63d, vol_252d etc.
    """
    if windows is None:
        windows = [21, 63, 252]

    rets = _log_returns(nav_series)
    result = pd.DataFrame(index=rets.index)
    for w in windows:
        result[f"vol_{w}d"] = rets.rolling(w).std(ddof=1) * math.sqrt(TRADING_DAYS)
    return result


def volatility_regime(
    nav_series: pd.Series,
    lookback: int = 252,
) -> str:
    """Classify current volatility regime vs historical.

    Args:
        nav_series: Daily NAV values.
        lookback: Historical lookback window.

    Returns:
        "LOW", "NORMAL", "HIGH", or "EXTREME".
    """
    rets = _log_returns(nav_series)
    if len(rets) < lookback:
        return "INSUFFICIENT_DATA"

    recent = rets.iloc[-lookback:]
    current_vol = rets.iloc[-21:].std(ddof=1) * math.sqrt(TRADING_DAYS)
    hist_vol = recent.std(ddof=1) * math.sqrt(TRADING_DAYS)

    # Percentile of current 21d vol vs rolling 21d vol distribution
    rolling_21 = rets.rolling(21).std(ddof=1) * math.sqrt(TRADING_DAYS)
    rolling_21 = rolling_21.dropna()
    if len(rolling_21) < 2:
        return "INSUFFICIENT_DATA"

    percentile = float((rolling_21 < current_vol).mean() * 100)

    if percentile < 25:
        return "LOW"
    elif percentile < 75:
        return "NORMAL"
    elif percentile < 95:
        return "HIGH"
    else:
        return "EXTREME"


# ---------------------------------------------------------------------------
# Moving Average Crossovers
# ---------------------------------------------------------------------------

def nav_ma_signals(
    nav_series: pd.Series,
    fast: int = 20,
    slow: int = 50,
) -> pd.DataFrame:
    """Moving average crossover signals on fund NAV.

    Args:
        nav_series: Daily NAV values.
        fast: Fast MA window (default 20).
        slow: Slow MA window (default 50).

    Returns:
        DataFrame with nav, ma_fast, ma_slow, signal (1/-1/0),
        crossover_date, days_since_cross.
    """
    nav = nav_series.dropna().sort_index()
    df = pd.DataFrame({"nav": nav})
    df["ma_fast"] = nav.rolling(fast).mean()
    df["ma_slow"] = nav.rolling(slow).mean()
    df = df.dropna()

    if df.empty:
        return df

    # Position: 1 when fast > slow (bullish), -1 when below
    df["position"] = np.where(df["ma_fast"] > df["ma_slow"], 1, -1)

    # Signal: detect crossover points (position changes)
    df["signal"] = df["position"].diff().fillna(0).astype(int)
    # Normalize to -1, 0, 1
    df["signal"] = df["signal"].clip(-1, 1)

    # Last crossover date and days since
    crossovers = df[df["signal"] != 0]
    if not crossovers.empty:
        last_cross = crossovers.index[-1]
        df["crossover_date"] = last_cross
        df["days_since_cross"] = (df.index - last_cross).days
    else:
        df["crossover_date"] = pd.NaT
        df["days_since_cross"] = np.nan

    return df


# ---------------------------------------------------------------------------
# Factor Regressions
# ---------------------------------------------------------------------------

def single_factor_regression(
    fund_nav: pd.Series,
    benchmark_nav: pd.Series,
    risk_free_rate: float = 0.1208,
) -> dict:
    """CAPM single-factor regression: R_fund - R_f = alpha + beta*(R_mkt - R_f) + eps.

    Uses numpy.linalg.lstsq (no sklearn dependency).

    Args:
        fund_nav: Daily fund NAV values.
        benchmark_nav: Daily benchmark NAV values.
        risk_free_rate: Annualized risk-free rate.

    Returns:
        Dict with alpha, beta, r_squared, alpha_pvalue, beta_pvalue, residual_std.
    """
    fr = _log_returns(fund_nav)
    br = _log_returns(benchmark_nav)
    aligned = pd.DataFrame({"fund": fr, "bench": br}).dropna()

    if len(aligned) < 30:
        return {
            "alpha": None, "beta": None, "r_squared": None,
            "alpha_pvalue": None, "beta_pvalue": None, "residual_std": None,
        }

    rf_daily = _daily_rf(risk_free_rate)
    y = (aligned["fund"] - rf_daily).values
    x = (aligned["bench"] - rf_daily).values

    # OLS: y = alpha + beta * x
    X = np.column_stack([np.ones(len(x)), x])
    result, residuals, rank, sv = np.linalg.lstsq(X, y, rcond=None)
    alpha_daily, beta = result

    # Predictions and R-squared
    y_hat = X @ result
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Standard errors and p-values
    n = len(y)
    k = 2  # intercept + slope
    mse = ss_res / (n - k) if n > k else 0.0
    residual_std = math.sqrt(mse)

    # Covariance matrix of coefficients
    try:
        cov = mse * np.linalg.inv(X.T @ X)
        se_alpha = math.sqrt(cov[0, 0])
        se_beta = math.sqrt(cov[1, 1])

        # t-statistics → two-tailed p-values
        from scipy.stats import t as t_dist
        t_alpha = alpha_daily / se_alpha if se_alpha > 0 else 0.0
        t_beta = beta / se_beta if se_beta > 0 else 0.0
        alpha_pval = float(2 * (1 - t_dist.cdf(abs(t_alpha), df=n - k)))
        beta_pval = float(2 * (1 - t_dist.cdf(abs(t_beta), df=n - k)))
    except (np.linalg.LinAlgError, ValueError):
        alpha_pval = None
        beta_pval = None

    return {
        "alpha": float(alpha_daily * TRADING_DAYS),  # annualized
        "beta": float(beta),
        "r_squared": float(r_squared),
        "alpha_pvalue": alpha_pval,
        "beta_pvalue": beta_pval,
        "residual_std": float(residual_std),
    }


def multi_factor_regression(
    fund_nav: pd.Series,
    factors: dict[str, pd.Series],
) -> dict:
    """Multi-factor regression for fund style analysis.

    Args:
        fund_nav: Daily fund NAV values.
        factors: Dict mapping factor names to NAV/rate series.
            e.g., {"KSE100": kse_nav, "KIBOR": kibor_series}

    Returns:
        Dict with alpha, betas, r_squared, factor_pvalues.
    """
    fr = _log_returns(fund_nav)
    factor_rets = {}
    for name, series in factors.items():
        factor_rets[name] = _log_returns(series)

    # Align all series
    df = pd.DataFrame({"fund": fr, **factor_rets}).dropna()
    if len(df) < 30:
        return {"alpha": None, "betas": {}, "r_squared": None, "factor_pvalues": {}}

    y = df["fund"].values
    factor_names = [c for c in df.columns if c != "fund"]
    X = np.column_stack([np.ones(len(y))] + [df[f].values for f in factor_names])

    result, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    alpha_daily = result[0]
    betas = {name: float(result[i + 1]) for i, name in enumerate(factor_names)}

    # R-squared
    y_hat = X @ result
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # P-values
    n = len(y)
    k = len(result)
    mse = ss_res / (n - k) if n > k else 0.0
    factor_pvalues = {}
    try:
        cov = mse * np.linalg.inv(X.T @ X)
        from scipy.stats import t as t_dist
        for i, name in enumerate(factor_names):
            se = math.sqrt(cov[i + 1, i + 1])
            t_stat = betas[name] / se if se > 0 else 0.0
            factor_pvalues[name] = float(2 * (1 - t_dist.cdf(abs(t_stat), df=n - k)))
    except (np.linalg.LinAlgError, ValueError):
        pass

    return {
        "alpha": float(alpha_daily * TRADING_DAYS),
        "betas": betas,
        "r_squared": float(r_squared),
        "factor_pvalues": factor_pvalues,
    }


# ---------------------------------------------------------------------------
# Peer Comparison
# ---------------------------------------------------------------------------

def peer_rank(
    fund_analytics: dict,
    peer_analytics: list[dict],
    metric: str = "sharpe_1y",
) -> dict:
    """Rank fund within its category peers.

    Args:
        fund_analytics: Output of generate_fund_analytics() for the target fund.
        peer_analytics: List of generate_fund_analytics() outputs for peers.
        metric: Metric to rank by (e.g., "sharpe_1y", "max_drawdown").

    Returns:
        Dict with rank, total_peers, percentile, quartile, category stats.
    """
    def _extract(a: dict, m: str):
        if m == "sharpe_1y":
            return a.get("risk", {}).get("sharpe_1y")
        elif m == "sortino_1y":
            return a.get("risk", {}).get("sortino_1y")
        elif m == "max_drawdown":
            return a.get("risk", {}).get("max_drawdown")
        elif m == "volatility_1y":
            return a.get("risk", {}).get("volatility_1y_ann")
        elif m.startswith("return_"):
            period = m.replace("return_", "")
            perf = a.get("performance", {}).get(period, {})
            return perf.get("return") if isinstance(perf, dict) else None
        return None

    fund_val = _extract(fund_analytics, metric)
    if fund_val is None:
        return {
            "rank": None, "total_peers": len(peer_analytics),
            "percentile": None, "quartile": None,
            "category_avg": None, "category_median": None,
            "outperforms_peers_pct": None,
        }

    # Collect peer values
    all_vals = [fund_val]
    for pa in peer_analytics:
        v = _extract(pa, metric)
        if v is not None:
            all_vals.append(v)

    all_vals_sorted = sorted(all_vals, reverse=(metric != "max_drawdown"))
    rank = all_vals_sorted.index(fund_val) + 1
    total = len(all_vals)
    percentile = (1 - (rank - 1) / total) * 100 if total > 0 else 0
    quartile = min(4, max(1, math.ceil(rank / total * 4))) if total > 0 else None

    peer_vals = [v for v in all_vals if v != fund_val] or all_vals
    outperforms = sum(1 for v in peer_vals if fund_val >= v) / len(peer_vals) * 100 if peer_vals else 0

    return {
        "rank": rank,
        "total_peers": total,
        "percentile": round(percentile, 1),
        "quartile": quartile,
        "category_avg": float(np.mean(all_vals)),
        "category_median": float(np.median(all_vals)),
        "outperforms_peers_pct": round(outperforms, 1),
    }
