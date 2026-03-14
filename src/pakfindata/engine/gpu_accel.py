"""
GPU Acceleration for Fund Analytics using RAPIDS cuDF.

Architecture:
- Try importing cudf at module level
- If available (RTX 4080 detected): use cuDF for bulk operations
- If not: fall back to pandas/numpy (identical API)
- GPU is MOST useful for:
  1. Batch NAV processing (1.9M rows x multiple windows)
  2. Rolling statistics across 1,190 funds simultaneously
  3. Correlation matrix computation (1190 x 1190)
  4. Monte Carlo simulations for VaR

Setup:
    conda install -c rapidsai -c conda-forge -c nvidia cudf=24.12 cupy cuda-version=12.0
    Falls back gracefully if install fails.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

try:
    import cudf
    import cupy as cp
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False


TRADING_DAYS = 252


def get_engine():
    """Return cudf or pandas based on availability."""
    return cudf if GPU_AVAILABLE else pd


def _to_gpu(df: pd.DataFrame) -> "cudf.DataFrame | pd.DataFrame":
    """Convert pandas DataFrame to cuDF if GPU available."""
    if GPU_AVAILABLE:
        return cudf.DataFrame.from_pandas(df)
    return df


def _to_cpu(df) -> pd.DataFrame:
    """Convert cuDF DataFrame back to pandas."""
    if GPU_AVAILABLE and hasattr(df, "to_pandas"):
        return df.to_pandas()
    return df


# ---------------------------------------------------------------------------
# Batch Rolling Sharpe
# ---------------------------------------------------------------------------

def batch_rolling_sharpe(
    all_navs: pd.DataFrame,
    window: int = 63,
    risk_free_rate: float = 0.1208,
) -> pd.DataFrame:
    """Compute rolling Sharpe for ALL funds simultaneously.

    On GPU (cuDF): ~10-50x speedup for 1,190 funds x 1,900 days.
    On CPU (pandas): still vectorized, just slower.

    Args:
        all_navs: DataFrame with columns = fund_names, index = dates.
        window: Rolling window in trading days.
        risk_free_rate: Annualized risk-free rate.

    Returns:
        DataFrame of rolling Sharpe values (same shape as input).
    """
    rf_daily = (1 + risk_free_rate) ** (1 / TRADING_DAYS) - 1

    if GPU_AVAILABLE:
        gdf = cudf.DataFrame.from_pandas(all_navs)
        log_rets = cudf.DataFrame()
        for col in gdf.columns:
            log_rets[col] = cp.log(gdf[col] / gdf[col].shift(1))

        excess = log_rets - rf_daily
        roll_mean = excess.rolling(window).mean()
        roll_std = log_rets.rolling(window).std(ddof=1)

        sharpe = (roll_mean / roll_std) * math.sqrt(TRADING_DAYS)
        return sharpe.to_pandas()
    else:
        log_rets = np.log(all_navs / all_navs.shift(1))
        excess = log_rets - rf_daily
        roll_mean = excess.rolling(window).mean()
        roll_std = log_rets.rolling(window).std(ddof=1)
        return (roll_mean / roll_std) * math.sqrt(TRADING_DAYS)


# ---------------------------------------------------------------------------
# Batch Correlation Matrix
# ---------------------------------------------------------------------------

def batch_correlation_matrix(
    all_navs: pd.DataFrame,
    window: int = 252,
) -> pd.DataFrame:
    """Fund-to-fund correlation matrix.

    1,190 funds = 1,190 x 1,190 = 1.4M correlations.
    On GPU: cupy.corrcoef -> sub-second.
    On CPU: numpy.corrcoef -> ~2-5 seconds.

    Args:
        all_navs: DataFrame with columns = fund_names, index = dates.
        window: Lookback window for returns.

    Returns:
        DataFrame correlation matrix (fund x fund).
    """
    log_rets = np.log(all_navs / all_navs.shift(1)).dropna()
    recent = log_rets.iloc[-window:] if len(log_rets) > window else log_rets

    # Drop columns with all NaN
    recent = recent.dropna(axis=1, how="all")

    if GPU_AVAILABLE:
        gpu_arr = cp.asarray(recent.values.T)
        # Replace NaN with 0 for correlation computation
        gpu_arr = cp.nan_to_num(gpu_arr, nan=0.0)
        corr = cp.corrcoef(gpu_arr)
        corr_np = cp.asnumpy(corr)
    else:
        arr = recent.values.T
        arr = np.nan_to_num(arr, nan=0.0)
        corr_np = np.corrcoef(arr)

    return pd.DataFrame(corr_np, index=recent.columns, columns=recent.columns)


# ---------------------------------------------------------------------------
# Monte Carlo VaR
# ---------------------------------------------------------------------------

def monte_carlo_var(
    nav_series: pd.Series,
    n_simulations: int = 100_000,
    horizon_days: int = 21,
    confidence: float = 0.95,
) -> dict:
    """Monte Carlo VaR using GPU-accelerated random number generation.

    On GPU: cupy.random.normal -> 100K sims in <100ms.
    On CPU: numpy.random.normal -> 100K sims in ~500ms.

    Args:
        nav_series: Daily NAV values.
        n_simulations: Number of Monte Carlo simulations.
        horizon_days: Forecast horizon in trading days.
        confidence: VaR confidence level.

    Returns:
        Dict with var_95, var_99, cvar_95, cvar_99, expected_nav,
        nav_5th_pct, nav_95th_pct.
    """
    nav = nav_series.dropna()
    if len(nav) < 30:
        return {
            "var_95": None, "var_99": None,
            "cvar_95": None, "cvar_99": None,
            "expected_nav": None, "nav_5th_pct": None, "nav_95th_pct": None,
        }

    log_rets = np.log(nav / nav.shift(1)).dropna()
    mu = float(log_rets.mean())
    sigma = float(log_rets.std(ddof=1))
    current_nav = float(nav.iloc[-1])

    if GPU_AVAILABLE:
        rng = cp.random.normal(mu, sigma, size=(n_simulations, horizon_days))
        cumulative = cp.exp(cp.cumsum(rng, axis=1))
        final_values = cp.asnumpy(cumulative[:, -1]) * current_nav
    else:
        rng = np.random.normal(mu, sigma, size=(n_simulations, horizon_days))
        cumulative = np.exp(np.cumsum(rng, axis=1))
        final_values = cumulative[:, -1] * current_nav

    returns = (final_values - current_nav) / current_nav

    var_95 = float(np.percentile(returns, (1 - 0.95) * 100))
    var_99 = float(np.percentile(returns, (1 - 0.99) * 100))
    cvar_95 = float(returns[returns <= var_95].mean()) if (returns <= var_95).any() else var_95
    cvar_99 = float(returns[returns <= var_99].mean()) if (returns <= var_99).any() else var_99

    return {
        "var_95": var_95,
        "var_99": var_99,
        "cvar_95": cvar_95,
        "cvar_99": cvar_99,
        "expected_nav": float(np.mean(final_values)),
        "nav_5th_pct": float(np.percentile(final_values, 5)),
        "nav_95th_pct": float(np.percentile(final_values, 95)),
    }


# ---------------------------------------------------------------------------
# Batch Volatility
# ---------------------------------------------------------------------------

def batch_rolling_volatility(
    all_navs: pd.DataFrame,
    window: int = 63,
) -> pd.DataFrame:
    """Compute annualized rolling volatility for all funds simultaneously.

    Args:
        all_navs: DataFrame with columns = fund_names, index = dates.
        window: Rolling window.

    Returns:
        DataFrame of rolling annualized volatility.
    """
    if GPU_AVAILABLE:
        gdf = cudf.DataFrame.from_pandas(all_navs)
        log_rets = cudf.DataFrame()
        for col in gdf.columns:
            log_rets[col] = cp.log(gdf[col] / gdf[col].shift(1))
        vol = log_rets.rolling(window).std(ddof=1) * math.sqrt(TRADING_DAYS)
        return vol.to_pandas()
    else:
        log_rets = np.log(all_navs / all_navs.shift(1))
        return log_rets.rolling(window).std(ddof=1) * math.sqrt(TRADING_DAYS)
