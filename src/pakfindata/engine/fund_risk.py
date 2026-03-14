"""
Core Risk Metrics Engine for Mutual Fund Analytics.

All functions accept NAV series (not returns) and compute returns internally
for consistency. Annualization uses 252 trading days (industry standard).

Usage:
    from pakfindata.engine.fund_risk import rolling_sharpe, maximum_drawdown

    sharpe = rolling_sharpe(nav_series, window=63, risk_free_rate=0.1208)
    dd = maximum_drawdown(nav_series)
"""

from __future__ import annotations

import math
from datetime import datetime

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

TRADING_DAYS = 252


def _log_returns(nav: pd.Series) -> pd.Series:
    """Compute log returns from NAV series, dropping first NaN."""
    nav = nav.dropna()
    return np.log(nav / nav.shift(1)).dropna()


def _simple_returns(nav: pd.Series) -> pd.Series:
    """Compute simple (arithmetic) returns from NAV series."""
    nav = nav.dropna()
    return nav.pct_change().dropna()


def _daily_rf(annual_rate: float) -> float:
    """Convert annual risk-free rate to daily."""
    return (1 + annual_rate) ** (1 / TRADING_DAYS) - 1


# ---------------------------------------------------------------------------
# Rolling Sharpe Ratio
# ---------------------------------------------------------------------------

def rolling_sharpe(
    nav_series: pd.Series,
    window: int = 63,
    risk_free_rate: float = 0.1208,
    annualize: bool = True,
) -> pd.Series:
    """Rolling Sharpe Ratio = (R_p - R_f) / sigma_p.

    Args:
        nav_series: Daily NAV values (not returns).
        window: Rolling window in trading days (63 ~ 3 months).
        risk_free_rate: Annualized risk-free rate (default KIBOR 6M = 12.08%).
        annualize: If True, annualize the ratio.

    Returns:
        Series of rolling Sharpe values, NaN where window insufficient.
    """
    rets = _log_returns(nav_series)
    rf_daily = _daily_rf(risk_free_rate)
    excess = rets - rf_daily

    roll_mean = excess.rolling(window).mean()
    roll_std = rets.rolling(window).std(ddof=1)

    sharpe = roll_mean / roll_std
    if annualize:
        sharpe = sharpe * math.sqrt(TRADING_DAYS)
    return sharpe


# ---------------------------------------------------------------------------
# Rolling Sortino Ratio
# ---------------------------------------------------------------------------

def rolling_sortino(
    nav_series: pd.Series,
    window: int = 63,
    risk_free_rate: float = 0.1208,
    mar: float = 0.0,
) -> pd.Series:
    """Rolling Sortino Ratio = (R_p - R_f) / sigma_downside.

    Args:
        nav_series: Daily NAV values.
        window: Rolling window in trading days.
        risk_free_rate: Annualized risk-free rate.
        mar: Minimum acceptable return (daily). Defaults to 0.

    Returns:
        Series of rolling Sortino values.
    """
    rets = _log_returns(nav_series)
    rf_daily = _daily_rf(risk_free_rate)
    excess = rets - rf_daily

    roll_mean = excess.rolling(window).mean()

    # Downside deviation: std of returns below MAR
    downside = rets.copy()
    downside[downside > mar] = 0.0
    roll_downside_std = downside.rolling(window).apply(
        lambda x: np.sqrt(np.mean(x[x < 0] ** 2)) if (x < 0).any() else np.nan,
        raw=False,
    )

    sortino = roll_mean / roll_downside_std * math.sqrt(TRADING_DAYS)
    return sortino


# ---------------------------------------------------------------------------
# Maximum Drawdown
# ---------------------------------------------------------------------------

def maximum_drawdown(nav_series: pd.Series) -> dict:
    """Maximum Drawdown analysis.

    Args:
        nav_series: Daily NAV values.

    Returns:
        Dict with max_drawdown, start/end/recovery dates, drawdown_series,
        current_drawdown, underwater_days.
    """
    nav = nav_series.dropna()
    if nav.empty:
        return {
            "max_drawdown": 0.0,
            "max_drawdown_start": None,
            "max_drawdown_end": None,
            "max_drawdown_recovery": None,
            "drawdown_series": pd.Series(dtype=float),
            "current_drawdown": 0.0,
            "underwater_days": 0,
        }

    running_max = nav.cummax()
    drawdown = (nav - running_max) / running_max

    # Worst drawdown
    max_dd = drawdown.min()
    trough_idx = drawdown.idxmin()

    # Peak before trough
    peak_idx = nav.loc[:trough_idx].idxmax()

    # Recovery: first date after trough where NAV >= peak NAV
    peak_nav = nav.loc[peak_idx]
    post_trough = nav.loc[trough_idx:]
    recovered = post_trough[post_trough >= peak_nav]
    recovery_idx = recovered.index[0] if not recovered.empty else None

    # Current drawdown
    current_dd = drawdown.iloc[-1] if len(drawdown) > 0 else 0.0

    # Days underwater (since last all-time-high)
    last_peak_idx = nav.loc[nav == running_max.iloc[-1]].index[-1]
    underwater = (nav.index[-1] - last_peak_idx).days if hasattr(nav.index[-1], 'days') else 0
    # Handle datetime index
    try:
        underwater = (nav.index[-1] - last_peak_idx).days
    except (TypeError, AttributeError):
        underwater = len(nav) - nav.index.get_loc(last_peak_idx) - 1

    return {
        "max_drawdown": float(max_dd),
        "max_drawdown_start": peak_idx,
        "max_drawdown_end": trough_idx,
        "max_drawdown_recovery": recovery_idx,
        "drawdown_series": drawdown,
        "current_drawdown": float(current_dd),
        "underwater_days": int(underwater),
    }


# ---------------------------------------------------------------------------
# Rolling Beta
# ---------------------------------------------------------------------------

def rolling_beta(
    fund_nav: pd.Series,
    benchmark_nav: pd.Series,
    window: int = 252,
) -> pd.Series:
    """Rolling Beta = Cov(R_fund, R_benchmark) / Var(R_benchmark).

    Args:
        fund_nav: Daily fund NAV values.
        benchmark_nav: Daily benchmark (e.g., KSE-100) values.
        window: Rolling window in trading days.

    Returns:
        Series of rolling beta values.
    """
    fr = _log_returns(fund_nav)
    br = _log_returns(benchmark_nav)

    # Align on common dates
    aligned = pd.DataFrame({"fund": fr, "bench": br}).dropna()

    cov = aligned["fund"].rolling(window).cov(aligned["bench"])
    var = aligned["bench"].rolling(window).var(ddof=1)

    return cov / var


# ---------------------------------------------------------------------------
# Jensen's Alpha
# ---------------------------------------------------------------------------

def calc_alpha(
    fund_returns: pd.Series,
    benchmark_returns: pd.Series,
    risk_free_rate: float = 0.1208,
) -> float:
    """Jensen's Alpha = R_fund - [R_f + beta * (R_benchmark - R_f)].

    Args:
        fund_returns: Daily fund log returns.
        benchmark_returns: Daily benchmark log returns.
        risk_free_rate: Annualized risk-free rate.

    Returns:
        Annualized alpha (float).
    """
    aligned = pd.DataFrame({"fund": fund_returns, "bench": benchmark_returns}).dropna()
    if len(aligned) < 30:
        return float("nan")

    rf_daily = _daily_rf(risk_free_rate)
    excess_fund = aligned["fund"].mean() - rf_daily
    excess_bench = aligned["bench"].mean() - rf_daily

    cov = np.cov(aligned["fund"], aligned["bench"])
    beta = cov[0, 1] / cov[1, 1] if cov[1, 1] != 0 else 0.0

    alpha_daily = excess_fund - beta * excess_bench
    return float(alpha_daily * TRADING_DAYS)


def calc_alpha_from_nav(
    fund_nav: pd.Series,
    benchmark_nav: pd.Series,
    risk_free_rate: float = 0.1208,
) -> float:
    """Convenience: Jensen's Alpha from NAV series."""
    return calc_alpha(
        _log_returns(fund_nav),
        _log_returns(benchmark_nav),
        risk_free_rate,
    )


# ---------------------------------------------------------------------------
# Value at Risk
# ---------------------------------------------------------------------------

def value_at_risk(
    nav_series: pd.Series,
    confidence: float = 0.95,
    window: int = 252,
    method: str = "historical",
) -> dict:
    """Value at Risk — worst expected daily loss at confidence level.

    Args:
        nav_series: Daily NAV values.
        confidence: Confidence level (default 0.95).
        window: Lookback window.
        method: "historical", "parametric", or "cornish_fisher".

    Returns:
        Dict with var_95, var_99, cvar_95, cvar_99.
    """
    rets = _log_returns(nav_series)
    if len(rets) < 30:
        return {"var_95": None, "var_99": None, "cvar_95": None, "cvar_99": None}

    recent = rets.iloc[-window:] if len(rets) > window else rets

    if method == "parametric":
        mu = recent.mean()
        sigma = recent.std(ddof=1)
        from scipy.stats import norm
        var_95 = float(mu + norm.ppf(1 - 0.95) * sigma)
        var_99 = float(mu + norm.ppf(1 - 0.99) * sigma)
    elif method == "cornish_fisher":
        mu = recent.mean()
        sigma = recent.std(ddof=1)
        from scipy.stats import norm, skew, kurtosis as kurt_fn
        s = float(skew(recent))
        k = float(kurt_fn(recent))
        z95 = norm.ppf(1 - 0.95)
        z99 = norm.ppf(1 - 0.99)
        cf95 = z95 + (z95**2 - 1) * s / 6 + (z95**3 - 3*z95) * k / 24 - (2*z95**3 - 5*z95) * s**2 / 36
        cf99 = z99 + (z99**2 - 1) * s / 6 + (z99**3 - 3*z99) * k / 24 - (2*z99**3 - 5*z99) * s**2 / 36
        var_95 = float(mu + cf95 * sigma)
        var_99 = float(mu + cf99 * sigma)
    else:  # historical
        var_95 = float(np.percentile(recent, (1 - 0.95) * 100))
        var_99 = float(np.percentile(recent, (1 - 0.99) * 100))

    # Conditional VaR (Expected Shortfall)
    cvar_95 = float(recent[recent <= var_95].mean()) if (recent <= var_95).any() else var_95
    cvar_99 = float(recent[recent <= var_99].mean()) if (recent <= var_99).any() else var_99

    return {
        "var_95": var_95,
        "var_99": var_99,
        "cvar_95": cvar_95,
        "cvar_99": cvar_99,
    }


# ---------------------------------------------------------------------------
# Information Ratio
# ---------------------------------------------------------------------------

def information_ratio(
    fund_nav: pd.Series,
    benchmark_nav: pd.Series,
    annualize: bool = True,
) -> float:
    """IR = (R_fund - R_benchmark) / Tracking Error.

    Args:
        fund_nav: Daily fund NAV values.
        benchmark_nav: Daily benchmark NAV values.
        annualize: Annualize the ratio.

    Returns:
        Information ratio (float).
    """
    fr = _log_returns(fund_nav)
    br = _log_returns(benchmark_nav)
    aligned = pd.DataFrame({"fund": fr, "bench": br}).dropna()

    if len(aligned) < 30:
        return float("nan")

    active = aligned["fund"] - aligned["bench"]
    te = active.std(ddof=1)
    if te == 0:
        return float("nan")

    ir = active.mean() / te
    if annualize:
        ir = ir * math.sqrt(TRADING_DAYS)
    return float(ir)


# ---------------------------------------------------------------------------
# Capture Ratios
# ---------------------------------------------------------------------------

def capture_ratios(
    fund_nav: pd.Series,
    benchmark_nav: pd.Series,
) -> dict:
    """Up-Capture / Down-Capture ratios.

    Args:
        fund_nav: Daily fund NAV values.
        benchmark_nav: Daily benchmark NAV values.

    Returns:
        Dict with up_capture, down_capture, capture_ratio.
    """
    fr = _simple_returns(fund_nav)
    br = _simple_returns(benchmark_nav)
    aligned = pd.DataFrame({"fund": fr, "bench": br}).dropna()

    if len(aligned) < 30:
        return {"up_capture": None, "down_capture": None, "capture_ratio": None}

    up_days = aligned[aligned["bench"] > 0]
    down_days = aligned[aligned["bench"] < 0]

    up_cap = (up_days["fund"].mean() / up_days["bench"].mean() * 100) if len(up_days) > 0 else None
    down_cap = (down_days["fund"].mean() / down_days["bench"].mean() * 100) if len(down_days) > 0 else None

    cap_ratio = None
    if up_cap is not None and down_cap is not None and down_cap != 0:
        cap_ratio = up_cap / down_cap

    return {
        "up_capture": float(up_cap) if up_cap is not None else None,
        "down_capture": float(down_cap) if down_cap is not None else None,
        "capture_ratio": float(cap_ratio) if cap_ratio is not None else None,
    }


# ---------------------------------------------------------------------------
# Comprehensive Fund Report
# ---------------------------------------------------------------------------

def _period_return(nav: pd.Series, days: int | None = None) -> float | None:
    """Compute return over a period. None if insufficient data."""
    if days is None:
        # Since inception
        if len(nav) < 2:
            return None
        return float((nav.iloc[-1] / nav.iloc[0]) - 1)
    if len(nav) < days:
        return None
    return float((nav.iloc[-1] / nav.iloc[-days]) - 1)


def _annualized_return(total_return: float, days: int) -> float:
    """Annualize a total return given number of calendar days."""
    if days <= 0 or total_return is None:
        return float("nan")
    years = days / 365.25
    if years < 1 / 365.25:
        return float("nan")
    if total_return <= -1:
        return float("nan")
    return float((1 + total_return) ** (1 / years) - 1)


_PERIOD_DAYS = {
    "1M": 21, "3M": 63, "6M": 126,
    "1Y": 252, "3Y": 756, "5Y": 1260, "SI": None,
}


def generate_fund_analytics(
    fund_name: str,
    nav_series: pd.Series,
    benchmark_nav: pd.Series | None = None,
    risk_free_rate: float = 0.1208,
    periods: list[str] | None = None,
) -> dict:
    """Comprehensive analytics for a single fund.

    Args:
        fund_name: Fund identifier/name.
        nav_series: Daily NAV values (DatetimeIndex).
        benchmark_nav: Optional benchmark NAV (e.g., KSE-100).
        risk_free_rate: Annualized risk-free rate.
        periods: Return periods to compute.

    Returns:
        Dict with performance, risk, relative, technical, classification sections.
    """
    if periods is None:
        periods = ["1M", "3M", "6M", "1Y", "3Y", "5Y", "SI"]

    nav = nav_series.dropna().sort_index()
    if len(nav) < 2:
        return {"fund_name": fund_name, "error": "insufficient_data", "nav_count": len(nav)}

    rets = _log_returns(nav)

    # ── Performance ──
    performance = {}
    for p in periods:
        days = _PERIOD_DAYS.get(p)
        ret = _period_return(nav, days)
        ann = None
        if ret is not None and days is not None and days > 252:
            ann = _annualized_return(ret, int(days * 365.25 / 252))
        performance[p] = {"return": ret, "annualized": ann}

    # ── Risk ──
    vol_1y = float(rets.iloc[-252:].std(ddof=1) * math.sqrt(TRADING_DAYS)) if len(rets) >= 252 else None
    vol_3m = float(rets.iloc[-63:].std(ddof=1) * math.sqrt(TRADING_DAYS)) if len(rets) >= 63 else None

    sharpe_series = rolling_sharpe(nav, window=252, risk_free_rate=risk_free_rate)
    sharpe_1y = float(sharpe_series.iloc[-1]) if len(sharpe_series) > 0 and not np.isnan(sharpe_series.iloc[-1]) else None

    sortino_series = rolling_sortino(nav, window=252, risk_free_rate=risk_free_rate)
    sortino_1y = float(sortino_series.iloc[-1]) if len(sortino_series) > 0 and not np.isnan(sortino_series.iloc[-1]) else None

    dd = maximum_drawdown(nav)
    var = value_at_risk(nav, window=min(252, len(nav)))

    risk = {
        "volatility_1y_ann": vol_1y,
        "volatility_3m_ann": vol_3m,
        "sharpe_1y": sharpe_1y,
        "sortino_1y": sortino_1y,
        "max_drawdown": dd["max_drawdown"],
        "max_drawdown_start": str(dd["max_drawdown_start"]) if dd["max_drawdown_start"] else None,
        "max_drawdown_end": str(dd["max_drawdown_end"]) if dd["max_drawdown_end"] else None,
        "max_drawdown_recovery": str(dd["max_drawdown_recovery"]) if dd["max_drawdown_recovery"] else None,
        "current_drawdown": dd["current_drawdown"],
        "underwater_days": dd["underwater_days"],
        "var_95_daily": var["var_95"],
        "var_99_daily": var["var_99"],
        "cvar_95": var["cvar_95"],
        "cvar_99": var["cvar_99"],
    }

    # ── Relative (if benchmark provided) ──
    relative = {}
    if benchmark_nav is not None:
        bm = benchmark_nav.dropna().sort_index()
        if len(bm) >= 30:
            beta_series = rolling_beta(nav, bm, window=min(252, len(nav) - 1))
            beta_val = float(beta_series.iloc[-1]) if len(beta_series) > 0 and not np.isnan(beta_series.iloc[-1]) else None

            alpha_val = calc_alpha_from_nav(nav, bm, risk_free_rate)
            ir_val = information_ratio(nav, bm)
            cap = capture_ratios(nav, bm)

            fr = _log_returns(nav)
            br = _log_returns(bm)
            aligned = pd.DataFrame({"fund": fr, "bench": br}).dropna()
            te = float(aligned["fund"].sub(aligned["bench"]).std(ddof=1) * math.sqrt(TRADING_DAYS)) if len(aligned) > 1 else None

            relative = {
                "beta": beta_val,
                "alpha": alpha_val if not np.isnan(alpha_val) else None,
                "information_ratio": ir_val if not np.isnan(ir_val) else None,
                "tracking_error": te,
                "up_capture": cap["up_capture"],
                "down_capture": cap["down_capture"],
                "capture_ratio": cap["capture_ratio"],
            }

    # ── Classification ──
    classification = {}
    if vol_1y is not None:
        if vol_1y < 5:
            classification["risk_bucket"] = "conservative"
        elif vol_1y < 15:
            classification["risk_bucket"] = "moderate"
        else:
            classification["risk_bucket"] = "aggressive"

    return {
        "fund_name": fund_name,
        "nav_count": len(nav),
        "nav_start": str(nav.index[0]),
        "nav_end": str(nav.index[-1]),
        "nav_current": float(nav.iloc[-1]),
        "performance": performance,
        "risk": risk,
        "relative": relative,
        "classification": classification,
    }
