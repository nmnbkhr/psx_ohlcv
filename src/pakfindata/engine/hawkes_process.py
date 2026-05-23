"""
Hawkes Process for PSX Tick Event Clustering.

Models tick arrivals as a self-exciting point process where each event
increases the probability of future events, with exponential decay.

Univariate Hawkes intensity:
  lambda(t) = mu + SUM alpha * exp(-beta * (t - t_i))

  where:
    mu = baseline intensity (ticks/second in calm market)
    alpha = excitation jump per event
    beta = decay rate (1/seconds)
    t_i = timestamps of past events

Key derived metrics:
  - Branching ratio: n = alpha/beta  (fraction of events triggered by others)
    n < 1: stable (subcritical), n -> 1: near-critical (explosive bursts)
  - Half-life: ln(2)/beta (how long excitation persists in seconds)
  - Current intensity: lambda(now) vs mu
  - Burst detection: lambda(t)/mu > threshold -> burst regime

PSX Calibration:
  - Typical mu: 0.1-0.5 ticks/sec for liquid stocks (OGDC, HBL)
  - Typical alpha: 0.3-0.8 (moderate self-excitation)
  - Typical beta: 0.5-2.0 (half-life 30-120 seconds for PSX)
  - PSX half-life is MUCH longer than NYSE (no HFT) -> bursts are tradeable
  - Trading hours: 09:30-15:30 (21,600 seconds)
  - Circuit breakers: +/-7.5%

References:
  - Hawkes (1971). "Spectra of some self-exciting and mutually exciting point processes."
  - Bacry, Mastromatteo & Muzy (2015). "Hawkes processes in finance."
  - Filimonov & Sornette (2012). "Quantifying reflexivity in financial markets."
"""

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from scipy.optimize import minimize

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))

# Market hours in seconds from midnight (PKT)
MARKET_OPEN = 9 * 3600 + 30 * 60     # 09:30 = 34200
MARKET_CLOSE = 15 * 3600 + 30 * 60   # 15:30 = 55800


@dataclass
class HawkesParams:
    """Fitted Hawkes process parameters."""
    mu: float           # baseline intensity (events/second)
    alpha: float        # excitation magnitude
    beta: float         # decay rate (1/seconds)

    @property
    def branching_ratio(self) -> float:
        """n = alpha/beta. Fraction of events caused by other events. n<1 = stable."""
        return self.alpha / self.beta if self.beta > 0 else float('inf')

    @property
    def half_life(self) -> float:
        """Time (seconds) for excitation to decay by half."""
        return np.log(2) / self.beta if self.beta > 0 else float('inf')

    @property
    def avg_cluster_size(self) -> float:
        """Expected number of events per cluster = 1/(1-n)."""
        n = self.branching_ratio
        return 1 / (1 - n) if n < 1 else float('inf')

    @property
    def is_stable(self) -> bool:
        """Process is stable (subcritical) if branching ratio < 1."""
        return self.branching_ratio < 1


def load_tick_times(
    symbol: str,
    date: str = None,
    lookback_days: int = 1,
) -> pd.DataFrame:
    """
    Load tick timestamps from DuckDB tick_logs.

    tick_logs schema: symbol, timestamp (DOUBLE epoch), _ts (VARCHAR ISO),
    price, volume, bid, ask, etc. No date column -- derive from timestamp.

    Returns DataFrame with: ts_seconds (seconds since midnight), price, volume,
    timestamp (epoch), _ts (ISO string).
    """
    con = analytics_con()

    if date is None:
        # Get the latest date with data for this symbol
        row = con.execute("""
            SELECT MAX(CAST(_ts AS DATE))
            FROM tick_logs WHERE symbol = ?
        """, [symbol]).fetchone()
        if row is None or row[0] is None:
            con.close()
            return pd.DataFrame()
        date = str(row[0])

    # Build date range filter using _ts
    if lookback_days > 1:
        dates = con.execute("""
            SELECT DISTINCT CAST(_ts AS DATE) as d
            FROM tick_logs
            WHERE symbol = ? AND CAST(_ts AS DATE) <= ?
            ORDER BY d DESC LIMIT ?
        """, [symbol, date, lookback_days]).fetchall()
        date_list = [str(d[0]) for d in dates]
        date_filter = " OR ".join(f"CAST(_ts AS DATE) = '{d}'" for d in date_list)
        date_filter = f"({date_filter})"
    else:
        date_filter = f"CAST(_ts AS DATE) = '{date}'"

    df = con.execute(f"""
        SELECT timestamp, _ts, price, volume, bid, ask
        FROM tick_logs
        WHERE symbol = ? AND {date_filter}
        ORDER BY timestamp
    """, [symbol]).df()
    con.close()

    if df.empty:
        return df

    # Convert epoch timestamp to seconds-since-midnight (PKT)
    # timestamp is Unix epoch in seconds (DOUBLE)
    pkt_offset = 5 * 3600  # PKT = UTC+5
    df["epoch"] = df["timestamp"]
    df["seconds_of_day"] = (df["timestamp"] + pkt_offset) % 86400
    df["ts_seconds"] = df["seconds_of_day"]
    df["tick_date"] = pd.to_datetime(df["timestamp"] + pkt_offset, unit="s").dt.date

    return df


def hawkes_log_likelihood(
    params: np.ndarray,
    times: np.ndarray,
    T: float,
) -> float:
    """
    Negative log-likelihood for univariate Hawkes process.

    Uses vectorized recursive O(n) algorithm.
    """
    mu, alpha, beta = params

    if mu <= 0 or alpha <= 0 or beta <= 0:
        return 1e10
    if alpha / beta >= 1.0:
        return 1e10  # unstable

    n = len(times)
    if n == 0:
        return mu * T

    # Vectorized inter-arrival times
    dt = np.diff(times)

    # Recursive A(i) computation — vectorized via loop (unavoidable for recursion)
    # but with minimal Python overhead
    A_vals = np.zeros(n)
    for i in range(1, n):
        A_vals[i] = np.exp(-beta * dt[i - 1]) * (1.0 + A_vals[i - 1])

    # lambda(t_i) = mu + alpha * A(i)
    intensities = mu + alpha * A_vals
    if np.any(intensities <= 0):
        return 1e10

    log_lik = np.sum(np.log(intensities))

    # Integral: mu*T + (alpha/beta) * SUM (1 - exp(-beta*(T - t_i)))
    integral = mu * T + (alpha / beta) * np.sum(1.0 - np.exp(-beta * (T - times)))

    return -(log_lik - integral)


def fit_hawkes(
    times: np.ndarray,
    T: float = None,
    fast: bool = False,
) -> HawkesParams:
    """
    Fit univariate Hawkes process via MLE with multiple restarts.

    times: sorted event times in seconds
    T: observation window length
    fast: if True, use fewer restarts (for backtest/scanner speed)
    """
    if len(times) < 10:
        rate = len(times) / T if T and T > 0 else 0.1
        return HawkesParams(mu=rate, alpha=0.01, beta=1.0)

    # Subsample for speed if very large and in fast mode
    if fast and len(times) > 5000:
        step = len(times) // 4000
        times_fit = times[::step]
    else:
        times_fit = times

    t0 = times_fit[0]
    t_norm = times_fit - t0
    if T is None:
        T = t_norm[-1] - t_norm[0]
    else:
        T = T - t0

    if T <= 0:
        T = t_norm[-1] + 1.0

    n = len(t_norm)
    mu0 = n / T * 0.5

    best_nll = float('inf')
    best_params = HawkesParams(mu=mu0, alpha=0.5, beta=1.0)

    if fast:
        grid = [(mu0, 0.5, 1.0), (mu0 * 0.5, 0.3, 2.0), (mu0, 0.7, 0.5)]
    else:
        grid = [(mu0 * m, a, b) for m in [0.5, 1.0] for a in [0.3, 0.6] for b in [0.5, 1.0, 3.0]]

    for mu_init, alpha_init, beta_init in grid:
                try:
                    result = minimize(
                        hawkes_log_likelihood,
                        x0=[mu_init, alpha_init, beta_init],
                        args=(t_norm, T),
                        method="L-BFGS-B",
                        bounds=[(1e-6, None), (1e-6, None), (1e-6, None)],
                        options={"maxiter": 500, "ftol": 1e-10},
                    )

                    if result.success and result.fun < best_nll:
                        mu, alpha, beta = result.x
                        if alpha / beta < 0.99:
                            best_nll = result.fun
                            best_params = HawkesParams(mu=mu, alpha=alpha, beta=beta)
                except Exception:
                    continue

    return best_params


def compute_intensity(
    times: np.ndarray,
    params: HawkesParams,
    eval_times: np.ndarray = None,
    resolution: float = 1.0,
) -> pd.DataFrame:
    """
    Compute conditional intensity lambda(t) on a grid.

    Returns DataFrame: time, intensity, baseline, excitation, ratio, regime.
    """
    if eval_times is None:
        eval_times = np.arange(times[0], times[-1], resolution)

    mu = params.mu
    alpha = params.alpha
    beta = params.beta
    hl_cutoff = max(10 * params.half_life, 60.0)

    n_eval = len(eval_times)
    lam_arr = np.full(n_eval, mu)

    # Use searchsorted for efficient event window lookup
    sorted_times = np.sort(times)

    for i in range(n_eval):
        t = eval_times[i]
        # Binary search for events in [t - hl_cutoff, t)
        lo = np.searchsorted(sorted_times, t - hl_cutoff, side='left')
        hi = np.searchsorted(sorted_times, t, side='left')
        if hi > lo:
            recent = sorted_times[lo:hi]
            lam_arr[i] = mu + alpha * np.sum(np.exp(-beta * (t - recent)))

    ratios = lam_arr / mu if mu > 0 else np.zeros_like(lam_arr)

    regimes = np.where(ratios > 5.0, "EXPLOSIVE",
              np.where(ratios > 3.0, "BURST",
              np.where(ratios > 1.5, "ELEVATED", "CALM")))

    return pd.DataFrame({
        "time": eval_times,
        "intensity": lam_arr,
        "baseline": mu,
        "excitation": lam_arr - mu,
        "ratio": ratios,
        "regime": regimes,
    })


def detect_bursts(
    intensity_df: pd.DataFrame,
    min_ratio: float = 3.0,
    min_duration: float = 5.0,
    prices: pd.DataFrame = None,
) -> list[dict]:
    """
    Detect burst events from intensity time series.

    A burst is a contiguous period where intensity_ratio > min_ratio
    lasting at least min_duration seconds.
    """
    bursts = []
    in_burst = False
    burst_start = None
    burst_data = []

    for _, row in intensity_df.iterrows():
        if row["ratio"] >= min_ratio:
            if not in_burst:
                in_burst = True
                burst_start = row["time"]
                burst_data = []
            burst_data.append(row)
        else:
            if in_burst:
                duration = row["time"] - burst_start
                if duration >= min_duration and len(burst_data) >= 2:
                    peak_row = max(burst_data, key=lambda r: r["intensity"])

                    burst = {
                        "start_time": burst_start,
                        "end_time": row["time"],
                        "duration_seconds": duration,
                        "peak_intensity": peak_row["intensity"],
                        "peak_ratio": peak_row["ratio"],
                        "n_points": len(burst_data),
                    }

                    if prices is not None and not prices.empty:
                        mask_start = prices["ts_seconds"] <= burst_start
                        mask_peak = (prices["ts_seconds"] >= burst_start) & \
                                    (prices["ts_seconds"] <= row["time"])

                        if mask_start.any():
                            burst["price_at_start"] = prices.loc[mask_start, "price"].iloc[-1]
                        if mask_peak.any():
                            burst["price_at_peak"] = prices.loc[mask_peak, "price"].iloc[-1]
                            burst["volume_in_burst"] = int(prices.loc[mask_peak, "volume"].sum())

                        if "price_at_start" in burst and "price_at_peak" in burst:
                            burst["price_change_pct"] = (
                                (burst["price_at_peak"] - burst["price_at_start"])
                                / burst["price_at_start"] * 100
                            )

                    bursts.append(burst)

                in_burst = False
                burst_data = []

    return bursts


def analyze_symbol(
    symbol: str,
    date: str = None,
    intensity_resolution: float = 1.0,
    burst_threshold: float = 3.0,
    fast: bool = False,
) -> dict:
    """
    Full Hawkes analysis for a symbol on a given date.

    Returns dict with: params, intensity, bursts, summary, ticks.
    """
    df = load_tick_times(symbol, date, lookback_days=1)

    if df.empty or len(df) < 20:
        return {"error": f"Not enough ticks for {symbol} on {date or 'latest'}"}

    times = df["ts_seconds"].values
    T = times[-1] - times[0]

    if T <= 0:
        return {"error": f"Zero time range for {symbol}"}

    # Fit Hawkes
    params = fit_hawkes(times, T, fast=fast)

    # Compute intensity
    intensity = compute_intensity(times, params, resolution=intensity_resolution)

    # Detect bursts
    bursts = detect_bursts(intensity, min_ratio=burst_threshold, prices=df)

    # Summary
    actual_date = str(df["tick_date"].iloc[0]) if "tick_date" in df.columns else str(date or "latest")

    summary = {
        "symbol": symbol,
        "date": actual_date,
        "n_ticks": len(df),
        "duration_seconds": T,
        "ticks_per_second": len(df) / T if T > 0 else 0,
        "mu": params.mu,
        "alpha": params.alpha,
        "beta": params.beta,
        "branching_ratio": params.branching_ratio,
        "half_life_seconds": params.half_life,
        "avg_cluster_size": params.avg_cluster_size,
        "is_stable": params.is_stable,
        "n_bursts": len(bursts),
        "time_in_burst_pct": sum(
            b["duration_seconds"] for b in bursts
        ) / T * 100 if T > 0 and bursts else 0,
        "max_intensity_ratio": float(intensity["ratio"].max()),
        "regime_counts": intensity["regime"].value_counts().to_dict(),
    }

    return {
        "params": params,
        "intensity": intensity,
        "bursts": bursts,
        "summary": summary,
        "ticks": df,
    }


def scan_symbols(
    symbols: list[str] = None,
    date: str = None,
    top_n: int = 50,
) -> pd.DataFrame:
    """
    Scan multiple symbols for Hawkes parameters.
    Returns DataFrame ranked by branching_ratio.
    """
    con = analytics_con()

    if date is None:
        date = str(con.execute("SELECT MAX(CAST(_ts AS DATE)) FROM tick_logs").fetchone()[0])

    if symbols is None:
        rows = con.execute(f"""
            SELECT symbol, COUNT(*) as n
            FROM tick_logs WHERE CAST(_ts AS DATE) = '{date}'
            GROUP BY symbol ORDER BY n DESC LIMIT {top_n}
        """).fetchall()
        symbols = [r[0] for r in rows]

    con.close()

    results = []
    for sym in symbols:
        try:
            analysis = analyze_symbol(sym, date, intensity_resolution=5.0, fast=True)
            if "error" in analysis:
                continue
            results.append(analysis["summary"])
        except Exception:
            continue

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("branching_ratio", ascending=False)
    return df


def backtest_burst_signals(
    symbol: str,
    lookback_days: int = 20,
    burst_threshold: float = 3.0,
) -> dict:
    """
    Backtest: do Hawkes bursts predict subsequent volatility spikes?

    For each burst, measure realized vol before and after.
    """
    con = analytics_con()

    dates = con.execute(f"""
        SELECT DISTINCT CAST(_ts AS DATE) as d
        FROM tick_logs WHERE symbol = '{symbol}'
        ORDER BY d DESC LIMIT {lookback_days}
    """).fetchall()
    con.close()

    all_bursts = []

    for (d,) in dates:
        analysis = analyze_symbol(symbol, str(d),
                                  intensity_resolution=10.0,
                                  burst_threshold=burst_threshold,
                                  fast=True)
        if "error" in analysis:
            continue

        for burst in analysis["bursts"]:
            burst["date"] = str(d)

            ticks = analysis["ticks"]
            t_start = burst["start_time"]
            t_end = burst["end_time"]

            # 5 min = 300 seconds before and after
            pre = ticks[(ticks["ts_seconds"] >= t_start - 300) &
                        (ticks["ts_seconds"] < t_start)]
            post = ticks[(ticks["ts_seconds"] > t_end) &
                         (ticks["ts_seconds"] <= t_end + 300)]

            if len(pre) > 5:
                pre_ret = pre["price"].pct_change().dropna()
                burst["pre_vol"] = float(pre_ret.std() * np.sqrt(len(pre_ret))) if len(pre_ret) > 1 else 0
            else:
                burst["pre_vol"] = 0

            if len(post) > 5:
                post_ret = post["price"].pct_change().dropna()
                burst["post_vol"] = float(post_ret.std() * np.sqrt(len(post_ret))) if len(post_ret) > 1 else 0
            else:
                burst["post_vol"] = 0

            if burst["pre_vol"] > 0:
                burst["vol_amplification"] = burst["post_vol"] / burst["pre_vol"]
            else:
                burst["vol_amplification"] = 0

            all_bursts.append(burst)

    if not all_bursts:
        return {"error": "No bursts detected", "symbol": symbol}

    df = pd.DataFrame(all_bursts)

    metrics = {
        "symbol": symbol,
        "days_analyzed": len(dates),
        "total_bursts": len(df),
        "bursts_per_day": len(df) / len(dates) if dates else 0,
        "avg_duration_sec": float(df["duration_seconds"].mean()),
        "avg_peak_ratio": float(df["peak_ratio"].mean()),
        "avg_vol_amplification": float(df["vol_amplification"].mean()),
        "pct_vol_increase": float((df["vol_amplification"] > 1.0).mean() * 100),
        "avg_price_move_pct": float(df["price_change_pct"].abs().mean()) if "price_change_pct" in df.columns else 0,
    }

    return {"metrics": metrics, "bursts": df}
