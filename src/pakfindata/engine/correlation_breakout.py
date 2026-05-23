"""
Correlation Breakout Detector — detects sudden correlation regime changes.

Approach:
1. Compute rolling correlation matrix of top N symbols over EOD returns
2. Track the "normal" correlation structure using long-window average
3. When recent short-window correlations deviate significantly -> alert
4. Identify the cluster of symbols driving the breakout
5. Score by deviation magnitude and cluster size

Works on EOD data (daily correlations). No GPU required.
"""

import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("corr_breakout")

PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.db.connections import analytics_con
except ImportError:
    analytics_con = None


@dataclass
class CorrelationAlert:
    cluster: list[str]          # Symbols in the correlated cluster
    trigger_symbol: str         # Symbol that moved first/most
    normal_corr: float          # Average historical intra-cluster correlation
    current_corr: float         # Current intra-cluster correlation
    sigma: float                # Standard deviations above/below normal
    direction: str              # "CONVERGING" (corr spike) or "DIVERGING" (corr drop)
    sector: str                 # Primary sector of the cluster
    confidence: float           # 0-1
    detected_at: str
    evidence: str


def compute_correlation_regime(
    lookback_days: int = 60,
    window_days: int = 5,
    top_n_symbols: int = 50,
    min_sigma: float = 2.0,
) -> list[CorrelationAlert]:
    """Detect correlation breakouts using EOD data.

    Compares recent short-window correlation to long-window rolling average.
    """
    con = analytics_con()

    # Get top symbols by recent volume
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    min_days = lookback_days // 2
    sym_df = con.execute(f"""
        SELECT symbol, SUM(volume) as vol, MAX(sector_code) as sector
        FROM eod_ohlcv
        WHERE date >= '{cutoff}'
        GROUP BY symbol
        HAVING COUNT(*) >= {min_days}
        ORDER BY vol DESC
        LIMIT {top_n_symbols}
    """).df()
    symbols = sym_df["symbol"].tolist()
    sectors = dict(zip(sym_df["symbol"], sym_df["sector"].fillna("Unknown")))

    if len(symbols) < 5:
        con.close()
        return []

    # Load prices
    placeholders = ",".join(f"'{s}'" for s in symbols)
    prices = con.execute(f"""
        SELECT symbol, date, close FROM eod_ohlcv
        WHERE symbol IN ({placeholders})
        ORDER BY date
    """).df()
    con.close()

    # Pivot and compute returns
    pivot = prices.pivot(index="date", columns="symbol", values="close")
    pivot = pivot.dropna(axis=1, thresh=int(len(pivot) * 0.7))
    returns = pivot.pct_change().dropna()

    available = [s for s in symbols if s in returns.columns]
    if len(available) < 5:
        return []

    returns = returns[available]

    if len(returns) < lookback_days:
        return []

    # Historical correlation (long window)
    hist_corr = returns.iloc[-lookback_days:].corr()

    # Recent correlation (short window)
    recent_corr = returns.iloc[-window_days:].corr()

    # Correlation change matrix
    corr_change = recent_corr - hist_corr

    # Standard deviation of historical rolling correlations
    corr_std = pd.DataFrame(0.0, index=available, columns=available)
    n_windows = 0
    for start in range(0, len(returns) - window_days - 1, window_days):
        end = start + window_days
        window_corr = returns.iloc[start:end].corr()
        corr_std += (window_corr - hist_corr) ** 2
        n_windows += 1
    n_windows = max(1, n_windows)
    corr_std = np.sqrt(corr_std / n_windows)
    corr_std = corr_std.replace(0, 0.01)

    # Z-score of correlation change
    z_scores = corr_change / corr_std

    # Find clusters of symbols with significant correlation changes
    alerts = []
    processed: set[str] = set()

    for sym in available:
        if sym in processed:
            continue

        # Find symbols with significant correlation change to this symbol
        sig_peers = z_scores[sym][z_scores[sym].abs() > min_sigma].index.tolist()
        sig_peers = [s for s in sig_peers if s != sym and s not in processed]

        if len(sig_peers) < 2:
            continue

        cluster = [sym] + sig_peers[:7]
        processed.update(cluster)

        # Cluster metrics
        cluster_pairs = [(i, j) for i in cluster for j in cluster if i < j]
        if not cluster_pairs:
            continue

        normal = np.mean([hist_corr.loc[i, j] for i, j in cluster_pairs])
        current = np.mean([recent_corr.loc[i, j] for i, j in cluster_pairs])
        sigma = np.mean([abs(z_scores.loc[i, j]) for i, j in cluster_pairs])

        direction = "CONVERGING" if current > normal else "DIVERGING"

        # Find trigger (most volatile in recent window)
        recent_vol = returns[cluster].iloc[-window_days:].std()
        trigger = recent_vol.idxmax()

        # Sector
        cluster_sectors = [sectors.get(s, "Unknown") for s in cluster]
        primary_sector = max(set(cluster_sectors), key=cluster_sectors.count)

        confidence = min(1.0, sigma / 5.0) * min(1.0, len(cluster) / 8.0)

        alerts.append(CorrelationAlert(
            cluster=cluster,
            trigger_symbol=trigger,
            normal_corr=round(normal, 3),
            current_corr=round(current, 3),
            sigma=round(sigma, 1),
            direction=direction,
            sector=primary_sector,
            confidence=round(confidence, 3),
            detected_at=datetime.now(PKT).isoformat(),
            evidence=f"{direction}: {len(cluster)} symbols in {primary_sector}, "
                     f"corr {normal:.2f}->{current:.2f} ({sigma:.1f}s), "
                     f"trigger: {trigger}",
        ))

    alerts.sort(key=lambda a: a.sigma, reverse=True)
    return alerts
