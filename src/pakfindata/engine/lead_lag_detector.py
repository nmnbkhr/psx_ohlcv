"""
Lead-Lag Detector — finds stocks that lead/follow other stocks.

Approach:
1. Compute rolling cross-correlation at multiple lags (1-60 bars of 5s data)
2. Build a directed graph: edge A->B means A leads B
3. Edge weight = max cross-correlation, edge label = optimal lag
4. Score each relationship by consistency, strength, and recency

Two modes:
  - Backtest: uses ohlcv_5s bars from DuckDB
  - Live: falls back to backtest with latest date

Output: list of LeadLagSignal with leader, follower, lag, confidence
"""

import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("lead_lag")

PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.db.connections import analytics_con
except ImportError:
    analytics_con = None


@dataclass
class LeadLagSignal:
    leader: str
    follower: str
    lag_bars: int           # Number of 5s bars leader is ahead
    lag_minutes: float      # Converted to minutes
    correlation: float      # Cross-correlation at optimal lag
    consistency: float      # % of rolling windows where this lead-lag holds
    confidence: float       # Composite score 0-1
    direction: int          # 1 = same direction, -1 = inverse
    detected_at: str        # ISO timestamp
    evidence: str           # Human-readable description


def compute_cross_correlation(
    returns_a: np.ndarray,
    returns_b: np.ndarray,
    max_lag: int = 60,
) -> tuple[int, float]:
    """Find the lag that maximizes cross-correlation between two return series.

    Positive lag means A leads B (A's past predicts B's future).
    Returns (optimal_lag, max_correlation).
    """
    n = len(returns_a)
    if n < max_lag * 3:
        return 0, 0.0

    best_lag = 0
    best_corr = 0.0

    std_a = np.std(returns_a)
    std_b = np.std(returns_b)
    if std_a < 1e-10 or std_b < 1e-10:
        return 0, 0.0

    for lag in range(1, max_lag + 1):
        if lag >= n:
            break
        c = np.corrcoef(returns_a[:-lag], returns_b[lag:])[0, 1]
        if not np.isnan(c) and abs(c) > abs(best_corr):
            best_corr = c
            best_lag = lag

    return best_lag, best_corr


def compute_rolling_lead_lag(
    returns_a: np.ndarray,
    returns_b: np.ndarray,
    window: int = 500,
    max_lag: int = 60,
    step: int = 50,
) -> list[dict]:
    """Compute lead-lag over rolling windows to measure consistency."""
    results = []
    n = len(returns_a)

    for start in range(0, n - window, step):
        end = start + window
        ra = returns_a[start:end]
        rb = returns_b[start:end]

        lag, corr = compute_cross_correlation(ra, rb, max_lag)
        if abs(corr) > 0.05:
            results.append({"start": start, "lag": lag, "corr": corr})

    return results


def scan_lead_lag(
    symbols: list[str] | None = None,
    date: str | None = None,
    top_n: int = 30,
    min_confidence: float = 0.3,
    source: str = "backtest",
) -> list[LeadLagSignal]:
    """Scan for lead-lag relationships among symbols.

    Args:
        symbols: Symbols to scan. If None, uses top 50 by volume.
        date: Date to analyze (YYYY-MM-DD). If None, uses latest.
        top_n: Max number of lead-lag pairs to return.
        min_confidence: Minimum confidence threshold.
        source: "backtest" (ohlcv_5s) or "live" (falls back to backtest).

    Returns:
        Sorted list of LeadLagSignal (strongest first).
    """
    return _scan_backtest(symbols, date, top_n, min_confidence)


def _scan_backtest(
    symbols: list[str] | None = None,
    date: str | None = None,
    top_n: int = 30,
    min_confidence: float = 0.3,
) -> list[LeadLagSignal]:
    """Scan using historical ohlcv_5s bars."""
    con = analytics_con()

    # Get latest date
    if date is None:
        date = con.execute(
            "SELECT MAX(ts::DATE) FROM ohlcv_5s"
        ).fetchone()[0]
        if date is None:
            con.close()
            return []
        date = str(date)

    # Get symbols (top by volume if not specified)
    if symbols is None:
        sym_df = con.execute("""
            SELECT symbol, SUM(v) as vol FROM ohlcv_5s
            WHERE ts::DATE = ?
            GROUP BY symbol
            ORDER BY vol DESC
            LIMIT 50
        """, [date]).df()
        symbols = sym_df["symbol"].tolist()

    if len(symbols) < 2:
        con.close()
        return []

    # Bulk load all prices for the date
    placeholders = ",".join(f"'{s}'" for s in symbols)
    bars = con.execute(f"""
        SELECT symbol, ts, c, v FROM ohlcv_5s
        WHERE ts::DATE = ? AND symbol IN ({placeholders})
        ORDER BY ts
    """, [date]).df()
    con.close()

    if bars.empty:
        return []

    # Pivot to wide format (symbols as columns)
    pivot = bars.pivot_table(index="ts", columns="symbol", values="c", aggfunc="last")
    pivot = pivot.dropna(axis=1, thresh=int(len(pivot) * 0.7))
    available = [s for s in symbols if s in pivot.columns]

    if len(available) < 2:
        return []

    # Compute returns
    returns = pivot[available].pct_change().fillna(0)

    # Pairwise lead-lag scan
    signals = []

    for i, sym_a in enumerate(available):
        ra = returns[sym_a].values
        for sym_b in available[i + 1:]:
            rb = returns[sym_b].values

            # Test A leads B
            lag_ab, corr_ab = compute_cross_correlation(ra, rb, max_lag=60)
            # Test B leads A
            lag_ba, corr_ba = compute_cross_correlation(rb, ra, max_lag=60)

            # Pick the stronger direction
            if abs(corr_ab) >= abs(corr_ba) and abs(corr_ab) > 0.1:
                leader, follower = sym_a, sym_b
                lag, corr = lag_ab, corr_ab
            elif abs(corr_ba) > 0.1:
                leader, follower = sym_b, sym_a
                lag, corr = lag_ba, corr_ba
            else:
                continue

            # Measure consistency over rolling windows
            r_leader = returns[leader].values
            r_follower = returns[follower].values
            rolling = compute_rolling_lead_lag(r_leader, r_follower, max_lag=60)

            if not rolling:
                continue

            # Consistency: what fraction of windows show same leader?
            consistent = sum(1 for r in rolling if r["lag"] > 0 and r["corr"] * corr > 0)
            consistency = consistent / len(rolling)

            # Composite confidence
            confidence = (
                abs(corr) * 0.4 +
                consistency * 0.4 +
                min(lag / 30, 1.0) * 0.2
            )

            if confidence < min_confidence:
                continue

            direction = 1 if corr > 0 else -1

            signals.append(LeadLagSignal(
                leader=leader,
                follower=follower,
                lag_bars=lag,
                lag_minutes=round(lag * 5 / 60, 1),
                correlation=round(corr, 3),
                consistency=round(consistency, 2),
                confidence=round(confidence, 3),
                direction=direction,
                detected_at=datetime.now(PKT).isoformat(),
                evidence=f"{leader} leads {follower} by {round(lag * 5 / 60, 1)}min "
                         f"(r={corr:.2f}, {consistency:.0%} consistent)",
            ))

    # Sort by confidence, return top N
    signals.sort(key=lambda s: s.confidence, reverse=True)
    return signals[:top_n]


def build_lead_lag_graph(signals: list[LeadLagSignal]) -> dict:
    """Convert signals to a graph structure for D3 visualization.

    Returns {nodes: [{id, sector}], edges: [{source, target, ...}]}
    """
    node_set = set()
    for s in signals:
        node_set.add(s.leader)
        node_set.add(s.follower)

    # Get sector info from eod_ohlcv
    sectors: dict[str, str] = {}
    try:
        con = analytics_con()
        placeholders = ",".join(f"'{s}'" for s in node_set)
        sec_df = con.execute(f"""
            SELECT DISTINCT symbol, sector_code FROM eod_ohlcv
            WHERE symbol IN ({placeholders}) AND sector_code IS NOT NULL
        """).df()
        sectors = dict(zip(sec_df["symbol"], sec_df["sector_code"]))
        con.close()
    except Exception:
        pass

    nodes = [{"id": s, "sector": sectors.get(s, "Unknown")} for s in node_set]

    edges = [{
        "source": s.leader,
        "target": s.follower,
        "lag": s.lag_bars,
        "lag_min": s.lag_minutes,
        "corr": s.correlation,
        "consistency": s.consistency,
        "confidence": s.confidence,
        "direction": s.direction,
        "evidence": s.evidence,
    } for s in signals]

    return {"nodes": nodes, "edges": edges}
