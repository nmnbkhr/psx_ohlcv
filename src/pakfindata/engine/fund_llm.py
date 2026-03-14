"""
LLM Integration — Structured Output for Fund Analytics.

Generates JSON-serializable dicts designed for LLM consumption.
All outputs are valid JSON (NaN/Inf replaced with None).

Usage:
    from pakfindata.engine.fund_llm import fund_summary_for_llm, generate_market_context
"""

from __future__ import annotations

import json
import math
from datetime import date

import numpy as np
import pandas as pd

from pakfindata.engine.fund_risk import generate_fund_analytics
from pakfindata.engine.fund_factors import (
    nav_ma_signals,
    volatility_regime,
    peer_rank,
)


def _sanitize(obj):
    """Replace NaN/Inf with None for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, 6)
    if isinstance(obj, np.floating):
        val = float(obj)
        return None if math.isnan(val) or math.isinf(val) else round(val, 6)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, (pd.Timestamp, date)):
        return str(obj)
    return obj


# ---------------------------------------------------------------------------
# Fund Summary for LLM
# ---------------------------------------------------------------------------

def fund_summary_for_llm(
    fund_name: str,
    analytics: dict,
    peer_ranking: dict | None = None,
    nav_history: pd.Series | None = None,
    metadata: dict | None = None,
) -> dict:
    """Generate structured JSON for LLM consumption.

    Args:
        fund_name: Fund display name.
        analytics: Output from generate_fund_analytics().
        peer_ranking: Output from peer_rank().
        nav_history: NAV time series for signal generation.
        metadata: Fund metadata (category, AMC, benchmark, etc.).

    Returns:
        Structured dict, JSON-serializable, with all sections.
    """
    meta = metadata or {}

    # Fund identity
    identity = {
        "name": fund_name,
        "amc": meta.get("amc_name"),
        "category": meta.get("category"),
        "fund_type": meta.get("fund_type"),
        "benchmark": meta.get("benchmark"),
        "inception_date": meta.get("launch_date"),
        "expense_ratio": meta.get("expense_ratio"),
        "is_shariah_compliant": bool(meta.get("is_shariah", 0)),
    }

    # Performance
    perf = analytics.get("performance", {})
    returns = {}
    for period, data in perf.items():
        if isinstance(data, dict):
            ret = data.get("return")
            if ret is not None:
                returns[period] = round(ret * 100, 2)  # as percentage

    performance = {
        "nav_current": analytics.get("nav_current"),
        "nav_date": analytics.get("nav_end"),
        "returns_pct": returns,
    }

    # Risk
    risk_data = analytics.get("risk", {})
    risk = {
        "sharpe_1y": risk_data.get("sharpe_1y"),
        "sortino_1y": risk_data.get("sortino_1y"),
        "max_drawdown_pct": round(risk_data.get("max_drawdown", 0) * 100, 2),
        "max_drawdown_period": (
            f"{risk_data.get('max_drawdown_start')} to {risk_data.get('max_drawdown_end')}"
            if risk_data.get("max_drawdown_start") else None
        ),
        "current_drawdown_pct": round(risk_data.get("current_drawdown", 0) * 100, 2),
        "volatility_1y_ann_pct": (
            round(risk_data.get("volatility_1y_ann", 0) * 100, 2)
            if risk_data.get("volatility_1y_ann") else None
        ),
        "var_95_daily_pct": (
            round(risk_data.get("var_95_daily", 0) * 100, 4)
            if risk_data.get("var_95_daily") else None
        ),
    }

    # Relative (vs benchmark)
    rel_data = analytics.get("relative", {})
    relative = {
        "beta": rel_data.get("beta"),
        "alpha_ann": rel_data.get("alpha"),
        "information_ratio": rel_data.get("information_ratio"),
        "tracking_error": rel_data.get("tracking_error"),
        "up_capture": rel_data.get("up_capture"),
        "down_capture": rel_data.get("down_capture"),
    }

    # Peer comparison
    peer = peer_ranking or {}
    peer_section = {
        "category": meta.get("category"),
        "rank": peer.get("rank"),
        "total_peers": peer.get("total_peers"),
        "percentile": peer.get("percentile"),
        "quartile": peer.get("quartile"),
    }

    # Signals (from NAV history)
    signals = {}
    if nav_history is not None and len(nav_history) >= 50:
        ma = nav_ma_signals(nav_history, fast=20, slow=50)
        if not ma.empty:
            last_pos = int(ma["position"].iloc[-1])
            signals["ma_crossover"] = "BULLISH" if last_pos == 1 else "BEARISH"
            days_since = ma["days_since_cross"].iloc[-1]
            signals["days_since_cross"] = int(days_since) if not pd.isna(days_since) else None

        vol_reg = volatility_regime(nav_history)
        signals["volatility_regime"] = vol_reg

        # Momentum score: 0-1 based on returns rank
        rets_3m = perf.get("3M", {})
        if isinstance(rets_3m, dict) and rets_3m.get("return") is not None:
            signals["momentum_3m_pct"] = round(rets_3m["return"] * 100, 2)

        signals["drawdown_alert"] = risk_data.get("current_drawdown", 0) < -0.1

    # Narrative hints
    hints = _generate_narrative_hints(analytics, peer_ranking, signals, meta)

    result = {
        "fund_identity": identity,
        "performance": performance,
        "risk": risk,
        "relative": relative,
        "peer_comparison": peer_section,
        "signals": signals,
        "llm_narrative_hints": hints,
    }

    return _sanitize(result)


def _generate_narrative_hints(
    analytics: dict,
    peer_ranking: dict | None,
    signals: dict,
    metadata: dict,
) -> list[str]:
    """Generate natural language hints for LLM narrative."""
    hints = []

    # Peer ranking
    if peer_ranking and peer_ranking.get("quartile"):
        q = peer_ranking["quartile"]
        cat = metadata.get("category", "its category")
        if q == 1:
            hints.append(f"Top quartile performer in {cat}")
        elif q == 4:
            hints.append(f"Bottom quartile performer in {cat}")

    # Beta insight
    rel = analytics.get("relative", {})
    beta = rel.get("beta")
    if beta is not None:
        if beta < 0.7:
            hints.append("Low beta indicates defensive positioning")
        elif beta > 1.3:
            hints.append("High beta amplifies market movements")

    # Drawdown
    risk = analytics.get("risk", {})
    cd = risk.get("current_drawdown", 0)
    if cd and cd < -0.05:
        hints.append(f"Currently {abs(cd)*100:.1f}% below all-time high")

    # MA crossover
    if signals.get("ma_crossover") == "BULLISH":
        days = signals.get("days_since_cross")
        if days and days < 30:
            hints.append(f"Recent golden cross on 20/50 day MA ({days} days ago)")
    elif signals.get("ma_crossover") == "BEARISH":
        days = signals.get("days_since_cross")
        if days and days < 30:
            hints.append(f"Recent death cross on 20/50 day MA ({days} days ago)")

    # Volatility regime
    vol_reg = signals.get("volatility_regime")
    if vol_reg in ("HIGH", "EXTREME"):
        hints.append(f"Volatility regime: {vol_reg} — elevated risk environment")

    # Expense ratio vs typical
    er = metadata.get("expense_ratio")
    if er is not None and er > 0.025:
        hints.append(f"Expense ratio ({er*100:.2f}%) is above typical range")

    return hints


# ---------------------------------------------------------------------------
# Market Context Generator
# ---------------------------------------------------------------------------

def generate_market_context(
    con,
    top_n: int = 10,
    category: str | None = None,
) -> dict:
    """Generate market-wide context for LLM commentary.

    Args:
        con: SQLite connection.
        top_n: Number of top/bottom performers.
        category: Optional category filter.

    Returns:
        Structured dict with market summary, performers, categories.
    """
    try:
        # Fund counts
        total_q = "SELECT COUNT(*) FROM mutual_funds WHERE is_active = 1"
        total = con.execute(total_q).fetchone()[0]

        # Performance data
        perf_q = """
            SELECT fund_name, category, nav, return_ytd, return_mtd,
                   return_30d, return_90d, return_365d
            FROM fund_performance_latest
            ORDER BY return_30d DESC
        """
        df = pd.read_sql_query(perf_q, con)
        if category:
            df = df[df["category"] == category]

        # Category summary
        cat_summary = []
        if not df.empty:
            for cat, grp in df.groupby("category"):
                cat_summary.append({
                    "category": cat,
                    "count": len(grp),
                    "avg_return_1m": round(float(grp["return_30d"].mean()), 2) if grp["return_30d"].notna().any() else None,
                    "avg_return_ytd": round(float(grp["return_ytd"].mean()), 2) if grp["return_ytd"].notna().any() else None,
                })
            cat_summary.sort(key=lambda x: x.get("avg_return_1m") or 0, reverse=True)

        # Top/bottom performers
        top_perfs = []
        bottom_perfs = []
        if not df.empty and "return_30d" in df.columns:
            valid = df.dropna(subset=["return_30d"])
            for _, r in valid.head(top_n).iterrows():
                top_perfs.append({
                    "fund_name": r["fund_name"],
                    "category": r["category"],
                    "return_1m": round(float(r["return_30d"]), 2),
                })
            for _, r in valid.tail(top_n).iterrows():
                bottom_perfs.append({
                    "fund_name": r["fund_name"],
                    "category": r["category"],
                    "return_1m": round(float(r["return_30d"]), 2),
                })

        result = {
            "date": str(date.today()),
            "market_summary": {
                "total_active_funds": total,
                "avg_return_1m": round(float(df["return_30d"].mean()), 2) if df["return_30d"].notna().any() else None,
                "best_category_1m": cat_summary[0]["category"] if cat_summary else None,
                "worst_category_1m": cat_summary[-1]["category"] if cat_summary else None,
            },
            "top_performers": top_perfs,
            "worst_performers": bottom_perfs,
            "category_rankings": cat_summary,
        }
        return _sanitize(result)
    except Exception as e:
        return {"error": str(e), "date": str(date.today())}


# ---------------------------------------------------------------------------
# Trading Signal Generator
# ---------------------------------------------------------------------------

def generate_fund_signals(
    fund_analytics_list: list[dict],
    strategy: str = "momentum",
) -> list[dict]:
    """Generate actionable signals for LLM formatting.

    Args:
        fund_analytics_list: List of generate_fund_analytics() outputs.
        strategy: "momentum", "value", "contrarian", or "risk_parity".

    Returns:
        List of signal dicts with fund_name, signal, confidence, reasoning.
    """
    signals = []

    for fa in fund_analytics_list:
        fund_name = fa.get("fund_name", "Unknown")
        risk = fa.get("risk", {})
        perf = fa.get("performance", {})

        ret_3m = perf.get("3M", {}).get("return") if isinstance(perf.get("3M"), dict) else None
        sharpe = risk.get("sharpe_1y")
        dd = risk.get("current_drawdown", 0)
        vol = risk.get("volatility_1y_ann")

        if strategy == "momentum":
            signal, confidence, reasons = _momentum_signal(ret_3m, sharpe, dd, vol)
        elif strategy == "contrarian":
            signal, confidence, reasons = _contrarian_signal(ret_3m, sharpe, dd, vol)
        elif strategy == "risk_parity":
            signal, confidence, reasons = _risk_parity_signal(ret_3m, sharpe, dd, vol)
        else:
            signal, confidence, reasons = "HOLD", 0.5, ["Default hold signal"]

        signals.append({
            "fund_name": fund_name,
            "signal": signal,
            "confidence": round(confidence, 2),
            "reasoning": reasons,
        })

    return _sanitize(signals)


def _momentum_signal(ret_3m, sharpe, dd, vol):
    """Momentum strategy scoring."""
    score = 0.5
    reasons = []

    if ret_3m is not None:
        if ret_3m > 0.05:
            score += 0.2
            reasons.append(f"Strong 3M return ({ret_3m*100:.1f}%)")
        elif ret_3m < -0.03:
            score -= 0.2
            reasons.append(f"Weak 3M return ({ret_3m*100:.1f}%)")

    if sharpe is not None:
        if sharpe > 1.0:
            score += 0.15
            reasons.append(f"High Sharpe ratio ({sharpe:.2f})")
        elif sharpe < 0:
            score -= 0.15
            reasons.append(f"Negative Sharpe ({sharpe:.2f})")

    if dd is not None and dd < -0.1:
        score -= 0.1
        reasons.append(f"In drawdown ({dd*100:.1f}%)")

    score = max(0, min(1, score))

    if score >= 0.7:
        return "BUY", score, reasons
    elif score <= 0.3:
        return "SELL", 1 - score, reasons
    else:
        return "HOLD", 0.5, reasons or ["No strong signal"]


def _contrarian_signal(ret_3m, sharpe, dd, vol):
    """Contrarian strategy — buy fear, sell greed."""
    score = 0.5
    reasons = []

    if dd is not None and dd < -0.15:
        score += 0.25
        reasons.append(f"Deep drawdown presents opportunity ({dd*100:.1f}%)")
    if ret_3m is not None and ret_3m > 0.15:
        score -= 0.2
        reasons.append(f"Overheated momentum ({ret_3m*100:.1f}%)")

    score = max(0, min(1, score))
    if score >= 0.7:
        return "BUY", score, reasons
    elif score <= 0.3:
        return "SELL", 1 - score, reasons
    return "HOLD", 0.5, reasons or ["No contrarian signal"]


def _risk_parity_signal(ret_3m, sharpe, dd, vol):
    """Risk parity — favor low-vol, risk-adjusted returns."""
    score = 0.5
    reasons = []

    if vol is not None:
        if vol < 0.10:
            score += 0.2
            reasons.append(f"Low volatility ({vol*100:.1f}%)")
        elif vol > 0.25:
            score -= 0.2
            reasons.append(f"High volatility ({vol*100:.1f}%)")

    if sharpe is not None and sharpe > 0.8:
        score += 0.15
        reasons.append(f"Good risk-adjusted return (Sharpe {sharpe:.2f})")

    score = max(0, min(1, score))
    if score >= 0.7:
        return "OVERWEIGHT", score, reasons
    elif score <= 0.3:
        return "UNDERWEIGHT", 1 - score, reasons
    return "NEUTRAL", 0.5, reasons or ["Balanced risk profile"]
