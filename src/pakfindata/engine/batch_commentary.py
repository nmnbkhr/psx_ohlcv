"""Batch-level market narrative templates.

ADDITIVE to engine/commentary.py — imports the existing LLM commentary engine
and provides additional context templates for batch scan results.

The existing commentary engine handles:
  - Single-symbol analysis narratives
  - Rules-based commentary
  - AI-powered commentary via LLM

This module adds:
  - Market-wide scan summaries
  - Sector rotation narratives
  - Regime distribution commentary
  - Top opportunities briefing
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

# IMPORT the existing commentary engine — DO NOT DUPLICATE
try:
    from pakfindata.engine.commentary import _llm_call

    HAS_LLM = True
except ImportError:
    HAS_LLM = False


@dataclass
class BatchNarrative:
    """Structured context for batch-level commentary."""

    # Market overview
    total_scanned: int = 0
    total_with_data: int = 0
    avg_score: float = 0.0
    median_score: float = 0.0
    market_bias: str = "NEUTRAL"  # BULLISH / BEARISH / NEUTRAL

    # Regime breakdown
    trending_count: int = 0
    trending_pct: float = 0.0
    mean_rev_count: int = 0
    random_walk_count: int = 0

    # Momentum
    bullish_count: int = 0
    bearish_count: int = 0

    # SMA
    above_sma_count: int = 0
    avg_volatility: float = 0.0

    # Top opportunities
    top_symbols: list[str] = field(default_factory=list)
    top_scores: list[int] = field(default_factory=list)
    top_sectors: list[str] = field(default_factory=list)

    # Sector rotation
    sector_rankings: dict[str, float] = field(default_factory=dict)
    strongest_sector: str = ""
    weakest_sector: str = ""

    # Warnings
    circuit_breaker_symbols: list[str] = field(default_factory=list)
    high_vol_symbols: list[str] = field(default_factory=list)


def build_batch_narrative(results: list) -> BatchNarrative:
    """Build structured narrative context from batch scan results.

    Args:
        results: List of BatchScanResult from batch_score_symbols()
    """
    narrative = BatchNarrative()

    if not results:
        return narrative

    valid = [r for r in results if r.signal_score > 0]
    narrative.total_scanned = len(results)
    narrative.total_with_data = len(valid)

    if not valid:
        return narrative

    scores = [r.signal_score for r in valid]
    narrative.avg_score = round(float(np.mean(scores)), 1)
    narrative.median_score = round(float(np.median(scores)), 1)

    # Market bias
    if narrative.avg_score > 55:
        narrative.market_bias = "BULLISH"
    elif narrative.avg_score < 40:
        narrative.market_bias = "BEARISH"
    else:
        narrative.market_bias = "NEUTRAL"

    # Regime breakdown
    narrative.trending_count = sum(1 for r in valid if r.regime == "TRENDING")
    narrative.mean_rev_count = sum(1 for r in valid if r.regime == "MEAN_REVERTING")
    narrative.random_walk_count = sum(1 for r in valid if r.regime == "RANDOM_WALK")
    narrative.trending_pct = round(
        narrative.trending_count / len(valid) * 100, 1
    )

    # Momentum
    narrative.bullish_count = sum(
        1 for r in valid if getattr(r, "momentum", "") in ("BULLISH", "BULLISH_CROSS")
    )
    narrative.bearish_count = sum(
        1 for r in valid if getattr(r, "momentum", "") in ("BEARISH", "BEARISH_CROSS")
    )

    # SMA
    narrative.above_sma_count = sum(1 for r in valid if r.sma_distance_pct > 0)
    narrative.avg_volatility = round(
        float(np.mean([r.ann_volatility for r in valid])), 1
    )

    # Top opportunities (already sorted by score desc)
    top_n = valid[:10]
    narrative.top_symbols = [r.symbol for r in top_n]
    narrative.top_scores = [r.signal_score for r in top_n]
    narrative.top_sectors = [getattr(r, "sector_name", "") for r in top_n]

    # Sector rankings
    sector_scores: dict[str, list[int]] = defaultdict(list)
    for r in valid:
        key = getattr(r, "sector_name", "") or "Unknown"
        sector_scores[key].append(r.signal_score)

    narrative.sector_rankings = {
        sector: round(float(np.mean(sc)), 1)
        for sector, sc in sector_scores.items()
        if len(sc) >= 3
    }

    if narrative.sector_rankings:
        narrative.strongest_sector = max(
            narrative.sector_rankings, key=narrative.sector_rankings.get  # type: ignore[arg-type]
        )
        narrative.weakest_sector = min(
            narrative.sector_rankings, key=narrative.sector_rankings.get  # type: ignore[arg-type]
        )

    return narrative


def generate_batch_summary_text(narrative: BatchNarrative) -> str:
    """Rules-based template market summary from batch results."""
    if narrative.total_scanned == 0:
        return "No symbols were scanned."

    lines = []

    lines.append(
        f"**Market Scan Summary** — {narrative.total_with_data} of "
        f"{narrative.total_scanned} symbols analyzed"
    )

    # Market bias
    lines.append(
        f"**Market Bias: {narrative.market_bias}** — "
        f"Average score: {narrative.avg_score}/100 "
        f"(Median: {narrative.median_score})"
    )

    # Regime breakdown
    lines.append(
        f"**Regime:** {narrative.trending_pct:.0f}% trending, "
        f"{narrative.mean_rev_count} mean-reverting, "
        f"{narrative.random_walk_count} random walk. "
        f"{narrative.bullish_count} bullish momentum, "
        f"{narrative.above_sma_count} above 200-SMA."
    )

    # Top opportunities
    if narrative.top_symbols:
        top_str = ", ".join(
            f"{s} ({sc})"
            for s, sc in zip(narrative.top_symbols[:5], narrative.top_scores[:5])
        )
        lines.append(f"**Top Signals:** {top_str}")

    # Sector rotation
    if narrative.strongest_sector and narrative.weakest_sector:
        lines.append(
            f"**Sectors:** Strongest = {narrative.strongest_sector} "
            f"(avg {narrative.sector_rankings.get(narrative.strongest_sector, 0):.0f}), "
            f"Weakest = {narrative.weakest_sector} "
            f"(avg {narrative.sector_rankings.get(narrative.weakest_sector, 0):.0f})"
        )

    return "\n\n".join(lines)


def _build_llm_prompt(narrative: BatchNarrative) -> tuple[str, str]:
    """Build system + user prompts for the LLM commentary engine."""
    # Top sectors for context
    top_sectors = sorted(
        narrative.sector_rankings.items(), key=lambda x: x[1], reverse=True
    )[:5]
    sector_text = ", ".join(
        f"{s} (avg {v:.0f})" for s, v in top_sectors
    )

    system = (
        "You are a senior market strategist at a Pakistani brokerage. "
        "Write a 3-4 sentence market scan summary. Be direct, data-driven, "
        "and actionable. Reference specific numbers. No bullet points — "
        "flowing prose. PSX context: Pakistan Stock Exchange, PKR currency."
    )

    top_str = ", ".join(
        f"{s}({sc})"
        for s, sc in zip(narrative.top_symbols[:5], narrative.top_scores[:5])
    )

    user = (
        f"Batch signal scan of {narrative.total_with_data} PSX symbols:\n"
        f"- Avg signal score: {narrative.avg_score:.0f}/100 ({narrative.market_bias})\n"
        f"- Regime: {narrative.trending_count} trending, "
        f"{narrative.mean_rev_count} mean-reverting, "
        f"{narrative.random_walk_count} random walk\n"
        f"- {narrative.bullish_count} bullish momentum, "
        f"{narrative.above_sma_count} above 200-SMA\n"
        f"- Avg volatility: {narrative.avg_volatility:.1f}%\n"
        f"- Top 5: {top_str}\n"
        f"- Strongest sectors: {sector_text}\n"
        "Summarize the market posture and highlight key opportunities."
    )

    return system, user


def render_batch_commentary(narrative: BatchNarrative) -> tuple[str, Optional[str]]:
    """Render batch commentary — tries AI first, falls back to rules-based.

    Returns:
        (main_text, raw_stats_text_or_none)
        If LLM succeeds: main_text = AI narrative, raw_stats = template
        If LLM fails: main_text = template, raw_stats = None
    """
    template = generate_batch_summary_text(narrative)

    if HAS_LLM:
        try:
            system, user = _build_llm_prompt(narrative)
            ai_text = _llm_call(system, user, max_tokens=300)
            if ai_text and not ai_text.startswith("LLM Error"):
                return ai_text, template
        except Exception:
            pass

    return template, None
