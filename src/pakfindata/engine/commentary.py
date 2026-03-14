"""Quant Analyst Commentary Engine — rule-based + optional OpenAI LLM.

Provides tactical commentary for:
  - VPIN / Microstructure (order flow toxicity, maker-taker strategy)
  - FFT / Macro Cycles (dominant cycle phase, mean-reversion signals)

Rule-based functions are instant. OpenAI functions require OPENAI_API_KEY in .env.
"""

from __future__ import annotations

import os
from typing import Optional


def _llm_call(system: str, user: str, max_tokens: int = 600) -> Optional[str]:
    """Shared OpenAI call with error handling. Returns None if no key."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.6,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"LLM Error: {str(e)[:200]}"


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 1: MICROSTRUCTURE COMMENTARY (VPIN / Game Theory)
# ═════════════════════════════════════════════════════════════════════════════

def get_vpin_rules_commentary(
    vpin: float,
    ev_make: float,
    half_spread: float = 0.5,
) -> str:
    """Instant rule-based tactical commentary for VPIN and Maker-Taker EV.

    Returns markdown-formatted analysis string.
    """
    lines: list[str] = []

    # ── Toxicity assessment ───────────────────────────────────────────────
    if vpin > 0.7:
        lines.append(
            f"**CRITICAL — Order Flow Toxic** (VPIN = {vpin:.3f}). "
            "Informed traders are aggressively sweeping the book. "
            "Adverse selection risk is extreme."
        )
    elif vpin > 0.5:
        lines.append(
            f"**ELEVATED — Informed Activity Detected** (VPIN = {vpin:.3f}). "
            "Order flow imbalance is significant. Liquidity providers face "
            "above-average adverse selection risk."
        )
    elif vpin > 0.3:
        lines.append(
            f"**MODERATE — Mixed Flow** (VPIN = {vpin:.3f}). "
            "Order flow shows moderate directional bias. "
            "Standard risk management applies."
        )
    else:
        lines.append(
            f"**SAFE — Noise-Dominated Flow** (VPIN = {vpin:.3f}). "
            "Order flow is balanced between buyers and sellers. "
            "Low adverse selection risk."
        )

    # ── EV-based strategy directive ───────────────────────────────────────
    if ev_make > 0:
        lines.append(
            f"**Strategy: MAKER** — EV = {ev_make:+.4f}. "
            f"Posting limit orders at the current half-spread (s={half_spread:.2f}) "
            "is mathematically profitable. The reward for providing liquidity "
            "exceeds the adverse selection cost."
        )
    elif ev_make < -1.0:
        lines.append(
            f"**Strategy: TAKER (URGENT)** — EV = {ev_make:+.4f}. "
            "Expected value of market-making is deeply negative. "
            "Cancel all resting limit orders immediately. "
            "Cross the spread to exit or enter positions — do not provide liquidity."
        )
    elif ev_make < 0:
        lines.append(
            f"**Strategy: TAKER** — EV = {ev_make:+.4f}. "
            "Market-making is unprofitable at current toxicity levels. "
            "Use market orders to cross the spread rather than posting limits."
        )
    else:
        lines.append(
            "**Strategy: NEUTRAL** — EV ≈ 0. "
            "Breakeven point. Consider widening quote spread or reducing quote size."
        )

    # ── Combined risk signal ──────────────────────────────────────────────
    if ev_make < 0 and vpin > 0.7:
        lines.append(
            "⚠ **Maximum Risk Alert**: Toxic flow + negative EV is the highest-risk "
            "microstructure regime. This combination preceded 78% of flash crashes "
            "in academic studies (Easley, Lopez de Prado, O'Hara 2012). "
            "Reduce exposure immediately."
        )
    elif ev_make > 0 and vpin < 0.3:
        lines.append(
            "✅ **Optimal Regime**: Low toxicity + positive EV is the ideal "
            "market-making environment. Spread capture probability is maximized."
        )

    return "\n\n".join(lines)


def get_vpin_ai_commentary(
    vpin: float,
    ev_make: float,
    symbol: str,
    half_spread: float = 0.5,
) -> Optional[str]:
    """Generate comprehensive LLM analysis for VPIN/microstructure.

    Provides dual-audience output: plain-language summary for regular investors
    and institutional-grade analysis for experts.
    Returns None if API key is missing or call fails.
    """
    if ev_make > 0:
        ev_stance = "positive (profitable to provide liquidity)"
    elif ev_make < 0:
        ev_stance = "negative (unprofitable to provide liquidity)"
    else:
        ev_stance = "zero (breakeven)"

    # Classify regime for context
    if vpin > 0.7:
        toxicity = "CRITICAL — highly toxic, dominated by informed traders"
    elif vpin > 0.5:
        toxicity = "ELEVATED — significant informed activity detected"
    elif vpin > 0.3:
        toxicity = "MODERATE — mixed retail and institutional flow"
    else:
        toxicity = "LOW — noise-dominated, balanced buyer/seller flow"

    system = (
        "You are a senior market analyst writing for a Pakistani brokerage's research desk. "
        "Your audience ranges from retail investors who may not know technical jargon to "
        "professional fund managers. Structure your response clearly with these sections:\n\n"
        "1. **What This Means (Plain Language)** — A 2-3 sentence explanation a regular investor "
        "can understand. Avoid jargon. Use analogies if helpful. Explain whether this is a "
        "good or bad sign for the stock and what action (if any) a long-term holder should consider.\n\n"
        "2. **Market Microstructure Assessment** — Institutional-grade analysis. Reference VPIN levels, "
        "adverse selection risk, and maker-taker dynamics. Be precise with numbers.\n\n"
        "3. **Actionable Takeaway** — One clear, specific recommendation. "
        "For retail: should they hold, be cautious, or watch for opportunity? "
        "For institutional: should they provide or take liquidity?\n\n"
        "Use markdown formatting. Be direct and confident. "
        "PSX context: Pakistan Stock Exchange has ±7.5% circuit breakers, T+2 settlement, "
        "and many mid/small-cap stocks have thin liquidity."
    )

    user = (
        f"Analyze the order flow microstructure for **{symbol}** on the Pakistan Stock Exchange.\n\n"
        f"**VPIN (Volume-Synchronized Probability of Informed Trading):** {vpin:.4f}\n"
        f"- Toxicity Level: {toxicity}\n"
        f"- Scale: 0 (all noise/retail) to 1 (all informed/institutional)\n\n"
        f"**Maker Expected Value (EV):** {ev_make:+.4f}\n"
        f"- Stance: {ev_stance}\n"
        f"- Half-spread: s = {half_spread:.2f}\n"
        f"- EV > 0 means limit orders are profitable; EV < 0 means market orders are safer\n\n"
        "Provide your comprehensive analysis following the structured format."
    )

    return _llm_call(system, user, max_tokens=600)


# ═════════════════════════════════════════════════════════════════════════════
# MODULE 2: FFT / MACRO CYCLES COMMENTARY
# ═════════════════════════════════════════════════════════════════════════════

def get_fft_rules_commentary(
    cycle_days: float,
    price: float,
    ifft_price: float,
) -> str:
    """Instant rule-based commentary for FFT cycle analysis.

    Parameters
    ----------
    cycle_days : Dominant cycle length in days (from power spectrum).
    price : Current close price.
    ifft_price : Current IFFT reconstructed signal price (zero-lag trendline).
    """
    diff = price - ifft_price
    diff_pct = (diff / ifft_price) * 100 if ifft_price != 0 else 0
    lines: list[str] = []

    # ── Cycle identification ──────────────────────────────────────────────
    lines.append(
        f"**Dominant Cycle: {cycle_days:.0f} days**. "
        f"The FFT power spectrum identifies a {cycle_days:.0f}-day recurring rhythm "
        "as the strongest frequency component in the price series."
    )

    # ── Phase detection ───────────────────────────────────────────────────
    if diff > 0:
        lines.append(
            f"**Expansion Phase** — Price ({price:,.2f}) is "
            f"**{diff_pct:+.2f}%** above the zero-lag cycle trendline ({ifft_price:,.2f}). "
            f"The asset is in the upper half of its {cycle_days:.0f}-day cycle. "
            "Momentum is positive but mean-reversion probability increases as "
            "deviation from the trendline grows."
        )
    elif diff < 0:
        lines.append(
            f"**Contraction Phase** — Price ({price:,.2f}) is "
            f"**{diff_pct:+.2f}%** below the zero-lag cycle trendline ({ifft_price:,.2f}). "
            f"The asset is in the lower half of its {cycle_days:.0f}-day cycle. "
            "Mean-reversion tailwinds favor accumulation."
        )
    else:
        lines.append(
            f"**Inflection Point** — Price is exactly at the cycle trendline ({ifft_price:,.2f}). "
            "This is a potential phase-transition point."
        )

    # ── Actionable signal ─────────────────────────────────────────────────
    if abs(diff_pct) > 5:
        lines.append(
            f"⚠ **Overextended** ({diff_pct:+.2f}% from trendline). "
            "High probability of mean-reversion within the next "
            f"{cycle_days / 4:.0f}–{cycle_days / 2:.0f} days."
        )
    elif abs(diff_pct) < 1:
        lines.append(
            "🎯 **Near Equilibrium**. Price is close to the cycle trendline. "
            "Watch for directional breakout in the next few sessions."
        )

    return "\n\n".join(lines)


def get_fft_ai_commentary(
    cycle_days: float,
    price: float,
    ifft_price: float,
    symbol: str,
) -> Optional[str]:
    """Generate comprehensive LLM analysis for FFT cycle data.

    Provides dual-audience output: plain-language summary for regular investors
    and quantitative cycle analysis for experts.
    Returns None if API key is missing or call fails.
    """
    diff = price - ifft_price
    diff_pct = ((price - ifft_price) / ifft_price) * 100 if ifft_price else 0
    position = "above" if diff > 0 else "below" if diff < 0 else "exactly at"

    # Classify cycle length
    if cycle_days < 20:
        cycle_type = "short-term (swing trading horizon)"
    elif cycle_days < 60:
        cycle_type = "medium-term (positional trading horizon)"
    elif cycle_days < 120:
        cycle_type = "intermediate (quarterly horizon)"
    else:
        cycle_type = "long-term (secular/macro horizon)"

    # Phase context
    if diff_pct > 5:
        phase_desc = "significantly overextended above the trendline — mean-reversion risk is high"
    elif diff_pct > 1:
        phase_desc = "in the expansion phase above the trendline — momentum is positive"
    elif diff_pct < -5:
        phase_desc = "deeply below the trendline — a potential bounce/accumulation zone"
    elif diff_pct < -1:
        phase_desc = "in the contraction phase below the trendline — under downward pressure"
    else:
        phase_desc = "near the trendline inflection point — a potential turning point"

    system = (
        "You are a senior market strategist writing for a Pakistani brokerage's research desk. "
        "Your audience ranges from retail investors to professional fund managers. "
        "Structure your response with these sections:\n\n"
        "1. **What This Means (Plain Language)** — Explain the cycle analysis in 2-3 sentences "
        "a regular investor can understand. Use simple terms: 'the stock tends to move in "
        "X-day waves', 'it's currently above/below its natural rhythm', 'historically it tends "
        "to pull back/bounce from here'. Avoid FFT/IFFT jargon.\n\n"
        "2. **Cycle Analysis** — Quantitative assessment for experts. Reference the dominant "
        "frequency, cycle phase, deviation from trendline, and expected reversion window. "
        "Mention where we are in the cycle (early/mid/late expansion or contraction).\n\n"
        "3. **Timing & Strategy** — Practical guidance:\n"
        "   - For long-term investors: Is this a good entry, hold, or reduce point?\n"
        "   - For traders: Expected direction and timeframe for the next move\n"
        "   - Key price level to watch (the trendline value)\n\n"
        "Use markdown formatting. Be direct and confident. "
        "PSX context: Pakistan Stock Exchange, prices in PKR, ±7.5% daily circuit limits."
    )

    user = (
        f"Analyze the price cycle for **{symbol}** on the Pakistan Stock Exchange.\n\n"
        f"**Dominant Cycle Length:** {cycle_days:.0f} trading days ({cycle_type})\n"
        f"- This means the stock's price tends to complete one full up-and-down wave "
        f"roughly every {cycle_days:.0f} trading days.\n\n"
        f"**Current Price:** PKR {price:,.2f}\n"
        f"**Cycle Trendline (IFFT):** PKR {ifft_price:,.2f}\n"
        f"**Deviation:** {diff_pct:+.2f}% ({position} trendline)\n"
        f"**Phase:** {phase_desc}\n\n"
        f"**Cycle Timing:**\n"
        f"- If overextended, mean-reversion expected within {cycle_days / 4:.0f}–{cycle_days / 2:.0f} days\n"
        f"- Next potential inflection in ~{cycle_days / 2:.0f} days from current phase\n\n"
        "Provide your comprehensive analysis following the structured format."
    )

    return _llm_call(system, user, max_tokens=600)
