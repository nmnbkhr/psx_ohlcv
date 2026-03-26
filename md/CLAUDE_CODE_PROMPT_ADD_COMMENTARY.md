# Claude Code Prompt: Add AI Commentary to Signal Analysis Page

## Context

The Signal Analysis page (`ui/page_views/signal_dashboard.py`) has two modes working:
- **Single Symbol** — 3-layer analysis with score gauge, charts, metrics
- **Batch Scanner** — scans 207+ symbols, sector heatmap, regime donut, top 10 table

**Problem:** Neither mode has AI commentary. Every other analysis page in the app (Microstructure/VPIN, Tick Analytics, etc.) has an expandable AI commentary section that calls `engine/commentary.py`. The Signal Analysis page is missing this entirely.

**Goal:** Add the same AI commentary pattern used in other pages to BOTH modes of the Signal Analysis page.

---

## Rules

1. **READ `engine/commentary.py` FIRST** — understand the existing API (function signatures, parameters, return types)
2. **READ another page that uses commentary** — e.g., `ui/page_views/microstructure.py` or `ui/page_views/tick_analytics.py` — to see the exact pattern for calling the commentary engine and rendering the expander
3. **MODIFY only `ui/page_views/signal_dashboard.py`** — add commentary integration following the SAME pattern as other pages
4. **DO NOT modify `engine/commentary.py`** — import and call it, don't change it
5. **Match the existing UI pattern exactly** — same expander style, same loading spinner, same error handling as other pages

---

## Step 1: Discover the Existing Pattern

Run these commands to understand how commentary works in the codebase:

```bash
# Find the commentary engine API
cat engine/commentary.py | head -100
grep -n "def " engine/commentary.py

# Find how other pages call it
grep -rn "commentary" ui/page_views/microstructure.py
grep -rn "commentary" ui/page_views/tick_analytics.py
grep -rn "generate_commentary\|ai_commentary\|commentary" ui/page_views/*.py

# Find the import pattern
grep -n "from engine" ui/page_views/microstructure.py | head -10
grep -n "from engine" ui/page_views/tick_analytics.py | head -10

# Find the expander UI pattern
grep -n "expander\|Commentary\|AI\|LLM\|🤖\|💡" ui/page_views/microstructure.py
grep -n "expander\|Commentary\|AI\|LLM\|🤖\|💡" ui/page_views/tick_analytics.py
```

## Step 2: Add Commentary to Single Symbol Mode

After the 3-layer analysis renders (after the methodology expander at the bottom), add an AI commentary section. Follow the EXACT same pattern found in Step 1.

**The commentary context for single symbol should include:**

```python
signal_context = {
    "analysis_type": "signal_analysis",
    "symbol": symbol,
    "sector": macro_result.sector,
    
    # Score
    "signal_score": report.signal_score,
    "interpretation": report.interpretation,
    "macro_score": macro_result.score,
    "intraday_score": intraday_result.score,
    "execution_score": execution_result.score,
    
    # Layer 1 - Macro
    "hurst_exponent": macro_result.hurst_exponent,
    "regime": macro_result.regime,
    "ann_volatility": macro_result.ann_volatility,
    "sma_200": macro_result.sma_200,
    "sma_distance_pct": macro_result.sma_distance_pct,
    "circuit_breakers": macro_result.circuit_breaker_dates,
    "momentum_signal": getattr(macro_result, 'momentum_signal', 'N/A'),
    
    # Layer 2 - Intraday (if available)
    "vwap_distance_std": intraday_result.vwap_distance_std,
    "poc_price": intraday_result.poc_price,
    "er_spike": intraday_result.er_spike_active,
    
    # Layer 3 - Execution (if available)
    "has_tick_data": execution_result.has_tick_data,
    "buy_sell_ratio": f"{execution_result.buy_pct:.1f}/{execution_result.sell_pct:.1f}",
    "cvd_slope": execution_result.cvd_slope,
    "recent_ofi": execution_result.recent_ofi,
    "vpin_value": execution_result.vpin_value,
    "block_count": execution_result.block_count,
}
```

Then call the commentary engine using whatever pattern the other pages use. It's probably something like ONE of these:

```python
# Pattern A (if commentary.py takes a dict):
from engine.commentary import generate_commentary
commentary = generate_commentary(signal_context)

# Pattern B (if commentary.py takes symbol + specific params):
from engine.commentary import generate_commentary
commentary = generate_commentary(symbol=symbol, metrics=signal_context)

# Pattern C (if there's a class):
from engine.commentary import CommentaryEngine
engine = CommentaryEngine()
commentary = engine.generate(context=signal_context)
```

**Use whichever pattern matches what you found in Step 1.**

**Render it in the same expander style as other pages:**

```python
# Match the EXACT UI pattern from microstructure.py or tick_analytics.py
# It's probably something like:

with st.expander("🤖 AI Analysis", expanded=False):
    if st.button("Generate Commentary", key="single_commentary"):
        with st.spinner("Generating analysis..."):
            try:
                commentary = generate_commentary(signal_context)  # adapt to actual API
                st.markdown(commentary)
            except Exception as e:
                st.error(f"Commentary generation failed: {e}")
```

Or if other pages auto-generate without a button:

```python
with st.expander("🤖 AI Analysis", expanded=False):
    with st.spinner("Generating analysis..."):
        try:
            commentary = generate_commentary(signal_context)
            st.markdown(commentary)
        except Exception as e:
            st.warning("AI commentary unavailable. Check LLM configuration.")
```

**Copy the EXACT pattern from the other pages. Don't invent a new one.**

---

## Step 3: Add Commentary to Batch Scanner Mode

After the batch scan summary stats and BEFORE the sector heatmap, add an AI commentary section for the batch results.

**The commentary context for batch mode should include:**

```python
batch_context = {
    "analysis_type": "batch_market_scan",
    "total_scanned": len(results),
    "avg_score": round(np.mean([r.signal_score for r in results if r.signal_score > 0]), 1),
    "market_bias": "BULLISH" if avg_score > 55 else "BEARISH" if avg_score < 40 else "NEUTRAL",
    
    # Regime breakdown
    "trending_count": sum(1 for r in results if r.regime == 'TRENDING'),
    "trending_pct": round(sum(1 for r in results if r.regime == 'TRENDING') / len(results) * 100, 1),
    "mean_rev_count": sum(1 for r in results if r.regime == 'MEAN_REVERTING'),
    
    # Top opportunities
    "top_5_symbols": [(r.symbol, r.signal_score, r.sector) for r in results[:5]],
    
    # Sector rotation
    "above_sma_count": sum(1 for r in results if r.sma_distance_pct > 0),
    "below_sma_count": sum(1 for r in results if r.sma_distance_pct <= 0),
    "avg_volatility": round(np.mean([r.ann_volatility for r in results if r.ann_volatility > 0]), 1),
    
    # Strongest/weakest sectors (compute from results)
    "sector_rankings": sector_avg_scores,  # dict of {sector: avg_score}
    
    "prompt_hint": (
        "Generate a concise PSX market commentary based on this batch signal scan. "
        "Cover: (1) overall market direction and conviction, "
        "(2) which sectors show relative strength, "
        "(3) top individual stock opportunities and why they score highest, "
        "(4) what the regime distribution (trending vs mean-reverting) implies for strategy. "
        "Keep it under 250 words. Professional financial analyst tone. "
        "Reference specific symbols and scores."
    )
}
```

**Render using the same expander pattern:**

```python
# After the summary stats row, before sector heatmap:
with st.expander("🤖 AI Market Commentary", expanded=True):  # expanded=True for batch since it's the key insight
    with st.spinner("Analyzing market signals..."):
        try:
            commentary = generate_commentary(batch_context)  # adapt to actual API
            st.markdown(commentary)
        except Exception as e:
            # Fallback to rules-based summary if LLM fails
            st.markdown(generate_rules_based_summary(results))
```

**Rules-based fallback** (in case LLM is unavailable or fails):

```python
def generate_rules_based_summary(results):
    """Fallback when AI commentary is unavailable."""
    valid = [r for r in results if r.signal_score > 0]
    if not valid:
        return "No symbols with sufficient data were found."
    
    avg = np.mean([r.signal_score for r in valid])
    trending = sum(1 for r in valid if r.regime == 'TRENDING')
    top5 = valid[:5]
    
    bias_word = "bullish" if avg > 55 else "bearish" if avg < 40 else "neutral"
    
    lines = [
        f"**Market Overview:** {len(valid)} symbols scanned with an average signal score of "
        f"{avg:.0f}/100, indicating a **{bias_word}** market posture.",
        "",
        f"**Regime:** {trending} of {len(valid)} stocks ({trending/len(valid)*100:.0f}%) are in "
        f"trending regimes, suggesting directional strategies may outperform mean-reversion approaches.",
        "",
        f"**Top Signals:** {', '.join(f'{r.symbol} ({r.signal_score})' for r in top5)} — "
        f"concentrated in {', '.join(set(r.sector for r in top5 if r.sector))}.",
    ]
    
    return "\n".join(lines)
```

---

## Step 4: Replace the Existing Hardcoded Paragraph

The current batch scanner shows a hardcoded template paragraph (visible in the screenshot). Find it and replace it with the AI commentary expander. It's probably a block like:

```python
# FIND something like this:
st.write(f"The current analysis of {len(results)} PSX symbols reveals...")
# or
st.markdown(f"The current analysis of {total} PSX symbols reveals a {bias} market posture...")
```

**Replace it with the expander from Step 3.** The AI commentary is better than the template, and the rules-based fallback covers the case where the LLM is unavailable.

---

## Verification

```bash
# 1. Check commentary engine is importable
python -c "from engine.commentary import generate_commentary; print('OK')"

# 2. Check the signal dashboard still loads
streamlit run src/psx_ohlcv/ui/app.py
# → Signal Analysis → Single Symbol → Select HUBC → Run Analysis
# → Scroll to bottom → Should see "🤖 AI Analysis" expander
# → Click it → Should generate commentary

# → Signal Analysis → Batch Scanner → Run Analysis  
# → Should see "🤖 AI Market Commentary" expander (expanded by default)
# → Commentary should reference specific symbols, scores, sectors

# 3. Verify fallback works (disconnect LLM / set wrong config)
# → Commentary expander should still show rules-based text without crashing
```

---

## Summary

| Change | Where |
|--------|-------|
| Add AI commentary expander to single symbol mode | `signal_dashboard.py` |
| Add AI market commentary expander to batch mode | `signal_dashboard.py` |
| Replace hardcoded template paragraph with AI/fallback | `signal_dashboard.py` |
| Rules-based fallback function | `signal_dashboard.py` |

**Files modified: 1** (`signal_dashboard.py`) | **Files created: 0** | **commentary.py: UNTOUCHED**
