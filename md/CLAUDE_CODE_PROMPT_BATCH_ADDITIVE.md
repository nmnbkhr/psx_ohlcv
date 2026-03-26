# Claude Code Prompt: Batch Scanner (Additive) + QA Fixes

## Context

The Signal Dashboard is live with 3-layer analysis, market/symbol filtering, and LLM AI commentary already working. This prompt adds batch scanning and fixes two QA issues found in review.

**CRITICAL: The existing LLM AI commentary engine in `engine/commentary.py` must NOT be modified or overwritten.** The batch scanner's market narrative is a NEW template that CALLS the existing commentary engine — it's additive, not a replacement.

**Read these files FIRST:**
```
engine/signal_score.py              — Current scoring (modify)
engine/macro_regime.py              — Layer 1 macro (modify slightly for momentum)
engine/commentary.py                — EXISTING LLM AI commentary (READ ONLY — do NOT modify)
engine/microstructure.py            — EXISTING VPIN engine (READ ONLY)
ui/page_views/signal_dashboard.py   — Current page (modify — add batch mode tab)
```

---

## Rules

1. **DO NOT modify `engine/commentary.py`** — the existing LLM AI commentary engine stays exactly as-is
2. **DO NOT modify `engine/microstructure.py`** — the VPIN engine stays as-is
3. **MODIFY `engine/signal_score.py`** — add BatchScanResult, batch_score_symbols, scoring enhancements
4. **MODIFY `engine/macro_regime.py`** — add momentum crossover field + volume confirmation
5. **MODIFY `ui/page_views/signal_dashboard.py`** — add batch scanner mode + fix QA issues
6. **CREATE `engine/batch_commentary.py`** — NEW file that IMPORTS from commentary.py and adds batch-level templates

---

## Part 1: QA Fixes (Apply First)

### Fix 1A: Recent OFI = -1.000 Bug

**Problem:** The Recent OFI (15 min) metric shows -1.000 for HUBC, which means 100% sell imbalance. This contradicts the 52/48 buy-dominant classification. Root cause: the 15-minute lookback window likely falls in the closing auction period (15:28-15:30) or a zero-tick gap, causing division by zero or all-sell classification.

**Fix in `engine/signal_score.py`**, in the function that computes recent OFI:

```python
def compute_recent_ofi(ofi_df: pd.DataFrame, lookback_minutes: int = 15) -> float:
    """
    Get average OFI over the last N minutes of ACTIVE trading.
    
    FIX: Exclude auction periods and extend lookback if insufficient data.
    - Skip closing auction (15:28-15:30) 
    - Skip opening auction (09:15-09:30)
    - If fewer than 5 minutes of data in window, extend to 30 minutes
    - If still insufficient, return 0.0 (neutral) instead of -1.0
    """
    if ofi_df.empty or 'ofi' not in ofi_df.columns:
        return 0.0  # Neutral, NOT -1.0
    
    idx = ofi_df.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.to_datetime(idx)
    
    # Filter out auction periods
    minutes = idx.hour * 60 + idx.minute
    active_mask = ~(
        ((minutes >= 9*60+15) & (minutes < 9*60+30)) |   # Opening auction
        ((minutes >= 15*60+28) & (minutes <= 15*60+30))   # Closing auction
    )
    active_ofi = ofi_df.loc[active_mask]
    
    if active_ofi.empty:
        return 0.0
    
    # Get last N minutes from active trading
    last_ts = active_ofi.index.max()
    cutoff = last_ts - pd.Timedelta(minutes=lookback_minutes)
    recent = active_ofi.loc[active_ofi.index >= cutoff]
    
    # If too few data points, extend lookback
    if len(recent) < 5:
        cutoff = last_ts - pd.Timedelta(minutes=30)
        recent = active_ofi.loc[active_ofi.index >= cutoff]
    
    if recent.empty:
        return 0.0
    
    return float(recent['ofi'].mean())
```

**Where to apply:** Find the place in signal_score.py or signal_dashboard.py where `recent_ofi` is computed and replace it with this logic. Search for code that does something like:
```python
# FIND THIS PATTERN (approximate):
recent_ofi = ofi_df['ofi'].tail(15).mean()
# or
recent_ofi = ofi_df.iloc[-15:]['ofi'].mean()
```

### Fix 1B: VPIN Display Leaking HTML Style Tag

**Problem:** The VPIN line shows raw text like `copper style='color:#87CEEB'` instead of properly styled output.

**Fix in `ui/page_views/signal_dashboard.py`**, find where VPIN is displayed. It's probably doing something like:

```python
# BROKEN (leaking raw style into text):
st.write(f"VPIN: {vpin_val} (copper style='color:#87CEEB')")
# or building an HTML string incorrectly
```

**Replace with clean display:**

```python
# Option A: Simple metric card (preferred)
if execution_result.vpin_value is not None:
    vpin = execution_result.vpin_value
    if vpin < 0.3:
        toxicity_label = "Low"
        toxicity_color = "#5DCAA5"   # Green
    elif vpin < 0.5:
        toxicity_label = "Moderate"
        toxicity_color = "#87CEEB"   # Light blue
    elif vpin < 0.7:
        toxicity_label = "High"
        toxicity_color = "#EF9F27"   # Amber
    else:
        toxicity_label = "Toxic"
        toxicity_color = "#E24B4A"   # Red
    
    st.markdown(
        f'VPIN: **{vpin:.3f}** — '
        f'<span style="color:{toxicity_color}">{toxicity_label}</span>',
        unsafe_allow_html=True
    )
```

### Fix 1C: Circuit Breaker Score Penalty (Enhancement)

**Problem from QA:** HUBC triggered circuit breakers on 2026-03-03 and 2026-03-10 but the macro score (28/33) doesn't penalize this. Recent circuit breaker hits indicate extreme volatility risk.

**Fix in `engine/macro_regime.py`**, in the scoring section of `compute_macro_regime()`:

```python
# Add AFTER existing macro score calculation:

# Circuit breaker penalty
if len(result.circuit_breaker_dates) > 0:
    # -2 per recent circuit breaker, max -6
    penalty = min(len(result.circuit_breaker_dates) * 2, 6)
    score -= penalty

result.score = max(min(score, 33), 0)  # Clamp to 0-33
```

---

## Part 2: Batch Scanner

### 2A. Add to `engine/signal_score.py`

Add `BatchScanResult` dataclass and `batch_score_symbols()` function as specified in the previous prompt (CLAUDE_CODE_PROMPT_BATCH_SCORING.md). Key points:

- ONE bulk SQL query for all symbols (not N individual queries)
- Progress callback for Streamlit progress bar
- In batch mode, Layers 2+3 default to neutral (16/33) — only macro is fully computed
- Volume filter: skip symbols with < 100K average daily volume (last 20 days)
- Return sorted by signal_score descending
- Include `batch_results_to_dataframe()` for display

**Performance target: 500 symbols in < 60 seconds.**

### 2B. Scoring Enhancements — Add to Existing Scoring

**All of these are ADDITIONS to the existing score, not replacements:**

**a) Momentum crossover (add to `engine/macro_regime.py` MacroRegime dataclass):**
```python
# Add fields to MacroRegime:
momentum_signal: str = "NEUTRAL"    # BULLISH_CROSS / BEARISH_CROSS / BULLISH / BEARISH / NEUTRAL
momentum_score_adj: int = 0         # ±3
```

Compute 20-day vs 60-day SMA crossover in `compute_macro_regime()`. Golden cross = +3, death cross = -3, bullish alignment = +1, bearish alignment = -1.

**b) Volume confirmation (add to macro scoring):**
Compare 20-day avg volume to 40-day avg volume. Rising volume + uptrend = +3, falling volume + uptrend = -2.

**c) VPIN integration into execution score (add to signal_score.py):**
If VPIN < 0.3: +5 points (clean flow). If VPIN > 0.7: -5 points (toxic). Connect to existing `engine/microstructure.py` via import.

**d) Sector-relative adjustment (apply in batch mode after all scores computed):**
If symbol's score is >1 std above sector mean: +5 bonus. If >1 std below: -3 penalty.

**e) Configurable weights (add ScoringConfig dataclass):**
Default: macro 40%, intraday 30%, execution 30%. Expose as sidebar sliders in the dashboard page.

---

## Part 3: Batch Commentary — ADDITIVE to Existing LLM Engine

### CREATE `engine/batch_commentary.py`

This file IMPORTS from the existing commentary engine and adds batch-specific templates. It does NOT replace or modify commentary.py.

```python
"""
engine/batch_commentary.py
Batch-level market narrative templates.

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
import numpy as np
from typing import List, Dict, Optional
from dataclasses import dataclass

# IMPORT the existing commentary engine — DO NOT DUPLICATE
try:
    from engine.commentary import generate_commentary  # or whatever the actual function is
    HAS_COMMENTARY_ENGINE = True
except ImportError:
    HAS_COMMENTARY_ENGINE = False


@dataclass
class BatchNarrative:
    """Structured context for batch-level commentary."""
    # Market overview
    total_scanned: int = 0
    total_with_data: int = 0
    avg_score: float = 0.0
    median_score: float = 0.0
    market_bias: str = "NEUTRAL"       # BULLISH / BEARISH / NEUTRAL
    
    # Regime breakdown
    trending_count: int = 0
    trending_pct: float = 0.0
    mean_rev_count: int = 0
    random_walk_count: int = 0
    
    # Top opportunities
    top_symbols: List[str] = None
    top_scores: List[int] = None
    top_sectors: List[str] = None
    
    # Sector rotation
    sector_rankings: Dict[str, float] = None   # {sector: avg_score}
    strongest_sector: str = ""
    weakest_sector: str = ""
    
    # Warnings
    circuit_breaker_symbols: List[str] = None   # Symbols with recent CB hits
    high_vol_symbols: List[str] = None          # Ann vol > 50%


def build_batch_narrative(results: list) -> BatchNarrative:
    """
    Build structured narrative context from batch scan results.
    
    Args:
        results: List of BatchScanResult from batch_score_symbols()
    
    Returns:
        BatchNarrative with all fields populated
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
    narrative.avg_score = round(np.mean(scores), 1)
    narrative.median_score = round(np.median(scores), 1)
    
    # Market bias
    if narrative.avg_score > 55:
        narrative.market_bias = "BULLISH"
    elif narrative.avg_score < 40:
        narrative.market_bias = "BEARISH"
    else:
        narrative.market_bias = "NEUTRAL"
    
    # Regime breakdown
    narrative.trending_count = sum(1 for r in valid if r.regime == 'TRENDING')
    narrative.mean_rev_count = sum(1 for r in valid if r.regime == 'MEAN_REVERTING')
    narrative.random_walk_count = sum(1 for r in valid if r.regime == 'RANDOM_WALK')
    narrative.trending_pct = round(narrative.trending_count / len(valid) * 100, 1)
    
    # Top opportunities
    top_n = valid[:10]  # Already sorted by score desc
    narrative.top_symbols = [r.symbol for r in top_n]
    narrative.top_scores = [r.signal_score for r in top_n]
    narrative.top_sectors = [r.sector for r in top_n]
    
    # Sector rankings
    from collections import defaultdict
    sector_scores = defaultdict(list)
    for r in valid:
        if r.sector and r.sector != 'UNKNOWN':
            sector_scores[r.sector].append(r.signal_score)
    
    narrative.sector_rankings = {
        sector: round(np.mean(scores), 1) 
        for sector, scores in sector_scores.items()
        if len(scores) >= 3  # Need at least 3 symbols for meaningful sector avg
    }
    
    if narrative.sector_rankings:
        narrative.strongest_sector = max(narrative.sector_rankings, key=narrative.sector_rankings.get)
        narrative.weakest_sector = min(narrative.sector_rankings, key=narrative.sector_rankings.get)
    
    return narrative


def generate_batch_summary_text(narrative: BatchNarrative) -> str:
    """
    Generate a plain-text market summary from batch results.
    
    This is the RULES-BASED template. For AI-powered commentary,
    pass this context to the existing commentary engine.
    """
    if narrative.total_scanned == 0:
        return "No symbols were scanned."
    
    lines = []
    
    # Header
    lines.append(f"**Market Scan Summary** — {narrative.total_with_data} of {narrative.total_scanned} symbols analyzed")
    lines.append("")
    
    # Market bias
    bias_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "🟡"}.get(narrative.market_bias, "⚪")
    lines.append(f"{bias_emoji} **Market Bias: {narrative.market_bias}** — Average score: {narrative.avg_score}/100 (Median: {narrative.median_score})")
    lines.append("")
    
    # Regime breakdown
    lines.append(f"**Regime Distribution:** {narrative.trending_pct:.0f}% trending, "
                 f"{narrative.mean_rev_count} mean-reverting, "
                 f"{narrative.random_walk_count} random walk")
    lines.append("")
    
    # Top opportunities
    if narrative.top_symbols:
        top_str = ", ".join(f"{s} ({sc})" for s, sc in zip(narrative.top_symbols[:5], narrative.top_scores[:5]))
        lines.append(f"**Top Signals:** {top_str}")
        lines.append("")
    
    # Sector rotation
    if narrative.strongest_sector and narrative.weakest_sector:
        lines.append(f"**Sector Rotation:** Strongest = {narrative.strongest_sector} "
                     f"(avg {narrative.sector_rankings.get(narrative.strongest_sector, 0):.0f}), "
                     f"Weakest = {narrative.weakest_sector} "
                     f"(avg {narrative.sector_rankings.get(narrative.weakest_sector, 0):.0f})")
    
    return "\n".join(lines)


def generate_batch_llm_context(narrative: BatchNarrative) -> dict:
    """
    Build a structured context dict for the EXISTING LLM commentary engine.
    
    This dict can be passed to the existing generate_commentary() function
    (or whatever the existing API is) as additional context for AI-powered
    market commentary generation.
    
    READ engine/commentary.py to find the exact function signature, then
    pass this context in the appropriate parameter.
    """
    return {
        "analysis_type": "batch_market_scan",
        "market_overview": {
            "symbols_scanned": narrative.total_scanned,
            "symbols_with_data": narrative.total_with_data,
            "average_signal_score": narrative.avg_score,
            "median_signal_score": narrative.median_score,
            "market_bias": narrative.market_bias,
        },
        "regime_distribution": {
            "trending_count": narrative.trending_count,
            "trending_pct": narrative.trending_pct,
            "mean_reverting_count": narrative.mean_rev_count,
            "random_walk_count": narrative.random_walk_count,
        },
        "top_opportunities": [
            {"symbol": s, "score": sc, "sector": sec}
            for s, sc, sec in zip(
                narrative.top_symbols or [],
                narrative.top_scores or [],
                narrative.top_sectors or []
            )
        ],
        "sector_rotation": {
            "rankings": narrative.sector_rankings or {},
            "strongest": narrative.strongest_sector,
            "weakest": narrative.weakest_sector,
        },
        "prompt_hint": (
            "Generate a concise market commentary based on this batch scan. "
            "Focus on: (1) overall market direction, (2) which sectors show strength, "
            "(3) top individual opportunities and why, (4) regime distribution and what it implies. "
            "Keep it under 200 words. Use a professional financial analyst tone."
        )
    }


def render_batch_commentary(narrative: BatchNarrative) -> str:
    """
    Render batch commentary — tries AI first, falls back to rules-based.
    
    1. If existing LLM commentary engine is available, build context and call it
    2. If not available or fails, use the rules-based template
    
    This function is called from the UI page.
    """
    # Try AI-powered commentary via existing engine
    if HAS_COMMENTARY_ENGINE:
        try:
            llm_context = generate_batch_llm_context(narrative)
            
            # IMPORTANT: Read engine/commentary.py to find the actual function signature.
            # It might be:
            #   generate_commentary(context_dict) -> str
            #   generate_commentary(symbol, metrics, context) -> str
            #   commentary_engine.generate(prompt, context) -> str
            #
            # Adapt this call to match the ACTUAL API:
            ai_commentary = generate_commentary(llm_context)
            
            if ai_commentary and len(ai_commentary) > 50:
                return ai_commentary
        except Exception:
            pass  # Fall through to rules-based
    
    # Fallback: rules-based template
    return generate_batch_summary_text(narrative)
```

---

## Part 4: UI Integration — Batch Mode Tab

### Modify `ui/page_views/signal_dashboard.py`

Add a mode switcher at the top of the page. The existing single-symbol analysis stays exactly as-is.

```python
# At the top of the page, after symbol selector area:
mode = st.sidebar.radio("Analysis Mode", ["Single Symbol", "Batch Scanner"], index=0)

if mode == "Batch Scanner":
    render_batch_scanner_mode()
else:
    render_single_symbol_mode()  # All existing code goes here, unchanged
```

**Batch Scanner Mode function:**

```python
def render_batch_scanner_mode():
    """Batch scanner UI — scores all symbols in selected market."""
    
    st.markdown("### 🔍 Batch Signal Scanner")
    
    # --- Sidebar controls ---
    # Market selector (if you have market filtering)
    # Sector filter: All / specific sector
    # Min volume filter
    # Top N display count
    
    with st.sidebar:
        sector_filter = st.selectbox("Sector Filter", ["All Sectors"] + get_sectors_list())
        min_volume = st.number_input("Min Avg Daily Volume", value=100_000, step=50_000)
        top_n = st.selectbox("Show Top", [10, 20, 50, 100], index=0)
        
        run_scan = st.button("🚀 Run Batch Scan", type="primary", use_container_width=True)
        
        # Scoring weights (configurable)
        with st.expander("⚙️ Scoring Weights"):
            macro_w = st.slider("Macro", 0.2, 0.6, 0.4, 0.05)
            intra_w = st.slider("Intraday", 0.1, 0.5, 0.3, 0.05)
            exec_w = st.slider("Execution", 0.1, 0.5, 0.3, 0.05)
    
    if run_scan:
        # Get symbols
        symbols = get_active_symbols(sector=sector_filter if sector_filter != "All Sectors" else None)
        sector_map = get_sector_map()
        
        # Progress bar
        progress = st.progress(0, text="Initializing scan...")
        start_time = time.time()
        
        def update_progress(current, total):
            progress.progress(current / max(total, 1), text=f"Scoring {current}/{total} symbols...")
        
        # Run batch
        from engine.signal_score import batch_score_symbols, batch_results_to_dataframe
        results = batch_score_symbols(
            symbols, sector_map, 
            eod_db_path=str(DB_PATH),  # Use actual path variable
            tick_db_path=str(TICK_DB_PATH) if TICK_DB_PATH.exists() else None,
            progress_callback=update_progress
        )
        
        elapsed = time.time() - start_time
        progress.empty()
        
        # Stats bar
        st.markdown(f"Scanned **{len(results)}** symbols in **{elapsed:.1f}s** — "
                     f"Top score: **{results[0].signal_score}** ({results[0].symbol})")
        
        # --- AI Commentary (additive, uses existing engine) ---
        from engine.batch_commentary import build_batch_narrative, render_batch_commentary
        narrative = build_batch_narrative(results)
        commentary_text = render_batch_commentary(narrative)
        st.markdown(commentary_text)
        
        st.markdown("---")
        
        # --- Top N Table ---
        st.markdown(f"#### Top {top_n} Signals")
        df = batch_results_to_dataframe(results[:top_n])
        
        # Color-code by score
        def highlight_score(val):
            if isinstance(val, (int, float)):
                if val >= 71: return 'color: #5DCAA5'
                elif val >= 51: return 'color: #87CEEB'
                elif val >= 31: return 'color: #EF9F27'
                else: return 'color: #E24B4A'
            return ''
        
        styled = df.style.applymap(highlight_score, subset=['Score'])
        st.dataframe(styled, use_container_width=True, hide_index=True)
        
        # Deep dive button
        selected_symbol = st.selectbox("Select symbol for deep dive:", 
                                        [r.symbol for r in results[:top_n]])
        if st.button("📊 Deep Dive →"):
            st.session_state['signal_symbol'] = selected_symbol
            st.session_state['signal_mode'] = 'Single Symbol'
            st.rerun()
        
        # --- Sector Heatmap ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Sector Strength")
            if narrative.sector_rankings:
                import plotly.express as px
                sector_df = pd.DataFrame([
                    {'Sector': k, 'Avg Score': v} 
                    for k, v in sorted(narrative.sector_rankings.items(), 
                                       key=lambda x: x[1], reverse=True)
                ])
                fig = px.bar(sector_df, x='Avg Score', y='Sector', orientation='h',
                             color='Avg Score', color_continuous_scale=['#E24B4A', '#EF9F27', '#5DCAA5'],
                             template='plotly_dark')
                fig.update_layout(
                    paper_bgcolor='#0B0E11', plot_bgcolor='#0B0E11',
                    font=dict(family='JetBrains Mono, monospace'),
                    height=max(300, len(sector_df) * 28),
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Regime Distribution")
            import plotly.graph_objects as go
            regime_fig = go.Figure(data=[go.Pie(
                labels=['Trending', 'Mean Reverting', 'Random Walk'],
                values=[narrative.trending_count, narrative.mean_rev_count, narrative.random_walk_count],
                marker=dict(colors=['#5DCAA5', '#7F77DD', '#888780']),
                hole=0.5,
                textinfo='label+percent',
                textfont=dict(size=12)
            )])
            regime_fig.update_layout(
                paper_bgcolor='#0B0E11', plot_bgcolor='#0B0E11',
                font=dict(family='JetBrains Mono, monospace', color='#CCCCCC'),
                height=350, showlegend=False
            )
            st.plotly_chart(regime_fig, use_container_width=True)
        
        # --- Full Results Table + CSV Download ---
        with st.expander(f"📋 Full Results ({len(results)} symbols)"):
            full_df = batch_results_to_dataframe(results)
            st.dataframe(full_df, use_container_width=True, hide_index=True)
            
            csv = full_df.to_csv(index=False)
            st.download_button(
                "⬇️ Download CSV",
                csv,
                f"signal_scan_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv"
            )
```

---

## Commentary Integration — How It Works

```
                    ┌──────────────────────────────┐
                    │  engine/commentary.py         │
                    │  (EXISTING — DO NOT MODIFY)   │
                    │                               │
                    │  • Rules-based commentary     │
                    │  • LLM AI-powered narratives  │
                    │  • Single-symbol analysis      │
                    └──────────┬───────────────────┘
                               │ imported by
                               ▼
                    ┌──────────────────────────────┐
                    │  engine/batch_commentary.py   │
                    │  (NEW — ADDITIVE)             │
                    │                               │
                    │  • build_batch_narrative()    │
                    │  • generate_batch_llm_context │
                    │  • render_batch_commentary()  │
                    │    └→ calls generate_commentary│
                    │       from existing engine    │
                    │    └→ falls back to template  │
                    └──────────┬───────────────────┘
                               │ called by
                               ▼
                    ┌──────────────────────────────┐
                    │  signal_dashboard.py          │
                    │  Batch Scanner tab            │
                    │                               │
                    │  render_batch_scanner_mode()  │
                    │    └→ render_batch_commentary │
                    └──────────────────────────────┘
```

**The flow:**
1. Batch scanner scores all symbols → produces `List[BatchScanResult]`
2. `build_batch_narrative()` aggregates results into `BatchNarrative` struct
3. `render_batch_commentary()` tries to call the EXISTING `generate_commentary()` from `engine/commentary.py` with batch context
4. If that works → AI-powered market narrative
5. If it fails (wrong API, engine unavailable) → falls back to rules-based markdown template
6. Either way, the existing single-symbol commentary in the regular analysis tab continues to work exactly as before

---

## Verification

```bash
# 1. Existing commentary engine still works
python -c "from engine.commentary import generate_commentary; print('existing commentary OK')"

# 2. New batch commentary imports correctly
python -c "from engine.batch_commentary import build_batch_narrative, render_batch_commentary; print('batch commentary OK')"

# 3. Batch scorer works
python -c "
from engine.signal_score import batch_score_symbols
import sqlite3
conn = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
syms = [r[0] for r in conn.execute('SELECT DISTINCT symbol FROM eod_data LIMIT 10').fetchall()]
conn.close()
results = batch_score_symbols(syms, {}, '/mnt/e/psxdata/psx.sqlite')
print(f'Scored {len(results)} symbols. Top: {results[0].symbol} = {results[0].signal_score}')
"

# 4. OFI fix — verify recent_ofi no longer returns -1.000
python -c "
from engine.signal_score import compute_recent_ofi
import pandas as pd
# Empty OFI should return 0.0, not -1.0
assert compute_recent_ofi(pd.DataFrame()) == 0.0
print('OFI fix OK')
"

# 5. Page loads in both modes
streamlit run src/psx_ohlcv/ui/app.py
# → Signal Dashboard → Single Symbol → Run Analysis (existing works)
# → Signal Dashboard → Batch Scanner → Run Batch Scan (new works)
```

---

## Summary

| Change | File | Type |
|--------|------|------|
| Fix OFI -1.000 bug | `signal_score.py` | MODIFY |
| Fix VPIN display leak | `signal_dashboard.py` | MODIFY |
| Add circuit breaker penalty | `macro_regime.py` | MODIFY |
| Add momentum crossover | `macro_regime.py` | MODIFY |
| Add volume confirmation | `macro_regime.py` | MODIFY |
| Add BatchScanResult + batch_score_symbols | `signal_score.py` | MODIFY |
| Add ScoringConfig + configurable weights | `signal_score.py` | MODIFY |
| Add VPIN into execution score | `signal_score.py` | MODIFY |
| Add sector-relative adjustment | `signal_score.py` | MODIFY |
| Batch commentary (imports existing engine) | `batch_commentary.py` | CREATE |
| Batch scanner UI tab | `signal_dashboard.py` | MODIFY |

**Files modified: 3** | **Files created: 1** | **Existing commentary engine: UNTOUCHED**
