# Claude Code Prompt: Signal Dashboard — Batch Scanner + Scoring Enhancements

## Context

The Signal Dashboard page (`ui/page_views/signal_dashboard.py`) is live with market/symbol filtering and the 3-layer analysis (Macro → Intraday → Execution → Score 1-100). This prompt adds two major capabilities:

1. **Batch Scanner** — score ALL active symbols in a market, rank by signal score, surface the top N
2. **Scoring Enhancements** — add new signals, refine weights, integrate VPIN toxicity

**Read these files FIRST before writing any code:**
```
engine/macro_regime.py          — Layer 1 (Hurst, SMA, volatility, circuit breakers)
engine/signal_score.py          — Layers 2+3 + composite scoring (IntradayAnchor, ExecutionDNA, SignalReport)
engine/microstructure.py        — VPIN engine (currently NOT integrated into signal score)
engine/commentary.py            — Narrative engine
ui/page_views/signal_dashboard.py — Current single-symbol analysis page
ui/page_views/tick_analytics.py   — Existing CVD, volume profile, OFI, volatility
db/repositories/tick_logs.py      — Tick data repository
```

---

## Rules

1. **MODIFY `engine/signal_score.py`** — add batch functions + enhanced scoring
2. **MODIFY `ui/page_views/signal_dashboard.py`** — add Batch Scanner tab/section
3. **DO NOT modify any other files** — zero changes to macro_regime.py, microstructure.py, commentary.py, tick_analytics.py, app.py, or anything else
4. **Performance matters** — batch scoring 500 symbols must complete in < 60 seconds. Use vectorized pandas, not per-symbol loops where avoidable.
5. **Graceful degradation** — if a symbol has no intraday or tick data, score it on macro alone (don't skip it).

---

## Part 1: Batch Scanner

### 1A. Engine: Add to `engine/signal_score.py`

```python
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
import time

@dataclass 
class BatchScanResult:
    """Result of scoring a single symbol in batch mode."""
    symbol: str
    sector: str
    current_price: float
    signal_score: int
    interpretation: str
    macro_score: int
    intraday_score: int
    execution_score: int
    regime: str              # TRENDING / MEAN_REVERTING / RANDOM_WALK
    hurst: float
    sma_distance_pct: float
    ann_volatility: float
    has_intraday: bool
    has_ticks: bool
    error: str = ""          # Non-empty if analysis failed


def batch_score_symbols(
    symbols: List[str],
    sector_map: dict,
    eod_db_path: str,
    tick_db_path: str = None,
    max_workers: int = 4,
    progress_callback=None     # For Streamlit progress bar: callback(current, total)
) -> List[BatchScanResult]:
    """
    Score multiple symbols and return sorted results.
    
    PERFORMANCE STRATEGY:
    1. Load ALL eod_data for ALL requested symbols in ONE query (not per-symbol)
    2. Group by symbol, compute macro for each
    3. For intraday/tick layers, only attempt if DB exists and has data
    4. Use ThreadPoolExecutor for parallel macro computation (CPU-bound numpy)
    
    Args:
        symbols: List of symbol strings to score
        sector_map: dict of {symbol: sector_name}
        eod_db_path: Path to psx.sqlite (or whatever the main DB is)
        tick_db_path: Path to tick_bars.db (optional, may not exist)
        max_workers: Thread pool size (4 is safe for SQLite)
        progress_callback: Optional callable(current_idx, total) for UI progress
    
    Returns:
        List of BatchScanResult sorted by signal_score descending
    """
    results = []
    
    # === STEP 1: Bulk load EOD data ===
    # ONE query for all symbols — much faster than N queries
    import sqlite3
    conn = sqlite3.connect(eod_db_path)
    
    # Build placeholders for IN clause
    placeholders = ",".join(["?"] * len(symbols))
    
    # Get last 2 years of data for all symbols at once
    # IMPORTANT: Check actual table name — might be eod_data or eod_ohlcv
    cutoff_date = (pd.Timestamp.now() - pd.DateOffset(years=2)).strftime('%Y-%m-%d')
    
    all_eod = pd.read_sql(f"""
        SELECT symbol, date, open, high, low, close, volume
        FROM eod_data
        WHERE symbol IN ({placeholders})
          AND date >= ?
        ORDER BY symbol, date
    """, conn, params=symbols + [cutoff_date])
    conn.close()
    
    # Group by symbol
    grouped = dict(list(all_eod.groupby('symbol')))
    
    # === STEP 2: Try to load KSE-100 for relative strength ===
    try:
        conn = sqlite3.connect(eod_db_path)
        index_df = pd.read_sql("""
            SELECT date, close FROM eod_data 
            WHERE symbol IN ('KSE100', 'KSE-100', 'KSEI')
              AND date >= ?
            ORDER BY date
        """, conn, params=[cutoff_date])
        conn.close()
    except Exception:
        index_df = pd.DataFrame()
    
    # === STEP 3: Score each symbol ===
    total = len(symbols)
    
    for i, sym in enumerate(symbols):
        if progress_callback:
            progress_callback(i, total)
        
        try:
            sym_df = grouped.get(sym, pd.DataFrame())
            
            if sym_df.empty or len(sym_df) < 5:
                results.append(BatchScanResult(
                    symbol=sym,
                    sector=sector_map.get(sym, 'UNKNOWN'),
                    current_price=0,
                    signal_score=0,
                    interpretation="Insufficient data",
                    macro_score=0, intraday_score=0, execution_score=0,
                    regime="UNKNOWN", hurst=0.5,
                    sma_distance_pct=0, ann_volatility=0,
                    has_intraday=False, has_ticks=False,
                    error=f"Only {len(sym_df)} days of data"
                ))
                continue
            
            # Compute Layer 1 (Macro) — this is the core
            from engine.macro_regime import compute_macro_regime
            macro = compute_macro_regime(
                sym_df, sym,
                sector=sector_map.get(sym),
                index_df=index_df if not index_df.empty else None
            )
            
            # Layer 2 & 3 — lightweight check only in batch mode
            # Full intraday/tick analysis is expensive; batch mode uses macro + quick checks
            intraday_score = 16  # Neutral default
            execution_score = 16  # Neutral default
            has_intraday = False
            has_ticks = False
            
            # Quick intraday check: does this symbol have recent intraday data?
            # (Don't run full VWAP/POC in batch — too slow for 500 symbols)
            try:
                conn = sqlite3.connect(eod_db_path)
                row = conn.execute(
                    "SELECT COUNT(*) FROM intraday_data WHERE symbol = ?", (sym,)
                ).fetchone()
                conn.close()
                has_intraday = row[0] > 0 if row else False
            except Exception:
                pass
            
            # Quick tick check
            if tick_db_path:
                try:
                    import os
                    if os.path.exists(tick_db_path):
                        conn = sqlite3.connect(tick_db_path)
                        row = conn.execute(
                            "SELECT COUNT(*) FROM raw_ticks WHERE symbol = ?", (sym,)
                        ).fetchone()
                        conn.close()
                        has_ticks = row[0] > 0 if row else False
                except Exception:
                    pass
            
            # Composite score (macro-dominant in batch mode)
            # Scale macro to fill more of the range since L2/L3 are neutral
            total_score = 1 + macro.score + intraday_score + execution_score
            total_score = min(total_score, 100)
            
            from engine.signal_score import interpret_score
            
            results.append(BatchScanResult(
                symbol=sym,
                sector=sector_map.get(sym, 'UNKNOWN'),
                current_price=macro.current_price,
                signal_score=total_score,
                interpretation=interpret_score(total_score),
                macro_score=macro.score,
                intraday_score=intraday_score,
                execution_score=execution_score,
                regime=macro.regime,
                hurst=macro.hurst_exponent,
                sma_distance_pct=macro.sma_distance_pct,
                ann_volatility=macro.ann_volatility,
                has_intraday=has_intraday,
                has_ticks=has_ticks
            ))
            
        except Exception as e:
            results.append(BatchScanResult(
                symbol=sym,
                sector=sector_map.get(sym, 'UNKNOWN'),
                current_price=0,
                signal_score=0,
                interpretation="Error",
                macro_score=0, intraday_score=0, execution_score=0,
                regime="ERROR", hurst=0.5,
                sma_distance_pct=0, ann_volatility=0,
                has_intraday=False, has_ticks=False,
                error=str(e)
            ))
    
    if progress_callback:
        progress_callback(total, total)
    
    # Sort by signal_score descending
    results.sort(key=lambda r: r.signal_score, reverse=True)
    return results


def batch_results_to_dataframe(results: List[BatchScanResult]) -> pd.DataFrame:
    """Convert batch results to a display-ready DataFrame."""
    records = []
    for r in results:
        records.append({
            'Symbol': r.symbol,
            'Sector': r.sector,
            'Price': r.current_price,
            'Score': r.signal_score,
            'Signal': r.interpretation,
            'Macro': f"{r.macro_score}/33",
            'Intra': f"{r.intraday_score}/33",
            'Exec': f"{r.execution_score}/33",
            'Regime': r.regime,
            'Hurst': r.hurst,
            'SMA Dist %': r.sma_distance_pct,
            'Ann Vol %': r.ann_volatility,
            'Intraday': '✅' if r.has_intraday else '—',
            'Ticks': '✅' if r.has_ticks else '—',
        })
    return pd.DataFrame(records)
```

### 1B. UI: Add Batch Scanner Tab to `signal_dashboard.py`

Add a new tab/section at the top of the page. The page should now have two modes:

```python
mode = st.sidebar.radio("Mode", ["Single Symbol Analysis", "Batch Scanner"], index=0)

if mode == "Batch Scanner":
    render_batch_scanner()
elif mode == "Single Symbol Analysis":
    render_single_analysis()  # existing logic
```

**Batch Scanner UI Layout:**

```
┌─ SIDEBAR ─────────────────────┐  ┌─ MAIN AREA ──────────────────────────────────┐
│                                 │  │                                                │
│ Mode: ● Batch Scanner           │  │  BATCH SIGNAL SCANNER                          │
│                                 │  │  ──────────────────                             │
│ Market [▼ REG]                  │  │                                                │
│ Sector [▼ All / specific]       │  │  Scanned: 487 symbols in 42.3s                │
│ Min Volume [100,000]            │  │                                                │
│ Show Top [▼ 10 / 20 / 50]      │  │  ═══ TOP 10 SIGNALS ═══                        │
│                                 │  │  ┌─────┬────────┬───────┬───────┬────────┐     │
│ [🚀 Run Batch Scan]            │  │  │ Sym  │ Score  │ Regime│ Hurst │ vs SMA │     │
│                                 │  │  ├─────┼────────┼───────┼───────┼────────┤     │
│ ── Last Scan ──                 │  │  │ HUBC│  78    │ TREND │ 0.62  │ +8.7%  │     │
│ Symbols: 487                    │  │  │ LUCK│  74    │ TREND │ 0.59  │ +5.2%  │     │
│ Time: 42.3s                     │  │  │ ENGRO│ 71    │ TREND │ 0.57  │ +3.1%  │     │
│ Top Score: 78 (HUBC)            │  │  │ ...  │ ...   │ ...   │ ...   │ ...    │     │
│                                 │  │  └─────┴────────┴───────┴───────┴────────┘     │
│ [📊 Deep Dive Selected]        │  │                                                │
│ (switches to single mode)       │  │  ═══ SECTOR HEATMAP ═══                        │
│                                 │  │  [Treemap or grid showing avg score by sector]  │
│                                 │  │                                                │
│                                 │  │  ═══ REGIME DISTRIBUTION ═══                   │
│                                 │  │  [Pie/donut: % trending vs mean-rev vs random] │
│                                 │  │                                                │
│                                 │  │  ═══ FULL RESULTS TABLE ═══                    │
│                                 │  │  [Sortable dataframe with all scanned symbols]  │
│                                 │  │  [Download CSV button]                          │
└─────────────────────────────────┘  └────────────────────────────────────────────────┘
```

**Key UI details:**

1. **Progress bar:** Use `st.progress()` during the scan. Update via the `progress_callback` parameter:
   ```python
   progress_bar = st.progress(0, text="Scanning symbols...")
   def update_progress(current, total):
       progress_bar.progress(current / total, text=f"Scoring {current}/{total}...")
   
   results = batch_score_symbols(symbols, sector_map, db_path, 
                                  progress_callback=update_progress)
   progress_bar.empty()
   ```

2. **Top N table:** Use `st.dataframe` with row coloring based on score:
   - Score 71+: green row highlight
   - Score 51-70: subtle green
   - Score 31-50: neutral
   - Score < 31: subtle red

3. **Sector heatmap:** Group results by sector, compute average score per sector. Display as a colored grid using Plotly heatmap or treemap. Sectors with higher average scores = warmer colors.

4. **Regime distribution:** Plotly donut chart showing count of TRENDING vs MEAN_REVERTING vs RANDOM_WALK symbols.

5. **"Deep Dive" button:** When user selects a row in the batch table, clicking "Deep Dive" switches to single-symbol mode with that symbol pre-selected. Use `st.session_state` to pass the symbol.

6. **Download CSV:** `st.download_button` with `batch_results_to_dataframe(results).to_csv()`.

7. **Volume filter:** Before scanning, filter symbols by minimum average daily volume (last 20 days). This prevents scoring illiquid symbols that produce meaningless signals:
   ```python
   # Pre-filter: only score symbols with avg volume > threshold
   avg_vol = all_eod.groupby('symbol')['volume'].apply(
       lambda x: x.tail(20).mean()
   )
   liquid_symbols = avg_vol[avg_vol >= min_volume].index.tolist()
   ```

---

## Part 2: Scoring Enhancements

### 2A. Integrate VPIN Toxicity into Execution Score

VPIN already exists in `engine/microstructure.py` but is NOT connected to the signal score. Fix this.

In `engine/signal_score.py`, when computing the execution layer:

```python
# After computing CVD, OFI, blocks...

# VPIN integration (if available)
try:
    from engine.microstructure import compute_vpin  # or whatever the actual function name is
    # Read the source to find the correct function signature
    # Likely: compute_vpin(df, bucket_size=...) -> returns VPIN value 0-1
    
    if tick_df is not None and not tick_df.empty:
        vpin_result = compute_vpin(tick_df)  # Adapt to actual API
        execution.vpin_value = vpin_result
        
        # VPIN scoring: low toxicity = good for buying
        if vpin_result < 0.3:
            execution.vpin_toxicity = 'LOW'
            # Bonus: low toxicity environment is favorable
        elif vpin_result < 0.5:
            execution.vpin_toxicity = 'MODERATE'
        elif vpin_result < 0.7:
            execution.vpin_toxicity = 'HIGH'
            # Penalty: high toxicity means adverse selection risk
        else:
            execution.vpin_toxicity = 'TOXIC'
            # Heavy penalty: extreme toxicity
except ImportError:
    pass  # VPIN not available, skip
```

**Add VPIN to execution score calculation:**

```python
# Inside the execution score section of compute_signal_score():

# VPIN bonus/penalty (±5 points)
if execution.vpin_value is not None:
    if execution.vpin_value < 0.3:
        exec_score += 5   # Clean order flow
    elif execution.vpin_value > 0.7:
        exec_score -= 5   # Toxic — subtract from score
    # 0.3-0.7 = no adjustment
```

### 2B. Add Volume Confirmation to Macro Score

A trend is only meaningful if confirmed by volume. Add to macro scoring:

```python
# In compute_macro_regime() or in the scoring section:

# Volume trend confirmation
if len(df) >= 40:
    vol_20d = df['volume'].tail(20).mean()
    vol_40d = df['volume'].tail(40).mean()
    
    if vol_40d > 0:
        vol_ratio = vol_20d / vol_40d
        
        # Rising volume + uptrend = bullish confirmation (+3)
        if vol_ratio > 1.2 and sma_distance_pct > 0:
            macro_score += 3
        # Falling volume + uptrend = weak trend (-2)  
        elif vol_ratio < 0.8 and sma_distance_pct > 0:
            macro_score -= 2
        # Rising volume + downtrend = bearish confirmation (-2)
        elif vol_ratio > 1.2 and sma_distance_pct < 0:
            macro_score -= 2
```

### 2C. Add Sector-Relative Scoring

A stock scoring 70 in a sector where everything scores 70 is less interesting than one scoring 60 in a sector averaging 40.

```python
def adjust_score_sector_relative(
    symbol_score: int,
    sector_avg_score: float,
    sector_std_score: float
) -> int:
    """
    Adjust signal score relative to sector peers.
    +5 bonus if the symbol is >1 std above sector mean.
    -3 penalty if the symbol is >1 std below sector mean.
    """
    if sector_std_score < 1:
        return symbol_score  # Not enough variance
    
    z_score = (symbol_score - sector_avg_score) / sector_std_score
    
    if z_score > 1.0:
        return min(symbol_score + 5, 100)   # Sector outperformer
    elif z_score < -1.0:
        return max(symbol_score - 3, 1)     # Sector laggard
    return symbol_score
```

Apply this in the batch scanner after all symbols are scored — iterate results by sector, compute sector stats, then adjust.

### 2D. Add Momentum Factor

Short-term momentum (20-day) vs medium-term (60-day) crossover:

```python
# Add to MacroRegime dataclass:
momentum_signal: str = "NEUTRAL"  # BULLISH_CROSS / BEARISH_CROSS / NEUTRAL
momentum_score_adj: int = 0       # ±3 bonus/penalty

# Add to compute_macro_regime():
if len(df) >= 60:
    sma_20 = df['close'].rolling(20).mean().iloc[-1]
    sma_60 = df['close'].rolling(60).mean().iloc[-1]
    sma_20_prev = df['close'].rolling(20).mean().iloc[-2]
    sma_60_prev = df['close'].rolling(60).mean().iloc[-2]
    
    # Golden cross: 20-day crosses above 60-day
    if sma_20 > sma_60 and sma_20_prev <= sma_60_prev:
        result.momentum_signal = "BULLISH_CROSS"
        result.momentum_score_adj = 3
    # Death cross: 20-day crosses below 60-day
    elif sma_20 < sma_60 and sma_20_prev >= sma_60_prev:
        result.momentum_signal = "BEARISH_CROSS"
        result.momentum_score_adj = -3
    # Bullish alignment: 20 > 60 (sustained trend)
    elif sma_20 > sma_60:
        result.momentum_signal = "BULLISH"
        result.momentum_score_adj = 1
    # Bearish alignment
    else:
        result.momentum_signal = "BEARISH"
        result.momentum_score_adj = -1
```

### 2E. Revised Score Weights

The current scoring gives equal weight (33/33/33) to all three layers. In batch mode where L2/L3 data may be sparse, and even in single mode, macro should carry more weight since it's the most reliable signal on PSX (thin intraday, sporadic tick data).

**Proposed revised weights — make configurable:**

```python
@dataclass
class ScoringConfig:
    """Configurable scoring weights."""
    macro_weight: float = 0.40     # 40% (was 33%)
    intraday_weight: float = 0.30  # 30% (was 33%)
    execution_weight: float = 0.30 # 30% (was 33%)
    
    # Bonus/penalty caps
    vpin_bonus_max: int = 5
    volume_confirm_max: int = 3
    sector_relative_max: int = 5
    momentum_max: int = 3
    
    # Thresholds
    hurst_trending: float = 0.55
    hurst_mean_rev: float = 0.45
    vwap_near_std: float = 0.5
    ofi_strong_buy: float = 0.3
    circuit_breaker_pct: float = 7.0


DEFAULT_CONFIG = ScoringConfig()

def compute_signal_score_v2(
    macro: MacroRegime,
    intraday: IntradayAnchor,
    execution: ExecutionDNA,
    config: ScoringConfig = DEFAULT_CONFIG
) -> int:
    """
    Enhanced composite score with configurable weights.
    
    Base score = weighted sum of layer scores (each 0-100 internally, then weighted)
    + bonuses (momentum, volume confirmation, sector relative)
    + penalties (VPIN toxicity, circuit breakers)
    
    Final range: 1-100
    """
    # Normalize each layer to 0-100 first
    macro_pct = (macro.score / 33) * 100 if macro else 50
    intraday_pct = (intraday.score / 33) * 100 if intraday else 50
    execution_pct = (execution.score / 33) * 100 if execution else 50
    
    # Weighted base
    base = (
        macro_pct * config.macro_weight +
        intraday_pct * config.intraday_weight +
        execution_pct * config.execution_weight
    )
    
    # Bonuses
    bonus = 0
    if macro:
        bonus += macro.momentum_score_adj  # ±3
    if execution and execution.vpin_value is not None:
        if execution.vpin_value < 0.3:
            bonus += config.vpin_bonus_max
        elif execution.vpin_value > 0.7:
            bonus -= config.vpin_bonus_max
    
    total = int(np.clip(base + bonus, 1, 100))
    return total
```

**Add a sidebar config expander** in the signal dashboard page:
```python
with st.sidebar.expander("⚙️ Scoring Weights"):
    macro_w = st.slider("Macro Weight", 0.2, 0.6, 0.4, 0.05)
    intra_w = st.slider("Intraday Weight", 0.1, 0.5, 0.3, 0.05)
    exec_w = st.slider("Execution Weight", 0.1, 0.5, 0.3, 0.05)
    
    # Normalize to sum to 1.0
    total_w = macro_w + intra_w + exec_w
    config = ScoringConfig(
        macro_weight=macro_w / total_w,
        intraday_weight=intra_w / total_w,
        execution_weight=exec_w / total_w
    )
```

---

## Part 3: Commentary Integration

After batch scan completes, generate a market-level narrative using the existing commentary engine:

```python
# After batch_results are computed:
try:
    from engine.commentary import generate_commentary  # Check actual function name
    
    # Build market context from batch results
    trending_count = sum(1 for r in results if r.regime == 'TRENDING')
    mean_rev_count = sum(1 for r in results if r.regime == 'MEAN_REVERTING')
    avg_score = np.mean([r.signal_score for r in results])
    top_5 = [r.symbol for r in results[:5]]
    
    market_context = {
        'total_scanned': len(results),
        'avg_score': avg_score,
        'trending_pct': trending_count / len(results) * 100,
        'top_symbols': top_5,
        'market_regime': 'BULLISH' if avg_score > 55 else 'BEARISH' if avg_score < 40 else 'NEUTRAL'
    }
    
    # If commentary engine accepts this format, generate narrative
    # Otherwise, build a simple template:
    narrative = f"""
    **Market Scan Summary** — {len(results)} symbols analyzed
    
    Average signal score: {avg_score:.0f}/100 ({interpret_score(int(avg_score))})
    
    Regime breakdown: {trending_count} trending ({trending_count/len(results)*100:.0f}%), 
    {mean_rev_count} mean-reverting, {len(results)-trending_count-mean_rev_count} random walk.
    
    Top opportunities: {', '.join(top_5)}
    """
    
    st.markdown(narrative)
except ImportError:
    pass
```

---

## Verification

```bash
# 1. Batch function imports
python -c "from engine.signal_score import batch_score_symbols, BatchScanResult; print('batch OK')"

# 2. Scoring config imports
python -c "from engine.signal_score import ScoringConfig, compute_signal_score_v2; print('config OK')"

# 3. Quick batch test (10 symbols)
python -c "
from engine.signal_score import batch_score_symbols, batch_results_to_dataframe
import sqlite3

conn = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
symbols = [row[0] for row in conn.execute(
    'SELECT DISTINCT symbol FROM eod_data ORDER BY symbol LIMIT 10'
).fetchall()]
conn.close()

print(f'Testing {len(symbols)} symbols...')
results = batch_score_symbols(symbols, {}, '/mnt/e/psxdata/psx.sqlite')
df = batch_results_to_dataframe(results)
print(df[['Symbol', 'Score', 'Signal', 'Regime', 'Hurst']].to_string())
"

# 4. Full batch (all REG symbols) — expect < 60s
python -c "
import time
from engine.signal_score import batch_score_symbols
import sqlite3

conn = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
symbols = [row[0] for row in conn.execute(
    'SELECT symbol FROM symbols WHERE status = \"active\"'
).fetchall()]
conn.close()

start = time.time()
results = batch_score_symbols(symbols, {}, '/mnt/e/psxdata/psx.sqlite')
elapsed = time.time() - start
print(f'{len(symbols)} symbols scored in {elapsed:.1f}s')
print(f'Top 5: {[(r.symbol, r.signal_score) for r in results[:5]]}')
assert elapsed < 60, f'Too slow: {elapsed}s > 60s target'
"

# 5. Streamlit page loads without errors
streamlit run src/psx_ohlcv/ui/app.py
# → Signal Dashboard → Switch to Batch Scanner mode
# → Select REG market → Run Batch Scan
# → Should see progress bar, then top 10 table, sector heatmap, regime donut
```

---

## Summary

| Enhancement | Where | Impact |
|------------|-------|--------|
| Batch scanner engine | `engine/signal_score.py` | Score all 500 symbols in <60s |
| Batch scanner UI | `signal_dashboard.py` | Top N table, sector heatmap, regime donut, CSV export |
| VPIN integration | `signal_score.py` | ±5 points based on order flow toxicity |
| Volume confirmation | `macro_regime.py` scoring | ±3 points for volume-trend alignment |
| Sector-relative adjust | `signal_score.py` | ±5 points vs sector peers |
| Momentum crossover | `macro_regime.py` | ±3 points for 20/60 SMA cross |
| Configurable weights | `signal_score.py` | Sidebar sliders for macro/intra/exec weights |
| Commentary integration | `signal_dashboard.py` | AI narrative of batch scan results |

**Files modified: 2** (`engine/signal_score.py`, `ui/page_views/signal_dashboard.py`)
**Files created: 0**
