# Claude Code Prompt: Phase 1 — Quick Wins Integration

## Context

pakfindata (~/pakfindata/) is a Streamlit multi-page app with Bloomberg Terminal 
dark theme (#0B0E11 bg, #C8A96E gold, JetBrains Mono). DB: `/mnt/e/psxdata/psx.sqlite`.

We have new data files downloaded to `/mnt/e/psxdata/downloads/` from PSX DPS.
This prompt integrates 3 quick wins into EXISTING pages — no new pages.

**Rules:**
- Additive only — do NOT modify existing working features
- Read existing page code FIRST before making changes
- Use pakfindata theme constants (don't hardcode colors)
- All math in raw numpy/pandas (no TA libraries)

## Step 1: Audit existing state

```bash
# Find existing pages
ls ~/pakfindata/src/pakfindata/ui/page_views/

# Check downloaded files
ls -la /mnt/e/psxdata/downloads/daily/$(date +%Y-%m-%d)/ 2>/dev/null
ls -la /mnt/e/psxdata/downloads/reference/

# Check what's in the downloads
file /mnt/e/psxdata/downloads/daily/*/limits/* 2>/dev/null
file /mnt/e/psxdata/downloads/daily/*/margins/* 2>/dev/null
file /mnt/e/psxdata/downloads/reference/*constituent* 2>/dev/null
file /mnt/e/psxdata/downloads/reference/*kse100* 2>/dev/null

# Read a sample of each file to understand format
for f in /mnt/e/psxdata/downloads/daily/*/limits/*; do
    echo "=== $f ==="
    python3 -c "
import zipfile, os
if '$f'.endswith('.zip') or '$f'.endswith('.Z'):
    try:
        z = zipfile.ZipFile('$f')
        print('Contents:', z.namelist())
        for name in z.namelist()[:1]:
            with z.open(name) as inner:
                print(inner.read(500).decode('utf-8', errors='replace'))
    except: print('Not a zip')
else:
    with open('$f', 'rb') as fh:
        print(fh.read(500).decode('utf-8', errors='replace'))
" 2>/dev/null
done

# Same for VAR margins
for f in /mnt/e/psxdata/downloads/daily/*/margins/*; do
    echo "=== $f ==="
    python3 -c "
import zipfile
try:
    z = zipfile.ZipFile('$f')
    print('Contents:', z.namelist())
    for name in z.namelist()[:1]:
        with z.open(name) as inner:
            print(inner.read(500).decode('utf-8', errors='replace'))
except Exception as e: print(f'Error: {e}')
" 2>/dev/null
done

# Index constituents
for f in /mnt/e/psxdata/downloads/daily/*/indices/* /mnt/e/psxdata/downloads/reference/*constituent* /mnt/e/psxdata/downloads/reference/*kse100*; do
    echo "=== $f ==="
    python3 -c "
try:
    import openpyxl
    wb = openpyxl.load_workbook('$f')
    ws = wb.active
    for row in ws.iter_rows(max_row=5, values_only=True):
        print(row)
except:
    try:
        import xlrd
        wb = xlrd.open_workbook('$f')
        ws = wb.sheet_by_index(0)
        for i in range(min(5, ws.nrows)):
            print(ws.row_values(i))
    except Exception as e:
        print(f'Error: {e}')
" 2>/dev/null
done
```

**STOP — read ALL output before proceeding.** Need to understand file formats.

## Step 2: Find existing pages to enhance

```bash
# Live Market page
find ~/pakfindata/src/ -name "*.py" | xargs grep -l "live_market\|Live Market\|market_watch" 2>/dev/null

# Sector Heatmap page
find ~/pakfindata/src/ -name "*.py" | xargs grep -l "sector.*heat\|heatmap\|treemap" 2>/dev/null

# Signal Scanner / Batch Scanner page
find ~/pakfindata/src/ -name "*.py" | xargs grep -l "signal.*scan\|batch.*scan\|signal_score\|composite" 2>/dev/null

# Read each one
cat <live_market_page_path>
cat <heatmap_page_path>
cat <scanner_page_path>
```

**STOP — read ALL page code before modifying.**

## Win 1: Circuit Limits → Live Market Table

### What
Add two columns to the Live Market table: `Upper Limit` and `Lower Limit`.
Highlight symbols that are within 1% of either limit (near circuit breaker).

### Data source
`/mnt/e/psxdata/downloads/daily/{date}/limits/` — contains symbol upper/lower price bounds.

### Implementation

1. Create a loader function in `src/pakfindata/sources/psx_downloads.py` (or a new module):

```python
def load_circuit_limits(date_str: str = None) -> pd.DataFrame:
    """
    Load symbol price upper/lower limits from downloaded files.
    Returns DataFrame with columns: symbol, upper_limit, lower_limit
    """
    if date_str is None:
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")
    
    limits_dir = Path("/mnt/e/psxdata/downloads/daily") / date_str / "limits"
    
    # Find the file (could be .zip, .Z, .csv, .xls)
    # Parse based on format discovered in Step 1
    # Return: pd.DataFrame with columns [symbol, upper_limit, lower_limit]
    ...
```

2. In the Live Market page, after building the main DataFrame:

```python
# Load circuit limits
limits_df = load_circuit_limits()

# Merge
if not limits_df.empty:
    df = df.merge(limits_df, on="symbol", how="left")
    
    # Calculate proximity to circuit
    df["upper_pct"] = ((df["upper_limit"] - df["current"]) / df["current"] * 100).round(2)
    df["lower_pct"] = ((df["current"] - df["lower_limit"]) / df["current"] * 100).round(2)
    
    # Flag near-circuit symbols
    df["near_circuit"] = (df["upper_pct"] < 1.0) | (df["lower_pct"] < 1.0)
```

3. Add columns to the displayed table:
   - `Upper` — upper limit price
   - `Lower` — lower limit price
   - Conditional formatting: red background if `near_circuit == True`

4. Add a filter toggle: "Show only near-circuit symbols"

### Theme
- Near upper circuit: gold (#C8A96E) text or green (#22c55e)
- Near lower circuit: red (#ef4444)
- Normal: default text color

---

## Win 2: Index Weights → Sector Heatmap

### What
Use actual KSE100/KSE30 index weights to size the treemap boxes instead of 
equal-weight or volume-based sizing. Real weight = real market impact.

### Data source
Two possible files:
- `/mnt/e/psxdata/downloads/daily/{date}/indices/constituent_data*.xls`
- `/mnt/e/psxdata/downloads/reference/kse100_companies*.zip`

### Implementation

1. Create a loader:

```python
def load_index_weights(index_name: str = "KSE100", date_str: str = None) -> pd.DataFrame:
    """
    Load index constituent weights.
    Returns DataFrame with columns: symbol, weight, index_name
    """
    # Try daily file first (most recent)
    # Fall back to reference file
    # Parse XLS/CSV based on format discovered in Step 1
    ...
```

2. In the Sector Heatmap page, add an option to size by:
   - Volume (existing)
   - Market Cap (if available)
   - **Index Weight** (new)

```python
size_by = st.radio("Size by", ["Volume", "Index Weight"], horizontal=True)

if size_by == "Index Weight":
    weights_df = load_index_weights("KSE100")
    if not weights_df.empty:
        df = df.merge(weights_df[["symbol", "weight"]], on="symbol", how="left")
        df["size"] = df["weight"].fillna(0)
    else:
        st.warning("Index weights not available — using volume")
        df["size"] = df["volume"]
else:
    df["size"] = df["volume"]
```

3. Show index contribution:
   - Each box shows: symbol, change%, weight%, contribution to index
   - Contribution = weight × change% (how much this stock moved the index)

### Theme
- Positive contribution: green gradient
- Negative contribution: red gradient
- Box size = index weight (larger weight = larger box)

---

## Win 3: VAR Margins → Signal Scanner Factor

### What
Add VAR margin as a factor in the composite signal score. High/increasing VAR = 
exchange expects volatility = potential breakout signal.

### Data source
`/mnt/e/psxdata/downloads/daily/{date}/margins/var_margins*`

### Implementation

1. Create a loader:

```python
def load_var_margins(date_str: str = None) -> pd.DataFrame:
    """
    Load VAR margins per symbol.
    Returns DataFrame with columns: symbol, var_margin_pct, var_margin_pkr
    """
    ...
```

2. Find the signal scoring engine:

```bash
grep -rn "signal_score\|composite_score\|SignalScore" ~/pakfindata/src/pakfindata/engine/ 2>/dev/null
cat ~/pakfindata/src/pakfindata/engine/signal_score.py 2>/dev/null | head -50
```

3. Add VAR margin as a factor. In the scoring engine:

```python
# Existing factors (don't modify):
# - momentum, volume, volatility, VPIN, etc.

# NEW factor: VAR margin regime
def var_margin_score(symbol: str, date_str: str) -> float:
    """
    Score 0-100 based on VAR margin level and change.
    High VAR = exchange expects volatility = higher score.
    
    Logic:
    - Load today's VAR margin for symbol
    - Load yesterday's VAR margin (if available)
    - If VAR increased → score 70-100 (exchange raising risk, expect move)
    - If VAR stable → score 40-60
    - If VAR decreased → score 20-40 (calming down)
    - If no data → score 50 (neutral)
    """
    margins = load_var_margins(date_str)
    if margins.empty or symbol not in margins["symbol"].values:
        return 50.0  # neutral if no data
    
    current_var = margins.loc[margins["symbol"] == symbol, "var_margin_pct"].iloc[0]
    
    # Compare with previous day
    prev_date = get_previous_trading_day(date_str)
    prev_margins = load_var_margins(prev_date)
    
    if not prev_margins.empty and symbol in prev_margins["symbol"].values:
        prev_var = prev_margins.loc[prev_margins["symbol"] == symbol, "var_margin_pct"].iloc[0]
        change = current_var - prev_var
        
        if change > 2:    # VAR increased significantly
            return 85.0
        elif change > 0:  # VAR increased slightly
            return 70.0
        elif change == 0: # Stable
            return 50.0
        elif change > -2: # Decreased slightly
            return 35.0
        else:             # Decreased significantly
            return 20.0
    
    # No previous data — score based on absolute level
    # Typical VAR: 10-25% for most symbols
    if current_var > 25:
        return 75.0  # High VAR = high expected volatility
    elif current_var > 15:
        return 55.0  # Normal
    else:
        return 35.0  # Low VAR = calm
```

4. Add to composite score with weight:

```python
# In the composite scoring function, add:
weights = {
    # ... existing weights ...
    "var_margin": 0.08,  # 8% weight — supplementary factor
}

scores["var_margin"] = var_margin_score(symbol, date_str)
```

5. Show in the scanner results table:
   - New column: `VAR` — current VAR margin %
   - New column: `VAR Δ` — change from previous day
   - Color: red if VAR increased (exchange worried), green if decreased

---

## VERIFY

After implementing all three wins:

```bash
# Run the Streamlit app
cd ~/pakfindata
streamlit run src/pakfindata/ui/app.py

# Check:
# 1. Live Market page → see Upper/Lower columns, near-circuit highlights
# 2. Sector Heatmap → toggle "Size by Index Weight", see weighted boxes
# 3. Signal Scanner → see VAR column, run batch scan, check composite scores
```

## IMPORTANT

1. **Read existing page code FIRST** — understand current structure before adding
2. **Additive only** — don't break existing features
3. **Graceful fallback** — if downloaded files don't exist, show existing behavior (no crash)
4. **Cache loaders** — use `@st.cache_data(ttl=300)` for file reads
5. **File format may surprise you** — Step 1 reveals actual format. Adapt parsing accordingly.
6. **Store nothing in psx.sqlite** — read directly from downloaded files for now. DB integration comes later.
