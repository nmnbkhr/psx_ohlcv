# Claude Code Prompt: Populate fund_risk_metrics — Batch Compute & Store

## Context

The `fund_risk_metrics` table exists in `/mnt/e/psxdata/psx.sqlite` with 0 rows.
The schema is already defined in `src/pakfindata/db/schema.py`.
The analytics engine already computes these metrics on-the-fly in:
- `src/pakfindata/engine/fund_risk.py` (Sharpe, Sortino, drawdown, VaR, Beta, Alpha, Info Ratio, capture ratios)
- `src/pakfindata/engine/fund_factors.py` (rolling vol, CAPM regression, peer rank)
- `src/pakfindata/analytics_mufap.py` (returns, volatility, Sharpe, drawdown)

The NAV history table `mutual_fund_nav` has 1,923,323 rows across 1,204 funds (1996→2026).

## Task

Build a batch job that computes risk metrics for ALL funds and stores results in `fund_risk_metrics`.

### Step 1: Inspect the existing schema

```bash
# Check fund_risk_metrics table schema
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
cols = con.execute('PRAGMA table_info(fund_risk_metrics)').fetchall()
print('fund_risk_metrics columns:')
for c in cols:
    print(f'  {c[1]} ({c[2]})')
con.close()
"
```

Also read:
- `src/pakfindata/db/schema.py` — find fund_risk_metrics CREATE TABLE
- `src/pakfindata/engine/fund_risk.py` — find generate_fund_analytics()
- `src/pakfindata/engine/fund_factors.py` — find available functions
- `src/pakfindata/analytics_mufap.py` — find compute functions

### Step 2: Build the batch compute job

Create `src/pakfindata/engine/compute_risk_batch.py`:

```python
"""
Batch compute risk metrics for ALL mutual funds and store in fund_risk_metrics table.

Usage:
    python -m pakfindata.engine.compute_risk_batch
    python -m pakfindata.engine.compute_risk_batch --fund-id 12768
    python -m pakfindata.engine.compute_risk_batch --since 2025-01-01
"""
```

This script should:

1. **Query all funds** from `mutual_funds` table (or a specific fund via `--fund-id`)
2. **For each fund**, pull NAV history from `mutual_fund_nav`
3. **Skip funds** with < 30 NAV records (not enough data for meaningful metrics)
4. **Compute these metrics** using the EXISTING engine functions (don't rewrite — import and call):

   From `fund_risk.py`:
   - Sharpe Ratio (use KIBOR as risk-free rate — check what analytics_mufap.py uses, likely 15%)
   - Sortino Ratio
   - Max Drawdown (value + start/end dates)
   - Beta (vs KSE-100 benchmark — use engine/benchmark.py)
   - Alpha (Jensen's)
   - Value at Risk (VaR 95%)
   - CVaR (Conditional VaR)
   - Information Ratio
   - Up Capture Ratio
   - Down Capture Ratio

   From `fund_factors.py`:
   - Volatility (annualized, 1Y window)
   - R-squared (from CAPM regression)

   Compute directly (simple calculations):
   - Returns: 1M, 3M, 6M, 1Y, 2Y, 3Y, 5Y, YTD, Since Inception
   - Rolling 1Y return (annualized)

5. **Store results** in `fund_risk_metrics` table via INSERT OR REPLACE
6. **Add computed_at timestamp** to each row

### Step 3: Handle the schema

Check the existing `fund_risk_metrics` schema. If it doesn't have all the columns 
needed for the metrics above, ALTER TABLE to add missing columns. Do NOT drop 
the table — use ALTER TABLE ADD COLUMN for any missing columns.

Minimum columns needed:
```
fund_id TEXT PRIMARY KEY,
fund_name TEXT,
category TEXT,
computed_at TEXT,
-- Returns
return_1m REAL, return_3m REAL, return_6m REAL, return_1y REAL,
return_2y REAL, return_3y REAL, return_5y REAL, return_ytd REAL,
return_since_inception REAL,
-- Risk metrics
volatility_1y REAL,
sharpe_ratio REAL,
sortino_ratio REAL,
max_drawdown REAL,
max_drawdown_start TEXT,
max_drawdown_end TEXT,
beta REAL,
alpha REAL,
r_squared REAL,
var_95 REAL,
cvar_95 REAL,
information_ratio REAL,
up_capture REAL,
down_capture REAL,
-- Metadata
nav_count INTEGER,
first_nav_date TEXT,
last_nav_date TEXT
```

### Step 4: Performance considerations

- 1,204 funds × NAV lookups = could be slow
- Use batch query: pull ALL nav data sorted by fund_id+date in one query, then iterate
- Print progress: `[142/1204] ABL Cash Fund — Sharpe: 1.23, Beta: 0.15, MaxDD: -3.2%`
- Use try/except per fund — one fund failing shouldn't stop the batch
- Print summary at end: `✅ Computed metrics for 1,047 funds (157 skipped — insufficient data)`

### Step 5: Integration

- Add CLI entry point so it can be run as: `python -m pakfindata.engine.compute_risk_batch`
- Add `--fund-id` flag for single fund recompute
- Add `--since` flag to only recompute funds with new NAV data since date
- Add a "Recompute Risk Metrics" button in the Fund Explorer Sync tab 
  (check `fund_explorer.py` Sync & Tools tab — add a button that calls the batch)

### Step 6: Wire into Fund Explorer UI

The Risk Analytics tab in fund_explorer.py currently computes on-the-fly.
After this batch job populates the table, add an option to:
- Read pre-computed metrics from `fund_risk_metrics` for fast page loads
- Fall back to on-the-fly computation if metrics are stale (> 7 days old)
- Show "Last computed: 2026-03-15" in the UI

## VERIFY

```bash
# Run batch for a single fund first
python -m pakfindata.engine.compute_risk_batch --fund-id 12768

# Check it stored
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
row = con.execute('SELECT * FROM fund_risk_metrics WHERE fund_id=\"12768\"').fetchone()
cols = [c[1] for c in con.execute('PRAGMA table_info(fund_risk_metrics)').fetchall()]
if row:
    for c, v in zip(cols, row):
        print(f'  {c}: {v}')
else:
    print('NO DATA')
con.close()
"

# Run full batch
python -m pakfindata.engine.compute_risk_batch

# Check total
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
n = con.execute('SELECT COUNT(*) FROM fund_risk_metrics').fetchone()[0]
print(f'fund_risk_metrics: {n} rows')
con.close()
"
```
