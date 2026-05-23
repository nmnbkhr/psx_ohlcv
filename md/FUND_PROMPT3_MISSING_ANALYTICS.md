# Claude Code Prompt: Add Missing Fund Analytics

## Context

The analytics engine in pakfindata is ~85% complete. These specific metrics are missing:

- **Treynor Ratio** — not implemented anywhere
- **Calendar year returns** — not computed or stored
- **Rolling 1Y returns** (annualized) — not computed
- **Since-inception return** — not computed
- **5Y return** — analytics_mufap.py only does up to 1Y
- **2Y, 3Y returns** — exist in fund_performance (scraped) but not computed from NAV

## Files to modify

Read these first to understand the existing patterns:
- `src/pakfindata/engine/fund_risk.py` — existing risk metrics
- `src/pakfindata/engine/fund_factors.py` — existing factor analytics
- `src/pakfindata/analytics_mufap.py` — existing return/volatility calculations
- `src/pakfindata/ui/page_views/fund_explorer.py` — UI tabs

## Task 1: Add Treynor Ratio to fund_risk.py

```python
def compute_treynor_ratio(
    fund_returns: pd.Series,
    benchmark_returns: pd.Series,
    risk_free_rate: float = 0.15  # KIBOR ~15%
) -> float | None:
    """
    Treynor Ratio = (Fund Return - Risk Free Rate) / Beta
    
    Higher is better. Measures excess return per unit of systematic risk.
    Unlike Sharpe (which uses total risk/SD), Treynor uses only market risk (beta).
    """
    # Compute beta first (covariance method or reuse existing beta function)
    # If beta is 0 or negative, return None (undefined)
    # Annualize fund return, subtract risk-free rate, divide by beta
```

Add this to the `generate_fund_analytics()` function output so it's included 
in the full analytics bundle.

## Task 2: Add extended return periods to analytics_mufap.py

Currently `analytics_mufap.py` computes: 1W, 1M, 3M, 6M, 1Y.

Add these periods:
```python
RETURN_PERIODS = {
    "1W": 5,      # existing
    "1M": 21,     # existing  
    "3M": 63,     # existing
    "6M": 126,    # existing
    "1Y": 252,    # existing
    "2Y": 504,    # NEW
    "3Y": 756,    # NEW
    "5Y": 1260,   # NEW
    "YTD": None,  # NEW — compute from Jan 1 of current year
    "Since Inception": None,  # NEW — compute from first available NAV
}
```

For YTD:
```python
# Find NAV on last trading day of previous year (or first of current year)
# Return = (current_nav / jan1_nav - 1) * 100
```

For Since Inception:
```python
# Return = (current_nav / first_nav - 1) * 100
# Also compute annualized: ((current/first) ^ (252/trading_days) - 1) * 100
```

## Task 3: Add Calendar Year Returns

Create a function in `analytics_mufap.py` or a new file:

```python
def compute_calendar_year_returns(
    fund_id: str, 
    con: sqlite3.Connection
) -> dict[int, float]:
    """
    Compute annual return for each calendar year the fund was active.
    
    Returns: {2020: 12.5, 2021: -3.2, 2022: 8.7, ...}
    
    For each year:
      - Get first NAV of year (or last NAV of previous year)
      - Get last NAV of year
      - Return = (last/first - 1) * 100
      - Skip years with < 20 trading days of data
    """
```

Also create a DB table to store pre-computed calendar returns:
```sql
CREATE TABLE IF NOT EXISTS fund_calendar_returns (
    fund_id TEXT,
    year INTEGER,
    return_pct REAL,
    first_nav REAL,
    last_nav REAL,
    trading_days INTEGER,
    computed_at TEXT,
    PRIMARY KEY (fund_id, year)
);
```

## Task 4: Add Rolling 1Y Returns

```python
def compute_rolling_returns(
    navs: pd.Series,  # DatetimeIndex → NAV values
    window: int = 252  # 1Y
) -> pd.Series:
    """
    Rolling annualized return over trailing window.
    
    For each date t:
      rolling_return[t] = (nav[t] / nav[t - window] - 1) * 100
    
    Returns a Series with same index, NaN for first `window` entries.
    Useful for charting "rolling 1Y return over time".
    """
```

## Task 5: Wire into Fund Explorer UI

### Risk Analytics tab — add Treynor Ratio
Find the Risk Analytics tab in fund_explorer.py. It already shows:
Sharpe, Sortino, Max Drawdown, Beta, Volatility, Alpha, Info Ratio.

Add Treynor Ratio to this display. Show it alongside Sharpe:
```
Sharpe Ratio: 1.23    |    Treynor Ratio: 0.85    |    Sortino Ratio: 1.67
```

### Top Performers tab — add extended return periods
If this tab shows rankings, add 2Y, 3Y, 5Y, YTD, Since Inception columns.
These can come from fund_performance table (already scraped) OR computed from NAV.

### New: Calendar Year Returns section
Add to the fund detail view (when user selects a specific fund):
- Table showing year | return% for each calendar year
- Color: green for positive, red for negative
- Optional: horizontal bar chart or heatmap

### Compare Funds tab — add rolling return chart
When comparing 2+ funds, add a "Rolling 1Y Return" chart option 
alongside the existing NAV overlay chart.

## VERIFY

```bash
# Test Treynor
python3 -c "
from pakfindata.engine.fund_risk import compute_treynor_ratio
import pandas as pd
import numpy as np
np.random.seed(42)
fund = pd.Series(np.random.normal(0.0005, 0.01, 252))
bench = pd.Series(np.random.normal(0.0003, 0.008, 252))
result = compute_treynor_ratio(fund, bench, risk_free_rate=0.15)
print(f'Treynor Ratio: {result}')
"

# Test calendar returns
python3 -c "
import sqlite3
from pakfindata.analytics_mufap import compute_calendar_year_returns
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
result = compute_calendar_year_returns('12768', con)
for year, ret in sorted(result.items()):
    print(f'  {year}: {ret:+.2f}%')
con.close()
"

# Test extended returns
python3 -c "
import sqlite3
from pakfindata.analytics_mufap import compute_fund_returns
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
result = compute_fund_returns('12768', con)
for period, ret in result.items():
    print(f'  {period}: {ret:+.2f}%' if ret else f'  {period}: N/A')
con.close()
"

# Check calendar returns table
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
n = con.execute('SELECT COUNT(*) FROM fund_calendar_returns').fetchone()[0]
print(f'fund_calendar_returns: {n} rows')
con.close()
"
```
