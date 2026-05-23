# Claude Code Prompt: Strategy 7 — Sector Rotation Momentum

## Context

pakfindata already has sector breadth, 18 PSX sector indices, 5 years of EOD data, and 
KSE-100 index weights per sector. This strategy exploits Pakistan's pronounced sector 
rotation cycle: banking → cement → pharma → energy → textiles, driven by rate cycles, 
commodity prices, and government policy.

**The strategy:** Each month, rank all PSX sectors by 1-month momentum. Go long the top 3 
sectors, short (or underweight) the bottom 3. Rebalance monthly. Simple, but highly effective 
in Pakistan because sector cycles are persistent (3-6 months) and driven by macro fundamentals.

**Why it works on PSX:**
- SBP rate cuts → banks rally first (NIM expansion expectations)
- Construction stimulus → cement follows (PSDP spending)
- PKR stability → pharma re-rates (import-dependent costs)
- Oil price drop → E&P falls, refineries rally (input cost)
- These rotations take MONTHS, not days — plenty of time to position

## What already exists

```bash
# Find existing sector/breadth code
grep -rn "sector\|breadth\|index.*weight\|KSE.*100\|kse100" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check sector index data in DuckDB
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# List all tables
for t in con.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=\"main\"').fetchall():
    tl = t[0].lower()
    if any(k in tl for k in ['index','sector','breadth','weight','constituent']):
        count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
        print(f'{t[0]}: {count:,}')

# Check if sector indices exist in eod_ohlcv
sectors = con.execute('''
    SELECT DISTINCT symbol FROM eod_ohlcv 
    WHERE symbol LIKE \"%IDX%\" OR symbol LIKE \"%INDEX%\" OR symbol LIKE \"KSE%\"
    OR symbol LIKE \"%ALL%\" OR symbol LIKE \"%SECT%\"
    ORDER BY symbol
''').fetchall()
print(f'\nIndex-like symbols: {[s[0] for s in sectors]}')
con.close()
"

# Check psx.sqlite for sector data
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
tables = [r[0] for r in con.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]
for t in tables:
    tl = t.lower()
    if any(k in tl for k in ['sector','index','breadth','weight','constituent']):
        count = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[1] for r in con.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'{t}: {count:,} rows — {cols[:8]}')
con.close()
"

# Find the 18 sector indices — what are they called?
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# Try to find all unique sectors
for q in [
    'SELECT DISTINCT sector FROM eod_ohlcv WHERE sector IS NOT NULL LIMIT 30',
    'SELECT DISTINCT sector FROM sector_summary LIMIT 30',
    'SELECT DISTINCT index_name FROM index_ohlcv LIMIT 30',
]:
    try:
        r = con.execute(q).fetchall()
        print(f'{q[:50]}: {[x[0] for x in r]}')
    except: pass
con.close()
"
```

**READ ALL OUTPUT — identify exact table names, column names, and sector list before proceeding.**

## Step 1: Create the Sector Rotation Engine

Create `src/pakfindata/engine/sector_rotation.py`:

```python
"""
Sector Rotation Momentum Strategy.

Monthly rebalance: long top 3 sectors by 1M momentum, short bottom 3.
Exploits Pakistan's pronounced macro-driven sector cycles.

PSX Sector Indices (18):
  Automobile, Banking, Cement, Chemical, Commercial Banks, Engineering,
  Fertilizer, Food, Glass & Ceramics, Insurance, Inv Banks/Securities,
  Oil & Gas Exploration, Oil & Gas Marketing, Paper, Pharma, Power,
  Refinery, Technology, Textile (and more)

Key rotations:
  Rate cuts → Banking (NIM), Cement (construction), Auto (consumer credit)
  PKR stable → Pharma (import costs), Tech (dollar revenues neutral)
  Oil down → E&P falls, Refineries rally, Power costs improve
  Fiscal stimulus → Cement, Steel, Engineering

PSX-Specific:
  - 245 trading days/year
  - Sector data from PSX sector indices or computed from constituent stocks
  - Index weights from PSX constituent_data XLS files
  - Monthly rebalance = ~20 trading days
"""

import numpy as np
import pandas as pd
import duckdb
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
TRADING_DAYS = 245

# PSX sector names (canonical)
PSX_SECTORS = [
    "Automobile Assembler", "Automobile Parts", "Banking", "Cement",
    "Chemical", "Close-End Mutual Fund", "Commercial Banks",
    "Engineering", "Fertilizer", "Food & Personal Care",
    "Glass & Ceramics", "Insurance", "Inv. Banks / Inv. Cos. / Securities",
    "Jute", "Leather & Tanneries", "Miscellaneous",
    "Modaraba", "Oil & Gas Exploration", "Oil & Gas Marketing",
    "Paper & Board", "Pharmaceuticals", "Power Generation",
    "Property", "Refinery", "Sugar",
    "Synthetic & Rayon", "Technology & Communication", "Textile Composite",
    "Textile Spinning", "Textile Weaving", "Tobacco",
    "Transport", "Vanaspati & Allied", "Woollen",
]


@dataclass
class SectorScore:
    sector: str
    momentum_1m: float       # 1-month return
    momentum_3m: float       # 3-month return
    momentum_6m: float       # 6-month return (for confirmation)
    breadth: float           # % of stocks in sector that are positive
    volume_change: float     # volume vs 20-day average
    rank: int                # rank among all sectors (1 = best)
    signal: str              # "LONG", "SHORT", "NEUTRAL"
    weight: float            # allocation weight (0 to 1)
    stocks_count: int        # number of stocks in sector
    top_stocks: list         # top 3 contributing stocks


@dataclass
class RotationSignal:
    date: str
    long_sectors: list[SectorScore]    # top 3
    short_sectors: list[SectorScore]   # bottom 3
    neutral_sectors: list[SectorScore] # middle
    regime_note: str                   # macro context


def load_sector_data(lookback_months: int = 24) -> pd.DataFrame:
    """
    Load sector-level return data. Tries multiple approaches:
    1. Direct sector index prices (if available)
    2. Computed from constituent stock returns
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=lookback_months * 30)).strftime("%Y-%m-%d")
    
    # ── Approach 1: Sector index table ──
    sector_df = pd.DataFrame()
    for query in [
        f"SELECT * FROM sector_index WHERE date >= '{cutoff}' ORDER BY date",
        f"SELECT * FROM index_ohlcv WHERE date >= '{cutoff}' ORDER BY date",
        f"SELECT * FROM sector_ohlcv WHERE date >= '{cutoff}' ORDER BY date",
    ]:
        try:
            sector_df = con.execute(query).df()
            if not sector_df.empty:
                break
        except:
            continue
    
    # ── Approach 2: Compute from stock-level data with sector mapping ──
    if sector_df.empty:
        # Get sector mapping
        sector_map = pd.DataFrame()
        for query in [
            "SELECT symbol, sector FROM stock_sectors",
            "SELECT symbol, sector FROM company_info WHERE sector IS NOT NULL",
            "SELECT symbol, sector FROM eod_ohlcv WHERE sector IS NOT NULL GROUP BY symbol, sector",
        ]:
            try:
                sector_map = con.execute(query).df()
                if not sector_map.empty:
                    break
            except:
                continue
        
        if sector_map.empty:
            # Try psx.sqlite
            try:
                scon = sqlite3.connect(str(PSX_SQLITE))
                for t in ['sectors', 'stock_sectors', 'company_info', 'listed_companies']:
                    try:
                        sector_map = pd.read_sql(f"SELECT symbol, sector FROM {t} WHERE sector IS NOT NULL", scon)
                        if not sector_map.empty:
                            break
                    except:
                        continue
                scon.close()
            except:
                pass
        
        # Get EOD data
        eod = con.execute(f"""
            SELECT date, symbol, close, volume FROM eod_ohlcv
            WHERE date >= '{cutoff}' ORDER BY date, symbol
        """).df()
        
        if not eod.empty and not sector_map.empty:
            # Merge sector info
            eod = eod.merge(sector_map[["symbol", "sector"]], on="symbol", how="left")
            eod = eod.dropna(subset=["sector"])
            
            # Compute equal-weighted sector returns per day
            eod["return"] = eod.groupby("symbol")["close"].pct_change()
            
            sector_df = eod.groupby(["date", "sector"]).agg(
                sector_return=("return", "mean"),
                sector_volume=("volume", "sum"),
                stock_count=("symbol", "nunique"),
                advancing=("return", lambda x: (x > 0).sum()),
                declining=("return", lambda x: (x < 0).sum()),
            ).reset_index()
            
            # Compute sector "close" as cumulative return index (base 1000)
            sector_df = sector_df.sort_values(["sector", "date"])
            sector_df["cum_return"] = sector_df.groupby("sector")["sector_return"].transform(
                lambda x: (1 + x.fillna(0)).cumprod() * 1000
            )
            sector_df = sector_df.rename(columns={"cum_return": "close"})
    
    con.close()
    return sector_df


def compute_sector_momentum(
    sector_df: pd.DataFrame,
    as_of_date: str = None,
) -> list[SectorScore]:
    """
    Compute momentum scores for all sectors as of a given date.
    
    Returns list of SectorScore sorted by momentum (best first).
    """
    if sector_df.empty:
        return []
    
    # Normalize column names
    cols_lower = {c.lower(): c for c in sector_df.columns}
    date_col = next((cols_lower[k] for k in ['date','trade_date'] if k in cols_lower), 'date')
    sector_col = next((cols_lower[k] for k in ['sector','index_name','name'] if k in cols_lower), 'sector')
    close_col = next((cols_lower[k] for k in ['close','value','level'] if k in cols_lower), 'close')
    volume_col = next((cols_lower[k] for k in ['sector_volume','volume','turnover'] if k in cols_lower), None)
    
    sector_df[date_col] = pd.to_datetime(sector_df[date_col])
    
    if as_of_date:
        as_of = pd.to_datetime(as_of_date)
    else:
        as_of = sector_df[date_col].max()
    
    scores = []
    sectors = sector_df[sector_col].unique()
    
    for sector in sectors:
        sdf = sector_df[sector_df[sector_col] == sector].sort_values(date_col)
        sdf = sdf[sdf[date_col] <= as_of]
        
        if len(sdf) < 22:  # need at least 1 month
            continue
        
        latest_close = sdf[close_col].iloc[-1]
        
        # 1-month momentum (last ~22 trading days)
        idx_1m = max(0, len(sdf) - 22)
        mom_1m = (latest_close / sdf[close_col].iloc[idx_1m] - 1) if sdf[close_col].iloc[idx_1m] > 0 else 0
        
        # 3-month momentum
        idx_3m = max(0, len(sdf) - 66)
        mom_3m = (latest_close / sdf[close_col].iloc[idx_3m] - 1) if sdf[close_col].iloc[idx_3m] > 0 else 0
        
        # 6-month momentum
        idx_6m = max(0, len(sdf) - 132)
        mom_6m = (latest_close / sdf[close_col].iloc[idx_6m] - 1) if sdf[close_col].iloc[idx_6m] > 0 else 0
        
        # Breadth (% positive over last month)
        if "advancing" in sdf.columns and "declining" in sdf.columns:
            recent = sdf.iloc[-22:]
            total_adv = recent["advancing"].sum()
            total_dec = recent["declining"].sum()
            breadth = total_adv / (total_adv + total_dec) if (total_adv + total_dec) > 0 else 0.5
        elif "sector_return" in sdf.columns:
            breadth = (sdf["sector_return"].iloc[-22:] > 0).mean()
        else:
            breadth = 0.5
        
        # Volume change vs 20-day average
        vol_change = 0
        if volume_col and volume_col in sdf.columns:
            recent_vol = sdf[volume_col].iloc[-5:].mean()
            avg_vol = sdf[volume_col].iloc[-25:-5].mean()
            vol_change = (recent_vol / avg_vol - 1) if avg_vol > 0 else 0
        
        # Stock count
        stock_count = int(sdf["stock_count"].iloc[-1]) if "stock_count" in sdf.columns else 0
        
        scores.append(SectorScore(
            sector=sector,
            momentum_1m=mom_1m,
            momentum_3m=mom_3m,
            momentum_6m=mom_6m,
            breadth=breadth,
            volume_change=vol_change,
            rank=0,
            signal="",
            weight=0,
            stocks_count=stock_count,
            top_stocks=[],
        ))
    
    # Rank by composite score: 60% 1M momentum + 30% 3M + 10% breadth
    for s in scores:
        s._composite = s.momentum_1m * 0.6 + s.momentum_3m * 0.3 + (s.breadth - 0.5) * 0.1
    
    scores.sort(key=lambda x: x._composite, reverse=True)
    
    for i, s in enumerate(scores):
        s.rank = i + 1
        if i < 3:
            s.signal = "LONG"
            s.weight = 1.0 / 3  # equal weight top 3
        elif i >= len(scores) - 3:
            s.signal = "SHORT"
            s.weight = -1.0 / 3
        else:
            s.signal = "NEUTRAL"
            s.weight = 0
    
    return scores


def get_sector_top_stocks(sector: str, n: int = 5) -> list[dict]:
    """Get top stocks by weight/volume in a sector."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    stocks = []
    # Try to find stocks in this sector
    for query in [
        f"""SELECT symbol, close, volume FROM eod_ohlcv 
            WHERE sector = '{sector}' AND date = (SELECT MAX(date) FROM eod_ohlcv WHERE sector = '{sector}')
            ORDER BY volume DESC LIMIT {n}""",
    ]:
        try:
            df = con.execute(query).df()
            if not df.empty:
                stocks = df.to_dict("records")
                break
        except:
            continue
    
    con.close()
    return stocks


def generate_rotation_signal(
    sector_df: pd.DataFrame,
    as_of_date: str = None,
    top_n: int = 3,
    bottom_n: int = 3,
) -> RotationSignal:
    """
    Generate sector rotation signal: long top N, short bottom N.
    """
    scores = compute_sector_momentum(sector_df, as_of_date)
    
    if not scores:
        return None
    
    long_sectors = [s for s in scores if s.signal == "LONG"][:top_n]
    short_sectors = [s for s in scores if s.signal == "SHORT"][:bottom_n]
    neutral_sectors = [s for s in scores if s.signal == "NEUTRAL"]
    
    # Add top stocks
    for s in long_sectors + short_sectors:
        s.top_stocks = get_sector_top_stocks(s.sector)
    
    # Macro context note
    notes = []
    if long_sectors:
        top = long_sectors[0]
        if "bank" in top.sector.lower():
            notes.append("Banking leading — typical rate-cut rally")
        elif "cement" in top.sector.lower():
            notes.append("Cement leading — construction/PSDP stimulus cycle")
        elif "pharma" in top.sector.lower():
            notes.append("Pharma leading — PKR stability benefiting importers")
        elif "oil" in top.sector.lower() and "explor" in top.sector.lower():
            notes.append("E&P leading — oil price rally or OGDC/PPL re-rating")
        elif "tech" in top.sector.lower():
            notes.append("Tech leading — IT exports growth narrative")
        elif "power" in top.sector.lower():
            notes.append("Power leading — capacity payment flows or circular debt resolution")
        elif "fertil" in top.sector.lower():
            notes.append("Fertilizer leading — urea price hike or subsidy announcement")
    
    regime_note = "; ".join(notes) if notes else "No dominant macro theme detected"
    
    return RotationSignal(
        date=as_of_date or str(datetime.now(PKT).date()),
        long_sectors=long_sectors,
        short_sectors=short_sectors,
        neutral_sectors=neutral_sectors,
        regime_note=regime_note,
    )


def backtest_sector_rotation(
    lookback_months: int = 60,
    top_n: int = 3,
    bottom_n: int = 3,
    rebalance_days: int = 22,    # ~monthly
    long_only: bool = False,     # True = long top N only (no shorts)
) -> dict:
    """
    Backtest sector rotation strategy.
    
    Monthly:
      1. Rank sectors by 1M momentum
      2. Long top N, short bottom N (or long-only)
      3. Equal weight within long/short baskets
      4. Rebalance next month
    
    Benchmark: equal-weight all sectors (market portfolio)
    """
    sector_df = load_sector_data(lookback_months)
    
    if sector_df.empty:
        return {"error": "No sector data available"}
    
    # Normalize columns
    cols_lower = {c.lower(): c for c in sector_df.columns}
    date_col = next((cols_lower[k] for k in ['date','trade_date'] if k in cols_lower), 'date')
    sector_col = next((cols_lower[k] for k in ['sector','index_name','name'] if k in cols_lower), 'sector')
    
    sector_df[date_col] = pd.to_datetime(sector_df[date_col])
    dates = sorted(sector_df[date_col].unique())
    
    if len(dates) < rebalance_days * 3:
        return {"error": f"Only {len(dates)} dates — need at least {rebalance_days * 3}"}
    
    # Get daily sector returns
    return_col = "sector_return" if "sector_return" in sector_df.columns else None
    if return_col is None:
        close_col = next((cols_lower[k] for k in ['close','value','level'] if k in cols_lower), 'close')
        sector_df = sector_df.sort_values([sector_col, date_col])
        sector_df["sector_return"] = sector_df.groupby(sector_col)[close_col].pct_change()
        return_col = "sector_return"
    
    # Pivot: dates × sectors
    pivot = sector_df.pivot_table(
        index=date_col, columns=sector_col, values=return_col
    ).fillna(0)
    
    # Rebalance dates (every N trading days)
    rebal_indices = list(range(rebalance_days * 3, len(pivot), rebalance_days))
    
    strategy_returns = []
    benchmark_returns = []
    holdings_history = []
    trade_log = []
    
    current_longs = []
    current_shorts = []
    
    for i in range(len(pivot)):
        date = pivot.index[i]
        daily_returns = pivot.iloc[i]
        
        # Rebalance?
        if i in rebal_indices:
            as_of = str(date)[:10]
            scores = compute_sector_momentum(sector_df, as_of)
            
            if scores:
                old_longs = current_longs.copy()
                old_shorts = current_shorts.copy()
                
                current_longs = [s.sector for s in scores if s.signal == "LONG"][:top_n]
                current_shorts = [s.sector for s in scores if s.signal == "SHORT"][:bottom_n]
                
                # Log trades
                new_longs = set(current_longs) - set(old_longs)
                new_shorts = set(current_shorts) - set(old_shorts)
                if new_longs or new_shorts:
                    trade_log.append({
                        "date": as_of,
                        "action": "REBALANCE",
                        "long": current_longs,
                        "short": current_shorts,
                        "new_long": list(new_longs),
                        "new_short": list(new_shorts),
                        "top_momentum": scores[0].momentum_1m if scores else 0,
                        "bottom_momentum": scores[-1].momentum_1m if scores else 0,
                    })
                
                holdings_history.append({
                    "date": as_of,
                    "longs": current_longs.copy(),
                    "shorts": current_shorts.copy(),
                    "scores": {s.sector: s.momentum_1m for s in scores},
                })
        
        # Compute strategy return
        if current_longs:
            long_ret = daily_returns.reindex(current_longs).mean()
        else:
            long_ret = 0
        
        if current_shorts and not long_only:
            short_ret = -daily_returns.reindex(current_shorts).mean()  # negative = profit when shorts fall
        else:
            short_ret = 0
        
        if long_only:
            strat_ret = long_ret
        else:
            strat_ret = (long_ret + short_ret) / 2  # 50% long, 50% short
        
        # Benchmark: equal-weight all sectors
        bench_ret = daily_returns.mean()
        
        strategy_returns.append({"date": date, "return": strat_ret})
        benchmark_returns.append({"date": date, "return": bench_ret})
    
    # Compute metrics
    strat_df = pd.DataFrame(strategy_returns)
    bench_df = pd.DataFrame(benchmark_returns)
    
    strat_cum = (1 + strat_df["return"]).cumprod()
    bench_cum = (1 + bench_df["return"]).cumprod()
    
    strat_total = strat_cum.iloc[-1] - 1
    bench_total = bench_cum.iloc[-1] - 1
    
    strat_ann = (1 + strat_total) ** (TRADING_DAYS / len(strat_df)) - 1
    bench_ann = (1 + bench_total) ** (TRADING_DAYS / len(bench_df)) - 1
    
    strat_vol = strat_df["return"].std() * np.sqrt(TRADING_DAYS)
    bench_vol = bench_df["return"].std() * np.sqrt(TRADING_DAYS)
    
    strat_sharpe = strat_ann / strat_vol if strat_vol > 0 else 0
    bench_sharpe = bench_ann / bench_vol if bench_vol > 0 else 0
    
    # Max drawdown
    def max_dd(cum):
        peak = cum.cummax()
        return ((cum - peak) / peak).min()
    
    strat_dd = max_dd(strat_cum)
    bench_dd = max_dd(bench_cum)
    
    # Win rate by rebalance period
    period_returns = []
    for j in range(1, len(rebal_indices)):
        start = rebal_indices[j-1]
        end = rebal_indices[j]
        period_ret = strat_df["return"].iloc[start:end].sum()
        bench_period = bench_df["return"].iloc[start:end].sum()
        period_returns.append({
            "period_start": str(strat_df.iloc[start]["date"])[:10],
            "strategy": period_ret,
            "benchmark": bench_period,
            "outperformed": period_ret > bench_period,
        })
    
    periods_df = pd.DataFrame(period_returns)
    win_rate = periods_df["outperformed"].mean() if len(periods_df) > 0 else 0
    
    # Sector frequency in long/short baskets
    long_freq = {}
    short_freq = {}
    for h in holdings_history:
        for s in h["longs"]:
            long_freq[s] = long_freq.get(s, 0) + 1
        for s in h["shorts"]:
            short_freq[s] = short_freq.get(s, 0) + 1
    
    return {
        "equity_curve": pd.DataFrame({
            "date": strat_df["date"],
            "strategy": strat_cum.values,
            "benchmark": bench_cum.values,
        }),
        "period_returns": periods_df,
        "trade_log": pd.DataFrame(trade_log) if trade_log else pd.DataFrame(),
        "holdings_history": holdings_history,
        "metrics": {
            "strategy_return": float(strat_total),
            "benchmark_return": float(bench_total),
            "alpha": float(strat_total - bench_total),
            "strategy_ann_return": float(strat_ann),
            "benchmark_ann_return": float(bench_ann),
            "strategy_volatility": float(strat_vol),
            "benchmark_volatility": float(bench_vol),
            "strategy_sharpe": float(strat_sharpe),
            "benchmark_sharpe": float(bench_sharpe),
            "strategy_maxdd": float(strat_dd),
            "benchmark_maxdd": float(bench_dd),
            "rebalance_periods": len(rebal_indices),
            "outperformance_rate": float(win_rate),
            "long_only": long_only,
            "sectors_analyzed": len(pivot.columns),
        },
        "sector_frequency": {
            "most_longed": sorted(long_freq.items(), key=lambda x: -x[1])[:5],
            "most_shorted": sorted(short_freq.items(), key=lambda x: -x[1])[:5],
        },
    }


def rotation_heatmap_data(lookback_months: int = 12) -> pd.DataFrame:
    """
    Build sector × month return heatmap for visualization.
    """
    sector_df = load_sector_data(lookback_months)
    if sector_df.empty:
        return pd.DataFrame()
    
    cols_lower = {c.lower(): c for c in sector_df.columns}
    date_col = next((cols_lower[k] for k in ['date','trade_date'] if k in cols_lower), 'date')
    sector_col = next((cols_lower[k] for k in ['sector','index_name','name'] if k in cols_lower), 'sector')
    close_col = next((cols_lower[k] for k in ['close','value','level'] if k in cols_lower), 'close')
    
    sector_df[date_col] = pd.to_datetime(sector_df[date_col])
    sector_df["month"] = sector_df[date_col].dt.to_period("M")
    
    # Monthly returns per sector
    monthly = sector_df.groupby([sector_col, "month"]).agg(
        first_close=(close_col, "first"),
        last_close=(close_col, "last"),
    ).reset_index()
    
    monthly["monthly_return"] = monthly["last_close"] / monthly["first_close"] - 1
    
    heatmap = monthly.pivot_table(
        index=sector_col, columns="month", values="monthly_return"
    )
    
    return heatmap
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_sector.py`:

### Tab 1: Current Rotation Signal
```
├── Sector momentum ranking table:
│   Rank | Sector | 1M% | 3M% | 6M% | Breadth | Volume Chg | Signal | Weight
│   Color: green rows = LONG (top 3), red = SHORT (bottom 3), gray = NEUTRAL
├── Allocation summary: "LONG: Banking, Cement, Pharma | SHORT: Textile, Jute, Glass"
├── Macro context card: "Banking leading — typical rate-cut rally"
├── Bar chart: all sectors sorted by 1M momentum (horizontal bars, green/red)
├── Breadth chart: % of stocks positive per sector (stacked or grouped)
└── Top stocks per selected sector (expandable rows)
```

### Tab 2: Rotation Heatmap
```
├── Sector × Month heatmap (green = positive, red = negative)
│   Rows: sectors, Columns: months (last 12-24)
│   Color intensity = magnitude
├── Sector correlation matrix heatmap
│   (which sectors move together? which are diversifiers?)
├── Rotation arrow diagram:
│   Visual showing money flow from losing → winning sectors
├── Momentum persistence chart: does last month's winner win next month?
└── Sector cycle timeline: annotated chart of major rotations
```

### Tab 3: Backtest
```
├── Parameters:
│   ├── Lookback: 2Y / 3Y / 5Y
│   ├── Top N sectors: slider 2-5 (default 3)
│   ├── Bottom N sectors: slider 2-5 (default 3)
│   ├── Rebalance frequency: Monthly / Bi-monthly / Quarterly
│   ├── Long-only toggle (no shorts)
│   └── [Run Backtest]
├── Equity curve: Strategy vs Equal-Weight Benchmark
├── Metrics: Return, Alpha, Sharpe, MaxDD, Outperformance Rate
├── Monthly alpha heatmap (months × years)
├── Period-by-period returns table (each rebalance period)
├── Sector frequency: which sectors appear most in long/short baskets
├── Trade log: every rebalance with sectors entered/exited
└── Rolling 12M alpha chart
```

### Tab 4: Sector Research
```
├── Individual sector analysis (dropdown):
│   ├── Price chart with KSE-100 overlay (relative strength)
│   ├── Momentum vs breadth scatter for all sectors
│   ├── Sector vs KIBOR correlation
│   ├── Sector vs PKR correlation
│   ├── Sector vs Oil correlation
│   └── Best/worst months for this sector
├── Pakistan rotation playbook:
│   ├── Rate cut cycle → Banking → Cement → Auto
│   ├── PKR stability → Pharma → Tech
│   ├── Oil crash → E&P down, Refinery up, Power up
│   ├── Fiscal expansion → Cement → Engineering → Steel
│   └── Election year → broad rally, then correction
└── Methodology explanation
```

### Key chart — Horizontal momentum bars:
```python
import plotly.graph_objects as go

# Sort by momentum
sorted_scores = sorted(scores, key=lambda s: s.momentum_1m)

colors = []
for s in sorted_scores:
    if s.signal == "LONG": colors.append("#22C55E")
    elif s.signal == "SHORT": colors.append("#EF4444")
    else: colors.append("#6B7280")

fig = go.Figure(go.Bar(
    x=[s.momentum_1m * 100 for s in sorted_scores],
    y=[s.sector for s in sorted_scores],
    orientation="h",
    marker_color=colors,
    text=[f"{s.momentum_1m:+.1%}" for s in sorted_scores],
    textposition="outside",
))

fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="#0B0E11",
    plot_bgcolor="#0B0E11",
    height=max(400, len(sorted_scores) * 28),
    xaxis_title="1-Month Return (%)",
    title="Sector Momentum Ranking",
    margin=dict(l=200),
)
```

### Key chart — Rotation heatmap:
```python
import plotly.express as px

fig = px.imshow(
    heatmap.values * 100,
    x=[str(c) for c in heatmap.columns],
    y=heatmap.index,
    color_continuous_scale=["#EF4444", "#1a1a2e", "#22C55E"],
    color_continuous_midpoint=0,
    aspect="auto",
    labels={"color": "Return %"},
)
fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="#0B0E11",
    height=max(400, len(heatmap) * 25),
    title="Sector × Month Returns",
)
```

## Step 3: Add to sidebar

In `app.py`, add under STRATEGIES section:

```python
st.page_link("page_views/strategy_sector.py", label="Sector Rotation", icon="🔄")
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# Test sector data loading
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sector_rotation import load_sector_data

df = load_sector_data(lookback_months=24)
print(f'Sector data: {len(df)} rows')
print(f'Columns: {list(df.columns)}')
if not df.empty:
    sector_col = 'sector' if 'sector' in df.columns else df.columns[1]
    print(f'Sectors: {df[sector_col].nunique()}')
    print(f'Sectors list: {sorted(df[sector_col].unique())}')
    date_col = 'date' if 'date' in df.columns else df.columns[0]
    print(f'Date range: {df[date_col].min()} → {df[date_col].max()}')
"

# Test momentum ranking
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sector_rotation import load_sector_data, compute_sector_momentum

df = load_sector_data(24)
scores = compute_sector_momentum(df)
print(f'Sectors ranked: {len(scores)}')
for s in scores:
    print(f'  #{s.rank} {s.sector:30s} 1M:{s.momentum_1m:+6.1%} 3M:{s.momentum_3m:+6.1%} Breadth:{s.breadth:.0%} → {s.signal}')
"

# Test rotation signal
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sector_rotation import load_sector_data, generate_rotation_signal

df = load_sector_data(24)
signal = generate_rotation_signal(df)
if signal:
    print(f'Date: {signal.date}')
    print(f'Context: {signal.regime_note}')
    print(f'\nLONG:')
    for s in signal.long_sectors:
        print(f'  {s.sector}: {s.momentum_1m:+.1%}')
    print(f'\nSHORT:')
    for s in signal.short_sectors:
        print(f'  {s.sector}: {s.momentum_1m:+.1%}')
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sector_rotation import backtest_sector_rotation

# Long-short
result = backtest_sector_rotation(lookback_months=60, top_n=3, bottom_n=3, long_only=False)
if 'error' not in result:
    m = result['metrics']
    print(f'=== SECTOR ROTATION (Long/Short) ===')
    print(f'Strategy Return:  {m[\"strategy_return\"]:.1%}')
    print(f'Benchmark Return: {m[\"benchmark_return\"]:.1%}')
    print(f'Alpha:            {m[\"alpha\"]:+.1%}')
    print(f'Strategy Sharpe:  {m[\"strategy_sharpe\"]:.2f}')
    print(f'Benchmark Sharpe: {m[\"benchmark_sharpe\"]:.2f}')
    print(f'Strategy MaxDD:   {m[\"strategy_maxdd\"]:.1%}')
    print(f'Benchmark MaxDD:  {m[\"benchmark_maxdd\"]:.1%}')
    print(f'Outperformance:   {m[\"outperformance_rate\"]:.0%} of periods')
    print(f'Sectors analyzed: {m[\"sectors_analyzed\"]}')
    print(f'\nMost longed: {result[\"sector_frequency\"][\"most_longed\"][:3]}')
    print(f'Most shorted: {result[\"sector_frequency\"][\"most_shorted\"][:3]}')
else:
    print(result)

# Long-only variant
result2 = backtest_sector_rotation(lookback_months=60, top_n=3, long_only=True)
if 'error' not in result2:
    m2 = result2['metrics']
    print(f'\n=== SECTOR ROTATION (Long-Only) ===')
    print(f'Strategy Return:  {m2[\"strategy_return\"]:.1%}')
    print(f'Alpha:            {m2[\"alpha\"]:+.1%}')
    print(f'Sharpe:           {m2[\"strategy_sharpe\"]:.2f}')
"
```

## IMPORTANT NOTES

1. **Sector data discovery is dynamic** — tries sector index tables first, falls back to computing from stock-level EOD + sector mapping
2. **18 PSX sectors** but some are illiquid (Jute, Woollen, Leather) — strategy still ranks them but illiquid sectors naturally get low breadth scores
3. **Composite score: 60% 1M momentum + 30% 3M + 10% breadth** — avoids chasing one-day spikes
4. **Monthly rebalance (~22 trading days)** — matches PSX sector cycle duration (3-6 months)
5. **Long-only variant available** — practical since PSX short selling is limited
6. **Benchmark: equal-weight all sectors** — more realistic than KSE-100 (which is cap-weighted toward banks)
7. **Outperformance rate** = % of rebalance periods where strategy beat benchmark — should be >55% to be useful
8. **Sector frequency analysis** shows which sectors appear most in long/short baskets — confirms Pakistan's known rotation patterns
9. **Heatmap** reveals seasonality and persistent trends — cement always strong in Jan-Mar (PSDP budget), banking in Jun-Aug (rate cycle)
10. **No TA libraries** — all in numpy/pandas
11. **Add under STRATEGIES** in sidebar after Macro Regimes
12. **This complements Strategy 6 (Macro HMM)** — HMM tells you WHICH regime, Sector Rotation tells you WHERE within that regime the money is flowing
