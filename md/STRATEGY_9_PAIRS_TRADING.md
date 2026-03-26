# Claude Code Prompt: Strategy 9 — Pairs Trading (Statistical Arbitrage)

## Context

pakfindata has 598K EOD bars across 5 years in DuckDB (104x faster than SQLite). 
This strategy finds cointegrated stock pairs on PSX, then trades their spread 
mean-reversion using dynamic hedge ratios from a Kalman filter.

**Classic PSX pairs:**
- OGDC / PPL — both E&P, same oil price exposure, government-owned
- HBL / UBL — top-2 private banks, similar NIM/NPL profiles
- LUCK / DGKC — cement majors, same demand cycle (PSDP construction)
- ENGRO / FFC — fertilizer duopoly, same urea pricing
- MCB / ABL — mid-tier banks, similar branch networks
- HUBC / KAPCO — power IPPs, same capacity payment regime
- PSO / SHEL — oil marketing, same fuel margins

**Why pairs work on PSX:**
- Small universe (~200 liquid stocks) → stable relationships
- Sector-driven market → pairs within sectors stay cointegrated longer
- Low arbitrage capital → mispricings persist for days/weeks
- No HFT → spread mean-reversion is slower but more reliable
- Physical settlement in futures → can hedge with derivatives if needed

## What already exists

```bash
# Check EOD data volume
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
print('eod_ohlcv:', con.execute('SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT symbol) FROM eod_ohlcv').fetchone())

# Top 50 by liquidity
df = con.execute('''
    SELECT symbol, COUNT(*) as days, AVG(volume) as avg_vol
    FROM eod_ohlcv
    WHERE date >= CURRENT_DATE - INTERVAL 365 DAY
    GROUP BY symbol
    HAVING COUNT(*) > 200 AND AVG(volume) > 100000
    ORDER BY AVG(volume) DESC
    LIMIT 50
''').df()
print(f'\nLiquid symbols (>100K avg vol, >200 days): {len(df)}')
print(df.head(20).to_string())
con.close()
"

# Check if sector mapping exists
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for q in [
    'SELECT DISTINCT sector FROM eod_ohlcv WHERE sector IS NOT NULL LIMIT 5',
    'SELECT symbol, sector FROM stock_sectors LIMIT 5',
]:
    try:
        print(con.execute(q).df().to_string())
        break
    except: pass
con.close()
"

# Check scipy/statsmodels availability
python3 -c "
try:
    from scipy import stats; print('scipy OK')
except: print('scipy MISSING')
try:
    from statsmodels.tsa.stattools import coint; print('statsmodels OK')
except: print('statsmodels MISSING — install: pip install statsmodels')
try:
    from pykalman import KalmanFilter; print('pykalman OK')
except: print('pykalman MISSING — install: pip install pykalman')
"
```

**READ ALL OUTPUT before proceeding. Install missing packages.**

## Step 1: Install dependencies

```bash
conda activate psx
pip install statsmodels pykalman --break-system-packages 2>/dev/null || pip install statsmodels pykalman
```

## Step 2: Create the Pairs Trading Engine

Create `src/pakfindata/engine/pairs_trading.py`:

```python
"""
Pairs Trading (Statistical Arbitrage) Engine.

Finds cointegrated stock pairs on PSX using:
  1. Correlation pre-filter (fast) — find candidate pairs
  2. Engle-Granger cointegration test — confirm long-run equilibrium
  3. Johansen test — multi-variate cointegration (for robustness)
  4. Kalman filter — dynamic hedge ratio that adapts over time

Trading logic:
  Spread = Price_A - β × Price_B   (β = hedge ratio)
  Z-score = (Spread - μ) / σ       (rolling mean/std)
  Entry: |Z| > entry_threshold (default 2.0)
  Exit:  |Z| < exit_threshold (default 0.5)

PSX-Specific:
  - Pre-filter by sector (pairs within same sector cointegrate better)
  - Minimum liquidity filter (avg volume > 100K)
  - Circuit breaker awareness (±7.5% — spread can gap)
  - 245 trading days/year
  - Transaction costs: ~0.5% round-trip (brokerage + CVT + FED)
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, Tuple
from itertools import combinations

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245
TRANSACTION_COST_PCT = 0.005  # 0.5% round-trip (PSX brokerage + taxes)

# Known PSX sector pairs (pre-screened for cointegration likelihood)
KNOWN_PAIR_CANDIDATES = [
    ("OGDC", "PPL"),    # E&P
    ("HBL", "UBL"),     # Banking
    ("LUCK", "DGKC"),   # Cement
    ("ENGRO", "FFC"),   # Fertilizer
    ("MCB", "ABL"),     # Banking
    ("HUBC", "KAPCO"),  # Power
    ("PSO", "SHEL"),    # Oil marketing
    ("LUCK", "MLCF"),   # Cement
    ("BAHL", "MEBL"),   # Banking
    ("NBP", "BOP"),     # Banking (govt)
    ("MARI", "PPL"),    # E&P
    ("ATRL", "NRL"),    # Refinery
    ("MTL", "CHCC"),    # Cement
]


@dataclass
class PairStats:
    """Statistical properties of a trading pair."""
    symbol_a: str
    symbol_b: str
    sector: str
    correlation: float
    cointegration_pvalue: float      # Engle-Granger p-value (<0.05 = cointegrated)
    is_cointegrated: bool
    hedge_ratio_static: float        # OLS hedge ratio
    hedge_ratio_kalman: float        # Kalman-filtered (latest)
    half_life: float                 # mean-reversion half-life in days
    spread_mean: float
    spread_std: float
    current_zscore: float
    hurst_exponent: float            # <0.5 = mean-reverting
    lookback_days: int
    adf_statistic: float             # ADF test on spread
    adf_pvalue: float


@dataclass
class PairsSignal:
    symbol_a: str
    symbol_b: str
    date: str
    spread: float
    zscore: float
    hedge_ratio: float
    signal: str              # "LONG_SPREAD", "SHORT_SPREAD", "EXIT", "HOLD"
    confidence: float
    position_a: str          # "BUY" or "SELL" for symbol A
    position_b: str          # "BUY" or "SELL" for symbol B
    shares_a: int            # suggested shares for A (per 1M PKR capital)
    shares_b: int            # suggested shares for B
    reason: str


def load_pair_prices(symbol_a: str, symbol_b: str, days: int = 500) -> Tuple[pd.Series, pd.Series, pd.DataFrame]:
    """
    Load aligned close prices for a pair from DuckDB.
    Returns (series_a, series_b, merged_df)
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    df_a = con.execute("""
        SELECT date, close FROM eod_ohlcv
        WHERE symbol = ? AND date >= ? ORDER BY date
    """, [symbol_a, cutoff]).df()
    
    df_b = con.execute("""
        SELECT date, close FROM eod_ohlcv
        WHERE symbol = ? AND date >= ? ORDER BY date
    """, [symbol_b, cutoff]).df()
    
    con.close()
    
    if df_a.empty or df_b.empty:
        return pd.Series(), pd.Series(), pd.DataFrame()
    
    df_a = df_a.rename(columns={"close": "price_a"})
    df_b = df_b.rename(columns={"close": "price_b"})
    
    merged = df_a.merge(df_b, on="date", how="inner").sort_values("date").reset_index(drop=True)
    
    return merged["price_a"], merged["price_b"], merged


def test_cointegration(prices_a: pd.Series, prices_b: pd.Series) -> dict:
    """
    Test cointegration between two price series using Engle-Granger method.
    
    Returns dict with p-value, test statistic, hedge ratio, and half-life.
    """
    if len(prices_a) < 60 or len(prices_b) < 60:
        return {"is_cointegrated": False, "error": "Insufficient data"}
    
    try:
        from statsmodels.tsa.stattools import coint, adfuller
        
        # Engle-Granger cointegration test
        score, pvalue, _ = coint(prices_a.values, prices_b.values)
        
        # OLS hedge ratio: A = β × B + α + ε
        from numpy.polynomial.polynomial import polyfit
        beta = np.polyfit(prices_b.values, prices_a.values, 1)[0]
        
        # Compute spread
        spread = prices_a.values - beta * prices_b.values
        
        # ADF test on spread (should be stationary if cointegrated)
        adf_result = adfuller(spread, maxlag=20)
        
        # Half-life of mean reversion
        spread_lag = spread[:-1]
        spread_diff = np.diff(spread)
        if len(spread_lag) > 10:
            beta_mr = np.polyfit(spread_lag, spread_diff, 1)[0]
            half_life = -np.log(2) / beta_mr if beta_mr < 0 else 999
        else:
            half_life = 999
        
        # Hurst exponent (simplified R/S)
        returns = np.diff(spread) / np.abs(spread[:-1] + 1e-10)
        hurst = _compute_hurst(returns)
        
        return {
            "is_cointegrated": pvalue < 0.05,
            "coint_pvalue": float(pvalue),
            "coint_statistic": float(score),
            "hedge_ratio": float(beta),
            "spread": spread,
            "spread_mean": float(np.mean(spread)),
            "spread_std": float(np.std(spread)),
            "half_life": float(half_life),
            "adf_statistic": float(adf_result[0]),
            "adf_pvalue": float(adf_result[1]),
            "hurst": float(hurst),
        }
    
    except ImportError:
        return _simple_cointegration(prices_a, prices_b)


def _simple_cointegration(prices_a: pd.Series, prices_b: pd.Series) -> dict:
    """Fallback cointegration test without statsmodels (correlation + spread stationarity)."""
    corr = prices_a.corr(prices_b)
    beta = np.polyfit(prices_b.values, prices_a.values, 1)[0]
    spread = prices_a.values - beta * prices_b.values
    
    # Simple stationarity check: variance ratio test
    n = len(spread)
    half = n // 2
    var1 = np.var(spread[:half])
    var2 = np.var(spread[half:])
    var_ratio = var1 / var2 if var2 > 0 else 999
    is_stationary = 0.5 < var_ratio < 2.0  # roughly equal variance = likely stationary
    
    half_life = 999
    spread_lag = spread[:-1]
    spread_diff = np.diff(spread)
    if len(spread_lag) > 10:
        beta_mr = np.polyfit(spread_lag, spread_diff, 1)[0]
        half_life = -np.log(2) / beta_mr if beta_mr < 0 else 999
    
    return {
        "is_cointegrated": corr > 0.8 and is_stationary,
        "coint_pvalue": 1.0 - corr,  # approximate
        "coint_statistic": 0,
        "hedge_ratio": float(beta),
        "spread": spread,
        "spread_mean": float(np.mean(spread)),
        "spread_std": float(np.std(spread)),
        "half_life": float(half_life),
        "adf_statistic": 0,
        "adf_pvalue": 1.0 - corr,
        "hurst": 0.5,
    }


def _compute_hurst(returns: np.ndarray, max_lag: int = 50) -> float:
    """Compute Hurst exponent from returns."""
    if len(returns) < max_lag:
        return 0.5
    
    lags = range(2, min(max_lag, len(returns) // 2))
    tau = []
    for lag in lags:
        tau.append(np.std(np.subtract(returns[lag:], returns[:-lag])))
    
    if not tau or any(t <= 0 for t in tau):
        return 0.5
    
    try:
        poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
        return float(poly[0])
    except:
        return 0.5


def kalman_hedge_ratio(prices_a: pd.Series, prices_b: pd.Series) -> pd.Series:
    """
    Compute time-varying hedge ratio using Kalman filter.
    
    The hedge ratio β_t evolves as a random walk:
      β_t = β_{t-1} + w_t     (state transition)
      A_t = β_t × B_t + v_t   (observation)
    
    Returns Series of hedge ratios over time.
    """
    try:
        from pykalman import KalmanFilter
        
        obs = prices_a.values
        n = len(obs)
        
        # State: hedge ratio β
        # Observation: price_a = β × price_b
        
        obs_mat = np.expand_dims(prices_b.values, axis=(1, 2))  # (n, 1, 1)
        
        kf = KalmanFilter(
            n_dim_obs=1,
            n_dim_state=1,
            initial_state_mean=[np.polyfit(prices_b.values[:60], prices_a.values[:60], 1)[0]],
            initial_state_covariance=np.array([[1.0]]),
            transition_matrices=np.array([[1.0]]),
            observation_matrices=obs_mat,
            observation_covariance=np.array([[1.0]]),
            transition_covariance=np.array([[0.01]]),  # how fast β changes
        )
        
        state_means, _ = kf.filter(obs.reshape(-1, 1))
        
        return pd.Series(state_means.flatten(), index=prices_a.index)
    
    except ImportError:
        # Fallback: rolling OLS
        window = 60
        ratios = []
        for i in range(len(prices_a)):
            if i < window:
                ratios.append(np.nan)
            else:
                beta = np.polyfit(
                    prices_b.values[i-window:i],
                    prices_a.values[i-window:i], 1
                )[0]
                ratios.append(beta)
        
        return pd.Series(ratios, index=prices_a.index)


def find_cointegrated_pairs(
    min_correlation: float = 0.7,
    max_pvalue: float = 0.05,
    min_half_life: float = 5,
    max_half_life: float = 60,
    min_days: int = 250,
    sector_only: bool = True,
    top_n: int = 20,
) -> list[PairStats]:
    """
    Scan all liquid PSX pairs for cointegration.
    
    Pipeline:
      1. Get liquid symbols (avg vol > 100K, > min_days of data)
      2. Pre-filter by correlation (> min_correlation)
      3. Test cointegration (Engle-Granger p-value < max_pvalue)
      4. Filter by half-life (5-60 days — too fast or too slow is untradeable)
      5. Rank by half-life (shorter = faster mean-reversion = better)
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=min_days * 1.5)).strftime("%Y-%m-%d")
    
    # Get liquid symbols with sector
    symbols_df = con.execute(f"""
        SELECT symbol, 
               COUNT(*) as days, 
               AVG(volume) as avg_vol,
               MAX(sector) as sector
        FROM eod_ohlcv
        WHERE date >= '{cutoff}'
        GROUP BY symbol
        HAVING COUNT(*) >= {min_days} AND AVG(volume) > 100000
        ORDER BY AVG(volume) DESC
        LIMIT 100
    """).df()
    
    con.close()
    
    if symbols_df.empty:
        return []
    
    symbols = symbols_df["symbol"].tolist()
    sectors = dict(zip(symbols_df["symbol"], symbols_df["sector"].fillna("Unknown")))
    
    # Generate candidate pairs
    if sector_only and any(s != "Unknown" for s in sectors.values()):
        # Only pairs within same sector
        sector_groups = {}
        for sym, sec in sectors.items():
            sector_groups.setdefault(sec, []).append(sym)
        
        candidates = []
        for sec, syms in sector_groups.items():
            if len(syms) >= 2:
                candidates.extend(combinations(syms, 2))
    else:
        # All combinations (slower — O(n²))
        candidates = list(combinations(symbols[:50], 2))  # limit to top 50
    
    # Add known candidates that might be missing
    for a, b in KNOWN_PAIR_CANDIDATES:
        if a in symbols and b in symbols and (a, b) not in candidates and (b, a) not in candidates:
            candidates.append((a, b))
    
    results = []
    
    for sym_a, sym_b in candidates:
        prices_a, prices_b, merged = load_pair_prices(sym_a, sym_b, days=min_days)
        
        if len(prices_a) < min_days * 0.8:
            continue
        
        # Pre-filter: correlation
        corr = prices_a.corr(prices_b)
        if abs(corr) < min_correlation:
            continue
        
        # Cointegration test
        coint_result = test_cointegration(prices_a, prices_b)
        
        if not coint_result.get("is_cointegrated", False):
            continue
        
        half_life = coint_result.get("half_life", 999)
        if half_life < min_half_life or half_life > max_half_life:
            continue
        
        # Kalman hedge ratio
        kalman_ratios = kalman_hedge_ratio(prices_a, prices_b)
        latest_kalman = kalman_ratios.dropna().iloc[-1] if not kalman_ratios.dropna().empty else coint_result["hedge_ratio"]
        
        # Current z-score
        spread = prices_a.values - latest_kalman * prices_b.values
        window = max(20, int(half_life * 2))
        spread_mean = np.mean(spread[-window:])
        spread_std = np.std(spread[-window:])
        current_z = (spread[-1] - spread_mean) / spread_std if spread_std > 0 else 0
        
        sector = sectors.get(sym_a, sectors.get(sym_b, "Unknown"))
        
        results.append(PairStats(
            symbol_a=sym_a,
            symbol_b=sym_b,
            sector=sector,
            correlation=corr,
            cointegration_pvalue=coint_result["coint_pvalue"],
            is_cointegrated=True,
            hedge_ratio_static=coint_result["hedge_ratio"],
            hedge_ratio_kalman=float(latest_kalman),
            half_life=half_life,
            spread_mean=spread_mean,
            spread_std=spread_std,
            current_zscore=float(current_z),
            hurst_exponent=coint_result.get("hurst", 0.5),
            lookback_days=len(prices_a),
            adf_statistic=coint_result.get("adf_statistic", 0),
            adf_pvalue=coint_result.get("adf_pvalue", 1),
        ))
    
    # Sort by half-life (shorter = better)
    results.sort(key=lambda x: x.half_life)
    
    return results[:top_n]


def generate_pairs_signal(
    symbol_a: str,
    symbol_b: str,
    entry_zscore: float = 2.0,
    exit_zscore: float = 0.5,
    lookback_days: int = 500,
    capital: float = 1_000_000,
) -> PairsSignal:
    """
    Generate current trading signal for a pair.
    """
    prices_a, prices_b, merged = load_pair_prices(symbol_a, symbol_b, lookback_days)
    
    if merged.empty or len(merged) < 60:
        return None
    
    # Kalman hedge ratio
    kalman_ratios = kalman_hedge_ratio(prices_a, prices_b)
    hedge_ratio = kalman_ratios.dropna().iloc[-1] if not kalman_ratios.dropna().empty else \
                  np.polyfit(prices_b.values, prices_a.values, 1)[0]
    
    # Spread and z-score
    spread = prices_a.values - hedge_ratio * prices_b.values
    
    # Use half-life-based window
    coint_result = test_cointegration(prices_a, prices_b)
    half_life = coint_result.get("half_life", 30)
    window = max(20, min(120, int(half_life * 2)))
    
    spread_mean = np.mean(spread[-window:])
    spread_std = np.std(spread[-window:])
    zscore = (spread[-1] - spread_mean) / spread_std if spread_std > 0 else 0
    
    # Current prices
    price_a = prices_a.iloc[-1]
    price_b = prices_b.iloc[-1]
    date = str(merged["date"].iloc[-1])[:10]
    
    # Signal logic
    signal = "HOLD"
    position_a = ""
    position_b = ""
    confidence = 0.0
    reason = ""
    
    if zscore > entry_zscore:
        # Spread too high → sell A, buy B (short spread)
        signal = "SHORT_SPREAD"
        position_a = "SELL"
        position_b = "BUY"
        confidence = min(1.0, abs(zscore) / 4)
        reason = (f"Z-score {zscore:.2f} > {entry_zscore} — spread overextended. "
                 f"Sell {symbol_a}, Buy {symbol_b}. "
                 f"Half-life: {half_life:.0f} days.")
    
    elif zscore < -entry_zscore:
        # Spread too low → buy A, sell B (long spread)
        signal = "LONG_SPREAD"
        position_a = "BUY"
        position_b = "SELL"
        confidence = min(1.0, abs(zscore) / 4)
        reason = (f"Z-score {zscore:.2f} < -{entry_zscore} — spread compressed. "
                 f"Buy {symbol_a}, Sell {symbol_b}. "
                 f"Half-life: {half_life:.0f} days.")
    
    elif abs(zscore) < exit_zscore:
        signal = "EXIT"
        confidence = 0.5
        reason = f"Z-score {zscore:.2f} normalized — close any open position"
    
    else:
        signal = "HOLD"
        confidence = 0.2
        reason = f"Z-score {zscore:.2f} between entry/exit thresholds"
    
    # Position sizing (dollar-neutral)
    half_capital = capital / 2
    shares_a = int(half_capital / price_a) if price_a > 0 else 0
    shares_b = int(half_capital / (price_b * abs(hedge_ratio))) if price_b > 0 and hedge_ratio != 0 else 0
    
    return PairsSignal(
        symbol_a=symbol_a,
        symbol_b=symbol_b,
        date=date,
        spread=float(spread[-1]),
        zscore=float(zscore),
        hedge_ratio=float(hedge_ratio),
        signal=signal,
        confidence=confidence,
        position_a=position_a,
        position_b=position_b,
        shares_a=shares_a,
        shares_b=shares_b,
        reason=reason,
    )


def backtest_pairs_strategy(
    symbol_a: str,
    symbol_b: str,
    entry_zscore: float = 2.0,
    exit_zscore: float = 0.5,
    stop_loss_zscore: float = 4.0,
    max_hold_days: int = 60,
    use_kalman: bool = True,
    lookback_days: int = 500,
    transaction_cost: float = TRANSACTION_COST_PCT,
) -> dict:
    """
    Backtest pairs trading strategy.
    
    For each day:
      1. Compute hedge ratio (Kalman or static)
      2. Compute spread and z-score
      3. Entry: |z| > entry_zscore
      4. Exit: |z| < exit_zscore, |z| > stop_loss, or max_hold
      5. P&L: spread change × position (dollar-neutral)
    """
    prices_a, prices_b, merged = load_pair_prices(symbol_a, symbol_b, lookback_days)
    
    if merged.empty or len(merged) < 120:
        return {"error": f"Insufficient data for {symbol_a}/{symbol_b}"}
    
    # Compute hedge ratios
    if use_kalman:
        hedge_ratios = kalman_hedge_ratio(prices_a, prices_b)
    else:
        static_beta = np.polyfit(prices_b.values[:120], prices_a.values[:120], 1)[0]
        hedge_ratios = pd.Series([static_beta] * len(prices_a), index=prices_a.index)
    
    # Compute spread and z-score
    spread = prices_a.values - hedge_ratios.values * prices_b.values
    
    # Rolling z-score
    coint_result = test_cointegration(prices_a, prices_b)
    half_life = coint_result.get("half_life", 30)
    window = max(20, min(120, int(half_life * 2)))
    
    spread_mean = pd.Series(spread).rolling(window, min_periods=20).mean().values
    spread_std = pd.Series(spread).rolling(window, min_periods=20).std().values
    zscore = np.where(spread_std > 0, (spread - spread_mean) / spread_std, 0)
    
    # Simulate trades
    trades = []
    position = None  # {"entry_idx", "direction", "entry_spread", "entry_z", "entry_date"}
    daily_pnl = np.zeros(len(merged))
    
    for i in range(window, len(merged)):
        z = zscore[i]
        
        if np.isnan(z):
            continue
        
        # Check exits
        if position is not None:
            days_held = i - position["entry_idx"]
            spread_change = spread[i] - position["entry_spread"]
            
            if position["direction"] == "LONG_SPREAD":
                pnl_raw = spread_change
            else:
                pnl_raw = -spread_change
            
            # Normalize P&L to percentage of capital
            entry_price_a = prices_a.iloc[position["entry_idx"]]
            pnl_pct = pnl_raw / entry_price_a if entry_price_a > 0 else 0
            
            daily_pnl[i] = pnl_pct - (daily_pnl[i-1] if i > 0 else 0)  # incremental
            
            exit_reason = None
            if abs(z) < exit_zscore:
                exit_reason = "MEAN_REVERT"
            elif abs(z) > stop_loss_zscore:
                exit_reason = "STOP_LOSS"
            elif days_held >= max_hold_days:
                exit_reason = "MAX_HOLD"
            
            if exit_reason:
                total_pnl = pnl_pct - transaction_cost  # subtract costs
                trades.append({
                    "entry_date": str(merged.iloc[position["entry_idx"]]["date"])[:10],
                    "exit_date": str(merged.iloc[i]["date"])[:10],
                    "direction": position["direction"],
                    "entry_z": position["entry_z"],
                    "exit_z": z,
                    "entry_spread": position["entry_spread"],
                    "exit_spread": spread[i],
                    "pnl_pct": total_pnl,
                    "days_held": days_held,
                    "exit_reason": exit_reason,
                    "hedge_ratio": hedge_ratios.iloc[i],
                })
                position = None
        
        # Check entries (only if flat)
        if position is None:
            if z > entry_zscore:
                position = {
                    "entry_idx": i,
                    "direction": "SHORT_SPREAD",
                    "entry_spread": spread[i],
                    "entry_z": z,
                    "entry_date": str(merged.iloc[i]["date"])[:10],
                }
            elif z < -entry_zscore:
                position = {
                    "entry_idx": i,
                    "direction": "LONG_SPREAD",
                    "entry_spread": spread[i],
                    "entry_z": z,
                    "entry_date": str(merged.iloc[i]["date"])[:10],
                }
    
    if not trades:
        return {
            "error": "No trades generated",
            "spread_data": pd.DataFrame({
                "date": merged["date"], "spread": spread, "zscore": zscore,
                "price_a": prices_a.values, "price_b": prices_b.values,
                "hedge_ratio": hedge_ratios.values,
            }),
        }
    
    trades_df = pd.DataFrame(trades)
    
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]
    
    trades_df["cum_return"] = (1 + trades_df["pnl_pct"]).cumprod()
    total_return = trades_df["cum_return"].iloc[-1] - 1
    max_dd = (trades_df["cum_return"] / trades_df["cum_return"].cummax() - 1).min()
    
    # Annualized metrics
    days_span = (pd.to_datetime(trades_df["exit_date"].iloc[-1]) - 
                 pd.to_datetime(trades_df["entry_date"].iloc[0])).days
    years = days_span / 365 if days_span > 0 else 1
    ann_return = (1 + total_return) ** (1 / years) - 1
    ann_vol = trades_df["pnl_pct"].std() * np.sqrt(TRADING_DAYS / trades_df["days_held"].mean()) if trades_df["days_held"].mean() > 0 else 0
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0
    
    return {
        "trades": trades_df,
        "spread_data": pd.DataFrame({
            "date": merged["date"],
            "spread": spread,
            "zscore": zscore,
            "price_a": prices_a.values,
            "price_b": prices_b.values,
            "hedge_ratio": hedge_ratios.values,
        }),
        "pair_stats": coint_result,
        "metrics": {
            "total_trades": len(trades_df),
            "win_rate": len(winning) / len(trades_df),
            "avg_win": winning["pnl_pct"].mean() if len(winning) > 0 else 0,
            "avg_loss": losing["pnl_pct"].mean() if len(losing) > 0 else 0,
            "profit_factor": abs(winning["pnl_pct"].sum() / losing["pnl_pct"].sum()) if len(losing) > 0 and losing["pnl_pct"].sum() != 0 else 0,
            "total_return": float(total_return),
            "annualized_return": float(ann_return),
            "annualized_vol": float(ann_vol),
            "sharpe_ratio": float(sharpe),
            "max_drawdown": float(max_dd),
            "avg_days_held": trades_df["days_held"].mean(),
            "half_life": coint_result.get("half_life", 0),
            "hurst": coint_result.get("hurst", 0.5),
            "cointegration_pvalue": coint_result.get("coint_pvalue", 1),
            "exit_reasons": trades_df["exit_reason"].value_counts().to_dict(),
            "use_kalman": use_kalman,
            "transaction_cost": transaction_cost,
        },
    }


def scan_pair_opportunities(
    entry_zscore: float = 1.5,
    max_pairs: int = 20,
) -> pd.DataFrame:
    """
    Scan all cointegrated pairs for current trading opportunities.
    Returns pairs where |z-score| > entry_zscore threshold.
    """
    pairs = find_cointegrated_pairs(top_n=30)
    
    results = []
    for pair in pairs:
        if abs(pair.current_zscore) > entry_zscore:
            direction = "SHORT_SPREAD" if pair.current_zscore > 0 else "LONG_SPREAD"
            results.append({
                "pair": f"{pair.symbol_a}/{pair.symbol_b}",
                "symbol_a": pair.symbol_a,
                "symbol_b": pair.symbol_b,
                "sector": pair.sector,
                "zscore": pair.current_zscore,
                "direction": direction,
                "half_life": pair.half_life,
                "correlation": pair.correlation,
                "coint_pvalue": pair.cointegration_pvalue,
                "hedge_ratio": pair.hedge_ratio_kalman,
                "hurst": pair.hurst_exponent,
                "spread_std": pair.spread_std,
            })
        elif abs(pair.current_zscore) > 1.0:
            results.append({
                "pair": f"{pair.symbol_a}/{pair.symbol_b}",
                "symbol_a": pair.symbol_a,
                "symbol_b": pair.symbol_b,
                "sector": pair.sector,
                "zscore": pair.current_zscore,
                "direction": "WATCH",
                "half_life": pair.half_life,
                "correlation": pair.correlation,
                "coint_pvalue": pair.cointegration_pvalue,
                "hedge_ratio": pair.hedge_ratio_kalman,
                "hurst": pair.hurst_exponent,
                "spread_std": pair.spread_std,
            })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results).sort_values("zscore", key=abs, ascending=False).head(max_pairs).reset_index(drop=True)
```

## Step 3: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_pairs.py`:

### Tab 1: Pair Explorer
```
For selected pair (dropdown or manual A/B input):
├── Price ratio chart (A/B) with mean ± 2σ bands
├── Spread chart with z-score overlay (dual axis)
├── Hedge ratio chart (Kalman vs static — shows how β evolves)
├── Signal badge: LONG_SPREAD / SHORT_SPREAD / EXIT / HOLD
├── Metric cards: Z-score, Hedge Ratio, Half-Life, Correlation, Coint p-value, Hurst
├── Position sizing: "Buy X shares of A, Sell Y shares of B"
├── Known pairs quick-select buttons (OGDC/PPL, HBL/UBL, etc.)
└── Scatter plot: Price A vs Price B with regression line (visual cointegration)
```

### Tab 2: Pair Discovery
```
├── [Scan for Cointegrated Pairs] button
├── Parameters:
│   ├── Min correlation: slider 0.5-0.9 (default 0.7)
│   ├── Max p-value: slider 0.01-0.10 (default 0.05)
│   ├── Half-life range: 5-60 days
│   ├── Sector-only: toggle (default ON)
│   └── Min lookback: 1Y / 2Y / 3Y
├── Results table: Pair | Sector | Corr | Coint p | Half-Life | Hurst | Z-score | Signal
├── Sort by half-life or |z-score|
├── Color: green rows (active signal), gray (cointegrated but no signal)
└── Click pair → jump to Tab 1 with that pair loaded
```

### Tab 3: Backtest
```
├── Pair selector
├── Parameters:
│   ├── Entry z-score: 1.5-3.0 (default 2.0)
│   ├── Exit z-score: 0.3-1.0 (default 0.5)
│   ├── Stop loss z-score: 3.0-6.0 (default 4.0)
│   ├── Max hold days: 20-120 (default 60)
│   ├── Use Kalman: toggle (default ON)
│   ├── Transaction cost: 0.1%-1.0% (default 0.5%)
│   └── Lookback: 2Y / 3Y / 5Y
├── [Run Backtest]
├── Metrics: Trades, Win Rate, PF, Return, Sharpe, MaxDD, Avg Hold
├── Equity curve
├── Spread + z-score chart with entry/exit markers
├── Trade log table
├── Exit reason breakdown
├── Kalman vs Static hedge ratio comparison
└── Rolling cointegration stability check (is the pair still cointegrated?)
```

### Tab 4: Research
```
├── Cointegration stability over time: rolling 120-day coint p-value
├── Hedge ratio evolution: how stable is β?
├── Half-life stability: does mean-reversion speed change?
├── Sector pair matrix: heatmap of pairwise correlations within each sector
├── Structural break detection: when did cointegration break/reform?
├── Johansen test results (multi-variate) if statsmodels available
├── Pakistan sector pair playbook:
│   ├── Banking: HBL/UBL (most stable), MCB/ABL, NBP/BOP
│   ├── E&P: OGDC/PPL (government-owned pair), MARI/PPL
│   ├── Cement: LUCK/DGKC (market leaders), LUCK/MLCF, MTL/CHCC
│   ├── Fertilizer: ENGRO/FFC (duopoly pair)
│   ├── Power: HUBC/KAPCO (IPP pair)
│   └── Oil Marketing: PSO/SHEL
└── Methodology: cointegration theory, Kalman filter, z-score trading
```

### Key chart — Spread with z-score bands and trade markers:
```python
from plotly.subplots import make_subplots
import plotly.graph_objects as go

fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                    row_heights=[0.4, 0.35, 0.25], vertical_spacing=0.05,
                    subplot_titles=[f"{sym_a} vs {sym_b} Prices", "Spread Z-Score", "Hedge Ratio"])

# Prices (normalized to 100)
fig.add_trace(go.Scatter(x=df["date"], y=df["price_a"]/df["price_a"].iloc[0]*100,
    name=sym_a, line=dict(color="#22C55E")), row=1, col=1)
fig.add_trace(go.Scatter(x=df["date"], y=df["price_b"]/df["price_b"].iloc[0]*100,
    name=sym_b, line=dict(color="#3B82F6")), row=1, col=1)

# Z-score with bands
fig.add_trace(go.Scatter(x=df["date"], y=df["zscore"],
    name="Z-Score", line=dict(color="#C8A96E", width=1.5)), row=2, col=1)
fig.add_hline(y=2, line_dash="dash", line_color="#EF4444", row=2, col=1)
fig.add_hline(y=-2, line_dash="dash", line_color="#22C55E", row=2, col=1)
fig.add_hline(y=0, line_dash="dot", line_color="#6B7280", row=2, col=1)
fig.add_hrect(y0=2, y1=5, fillcolor="rgba(239,68,68,0.1)", row=2, col=1, line_width=0)
fig.add_hrect(y0=-5, y1=-2, fillcolor="rgba(34,197,94,0.1)", row=2, col=1, line_width=0)

# Hedge ratio
fig.add_trace(go.Scatter(x=df["date"], y=df["hedge_ratio"],
    name="β (Kalman)", line=dict(color="#A855F7", width=1.5)), row=3, col=1)

fig.update_layout(template="plotly_dark", paper_bgcolor="#0B0E11",
                  plot_bgcolor="#0B0E11", height=700)
```

## Step 4: Add to sidebar

```python
st.page_link("page_views/strategy_pairs.py", label="Pairs Trading", icon="🔗")
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test pair cointegration
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.pairs_trading import load_pair_prices, test_cointegration, kalman_hedge_ratio

# Test OGDC/PPL
pa, pb, merged = load_pair_prices('OGDC', 'PPL', days=500)
print(f'Data points: {len(merged)}')

result = test_cointegration(pa, pb)
print(f'Cointegrated: {result[\"is_cointegrated\"]}')
print(f'P-value: {result[\"coint_pvalue\"]:.4f}')
print(f'Hedge ratio: {result[\"hedge_ratio\"]:.4f}')
print(f'Half-life: {result[\"half_life\"]:.1f} days')
print(f'Hurst: {result[\"hurst\"]:.3f}')
print(f'ADF p-value: {result[\"adf_pvalue\"]:.4f}')

# Kalman hedge ratio
kr = kalman_hedge_ratio(pa, pb)
print(f'Kalman β (latest): {kr.dropna().iloc[-1]:.4f}')
print(f'Kalman β range: {kr.dropna().min():.4f} — {kr.dropna().max():.4f}')
"

# Scan for pairs
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.pairs_trading import find_cointegrated_pairs

pairs = find_cointegrated_pairs(sector_only=True, top_n=10)
print(f'Cointegrated pairs found: {len(pairs)}')
for p in pairs:
    print(f'  {p.symbol_a}/{p.symbol_b} ({p.sector}) corr:{p.correlation:.2f} coint_p:{p.cointegration_pvalue:.4f} HL:{p.half_life:.0f}d Hurst:{p.hurst_exponent:.3f} Z:{p.current_zscore:+.2f}')
"

# Backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.pairs_trading import backtest_pairs_strategy

result = backtest_pairs_strategy('OGDC', 'PPL', entry_zscore=2.0, use_kalman=True, lookback_days=500)
if 'error' not in result:
    m = result['metrics']
    print(f'=== OGDC/PPL PAIRS BACKTEST ===')
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Profit Factor: {m[\"profit_factor\"]:.2f}')
    print(f'Total Return: {m[\"total_return\"]:.1%}')
    print(f'Ann Return: {m[\"annualized_return\"]:.1%}')
    print(f'Sharpe: {m[\"sharpe_ratio\"]:.2f}')
    print(f'Max DD: {m[\"max_drawdown\"]:.1%}')
    print(f'Avg Hold: {m[\"avg_days_held\"]:.0f} days')
    print(f'Half-Life: {m[\"half_life\"]:.0f} days')
    print(f'Coint p: {m[\"cointegration_pvalue\"]:.4f}')
    print(f'Exits: {m[\"exit_reasons\"]}')
else:
    print(result)
"

# Scan opportunities
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.pairs_trading import scan_pair_opportunities

df = scan_pair_opportunities(entry_zscore=1.5)
print(f'Opportunities: {len(df)}')
if not df.empty:
    print(df[['pair','sector','zscore','direction','half_life','correlation','coint_pvalue','hurst']].to_string())
"
```

## IMPORTANT NOTES

1. **Install statsmodels + pykalman** — core dependencies. Fallbacks exist but are weaker.
2. **Sector-only filtering** reduces O(n²) pairs from 10K+ to ~200 (much faster scan).
3. **Engle-Granger p < 0.05** is the cointegration threshold — stricter than correlation.
4. **Half-life 5-60 days** is the tradeable range — too short = noise, too long = no mean-reversion.
5. **Kalman filter** adapts hedge ratio over time — critical because relationships drift.
6. **Transaction cost 0.5%** baked into backtest — PSX brokerage + CVT + FED.
7. **Stop loss at z = 4** — if spread blows out past 4σ, cointegration may have broken.
8. **Dollar-neutral** — equal capital on each leg. Market direction doesn't matter.
9. **13 known PSX pair candidates** pre-screened — these are the most likely to work.
10. **Hurst < 0.5** confirms mean-reversion — pairs with Hurst > 0.5 are trending, not reverting.
11. **Rolling cointegration check** in Research tab — cointegration can break during regime changes.
12. **No TA libraries** — statsmodels + pykalman + numpy/scipy only.
13. **Add under STRATEGIES** in sidebar.
14. **PSX edge:** Small universe (200 liquid names) means stable pairs. On NYSE you'd need to re-scan weekly. On PSX the same pairs work for months.
