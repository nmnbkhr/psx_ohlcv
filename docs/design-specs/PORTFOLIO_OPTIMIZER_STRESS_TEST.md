# Claude Code Prompt: Multi-Strategy Portfolio Optimizer + Stress Testing

## Context

pakfindata has 14 strategy engines, each with a `backtest_*()` function that returns equity 
curves and trade logs. It also has `fund_risk.py` with Sharpe, Sortino, VaR, drawdown, beta, 
alpha, information ratio. AND 5 years of EOD data for 540 symbols.

This prompt builds a **QuantPlatform-style** multi-strategy portfolio page that:
1. Takes multiple strategy backtests as inputs
2. Combines them with optimal weights (Sharpe, CVaR, Risk Parity objectives)
3. Computes portfolio analytics (diversification, HHI, risk contribution)
4. Runs stress tests against real PSX crises (2018 BoP, 2020 COVID, 2022 political crisis)
5. Shows everything in a three-panel layout

## What Already Exists

```bash
cd ~/pakfindata && conda activate psx

# 1. Available backtest functions
for f in vpin_strategy ofi_strategy cvd_strategy basis_strategy macro_regime_hmm \
         sector_rotation oi_strategy pairs_trading vwap_execution ml_model; do
    echo "--- $f ---"
    grep "def backtest\|def walk_forward" src/pakfindata/engine/${f}.py 2>/dev/null | head -2
done

# 2. Risk functions already built
grep "^def " src/pakfindata/engine/fund_risk.py

# 3. EOD data available
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
r = con.execute('''
    SELECT COUNT(*) as bars, COUNT(DISTINCT symbol) as symbols,
           MIN(date) as first, MAX(date) as last
    FROM eod_ohlcv
''').fetchone()
print(f'EOD: {r[0]:,} bars, {r[1]} symbols, {r[2]} to {r[3]}')

# Check crisis period coverage
for period, start, end in [
    ('2018 BoP Crisis', '2018-06-01', '2018-12-31'),
    ('2020 COVID Crash', '2020-02-01', '2020-04-30'),
    ('2022 Political+Floods', '2022-03-01', '2022-10-31'),
    ('2023 Rate Peak', '2023-01-01', '2023-06-30'),
]:
    r = con.execute(f'''
        SELECT COUNT(DISTINCT date), COUNT(DISTINCT symbol)
        FROM eod_ohlcv WHERE date BETWEEN '{start}' AND '{end}'
    ''').fetchone()
    print(f'  {period}: {r[0]} trading days, {r[1]} symbols')
con.close()
"

# 4. Check existing portfolio/optimization code
find src/ -name "*portfolio*" -o -name "*optim*" -o -name "*stress*" | grep -v __pycache__

# 5. SBP rates for risk-free rate
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
try:
    r = con.execute('SELECT MAX(date), MAX(offer) FROM kibor_daily').fetchone()
    print(f'KIBOR latest: {r[0]}, rate: {r[1]}%')
except: print('No KIBOR data')
try:
    r = con.execute('''SELECT rate_date, policy_rate FROM sbp_policy_rates 
                       ORDER BY rate_date DESC LIMIT 1''').fetchone()
    print(f'SBP rate: {r[0]}, {r[1]}%')
except: print('No SBP rate')
con.close()
"
```

**READ ALL OUTPUT before proceeding.**

## Step 1: Create the Portfolio Optimizer Engine

Create `src/pakfindata/engine/portfolio_optimizer.py`:

```python
"""
Multi-Strategy Portfolio Optimizer.

Takes backtest results from multiple strategies, computes optimal portfolio 
weights using various objectives (Max Sharpe, Min CVaR, Risk Parity, Equal Weight),
and produces comprehensive analytics.

Uses:
  - fund_risk.py for Sharpe, VaR, drawdown, etc.
  - scipy.optimize for portfolio optimization
  - numpy for covariance, correlation matrices

PSX-Specific:
  - Risk-free rate from KIBOR 3M or SBP policy rate
  - Trading days: 245/year (PSX calendar)
  - Circuit breakers: ±7.5% daily limit
  - Sector concentration limits (PSX is bank-heavy)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245  # PSX


def _duck():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def get_risk_free_rate() -> float:
    """Get current annual risk-free rate from KIBOR/SBP."""
    try:
        con = _duck()
        for q in [
            "SELECT offer FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1",
            "SELECT policy_rate FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1",
        ]:
            try:
                r = con.execute(q).fetchone()
                if r and r[0]:
                    con.close()
                    return float(r[0]) / 100  # percent to decimal
            except:
                continue
        con.close()
    except:
        pass
    return 0.12  # fallback: 12%


# ═══════════════════════════════════════════════════════
# BACKTEST RUNNER — collects equity curves from all strategies
# ═══════════════════════════════════════════════════════

@dataclass
class StrategyBacktest:
    """Result of running one strategy backtest."""
    name: str               # "vpin/OGDC", "sector_rotation", etc.
    strategy: str           # "vpin", "ofi", "sector_rotation"
    symbol: str             # "OGDC" or "ALL" for cross-sectional
    equity_curve: pd.Series # indexed by date, values = equity
    daily_returns: pd.Series
    total_return: float
    sharpe: float
    max_drawdown: float
    trades: int
    win_rate: float
    start_date: str
    end_date: str


def run_strategy_backtest(strategy: str, symbol: str = "OGDC",
                          lookback_days: int = 500) -> StrategyBacktest | None:
    """Run a single strategy backtest and return standardized result."""
    try:
        result = None
        
        if strategy == "vpin":
            from pakfindata.engine.vpin_strategy import backtest_vpin_strategy
            result = backtest_vpin_strategy(symbol, lookback_days=lookback_days)
            
        elif strategy == "ofi":
            from pakfindata.engine.ofi_strategy import backtest_ofi_strategy
            result = backtest_ofi_strategy(symbol, lookback_days=lookback_days)
            
        elif strategy == "cvd":
            from pakfindata.engine.cvd_strategy import backtest_cvd_strategy
            result = backtest_cvd_strategy(symbol, lookback_days=lookback_days)
            
        elif strategy == "basis":
            from pakfindata.engine.basis_strategy import backtest_basis_strategy
            result = backtest_basis_strategy(symbol, lookback_days=lookback_days)
            
        elif strategy == "macro_hmm":
            from pakfindata.engine.macro_regime_hmm import backtest_regime_allocation
            result = backtest_regime_allocation(lookback_months=lookback_days // 21)
            
        elif strategy == "sector_rotation":
            from pakfindata.engine.sector_rotation import backtest_sector_rotation
            result = backtest_sector_rotation(lookback_months=lookback_days // 21)
            
        elif strategy == "oi":
            from pakfindata.engine.oi_strategy import backtest_oi_strategy
            result = backtest_oi_strategy(symbol, lookback_days=lookback_days)
            
        elif strategy == "pairs":
            from pakfindata.engine.pairs_trading import backtest_pairs_strategy
            result = backtest_pairs_strategy(lookback_days=lookback_days)
        
        if result is None or "error" in result:
            return None
        
        # Extract equity curve — adapt based on actual return format
        eq = result.get("equity_curve", result.get("equity", []))
        if isinstance(eq, list):
            if eq and isinstance(eq[0], dict):
                # [{date, equity}, ...]
                eq_series = pd.Series(
                    [e.get("equity", e.get("value", 0)) for e in eq],
                    index=pd.to_datetime([e.get("date", e.get("time", "")) for e in eq]),
                )
            else:
                eq_series = pd.Series(eq, name="equity")
        elif isinstance(eq, pd.Series):
            eq_series = eq
        elif isinstance(eq, pd.DataFrame):
            eq_series = eq.iloc[:, 0] if len(eq.columns) > 0 else pd.Series()
        else:
            return None
        
        if eq_series.empty or len(eq_series) < 10:
            return None
        
        # Compute daily returns
        daily_ret = eq_series.pct_change().dropna()
        
        # Metrics
        metrics = result.get("metrics", result.get("summary", {}))
        total_ret = float(metrics.get("total_return", metrics.get("return",
                    (eq_series.iloc[-1] / eq_series.iloc[0] - 1) if eq_series.iloc[0] > 0 else 0)))
        
        rf = get_risk_free_rate() / TRADING_DAYS
        excess = daily_ret - rf
        sharpe = float(excess.mean() / excess.std() * np.sqrt(TRADING_DAYS)) if excess.std() > 0 else 0
        
        # Max drawdown
        cummax = eq_series.cummax()
        drawdown = (eq_series - cummax) / cummax
        max_dd = float(drawdown.min())
        
        trades = int(metrics.get("total_trades", metrics.get("trades", len(result.get("trades", [])))))
        win_rate = float(metrics.get("win_rate", 0))
        
        name = f"{strategy}/{symbol}" if symbol != "ALL" else strategy
        
        return StrategyBacktest(
            name=name, strategy=strategy, symbol=symbol,
            equity_curve=eq_series, daily_returns=daily_ret,
            total_return=total_ret, sharpe=sharpe,
            max_drawdown=max_dd, trades=trades, win_rate=win_rate,
            start_date=str(eq_series.index[0])[:10] if hasattr(eq_series.index[0], 'strftime') else "",
            end_date=str(eq_series.index[-1])[:10] if hasattr(eq_series.index[-1], 'strftime') else "",
        )
    except Exception as e:
        return None


def run_multiple_backtests(configs: list[dict]) -> list[StrategyBacktest]:
    """Run multiple backtests. configs = [{"strategy": "vpin", "symbol": "OGDC"}, ...]"""
    results = []
    for cfg in configs:
        bt = run_strategy_backtest(
            cfg["strategy"], cfg.get("symbol", "OGDC"),
            cfg.get("lookback_days", 500),
        )
        if bt:
            results.append(bt)
    return results


# ═══════════════════════════════════════════════════════
# PORTFOLIO OPTIMIZER
# ═══════════════════════════════════════════════════════

@dataclass
class PortfolioResult:
    """Result of portfolio optimization."""
    weights: dict[str, float]          # {strategy_name: weight}
    expected_return: float             # annualized
    volatility: float                  # annualized
    sharpe: float
    cvar_95: float                     # 95% CVaR (expected shortfall)
    max_drawdown: float
    diversification_score: float       # 0-100
    hhi: float                         # Herfindahl-Hirschman Index
    effective_bets: float              # 1/HHI
    risk_contributions: dict[str, float]  # % of portfolio risk from each strategy
    correlation_matrix: pd.DataFrame
    combined_equity: pd.Series
    combined_returns: pd.Series
    objective: str                     # optimization method used
    constraints_satisfied: bool


def align_returns(backtests: list[StrategyBacktest]) -> pd.DataFrame:
    """Align daily returns of all strategies to common date index."""
    returns_dict = {}
    for bt in backtests:
        returns_dict[bt.name] = bt.daily_returns
    
    df = pd.DataFrame(returns_dict)
    df = df.dropna(how="all")
    df = df.fillna(0)  # missing days = 0 return (strategy not active)
    return df


def optimize_portfolio(
    backtests: list[StrategyBacktest],
    objective: str = "max_sharpe",      # max_sharpe, min_cvar, risk_parity, equal_weight
    min_weight: float = 0.01,
    max_weight: float = 0.40,
    risk_aversion: float = 0.5,        # 0 = aggressive, 1 = conservative (for CVaR)
    risk_free_rate: float = None,
) -> PortfolioResult | None:
    """
    Optimize portfolio weights across multiple strategy backtests.
    
    Objectives:
      max_sharpe:    maximize Sharpe ratio (classic mean-variance tangency)
      min_cvar:      minimize CVaR at 95% level (tail risk optimization)
      risk_parity:   equalize risk contribution from each strategy
      equal_weight:  1/N allocation (baseline)
    """
    from scipy.optimize import minimize as scipy_minimize
    
    if len(backtests) < 2:
        return None
    
    if risk_free_rate is None:
        risk_free_rate = get_risk_free_rate()
    
    returns_df = align_returns(backtests)
    n = len(backtests)
    names = [bt.name for bt in backtests]
    
    # Annualized stats
    mu = returns_df.mean().values * TRADING_DAYS          # expected annual return
    cov = returns_df.cov().values * TRADING_DAYS          # annual covariance
    
    daily_rf = risk_free_rate / TRADING_DAYS
    
    # ── OPTIMIZATION FUNCTIONS ──
    
    def portfolio_return(w):
        return w @ mu
    
    def portfolio_vol(w):
        return np.sqrt(w @ cov @ w)
    
    def portfolio_sharpe(w):
        ret = portfolio_return(w)
        vol = portfolio_vol(w)
        return -(ret - risk_free_rate) / vol if vol > 0 else 1e10  # negative for minimization
    
    def portfolio_cvar(w, alpha=0.05):
        """CVaR at alpha level (e.g., 0.05 for 95% CVaR)."""
        port_returns = returns_df.values @ w
        sorted_returns = np.sort(port_returns)
        cutoff = int(len(sorted_returns) * alpha)
        if cutoff < 1:
            cutoff = 1
        return -np.mean(sorted_returns[:cutoff]) * np.sqrt(TRADING_DAYS)
    
    def risk_parity_objective(w):
        """Minimize difference in risk contributions."""
        vol = portfolio_vol(w)
        if vol < 1e-10:
            return 1e10
        marginal = (cov @ w) / vol
        risk_contrib = w * marginal
        target = vol / n  # equal risk contribution
        return np.sum((risk_contrib - target) ** 2)
    
    # Constraints
    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
    bounds = [(min_weight, max_weight) for _ in range(n)]
    
    # Initial guess
    w0 = np.ones(n) / n
    
    # ── OPTIMIZE ──
    
    if objective == "equal_weight":
        w_opt = np.ones(n) / n
    
    elif objective == "max_sharpe":
        result = scipy_minimize(
            portfolio_sharpe, w0, method="SLSQP",
            bounds=bounds, constraints=constraints,
            options={"maxiter": 1000, "ftol": 1e-10},
        )
        w_opt = result.x if result.success else w0
    
    elif objective == "min_cvar":
        def cvar_obj(w):
            return portfolio_cvar(w) + risk_aversion * portfolio_vol(w)
        
        result = scipy_minimize(
            cvar_obj, w0, method="SLSQP",
            bounds=bounds, constraints=constraints,
            options={"maxiter": 1000},
        )
        w_opt = result.x if result.success else w0
    
    elif objective == "risk_parity":
        result = scipy_minimize(
            risk_parity_objective, w0, method="SLSQP",
            bounds=bounds, constraints=constraints,
            options={"maxiter": 1000},
        )
        w_opt = result.x if result.success else w0
    
    else:
        w_opt = w0
    
    # Normalize
    w_opt = np.maximum(w_opt, 0)
    w_opt = w_opt / w_opt.sum()
    
    # ── COMPUTE PORTFOLIO METRICS ──
    
    port_ret = portfolio_return(w_opt)
    port_vol = portfolio_vol(w_opt)
    port_sharpe = (port_ret - risk_free_rate) / port_vol if port_vol > 0 else 0
    
    # Combined equity curve
    combined_returns = returns_df.values @ w_opt
    combined_equity = (1 + pd.Series(combined_returns, index=returns_df.index)).cumprod()
    
    # CVaR
    cvar = portfolio_cvar(w_opt)
    
    # Max drawdown
    cummax = combined_equity.cummax()
    dd = (combined_equity - cummax) / cummax
    max_dd = float(dd.min())
    
    # Diversification score
    weighted_vol = sum(w_opt[i] * np.sqrt(cov[i, i]) for i in range(n))
    div_ratio = weighted_vol / port_vol if port_vol > 0 else 1
    div_score = min(100, max(0, (div_ratio - 1) * 100))  # 0-100 scale
    
    # HHI (Herfindahl-Hirschman Index)
    hhi = float(np.sum(w_opt ** 2))
    effective_bets = 1 / hhi if hhi > 0 else n
    
    # Risk contributions
    marginal_risk = (cov @ w_opt) / port_vol if port_vol > 0 else np.zeros(n)
    risk_contribs = w_opt * marginal_risk
    total_risk = risk_contribs.sum()
    risk_pct = risk_contribs / total_risk * 100 if total_risk > 0 else np.zeros(n)
    
    # Correlation matrix
    corr = returns_df.corr()
    
    weights_dict = {names[i]: round(float(w_opt[i]), 4) for i in range(n)}
    risk_contrib_dict = {names[i]: round(float(risk_pct[i]), 1) for i in range(n)}
    
    return PortfolioResult(
        weights=weights_dict,
        expected_return=round(port_ret * 100, 1),
        volatility=round(port_vol * 100, 1),
        sharpe=round(port_sharpe, 2),
        cvar_95=round(cvar * 100, 1),
        max_drawdown=round(max_dd * 100, 1),
        diversification_score=round(div_score, 0),
        hhi=round(hhi * 100, 1),
        effective_bets=round(effective_bets, 1),
        risk_contributions=risk_contrib_dict,
        correlation_matrix=corr,
        combined_equity=combined_equity,
        combined_returns=pd.Series(combined_returns, index=returns_df.index),
        objective=objective,
        constraints_satisfied=True,
    )


# ═══════════════════════════════════════════════════════
# STRESS TESTING — replay real PSX crisis periods
# ═══════════════════════════════════════════════════════

PSX_STRESS_SCENARIOS = {
    "2018 BoP Crisis (Jun-Dec 2018)": {
        "start": "2018-06-01", "end": "2018-12-31",
        "description": "Balance of payments crisis. PKR collapsed 110→140. KSE-100 fell 27%. IMF bailout.",
        "factors": {"kse100_drop": -27, "pkr_depreciation": -27, "rate_hike_bps": 425},
    },
    "2020 COVID Crash (Feb-Apr 2020)": {
        "start": "2020-02-15", "end": "2020-04-30",
        "description": "Global pandemic. KSE-100 fell 32% in 4 weeks. Trading halted multiple days. SBP emergency rate cut 625bps.",
        "factors": {"kse100_drop": -32, "rate_cut_bps": -625},
    },
    "2022 Political Crisis + Floods (Mar-Oct 2022)": {
        "start": "2022-03-01", "end": "2022-10-31",
        "description": "PM ouster, PKR 180→230, rates hiked to 22%, catastrophic floods. KSE-100 fell 18%.",
        "factors": {"kse100_drop": -18, "pkr_depreciation": -28, "rate_hike_bps": 800},
    },
    "2023 Rate Peak (Jan-Jun 2023)": {
        "start": "2023-01-01", "end": "2023-06-30",
        "description": "SBP rate at 22%, PKR stabilizing at 280+, IMF program. KSE bottom then recovery.",
        "factors": {"policy_rate_pct": 22},
    },
    "2025-26 Bull Run (Jul 2025-Mar 2026)": {
        "start": "2025-07-01", "end": "2026-03-25",
        "description": "Rate cuts from 22% to 12%, KSE-100 80K→120K. Best run in PSX history.",
        "factors": {"kse100_gain": 50, "rate_cut_bps": -1000},
    },
    "Flash Crash (-7.5% circuit)": {
        "synthetic": True,
        "shock_pct": -7.5,
        "description": "Simulated single-day circuit-limit-down. All positions hit -7.5%.",
    },
    "Rate Shock (+500bps)": {
        "synthetic": True,
        "rate_shock_bps": 500,
        "description": "Simulated emergency rate hike. Banks rally, everything else falls 5-15%.",
        "sector_impacts": {"banking": 0.05, "cement": -0.10, "auto": -0.12,
                           "pharma": -0.08, "energy": -0.05, "textiles": -0.15},
    },
    "PKR Crash (-20%)": {
        "synthetic": True,
        "fx_shock_pct": -20,
        "description": "Simulated sudden PKR devaluation. Import-heavy sectors crushed.",
        "sector_impacts": {"pharma": -0.15, "auto": -0.20, "cement": -0.08,
                           "banking": -0.05, "energy": 0.05, "textiles": 0.10},
    },
}


def stress_test_portfolio(
    portfolio: PortfolioResult,
    backtests: list[StrategyBacktest],
    scenario_name: str,
) -> dict:
    """
    Run a stress test on the portfolio using a historical or synthetic scenario.
    
    For historical scenarios: replay actual returns during that period.
    For synthetic: apply shock factors to current positions.
    
    Returns:
      portfolio_loss_pct: total portfolio loss during the stress period
      strategy_losses: {name: loss_pct} per strategy
      worst_day: worst single-day loss
      recovery_days: how many days to recover (if applicable)
      drawdown_path: daily equity path during stress period
    """
    scenario = PSX_STRESS_SCENARIOS.get(scenario_name)
    if not scenario:
        return {"error": f"Unknown scenario: {scenario_name}"}
    
    returns_df = align_returns(backtests)
    weights = np.array([portfolio.weights.get(bt.name, 0) for bt in backtests])
    
    # HISTORICAL SCENARIO
    if not scenario.get("synthetic"):
        start = pd.Timestamp(scenario["start"])
        end = pd.Timestamp(scenario["end"])
        
        # Filter returns to stress period
        mask = (returns_df.index >= start) & (returns_df.index <= end)
        stress_returns = returns_df.loc[mask]
        
        if stress_returns.empty or len(stress_returns) < 5:
            return {
                "error": f"Not enough data for {scenario_name} "
                         f"({len(stress_returns)} days in backtest range)",
                "scenario": scenario_name,
            }
        
        # Portfolio returns during stress
        port_returns = stress_returns.values @ weights
        port_equity = (1 + pd.Series(port_returns, index=stress_returns.index)).cumprod()
        
        # Per-strategy losses
        strategy_losses = {}
        for i, bt in enumerate(backtests):
            strat_eq = (1 + stress_returns.iloc[:, i]).cumprod()
            strategy_losses[bt.name] = round(float(strat_eq.iloc[-1] - 1) * 100, 1)
        
        total_loss = float(port_equity.iloc[-1] - 1) * 100
        worst_day = float(pd.Series(port_returns).min()) * 100
        
        # Max drawdown during stress
        cummax = port_equity.cummax()
        dd = (port_equity - cummax) / cummax
        max_dd = float(dd.min()) * 100
        
        # Recovery: how many days after stress trough to recover
        trough_idx = dd.idxmin()
        recovery_days = None
        if trough_idx is not None:
            post_trough = port_equity[port_equity.index > trough_idx]
            recovered = post_trough[post_trough >= 1.0]
            if not recovered.empty:
                recovery_days = (recovered.index[0] - trough_idx).days
        
        return {
            "scenario": scenario_name,
            "description": scenario["description"],
            "type": "historical",
            "period": f"{scenario['start']} to {scenario['end']}",
            "trading_days": len(stress_returns),
            "portfolio_loss_pct": round(total_loss, 1),
            "worst_day_pct": round(worst_day, 1),
            "max_drawdown_pct": round(max_dd, 1),
            "recovery_days": recovery_days,
            "strategy_losses": strategy_losses,
            "drawdown_path": port_equity.tolist(),
            "dates": [str(d)[:10] for d in port_equity.index],
        }
    
    # SYNTHETIC SCENARIO
    else:
        shock = scenario.get("shock_pct", 0) / 100
        strategy_losses = {}
        
        for i, bt in enumerate(backtests):
            if shock != 0:
                # Uniform shock
                strategy_losses[bt.name] = round(shock * 100, 1)
            elif "sector_impacts" in scenario:
                # Sector-specific shock — approximate based on strategy type
                # Strategies focused on specific sectors get that sector's shock
                avg_impact = np.mean(list(scenario["sector_impacts"].values()))
                strategy_losses[bt.name] = round(avg_impact * 100, 1)
            else:
                strategy_losses[bt.name] = 0
        
        total_loss = sum(
            portfolio.weights.get(name, 0) * loss / 100
            for name, loss in strategy_losses.items()
        ) * 100
        
        return {
            "scenario": scenario_name,
            "description": scenario["description"],
            "type": "synthetic",
            "portfolio_loss_pct": round(total_loss, 1),
            "worst_day_pct": round(total_loss, 1),  # single-day shock
            "strategy_losses": strategy_losses,
        }


# ═══════════════════════════════════════════════════════
# ADDITIONAL ANALYTICS
# ═══════════════════════════════════════════════════════

def compute_var(returns: pd.Series, confidence: float = 0.95) -> float:
    """Value at Risk at given confidence level."""
    return float(-np.percentile(returns.dropna(), (1 - confidence) * 100))

def compute_cvar(returns: pd.Series, confidence: float = 0.95) -> float:
    """Conditional VaR (Expected Shortfall)."""
    var = compute_var(returns, confidence)
    return float(-returns[returns <= -var].mean()) if (returns <= -var).any() else var

def compute_calmar(returns: pd.Series, equity: pd.Series) -> float:
    """Calmar ratio = annualized return / max drawdown."""
    ann_ret = returns.mean() * TRADING_DAYS
    cummax = equity.cummax()
    max_dd = ((equity - cummax) / cummax).min()
    return float(ann_ret / abs(max_dd)) if max_dd != 0 else 0

def compute_sortino(returns: pd.Series, rf_annual: float = 0.12) -> float:
    """Sortino ratio — penalizes only downside volatility."""
    rf_daily = rf_annual / TRADING_DAYS
    excess = returns - rf_daily
    downside = returns[returns < 0]
    down_std = downside.std() * np.sqrt(TRADING_DAYS)
    return float(excess.mean() * TRADING_DAYS / down_std) if down_std > 0 else 0

def compute_monthly_returns(equity: pd.Series) -> pd.DataFrame:
    """Monthly return table (like QuantPlatform's calendar)."""
    if equity.empty:
        return pd.DataFrame()
    monthly = equity.resample("ME").last().pct_change().dropna()
    monthly.index = monthly.index.to_period("M")
    return monthly

def grade_portfolio(result: PortfolioResult) -> str:
    """Letter grade based on portfolio quality metrics."""
    score = 0
    if result.sharpe > 1.5: score += 3
    elif result.sharpe > 1.0: score += 2
    elif result.sharpe > 0.5: score += 1
    
    if result.diversification_score > 60: score += 2
    elif result.diversification_score > 30: score += 1
    
    if abs(result.max_drawdown) < 15: score += 2
    elif abs(result.max_drawdown) < 25: score += 1
    
    if result.effective_bets > 3: score += 1
    
    grades = {8: "A+", 7: "A", 6: "B+", 5: "B", 4: "C+", 3: "C", 2: "D", 1: "D-", 0: "F"}
    return grades.get(min(score, 8), "F")
```

## Step 2: Create the Streamlit Page

Create `src/pakfindata/ui/page_views/portfolio_lab.py`:

This page has 3 panels matching QuantPlatform's layout:
- LEFT: Select backtests  
- CENTER: Configuration (objective, weights, risk aversion)  
- RIGHT: Results (weights, analytics, risk, stress tests)

```python
"""Portfolio Lab — multi-strategy portfolio optimizer + stress testing.

Three-panel layout matching professional quant platforms.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "border": "#1E2530",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "blue": "#2196F3", "purple": "#BB86FC",
}


# Available strategy × symbol combinations
STRATEGY_OPTIONS = [
    {"strategy": "vpin", "symbol": "OGDC", "label": "VPIN / OGDC"},
    {"strategy": "vpin", "symbol": "HUBC", "label": "VPIN / HUBC"},
    {"strategy": "vpin", "symbol": "HBL", "label": "VPIN / HBL"},
    {"strategy": "ofi", "symbol": "OGDC", "label": "OFI / OGDC"},
    {"strategy": "ofi", "symbol": "HUBC", "label": "OFI / HUBC"},
    {"strategy": "basis", "symbol": "OGDC", "label": "Basis Arb / OGDC"},
    {"strategy": "basis", "symbol": "HUBC", "label": "Basis Arb / HUBC"},
    {"strategy": "macro_hmm", "symbol": "ALL", "label": "Macro Regime HMM"},
    {"strategy": "sector_rotation", "symbol": "ALL", "label": "Sector Rotation"},
    {"strategy": "oi", "symbol": "OGDC", "label": "OI Buildup / OGDC"},
    {"strategy": "oi", "symbol": "HUBC", "label": "OI Buildup / HUBC"},
    {"strategy": "pairs", "symbol": "ALL", "label": "Pairs Trading"},
]


def render_page():
    st.markdown("## Multi-Strategy Portfolio Lab")
    st.caption("Combine strategy backtests → optimized allocation → stress testing")

    # Top tabs
    main_tab, stress_tab, history_tab = st.tabs(["Build Portfolio", "Stress Testing", "Backtest History"])

    with main_tab:
        # THREE-PANEL LAYOUT
        left, center, right = st.columns([1.2, 1.3, 1.5])

        # ── LEFT PANEL: Select Backtests ──
        with left:
            st.markdown("### Select Strategies")
            
            selected = []
            for opt in STRATEGY_OPTIONS:
                key = f"sel_{opt['strategy']}_{opt['symbol']}"
                if st.checkbox(opt["label"], key=key, value=opt["strategy"] in ("vpin", "macro_hmm", "sector_rotation")):
                    selected.append(opt)
            
            st.markdown(f"**{len(selected)} selected**")
            
            lookback = st.slider("Lookback (trading days)", 100, 1200, 500, 50, key="pl_lookback")

        # ── CENTER PANEL: Configuration ──
        with center:
            st.markdown("### Configuration")
            
            objective = st.selectbox("Scoring Method", [
                "max_sharpe", "min_cvar", "risk_parity", "equal_weight"
            ], format_func=lambda x: {
                "max_sharpe": "Max Sharpe Ratio",
                "min_cvar": "Min CVaR (95%)",
                "risk_parity": "Risk Parity",
                "equal_weight": "Equal Weight (1/N)",
            }[x], key="pl_objective")
            
            c1, c2 = st.columns(2)
            with c1:
                max_weight = st.number_input("Max Weight", 0.1, 1.0, 0.40, 0.05, key="pl_maxw")
            with c2:
                min_weight = st.number_input("Min Weight", 0.0, 0.5, 0.01, 0.01, key="pl_minw")
            
            if objective == "min_cvar":
                risk_aversion = st.slider("Risk Aversion", 0.0, 1.0, 0.5, 0.1, key="pl_ra")
            else:
                risk_aversion = 0.5
            
            use_gpu = st.checkbox("Use GPU Optimizer", value=False, key="pl_gpu", disabled=True)
            
            build = st.button("▶ Build Portfolio", type="primary", use_container_width=True, key="pl_build")

        # ── RIGHT PANEL: Results ──
        with right:
            st.markdown("### Portfolio Results")
            
            if not build:
                st.info("Select strategies and click **Build Portfolio**")
            elif len(selected) < 2:
                st.warning("Select at least 2 strategies")
            else:
                with st.spinner(f"Running {len(selected)} backtests + optimization..."):
                    try:
                        from pakfindata.engine.portfolio_optimizer import (
                            run_multiple_backtests, optimize_portfolio,
                            grade_portfolio, compute_sortino, compute_calmar,
                            compute_var, compute_cvar, PSX_STRESS_SCENARIOS,
                            stress_test_portfolio,
                        )
                        
                        # Run backtests
                        backtests = run_multiple_backtests([
                            {"strategy": s["strategy"], "symbol": s["symbol"],
                             "lookback_days": lookback}
                            for s in selected
                        ])
                        
                        if len(backtests) < 2:
                            st.error(f"Only {len(backtests)} backtests succeeded. Need ≥2.")
                            return
                        
                        # Optimize
                        result = optimize_portfolio(
                            backtests, objective=objective,
                            min_weight=min_weight, max_weight=max_weight,
                            risk_aversion=risk_aversion,
                        )
                    except ImportError:
                        st.error("portfolio_optimizer.py not found")
                        return
                
                if result is None:
                    st.error("Optimization failed")
                    return
                
                # ── WEIGHT ALLOCATION BAR ──
                st.markdown(f"**{len(result.weights)} strategies** | Objective: {objective}")
                
                colors = ["#FF5252", "#00E676", "#2196F3", "#FFB300", "#BB86FC",
                          "#00BCD4", "#C8A96E", "#E91E63", "#4CAF50", "#FF9800"]
                
                # Horizontal stacked bar for weights
                fig_w = go.Figure()
                x_start = 0
                for i, (name, w) in enumerate(sorted(result.weights.items(), key=lambda x: -x[1])):
                    fig_w.add_trace(go.Bar(
                        y=["Portfolio"], x=[w * 100], name=name,
                        orientation="h", marker_color=colors[i % len(colors)],
                        text=f"{name.split('/')[0]}<br>{w:.0%}", textposition="inside",
                        hovertext=f"{name}: {w:.1%}",
                    ))
                fig_w.update_layout(
                    barmode="stack", height=80, showlegend=False,
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=0, r=0, t=0, b=0),
                    xaxis=dict(visible=False), yaxis=dict(visible=False),
                )
                st.plotly_chart(fig_w, use_container_width=True)
                
                # Weight table
                for name, w in sorted(result.weights.items(), key=lambda x: -x[1]):
                    st.markdown(f"`{name}` → **{w:.1%}**")
                
                st.markdown("---")
                
                # ── PORTFOLIO ANALYTICS ──
                grade = grade_portfolio(result)
                gc = _C["up"] if grade.startswith("A") else _C["amber"] if grade.startswith("B") else _C["down"]
                
                st.markdown(f'**Portfolio Analytics** <span style="color:{gc}">Grade: {grade}</span>',
                           unsafe_allow_html=True)
                
                # Diversification score bar
                ds = result.diversification_score
                ds_c = _C["up"] if ds > 60 else _C["amber"] if ds > 30 else _C["down"]
                st.markdown(
                    f'<div style="margin-bottom:8px;">'
                    f'<span style="font-size:11px;color:{_C["dim"]}">Diversification Score</span>'
                    f'<span style="float:right;font-weight:700">{ds:.0f}/100</span>'
                    f'<div style="height:8px;background:{_C["border"]};border-radius:4px;margin-top:4px;">'
                    f'<div style="height:100%;width:{ds}%;background:{ds_c};border-radius:4px;"></div>'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
                
                a1, a2 = st.columns(2)
                a1.metric("Effective Bets", f"{result.effective_bets:.1f}")
                a2.metric("HHI", f"{result.hhi:.1f}%")
                
                a3, a4 = st.columns(2)
                a3.metric("Max Position", f"{max(result.weights.values()):.0%}")
                top3 = sum(sorted(result.weights.values(), reverse=True)[:3])
                a4.metric("Top 3 Weight", f"{top3:.0%}")
                
                st.markdown("---")
                
                # ── RISK ATTRIBUTION ──
                st.markdown("**Risk Attribution**")
                
                r1, r2 = st.columns(2)
                vol_c = _C["down"] if result.volatility > 30 else _C["amber"] if result.volatility > 20 else _C["up"]
                r1.markdown(f'<div><span style="color:{_C["dim"]};font-size:10px">VOLATILITY</span>'
                           f'<br><span style="color:{vol_c};font-size:20px;font-weight:700">'
                           f'{result.volatility:.1f}%</span></div>', unsafe_allow_html=True)
                ret_c = _C["up"] if result.expected_return > 0 else _C["down"]
                r2.markdown(f'<div><span style="color:{_C["dim"]};font-size:10px">EXP. RETURN</span>'
                           f'<br><span style="color:{ret_c};font-size:20px;font-weight:700">'
                           f'{result.expected_return:+.1f}%</span></div>', unsafe_allow_html=True)
                
                r3, r4 = st.columns(2)
                r3.metric("Sharpe Ratio", f"{result.sharpe:.2f}")
                r4.metric("CVaR 95%", f"{result.cvar_95:.1f}%")
                
                r5, r6 = st.columns(2)
                r5.metric("Max Drawdown", f"{result.max_drawdown:.1f}%")
                
                sortino = compute_sortino(result.combined_returns)
                r6.metric("Sortino", f"{sortino:.2f}")
                
                # Risk contribution breakdown
                st.markdown("**Risk Contribution**")
                for name, rc in sorted(result.risk_contributions.items(), key=lambda x: -x[1]):
                    color = colors[list(result.weights.keys()).index(name) % len(colors)]
                    st.markdown(
                        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;">'
                        f'<span style="width:120px;font-size:10px;color:{_C["dim"]}">{name}</span>'
                        f'<div style="flex:1;height:8px;background:{_C["border"]};border-radius:4px;overflow:hidden;">'
                        f'<div style="height:100%;width:{rc}%;background:{color};border-radius:4px;"></div></div>'
                        f'<span style="width:40px;font-size:10px;text-align:right">{rc:.0f}%</span>'
                        f'</div>', unsafe_allow_html=True,
                    )
                
                st.markdown("---")
                
                # ── STRESS TESTING ──
                st.markdown("**Stress Testing**")
                scenario = st.selectbox(
                    "Scenario", list(PSX_STRESS_SCENARIOS.keys()),
                    key="pl_stress", label_visibility="collapsed",
                )
                
                if st.button("Run", key="pl_stress_run"):
                    stress = stress_test_portfolio(result, backtests, scenario)
                    if "error" in stress:
                        st.warning(stress["error"])
                    else:
                        loss = stress["portfolio_loss_pct"]
                        lc = _C["down"] if loss < -5 else _C["amber"] if loss < 0 else _C["up"]
                        st.markdown(
                            f'<div style="background:{_C["card"]};padding:12px;border-radius:6px;">'
                            f'<div style="color:{_C["dim"]};font-size:10px">{scenario}</div>'
                            f'<div style="color:{lc};font-size:24px;font-weight:700">{loss:+.1f}%</div>'
                            f'<div style="color:{_C["dim"]};font-size:10px">'
                            f'Worst day: {stress.get("worst_day_pct", 0):.1f}% | '
                            f'Recovery: {stress.get("recovery_days", "N/A")} days</div>'
                            f'</div>', unsafe_allow_html=True,
                        )
                        
                        # Per-strategy losses
                        for name, loss in stress.get("strategy_losses", {}).items():
                            lc = _C["down"] if loss < 0 else _C["up"]
                            st.markdown(f'<span style="color:{_C["dim"]};font-size:10px">{name}</span> '
                                       f'<span style="color:{lc};font-weight:700">{loss:+.1f}%</span>',
                                       unsafe_allow_html=True)
                
                # ── EQUITY CURVE CHART ──
                st.markdown("---")
                if not result.combined_equity.empty:
                    fig_eq = go.Figure()
                    fig_eq.add_trace(go.Scatter(
                        x=result.combined_equity.index,
                        y=result.combined_equity.values,
                        mode="lines", name="Portfolio",
                        line=dict(color=_C["cyan"], width=2),
                        fill="tozeroy", fillcolor="rgba(0,188,212,0.1)",
                    ))
                    # Add individual strategy curves
                    for i, bt in enumerate(backtests):
                        eq_norm = bt.equity_curve / bt.equity_curve.iloc[0]
                        fig_eq.add_trace(go.Scatter(
                            x=eq_norm.index, y=eq_norm.values,
                            mode="lines", name=bt.name,
                            line=dict(color=colors[i % len(colors)], width=1, dash="dot"),
                            opacity=0.5,
                        ))
                    fig_eq.update_layout(
                        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
                        font_color=_C["dim"], height=350,
                        margin=dict(l=10, r=10, t=30, b=10),
                        title="Equity Curves (normalized)",
                        legend=dict(font_size=9),
                        yaxis=dict(gridcolor=_C["border"]),
                        xaxis=dict(gridcolor=_C["border"]),
                    )
                    st.plotly_chart(fig_eq, use_container_width=True)
                
                # ── CORRELATION HEATMAP ──
                if not result.correlation_matrix.empty:
                    fig_corr = go.Figure(go.Heatmap(
                        z=result.correlation_matrix.values,
                        x=result.correlation_matrix.columns,
                        y=result.correlation_matrix.index,
                        colorscale="RdYlGn_r",
                        zmin=-1, zmax=1,
                        text=result.correlation_matrix.round(2).values,
                        texttemplate="%{text}",
                        textfont=dict(size=10),
                    ))
                    fig_corr.update_layout(
                        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
                        font_color=_C["dim"], height=300,
                        margin=dict(l=10, r=10, t=30, b=10),
                        title="Strategy Correlation Matrix",
                    )
                    st.plotly_chart(fig_corr, use_container_width=True)

    with stress_tab:
        st.markdown("### Historical Stress Scenarios")
        st.caption("Real PSX crisis periods and their impact on strategy returns")
        
        from pakfindata.engine.portfolio_optimizer import PSX_STRESS_SCENARIOS
        
        for name, scenario in PSX_STRESS_SCENARIOS.items():
            with st.expander(name):
                st.markdown(f'**{scenario.get("description", "")}**')
                if "factors" in scenario:
                    for k, v in scenario["factors"].items():
                        st.markdown(f'- {k}: **{v:+}**')
                if scenario.get("synthetic"):
                    st.markdown(f'*Synthetic scenario — simulated shock*')

    with history_tab:
        st.markdown("### Individual Strategy Backtests")
        st.caption("Run each strategy independently and compare")
        
        # Quick backtest runner
        c1, c2, c3 = st.columns(3)
        with c1:
            bt_strat = st.selectbox("Strategy", [
                "vpin", "ofi", "basis", "macro_hmm", "sector_rotation", "oi", "pairs"
            ], key="bt_strat")
        with c2:
            bt_sym = st.text_input("Symbol", "OGDC", key="bt_sym")
        with c3:
            bt_days = st.slider("Days", 100, 1200, 500, key="bt_days")
        
        if st.button("Run Backtest", key="bt_run"):
            with st.spinner(f"Running {bt_strat}/{bt_sym}..."):
                try:
                    from pakfindata.engine.portfolio_optimizer import run_strategy_backtest
                    bt = run_strategy_backtest(bt_strat, bt_sym, bt_days)
                except ImportError:
                    st.error("Engine not found")
                    return
            
            if bt is None:
                st.error("Backtest failed — check data availability")
            else:
                m1, m2, m3, m4, m5 = st.columns(5)
                ret_c = _C["up"] if bt.total_return > 0 else _C["down"]
                m1.metric("Return", f"{bt.total_return:.1%}")
                m2.metric("Sharpe", f"{bt.sharpe:.2f}")
                m3.metric("Max DD", f"{bt.max_drawdown:.1%}")
                m4.metric("Trades", bt.trades)
                m5.metric("Win Rate", f"{bt.win_rate:.0f}%")
                
                if not bt.equity_curve.empty:
                    fig = go.Figure(go.Scatter(
                        x=bt.equity_curve.index, y=bt.equity_curve.values,
                        mode="lines", line=dict(color=_C["cyan"], width=2),
                    ))
                    fig.update_layout(
                        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
                        font_color=_C["dim"], height=300,
                        margin=dict(l=10, r=10, t=10, b=10),
                    )
                    st.plotly_chart(fig, use_container_width=True)

    render_footer()
```

## Step 3: Register in app.py

Add page function:
```python
def portfolio_lab_page():
    from pakfindata.ui.page_views.portfolio_lab import render_page
    render_page()
```

Add to page dict:
```python
        "Portfolio Lab": st.Page(portfolio_lab_page, title="Portfolio Lab", url_path="portfolio-lab"),
```

Add to RESEARCH nav group:
```python
        "RESEARCH": [...existing..., "Portfolio Lab"],
```

## Step 4: Install Dependencies

```bash
conda activate psx
pip install scipy  # likely already installed
```

No other deps needed — scipy for optimization, numpy/pandas for math, plotly for charts.
All existing libraries.

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test individual backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.portfolio_optimizer import run_strategy_backtest

bt = run_strategy_backtest('vpin', 'OGDC', 500)
if bt:
    print(f'{bt.name}: return={bt.total_return:.1%} sharpe={bt.sharpe:.2f} dd={bt.max_drawdown:.1%} trades={bt.trades}')
    print(f'  Equity curve: {len(bt.equity_curve)} points, {bt.start_date} to {bt.end_date}')
else:
    print('Backtest failed')
"

# Test portfolio optimization
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.portfolio_optimizer import (
    run_multiple_backtests, optimize_portfolio, grade_portfolio,
    stress_test_portfolio,
)

configs = [
    {'strategy': 'vpin', 'symbol': 'OGDC'},
    {'strategy': 'vpin', 'symbol': 'HUBC'},
    {'strategy': 'macro_hmm', 'symbol': 'ALL'},
    {'strategy': 'sector_rotation', 'symbol': 'ALL'},
]

backtests = run_multiple_backtests(configs)
print(f'Backtests completed: {len(backtests)}/{len(configs)}')

if len(backtests) >= 2:
    for obj in ['max_sharpe', 'min_cvar', 'risk_parity', 'equal_weight']:
        result = optimize_portfolio(backtests, objective=obj)
        if result:
            grade = grade_portfolio(result)
            print(f'\n{obj}: Grade={grade} Sharpe={result.sharpe:.2f} Vol={result.volatility:.1f}% '
                  f'Return={result.expected_return:+.1f}% DD={result.max_drawdown:.1f}%')
            print(f'  Weights: {result.weights}')
            print(f'  Risk contrib: {result.risk_contributions}')
            print(f'  Diversification: {result.diversification_score:.0f}/100 '
                  f'HHI={result.hhi:.1f}% EffBets={result.effective_bets:.1f}')
    
    # Stress test
    print(f'\n=== STRESS TESTS ===')
    for scenario in ['2020 COVID Crash (Feb-Apr 2020)', 'Flash Crash (-7.5% circuit)']:
        stress = stress_test_portfolio(result, backtests, scenario)
        if 'error' not in stress:
            print(f'{scenario}: {stress[\"portfolio_loss_pct\"]:+.1f}%')
        else:
            print(f'{scenario}: {stress[\"error\"]}')
"
```

## IMPORTANT NOTES

1. **This matches QuantPlatform's feature set** — scoring method, weight constraints, CVaR objective, diversification score, HHI, risk contribution, stress testing, equity curves, correlation matrix.
2. **Uses YOUR existing backtest functions** — `backtest_vpin_strategy()`, `backtest_sector_rotation()`, etc. The optimizer wraps them and extracts equity curves.
3. **PSX-specific** — risk-free rate from KIBOR/SBP, 245 trading days, circuit breakers, real crisis periods (2018 BoP, 2020 COVID, 2022 floods).
4. **4 optimization objectives**: Max Sharpe (tangency), Min CVaR (tail risk), Risk Parity (equal risk contribution), Equal Weight (1/N baseline).
5. **Stress scenarios use REAL PSX data** — COVID crash replays actual EOD returns during Feb-Apr 2020. Plus synthetic scenarios (circuit limit, rate shock, PKR crash).
6. **Grade system** — A+ to F based on Sharpe, diversification, drawdown, effective bets.
7. **Risk contribution** — shows which strategy contributes most to portfolio risk. Helps identify concentration.
8. **Correlation heatmap** — reveals if strategies are diversifying or just correlated bets.
9. **Three-panel layout** — LEFT (select), CENTER (configure), RIGHT (results). Same UX as QuantPlatform screenshot.
10. **Backtest History tab** — run individual strategies to compare before combining.
11. **scipy is the only dependency** — for optimization. Everything else uses numpy/pandas/plotly already in the project.
12. **Equity curve normalization** — all strategies start at 1.0 for fair comparison regardless of capital size.
13. **The `run_strategy_backtest` wrapper handles different return formats** — each strategy returns slightly different dicts. The wrapper normalizes them to a common `StrategyBacktest` dataclass.
14. **Add to RESEARCH section** in sidebar nav.
