# Claude Code Prompt: Strategy 8 — OI Buildup/Unwind Trading

## Context

pakfindata's Derivatives page already shows real OI from PSX DFC XLS files, a buildup/unwind 
matrix, and a rollover tracker. This strategy automates the classic OI interpretation matrix 
into tradeable signals with entry/exit rules and rollover-aware positioning.

**The OI Matrix (Wyckoff meets derivatives):**

| Price | OI | Interpretation | Signal |
|-------|-----|---------------|--------|
| ↑ Up  | ↑ Up | **Long Buildup** — new longs entering, bullish | BUY |
| ↑ Up  | ↓ Down | **Short Covering** — shorts exiting, rally may exhaust | HOLD/EXIT LONG |
| ↓ Down | ↑ Up | **Short Buildup** — new shorts entering, bearish | SELL |
| ↓ Down | ↓ Down | **Long Unwinding** — longs exiting, decline may exhaust | HOLD/EXIT SHORT |

**Why it works on PSX:** 
- PSX futures have physical delivery → OI changes are real commitment, not just speculation
- Low institutional participation → OI signals persist for days
- Monthly expiry cycle creates predictable rollover patterns
- DFC XLS files give REAL OI (not estimated from volume like on many platforms)

## What already exists

```bash
# Find existing OI/derivatives code
grep -rn "open_interest\|OI\|buildup\|unwind\|rollover\|DFC\|futures" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -30

# Check derivatives data tables
python3 -c "
import duckdb, sqlite3

# DuckDB
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in con.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=\"main\"').fetchall():
    tl = t[0].lower()
    if any(k in tl for k in ['fut','contract','oi','open_interest','deriv','dfc','odd']):
        count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
        cols = [c[0] for c in con.execute(f'DESCRIBE {t[0]}').fetchall()]
        print(f'DuckDB {t[0]}: {count:,} — {cols[:10]}')
con.close()

# SQLite
scon = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for t in [r[0] for r in scon.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]:
    tl = t.lower()
    if any(k in tl for k in ['fut','contract','oi','open_interest','deriv','dfc','odd']):
        count = scon.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[1] for r in scon.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'SQLite {t}: {count:,} — {cols[:10]}')
scon.close()
"

# Sample the OI data — see actual structure
python3 -c "
import duckdb, sqlite3

# Try DuckDB first
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in ['futures_oi','dfc_data','contracts_oi','oi_daily']:
    try:
        df = con.execute(f'SELECT * FROM {t} ORDER BY ROWID DESC LIMIT 5').df()
        print(f'\n{t}:')
        print(df.to_string())
        break
    except: pass
con.close()

# Try SQLite
scon = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for t in [r[0] for r in scon.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]:
    if 'oi' in t.lower() or 'fut' in t.lower() or 'dfc' in t.lower():
        try:
            import pandas as pd
            df = pd.read_sql(f'SELECT * FROM {t} ORDER BY ROWID DESC LIMIT 5', scon)
            print(f'\nSQLite {t}:')
            print(df.to_string())
        except: pass
scon.close()
"

# Check DFC XLS files on disk
ls /mnt/e/psxdata/downloads/daily/*/dfc* 2>/dev/null | tail -10
find /mnt/e/psxdata/downloads -name "*dfc*" -o -name "*DFC*" -o -name "*future*" | head -10
```

**READ ALL OUTPUT — identify exact table names, column names, date ranges, and DFC file locations before proceeding.**

## Step 1: Create the OI Strategy Engine

Create `src/pakfindata/engine/oi_strategy.py`:

```python
"""
OI Buildup/Unwind Trading Strategy.

Uses the classic Open Interest interpretation matrix to generate
directional trading signals from PSX futures OI data.

OI Matrix:
  Price UP + OI UP     = LONG BUILDUP    → Strong BUY
  Price UP + OI DOWN   = SHORT COVERING  → Weak rally, caution
  Price DOWN + OI UP   = SHORT BUILDUP   → Strong SELL
  Price DOWN + OI DOWN = LONG UNWINDING  → Weak decline, caution

Enhanced with:
  - Multi-day confirmation (require 2+ consecutive days of same signal)
  - Volume filter (high volume + OI change = higher conviction)
  - Rollover awareness (reduce signals near expiry)
  - Spot-futures basis confirmation (basis direction aligns with OI signal)
  - Historical OI percentile (is OI unusually high or low?)

PSX-Specific:
  - Real OI from DFC XLS files (not estimated)
  - Monthly expiry: last Thursday of month
  - Physical delivery = OI represents real commitment
  - Rollover window: ~3-5 trading days before expiry
  - Circuit breakers ±7.5%
  - 245 trading days/year
"""

import numpy as np
import pandas as pd
import duckdb
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import calendar

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
TRADING_DAYS = 245

# Rollover window: reduce signal confidence in last N days before expiry
ROLLOVER_WINDOW_DAYS = 5


class OIState(Enum):
    LONG_BUILDUP = "LONG_BUILDUP"       # Price ↑ + OI ↑ → new longs entering
    SHORT_COVERING = "SHORT_COVERING"   # Price ↑ + OI ↓ → shorts exiting
    SHORT_BUILDUP = "SHORT_BUILDUP"     # Price ↓ + OI ↑ → new shorts entering
    LONG_UNWINDING = "LONG_UNWINDING"   # Price ↓ + OI ↓ → longs exiting
    NEUTRAL = "NEUTRAL"                  # Flat price or flat OI


@dataclass
class OISignal:
    symbol: str
    date: str
    spot_price: float
    futures_price: float
    oi_contracts: int
    oi_change: int                 # absolute change
    oi_change_pct: float           # % change
    price_change_pct: float        # spot % change
    volume: int                    # futures volume
    state: OIState
    signal: str                    # "BUY", "SELL", "HOLD", "EXIT_LONG", "EXIT_SHORT"
    confidence: float              # 0-1
    streak: int                    # consecutive days of same state
    oi_percentile: float           # current OI vs 60-day range (0-100)
    days_to_expiry: int
    in_rollover: bool              # within rollover window
    basis_pct: float               # (futures - spot) / spot * 100
    reason: str


def get_last_thursday(year: int, month: int) -> datetime:
    """Get last Thursday of a month (PSX futures expiry)."""
    last_day = calendar.monthrange(year, month)[1]
    dt = datetime(year, month, last_day)
    while dt.weekday() != 3:  # Thursday = 3
        dt -= timedelta(days=1)
    return dt


def get_next_expiry(from_date: datetime = None) -> datetime:
    """Get next futures expiry date from a given date."""
    if from_date is None:
        from_date = datetime.now(PKT).replace(tzinfo=None)
    
    # Try current month first
    expiry = get_last_thursday(from_date.year, from_date.month)
    if expiry.date() >= from_date.date():
        return expiry
    
    # Next month
    if from_date.month == 12:
        return get_last_thursday(from_date.year + 1, 1)
    return get_last_thursday(from_date.year, from_date.month + 1)


def load_oi_data(symbol: str = None, days: int = 180) -> pd.DataFrame:
    """
    Load OI data from available sources (DuckDB, SQLite, or DFC files).
    Returns DataFrame with: date, symbol, oi, oi_change, futures_close, 
    futures_volume, spot_close, spot_change_pct
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    result = pd.DataFrame()
    
    # ── Try DuckDB first ──
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    # Discover the right table
    tables = [t[0] for t in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]
    
    oi_table = None
    for t in tables:
        tl = t.lower()
        if any(k in tl for k in ['futures_oi', 'dfc', 'oi_daily', 'contracts_oi', 'open_interest']):
            oi_table = t
            break
    
    if oi_table:
        try:
            oi_df = con.execute(f"SELECT * FROM {oi_table} ORDER BY ROWID DESC LIMIT 5").df()
            cols = list(oi_df.columns)
            
            # Dynamically map columns
            col_map = {}
            for c in cols:
                cl = c.lower()
                if 'date' in cl: col_map['date'] = c
                elif cl in ['oi', 'open_interest', 'contracts', 'oi_contracts']: col_map['oi'] = c
                elif 'symbol' in cl or 'base' in cl or 'underlying' in cl: col_map['symbol'] = c
                elif cl in ['close', 'settlement', 'last_price', 'futures_close']: col_map['futures_close'] = c
                elif cl in ['volume', 'traded_volume', 'qty', 'futures_volume']: col_map['volume'] = c
                elif 'change' in cl and 'oi' in cl: col_map['oi_change'] = c
            
            where = f"WHERE {col_map.get('date', 'date')} >= '{cutoff}'"
            if symbol and 'symbol' in col_map:
                where += f" AND {col_map['symbol']} LIKE '%{symbol}%'"
            
            result = con.execute(f"""
                SELECT * FROM {oi_table} {where}
                ORDER BY {col_map.get('date', 'date')}
            """).df()
            
            # Rename columns to standard names
            rename = {}
            for std_name, actual_col in col_map.items():
                if actual_col in result.columns:
                    rename[actual_col] = std_name
            result = result.rename(columns=rename)
            
        except Exception as e:
            pass
    
    con.close()
    
    # ── Fallback: SQLite ──
    if result.empty:
        try:
            scon = sqlite3.connect(str(PSX_SQLITE))
            stables = [r[0] for r in scon.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            
            for t in stables:
                tl = t.lower()
                if any(k in tl for k in ['futures', 'oi', 'dfc', 'contract']):
                    try:
                        result = pd.read_sql(f"SELECT * FROM {t} WHERE date >= '{cutoff}'", scon)
                        if not result.empty:
                            break
                    except:
                        continue
            scon.close()
        except:
            pass
    
    # ── Merge spot data if not already present ──
    if not result.empty and 'spot_close' not in result.columns:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        
        sym_col = 'symbol' if 'symbol' in result.columns else result.columns[0]
        symbols = result[sym_col].unique().tolist() if sym_col in result.columns else []
        
        if symbols:
            placeholders = ",".join(f"'{s}'" for s in symbols)
            spot = con.execute(f"""
                SELECT date, symbol, close as spot_close, volume as spot_volume
                FROM eod_ohlcv
                WHERE symbol IN ({placeholders}) AND date >= '{cutoff}'
                ORDER BY date
            """).df()
            
            if not spot.empty:
                result['date'] = pd.to_datetime(result['date']).dt.strftime('%Y-%m-%d')
                spot['date'] = pd.to_datetime(spot['date']).dt.strftime('%Y-%m-%d')
                result = result.merge(spot, on=['date', 'symbol'], how='left')
        
        con.close()
    
    return result


def classify_oi_state(price_change_pct: float, oi_change_pct: float,
                      min_price_move: float = 0.005,
                      min_oi_move: float = 0.02) -> OIState:
    """
    Classify OI state based on price and OI changes.
    
    min_price_move: minimum price change to be directional (0.5%)
    min_oi_move: minimum OI change to be significant (2%)
    """
    price_up = price_change_pct > min_price_move
    price_down = price_change_pct < -min_price_move
    oi_up = oi_change_pct > min_oi_move
    oi_down = oi_change_pct < -min_oi_move
    
    if price_up and oi_up:
        return OIState.LONG_BUILDUP
    elif price_up and oi_down:
        return OIState.SHORT_COVERING
    elif price_down and oi_up:
        return OIState.SHORT_BUILDUP
    elif price_down and oi_down:
        return OIState.LONG_UNWINDING
    else:
        return OIState.NEUTRAL


def compute_oi_signals(
    oi_df: pd.DataFrame,
    min_streak: int = 2,           # require N consecutive same-state days
    volume_filter: float = 1.0,    # min volume vs 20-day average (1.0 = average)
    oi_percentile_threshold: float = 30,  # flag if OI below 30th percentile (unusually low)
) -> list[OISignal]:
    """
    Compute OI-based trading signals for a symbol.
    
    Signal logic:
      LONG_BUILDUP × 2+ days → BUY (strong conviction new longs)
      SHORT_BUILDUP × 2+ days → SELL (strong conviction new shorts)
      SHORT_COVERING → EXIT_LONG (rally running out of fuel)
      LONG_UNWINDING → EXIT_SHORT (decline running out of fuel)
      In rollover window → reduce confidence by 50%
    """
    if oi_df.empty:
        return []
    
    df = oi_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    
    # Compute changes if not present
    if 'oi_change' not in df.columns and 'oi' in df.columns:
        df['oi_change'] = df['oi'].diff()
    if 'oi_change_pct' not in df.columns and 'oi' in df.columns:
        df['oi_change_pct'] = df['oi'].pct_change()
    if 'price_change_pct' not in df.columns:
        price_col = 'spot_close' if 'spot_close' in df.columns else 'futures_close' if 'futures_close' in df.columns else None
        if price_col:
            df['price_change_pct'] = df[price_col].pct_change()
        else:
            df['price_change_pct'] = 0
    
    # OI percentile (rolling 60-day)
    if 'oi' in df.columns:
        df['oi_percentile'] = df['oi'].rolling(60, min_periods=10).apply(
            lambda x: (x.iloc[-1] - x.min()) / (x.max() - x.min()) * 100 if x.max() != x.min() else 50
        )
    else:
        df['oi_percentile'] = 50
    
    # Volume vs 20-day average
    vol_col = 'volume' if 'volume' in df.columns else 'futures_volume' if 'futures_volume' in df.columns else None
    if vol_col:
        df['vol_ratio'] = df[vol_col] / df[vol_col].rolling(20, min_periods=5).mean()
    else:
        df['vol_ratio'] = 1.0
    
    # Basis
    if 'futures_close' in df.columns and 'spot_close' in df.columns:
        df['basis_pct'] = (df['futures_close'] - df['spot_close']) / df['spot_close'] * 100
    else:
        df['basis_pct'] = 0
    
    # Days to expiry
    df['days_to_expiry'] = df['date'].apply(
        lambda d: (get_next_expiry(d.to_pydatetime()) - d.to_pydatetime()).days
    )
    df['in_rollover'] = df['days_to_expiry'] <= ROLLOVER_WINDOW_DAYS
    
    # Classify each day
    df['oi_state'] = df.apply(
        lambda r: classify_oi_state(
            r.get('price_change_pct', 0) or 0,
            r.get('oi_change_pct', 0) or 0,
        ), axis=1
    )
    
    # Compute streaks (consecutive same-state days)
    df['streak'] = 1
    for i in range(1, len(df)):
        if df.iloc[i]['oi_state'] == df.iloc[i-1]['oi_state']:
            df.at[i, 'streak'] = df.iloc[i-1]['streak'] + 1
    
    # Generate signals
    signals = []
    for i, row in df.iterrows():
        state = row['oi_state']
        streak = int(row['streak'])
        in_rollover = bool(row.get('in_rollover', False))
        vol_ratio = row.get('vol_ratio', 1.0) or 1.0
        oi_pctile = row.get('oi_percentile', 50) or 50
        basis = row.get('basis_pct', 0) or 0
        dte = int(row.get('days_to_expiry', 30))
        
        signal = "HOLD"
        confidence = 0.0
        reason = ""
        
        if state == OIState.LONG_BUILDUP:
            if streak >= min_streak:
                signal = "BUY"
                confidence = min(1.0, 0.5 + streak * 0.1 + (vol_ratio - 1) * 0.2)
                reason = (f"Long buildup {streak} days — new longs entering. "
                         f"OI +{row.get('oi_change_pct', 0)*100:.1f}%, "
                         f"Price +{row.get('price_change_pct', 0)*100:.1f}%")
                
                # Basis confirmation
                if basis > 0.5:
                    confidence += 0.1
                    reason += f". Futures at premium ({basis:.2f}%) confirms bullish"
            else:
                signal = "HOLD"
                confidence = 0.3
                reason = f"Long buildup day {streak} — wait for confirmation (need {min_streak})"
        
        elif state == OIState.SHORT_BUILDUP:
            if streak >= min_streak:
                signal = "SELL"
                confidence = min(1.0, 0.5 + streak * 0.1 + (vol_ratio - 1) * 0.2)
                reason = (f"Short buildup {streak} days — new shorts entering. "
                         f"OI +{row.get('oi_change_pct', 0)*100:.1f}%, "
                         f"Price {row.get('price_change_pct', 0)*100:.1f}%")
                
                if basis < -0.5:
                    confidence += 0.1
                    reason += f". Futures at discount ({basis:.2f}%) confirms bearish"
            else:
                signal = "HOLD"
                confidence = 0.3
                reason = f"Short buildup day {streak} — wait for confirmation"
        
        elif state == OIState.SHORT_COVERING:
            signal = "EXIT_LONG"
            confidence = min(0.7, 0.3 + streak * 0.1)
            reason = (f"Short covering — shorts exiting, rally may exhaust. "
                     f"OI {row.get('oi_change_pct', 0)*100:.1f}%")
        
        elif state == OIState.LONG_UNWINDING:
            signal = "EXIT_SHORT"
            confidence = min(0.7, 0.3 + streak * 0.1)
            reason = (f"Long unwinding — longs exiting, decline may exhaust. "
                     f"OI {row.get('oi_change_pct', 0)*100:.1f}%")
        
        else:
            signal = "HOLD"
            confidence = 0.1
            reason = "Neutral — no clear OI signal"
        
        # Rollover penalty
        if in_rollover and signal in ("BUY", "SELL"):
            confidence *= 0.5
            reason += f". ⚠️ ROLLOVER WINDOW ({dte} days to expiry) — reduced confidence"
        
        # Volume filter
        if vol_ratio < volume_filter and signal in ("BUY", "SELL"):
            confidence *= 0.7
            reason += f". Low volume ({vol_ratio:.1f}x avg)"
        
        # High volume boost
        if vol_ratio > 2.0 and signal in ("BUY", "SELL"):
            confidence = min(1.0, confidence * 1.2)
            reason += f". HIGH volume ({vol_ratio:.1f}x avg) — strong conviction"
        
        # OI at extreme percentile
        if oi_pctile > 90:
            reason += f". OI at {oi_pctile:.0f}th percentile — crowded"
        elif oi_pctile < 20:
            reason += f". OI at {oi_pctile:.0f}th percentile — low participation"
        
        signals.append(OISignal(
            symbol=row.get('symbol', ''),
            date=str(row['date'])[:10],
            spot_price=row.get('spot_close', 0) or 0,
            futures_price=row.get('futures_close', 0) or 0,
            oi_contracts=int(row.get('oi', 0) or 0),
            oi_change=int(row.get('oi_change', 0) or 0),
            oi_change_pct=float(row.get('oi_change_pct', 0) or 0),
            price_change_pct=float(row.get('price_change_pct', 0) or 0),
            volume=int(row.get(vol_col, 0) or 0) if vol_col else 0,
            state=state,
            signal=signal,
            confidence=round(confidence, 3),
            streak=streak,
            oi_percentile=round(oi_pctile, 1),
            days_to_expiry=dte,
            in_rollover=in_rollover,
            basis_pct=round(basis, 3),
            reason=reason,
        ))
    
    return signals


def backtest_oi_strategy(
    symbol: str,
    min_streak: int = 2,
    stop_loss_pct: float = 0.03,
    take_profit_pct: float = 0.05,
    max_hold_days: int = 15,
    exit_on_unwind: bool = True,    # exit when OI state flips to covering/unwinding
    skip_rollover: bool = True,     # skip signals in rollover window
    days: int = 365,
) -> dict:
    """
    Backtest OI buildup/unwind strategy.
    
    Entry: BUY on LONG_BUILDUP × min_streak, SELL on SHORT_BUILDUP × min_streak
    Exit: TP, SL, max hold, or OI state flip (covering/unwinding)
    Skip: signals during rollover window (optional)
    """
    oi_df = load_oi_data(symbol, days)
    if oi_df.empty or len(oi_df) < 30:
        return {"error": f"Not enough OI data for {symbol}"}
    
    signals = compute_oi_signals(oi_df, min_streak=min_streak)
    if not signals:
        return {"error": "No signals generated"}
    
    trades = []
    position = None  # {"entry_date", "entry_price", "direction", "entry_oi"}
    
    for sig in signals:
        price = sig.spot_price if sig.spot_price > 0 else sig.futures_price
        if price <= 0:
            continue
        
        # Check exit conditions for open position
        if position is not None:
            days_held = (pd.to_datetime(sig.date) - pd.to_datetime(position['entry_date'])).days
            
            if position['direction'] == 'LONG':
                pnl_pct = price / position['entry_price'] - 1
            else:
                pnl_pct = 1 - price / position['entry_price']
            
            exit_reason = None
            
            if pnl_pct >= take_profit_pct:
                exit_reason = "TAKE_PROFIT"
            elif pnl_pct <= -stop_loss_pct:
                exit_reason = "STOP_LOSS"
            elif days_held >= max_hold_days:
                exit_reason = "MAX_HOLD"
            elif exit_on_unwind:
                if position['direction'] == 'LONG' and sig.state in (OIState.SHORT_COVERING, OIState.LONG_UNWINDING):
                    exit_reason = "OI_UNWIND"
                elif position['direction'] == 'SHORT' and sig.state in (OIState.LONG_BUILDUP, OIState.LONG_UNWINDING):
                    exit_reason = "OI_UNWIND"
            
            # Force exit during rollover
            if sig.in_rollover and sig.days_to_expiry <= 2:
                exit_reason = "ROLLOVER_EXIT"
            
            if exit_reason:
                trades.append({
                    "symbol": symbol,
                    "entry_date": position['entry_date'],
                    "exit_date": sig.date,
                    "direction": position['direction'],
                    "entry_price": position['entry_price'],
                    "exit_price": price,
                    "pnl_pct": pnl_pct,
                    "days_held": days_held,
                    "exit_reason": exit_reason,
                    "entry_oi": position['entry_oi'],
                    "exit_oi": sig.oi_contracts,
                    "entry_state": position['entry_state'],
                    "exit_state": sig.state.value,
                })
                position = None
        
        # Check entry conditions (only if flat)
        if position is None:
            if sig.signal == "BUY" and not (skip_rollover and sig.in_rollover):
                position = {
                    'entry_date': sig.date,
                    'entry_price': price,
                    'direction': 'LONG',
                    'entry_oi': sig.oi_contracts,
                    'entry_state': sig.state.value,
                }
            elif sig.signal == "SELL" and not (skip_rollover and sig.in_rollover):
                position = {
                    'entry_date': sig.date,
                    'entry_price': price,
                    'direction': 'SHORT',
                    'entry_oi': sig.oi_contracts,
                    'entry_state': sig.state.value,
                }
    
    # Close any remaining position
    if position is not None and signals:
        last = signals[-1]
        price = last.spot_price if last.spot_price > 0 else last.futures_price
        if price > 0:
            if position['direction'] == 'LONG':
                pnl_pct = price / position['entry_price'] - 1
            else:
                pnl_pct = 1 - price / position['entry_price']
            trades.append({
                "symbol": symbol, "entry_date": position['entry_date'],
                "exit_date": last.date, "direction": position['direction'],
                "entry_price": position['entry_price'], "exit_price": price,
                "pnl_pct": pnl_pct, "days_held": 0, "exit_reason": "END_OF_DATA",
                "entry_oi": position['entry_oi'], "exit_oi": last.oi_contracts,
                "entry_state": position['entry_state'], "exit_state": last.state.value,
            })
    
    if not trades:
        return {"error": "No trades generated", "signals_count": len(signals)}
    
    trades_df = pd.DataFrame(trades)
    
    winning = trades_df[trades_df['pnl_pct'] > 0]
    losing = trades_df[trades_df['pnl_pct'] <= 0]
    
    trades_df['cum_return'] = (1 + trades_df['pnl_pct']).cumprod()
    total_return = trades_df['cum_return'].iloc[-1] - 1
    max_dd = (trades_df['cum_return'] / trades_df['cum_return'].cummax() - 1).min()
    
    # By state breakdown
    state_stats = trades_df.groupby('entry_state').agg(
        trades=('pnl_pct', 'count'),
        win_rate=('pnl_pct', lambda x: (x > 0).mean()),
        avg_pnl=('pnl_pct', 'mean'),
    ).to_dict('index')
    
    # By exit reason
    exit_stats = trades_df['exit_reason'].value_counts().to_dict()
    
    # OI change analysis: did OI continue rising/falling after entry?
    trades_df['oi_change_during'] = trades_df['exit_oi'] - trades_df['entry_oi']
    
    return {
        "trades": trades_df,
        "signals": pd.DataFrame([{
            "date": s.date, "state": s.state.value, "signal": s.signal,
            "confidence": s.confidence, "oi": s.oi_contracts,
            "oi_change_pct": s.oi_change_pct, "price_change_pct": s.price_change_pct,
            "streak": s.streak, "in_rollover": s.in_rollover,
        } for s in signals]),
        "metrics": {
            "total_trades": len(trades_df),
            "win_rate": len(winning) / len(trades_df),
            "avg_win": winning['pnl_pct'].mean() if len(winning) > 0 else 0,
            "avg_loss": losing['pnl_pct'].mean() if len(losing) > 0 else 0,
            "profit_factor": abs(winning['pnl_pct'].sum() / losing['pnl_pct'].sum()) if len(losing) > 0 and losing['pnl_pct'].sum() != 0 else 0,
            "total_return": total_return,
            "max_drawdown": max_dd,
            "avg_days_held": trades_df['days_held'].mean(),
            "long_trades": len(trades_df[trades_df['direction'] == 'LONG']),
            "short_trades": len(trades_df[trades_df['direction'] == 'SHORT']),
            "by_state": state_stats,
            "by_exit_reason": exit_stats,
        },
    }


def scan_oi_signals(symbols: list[str] = None, days: int = 30) -> pd.DataFrame:
    """
    Scan multiple symbols for current OI signals.
    Returns table sorted by confidence.
    """
    if symbols is None:
        # Get symbols with OI data
        oi_df = load_oi_data(days=7)
        if oi_df.empty:
            return pd.DataFrame()
        sym_col = 'symbol' if 'symbol' in oi_df.columns else oi_df.columns[0]
        symbols = oi_df[sym_col].unique().tolist()
    
    results = []
    for sym in symbols:
        oi_df = load_oi_data(sym, days=days)
        if oi_df.empty or len(oi_df) < 5:
            continue
        
        signals = compute_oi_signals(oi_df, min_streak=1)
        if not signals:
            continue
        
        latest = signals[-1]
        if latest.state != OIState.NEUTRAL:
            results.append({
                "symbol": sym,
                "date": latest.date,
                "state": latest.state.value,
                "signal": latest.signal,
                "confidence": latest.confidence,
                "streak": latest.streak,
                "oi": latest.oi_contracts,
                "oi_change_pct": latest.oi_change_pct,
                "price_change_pct": latest.price_change_pct,
                "basis_pct": latest.basis_pct,
                "oi_percentile": latest.oi_percentile,
                "days_to_expiry": latest.days_to_expiry,
                "in_rollover": latest.in_rollover,
                "volume": latest.volume,
            })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results).sort_values('confidence', ascending=False).reset_index(drop=True)


def get_rollover_calendar(months_ahead: int = 3) -> pd.DataFrame:
    """
    Generate rollover calendar — expiry dates and rollover windows.
    """
    now = datetime.now(PKT).replace(tzinfo=None)
    entries = []
    
    for i in range(months_ahead + 1):
        month = now.month + i
        year = now.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        
        expiry = get_last_thursday(year, month)
        rollover_start = expiry - timedelta(days=ROLLOVER_WINDOW_DAYS)
        
        is_current = (expiry.month == now.month and expiry.year == now.year)
        days_away = (expiry - now).days
        
        entries.append({
            "contract": f"{calendar.month_abbr[month].upper()}-{year}",
            "expiry_date": expiry.strftime("%Y-%m-%d"),
            "expiry_day": expiry.strftime("%A"),
            "rollover_start": rollover_start.strftime("%Y-%m-%d"),
            "days_away": max(0, days_away),
            "is_current": is_current,
            "status": "EXPIRED" if days_away < 0 else "ROLLOVER" if days_away <= ROLLOVER_WINDOW_DAYS else "ACTIVE",
        })
    
    return pd.DataFrame(entries)
```

## Step 2: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_oi.py`:

### Tab 1: OI Matrix Live
```
For selected symbol:
├── OI Matrix badge (LONG_BUILDUP / SHORT_BUILDUP / SHORT_COVERING / LONG_UNWINDING / NEUTRAL)
│   Large colored card: green=buildup, red=short buildup, yellow=covering, gray=neutral
├── Signal: BUY / SELL / HOLD / EXIT with confidence bar
├── Streak counter: "Day 3 of LONG BUILDUP"
├── OI + Price dual-axis chart:
│   ├── Top: Price line (white)
│   ├── Bottom: OI area chart (gold fill)
│   ├── Background bands colored by OI state per day
│   └── Volume bars at bottom
├── OI Matrix history table (last 20 days):
│   Date | Price Chg% | OI Chg% | State | Signal | Streak | Volume
├── Metric cards: OI (contracts), OI Δ, OI Percentile, Basis%, DTE
├── Rollover warning banner (if in rollover window)
└── Rollover calendar (next 3 months)
```

### Tab 2: Backtest
```
├── Symbol selector (only symbols with OI data)
├── Parameters:
│   ├── Min streak: slider 1-5 (default 2)
│   ├── Stop loss: 1%-5% (default 3%)
│   ├── Take profit: 2%-10% (default 5%)
│   ├── Max hold days: 5-30 (default 15)
│   ├── Exit on unwind: toggle (default ON)
│   ├── Skip rollover: toggle (default ON)
│   └── Lookback: 6M / 1Y / 2Y
├── [Run Backtest]
├── Metrics: Trades, Win Rate, PF, Return, MaxDD, Avg Hold
├── By OI-state breakdown table (LONG_BUILDUP vs SHORT_BUILDUP win rates)
├── Exit reason pie chart
├── Equity curve
├── Trade log
└── OI change during trade analysis (did OI confirm or reverse?)
```

### Tab 3: OI Scanner
```
├── Scan all symbols with active futures OI
├── Table: Symbol | State | Signal | Confidence | Streak | OI | OI Δ% | Price Δ% | Basis% | DTE | Rollover?
├── Color: green=LONG_BUILDUP, red=SHORT_BUILDUP, yellow=COVERING/UNWINDING
├── Sort by confidence
├── Filter: show only BUY/SELL signals (hide HOLD)
└── Rollover status badge per symbol
```

### Tab 4: Rollover & Research
```
├── Rollover calendar: next 3 months with expiry dates + rollover windows
│   (Gantt-like visual showing rollover periods)
├── OI distribution: histogram of OI levels across symbols
├── OI vs Price scatter (does high OI predict direction?)
├── State transition matrix: P(LONG_BUILDUP → SHORT_BUILDUP) etc.
├── Monthly seasonality: does OI behavior change near expiry?
├── OI buildup persistence: when buildup starts, how long does it last?
└── Methodology + Wyckoff connection
```

### Key chart — OI + Price with state bands:
```python
from plotly.subplots import make_subplots
import plotly.graph_objects as go

fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                    row_heights=[0.6, 0.4], vertical_spacing=0.05)

# Price
fig.add_trace(go.Scatter(x=df['date'], y=df['spot_close'],
    line=dict(color='#E0E0E0', width=1.5), name='Price'), row=1, col=1)

# State background bands
state_colors = {
    'LONG_BUILDUP': 'rgba(34,197,94,0.15)',
    'SHORT_BUILDUP': 'rgba(239,68,68,0.15)',
    'SHORT_COVERING': 'rgba(234,179,8,0.15)',
    'LONG_UNWINDING': 'rgba(168,85,247,0.15)',
}
for state, color in state_colors.items():
    mask = df['oi_state'] == state
    if mask.any():
        for start, end in _get_contiguous_ranges(df, mask):
            fig.add_vrect(x0=start, x1=end, fillcolor=color,
                         layer='below', line_width=0, row=1, col=1)

# OI area chart
fig.add_trace(go.Scatter(x=df['date'], y=df['oi'],
    fill='tozeroy', fillcolor='rgba(200,169,110,0.2)',
    line=dict(color='#C8A96E', width=1.5), name='Open Interest'), row=2, col=1)

fig.update_layout(template='plotly_dark', paper_bgcolor='#0B0E11',
                  plot_bgcolor='#0B0E11', height=500)
```

## Step 3: Add to sidebar

```python
st.page_link("page_views/strategy_oi.py", label="OI Buildup/Unwind", icon="📈")
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# Test OI data loading
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.oi_strategy import load_oi_data, get_rollover_calendar

df = load_oi_data('HUBC', days=90)
print(f'OI data: {len(df)} rows')
print(f'Columns: {list(df.columns)}')
if not df.empty:
    print(df.tail(5).to_string())

print('\nRollover Calendar:')
print(get_rollover_calendar(3).to_string())
"

# Test signal generation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.oi_strategy import load_oi_data, compute_oi_signals

df = load_oi_data('HUBC', days=60)
signals = compute_oi_signals(df, min_streak=2)
print(f'Signals: {len(signals)}')
for s in signals[-10:]:
    print(f'  {s.date} {s.state.value:20s} {s.signal:12s} conf:{s.confidence:.0%} streak:{s.streak} OI:{s.oi_contracts:,} Δ:{s.oi_change_pct:+.1%} price:{s.price_change_pct:+.1%}')
"

# Test scanner
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.oi_strategy import scan_oi_signals

df = scan_oi_signals()
print(f'Symbols with OI signals: {len(df)}')
if not df.empty:
    print(df[['symbol','state','signal','confidence','streak','oi','oi_change_pct','basis_pct','days_to_expiry']].head(10).to_string())
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.oi_strategy import backtest_oi_strategy

result = backtest_oi_strategy('HUBC', min_streak=2, days=365)
if 'error' not in result:
    m = result['metrics']
    print(f'=== OI BUILDUP/UNWIND BACKTEST ===')
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Profit Factor: {m[\"profit_factor\"]:.2f}')
    print(f'Return: {m[\"total_return\"]:.1%}')
    print(f'Max DD: {m[\"max_drawdown\"]:.1%}')
    print(f'Avg Hold: {m[\"avg_days_held\"]:.1f} days')
    print(f'Long: {m[\"long_trades\"]}, Short: {m[\"short_trades\"]}')
    print(f'By state: {m[\"by_state\"]}')
    print(f'By exit: {m[\"by_exit_reason\"]}')
else:
    print(result)
"
```

## IMPORTANT NOTES

1. **OI table discovery is dynamic** — code scans DuckDB + SQLite for any table with 'oi', 'futures', 'dfc' in name
2. **Column mapping is dynamic** — handles different column naming across tables
3. **Real OI from DFC XLS** — not estimated from volume like many platforms
4. **Min streak = 2** — requires 2 consecutive days of same state before entry. Prevents whipsaws.
5. **Rollover window = 5 days** — reduces confidence by 50% in last 5 days before expiry. Skip entirely with toggle.
6. **Force exit at DTE ≤ 2** — avoid physical delivery risk
7. **Exit on unwind/covering** — when OI state flips against position, exit early
8. **Basis confirmation** — if futures premium aligns with OI signal, boost confidence
9. **OI percentile** — flags when OI is at extremes (>90th crowded, <20th low participation)
10. **Volume filter** — low volume OI changes may be noise
11. **No TA libraries** — all in numpy/pandas
12. **Add under STRATEGIES** in sidebar
13. **PSX edge:** Physical delivery means OI = real commitment. On cash-settled markets, OI can be artificial. On PSX it represents actual shares that must be delivered.
