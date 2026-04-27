"""
OI Buildup/Unwind Trading Strategy.

Uses the classic Open Interest interpretation matrix to generate
directional trading signals from PSX futures OI data.

OI Matrix:
  Price UP + OI UP     = LONG BUILDUP    -> Strong BUY
  Price UP + OI DOWN   = SHORT COVERING  -> Weak rally, caution
  Price DOWN + OI UP   = SHORT BUILDUP   -> Strong SELL
  Price DOWN + OI DOWN = LONG UNWINDING  -> Weak decline, caution

Data sources:
  1. DFC XLS files (real OI from PSX) - limited to available dates
  2. futures_eod (SQLite) - OHLCV + volume for futures contracts
  3. eod_ohlcv (DuckDB) - spot OHLCV for basis calculation

PSX-Specific:
  - Physical delivery -> OI represents real commitment
  - Monthly expiry: last Thursday of month
  - Rollover window: ~5 trading days before expiry
  - Circuit breakers +/-7.5%
"""

import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum
import calendar
import os

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
PSX_SQLITE = Path("/home/smnb/psxdata_rescue/psx.sqlite")
DFC_BASE = Path("/mnt/e/psxdata/downloads/daily")
TRADING_DAYS = 245
ROLLOVER_WINDOW_DAYS = 5


def _psx_con() -> sqlite3.Connection:
    """Open a fresh SQLite connection without renegotiating WAL.

    The DB is already in WAL mode persistently. Re-issuing PRAGMA journal_mode
    on every fresh connection while another (Streamlit-cached) connection has
    pending schema work triggers a transient `malformed database schema` race.
    We skip journal_mode and only set the read-side pragmas, each guarded.
    """
    con = sqlite3.connect(str(PSX_SQLITE), timeout=30, check_same_thread=False)
    for pragma in ("busy_timeout=30000", "cache_size=-32000"):
        try:
            con.execute(f"PRAGMA {pragma}")
        except sqlite3.DatabaseError:
            # Schema cookie race — recover by reopening once.
            con.close()
            con = sqlite3.connect(str(PSX_SQLITE), timeout=30, check_same_thread=False)
            try:
                con.execute(f"PRAGMA {pragma}")
            except sqlite3.DatabaseError:
                pass
    return con


class OIState(Enum):
    LONG_BUILDUP = "LONG_BUILDUP"
    SHORT_COVERING = "SHORT_COVERING"
    SHORT_BUILDUP = "SHORT_BUILDUP"
    LONG_UNWINDING = "LONG_UNWINDING"
    NEUTRAL = "NEUTRAL"


@dataclass
class OISignal:
    symbol: str
    date: str
    spot_price: float
    futures_price: float
    oi_contracts: int
    oi_change: int
    oi_change_pct: float
    price_change_pct: float
    volume: int
    state: OIState
    signal: str
    confidence: float
    streak: int
    oi_percentile: float
    days_to_expiry: int
    in_rollover: bool
    basis_pct: float
    reason: str


# ---------------------------------------------------------------------------
# Expiry helpers
# ---------------------------------------------------------------------------

def get_last_thursday(year: int, month: int) -> datetime:
    """Get last Thursday of a month (PSX futures expiry)."""
    last_day = calendar.monthrange(year, month)[1]
    dt = datetime(year, month, last_day)
    while dt.weekday() != 3:
        dt -= timedelta(days=1)
    return dt


def get_next_expiry(from_date: datetime = None) -> datetime:
    """Get next futures expiry date from a given date."""
    if from_date is None:
        from_date = datetime.now(PKT).replace(tzinfo=None)
    expiry = get_last_thursday(from_date.year, from_date.month)
    if expiry.date() >= from_date.date():
        return expiry
    if from_date.month == 12:
        return get_last_thursday(from_date.year + 1, 1)
    return get_last_thursday(from_date.year, from_date.month + 1)


def get_rollover_calendar(months_ahead: int = 3) -> pd.DataFrame:
    """Generate rollover calendar -- expiry dates and rollover windows."""
    now = datetime.now(PKT).replace(tzinfo=None)
    entries = []
    for i in range(months_ahead + 1):
        month = now.month + i
        year = now.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        expiry = get_last_thursday(year, month)
        rollover_start = expiry - timedelta(days=ROLLOVER_WINDOW_DAYS)
        days_away = (expiry - now).days
        entries.append({
            "contract": f"{calendar.month_abbr[month].upper()}-{year}",
            "expiry_date": expiry.strftime("%Y-%m-%d"),
            "expiry_day": expiry.strftime("%A"),
            "rollover_start": rollover_start.strftime("%Y-%m-%d"),
            "days_away": max(0, days_away),
            "is_current": expiry.month == now.month and expiry.year == now.year,
            "status": (
                "EXPIRED" if days_away < 0
                else "ROLLOVER" if days_away <= ROLLOVER_WINDOW_DAYS
                else "ACTIVE"
            ),
        })
    return pd.DataFrame(entries)


# ---------------------------------------------------------------------------
# DFC XLS parsing
# ---------------------------------------------------------------------------

def _parse_dfc_xls(filepath: str, date_str: str) -> pd.DataFrame:
    """Parse a single DFC XLS file into a clean DataFrame."""
    try:
        xl = pd.ExcelFile(filepath)
        # Data is in the second sheet (first is summary header)
        sheet = xl.sheet_names[1] if len(xl.sheet_names) > 1 else xl.sheet_names[0]
        df = pd.read_excel(filepath, sheet_name=sheet, header=None, skiprows=6)
        df.columns = [
            "sr", "symbol", "category", "oi_contracts",
            "oi_volume", "oi_value", "free_float_vol", "pct_free_float",
        ]
        df = df.dropna(subset=["symbol"])
        df = df[df["oi_contracts"].notna() & (df["oi_contracts"] > 0)]

        # Parse base_symbol and contract_month from "HUBC-MAR"
        parts = df["symbol"].str.rsplit("-", n=1)
        df["base_symbol"] = parts.str[0]
        df["contract_month"] = parts.str[1]
        df["date"] = date_str
        df["oi_contracts"] = df["oi_contracts"].astype(int)
        df["oi_volume"] = pd.to_numeric(df["oi_volume"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()


def load_dfc_oi(symbol: str = None, days: int = 60) -> pd.DataFrame:
    """Load OI data from DFC XLS files on disk.

    Returns per-symbol aggregated OI (sum across contract months) per date.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    all_frames = []

    if not DFC_BASE.exists():
        return pd.DataFrame()

    for date_dir in sorted(os.listdir(DFC_BASE)):
        if date_dir < cutoff:
            continue
        dfc_path = DFC_BASE / date_dir / "futures" / f"futures_oi_dfc_{date_dir}.xls"
        if dfc_path.exists():
            df = _parse_dfc_xls(str(dfc_path), date_dir)
            if not df.empty:
                all_frames.append(df)

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)

    # Aggregate: sum OI across all contract months per symbol per date
    agg = combined.groupby(["date", "base_symbol"]).agg(
        oi_contracts=("oi_contracts", "sum"),
        oi_volume=("oi_volume", "sum"),
        oi_value=("oi_value", "sum"),
    ).reset_index()
    agg = agg.rename(columns={"base_symbol": "symbol"})

    if symbol:
        agg = agg[agg["symbol"] == symbol]

    return agg.sort_values(["symbol", "date"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Futures EOD data (SQLite) -- has volume but not OI
# ---------------------------------------------------------------------------

def load_futures_eod(symbol: str = None, days: int = 365) -> pd.DataFrame:
    """Load futures OHLCV from SQLite futures_eod table.

    Aggregates across contract months (near-month preferred).
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    scon = _psx_con()
    where = f"WHERE market_type = 'FUT' AND date >= '{cutoff}'"
    if symbol:
        where += f" AND base_symbol = '{symbol}'"

    df = pd.read_sql(f"""
        SELECT date, base_symbol as symbol, contract_month,
               open, high, low, close, volume, prev_close, change_pct
        FROM futures_eod
        {where}
        ORDER BY date, base_symbol, contract_month
    """, scon)
    scon.close()

    if df.empty:
        return df

    # Keep near-month contract per symbol per date (highest volume, or first row if all zero)
    def _pick_near(group):
        if group["volume"].max() > 0:
            return group.loc[group["volume"].idxmax()]
        return group.iloc[0]

    near = df.groupby(["date", "symbol"], group_keys=False).apply(
        _pick_near, include_groups=False,
    ).reset_index()
    near["futures_close"] = near["close"]
    near["futures_volume"] = near["volume"]
    near = near[["date", "symbol", "contract_month", "futures_close", "futures_volume",
                  "open", "high", "low", "prev_close", "change_pct"]]

    # Total volume across all months (proxy for activity)
    total_vol = df.groupby(["date", "symbol"])["volume"].sum().reset_index()
    total_vol = total_vol.rename(columns={"volume": "total_futures_volume"})
    near = near.merge(total_vol, on=["date", "symbol"], how="left")

    return near.sort_values(["symbol", "date"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Spot data
# ---------------------------------------------------------------------------

def _load_spot(symbols: list[str], days: int = 365) -> pd.DataFrame:
    """Load spot OHLCV from DuckDB eod_ohlcv."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    con = analytics_con()
    placeholders = ",".join(f"'{s}'" for s in symbols)
    df = con.execute(f"""
        SELECT date, symbol, close AS spot_close, volume AS spot_volume
        FROM eod_ohlcv
        WHERE symbol IN ({placeholders})
          AND date >= '{cutoff}'
        ORDER BY date
    """).df()
    con.close()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


# ---------------------------------------------------------------------------
# Unified OI data loader
# ---------------------------------------------------------------------------

def load_oi_data(symbol: str = None, days: int = 180) -> pd.DataFrame:
    """Load and merge OI + futures + spot data.

    Priority:
      1. DFC XLS files for real OI (limited dates)
      2. futures_eod for price/volume
      3. eod_ohlcv for spot close and basis
    """
    # Load futures EOD (always available, 5+ years)
    futures = load_futures_eod(symbol, days)
    if futures.empty:
        return pd.DataFrame()

    symbols = futures["symbol"].unique().tolist()

    # Load DFC OI data (may be empty or few dates)
    dfc = load_dfc_oi(symbol, days)

    # Merge DFC OI into futures if available
    if not dfc.empty:
        futures = futures.merge(
            dfc[["date", "symbol", "oi_contracts", "oi_volume"]],
            on=["date", "symbol"], how="left",
        )
    else:
        futures["oi_contracts"] = np.nan
        futures["oi_volume"] = np.nan

    # Load spot data
    spot = _load_spot(symbols, days)
    if not spot.empty:
        futures = futures.merge(spot, on=["date", "symbol"], how="left")
    else:
        futures["spot_close"] = np.nan
        futures["spot_volume"] = np.nan

    # Compute derived columns
    futures = futures.sort_values(["symbol", "date"]).reset_index(drop=True)

    # Use cumulative volume as OI proxy when real OI is missing
    # Idea: rising cumulative volume with rising price ~ long buildup
    for sym in futures["symbol"].unique():
        mask = futures["symbol"] == sym
        sub = futures.loc[mask].copy()

        # Use DFC real OI where available, volume proxy elsewhere
        vol_proxy = sub["total_futures_volume"].replace(0, np.nan).ffill().fillna(0)
        if sub["oi_contracts"].notna().any():
            # Fill DFC OI where available; use volume proxy for gaps
            sub["oi"] = sub["oi_contracts"]
            # Only interpolate between known DFC points; use volume proxy outside
            sub["oi"] = sub["oi"].interpolate(method="linear", limit_area="inside")
            sub["oi"] = sub["oi"].fillna(vol_proxy)
        else:
            sub["oi"] = vol_proxy

        sub["oi_change"] = sub["oi"].diff()
        sub["oi_change_pct"] = sub["oi"].pct_change()

        price_col = "spot_close" if sub["spot_close"].notna().any() else "futures_close"
        sub["price_change_pct"] = sub[price_col].pct_change()

        if sub["spot_close"].notna().any() and sub["futures_close"].notna().any():
            sub["basis_pct"] = (
                (sub["futures_close"] - sub["spot_close"]) / sub["spot_close"] * 100
            )
        else:
            sub["basis_pct"] = 0.0

        futures.loc[mask, ["oi", "oi_change", "oi_change_pct",
                           "price_change_pct", "basis_pct"]] = sub[
            ["oi", "oi_change", "oi_change_pct", "price_change_pct", "basis_pct"]
        ].values

    return futures


# ---------------------------------------------------------------------------
# OI state classification
# ---------------------------------------------------------------------------

def classify_oi_state(
    price_change_pct: float,
    oi_change_pct: float,
    min_price_move: float = 0.005,
    min_oi_move: float = 0.02,
) -> OIState:
    """Classify OI state based on price and OI changes."""
    if pd.isna(price_change_pct) or pd.isna(oi_change_pct):
        return OIState.NEUTRAL

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
    return OIState.NEUTRAL


# ---------------------------------------------------------------------------
# Signal generation
# ---------------------------------------------------------------------------

def compute_oi_signals(
    oi_df: pd.DataFrame,
    min_streak: int = 2,
    volume_filter: float = 1.0,
    oi_percentile_threshold: float = 30,
) -> list[OISignal]:
    """Compute OI-based trading signals.

    Signal logic:
      LONG_BUILDUP x 2+ days -> BUY
      SHORT_BUILDUP x 2+ days -> SELL
      SHORT_COVERING -> EXIT_LONG
      LONG_UNWINDING -> EXIT_SHORT
      In rollover window -> reduce confidence by 50%
    """
    if oi_df.empty:
        return []

    df = oi_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # OI percentile (rolling 60-day)
    if "oi" in df.columns:
        df["oi_percentile"] = df["oi"].rolling(60, min_periods=10).apply(
            lambda x: (
                (x.iloc[-1] - x.min()) / (x.max() - x.min()) * 100
                if x.max() != x.min() else 50
            )
        )
    else:
        df["oi_percentile"] = 50.0

    # Volume ratio
    vol_col = (
        "futures_volume" if "futures_volume" in df.columns
        else "volume" if "volume" in df.columns
        else None
    )
    if vol_col and df[vol_col].notna().any():
        df["vol_ratio"] = df[vol_col] / df[vol_col].rolling(20, min_periods=5).mean()
    else:
        df["vol_ratio"] = 1.0

    # Days to expiry
    df["days_to_expiry"] = df["date"].apply(
        lambda d: (get_next_expiry(d.to_pydatetime()) - d.to_pydatetime()).days
    )
    df["in_rollover"] = df["days_to_expiry"] <= ROLLOVER_WINDOW_DAYS

    # Classify each day
    df["oi_state"] = df.apply(
        lambda r: classify_oi_state(
            r.get("price_change_pct", 0) or 0,
            r.get("oi_change_pct", 0) or 0,
        ),
        axis=1,
    )

    # Streaks
    df["streak"] = 1
    for i in range(1, len(df)):
        if df.iloc[i]["oi_state"] == df.iloc[i - 1]["oi_state"]:
            df.at[i, "streak"] = df.iloc[i - 1]["streak"] + 1

    # Generate signals
    signals = []
    for _, row in df.iterrows():
        state = row["oi_state"]
        streak = int(row["streak"])
        in_rollover = bool(row.get("in_rollover", False))
        vol_ratio = float(row.get("vol_ratio", 1.0) or 1.0)
        oi_pctile = float(row.get("oi_percentile", 50) or 50)
        basis = float(row.get("basis_pct", 0) or 0)
        dte = int(row.get("days_to_expiry", 30))

        signal = "HOLD"
        confidence = 0.0
        reason = ""

        if state == OIState.LONG_BUILDUP:
            if streak >= min_streak:
                signal = "BUY"
                confidence = min(1.0, 0.5 + streak * 0.1 + (vol_ratio - 1) * 0.2)
                reason = (
                    f"Long buildup {streak} days. "
                    f"OI +{(row.get('oi_change_pct', 0) or 0) * 100:.1f}%, "
                    f"Price +{(row.get('price_change_pct', 0) or 0) * 100:.1f}%"
                )
                if basis > 0.5:
                    confidence += 0.1
                    reason += f". Futures premium ({basis:.2f}%) confirms bullish"
            else:
                confidence = 0.3
                reason = f"Long buildup day {streak} -- wait for confirmation (need {min_streak})"

        elif state == OIState.SHORT_BUILDUP:
            if streak >= min_streak:
                signal = "SELL"
                confidence = min(1.0, 0.5 + streak * 0.1 + (vol_ratio - 1) * 0.2)
                reason = (
                    f"Short buildup {streak} days. "
                    f"OI +{(row.get('oi_change_pct', 0) or 0) * 100:.1f}%, "
                    f"Price {(row.get('price_change_pct', 0) or 0) * 100:.1f}%"
                )
                if basis < -0.5:
                    confidence += 0.1
                    reason += f". Futures discount ({basis:.2f}%) confirms bearish"
            else:
                confidence = 0.3
                reason = f"Short buildup day {streak} -- wait for confirmation"

        elif state == OIState.SHORT_COVERING:
            signal = "EXIT_LONG"
            confidence = min(0.7, 0.3 + streak * 0.1)
            reason = (
                f"Short covering -- shorts exiting, rally may exhaust. "
                f"OI {(row.get('oi_change_pct', 0) or 0) * 100:.1f}%"
            )

        elif state == OIState.LONG_UNWINDING:
            signal = "EXIT_SHORT"
            confidence = min(0.7, 0.3 + streak * 0.1)
            reason = (
                f"Long unwinding -- longs exiting, decline may exhaust. "
                f"OI {(row.get('oi_change_pct', 0) or 0) * 100:.1f}%"
            )

        else:
            signal = "HOLD"
            confidence = 0.1
            reason = "Neutral -- no clear OI signal"

        # Rollover penalty
        if in_rollover and signal in ("BUY", "SELL"):
            confidence *= 0.5
            reason += f". ROLLOVER WINDOW ({dte} days to expiry)"

        # Volume filter
        if vol_ratio < volume_filter and signal in ("BUY", "SELL"):
            confidence *= 0.7
            reason += f". Low volume ({vol_ratio:.1f}x avg)"

        if vol_ratio > 2.0 and signal in ("BUY", "SELL"):
            confidence = min(1.0, confidence * 1.2)
            reason += f". HIGH volume ({vol_ratio:.1f}x avg)"

        if oi_pctile > 90:
            reason += f". OI at {oi_pctile:.0f}th percentile -- crowded"
        elif oi_pctile < 20:
            reason += f". OI at {oi_pctile:.0f}th percentile -- low participation"

        def _safe_float(v, default=0.0):
            return default if pd.isna(v) else float(v)

        def _safe_int(v, default=0):
            return default if pd.isna(v) else int(v)

        signals.append(OISignal(
            symbol=row.get("symbol", ""),
            date=str(row["date"])[:10],
            spot_price=_safe_float(row.get("spot_close")),
            futures_price=_safe_float(row.get("futures_close")),
            oi_contracts=_safe_int(row.get("oi")),
            oi_change=_safe_int(row.get("oi_change")),
            oi_change_pct=_safe_float(row.get("oi_change_pct")),
            price_change_pct=_safe_float(row.get("price_change_pct")),
            volume=_safe_int(row.get(vol_col)) if vol_col else 0,
            state=state,
            signal=signal,
            confidence=round(max(0, min(1, confidence)), 3),
            streak=streak,
            oi_percentile=round(oi_pctile, 1),
            days_to_expiry=dte,
            in_rollover=in_rollover,
            basis_pct=round(basis, 3),
            reason=reason,
        ))

    return signals


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------

def backtest_oi_strategy(
    symbol: str,
    min_streak: int = 2,
    stop_loss_pct: float = 0.03,
    take_profit_pct: float = 0.05,
    max_hold_days: int = 15,
    exit_on_unwind: bool = True,
    skip_rollover: bool = True,
    days: int = 365,
) -> dict:
    """Backtest OI buildup/unwind strategy.

    Entry: BUY on LONG_BUILDUP x min_streak, SELL on SHORT_BUILDUP x min_streak
    Exit: TP, SL, max hold, or OI state flip
    """
    oi_df = load_oi_data(symbol, days)
    if oi_df.empty or len(oi_df) < 30:
        return {"error": f"Not enough OI data for {symbol}"}

    signals = compute_oi_signals(oi_df, min_streak=min_streak)
    if not signals:
        return {"error": "No signals generated"}

    trades = []
    position = None

    for sig in signals:
        price = sig.spot_price if sig.spot_price > 0 else sig.futures_price
        if price <= 0:
            continue

        # Check exit for open position
        if position is not None:
            days_held = (
                pd.to_datetime(sig.date) - pd.to_datetime(position["entry_date"])
            ).days

            if position["direction"] == "LONG":
                pnl_pct = price / position["entry_price"] - 1
            else:
                pnl_pct = 1 - price / position["entry_price"]

            exit_reason = None

            if pnl_pct >= take_profit_pct:
                exit_reason = "TAKE_PROFIT"
            elif pnl_pct <= -stop_loss_pct:
                exit_reason = "STOP_LOSS"
            elif days_held >= max_hold_days:
                exit_reason = "MAX_HOLD"
            elif exit_on_unwind:
                if position["direction"] == "LONG" and sig.state in (
                    OIState.SHORT_COVERING, OIState.LONG_UNWINDING,
                ):
                    exit_reason = "OI_UNWIND"
                elif position["direction"] == "SHORT" and sig.state in (
                    OIState.LONG_BUILDUP, OIState.LONG_UNWINDING,
                ):
                    exit_reason = "OI_UNWIND"

            if sig.in_rollover and sig.days_to_expiry <= 2:
                exit_reason = "ROLLOVER_EXIT"

            if exit_reason:
                trades.append({
                    "symbol": symbol,
                    "entry_date": position["entry_date"],
                    "exit_date": sig.date,
                    "direction": position["direction"],
                    "entry_price": position["entry_price"],
                    "exit_price": price,
                    "pnl_pct": pnl_pct,
                    "days_held": days_held,
                    "exit_reason": exit_reason,
                    "entry_oi": position["entry_oi"],
                    "exit_oi": sig.oi_contracts,
                    "entry_state": position["entry_state"],
                    "exit_state": sig.state.value,
                })
                position = None

        # Check entry (only if flat)
        if position is None:
            if sig.signal == "BUY" and not (skip_rollover and sig.in_rollover):
                position = {
                    "entry_date": sig.date,
                    "entry_price": price,
                    "direction": "LONG",
                    "entry_oi": sig.oi_contracts,
                    "entry_state": sig.state.value,
                }
            elif sig.signal == "SELL" and not (skip_rollover and sig.in_rollover):
                position = {
                    "entry_date": sig.date,
                    "entry_price": price,
                    "direction": "SHORT",
                    "entry_oi": sig.oi_contracts,
                    "entry_state": sig.state.value,
                }

    # Close remaining position
    if position is not None and signals:
        last = signals[-1]
        price = last.spot_price if last.spot_price > 0 else last.futures_price
        if price > 0:
            pnl_pct = (
                price / position["entry_price"] - 1
                if position["direction"] == "LONG"
                else 1 - price / position["entry_price"]
            )
            days_held = (
                pd.to_datetime(last.date) - pd.to_datetime(position["entry_date"])
            ).days
            trades.append({
                "symbol": symbol,
                "entry_date": position["entry_date"],
                "exit_date": last.date,
                "direction": position["direction"],
                "entry_price": position["entry_price"],
                "exit_price": price,
                "pnl_pct": pnl_pct,
                "days_held": days_held,
                "exit_reason": "END_OF_DATA",
                "entry_oi": position["entry_oi"],
                "exit_oi": last.oi_contracts,
                "entry_state": position["entry_state"],
                "exit_state": last.state.value,
            })

    if not trades:
        return {"error": "No trades generated", "signals_count": len(signals)}

    trades_df = pd.DataFrame(trades)
    winning = trades_df[trades_df["pnl_pct"] > 0]
    losing = trades_df[trades_df["pnl_pct"] <= 0]

    trades_df["cum_return"] = (1 + trades_df["pnl_pct"]).cumprod()
    total_return = trades_df["cum_return"].iloc[-1] - 1
    max_dd = (trades_df["cum_return"] / trades_df["cum_return"].cummax() - 1).min()

    state_stats = trades_df.groupby("entry_state").agg(
        trades=("pnl_pct", "count"),
        win_rate=("pnl_pct", lambda x: (x > 0).mean()),
        avg_pnl=("pnl_pct", "mean"),
    ).to_dict("index")

    exit_stats = trades_df["exit_reason"].value_counts().to_dict()
    trades_df["oi_change_during"] = trades_df["exit_oi"] - trades_df["entry_oi"]

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
            "win_rate": len(winning) / len(trades_df) if len(trades_df) > 0 else 0,
            "avg_win": float(winning["pnl_pct"].mean()) if len(winning) > 0 else 0,
            "avg_loss": float(losing["pnl_pct"].mean()) if len(losing) > 0 else 0,
            "profit_factor": (
                abs(winning["pnl_pct"].sum() / losing["pnl_pct"].sum())
                if len(losing) > 0 and losing["pnl_pct"].sum() != 0 else 0
            ),
            "total_return": float(total_return),
            "max_drawdown": float(max_dd),
            "avg_days_held": float(trades_df["days_held"].mean()),
            "long_trades": int((trades_df["direction"] == "LONG").sum()),
            "short_trades": int((trades_df["direction"] == "SHORT").sum()),
            "by_state": state_stats,
            "by_exit_reason": exit_stats,
        },
    }


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def scan_oi_signals(symbols: list[str] = None, days: int = 30) -> pd.DataFrame:
    """Scan multiple symbols for current OI signals. Returns table sorted by confidence."""
    if symbols is None:
        # Get symbols with futures data
        scon = _psx_con()
        symbols = [r[0] for r in scon.execute("""
            SELECT DISTINCT base_symbol FROM futures_eod
            WHERE market_type = 'FUT' AND date >= date('now', '-7 days')
              AND volume > 0
            ORDER BY base_symbol
        """).fetchall()]
        scon.close()

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
                "spot_price": latest.spot_price,
                "futures_price": latest.futures_price,
            })

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results).sort_values("confidence", ascending=False).reset_index(drop=True)
