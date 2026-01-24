"""Company analytics module for computing derived signals.

This module computes signals from company quote snapshots:
- Position within 52-week range
- Position within day range
- Circuit breaker proximity
- Volume signals (relative volume, spikes)
"""

import sqlite3
from typing import Any

import pandas as pd

# Configurable thresholds
NEAR_HIGH_THRESHOLD = 0.90  # Position >= 90% means near high
NEAR_LOW_THRESHOLD = 0.10   # Position <= 10% means near low
VOLUME_SPIKE_THRESHOLD = 2.0  # Relative volume >= 2x median is spike
VOLUME_HISTORY_DAYS = 20  # Days of history for median volume


def compute_company_signals(
    latest_quote: dict[str, Any],
    history_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Compute derived signals from a company quote snapshot.

    Args:
        latest_quote: Dict with latest quote data (price, ranges, volume, etc.)
        history_df: Optional DataFrame of historical snapshots for volume analysis.
                    Should have 'volume' column and be sorted by ts descending.

    Returns:
        Dict of signal_key -> signal_value pairs.
        Values are strings for storage (booleans as "true"/"false").
    """
    signals: dict[str, Any] = {}

    price = latest_quote.get("price")
    volume = latest_quote.get("volume")

    # 52-week position
    wk52_low = latest_quote.get("wk52_low")
    wk52_high = latest_quote.get("wk52_high")
    pos_52w = _compute_position(price, wk52_low, wk52_high)
    signals["pos_52w"] = _format_float(pos_52w)

    if pos_52w is not None:
        signals["near_52w_high"] = _format_bool(pos_52w >= NEAR_HIGH_THRESHOLD)
        signals["near_52w_low"] = _format_bool(pos_52w <= NEAR_LOW_THRESHOLD)
    else:
        signals["near_52w_high"] = "null"
        signals["near_52w_low"] = "null"

    # Day range position
    day_low = latest_quote.get("day_range_low")
    day_high = latest_quote.get("day_range_high")
    pos_day = _compute_position(price, day_low, day_high)
    signals["pos_day"] = _format_float(pos_day)

    if pos_day is not None:
        signals["near_day_high"] = _format_bool(pos_day >= NEAR_HIGH_THRESHOLD)
        signals["near_day_low"] = _format_bool(pos_day <= NEAR_LOW_THRESHOLD)
    else:
        signals["near_day_high"] = "null"
        signals["near_day_low"] = "null"

    # Circuit breaker proximity
    circuit_low = latest_quote.get("circuit_low")
    circuit_high = latest_quote.get("circuit_high")
    signals["circuit_prox_high_pct"] = _format_float(
        _compute_circuit_prox(price, circuit_high, is_high=True)
    )
    signals["circuit_prox_low_pct"] = _format_float(
        _compute_circuit_prox(price, circuit_low, is_high=False)
    )

    # Volume signals
    rel_volume = None
    if (
        history_df is not None
        and not history_df.empty
        and "volume" in history_df.columns
        and volume is not None
    ):
        # Get last N volume values for median calculation
        vol_series = history_df["volume"].dropna().head(VOLUME_HISTORY_DAYS)
        if len(vol_series) >= 5:  # Need at least 5 data points
            median_vol = vol_series.median()
            if median_vol and median_vol > 0:
                rel_volume = volume / median_vol

    signals["rel_volume"] = _format_float(rel_volume)
    if rel_volume is not None:
        signals["volume_spike"] = _format_bool(rel_volume >= VOLUME_SPIKE_THRESHOLD)
    else:
        signals["volume_spike"] = "null"

    # Generate signal summary (list of triggered signals)
    triggered = []
    if signals.get("near_52w_high") == "true":
        triggered.append("near_52w_high")
    if signals.get("near_52w_low") == "true":
        triggered.append("near_52w_low")
    if signals.get("near_day_high") == "true":
        triggered.append("near_day_high")
    if signals.get("near_day_low") == "true":
        triggered.append("near_day_low")
    if signals.get("volume_spike") == "true":
        triggered.append("volume_spike")

    # Check circuit proximity (if within 2% of circuit)
    try:
        prox_high = float(signals.get("circuit_prox_high_pct", "null"))
        if prox_high <= 2.0:
            triggered.append("near_circuit_high")
    except (ValueError, TypeError):
        pass

    try:
        prox_low = float(signals.get("circuit_prox_low_pct", "null"))
        if prox_low <= 2.0:
            triggered.append("near_circuit_low")
    except (ValueError, TypeError):
        pass

    signals["signal_summary"] = ",".join(triggered) if triggered else "none"

    return signals


def _compute_position(
    value: float | None,
    low: float | None,
    high: float | None,
) -> float | None:
    """Compute position within a range as 0.0 to 1.0.

    Returns None if inputs invalid or range is zero.
    """
    if value is None or low is None or high is None:
        return None
    try:
        denom = high - low
        if denom <= 0:
            return None
        return (value - low) / denom
    except (TypeError, ZeroDivisionError):
        return None


def _compute_circuit_prox(
    price: float | None,
    circuit: float | None,
    is_high: bool,
) -> float | None:
    """Compute proximity to circuit breaker as percentage.

    For high circuit: (circuit_high - price) / price * 100
    For low circuit: (price - circuit_low) / price * 100

    Returns None if price is zero or inputs invalid.
    """
    if price is None or circuit is None or price <= 0:
        return None
    try:
        if is_high:
            return (circuit - price) / price * 100
        else:
            return (price - circuit) / price * 100
    except (TypeError, ZeroDivisionError):
        return None


def _format_float(value: float | None, precision: int = 4) -> str:
    """Format float as string, or 'null' if None."""
    if value is None:
        return "null"
    return f"{value:.{precision}f}"


def _format_bool(value: bool | None) -> str:
    """Format boolean as 'true'/'false' string, or 'null'."""
    if value is None:
        return "null"
    return "true" if value else "false"


def persist_company_signals(
    con: sqlite3.Connection,
    symbol: str,
    ts: str,
    signals: dict[str, Any],
) -> int:
    """Persist computed signals to company_signal_snapshots table.

    Args:
        con: Database connection.
        symbol: Stock symbol.
        ts: Timestamp of the quote snapshot.
        signals: Dict of signal_key -> signal_value.

    Returns:
        Number of signals inserted.
    """
    count = 0
    for key, value in signals.items():
        try:
            con.execute(
                """
                INSERT INTO company_signal_snapshots
                    (symbol, ts, signal_key, signal_value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, ts, signal_key) DO UPDATE SET
                    signal_value = excluded.signal_value
                """,
                (symbol.upper(), ts, key, str(value)),
            )
            count += 1
        except Exception:
            pass  # Skip individual signal errors

    con.commit()
    return count


def get_company_signals(
    con: sqlite3.Connection,
    symbol: str,
    ts: str | None = None,
) -> dict[str, str]:
    """Get signals for a symbol at a specific timestamp.

    Args:
        con: Database connection.
        symbol: Stock symbol.
        ts: Timestamp to get signals for. If None, gets latest.

    Returns:
        Dict of signal_key -> signal_value.
    """
    if ts is None:
        # Get latest timestamp for this symbol
        cur = con.execute(
            """
            SELECT ts FROM company_signal_snapshots
            WHERE symbol = ?
            ORDER BY ts DESC LIMIT 1
            """,
            (symbol.upper(),),
        )
        row = cur.fetchone()
        if not row:
            return {}
        ts = row[0]

    cur = con.execute(
        """
        SELECT signal_key, signal_value
        FROM company_signal_snapshots
        WHERE symbol = ? AND ts = ?
        """,
        (symbol.upper(), ts),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def get_signals_history(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 2000,
) -> pd.DataFrame:
    """Get signal history for a symbol.

    Args:
        con: Database connection.
        symbol: Stock symbol.
        limit: Maximum rows to return.

    Returns:
        DataFrame with ts, signal_key, signal_value columns.
    """
    return pd.read_sql_query(
        """
        SELECT ts, signal_key, signal_value
        FROM company_signal_snapshots
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        con,
        params=[symbol.upper(), limit],
    )


def compute_and_persist_signals(
    con: sqlite3.Connection,
    symbol: str,
    ts: str,
    latest_quote: dict[str, Any],
    history_limit: int = 50,
) -> dict[str, Any]:
    """Compute signals from quote and persist to database.

    This is the main entry point for signal computation after a snapshot.

    Args:
        con: Database connection.
        symbol: Stock symbol.
        ts: Timestamp of the snapshot.
        latest_quote: Quote data dict.
        history_limit: Number of historical snapshots to load for volume analysis.

    Returns:
        Dict of computed signals.
    """
    # Load historical snapshots for volume analysis
    history_df = pd.read_sql_query(
        """
        SELECT ts, volume FROM company_quote_snapshots
        WHERE symbol = ? AND ts < ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        con,
        params=[symbol.upper(), ts, history_limit],
    )

    # Compute signals
    signals = compute_company_signals(latest_quote, history_df)

    # Persist to database
    persist_company_signals(con, symbol, ts, signals)

    return signals
