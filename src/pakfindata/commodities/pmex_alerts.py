"""PMEX Alerts — threshold-based monitoring and alert generation.

All checks are computed on-demand (no background daemon required).
Call run_all_checks() to get a list of current alerts.

Data sources:
  - pmex_ohlc (commod.db) — close prices, volume
  - pmex_market_watch (psx.sqlite) — bid/ask spreads
  - pmex_margins (commod.db) — upper/lower limit bands
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger("pakfindata.commodities.pmex_alerts")


# ─────────────────────────────────────────────────────────────────────────────
# Alert data class
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class PmexAlert:
    """A single PMEX monitoring alert."""
    alert_type: str       # "price_threshold", "volume_spike", "spread_wide", "limit_proximity"
    severity: str         # "info", "warning", "critical"
    contract: str         # Contract or symbol name
    message: str          # Human-readable description
    value: float          # Current value that triggered the alert
    threshold: float      # Threshold that was breached
    triggered_at: str     # ISO 8601 timestamp

    def to_dict(self) -> dict:
        return asdict(self)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Price Threshold Alerts (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def check_price_thresholds(
    con: sqlite3.Connection,
    thresholds: dict[str, dict] | None = None,
) -> list[PmexAlert]:
    """Check latest PMEX OHLC close prices against configured thresholds.

    Args:
        con: Connection to commod.db.
        thresholds: Dict of {symbol: {"above": float, "below": float}}.
                    If None, uses sensible defaults for major contracts.

    Returns:
        List of PmexAlert for breached thresholds.
    """
    if thresholds is None:
        thresholds = _default_price_thresholds()

    alerts = []
    for symbol, bounds in thresholds.items():
        row = con.execute(
            """
            SELECT close, trading_date FROM pmex_ohlc
            WHERE symbol LIKE ? AND close > 0
            ORDER BY trading_date DESC LIMIT 1
            """,
            (f"{symbol}%",),
        ).fetchone()
        if not row:
            continue

        close = row["close"]
        above = bounds.get("above")
        below = bounds.get("below")

        if above is not None and close >= above:
            alerts.append(PmexAlert(
                alert_type="price_threshold",
                severity="warning",
                contract=symbol,
                message=f"{symbol} at {close:.2f}, above threshold {above:.2f}",
                value=close,
                threshold=above,
                triggered_at=_now_iso(),
            ))
        if below is not None and close <= below:
            alerts.append(PmexAlert(
                alert_type="price_threshold",
                severity="warning",
                contract=symbol,
                message=f"{symbol} at {close:.2f}, below threshold {below:.2f}",
                value=close,
                threshold=below,
                triggered_at=_now_iso(),
            ))

    return alerts


def _default_price_thresholds() -> dict[str, dict]:
    """Sensible default price thresholds (can be overridden by config)."""
    return {
        "GO1OZ": {"above": 3500, "below": 1800},
        "CRUDE10": {"above": 120, "below": 40},
        "BRENT10": {"above": 130, "below": 45},
        "NGAS1K": {"above": 8, "below": 1.5},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Volume Spike Detection (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def check_volume_spikes(
    con: sqlite3.Connection,
    z_threshold: float = 2.5,
    lookback: int = 20,
) -> list[PmexAlert]:
    """Detect symbols with abnormally high volume (z-score above threshold).

    Args:
        con: Connection to commod.db.
        z_threshold: Z-score cutoff (default 2.5 = ~99th percentile).
        lookback: Rolling window for mean/std computation.

    Returns:
        List of PmexAlert for volume spikes.
    """
    # Get symbols with recent activity
    cutoff = (date.today() - timedelta(days=lookback + 5)).isoformat()
    symbols = con.execute(
        """
        SELECT DISTINCT symbol FROM pmex_ohlc
        WHERE trading_date >= ? AND traded_volume > 0
        """,
        (cutoff,),
    ).fetchall()

    alerts = []
    for sym_row in symbols:
        symbol = sym_row["symbol"]
        rows = con.execute(
            """
            SELECT trading_date, traded_volume
            FROM pmex_ohlc
            WHERE symbol = ? AND trading_date >= ?
            ORDER BY trading_date
            """,
            (symbol, cutoff),
        ).fetchall()
        if len(rows) < lookback:
            continue

        vols = [r["traded_volume"] or 0 for r in rows]
        latest_vol = vols[-1]
        historical = vols[:-1]

        mean_vol = np.mean(historical)
        std_vol = np.std(historical)
        if std_vol <= 0:
            continue

        z_score = (latest_vol - mean_vol) / std_vol
        if z_score >= z_threshold:
            severity = "critical" if z_score >= 4.0 else "warning"
            alerts.append(PmexAlert(
                alert_type="volume_spike",
                severity=severity,
                contract=symbol,
                message=f"{symbol} volume {latest_vol} (z={z_score:.1f}, avg={mean_vol:.0f})",
                value=float(latest_vol),
                threshold=float(mean_vol + z_threshold * std_vol),
                triggered_at=_now_iso(),
            ))

    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# Spread Widening (from pmex_market_watch in psx.sqlite)
# ─────────────────────────────────────────────────────────────────────────────


def check_spread_widening(
    con: sqlite3.Connection,
    percentile_threshold: float = 95,
) -> list[PmexAlert]:
    """Alert when current bid-ask spread exceeds historical percentile.

    Args:
        con: Connection to psx.sqlite (pmex_market_watch table).
        percentile_threshold: Percentile cutoff (default 95th).

    Returns:
        List of PmexAlert for unusually wide spreads.
    """
    # Get contracts with enough spread history (30+ days)
    cutoff_30d = (date.today() - timedelta(days=30)).isoformat()

    contracts = con.execute(
        """
        SELECT contract, COUNT(*) as cnt
        FROM pmex_market_watch
        WHERE snapshot_date >= ? AND bid > 0 AND ask > 0
        GROUP BY contract HAVING cnt >= 10
        """,
        (cutoff_30d,),
    ).fetchall()

    alerts = []
    for c_row in contracts:
        contract = c_row["contract"]

        # Historical spreads
        hist_rows = con.execute(
            """
            SELECT (ask - bid) / ((bid + ask) / 2.0) * 100 as spread_pct
            FROM pmex_market_watch
            WHERE contract = ? AND snapshot_date >= ? AND bid > 0 AND ask > 0
            ORDER BY snapshot_date
            """,
            (contract, cutoff_30d),
        ).fetchall()
        spreads = [r["spread_pct"] for r in hist_rows if r["spread_pct"] is not None]
        if len(spreads) < 10:
            continue

        current_spread = spreads[-1]
        pctile = float(np.percentile(spreads[:-1], percentile_threshold))

        if current_spread > pctile:
            alerts.append(PmexAlert(
                alert_type="spread_wide",
                severity="warning",
                contract=contract,
                message=f"{contract} spread {current_spread:.3f}% > {percentile_threshold}th pctile ({pctile:.3f}%)",
                value=current_spread,
                threshold=pctile,
                triggered_at=_now_iso(),
            ))

    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# Limit Band Proximity (joins pmex_ohlc + pmex_margins in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def check_limit_proximity(
    con: sqlite3.Connection,
    proximity_pct: float = 5.0,
) -> list[PmexAlert]:
    """Alert when latest close is within proximity_pct of daily limit bands.

    Args:
        con: Connection to commod.db (both pmex_ohlc and pmex_margins).
        proximity_pct: Percentage distance from limit to trigger alert.

    Returns:
        List of PmexAlert for limit-proximate contracts.
    """
    # Get latest margins with limits
    margins = con.execute(
        """
        SELECT m.contract_code, m.lower_limit, m.upper_limit, m.reference_price
        FROM pmex_margins m
        INNER JOIN (
            SELECT contract_code, MAX(report_date) as max_date
            FROM pmex_margins
            GROUP BY contract_code
        ) latest ON m.contract_code = latest.contract_code AND m.report_date = latest.max_date
        WHERE m.lower_limit > 0 AND m.upper_limit > 0
        """
    ).fetchall()

    alerts = []
    for m_row in margins:
        code = m_row["contract_code"]
        lower = m_row["lower_limit"]
        upper = m_row["upper_limit"]

        # Find matching OHLC symbol (margin contract codes may differ slightly)
        ohlc_row = con.execute(
            """
            SELECT close, symbol, trading_date FROM pmex_ohlc
            WHERE symbol LIKE ? AND close > 0
            ORDER BY trading_date DESC LIMIT 1
            """,
            (f"%{code}%",),
        ).fetchone()
        if not ohlc_row:
            continue

        close = ohlc_row["close"]
        symbol = ohlc_row["symbol"]

        # Check proximity to upper limit
        if upper > 0:
            dist_upper = (upper - close) / upper * 100
            if 0 < dist_upper <= proximity_pct:
                alerts.append(PmexAlert(
                    alert_type="limit_proximity",
                    severity="critical" if dist_upper <= 2.0 else "warning",
                    contract=symbol,
                    message=f"{symbol} at {close:.2f}, {dist_upper:.1f}% from upper limit {upper:.2f}",
                    value=close,
                    threshold=upper,
                    triggered_at=_now_iso(),
                ))

        # Check proximity to lower limit
        if lower > 0:
            dist_lower = (close - lower) / lower * 100
            if 0 < dist_lower <= proximity_pct:
                alerts.append(PmexAlert(
                    alert_type="limit_proximity",
                    severity="critical" if dist_lower <= 2.0 else "warning",
                    contract=symbol,
                    message=f"{symbol} at {close:.2f}, {dist_lower:.1f}% from lower limit {lower:.2f}",
                    value=close,
                    threshold=lower,
                    triggered_at=_now_iso(),
                ))

    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# Intraday Alerts (from pmex_intraday_snapshots in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def check_intraday_price_moves(
    con: sqlite3.Connection,
    move_pct: float = 3.0,
) -> list[PmexAlert]:
    """Detect contracts with large intraday price moves.

    Compares the latest intraday last_price to the day's open.

    Args:
        con: Connection to commod.db (pmex_intraday_snapshots).
        move_pct: Minimum absolute % move to trigger alert.

    Returns:
        List of PmexAlert for large intraday moves.
    """
    today = date.today().isoformat()
    rows = con.execute(
        """
        SELECT contract, category,
               MIN(CASE WHEN rn = 1 THEN last_price END) as day_open,
               last_price as current_price
        FROM (
            SELECT contract, category, last_price,
                   ROW_NUMBER() OVER (PARTITION BY contract ORDER BY snapshot_ts) as rn
            FROM pmex_intraday_snapshots
            WHERE snapshot_date = ? AND last_price > 0
        )
        GROUP BY contract
        HAVING day_open > 0
        """,
        (today,),
    ).fetchall()

    # Fallback: simpler query if window functions fail
    if not rows:
        rows = con.execute(
            """
            SELECT s.contract, s.category, s.last_price as current_price,
                   first_poll.last_price as day_open
            FROM pmex_intraday_snapshots s
            INNER JOIN (
                SELECT contract, MIN(snapshot_ts) as min_ts
                FROM pmex_intraday_snapshots
                WHERE snapshot_date = ? AND last_price > 0
                GROUP BY contract
            ) fp ON s.contract = fp.contract
            INNER JOIN pmex_intraday_snapshots first_poll
                ON first_poll.contract = fp.contract AND first_poll.snapshot_ts = fp.min_ts
            INNER JOIN (
                SELECT contract, MAX(snapshot_ts) as max_ts
                FROM pmex_intraday_snapshots
                WHERE snapshot_date = ?
                GROUP BY contract
            ) lp ON s.contract = lp.contract AND s.snapshot_ts = lp.max_ts
            WHERE first_poll.last_price > 0
            """,
            (today, today),
        ).fetchall()

    alerts = []
    for r in rows:
        row = dict(r)
        day_open = row.get("day_open", 0)
        current = row.get("current_price", 0)
        if not day_open or not current:
            continue

        pct_move = (current - day_open) / day_open * 100
        if abs(pct_move) >= move_pct:
            direction = "up" if pct_move > 0 else "down"
            severity = "critical" if abs(pct_move) >= move_pct * 2 else "warning"
            alerts.append(PmexAlert(
                alert_type="intraday_move",
                severity=severity,
                contract=row["contract"],
                message=f"{row['contract']} {direction} {abs(pct_move):.1f}% intraday (open={day_open:.2f}, now={current:.2f})",
                value=pct_move,
                threshold=move_pct,
                triggered_at=_now_iso(),
            ))

    return alerts


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────


def run_all_checks(
    con_commod: sqlite3.Connection,
    con_psx: sqlite3.Connection,
    config: dict | None = None,
) -> list[PmexAlert]:
    """Run all PMEX alert checks and return combined results.

    Args:
        con_commod: Connection to commod.db.
        con_psx: Connection to psx.sqlite.
        config: Optional overrides for thresholds. Keys:
            - price_thresholds: dict[str, dict]
            - volume_z_threshold: float
            - volume_lookback: int
            - spread_percentile: float
            - limit_proximity_pct: float

    Returns:
        List of PmexAlert sorted by severity (critical first).
    """
    cfg = config or {}
    alerts: list[PmexAlert] = []

    try:
        alerts.extend(check_price_thresholds(
            con_commod,
            thresholds=cfg.get("price_thresholds"),
        ))
    except Exception as e:
        logger.warning("Price threshold check failed: %s", e)

    try:
        alerts.extend(check_volume_spikes(
            con_commod,
            z_threshold=cfg.get("volume_z_threshold", 2.5),
            lookback=cfg.get("volume_lookback", 20),
        ))
    except Exception as e:
        logger.warning("Volume spike check failed: %s", e)

    try:
        alerts.extend(check_spread_widening(
            con_psx,
            percentile_threshold=cfg.get("spread_percentile", 95),
        ))
    except Exception as e:
        logger.warning("Spread widening check failed: %s", e)

    try:
        alerts.extend(check_limit_proximity(
            con_commod,
            proximity_pct=cfg.get("limit_proximity_pct", 5.0),
        ))
    except Exception as e:
        logger.warning("Limit proximity check failed: %s", e)

    try:
        alerts.extend(check_intraday_price_moves(
            con_commod,
            move_pct=cfg.get("intraday_move_pct", 3.0),
        ))
    except Exception as e:
        logger.warning("Intraday move check failed: %s", e)

    # Sort: critical first, then warning, then info
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: severity_order.get(a.severity, 9))

    return alerts
