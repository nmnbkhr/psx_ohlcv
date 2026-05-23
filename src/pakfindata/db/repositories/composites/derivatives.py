"""Composite repository: derivatives views.

One composite today: ``overview``. Combines futures_eod (near-month
FUT/CONT) + eod_ohlcv (spot close) to compute basis per base_symbol.

Open Interest (OI) is NOT in this composite — see
``composite_aggregator_pattern.md`` §8. OI lives only in disk XLS at
``/mnt/e/psxdata/downloads/daily/<date>/futures/futures_oi_dfc_*.xls``;
composites are DB-native. The response surfaces
``data_quality.oi = {"status": "not_available", "source_path_pattern": ...}``
so clients can render "OI data not yet in DB" without guessing.
Adoption of OI into the composite is deferred to whichever milestone
ingests it into a DB table.

Reads from:
    futures_eod    — FUT/CONT near-month rows
    eod_ohlcv      — spot close for the base symbol on the same date
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd

OI_DISK_PATTERN = (
    "/mnt/e/psxdata/downloads/daily/<date>/futures/futures_oi_dfc_*.xls"
)


def _latest_futures_date(con: sqlite3.Connection) -> str | None:
    row = con.execute(
        "SELECT MAX(date) FROM futures_eod "
        "WHERE market_type IN ('FUT', 'CONT')"
    ).fetchone()
    return row[0] if row and row[0] else None


def get_derivatives_overview(
    con: sqlite3.Connection,
    *,
    date: str | None = None,
    top_n: int = 10,
) -> dict[str, Any]:
    """Return basis + summary composite for the given (or latest) date.

    Response shape per ``composite_aggregator_pattern.md`` §5:
        {as_of, summary, basis_premium[], basis_discount[], data_quality{...}}

    OI absent by design — see module docstring and §8 of the pattern.
    """
    if date is None:
        date = _latest_futures_date(con)

    if date is None:
        # No futures data at all. Return empty but valid shape.
        return {
            "as_of": None,
            "summary": _empty_summary(),
            "basis_premium": [],
            "basis_discount": [],
            "data_quality": _data_quality(con, None),
        }

    # Step 1: futures near-month per base_symbol
    fut_df = pd.read_sql_query(
        """
        SELECT base_symbol, contract_month,
               close      AS fut_close,
               volume     AS fut_volume,
               prev_close AS fut_prev_close
          FROM futures_eod
         WHERE date = ?
           AND market_type IN ('FUT', 'CONT')
           AND close > 0
           AND contract_month IS NOT NULL
         ORDER BY base_symbol, contract_month
        """,
        con,
        params=(date,),
    )

    if fut_df.empty:
        return {
            "as_of": date,
            "summary": _empty_summary(),
            "basis_premium": [],
            "basis_discount": [],
            "data_quality": _data_quality(con, date),
        }

    near = fut_df.drop_duplicates("base_symbol", keep="first")

    # Step 2: spot prices
    spot_df = pd.read_sql_query(
        """
        SELECT symbol, close AS spot_close, volume AS spot_volume
          FROM eod_ohlcv
         WHERE date = ? AND close > 0
        """,
        con,
        params=(date,),
    )

    # Step 3: merge + compute basis
    merged = near.merge(spot_df, left_on="base_symbol", right_on="symbol", how="left")
    merged = merged[merged["spot_close"].notna() & (merged["spot_close"] > 0)].copy()
    merged["basis"] = (merged["fut_close"] - merged["spot_close"]).round(4)
    merged["basis_pct"] = (merged["basis"] / merged["spot_close"] * 100).round(3)

    # Step 4: summary
    premium_count = int((merged["basis_pct"] > 0).sum())
    discount_count = int((merged["basis_pct"] < 0).sum())
    flat_count = int((merged["basis_pct"] == 0).sum())
    summary = {
        "futures_count": int(len(merged)),
        "premium_count": premium_count,
        "discount_count": discount_count,
        "flat_count": flat_count,
        "avg_basis_pct": (
            round(float(merged["basis_pct"].mean()), 4) if len(merged) else None
        ),
        "total_futures_volume": int(merged["fut_volume"].fillna(0).sum()),
    }

    # Step 5: top premium / discount slices
    keep_cols = [
        "base_symbol", "contract_month", "fut_close", "spot_close",
        "basis", "basis_pct", "fut_volume",
    ]

    premium_rows = (
        merged[merged["basis_pct"] > 0]
        .sort_values("basis_pct", ascending=False)
        .head(top_n)[keep_cols]
        .to_dict(orient="records")
    )
    discount_rows = (
        merged[merged["basis_pct"] < 0]
        .sort_values("basis_pct", ascending=True)
        .head(top_n)[keep_cols]
        .to_dict(orient="records")
    )

    return {
        "as_of": date,
        "summary": summary,
        "basis_premium": premium_rows,
        "basis_discount": discount_rows,
        "data_quality": _data_quality(con, date),
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "futures_count": 0,
        "premium_count": 0,
        "discount_count": 0,
        "flat_count": 0,
        "avg_basis_pct": None,
        "total_futures_volume": 0,
    }


def _data_quality(
    con: sqlite3.Connection,
    asof: str | None,
) -> dict[str, dict[str, Any]]:
    """Per-source freshness. OI is explicitly `not_available` because
    its only source is disk XLS — see pattern §8.
    """
    out: dict[str, dict[str, Any]] = {}

    # futures_eod has no catalog row; compute MAX directly.
    fut_max = con.execute(
        "SELECT MAX(date) FROM futures_eod WHERE market_type IN ('FUT', 'CONT')"
    ).fetchone()[0]
    out["futures_eod"] = {
        "status": "ok" if fut_max else "unknown",
        "last_row_date": fut_max,
    }

    # eod_ohlcv via the equity_eod catalog row.
    catalog = con.execute(
        "SELECT status, last_row_date FROM data_freshness "
        "WHERE domain = 'equity_eod'"
    ).fetchone()
    if catalog:
        out["eod_ohlcv"] = {
            "status": catalog[0],
            "last_row_date": catalog[1],
        }
    else:
        out["eod_ohlcv"] = {"status": "unknown", "last_row_date": None}

    # OI — not in DB. Status string is the marker per pattern §8.
    out["oi"] = {
        "status": "not_available",
        "source_path_pattern": OI_DISK_PATTERN,
    }

    return out
