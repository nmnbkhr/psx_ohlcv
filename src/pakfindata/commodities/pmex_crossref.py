"""PMEX Cross-Reference — map PMEX contracts to global benchmarks, compute premiums.

Maps PMEX base products to their COMEX/NYMEX/ICE equivalents via yfinance tickers.
Computes premium/discount of PMEX prices relative to international benchmarks.

Data sources:
  - pmex_ohlc (commod.db) — PMEX daily close + fx_rate
  - commodity_eod (psx.sqlite) — yfinance global commodity prices
  - commodity_fx_rates (psx.sqlite) — USD/PKR daily rate
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, timedelta

import pandas as pd

from .pmex_contract_calendar import parse_contract

logger = logging.getLogger("pakfindata.commodities.pmex_crossref")


# ─────────────────────────────────────────────────────────────────────────────
# PMEX → Global benchmark mapping
# ─────────────────────────────────────────────────────────────────────────────

PMEX_GLOBAL_MAP: dict[str, dict] = {
    # Gold (USD-denominated PMEX contracts → COMEX Gold)
    "GOMOZ":      {"yf_ticker": "GC=F", "commodity": "GOLD", "unit": "USD/oz", "needs_fx": False},
    "GO1OZ":      {"yf_ticker": "GC=F", "commodity": "GOLD", "unit": "USD/oz", "needs_fx": False},
    "GO10OZ":     {"yf_ticker": "GC=F", "commodity": "GOLD", "unit": "USD/oz", "needs_fx": False},
    "GO100OZ":    {"yf_ticker": "GC=F", "commodity": "GOLD", "unit": "USD/oz", "needs_fx": False},
    # Gold (PKR-denominated → needs FX conversion)
    "TOLAGOLD":   {"yf_ticker": "GC=F", "commodity": "GOLD", "unit": "PKR/tola", "needs_fx": True,
                   "conversion": "tola_to_oz"},
    "MTOLAGOLD":  {"yf_ticker": "GC=F", "commodity": "GOLD", "unit": "PKR/tola", "needs_fx": True,
                   "conversion": "tola_to_oz"},
    # Silver
    "SL100OZ":    {"yf_ticker": "SI=F", "commodity": "SILVER", "unit": "USD/oz", "needs_fx": False},
    "SL1000OZ":   {"yf_ticker": "SI=F", "commodity": "SILVER", "unit": "USD/oz", "needs_fx": False},
    # Crude Oil
    "CRUDE10":    {"yf_ticker": "CL=F", "commodity": "CRUDE_WTI", "unit": "USD/bbl", "needs_fx": False},
    "CRUDE100":   {"yf_ticker": "CL=F", "commodity": "CRUDE_WTI", "unit": "USD/bbl", "needs_fx": False},
    "CRUDE1000":  {"yf_ticker": "CL=F", "commodity": "CRUDE_WTI", "unit": "USD/bbl", "needs_fx": False},
    "BRENT10":    {"yf_ticker": "BZ=F", "commodity": "BRENT", "unit": "USD/bbl", "needs_fx": False},
    "BRENT100":   {"yf_ticker": "BZ=F", "commodity": "BRENT", "unit": "USD/bbl", "needs_fx": False},
    "BRENT1000":  {"yf_ticker": "BZ=F", "commodity": "BRENT", "unit": "USD/bbl", "needs_fx": False},
    # Natural Gas
    "NGAS10K":    {"yf_ticker": "NG=F", "commodity": "NATURAL_GAS", "unit": "USD/mmbtu", "needs_fx": False},
    "NGAS1K":     {"yf_ticker": "NG=F", "commodity": "NATURAL_GAS", "unit": "USD/mmbtu", "needs_fx": False},
    # Copper
    "COPPER":     {"yf_ticker": "HG=F", "commodity": "COPPER", "unit": "USD/lb", "needs_fx": False},
    "COPPER25K":  {"yf_ticker": "HG=F", "commodity": "COPPER", "unit": "USD/lb", "needs_fx": False},
    # Indices
    "NSDQ100":    {"yf_ticker": "NQ=F", "commodity": None, "unit": "index", "needs_fx": False},
    "2NSDQ100":   {"yf_ticker": "NQ=F", "commodity": None, "unit": "index", "needs_fx": False},
    "SP500":      {"yf_ticker": "ES=F", "commodity": None, "unit": "index", "needs_fx": False},
    "DJ":         {"yf_ticker": "YM=F", "commodity": None, "unit": "index", "needs_fx": False},
    # Platinum / Palladium
    "PLATINUM5":  {"yf_ticker": "PL=F", "commodity": "PLATINUM", "unit": "USD/oz", "needs_fx": False},
    "PLATINUM50": {"yf_ticker": "PL=F", "commodity": "PLATINUM", "unit": "USD/oz", "needs_fx": False},
    "PALDIUM100": {"yf_ticker": "PA=F", "commodity": "PALLADIUM", "unit": "USD/oz", "needs_fx": False},
    # Aluminum
    "ALUMINUM1":  {"yf_ticker": "ALI=F", "commodity": "ALUMINUM", "unit": "USD/MT", "needs_fx": False},
    "ALUMINUM5":  {"yf_ticker": "ALI=F", "commodity": "ALUMINUM", "unit": "USD/MT", "needs_fx": False},
}

# 1 troy oz = 2.43 tola (approx); 1 tola = 11.6638 grams
TOLA_PER_OZ = 2.430


# ─────────────────────────────────────────────────────────────────────────────
# Lookup
# ─────────────────────────────────────────────────────────────────────────────


def get_global_benchmark(pmex_base: str) -> dict | None:
    """Look up the global equivalent for a PMEX base product.

    Args:
        pmex_base: e.g. "GO1OZ", "CRUDE100"

    Returns:
        Dict with yf_ticker, commodity, unit, needs_fx, or None if unmapped.
    """
    return PMEX_GLOBAL_MAP.get(pmex_base)


def get_mappable_bases() -> list[str]:
    """Return list of all PMEX base products that have a global mapping."""
    return sorted(PMEX_GLOBAL_MAP.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Premium/Discount Calculation
# ─────────────────────────────────────────────────────────────────────────────


def compute_premium(
    pmex_price: float,
    global_price: float,
    fx_rate: float | None = None,
    conversion: str | None = None,
) -> dict:
    """Compute premium/discount of PMEX price vs global benchmark.

    For USD-denominated PMEX contracts, comparison is direct.
    For PKR-denominated (TOLAGOLD), convert PMEX to USD/oz first.

    Args:
        pmex_price: PMEX contract close price.
        global_price: Global benchmark close (e.g. COMEX gold).
        fx_rate: USD/PKR rate (required if conversion needed).
        conversion: "tola_to_oz" to convert PKR/tola → USD/oz.

    Returns:
        Dict with keys: pmex_price_usd, global_price, premium_abs, premium_pct.
    """
    if global_price <= 0:
        return {"pmex_price_usd": None, "global_price": global_price,
                "premium_abs": None, "premium_pct": None}

    pmex_usd = pmex_price
    if conversion == "tola_to_oz" and fx_rate and fx_rate > 0:
        # Convert PKR/tola → USD/oz
        # pmex_price is in PKR per tola
        # USD per tola = PKR_per_tola / USD_PKR_rate
        # USD per oz = USD_per_tola * tola_per_oz
        pmex_usd = (pmex_price / fx_rate) * TOLA_PER_OZ
    elif fx_rate and fx_rate > 0 and conversion:
        # Generic PKR → USD
        pmex_usd = pmex_price / fx_rate

    premium_abs = pmex_usd - global_price
    premium_pct = (premium_abs / global_price) * 100

    return {
        "pmex_price_usd": round(pmex_usd, 4),
        "global_price": global_price,
        "premium_abs": round(premium_abs, 4),
        "premium_pct": round(premium_pct, 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Time Series Premium (joins PMEX OHLC with yfinance commodity_eod)
# ─────────────────────────────────────────────────────────────────────────────


def premium_timeseries(
    con_commod: sqlite3.Connection,
    con_psx: sqlite3.Connection,
    pmex_symbol: str,
    lookback_days: int = 90,
) -> pd.DataFrame:
    """Join PMEX close prices with global benchmark close by date.

    Args:
        con_commod: Connection to commod.db (pmex_ohlc).
        con_psx: Connection to psx.sqlite (commodity_eod).
        pmex_symbol: Full PMEX contract, e.g. "GO1OZ-JU26".
        lookback_days: Days to look back.

    Returns:
        DataFrame with columns: date, pmex_close, global_close, fx_rate,
        pmex_usd, premium_abs, premium_pct.
    """
    pc = parse_contract(pmex_symbol)
    mapping = get_global_benchmark(pc.base)
    if mapping is None:
        logger.warning("No global mapping for PMEX base: %s", pc.base)
        return pd.DataFrame()

    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    # PMEX OHLC data
    pmex_rows = con_commod.execute(
        """
        SELECT trading_date as date, close as pmex_close, fx_rate
        FROM pmex_ohlc
        WHERE symbol = ? AND trading_date >= ? AND close > 0
        ORDER BY trading_date
        """,
        (pmex_symbol, cutoff),
    ).fetchall()
    if not pmex_rows:
        return pd.DataFrame()
    pmex_df = pd.DataFrame([dict(r) for r in pmex_rows])

    # Global benchmark data (yfinance commodity_eod uses the commodity symbol, not ticker)
    yf_symbol = mapping["commodity"] or mapping["yf_ticker"]
    global_rows = con_psx.execute(
        """
        SELECT date, close as global_close
        FROM commodity_eod
        WHERE symbol = ? AND source = 'yfinance' AND date >= ? AND close > 0
        ORDER BY date
        """,
        (yf_symbol, cutoff),
    ).fetchall()
    if not global_rows:
        return pd.DataFrame()
    global_df = pd.DataFrame([dict(r) for r in global_rows])

    # Merge on date
    merged = pd.merge(pmex_df, global_df, on="date", how="inner")
    if merged.empty:
        return pd.DataFrame()

    # Compute premium
    conversion = mapping.get("conversion")
    results = []
    for _, row in merged.iterrows():
        prem = compute_premium(
            row["pmex_close"],
            row["global_close"],
            fx_rate=row.get("fx_rate"),
            conversion=conversion,
        )
        results.append({
            "date": row["date"],
            "pmex_close": row["pmex_close"],
            "global_close": row["global_close"],
            "fx_rate": row.get("fx_rate"),
            "pmex_usd": prem["pmex_price_usd"],
            "premium_abs": prem["premium_abs"],
            "premium_pct": prem["premium_pct"],
        })

    return pd.DataFrame(results)


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard Summary (latest premium for all mappable products)
# ─────────────────────────────────────────────────────────────────────────────


def premium_dashboard_data(
    con_commod: sqlite3.Connection,
    con_psx: sqlite3.Connection,
) -> pd.DataFrame:
    """Compute latest premium for all mappable PMEX products.

    Args:
        con_commod: Connection to commod.db.
        con_psx: Connection to psx.sqlite.

    Returns:
        DataFrame with columns: base, commodity, pmex_symbol, pmex_close,
        global_close, fx_rate, pmex_usd, premium_abs, premium_pct.
    """
    # Get all distinct symbols from pmex_ohlc
    ohlc_symbols = con_commod.execute(
        "SELECT DISTINCT symbol FROM pmex_ohlc WHERE traded_volume > 0"
    ).fetchall()
    all_symbols = [r["symbol"] for r in ohlc_symbols]

    rows = []
    seen_bases = set()

    for sym in all_symbols:
        pc = parse_contract(sym)
        if pc.is_intraday or pc.base in seen_bases:
            continue

        mapping = get_global_benchmark(pc.base)
        if mapping is None:
            continue

        # Latest PMEX close
        pmex_row = con_commod.execute(
            """
            SELECT close, fx_rate, trading_date
            FROM pmex_ohlc
            WHERE symbol = ? AND close > 0
            ORDER BY trading_date DESC LIMIT 1
            """,
            (sym,),
        ).fetchone()
        if not pmex_row:
            continue

        # Latest global close
        yf_symbol = mapping["commodity"] or mapping["yf_ticker"]
        global_row = con_psx.execute(
            """
            SELECT close, date
            FROM commodity_eod
            WHERE symbol = ? AND source = 'yfinance' AND close > 0
            ORDER BY date DESC LIMIT 1
            """,
            (yf_symbol,),
        ).fetchone()
        if not global_row:
            continue

        prem = compute_premium(
            pmex_row["close"],
            global_row["close"],
            fx_rate=pmex_row["fx_rate"],
            conversion=mapping.get("conversion"),
        )

        seen_bases.add(pc.base)
        rows.append({
            "base": pc.base,
            "commodity": pc.commodity,
            "pmex_symbol": sym,
            "pmex_date": pmex_row["trading_date"],
            "pmex_close": pmex_row["close"],
            "global_symbol": yf_symbol,
            "global_date": global_row["date"],
            "global_close": global_row["close"],
            "fx_rate": pmex_row["fx_rate"],
            "pmex_usd": prem["pmex_price_usd"],
            "premium_abs": prem["premium_abs"],
            "premium_pct": prem["premium_pct"],
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()
