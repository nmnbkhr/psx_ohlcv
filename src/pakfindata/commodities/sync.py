"""Commodity sync orchestrator.

Coordinates fetching from all tiers, deduplication, storage,
and PKR conversion. Follows the same SyncSummary pattern as
the main PSX sync module.
"""

import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from ..config import get_db_path
from ..db import connect, init_schema
from .config import (
    COMMODITY_UNIVERSE,
    CommodityDef,
    get_commodities_by_category,
    get_fred_series,
    get_pk_high_commodities,
    get_yfinance_tickers,
)
from .models import (
    get_commodity_max_date,
    init_commodity_schema,
    record_commodity_sync_end,
    record_commodity_sync_start,
    upsert_commodity_eod,
    upsert_commodity_fx,
    upsert_commodity_monthly,
    upsert_commodity_pkr,
    upsert_commodity_symbol,
)
from .utils import convert_to_pkr

logger = logging.getLogger("pakfindata.commodities.sync")


@dataclass
class CommoditySyncSummary:
    """Summary of a commodity sync operation."""

    run_id: str
    source: str
    symbols_total: int = 0
    symbols_ok: int = 0
    symbols_failed: int = 0
    rows_upserted: int = 0
    pkr_rows: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Seed universe into DB
# ─────────────────────────────────────────────────────────────────────────────

def seed_commodity_universe(db_path: Path | str | None = None) -> int:
    """Seed all commodity definitions from config into the database.

    Returns count of symbols seeded.
    """
    con = connect(db_path)
    init_schema(con)
    init_commodity_schema(con)

    count = 0
    for symbol, cdef in COMMODITY_UNIVERSE.items():
        data = {
            "symbol": cdef.symbol,
            "name": cdef.name,
            "category": cdef.category,
            "unit": cdef.unit,
            "pk_relevance": cdef.pk_relevance,
            "yf_ticker": cdef.yf_ticker,
            "yf_etf": cdef.yf_etf,
            "fred_series": cdef.fred_series,
            "wb_column": cdef.wb_column,
            "pk_unit": cdef.pk_unit,
            "pk_conversion": cdef.pk_conversion,
        }
        upsert_commodity_symbol(con, data)
        count += 1

    con.commit()
    logger.info("Seeded %d commodity symbols", count)
    return count


# ─────────────────────────────────────────────────────────────────────────────
# Tier 1: yfinance daily sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_yfinance(
    db_path: Path | str | None = None,
    symbols: list[str] | None = None,
    category: str | None = None,
    incremental: bool = True,
    period: str = "1y",
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync daily OHLCV from yfinance for commodities + FX.

    Args:
        db_path: Database path.
        symbols: Specific symbols to sync. None = all with yf_ticker.
        category: Filter by category.
        incremental: If True, only fetch data after the latest date in DB.
        period: yfinance period (used if not incremental or no existing data).
        progress_callback: Optional callback(current, total, symbol).

    Returns:
        CommoditySyncSummary with results.
    """
    from .fetcher_yfinance import fetch_single_commodity

    run_id = str(uuid.uuid4())
    con = connect(db_path)
    init_schema(con)
    init_commodity_schema(con)
    record_commodity_sync_start(con, run_id, "yfinance_daily", "yfinance")

    # Build target list
    if symbols:
        targets = [COMMODITY_UNIVERSE[s] for s in symbols if s in COMMODITY_UNIVERSE and COMMODITY_UNIVERSE[s].yf_ticker]
    elif category:
        targets = [c for c in get_commodities_by_category(category) if c.yf_ticker]
    else:
        targets = [c for c in COMMODITY_UNIVERSE.values() if c.yf_ticker]

    summary = CommoditySyncSummary(run_id=run_id, source="yfinance", symbols_total=len(targets))

    for i, commodity in enumerate(targets):
        if progress_callback:
            progress_callback(i + 1, len(targets), commodity.symbol)

        try:
            # Determine start date for incremental
            start = None
            if incremental:
                max_date = get_commodity_max_date(con, commodity.symbol, "yfinance")
                if max_date:
                    start = max_date  # yfinance will overlap, upsert handles dedup

            rows = fetch_single_commodity(commodity, start=start, period=period)

            if rows:
                # Store as EOD or FX depending on category
                if commodity.category == "fx":
                    fx_rows = [
                        {
                            "pair": r["symbol"],
                            "date": r["date"],
                            "open": r["open"],
                            "high": r["high"],
                            "low": r["low"],
                            "close": r["close"],
                            "volume": r["volume"],
                            "source": "yfinance",
                        }
                        for r in rows
                    ]
                    n = upsert_commodity_fx(con, fx_rows)
                else:
                    n = upsert_commodity_eod(con, rows)

                summary.rows_upserted += n
                summary.symbols_ok += 1
                logger.info("Synced %s: %d rows", commodity.symbol, n)
            else:
                summary.symbols_ok += 1  # No data is not a failure
                logger.info("No new data for %s", commodity.symbol)

        except Exception as e:
            summary.symbols_failed += 1
            summary.errors.append((commodity.symbol, str(e)))
            logger.warning("Failed to sync %s: %s", commodity.symbol, e)

    record_commodity_sync_end(
        con, run_id,
        symbols_total=summary.symbols_total,
        symbols_ok=summary.symbols_ok,
        symbols_failed=summary.symbols_failed,
        rows_upserted=summary.rows_upserted,
        error_summary="; ".join(f"{s}: {e}" for s, e in summary.errors[:10]) if summary.errors else None,
    )

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Tier 2: FRED monthly sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_fred(
    db_path: Path | str | None = None,
    api_key: str | None = None,
    symbols: list[str] | None = None,
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync monthly commodity data from FRED.

    Args:
        db_path: Database path.
        api_key: FRED API key. If None, reads from FRED_API_KEY env var.
        symbols: Specific symbols to sync. None = all with fred_series.
        progress_callback: Optional callback(current, total, symbol).

    Returns:
        CommoditySyncSummary.
    """
    from .fetcher_fred import fetch_fred_series

    run_id = str(uuid.uuid4())
    con = connect(db_path)
    init_schema(con)
    init_commodity_schema(con)
    record_commodity_sync_start(con, run_id, "fred_monthly", "fred")

    fred_map = get_fred_series()
    if symbols:
        fred_map = {s: sid for s, sid in fred_map.items() if s in symbols}

    summary = CommoditySyncSummary(run_id=run_id, source="fred", symbols_total=len(fred_map))

    for i, (symbol, series_id) in enumerate(fred_map.items()):
        if progress_callback:
            progress_callback(i + 1, len(fred_map), symbol)

        try:
            rows = fetch_fred_series(series_id, symbol, api_key=api_key)
            if rows:
                n = upsert_commodity_monthly(con, rows)
                summary.rows_upserted += n
                summary.symbols_ok += 1
                logger.info("FRED synced %s (%s): %d rows", symbol, series_id, n)
            else:
                summary.symbols_ok += 1
        except Exception as e:
            summary.symbols_failed += 1
            summary.errors.append((symbol, str(e)))
            logger.warning("FRED failed for %s: %s", symbol, e)

    record_commodity_sync_end(
        con, run_id,
        symbols_total=summary.symbols_total,
        symbols_ok=summary.symbols_ok,
        symbols_failed=summary.symbols_failed,
        rows_upserted=summary.rows_upserted,
    )

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Tier 3: World Bank Pink Sheet sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_worldbank(
    db_path: Path | str | None = None,
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync monthly commodity data from the World Bank Pink Sheet.

    Returns CommoditySyncSummary.
    """
    from .fetcher_worldbank import fetch_worldbank_pinksheet

    run_id = str(uuid.uuid4())
    con = connect(db_path)
    init_schema(con)
    init_commodity_schema(con)
    record_commodity_sync_start(con, run_id, "worldbank_monthly", "worldbank")

    summary = CommoditySyncSummary(run_id=run_id, source="worldbank")

    try:
        all_rows = fetch_worldbank_pinksheet()
        if all_rows:
            # Group by symbol for reporting
            by_symbol: dict[str, list[dict]] = {}
            for row in all_rows:
                by_symbol.setdefault(row["symbol"], []).append(row)

            summary.symbols_total = len(by_symbol)
            for symbol, rows in by_symbol.items():
                n = upsert_commodity_monthly(con, rows)
                summary.rows_upserted += n
                summary.symbols_ok += 1

            if progress_callback:
                progress_callback(summary.symbols_total, summary.symbols_total, "worldbank")
        else:
            logger.info("No data from World Bank Pink Sheet")
    except Exception as e:
        summary.symbols_failed += 1
        summary.errors.append(("worldbank", str(e)))
        logger.warning("World Bank sync failed: %s", e)

    record_commodity_sync_end(
        con, run_id,
        symbols_total=summary.symbols_total,
        symbols_ok=summary.symbols_ok,
        symbols_failed=summary.symbols_failed,
        rows_upserted=summary.rows_upserted,
    )

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# PKR conversion pass
# ─────────────────────────────────────────────────────────────────────────────

def compute_pkr_prices(
    db_path: Path | str | None = None,
    usd_pkr_override: float | None = None,
) -> int:
    """Compute PKR prices for all commodities with a pk_conversion function.

    Uses the latest USD/PKR rate from commodity_fx_rates (or override).
    Processes all commodity_eod rows that don't have a commodity_pkr entry.

    Returns count of PKR rows upserted.
    """
    con = connect(db_path)
    init_commodity_schema(con)

    # Get USD/PKR rate
    usd_pkr = usd_pkr_override
    if usd_pkr is None:
        row = con.execute(
            "SELECT close FROM commodity_fx_rates WHERE pair='USD_PKR' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if row:
            usd_pkr = row["close"]
        else:
            # Fallback: try fetching live
            from .fetcher_yfinance import get_latest_usd_pkr
            usd_pkr = get_latest_usd_pkr()

    if not usd_pkr:
        logger.warning("No USD/PKR rate available. Cannot compute PKR prices.")
        return 0

    # Find commodities with PKR conversion
    convertible = [c for c in COMMODITY_UNIVERSE.values() if c.pk_conversion and c.pk_unit]

    total = 0
    for commodity in convertible:
        # Get EOD data that doesn't have a PKR entry yet
        rows = con.execute(
            """
            SELECT e.symbol, e.date, e.close
            FROM commodity_eod e
            LEFT JOIN commodity_pkr p ON e.symbol=p.symbol AND e.date=p.date
            WHERE e.symbol=? AND e.close IS NOT NULL AND p.symbol IS NULL
            ORDER BY e.date
            """,
            (commodity.symbol,),
        ).fetchall()

        if not rows:
            continue

        pkr_rows = []
        for r in rows:
            pkr_price = convert_to_pkr(commodity.pk_conversion, r["close"], usd_pkr)
            if pkr_price is not None:
                pkr_rows.append({
                    "symbol": commodity.symbol,
                    "date": r["date"],
                    "pkr_price": round(pkr_price, 2),
                    "pk_unit": commodity.pk_unit,
                    "usd_price": r["close"],
                    "usd_pkr": usd_pkr,
                    "source": "computed",
                })

        if pkr_rows:
            n = upsert_commodity_pkr(con, pkr_rows)
            total += n
            logger.info("PKR computed for %s: %d rows", commodity.symbol, n)

    return total


# ─────────────────────────────────────────────────────────────────────────────
# Tier 4a: khistocks.com sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_khistocks(
    db_path: Path | str | None = None,
    feeds: list[str] | None = None,
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync Pakistan local market data from khistocks.com (Business Recorder).

    Feeds: pmex, sarafa, intl_bullion, mandi, lme.

    Returns CommoditySyncSummary.
    """
    from .fetcher_khistocks import (
        fetch_all_pmex,
        fetch_all_bullion,
        fetch_all_intl_bullion,
        fetch_all_mandi,
        fetch_all_lme,
    )
    from .models import upsert_khistocks_prices

    run_id = str(uuid.uuid4())
    con = connect(db_path)
    init_schema(con)
    init_commodity_schema(con)
    record_commodity_sync_start(con, run_id, "khistocks_daily", "khistocks")

    summary = CommoditySyncSummary(run_id=run_id, source="khistocks")

    active_feeds = feeds or ["pmex", "sarafa", "intl_bullion", "mandi", "lme"]

    feed_fetchers = {
        "pmex": ("khistocks_pmex", fetch_all_pmex),
        "sarafa": ("khistocks_sarafa", fetch_all_bullion),
        "intl_bullion": ("khistocks_intl_bullion", fetch_all_intl_bullion),
        "mandi": ("khistocks_mandi", fetch_all_mandi),
        "lme": ("khistocks_lme", fetch_all_lme),
    }

    for feed_name in active_feeds:
        if feed_name not in feed_fetchers:
            continue

        feed_label, fetcher_fn = feed_fetchers[feed_name]

        if progress_callback:
            progress_callback(active_feeds.index(feed_name) + 1, len(active_feeds), feed_name)

        try:
            data = fetcher_fn()
            for symbol, rows in data.items():
                # Normalize rows to khistocks_prices schema
                db_rows = []
                for r in rows:
                    db_rows.append({
                        "symbol": r.get("symbol", symbol),
                        "date": r.get("date", ""),
                        "feed": feed_label,
                        "name": r.get("name", r.get("instrument", "")),
                        "quotation": r.get("quotation", ""),
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "rate": r.get("rate"),
                        "cash_buyer": r.get("cash_buyer"),
                        "cash_seller": r.get("cash_seller"),
                        "three_month_buyer": r.get("three_month_buyer"),
                        "three_month_seller": r.get("three_month_seller"),
                        "net_change": r.get("net_change"),
                        "change_pct": r.get("change_pct", ""),
                        "source": r.get("source", "khistocks"),
                    })

                if db_rows:
                    n = upsert_khistocks_prices(con, db_rows)
                    summary.rows_upserted += n
                    summary.symbols_ok += 1
                    summary.symbols_total += 1

            logger.info("khistocks %s: %d symbols synced", feed_name, len(data))

        except Exception as e:
            summary.symbols_failed += 1
            summary.errors.append((feed_name, str(e)))
            logger.warning("khistocks %s sync failed: %s", feed_name, e)

    record_commodity_sync_end(
        con, run_id,
        symbols_total=summary.symbols_total,
        symbols_ok=summary.symbols_ok,
        symbols_failed=summary.symbols_failed,
        rows_upserted=summary.rows_upserted,
    )

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Tier 4b: PMEX Portal direct API sync
# ─────────────────────────────────────────────────────────────────────────────

def sync_pmex(
    db_path: Path | str | None = None,
    category: str | None = None,
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync PMEX market watch data from the PMEX dportal JSON API.

    Fetches all 134 instruments in a single API call. Optionally filter
    by category: Indices, Metals, Oil, Cots, Energy, Agri, Phy_Agri,
    Phy_Gold, Financials.

    Returns CommoditySyncSummary.
    """
    from .fetcher_pmex import fetch_pmex_snapshot
    from .models import upsert_pmex_market_watch

    run_id = str(uuid.uuid4())
    con = connect(db_path)
    init_schema(con)
    init_commodity_schema(con)
    record_commodity_sync_start(con, run_id, "pmex_portal", "pmex_portal")

    summary = CommoditySyncSummary(run_id=run_id, source="pmex_portal")

    try:
        all_records = fetch_pmex_snapshot()

        if category:
            all_records = [r for r in all_records if r["category"].lower() == category.lower()]

        if all_records:
            by_category: dict[str, list[dict]] = {}
            for rec in all_records:
                by_category.setdefault(rec["category"], []).append(rec)

            summary.symbols_total = len(all_records)

            for cat_name, cat_records in by_category.items():
                try:
                    n = upsert_pmex_market_watch(con, cat_records)
                    summary.rows_upserted += n
                    summary.symbols_ok += len(cat_records)
                    logger.info("PMEX %s: %d contracts synced", cat_name, len(cat_records))
                except Exception as e:
                    summary.symbols_failed += len(cat_records)
                    summary.errors.append((cat_name, str(e)))
                    logger.warning("PMEX %s sync failed: %s", cat_name, e)

            if progress_callback:
                progress_callback(summary.symbols_total, summary.symbols_total, "pmex_portal")
        else:
            logger.info("No data from PMEX portal")

    except Exception as e:
        summary.symbols_failed += 1
        summary.errors.append(("pmex_portal", str(e)))
        logger.warning("PMEX portal sync failed: %s", e)

    record_commodity_sync_end(
        con, run_id,
        symbols_total=summary.symbols_total,
        symbols_ok=summary.symbols_ok,
        symbols_failed=summary.symbols_failed,
        rows_upserted=summary.rows_upserted,
        error_summary="; ".join(f"{s}: {e}" for s, e in summary.errors[:10]) if summary.errors else None,
    )

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# PMEX OHLC sync (commod.db — separate database)
# ─────────────────────────────────────────────────────────────────────────────

def sync_pmex_ohlc(
    days: int = 3,
    save_json: bool = False,
    active_only: bool = False,
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync recent PMEX OHLC data into commod.db.

    Fetches last N days of OHLCV data from the PMEX GetOHLC API
    and upserts into the separate commod.db database.
    """
    from .fetcher_pmex_ohlc import sync_recent
    from .commod_db import (
        get_commod_connection, init_commod_schema,
        upsert_pmex_ohlc, save_ohlc_json,
    )

    summary = CommoditySyncSummary(run_id=str(uuid.uuid4()), source="pmex_ohlc")

    try:
        df = sync_recent(days=days)

        if df.empty:
            logger.info("No PMEX OHLC data returned")
            return summary

        if active_only:
            df = df[df["traded_volume"] > 0]

        df["trading_date"] = df["trading_date"].dt.strftime("%Y-%m-%d")
        rows = df.to_dict("records")

        summary.symbols_total = df["symbol"].nunique()

        if save_json:
            from datetime import date, timedelta
            save_ohlc_json(rows, date.today() - timedelta(days=days), date.today())

        con = get_commod_connection()
        init_commod_schema(con)
        n = upsert_pmex_ohlc(con, rows)
        summary.rows_upserted = n
        summary.symbols_ok = summary.symbols_total
        con.close()

        logger.info("PMEX OHLC: synced %d rows, %d symbols", n, summary.symbols_total)

        if progress_callback:
            progress_callback(1, 1, "pmex_ohlc")

    except Exception as e:
        summary.symbols_failed += 1
        summary.errors.append(("pmex_ohlc", str(e)))
        logger.warning("PMEX OHLC sync failed: %s", e)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# PMEX Margins sync (commod.db — separate database)
# ─────────────────────────────────────────────────────────────────────────────

def sync_pmex_margins(
    save_xlsx: bool = False,
    progress_callback=None,
) -> CommoditySyncSummary:
    """Sync today's PMEX margins data into commod.db.

    Downloads the latest margins Excel file and upserts
    into the separate commod.db database.
    """
    from .fetcher_pmex_margins import fetch_margins_file, parse_margins_excel
    from .commod_db import (
        get_commod_connection, init_commod_schema,
        upsert_pmex_margins, save_margins_excel,
    )

    summary = CommoditySyncSummary(run_id=str(uuid.uuid4()), source="pmex_margins")

    try:
        raw_bytes, actual_date = fetch_margins_file()

        if raw_bytes is None:
            logger.info("No PMEX margins file found")
            return summary

        if save_xlsx:
            save_margins_excel(raw_bytes, actual_date)

        df = parse_margins_excel(raw_bytes, actual_date)

        if df.empty:
            logger.info("PMEX margins parsing returned no data")
            return summary

        rows = df.to_dict("records")
        summary.symbols_total = len(rows)

        con = get_commod_connection()
        init_commod_schema(con)
        n = upsert_pmex_margins(con, rows)
        summary.rows_upserted = n
        summary.symbols_ok = n
        con.close()

        logger.info("PMEX Margins: synced %d contracts for %s", n, actual_date)

        if progress_callback:
            progress_callback(1, 1, "pmex_margins")

    except Exception as e:
        summary.symbols_failed += 1
        summary.errors.append(("pmex_margins", str(e)))
        logger.warning("PMEX Margins sync failed: %s", e)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Full sync orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def sync_all_commodities(
    db_path: Path | str | None = None,
    incremental: bool = True,
    sources: list[str] | None = None,
    category: str | None = None,
    symbols: list[str] | None = None,
    fred_api_key: str | None = None,
    progress_callback=None,
) -> dict[str, CommoditySyncSummary]:
    """Run the full commodity sync pipeline.

    Priority order:
    1. yfinance (daily OHLCV, free, fast)
    2. FRED (monthly, rate-limited)
    3. World Bank Pink Sheet (monthly, direct download)
    4. PKR conversion for all USD-denominated prices

    Args:
        db_path: Database path.
        incremental: Only fetch new data.
        sources: Limit to specific sources ["yfinance", "fred", "worldbank"].
        category: Filter by commodity category.
        symbols: Specific symbols.
        fred_api_key: FRED API key (or env var).
        progress_callback: Optional callback.

    Returns:
        Dict of source -> CommoditySyncSummary.
    """
    # Ensure schema & universe exist
    seed_commodity_universe(db_path)

    all_sources = sources or ["yfinance", "fred", "worldbank", "khistocks", "pmex_portal", "pmex_ohlc", "pmex_margins"]
    results = {}

    # 1. yfinance (daily)
    if "yfinance" in all_sources:
        logger.info("=== Sync: yfinance daily ===")
        results["yfinance"] = sync_yfinance(
            db_path=db_path,
            symbols=symbols,
            category=category,
            incremental=incremental,
            progress_callback=progress_callback,
        )

    # 2. FRED (monthly)
    if "fred" in all_sources:
        logger.info("=== Sync: FRED monthly ===")
        results["fred"] = sync_fred(
            db_path=db_path,
            api_key=fred_api_key,
            symbols=symbols,
            progress_callback=progress_callback,
        )

    # 3. World Bank Pink Sheet (monthly)
    if "worldbank" in all_sources:
        logger.info("=== Sync: World Bank Pink Sheet ===")
        results["worldbank"] = sync_worldbank(
            db_path=db_path,
            progress_callback=progress_callback,
        )

    # 4. khistocks.com (Pakistan local market data)
    if "khistocks" in all_sources:
        logger.info("=== Sync: khistocks.com ===")
        results["khistocks"] = sync_khistocks(
            db_path=db_path,
            progress_callback=progress_callback,
        )

    # 5. PMEX Portal (direct API, all 134 instruments)
    if "pmex_portal" in all_sources:
        logger.info("=== Sync: PMEX Portal ===")
        results["pmex_portal"] = sync_pmex(
            db_path=db_path,
            progress_callback=progress_callback,
        )

    # 6. PMEX OHLC (commod.db)
    if "pmex_ohlc" in all_sources:
        logger.info("=== Sync: PMEX OHLC (commod.db) ===")
        results["pmex_ohlc"] = sync_pmex_ohlc(
            progress_callback=progress_callback,
        )

    # 7. PMEX Margins (commod.db)
    if "pmex_margins" in all_sources:
        logger.info("=== Sync: PMEX Margins (commod.db) ===")
        results["pmex_margins"] = sync_pmex_margins(
            progress_callback=progress_callback,
        )

    # 8. PKR conversion pass
    logger.info("=== Computing PKR prices ===")
    pkr_count = compute_pkr_prices(db_path=db_path)
    logger.info("PKR conversion: %d rows", pkr_count)

    return results
