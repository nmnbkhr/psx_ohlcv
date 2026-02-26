"""
PSX Debt Market sync module.

This module syncs debt securities data from PSX DPS to the database:
- Instruments: All debt securities (T-Bills, PIBs, GIS, TFCs, Sukuk)
- Quotes: Daily price/yield data via timeseries API

Uses the existing fi_instruments and fi_quotes tables.

All data is READ-ONLY and for informational purposes only.
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

from .db import (
    connect,
    get_fi_data_summary,
    get_fi_instrument,
    get_fi_instruments,
    get_fi_latest_quote,
    get_fi_sync_runs,
    init_schema,
    record_fi_sync_run,
    update_fi_sync_run,
    upsert_fi_instrument,
    upsert_fi_quote,
)
from .sources.psx_debt import (
    DEBT_CATEGORIES,
    KNOWN_DEBT_SYMBOLS,
    DebtSecurity,
    fetch_all_debt_securities,
    fetch_debt_ohlcv,
    fetch_debt_security_detail,
    fetch_debt_symbols,
    get_securities_flat_list,
    get_securities_summary,
    parse_symbol_info,
)

logger = logging.getLogger(__name__)


@dataclass
class DebtSyncSummary:
    """Summary of debt sync operation."""

    total: int = 0
    ok: int = 0
    failed: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


def _security_to_fi_instrument(security: DebtSecurity) -> dict:
    """Convert DebtSecurity to fi_instruments format."""

    # Map security type to category
    category_map = {
        "T-Bill": "MTB",
        "PIB": "PIB",
        "GIS": "GOP_SUKUK",
        "FRR": "GOP_SUKUK",
        "VRR": "GOP_SUKUK",
        "GVR": "GOP_SUKUK",
        "Floating": "PIB",
        "TFC": "CORP_BOND",
        "Term Finance": "CORP_BOND",
        "Sukuk": "CORP_SUKUK",
    }

    category = "MTB"  # default
    if security.security_type:
        for key, val in category_map.items():
            if key in security.security_type:
                category = val
                break

    # Determine coupon frequency from security type
    coupon_freq = 2  # Semi-annual default
    if security.security_type:
        st = security.security_type.lower()
        if "t-bill" in st or "frz" in st or "gis" in st:
            coupon_freq = 0  # Zero coupon / discount
        elif "tfc" in st or "corporate" in st:
            coupon_freq = 4  # Quarterly for corporates

    # Determine issuer based on is_government flag
    issuer = "GOVT_OF_PAKISTAN" if security.is_government else "CORPORATE"

    # Determine active status - check outstanding_days if available
    is_active = 1
    if security.outstanding_days is not None and security.outstanding_days <= 0:
        is_active = 0
    elif security.status == "MATURED":
        is_active = 0

    return {
        "instrument_id": f"PSX_DEBT:{security.symbol}",
        "isin": security.symbol,  # Use symbol as ISIN for PSX debt
        "issuer": issuer,
        "name": security.name or security.symbol,
        "category": category,
        "currency": "PKR",
        "issue_date": security.issue_date,
        "maturity_date": security.maturity_date,
        "coupon_rate": security.coupon_rate,
        "coupon_frequency": coupon_freq,
        "day_count": "ACT/ACT",
        "face_value": security.face_value or 5000,
        "shariah_compliant": 1 if security.is_islamic else 0,
        "is_active": is_active,
        "source": "PSX_DPS",
    }


def _ohlcv_to_fi_quote(symbol: str, ohlcv: dict) -> dict:
    """Convert OHLCV record to fi_quotes format."""
    return {
        "instrument_id": f"PSX_DEBT:{symbol}",
        "quote_date": ohlcv["date"],
        "clean_price": ohlcv["price"],
        "ytm": None,  # Would need to calculate
        "bid": None,
        "ask": None,
        "volume": ohlcv["volume"],
        "source": "PSX_DPS",
    }


def sync_debt_instruments(
    db_path: Path | str | None = None,
    fetch_details: bool = False,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> DebtSyncSummary:
    """
    Sync debt instruments from PSX DPS /debt-market page.

    Parses the HTML table which has all master data (name, dates, coupon,
    face value, etc.) in a single request — no need for 271 detail fetches.

    Args:
        db_path: Database path
        fetch_details: If True, also fetch individual detail pages (slow)
        progress_callback: Optional callback(current, total, symbol)

    Returns:
        DebtSyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    run_id = str(uuid.uuid4())[:8]
    record_fi_sync_run(con, run_id, "SYNC_PSX_DEBT_INSTRUMENTS", [])

    summary = DebtSyncSummary()

    try:
        # Fetch and parse the full debt-market HTML table (single request)
        securities_by_cat = fetch_all_debt_securities()
        all_securities = get_securities_flat_list(securities_by_cat)

        if not all_securities:
            logger.warning("No securities from PSX debt-market page")
            update_fi_sync_run(con, run_id, "failed", 0, "No data from PSX")
            con.close()
            return summary

        summary.total = len(all_securities)
        logger.info(f"Parsed {len(all_securities)} debt securities from PSX")

        for i, security in enumerate(all_securities):
            if progress_callback:
                progress_callback(i + 1, len(all_securities), security.symbol)

            try:
                fi_data = _security_to_fi_instrument(security)
                if upsert_fi_instrument(con, fi_data):
                    summary.ok += 1
                    summary.rows_upserted += 1
                else:
                    summary.failed += 1
                    summary.errors.append((security.symbol, "upsert failed"))
            except Exception as e:
                summary.failed += 1
                summary.errors.append((security.symbol, str(e)))

        status = "completed" if summary.failed == 0 else "partial"
        error_msg = None
        if summary.errors:
            error_msg = "; ".join([f"{k}: {v}" for k, v in summary.errors[:5]])
        update_fi_sync_run(con, run_id, status, summary.rows_upserted, error_msg)

    except Exception as e:
        update_fi_sync_run(con, run_id, "failed", summary.rows_upserted, str(e))
        summary.errors.append(("exception", str(e)))
        logger.exception(f"Error syncing debt instruments: {e}")

    con.close()
    return summary


def sync_debt_quotes(
    db_path: Path | str | None = None,
    symbols: list[str] | None = None,
    category: str | None = None,
    limit_per_symbol: int = 365,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> DebtSyncSummary:
    """
    Sync debt quotes (price data) from PSX DPS.

    Args:
        db_path: Database path
        symbols: Specific symbols to sync, or None for all
        category: Filter by category (MTB, PIB, GOP_SUKUK, etc.)
        limit_per_symbol: Max quotes per symbol
        progress_callback: Optional callback(current, total, symbol)

    Returns:
        DebtSyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_fi_sync_run(con, run_id, "SYNC_PSX_DEBT_QUOTES", [])

    summary = DebtSyncSummary()

    try:
        # Get symbols to sync
        if symbols:
            target_symbols = symbols
        else:
            # Get from database (already synced instruments)
            instruments = get_fi_instruments(con, category=category, active_only=True)
            target_symbols = [
                inst["isin"]
                for inst in instruments
                if inst.get("source") == "PSX_DPS"
            ]

            # If no instruments yet, use known list
            if not target_symbols:
                target_symbols = fetch_debt_symbols() or KNOWN_DEBT_SYMBOLS

        summary.total = len(target_symbols)
        logger.info(f"Syncing quotes for {len(target_symbols)} debt symbols")

        for i, symbol in enumerate(target_symbols):
            if progress_callback:
                progress_callback(i + 1, len(target_symbols), symbol)

            try:
                ohlcv_data = fetch_debt_ohlcv(symbol)

                if ohlcv_data:
                    # Limit to most recent
                    records = ohlcv_data[:limit_per_symbol]

                    for record in records:
                        quote = _ohlcv_to_fi_quote(symbol, record)
                        if upsert_fi_quote(con, quote):
                            summary.rows_upserted += 1

                    summary.ok += 1
                else:
                    summary.failed += 1
                    summary.errors.append((symbol, "no data"))

            except Exception as e:
                summary.failed += 1
                summary.errors.append((symbol, str(e)))

        status = "completed" if summary.failed == 0 else "partial"
        error_msg = None
        if summary.errors:
            error_msg = "; ".join([f"{k}: {v}" for k, v in summary.errors[:5]])
        update_fi_sync_run(con, run_id, status, summary.rows_upserted, error_msg)

    except Exception as e:
        update_fi_sync_run(con, run_id, "failed", summary.rows_upserted, str(e))
        summary.errors.append(("exception", str(e)))
        logger.exception(f"Error syncing debt quotes: {e}")

    con.close()
    return summary


def sync_all_psx_debt(
    db_path: Path | str | None = None,
    fetch_details: bool = True,
    include_quotes: bool = True,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> dict:
    """
    Sync all PSX debt market data.

    Args:
        db_path: Database path
        fetch_details: If True, fetch full details for each security
        include_quotes: If True, also sync price quotes
        progress_callback: Optional callback(stage, current, total)

    Returns:
        Combined summary dict
    """
    results = {}

    # 1. Sync instruments
    if progress_callback:
        progress_callback("instruments", 0, 1)

    inst_summary = sync_debt_instruments(
        db_path=db_path,
        fetch_details=fetch_details,
    )
    results["instruments"] = {
        "total": inst_summary.total,
        "ok": inst_summary.ok,
        "failed": inst_summary.failed,
        "rows_upserted": inst_summary.rows_upserted,
    }

    # 2. Sync quotes
    if include_quotes:
        if progress_callback:
            progress_callback("quotes", 0, 1)

        quote_summary = sync_debt_quotes(db_path=db_path)
        results["quotes"] = {
            "total": quote_summary.total,
            "ok": quote_summary.ok,
            "failed": quote_summary.failed,
            "rows_upserted": quote_summary.rows_upserted,
        }

    # 3. Bridge sukuk data to sukuk_master/sukuk_quotes tables
    if progress_callback:
        progress_callback("bridge_sukuk", 0, 1)

    bridge_result = bridge_to_sukuk_tables(db_path=db_path)
    results["sukuk_bridge"] = bridge_result

    return results


def get_psx_debt_status(db_path: Path | str | None = None) -> dict:
    """
    Get status of PSX debt data in database.

    Returns:
        Dict with counts and latest dates
    """
    con = connect(db_path)
    init_schema(con)

    status = {
        "total_instruments": 0,
        "government": 0,
        "corporate": 0,
        "total_quotes": 0,
        "latest_quote_date": None,
        "categories": {},
    }

    try:
        # Count PSX debt instruments
        cur = con.execute("""
            SELECT category, COUNT(*) as count
            FROM fi_instruments
            WHERE source = 'PSX_DPS'
            GROUP BY category
        """)
        for row in cur.fetchall():
            cat = row[0]
            count = row[1]
            status["categories"][cat] = count
            status["total_instruments"] += count
            if cat in ("MTB", "PIB", "GOP_SUKUK"):
                status["government"] += count
            else:
                status["corporate"] += count

        # Count quotes
        cur = con.execute("""
            SELECT COUNT(*), MAX(quote_date)
            FROM fi_quotes
            WHERE source = 'PSX_DPS'
        """)
        row = cur.fetchone()
        if row:
            status["total_quotes"] = row[0] or 0
            status["latest_quote_date"] = row[1]

    except Exception as e:
        logger.error(f"Error getting PSX debt status: {e}")

    con.close()
    return status


def get_debt_securities_list(
    db_path: Path | str | None = None,
    category: str | None = None,
    include_matured: bool = False,
) -> list[dict]:
    """
    Get list of debt securities with latest quotes.

    Args:
        db_path: Database path
        category: Filter by category
        include_matured: Include matured securities

    Returns:
        List of security dicts with latest price
    """
    con = connect(db_path)
    init_schema(con)

    securities = []

    try:
        # Get instruments
        instruments = get_fi_instruments(
            con,
            category=category,
            active_only=not include_matured,
        )

        for inst in instruments:
            if inst.get("source") != "PSX_DPS":
                continue

            # Get latest quote
            latest = get_fi_latest_quote(con, inst["instrument_id"])

            security = {
                "symbol": inst.get("isin"),
                "name": inst.get("name"),
                "category": inst.get("category"),
                "issuer": inst.get("issuer"),
                "maturity_date": inst.get("maturity_date"),
                "coupon_rate": inst.get("coupon_rate"),
                "face_value": inst.get("face_value"),
                "is_islamic": inst.get("shariah_compliant") == 1,
                "is_active": inst.get("is_active") == 1,
            }

            if latest:
                security["latest_price"] = latest.get("clean_price")
                security["latest_date"] = latest.get("quote_date")
                security["volume"] = latest.get("volume")

            # Calculate days to maturity
            if inst.get("maturity_date"):
                try:
                    maturity = datetime.strptime(inst["maturity_date"], "%Y-%m-%d")
                    security["days_to_maturity"] = (maturity - datetime.now()).days
                except ValueError:
                    pass

            securities.append(security)

    except Exception as e:
        logger.error(f"Error getting debt securities list: {e}")

    con.close()
    return securities


def bridge_to_sukuk_tables(db_path: Path | str | None = None) -> dict:
    """
    Copy shariah-compliant instruments and quotes from fi_* tables
    into sukuk_master and sukuk_quotes so the Sukuk UI page can display them.

    Returns:
        Summary dict with counts
    """
    con = connect(db_path)
    init_schema(con)

    result = {"instruments": 0, "quotes": 0}

    try:
        # 1. Copy shariah-compliant instruments from fi_instruments → sukuk_master
        cur = con.execute("""
            SELECT instrument_id, issuer, name, category, currency,
                   issue_date, maturity_date, coupon_rate, coupon_frequency,
                   face_value, shariah_compliant, is_active, source
            FROM fi_instruments
            WHERE shariah_compliant = 1 AND source = 'PSX_DPS'
        """)
        for row in cur.fetchall():
            r = dict(row)
            try:
                con.execute("""
                    INSERT INTO sukuk_master (
                        instrument_id, issuer, name, category, currency,
                        issue_date, maturity_date, coupon_rate, coupon_frequency,
                        face_value, shariah_compliant, is_active, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instrument_id) DO UPDATE SET
                        name = excluded.name,
                        category = excluded.category,
                        maturity_date = excluded.maturity_date,
                        coupon_rate = excluded.coupon_rate,
                        is_active = excluded.is_active,
                        source = excluded.source
                """, (
                    r["instrument_id"], r["issuer"], r["name"],
                    r["category"], r["currency"], r["issue_date"],
                    r["maturity_date"], r["coupon_rate"], r["coupon_frequency"],
                    r["face_value"], r["shariah_compliant"], r["is_active"],
                    r["source"],
                ))
                result["instruments"] += 1
            except Exception as e:
                logger.warning(f"Bridge sukuk_master {r['instrument_id']}: {e}")

        con.commit()

        # 2. Copy quotes from fi_quotes → sukuk_quotes for sukuk instruments
        cur = con.execute("""
            SELECT fq.instrument_id, fq.quote_date, fq.clean_price,
                   fq.ytm, fq.bid, fq.ask, fq.volume, fq.source
            FROM fi_quotes fq
            JOIN fi_instruments fi ON fq.instrument_id = fi.instrument_id
            WHERE fi.shariah_compliant = 1 AND fq.source = 'PSX_DPS'
        """)
        for row in cur.fetchall():
            r = dict(row)
            try:
                con.execute("""
                    INSERT INTO sukuk_quotes (
                        instrument_id, quote_date, clean_price,
                        yield_to_maturity, bid_yield, ask_yield,
                        volume, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instrument_id, quote_date) DO UPDATE SET
                        clean_price = excluded.clean_price,
                        yield_to_maturity = excluded.yield_to_maturity,
                        volume = excluded.volume,
                        source = excluded.source
                """, (
                    r["instrument_id"], r["quote_date"], r["clean_price"],
                    r["ytm"], r["bid"], r["ask"],
                    r["volume"], r["source"],
                ))
                result["quotes"] += 1
            except Exception as e:
                logger.warning(f"Bridge sukuk_quote {r['instrument_id']}: {e}")

        con.commit()

    except Exception as e:
        logger.exception(f"Error bridging to sukuk tables: {e}")

    con.close()
    return result
