"""NCCPL historical backfill via KhiStocks JSON API.

KhiStocks (khistocks.com) mirrors NCCPL FIPI/LIPI data through a
server-side DataTables API that supports date ranges back to 2020+.

Endpoints:
  POST khistocks.com/ajax/fipi_sectors  — FIPI by sector + client type
  POST khistocks.com/ajax/lipi_sectors  — LIPI by sector + investor type

Params: draw, start, length, from (YYYY-MM-DD), to (YYYY-MM-DD)

Usage:
    conda activate psx

    # Dry run — discover how many records exist, don't store
    python -m pakfindata.sources.nccpl_backfill --backfill --from 2025-01-01 --to 2026-04-01 --dry-run

    # Actual backfill
    python -m pakfindata.sources.nccpl_backfill --backfill --from 2025-01-01 --to 2026-04-01

    # Fetch today only (cron use)
    python -m pakfindata.sources.nccpl_backfill --daily

    # Probe NCCPL Excel URLs (unlikely to find any)
    python -m pakfindata.sources.nccpl_backfill --probe-excel --year 2025 --month 6
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd

from pakfindata.db.connection import connect, init_schema
from pakfindata.db.repositories.nccpl_flows import (
    date_already_fetched,
    upsert_fipi,
    upsert_lipi,
    upsert_fipi_sector,
)
from pakfindata.sources.nccpl_flows import (
    HEADERS,
    _get_session,
    _match_client_type,
    _parse_value,
    _FIPI_CLIENT_MAP,
    _LIPI_CLIENT_MAP,
    compute_derived_signals,
    fetch_with_fallback,
)

log = logging.getLogger("pakfindata.nccpl_backfill")

KHISTOCKS_FIPI_API = "https://www.khistocks.com/ajax/fipi_sectors"
KHISTOCKS_LIPI_API = "https://www.khistocks.com/ajax/lipi_sectors"

_AJAX_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.khistocks.com/clearance/fipi-sector-wise.html",
}


# ═══════════════════════════════════════════════════════
# KHISTOCKS JSON API — HISTORICAL BACKFILL
# ═══════════════════════════════════════════════════════


def _fetch_khistocks_json(
    session, endpoint: str, from_date: str, to_date: str,
) -> list[dict]:
    """Fetch all records from a KhiStocks DataTables API endpoint.

    Paginates automatically if recordsTotal > length.
    """
    all_rows = []
    start = 0
    page_size = 5000

    while True:
        resp = session.post(
            endpoint,
            data={
                "draw": "1",
                "start": str(start),
                "length": str(page_size),
                "from": from_date,
                "to": to_date,
            },
            headers=_AJAX_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data", [])
        total = data.get("recordsTotal", 0)
        all_rows.extend(rows)

        if len(all_rows) >= total or not rows:
            break
        start += page_size

    return all_rows


def discover_khistocks(
    from_date: str, to_date: str,
) -> dict:
    """Discover how many FIPI + LIPI records exist for a date range.

    Returns summary dict without fetching all data.
    """
    session = _get_session()

    # Quick probe with length=1 to get totals
    fipi_resp = session.post(
        KHISTOCKS_FIPI_API,
        data={"draw": "1", "start": "0", "length": "1", "from": from_date, "to": to_date},
        headers=_AJAX_HEADERS,
        timeout=15,
    ).json()

    lipi_resp = session.post(
        KHISTOCKS_LIPI_API,
        data={"draw": "1", "start": "0", "length": "1", "from": from_date, "to": to_date},
        headers=_AJAX_HEADERS,
        timeout=15,
    ).json()

    # Get unique date count from a larger sample
    fipi_sample = session.post(
        KHISTOCKS_FIPI_API,
        data={"draw": "1", "start": "0", "length": "5000", "from": from_date, "to": to_date},
        headers=_AJAX_HEADERS,
        timeout=30,
    ).json()

    fipi_dates = sorted(set(r["date"] for r in fipi_sample.get("data", [])))

    return {
        "fipi_records": fipi_resp.get("recordsTotal", 0),
        "lipi_records": lipi_resp.get("recordsTotal", 0),
        "fipi_dates": len(fipi_dates),
        "date_range": f"{fipi_dates[0]} to {fipi_dates[-1]}" if fipi_dates else "N/A",
        "dates": fipi_dates,
    }


def _aggregate_fipi_by_date(rows: list[dict]) -> dict[str, dict]:
    """Aggregate raw KhiStocks FIPI rows into per-date FIPI dicts.

    Raw rows have: date, ctype_name, smallname, mtype_name,
                   buy_value, sell_value, net_value (as comma strings).
    """
    by_date: dict[str, dict] = {}

    for row in rows:
        date_str = row["date"]
        if date_str not in by_date:
            by_date[date_str] = {
                "date": date_str,
                "fpi_buy": 0, "fpi_sell": 0, "fpi_net": 0,
            }

        bv = _parse_value(row.get("buy_value", "0")) or 0
        sv = abs(_parse_value(row.get("sell_value", "0")) or 0)
        nv = _parse_value(row.get("net_value", "0")) or (bv - sv)

        rec = by_date[date_str]
        rec["fpi_buy"] += bv
        rec["fpi_sell"] += sv
        rec["fpi_net"] += nv

        # Breakdown by client type
        ct = row.get("ctype_name", "")
        prefix = _match_client_type(ct, _FIPI_CLIENT_MAP)
        if prefix:
            key = f"{prefix}_net"
            rec[key] = rec.get(key, 0) + nv

    return by_date


def _aggregate_fipi_sectors_by_date(rows: list[dict]) -> dict[str, list[dict]]:
    """Aggregate raw FIPI rows into per-date sector lists."""
    by_date: dict[str, dict[str, dict]] = {}

    for row in rows:
        date_str = row["date"]
        sector = row.get("smallname", "Other")

        if date_str not in by_date:
            by_date[date_str] = {}
        if sector not in by_date[date_str]:
            by_date[date_str][sector] = {"buy": 0, "sell": 0, "net": 0}

        bv = _parse_value(row.get("buy_value", "0")) or 0
        sv = abs(_parse_value(row.get("sell_value", "0")) or 0)
        nv = _parse_value(row.get("net_value", "0")) or (bv - sv)

        by_date[date_str][sector]["buy"] += bv
        by_date[date_str][sector]["sell"] += sv
        by_date[date_str][sector]["net"] += nv

    result = {}
    for date_str, sectors in by_date.items():
        result[date_str] = [
            {"date": date_str, "sector": sec, "fpi_buy": v["buy"], "fpi_sell": v["sell"], "fpi_net": v["net"]}
            for sec, v in sectors.items()
        ]
    return result


def _aggregate_lipi_by_date(rows: list[dict]) -> dict[str, dict]:
    """Aggregate raw KhiStocks LIPI rows into per-date LIPI dicts."""
    by_date: dict[str, dict[str, dict]] = {}

    for row in rows:
        date_str = row["date"]
        ct = row.get("ctype_name", "")
        prefix = _match_client_type(ct, _LIPI_CLIENT_MAP)
        if prefix is None:
            continue

        if date_str not in by_date:
            by_date[date_str] = {}

        bv = _parse_value(row.get("buy_value", "0")) or 0
        sv = abs(_parse_value(row.get("sell_value", "0")) or 0)
        nv = _parse_value(row.get("net_value", "0")) or (bv - sv)

        if prefix not in by_date[date_str]:
            by_date[date_str][prefix] = {"buy": 0, "sell": 0, "net": 0}

        by_date[date_str][prefix]["buy"] += bv
        by_date[date_str][prefix]["sell"] += sv
        by_date[date_str][prefix]["net"] += nv

    result = {}
    for date_str, types in by_date.items():
        rec = {"date": date_str}
        for prefix, vals in types.items():
            rec[f"{prefix}_buy"] = vals["buy"]
            rec[f"{prefix}_sell"] = vals["sell"]
            rec[f"{prefix}_net"] = vals["net"]
        result[date_str] = rec

    return result


def backfill_from_khistocks(
    from_date: str,
    to_date: str,
    dry_run: bool = False,
) -> dict:
    """Backfill NCCPL flows from KhiStocks JSON API.

    Fetches FIPI + LIPI for the full range in bulk (fast), then
    stores per-date to SQLite, skipping dates already in DB.
    """
    session = _get_session()
    con = connect()
    init_schema(con)

    # ── Fetch all FIPI rows for range ──
    log.info("Fetching FIPI data from KhiStocks: %s to %s", from_date, to_date)
    fipi_rows = _fetch_khistocks_json(session, KHISTOCKS_FIPI_API, from_date, to_date)
    log.info("FIPI: %d raw rows fetched", len(fipi_rows))

    fipi_by_date = _aggregate_fipi_by_date(fipi_rows)
    sectors_by_date = _aggregate_fipi_sectors_by_date(fipi_rows)

    # ── Fetch all LIPI rows for range ──
    log.info("Fetching LIPI data from KhiStocks: %s to %s", from_date, to_date)
    lipi_rows = _fetch_khistocks_json(session, KHISTOCKS_LIPI_API, from_date, to_date)
    log.info("LIPI: %d raw rows fetched", len(lipi_rows))

    lipi_by_date = _aggregate_lipi_by_date(lipi_rows)

    # ── Merge dates ──
    all_dates = sorted(set(fipi_by_date.keys()) | set(lipi_by_date.keys()))
    new_dates = [d for d in all_dates if not date_already_fetched(con, d)]
    skip_dates = len(all_dates) - len(new_dates)

    summary = {
        "fipi_raw_rows": len(fipi_rows),
        "lipi_raw_rows": len(lipi_rows),
        "total_dates": len(all_dates),
        "new_dates": len(new_dates),
        "skipped_dates": skip_dates,
        "date_range": f"{all_dates[0]} to {all_dates[-1]}" if all_dates else "N/A",
        "stored": 0,
    }

    if dry_run:
        return summary

    # ── Store to SQLite ──
    stored = 0
    for date_str in new_dates:
        fipi = fipi_by_date.get(date_str)
        lipi = lipi_by_date.get(date_str)
        sectors = sectors_by_date.get(date_str)

        if fipi:
            upsert_fipi(con, fipi)
        if lipi:
            upsert_lipi(con, lipi)
        if sectors:
            upsert_fipi_sector(con, sectors)

        stored += 1

    summary["stored"] = stored
    log.info("Stored %d new dates to SQLite", stored)

    # ── Recompute derived signals ──
    if stored > 0:
        compute_derived_signals(con)

    return summary


# ═══════════════════════════════════════════════════════
# NCCPL EXCEL PROBING (kept for completeness)
# ═══════════════════════════════════════════════════════


def _excel_candidate_urls(year: int, month: int, day: int) -> list[str]:
    """Generate candidate NCCPL Excel URLs for a given date."""
    d1 = f"{year:04d}_{month:02d}_{day:02d}"
    d2 = f"{day:02d}{month:02d}{year:04d}"
    d3 = f"{year:04d}-{month:02d}-{day:02d}"
    d4 = f"{day:02d}-{month:02d}-{year:04d}"
    d5 = f"{year:04d}{month:02d}{day:02d}"

    bases = [
        "https://www.nccpl.com.pk/uploads/files/fipi",
        "https://www.nccpl.com.pk/uploads/fipi",
        "https://www.nccpl.com.pk/media/fipi",
        "https://www.nccpl.com.pk/uploads/files",
    ]
    prefixes = ["FIPI_LIPI", "FIPI", "fipi_lipi", "fipi"]
    date_fmts = [d1, d2, d3, d4, d5]

    urls = []
    for base in bases:
        for prefix in prefixes:
            for dfmt in date_fmts:
                urls.append(f"{base}/{prefix}_{dfmt}.xlsx")
                urls.append(f"{base}/{prefix}_{dfmt}.xls")
    return urls


def probe_nccpl_excel_urls(year: int, month: int) -> list[dict]:
    """Probe NCCPL Excel URLs for a given month. Returns list of results."""
    import calendar

    session = _get_session()
    results = []
    _, days_in_month = calendar.monthrange(year, month)

    for day in range(1, days_in_month + 1):
        d = datetime(year, month, day)
        if d.weekday() >= 5:
            continue

        date_str = d.strftime("%Y-%m-%d")
        urls = _excel_candidate_urls(year, month, day)
        found = False

        for url in urls[:8]:
            try:
                resp = session.get(url, headers=HEADERS, timeout=10)
                if resp.status_code == 200 and len(resp.content) > 500:
                    results.append({"date": date_str, "url": url, "status": 200, "size": len(resp.content)})
                    log.info("FOUND: %s (%d bytes)", url, len(resp.content))
                    found = True
                    break
            except Exception:
                continue

        if not found:
            results.append({"date": date_str, "url": None, "status": 404, "size": 0})

        time.sleep(0.5)

    return results


# ═══════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="NCCPL historical backfill via KhiStocks API"
    )

    parser.add_argument(
        "--backfill", action="store_true",
        help="Backfill FIPI + LIPI from KhiStocks JSON API",
    )
    parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Discover records without storing (print counts only)",
    )

    parser.add_argument(
        "--daily", action="store_true",
        help="Fetch today's data via BRecorder (for cron use)",
    )

    parser.add_argument(
        "--probe-excel", action="store_true",
        help="Probe NCCPL Excel URLs for a given month",
    )
    parser.add_argument("--year", type=int, help="Year to probe (e.g. 2025)")
    parser.add_argument("--month", type=int, help="Month to probe (1-12)")

    args = parser.parse_args()

    # Ensure log directory
    log_dir = Path.home() / "pakfindata" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / "nccpl_backfill.log"),
        ],
    )

    if args.backfill:
        if not args.from_date:
            parser.error("--backfill requires --from date")
        to_date = args.to_date or datetime.now().strftime("%Y-%m-%d")

        if args.dry_run:
            print(f"Discovering KhiStocks data: {args.from_date} to {to_date}...")
            info = discover_khistocks(args.from_date, to_date)
            print(f"\n  FIPI records:  {info['fipi_records']:,}")
            print(f"  LIPI records:  {info['lipi_records']:,}")
            print(f"  Trading days:  {info['fipi_dates']}")
            print(f"  Date range:    {info['date_range']}")
        else:
            print(f"Backfilling from KhiStocks: {args.from_date} to {to_date}...")
            result = backfill_from_khistocks(args.from_date, to_date)
            print(f"\n  FIPI raw rows: {result['fipi_raw_rows']:,}")
            print(f"  LIPI raw rows: {result['lipi_raw_rows']:,}")
            print(f"  Date range:    {result['date_range']}")
            print(f"  Total dates:   {result['total_dates']}")
            print(f"  New (stored):  {result['stored']}")
            print(f"  Skipped:       {result['skipped_dates']}")

    elif args.daily:
        con = connect()
        init_schema(con)
        today = datetime.now().strftime("%Y-%m-%d")

        if date_already_fetched(con, today):
            print(f"{today} already fetched")
            return

        result = fetch_with_fallback(today, con)
        compute_derived_signals(con)
        print(f"{today}: source={result['source']}, tier={result['tier']}")

    elif args.probe_excel:
        if not args.year or not args.month:
            parser.error("--probe-excel requires --year and --month")

        print(f"Probing NCCPL Excel URLs for {args.year}-{args.month:02d}...")
        results = probe_nccpl_excel_urls(args.year, args.month)
        found = [r for r in results if r["url"]]
        missing = [r for r in results if not r["url"]]
        print(f"\nResults: {len(found)} found, {len(missing)} missing")
        if found:
            for r in found:
                print(f"  {r['date']}: {r['url']} ({r['size']:,} bytes)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
