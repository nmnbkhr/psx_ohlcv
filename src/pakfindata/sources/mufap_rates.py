"""MUFAP PKRV/PKISRV/PKFRV rate file downloader and parser.

Downloads monthly/daily rate files from MUFAP API and parses CSV/XLSX
into pkrv_daily, pkisrv_daily, pkfrv_daily tables.
"""

import csv
import io
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from ..config import DATA_ROOT
from ..db.repositories.yield_curves import (
    init_yield_curve_schema,
    upsert_pkfrv_point,
    upsert_pkisrv_point,
    upsert_pkrv_point,
)

MUFAP_BASE = "https://www.mufap.com.pk"
MUFAP_FILE_API = MUFAP_BASE + "/WebRegulations/GetSecpFileById"
PKRV_CATEGORY_ID = 46
RATES_DIR = DATA_ROOT / "rates"

# Tenor label → months mapping for PKRV
TENOR_MAP = {
    "1W": 0.25, "2W": 0.5,
    "1M": 1, "2M": 2, "3M": 3, "4M": 4, "6M": 6, "9M": 9,
    "1Y": 12, "2Y": 24, "3Y": 36, "4Y": 48, "5Y": 60,
    "6Y": 72, "7Y": 84, "8Y": 96, "9Y": 108, "10Y": 120,
    "15Y": 180, "20Y": 240, "25Y": 300, "30Y": 360,
}

# Also handle "1 - Month" style from PKISRV
PKISRV_TENOR_RE = re.compile(r"(\d+)\s*-\s*(Month|Year)", re.IGNORECASE)


def _tenor_to_months(label: str) -> int | None:
    """Convert tenor label to months (rounded)."""
    label = label.strip().upper().replace(" ", "")
    val = TENOR_MAP.get(label)
    if val is not None:
        return round(val) if val >= 1 else round(val * 4) / 4
    return None


def _extract_date_from_filename(fname: str) -> str | None:
    """Extract date from MUFAP filename like PKRV1702202624687.csv or PKRV_JAN_20202953.xlsx."""
    # Daily format: PKRV17022026xxxxx.csv → 17-02-2026
    m = re.search(r"(?:PKRV|PKISRV|PKFRV)(\d{2})(\d{2})(\d{4})\d+\.", fname, re.IGNORECASE)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        try:
            return datetime.strptime(f"{yyyy}-{mm}-{dd}", "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Monthly format: PKRV_JAN_20202953.xlsx
    m = re.search(
        r"(?:PKRV|PKISRV|PKFRV)_([A-Z]{3})_(\d{4})\d+\.", fname, re.IGNORECASE
    )
    if m:
        mon, yr = m.group(1), m.group(2)
        try:
            dt = datetime.strptime(f"{yr}-{mon}-01", "%Y-%b-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def fetch_mufap_file_list() -> list[dict]:
    """Fetch the file list from MUFAP API for PKRV/PKISRV/PKFRV."""
    resp = requests.post(
        MUFAP_FILE_API,
        json={"fk_HeaderSubMenuTabId": PKRV_CATEGORY_ID},
        headers={"Content-Type": "application/json;charset=utf-8"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def parse_pkrv_csv(content: str, date: str) -> list[dict]:
    """Parse PKRV CSV content into rate records."""
    records = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        tenor_label = row.get("Tenor", "").strip()
        mid_rate = row.get("Mid Rate", "").strip()
        change = row.get("Change", "").strip()

        if not tenor_label or not mid_rate:
            continue

        months = _tenor_to_months(tenor_label)
        if months is None:
            continue

        try:
            yield_pct = float(mid_rate)
        except (ValueError, TypeError):
            continue

        change_bps = None
        if change:
            try:
                change_bps = float(change) * 100  # convert to bps
            except (ValueError, TypeError):
                pass

        # Map sub-month tenors: 1W→0, 2W→0 would collide; store as-is in int
        # 1W → 0 (not stored, too short) — actually just skip sub-1M tenors
        # since pkrv_daily PK is (date, tenor_months) and INTEGER
        tenor_int = round(months)
        if tenor_int < 1:
            continue  # skip 1W, 2W — no integer month representation

        records.append({
            "date": date,
            "tenor_months": tenor_int,
            "yield_pct": yield_pct,
            "change_bps": change_bps,
            "source": "MUFAP",
        })
    return records


def parse_pkisrv_csv(content: str, date: str) -> list[dict]:
    """Parse PKISRV CSV — extract yield rates from the right-side columns.

    Lines look like: 'GOPIS-33-26-10-2027,GIS (VRR) - 26,100.24,0.01,,1 - Year,10.81%'
    The tenor+yield is on the RIGHT side after the GIS bond data.
    """
    records = []
    for line in content.splitlines():
        m = PKISRV_TENOR_RE.search(line)
        if not m:
            continue

        # Only look at text AFTER the tenor match for the yield value
        after_tenor = line[m.end():]
        parts = after_tenor.split(",")
        for part in parts:
            part = part.strip().rstrip("%")
            try:
                val = float(part)
                if 1.0 < val < 50:  # reasonable PKR yield range
                    records.append({
                        "date": date,
                        "tenor": f"{m.group(1)}{m.group(2)[0].upper()}",
                        "yield_pct": val,
                        "source": "MUFAP",
                    })
                    break
            except (ValueError, TypeError):
                continue
    return records


def parse_pkfrv_csv(content: str, date: str) -> list[dict]:
    """Parse PKFRV CSV — floating rate bond valuations."""
    records = []
    reader = csv.reader(io.StringIO(content))
    header = None
    for row in reader:
        if not row or len(row) < 3:
            continue
        # Header row has "Issue Date" in it
        if any("Issue Date" in str(c) for c in row):
            header = row
            continue
        if header is None:
            continue

        bond_code = row[0].strip() if row[0] else ""
        if not bond_code or not bond_code.startswith("PIB-FRB"):
            continue

        issue_date = row[1].strip() if len(row) > 1 else None
        maturity_date = row[2].strip() if len(row) > 2 else None
        coupon_freq = row[3].strip() if len(row) > 3 else None

        # FMA price is the last meaningful column (labelled "FMA" in header)
        fma_price = None
        fma_idx = None
        for i, h in enumerate(header):
            if str(h).strip().upper() == "FMA":
                fma_idx = i
                break
        if fma_idx and len(row) > fma_idx:
            try:
                fma_price = float(row[fma_idx])
            except (ValueError, TypeError):
                pass

        records.append({
            "date": date,
            "bond_code": bond_code,
            "issue_date": issue_date,
            "maturity_date": maturity_date,
            "coupon_frequency": coupon_freq,
            "fma_price": fma_price,
            "source": "MUFAP",
        })
    return records


def _xlsx_to_csv_text(filepath: Path) -> str:
    """Convert xlsx file to CSV text for parsing."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        lines = []
        for row in ws.iter_rows(values_only=True):
            lines.append(",".join(str(c or "") for c in row))
        wb.close()
        return "\n".join(lines)
    except Exception:
        return ""


def _read_rate_file(filepath: Path) -> str:
    """Read a rate file (CSV or XLSX) and return text content."""
    if filepath.suffix.lower() == ".xlsx":
        return _xlsx_to_csv_text(filepath)
    return filepath.read_text(errors="replace")


def sync_rates_from_files(
    con: sqlite3.Connection,
    rates_dir: Path | None = None,
    since_dates: dict[str, str | None] | None = None,
) -> dict:
    """Parse downloaded rate files and insert into DB.

    Args:
        con: SQLite connection.
        rates_dir: Directory with pkrv/pkisrv/pkfrv subdirs.
        since_dates: Optional dict {"pkrv": "2026-02-15", ...}.
            If provided, only files with dates AFTER the cutoff are parsed.
            Pass None to parse all files (full re-sync).
    """
    if rates_dir is None:
        rates_dir = RATES_DIR
    if since_dates is None:
        since_dates = {}

    init_yield_curve_schema(con)

    stats = {
        "pkrv_files": 0, "pkrv_records": 0,
        "pkisrv_files": 0, "pkisrv_records": 0,
        "pkfrv_files": 0, "pkfrv_records": 0,
        "skipped_parsed": 0,
        "failed": 0,
    }

    for curve_type, parser_fn, upsert_fn in [
        ("pkrv", parse_pkrv_csv, upsert_pkrv_point),
        ("pkisrv", parse_pkisrv_csv, upsert_pkisrv_point),
        ("pkfrv", parse_pkfrv_csv, upsert_pkfrv_point),
    ]:
        subdir = rates_dir / curve_type
        if not subdir.exists():
            continue
        cutoff = since_dates.get(curve_type)
        for f in sorted(subdir.iterdir()):
            if f.suffix.lower() not in (".csv", ".xlsx"):
                continue
            date = _extract_date_from_filename(f.name)
            if not date:
                stats["failed"] += 1
                continue
            # Skip files already in DB
            if cutoff and date <= cutoff:
                stats["skipped_parsed"] += 1
                continue
            try:
                content = _read_rate_file(f)
                if not content:
                    continue
                records = parser_fn(content, date)
                for rec in records:
                    upsert_fn(con, rec)
                if records:
                    stats[f"{curve_type}_files"] += 1
                    stats[f"{curve_type}_records"] += len(records)
            except Exception:
                stats["failed"] += 1

    return stats


def _get_latest_dates(con: sqlite3.Connection) -> dict[str, str | None]:
    """Get the latest rate_date from each yield curve table.

    Returns dict like {"pkrv": "2026-02-15", "pkisrv": "2026-02-10", "pkfrv": None}.
    """
    init_yield_curve_schema(con)
    result = {}
    for table in ("pkrv_daily", "pkisrv_daily", "pkfrv_daily"):
        key = table.replace("_daily", "")
        try:
            row = con.execute(f"SELECT MAX(date) AS d FROM {table}").fetchone()
            result[key] = row["d"] if row and row["d"] else None
        except Exception:
            result[key] = None
    return result


def download_and_sync(
    con: sqlite3.Connection,
    rates_dir: Path | None = None,
) -> dict:
    """Download new files from MUFAP (since last DB date) and sync to DB."""
    if rates_dir is None:
        rates_dir = RATES_DIR

    for sub in ("pkrv", "pkisrv", "pkfrv"):
        (rates_dir / sub).mkdir(parents=True, exist_ok=True)

    # Get latest dates already in DB — only download files newer than these
    latest_dates = _get_latest_dates(con)

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)"

    file_list = fetch_mufap_file_list()
    dl_stats = {"downloaded": 0, "skipped": 0, "skipped_old": 0, "failed": 0}

    import time

    for item in file_list:
        title = item["Title"].strip()
        path = item["FilePath"]
        url = MUFAP_BASE + path

        if title.upper().startswith("PKISRV"):
            curve = "pkisrv"
        elif title.upper().startswith("PKFRV"):
            curve = "pkfrv"
        else:
            curve = "pkrv"

        subdir = rates_dir / curve
        fname = path.rsplit("/", 1)[-1]
        dest = subdir / fname

        # Skip files whose date is already in DB
        file_date = _extract_date_from_filename(fname)
        cutoff = latest_dates.get(curve)
        if file_date and cutoff and file_date <= cutoff:
            dl_stats["skipped_old"] += 1
            continue

        if dest.exists() and dest.stat().st_size > 0:
            dl_stats["skipped"] += 1
            continue

        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 10:
                dest.write_bytes(resp.content)
                dl_stats["downloaded"] += 1
            else:
                dl_stats["failed"] += 1
        except Exception:
            dl_stats["failed"] += 1

        time.sleep(0.1)

    # Only parse files newer than the latest DB date
    sync_stats = sync_rates_from_files(con, rates_dir, since_dates=latest_dates)
    return {**dl_stats, **sync_stats}


# ═══════════════════════════════════════════════════════════════════════════════
# FAST BATCH DB SYNC (parallel parse + batch commit)
# ═══════════════════════════════════════════════════════════════════════════════


def backfill_to_db_fast(
    con: sqlite3.Connection,
    rates_dir: Path | None = None,
    since_dates: dict[str, str | None] | None = None,
    workers: int = 8,
) -> dict:
    """Fast bulk parse of all rate files into DB.

    Uses parallel file reads + batch INSERT (one commit per curve type)
    instead of per-row commits.  ~50x faster than sync_rates_from_files().

    Existing rows are upserted (ON CONFLICT ... DO UPDATE).
    """
    from concurrent.futures import ThreadPoolExecutor

    if rates_dir is None:
        rates_dir = RATES_DIR
    if since_dates is None:
        since_dates = {}

    init_yield_curve_schema(con)

    stats = {
        "pkrv_files": 0, "pkrv_records": 0,
        "pkisrv_files": 0, "pkisrv_records": 0,
        "pkfrv_files": 0, "pkfrv_records": 0,
        "skipped": 0, "failed": 0,
    }

    # NOTE(market-sync-v1): these bulk INSERTs duplicate the per-point
    # canonical writers in db/repositories/yield_curves.py
    # (upsert_pkrv_point / upsert_pkisrv_point / upsert_pkfrv_point). Kept
    # inline here for bulk-commit speed during backfill. Future cleanup:
    # refactor backfill loop to call the repo upserts in batches.
    for curve_type, parser_fn, sql in [
        (
            "pkrv",
            parse_pkrv_csv,
            """INSERT INTO pkrv_daily (date, tenor_months, yield_pct, change_bps, source, scraped_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, tenor_months) DO UPDATE SET
                 yield_pct=excluded.yield_pct, change_bps=excluded.change_bps,
                 source=excluded.source, scraped_at=datetime('now')""",
        ),
        (
            "pkisrv",
            parse_pkisrv_csv,
            """INSERT INTO pkisrv_daily (date, tenor, yield_pct, source, scraped_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, tenor) DO UPDATE SET
                 yield_pct=excluded.yield_pct, source=excluded.source,
                 scraped_at=datetime('now')""",
        ),
        (
            "pkfrv",
            parse_pkfrv_csv,
            """INSERT INTO pkfrv_daily (date, bond_code, issue_date, maturity_date,
                   coupon_frequency, fma_price, source, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, bond_code) DO UPDATE SET
                 issue_date=excluded.issue_date, maturity_date=excluded.maturity_date,
                 coupon_frequency=excluded.coupon_frequency,
                 fma_price=excluded.fma_price, source=excluded.source,
                 scraped_at=datetime('now')""",
        ),
    ]:
        subdir = rates_dir / curve_type
        if not subdir.exists():
            continue
        cutoff = since_dates.get(curve_type)

        # Collect files to parse
        files_to_parse = []
        for f in sorted(subdir.iterdir()):
            if f.suffix.lower() not in (".csv", ".xlsx"):
                continue
            date = _extract_date_from_filename(f.name)
            if not date:
                stats["failed"] += 1
                continue
            if cutoff and date <= cutoff:
                stats["skipped"] += 1
                continue
            files_to_parse.append((f, date))

        # Parse files in parallel (I/O bound disk reads)
        def _parse_one(args):
            fpath, date = args
            try:
                content = _read_rate_file(fpath)
                if not content:
                    return []
                return parser_fn(content, date)
            except Exception:
                return None

        all_records = []
        file_count = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for result in pool.map(_parse_one, files_to_parse):
                if result is None:
                    stats["failed"] += 1
                elif result:
                    all_records.extend(result)
                    file_count += 1

        # Batch insert in one transaction
        if all_records:
            if curve_type == "pkrv":
                rows = [
                    (r["date"], r["tenor_months"], r["yield_pct"],
                     r.get("change_bps"), r.get("source", "MUFAP"))
                    for r in all_records
                ]
            elif curve_type == "pkisrv":
                rows = [
                    (r["date"], r["tenor"], r["yield_pct"],
                     r.get("source", "MUFAP"))
                    for r in all_records
                ]
            else:  # pkfrv
                rows = [
                    (r["date"], r["bond_code"], r.get("issue_date"),
                     r.get("maturity_date"), r.get("coupon_frequency"),
                     r.get("fma_price"), r.get("source", "MUFAP"))
                    for r in all_records
                ]

            con.executemany(sql, rows)
            con.commit()

        stats[f"{curve_type}_files"] = file_count
        stats[f"{curve_type}_records"] = len(all_records)
        print(
            f"  {curve_type.upper():8s}: {file_count} files, "
            f"{len(all_records)} records"
        )

    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# BACKFILL — download all historical files from MUFAP API
# ═══════════════════════════════════════════════════════════════════════════════

DEBT_PRICING_CATEGORY = 44
DEBT_TRADING_CATEGORY = 45


def _classify_file(title: str) -> str:
    """Classify a MUFAP file by its title into pkrv/pkisrv/pkfrv."""
    t = title.upper().strip()
    if t.startswith("PKISRV"):
        return "pkisrv"
    if t.startswith("PKFRV"):
        return "pkfrv"
    return "pkrv"


def backfill_to_disk(
    rates_dir: Path | None = None,
    since_year: int = 2020,
    delay: float = 0.2,
    curve_types: list[str] | None = None,
) -> dict:
    """Download ALL PKRV/PKISRV/PKFRV files from MUFAP API to disk.

    No DB writes — just saves raw CSV/XLSX files to disk.
    Skips files already on disk.  Safe to re-run.

    Args:
        rates_dir: Root directory (default: DATA_ROOT / "rates").
        since_year: Only download files from this year onward.
        delay: Seconds between downloads (rate limiting).
        curve_types: Optional filter, e.g. ["pkrv", "pkisrv"].

    Returns:
        Stats dict with downloaded/skipped/failed counts.
    """
    import time as _time

    if rates_dir is None:
        rates_dir = RATES_DIR

    if curve_types is None:
        curve_types = ["pkrv", "pkisrv", "pkfrv"]

    for sub in curve_types:
        (rates_dir / sub).mkdir(parents=True, exist_ok=True)

    print(f"Fetching MUFAP file manifest (category {PKRV_CATEGORY_ID})...")
    file_list = fetch_mufap_file_list()
    print(f"  {len(file_list)} files in manifest")

    # Parse /Date(ts)/ format and filter
    filtered: list[tuple[str, str, str]] = []  # (curve, fname, url)
    for item in file_list:
        title = item.get("Title", "")
        path = item.get("FilePath", "")
        if not path:
            continue

        curve = _classify_file(title)
        if curve not in curve_types:
            continue

        # Extract year from the /Date(...)/ field
        date_str = item.get("Date", "")
        try:
            ts = int(date_str.replace("/Date(", "").replace(")/", ""))
            year = datetime.fromtimestamp(ts / 1000).year
        except (ValueError, TypeError):
            year = 0

        if year < since_year:
            continue

        fname = path.rsplit("/", 1)[-1]
        url = MUFAP_BASE + path
        filtered.append((curve, fname, url))

    print(f"  {len(filtered)} files match filters (since {since_year}, types={curve_types})")

    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)"

    for i, (curve, fname, url) in enumerate(filtered, 1):
        dest = rates_dir / curve / fname
        if dest.exists() and dest.stat().st_size > 0:
            stats["skipped"] += 1
            continue

        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 10:
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(resp.content)
                stats["downloaded"] += 1
                if stats["downloaded"] % 50 == 0:
                    print(f"  ... {stats['downloaded']} downloaded ({i}/{len(filtered)})")
            else:
                stats["failed"] += 1
        except Exception:
            stats["failed"] += 1

        _time.sleep(delay)

    print(
        f"  Done: {stats['downloaded']} downloaded, "
        f"{stats['skipped']} skipped, {stats['failed']} failed"
    )
    return stats


def backfill_to_db(
    con: sqlite3.Connection,
    rates_dir: Path | None = None,
) -> dict:
    """Parse ALL downloaded rate files and upsert to DB.

    Uses backfill_to_db_fast() — parallel file reads + batch commits.
    """
    if rates_dir is None:
        rates_dir = RATES_DIR
    init_yield_curve_schema(con)
    return backfill_to_db_fast(con, rates_dir)


def fetch_debt_file_list(category_id: int = DEBT_PRICING_CATEGORY) -> list[dict]:
    """Fetch debt instrument file list from MUFAP API.

    Args:
        category_id: 44 = Debt Pricing, 45 = Debt Trading.
    """
    resp = requests.post(
        MUFAP_FILE_API,
        json={"fk_HeaderSubMenuTabId": category_id},
        headers={"Content-Type": "application/json;charset=utf-8"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def backfill_debt_to_disk(
    rates_dir: Path | None = None,
    delay: float = 0.3,
) -> dict:
    """Download debt pricing and trading files from MUFAP API to disk."""
    import time as _time

    if rates_dir is None:
        rates_dir = RATES_DIR

    stats = {}
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)"

    for cat_id, subdir in [
        (DEBT_PRICING_CATEGORY, "debt_pricing"),
        (DEBT_TRADING_CATEGORY, "debt_trading"),
    ]:
        out_dir = rates_dir / subdir
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"Fetching {subdir} manifest (category {cat_id})...")
        file_list = fetch_debt_file_list(cat_id)
        print(f"  {len(file_list)} files")

        cat_stats = {"downloaded": 0, "skipped": 0, "failed": 0}
        for item in file_list:
            path = item.get("FilePath", "")
            if not path:
                continue
            fname = path.rsplit("/", 1)[-1]
            dest = out_dir / fname
            if dest.exists() and dest.stat().st_size > 0:
                cat_stats["skipped"] += 1
                continue
            try:
                resp = session.get(MUFAP_BASE + path, timeout=20)
                if resp.status_code == 200 and len(resp.content) > 10:
                    dest.write_bytes(resp.content)
                    cat_stats["downloaded"] += 1
                else:
                    cat_stats["failed"] += 1
            except Exception:
                cat_stats["failed"] += 1
            _time.sleep(delay)

        print(
            f"  {subdir}: {cat_stats['downloaded']} downloaded, "
            f"{cat_stats['skipped']} skipped, {cat_stats['failed']} failed"
        )
        stats[subdir] = cat_stats

    return stats


def show_status(rates_dir: Path | None = None):
    """Print summary of downloaded MUFAP files on disk."""
    if rates_dir is None:
        rates_dir = RATES_DIR

    print(f"\n{'=' * 60}")
    print(f"  MUFAP Download Status — {rates_dir}")
    print(f"{'=' * 60}\n")

    total_bytes = 0
    for subdir in ("pkrv", "pkisrv", "pkfrv", "debt_pricing", "debt_trading"):
        d = rates_dir / subdir
        if not d.exists():
            print(f"  {subdir:16s}: not downloaded yet")
            continue

        files = list(d.rglob("*.csv")) + list(d.rglob("*.xlsx"))
        sizes = sum(f.stat().st_size for f in files)
        total_bytes += sizes

        if files:
            # Extract dates for range
            dates = []
            for f in files:
                date = _extract_date_from_filename(f.name)
                if date:
                    dates.append(date)
            dates.sort()
            date_range = f"{dates[0]} to {dates[-1]}" if dates else "unknown"
            print(f"  {subdir:16s}: {len(files):>5} files  ({sizes / 1e3:.0f} KB)  {date_range}")
        else:
            print(f"  {subdir:16s}: 0 files")

    print(f"\n  Total disk: {total_bytes / 1e6:.1f} MB\n")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="MUFAP PKRV/PKISRV/PKFRV rate file manager"
    )
    sub = parser.add_subparsers(dest="command")

    # backfill-disk
    bd = sub.add_parser("backfill-disk", help="Download all files to disk (no DB)")
    bd.add_argument(
        "--since", type=int, default=2020, help="Start year (default: 2020)"
    )
    bd.add_argument(
        "--types", default="pkrv,pkisrv,pkfrv", help="Curve types (comma-separated)"
    )
    bd.add_argument("--delay", type=float, default=0.2, help="Delay between requests")
    bd.add_argument("--debt", action="store_true", help="Also download debt instruments")

    # backfill-db
    sub.add_parser("backfill-db", help="Parse all disk files into DB")

    # sync
    sub.add_parser("sync", help="Download new files since last DB date and sync to DB")

    # status
    sub.add_parser("status", help="Show download status")

    args = parser.parse_args()

    if args.command == "backfill-disk":
        curve_types = [t.strip() for t in args.types.split(",")]
        backfill_to_disk(
            since_year=args.since, delay=args.delay, curve_types=curve_types
        )
        if args.debt:
            backfill_debt_to_disk(delay=args.delay)

    elif args.command == "backfill-db":
        from pakfindata.db.connection import get_connection

        con = get_connection()
        stats = backfill_to_db(con)
        print(f"DB sync: {stats}")
        con.close()

    elif args.command == "sync":
        from pakfindata.db.connection import get_connection

        con = get_connection()
        stats = download_and_sync(con)
        print(f"Sync: {stats}")
        con.close()

    elif args.command == "status":
        show_status()

    else:
        parser.print_help()
