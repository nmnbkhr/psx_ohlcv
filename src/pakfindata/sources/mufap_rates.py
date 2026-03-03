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
    """Parse PKISRV CSV — extract yield rates from the right-side columns."""
    records = []
    for line in content.splitlines():
        # Look for "N - Month,X.XX%" or "N - Year,X.XX%" pattern
        m = PKISRV_TENOR_RE.search(line)
        if not m:
            continue

        num = int(m.group(1))
        unit = m.group(2).lower()
        if unit == "year":
            num *= 12

        # Extract the percentage value after the tenor
        parts = line.split(",")
        for part in parts:
            part = part.strip().rstrip("%")
            try:
                val = float(part)
                if 0 < val < 50:  # reasonable yield range
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
