"""Parse SBP Lending & Deposit Rates archive XLS.

Source: https://www.sbp.org.pk/ecodata/Lendingdepositrates_Arch.xls

Four sheets spanning 1997-2024+, each with different column layouts.
We extract monthly weighted-average lending & deposit rates by bank type.
"""

import logging
import re
import sqlite3
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

XLS_URL = "https://www.sbp.org.pk/ecodata/Lendingdepositrates_Arch.xls"

# Canonical bank_type keys
_BANK_MAP_SHEET1 = {
    "Nationalized Banks": "public",
    "Privatized Domestic Banks": "privatized",
    "Private Domestic Banks": "private",
    "Foreign Banks": "foreign",
    "Specialized Banks": "specialized",
    "All Groups": "all_banks",
}

_BANK_MAP_SHEET2 = {
    "Public": "public",
    "Private": "private",
    "Foreign": "foreign",
    "Specialized": "specialized",
    "All Banks": "all_banks",
}

_BANK_MAP_SHEET4 = {
    "1. Scheduled Banks": "all_banks",
    "1.1. Public": "public",
    "1.2. Private": "private",
    "1.3. Foreign": "foreign",
    "1.4. Specialized": "specialized",
    "2. DFIs": "dfis",
    "3. MFBs": "mfbs",
    "All Financial Institutions": "all_fi",
}

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_month_year(text: str) -> str | None:
    """Parse date strings like 'Jul-22', 'Jan - 11 ***', 'Jun.', etc. to YYYY-MM-DD."""
    if not text or str(text) == "nan":
        return None
    text = str(text).strip().rstrip("*$ ").strip()

    # Format: "Jul-22", "Aug-22", "Nov-22 "
    m = re.match(r"([A-Za-z]{3})\s*[-]\s*(\d{2,4})", text)
    if m:
        mon = _MONTH_ABBR.get(m.group(1).lower()[:3])
        yr = int(m.group(2))
        if yr < 100:
            yr += 2000 if yr < 50 else 1900
        if mon:
            return f"{yr:04d}-{mon:02d}-01"

    # Format: "Jan - 11 ***"
    m = re.match(r"([A-Za-z]{3})\s*-\s*(\d{2,4})", text)
    if m:
        mon = _MONTH_ABBR.get(m.group(1).lower()[:3])
        yr = int(m.group(2))
        if yr < 100:
            yr += 2000 if yr < 50 else 1900
        if mon:
            return f"{yr:04d}-{mon:02d}-01"

    return None


def _parse_year_month(year_str: str, month_str: str) -> str | None:
    """Parse Sheet 1 format: year='1997*', month='Jun.'."""
    if not year_str or str(year_str) == "nan":
        return None
    yr_match = re.match(r"(\d{4})", str(year_str).strip())
    if not yr_match:
        return None
    yr = int(yr_match.group(1))

    mon_match = re.match(r"([A-Za-z]{3})", str(month_str).strip())
    if not mon_match:
        return None
    mon = _MONTH_ABBR.get(mon_match.group(1).lower()[:3])
    if not mon:
        return None
    return f"{yr:04d}-{mon:02d}-01"


def _safe_float(v) -> float | None:
    """Convert to float, return None for non-numeric."""
    try:
        f = float(v)
        if pd.isna(f):
            return None
        return round(f, 4)
    except (ValueError, TypeError):
        return None


def parse_sheet1(xls: pd.ExcelFile) -> list[dict]:
    """Parse Sheet 1: Jan 1997 - Dec 2003 (semi-annual, simple layout)."""
    df = pd.read_excel(xls, sheet_name=0, header=None)
    rows = []
    current_year = None

    for i in range(6, len(df)):
        yr_val = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else None
        mon_val = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else None

        if yr_val and yr_val != "nan":
            current_year = yr_val

        if not mon_val or mon_val == "nan" or not current_year:
            continue

        rate_date = _parse_year_month(current_year, mon_val)
        if not rate_date:
            continue

        # Cols: 2=Nationalized Lending, 3=Nationalized Deposit, 4=Privatized L, 5=Privatized D,
        #       6=Private L, 7=Private D, 8=Foreign L, 9=Foreign D, 10=Specialized L, 11=Specialized D,
        #       12=All L, 13=All D
        pairs = [
            ("public", 2, 3),
            ("privatized", 4, 5),
            ("private", 6, 7),
            ("foreign", 8, 9),
            ("specialized", 10, 11),
            ("all_banks", 12, 13),
        ]
        for bank_type, lcol, dcol in pairs:
            lend = _safe_float(df.iloc[i, lcol]) if lcol < len(df.columns) else None
            dep = _safe_float(df.iloc[i, dcol]) if dcol < len(df.columns) else None
            if lend is not None or dep is not None:
                rows.append({
                    "rate_date": rate_date,
                    "bank_type": bank_type,
                    "lending_rate": lend,
                    "deposit_rate": dep,
                })

    return rows


def _parse_grouped_sheet(xls: pd.ExcelFile, sheet_name: str, bank_map: dict,
                         lending_col: int = 2, lending_excl_col: int | None = None,
                         deposit_col: int | None = None, deposit_excl_col: int | None = None,
                         ) -> list[dict]:
    """Parse sheets 2-4 which have grouped rows: date header, then bank-type sub-rows."""
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    ncols = len(df.columns)
    rows = []
    current_date = None

    def _get(row_idx, col_idx):
        if col_idx is not None and col_idx < ncols:
            return _safe_float(df.iloc[row_idx, col_idx])
        return None

    def _make_row(row_idx, bank_type):
        lend = _get(row_idx, lending_col)
        lend_ex = _get(row_idx, lending_excl_col)
        dep = _get(row_idx, deposit_col)
        dep_ex = _get(row_idx, deposit_excl_col)
        if any(v is not None for v in (lend, lend_ex, dep, dep_ex)):
            return {
                "rate_date": current_date,
                "bank_type": bank_type,
                "lending_rate": lend,
                "lending_excl_zero": lend_ex,
                "deposit_rate": dep,
                "deposit_excl_zero": dep_ex,
            }
        return None

    for i in range(5, len(df)):
        col0 = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else None
        col1 = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else None

        # Date header row (col0 has date, col1 is NaN or bank type for sheet4)
        if col0 and col0 != "nan":
            d = _parse_month_year(col0)
            if d:
                current_date = d
                # Sheet4: same row has bank data in col1
                if col1 and col1 != "nan" and col1 in bank_map:
                    r = _make_row(i, bank_map[col1])
                    if r:
                        rows.append(r)
                continue

        if not current_date or not col1 or col1 == "nan":
            continue

        # Check if col1 is a bank type
        bank_type = bank_map.get(col1)
        if not bank_type:
            # Stop if we hit footnotes
            if col1 and any(k in col1.lower() for k in ["note", "source", "compilation", "contact", "stand"]):
                break
            continue

        r = _make_row(i, bank_type)
        if r:
            rows.append(r)

    return rows


def parse_xls(xls_path: str | Path) -> list[dict]:
    """Parse all sheets from the SBP Lending & Deposit Rates XLS."""
    xls = pd.ExcelFile(str(xls_path))
    all_rows = []

    # Sheet 1: 1997-2003 (simple columns)
    try:
        all_rows.extend(parse_sheet1(xls))
    except Exception as e:
        logger.warning("Sheet 1 parse error: %s", e)

    # Sheet 2: Jan 2004 - Dec 2010
    # Cols: 2=Lending incl zero, 3=Lending excl zero, 6=Deposit incl zero, 7=Deposit excl zero
    try:
        all_rows.extend(_parse_grouped_sheet(
            xls, xls.sheet_names[1], _BANK_MAP_SHEET2,
            lending_col=2, lending_excl_col=3,
            deposit_col=6, deposit_excl_col=7,
        ))
    except Exception as e:
        logger.warning("Sheet 2 parse error: %s", e)

    # Sheet 3: Jan 2011 - Jun 2022
    # Cols: 2=Lending incl zero+interbank, 4=Lending excl zero, 10=Deposit incl, 12=Deposit excl
    try:
        all_rows.extend(_parse_grouped_sheet(
            xls, xls.sheet_names[2], _BANK_MAP_SHEET2,
            lending_col=2, lending_excl_col=4,
            deposit_col=10, deposit_excl_col=12,
        ))
    except Exception as e:
        logger.warning("Sheet 3 parse error: %s", e)

    # Sheet 4: Jul 2022 onwards (same layout as Sheet 3 + DFIs/MFBs)
    # Cols: 2=Lending incl, 4=Lending excl, 10=Deposit incl, 12=Deposit excl
    try:
        all_rows.extend(_parse_grouped_sheet(
            xls, xls.sheet_names[3], _BANK_MAP_SHEET4,
            lending_col=2, lending_excl_col=4,
            deposit_col=10, deposit_excl_col=12,
        ))
    except Exception as e:
        logger.warning("Sheet 4 parse error: %s", e)

    return all_rows


def download_and_seed(con: sqlite3.Connection, xls_path: str | Path | None = None) -> dict:
    """Download XLS from SBP, parse, and seed into sbp_lending_deposit_rates.

    Args:
        con: Database connection
        xls_path: Optional local path to XLS file. If None, downloads from SBP.

    Returns:
        dict with status, rows_inserted, date_range
    """
    from pakfindata import init_schema
    init_schema(con)

    if xls_path and Path(xls_path).exists():
        logger.info("Using local XLS: %s", xls_path)
        rows = parse_xls(xls_path)
    else:
        logger.info("Downloading from %s", XLS_URL)
        resp = requests.get(XLS_URL, timeout=30)
        resp.raise_for_status()

        # Save to disk
        from pakfindata.config import DATA_ROOT
        save_dir = DATA_ROOT / "sbp"
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / "Lendingdepositrates_Arch.xls"
        save_path.write_bytes(resp.content)
        logger.info("Saved XLS to %s (%d bytes)", save_path, len(resp.content))

        rows = parse_xls(save_path)

    if not rows:
        return {"status": "error", "message": "No rows parsed from XLS"}

    inserted = 0
    for r in rows:
        try:
            con.execute(
                """INSERT INTO sbp_lending_deposit_rates
                   (rate_date, bank_type, lending_rate, deposit_rate,
                    lending_excl_zero, deposit_excl_zero, source)
                   VALUES (?, ?, ?, ?, ?, ?, 'SBP_XLS')
                   ON CONFLICT(rate_date, bank_type) DO UPDATE SET
                       lending_rate = COALESCE(excluded.lending_rate, lending_rate),
                       deposit_rate = COALESCE(excluded.deposit_rate, deposit_rate),
                       lending_excl_zero = COALESCE(excluded.lending_excl_zero, lending_excl_zero),
                       deposit_excl_zero = COALESCE(excluded.deposit_excl_zero, deposit_excl_zero)
                """,
                (r["rate_date"], r["bank_type"], r.get("lending_rate"),
                 r.get("deposit_rate"), r.get("lending_excl_zero"), r.get("deposit_excl_zero")),
            )
            inserted += 1
        except Exception as e:
            logger.debug("Insert skip: %s", e)

    con.commit()

    dates = sorted(set(r["rate_date"] for r in rows))
    total = con.execute("SELECT COUNT(*) FROM sbp_lending_deposit_rates").fetchone()[0]

    return {
        "status": "ok",
        "rows_parsed": len(rows),
        "rows_in_db": total,
        "date_range": f"{dates[0]} to {dates[-1]}" if dates else "none",
        "bank_types": sorted(set(r["bank_type"] for r in rows)),
    }
