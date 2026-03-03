"""
MUFAP (Mutual Funds Association of Pakistan) data source module.

Scrapes live data from mufap.com.pk:
- Fund master list (1,100+ funds across 26 AMCs)
- Daily NAV + loads from HTML table (519 active funds)
- Historical NAV via JSON API per fund
- Performance returns (YTD, MTD, 1D..3Y)
- Asset allocation / portfolio breakdown

All endpoints are POST-based JSON APIs (X-Requested-With: XMLHttpRequest).
"""

import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests
from lxml import html

from ..config import DATA_ROOT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MUFAP_BASE = "https://www.mufap.com.pk"

_HEADERS_HTML = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

_HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

_HEADERS_JSON = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

_TIMEOUT = 60

# Map MUFAP sector names to our fund_type codes
_SECTOR_MAP = {
    "Open-End Funds": "OPEN_END",
    "Exchange Traded Fund (ETF)": "ETF",
    "Dedicated Equity Funds": "DEDICATED",
    "Voluntary Pension Scheme (VPS)": "VPS",
    "Employer Pension Funds": "EMPLOYER_PENSION",
    "Pension Funds (Open-End Funds)": "VPS",
}

MUFAP_CONFIG_PATH = DATA_ROOT / "mufap_config.json"

# Fund categories (MUFAP standard) — kept for backward compat
FUND_CATEGORIES = [
    {"code": "Equity", "name": "Equity"},
    {"code": "Money Market", "name": "Money Market"},
    {"code": "Income", "name": "Income"},
    {"code": "Balanced", "name": "Balanced"},
    {"code": "Asset Allocation", "name": "Asset Allocation"},
    {"code": "Islamic Equity", "name": "Islamic Equity"},
    {"code": "Islamic Income", "name": "Islamic Income"},
    {"code": "Islamic Money Market", "name": "Islamic Money Market"},
    {"code": "VPS", "name": "Voluntary Pension"},
    {"code": "ETF", "name": "Exchange Traded"},
    {"code": "Fund of Funds", "name": "Fund of Funds"},
    {"code": "Capital Protected", "name": "Capital Protected"},
]


# ---------------------------------------------------------------------------
# Low-level API helpers
# ---------------------------------------------------------------------------

def _post_json(
    path: str, data: dict | None = None, timeout: int = _TIMEOUT, max_retries: int = 3,
) -> dict:
    """POST to MUFAP JSON API and return parsed response.

    Retries on transient network errors with exponential backoff.
    """
    url = f"{MUFAP_BASE}{path}"
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            if data:
                r = requests.post(url, headers=_HEADERS_API, data=data, timeout=timeout)
            else:
                r = requests.post(url, headers=_HEADERS_API, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            delay = (2 ** attempt) + random.uniform(0, 1.0)
            logger.warning(
                "MUFAP API transient error (%s, attempt %d/%d): %s — retrying in %.1fs",
                path, attempt + 1, max_retries, e, delay,
            )
            time.sleep(delay)
    raise last_error  # type: ignore[misc]


def _get_html(path: str, params: dict | None = None, max_retries: int = 3) -> html.HtmlElement:
    """GET an HTML page and return parsed tree.

    Retries on transient network errors with exponential backoff.
    """
    url = f"{MUFAP_BASE}{path}"
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=_HEADERS_HTML, params=params, timeout=_TIMEOUT)
            r.raise_for_status()
            return html.fromstring(r.text)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            delay = (2 ** attempt) + random.uniform(0, 1.0)
            logger.warning(
                "MUFAP HTML transient error (%s, attempt %d/%d): %s — retrying in %.1fs",
                path, attempt + 1, max_retries, e, delay,
            )
            time.sleep(delay)
    raise last_error  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Fund master data (API)
# ---------------------------------------------------------------------------

def fetch_amc_list() -> list[dict]:
    """Fetch all Asset Management Companies from MUFAP API.

    Returns list of dicts with keys: AMCId, AMC_Desc, etc.
    """
    resp = _post_json("/AMC/GetAMCList")
    return resp.get("data", [])


def fetch_fund_profiles() -> list[dict]:
    """Fetch all fund profiles (1,100+ funds) from MUFAP API.

    Returns list of dicts with rich metadata:
    AMCId, AMC_Code, AMC_Desc, FundID, Fund_Desc, Fund_Code,
    inception, Sector_Desc, Cat_Desc, FrontLoad, BackLoad,
    ManagementFee, ProfileRiskOfFund, BenchMark, etc.
    """
    resp = _post_json("/FundProfile/GetAllFundProfile")
    return resp.get("data", [])


def fetch_fund_categories() -> list[dict]:
    """Fetch fund categories from MUFAP API."""
    resp = _post_json("/Industry/GetCategory")
    return resp.get("data", [])


def fetch_fund_types() -> list[dict]:
    """Fetch fund types (Open-End, ETF, VPS, etc.)."""
    resp = _post_json("/FundProfile/GetAllFundType")
    return resp.get("data", [])


# ---------------------------------------------------------------------------
# Daily NAV (HTML table scrape — all funds in one call)
# ---------------------------------------------------------------------------

def fetch_daily_nav_html() -> pd.DataFrame:
    """Scrape today's NAV for all ~519 active funds from HTML table.

    Returns DataFrame with columns:
        sector, amc, fund_name, category, inception_date,
        offer_price, repurchase_price, nav, validity_date,
        front_load, back_load, contingent_load, market_price, trustee
    """
    tree = _get_html("/Industry/IndustryStatDaily", params={"tab": "3"})
    table = tree.xpath('//table[@id="table_id"]')
    if not table:
        logger.warning("MUFAP: NAV table not found on IndustryStatDaily?tab=3")
        return pd.DataFrame()

    rows = table[0].xpath(".//tr")
    if len(rows) < 2:
        return pd.DataFrame()

    records = []
    for row in rows[1:]:  # skip header
        cells = [c.text_content().strip() for c in row.xpath(".//td")]
        if len(cells) < 14:
            continue

        # Parse numeric fields safely
        def _num(s):
            try:
                return float(s.replace(",", "")) if s and s != "N/A" else None
            except ValueError:
                return None

        records.append({
            "sector": cells[0],
            "amc": cells[1],
            "fund_name": cells[2],
            "category": cells[3],
            "inception_date": _parse_mufap_date(cells[4]),
            "offer_price": _num(cells[5]),
            "repurchase_price": _num(cells[6]),
            "nav": _num(cells[7]),
            "validity_date": _parse_mufap_date(cells[8]),
            "front_load": _num(cells[9]),
            "back_load": _num(cells[10]),
            "contingent_load": _num(cells[11]),
            "market_price": _num(cells[12]),
            "trustee": cells[13],
        })

    return pd.DataFrame(records)


def fetch_daily_performance_html() -> pd.DataFrame:
    """Scrape today's performance returns for all funds from HTML table.

    Returns DataFrame with columns:
        sector, category, fund_name, rating, benchmark, validity_date,
        nav, ytd, mtd, day_1, day_15, day_30, day_90, day_180, day_270,
        year_1, year_2, year_3
    """
    tree = _get_html("/Industry/IndustryStatDaily", params={"tab": "1"})
    table = tree.xpath("//table")
    if not table:
        return pd.DataFrame()

    rows = table[0].xpath(".//tr")
    if len(rows) < 2:
        return pd.DataFrame()

    records = []
    for row in rows[1:]:
        cells = [c.text_content().strip() for c in row.xpath(".//td")]
        if len(cells) < 18:
            continue

        def _num(s):
            try:
                return float(s.replace(",", "")) if s and s not in ("N/A", "-", "") else None
            except ValueError:
                return None

        records.append({
            "sector": cells[0],
            "category": cells[1],
            "fund_name": cells[2],
            "rating": cells[3] if cells[3] != "N/A" else None,
            "benchmark": cells[4] if cells[4] != "N/A" else None,
            "validity_date": _parse_mufap_date(cells[5]),
            "nav": _num(cells[6]),
            "ytd": _num(cells[7]),
            "mtd": _num(cells[8]),
            "day_1": _num(cells[9]),
            "day_15": _num(cells[10]),
            "day_30": _num(cells[11]),
            "day_90": _num(cells[12]),
            "day_180": _num(cells[13]),
            "day_270": _num(cells[14]),
            "year_1": _num(cells[15]),
            "year_2": _num(cells[16]),
            "year_3": _num(cells[17]),
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Historical NAV per fund (JSON API)
# ---------------------------------------------------------------------------

_TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)

_SYNC_MAX_RETRIES = 3


def fetch_fund_detail(
    mufap_int_id: str,
    date: str | None = None,
    max_retries: int = _SYNC_MAX_RETRIES,
) -> dict[str, Any]:
    """Fetch full fund detail via MUFAP JSON API.

    Uses /AMC/GetFundDetailbyAMCByDate which returns full NAV history
    (thousands of records back to inception).

    Retries up to *max_retries* times on transient network errors
    (ConnectionError, Timeout, ChunkedEncodingError) with exponential
    backoff + jitter.

    Args:
        mufap_int_id: Integer fund ID from the 'fund' field in FundProfile API.
        date: Date string (YYYY-M-D format accepted). Defaults to today.
        max_retries: Max retry attempts for transient failures (default 3).

    Returns dict with keys:
        profile    — fund metadata (AMC, category, loads, benchmark, etc.)
        nav_history — list of {FundID, netval, entryDate, CalDate}
        portfolio  — asset allocation breakdown
        returns    — performance returns (YTD, MTD, 1D..3Y)
        expenses   — expense ratios
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    # MUFAP accepts "YYYY-M-D" format
    url = f"{MUFAP_BASE}/AMC/GetFundDetailbyAMCByDate"
    body = {"FundID": str(mufap_int_id), "Date": date}

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=_HEADERS_JSON, json=body, timeout=120)
            r.raise_for_status()
            break
        except _TRANSIENT_ERRORS as e:
            last_error = e
            delay = (2 ** attempt) + random.uniform(0, 1.0)
            logger.warning(
                "MUFAP transient error (int_id=%s, attempt %d/%d): %s — retrying in %.1fs",
                mufap_int_id, attempt + 1, max_retries, e, delay,
            )
            time.sleep(delay)
        except requests.exceptions.HTTPError as e:
            # Non-transient HTTP error (4xx, etc.) — don't retry
            logger.error("MUFAP HTTP error (int_id=%s): %s", mufap_int_id, e)
            raise
    else:
        # All retries exhausted
        logger.error(
            "MUFAP fetch failed after %d retries (int_id=%s): %s",
            max_retries, mufap_int_id, last_error,
        )
        raise last_error  # type: ignore[misc]

    resp = r.json()

    # Response has double-encoded JSON: {"data": "<json_string>"}
    inner_str = resp.get("data", "{}")
    if isinstance(inner_str, str):
        inner = json.loads(inner_str) if inner_str else {}
    else:
        inner = inner_str

    result: dict[str, Any] = {
        "profile": None,
        "nav_history": [],
        "portfolio": None,
        "returns": None,
        "expenses": None,
    }

    # Table = fund profile (1 row)
    if inner.get("Table"):
        result["profile"] = inner["Table"][0]

    # Table1 = NAV history (thousands of rows)
    if inner.get("Table1"):
        result["nav_history"] = inner["Table1"]

    # Table2 = portfolio/asset allocation
    if inner.get("Table2"):
        result["portfolio"] = inner["Table2"][0] if inner["Table2"] else None

    # Table4 = performance returns
    if inner.get("Table4"):
        result["returns"] = inner["Table4"][0] if inner["Table4"] else None

    # Table5 = expense ratios
    if inner.get("Table5"):
        result["expenses"] = inner["Table5"][0] if inner["Table5"] else None

    return result


def fetch_nav_history(mufap_int_id: str, date: str | None = None) -> pd.DataFrame:
    """Fetch full NAV history for a single fund.

    Args:
        mufap_int_id: Integer fund ID from the 'fund' field in FundProfile.
        date: Optional date (defaults to today).

    Returns DataFrame with columns: fund_id, date, nav, offer_price, redemption_price
    """
    detail = fetch_fund_detail(mufap_int_id, date)
    history = detail.get("nav_history", [])
    if not history:
        return pd.DataFrame()

    records = []
    for row in history:
        date_str = row.get("entryDate") or row.get("CalDate")
        nav_val = row.get("netval")
        if date_str and nav_val is not None:
            # Normalize date (comes as "YYYY-MM-DD" or "YYYY-MM-DDT...")
            date_clean = str(date_str)[:10]
            try:
                nav_float = float(nav_val)
            except (ValueError, TypeError):
                continue
            offer = _safe_float(row.get("offer_price") or row.get("OfferPrice"))
            redemp = _safe_float(row.get("repurchase_price") or row.get("RedemptionPrice"))
            records.append({
                "fund_id": str(row.get("FundID", mufap_int_id)),
                "date": date_clean,
                "nav": nav_float,
                "offer_price": offer or nav_float,
                "redemption_price": redemp or nav_float,
                "aum": None,
                "nav_change_pct": None,
                "source": "MUFAP",
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Conversion helpers: MUFAP API → our DB schema
# ---------------------------------------------------------------------------

def _parse_mufap_date(s: str) -> str | None:
    """Parse MUFAP date formats to YYYY-MM-DD."""
    if not s or s in ("N/A", "-", ""):
        return None
    # "Feb 13, 2026" or "Oct 22, 2024"
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def map_mufap_category(raw_category: str) -> tuple[str, bool]:
    """Clean MUFAP category name — preserve granularity, remove display suffixes.

    Raw examples from tab=1:
      "Money Market (Annualized Return )" → "Money Market"
      "Shariah Compliant Commodity (Absolute Return )" → "Shariah Compliant Commodity"
      "VPS-Shariah Compliant Equity (Absolute Return )" → "VPS-Shariah Compliant Equity"
      "Aggressive Fixed Income (Annualized Return )" → "Aggressive Fixed Income"

    Returns (cleaned_category, is_shariah).
    """
    # Strip the "(Annualized Return )" or "(Absolute Return )" suffix from tab=1 HTML
    cleaned = re.sub(r'\s*\((?:Annualized|Absolute)\s+Return\s*\)\s*$', '', raw_category).strip()
    if not cleaned:
        cleaned = raw_category.strip()

    raw_lower = cleaned.lower()
    is_shariah = any(w in raw_lower for w in ("islamic", "shariah", "sharia"))

    return cleaned, is_shariah


def profile_to_fund_dict(p: dict) -> dict:
    """Convert a MUFAP FundProfile API record to our mutual_funds schema dict."""
    raw_cat = p.get("Cat_Desc", "")
    category, is_shariah = map_mufap_category(raw_cat)
    sector = p.get("Sector_Desc", "")
    fund_type = _SECTOR_MAP.get(sector, "OPEN_END")

    # Build a clean symbol from Fund_Code or Fund_Desc
    symbol = (p.get("Fund_Code") or "").strip()
    if not symbol:
        # Fallback: slugify fund name
        symbol = re.sub(r"[^A-Za-z0-9]+", "-", p.get("Fund_Desc", "")[:40]).strip("-")

    amc_code = (p.get("AMC_Code") or "").strip()
    fund_id = f"MUFAP:{p['FundID']}" if p.get("FundID") else f"MUFAP:{symbol}"

    # Integer fund ID (the "fund" field) — needed for historical NAV API
    mufap_int_id = p.get("fund")
    if mufap_int_id is not None:
        mufap_int_id = str(mufap_int_id)

    return {
        "fund_id": fund_id,
        "mufap_fund_id": str(p.get("FundID", "")),
        "mufap_int_id": mufap_int_id,
        "mufap_amc_id": str(p.get("AMCId", "")),
        "symbol": symbol,
        "fund_name": (p.get("Fund_Desc") or "").strip(),
        "amc_code": amc_code,
        "amc_name": (p.get("AMC_Desc") or "").strip(),
        "fund_type": fund_type,
        "category": category,
        "is_shariah": 1 if is_shariah else 0,
        "launch_date": _parse_mufap_date(p.get("inception") or p.get("LaunchDate", "")),
        "expense_ratio": None,
        "management_fee": _safe_float(p.get("ManagementFee")),
        "front_load": _safe_float(p.get("FrontLoad")),
        "back_load": _safe_float(p.get("BackLoad")),
        "risk_profile": (p.get("ProfileRiskOfFund") or "").strip() or None,
        "benchmark": (p.get("BenchMark") or "").strip() or None,
        "rating": (p.get("Rating1") or "").strip() or None,
        "trustee": (p.get("TrusteeCode") or "").strip() or None,
        "fund_manager": (p.get("FundManager") or "").strip() or None,
    }


def nav_row_to_dict(fund_id: str, row: dict) -> dict:
    """Convert a MUFAP daily NAV HTML row to our mutual_fund_nav schema dict."""
    return {
        "fund_id": fund_id,
        "date": row.get("validity_date"),
        "nav": row.get("nav"),
        "offer_price": row.get("offer_price"),
        "redemption_price": row.get("repurchase_price"),
        "aum": None,
        "nav_change_pct": None,
        "source": "MUFAP",
    }


def _safe_float(v) -> float | None:
    """Safely convert to float."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Backward-compatible functions (used by sync_mufap.py)
# ---------------------------------------------------------------------------

def get_fund_categories() -> list[dict]:
    """Get all MUFAP fund categories."""
    return FUND_CATEGORIES.copy()


def get_default_funds() -> list[dict]:
    """Get default/seed funds — now fetches live from MUFAP API."""
    try:
        profiles = fetch_fund_profiles()
        return [profile_to_fund_dict(p) for p in profiles]
    except Exception as e:
        logger.warning("MUFAP API unavailable, using static defaults: %s", e)
        return _STATIC_DEFAULTS


def load_mufap_config(config_path: Path | None = None) -> dict:
    """Load MUFAP configuration from JSON file."""
    path = config_path or MUFAP_CONFIG_PATH
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"funds": []}


def save_mufap_config(config: dict, config_path: Path | None = None) -> bool:
    """Save MUFAP configuration to JSON file."""
    path = config_path or MUFAP_CONFIG_PATH
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        return True
    except IOError:
        return False


def fetch_funds_from_mufap(category: str | None = None, fund_type: str = "OPEN_END") -> list[dict]:
    """Fetch mutual fund master data from MUFAP API."""
    funds = get_default_funds()
    if category:
        funds = [f for f in funds if f.get("category") == category]
    if fund_type != "ALL":
        funds = [f for f in funds if f.get("fund_type") == fund_type]
    return funds


def fetch_nav_from_mufap(
    fund_id: str | None = None, start_date: str | None = None, end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch NAV data — now uses live HTML scrape for daily data."""
    try:
        df = fetch_daily_nav_html()
        if df.empty:
            return df
        if fund_id:
            df = df[df["fund_name"].str.contains(fund_id.replace("MUFAP:", ""), case=False, na=False)]
        return df
    except Exception as e:
        logger.warning("MUFAP NAV fetch failed: %s", e)
        return pd.DataFrame()


def fetch_mutual_fund_data(
    fund_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    source: str = "AUTO",
    mufap_int_id: str | None = None,
) -> pd.DataFrame:
    """Fetch NAV data from MUFAP for a specific fund.

    If mufap_int_id is provided, fetches full history via the historical API
    (with automatic retries on transient network errors).
    Otherwise falls back to the daily HTML scrape.
    """
    if mufap_int_id:
        try:
            df = fetch_nav_history(mufap_int_id)
            if not df.empty and start_date:
                df = df[df["date"] >= start_date]
            if not df.empty and end_date:
                df = df[df["date"] <= end_date]
            return df
        except _TRANSIENT_ERRORS as e:
            # Already retried inside fetch_fund_detail — log at error level
            logger.error(
                "Historical NAV fetch failed for int_id=%s after retries "
                "(network unreachable / timeout): %s",
                mufap_int_id, e,
            )
        except requests.exceptions.HTTPError as e:
            logger.error(
                "Historical NAV fetch HTTP error for int_id=%s: %s",
                mufap_int_id, e,
            )
        except Exception as e:
            logger.warning(
                "Historical NAV fetch failed for int_id=%s: %s", mufap_int_id, e,
            )
    return fetch_nav_from_mufap(fund_id, start_date, end_date)


def normalize_nav_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize NAV DataFrame to standard schema."""
    if df.empty:
        cols = ["fund_id", "date", "nav", "offer_price", "redemption_price", "aum", "nav_change_pct", "source"]
        return pd.DataFrame(columns=cols)
    for col in ("offer_price", "redemption_price"):
        if col not in df.columns:
            df[col] = df.get("nav")
    for col in ("aum", "nav_change_pct"):
        if col not in df.columns:
            df[col] = None
    if "source" not in df.columns:
        df["source"] = "MUFAP"
    return df


# ---------------------------------------------------------------------------
# Static defaults (fallback when MUFAP API is unreachable)
# ---------------------------------------------------------------------------

_STATIC_DEFAULTS = [
    {"fund_id": "MUFAP:ABL-ISF", "symbol": "ABL-ISF", "fund_name": "ABL Islamic Stock Fund",
     "amc_code": "ABL", "amc_name": "ABL Asset Management", "fund_type": "OPEN_END",
     "category": "Islamic Equity", "is_shariah": 1, "launch_date": "2010-01-01"},
    {"fund_id": "MUFAP:ABL-CSF", "symbol": "ABL-CSF", "fund_name": "ABL Cash Fund",
     "amc_code": "ABL", "amc_name": "ABL Asset Management", "fund_type": "OPEN_END",
     "category": "Money Market", "is_shariah": 0, "launch_date": "2009-01-01"},
]


# ---------------------------------------------------------------------------
# Async batch fetcher (aiohttp)
# ---------------------------------------------------------------------------

MUFAP_FETCH_TIMEOUT = 120   # seconds per request
MUFAP_MAX_CONCURRENT = 4    # conservative — MUFAP is a small server
MUFAP_RATE_DELAY = 0.3      # polite delay between requests
MUFAP_MAX_RETRIES = 3


async def async_fetch_fund_detail(
    session,
    semaphore: asyncio.Semaphore,
    mufap_int_id: str,
    date: str | None = None,
) -> tuple[str, dict | None, str | None]:
    """Async version of fetch_fund_detail.

    Returns:
        Tuple of (mufap_int_id, parsed_result_dict, error_string_or_None).
    """
    import aiohttp  # lazy — avoid import error when aiohttp not installed

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    url = f"{MUFAP_BASE}/AMC/GetFundDetailbyAMCByDate"
    body = {"FundID": str(mufap_int_id), "Date": date}
    last_error = None

    for attempt in range(MUFAP_MAX_RETRIES):
        async with semaphore:
            try:
                timeout = aiohttp.ClientTimeout(total=MUFAP_FETCH_TIMEOUT)
                async with session.post(
                    url, json=body, headers=_HEADERS_JSON, timeout=timeout
                ) as resp:
                    if resp.status == 200:
                        resp_data = await resp.json(content_type=None)
                        await asyncio.sleep(MUFAP_RATE_DELAY)

                        # Parse double-encoded JSON (same as sync version)
                        inner_str = resp_data.get("data", "{}")
                        if isinstance(inner_str, str):
                            inner = json.loads(inner_str) if inner_str else {}
                        else:
                            inner = inner_str

                        result = {
                            "profile": (
                                inner["Table"][0] if inner.get("Table") else None
                            ),
                            "nav_history": inner.get("Table1", []),
                            "portfolio": (
                                inner["Table2"][0] if inner.get("Table2") else None
                            ),
                            "returns": (
                                inner["Table4"][0] if inner.get("Table4") else None
                            ),
                            "expenses": (
                                inner["Table5"][0] if inner.get("Table5") else None
                            ),
                        }
                        return mufap_int_id, result, None

                    last_error = "HTTP {}".format(resp.status)
            except asyncio.TimeoutError:
                last_error = "timeout"
            except aiohttp.ClientError as e:
                last_error = str(e)
            except json.JSONDecodeError as e:
                last_error = "JSON decode: {}".format(e)
            except Exception as e:
                last_error = str(e)

        # Exponential backoff before retry
        if attempt < MUFAP_MAX_RETRIES - 1:
            delay = (2 ** attempt) * 1.0 + random.uniform(0, 1.0)
            await asyncio.sleep(delay)

    return mufap_int_id, None, last_error


async def async_fetch_nav_batch(
    funds: list[dict],
    max_concurrent: int = MUFAP_MAX_CONCURRENT,
    on_result: Callable[[str, str, dict | None, str | None], None] | None = None,
) -> dict:
    """Fetch NAV history for multiple funds concurrently.

    Args:
        funds: List of fund dicts, each must have 'fund_id' and 'mufap_int_id'.
        max_concurrent: Max concurrent HTTP requests.
        on_result: Callback(fund_id, mufap_int_id, result_dict, error).

    Returns:
        Summary dict: {ok, failed, total, elapsed}.
    """
    import aiohttp  # lazy — avoid import error when aiohttp not installed

    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent + 2)

    ok = 0
    failed = 0
    start = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:

        async def _fetch_one(fund: dict) -> None:
            nonlocal ok, failed
            fund_id = fund["fund_id"]
            mufap_int_id = fund["mufap_int_id"]

            _mid, result, error = await async_fetch_fund_detail(
                session, semaphore, mufap_int_id
            )

            if error:
                failed += 1
            else:
                ok += 1

            if on_result:
                on_result(fund_id, mufap_int_id, result, error)

        tasks = [_fetch_one(f) for f in funds]
        await asyncio.gather(*tasks)

    return {
        "ok": ok,
        "failed": failed,
        "total": len(funds),
        "elapsed": time.time() - start,
    }
