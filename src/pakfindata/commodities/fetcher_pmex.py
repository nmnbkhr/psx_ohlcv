"""PMEX Portal direct JSON API fetcher — 134 instruments across 9 categories.

The PMEX Market Watch portal exposes a simple JSON POST endpoint that returns
the complete market snapshot. No authentication, no Selenium, no API key needed.

Endpoint: POST https://dportal.pmex.com.pk/MWatchNew/Home/GetJSONObject
Body:     {} (empty JSON)
Headers:  User-Agent (browser-like), Content-Type: application/json

Response: JSON array of ~134 instrument records with fields:
  Contract, Category, Bid, Ask, Open, Close, High, Low, Last_Price,
  Last_Vol, Total_Vol, Total_Volume, Change, Change_Per, BidDiff, AskDiff,
  State, _datetime (Unix seconds)

Categories: Indices, Metals, Oil, Cots, Energy, Agri, Phy_Agri, Phy_Gold, Financials
"""

import logging
import re
from datetime import datetime, timezone

import requests

logger = logging.getLogger("pakfindata.commodities.pmex")

PMEX_API_URL = "https://dportal.pmex.com.pk/MWatchNew/Home/GetJSONObject"

PMEX_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://dportal.pmex.com.pk/MWatchNew/",
}

PMEX_CATEGORIES = [
    "Indices", "Metals", "Oil", "Cots", "Energy",
    "Agri", "Phy_Agri", "Phy_Gold", "Financials",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_number(value) -> float | None:
    """Parse a number from PMEX JSON value (may be string, int, float, or None)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[^\d.\-]", "", str(value).replace(",", ""))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _parse_pmex_datetime(unix_ts) -> tuple[str, str]:
    """Convert PMEX _datetime (Unix seconds) to (date_str, iso_timestamp).

    Returns:
        (snapshot_date as 'YYYY-MM-DD', snapshot_ts as ISO 8601 string)
    """
    if not unix_ts:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.isoformat()

    try:
        ts = int(unix_ts)
        # Handle milliseconds if value is unreasonably large
        if ts > 9999999999:
            ts = ts // 1000
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d"), dt.isoformat()
    except (ValueError, TypeError, OSError):
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d"), now.isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Fetcher functions
# ─────────────────────────────────────────────────────────────────────────────

def fetch_pmex_snapshot() -> list[dict]:
    """Fetch the full PMEX market watch snapshot.

    Makes a single POST request to the PMEX dportal API.
    Returns list of dicts with standardized keys.
    Returns empty list on failure.
    """
    try:
        resp = requests.post(
            PMEX_API_URL,
            json={},
            headers=PMEX_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.warning("PMEX portal fetch failed: %s", e)
        return []

    if not isinstance(raw, list):
        logger.warning("PMEX portal: unexpected response type %s", type(raw))
        return []

    results = []
    for record in raw:
        contract = (record.get("Contract") or "").strip()
        if not contract:
            continue

        snapshot_date, snapshot_ts = _parse_pmex_datetime(record.get("_datetime"))

        results.append({
            "contract": contract,
            "category": (record.get("Category") or "").strip(),
            "bid": _parse_number(record.get("Bid")),
            "ask": _parse_number(record.get("Ask")),
            "open": _parse_number(record.get("Open")),
            "close": _parse_number(record.get("Close")),
            "high": _parse_number(record.get("High")),
            "low": _parse_number(record.get("Low")),
            "last_price": _parse_number(record.get("Last_Price")),
            "last_vol": _parse_number(record.get("Last_Vol")),
            "total_vol": _parse_number(record.get("Total_Vol")),
            "total_volume": _parse_number(record.get("Total_Volume")),
            "change": _parse_number(record.get("Change")),
            "change_pct": _parse_number(record.get("Change_Per")),
            "bid_diff": _parse_number(record.get("BidDiff")),
            "ask_diff": _parse_number(record.get("AskDiff")),
            "snapshot_date": snapshot_date,
            "snapshot_ts": snapshot_ts,
            "state": (record.get("State") or "").strip(),
            "source": "pmex_portal",
        })

    logger.info(
        "PMEX portal: fetched %d instruments across %d categories",
        len(results),
        len(set(r["category"] for r in results)),
    )
    return results


def fetch_pmex_by_category(category: str | None = None) -> dict[str, list[dict]]:
    """Fetch PMEX snapshot grouped by category.

    Args:
        category: Optional filter for a single category.

    Returns:
        Dict of {category_name: [instrument_dicts]}.
    """
    all_data = fetch_pmex_snapshot()
    grouped: dict[str, list[dict]] = {}
    for record in all_data:
        cat = record["category"]
        if category and cat.lower() != category.lower():
            continue
        grouped.setdefault(cat, []).append(record)
    return grouped
