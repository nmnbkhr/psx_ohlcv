"""PSX Index Scraper - Scrapes KSE-100 and other indices from PSX DPS.

Fetches index data from:
- https://dps.psx.com.pk/market-summary (main page)
- https://dps.psx.com.pk/indices (indices API)

Index codes:
- KSE100: KSE-100 Index
- KSE100PR: KSE-100 Price Return Index
- ALLSHR: All Share Index
- KSE30: KSE-30 Index
- KMI30: KMI-30 Index
- BKTI: Banking Index
- OGTI: Oil & Gas Index
- KMIALLSHR: KMI All Share Index
- PSXDIV20: PSX Dividend 20 Index
- And more...
"""

import logging
import re
import sqlite3
from datetime import datetime
from typing import Any

import requests
from lxml import html

logger = logging.getLogger(__name__)

# PSX DPS URLs
PSX_BASE_URL = "https://dps.psx.com.pk"
PSX_MARKET_SUMMARY_URL = f"{PSX_BASE_URL}/market-summary"
PSX_INDICES_API_URL = f"{PSX_BASE_URL}/indices"

# Known index codes
INDEX_CODES = [
    "KSE100",
    "KSE100PR",
    "ALLSHR",
    "KSE30",
    "KMI30",
    "BKTI",
    "OGTI",
    "KMIALLSHR",
    "PSXDIV20",
    "UPP9",
    "NITPGI",
    "NBPPGI",
    "MZNPI",
    "JSMFI",
]


def fetch_indices_data(timeout: int = 30) -> list[dict[str, Any]]:
    """
    Fetch all indices data from PSX DPS timeseries API.

    Uses the same endpoint as EOD stock data: /timeseries/eod/{INDEX_CODE}
    Format: [[timestamp, value, volume, vwap], ...]

    Returns:
        List of dicts with index data
    """
    indices_data = []

    # Fetch data for key indices using the timeseries API
    key_indices = ["KSE100", "KSE30", "KMI30", "ALLSHR"]

    for index_code in key_indices:
        try:
            url = f"{PSX_BASE_URL}/timeseries/eod/{index_code}"
            response = requests.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            response.raise_for_status()

            data = response.json()
            if data.get("status") == 1 and data.get("data"):
                # Get the most recent data point (first in the list)
                latest = data["data"][0]
                # Format: [timestamp, value, volume, vwap]
                timestamp = latest[0]
                value = latest[1]
                volume = latest[2] if len(latest) > 2 else None
                vwap = latest[3] if len(latest) > 3 else None

                # Get previous day for change calculation
                prev_value = None
                if len(data["data"]) > 1:
                    prev_value = data["data"][1][1]

                # Convert timestamp to date
                index_date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                index_time = datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")

                # Calculate change
                change = None
                change_pct = None
                if prev_value and value:
                    change = value - prev_value
                    change_pct = (change / prev_value) * 100

                # Get high/low from recent data
                high = max(d[1] for d in data["data"][:5]) if len(data["data"]) >= 5 else value
                low = min(d[1] for d in data["data"][:5]) if len(data["data"]) >= 5 else value

                indices_data.append({
                    "index_code": index_code,
                    "index_date": index_date,
                    "index_time": index_time,
                    "value": value,
                    "change": change,
                    "change_pct": change_pct,
                    "open": None,
                    "high": high,
                    "low": low,
                    "volume": volume,
                    "previous_close": prev_value,
                    "ytd_change_pct": None,
                    "one_year_change_pct": None,
                    "week_52_low": min(d[1] for d in data["data"]) if data["data"] else None,
                    "week_52_high": max(d[1] for d in data["data"]) if data["data"] else None,
                })

                logger.debug(f"Fetched {index_code}: {value:,.2f}")

        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {index_code}: {e}")
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"Failed to parse {index_code}: {e}")

    return indices_data


def _extract_kse100_from_page(tree) -> dict[str, Any] | None:
    """Extract KSE-100 data from the parsed HTML tree."""
    try:
        # Look for the main KSE100 display
        # Based on screenshot: Large number display with change

        # Try to find index value - usually a large prominent number
        # XPath patterns for PSX DPS structure

        # Pattern 1: Look for specific index container
        index_containers = tree.xpath(
            '//div[contains(@class, "index") or contains(@id, "index")]'
        )

        # Pattern 2: Look for the value with specific formatting
        # KSE100 shows as "188,202.85"
        large_numbers = tree.xpath(
            '//div[contains(@class, "value") or contains(@class, "price")]//text()'
        )

        # Pattern 3: Look in tables
        tables = tree.xpath('//table')

        for table in tables:
            rows = table.xpath('.//tr')
            for row in rows:
                text = row.text_content()
                if 'KSE100' in text.upper() or 'KSE-100' in text.upper():
                    cells = row.xpath('.//td/text() | .//th/text()')
                    logger.debug(f"Found KSE100 row: {cells}")

        # Pattern 4: Look for specific data-* attributes
        data_elements = tree.xpath('//*[@data-index or @data-value]')

        # Try to extract from the visible display
        # Based on the screenshot structure

        # Find the large index value display
        value_xpath_patterns = [
            '//div[@id="indices"]//div[contains(@class, "tab-pane")]//h2/text()',
            '//div[@id="indices"]//div[contains(@class, "tab-pane")]//span[contains(@class, "value")]/text()',
            '//div[contains(text(), "188,")]/..//text()',  # Looking for the actual number pattern
        ]

        return None  # Return None for now, will parse from API

    except Exception as e:
        logger.debug(f"Error extracting KSE100: {e}")
        return None


def _normalize_index_data(raw_data: dict) -> dict[str, Any]:
    """Normalize index data to standard format."""
    return {
        "index_code": raw_data.get("code", "").upper(),
        "index_date": datetime.now().strftime("%Y-%m-%d"),
        "index_time": datetime.now().strftime("%H:%M:%S"),
        "value": _parse_number(raw_data.get("value") or raw_data.get("current")),
        "change": _parse_number(raw_data.get("change")),
        "change_pct": _parse_number(raw_data.get("change_pct") or raw_data.get("percentChange")),
        "open": _parse_number(raw_data.get("open")),
        "high": _parse_number(raw_data.get("high")),
        "low": _parse_number(raw_data.get("low")),
        "volume": _parse_int(raw_data.get("volume")),
        "previous_close": _parse_number(raw_data.get("previousClose") or raw_data.get("prev_close")),
        "ytd_change_pct": _parse_number(raw_data.get("ytdChange")),
        "one_year_change_pct": _parse_number(raw_data.get("oneYearChange")),
        "week_52_low": _parse_number(raw_data.get("week52Low")),
        "week_52_high": _parse_number(raw_data.get("week52High")),
    }


def _parse_number(value) -> float | None:
    """Parse a number from various formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Remove commas and parse
        cleaned = value.replace(",", "").replace("%", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_int(value) -> int | None:
    """Parse an integer from various formats."""
    num = _parse_number(value)
    return int(num) if num is not None else None


def fetch_market_summary(timeout: int = 30) -> dict[str, Any]:
    """
    Fetch market summary (trading segments) from PSX DPS.

    Returns:
        Dict with segment data (REG, FUT, CSF, ODL, SQUAREUP)
    """
    try:
        response = requests.get(
            PSX_MARKET_SUMMARY_URL,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()

        tree = html.fromstring(response.content)

        summary = {
            "stat_date": datetime.now().strftime("%Y-%m-%d"),
            "stat_time": datetime.now().strftime("%H:%M:%S"),
            "board_type": "MAIN",
        }

        # Parse each segment
        # Based on screenshot: REGULAR, DELIVERABLE FUTURES, CASH SETTLED FUTURES, ODD LOT, SQUARE UP

        segment_mapping = {
            "REGULAR": "reg",
            "DELIVERABLE FUTURES": "fut",
            "CASH SETTLED FUTURES": "csf",
            "ODD LOT": "odl",
            "SQUARE UP": "squareup",
        }

        # Find segment containers - they appear as colored boxes
        segment_divs = tree.xpath('//div[contains(@class, "segment") or contains(@class, "market-type")]')

        # Alternative: Look for table structure
        tables = tree.xpath('//table[contains(@class, "market") or contains(@class, "summary")]')

        for table in tables:
            headers = table.xpath('.//th/text() | .//thead//td/text()')
            rows = table.xpath('.//tbody//tr')

            for row in rows:
                cells = row.xpath('.//td')
                if cells:
                    segment_name = cells[0].text_content().strip().upper()
                    for key, prefix in segment_mapping.items():
                        if key in segment_name:
                            if len(cells) >= 4:
                                summary[f"{prefix}_state"] = cells[1].text_content().strip() if len(cells) > 1 else None
                                summary[f"{prefix}_trades"] = _parse_int(cells[2].text_content()) if len(cells) > 2 else None
                                summary[f"{prefix}_volume"] = _parse_int(cells[3].text_content()) if len(cells) > 3 else None
                                summary[f"{prefix}_value"] = _parse_number(cells[4].text_content()) if len(cells) > 4 else None

        return summary

    except Exception as e:
        logger.error(f"Failed to fetch market summary: {e}")
        return {}


def save_index_data(con: sqlite3.Connection, index_data: dict[str, Any]) -> bool:
    """
    Save index data to database.

    Args:
        con: Database connection
        index_data: Index data dict

    Returns:
        True if saved successfully
    """
    try:
        con.execute("""
            INSERT OR REPLACE INTO psx_indices (
                index_code, index_date, index_time,
                value, change, change_pct,
                open, high, low, volume,
                previous_close,
                ytd_change_pct, one_year_change_pct,
                week_52_low, week_52_high,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            index_data.get("index_code"),
            index_data.get("index_date"),
            index_data.get("index_time"),
            index_data.get("value"),
            index_data.get("change"),
            index_data.get("change_pct"),
            index_data.get("open"),
            index_data.get("high"),
            index_data.get("low"),
            index_data.get("volume"),
            index_data.get("previous_close"),
            index_data.get("ytd_change_pct"),
            index_data.get("one_year_change_pct"),
            index_data.get("week_52_low"),
            index_data.get("week_52_high"),
        ))
        con.commit()
        return True
    except Exception as e:
        logger.error(f"Failed to save index data: {e}")
        return False


def save_market_stats(con: sqlite3.Connection, stats: dict[str, Any]) -> bool:
    """Save market summary stats to database."""
    try:
        con.execute("""
            INSERT OR REPLACE INTO psx_market_stats (
                stat_date, stat_time, board_type,
                reg_trades, reg_volume, reg_value, reg_state,
                fut_trades, fut_volume, fut_value, fut_state,
                csf_trades, csf_volume, csf_value, csf_state,
                odl_trades, odl_volume, odl_value, odl_state,
                squareup_trades, squareup_volume, squareup_value, squareup_state,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            stats.get("stat_date"),
            stats.get("stat_time"),
            stats.get("board_type", "MAIN"),
            stats.get("reg_trades"),
            stats.get("reg_volume"),
            stats.get("reg_value"),
            stats.get("reg_state"),
            stats.get("fut_trades"),
            stats.get("fut_volume"),
            stats.get("fut_value"),
            stats.get("fut_state"),
            stats.get("csf_trades"),
            stats.get("csf_volume"),
            stats.get("csf_value"),
            stats.get("csf_state"),
            stats.get("odl_trades"),
            stats.get("odl_volume"),
            stats.get("odl_value"),
            stats.get("odl_state"),
            stats.get("squareup_trades"),
            stats.get("squareup_volume"),
            stats.get("squareup_value"),
            stats.get("squareup_state"),
        ))
        con.commit()
        return True
    except Exception as e:
        logger.error(f"Failed to save market stats: {e}")
        return False


def get_latest_index(con: sqlite3.Connection, index_code: str = "KSE100") -> dict[str, Any] | None:
    """
    Get the latest index data from database.

    Args:
        con: Database connection
        index_code: Index code (default: KSE100)

    Returns:
        Dict with index data or None
    """
    try:
        result = con.execute("""
            SELECT * FROM psx_indices
            WHERE index_code = ?
            ORDER BY index_date DESC, index_time DESC
            LIMIT 1
        """, (index_code,)).fetchone()

        if result:
            return dict(result)
        return None
    except Exception:
        return None


def get_latest_market_stats(con: sqlite3.Connection) -> dict[str, Any] | None:
    """Get the latest market stats from database."""
    try:
        result = con.execute("""
            SELECT * FROM psx_market_stats
            ORDER BY stat_date DESC, stat_time DESC
            LIMIT 1
        """).fetchone()

        if result:
            return dict(result)
        return None
    except Exception:
        return None


# Manual entry function for when scraping fails
def insert_kse100_manual(
    con: sqlite3.Connection,
    value: float,
    change: float,
    change_pct: float,
    high: float | None = None,
    low: float | None = None,
    volume: int | None = None,
    previous_close: float | None = None,
    ytd_change_pct: float | None = None,
    one_year_change_pct: float | None = None,
    week_52_low: float | None = None,
    week_52_high: float | None = None,
) -> bool:
    """
    Manually insert KSE-100 index data.

    Example from screenshot:
    insert_kse100_manual(
        con,
        value=188202.85,
        change=-384.81,
        change_pct=-0.20,
        high=189521.32,
        low=187538.23,
        volume=341592014,
        previous_close=188587.66,
        ytd_change_pct=8.13,
        one_year_change_pct=65.79,
        week_52_low=101598.91,
        week_52_high=191032.73,
    )
    """
    return save_index_data(con, {
        "index_code": "KSE100",
        "index_date": datetime.now().strftime("%Y-%m-%d"),
        "index_time": datetime.now().strftime("%H:%M:%S"),
        "value": value,
        "change": change,
        "change_pct": change_pct,
        "open": None,
        "high": high,
        "low": low,
        "volume": volume,
        "previous_close": previous_close,
        "ytd_change_pct": ytd_change_pct,
        "one_year_change_pct": one_year_change_pct,
        "week_52_low": week_52_low,
        "week_52_high": week_52_high,
    })
