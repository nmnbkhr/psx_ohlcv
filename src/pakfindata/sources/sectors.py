"""Sector master data fetcher and parser.

Fetches sector codes and names from PSX DPS sector-summary page.
Source: https://dps.psx.com.pk/sector-summary
"""

import sqlite3
from typing import Any

import pandas as pd
import requests
from lxml import html

from ..db import get_sectors, upsert_sectors
from ..models import now_iso

# Constants
SECTOR_SUMMARY_URL = "https://dps.psx.com.pk/sector-summary"


def fetch_sector_summary_html(
    timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 0.5,
) -> str:
    """Fetch HTML from PSX sector-summary page.

    Args:
        timeout: Request timeout in seconds.
        max_retries: Number of retry attempts.
        backoff_factor: Backoff multiplier between retries.

    Returns:
        Raw HTML content as string.

    Raises:
        requests.RequestException: If all retries fail.
    """
    import time

    last_error = None
    for attempt in range(max_retries):
        try:
            response = requests.get(
                SECTOR_SUMMARY_URL,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(backoff_factor * (2**attempt))

    raise last_error  # type: ignore


def parse_sectors_from_sector_summary(html_content: str) -> pd.DataFrame:
    """Parse sector codes and names from sector-summary HTML.

    The sector-summary page displays a table with sector information
    including sector code and sector name.

    Args:
        html_content: Raw HTML from sector-summary page.

    Returns:
        DataFrame with columns: sector_code, sector_name
    """
    tree = html.fromstring(html_content)

    # Strategy 1: Look for table with sector data
    # The page has a table with columns like: Sector, Turnover, Volume, etc.
    tables = tree.xpath("//table")

    sectors = []

    for table in tables:
        # Check if this table has sector-related headers
        headers = table.xpath(".//thead//th/text() | .//tr[1]//th/text()")
        headers = [h.strip().upper() for h in headers if h.strip()]

        # Look for SECTOR column in headers
        if not any("SECTOR" in h for h in headers):
            continue

        # Find sector column index
        sector_idx = None
        for i, h in enumerate(headers):
            if "SECTOR" in h and "CODE" not in h:
                sector_idx = i
                break

        if sector_idx is None:
            continue

        # Extract rows
        body_rows = table.xpath(".//tbody//tr")
        if not body_rows:
            body_rows = table.xpath(".//tr")[1:]  # Skip header row

        for row in body_rows:
            cells = row.xpath(".//td")
            if len(cells) <= sector_idx:
                continue

            # Get cell text - may contain sector code and name
            cell_text = "".join(cells[sector_idx].itertext()).strip()
            if not cell_text:
                continue

            # Try to extract sector code from data attribute or class
            cell_elem = cells[sector_idx]
            sector_code = cell_elem.get("data-sector-code") or cell_elem.get(
                "data-code"
            )

            if sector_code:
                sectors.append(
                    {"sector_code": sector_code.strip(), "sector_name": cell_text}
                )
            else:
                # If no data attribute, the cell might just have the name
                # We'll need to map this later
                sectors.append(
                    {"sector_code": cell_text[:10], "sector_name": cell_text}
                )

    # Strategy 2: Look for sector select/dropdown elements
    if not sectors:
        select_opts = tree.xpath(
            "//select[contains(@name, 'sector') or contains(@id, 'sector')]//option"
        )
        for opt in select_opts:
            value = opt.get("value", "").strip()
            text = "".join(opt.itertext()).strip()
            if value and text and value != "":
                sectors.append({"sector_code": value, "sector_name": text})

    # Strategy 3: Look for links/divs with sector references
    if not sectors:
        sector_links = tree.xpath(
            "//a[contains(@href, 'sector')] | "
            "//div[contains(@class, 'sector-item')]"
        )
        for elem in sector_links:
            href = elem.get("href", "")
            text = "".join(elem.itertext()).strip()

            # Extract sector code from href like /sector/0101
            if "/sector/" in href:
                code = href.split("/sector/")[-1].split("/")[0].split("?")[0]
                if code and text:
                    sectors.append({"sector_code": code, "sector_name": text})

    if not sectors:
        return _empty_sectors_df()

    df = pd.DataFrame(sectors)

    # Clean up
    df["sector_code"] = df["sector_code"].str.strip()
    df["sector_name"] = df["sector_name"].str.strip()

    # Remove empty entries
    df = df[df["sector_code"].str.len() > 0]
    df = df[df["sector_name"].str.len() > 0]

    # Remove duplicates, keep first
    df = df.drop_duplicates(subset=["sector_code"], keep="first")

    # Sort by sector code
    df = df.sort_values("sector_code").reset_index(drop=True)

    return df


def _empty_sectors_df() -> pd.DataFrame:
    """Return empty DataFrame with correct schema."""
    return pd.DataFrame(columns=["sector_code", "sector_name"])


def refresh_sectors(
    con: sqlite3.Connection,
    html_content: str | None = None,
) -> dict[str, Any]:
    """Refresh sectors table from PSX sector-summary page.

    Args:
        con: Database connection.
        html_content: Optional pre-fetched HTML content.
            If None, will fetch from PSX.

    Returns:
        Summary dict with keys:
        - fetched_at: ISO timestamp
        - sectors_found: Number of sectors parsed
        - sectors_upserted: Number of sectors inserted/updated
        - success: Boolean indicating success
        - error: Error message if failed
    """
    result: dict[str, Any] = {
        "fetched_at": now_iso(),
        "sectors_found": 0,
        "sectors_upserted": 0,
        "success": False,
        "error": None,
    }

    try:
        if html_content is None:
            html_content = fetch_sector_summary_html()

        df = parse_sectors_from_sector_summary(html_content)
        result["sectors_found"] = len(df)

        if not df.empty:
            count = upsert_sectors(con, df)
            result["sectors_upserted"] = count

        result["success"] = True

    except requests.RequestException as e:
        result["error"] = f"HTTP error: {e}"
    except Exception as e:
        result["error"] = f"Parse error: {e}"

    return result


def get_sector_list(con: sqlite3.Connection) -> list[dict[str, str]]:
    """Get list of all sectors.

    Args:
        con: Database connection.

    Returns:
        List of dicts with sector_code and sector_name.
    """
    df = get_sectors(con)
    return df.to_dict("records")


def export_sectors_csv(con: sqlite3.Connection, out_path: str) -> int:
    """Export sectors to CSV file.

    Args:
        con: Database connection.
        out_path: Output file path.

    Returns:
        Number of rows exported.
    """
    df = get_sectors(con)
    df.to_csv(out_path, index=False, encoding="utf-8")
    return len(df)
