"""Deep Scraper for PSX Company Pages - Bloomberg-Style Quant Data.

This module extracts ALL available data from PSX company pages including:
- Quote/Trading data (REG, FUT, CSF, ODL)
- Equity structure (market cap, shares, float)
- Company profile (description, key people, etc.)
- Financial statements (annual/quarterly)
- Financial ratios
- Corporate announcements
- Dividend/payout history

Data is stored in a flexible JSON document format for quant analysis.
"""

import hashlib
import re
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

import requests
from lxml import html

from ..models import now_iso

# PSX company page URL template
DPS_COMPANY_URL = "https://dps.psx.com.pk/company/{symbol}"

# Request headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Request timeout
TIMEOUT = 30


def _parse_numeric(value: str | None) -> float | None:
    """Parse numeric value from string, handling commas and parentheses."""
    if not value:
        return None

    value = value.strip()
    if not value or value in ("-", "--", "N/A", "n/a"):
        return None

    # Handle negative in parentheses: (123) -> -123
    is_negative = value.startswith("(") and value.endswith(")")
    if is_negative:
        value = value[1:-1]

    # Remove commas and percentage signs
    value = value.replace(",", "").replace("%", "").replace("Rs.", "").strip()

    try:
        result = float(value)
        return -result if is_negative else result
    except ValueError:
        return None


def _parse_range(value: str | None) -> tuple[float | None, float | None]:
    """Parse a range value like '174.26 — 337.10' into (low, high)."""
    if not value:
        return None, None

    # Split on various dash/range separators
    parts = re.split(r'\s*[—–-]\s*', value)
    if len(parts) == 2:
        return _parse_numeric(parts[0]), _parse_numeric(parts[1])
    return None, None


def _parse_date_str(date_str: str | None) -> str | None:
    """Parse date string to YYYY-MM-DD format."""
    if not date_str:
        return None

    date_str = date_str.strip()
    if not date_str or date_str == "-":
        return None

    # Try common formats
    formats = [
        "%Y-%m-%d",
        "%b %d, %Y",     # Jan 20, 2026
        "%B %d, %Y",     # January 20, 2026
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d %b %Y",
        "%d %B %Y",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def fetch_company_html(symbol: str) -> str:
    """Fetch raw HTML from PSX company page."""
    url = DPS_COMPANY_URL.format(symbol=symbol.upper())
    response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()
    return response.text


def parse_quote_data(tree: html.HtmlElement) -> dict[str, Any]:
    """Extract quote/price data from page."""
    data: dict[str, Any] = {}

    # Company name and sector
    name_elem = tree.xpath('//div[contains(@class, "quote__name")]//text()')
    if name_elem:
        data["company_name"] = " ".join([n.strip() for n in name_elem if n.strip()])

    sector_elem = tree.xpath('//div[contains(@class, "quote__sector")]//text()')
    if sector_elem:
        data["sector_name"] = " ".join([s.strip() for s in sector_elem if s.strip()])

    # Current price and change
    close_elem = tree.xpath('//div[contains(@class, "quote__close")]//text()')
    if close_elem:
        data["close"] = _parse_numeric(close_elem[0])

    change_value = tree.xpath('//div[contains(@class, "change__value")]//text()')
    if change_value:
        data["change_value"] = _parse_numeric(change_value[0])

    change_pct = tree.xpath('//div[contains(@class, "change__percent")]//text()')
    if change_pct:
        pct_str = change_pct[0].strip().replace("(", "").replace(")", "")
        data["change_percent"] = _parse_numeric(pct_str)

    # As-of date
    date_elem = tree.xpath('//div[contains(@class, "quote__date")]//text()')
    if date_elem:
        date_str = date_elem[0].strip()
        # Extract date from "^ As of Thu, Jan 22, 2026 3:49 PM"
        match = re.search(r'(\w+,\s+\w+\s+\d+,\s+\d+)', date_str)
        if match:
            data["as_of_date"] = _parse_date_str(match.group(1))
        data["as_of_raw"] = date_str

    return data


def parse_stats_section(tree: html.HtmlElement) -> dict[str, dict]:
    """Extract all stats (REG, FUT, CSF, ODL) from page."""
    all_stats: dict[str, dict] = {}

    # Find all stats_item divs on the page
    all_items = tree.xpath('//div[contains(@class, "stats_item")]')

    # Group items by their position in the page
    # First ~17 items are REG market data, then FUT, CSF, ODL sections follow
    reg_stats: dict[str, Any] = {}
    fut_stats: dict[str, Any] = {}
    csf_stats: dict[str, Any] = {}
    odl_stats: dict[str, Any] = {}

    # Process first 17 items as REG data (before the "Last update" markers)
    item_index = 0
    for item in all_items:
        label_elems = item.xpath('.//div[contains(@class, "stats_label")]//text()')
        value_elems = item.xpath('.//div[contains(@class, "stats_value")]//text()')

        if not label_elems or not value_elems:
            item_index += 1
            continue

        label_text = label_elems[0].strip().upper()
        value_text = value_elems[0].strip()

        # Determine which market section we're in based on item index
        # Items 0-16: REG, 17-33: FUT, 34-48: CSF, 49+: ODL
        if item_index <= 16:
            stats = reg_stats
        elif item_index <= 33:
            stats = fut_stats
        elif item_index <= 48:
            stats = csf_stats
        else:
            stats = odl_stats

        # Map labels to standardized keys
        if "OPEN" == label_text:
            stats["open"] = _parse_numeric(value_text)
        elif "HIGH" == label_text:
            stats["high"] = _parse_numeric(value_text)
        elif "LOW" == label_text:
            stats["low"] = _parse_numeric(value_text)
        elif "CLOSE" == label_text:
            stats["close"] = _parse_numeric(value_text)
        elif "VOLUME" == label_text:
            stats["volume"] = _parse_numeric(value_text)
        elif "LDCP" == label_text:
            stats["ldcp"] = _parse_numeric(value_text)
        elif "CIRCUIT BREAKER" in label_text:
            low, high = _parse_range(value_text)
            stats["circuit_low"] = low
            stats["circuit_high"] = high
        elif "DAY RANGE" in label_text:
            low, high = _parse_range(value_text)
            stats["day_range_low"] = low
            stats["day_range_high"] = high
        elif "52-WEEK" in label_text or "52 WEEK" in label_text:
            low, high = _parse_range(value_text)
            stats["week_52_low"] = low
            stats["week_52_high"] = high
        elif "ASK PRICE" in label_text:
            stats["ask_price"] = _parse_numeric(value_text)
        elif "ASK VOLUME" in label_text:
            stats["ask_volume"] = _parse_numeric(value_text)
        elif "BID PRICE" in label_text:
            stats["bid_price"] = _parse_numeric(value_text)
        elif "BID VOLUME" in label_text:
            stats["bid_volume"] = _parse_numeric(value_text)
        elif "VAR" == label_text:
            stats["var_percent"] = _parse_numeric(value_text)
        elif "HAIRCUT" in label_text:
            stats["haircut_percent"] = _parse_numeric(value_text)
        elif "P/E" in label_text or "PE RATIO" in label_text:
            stats["pe_ratio_ttm"] = _parse_numeric(value_text)
        elif "1-YEAR" in label_text or "1 YEAR" in label_text:
            stats["year_1_change"] = _parse_numeric(value_text)
        elif "YTD" in label_text:
            stats["ytd_change"] = _parse_numeric(value_text)
        elif "TOTAL TRADES" in label_text:
            stats["total_trades"] = _parse_numeric(value_text)
        elif "CHANGE" == label_text:
            stats["change_percent"] = _parse_numeric(value_text)

        item_index += 1

    # Add non-empty stats to result
    if reg_stats:
        all_stats["REG"] = reg_stats
    if fut_stats:
        all_stats["FUT"] = fut_stats
    if csf_stats:
        all_stats["CSF"] = csf_stats
    if odl_stats:
        all_stats["ODL"] = odl_stats

    return all_stats


def parse_equity_data(tree: html.HtmlElement) -> dict[str, Any]:
    """Extract equity structure data."""
    data: dict[str, Any] = {}

    # Look in companyEquity section
    equity_div = tree.xpath('//div[contains(@class, "companyEquity")]')
    if equity_div:
        text = equity_div[0].text_content()

        # Extract market cap
        match = re.search(r"Market Cap.*?([0-9,]+\.?\d*)", text, re.IGNORECASE)
        if match:
            data["market_cap"] = _parse_numeric(match.group(1))

        # Extract shares
        match = re.search(r"Shares.*?([0-9,]+)", text, re.IGNORECASE)
        if match:
            data["outstanding_shares"] = _parse_numeric(match.group(1))

        # Extract free float
        match = re.search(r"Free Float.*?([0-9,]+)", text, re.IGNORECASE)
        if match:
            data["free_float_shares"] = _parse_numeric(match.group(1))

        # Extract free float percentage
        match = re.search(r"Float.*?([0-9.]+)%", text, re.IGNORECASE)
        if match:
            data["free_float_percent"] = _parse_numeric(match.group(1))

    return data


def parse_profile_data(tree: html.HtmlElement) -> dict[str, Any]:
    """Extract company profile data."""
    data: dict[str, Any] = {}

    profile_div = tree.xpath('//div[@id="profile"]')
    if not profile_div:
        return data

    profile = profile_div[0]

    # Extract profile items
    items = profile.xpath('.//div[contains(@class, "profile__item")]')
    for item in items:
        label = item.xpath('.//span[contains(@class, "profile__label")]//text()')
        value = item.xpath('.//span[contains(@class, "profile__value") or contains(@class, "profile__text")]//text()')

        if label and value:
            label_text = label[0].strip().lower().replace(" ", "_")
            value_text = " ".join([v.strip() for v in value if v.strip()])

            if label_text and value_text:
                data[label_text] = value_text

    # Extract key people
    key_people = []
    people_table = profile.xpath('.//div[contains(@class, "profile__item--people")]//table')
    if people_table:
        rows = people_table[0].xpath('.//tr')
        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) >= 2:
                name = cells[0].text_content().strip()
                role = cells[1].text_content().strip()
                if name and role:
                    key_people.append({"name": name, "role": role})

    if key_people:
        data["key_people"] = key_people

    return data


def parse_financials_data(tree: html.HtmlElement) -> dict[str, Any]:
    """Extract financial statement data."""
    data: dict[str, Any] = {"annual": [], "quarterly": []}

    financials_div = tree.xpath('//div[@id="financials"]')
    if not financials_div:
        return data

    tables = financials_div[0].xpath('.//table[contains(@class, "tbl")]')

    for table in tables:
        # Get headers to identify periods
        headers = table.xpath('.//thead//th//text() | .//tr[1]//th//text() | .//tr[1]//td//text()')
        headers = [h.strip() for h in headers if h.strip()]

        # Identify period columns
        period_cols = []
        for i, h in enumerate(headers):
            if re.match(r"^\d{4}$", h):
                period_cols.append((i, h, "annual"))
            elif re.match(r"^Q[1-4]\s*\d{4}$", h):
                period_cols.append((i, h, "quarterly"))

        if not period_cols:
            continue

        # Initialize period data
        period_data: dict = {}
        for idx, period, ptype in period_cols:
            period_data[period] = {"period_end": period, "period_type": ptype}

        # Extract row data
        rows = table.xpath('.//tbody//tr | .//tr[position()>1]')
        for row in rows:
            cells = row.xpath('.//td//text() | .//th//text()')
            cells = [c.strip() for c in cells if c.strip()]

            if len(cells) < 2:
                continue

            metric = cells[0].upper()

            # Map metric to key
            key = None
            if "SALES" in metric or "REVENUE" in metric:
                key = "sales"
            elif "PROFIT AFTER" in metric or "NET INCOME" in metric or "PAT" in metric:
                key = "profit_after_tax"
            elif "EPS" in metric or "EARNINGS PER" in metric:
                key = "eps"
            elif "GROSS PROFIT" in metric and "MARGIN" not in metric:
                key = "gross_profit"
            elif "OPERATING" in metric and "MARGIN" not in metric:
                key = "operating_profit"

            if key:
                for idx, period, ptype in period_cols:
                    if idx < len(cells):
                        val = _parse_numeric(cells[idx])
                        if val is not None:
                            period_data[period][key] = val

        # Add to appropriate list
        for period, pdata in period_data.items():
            if any(k not in ("period_end", "period_type") for k in pdata):
                if pdata["period_type"] == "annual":
                    data["annual"].append(pdata)
                else:
                    data["quarterly"].append(pdata)

    return data


def parse_ratios_data(tree: html.HtmlElement) -> dict[str, Any]:
    """Extract financial ratios data."""
    data: dict[str, Any] = {"annual": [], "quarterly": []}

    ratios_div = tree.xpath('//div[@id="ratios"]')
    if not ratios_div:
        return data

    tables = ratios_div[0].xpath('.//table[contains(@class, "tbl")]')

    for table in tables:
        headers = table.xpath('.//thead//th//text() | .//tr[1]//th//text() | .//tr[1]//td//text()')
        headers = [h.strip() for h in headers if h.strip()]

        period_cols = []
        for i, h in enumerate(headers):
            if re.match(r"^\d{4}$", h):
                period_cols.append((i, h, "annual"))
            elif re.match(r"^Q[1-4]\s*\d{4}$", h):
                period_cols.append((i, h, "quarterly"))

        if not period_cols:
            continue

        period_data: dict = {}
        for idx, period, ptype in period_cols:
            period_data[period] = {"period_end": period, "period_type": ptype}

        rows = table.xpath('.//tbody//tr | .//tr[position()>1]')
        for row in rows:
            cells = row.xpath('.//td//text() | .//th//text()')
            cells = [c.strip() for c in cells if c.strip()]

            if len(cells) < 2:
                continue

            metric = cells[0].upper()

            key = None
            if "GROSS" in metric and "MARGIN" in metric:
                key = "gross_profit_margin"
            elif "NET" in metric and "MARGIN" in metric:
                key = "net_profit_margin"
            elif "OPERATING" in metric and "MARGIN" in metric:
                key = "operating_margin"
            elif "ROE" in metric or "RETURN ON EQUITY" in metric:
                key = "return_on_equity"
            elif "ROA" in metric or "RETURN ON ASSETS" in metric:
                key = "return_on_assets"
            elif "EPS GROWTH" in metric:
                key = "eps_growth"
            elif "PEG" in metric:
                key = "peg_ratio"

            if key:
                for idx, period, ptype in period_cols:
                    if idx < len(cells):
                        val = _parse_numeric(cells[idx].replace("%", ""))
                        if val is not None:
                            period_data[period][key] = val

        for period, pdata in period_data.items():
            if any(k not in ("period_end", "period_type") for k in pdata):
                if pdata["period_type"] == "annual":
                    data["annual"].append(pdata)
                else:
                    data["quarterly"].append(pdata)

    return data


def parse_announcements_data(tree: html.HtmlElement) -> list[dict]:
    """Extract corporate announcements."""
    announcements = []

    ann_div = tree.xpath('//div[@id="announcements"]')
    if not ann_div:
        return announcements

    tables = ann_div[0].xpath('.//table')

    # Announcement type mapping based on table position
    ann_types = ["financial_result", "board_meeting", "material_info"]

    for i, table in enumerate(tables):
        ann_type = ann_types[i] if i < len(ann_types) else "other"

        rows = table.xpath('.//tbody//tr | .//tr')
        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) >= 2:
                date_text = cells[0].text_content().strip()
                title_text = cells[1].text_content().strip()

                if date_text and title_text:
                    announcements.append({
                        "date": _parse_date_str(date_text),
                        "date_raw": date_text,
                        "title": title_text,
                        "type": ann_type,
                    })

    return announcements


def parse_payouts_data(tree: html.HtmlElement) -> list[dict]:
    """Extract dividend/payout data."""
    payouts = []

    payouts_div = tree.xpath('//div[@id="payouts"]')
    if not payouts_div:
        return payouts

    tables = payouts_div[0].xpath('.//table')

    for table in tables:
        headers = table.xpath('.//thead//th//text()')
        headers = [h.strip().upper() for h in headers if h.strip()]

        if not headers:
            continue

        # Build column map
        col_map = {}
        for i, h in enumerate(headers):
            if "EX" in h and "DATE" in h:
                col_map["ex_date"] = i
            elif "ANNOUNCEMENT" in h:
                col_map["announcement_date"] = i
            elif "BOOK" in h and "FROM" in h:
                col_map["book_closure_from"] = i
            elif "BOOK" in h and "TO" in h:
                col_map["book_closure_to"] = i
            elif "AMOUNT" in h or "DIVIDEND" in h:
                col_map["amount"] = i
            elif "TYPE" in h:
                col_map["payout_type"] = i
            elif "YEAR" in h or "FISCAL" in h:
                col_map["fiscal_year"] = i

        rows = table.xpath('.//tbody//tr | .//tr[position()>1]')
        for row in rows:
            cells = row.xpath('.//td//text()')
            cells = [c.strip() for c in cells]

            if len(cells) < 2:
                continue

            payout = {"payout_type": "cash"}

            for key, idx in col_map.items():
                if idx < len(cells):
                    val = cells[idx]
                    if key == "amount":
                        payout[key] = _parse_numeric(val)
                    elif "date" in key:
                        payout[key] = _parse_date_str(val)
                    else:
                        payout[key] = val if val else None

            if payout.get("ex_date") or payout.get("amount"):
                payouts.append(payout)

    return payouts


def scrape_company_deep(
    symbol: str,
    html_content: str | None = None,
) -> dict[str, Any]:
    """
    Deep scrape all available data from PSX company page.

    Args:
        symbol: Stock symbol
        html_content: Optional pre-fetched HTML

    Returns:
        Comprehensive dict with all extracted data
    """
    symbol = symbol.upper()
    now = now_iso()

    result: dict[str, Any] = {
        "symbol": symbol,
        "scraped_at": now,
        "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
        "snapshot_time": datetime.now().strftime("%H:%M:%S"),
        "source_url": DPS_COMPANY_URL.format(symbol=symbol),
        "success": False,
        "error": None,
    }

    try:
        # Fetch HTML if not provided
        if html_content is None:
            html_content = fetch_company_html(symbol)

        tree = html.fromstring(html_content)

        # Extract all data sections
        quote_data = parse_quote_data(tree)
        result["company_name"] = quote_data.get("company_name")
        result["sector_name"] = quote_data.get("sector_name")

        # Stats for all market types (REG, FUT, CSF, ODL)
        stats_data = parse_stats_section(tree)

        # Build trading data
        result["quote_data"] = quote_data
        result["trading_data"] = stats_data

        # Equity structure
        result["equity_data"] = parse_equity_data(tree)

        # Profile
        result["profile_data"] = parse_profile_data(tree)

        # Financials
        result["financials_data"] = parse_financials_data(tree)

        # Ratios
        result["ratios_data"] = parse_ratios_data(tree)

        # Announcements
        result["announcements_data"] = parse_announcements_data(tree)

        # Payouts
        result["payouts_data"] = parse_payouts_data(tree)

        # Store raw HTML (optional - can be large)
        result["raw_html"] = html_content

        result["success"] = True

    except requests.RequestException as e:
        result["error"] = f"HTTP error: {e}"
    except Exception as e:
        result["error"] = f"Parse error: {e}"

    return result


def save_company_snapshot(
    con: sqlite3.Connection,
    data: dict[str, Any],
    save_raw_html: bool = False,
) -> dict:
    """
    Save scraped company data to database.

    Args:
        con: Database connection
        data: Scraped data from scrape_company_deep()
        save_raw_html: Whether to store raw HTML

    Returns:
        Status dict
    """
    from ..db import (
        upsert_company_snapshot,
        upsert_corporate_announcement,
        upsert_equity_structure,
        upsert_trading_session,
    )

    symbol = data["symbol"]
    snapshot_date = data["snapshot_date"]

    result = {
        "symbol": symbol,
        "snapshot_saved": False,
        "trading_sessions_saved": 0,
        "announcements_saved": 0,
        "equity_saved": False,
    }

    # Save main snapshot
    raw_html = data.get("raw_html") if save_raw_html else None
    snapshot_result = upsert_company_snapshot(
        con, symbol, snapshot_date, data, raw_html
    )
    result["snapshot_saved"] = snapshot_result.get("status") == "ok"

    # Save trading sessions for each market type
    trading_data = data.get("trading_data", {})
    for market_type, stats in trading_data.items():
        if stats:
            stats["contract_month"] = stats.get("contract_month", "")
            upsert_trading_session(con, symbol, snapshot_date, market_type, stats)
            result["trading_sessions_saved"] += 1

    # Save announcements
    announcements = data.get("announcements_data", [])
    for ann in announcements:
        if ann.get("date") and ann.get("title"):
            upsert_corporate_announcement(
                con,
                symbol,
                ann["date"],
                ann.get("type", "other"),
                ann["title"],
                ann,
            )
            result["announcements_saved"] += 1

    # Save equity structure
    equity_data = data.get("equity_data", {})
    if equity_data:
        upsert_equity_structure(con, symbol, snapshot_date, equity_data)
        result["equity_saved"] = True

    return result


def deep_scrape_symbol(
    con: sqlite3.Connection,
    symbol: str,
    save_raw_html: bool = False,
) -> dict:
    """
    Full deep scrape and save for a single symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        save_raw_html: Whether to store raw HTML

    Returns:
        Combined result dict
    """
    # Scrape
    data = scrape_company_deep(symbol)

    if not data["success"]:
        return {
            "symbol": symbol,
            "success": False,
            "error": data.get("error"),
        }

    # Save
    save_result = save_company_snapshot(con, data, save_raw_html)

    return {
        "symbol": symbol,
        "success": True,
        **save_result,
    }


def deep_scrape_batch(
    con: sqlite3.Connection,
    symbols: list[str],
    delay: float = 1.0,
    save_raw_html: bool = False,
    progress_callback: Callable | None = None,
) -> dict:
    """
    Deep scrape multiple symbols with rate limiting.

    Args:
        con: Database connection
        symbols: List of symbols to scrape
        delay: Delay between requests (seconds)
        save_raw_html: Whether to store raw HTML
        progress_callback: Optional callback(current, total, symbol, result)

    Returns:
        Summary dict with results
    """
    from ..db import create_scrape_job, update_scrape_job

    # Create job
    job_id = create_scrape_job(con, "company_snapshot", {
        "symbols": symbols,
        "save_raw_html": save_raw_html,
    })

    update_scrape_job(con, job_id, symbols_requested=len(symbols))

    summary = {
        "job_id": job_id,
        "total": len(symbols),
        "completed": 0,
        "failed": 0,
        "results": [],
        "errors": [],
    }

    for i, symbol in enumerate(symbols):
        try:
            result = deep_scrape_symbol(con, symbol, save_raw_html)
            summary["results"].append(result)

            if result.get("success"):
                summary["completed"] += 1
            else:
                summary["failed"] += 1
                summary["errors"].append({
                    "symbol": symbol,
                    "error": result.get("error"),
                })

            # Update job progress
            update_scrape_job(
                con, job_id,
                symbols_completed=summary["completed"],
                symbols_failed=summary["failed"],
            )

            # Callback
            if progress_callback:
                progress_callback(i + 1, len(symbols), symbol, result)

        except Exception as e:
            summary["failed"] += 1
            summary["errors"].append({"symbol": symbol, "error": str(e)})

        # Rate limiting
        if i < len(symbols) - 1:
            time.sleep(delay)

    # Complete job
    update_scrape_job(
        con, job_id,
        status="completed",
        errors=summary["errors"] if summary["errors"] else None,
    )

    return summary
