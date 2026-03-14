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

    # Try common formats (including datetime formats with time)
    formats = [
        "%Y-%m-%d",
        "%b %d, %Y",           # Jan 20, 2026
        "%B %d, %Y",           # January 20, 2026
        "%B %d, %Y %I:%M %p",  # October 23, 2025 3:48 PM
        "%B %d, %Y %H:%M",     # October 23, 2025 15:48
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
    
    max_retries = 3
    backoff_factor = 1.0
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff_factor * (2 ** attempt))
    
    raise requests.RequestException(f"Failed to fetch {symbol} after {max_retries} attempts")


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

        # Extract market cap — value is in 000's (thousands)
        # Regex skips "(000's)" label to avoid capturing "000" from it
        match = re.search(r"Market Cap\s*\([^)]*\)\s*([0-9,]+\.?\d*)", text, re.IGNORECASE)
        if match:
            raw = _parse_numeric(match.group(1))
            if raw is not None:
                data["market_cap"] = raw * 1000  # PSX reports in 000's

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
        # Extract headers PER-ELEMENT (not per-text-node) to preserve column positions.
        # The first <th> is the metric-label column (empty text) — must keep it so
        # period indices align with data-row <td> positions.
        header_elems = table.xpath('.//thead/tr/th')
        if not header_elems:
            header_elems = table.xpath('.//tr[1]/th | .//tr[1]/td')
        headers = [elem.text_content().strip() for elem in header_elems]

        # Identify period columns (indices now match <td> positions in data rows)
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

        # Extract row data — per-element to stay aligned with header positions
        rows = table.xpath('.//tbody//tr | .//tr[position()>1]')
        for row in rows:
            cell_elems = row.xpath('./td | ./th')
            cells = [c.text_content().strip() for c in cell_elems]

            if len(cells) < 2:
                continue

            metric = cells[0].upper()

            # Map metric to key
            # Non-banks: Sales/Revenue → sales, Gross Profit → gross_profit
            # Banks: Total Income → sales, Mark-up Earned → markup_earned
            # Bank GM = (markup_earned - markup_expensed) / markup_earned
            # markup_expensed comes from PDF financial reports (not on PSX DPS page)
            key = None
            if "TOTAL INCOME" in metric:
                key = "sales"  # Banks: net revenue (the operating top-line)
            elif "MARK-UP EARNED" in metric or "MARKUP EARNED" in metric or "INTEREST EARNED" in metric:
                key = "markup_earned"  # Banks: interest/markup income (top-line)
            elif "SALES" in metric or "REVENUE" in metric:
                key = "sales"  # Non-banks: top-line revenue
            elif "PROFIT AFTER" in metric or "NET INCOME" in metric or "PAT" in metric:
                key = "profit_after_tax"
            elif "EPS" in metric or "EARNINGS PER" in metric:
                key = "eps"
            elif "GROSS PROFIT" in metric and "MARGIN" not in metric:
                key = "gross_profit"  # Non-banks: sales minus COGS
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
        # Extract headers per-element to preserve column positions (same fix as financials)
        header_elems = table.xpath('.//thead/tr/th')
        if not header_elems:
            header_elems = table.xpath('.//tr[1]/th | .//tr[1]/td')
        headers = [elem.text_content().strip() for elem in header_elems]

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
            cell_elems = row.xpath('./td | ./th')
            cells = [c.text_content().strip() for c in cell_elems]

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
    """Extract dividend/payout data.

    PSX company page payout structure:
    - Headers: Date, Financial Results, Details, Book Closure
    - Date: Announcement date (e.g., "October 23, 2025 3:48 PM")
    - Financial Results: Fiscal period (e.g., "30/09/2025(IIIQ)")
    - Details: Payout % and type (e.g., "50%(iii) (D)")
    - Book Closure: Date range (e.g., "04/11/2025 - 05/11/2025")
    """
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

        # Build column map - support multiple PSX formats
        col_map = {}
        for i, h in enumerate(headers):
            # Standard format columns
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
            elif h == "TYPE":
                col_map["payout_type"] = i
            elif "YEAR" in h or "FISCAL" in h:
                col_map["fiscal_year"] = i
            # PSX company page format: Date, Financial Results, Details, Book Closure
            elif h == "DATE":
                col_map["announcement_date"] = i
            elif "FINANCIAL" in h and "RESULT" in h:
                col_map["fiscal_period"] = i
            elif "DETAIL" in h:
                col_map["details"] = i
            elif h == "BOOK CLOSURE":
                col_map["book_closure"] = i

        rows = table.xpath('.//tbody//tr | .//tr[position()>1]')
        for row in rows:
            cells = row.xpath('.//td//text()')
            cells = [c.strip() for c in cells]

            if len(cells) < 2:
                continue

            payout = {"payout_type": "cash"}

            # Handle standard format
            for key, idx in col_map.items():
                if idx < len(cells):
                    val = cells[idx]
                    if key == "amount":
                        payout[key] = _parse_numeric(val)
                    elif "date" in key and key != "fiscal_period":
                        payout[key] = _parse_date_str(val)
                    elif key == "fiscal_period":
                        payout["fiscal_year"] = val
                    elif key == "details":
                        # Parse "50%(iii) (D)" format
                        payout["details_raw"] = val
                        # Extract percentage
                        pct_match = re.search(r"([\d.]+)%", val)
                        if pct_match:
                            payout["amount"] = float(pct_match.group(1))
                        # Determine type: (D) = dividend/cash, (B) = bonus
                        if "(D)" in val.upper():
                            payout["payout_type"] = "cash"
                        elif "(B)" in val.upper():
                            payout["payout_type"] = "bonus"
                        elif "(R)" in val.upper():
                            payout["payout_type"] = "right"
                    elif key == "book_closure":
                        # Parse "04/11/2025 - 05/11/2025" format
                        parts = re.split(r'\s*-\s*', val)
                        if len(parts) >= 1:
                            payout["book_closure_from"] = _parse_date_str(parts[0])
                        if len(parts) >= 2:
                            payout["book_closure_to"] = _parse_date_str(parts[1])
                    else:
                        payout[key] = val if val else None

            # Derive ex_date if not explicitly provided
            # PSX company page shows announcement_date, not ex_date
            # Use book_closure_from as proxy (ex-date is typically 1-2 days before)
            if not payout.get("ex_date"):
                if payout.get("book_closure_from"):
                    payout["ex_date"] = payout["book_closure_from"]
                elif payout.get("announcement_date"):
                    payout["ex_date"] = payout["announcement_date"]

            # Only add if we have meaningful data (must have ex_date for DB key)
            if payout.get("ex_date") and (payout.get("amount") or payout.get("announcement_date")):
                payouts.append(payout)

    return payouts


def check_symbol_filings(
    symbol: str,
    con: "sqlite3.Connection | None" = None,
) -> dict[str, Any]:
    """
    Probe a PSX company page and report what financial data is available
    for scraping, plus what's already stored in the DB.

    Returns a dict with sections: quote, profile, financials, ratios,
    announcements, payouts, equity — each with availability & counts.
    """
    symbol = symbol.upper()
    report: dict[str, Any] = {
        "symbol": symbol,
        "url": DPS_COMPANY_URL.format(symbol=symbol),
        "success": False,
        "error": None,
        "page": {},   # what's on the PSX page
        "db": {},     # what's already in our DB
    }

    # --- Probe the PSX page ---
    try:
        html_content = fetch_company_html(symbol)
        tree = html.fromstring(html_content)
    except Exception as e:
        report["error"] = str(e)
        return report

    # Quote / header
    quote = parse_quote_data(tree)
    report["page"]["company_name"] = quote.get("company_name", "N/A")
    report["page"]["sector"] = quote.get("sector_name", "N/A")
    report["page"]["price"] = quote.get("close")
    report["page"]["change_pct"] = quote.get("change_percent")

    # Trading stats (market types)
    stats = parse_stats_section(tree)
    report["page"]["market_types"] = list(stats.keys()) if stats else []

    # Equity
    equity = parse_equity_data(tree)
    report["page"]["equity"] = {
        "available": bool(equity),
        "market_cap": equity.get("market_cap"),
        "outstanding_shares": equity.get("outstanding_shares"),
        "free_float_pct": equity.get("free_float_percent"),
    }

    # Profile
    profile = parse_profile_data(tree)
    report["page"]["profile"] = {
        "available": bool(profile),
        "has_description": bool(profile.get("business_description")),
        "key_people_count": len(profile.get("key_people", [])),
    }

    # Financials
    fins = parse_financials_data(tree)
    ann_fins = fins.get("annual", [])
    qtr_fins = fins.get("quarterly", [])
    fin_metrics = set()
    for row in ann_fins + qtr_fins:
        fin_metrics.update(k for k in row if k not in ("period_end", "period_type"))
    report["page"]["financials"] = {
        "available": bool(ann_fins or qtr_fins),
        "annual_periods": sorted([r["period_end"] for r in ann_fins]),
        "quarterly_periods": sorted([r["period_end"] for r in qtr_fins]),
        "metrics": sorted(fin_metrics),
    }

    # Ratios
    rats = parse_ratios_data(tree)
    ann_rats = rats.get("annual", [])
    qtr_rats = rats.get("quarterly", [])
    rat_metrics = set()
    for row in ann_rats + qtr_rats:
        rat_metrics.update(k for k in row if k not in ("period_end", "period_type"))
    report["page"]["ratios"] = {
        "available": bool(ann_rats or qtr_rats),
        "annual_periods": sorted([r["period_end"] for r in ann_rats]),
        "quarterly_periods": sorted([r["period_end"] for r in qtr_rats]),
        "metrics": sorted(rat_metrics),
    }

    # Announcements
    anns = parse_announcements_data(tree)
    ann_types: dict[str, int] = {}
    for a in anns:
        t = a.get("type", "other")
        ann_types[t] = ann_types.get(t, 0) + 1
    report["page"]["announcements"] = {
        "available": bool(anns),
        "total": len(anns),
        "by_type": ann_types,
    }

    # Payouts
    pays = parse_payouts_data(tree)
    pay_types: dict[str, int] = {}
    for p in pays:
        t = p.get("payout_type", "unknown")
        pay_types[t] = pay_types.get(t, 0) + 1
    report["page"]["payouts"] = {
        "available": bool(pays),
        "total": len(pays),
        "by_type": pay_types,
        "fiscal_years": sorted({p.get("fiscal_year", "?") for p in pays if p.get("fiscal_year")}),
    }

    report["success"] = True

    # --- Check what's already in the DB ---
    if con is not None:
        try:
            row = con.execute(
                "SELECT COUNT(*) as cnt FROM company_financials WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            db_fins = row["cnt"] if row else 0

            row = con.execute(
                "SELECT COUNT(*) as cnt FROM company_ratios WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            db_rats = row["cnt"] if row else 0

            row = con.execute(
                "SELECT COUNT(*) as cnt FROM company_payouts WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            db_pays = row["cnt"] if row else 0

            row = con.execute(
                "SELECT COUNT(*) as cnt FROM corporate_announcements WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            db_anns = row["cnt"] if row else 0

            row = con.execute(
                "SELECT COUNT(*) as cnt FROM company_profile WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            db_profile = row["cnt"] if row else 0

            row = con.execute(
                "SELECT COUNT(*) as cnt FROM equity_structure WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            db_equity = row["cnt"] if row else 0

            # Financial periods already stored
            db_fin_periods = [
                r["period_end"]
                for r in con.execute(
                    "SELECT DISTINCT period_end FROM company_financials WHERE symbol = ? ORDER BY period_end",
                    (symbol,),
                ).fetchall()
            ]

            db_rat_periods = [
                r["period_end"]
                for r in con.execute(
                    "SELECT DISTINCT period_end FROM company_ratios WHERE symbol = ? ORDER BY period_end",
                    (symbol,),
                ).fetchall()
            ]

            report["db"] = {
                "financials_rows": db_fins,
                "financials_periods": db_fin_periods,
                "ratios_rows": db_rats,
                "ratios_periods": db_rat_periods,
                "payouts_rows": db_pays,
                "announcements_rows": db_anns,
                "profile_exists": db_profile > 0,
                "equity_snapshots": db_equity,
            }
        except Exception:
            report["db"] = {"error": "could not query DB"}

    return report


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
        get_last_quote_hash,
        insert_quote_snapshot,
        upsert_company_financials,
        upsert_company_payouts,
        upsert_company_ratios,
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
        "quote_snapshot_saved": False,
        "trading_sessions_saved": 0,
        "announcements_saved": 0,
        "equity_saved": False,
        "payouts_saved": 0,
    }

    # Save main snapshot
    raw_html = data.get("raw_html") if save_raw_html else None
    snapshot_result = upsert_company_snapshot(
        con, symbol, snapshot_date, data, raw_html
    )
    result["snapshot_saved"] = snapshot_result.get("status") == "ok"

    # Save quote snapshot (for charts)
    quote_data = data.get("quote_data", {})
    trading_data = data.get("trading_data", {})
    reg_data = trading_data.get("REG", {})

    if quote_data.get("close"):
        import hashlib
        import json as _json
        ts = data.get("ingested_at", snapshot_date)
        quote_record = {
            "price": quote_data.get("close"),
            "change": quote_data.get("change"),
            "change_pct": quote_data.get("change_pct"),
            "open": reg_data.get("open"),
            "high": reg_data.get("high"),
            "low": reg_data.get("low"),
            "volume": reg_data.get("volume"),
            "as_of": quote_data.get("as_of"),
            "raw_hash": hashlib.md5(
                _json.dumps(quote_data, sort_keys=True, default=str).encode()
            ).hexdigest(),
        }
        # Only insert if data changed since last snapshot
        last_hash = get_last_quote_hash(con, symbol)
        if last_hash != quote_record["raw_hash"]:
            inserted = insert_quote_snapshot(con, symbol, ts, quote_record)
            result["quote_snapshot_saved"] = inserted

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

    # Save payouts (dividend/bonus history)
    payouts_data = data.get("payouts_data", [])
    if payouts_data:
        payouts_saved = upsert_company_payouts(con, symbol, payouts_data)
        result["payouts_saved"] = payouts_saved

    # Save financials (annual + quarterly)
    financials_data = data.get("financials_data", {})
    all_financials = financials_data.get("annual", []) + financials_data.get("quarterly", [])
    if all_financials:
        financials_saved = upsert_company_financials(con, symbol, all_financials)
        result["financials_saved"] = financials_saved

    # Save ratios (annual + quarterly)
    ratios_data = data.get("ratios_data", {})
    all_ratios = ratios_data.get("annual", []) + ratios_data.get("quarterly", [])
    if all_ratios:
        ratios_saved = upsert_company_ratios(con, symbol, all_ratios)
        result["ratios_saved"] = ratios_saved

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


# =============================================================================
# Background Deep Scrape (thread-based, progress via JSON file)
# =============================================================================

import json
import logging
import threading
from pathlib import Path

from ..config import DATA_ROOT

_log = logging.getLogger("pakfindata.deep_scraper")

DEEP_SCRAPE_PROGRESS_FILE = DATA_ROOT / "deep_scrape_progress.json"

_deep_scrape_thread: threading.Thread | None = None
_deep_scrape_stop = threading.Event()


def _write_deep_scrape_progress(data: dict) -> None:
    """Write progress dict to JSON file atomically."""
    tmp = DEEP_SCRAPE_PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(DEEP_SCRAPE_PROGRESS_FILE)


def read_deep_scrape_progress() -> dict | None:
    """Read the current deep scrape progress. Returns None if no job has run."""
    if not DEEP_SCRAPE_PROGRESS_FILE.exists():
        return None
    try:
        return json.loads(DEEP_SCRAPE_PROGRESS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _run_deep_scrape_background(
    symbols: list[str],
    delay: float = 1.0,
    save_raw_html: bool = False,
    db_path: str | Path | None = None,
) -> None:
    """Worker function that runs in a background thread."""
    from ..db.connection import connect, init_schema
    from ..db.repositories.symbols import get_scrapable_symbols, normalize_symbol

    con = connect(db_path)
    init_schema(con)

    # Normalize symbols — strip PSX status suffixes (XD, XB, NC …)
    # and deduplicate using the master symbol list as reference.
    master_rows = con.execute(
        "SELECT symbol FROM symbols WHERE is_active = 1"
    ).fetchall()
    master_set = {r["symbol"] for r in master_rows}

    seen: set[str] = set()
    clean_symbols: list[str] = []
    for sym in symbols:
        base, _ = normalize_symbol(sym, master_set)
        if base not in seen:
            seen.add(base)
            clean_symbols.append(base)

    symbols = clean_symbols
    total = len(symbols)
    progress = {
        "status": "running",
        "total": total,
        "current": 0,
        "ok": 0,
        "failed": 0,
        "current_symbol": "",
        "errors": [],
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    }
    _write_deep_scrape_progress(progress)

    for i, symbol in enumerate(symbols):
        if _deep_scrape_stop.is_set():
            progress["status"] = "stopped"
            progress["finished_at"] = datetime.now().isoformat()
            progress["current_symbol"] = ""
            _write_deep_scrape_progress(progress)
            _log.info("Deep scrape stopped by user at %d/%d", i, total)
            break

        progress["current"] = i + 1
        progress["current_symbol"] = symbol
        _write_deep_scrape_progress(progress)

        try:
            result = deep_scrape_symbol(con, symbol, save_raw_html)
            if result.get("success"):
                progress["ok"] += 1
            else:
                progress["failed"] += 1
                progress["errors"].append(
                    f"{symbol}: {result.get('error', 'unknown')}"
                )
        except Exception as e:
            progress["failed"] += 1
            progress["errors"].append(f"{symbol}: {e}")

        _write_deep_scrape_progress(progress)

        if i < total - 1:
            time.sleep(delay)
    else:
        # Loop completed without break (not stopped)
        progress["status"] = "completed"
        progress["finished_at"] = datetime.now().isoformat()
        progress["current_symbol"] = ""
        _write_deep_scrape_progress(progress)

    try:
        con.close()
    except Exception:
        pass

    _log.info(
        "Deep scrape complete: %d/%d OK, %d failed",
        progress["ok"], total, progress["failed"],
    )


def start_deep_scrape_background(
    symbols: list[str],
    delay: float = 1.0,
    save_raw_html: bool = False,
    db_path: str | Path | None = None,
) -> bool:
    """Launch deep scrape in a background thread.

    Returns True if started, False if already running.
    """
    global _deep_scrape_thread
    if _deep_scrape_thread is not None and _deep_scrape_thread.is_alive():
        return False

    _deep_scrape_stop.clear()

    _deep_scrape_thread = threading.Thread(
        target=_run_deep_scrape_background,
        kwargs={
            "symbols": symbols,
            "delay": delay,
            "save_raw_html": save_raw_html,
            "db_path": db_path,
        },
        daemon=True,
        name="deep-scrape-batch",
    )
    _deep_scrape_thread.start()
    return True


def stop_deep_scrape() -> bool:
    """Signal the running deep scrape to stop gracefully.

    The thread will finish the current symbol then exit.
    Returns True if a running scrape was signalled, False if nothing was running.
    """
    if _deep_scrape_thread is None or not _deep_scrape_thread.is_alive():
        return False
    _deep_scrape_stop.set()
    return True


def is_deep_scrape_running() -> bool:
    """Check if a deep scrape thread is currently running."""
    return _deep_scrape_thread is not None and _deep_scrape_thread.is_alive()


# =============================================================================
# PSX Financial Announcements Scraper (Dividends from www.psx.com.pk)
# =============================================================================

PSX_FINANCIAL_ANNOUNCEMENTS_URL = "https://www.psx.com.pk/psx/announcement/financial-announcements"


def fetch_psx_financial_announcements(timeout: int = 60) -> str:
    """Fetch HTML from PSX financial announcements page.

    Args:
        timeout: Request timeout in seconds (default: 60, PSX can be slow)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    response = requests.get(
        PSX_FINANCIAL_ANNOUNCEMENTS_URL,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def parse_financial_announcements(html_content: str) -> list[dict]:
    """Parse ALL financial announcement data from PSX financial announcements page.

    The page has a table with columns:
    - Column 0: Company name
    - Column 1: Date/timestamp
    - Column 2: Fiscal period (e.g., 31/12/2025(YR), 30/06/2025(HYR))
    - Column 3: Dividend/Bonus/Right (e.g., "83%(i) (D)")
    - Column 4: Profit Before Tax (millions Rs.)
    - Column 5: Profit After Tax (millions Rs.)
    - Column 6: EPS (Earnings Per Share)
    - Column 7: AGM/EOGM Date
    - Column 8: Book Closure dates (e.g., "19/03/2025 - 26/03/2025")

    Returns:
        List of dicts with ALL financial announcement data.
    """
    tree = html.fromstring(html_content)
    announcements = []

    # Find the main table
    tables = tree.xpath('//table')

    for table in tables:
        rows = table.xpath('.//tbody//tr | .//tr[position()>1]')

        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) < 5:
                continue

            # Extract text from cells
            cell_texts = []
            for cell in cells:
                text = cell.text_content().strip()
                cell_texts.append(text)

            if len(cell_texts) < 9:
                continue

            # Parse the row data
            company_name = cell_texts[0] if len(cell_texts) > 0 else ""
            ann_date = cell_texts[1] if len(cell_texts) > 1 else ""
            fiscal_period = cell_texts[2] if len(cell_texts) > 2 else ""
            dividend_str = cell_texts[3] if len(cell_texts) > 3 else ""
            profit_before_str = cell_texts[4] if len(cell_texts) > 4 else ""
            profit_after_str = cell_texts[5] if len(cell_texts) > 5 else ""
            eps_str = cell_texts[6] if len(cell_texts) > 6 else ""
            agm_date_str = cell_texts[7] if len(cell_texts) > 7 else ""
            book_closure = cell_texts[8] if len(cell_texts) > 8 else ""

            # Parse announcement date
            announcement_date = _parse_date_str(ann_date.split()[0] if ann_date else "")

            # Skip rows without valid data
            if not announcement_date and not fiscal_period:
                continue

            # Parse dividend string (e.g., "83%(i) (D)" or "130%(F) (D)")
            payout_type = None
            dividend_amount = None

            if dividend_str and dividend_str not in ("-", "—", ""):
                # Extract percentage
                pct_match = re.search(r"([\d.]+)%", dividend_str)
                if pct_match:
                    dividend_amount = float(pct_match.group(1))

                # Determine type
                if "(D)" in dividend_str.upper():
                    payout_type = "cash"
                elif "(B)" in dividend_str.upper():
                    payout_type = "bonus"
                elif "(R)" in dividend_str.upper():
                    payout_type = "right"

            # Parse profit values (remove commas and parse as float)
            profit_before_tax = _parse_numeric(profit_before_str)
            profit_after_tax = _parse_numeric(profit_after_str)
            eps = _parse_numeric(eps_str)

            # Parse AGM date
            agm_date = _parse_date_str(agm_date_str) if agm_date_str and agm_date_str not in ("-", "—") else None

            # Parse book closure dates
            book_from, book_to = None, None
            if book_closure and book_closure not in ("-", "—", ""):
                parts = re.split(r'\s*-\s*', book_closure)
                if len(parts) >= 1:
                    book_from = _parse_date_str(parts[0])
                if len(parts) >= 2:
                    book_to = _parse_date_str(parts[1])

            announcement = {
                "company_name": company_name,
                "announcement_date": announcement_date,
                "fiscal_period": fiscal_period,
                "fiscal_year": fiscal_period,  # For backward compatibility with payouts
                # Financial results
                "profit_before_tax": profit_before_tax,
                "profit_after_tax": profit_after_tax,
                "eps": eps,
                # Dividend info
                "dividend_payout": dividend_str if dividend_str not in ("-", "—", "") else None,
                "dividend_amount": dividend_amount,
                "amount": dividend_amount,  # For backward compatibility with payouts
                "details_raw": dividend_str,  # For backward compatibility
                "payout_type": payout_type,
                # Corporate events
                "agm_date": agm_date,
                "book_closure_from": book_from,
                "book_closure_to": book_to,
                # Derive ex_date from book_closure_from for payout compatibility
                "ex_date": book_from if book_from else announcement_date,
            }

            announcements.append(announcement)

    return announcements


def scrape_psx_financial_announcements(
    con: sqlite3.Connection,
    symbol_map: dict[str, str] | None = None,
) -> dict:
    """Scrape ALL financial announcement data from PSX financial announcements page.

    Saves data to BOTH tables:
    - company_payouts: Dividend/bonus/rights payout history (backward compatible)
    - financial_announcements: Full financial results with EPS, profit, AGM, etc.

    Args:
        con: Database connection.
        symbol_map: Optional mapping of company names to symbols.
                   If not provided, will try to match using symbols table.

    Returns:
        Summary dict with results.
    """
    from ..db import upsert_company_payouts, upsert_financial_announcements

    result = {
        "success": False,
        "total_announcements": 0,
        "payouts_saved": 0,
        "financial_announcements_saved": 0,
        "companies_without_symbol": [],
        "error": None,
    }

    try:
        # Fetch the page
        html_content = fetch_psx_financial_announcements()

        # Parse announcements
        announcements = parse_financial_announcements(html_content)
        result["total_announcements"] = len(announcements)

        if not announcements:
            result["success"] = True
            return result

        # Build symbol map if not provided
        if symbol_map is None:
            symbol_map = {}
            cur = con.execute("SELECT symbol, name FROM symbols")
            for row in cur.fetchall():
                # Map both symbol and company name
                symbol_map[row[0].upper()] = row[0].upper()
                if row[1]:
                    # Normalize company name for matching
                    normalized = row[1].upper().replace("LIMITED", "").strip()
                    symbol_map[normalized] = row[0].upper()

        # Group announcements by symbol
        announcements_by_symbol: dict[str, list] = {}

        for ann in announcements:
            company = ann["company_name"]
            symbol = None

            # Try to find symbol
            if company:
                normalized = company.upper().replace("LIMITED", "").strip()
                symbol = symbol_map.get(normalized)

                # Also try exact match
                if not symbol:
                    for name, sym in symbol_map.items():
                        if normalized in name or name in normalized:
                            symbol = sym
                            break

            if symbol:
                if symbol not in announcements_by_symbol:
                    announcements_by_symbol[symbol] = []
                announcements_by_symbol[symbol].append(ann)
            else:
                result["companies_without_symbol"].append(company)

        # Save to BOTH tables for each symbol
        for symbol, symbol_announcements in announcements_by_symbol.items():
            # Save to company_payouts (backward compatible - dividends only)
            count = upsert_company_payouts(con, symbol, symbol_announcements)
            result["payouts_saved"] += count

            # Save to financial_announcements (full data)
            fin_count = upsert_financial_announcements(con, symbol, symbol_announcements)
            result["financial_announcements_saved"] += fin_count

        result["success"] = True

    except requests.RequestException as e:
        result["error"] = f"HTTP error: {e}"
    except Exception as e:
        result["error"] = f"Parse error: {e}"

    return result
