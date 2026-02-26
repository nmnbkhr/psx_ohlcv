"""Company Page Parser - Fetch and parse DPS company analytics pages.

Source: https://dps.psx.com.pk/company/{SYMBOL}

Extracts:
- Quote snapshots (price, change, OHLCV, ranges, circuit breakers)
- Company profile (description, address, website, auditor, registrar)
- Key people (CEO, Chairman, Secretary, etc.)
"""

import hashlib
import re
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests
from lxml import html

from ..models import now_iso

# Constants
DPS_COMPANY_URL = "https://dps.psx.com.pk/company/{symbol}"
PKT_TZ = ZoneInfo("Asia/Karachi")

# HTTP settings
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.5
REQUEST_DELAY = 0.3  # Polite delay between requests


def _get_pkt_timestamp() -> str:
    """Get current timestamp in Asia/Karachi timezone as ISO string."""
    return datetime.now(PKT_TZ).isoformat()


def _parse_numeric(value: str | None) -> float | None:
    """Parse a numeric string, returning None if invalid."""
    if not value:
        return None
    # Remove commas, percentage signs, parentheses
    cleaned = re.sub(r"[,\s%()]", "", str(value).strip())
    if not cleaned or cleaned == "-" or cleaned == "—":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_range(range_str: str | None) -> tuple[float | None, float | None]:
    """Parse a range string like '330.00 — 336.60' into (low, high)."""
    if not range_str:
        return None, None
    # Split by em-dash, en-dash, or hyphen with spaces
    parts = re.split(r"\s*[—–-]\s*", range_str.strip())
    if len(parts) >= 2:
        return _parse_numeric(parts[0]), _parse_numeric(parts[1])
    return None, None


def _compute_raw_hash(quote: dict) -> str:
    """Compute SHA256 hash of quote data for deduplication."""
    # Use stable fields for hashing
    hash_fields = [
        str(quote.get("price", "")),
        str(quote.get("change", "")),
        str(quote.get("change_pct", "")),
        str(quote.get("open", "")),
        str(quote.get("high", "")),
        str(quote.get("low", "")),
        str(quote.get("volume", "")),
        str(quote.get("as_of", "")),
    ]
    hash_str = "|".join(hash_fields)
    return hashlib.sha256(hash_str.encode()).hexdigest()[:16]


def fetch_company_page_html(
    symbol: str,
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = MAX_RETRIES,
    backoff_factor: float = BACKOFF_FACTOR,
) -> str:
    """Fetch HTML from DPS company page.

    Args:
        symbol: Stock symbol (e.g., OGDC, HBL)
        timeout: Request timeout in seconds
        max_retries: Number of retry attempts
        backoff_factor: Backoff multiplier between retries

    Returns:
        Raw HTML content as string

    Raises:
        requests.RequestException: If all retries fail
    """
    url = DPS_COMPANY_URL.format(symbol=symbol.upper())
    last_error = None

    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
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


def _extract_stats_map(tree) -> dict[str, str]:
    """Extract label -> value map from DPS stats divs.

    DPS uses this pattern inside the REG tab panel:
    <div class="tabs__panel" data-name="REG">
        <div class="stats_item">
            <div class="stats_label">OPEN</div>
            <div class="stats_value">334.10</div>
        </div>
    </div>

    Note: Only extract from the REG panel to avoid overwriting
    with zeros from other tabs (FUT, CSF, ODL).
    """
    stats_map: dict[str, str] = {}
    stats_items = []

    # Find REG tab panels - may have multiple (mobile/desktop), use one with stats
    reg_panels = tree.xpath("//div[@data-name='REG']")
    for panel in reg_panels:
        items = panel.xpath(".//div[contains(@class, 'stats_item')]")
        if items:
            stats_items = items
            break

    # Fallback: if no REG panel found, use first occurrence of each stat
    if not stats_items:
        stats_items = tree.xpath("//div[contains(@class, 'stats_item')]")

    for item in stats_items:
        label_elems = item.xpath(".//div[contains(@class, 'stats_label')]//text()")
        value_elems = item.xpath(".//div[contains(@class, 'stats_value')]//text()")

        if label_elems and value_elems:
            label = " ".join(e.strip() for e in label_elems if e.strip()).upper()
            value = " ".join(e.strip() for e in value_elems if e.strip())
            # Only add if not already present (first occurrence wins)
            if label and value and label not in stats_map:
                stats_map[label] = value

    return stats_map


def parse_company_quote(html_content: str) -> dict[str, Any]:
    """Parse quote data from company page HTML.

    DPS page structure uses these CSS classes:
    - quote__name: company name
    - quote__sector: sector name
    - quote__close: price (Rs.XXX.XX)
    - change__value: change amount
    - change__percent: change percentage
    - quote__date: as of timestamp
    - stats_label + stats_value: OHLCV and ranges

    Args:
        html_content: Raw HTML from company page

    Returns:
        Dict with keys:
        - company_name, sector_name (if shown)
        - price, change, change_pct
        - as_of (timestamp string)
        - open, high, low, volume
        - day_range_low, day_range_high
        - wk52_low, wk52_high
        - circuit_low, circuit_high
        - market_mode
        - raw_hash (for deduplication)
    """
    tree = html.fromstring(html_content)
    quote: dict[str, Any] = {}

    # Company name - DPS uses quote__name class
    name_elems = tree.xpath(
        "//div[contains(@class, 'quote__name')]//text()"
    )
    for elem in name_elems:
        text = elem.strip()
        if text and len(text) > 2:
            quote["company_name"] = text
            break

    # Sector name - DPS uses quote__sector class
    sector_elems = tree.xpath(
        "//div[contains(@class, 'quote__sector')]//text()"
    )
    for elem in sector_elems:
        text = elem.strip()
        if text and len(text) > 2:
            quote["sector_name"] = text
            break

    # Price - DPS uses quote__close class with "Rs.XXX.XX" format
    price_elems = tree.xpath(
        "//div[contains(@class, 'quote__close')]//text()"
    )
    for elem in price_elems:
        text = elem.strip()
        # Extract number from "Rs.331.26" format
        match = re.search(r"Rs\.?\s*([0-9,]+\.?[0-9]*)", text)
        if match:
            val = _parse_numeric(match.group(1))
            if val is not None and val > 0:
                quote["price"] = val
                break

    # Change amount - DPS uses change__value class
    change_elems = tree.xpath(
        "//div[contains(@class, 'change__value')]//text()"
    )
    for elem in change_elems:
        val = _parse_numeric(elem.strip())
        if val is not None:
            quote["change"] = val
            break

    # Change percent - DPS uses change__percent class
    pct_elems = tree.xpath(
        "//div[contains(@class, 'change__percent')]//text()"
    )
    for elem in pct_elems:
        text = elem.strip()
        # Extract number from "(-0.79%)" format
        match = re.search(r"\(?\s*([+-]?\d+\.?\d*)\s*%?\s*\)?", text)
        if match:
            val = _parse_numeric(match.group(1))
            if val is not None:
                quote["change_pct"] = val
                break

    # As of timestamp - DPS uses quote__date class
    date_elems = tree.xpath(
        "//div[contains(@class, 'quote__date')]//text()"
    )
    for elem in date_elems:
        text = elem.strip()
        # Extract date/time after "As of" or "^"
        match = re.search(r"(?:[Aa]s of|[\^])\s*(.+?)$", text)
        if match:
            quote["as_of"] = match.group(1).strip()
            break

    # Stats - DPS uses stats_label and stats_value pairs
    # Build a map of label -> value from the stats divs
    stats_map = _extract_stats_map(tree)

    # OHLCV from stats
    ohlcv_map = {
        "open": ["OPEN"],
        "high": ["HIGH"],
        "low": ["LOW"],
        "volume": ["VOLUME"],
    }
    for key, labels in ohlcv_map.items():
        for label in labels:
            if label in stats_map:
                val = _parse_numeric(stats_map[label])
                if val is not None:
                    quote[key] = val
                    break

    # Day range from stats
    if "DAY RANGE" in stats_map:
        low, high = _parse_range(stats_map["DAY RANGE"])
        if low is not None:
            quote["day_range_low"] = low
        if high is not None:
            quote["day_range_high"] = high

    # 52-week range from stats (may have ^ suffix in label)
    for label_key in ["52-WEEK RANGE", "52-WEEK RANGE ^"]:
        if label_key in stats_map:
            low, high = _parse_range(stats_map[label_key])
            if low is not None:
                quote["wk52_low"] = low
            if high is not None:
                quote["wk52_high"] = high
            break

    # Circuit breaker from stats
    if "CIRCUIT BREAKER" in stats_map:
        low, high = _parse_range(stats_map["CIRCUIT BREAKER"])
        if low is not None:
            quote["circuit_low"] = low
        if high is not None:
            quote["circuit_high"] = high

    # LDCP (Last Day Close Price) - also known as previous close
    for label_key in ["LDCP", "PREV CLOSE", "PREVIOUS CLOSE"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key])
            if val is not None:
                quote["ldcp"] = val
                break

    # Bid/Ask from stats
    # Format: "333.18 (1,661)" or just "333.18"
    if "ASK" in stats_map:
        ask_text = stats_map["ASK"]
        # Try to extract price and size
        match = re.match(r"([0-9,]+\.?[0-9]*)\s*(?:\(([0-9,]+)\))?", ask_text)
        if match:
            quote["ask_price"] = _parse_numeric(match.group(1))
            if match.group(2):
                quote["ask_size"] = int(match.group(2).replace(",", ""))

    if "BID" in stats_map:
        bid_text = stats_map["BID"]
        match = re.match(r"([0-9,]+\.?[0-9]*)\s*(?:\(([0-9,]+)\))?", bid_text)
        if match:
            quote["bid_price"] = _parse_numeric(match.group(1))
            if match.group(2):
                quote["bid_size"] = int(match.group(2).replace(",", ""))

    # Performance metrics
    for label_key in ["YTD", "YTD CHANGE", "YTD %"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key].replace("%", ""))
            if val is not None:
                quote["ytd_change_pct"] = val
                break

    for label_key in ["1 YEAR", "1-YEAR", "1 YEAR CHANGE", "1-YEAR %"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key].replace("%", ""))
            if val is not None:
                quote["one_year_change_pct"] = val
                break

    # Valuation metrics
    for label_key in ["P/E", "PE", "P/E RATIO", "P/E (TTM)"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key])
            if val is not None:
                quote["pe_ratio"] = val
                break

    for label_key in ["MARKET CAP", "MKT CAP"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key])
            if val is not None:
                quote["market_cap"] = val
                break

    # Equity structure
    for label_key in ["TOTAL SHARES", "SHARES"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key])
            if val is not None:
                quote["total_shares"] = int(val)
                break

    for label_key in ["FREE FLOAT", "FREE-FLOAT"]:
        if label_key in stats_map:
            ff_text = stats_map[label_key]
            # Format: "645,139,260 (15.00%)" or just "645,139,260"
            match = re.match(r"([0-9,]+)\s*(?:\(([0-9.]+)%?\))?", ff_text)
            if match:
                quote["free_float_shares"] = int(match.group(1).replace(",", ""))
                if match.group(2):
                    quote["free_float_pct"] = float(match.group(2))
            break

    # Risk parameters
    if "HAIRCUT" in stats_map:
        val = _parse_numeric(stats_map["HAIRCUT"].replace("%", ""))
        if val is not None:
            quote["haircut"] = val

    for label_key in ["VAR", "VARIANCE"]:
        if label_key in stats_map:
            val = _parse_numeric(stats_map[label_key])
            if val is not None:
                quote["variance"] = val
                break

    # Market mode - not always present
    mode_patterns = [
        "//span[contains(@class, 'market-mode')]//text()",
        "//span[contains(@class, 'mode')]//text()",
    ]
    for pattern in mode_patterns:
        elems = tree.xpath(pattern)
        for elem in elems:
            text = elem.strip().upper()
            if text in ("REG", "ODD", "FUT", "SPOT"):
                quote["market_mode"] = text
                break

    # Compute raw hash for deduplication
    quote["raw_hash"] = _compute_raw_hash(quote)

    return quote


def _extract_profile_section(tree, heading: str) -> str | None:
    """Extract text from DPS profile section by heading.

    DPS profile structure:
    <div class="profile__item">
        <div class="item__head">HEADING</div>
        <p>Content text...</p>
    </div>
    """
    # Find item__head with matching text, then get the following <p>
    patterns = [
        f"//div[contains(@class, 'item__head') and contains(text(), '{heading}')]"
        f"/following-sibling::p[1]//text()",
        f"//div[contains(@class, 'item__head') and contains(text(), '{heading}')]"
        f"/following-sibling::p[1]/a/@href",
    ]

    for pattern in patterns:
        try:
            elems = tree.xpath(pattern)
            if elems:
                # Join text parts
                text = " ".join(e.strip() for e in elems if e.strip())
                if text and len(text) > 2:
                    return text
        except Exception:
            continue

    return None


def parse_company_profile(html_content: str) -> dict[str, Any]:
    """Parse company profile data from company page HTML.

    DPS profile structure uses profile__item divs with item__head labels:
    - BUSINESS DESCRIPTION
    - ADDRESS
    - WEBSITE
    - REGISTRAR
    - AUDITOR
    - Fiscal Year End

    Args:
        html_content: Raw HTML from company page

    Returns:
        Dict with keys:
        - company_name, sector_name
        - business_description
        - address, website, registrar, auditor, fiscal_year_end
    """
    tree = html.fromstring(html_content)
    profile: dict[str, Any] = {}

    # Company name - DPS uses quote__name class
    name_elems = tree.xpath("//div[contains(@class, 'quote__name')]//text()")
    for elem in name_elems:
        text = elem.strip()
        if text and len(text) > 2:
            profile["company_name"] = text
            break

    # Sector name - DPS uses quote__sector class
    sector_elems = tree.xpath("//div[contains(@class, 'quote__sector')]//text()")
    for elem in sector_elems:
        text = elem.strip()
        if text and len(text) > 2:
            profile["sector_name"] = text
            break

    # Business description - in profile__item with item__head "BUSINESS DESCRIPTION"
    desc = _extract_profile_section(tree, "BUSINESS DESCRIPTION")
    if desc:
        profile["business_description"] = desc

    # Address
    addr = _extract_profile_section(tree, "ADDRESS")
    if addr:
        profile["address"] = addr

    # Website - get the href from the link
    website_elems = tree.xpath(
        "//div[contains(@class, 'item__head') and contains(text(), 'WEBSITE')]"
        "/following-sibling::p[1]//a/@href"
    )
    if website_elems:
        url = website_elems[0].strip()
        if url:
            profile["website"] = url

    # Registrar
    registrar = _extract_profile_section(tree, "REGISTRAR")
    if registrar:
        profile["registrar"] = registrar

    # Auditor
    auditor = _extract_profile_section(tree, "AUDITOR")
    if auditor:
        profile["auditor"] = auditor

    # Fiscal year end
    fiscal = _extract_profile_section(tree, "Fiscal Year End")
    if fiscal:
        profile["fiscal_year_end"] = fiscal

    # Incorporation date
    incorp = _extract_profile_section(tree, "INCORPORATION")
    if not incorp:
        incorp = _extract_profile_section(tree, "Incorporation Date")
    if not incorp:
        incorp = _extract_profile_section(tree, "INCORPORATED")
    if incorp:
        profile["incorporation_date"] = incorp

    # Listed in (exchange)
    listed = _extract_profile_section(tree, "LISTED IN")
    if not listed:
        listed = _extract_profile_section(tree, "Listed In")
    if listed:
        profile["listed_in"] = listed

    return profile


def parse_all_fundamentals(html_content: str) -> dict[str, Any]:
    """Parse all fundamentals data from company page HTML.

    Combines quote data, profile data, and additional metrics into a single
    comprehensive dict suitable for company_fundamentals table.

    Args:
        html_content: Raw HTML from company page

    Returns:
        Dict with all fundamentals fields
    """
    # Get quote data (price, OHLCV, ranges, bid/ask, metrics)
    fundamentals = parse_company_quote(html_content)

    # Get profile data (company info, description, address, etc.)
    profile = parse_company_profile(html_content)

    # Merge profile into fundamentals
    fundamentals.update(profile)

    return fundamentals


def parse_financials(html_content: str) -> list[dict[str, Any]]:
    """Parse financial data from FINANCIALS tab.

    Extracts annual and quarterly financial data:
    - Sales (Revenue)
    - Profit After Tax (Net Income)
    - EPS (Earnings Per Share)

    Args:
        html_content: Raw HTML from company page

    Returns:
        List of dicts with keys: period_end, period_type, sales, profit_after_tax, eps
    """
    tree = html.fromstring(html_content)
    financials: list[dict[str, Any]] = []

    # Find FINANCIALS tab panel
    fin_panels = tree.xpath("//div[@id='financials' or contains(@class, 'financials')]")
    if not fin_panels:
        # Try looking for tab content with FINANCIALS header
        fin_panels = tree.xpath(
            "//div[contains(@class, 'tabs__content') or contains(@class, 'tab-content')]"
        )

    # Find all tables in the page that might contain financial data
    tables = tree.xpath("//table[contains(@class, 'tbl')]")

    for table in tables:
        # Check if this table has financial headers (Sales, Profit, EPS)
        headers = table.xpath(".//thead//th//text() | .//tr[1]//th//text() | .//tr[1]//td//text()")
        headers = [h.strip() for h in headers if h.strip()]

        if not headers:
            continue

        # Look for period columns (years or quarters)
        period_cols = []
        for i, h in enumerate(headers):
            # Match year patterns like "2024", "2023"
            if re.match(r"^\d{4}$", h):
                period_cols.append((i, h, "annual"))
            # Match quarter patterns like "Q1 2024", "Q3 2025"
            elif re.match(r"^Q[1-4]\s*\d{4}$", h):
                period_cols.append((i, h, "quarterly"))

        if not period_cols:
            continue

        # Parse data rows
        rows = table.xpath(".//tbody//tr | .//tr[position()>1]")
        row_data: dict[str, dict[str, float | None]] = {}

        for row in rows:
            cells = row.xpath(".//td//text() | .//th//text()")
            cells = [c.strip() for c in cells if c.strip()]

            if len(cells) < 2:
                continue

            # First cell is the metric name
            metric = cells[0].upper()

            # Map metrics
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
            else:
                continue

            # Extract values for each period column
            for col_idx, period, period_type in period_cols:
                if col_idx < len(cells):
                    val = _parse_numeric(cells[col_idx])
                    if period not in row_data:
                        row_data[period] = {"period_end": period, "period_type": period_type}
                    row_data[period][key] = val

        # Convert to list
        for period, data in row_data.items():
            if any(v is not None for k, v in data.items() if k not in ("period_end", "period_type")):
                financials.append(data)

    return financials


def parse_ratios(html_content: str) -> list[dict[str, Any]]:
    """Parse financial ratios from RATIOS tab.

    Extracts:
    - Gross Profit Margin (%)
    - Net Profit Margin (%)
    - EPS Growth (%)
    - PEG Ratio

    Args:
        html_content: Raw HTML from company page

    Returns:
        List of dicts with keys: period_end, period_type, and ratio fields
    """
    tree = html.fromstring(html_content)
    ratios: list[dict[str, Any]] = []

    # Find all tables that might contain ratio data
    tables = tree.xpath("//table[contains(@class, 'tbl')]")

    for table in tables:
        headers = table.xpath(".//thead//th//text() | .//tr[1]//th//text() | .//tr[1]//td//text()")
        headers = [h.strip() for h in headers if h.strip()]

        if not headers:
            continue

        # Look for period columns
        period_cols = []
        for i, h in enumerate(headers):
            if re.match(r"^\d{4}$", h):
                period_cols.append((i, h, "annual"))
            elif re.match(r"^Q[1-4]\s*\d{4}$", h):
                period_cols.append((i, h, "quarterly"))

        if not period_cols:
            continue

        # Parse data rows
        rows = table.xpath(".//tbody//tr | .//tr[position()>1]")
        row_data: dict[str, dict[str, float | None]] = {}

        for row in rows:
            cells = row.xpath(".//td//text() | .//th//text()")
            cells = [c.strip() for c in cells if c.strip()]

            if len(cells) < 2:
                continue

            metric = cells[0].upper()

            # Map ratio metrics
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
            elif "SALES GROWTH" in metric or "REVENUE GROWTH" in metric:
                key = "sales_growth"
            elif "EPS GROWTH" in metric:
                key = "eps_growth"
            elif "PROFIT GROWTH" in metric:
                key = "profit_growth"
            elif "PEG" in metric:
                key = "peg_ratio"
            elif "P/E" in metric or "PE RATIO" in metric:
                key = "pe_ratio"
            elif "P/B" in metric or "PB RATIO" in metric:
                key = "pb_ratio"
            else:
                continue

            for col_idx, period, period_type in period_cols:
                if col_idx < len(cells):
                    val = _parse_numeric(cells[col_idx].replace("%", ""))
                    if period not in row_data:
                        row_data[period] = {"period_end": period, "period_type": period_type}
                    row_data[period][key] = val

        for period, data in row_data.items():
            if any(v is not None for k, v in data.items() if k not in ("period_end", "period_type")):
                ratios.append(data)

    return ratios


def parse_payouts(html_content: str) -> list[dict[str, Any]]:
    """Parse dividend/payout data from PAYOUTS tab.

    Extracts:
    - Ex-dividend date
    - Announcement date
    - Book closure dates
    - Dividend amount
    - Payout type (cash, bonus, right)
    - Fiscal year

    Args:
        html_content: Raw HTML from company page

    Returns:
        List of dicts with payout data
    """
    tree = html.fromstring(html_content)
    payouts: list[dict[str, Any]] = []

    # Find payout tables
    tables = tree.xpath("//table[contains(@class, 'tbl')]")

    for table in tables:
        headers = table.xpath(".//thead//th//text() | .//tr[1]//th//text() | .//tr[1]//td//text()")
        headers = [h.strip().upper() for h in headers if h.strip()]

        if not headers:
            continue

        # Check if this looks like a payout table
        payout_indicators = ["DIVIDEND", "EX-DATE", "BOOK CLOSURE", "ANNOUNCEMENT", "AMOUNT"]
        if not any(ind in " ".join(headers) for ind in payout_indicators):
            continue

        # Build column index map
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

        if "ex_date" not in col_map:
            continue

        # Parse data rows
        rows = table.xpath(".//tbody//tr | .//tr[position()>1]")

        for row in rows:
            cells = row.xpath(".//td//text()")
            cells = [c.strip() for c in cells]

            if len(cells) < 2:
                continue

            payout: dict[str, Any] = {"payout_type": "cash"}  # default

            for key, idx in col_map.items():
                if idx < len(cells):
                    val = cells[idx].strip()
                    if key == "amount":
                        payout[key] = _parse_numeric(val)
                    elif key in ("ex_date", "announcement_date", "book_closure_from", "book_closure_to"):
                        # Try to parse date
                        payout[key] = _parse_date(val)
                    else:
                        payout[key] = val if val else None

            if payout.get("ex_date"):
                payouts.append(payout)

    return payouts


def _parse_date(date_str: str | None) -> str | None:
    """Parse date string to YYYY-MM-DD format."""
    if not date_str:
        return None

    date_str = date_str.strip()
    if not date_str or date_str == "-":
        return None

    # Try common formats
    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def parse_key_people(html_content: str) -> list[dict[str, str]]:
    """Parse key people from company page HTML.

    DPS key people structure (under profile__item--people):
    <table class="tbl">
        <tbody class="tbl__body">
            <tr>
                <td><strong>Ahmed Hayat Lak</strong></td>
                <td>CEO</td>
            </tr>
            ...
        </tbody>
    </table>

    Args:
        html_content: Raw HTML from company page

    Returns:
        List of dicts with 'role' and 'name' keys
    """
    tree = html.fromstring(html_content)
    people: list[dict[str, str]] = []

    # DPS pattern: Find KEY PEOPLE section, then parse the table rows
    # Each row has: <td><strong>Name</strong></td><td>Role</td>
    key_people_rows = tree.xpath(
        "//div[contains(@class, 'profile__item--people')]"
        "//table[contains(@class, 'tbl')]//tbody//tr"
    )

    for row in key_people_rows:
        try:
            # Get name from first td (inside <strong>)
            name_elems = row.xpath(".//td[1]//strong//text()")
            if not name_elems:
                # Fallback: try without <strong>
                name_elems = row.xpath(".//td[1]//text()")

            # Get role from second td
            role_elems = row.xpath(".//td[2]//text()")

            if name_elems and role_elems:
                name = " ".join(e.strip() for e in name_elems if e.strip())
                role = " ".join(e.strip() for e in role_elems if e.strip())

                if name and role and len(name) > 2:
                    people.append({"role": role, "name": name})
        except Exception:
            continue

    return people


def refresh_company_profile(
    con,
    symbol: str,
    html_content: str | None = None,
) -> dict[str, Any]:
    """Fetch and update company profile and key people.

    Args:
        con: Database connection
        symbol: Stock symbol
        html_content: Optional pre-fetched HTML content

    Returns:
        Summary dict with success status and counts
    """
    from ..db import (
        replace_company_key_people,
        sync_sector_names_from_company_profile,
        upsert_company_profile,
    )

    result: dict[str, Any] = {
        "symbol": symbol.upper(),
        "fetched_at": now_iso(),
        "profile_updated": False,
        "key_people_count": 0,
        "success": False,
        "error": None,
    }

    try:
        if html_content is None:
            html_content = fetch_company_page_html(symbol)

        # Parse profile
        profile = parse_company_profile(html_content)
        profile["symbol"] = symbol.upper()
        profile["source_url"] = DPS_COMPANY_URL.format(symbol=symbol.upper())

        # Parse key people
        key_people = parse_key_people(html_content)

        # Update database
        upsert_company_profile(con, profile)
        result["profile_updated"] = True

        count = replace_company_key_people(con, symbol, key_people)
        result["key_people_count"] = count

        # Sync sector name to symbols table
        sync_sector_names_from_company_profile(con)

        result["success"] = True

    except requests.RequestException as e:
        result["error"] = f"HTTP error: {e}"
    except Exception as e:
        result["error"] = f"Parse error: {e}"

    return result


def refresh_fundamentals(
    con,
    symbol: str,
    html_content: str | None = None,
    save_history: bool = True,
) -> dict[str, Any]:
    """Fetch and update all company fundamentals.

    Fetches comprehensive data from PSX company page including:
    - Price/quote data (OHLCV, bid/ask, ranges)
    - Performance metrics (YTD, 1-year change, P/E)
    - Equity structure (market cap, shares, free float)
    - Risk parameters (haircut, variance)
    - Company profile (description, address, etc.)
    - Key people (directors, officers)
    - Financial data (sales, profit, EPS) from FINANCIALS tab
    - Ratio data (margins, growth) from RATIOS tab
    - Payout/dividend history from PAYOUTS tab

    Stores to company_fundamentals table (latest) and optionally
    to company_fundamentals_history (daily snapshots).

    Args:
        con: Database connection
        symbol: Stock symbol
        html_content: Optional pre-fetched HTML content
        save_history: If True, also save to history table

    Returns:
        Summary dict with success status and update details
    """
    from ..db import (
        replace_company_key_people,
        upsert_company_financials,
        upsert_company_fundamentals,
        upsert_company_payouts,
        upsert_company_ratios,
    )

    result: dict[str, Any] = {
        "symbol": symbol.upper(),
        "fetched_at": now_iso(),
        "fundamentals_updated": False,
        "history_saved": False,
        "key_people_count": 0,
        "financials_count": 0,
        "ratios_count": 0,
        "payouts_count": 0,
        "success": False,
        "error": None,
        "data": None,
    }

    try:
        if html_content is None:
            html_content = fetch_company_page_html(symbol)

        # Parse all fundamentals (quote + profile combined)
        fundamentals = parse_all_fundamentals(html_content)
        fundamentals["symbol"] = symbol.upper()
        fundamentals["source_url"] = DPS_COMPANY_URL.format(symbol=symbol.upper())

        # Parse key people
        key_people = parse_key_people(html_content)

        # Parse financials (FINANCIALS tab)
        financials = parse_financials(html_content)

        # Parse ratios (RATIOS tab)
        ratios = parse_ratios(html_content)

        # Parse payouts (PAYOUTS tab)
        payouts = parse_payouts(html_content)

        # Update fundamentals table (and optionally history)
        db_result = upsert_company_fundamentals(
            con, symbol, fundamentals, save_history=save_history
        )
        result["fundamentals_updated"] = db_result.get("updated", False)
        result["history_saved"] = db_result.get("history_saved", False)

        # Update key people
        count = replace_company_key_people(con, symbol, key_people)
        result["key_people_count"] = count

        # Update financials
        if financials:
            fin_count = upsert_company_financials(con, symbol, financials)
            result["financials_count"] = fin_count

        # Update ratios
        if ratios:
            ratio_count = upsert_company_ratios(con, symbol, ratios)
            result["ratios_count"] = ratio_count

        # Update payouts
        if payouts:
            payout_count = upsert_company_payouts(con, symbol, payouts)
            result["payouts_count"] = payout_count

        result["success"] = True
        result["data"] = fundamentals

    except requests.RequestException as e:
        result["error"] = f"HTTP error: {e}"
    except Exception as e:
        result["error"] = f"Parse error: {e}"

    return result


def take_quote_snapshot(
    con,
    symbol: str,
    html_content: str | None = None,
    skip_if_unchanged: bool = False,
    compute_signals: bool = True,
) -> dict[str, Any]:
    """Fetch and store a quote snapshot.

    Args:
        con: Database connection
        symbol: Stock symbol
        html_content: Optional pre-fetched HTML content
        skip_if_unchanged: If True, skip insert if raw_hash unchanged
        compute_signals: If True, compute and persist derived signals

    Returns:
        Summary dict with quote data and success status
    """
    from ..db import get_last_quote_hash, insert_quote_snapshot

    ts = _get_pkt_timestamp()
    result: dict[str, Any] = {
        "symbol": symbol.upper(),
        "ts": ts,
        "inserted": False,
        "skipped": False,
        "success": False,
        "error": None,
        "quote": {},
        "signals": {},
    }

    try:
        if html_content is None:
            html_content = fetch_company_page_html(symbol)

        # Parse quote
        quote = parse_company_quote(html_content)
        result["quote"] = quote

        # Check if unchanged
        if skip_if_unchanged:
            last_hash = get_last_quote_hash(con, symbol)
            if last_hash and last_hash == quote.get("raw_hash"):
                result["skipped"] = True
                result["success"] = True
                return result

        # Insert snapshot
        inserted = insert_quote_snapshot(con, symbol, ts, quote)
        result["inserted"] = inserted
        result["success"] = True

        # Compute and persist signals if requested and snapshot was inserted
        if compute_signals and inserted:
            try:
                from ..company_analytics import compute_and_persist_signals
                signals = compute_and_persist_signals(con, symbol, ts, quote)
                result["signals"] = signals
            except Exception as sig_err:
                # Don't fail the whole snapshot if signals fail
                result["signals_error"] = str(sig_err)

    except requests.RequestException as e:
        result["error"] = f"HTTP error: {e}"
    except Exception as e:
        result["error"] = f"Error: {e}"

    return result


def listen_quotes(
    con,
    symbols: list[str],
    interval: int = 60,
    callback=None,
) -> None:
    """Continuously snapshot quotes at specified interval.

    Args:
        con: Database connection
        symbols: List of stock symbols to monitor
        interval: Seconds between snapshots
        callback: Optional function to call after each cycle with results

    Raises:
        KeyboardInterrupt: When user interrupts with Ctrl+C
    """
    print(f"Listening for quotes on {len(symbols)} symbols...")
    print(f"Interval: {interval}s | Press Ctrl+C to stop")
    print("-" * 50)

    try:
        while True:
            cycle_results = []
            for symbol in symbols:
                result = take_quote_snapshot(
                    con, symbol, skip_if_unchanged=True
                )
                cycle_results.append(result)

                # Print status
                if result["success"]:
                    q = result["quote"]
                    status = "SKIP" if result["skipped"] else "OK"
                    price = q.get("price", "N/A")
                    change_pct = q.get("change_pct", "N/A")
                    print(f"[{status}] {symbol}: {price} ({change_pct}%)")
                else:
                    print(f"[ERR] {symbol}: {result['error']}")

                # Polite delay between symbols
                if len(symbols) > 1:
                    time.sleep(REQUEST_DELAY)

            if callback:
                callback(cycle_results)

            print(f"\n--- Sleeping {interval}s ---\n")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopped by user.")
        raise
