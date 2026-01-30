"""
PSX DPS Debt Market data source.

This module fetches debt securities data from PSX Data Portal Services:
- GoP Ijarah Sukuk (gop)
- Public Debt Securities (pds)
- Privately Debt Securities (cds) - Corporate
- Government Debt Securities (gds)

API Endpoint: https://dps.psx.com.pk/timeseries/eod/{SYMBOL}
Detail Page: https://dps.psx.com.pk/debt/{SYMBOL}
Market Page: https://dps.psx.com.pk/debt-market

All data is READ-ONLY and for informational purposes only.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# PSX DPS URLs
PSX_BASE_URL = "https://dps.psx.com.pk"
PSX_DEBT_MARKET_URL = f"{PSX_BASE_URL}/debt-market"
PSX_DEBT_DETAIL_URL = f"{PSX_BASE_URL}/debt"
PSX_TIMESERIES_URL = f"{PSX_BASE_URL}/timeseries/eod"

# Request headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html",
}

# Debt market categories from PSX page
DEBT_CATEGORIES = {
    "gop": "GoP Ijarah Sukuk",
    "pds": "Public Debt Securities",
    "cds": "Privately Debt Securities",  # Corporate
    "gds": "Government Debt Securities",
}


@dataclass
class DebtSecurity:
    """Parsed debt security information from PSX table."""

    symbol: str
    name: str | None = None
    category_code: str | None = None  # gop, pds, cds, gds
    category_name: str | None = None
    face_value: float | None = None
    listing_date: str | None = None
    issue_date: str | None = None
    issue_size: str | None = None  # Keep as string (e.g., "4.15B")
    issue_size_value: float | None = None  # Numeric value in millions
    maturity_date: str | None = None
    coupon_rate: float | None = None  # Percentage
    prev_coupon_date: str | None = None
    next_coupon_date: str | None = None
    outstanding_days: int | None = None
    remaining_years: float | None = None

    # Additional parsed fields
    security_type: str | None = None  # T-Bill, PIB, GIS, TFC, etc.
    tenor_years: float | None = None
    is_islamic: bool = False
    is_government: bool = True

    # Price data (from detail page or API)
    current_price: float | None = None
    ldcp: float | None = None
    volume: int | None = None
    status: str | None = None


def parse_date(date_str: str | None) -> str | None:
    """Parse date string to YYYY-MM-DD format."""
    if not date_str:
        return None

    date_str = date_str.strip()
    if not date_str:
        return None

    # Try various formats
    formats = [
        "%B %d, %Y",      # February 10, 2025
        "%B %d %Y",       # February 10 2025
        "%d-%m-%Y",       # 10-02-2025
        "%Y-%m-%d",       # 2025-02-10
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return date_str


def parse_number(value_str: str | None) -> float | None:
    """Parse numeric value, handling commas and suffixes."""
    if not value_str:
        return None

    value_str = value_str.strip().replace(",", "")
    if not value_str:
        return None

    # Handle percentage
    if "%" in value_str:
        value_str = value_str.replace("%", "")
        try:
            return float(value_str)
        except ValueError:
            return None

    # Handle B (billions), M (millions) suffixes
    multiplier = 1
    if value_str.endswith("B"):
        multiplier = 1000  # Convert to millions
        value_str = value_str[:-1]
    elif value_str.endswith("M"):
        multiplier = 1
        value_str = value_str[:-1]

    try:
        return float(value_str) * multiplier
    except ValueError:
        return None


def parse_symbol_info(symbol: str) -> dict[str, Any]:
    """
    Parse debt security symbol to extract type and tenor info.

    Symbol formats:
    - Government: P{tenor}{type}{maturity} e.g., P10PIB150136 (10yr PIB)
    - T-Bills: PK{tenor}TB{maturity} e.g., PK12TB210127 (12m T-Bill)
    - Corporate: {issuer}{type}{series} e.g., HBLTFC3, KELSC5
    """
    result = {
        "security_type": None,
        "tenor_years": None,
        "is_islamic": False,
        "is_government": True,
    }

    # Check government patterns
    gov_patterns = [
        (r"^PK(\d+)TB", "T-Bill", lambda m: int(m.group(1)) / 12),
        (r"^P(\d+)GIS", "GIS", lambda m: int(m.group(1))),
        (r"^P(\d+)FRR", "FRR Sukuk", lambda m: int(m.group(1))),
        (r"^P(\d+)VRR", "VRR Sukuk", lambda m: int(m.group(1))),
        (r"^P(\d+)FRZ", "FRZ", lambda m: int(m.group(1))),
        (r"^P(\d+)PIB", "PIB", lambda m: int(m.group(1))),
        (r"^P(\d+)PFL", "Floating", lambda m: int(m.group(1))),
        (r"^P(\d+)PFA", "Floating", lambda m: int(m.group(1))),
        (r"^P(\d+)GVR", "Variable GIS", lambda m: int(m.group(1))),
    ]

    for pattern, sec_type, tenor_fn in gov_patterns:
        match = re.match(pattern, symbol)
        if match:
            result["security_type"] = sec_type
            result["tenor_years"] = tenor_fn(match)
            result["is_islamic"] = sec_type in ["GIS", "FRR Sukuk", "VRR Sukuk", "Variable GIS"]
            return result

    # Check corporate patterns
    if "TFC" in symbol:
        result["security_type"] = "TFC"
        result["is_government"] = False
    elif "STSC" in symbol:
        result["security_type"] = "Corporate Sukuk"
        result["is_government"] = False
        result["is_islamic"] = True
    elif "SC" in symbol:
        result["security_type"] = "Corporate Sukuk"
        result["is_government"] = False
        result["is_islamic"] = True

    return result


def fetch_debt_market_html(timeout: int = 30) -> str | None:
    """Fetch the debt market page HTML."""
    try:
        response = requests.get(
            PSX_DEBT_MARKET_URL,
            headers=HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching debt market page: {e}")
        return None


def parse_debt_market_tables(html: str) -> dict[str, list[DebtSecurity]]:
    """
    Parse all debt securities from the debt market page tables.

    Returns:
        Dict with category codes as keys and lists of DebtSecurity as values
    """
    soup = BeautifulSoup(html, "html.parser")
    result = {cat: [] for cat in DEBT_CATEGORIES}

    # Find all tab panels
    panels = soup.find_all("div", class_="tabs__panel")

    # Find tab names to match with panels
    tab_items = soup.find_all("div", class_="tabs__list__item")
    tab_names = []
    for item in tab_items:
        name = item.get("data-name")
        if name:
            tab_names.append(name)

    # Process each panel
    for i, panel in enumerate(panels):
        if i >= len(tab_names):
            break

        category_code = tab_names[i]
        category_name = DEBT_CATEGORIES.get(category_code, category_code)

        # Find the table in this panel
        table = panel.find("table")
        if not table:
            continue

        # Find all rows
        rows = table.find_all("tr")

        for row in rows:
            # Get symbol from anchor tag
            symbol_link = row.find("a", class_="tbl__symbol")
            if not symbol_link:
                continue

            symbol = symbol_link.get_text(strip=True)
            if not symbol:
                continue

            # Get all cells
            cells = row.find_all("td")
            if len(cells) < 9:
                continue

            # Parse cells - structure depends on category
            # GoP Sukuk has more columns (coupon info)
            # Structure: Name, FaceValue, ListingDate, IssueDate, IssueSize,
            #            MaturityDate, [CouponRate, PrevCoupon, NextCoupon], OutstandingDays, RemainingYears

            try:
                cell_texts = [c.get_text(strip=True) for c in cells]

                # Parse symbol info
                sym_info = parse_symbol_info(symbol)

                security = DebtSecurity(
                    symbol=symbol,
                    category_code=category_code,
                    category_name=category_name,
                    security_type=sym_info.get("security_type"),
                    tenor_years=sym_info.get("tenor_years"),
                    is_islamic=sym_info.get("is_islamic", False),
                    is_government=sym_info.get("is_government", True),
                )

                # Parse cells based on count
                # Column order: 0=Symbol, 1=Name, 2=FaceValue, 3=ListingDate,
                #               4=IssueDate, 5=IssueSize, 6=MaturityDate,
                #               7=CouponRate, 8=PrevCoupon, 9=NextCoupon,
                #               10=OutstandingDays, 11=RemainingYears
                if len(cell_texts) >= 12:
                    # Full structure with all columns (GoP Sukuk, etc.)
                    security.name = cell_texts[1]  # Cell 1 is name
                    security.face_value = parse_number(cell_texts[2])
                    security.listing_date = parse_date(cell_texts[3])
                    security.issue_date = parse_date(cell_texts[4])
                    security.issue_size = cell_texts[5]
                    security.issue_size_value = parse_number(cell_texts[5])
                    security.maturity_date = parse_date(cell_texts[6])
                    security.coupon_rate = parse_number(cell_texts[7])
                    security.prev_coupon_date = parse_date(cell_texts[8])
                    security.next_coupon_date = parse_date(cell_texts[9])
                    security.outstanding_days = int(parse_number(cell_texts[10]) or 0)
                    security.remaining_years = parse_number(cell_texts[11])
                elif len(cell_texts) >= 10:
                    # Structure without coupon columns
                    # 0=Symbol, 1=Name, 2=FaceValue, 3=ListingDate, 4=IssueDate,
                    # 5=IssueSize, 6=MaturityDate, 7=OutstandingDays, 8=RemainingYears
                    security.name = cell_texts[1]
                    security.face_value = parse_number(cell_texts[2])
                    security.listing_date = parse_date(cell_texts[3])
                    security.issue_date = parse_date(cell_texts[4])
                    security.issue_size = cell_texts[5]
                    security.issue_size_value = parse_number(cell_texts[5])
                    security.maturity_date = parse_date(cell_texts[6])
                    security.outstanding_days = int(parse_number(cell_texts[-2]) or 0)
                    security.remaining_years = parse_number(cell_texts[-1])

                result[category_code].append(security)

            except (ValueError, IndexError) as e:
                logger.warning(f"Error parsing row for {symbol}: {e}")
                continue

    return result


def fetch_all_debt_securities(timeout: int = 30) -> dict[str, list[DebtSecurity]]:
    """
    Fetch all debt securities from PSX debt market page.

    Returns:
        Dict with category codes as keys and lists of DebtSecurity as values
    """
    html = fetch_debt_market_html(timeout)
    if not html:
        return {cat: [] for cat in DEBT_CATEGORIES}

    return parse_debt_market_tables(html)


def fetch_debt_symbols(timeout: int = 30) -> list[str]:
    """
    Fetch all debt security symbols from PSX debt market page.

    Returns:
        List of debt security symbols
    """
    html = fetch_debt_market_html(timeout)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    symbols = []

    for link in soup.find_all("a", class_="tbl__symbol"):
        symbol = link.get_text(strip=True)
        if symbol:
            symbols.append(symbol)

    return symbols


def fetch_debt_security_detail(symbol: str, timeout: int = 30) -> DebtSecurity | None:
    """
    Fetch detailed information for a specific debt security.

    Args:
        symbol: Debt security symbol
        timeout: Request timeout

    Returns:
        DebtSecurity with full details, or None on error
    """
    try:
        url = f"{PSX_DEBT_DETAIL_URL}/{symbol}"
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Start with parsed symbol info
        sym_info = parse_symbol_info(symbol)
        security = DebtSecurity(
            symbol=symbol,
            security_type=sym_info.get("security_type"),
            tenor_years=sym_info.get("tenor_years"),
            is_islamic=sym_info.get("is_islamic", False),
            is_government=sym_info.get("is_government", True),
        )

        # Extract data from page
        text = soup.get_text()

        # Security name from title
        title = soup.find("h1") or soup.find("title")
        if title:
            security.name = title.get_text(strip=True)

        # Current price
        price_match = re.search(r"Current\s*Price[:\s]*Rs\.?\s*([\d,.]+)", text, re.I)
        if price_match:
            security.current_price = parse_number(price_match.group(1))

        # LDCP
        ldcp_match = re.search(r"LDCP[:\s]*([\d,.]+)", text, re.I)
        if ldcp_match:
            security.ldcp = parse_number(ldcp_match.group(1))

        # Face value
        face_match = re.search(r"Face\s*Value[:\s]*(?:Rs\.?)?\s*([\d,]+)", text, re.I)
        if face_match:
            security.face_value = parse_number(face_match.group(1))

        # Issue size
        size_match = re.search(r"(?:Issue|Issuance)\s*Size[:\s]*([\d,.]+[BM]?)", text, re.I)
        if size_match:
            security.issue_size = size_match.group(1)
            security.issue_size_value = parse_number(size_match.group(1))

        # Coupon/Rental rate
        coupon_match = re.search(r"(?:Coupon|Rental)\s*Rate[:\s]*([\d.]+)\s*%?", text, re.I)
        if coupon_match:
            security.coupon_rate = float(coupon_match.group(1))

        # Status
        if "SUSPENDED" in text.upper():
            security.status = "SUSPENDED"
        elif "MATURED" in text.upper():
            security.status = "MATURED"
        else:
            security.status = "ACTIVE"

        # Parse dates
        listing_match = re.search(r"Listing\s*Date[:\s]*(\w+\s+\d+,?\s+\d{4})", text, re.I)
        if listing_match:
            security.listing_date = parse_date(listing_match.group(1))

        issue_match = re.search(r"(?:Issue|Issuance)\s*Date[:\s]*(\w+\s+\d+,?\s+\d{4})", text, re.I)
        if issue_match:
            security.issue_date = parse_date(issue_match.group(1))

        maturity_match = re.search(r"Maturity\s*Date[:\s]*(\w+\s+\d+,?\s+\d{4})", text, re.I)
        if maturity_match:
            security.maturity_date = parse_date(maturity_match.group(1))

        # Calculate days to maturity
        if security.maturity_date:
            try:
                maturity = datetime.strptime(security.maturity_date, "%Y-%m-%d")
                security.outstanding_days = (maturity - datetime.now()).days
                security.remaining_years = round(security.outstanding_days / 365, 1)
            except ValueError:
                pass

        return security

    except requests.RequestException as e:
        logger.error(f"Error fetching debt detail for {symbol}: {e}")
        return None


def fetch_debt_ohlcv(symbol: str, timeout: int = 30) -> list[dict]:
    """
    Fetch OHLCV timeseries data for a debt security.

    Uses the same API as equities: /timeseries/eod/{SYMBOL}

    Args:
        symbol: Debt security symbol
        timeout: Request timeout

    Returns:
        List of OHLCV dicts with date, price, volume, vwap
    """
    try:
        url = f"{PSX_TIMESERIES_URL}/{symbol}"
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()

        data = response.json()

        if data.get("status") != 1:
            logger.warning(f"API returned status != 1 for {symbol}")
            return []

        records = []
        for row in data.get("data", []):
            if len(row) >= 4:
                timestamp, price, volume, vwap = row[0], row[1], row[2], row[3]

                # Convert timestamp to date
                dt = datetime.fromtimestamp(timestamp)

                records.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                    "vwap": vwap if vwap else price,
                })

        return records

    except requests.RequestException as e:
        logger.error(f"Error fetching OHLCV for {symbol}: {e}")
        return []
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing OHLCV for {symbol}: {e}")
        return []


def get_securities_flat_list(
    securities_by_cat: dict[str, list[DebtSecurity]],
) -> list[DebtSecurity]:
    """Flatten securities dict to a single list."""
    result = []
    for securities in securities_by_cat.values():
        result.extend(securities)
    return result


def get_securities_summary(securities_by_cat: dict[str, list[DebtSecurity]]) -> dict:
    """
    Get summary statistics for debt securities.

    Returns:
        Dict with counts and breakdowns
    """
    flat_list = get_securities_flat_list(securities_by_cat)

    summary = {
        "total": len(flat_list),
        "by_category": {k: len(v) for k, v in securities_by_cat.items()},
        "government": sum(1 for s in flat_list if s.is_government),
        "corporate": sum(1 for s in flat_list if not s.is_government),
        "islamic": sum(1 for s in flat_list if s.is_islamic),
        "by_type": {},
    }

    # Count by security type
    for s in flat_list:
        st = s.security_type or "Unknown"
        summary["by_type"][st] = summary["by_type"].get(st, 0) + 1

    return summary


# Hardcoded list of known symbols for fallback
KNOWN_DEBT_SYMBOLS = [
    # T-Bills
    "PK01TB080126", "PK01TB220126", "PK03TB080126", "PK03TB220126",
    "PK06TB080126", "PK06TB220126", "PK12TB080126", "PK12TB210127",

    # PIBs
    "P02PIB160127", "P02PIB200926", "P03PIB150227", "P03PIB170728",
    "P05PIB150131", "P05PIB170730", "P10PIB150136", "P10PIB200934",
    "P15PIB160435", "P20PIB190939",

    # GIS
    "P01GIS040226", "P01GIS060326", "P01GIS210127", "P05GIS091225",

    # FRR/VRR Sukuk
    "P03FRR020528", "P03FRR240127", "P05FRR090130", "P10FRR090135",
    "P03VRR240127", "P05VRR090130", "P10VRR090135",

    # Floating Rate
    "P03PFL090226", "P05PFL060526", "P10PFL090135",

    # Corporate TFCs
    "HBLTFC2", "HBLTFC3", "UBLTFC5", "AKBLTFC6", "SNBLTFC3", "SNBLTFC4",

    # Corporate Sukuk
    "KELSC5", "KELSC6", "HUBPHLSC", "BIPLSC", "MUGHALSC",
]
