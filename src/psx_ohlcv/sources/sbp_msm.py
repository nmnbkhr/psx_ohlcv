"""
SBP Money & Securities Markets (MSM) data scraper.

This module fetches live rate data from the SBP MSM page:
- SBP Policy Rates (policy rate, ceiling, floor)
- KIBOR Rates (3M, 6M, 12M with bid/offer)
- Treasury Bill (MTB) cut-off yields
- Pakistan Investment Bond (PIB) yields

Source: https://www.sbp.org.pk/dfmd/msm.asp

All data is READ-ONLY and for informational purposes only.
"""

import re
from dataclasses import dataclass
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# SBP MSM page URL
SBP_MSM_URL = "https://www.sbp.org.pk/dfmd/msm.asp"


@dataclass
class PolicyRates:
    """SBP Policy Rate data."""

    date: str
    policy_rate: float | None
    ceiling_rate: float | None  # Overnight Reverse Repo
    floor_rate: float | None  # Overnight Repo
    overnight_repo_rate: float | None  # Weighted average


@dataclass
class KIBORRate:
    """KIBOR rate for a specific tenor."""

    date: str
    tenor_months: int
    bid: float | None
    offer: float | None


@dataclass
class MTBYield:
    """Market Treasury Bill yield."""

    date: str
    tenor_months: int
    cut_off_yield: float | None


@dataclass
class PIBYield:
    """Pakistan Investment Bond yield."""

    date: str
    tenor_years: int
    rate_type: str  # 'FIXED' or 'FLOATING'
    yield_value: float | None
    coupon_frequency: str | None  # 'Q' for quarterly, 'S' for semi-annual


def fetch_msm_html(timeout: int = 30) -> str | None:
    """
    Fetch HTML content from SBP MSM page.

    Args:
        timeout: Request timeout in seconds

    Returns:
        HTML content as string, or None on error
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)"
        }
        response = requests.get(SBP_MSM_URL, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching MSM page: {e}")
        return None


def parse_rate(text: str) -> float | None:
    """Parse a rate value from text like '10.50%' or '10.50'."""
    if not text:
        return None
    # Remove % and whitespace
    cleaned = text.strip().replace("%", "").replace(",", "")
    try:
        value = float(cleaned)
        # If value > 1, assume it's already in percentage form
        if value > 1:
            return value / 100
        return value
    except ValueError:
        return None


def parse_policy_rates(soup: BeautifulSoup) -> PolicyRates | None:
    """
    Parse SBP Policy Rates from the page.

    Returns:
        PolicyRates dataclass or None if not found
    """
    today = datetime.now().strftime("%Y-%m-%d")

    policy_rate = None
    ceiling_rate = None
    floor_rate = None
    overnight_rate = None

    # Look for policy rate patterns in text
    text = soup.get_text()

    # Pattern: "SBP Policy Rate: 10.50% p.a."
    policy_match = re.search(
        r"(?:SBP\s+)?Policy\s+Rate[:\s]+(\d+\.?\d*)\s*%",
        text,
        re.IGNORECASE
    )
    if policy_match:
        policy_rate = float(policy_match.group(1)) / 100

    # Pattern: "Overnight Reverse Repo (Ceiling): 11.50%"
    ceiling_match = re.search(
        r"(?:Overnight\s+)?Reverse\s+Repo[^:]*(?:Ceiling)?[:\s]+(\d+\.?\d*)\s*%",
        text,
        re.IGNORECASE
    )
    if ceiling_match:
        ceiling_rate = float(ceiling_match.group(1)) / 100

    # Pattern: "Overnight Repo (Floor): 9.50%"
    floor_match = re.search(
        r"(?:Overnight\s+)?Repo[^:]*(?:Floor)?[:\s]+(\d+\.?\d*)\s*%",
        text,
        re.IGNORECASE
    )
    if floor_match:
        floor_rate = float(floor_match.group(1)) / 100

    # Pattern: "Weighted-average Overnight Repo Rate: 9.82%"
    overnight_match = re.search(
        r"Weighted[- ]?average\s+(?:Overnight\s+)?Repo\s+Rate[:\s]+(\d+\.?\d*)\s*%",
        text,
        re.IGNORECASE
    )
    if overnight_match:
        overnight_rate = float(overnight_match.group(1)) / 100

    if any([policy_rate, ceiling_rate, floor_rate, overnight_rate]):
        return PolicyRates(
            date=today,
            policy_rate=policy_rate,
            ceiling_rate=ceiling_rate,
            floor_rate=floor_rate,
            overnight_repo_rate=overnight_rate,
        )

    return None


def parse_kibor_rates(soup: BeautifulSoup) -> list[KIBORRate]:
    """
    Parse KIBOR rates from the page.

    Returns:
        List of KIBORRate dataclasses
    """
    today = datetime.now().strftime("%Y-%m-%d")
    rates = []

    text = soup.get_text()

    # Look for KIBOR patterns
    # Pattern: "3-M: 10.24 (bid) / 10.49 (offer)" or similar
    kibor_patterns = [
        (3, r"3[\s-]*M[onth]*[:\s]+(\d+\.?\d*)[^/]*/[^0-9]*(\d+\.?\d*)"),
        (6, r"6[\s-]*M[onth]*[:\s]+(\d+\.?\d*)[^/]*/[^0-9]*(\d+\.?\d*)"),
        (12, r"12[\s-]*M[onth]*[:\s]+(\d+\.?\d*)[^/]*/[^0-9]*(\d+\.?\d*)"),
    ]

    for tenor, pattern in kibor_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            bid = float(match.group(1)) / 100
            offer = float(match.group(2)) / 100
            rates.append(KIBORRate(
                date=today,
                tenor_months=tenor,
                bid=bid,
                offer=offer,
            ))

    # Also try to find in tables
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 3:
                cell_text = cells[0].get_text(strip=True).lower()

                # Check for KIBOR tenor patterns
                tenor = None
                if "3" in cell_text and ("month" in cell_text or "m" in cell_text):
                    tenor = 3
                elif "6" in cell_text and ("month" in cell_text or "m" in cell_text):
                    tenor = 6
                elif "12" in cell_text or "1 year" in cell_text:
                    tenor = 12

                if tenor:
                    bid = parse_rate(cells[1].get_text(strip=True))
                    offer = parse_rate(cells[2].get_text(strip=True))
                    if bid and offer:
                        # Check if we already have this tenor
                        existing = [r for r in rates if r.tenor_months == tenor]
                        if not existing:
                            rates.append(KIBORRate(
                                date=today,
                                tenor_months=tenor,
                                bid=bid,
                                offer=offer,
                            ))

    return rates


def parse_mtb_yields(soup: BeautifulSoup) -> list[MTBYield]:
    """
    Parse Treasury Bill yields from the page.

    Returns:
        List of MTBYield dataclasses
    """
    today = datetime.now().strftime("%Y-%m-%d")
    yields = []

    text = soup.get_text()

    # Look for MTB yield patterns in text
    # Pattern: "3-month: 9.8996%" or "3M MTB: 9.90%"
    mtb_patterns = [
        (3, r"3[\s-]*(?:month|m)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
        (6, r"6[\s-]*(?:month|m)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
        (12, r"12[\s-]*(?:month|m|1[\s-]*year)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
    ]

    for tenor, pattern in mtb_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            cut_off = float(match.group(1)) / 100
            yields.append(MTBYield(
                date=today,
                tenor_months=tenor,
                cut_off_yield=cut_off,
            ))

    # Also look for general MTB yield range
    # Pattern: "Cut-off yield ranges from 9.8996% to 10.0010%"
    range_match = re.search(
        r"(?:MTB|T-?Bill)[^0-9]*(\d+\.?\d*)\s*%\s*(?:to|[-])\s*(\d+\.?\d*)\s*%",
        text,
        re.IGNORECASE
    )
    if range_match and not yields:
        low = float(range_match.group(1)) / 100
        high = float(range_match.group(2)) / 100
        # Use average as representative
        avg = (low + high) / 2
        yields.append(MTBYield(
            date=today,
            tenor_months=6,  # Assume 6-month as benchmark
            cut_off_yield=avg,
        ))

    return yields


def parse_pib_yields(soup: BeautifulSoup) -> list[PIBYield]:
    """
    Parse PIB yields from the page.

    Returns:
        List of PIBYield dataclasses
    """
    today = datetime.now().strftime("%Y-%m-%d")
    yields = []

    text = soup.get_text()

    # Look for PIB yield patterns
    # Pattern: "2-year: 10.14%" or "5Y PIB: 10.50%"
    pib_patterns = [
        (2, r"2[\s-]*(?:year|y)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
        (3, r"3[\s-]*(?:year|y)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
        (5, r"5[\s-]*(?:year|y)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
        (10, r"10[\s-]*(?:year|y)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
        (15, r"15[\s-]*(?:year|y)[^:]*[:\s]+(\d+\.?\d*)\s*%"),
    ]

    for tenor, pattern in pib_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            yield_val = float(match.group(1)) / 100
            yields.append(PIBYield(
                date=today,
                tenor_years=tenor,
                rate_type="FIXED",
                yield_value=yield_val,
                coupon_frequency="S",  # Semi-annual default
            ))

    # Try to parse tables for more structured data
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                cell_text = cells[0].get_text(strip=True).lower()

                # Check for PIB tenor patterns
                tenor = None
                for t in [2, 3, 5, 10, 15, 20]:
                    has_tenor = f"{t}" in cell_text
                    has_year = "year" in cell_text or "y" in cell_text
                    if has_tenor and has_year:
                        tenor = t
                        break

                if tenor:
                    yield_val = parse_rate(cells[-1].get_text(strip=True))
                    if yield_val:
                        # Check for rate type
                        rate_type = "FIXED"
                        if "float" in cell_text:
                            rate_type = "FLOATING"

                        # Check if we already have this
                        existing = [
                            y for y in yields
                            if y.tenor_years == tenor and y.rate_type == rate_type
                        ]
                        if not existing:
                            yields.append(PIBYield(
                                date=today,
                                tenor_years=tenor,
                                rate_type=rate_type,
                                yield_value=yield_val,
                                coupon_frequency="S",
                            ))

    return yields


def fetch_all_msm_data() -> dict:
    """
    Fetch all available data from SBP MSM page.

    Returns:
        Dict with policy_rates, kibor_rates, mtb_yields, pib_yields
    """
    result = {
        "policy_rates": None,
        "kibor_rates": [],
        "mtb_yields": [],
        "pib_yields": [],
        "fetch_date": datetime.now().isoformat(),
        "source": "SBP_MSM",
        "error": None,
    }

    html = fetch_msm_html()
    if not html:
        result["error"] = "Failed to fetch MSM page"
        return result

    soup = BeautifulSoup(html, "html.parser")

    # Parse all data types
    result["policy_rates"] = parse_policy_rates(soup)
    result["kibor_rates"] = parse_kibor_rates(soup)
    result["mtb_yields"] = parse_mtb_yields(soup)
    result["pib_yields"] = parse_pib_yields(soup)

    return result


def convert_msm_to_curve_points(msm_data: dict) -> list[dict]:
    """
    Convert MSM data to yield curve points for database storage.

    Args:
        msm_data: Result from fetch_all_msm_data()

    Returns:
        List of curve point dicts ready for upsert_fi_curve_point()
    """
    points = []
    today = datetime.now().strftime("%Y-%m-%d")

    # MTB yields -> PKR_MTB curve
    for mtb in msm_data.get("mtb_yields", []):
        if mtb.cut_off_yield:
            points.append({
                "curve_name": "PKR_MTB",
                "curve_date": today,
                "tenor_months": mtb.tenor_months,
                "yield_value": mtb.cut_off_yield,
                "source": "SBP_MSM",
            })

    # PIB yields -> PKR_PIB curve
    for pib in msm_data.get("pib_yields", []):
        if pib.yield_value and pib.rate_type == "FIXED":
            points.append({
                "curve_name": "PKR_PIB",
                "curve_date": today,
                "tenor_months": pib.tenor_years * 12,
                "yield_value": pib.yield_value,
                "source": "SBP_MSM",
            })

    # KIBOR rates -> PKR_KIBOR curve (using offer rates)
    for kibor in msm_data.get("kibor_rates", []):
        if kibor.offer:
            points.append({
                "curve_name": "PKR_KIBOR",
                "curve_date": today,
                "tenor_months": kibor.tenor_months,
                "yield_value": kibor.offer,
                "source": "SBP_MSM",
            })

    return points


def get_sample_msm_data() -> dict:
    """
    Get sample MSM data for testing when SBP is unavailable.

    Returns:
        Sample MSM data dict
    """
    today = datetime.now().strftime("%Y-%m-%d")

    return {
        "policy_rates": PolicyRates(
            date=today,
            policy_rate=0.1050,
            ceiling_rate=0.1150,
            floor_rate=0.0950,
            overnight_repo_rate=0.0982,
        ),
        "kibor_rates": [
            KIBORRate(date=today, tenor_months=3, bid=0.1024, offer=0.1049),
            KIBORRate(date=today, tenor_months=6, bid=0.1024, offer=0.1049),
            KIBORRate(date=today, tenor_months=12, bid=0.1024, offer=0.1074),
        ],
        "mtb_yields": [
            MTBYield(date=today, tenor_months=3, cut_off_yield=0.0990),
            MTBYield(date=today, tenor_months=6, cut_off_yield=0.0995),
            MTBYield(date=today, tenor_months=12, cut_off_yield=0.1000),
        ],
        "pib_yields": [
            PIBYield(date=today, tenor_years=2, rate_type="FIXED",
                     yield_value=0.1014, coupon_frequency="S"),
            PIBYield(date=today, tenor_years=3, rate_type="FIXED",
                     yield_value=0.1030, coupon_frequency="S"),
            PIBYield(date=today, tenor_years=5, rate_type="FIXED",
                     yield_value=0.1050, coupon_frequency="S"),
            PIBYield(date=today, tenor_years=10, rate_type="FIXED",
                     yield_value=0.1080, coupon_frequency="S"),
        ],
        "fetch_date": datetime.now().isoformat(),
        "source": "SAMPLE",
        "error": None,
    }
