"""
Manual CSV data source for Sukuk/Debt instruments.

This module provides CSV loaders for sukuk master data, quotes, and yield curves.
Data is loaded from user-provided CSV files in the data/sukuk/ directory.

Data sources:
- PSX GIS (https://dps.psx.com.pk/gis/debt-market)
- SBP DFMD primary market documents
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

# Default data directory
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data" / "sukuk"


def load_sukuk_master_csv(path: Path | str) -> list[dict[str, Any]]:
    """
    Load sukuk master data from CSV file.

    Expected CSV columns:
        instrument_id, issuer, name, category, currency, issue_date,
        maturity_date, coupon_rate, coupon_frequency, face_value,
        issue_size, shariah_compliant, is_active, source, notes

    Args:
        path: Path to CSV file

    Returns:
        List of sukuk dicts

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If CSV is malformed
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Sukuk master CSV not found: {path}")

    sukuk_list = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Skip empty rows
            if not row.get("instrument_id"):
                continue

            sukuk = {
                "instrument_id": row["instrument_id"].strip(),
                "issuer": row.get("issuer", "").strip(),
                "name": row.get("name", "").strip(),
                "category": row.get("category", "GOP_SUKUK").strip(),
                "currency": row.get("currency", "PKR").strip(),
                "issue_date": _parse_date(row.get("issue_date")),
                "maturity_date": _parse_date(row.get("maturity_date")),
                "coupon_rate": _parse_float(row.get("coupon_rate")),
                "coupon_frequency": _parse_int(row.get("coupon_frequency"), 2),
                "face_value": _parse_float(row.get("face_value"), 100.0),
                "issue_size": _parse_float(row.get("issue_size")),
                "shariah_compliant": _parse_bool(row.get("shariah_compliant"), True),
                "is_active": _parse_bool(row.get("is_active"), True),
                "source": row.get("source", "MANUAL").strip(),
                "notes": row.get("notes", "").strip() or None,
            }

            # Validate required fields
            if not sukuk["instrument_id"] or not sukuk["maturity_date"]:
                continue

            sukuk_list.append(sukuk)

    return sukuk_list


def load_sukuk_quotes_csv(path: Path | str) -> list[dict[str, Any]]:
    """
    Load sukuk quotes from CSV file.

    Expected CSV columns:
        instrument_id, quote_date, clean_price, dirty_price,
        yield_to_maturity, bid_yield, ask_yield, volume, source

    Args:
        path: Path to CSV file

    Returns:
        List of quote dicts

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Sukuk quotes CSV not found: {path}")

    quotes = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Skip empty rows
            if not row.get("instrument_id") or not row.get("quote_date"):
                continue

            quote = {
                "instrument_id": row["instrument_id"].strip(),
                "quote_date": _parse_date(row["quote_date"]),
                "clean_price": _parse_float(row.get("clean_price")),
                "dirty_price": _parse_float(row.get("dirty_price")),
                "yield_to_maturity": _parse_float(row.get("yield_to_maturity")),
                "bid_yield": _parse_float(row.get("bid_yield")),
                "ask_yield": _parse_float(row.get("ask_yield")),
                "volume": _parse_float(row.get("volume")),
                "source": row.get("source", "MANUAL").strip(),
            }

            # Skip if no quote date
            if not quote["quote_date"]:
                continue

            quotes.append(quote)

    return quotes


def load_sukuk_curve_csv(path: Path | str) -> list[dict[str, Any]]:
    """
    Load sukuk yield curve points from CSV file.

    Expected CSV columns:
        curve_name, curve_date, tenor_days, yield_rate, source

    Args:
        path: Path to CSV file

    Returns:
        List of yield curve point dicts

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Sukuk yield curve CSV not found: {path}")

    points = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Skip empty rows
            if not row.get("curve_name") or not row.get("curve_date"):
                continue

            point = {
                "curve_name": row["curve_name"].strip(),
                "curve_date": _parse_date(row["curve_date"]),
                "tenor_days": _parse_int(row.get("tenor_days")),
                "yield_rate": _parse_float(row.get("yield_rate")),
                "source": row.get("source", "SBP").strip(),
            }

            # Skip if missing required fields
            if not point["curve_date"] or point["tenor_days"] is None:
                continue

            points.append(point)

    return points


def get_default_sukuk() -> list[dict[str, Any]]:
    """
    Get default sukuk instruments for seeding.

    Returns list of common GOP Ijarah Sukuk and T-Bills for demo/testing.
    """
    return [
        # GOP Ijarah Sukuk
        {
            "instrument_id": "GOP-IJARA-3Y-2027-06",
            "issuer": "Government of Pakistan",
            "name": "GOP Ijarah Sukuk 3Y Jun 2027",
            "category": "GOP_SUKUK",
            "currency": "PKR",
            "issue_date": "2024-06-15",
            "maturity_date": "2027-06-15",
            "coupon_rate": 15.5,
            "coupon_frequency": 2,
            "face_value": 100.0,
            "issue_size": 50000000000,
            "shariah_compliant": True,
            "is_active": True,
            "source": "PSX_GIS",
        },
        {
            "instrument_id": "GOP-IJARA-5Y-2029-01",
            "issuer": "Government of Pakistan",
            "name": "GOP Ijarah Sukuk 5Y Jan 2029",
            "category": "GOP_SUKUK",
            "currency": "PKR",
            "issue_date": "2024-01-15",
            "maturity_date": "2029-01-15",
            "coupon_rate": 16.0,
            "coupon_frequency": 2,
            "face_value": 100.0,
            "issue_size": 75000000000,
            "shariah_compliant": True,
            "is_active": True,
            "source": "PSX_GIS",
        },
        {
            "instrument_id": "GOP-IJARA-10Y-2034-03",
            "issuer": "Government of Pakistan",
            "name": "GOP Ijarah Sukuk 10Y Mar 2034",
            "category": "GOP_SUKUK",
            "currency": "PKR",
            "issue_date": "2024-03-20",
            "maturity_date": "2034-03-20",
            "coupon_rate": 16.5,
            "coupon_frequency": 2,
            "face_value": 100.0,
            "issue_size": 100000000000,
            "shariah_compliant": True,
            "is_active": True,
            "source": "PSX_GIS",
        },
        # PIBs (Pakistan Investment Bonds) - Fixed Rate
        {
            "instrument_id": "PIB-FR-3Y-2027-04",
            "issuer": "Government of Pakistan",
            "name": "PIB Fixed Rate 3Y Apr 2027",
            "category": "PIB",
            "currency": "PKR",
            "issue_date": "2024-04-15",
            "maturity_date": "2027-04-15",
            "coupon_rate": 15.25,
            "coupon_frequency": 2,
            "face_value": 100.0,
            "issue_size": 60000000000,
            "shariah_compliant": False,
            "is_active": True,
            "source": "PSX_GIS",
        },
        {
            "instrument_id": "PIB-FR-5Y-2029-07",
            "issuer": "Government of Pakistan",
            "name": "PIB Fixed Rate 5Y Jul 2029",
            "category": "PIB",
            "currency": "PKR",
            "issue_date": "2024-07-01",
            "maturity_date": "2029-07-01",
            "coupon_rate": 15.75,
            "coupon_frequency": 2,
            "face_value": 100.0,
            "issue_size": 80000000000,
            "shariah_compliant": False,
            "is_active": True,
            "source": "PSX_GIS",
        },
        # T-Bills
        {
            "instrument_id": "TBILL-3M-2026-04",
            "issuer": "Government of Pakistan",
            "name": "T-Bill 3M Apr 2026",
            "category": "TBILL",
            "currency": "PKR",
            "issue_date": "2026-01-15",
            "maturity_date": "2026-04-15",
            "coupon_rate": None,  # Zero coupon
            "coupon_frequency": 0,
            "face_value": 100.0,
            "issue_size": 200000000000,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SBP",
        },
        {
            "instrument_id": "TBILL-6M-2026-07",
            "issuer": "Government of Pakistan",
            "name": "T-Bill 6M Jul 2026",
            "category": "TBILL",
            "currency": "PKR",
            "issue_date": "2026-01-15",
            "maturity_date": "2026-07-15",
            "coupon_rate": None,
            "coupon_frequency": 0,
            "face_value": 100.0,
            "issue_size": 150000000000,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SBP",
        },
        {
            "instrument_id": "TBILL-12M-2027-01",
            "issuer": "Government of Pakistan",
            "name": "T-Bill 12M Jan 2027",
            "category": "TBILL",
            "currency": "PKR",
            "issue_date": "2026-01-15",
            "maturity_date": "2027-01-15",
            "coupon_rate": None,
            "coupon_frequency": 0,
            "face_value": 100.0,
            "issue_size": 100000000000,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SBP",
        },
        # Corporate Sukuk
        {
            "instrument_id": "SUKUK-ENGRO-5Y-2028",
            "issuer": "Engro Corporation",
            "name": "Engro Sukuk 5Y 2028",
            "category": "CORPORATE_SUKUK",
            "currency": "PKR",
            "issue_date": "2023-06-01",
            "maturity_date": "2028-06-01",
            "coupon_rate": 17.5,
            "coupon_frequency": 2,
            "face_value": 100.0,
            "issue_size": 10000000000,
            "shariah_compliant": True,
            "is_active": True,
            "source": "PSX_GIS",
        },
        # TFC (Term Finance Certificate)
        {
            "instrument_id": "TFC-HBLP-3Y-2027",
            "issuer": "Habib Bank Limited",
            "name": "HBL TFC 3Y 2027",
            "category": "TFC",
            "currency": "PKR",
            "issue_date": "2024-02-15",
            "maturity_date": "2027-02-15",
            "coupon_rate": 16.25,
            "coupon_frequency": 4,
            "face_value": 100.0,
            "issue_size": 15000000000,
            "shariah_compliant": False,
            "is_active": True,
            "source": "PSX_GIS",
        },
    ]


def generate_sample_quotes(
    sukuk_list: list[dict[str, Any]],
    days: int = 90,
) -> list[dict[str, Any]]:
    """
    Generate sample quote data for testing.

    Args:
        sukuk_list: List of sukuk instruments
        days: Number of days of historical data

    Returns:
        List of sample quote dicts
    """
    import random
    from datetime import timedelta

    quotes = []
    today = datetime.now().date()

    for sukuk in sukuk_list:
        instrument_id = sukuk["instrument_id"]
        category = sukuk.get("category", "GOP_SUKUK")

        # Base yield depends on category and tenor
        maturity = datetime.strptime(sukuk["maturity_date"], "%Y-%m-%d").date()
        years_to_maturity = (maturity - today).days / 365.0

        # Base yields by category (approximate Pakistani market rates)
        base_yields = {
            "TBILL": 13.0 + min(years_to_maturity * 0.5, 1.0),
            "PIB": 14.5 + min(years_to_maturity * 0.3, 2.0),
            "GOP_SUKUK": 14.0 + min(years_to_maturity * 0.3, 2.0),
            "CORPORATE_SUKUK": 16.0 + min(years_to_maturity * 0.2, 1.5),
            "TFC": 15.5 + min(years_to_maturity * 0.25, 1.5),
        }
        base_ytm = base_yields.get(category, 15.0)

        # Generate daily quotes
        for i in range(days):
            quote_date = today - timedelta(days=days - i - 1)

            # Add some random walk to yield
            drift = (i / days) * random.uniform(-0.5, 0.5)
            ytm = base_ytm + random.gauss(0, 0.1) + drift
            ytm = max(5.0, min(25.0, ytm))  # Clamp to reasonable range

            # Calculate price from yield (simplified)
            coupon_rate = sukuk.get("coupon_rate") or 0
            price = 100 * (1 + (coupon_rate - ytm) / 100 * years_to_maturity * 0.5)
            price = max(80.0, min(120.0, price))

            # Bid-ask spread
            spread = random.uniform(0.02, 0.08)
            bid_yield = ytm + spread / 2
            ask_yield = ytm - spread / 2

            # Volume varies
            base_volume = 1000000 if category == "TBILL" else 500000
            volume = base_volume * random.uniform(0.5, 2.0)

            quotes.append({
                "instrument_id": instrument_id,
                "quote_date": quote_date.isoformat(),
                "clean_price": round(price, 4),
                "dirty_price": round(price * 1.01, 4),  # Simplified accrued
                "yield_to_maturity": round(ytm, 4),
                "bid_yield": round(bid_yield, 4),
                "ask_yield": round(ask_yield, 4),
                "volume": round(volume, 0),
                "source": "SAMPLE",
            })

    return quotes


def generate_sample_yield_curve(
    curve_date: str | None = None,
    curve_name: str = "GOP_SUKUK",
) -> list[dict[str, Any]]:
    """
    Generate sample yield curve points.

    Args:
        curve_date: Date for curve (default: today)
        curve_name: Name of curve (e.g., 'GOP_SUKUK', 'PIB', 'TBILL')

    Returns:
        List of yield curve point dicts
    """
    import random

    if curve_date is None:
        curve_date = datetime.now().date().isoformat()

    # Standard tenors in days
    tenors = [
        (30, "1M"),
        (90, "3M"),
        (180, "6M"),
        (365, "1Y"),
        (730, "2Y"),
        (1095, "3Y"),
        (1825, "5Y"),
        (2555, "7Y"),
        (3650, "10Y"),
    ]

    # Base rates for different curves
    base_rates = {
        "TBILL": {30: 12.5, 90: 13.0, 180: 13.25, 365: 13.5},
        "PIB": {365: 14.0, 730: 14.5, 1095: 15.0, 1825: 15.5, 3650: 16.0},
        "GOP_SUKUK": {365: 13.75, 730: 14.25, 1095: 14.75, 1825: 15.25, 3650: 15.75},
    }

    curve_rates = base_rates.get(curve_name, base_rates["GOP_SUKUK"])
    points = []

    for tenor_days, _ in tenors:
        # Interpolate or use closest rate
        closest_tenor = min(curve_rates.keys(), key=lambda x: abs(x - tenor_days))
        base_rate = curve_rates[closest_tenor]

        # Adjust for tenor difference
        tenor_adj = (tenor_days - closest_tenor) / 365 * 0.3
        rate = base_rate + tenor_adj + random.gauss(0, 0.05)
        rate = round(max(5.0, min(20.0, rate)), 4)

        points.append({
            "curve_name": curve_name,
            "curve_date": curve_date,
            "tenor_days": tenor_days,
            "yield_rate": rate,
            "source": "SAMPLE",
        })

    return points


def save_sukuk_config(config: dict, path: Path | str | None = None) -> None:
    """Save sukuk configuration to JSON file."""
    import json

    if path is None:
        path = DATA_DIR / "sukuk_config.json"

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, default=str)


# Category definitions
SUKUK_CATEGORIES = {
    "GOP_SUKUK": "Government of Pakistan Ijarah Sukuk",
    "PIB": "Pakistan Investment Bonds",
    "TBILL": "Treasury Bills",
    "CORPORATE_SUKUK": "Corporate Sukuk",
    "TFC": "Term Finance Certificates",
}


# Helper functions
def _parse_date(value: str | None) -> str | None:
    """Parse date string to ISO format."""
    if not value or not value.strip():
        return None

    value = value.strip()

    # Try common formats
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def _parse_float(value: str | None, default: float | None = None) -> float | None:
    """Parse string to float."""
    if not value or not str(value).strip():
        return default

    try:
        return float(str(value).strip().replace(",", ""))
    except ValueError:
        return default


def _parse_int(value: str | None, default: int | None = None) -> int | None:
    """Parse string to int."""
    if not value or not str(value).strip():
        return default

    try:
        return int(float(str(value).strip()))
    except ValueError:
        return default


def _parse_bool(value: str | None, default: bool = True) -> bool:
    """Parse string to bool."""
    if not value or not str(value).strip():
        return default

    v = str(value).strip().lower()
    if v in ("1", "true", "yes", "y"):
        return True
    if v in ("0", "false", "no", "n"):
        return False

    return default
