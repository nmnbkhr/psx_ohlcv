"""
Manual CSV data source for Fixed Income instruments.

This module provides CSV loaders for fixed income instruments, quotes, and curves.
Data is loaded from user-provided CSV files in the data/fixed_income/ directory.

Instrument categories:
- MTB: Market Treasury Bills (T-Bills)
- PIB: Pakistan Investment Bonds
- GOP_SUKUK: Government of Pakistan Ijara Sukuk (GIS)
- CORP_BOND: Corporate Bonds
- CORP_SUKUK: Corporate Sukuk
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# Default data directory
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data" / "fixed_income"

# Category definitions
FI_CATEGORIES = {
    "MTB": "Market Treasury Bills",
    "PIB": "Pakistan Investment Bonds",
    "GOP_SUKUK": "Government of Pakistan Ijara Sukuk",
    "CORP_BOND": "Corporate Bonds",
    "CORP_SUKUK": "Corporate Sukuk",
}

# Curve names
CURVE_NAMES = {
    "PKR_MTB": "PKR T-Bill Curve",
    "PKR_PIB": "PKR PIB Curve",
    "PKR_GOP_SUKUK": "PKR GOP Sukuk Curve",
}


def load_fi_instruments_csv(path: Path | str) -> tuple[list[dict], list[str]]:
    """
    Load fixed income instruments from CSV file.

    Expected CSV columns:
        instrument_id, isin, issuer, name, category, issue_date, maturity_date,
        coupon_rate, coupon_frequency, day_count, face_value, shariah_compliant,
        is_active

    Args:
        path: Path to CSV file

    Returns:
        Tuple of (list of instrument dicts, list of error messages)
    """
    path = Path(path)
    if not path.exists():
        return [], [f"File not found: {path}"]

    instruments = []
    errors = []

    try:
        df = pd.read_csv(path, dtype=str)
        df = df.fillna("")

        for idx, row in df.iterrows():
            line_num = idx + 2  # Account for header

            # Required fields
            instrument_id = str(row.get("instrument_id", "")).strip()
            maturity_date = _parse_date(row.get("maturity_date"))

            if not instrument_id:
                errors.append(f"Line {line_num}: Missing instrument_id")
                continue

            if not maturity_date:
                errors.append(f"Line {line_num}: Missing or invalid maturity_date")
                continue

            # Parse optional fields
            coupon_rate = pd.to_numeric(row.get("coupon_rate"), errors="coerce")
            coupon_frequency = pd.to_numeric(
                row.get("coupon_frequency"), errors="coerce"
            )
            face_value = pd.to_numeric(row.get("face_value"), errors="coerce")

            # Handle shariah_compliant
            shariah_val = str(row.get("shariah_compliant", "0")).strip().lower()
            shariah_compliant = shariah_val in ("1", "true", "yes", "y")

            # Handle is_active
            active_val = str(row.get("is_active", "1")).strip().lower()
            is_active = active_val not in ("0", "false", "no", "n")

            instrument = {
                "instrument_id": instrument_id,
                "isin": str(row.get("isin", "")).strip() or None,
                "issuer": str(row.get("issuer", "")).strip() or "GOVT_OF_PAKISTAN",
                "name": str(row.get("name", "")).strip() or instrument_id,
                "category": str(row.get("category", "")).strip().upper() or "MTB",
                "currency": str(row.get("currency", "")).strip() or "PKR",
                "issue_date": _parse_date(row.get("issue_date")),
                "maturity_date": maturity_date,
                "coupon_rate": coupon_rate if pd.notna(coupon_rate) else None,
                "coupon_frequency": (
                    int(coupon_frequency) if pd.notna(coupon_frequency) else None
                ),
                "day_count": str(row.get("day_count", "")).strip() or "ACT/365",
                "face_value": face_value if pd.notna(face_value) else 100.0,
                "shariah_compliant": shariah_compliant,
                "is_active": is_active,
                "source": "CSV",
            }

            instruments.append(instrument)

    except Exception as e:
        errors.append(f"Error reading CSV: {e}")

    return instruments, errors


def load_fi_quotes_csv(path: Path | str) -> tuple[list[dict], list[str]]:
    """
    Load fixed income quotes from CSV file.

    Expected CSV columns:
        instrument_id, quote_date, clean_price, ytm, bid, ask, volume

    Args:
        path: Path to CSV file

    Returns:
        Tuple of (list of quote dicts, list of error messages)
    """
    path = Path(path)
    if not path.exists():
        return [], [f"File not found: {path}"]

    quotes = []
    errors = []

    try:
        df = pd.read_csv(path, dtype=str)
        df = df.fillna("")

        for idx, row in df.iterrows():
            line_num = idx + 2

            instrument_id = str(row.get("instrument_id", "")).strip()
            quote_date = _parse_date(row.get("quote_date"))

            if not instrument_id:
                errors.append(f"Line {line_num}: Missing instrument_id")
                continue

            if not quote_date:
                errors.append(f"Line {line_num}: Missing or invalid quote_date")
                continue

            # Parse numeric fields
            clean_price = pd.to_numeric(row.get("clean_price"), errors="coerce")
            ytm = pd.to_numeric(row.get("ytm"), errors="coerce")
            bid = pd.to_numeric(row.get("bid"), errors="coerce")
            ask = pd.to_numeric(row.get("ask"), errors="coerce")
            volume = pd.to_numeric(row.get("volume"), errors="coerce")

            quote = {
                "instrument_id": instrument_id,
                "quote_date": quote_date,
                "clean_price": clean_price if pd.notna(clean_price) else None,
                "ytm": ytm if pd.notna(ytm) else None,
                "bid": bid if pd.notna(bid) else None,
                "ask": ask if pd.notna(ask) else None,
                "volume": volume if pd.notna(volume) else None,
                "source": "CSV",
            }

            quotes.append(quote)

    except Exception as e:
        errors.append(f"Error reading CSV: {e}")

    return quotes, errors


def load_fi_curves_csv(path: Path | str) -> tuple[list[dict], list[str]]:
    """
    Load fixed income yield curves from CSV file.

    Expected CSV columns:
        curve_name, curve_date, tenor_days, rate, source

    Args:
        path: Path to CSV file

    Returns:
        Tuple of (list of curve point dicts, list of error messages)
    """
    path = Path(path)
    if not path.exists():
        return [], [f"File not found: {path}"]

    points = []
    errors = []

    try:
        df = pd.read_csv(path, dtype=str)
        df = df.fillna("")

        for idx, row in df.iterrows():
            line_num = idx + 2

            curve_name = str(row.get("curve_name", "")).strip()
            curve_date = _parse_date(row.get("curve_date"))
            tenor_days = pd.to_numeric(row.get("tenor_days"), errors="coerce")
            rate = pd.to_numeric(row.get("rate"), errors="coerce")

            if not curve_name:
                errors.append(f"Line {line_num}: Missing curve_name")
                continue

            if not curve_date:
                errors.append(f"Line {line_num}: Missing or invalid curve_date")
                continue

            if pd.isna(tenor_days):
                errors.append(f"Line {line_num}: Missing or invalid tenor_days")
                continue

            if pd.isna(rate):
                errors.append(f"Line {line_num}: Missing or invalid rate")
                continue

            point = {
                "curve_name": curve_name,
                "curve_date": curve_date,
                "tenor_days": int(tenor_days),
                "rate": float(rate),
                "source": str(row.get("source", "")).strip() or "CSV",
            }

            points.append(point)

    except Exception as e:
        errors.append(f"Error reading CSV: {e}")

    return points, errors


def get_default_instruments() -> list[dict]:
    """
    Get sample fixed income instruments for initialization.

    Returns list of government securities for demo/testing.
    """
    return [
        # T-Bills (zero coupon)
        {
            "instrument_id": "MTB-3M-2026-04",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "T-Bill 3-Month Apr 2026",
            "category": "MTB",
            "currency": "PKR",
            "issue_date": "2026-01-15",
            "maturity_date": "2026-04-15",
            "coupon_rate": None,
            "coupon_frequency": 0,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SAMPLE",
        },
        {
            "instrument_id": "MTB-6M-2026-07",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "T-Bill 6-Month Jul 2026",
            "category": "MTB",
            "currency": "PKR",
            "issue_date": "2026-01-15",
            "maturity_date": "2026-07-15",
            "coupon_rate": None,
            "coupon_frequency": 0,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SAMPLE",
        },
        {
            "instrument_id": "MTB-12M-2027-01",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "T-Bill 12-Month Jan 2027",
            "category": "MTB",
            "currency": "PKR",
            "issue_date": "2026-01-15",
            "maturity_date": "2027-01-15",
            "coupon_rate": None,
            "coupon_frequency": 0,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SAMPLE",
        },
        # PIBs (coupon bonds)
        {
            "instrument_id": "PIB-3Y-2027-06",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "PIB 3-Year Jun 2027",
            "category": "PIB",
            "currency": "PKR",
            "issue_date": "2024-06-15",
            "maturity_date": "2027-06-15",
            "coupon_rate": 0.155,  # 15.5%
            "coupon_frequency": 2,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SAMPLE",
        },
        {
            "instrument_id": "PIB-5Y-2029-01",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "PIB 5-Year Jan 2029",
            "category": "PIB",
            "currency": "PKR",
            "issue_date": "2024-01-15",
            "maturity_date": "2029-01-15",
            "coupon_rate": 0.16,  # 16%
            "coupon_frequency": 2,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SAMPLE",
        },
        {
            "instrument_id": "PIB-10Y-2034-03",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "PIB 10-Year Mar 2034",
            "category": "PIB",
            "currency": "PKR",
            "issue_date": "2024-03-20",
            "maturity_date": "2034-03-20",
            "coupon_rate": 0.165,  # 16.5%
            "coupon_frequency": 2,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": False,
            "is_active": True,
            "source": "SAMPLE",
        },
        # GOP Sukuk (shariah-compliant)
        {
            "instrument_id": "GIS-3Y-2027-06",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "GOP Ijara Sukuk 3Y Jun 2027",
            "category": "GOP_SUKUK",
            "currency": "PKR",
            "issue_date": "2024-06-15",
            "maturity_date": "2027-06-15",
            "coupon_rate": 0.155,
            "coupon_frequency": 2,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": True,
            "is_active": True,
            "source": "SAMPLE",
        },
        {
            "instrument_id": "GIS-5Y-2029-01",
            "issuer": "GOVT_OF_PAKISTAN",
            "name": "GOP Ijara Sukuk 5Y Jan 2029",
            "category": "GOP_SUKUK",
            "currency": "PKR",
            "issue_date": "2024-01-15",
            "maturity_date": "2029-01-15",
            "coupon_rate": 0.16,
            "coupon_frequency": 2,
            "day_count": "ACT/365",
            "face_value": 100.0,
            "shariah_compliant": True,
            "is_active": True,
            "source": "SAMPLE",
        },
    ]


def get_default_quotes(instruments: list[dict], days: int = 30) -> list[dict]:
    """
    Generate sample quote data for instruments.

    Args:
        instruments: List of instrument dicts
        days: Number of days of historical data

    Returns:
        List of sample quote dicts
    """
    import random
    from datetime import timedelta

    quotes = []
    today = datetime.now().date()

    for inst in instruments:
        instrument_id = inst["instrument_id"]
        category = inst.get("category", "MTB")
        coupon_rate = inst.get("coupon_rate") or 0
        maturity_str = inst.get("maturity_date", "")

        if not maturity_str:
            continue

        try:
            maturity = datetime.strptime(maturity_str, "%Y-%m-%d").date()
            years_to_mat = (maturity - today).days / 365.0
        except Exception:
            years_to_mat = 1.0

        # Base yield depends on category and tenor
        base_yields = {
            "MTB": 0.13 + min(years_to_mat * 0.005, 0.01),
            "PIB": 0.145 + min(years_to_mat * 0.003, 0.02),
            "GOP_SUKUK": 0.14 + min(years_to_mat * 0.003, 0.02),
        }
        base_ytm = base_yields.get(category, 0.15)

        random.seed(hash(instrument_id))

        for i in range(days):
            quote_date = today - timedelta(days=days - i - 1)

            # Skip weekends
            if quote_date.weekday() >= 5:
                continue

            # Random walk on yield
            ytm = base_ytm + random.gauss(0, 0.001)
            ytm += (i / days) * random.uniform(-0.005, 0.005)
            ytm = max(0.05, min(0.25, ytm))

            # Calculate price from yield (simplified)
            if coupon_rate and coupon_rate > 0:
                price = 100 * (1 + (coupon_rate - ytm) * years_to_mat * 0.5)
            else:
                # Zero coupon: discount from face value
                price = 100 / ((1 + ytm) ** years_to_mat)

            price = max(80.0, min(120.0, price))

            # Bid-ask spread
            spread = random.uniform(0.001, 0.003)
            bid = ytm + spread / 2
            ask = ytm - spread / 2

            quotes.append({
                "instrument_id": instrument_id,
                "quote_date": quote_date.isoformat(),
                "clean_price": round(price, 4),
                "ytm": round(ytm, 6),
                "bid": round(bid, 6),
                "ask": round(ask, 6),
                "volume": round(random.uniform(100, 1000) * 1e6, 0),
                "source": "SAMPLE",
            })

    return quotes


def get_default_curves(curve_date: str | None = None) -> list[dict]:
    """
    Generate sample yield curve data.

    Args:
        curve_date: Date for curves (default: today)

    Returns:
        List of curve point dicts
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

    curves_config = {
        "PKR_MTB": {"short": 0.13, "long": 0.14, "max_tenor": 365},
        "PKR_PIB": {"short": 0.14, "long": 0.165, "max_tenor": 3650},
        "PKR_GOP_SUKUK": {"short": 0.135, "long": 0.16, "max_tenor": 3650},
    }

    points = []

    for curve_name, config in curves_config.items():
        short_rate = config["short"]
        long_rate = config["long"]
        max_tenor = config["max_tenor"]

        for tenor_days, _ in tenors:
            if tenor_days > max_tenor:
                continue

            # Linear interpolation between short and long rates
            t_frac = tenor_days / max_tenor
            rate = short_rate + (long_rate - short_rate) * t_frac
            rate += random.gauss(0, 0.001)  # Small noise

            points.append({
                "curve_name": curve_name,
                "curve_date": curve_date,
                "tenor_days": tenor_days,
                "rate": round(rate, 6),
                "source": "SAMPLE",
            })

    return points


def create_csv_templates(output_dir: Path | str | None = None) -> dict[str, str]:
    """
    Create CSV template files for manual data entry.

    Args:
        output_dir: Directory for output files

    Returns:
        Dict mapping template name to file path
    """
    if output_dir is None:
        output_dir = DATA_DIR

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    templates = {}

    # Instruments template
    inst_path = output_dir / "fi_instruments.csv"
    # Build CSV content with header and data rows
    inst_header = (
        "instrument_id,isin,issuer,name,category,"
        "issue_date,maturity_date,coupon_rate,coupon_frequency,"
        "day_count,face_value,shariah_compliant,is_active"
    )
    inst_rows = [
        "MTB-3M-2026-04,,GOVT_OF_PAKISTAN,T-Bill 3M Apr 2026,MTB,"
        "2026-01-15,2026-04-15,,,ACT/365,100,0,1",
        "PIB-3Y-2027-06,,GOVT_OF_PAKISTAN,PIB 3Y Jun 2027,PIB,"
        "2024-06-15,2027-06-15,0.155,2,ACT/365,100,0,1",
        "GIS-3Y-2027-06,,GOVT_OF_PAKISTAN,GOP Sukuk 3Y Jun 2027,GOP_SUKUK,"
        "2024-06-15,2027-06-15,0.155,2,ACT/365,100,1,1",
    ]
    inst_content = inst_header + "\n" + "\n".join(inst_rows) + "\n"
    with open(inst_path, "w") as f:
        f.write(inst_content)
    templates["instruments"] = str(inst_path)

    # Quotes template
    quotes_path = output_dir / "fi_quotes.csv"
    quotes_content = """instrument_id,quote_date,clean_price,ytm,bid,ask,volume
MTB-3M-2026-04,2026-01-15,96.75,0.135,0.1355,0.1345,500000000
PIB-3Y-2027-06,2026-01-15,98.50,0.16,0.1605,0.1595,300000000
GIS-3Y-2027-06,2026-01-15,98.75,0.158,0.1585,0.1575,200000000
"""
    with open(quotes_path, "w") as f:
        f.write(quotes_content)
    templates["quotes"] = str(quotes_path)

    # Curves template
    curves_path = output_dir / "fi_curves.csv"
    curves_content = """curve_name,curve_date,tenor_days,rate,source
PKR_MTB,2026-01-15,30,0.13,MANUAL
PKR_MTB,2026-01-15,90,0.132,MANUAL
PKR_MTB,2026-01-15,180,0.134,MANUAL
PKR_MTB,2026-01-15,365,0.135,MANUAL
PKR_PIB,2026-01-15,365,0.145,MANUAL
PKR_PIB,2026-01-15,730,0.15,MANUAL
PKR_PIB,2026-01-15,1095,0.155,MANUAL
PKR_PIB,2026-01-15,1825,0.16,MANUAL
PKR_PIB,2026-01-15,3650,0.165,MANUAL
PKR_GOP_SUKUK,2026-01-15,365,0.14,MANUAL
PKR_GOP_SUKUK,2026-01-15,1095,0.15,MANUAL
PKR_GOP_SUKUK,2026-01-15,1825,0.155,MANUAL
PKR_GOP_SUKUK,2026-01-15,3650,0.16,MANUAL
"""
    with open(curves_path, "w") as f:
        f.write(curves_content)
    templates["curves"] = str(curves_path)

    # README
    readme_path = output_dir / "README.md"
    readme_content = """# Fixed Income Data

This directory contains CSV templates for fixed income data.

## Files

### fi_instruments.csv
Master data for fixed income instruments.

**Categories:**
- MTB: Market Treasury Bills (T-Bills)
- PIB: Pakistan Investment Bonds
- GOP_SUKUK: Government of Pakistan Ijara Sukuk
- CORP_BOND: Corporate Bonds
- CORP_SUKUK: Corporate Sukuk

**Coupon Rate:** Use decimal (e.g., 0.155 for 15.5%)

### fi_quotes.csv
Daily quotes for instruments.

**YTM:** Yield to maturity as decimal (e.g., 0.16 for 16%)

### fi_curves.csv
Yield curve points by tenor.

**Curve Names:**
- PKR_MTB: T-Bill curve
- PKR_PIB: PIB curve
- PKR_GOP_SUKUK: GOP Sukuk curve

## Loading Data

```bash
psxsync fixed-income load \\
    --master data/fixed_income/fi_instruments.csv \\
    --quotes data/fixed_income/fi_quotes.csv \\
    --curves data/fixed_income/fi_curves.csv
```
"""
    with open(readme_path, "w") as f:
        f.write(readme_content)
    templates["readme"] = str(readme_path)

    return templates


# Helper functions
def _parse_date(value: Any) -> str | None:
    """Parse date string to ISO format YYYY-MM-DD."""
    if not value or pd.isna(value):
        return None

    value = str(value).strip()
    if not value:
        return None

    # Try common formats
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    return None
