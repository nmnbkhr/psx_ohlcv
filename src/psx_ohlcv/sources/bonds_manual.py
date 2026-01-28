"""
Bonds/Sukuk manual data source module for Phase 3.

This module provides functions for loading bond data from CSV files.
Bonds are primarily quote/yield-based rather than OHLCV.

Manual ingestion approach:
- Bond master data from CSV
- Bond quotes from CSV
- Sample data generation for development/testing

All data is READ-ONLY and for analytics purposes only.
"""

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from random import gauss, uniform

import pandas as pd

# Default bond universe for Pakistan market
DEFAULT_BONDS = [
    # Pakistan Investment Bonds (PIBs) - Government Securities
    {
        "bond_id": "PIB:3Y:2027-01-15",
        "symbol": "PIB-3Y-2027",
        "issuer": "GOP",
        "bond_type": "PIB",
        "is_islamic": 0,
        "face_value": 100,
        "coupon_rate": 0.145,  # 14.5%
        "coupon_frequency": 2,
        "issue_date": "2024-01-15",
        "maturity_date": "2027-01-15",
        "day_count": "ACT/ACT",
    },
    {
        "bond_id": "PIB:5Y:2029-01-15",
        "symbol": "PIB-5Y-2029",
        "issuer": "GOP",
        "bond_type": "PIB",
        "is_islamic": 0,
        "face_value": 100,
        "coupon_rate": 0.155,  # 15.5%
        "coupon_frequency": 2,
        "issue_date": "2024-01-15",
        "maturity_date": "2029-01-15",
        "day_count": "ACT/ACT",
    },
    {
        "bond_id": "PIB:10Y:2034-01-15",
        "symbol": "PIB-10Y-2034",
        "issuer": "GOP",
        "bond_type": "PIB",
        "is_islamic": 0,
        "face_value": 100,
        "coupon_rate": 0.16,  # 16%
        "coupon_frequency": 2,
        "issue_date": "2024-01-15",
        "maturity_date": "2034-01-15",
        "day_count": "ACT/ACT",
    },
    # Treasury Bills (T-Bills) - Zero-coupon
    {
        "bond_id": "TBILL:3M:2026-04-01",
        "symbol": "T-Bill-3M",
        "issuer": "GOP",
        "bond_type": "T-Bill",
        "is_islamic": 0,
        "face_value": 100,
        "coupon_rate": None,  # Zero-coupon
        "coupon_frequency": 0,
        "issue_date": "2026-01-01",
        "maturity_date": "2026-04-01",
        "day_count": "ACT/360",
    },
    {
        "bond_id": "TBILL:6M:2026-07-01",
        "symbol": "T-Bill-6M",
        "issuer": "GOP",
        "bond_type": "T-Bill",
        "is_islamic": 0,
        "face_value": 100,
        "coupon_rate": None,
        "coupon_frequency": 0,
        "issue_date": "2026-01-01",
        "maturity_date": "2026-07-01",
        "day_count": "ACT/360",
    },
    {
        "bond_id": "TBILL:12M:2027-01-01",
        "symbol": "T-Bill-12M",
        "issuer": "GOP",
        "bond_type": "T-Bill",
        "is_islamic": 0,
        "face_value": 100,
        "coupon_rate": None,
        "coupon_frequency": 0,
        "issue_date": "2026-01-01",
        "maturity_date": "2027-01-01",
        "day_count": "ACT/360",
    },
    # Sukuk (Islamic bonds)
    {
        "bond_id": "SUKUK:GOP:3Y:2027-06-15",
        "symbol": "GOP-Sukuk-3Y",
        "issuer": "GOP",
        "bond_type": "Sukuk",
        "is_islamic": 1,
        "face_value": 100,
        "coupon_rate": 0.14,  # 14% profit rate
        "coupon_frequency": 2,
        "issue_date": "2024-06-15",
        "maturity_date": "2027-06-15",
        "day_count": "ACT/ACT",
    },
    {
        "bond_id": "SUKUK:GOP:5Y:2029-06-15",
        "symbol": "GOP-Sukuk-5Y",
        "issuer": "GOP",
        "bond_type": "Sukuk",
        "is_islamic": 1,
        "face_value": 100,
        "coupon_rate": 0.15,
        "coupon_frequency": 2,
        "issue_date": "2024-06-15",
        "maturity_date": "2029-06-15",
        "day_count": "ACT/ACT",
    },
    # Corporate TFC (Term Finance Certificate)
    {
        "bond_id": "TFC:HBL:2027-12-01",
        "symbol": "HBL-TFC-2027",
        "issuer": "HBL",
        "bond_type": "TFC",
        "is_islamic": 0,
        "face_value": 5000,
        "coupon_rate": 0.165,  # 16.5%
        "coupon_frequency": 2,
        "issue_date": "2024-12-01",
        "maturity_date": "2027-12-01",
        "day_count": "30/360",
    },
    {
        "bond_id": "TFC:ENGRO:2028-03-15",
        "symbol": "ENGRO-TFC-2028",
        "issuer": "ENGRO",
        "bond_type": "TFC",
        "is_islamic": 0,
        "face_value": 5000,
        "coupon_rate": 0.17,  # 17%
        "coupon_frequency": 2,
        "issue_date": "2025-03-15",
        "maturity_date": "2028-03-15",
        "day_count": "30/360",
    },
]

# Tenor mappings in months
TENOR_MONTHS = {
    "3M": 3,
    "6M": 6,
    "1Y": 12,
    "2Y": 24,
    "3Y": 36,
    "5Y": 60,
    "7Y": 84,
    "10Y": 120,
    "15Y": 180,
    "20Y": 240,
}


def get_default_bonds() -> list[dict]:
    """Get the default bond universe for seeding."""
    return DEFAULT_BONDS.copy()


def load_bonds_from_csv(csv_path: Path | str) -> list[dict]:
    """
    Load bond master data from CSV file.

    Expected CSV columns:
    - bond_id (required)
    - symbol (required)
    - issuer (required)
    - bond_type (required): PIB, T-Bill, Sukuk, TFC, Corporate
    - is_islamic: 0 or 1
    - face_value: default 100
    - coupon_rate: decimal (e.g., 0.15 for 15%)
    - coupon_frequency: 0, 1, 2, 4
    - issue_date: YYYY-MM-DD
    - maturity_date (required): YYYY-MM-DD
    - day_count: ACT/ACT, ACT/360, 30/360
    - isin: optional ISIN code
    - notes: optional notes

    Args:
        csv_path: Path to CSV file

    Returns:
        List of bond dicts
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Bond CSV file not found: {path}")

    bonds = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bond = {
                "bond_id": row.get("bond_id", "").strip(),
                "symbol": row.get("symbol", "").strip(),
                "issuer": row.get("issuer", "").strip(),
                "bond_type": row.get("bond_type", "").strip(),
                "is_islamic": int(row.get("is_islamic", 0) or 0),
                "face_value": float(row.get("face_value", 100) or 100),
                "coupon_rate": _parse_float(row.get("coupon_rate")),
                "coupon_frequency": int(row.get("coupon_frequency", 2) or 2),
                "issue_date": row.get("issue_date", "").strip() or None,
                "maturity_date": row.get("maturity_date", "").strip(),
                "day_count": row.get("day_count", "ACT/ACT").strip() or "ACT/ACT",
                "isin": row.get("isin", "").strip() or None,
                "notes": row.get("notes", "").strip() or None,
                "currency": row.get("currency", "PKR").strip() or "PKR",
                "source": "CSV",
            }

            # Validate required fields
            if bond["bond_id"] and bond["symbol"] and bond["maturity_date"]:
                bonds.append(bond)

    return bonds


def load_quotes_from_csv(csv_path: Path | str) -> list[dict]:
    """
    Load bond quotes from CSV file.

    Expected CSV columns:
    - bond_id (required)
    - date (required): YYYY-MM-DD
    - price: clean price as % of face value
    - ytm: yield to maturity as decimal
    - bid_yield, ask_yield: optional
    - bid_price, ask_price: optional
    - volume: optional

    Args:
        csv_path: Path to CSV file

    Returns:
        List of quote dicts
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Quotes CSV file not found: {path}")

    quotes = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            quote = {
                "bond_id": row.get("bond_id", "").strip(),
                "date": row.get("date", "").strip(),
                "price": _parse_float(row.get("price")),
                "dirty_price": _parse_float(row.get("dirty_price")),
                "ytm": _parse_float(row.get("ytm")),
                "bid_yield": _parse_float(row.get("bid_yield")),
                "ask_yield": _parse_float(row.get("ask_yield")),
                "bid_price": _parse_float(row.get("bid_price")),
                "ask_price": _parse_float(row.get("ask_price")),
                "volume": _parse_float(row.get("volume")),
                "source": "CSV",
            }

            # Validate required fields
            if quote["bond_id"] and quote["date"]:
                quotes.append(quote)

    return quotes


def generate_sample_quotes(
    bonds: list[dict],
    start_date: str | None = None,
    end_date: str | None = None,
    days: int = 90,
) -> list[dict]:
    """
    Generate sample bond quotes for development/testing.

    Args:
        bonds: List of bond master data
        start_date: Start date (default: today - days)
        end_date: End date (default: today)
        days: Number of days to generate

    Returns:
        List of quote dicts
    """
    if end_date is None:
        end_dt = datetime.now()
    else:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    if start_date is None:
        start_dt = end_dt - timedelta(days=days)
    else:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")

    quotes = []

    for bond in bonds:
        bond_id = bond.get("bond_id")
        coupon_rate = bond.get("coupon_rate")
        bond_type = bond.get("bond_type")

        # Base yield based on bond type and tenor
        maturity = bond.get("maturity_date")
        if maturity:
            mat_dt = datetime.strptime(maturity, "%Y-%m-%d")
            years_to_mat = (mat_dt - end_dt).days / 365.25
        else:
            years_to_mat = 5

        # Base yield curve (simplified)
        if bond_type == "T-Bill":
            base_yield = 0.13 + 0.001 * min(years_to_mat * 12, 12)
        elif bond_type == "Sukuk":
            base_yield = 0.135 + 0.005 * min(years_to_mat, 10)
        elif bond_type == "TFC":
            base_yield = 0.16 + 0.003 * min(years_to_mat, 10)
        else:  # PIB
            base_yield = 0.14 + 0.004 * min(years_to_mat, 10)

        # Generate daily quotes
        current_dt = start_dt
        prev_yield = base_yield
        while current_dt <= end_dt:
            # Skip weekends
            if current_dt.weekday() >= 5:
                current_dt += timedelta(days=1)
                continue

            # Random walk for yield
            yield_change = gauss(0, 0.001)  # Daily volatility ~0.1%
            ytm = max(0.05, min(0.25, prev_yield + yield_change))
            prev_yield = ytm

            # Calculate price from yield (simplified)
            if coupon_rate is None:
                # Zero-coupon bond
                price = 100 / ((1 + ytm) ** years_to_mat)
            else:
                # Coupon bond (simplified price formula)
                price = _simple_bond_price(coupon_rate, ytm, years_to_mat)

            # Bid/ask spread
            spread = uniform(0.001, 0.003)
            bid_yield = ytm + spread / 2
            ask_yield = ytm - spread / 2

            quote = {
                "bond_id": bond_id,
                "date": current_dt.strftime("%Y-%m-%d"),
                "price": round(price, 4),
                "ytm": round(ytm, 6),
                "bid_yield": round(bid_yield, 6),
                "ask_yield": round(ask_yield, 6),
                "source": "SAMPLE",
            }
            quotes.append(quote)

            current_dt += timedelta(days=1)

    return quotes


def _simple_bond_price(coupon_rate: float, ytm: float, years: float) -> float:
    """Simplified bond price calculation."""
    if years <= 0:
        return 100.0

    # Semi-annual payments
    periods = int(years * 2)
    if periods <= 0:
        return 100.0

    coupon_payment = coupon_rate * 100 / 2
    ytm_semi = ytm / 2

    if ytm_semi == 0:
        return 100 + coupon_payment * periods

    # PV of coupons + PV of principal
    pv_coupons = coupon_payment * (1 - (1 + ytm_semi) ** (-periods)) / ytm_semi
    pv_principal = 100 / ((1 + ytm_semi) ** periods)

    return pv_coupons + pv_principal


def _parse_float(val: str | None) -> float | None:
    """Parse a float value from string, returning None for empty/invalid."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def normalize_bond_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize bond master DataFrame to standard schema.

    Args:
        df: Input DataFrame

    Returns:
        Normalized DataFrame
    """
    required_cols = ["bond_id", "symbol", "issuer", "bond_type", "maturity_date"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Add default values
    if "is_islamic" not in df.columns:
        df["is_islamic"] = 0
    if "face_value" not in df.columns:
        df["face_value"] = 100
    if "coupon_frequency" not in df.columns:
        df["coupon_frequency"] = 2
    if "day_count" not in df.columns:
        df["day_count"] = "ACT/ACT"
    if "currency" not in df.columns:
        df["currency"] = "PKR"
    if "is_active" not in df.columns:
        df["is_active"] = 1
    if "source" not in df.columns:
        df["source"] = "CSV"

    return df


def normalize_quote_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize quote DataFrame to standard schema.

    Args:
        df: Input DataFrame

    Returns:
        Normalized DataFrame
    """
    required_cols = ["bond_id", "date"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Add source if missing
    if "source" not in df.columns:
        df["source"] = "CSV"

    return df


def save_bonds_config(config: dict, path: Path | str | None = None) -> Path:
    """
    Save bonds configuration to JSON file.

    Args:
        config: Configuration dict
        path: Output path (default: data/bonds/bonds_config.json)

    Returns:
        Path to saved file
    """
    if path is None:
        path = Path(__file__).parent.parent.parent.parent / "data" / "bonds"
        path.mkdir(parents=True, exist_ok=True)
        path = path / "bonds_config.json"
    else:
        path = Path(path)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    return path


def load_bonds_config(path: Path | str | None = None) -> dict:
    """
    Load bonds configuration from JSON file.

    Args:
        path: Config path (default: data/bonds/bonds_config.json)

    Returns:
        Configuration dict
    """
    if path is None:
        path = Path(__file__).parent.parent.parent.parent / "data" / "bonds"
        path = path / "bonds_config.json"
    else:
        path = Path(path)

    if not path.exists():
        return {"bonds": [], "sources": []}

    with open(path, encoding="utf-8") as f:
        return json.load(f)


def get_csv_template_bonds() -> str:
    """Get CSV template header for bond master data."""
    # Full template available in data/bonds/bonds_master_template.csv
    header = (
        "bond_id,symbol,issuer,bond_type,is_islamic,face_value,"
        "coupon_rate,coupon_frequency,issue_date,maturity_date,"
        "day_count,isin,notes"
    )
    return header + "\n"


def get_csv_template_quotes() -> str:
    """Get CSV template header for bond quotes."""
    # Full template available in data/bonds/quotes_template.csv
    header = (
        "bond_id,date,price,ytm,bid_yield,ask_yield,"
        "bid_price,ask_price,volume"
    )
    return header + "\n"
