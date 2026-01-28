"""
MUFAP (Mutual Funds Association of Pakistan) data source module for Phase 2.5.

This module provides mutual fund data fetching from various sources:
- MUFAP website (primary source)
- Sample/mock data - for testing when website unavailable

Mutual fund data is used for analytics only, NOT for investment recommendations.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from ..config import DATA_ROOT

# MUFAP API endpoints (discovered from website)
MUFAP_BASE_URL = "https://www.mufap.com.pk"

# Fund categories (MUFAP standard)
FUND_CATEGORIES = [
    {"code": "Equity", "name": "Equity"},
    {"code": "Money Market", "name": "Money Market"},
    {"code": "Income", "name": "Income"},
    {"code": "Balanced", "name": "Balanced"},
    {"code": "Asset Allocation", "name": "Asset Allocation"},
    {"code": "Islamic Equity", "name": "Islamic Equity"},
    {"code": "Islamic Income", "name": "Islamic Income"},
    {"code": "Islamic Money Market", "name": "Islamic Money Market"},
    {"code": "VPS", "name": "Voluntary Pension"},
    {"code": "ETF", "name": "Exchange Traded"},
    {"code": "Fund of Funds", "name": "Fund of Funds"},
    {"code": "Capital Protected", "name": "Capital Protected"},
]

# Config file paths
MUFAP_CONFIG_PATH = DATA_ROOT / "mufap_config.json"

# Default mutual funds - major AMCs and popular funds
DEFAULT_MUTUAL_FUNDS = [
    # ABL Asset Management
    {
        "fund_id": "MUFAP:ABL-ISF",
        "symbol": "ABL-ISF",
        "fund_name": "ABL Islamic Stock Fund",
        "amc_code": "ABL",
        "amc_name": "ABL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2010-01-01",
        "expense_ratio": 2.5,
        "management_fee": 2.0,
    },
    {
        "fund_id": "MUFAP:ABL-CSF",
        "symbol": "ABL-CSF",
        "fund_name": "ABL Cash Fund",
        "amc_code": "ABL",
        "amc_name": "ABL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Money Market",
        "is_shariah": 0,
        "launch_date": "2009-01-01",
        "expense_ratio": 1.0,
        "management_fee": 0.75,
    },
    # Alfalah GHP
    {
        "fund_id": "MUFAP:AGISF",
        "symbol": "AGISF",
        "fund_name": "Alfalah GHP Islamic Stock Fund",
        "amc_code": "ALFALAH",
        "amc_name": "Alfalah GHP Investment Management",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2007-01-01",
        "expense_ratio": 2.5,
        "management_fee": 2.0,
    },
    {
        "fund_id": "MUFAP:AGCSF",
        "symbol": "AGCSF",
        "fund_name": "Alfalah GHP Cash Fund",
        "amc_code": "ALFALAH",
        "amc_name": "Alfalah GHP Investment Management",
        "fund_type": "OPEN_END",
        "category": "Money Market",
        "is_shariah": 0,
        "launch_date": "2008-01-01",
        "expense_ratio": 1.0,
        "management_fee": 0.75,
    },
    # MCB Arif Habib
    {
        "fund_id": "MUFAP:MCB-ISF",
        "symbol": "MCB-ISF",
        "fund_name": "MCB Pakistan Islamic Stock Fund",
        "amc_code": "MCB",
        "amc_name": "MCB-Arif Habib Savings and Investments",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2009-01-01",
        "expense_ratio": 2.5,
        "management_fee": 2.0,
    },
    {
        "fund_id": "MUFAP:MCB-CF",
        "symbol": "MCB-CF",
        "fund_name": "MCB Cash Management Optimizer",
        "amc_code": "MCB",
        "amc_name": "MCB-Arif Habib Savings and Investments",
        "fund_type": "OPEN_END",
        "category": "Money Market",
        "is_shariah": 0,
        "launch_date": "2008-01-01",
        "expense_ratio": 0.8,
        "management_fee": 0.5,
    },
    # NIT
    {
        "fund_id": "MUFAP:NITEF",
        "symbol": "NITEF",
        "fund_name": "NIT Equity Fund",
        "amc_code": "NIT",
        "amc_name": "National Investment Trust",
        "fund_type": "OPEN_END",
        "category": "Equity",
        "is_shariah": 0,
        "launch_date": "2000-01-01",
        "expense_ratio": 2.0,
        "management_fee": 1.5,
    },
    {
        "fund_id": "MUFAP:NITISF",
        "symbol": "NITISF",
        "fund_name": "NIT Islamic Stock Fund",
        "amc_code": "NIT",
        "amc_name": "National Investment Trust",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2005-01-01",
        "expense_ratio": 2.0,
        "management_fee": 1.5,
    },
    # HBL Asset Management
    {
        "fund_id": "MUFAP:HBL-ISF",
        "symbol": "HBL-ISF",
        "fund_name": "HBL Islamic Stock Fund",
        "amc_code": "HBL",
        "amc_name": "HBL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2010-01-01",
        "expense_ratio": 2.5,
        "management_fee": 2.0,
    },
    {
        "fund_id": "MUFAP:HBL-MMF",
        "symbol": "HBL-MMF",
        "fund_name": "HBL Money Market Fund",
        "amc_code": "HBL",
        "amc_name": "HBL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Money Market",
        "is_shariah": 0,
        "launch_date": "2009-01-01",
        "expense_ratio": 0.9,
        "management_fee": 0.5,
    },
    # UBL Fund Managers
    {
        "fund_id": "MUFAP:UBL-ISF",
        "symbol": "UBL-ISF",
        "fund_name": "UBL Islamic Stock Fund",
        "amc_code": "UBL",
        "amc_name": "UBL Fund Managers",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2008-01-01",
        "expense_ratio": 2.5,
        "management_fee": 2.0,
    },
    {
        "fund_id": "MUFAP:UBL-LF",
        "symbol": "UBL-LF",
        "fund_name": "UBL Liquidity Plus Fund",
        "amc_code": "UBL",
        "amc_name": "UBL Fund Managers",
        "fund_type": "OPEN_END",
        "category": "Money Market",
        "is_shariah": 0,
        "launch_date": "2007-01-01",
        "expense_ratio": 0.8,
        "management_fee": 0.5,
    },
    # Faysal Asset Management
    {
        "fund_id": "MUFAP:FAYSAL-ISF",
        "symbol": "FAYSAL-ISF",
        "fund_name": "Faysal Islamic Stock Fund",
        "amc_code": "FAYSAL",
        "amc_name": "Faysal Asset Management",
        "fund_type": "OPEN_END",
        "category": "Islamic Equity",
        "is_shariah": 1,
        "launch_date": "2012-01-01",
        "expense_ratio": 2.5,
        "management_fee": 2.0,
    },
    # Income Funds
    {
        "fund_id": "MUFAP:ABL-IF",
        "symbol": "ABL-IF",
        "fund_name": "ABL Income Fund",
        "amc_code": "ABL",
        "amc_name": "ABL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Income",
        "is_shariah": 0,
        "launch_date": "2008-01-01",
        "expense_ratio": 1.5,
        "management_fee": 1.0,
    },
    {
        "fund_id": "MUFAP:MCB-IF",
        "symbol": "MCB-IF",
        "fund_name": "MCB Pakistan Income Fund",
        "amc_code": "MCB",
        "amc_name": "MCB-Arif Habib Savings and Investments",
        "fund_type": "OPEN_END",
        "category": "Income",
        "is_shariah": 0,
        "launch_date": "2010-01-01",
        "expense_ratio": 1.5,
        "management_fee": 1.0,
    },
    # Islamic Income
    {
        "fund_id": "MUFAP:ABL-IIF",
        "symbol": "ABL-IIF",
        "fund_name": "ABL Islamic Income Fund",
        "amc_code": "ABL",
        "amc_name": "ABL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Islamic Income",
        "is_shariah": 1,
        "launch_date": "2011-01-01",
        "expense_ratio": 1.5,
        "management_fee": 1.0,
    },
    # Balanced Fund
    {
        "fund_id": "MUFAP:ABL-BAL",
        "symbol": "ABL-BAL",
        "fund_name": "ABL Balanced Fund",
        "amc_code": "ABL",
        "amc_name": "ABL Asset Management",
        "fund_type": "OPEN_END",
        "category": "Balanced",
        "is_shariah": 0,
        "launch_date": "2009-01-01",
        "expense_ratio": 2.0,
        "management_fee": 1.5,
    },
    # VPS Funds
    {
        "fund_id": "MUFAP:ABL-VPS-EQ",
        "symbol": "ABL-VPS-EQ",
        "fund_name": "ABL Pension Fund - Equity Sub-Fund",
        "amc_code": "ABL",
        "amc_name": "ABL Asset Management",
        "fund_type": "VPS",
        "category": "VPS",
        "is_shariah": 0,
        "launch_date": "2015-01-01",
        "expense_ratio": 2.0,
        "management_fee": 1.5,
    },
    {
        "fund_id": "MUFAP:MCB-VPS-ISL",
        "symbol": "MCB-VPS-ISL",
        "fund_name": "MCB Islamic Pension Fund",
        "amc_code": "MCB",
        "amc_name": "MCB-Arif Habib Savings and Investments",
        "fund_type": "VPS",
        "category": "VPS",
        "is_shariah": 1,
        "launch_date": "2016-01-01",
        "expense_ratio": 2.0,
        "management_fee": 1.5,
    },
]


def get_fund_categories() -> list[dict]:
    """Get all MUFAP fund categories."""
    return FUND_CATEGORIES.copy()


def get_default_funds() -> list[dict]:
    """Get the default mutual funds configuration."""
    return DEFAULT_MUTUAL_FUNDS.copy()


def load_mufap_config(config_path: Path | None = None) -> dict:
    """
    Load MUFAP configuration from JSON file.

    Args:
        config_path: Path to config file, or None for default

    Returns:
        Config dict with 'funds' key
    """
    path = config_path or MUFAP_CONFIG_PATH

    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    return {"funds": DEFAULT_MUTUAL_FUNDS}


def save_mufap_config(config: dict, config_path: Path | None = None) -> bool:
    """Save MUFAP configuration to JSON file."""
    path = config_path or MUFAP_CONFIG_PATH

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        return True
    except IOError:
        return False


def fetch_funds_from_mufap(
    category: str | None = None,
    fund_type: str = "OPEN_END",
) -> list[dict]:
    """
    Fetch mutual fund master data from MUFAP website.

    Note: MUFAP doesn't have a public API, so this attempts to scrape
    or falls back to default funds.

    Args:
        category: Filter by category (None = all)
        fund_type: 'OPEN_END' | 'VPS' | 'ETF'

    Returns:
        List of fund metadata dicts
    """
    # Try to fetch from MUFAP website
    try:
        # MUFAP has dynamic content, would need proper scraping
        # For now, return default funds filtered by criteria
        funds = get_default_funds()

        if category:
            funds = [f for f in funds if f.get("category") == category]
        if fund_type != "ALL":
            funds = [f for f in funds if f.get("fund_type") == fund_type]

        return funds

    except Exception:
        # Fallback to defaults
        return get_default_funds()


def fetch_nav_from_mufap(
    fund_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Fetch NAV data from MUFAP.

    Note: MUFAP requires login for historical NAV data, so this
    falls back to sample data generation.

    Args:
        fund_id: Specific fund ID or None for all
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with fund_id, date, nav, offer_price, redemption_price columns
    """
    # MUFAP historical NAV data requires authentication
    # Fall back to sample data
    return fetch_nav_sample_data(fund_id, start_date, end_date)


def fetch_nav_sample_data(
    fund_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Generate sample NAV data for testing and development.

    This provides realistic-looking NAV data when MUFAP is unavailable.
    Different fund categories have different NAV behaviors:
    - Equity funds: Higher volatility, growth trend
    - Money Market: Low volatility, steady growth (KIBOR-like returns)
    - Income: Medium volatility, moderate growth
    - Balanced: Mix of equity and income behavior

    Args:
        fund_id: Fund ID (required to determine category behavior)
        start_date: Start date
        end_date: End date

    Returns:
        DataFrame with sample NAV data
    """
    if not fund_id:
        return pd.DataFrame()

    # Find fund to determine category
    funds = get_default_funds()
    fund_info = next((f for f in funds if f.get("fund_id") == fund_id), None)

    if not fund_info:
        # Try matching by symbol
        fund_info = next((f for f in funds if f.get("symbol") == fund_id), None)

    category = fund_info.get("category", "Equity") if fund_info else "Equity"

    # Set parameters based on fund category
    # Base NAV and volatility parameters (nav, daily_vol, annual_return)
    category_params = {
        "Equity": {"base_nav": 50.0, "daily_vol": 0.015, "annual_return": 0.12},
        "Islamic Equity": {"base_nav": 45.0, "daily_vol": 0.015, "annual_return": 0.11},
        "Money Market": {"base_nav": 10.5, "daily_vol": 0.0001, "annual_return": 0.15},
        "Islamic Money Market": {
            "base_nav": 10.5, "daily_vol": 0.0001, "annual_return": 0.14
        },
        "Income": {"base_nav": 15.0, "daily_vol": 0.002, "annual_return": 0.14},
        "Islamic Income": {"base_nav": 14.0, "daily_vol": 0.002, "annual_return": 0.13},
        "Balanced": {"base_nav": 25.0, "daily_vol": 0.008, "annual_return": 0.10},
        "VPS": {"base_nav": 20.0, "daily_vol": 0.012, "annual_return": 0.09},
        "Asset Allocation": {
            "base_nav": 20.0, "daily_vol": 0.006, "annual_return": 0.08
        },
        "Fund of Funds": {"base_nav": 15.0, "daily_vol": 0.010, "annual_return": 0.08},
        "Capital Protected": {
            "base_nav": 10.5, "daily_vol": 0.001, "annual_return": 0.07
        },
        "ETF": {"base_nav": 100.0, "daily_vol": 0.012, "annual_return": 0.10},
    }

    params = category_params.get(category, category_params["Equity"])
    base_nav = params["base_nav"]
    daily_vol = params["daily_vol"]
    annual_return = params["annual_return"]

    # Daily drift for expected return
    daily_drift = annual_return / 252

    # Date range
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=365)

    # Generate data
    data = []
    current_nav = base_nav

    # Use fund_id hash for reproducible but fund-specific randomness
    random.seed(hash(fund_id) % (2**32))

    current_dt = start_dt

    while current_dt <= end_dt:
        # Skip weekends (no NAV updates on weekends)
        if current_dt.weekday() < 5:
            # Random walk with drift
            daily_return = daily_drift + random.gauss(0, daily_vol)
            current_nav *= (1 + daily_return)

            # Ensure NAV doesn't go negative or too extreme
            current_nav = max(current_nav, base_nav * 0.3)

            # Calculate offer and redemption prices
            # Typically offer > NAV > redemption (fund loads)
            front_load = 0.01  # 1% front load
            back_load = 0.005  # 0.5% back load

            offer_price = round(current_nav * (1 + front_load), 4)
            redemption_price = round(current_nav * (1 - back_load), 4)

            # Calculate daily change
            nav_change_pct = daily_return * 100 if len(data) > 0 else 0.0

            data.append({
                "fund_id": fund_id,
                "date": current_dt.strftime("%Y-%m-%d"),
                "nav": round(current_nav, 4),
                "offer_price": offer_price,
                "redemption_price": redemption_price,
                "aum": round(random.uniform(500, 5000), 2),  # AUM in millions
                "nav_change_pct": round(nav_change_pct, 4),
                "source": "SAMPLE",
            })

        current_dt += timedelta(days=1)

    return pd.DataFrame(data)


def fetch_mutual_fund_data(
    fund_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    source: str = "AUTO",
) -> pd.DataFrame:
    """
    Fetch NAV data from the best available source.

    Args:
        fund_id: Fund ID to fetch
        start_date: Start date
        end_date: End date
        source: Data source ("MUFAP", "SAMPLE", "AUTO")

    Returns:
        DataFrame with normalized NAV data
    """
    if source == "MUFAP":
        return fetch_nav_from_mufap(fund_id, start_date, end_date)
    elif source == "SAMPLE":
        return fetch_nav_sample_data(fund_id, start_date, end_date)
    else:
        # AUTO: Try MUFAP first, then sample
        df = fetch_nav_from_mufap(fund_id, start_date, end_date)
        if not df.empty:
            return df

        # Fall back to sample data
        return fetch_nav_sample_data(fund_id, start_date, end_date)


def normalize_nav_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize NAV DataFrame to standard schema.

    Args:
        df: Raw DataFrame from any source

    Returns:
        DataFrame with standard columns
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "fund_id", "date", "nav", "offer_price",
            "redemption_price", "aum", "nav_change_pct", "source"
        ])

    # Ensure required columns
    required = ["date", "nav"]
    for col in required:
        if col not in df.columns:
            return pd.DataFrame(columns=[
                "fund_id", "date", "nav", "offer_price",
                "redemption_price", "aum", "nav_change_pct", "source"
            ])

    # Fill missing columns with defaults
    if "offer_price" not in df.columns:
        df["offer_price"] = df["nav"]
    if "redemption_price" not in df.columns:
        df["redemption_price"] = df["nav"]
    if "aum" not in df.columns:
        df["aum"] = None
    if "nav_change_pct" not in df.columns:
        df["nav_change_pct"] = None
    if "source" not in df.columns:
        df["source"] = "UNKNOWN"

    cols = ["fund_id", "date", "nav", "offer_price",
            "redemption_price", "aum", "nav_change_pct", "source"]

    return df[[c for c in cols if c in df.columns]]


def map_mufap_category(raw_category: str) -> tuple[str, bool]:
    """
    Map MUFAP raw category to standardized category and Shariah flag.

    Args:
        raw_category: Raw category string from MUFAP

    Returns:
        Tuple of (category_name, is_shariah)
    """
    raw_lower = raw_category.lower()

    # Check for Islamic/Shariah keywords
    is_shariah = any(word in raw_lower for word in ["islamic", "shariah", "sharia"])

    # Map to standard categories
    if "equity" in raw_lower or "stock" in raw_lower:
        category = "Islamic Equity" if is_shariah else "Equity"
    elif "money market" in raw_lower or "cash" in raw_lower or "liquidity" in raw_lower:
        category = "Islamic Money Market" if is_shariah else "Money Market"
    elif "income" in raw_lower or "debt" in raw_lower or "fixed" in raw_lower:
        category = "Islamic Income" if is_shariah else "Income"
    elif "balanced" in raw_lower or "hybrid" in raw_lower:
        category = "Balanced"
    elif "pension" in raw_lower or "vps" in raw_lower:
        category = "VPS"
    elif "allocation" in raw_lower:
        category = "Asset Allocation"
    elif "fund of" in raw_lower or "fof" in raw_lower:
        category = "Fund of Funds"
    elif "protected" in raw_lower or "capital" in raw_lower:
        category = "Capital Protected"
    elif "etf" in raw_lower or "exchange traded" in raw_lower:
        category = "ETF"
    else:
        category = "Equity"  # Default to equity

    return category, is_shariah
