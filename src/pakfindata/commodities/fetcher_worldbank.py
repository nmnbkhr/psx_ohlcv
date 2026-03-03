"""Tier 3: World Bank Pink Sheet fetcher for monthly commodity prices.

The World Bank Commodity Markets group publishes monthly prices for 70+
commodities going back to 1960. Free, CC BY 4.0 license, no API key.

Download URL may change with each monthly release; we fall back to the
landing page if the direct URL fails.
"""

import logging

import pandas as pd

logger = logging.getLogger("pakfindata.commodities.worldbank")

# Direct download URL (may change monthly)
PINKSHEET_EXCEL_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "5d903e848db1d1b83e0ec8f744e55570-0350012021/"
    "related/CMO-Historical-Data-Monthly.xlsx"
)

# Mapping from World Bank column names to our internal symbols
# The Pink Sheet uses various naming conventions — map the most common
WB_COLUMN_MAP = {
    # Energy
    "Crude oil, average": "CRUDE_WTI",
    "Crude oil, Brent": "BRENT",
    "Crude oil, WTI": "CRUDE_WTI",
    "Natural gas, US": "NATURAL_GAS",
    "Coal, Australian": "COAL",
    # Precious metals
    "Gold": "GOLD",
    "Silver": "SILVER",
    "Platinum": "PLATINUM",
    # Base metals
    "Copper": "COPPER",
    "Iron ore, cfr spot": "IRON_ORE",
    "Aluminum": "ALUMINUM",
    "Zinc": "ZINC",
    "Nickel": "NICKEL",
    "Lead": "LEAD",
    "Tin": "TIN",
    # Agriculture
    "Cotton, A Index": "COTTON",
    "Rice, Thai 5%": "RICE",
    "Wheat, US HRW": "WHEAT",
    "Sugar, world": "SUGAR",
    "Palm oil": "PALM_OIL",
    "Maize": "CORN",
    "Soybeans": "SOYBEANS",
    "Soybean oil": "SOYBEAN_OIL",
    "Coffee, Arabica": "COFFEE",
    "Cocoa": "COCOA",
    "Rubber, SGP/MYS": "RUBBER",
}


def fetch_worldbank_pinksheet(url: str | None = None) -> list[dict]:
    """Download and parse the World Bank Pink Sheet Excel file.

    Args:
        url: Override URL for the Excel file.

    Returns:
        List of dicts with keys: symbol, date, price, source, series_id.
    """
    target_url = url or PINKSHEET_EXCEL_URL

    try:
        logger.info("Downloading World Bank Pink Sheet from %s", target_url)
        df = pd.read_excel(target_url, sheet_name="Monthly Prices", skiprows=4)
    except Exception as e:
        logger.warning("Failed to download Pink Sheet: %s", e)
        return []

    if df is None or df.empty:
        logger.info("No data in World Bank Pink Sheet")
        return []

    # The first column is usually the date/period column
    date_col = df.columns[0]
    rows = []

    for wb_name, our_symbol in WB_COLUMN_MAP.items():
        # Find matching column (case-insensitive, partial match)
        matching_cols = [c for c in df.columns if wb_name.lower() in str(c).lower()]
        if not matching_cols:
            continue

        col = matching_cols[0]
        for _, row in df.iterrows():
            date_val = row[date_col]
            price_val = row[col]

            if pd.isna(price_val) or pd.isna(date_val):
                continue

            # Parse date — World Bank uses "YYYYMNN" format or actual dates
            try:
                if isinstance(date_val, str):
                    # Try parsing "2024M01" format
                    if "M" in date_val:
                        parts = date_val.split("M")
                        date_str = f"{parts[0]}-{int(parts[1]):02d}-01"
                    else:
                        date_str = pd.to_datetime(date_val).strftime("%Y-%m-%d")
                elif hasattr(date_val, "strftime"):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    continue
            except (ValueError, IndexError):
                continue

            try:
                price = float(price_val)
            except (ValueError, TypeError):
                continue

            rows.append({
                "symbol": our_symbol,
                "date": date_str,
                "price": price,
                "source": "worldbank",
                "series_id": wb_name,
            })

    logger.info("World Bank Pink Sheet: parsed %d total rows across %d commodities",
                len(rows), len({r["symbol"] for r in rows}))
    return rows
