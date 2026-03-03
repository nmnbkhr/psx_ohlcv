"""Tier 5: GoldPriceZ API fetcher for PKR/Tola gold and silver rates.

GoldPriceZ provides free spot prices for gold and silver in Pakistan's
native unit (Tola) and PKR currency. 30-60 requests/hour, no credit card.
Register at https://www.goldpricez.com/api
"""

import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger("pakfindata.commodities.goldpricez")

GOLDPRICEZ_BASE_URL = "https://www.goldpricez.com/api/rates/currency/pkr/measure/tola-pakistan"


def fetch_goldpricez_rates(api_key: str | None = None) -> dict | None:
    """Fetch current gold and silver rates in PKR/Tola from GoldPriceZ.

    Args:
        api_key: GoldPriceZ API key. If None, reads from GOLDPRICEZ_API_KEY env var.

    Returns:
        Dict with keys: gold_pkr_tola, silver_pkr_tola, usd_pkr, timestamp.
        None if the request fails.
    """
    key = api_key or os.environ.get("GOLDPRICEZ_API_KEY")
    if not key:
        logger.warning(
            "GoldPriceZ API key required. Set GOLDPRICEZ_API_KEY env var or pass api_key. "
            "Register free at https://www.goldpricez.com/api"
        )
        return None

    try:
        resp = requests.get(
            GOLDPRICEZ_BASE_URL,
            headers={"X-API-KEY": key},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("GoldPriceZ request failed: %s", e)
        return None

    # Parse response — structure varies; handle common formats
    try:
        result = {
            "gold_pkr_tola": float(data.get("gold", data.get("gold_price", 0))),
            "silver_pkr_tola": float(data.get("silver", data.get("silver_price", 0))),
            "usd_pkr": float(data.get("usd_pkr", data.get("exchange_rate", 0))),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
        }
        logger.info(
            "GoldPriceZ: Gold=PKR %.0f/tola, Silver=PKR %.0f/tola, USD/PKR=%.2f",
            result["gold_pkr_tola"], result["silver_pkr_tola"], result["usd_pkr"],
        )
        return result
    except (KeyError, TypeError, ValueError) as e:
        logger.warning("GoldPriceZ response parse error: %s (data=%s)", e, data)
        return None


def fetch_and_store_goldpricez(
    con,
    api_key: str | None = None,
) -> int:
    """Fetch GoldPriceZ rates and store as commodity_pkr rows.

    Returns number of rows upserted.
    """
    from .models import upsert_commodity_pkr

    rates = fetch_goldpricez_rates(api_key)
    if not rates:
        return 0

    rows = []
    today = rates["date"]

    if rates["gold_pkr_tola"]:
        rows.append({
            "symbol": "GOLD",
            "date": today,
            "pkr_price": rates["gold_pkr_tola"],
            "pk_unit": "PKR/tola",
            "usd_price": None,
            "usd_pkr": rates["usd_pkr"],
            "source": "goldpricez",
        })

    if rates["silver_pkr_tola"]:
        rows.append({
            "symbol": "SILVER",
            "date": today,
            "pkr_price": rates["silver_pkr_tola"],
            "pk_unit": "PKR/tola",
            "usd_price": None,
            "usd_pkr": rates["usd_pkr"],
            "source": "goldpricez",
        })

    if rows:
        return upsert_commodity_pkr(con, rows)
    return 0
