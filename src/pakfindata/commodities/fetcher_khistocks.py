"""Tier 4a: khistocks.com (Business Recorder) scraper — 7 Pakistan commodity feeds.

khistocks.com is Business Recorder's free market data portal. All data is loaded
via AJAX JSON endpoints — NO Selenium needed. Pure requests + JSON parsing.

Feeds:
  1. PMEX commodity prices (Open/Close, PKR & USD)
  2. Karachi Bullion rates (Sarafa Bazaar, PKR/Tola)
  3. International Bullion OHLC (USD)
  4. Karachi Cotton rates (PKR/Maund)
  5. Lahore Akbari Mandi wholesale (30+ food staples, PKR)
  6. LME base metals (Cash + 3-month, USD/tonne)
  7. Currency pages (Interbank, SBP rates)
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

logger = logging.getLogger("pakfindata.commodities.khistocks")

BASE_URL = "https://www.khistocks.com/"
AJAX_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.khistocks.com/",
}

# ─────────────────────────────────────────────────────────────────────────────
# PMEX commodity IDs (from /ajax/getAllPmexcomms)
# ─────────────────────────────────────────────────────────────────────────────
PMEX_COMMODITIES = {
    3: {"name": "GOLD", "symbol": "PMEX_GOLD", "quotation": "$ Per Ounce"},
    2: {"name": "SILVER", "symbol": "PMEX_SILVER", "quotation": "$ Per Ounce"},
    1: {"name": "WTI CRUDE OIL", "symbol": "PMEX_CRUDE_WTI", "quotation": "$ Per Barrel"},
    23: {"name": "BRENT CRUDE OIL", "symbol": "PMEX_BRENT", "quotation": "$ Per Barrel"},
    24: {"name": "NATURAL GAS", "symbol": "PMEX_NATGAS", "quotation": "US $ Per mmbtu"},
    10: {"name": "ICOTTON", "symbol": "PMEX_COTTON", "quotation": "US Cents per pound"},
    6: {"name": "TOLAGOLD", "symbol": "PMEX_TOLAGOLD", "quotation": "Rs Per Tola"},
    5: {"name": "MTOLAGOLD", "symbol": "PMEX_MTOLAGOLD", "quotation": "Rs Per Tola"},
    26: {"name": "COPPER", "symbol": "PMEX_COPPER", "quotation": "US $ Per Pound"},
    25: {"name": "PLATINUM", "symbol": "PMEX_PLATINUM", "quotation": "$ Per Ounce"},
    52: {"name": "ALUMINUM", "symbol": "PMEX_ALUMINUM", "quotation": "$ Per Metric Ton"},
    8: {"name": "PALMOLEIN", "symbol": "PMEX_PALMOLEIN", "quotation": "Rs Per Maund"},
    11: {"name": "WHEAT", "symbol": "PMEX_WHEAT", "quotation": "Rs Per 100 kg"},
    7: {"name": "RICEIRRI6", "symbol": "PMEX_RICE", "quotation": "Rs Per 100 kg"},
    9: {"name": "SUGAR", "symbol": "PMEX_SUGAR", "quotation": "Rs Per kg"},
    45: {"name": "IWHEAT", "symbol": "PMEX_IWHEAT", "quotation": "US Cents per bushel"},
    44: {"name": "ICORN", "symbol": "PMEX_ICORN", "quotation": "US Cents per bushel"},
    46: {"name": "ISOYBEAN", "symbol": "PMEX_ISOYBEAN", "quotation": "US Cents per bushel"},
    43: {"name": "PALLADIUM", "symbol": "PMEX_PALLADIUM", "quotation": "$ Per Ounce"},
    15: {"name": "RED CHILI", "symbol": "PMEX_REDCHILI", "quotation": "Rs Per kg"},
}

# Bullion instrument IDs (from /ajax/getAllbullions)
BULLION_INSTRUMENTS = {
    1: {"instrument": "Gold 24 CT (10 grams)", "symbol": "SARAFA_GOLD_24K"},
    3: {"instrument": "Gold 24 CT (10 grams)", "symbol": "SARAFA_GOLD_24K_ALT"},
    2: {"instrument": "Silver (10 grams)", "symbol": "SARAFA_SILVER"},
}

# International bullion instrument IDs
INTL_BULLION = {
    1: {"instrument": "Gold (USD/OZ)", "symbol": "INTL_GOLD"},
    2: {"instrument": "Silver (USD/OZ)", "symbol": "INTL_SILVER"},
    3: {"instrument": "Platinum (USD/OZ)", "symbol": "INTL_PLATINUM"},
    4: {"instrument": "Palladium (USD/OZ)", "symbol": "INTL_PALLADIUM"},
}

# LME metal IDs (from /ajax/getLondonMetal)
LME_METALS = {
    2: {"metal_name": "Aluminium", "symbol": "LME_ALUMINUM"},
    3: {"metal_name": "Copper", "symbol": "LME_COPPER"},
    4: {"metal_name": "Lead", "symbol": "LME_LEAD"},
    5: {"metal_name": "Nickel", "symbol": "LME_NICKEL"},
    7: {"metal_name": "Zinc", "symbol": "LME_ZINC"},
}

# Key Akbari Mandi commodities (from /ajax/getAllGrainCommodities)
MANDI_COMMODITIES = {
    1: {"com_name": "Sugar", "symbol": "MANDI_SUGAR"},
    3: {"com_name": "Shakar", "symbol": "MANDI_SHAKAR"},
    37: {"com_name": "Sugar (imported)", "symbol": "MANDI_SUGAR_IMPORTED"},
    2: {"com_name": "Gur", "symbol": "MANDI_GUR"},
    4: {"com_name": "Ghee (16 kg)", "symbol": "MANDI_GHEE"},
    31: {"com_name": "Basmati Super (new)", "symbol": "MANDI_BASMATI_NEW"},
    30: {"com_name": "Basmati Super (Old)", "symbol": "MANDI_BASMATI_OLD"},
    32: {"com_name": "Rice Basmati (386)", "symbol": "MANDI_BASMATI_386"},
    33: {"com_name": "Basmati broken", "symbol": "MANDI_BASMATI_BROKEN"},
    36: {"com_name": "Kainat 1121", "symbol": "MANDI_KAINAT_1121"},
    20: {"com_name": "Dal Masoor (Local)", "symbol": "MANDI_MASOOR_LOCAL"},
    21: {"com_name": "Dal Masoor (import)", "symbol": "MANDI_MASOOR_IMPORT"},
    14: {"com_name": "Dal Mong (Chilka)", "symbol": "MANDI_MONG_CHILKA"},
    15: {"com_name": "Dal Mong (Washed)", "symbol": "MANDI_MONG_WASHED"},
    26: {"com_name": "Dal Chana (Thin)", "symbol": "MANDI_CHANA_THIN"},
    27: {"com_name": "Dal Chana (Thick)", "symbol": "MANDI_CHANA_THICK"},
    24: {"com_name": "Gram White", "symbol": "MANDI_GRAM_WHITE"},
    25: {"com_name": "Gram Black", "symbol": "MANDI_GRAM_BLACK"},
    9: {"com_name": "Chilli (Sabat)", "symbol": "MANDI_CHILLI"},
    11: {"com_name": "Turmeric", "symbol": "MANDI_TURMERIC"},
    34: {"com_name": "Tea (Black)", "symbol": "MANDI_TEA_BLACK"},
    35: {"com_name": "Tea (Green)", "symbol": "MANDI_TEA_GREEN"},
    5: {"com_name": "Almond (Kaghzi)", "symbol": "MANDI_ALMOND"},
    8: {"com_name": "Dry Date", "symbol": "MANDI_DRY_DATE"},
    29: {"com_name": "Red Kidney Beans (Lobia)", "symbol": "MANDI_LOBIA_RED"},
    28: {"com_name": "White Kidney Beans (Lobia)", "symbol": "MANDI_LOBIA_WHITE"},
}


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def _parse_number(text: str | None) -> float | None:
    """Parse a comma-formatted number string."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(text).replace(",", ""))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _fetch_datatable(endpoint: str, params: dict, max_rows: int = 1000) -> list[dict]:
    """Fetch data from a khistocks DataTables server-side endpoint.

    Args:
        endpoint: Relative AJAX endpoint (e.g., "ajax/commodity/").
        params: POST parameters (must include 'id' for commodity filtering).
        max_rows: Maximum rows to fetch.

    Returns:
        List of data dicts from the DataTables response.
    """
    url = BASE_URL + endpoint
    post_data = {
        "draw": 1,
        "start": 0,
        "length": max_rows,
        **params,
    }

    try:
        resp = requests.post(url, data=post_data, headers=AJAX_HEADERS, timeout=20)
        resp.raise_for_status()
        result = resp.json()
        return result.get("data", [])
    except Exception as e:
        logger.warning("khistocks fetch failed for %s: %s", endpoint, e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# 1. PMEX Commodity Prices (Open/Close)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_pmex_commodity(pmex_id: int, symbol: str) -> list[dict]:
    """Fetch PMEX daily settlement prices for a single commodity.

    Returns list of dicts: symbol, date, name, quotation, open, close, source.
    """
    rows = _fetch_datatable("ajax/commodity/", {"id": pmex_id})
    result = []
    for r in rows:
        result.append({
            "symbol": symbol,
            "date": r.get("date", ""),
            "name": r.get("name", ""),
            "quotation": r.get("quotation", ""),
            "open": _parse_number(r.get("open")),
            "close": _parse_number(r.get("close")),
            "high": None,
            "low": None,
            "source": "khistocks_pmex",
        })
    return result


def fetch_all_pmex(commodity_ids: list[int] | None = None) -> dict[str, list[dict]]:
    """Fetch PMEX data for multiple commodities (parallel).

    Returns dict: symbol -> list of price rows.
    """
    targets = commodity_ids or list(PMEX_COMMODITIES.keys())
    results = {}

    def _fetch(pmex_id):
        info = PMEX_COMMODITIES.get(pmex_id)
        if not info:
            return None, None, 0
        rows = fetch_pmex_commodity(pmex_id, info["symbol"])
        return info["symbol"], rows, len(rows)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch, pid): pid for pid in targets}
        for fut in as_completed(futures):
            symbol, rows, count = fut.result()
            if symbol and rows:
                results[symbol] = rows
                logger.info("PMEX %s: %d rows", symbol, count)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 2. Karachi Bullion Rates (Sarafa Bazaar)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_karachi_bullion(instrument_id: int = 1) -> list[dict]:
    """Fetch Sarafa Bazaar gold/silver rates in PKR.

    Default id=1 is Gold 24K (10 grams).
    Returns list of dicts: symbol, date, instrument, rate, source.
    """
    info = BULLION_INSTRUMENTS.get(instrument_id, {})
    symbol = info.get("symbol", f"SARAFA_{instrument_id}")

    rows = _fetch_datatable("ajax/bullion_rates", {"id": instrument_id})
    result = []
    for r in rows:
        result.append({
            "symbol": symbol,
            "date": r.get("date", ""),
            "instrument": r.get("instrument", ""),
            "rate": _parse_number(r.get("rate")),
            "source": "khistocks_sarafa",
        })
    return result


def fetch_all_bullion() -> dict[str, list[dict]]:
    """Fetch all Sarafa Bazaar bullion rates (parallel)."""
    results = {}

    def _fetch(inst_id, info):
        rows = fetch_karachi_bullion(inst_id)
        return info["symbol"], info["instrument"], rows

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fetch, iid, inf) for iid, inf in BULLION_INSTRUMENTS.items()]
        for fut in as_completed(futures):
            symbol, name, rows = fut.result()
            if rows:
                results[symbol] = rows
                logger.info("Sarafa %s: %d rows", name, len(rows))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 3. International Bullion OHLC
# ─────────────────────────────────────────────────────────────────────────────

def fetch_intl_bullion(instrument_id: int = 1) -> list[dict]:
    """Fetch international bullion OHLC data (USD).

    Default id=1 is Gold (USD/OZ).
    Returns list of dicts with full OHLC + net change.
    """
    info = INTL_BULLION.get(instrument_id, {})
    symbol = info.get("symbol", f"INTL_{instrument_id}")

    rows = _fetch_datatable("ajax/gold_rates", {"id": instrument_id})
    result = []
    for r in rows:
        result.append({
            "symbol": symbol,
            "date": r.get("date", ""),
            "instrument": r.get("instrument", ""),
            "open": _parse_number(r.get("opening")),
            "high": _parse_number(r.get("high")),
            "low": _parse_number(r.get("low")),
            "close": _parse_number(r.get("closing")),
            "net_change": _parse_number(r.get("netchange")),
            "change_pct": r.get("change_percent", ""),
            "source": "khistocks_intl_bullion",
        })
    return result


def fetch_all_intl_bullion() -> dict[str, list[dict]]:
    """Fetch all international bullion OHLC data (parallel)."""
    results = {}

    def _fetch(inst_id, info):
        rows = fetch_intl_bullion(inst_id)
        return info["symbol"], info["instrument"], rows

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fetch, iid, inf) for iid, inf in INTL_BULLION.items()]
        for fut in as_completed(futures):
            symbol, name, rows = fut.result()
            if rows:
                results[symbol] = rows
                logger.info("Intl Bullion %s: %d rows", name, len(rows))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 5. Lahore Akbari Mandi (Wholesale Grain/Food)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_mandi_commodity(com_id: int, symbol: str) -> list[dict]:
    """Fetch Lahore Akbari Mandi wholesale prices for a commodity.

    Returns list of dicts: symbol, date, name, high, low, source.
    """
    rows = _fetch_datatable("ajax/grainmarketall/", {"id": com_id})
    result = []
    for r in rows:
        result.append({
            "symbol": symbol,
            "date": r.get("date", ""),
            "name": r.get("name", r.get("com_name", "")),
            "high": _parse_number(r.get("high")),
            "low": _parse_number(r.get("low")),
            "source": "khistocks_mandi",
        })
    return result


def fetch_all_mandi(com_ids: list[int] | None = None) -> dict[str, list[dict]]:
    """Fetch Akbari Mandi data for multiple commodities (parallel).

    Returns dict: symbol -> list of price rows.
    """
    targets = com_ids or list(MANDI_COMMODITIES.keys())
    results = {}

    def _fetch(com_id):
        info = MANDI_COMMODITIES.get(com_id)
        if not info:
            return None, None, 0
        rows = fetch_mandi_commodity(com_id, info["symbol"])
        return info["symbol"], rows, len(rows)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch, cid): cid for cid in targets}
        for fut in as_completed(futures):
            symbol, rows, count = fut.result()
            if symbol and rows:
                results[symbol] = rows
                logger.info("Mandi %s: %d rows", symbol, count)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 6. LME Base Metals
# ─────────────────────────────────────────────────────────────────────────────

def fetch_lme_metal(metal_id: int, symbol: str) -> list[dict]:
    """Fetch LME metal prices (cash + 3-month, bid/ask).

    Returns list of dicts: symbol, date, cash_buyer, cash_seller,
    three_month_buyer, three_month_seller, source.
    """
    rows = _fetch_datatable("ajax/london_metal_rates", {"id": metal_id})
    result = []
    for r in rows:
        result.append({
            "symbol": symbol,
            "date": r.get("date", ""),
            "cash_buyer": _parse_number(r.get("cash_buyer")),
            "cash_seller": _parse_number(r.get("cash_seller")),
            "three_month_buyer": _parse_number(r.get("3month_buyer")),
            "three_month_seller": _parse_number(r.get("3month_seller")),
            "source": "khistocks_lme",
        })
    return result


def fetch_all_lme() -> dict[str, list[dict]]:
    """Fetch all LME metal prices (parallel)."""
    results = {}

    def _fetch(metal_id, info):
        rows = fetch_lme_metal(metal_id, info["symbol"])
        return info["symbol"], info["metal_name"], rows

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fetch, mid, inf) for mid, inf in LME_METALS.items()]
        for fut in as_completed(futures):
            symbol, name, rows = fut.result()
            if rows:
                results[symbol] = rows
                logger.info("LME %s: %d rows", name, len(rows))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Combined fetch — all khistocks feeds
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_khistocks() -> dict[str, dict[str, list[dict]]]:
    """Fetch data from ALL khistocks feeds (parallel).

    Returns nested dict:
      { "pmex": {symbol: [rows]}, "sarafa": {symbol: [rows]},
        "intl_bullion": {symbol: [rows]}, "mandi": {symbol: [rows]},
        "lme": {symbol: [rows]} }
    """
    logger.info("=== khistocks.com: fetching all feeds ===")

    key_pmex = [6, 5, 3, 2, 1, 23, 24, 10, 8, 11, 7, 9]

    feed_tasks = {
        "pmex": lambda: fetch_all_pmex(key_pmex),
        "sarafa": fetch_all_bullion,
        "intl_bullion": fetch_all_intl_bullion,
        "mandi": fetch_all_mandi,
        "lme": fetch_all_lme,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fn): name for name, fn in feed_tasks.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                logger.warning("khistocks %s failed: %s", name, e)
                results[name] = {}

    total_symbols = sum(len(v) for v in results.values())
    total_rows = sum(len(rows) for feed in results.values() for rows in feed.values())
    logger.info("khistocks.com: %d symbols, %d total rows across %d feeds",
                total_symbols, total_rows, len(results))

    return results
