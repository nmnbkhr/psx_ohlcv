"""NCCPL Flow Intelligence fetcher — FIPI, LIPI, sector-wise data.

3-tier fallback strategy to bypass Cloudflare:
  Tier 1: curl_cffi with Chrome TLS impersonation → NCCPL direct
  Tier 2: PSX Data Portal / NCCPL weekly Excel files
  Tier 3: Mettis Global article text parsing (net FPI only)

NCCPL Table Structure (verified via khistocks mirror):
  Columns: Date, Client Type, Market Type, Buy Volume, Buy Value,
           Sell Volume, Sell Value, Net Volume, Net Value, USD

FIPI client types: Foreign Individual, Foreign Corporates, Overseas Pakistani
LIPI client types: Individuals, Companies, Banks/DFI, NBFC, Mutual Funds,
                   Other Organization, Broker Proprietary Trading, Insurance Companies

Usage:
    conda activate psx
    python -m pakfindata.sources.nccpl_flows --date 2026-04-02
    python -m pakfindata.sources.nccpl_flows --backfill --from 2025-12-01
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from pakfindata.db.connection import connect, init_schema
from pakfindata.db.repositories.nccpl_flows import (
    init_nccpl_schema,
    upsert_fipi,
    upsert_lipi,
    upsert_fipi_sector,
    upsert_derived,
    date_already_fetched,
)

log = logging.getLogger("pakfindata.nccpl_flows")

NCCPL_URLS = {
    "fipi": "https://www.nccpl.com.pk/en/market-information/fipi-lipi/fipi",
    "lipi": "https://www.nccpl.com.pk/en/market-information/fipi-lipi/lipi",
    "sector": "https://www.nccpl.com.pk/en/portfolio-investments/fipi-sector-wise",
}

PSX_FIPI_URL = "https://dps.psx.com.pk/market-summary/equity"

# BRecorder mirrors NCCPL data with no Cloudflare — primary fallback
BRECORDER_FIPI_SECTOR = "https://www.brecorder.com/markets/fipi-sector-wise"
BRECORDER_LIPI_SECTOR = "https://www.brecorder.com/markets/lipi-sector-wise"

METTIS_SEARCH = "https://www.mettis.com/news/?s=FIPI+LIPI"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nccpl.com.pk/",
}

# Maps NCCPL client type text -> our column prefix
_LIPI_CLIENT_MAP = {
    "mutual fund": "mf",
    "mutual funds": "mf",
    "insurance": "insurance",
    "insurance companies": "insurance",
    "bank": "bank",
    "banks": "bank",
    "banks/dfi": "bank",
    "banks / dfi": "bank",
    "individual": "retail",
    "individuals": "retail",
    "corporate": "corporate",
    "companies": "corporate",
    "broker": "broker",
    "broker proprietary trading": "broker",
    "proprietary trading": "broker",
    "nbfc": "nbfc",
    "non-banking": "nbfc",
    "other": "other",
    "other organization": "other",
    "other organizations": "other",
}

_FIPI_CLIENT_MAP = {
    "foreign individual": "fpi_foreign_individual",
    "foreign individuals": "fpi_foreign_individual",
    "foreign corporate": "fpi_foreign_corporate",
    "foreign corporates": "fpi_foreign_corporate",
    "overseas pakistani": "fpi_overseas_pak",
    "overseas pakistan": "fpi_overseas_pak",
}


# ═══════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════


def _parse_value(text: str) -> float | None:
    """Parse a numeric value from table cell, handling commas and parens."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace("(", "-").replace(")", "")
    try:
        return float(text)
    except ValueError:
        return None


def _match_client_type(raw: str, client_map: dict) -> str | None:
    """Fuzzy-match a client type string to our column prefix."""
    raw_lower = raw.strip().lower()
    for pattern, prefix in client_map.items():
        if pattern in raw_lower:
            return prefix
    return None


def _find_col(headers: list[str], candidates: list[str]) -> int | None:
    """Find column index matching any candidate substring."""
    for i, h in enumerate(headers):
        for c in candidates:
            if c in h:
                return i
    return None


def _get_session():
    """Create a curl_cffi session with Chrome TLS impersonation."""
    from curl_cffi import requests as cf_requests

    return cf_requests.Session(impersonate="chrome120")


def _soup_tables_to_rows(soup: BeautifulSoup) -> list[list[list[str]]]:
    """Extract all tables from soup as lists of row-lists of cell-strings."""
    result = []
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)
        if len(rows) >= 2:
            result.append(rows)
    return result


# ═══════════════════════════════════════════════════════
# TABLE PARSING (shared by all tiers)
# ═══════════════════════════════════════════════════════


def _parse_fipi_from_rows(tables: list[list[list[str]]], target_date: str) -> dict | None:
    """Parse FIPI data from table rows (works for any source)."""
    result = {"date": target_date, "fpi_buy": 0, "fpi_sell": 0, "fpi_net": 0}

    for rows in tables:
        headers = [h.lower().strip() for h in rows[0]]

        buy_col = _find_col(headers, ["buy value", "buy val", "buy"])
        sell_col = _find_col(headers, ["sell value", "sell val", "sell"])
        net_col = _find_col(headers, ["net value", "net val", "net"])
        type_col = _find_col(headers, ["client type", "client", "type", "category"])

        if buy_col is None or sell_col is None:
            continue

        total_buy = 0.0
        total_sell = 0.0
        total_net = 0.0

        for row in rows[1:]:
            if len(row) <= max(buy_col, sell_col):
                continue

            buy_val = _parse_value(row[buy_col]) or 0
            sell_val = _parse_value(row[sell_col]) or 0
            net_val = (
                _parse_value(row[net_col])
                if net_col is not None and net_col < len(row)
                else buy_val - sell_val
            )

            total_buy += buy_val
            total_sell += sell_val
            total_net += net_val or (buy_val - sell_val)

            if type_col is not None and type_col < len(row):
                prefix = _match_client_type(row[type_col], _FIPI_CLIENT_MAP)
                if prefix:
                    result[f"{prefix}_net"] = net_val or (buy_val - sell_val)

        result["fpi_buy"] = total_buy
        result["fpi_sell"] = total_sell
        result["fpi_net"] = total_net
        return result

    return None


def _parse_lipi_from_rows(tables: list[list[list[str]]], target_date: str) -> dict | None:
    """Parse LIPI data from table rows (works for any source)."""
    result = {"date": target_date}

    for rows in tables:
        headers = [h.lower().strip() for h in rows[0]]

        buy_col = _find_col(headers, ["buy value", "buy val", "buy"])
        sell_col = _find_col(headers, ["sell value", "sell val", "sell"])
        net_col = _find_col(headers, ["net value", "net val", "net"])
        type_col = _find_col(headers, ["client type", "client", "type", "category"])

        if buy_col is None or sell_col is None or type_col is None:
            continue

        found_any = False
        for row in rows[1:]:
            if len(row) <= max(buy_col, sell_col, type_col):
                continue

            prefix = _match_client_type(row[type_col], _LIPI_CLIENT_MAP)
            if prefix is None:
                continue

            buy_val = _parse_value(row[buy_col]) or 0
            sell_val = _parse_value(row[sell_col]) or 0
            net_val = (
                _parse_value(row[net_col])
                if net_col is not None and net_col < len(row)
                else buy_val - sell_val
            )

            result[f"{prefix}_buy"] = buy_val
            result[f"{prefix}_sell"] = sell_val
            result[f"{prefix}_net"] = net_val or (buy_val - sell_val)
            found_any = True

        if found_any:
            return result

    return None if len(result) <= 1 else result


def _parse_sector_from_rows(tables: list[list[list[str]]], target_date: str) -> list[dict] | None:
    """Parse sector-wise FIPI from table rows."""
    results = []

    for rows in tables:
        headers = [h.lower().strip() for h in rows[0]]

        sector_col = _find_col(headers, ["sector", "sector name"])
        buy_col = _find_col(headers, ["buy value", "buy val", "buy"])
        sell_col = _find_col(headers, ["sell value", "sell val", "sell"])
        net_col = _find_col(headers, ["net value", "net val", "net"])

        if sector_col is None or buy_col is None:
            continue

        for row in rows[1:]:
            if len(row) <= max(sector_col, buy_col):
                continue

            sector = row[sector_col].strip()
            if not sector or sector.lower() in ("total", "grand total", ""):
                continue

            buy_val = _parse_value(row[buy_col]) or 0
            sell_val = (
                _parse_value(row[sell_col])
                if sell_col is not None and sell_col < len(row)
                else 0
            )
            net_val = (
                _parse_value(row[net_col])
                if net_col is not None and net_col < len(row)
                else buy_val - (sell_val or 0)
            )

            results.append({
                "date": target_date,
                "sector": sector,
                "fpi_buy": buy_val,
                "fpi_sell": sell_val or 0,
                "fpi_net": net_val or (buy_val - (sell_val or 0)),
            })

        if results:
            return results

    return None


# ═══════════════════════════════════════════════════════
# TIER 1 — curl_cffi → NCCPL Direct
# ═══════════════════════════════════════════════════════


def _fetch_nccpl_soup(session, url: str, date_str: str) -> BeautifulSoup | None:
    """Fetch NCCPL page via curl_cffi with Chrome TLS impersonation."""
    try:
        resp = session.get(
            url, headers=HEADERS, timeout=15,
            params={"date": date_str} if date_str else {},
        )
        if resp.status_code == 200 and "just a moment" not in resp.text.lower()[:500]:
            log.info("Tier 1 curl_cffi SUCCESS: %s", url)
            return BeautifulSoup(resp.text, "html.parser")
        log.debug("Tier 1 got status %d or CF challenge for %s", resp.status_code, url)
        return None
    except Exception as e:
        log.debug("Tier 1 curl_cffi failed for %s: %s", url, e)
        return None


def _tier1_nccpl_direct(session, date_str: str) -> dict:
    """Tier 1: curl_cffi direct to NCCPL. Returns partial result dict."""
    result = {"fipi": None, "lipi": None, "sector": None}

    # Warm up session with homepage cookies
    try:
        session.get("https://www.nccpl.com.pk/", headers=HEADERS, timeout=15)
        time.sleep(1)
    except Exception:
        pass

    # FIPI
    soup = _fetch_nccpl_soup(session, NCCPL_URLS["fipi"], date_str)
    if soup:
        tables = _soup_tables_to_rows(soup)
        result["fipi"] = _parse_fipi_from_rows(tables, date_str)

    # LIPI
    soup = _fetch_nccpl_soup(session, NCCPL_URLS["lipi"], date_str)
    if soup:
        tables = _soup_tables_to_rows(soup)
        result["lipi"] = _parse_lipi_from_rows(tables, date_str)

    # Sector
    soup = _fetch_nccpl_soup(session, NCCPL_URLS["sector"], date_str)
    if soup:
        tables = _soup_tables_to_rows(soup)
        result["sector"] = _parse_sector_from_rows(tables, date_str)

    return result


# ═══════════════════════════════════════════════════════
# TIER 2a — BRecorder (mirrors NCCPL, no Cloudflare)
# ═══════════════════════════════════════════════════════


def _tier2a_brecorder_tables(session, date_str: str) -> dict:
    """Tier 2a: BRecorder FIPI + LIPI sector-wise tables.

    BRecorder mirrors NCCPL data at:
      /markets/fipi-sector-wise — Foreign investors by sector
      /markets/lipi-sector-wise — Local investors by sector + type

    Table columns: Client Type, Date, Sector, Market Type,
                   Buy Volume, Buy Value, Sell Volume, Sell Value,
                   Net Volume, Net Value [, USD]
    """
    result = {"fipi": None, "lipi": None, "sector": None}

    # ── FIPI sector-wise ──
    try:
        resp = session.get(BRECORDER_FIPI_SECTOR, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            fipi_data, sector_data = _parse_brecorder_fipi(soup, date_str)
            if fipi_data:
                result["fipi"] = fipi_data
                result["sector"] = sector_data
                log.info("Tier 2a BRecorder FIPI SUCCESS")
    except Exception as e:
        log.debug("Tier 2a BRecorder FIPI failed: %s", e)

    # ── LIPI sector-wise ──
    try:
        resp = session.get(BRECORDER_LIPI_SECTOR, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            lipi_data = _parse_brecorder_lipi(soup, date_str)
            if lipi_data:
                result["lipi"] = lipi_data
                log.info("Tier 2a BRecorder LIPI SUCCESS")
    except Exception as e:
        log.debug("Tier 2a BRecorder LIPI failed: %s", e)

    return result


def _parse_brecorder_fipi(soup: BeautifulSoup, date_str: str) -> tuple[dict | None, list[dict] | None]:
    """Parse BRecorder FIPI sector-wise table.

    Returns (fipi_summary, sector_list).
    BRecorder columns: Client Type, Date, Sector, Market Type,
                       Buy Volume, Buy Value, Sell Volume, Sell Value,
                       Net Volume, Net Value, USD
    """
    table = soup.find("table")
    if not table:
        return None, None

    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)

    if len(rows) < 2:
        return None, None

    headers = [h.lower().strip() for h in rows[0]]
    type_col = _find_col(headers, ["client type", "client"])
    date_col = _find_col(headers, ["date"])
    sector_col = _find_col(headers, ["sector"])
    buy_col = _find_col(headers, ["buy value"])
    sell_col = _find_col(headers, ["sell value"])
    net_col = _find_col(headers, ["net value"])

    if buy_col is None or sell_col is None:
        return None, None

    # Aggregate by client type (across all sectors, REG market only)
    type_totals: dict[str, dict] = {}
    sector_totals: dict[str, dict] = {}

    for row in rows[1:]:
        if len(row) <= max(buy_col, sell_col):
            continue

        buy_val = _parse_value(row[buy_col]) or 0
        sell_val = abs(_parse_value(row[sell_col]) or 0)  # BRecorder shows sell as negative
        net_val = _parse_value(row[net_col]) if net_col is not None and net_col < len(row) else buy_val - sell_val

        # By client type
        if type_col is not None and type_col < len(row):
            ct = row[type_col].strip()
            if ct not in type_totals:
                type_totals[ct] = {"buy": 0, "sell": 0, "net": 0}
            type_totals[ct]["buy"] += buy_val
            type_totals[ct]["sell"] += sell_val
            type_totals[ct]["net"] += net_val or (buy_val - sell_val)

        # By sector
        if sector_col is not None and sector_col < len(row):
            sec = row[sector_col].strip()
            if sec and sec.lower() not in ("total", "grand total"):
                if sec not in sector_totals:
                    sector_totals[sec] = {"buy": 0, "sell": 0, "net": 0}
                sector_totals[sec]["buy"] += buy_val
                sector_totals[sec]["sell"] += sell_val
                sector_totals[sec]["net"] += net_val or (buy_val - sell_val)

    if not type_totals:
        return None, None

    # Build FIPI summary
    total_buy = sum(v["buy"] for v in type_totals.values())
    total_sell = sum(v["sell"] for v in type_totals.values())
    total_net = sum(v["net"] for v in type_totals.values())

    fipi = {
        "date": date_str,
        "fpi_buy": total_buy,
        "fpi_sell": total_sell,
        "fpi_net": total_net,
    }

    # Map client type breakdowns
    for ct, vals in type_totals.items():
        prefix = _match_client_type(ct, _FIPI_CLIENT_MAP)
        if prefix:
            fipi[f"{prefix}_net"] = vals["net"]

    # Build sector list
    sectors = [
        {"date": date_str, "sector": sec, "fpi_buy": v["buy"], "fpi_sell": v["sell"], "fpi_net": v["net"]}
        for sec, v in sector_totals.items()
    ]

    return fipi, sectors or None


def _parse_brecorder_lipi(soup: BeautifulSoup, date_str: str) -> dict | None:
    """Parse BRecorder LIPI sector-wise table into aggregate by investor type."""
    table = soup.find("table")
    if not table:
        return None

    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)

    if len(rows) < 2:
        return None

    headers = [h.lower().strip() for h in rows[0]]
    type_col = _find_col(headers, ["client type", "client"])
    buy_col = _find_col(headers, ["buy value"])
    sell_col = _find_col(headers, ["sell value"])
    net_col = _find_col(headers, ["net value"])

    if type_col is None or buy_col is None or sell_col is None:
        return None

    # Aggregate by mapped investor type (across all sectors)
    type_totals: dict[str, dict] = {}

    for row in rows[1:]:
        if len(row) <= max(type_col, buy_col, sell_col):
            continue

        ct = row[type_col].strip()
        prefix = _match_client_type(ct, _LIPI_CLIENT_MAP)
        if prefix is None:
            continue

        buy_val = _parse_value(row[buy_col]) or 0
        sell_val = abs(_parse_value(row[sell_col]) or 0)
        net_val = _parse_value(row[net_col]) if net_col is not None and net_col < len(row) else buy_val - sell_val

        if prefix not in type_totals:
            type_totals[prefix] = {"buy": 0, "sell": 0, "net": 0}
        type_totals[prefix]["buy"] += buy_val
        type_totals[prefix]["sell"] += sell_val
        type_totals[prefix]["net"] += net_val or (buy_val - sell_val)

    if not type_totals:
        return None

    result = {"date": date_str}
    for prefix, vals in type_totals.items():
        result[f"{prefix}_buy"] = vals["buy"]
        result[f"{prefix}_sell"] = vals["sell"]
        result[f"{prefix}_net"] = vals["net"]

    return result


# ═══════════════════════════════════════════════════════
# TIER 2b — NCCPL Weekly Excel Files
# ═══════════════════════════════════════════════════════


def _tier2b_nccpl_excel(session, date_str: str) -> dict:
    """Tier 2b: NCCPL weekly Excel files — static URLs bypass CF."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    date_fmt1 = f"{d.year:04d}_{d.month:02d}_{d.day:02d}"
    date_fmt2 = f"{d.day:02d}{d.month:02d}{d.year:04d}"

    candidate_urls = [
        f"https://www.nccpl.com.pk/uploads/files/fipi/FIPI_LIPI_{date_fmt1}.xlsx",
        f"https://www.nccpl.com.pk/uploads/files/fipi/FIPI_{date_fmt2}.xlsx",
        f"https://www.nccpl.com.pk/uploads/fipi/FIPI_LIPI_{date_fmt1}.xlsx",
        f"https://www.nccpl.com.pk/media/fipi/FIPI_LIPI_{date_fmt1}.xlsx",
    ]

    result = {"fipi": None, "lipi": None, "sector": None}

    for url in candidate_urls:
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 1000:
                sheets = pd.read_excel(BytesIO(resp.content), sheet_name=None)
                log.info("Tier 2b Excel downloaded: %s (sheets: %s)", url, list(sheets.keys()))

                result["fipi"] = _parse_excel_fipi(sheets, date_str)
                result["lipi"] = _parse_excel_lipi(sheets, date_str)
                return result
        except Exception:
            continue

    return result


def _parse_excel_fipi(sheets: dict[str, pd.DataFrame], date_str: str) -> dict | None:
    """Parse FIPI data from Excel sheets."""
    for name, df in sheets.items():
        if "fipi" in name.lower() or "foreign" in name.lower():
            # Look for buy/sell/net columns
            cols = [c.lower() for c in df.columns]
            buy_col = next((i for i, c in enumerate(cols) if "buy" in c and "val" in c), None)
            sell_col = next((i for i, c in enumerate(cols) if "sell" in c and "val" in c), None)
            net_col = next((i for i, c in enumerate(cols) if "net" in c and "val" in c), None)

            if buy_col is not None and sell_col is not None:
                total_buy = df.iloc[:, buy_col].sum()
                total_sell = df.iloc[:, sell_col].sum()
                total_net = df.iloc[:, net_col].sum() if net_col is not None else total_buy - total_sell
                return {
                    "date": date_str,
                    "fpi_buy": float(total_buy),
                    "fpi_sell": float(total_sell),
                    "fpi_net": float(total_net),
                }
    return None


def _parse_excel_lipi(sheets: dict[str, pd.DataFrame], date_str: str) -> dict | None:
    """Parse LIPI data from Excel sheets."""
    for name, df in sheets.items():
        if "lipi" in name.lower() or "local" in name.lower():
            cols = [str(c).lower() for c in df.columns]
            type_col = next((i for i, c in enumerate(cols) if "client" in c or "type" in c), None)
            buy_col = next((i for i, c in enumerate(cols) if "buy" in c and "val" in c), None)
            sell_col = next((i for i, c in enumerate(cols) if "sell" in c and "val" in c), None)
            net_col = next((i for i, c in enumerate(cols) if "net" in c and "val" in c), None)

            if type_col is None or buy_col is None or sell_col is None:
                continue

            result = {"date": date_str}
            for _, row in df.iterrows():
                raw_type = str(row.iloc[type_col])
                prefix = _match_client_type(raw_type, _LIPI_CLIENT_MAP)
                if prefix:
                    bv = float(row.iloc[buy_col]) if pd.notna(row.iloc[buy_col]) else 0
                    sv = float(row.iloc[sell_col]) if pd.notna(row.iloc[sell_col]) else 0
                    nv = float(row.iloc[net_col]) if net_col is not None and pd.notna(row.iloc[net_col]) else bv - sv
                    result[f"{prefix}_buy"] = bv
                    result[f"{prefix}_sell"] = sv
                    result[f"{prefix}_net"] = nv

            if len(result) > 1:
                return result
    return None


# ═══════════════════════════════════════════════════════
# TIER 3 — Mettis Global / Business Recorder
# ═══════════════════════════════════════════════════════


def _tier3_mettis(session, date_str: str) -> dict | None:
    """Tier 3: Mettis Global article text — net FPI only, no breakdown."""
    try:
        resp = session.get(METTIS_SEARCH, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find recent FIPI article link
        articles = soup.find_all("a", href=True)
        fipi_links = [
            a["href"] for a in articles
            if "fipi" in a.get_text().lower() or "foreign" in a.get_text().lower()
        ]

        if not fipi_links:
            return None

        # Fetch article and extract net figure
        article_resp = session.get(fipi_links[0], headers=HEADERS, timeout=15)
        if article_resp.status_code != 200:
            return None

        text = BeautifulSoup(article_resp.text, "html.parser").get_text()

        # Pattern: "net foreign ... Rs. X million" or "FIPI net Rs X mn"
        net_match = re.search(
            r"(?:net foreign|FIPI net)[^\d\-]*([+-]?\d[\d,\.]+)\s*(?:million|mn|M)",
            text, re.IGNORECASE,
        )

        if net_match:
            fpi_net = float(net_match.group(1).replace(",", ""))
            log.info("Tier 3 Mettis SUCCESS: net FPI = %.1f", fpi_net)
            return {
                "date": date_str,
                "fpi_buy": None,
                "fpi_sell": None,
                "fpi_net": fpi_net,
            }

        return None
    except Exception as e:
        log.debug("Tier 3 Mettis failed: %s", e)
        return None


def _tier3_brecorder(session, date_str: str) -> dict | None:
    """Tier 3b: Business Recorder — similar article text parsing."""
    try:
        resp = session.get(BRECORDER_SEARCH, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.find_all("a", href=True)
        fipi_links = [
            a["href"] for a in articles
            if "fipi" in a.get_text().lower() or "foreign" in a.get_text().lower()
        ]

        if not fipi_links:
            return None

        article_resp = session.get(fipi_links[0], headers=HEADERS, timeout=15)
        if article_resp.status_code != 200:
            return None

        text = BeautifulSoup(article_resp.text, "html.parser").get_text()

        net_match = re.search(
            r"(?:net (?:foreign|FPI|FIPI))[^\d\-]*([+-]?\d[\d,\.]+)\s*(?:million|mn|M)",
            text, re.IGNORECASE,
        )

        if net_match:
            fpi_net = float(net_match.group(1).replace(",", ""))
            log.info("Tier 3b BRecorder SUCCESS: net FPI = %.1f", fpi_net)
            return {
                "date": date_str,
                "fpi_buy": None,
                "fpi_sell": None,
                "fpi_net": fpi_net,
            }

        return None
    except Exception as e:
        log.debug("Tier 3b BRecorder failed: %s", e)
        return None


# ═══════════════════════════════════════════════════════
# DERIVED SIGNAL COMPUTATION
# ═══════════════════════════════════════════════════════


def compute_derived_signals(con: sqlite3.Connection) -> pd.DataFrame | None:
    """Join FIPI + LIPI, compute rolling and contrarian signals.

    Returns the derived DataFrame (also written to nccpl_flows_derived).
    """
    try:
        df = pd.read_sql(
            """SELECT f.date,
                      f.fpi_net,
                      l.mf_net, l.insurance_net, l.bank_net,
                      l.retail_net, l.corporate_net, l.broker_net
               FROM nccpl_fipi f
               JOIN nccpl_lipi l ON f.date = l.date
               ORDER BY f.date""",
            con, parse_dates=["date"],
        )
    except Exception as e:
        log.error("Cannot compute derived signals: %s", e)
        return None

    if df.empty or len(df) < 2:
        log.warning("Not enough data for derived signals (%d rows)", len(df))
        return None

    # Rolling 4-week (20 trading days)
    df["fpi_net_4w"] = df["fpi_net"].rolling(20, min_periods=1).sum()
    df["mf_net_4w"] = df["mf_net"].rolling(20, min_periods=1).sum()
    df["retail_net_4w"] = df["retail_net"].rolling(20, min_periods=1).sum()
    df["bank_net_4w"] = df["bank_net"].rolling(20, min_periods=1).sum()

    # Smart vs Dumb money
    df["smart_money_net"] = df["fpi_net"] + df["mf_net"].fillna(0) + df["insurance_net"].fillna(0)
    df["dumb_money_net"] = df["retail_net"]
    df["smart_dumb_ratio"] = df["smart_money_net"] / (df["dumb_money_net"].abs() + 1)

    # Institutional consensus — FPI, MF, banks all same direction?
    df["institutional_consensus"] = (
        (np.sign(df["fpi_net"]) == np.sign(df["mf_net"]))
        & (np.sign(df["mf_net"]) == np.sign(df["bank_net"]))
    ).astype(int)

    # Foreign vs Retail divergence — the key contrarian signal
    df["foreign_domestic_divergence"] = (
        np.sign(df["fpi_net"]) != np.sign(df["retail_net"])
    ).astype(int)

    # Flow regime signal
    def _classify(row):
        if row["smart_dumb_ratio"] > 1.5:
            return "BULLISH"
        elif row["smart_dumb_ratio"] < -1.5:
            return "BEARISH"
        elif row["foreign_domestic_divergence"] == 1:
            return "DIVERGENT"
        return "NEUTRAL"

    df["flow_regime_signal"] = df.apply(_classify, axis=1)

    # Convert date back to string for storage
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Write to DB
    upsert_derived(con, df)
    log.info("Derived signals computed for %d rows", len(df))

    return df


# ═══════════════════════════════════════════════════════
# MASTER RUNNER — 3-TIER FALLBACK
# ═══════════════════════════════════════════════════════


def fetch_with_fallback(date_str: str, con: sqlite3.Connection) -> dict:
    """Fetch NCCPL flows for a single date using 3-tier fallback.

    Returns result dict with keys: date, fipi, lipi, sector, source, tier.
    """
    result = {
        "date": date_str,
        "fipi": None, "lipi": None, "sector": None,
        "source": None, "tier": None,
    }

    session = _get_session()

    # ── TIER 1: curl_cffi → NCCPL Direct ──
    log.info("Tier 1: curl_cffi NCCPL direct for %s", date_str)
    t1 = _tier1_nccpl_direct(session, date_str)

    if t1["fipi"]:
        result.update(t1)
        result["source"] = "nccpl_direct"
        result["tier"] = 1
        _store_result(result, con)
        return result

    # ── TIER 2a: BRecorder (mirrors NCCPL, no Cloudflare) ──
    log.info("Tier 2a: BRecorder tables for %s", date_str)
    t2a = _tier2a_brecorder_tables(session, date_str)
    if t2a["fipi"]:
        result.update(t2a)
        result["source"] = "brecorder"
        result["tier"] = 2
        _store_result(result, con)
        return result

    # ── TIER 2b: NCCPL Excel ──
    log.info("Tier 2b: NCCPL Excel for %s", date_str)
    t2b = _tier2b_nccpl_excel(session, date_str)
    if t2b["fipi"]:
        result.update(t2b)
        result["source"] = "nccpl_excel"
        result["tier"] = 2
        _store_result(result, con)
        return result

    # ── TIER 3: Mettis Global ──
    log.info("Tier 3: Mettis for %s", date_str)
    mettis = _tier3_mettis(session, date_str)
    if mettis:
        result["fipi"] = mettis
        result["source"] = "mettis"
        result["tier"] = 3
        log.warning("Tier 3 only — net FPI available, no breakdown for %s", date_str)
        _store_result(result, con)
        return result

    # ── ALL FAILED ──
    log.error("ALL TIERS FAILED for %s", date_str)
    return result


def _store_result(result: dict, con: sqlite3.Connection) -> None:
    """Store fetch results to SQLite."""
    if result["fipi"]:
        upsert_fipi(con, result["fipi"])
    if result["lipi"]:
        upsert_lipi(con, result["lipi"])
    if result["sector"]:
        upsert_fipi_sector(con, result["sector"])


def run_backfill(
    from_date: str,
    to_date: str,
    con: sqlite3.Connection,
    delay_sec: float = 2.0,
) -> list[dict]:
    """Backfill missing dates. Skips weekends and already-fetched dates.

    NOTE: BRecorder (Tier 2a) only serves the current day's data.
    Backfill only works reliably via:
      - Tier 1 (NCCPL direct, requires CF bypass)
      - Tier 2b (NCCPL Excel files, if URL patterns match)
    For daily use, run once per trading day to accumulate history.
    """
    log.warning(
        "Backfill note: BRecorder only shows today's data. "
        "Historical dates require NCCPL direct (Tier 1) or Excel (Tier 2b)."
    )

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    current = start
    all_results = []

    while current <= end:
        if current.weekday() < 5:  # Skip weekends
            date_str = current.strftime("%Y-%m-%d")
            if not date_already_fetched(con, date_str):
                log.info("Fetching %s...", date_str)
                result = fetch_with_fallback(date_str, con)
                all_results.append(result)
                time.sleep(delay_sec)  # Polite delay
            else:
                log.debug("Skipping %s — already in DB", date_str)
        current += timedelta(days=1)

    # Recompute derived signals after full backfill
    compute_derived_signals(con)

    return all_results


# ═══════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="NCCPL Flow Intelligence Fetcher")
    parser.add_argument("--date", help="Fetch single date (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="Backfill mode")
    parser.add_argument("--from", dest="from_date", help="Backfill start date")
    parser.add_argument("--to", dest="to_date", help="Backfill end date (default: today)")
    parser.add_argument("--compute-only", action="store_true", help="Only recompute derived signals")
    args = parser.parse_args()

    # Ensure log directory exists
    log_dir = Path.home() / "pakfindata" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / "nccpl_fetcher.log"),
        ],
    )

    con = connect()
    init_schema(con)

    if args.compute_only:
        df = compute_derived_signals(con)
        if df is not None:
            print(f"Derived signals computed for {len(df)} rows")
            print(df[["date", "smart_dumb_ratio", "flow_regime_signal"]].tail(5).to_string())
        return

    if args.date:
        result = fetch_with_fallback(args.date, con)
        # Recompute derived after single-date fetch
        compute_derived_signals(con)
        _print_result(result)

    elif args.backfill:
        if not args.from_date:
            parser.error("--backfill requires --from date")
        to_date = args.to_date or datetime.now().strftime("%Y-%m-%d")
        results = run_backfill(args.from_date, to_date, con)
        print(f"\nBackfill complete: {len(results)} dates processed")
        tiers = {}
        for r in results:
            t = r.get("tier") or "failed"
            tiers[t] = tiers.get(t, 0) + 1
        for t, c in sorted(tiers.items(), key=lambda x: str(x[0])):
            print(f"  Tier {t}: {c} dates")

    else:
        # Default: fetch today
        today = datetime.now().strftime("%Y-%m-%d")
        result = fetch_with_fallback(today, con)
        compute_derived_signals(con)
        _print_result(result)


def _print_result(result: dict) -> None:
    """Pretty-print a fetch result."""
    print(f"\n{'='*50}")
    print(f"  Date:   {result['date']}")
    print(f"  Source: {result['source']} (Tier {result['tier']})")
    print(f"  FIPI:   {result['fipi']}")
    print(f"  LIPI:   {'available' if result['lipi'] else 'N/A'}")
    print(f"  Sector: {'available' if result['sector'] else 'N/A'}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
