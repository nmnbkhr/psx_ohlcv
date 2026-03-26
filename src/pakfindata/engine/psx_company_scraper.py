"""
PSX DPS Global Page Scrapers — announcements, payouts, calendar, circuit breakers.

These scrape the *global* pages on dps.psx.com.pk that list data for ALL companies
at once, which is much faster than per-company scraping.

Per-company deep scraping is handled by pakfindata.sources.deep_scraper.
"""

import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from lxml import html as lxml_html

PKT = timezone(timedelta(hours=5))
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
BASE_URL = "https://dps.psx.com.pk"
RATE_LIMIT_DELAY = 0.5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

logger = logging.getLogger(__name__)


# ─── Global Announcements Scraper ─────────────────────────────────────────


def scrape_global_announcements(
    ann_type: str = "companies",
    max_pages: int = 5,
    delay: float = RATE_LIMIT_DELAY,
) -> list[dict]:
    """
    Scrape announcements from the global DPS announcements page.

    PSX uses POST /announcements with form data to load results.
    ann_type: 'companies' (type=C), 'cdc' (A), 'secp' (B), 'nccpl' (D), 'psx' (E)
    Each page has 50 entries. Returns list of dicts.
    """
    type_map = {"companies": "C", "cdc": "A", "secp": "B", "nccpl": "D", "psx": "E"}
    ann_code = type_map.get(ann_type, "C")
    url = f"{BASE_URL}/announcements"
    all_anns = []

    for page_num in range(max_pages):
        try:
            data = {
                "type": ann_code,
                "symbol": "",
                "query": "",
                "count": 50,
                "offset": page_num * 50,
                "date_from": "",
                "date_to": "",
            }
            resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                logger.error(f"HTTP {resp.status_code} for announcements page {page_num}")
                break

            tree = lxml_html.fromstring(resp.text)
            rows = tree.xpath('//tr')
            page_count = 0

            for row in rows:
                cells = row.xpath('.//td')
                if len(cells) < 3:
                    continue

                pdf_url = ""
                links = row.xpath('.//a/@href')
                for href in links:
                    if 'download' in href:
                        pdf_url = href if href.startswith('http') else f"{BASE_URL}{href}"
                        break

                texts = [c.text_content().strip() for c in cells]

                if len(texts) >= 4:
                    all_anns.append({
                        "date": texts[0],
                        "time": texts[1],
                        "company": texts[2],
                        "subject": texts[3],
                        "pdf_url": pdf_url,
                        "type": ann_code,
                    })
                    page_count += 1
                elif len(texts) >= 3:
                    all_anns.append({
                        "date": texts[0],
                        "time": "",
                        "company": texts[1],
                        "subject": texts[2],
                        "pdf_url": pdf_url,
                        "type": ann_code,
                    })
                    page_count += 1

            if page_count == 0:
                break  # No more results

            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error scraping announcements page {page_num}: {e}")
            break

    return all_anns


# ─── Global Payouts Scraper ───────────────────────────────────────────────


def scrape_global_payouts(max_pages: int = 20, delay: float = RATE_LIMIT_DELAY) -> list[dict]:
    """Scrape all payout records from the global payouts page (POST with pagination)."""
    url = f"{BASE_URL}/payouts"
    all_payouts = []

    for page_num in range(max_pages):
        try:
            data = {"symbol": "", "count": 25, "offset": page_num * 25}
            resp = requests.post(url, data=data, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                logger.error(f"HTTP {resp.status_code} for payouts page {page_num}")
                break

            tree = lxml_html.fromstring(resp.text)
            rows = tree.xpath('//tr')
            page_count = 0

            for row in rows:
                cells = row.xpath('.//td')
                if len(cells) < 5:
                    continue

                texts = [c.text_content().strip() for c in cells]
                symbol = texts[0]
                company = texts[1] if len(texts) > 5 else ""
                sector = texts[2] if len(texts) > 5 else ""

                details = texts[3] if len(texts) > 5 else texts[2]
                div_pct, div_type, is_interim = _parse_dividend_details(details)

                date_time = texts[4] if len(texts) > 5 else texts[3]
                date_str, time_str = _parse_datetime(date_time)

                book_closure = texts[5] if len(texts) > 5 else texts[4]
                bc_start, bc_end = _parse_book_closure(book_closure)

                all_payouts.append({
                    "symbol": symbol,
                    "company_name": company,
                    "sector": sector,
                    "details": details,
                    "dividend_pct": div_pct,
                    "dividend_type": div_type,
                    "is_interim": is_interim,
                    "date": date_str,
                    "time": time_str,
                    "book_closure_start": bc_start,
                    "book_closure_end": bc_end,
                })
                page_count += 1

            if page_count == 0:
                break

            time.sleep(delay)

        except Exception as e:
            logger.error(f"Error scraping payouts page {page_num}: {e}")
            break

    return all_payouts


# ─── Global Calendar Scraper ─────────────────────────────────────────────


def scrape_corporate_calendar(delay: float = RATE_LIMIT_DELAY) -> list[dict]:
    """Scrape corporate calendar (AGMs, EGMs, book closures)."""
    url = f"{BASE_URL}/calendar"
    events = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return []

        tree = lxml_html.fromstring(resp.text)
        rows = tree.xpath('//table//tr')

        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) < 3:
                continue
            texts = [c.text_content().strip() for c in cells]
            events.append({
                "date": texts[0],
                "symbol": texts[1] if len(texts) > 2 else "",
                "event": texts[2] if len(texts) > 2 else texts[1],
                "details": texts[3] if len(texts) > 3 else "",
            })

    except Exception as e:
        logger.error(f"Error scraping calendar: {e}")

    return events


# ─── Circuit Breakers ─────────────────────────────────────────────────────


def scrape_circuit_breakers() -> list[dict]:
    """Scrape daily circuit breaker limits for all symbols."""
    url = f"{BASE_URL}/circuit-breakers"
    records = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return []

        tree = lxml_html.fromstring(resp.text)
        rows = tree.xpath('//table//tr')

        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) < 3:
                continue
            texts = [c.text_content().strip() for c in cells]
            records.append({
                "symbol": texts[0],
                "lower_limit": texts[1] if len(texts) > 1 else "",
                "upper_limit": texts[2] if len(texts) > 2 else "",
            })

    except Exception as e:
        logger.error(f"Error scraping circuit breakers: {e}")

    return records


# ─── DB Save ──────────────────────────────────────────────────────────────


def save_announcements_to_db(announcements: list[dict], con: sqlite3.Connection = None) -> int:
    """Save announcements to corporate_announcements table."""
    own_con = con is None
    if own_con:
        con = sqlite3.connect(str(PSX_SQLITE))
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")

    con.execute("""
        CREATE TABLE IF NOT EXISTS corporate_announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, announcement_date TEXT, announcement_type TEXT,
            category TEXT, title TEXT, title_hash TEXT,
            document_url TEXT, scraped_at TEXT,
            UNIQUE(title_hash)
        )
    """)

    inserted = 0
    now = datetime.now(PKT).isoformat()

    for ann in announcements:
        title = ann.get("subject", "")
        company = ann.get("company", "")
        # Try to extract symbol from company text
        symbol = company.split("(")[-1].replace(")", "").strip() if "(" in company else company.strip()

        title_hash = f"{ann.get('date','')}_{symbol}_{title[:50]}"

        try:
            con.execute("""
                INSERT OR IGNORE INTO corporate_announcements
                (symbol, announcement_date, announcement_type, category, title, title_hash, document_url, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                ann.get("date", ""),
                ann.get("type", "C"),
                "company",
                title,
                title_hash,
                ann.get("pdf_url", ""),
                now,
            ))
            inserted += con.total_changes
        except Exception:
            pass

    con.commit()
    if own_con:
        con.close()

    return inserted


def save_payouts_to_db(payouts: list[dict], con: sqlite3.Connection = None) -> int:
    """Save payouts to dividend_payouts table."""
    own_con = con is None
    if own_con:
        con = sqlite3.connect(str(PSX_SQLITE))
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")

    con.execute("""
        CREATE TABLE IF NOT EXISTS dividend_payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, announcement_date TEXT, announcement_time TEXT,
            fiscal_period TEXT, dividend_percent REAL, dividend_type TEXT,
            dividend_number TEXT, book_closure_from TEXT, book_closure_to TEXT,
            scraped_at TEXT,
            UNIQUE(symbol, announcement_date, dividend_percent, dividend_type)
        )
    """)

    inserted = 0
    now = datetime.now(PKT).isoformat()

    for p in payouts:
        try:
            con.execute("""
                INSERT OR IGNORE INTO dividend_payouts
                (symbol, announcement_date, announcement_time, fiscal_period,
                 dividend_percent, dividend_type, dividend_number,
                 book_closure_from, book_closure_to, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                p["symbol"],
                p.get("date", ""),
                p.get("time", ""),
                "",
                p.get("dividend_pct", 0),
                p.get("dividend_type", "D"),
                "interim" if p.get("is_interim") else "final",
                p.get("book_closure_start", ""),
                p.get("book_closure_end", ""),
                now,
            ))
            inserted += con.total_changes
        except Exception:
            pass

    con.commit()
    if own_con:
        con.close()

    return inserted


# ─── Helpers ──────────────────────────────────────────────────────────────


def _parse_dividend_details(details: str) -> tuple:
    """Parse '42.50%(ii) (D)' -> (42.5, 'D', True)."""
    pct = 0.0
    div_type = "D"
    is_interim = False

    m = re.search(r'([\d.]+)%', details)
    if m:
        pct = float(m.group(1))

    if '(R)' in details:
        div_type = "R"
    elif '(B)' in details:
        div_type = "B"

    if any(x in details for x in ['(i)', '(ii)', '(iii)', '(iv)']):
        is_interim = True
    if '(F)' in details:
        is_interim = False

    return pct, div_type, is_interim


def _parse_book_closure(text: str) -> tuple:
    parts = text.split(' - ')
    return (parts[0].strip() if parts else "", parts[1].strip() if len(parts) > 1 else "")


def _parse_datetime(text: str) -> tuple:
    m = re.match(r'(.+?)\s+(\d{1,2}:\d{2}\s*[AP]M)', text, re.I)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return text, ""
