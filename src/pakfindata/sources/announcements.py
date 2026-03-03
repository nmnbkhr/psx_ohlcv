"""PSX Company Announcements Scraper.

Scrapes company announcements, AGM/EOGM calendar, and dividend payouts from PSX DPS.

API Endpoints:
- POST /announcements - Company announcements (HTML)
- POST /calendar - AGM/EOGM calendar (JSON)
- POST /company/payouts - Company dividend history (HTML)
- GET /company/reports/{symbol} - Financial reports (HTML)
"""

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from lxml import html

logger = logging.getLogger(__name__)

# PSX DPS Base URL
PSX_BASE_URL = "https://dps.psx.com.pk"

# Announcement type codes
ANNOUNCEMENT_TYPES = {
    "A": "CDC Notices",
    "B": "SECP Notices",
    "C": "Company Announcements",
    "D": "NCCPL Notices",
    "E": "PSX Notices",
}

# Default headers
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded",
}


@dataclass
class AnnouncementRecord:
    """Single company announcement record."""
    symbol: str
    company_name: str
    announcement_date: str
    announcement_time: str
    title: str
    category: str | None = None
    image_id: str | None = None
    pdf_id: str | None = None


@dataclass
class CorporateEvent:
    """AGM/EOGM/ARM event record."""
    id: int
    symbol: str
    company_name: str
    event_type: str
    event_date: str
    event_time: str | None
    city: str | None
    period_end: str | None


@dataclass
class DividendPayout:
    """Dividend payout record."""
    symbol: str
    announcement_date: str
    announcement_time: str | None
    fiscal_period: str | None
    dividend_percent: float | None
    dividend_type: str | None  # 'interim', 'final', 'bonus'
    dividend_number: str | None  # 'i', 'ii', 'iii', 'F'
    book_closure_from: str | None
    book_closure_to: str | None


# =============================================================================
# Fetching Functions
# =============================================================================


def fetch_announcements(
    symbol: str | None = None,
    announcement_type: str = "C",
    year: int | None = None,
    month: int | None = None,
    keyword: str | None = None,
    offset: int = 0,
    timeout: int = 30,
) -> tuple[list[AnnouncementRecord], int]:
    """
    Fetch company announcements from PSX DPS.

    Args:
        symbol: Stock symbol (optional, fetches all if None)
        announcement_type: Type code ('C' for company announcements)
        year: Year filter (optional)
        month: Month filter (optional)
        keyword: Search keyword (optional)
        offset: Pagination offset
        timeout: Request timeout

    Returns:
        Tuple of (list of AnnouncementRecord, total count)
    """
    url = f"{PSX_BASE_URL}/announcements"

    data = {
        "type": announcement_type,
        "symbol": symbol or "",
        "keyword": keyword or "",
        "month": str(month) if month else "",
        "year": str(year) if year else "",
        "offset": str(offset),
    }

    try:
        response = requests.post(
            url,
            data=data,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()

        return _parse_announcements_html(response.text, symbol)

    except requests.RequestException as e:
        logger.error(f"Failed to fetch announcements: {e}")
        return [], 0


def _parse_announcements_html(html_content: str, default_symbol: str | None = None) -> tuple[list[AnnouncementRecord], int]:
    """Parse announcements HTML response."""
    records = []
    total = 0

    try:
        tree = html.fromstring(html_content)

        # Extract total count from header
        header = tree.xpath('//div[@class="announcementsResults__header"]/div/text()')
        if header:
            match = re.search(r'of\s+(\d+)\s+entries', header[0])
            if match:
                total = int(match.group(1))

        # Parse table rows
        rows = tree.xpath('//table[@id="announcementsTable"]//tbody//tr')

        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) >= 5:
                # Date
                date_text = cells[0].text_content().strip()
                announcement_date = _parse_date(date_text)

                # Time
                time_text = cells[1].text_content().strip()
                announcement_time = _parse_time(time_text)

                # Symbol
                symbol_elem = cells[2].xpath('.//a/strong/text()')
                symbol = symbol_elem[0].strip() if symbol_elem else default_symbol or ""

                # Company name
                name_elem = cells[3].xpath('.//a/strong/text()')
                company_name = name_elem[0].strip() if name_elem else ""

                # Title
                title = cells[4].text_content().strip()

                # Extract image/PDF IDs from last cell
                image_id = None
                pdf_id = None
                if len(cells) > 5:
                    # Image ID from data-images attribute
                    img_attr = cells[5].xpath('.//a[@data-images]/@data-images')
                    if img_attr:
                        image_id = img_attr[0]

                    # PDF ID from href
                    pdf_href = cells[5].xpath('.//a[contains(@href, "/download/document/")]/@href')
                    if pdf_href:
                        match = re.search(r'/download/document/(\d+)\.pdf', pdf_href[0])
                        if match:
                            pdf_id = match.group(1)

                # Categorize based on title
                category = _categorize_announcement(title)

                records.append(AnnouncementRecord(
                    symbol=symbol,
                    company_name=company_name,
                    announcement_date=announcement_date,
                    announcement_time=announcement_time,
                    title=title,
                    category=category,
                    image_id=image_id,
                    pdf_id=pdf_id,
                ))

    except Exception as e:
        logger.error(f"Failed to parse announcements HTML: {e}")

    return records, total


def _categorize_announcement(title: str) -> str:
    """Categorize announcement based on title keywords."""
    title_lower = title.lower()

    if any(kw in title_lower for kw in ['financial result', 'quarterly', 'half year', 'annual result']):
        return 'results'
    elif any(kw in title_lower for kw in ['dividend', 'bonus', 'payout']):
        return 'dividend'
    elif any(kw in title_lower for kw in ['agm', 'annual general', 'eogm', 'extraordinary']):
        return 'agm'
    elif any(kw in title_lower for kw in ['board meeting']):
        return 'board_meeting'
    elif any(kw in title_lower for kw in ['book closure']):
        return 'book_closure'
    elif any(kw in title_lower for kw in ['credit of', 'transmission']):
        return 'corporate_action'
    else:
        return 'general'


def fetch_corporate_events(
    from_date: str,
    to_date: str,
    timeout: int = 30,
) -> list[CorporateEvent]:
    """
    Fetch AGM/EOGM/ARM calendar from PSX DPS (JSON API).

    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        timeout: Request timeout

    Returns:
        List of CorporateEvent records
    """
    url = f"{PSX_BASE_URL}/calendar"

    data = {
        "from": from_date,
        "to": to_date,
    }

    try:
        response = requests.post(
            url,
            data=data,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()

        json_data = response.json()

        if json_data.get("status") != 1:
            logger.warning(f"Calendar API returned non-success status")
            return []

        events = []
        for item in json_data.get("data", []):
            events.append(CorporateEvent(
                id=item.get("id"),
                symbol=item.get("symbol", ""),
                company_name=item.get("name", ""),
                event_type=item.get("type", ""),
                event_date=item.get("date", ""),
                event_time=item.get("time"),
                city=item.get("city"),
                period_end=item.get("period_end"),
            ))

        return events

    except requests.RequestException as e:
        logger.error(f"Failed to fetch corporate events: {e}")
        return []
    except (ValueError, KeyError) as e:
        logger.error(f"Failed to parse corporate events JSON: {e}")
        return []


def fetch_company_payouts(
    symbol: str,
    timeout: int = 30,
) -> list[DividendPayout]:
    """
    Fetch dividend payout history for a company.

    Args:
        symbol: Stock symbol
        timeout: Request timeout

    Returns:
        List of DividendPayout records
    """
    url = f"{PSX_BASE_URL}/company/payouts"

    data = {"symbol": symbol.upper()}

    try:
        response = requests.post(
            url,
            data=data,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()

        return _parse_payouts_html(response.text, symbol)

    except requests.RequestException as e:
        logger.error(f"Failed to fetch payouts for {symbol}: {e}")
        return []


def _parse_payouts_html(html_content: str, symbol: str) -> list[DividendPayout]:
    """Parse payouts HTML response."""
    payouts = []

    try:
        tree = html.fromstring(html_content)

        rows = tree.xpath('//table//tbody//tr')

        for row in rows:
            cells = row.xpath('.//td')
            if len(cells) >= 4:
                # Date/Time (e.g., "October 23, 2025 3:48 PM")
                date_time_text = cells[0].text_content().strip()
                announcement_date, announcement_time = _parse_datetime(date_time_text)

                # Fiscal period (e.g., "30/09/2025(IIIQ)")
                fiscal_period = cells[1].text_content().strip()

                # Dividend details (e.g., "50%(iii) (D)")
                details = cells[2].text_content().strip()
                dividend_percent, dividend_type, dividend_number = _parse_dividend_details(details)

                # Book closure dates (e.g., "04/11/2025  - 05/11/2025")
                book_closure = cells[3].text_content().strip()
                book_from, book_to = _parse_book_closure(book_closure)

                payouts.append(DividendPayout(
                    symbol=symbol.upper(),
                    announcement_date=announcement_date,
                    announcement_time=announcement_time,
                    fiscal_period=fiscal_period,
                    dividend_percent=dividend_percent,
                    dividend_type=dividend_type,
                    dividend_number=dividend_number,
                    book_closure_from=book_from,
                    book_closure_to=book_to,
                ))

    except Exception as e:
        logger.error(f"Failed to parse payouts HTML: {e}")

    return payouts


def _parse_dividend_details(text: str) -> tuple[float | None, str | None, str | None]:
    """Parse dividend details like '50%(iii) (D)' or '42.5%(F) (D)'."""
    dividend_percent = None
    dividend_type = None
    dividend_number = None

    # Extract percentage
    pct_match = re.search(r'(\d+\.?\d*)%', text)
    if pct_match:
        dividend_percent = float(pct_match.group(1))

    # Extract number (i, ii, iii, F)
    num_match = re.search(r'\((i+|F)\)', text)
    if num_match:
        num = num_match.group(1)
        dividend_number = num
        if num == 'F':
            dividend_type = 'final'
        else:
            dividend_type = 'interim'

    # Check for bonus
    if '(B)' in text or 'bonus' in text.lower():
        dividend_type = 'bonus'

    return dividend_percent, dividend_type, dividend_number


def _parse_book_closure(text: str) -> tuple[str | None, str | None]:
    """Parse book closure dates like '04/11/2025  - 05/11/2025'."""
    # Match date pattern DD/MM/YYYY
    dates = re.findall(r'(\d{2}/\d{2}/\d{4})', text)

    if len(dates) >= 2:
        # Convert to YYYY-MM-DD
        try:
            from_date = datetime.strptime(dates[0], '%d/%m/%Y').strftime('%Y-%m-%d')
            to_date = datetime.strptime(dates[1], '%d/%m/%Y').strftime('%Y-%m-%d')
            return from_date, to_date
        except ValueError:
            pass
    elif len(dates) == 1:
        try:
            from_date = datetime.strptime(dates[0], '%d/%m/%Y').strftime('%Y-%m-%d')
            return from_date, from_date
        except ValueError:
            pass

    return None, None


def _parse_date(text: str) -> str:
    """Parse date like 'Dec 17, 2025' to YYYY-MM-DD."""
    try:
        dt = datetime.strptime(text, '%b %d, %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return text


def _parse_time(text: str) -> str:
    """Parse time like '1:39 PM' to HH:MM."""
    try:
        dt = datetime.strptime(text, '%I:%M %p')
        return dt.strftime('%H:%M')
    except ValueError:
        return text


def _parse_datetime(text: str) -> tuple[str, str | None]:
    """Parse datetime like 'October 23, 2025 3:48 PM'."""
    try:
        dt = datetime.strptime(text, '%B %d, %Y %I:%M %p')
        return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M')
    except ValueError:
        # Try without time
        try:
            dt = datetime.strptime(text.split()[0:3], '%B %d, %Y')
            return dt.strftime('%Y-%m-%d'), None
        except:
            return text, None


# =============================================================================
# Database Functions
# =============================================================================


def save_announcement(con: sqlite3.Connection, record: AnnouncementRecord) -> bool:
    """Save announcement to database."""
    try:
        con.execute("""
            INSERT OR REPLACE INTO company_announcements (
                symbol, company_name, announcement_date, announcement_time,
                title, category, image_id, pdf_id, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            record.symbol,
            record.company_name,
            record.announcement_date,
            record.announcement_time,
            record.title,
            record.category,
            record.image_id,
            record.pdf_id,
        ))
        con.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Failed to save announcement: {e}")
        return False


def save_corporate_event(con: sqlite3.Connection, event: CorporateEvent) -> bool:
    """Save corporate event to database."""
    try:
        con.execute("""
            INSERT OR REPLACE INTO corporate_events (
                id, symbol, company_name, event_type, event_date,
                event_time, city, period_end, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            event.id,
            event.symbol,
            event.company_name,
            event.event_type,
            event.event_date,
            event.event_time,
            event.city,
            event.period_end,
        ))
        con.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Failed to save corporate event: {e}")
        return False


def save_dividend_payout(con: sqlite3.Connection, payout: DividendPayout) -> bool:
    """Save dividend payout to database."""
    try:
        con.execute("""
            INSERT OR REPLACE INTO dividend_payouts (
                symbol, announcement_date, announcement_time,
                fiscal_period, dividend_percent, dividend_type,
                dividend_number, book_closure_from, book_closure_to,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            payout.symbol,
            payout.announcement_date,
            payout.announcement_time,
            payout.fiscal_period,
            payout.dividend_percent,
            payout.dividend_type,
            payout.dividend_number,
            payout.book_closure_from,
            payout.book_closure_to,
        ))
        con.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Failed to save dividend payout: {e}")
        return False


def get_last_announcement_date(con: sqlite3.Connection, symbol: str | None = None) -> str | None:
    """Get the most recent announcement date for resume capability."""
    try:
        if symbol:
            cur = con.execute("""
                SELECT MAX(announcement_date) FROM company_announcements
                WHERE symbol = ?
            """, (symbol,))
        else:
            cur = con.execute("""
                SELECT MAX(announcement_date) FROM company_announcements
            """)
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.Error:
        return None


def get_last_event_sync(con: sqlite3.Connection) -> str | None:
    """Get the most recent corporate event sync date."""
    try:
        cur = con.execute("""
            SELECT MAX(scraped_at) FROM corporate_events
        """)
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.Error:
        return None
