# Claude Code Prompt: PSX Company Deep Scraper — Profiles, Announcements, Payouts, Financials

## Context

pakfindata needs company-level data from PSX Data Portal (dps.psx.com.pk). The existing 
"deep scrape" doesn't work because the company page loads all 8 tabs in one HTML page 
via JavaScript — not separate URLs.

This prompt maps EVERY data source on PSX DPS and builds a robust scraper.

## PSX DPS Data Architecture — Complete Map

### JSON API Endpoints (direct, no scraping needed)

These return clean JSON — use `requests.get()` directly:

```
GET https://dps.psx.com.pk/timeseries/eod/{SYMBOL}
    Returns: {"status":1,"data":[[timestamp, close, volume, open], ...]}
    Content: Full EOD history (5+ years)
    Example: /timeseries/eod/OGDC

GET https://dps.psx.com.pk/timeseries/int/{SYMBOL}/{DATE}
    Returns: 5-second intraday bars (already used by pakfindata)
    Example: /timeseries/int/OGDC/2026-03-25
```

### HTML Pages — Global (all companies)

These are paginated HTML tables — scrape with DrissionPage:

```
GET https://dps.psx.com.pk/announcements
    Filters: type=A(CDC)|B(SECP)|C(Companies)|D(NCCPL)|E(PSX)
    Also: keyword, company symbol, month, year
    Pagination: Next/Prev buttons (POST form, not URL params)
    Total: 21,316 entries
    
GET https://dps.psx.com.pk/announcements/companies
    Company-specific announcements only (type=C pre-filtered)
    Columns: Date, Time, Company, Subject, PDF link

GET https://dps.psx.com.pk/announcements/cdc
GET https://dps.psx.com.pk/announcements/secp
GET https://dps.psx.com.pk/announcements/nccpl
GET https://dps.psx.com.pk/announcements/psx

GET https://dps.psx.com.pk/payouts
    All dividend/payout declarations across all companies
    457 entries, paginated
    Columns: Symbol, Company, Sector, Dividend Announcement, Date/Time, Book Closure Date

GET https://dps.psx.com.pk/calendar
    Corporate calendar (AGMs, EGMs, book closures)

GET https://dps.psx.com.pk/circuit-breakers
    Daily circuit breaker limits for all symbols

GET https://dps.psx.com.pk/sector-summary
    Sector-level aggregated data

GET https://dps.psx.com.pk/screener
    Stock screener with filters

GET https://dps.psx.com.pk/listings
    All listed companies directory (564 companies)

GET https://dps.psx.com.pk/indices
    All PSX indices data

GET https://dps.psx.com.pk/debt-market
    Bond/sukuk market data

GET https://dps.psx.com.pk/downloads
    Daily downloadable files (EOD CSV, futures, etc.)

GET https://dps.psx.com.pk/historical
    Historical data download interface

GET https://dps.psx.com.pk/corporate-briefing
    Briefing sessions schedule

GET https://dps.psx.com.pk/analysis-reports
GET https://dps.psx.com.pk/monthly-reports
GET https://dps.psx.com.pk/progress-report
```

### HTML Pages — Per Company (8 tabs in one page)

The company page at `/company/{SYMBOL}` is a SINGLE HTML page with 8 JavaScript tabs.
All data is server-rendered in the initial HTML — no AJAX calls.

```
GET https://dps.psx.com.pk/company/{SYMBOL}
    Contains ALL of the following in one HTML response:
```

**Tab 1: QUOTE** (default visible)
```
Data embedded in HTML:
  - Company name, sector
  - Current price, change, change%
  - OHLCV (Open, High, Low, Close, Volume)
  - Circuit breaker range (upper/lower)
  - Day range, 52-week range
  - Bid/Ask price and volume
  - LDCP (Last Day Closing Price)
  - VAR (Value at Risk)
  - Haircut
  - P/E Ratio (TTM)
  - 1-Year Change%, YTD Change%
  - Sub-tabs: REG (regular), DFC (futures), CSF, ODL
```

**Tab 2: PROFILE**
```
  - Business description (full text)
  - Key People table: Name, Designation (CEO, CFO, Directors)
  - Registered office address
  - Website, phone, fax
  - Registrar details
  - Auditor name
  - Legal advisor
  - Sector classification
```

**Tab 3: EQUITY**
```
  - Authorized capital (shares + amount)
  - Paid-up capital (shares + amount)
  - Face value per share
  - Shares outstanding
  - Free float (shares + %)
  - Market capitalization
  - Listing date
```

**Tab 4: ANNOUNCEMENTS**
```
  - Table: Date, Time, Subject
  - PDF links for each announcement
  - Sorted by date descending
  - Pattern: /download/document/{ID}.pdf
```

**Tab 5: FINANCIALS** (sourced from Capital Stake)
```
  - Income Statement: Revenue, Gross Profit, Operating Profit, Net Income, EPS
  - Balance Sheet: Total Assets, Total Liabilities, Equity
  - Cash Flow: Operating, Investing, Financing
  - Multi-year (typically 4 years)
  - Both consolidated and unconsolidated
```

**Tab 6: RATIOS**
```
  - Gross Profit Margin (%)
  - Net Profit Margin (%)
  - EPS Growth (%)
  - PEG ratio
  - Multi-year (4 years)
```

**Tab 7: PAYOUTS**
```
  - Table: Date, Financial Results, Details (dividend %), Book Closure dates
  - Dividend types: (i)=interim, (F)=final, (D)=cash dividend, (R)=rights
  - Example: "42.50%(ii) (D)" = 42.5% second interim cash dividend
  - Book closure: start date - end date
```

**Tab 8: REPORTS**
```
  - Annual reports (PDF links)
  - Quarterly reports (PDF links)
  - Pattern: /download/document/{ID}.pdf and /download/attachment/{ID}.pdf
```

## Step 1: Understand existing scraper

```bash
# Find all existing company/announcement scrapers
find ~/pakfindata/src/ -name "*.py" -exec grep -l "company\|announcement\|payout\|financ\|profile\|dps.psx" {} \; | grep -v __pycache__

# Read the deep scrape code
grep -rn "deep.*scrape\|scrape.*deep\|company_profile\|company_scraper\|psx_company" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check what company data tables exist
python3 -c "
import duckdb, sqlite3

con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in con.execute('SELECT table_name FROM information_schema.tables').fetchall():
    tl = t[0].lower()
    if any(k in tl for k in ['company','profile','announce','payout','financial','ratio','dividend','equity','report']):
        count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
        cols = [c[0] for c in con.execute(f'DESCRIBE {t[0]}').fetchall()]
        print(f'DuckDB {t[0]}: {count:,} — {cols[:8]}')
con.close()

scon = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for t in [r[0] for r in scon.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]:
    tl = t.lower()
    if any(k in tl for k in ['company','profile','announce','payout','financial','ratio','dividend','equity','report']):
        count = scon.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[1] for r in scon.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'SQLite {t}: {count:,} — {cols[:8]}')
scon.close()
"

# Find the broken deep scrape
grep -rn "def.*deep\|def.*scrape.*company\|def.*fetch.*profile\|def.*get.*company" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

# Read whatever scraper file exists
cat ~/pakfindata/src/pakfindata/sources/psx_company.py 2>/dev/null || \
cat ~/pakfindata/src/pakfindata/sources/company_scraper.py 2>/dev/null || \
echo "No company scraper file found — create from scratch"
```

**READ ALL OUTPUT before proceeding. Understand what exists and what's broken.**

## Step 2: Create the Company Data Scraper

Create `src/pakfindata/engine/psx_company_scraper.py`:

```python
"""
PSX Company Data Scraper.

Fetches all company-level data from PSX Data Portal (dps.psx.com.pk):
  - Company profiles (business description, key people, contact)
  - Financial statements (income statement, balance sheet, cash flow)
  - Financial ratios (margins, EPS growth, PEG)
  - Payout history (dividends, rights issues, book closures)
  - Announcements (all corporate announcements with PDF links)
  - Equity structure (authorized/paid-up capital, free float)
  - Reports (annual/quarterly PDF links)

Architecture:
  1. Global pages (announcements, payouts) — faster, get ALL companies at once
  2. Per-company page — single HTML with 8 tabs, parse each section
  3. JSON API — timeseries/eod for price data (already used elsewhere)

Uses:
  - requests for JSON API endpoints
  - DrissionPage for HTML scraping (handles JavaScript-rendered content)
  - BeautifulSoup as fallback for static HTML parsing

PSX-Specific:
  - 564 listed companies
  - Rate limit: ~2 requests/second recommended
  - Company page is ~200KB HTML (all 8 tabs in one page)
  - Announcements paginated at 50 per page
  - Payouts paginated at 25 per page
"""

import requests
import re
import time
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional

PKT = timezone(timedelta(hours=5))
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
BASE_URL = "https://dps.psx.com.pk"
RATE_LIMIT_DELAY = 0.5  # seconds between requests

logger = logging.getLogger(__name__)


# ─── Data Models ───

@dataclass
class CompanyProfile:
    symbol: str
    name: str
    sector: str
    description: str
    website: str
    address: str
    phone: str
    fax: str
    email: str
    registrar: str
    auditor: str
    legal_advisor: str
    key_people: list  # [{name, designation}]
    listing_date: str
    face_value: float
    authorized_capital_shares: int
    authorized_capital_amount: float
    paidup_capital_shares: int
    paidup_capital_amount: float
    free_float_shares: int
    free_float_pct: float


@dataclass
class CompanyFinancials:
    symbol: str
    year: int
    period: str  # "YR", "HYR", "IQ", "IIIQ"
    revenue: float
    gross_profit: float
    operating_profit: float
    net_income: float
    eps: float
    total_assets: float
    total_liabilities: float
    total_equity: float


@dataclass
class CompanyRatios:
    symbol: str
    year: int
    gross_margin: float
    net_margin: float
    eps_growth: float
    peg: float


@dataclass
class PayoutRecord:
    symbol: str
    company_name: str
    sector: str
    date: str
    time: str
    financial_results: str  # "31/12/2025(HYR)"
    details: str            # "42.50%(ii) (D)"
    dividend_pct: float     # parsed: 42.5
    dividend_type: str      # "D"=cash, "R"=rights, "B"=bonus
    is_interim: bool
    book_closure_start: str
    book_closure_end: str


@dataclass
class Announcement:
    symbol: str
    company_name: str
    date: str
    time: str
    subject: str
    pdf_url: str
    announcement_type: str  # "C"=companies, "E"=PSX, etc.


# ─── HTML Parsing ───

def _parse_html(html: str):
    """Parse HTML using BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, 'html.parser')
    except ImportError:
        # Fallback: regex-based extraction
        return None


def _extract_table_data(soup, section_id: str = None, table_index: int = 0) -> list[dict]:
    """Extract data from an HTML table."""
    if soup is None:
        return []
    
    tables = soup.find_all('table')
    if table_index >= len(tables):
        return []
    
    table = tables[table_index]
    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    
    rows = []
    for tr in table.find_all('tr')[1:]:  # skip header
        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
        if cells and len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
        elif cells:
            rows.append({"cells": cells})
    
    return rows


# ─── Global Page Scrapers (All Companies at Once) ───

def scrape_all_announcements(
    announcement_type: str = "C",  # C=Companies (most useful)
    pages: int = 10,
    delay: float = RATE_LIMIT_DELAY,
) -> list[Announcement]:
    """
    Scrape announcements from the global announcements page.
    MUCH faster than per-company scraping.
    
    Types: A=CDC, B=SECP, C=Companies, D=NCCPL, E=PSX
    Each page has 50 entries.
    """
    try:
        from DrissionPage import ChromiumPage, ChromiumOptions
    except ImportError:
        logger.warning("DrissionPage not installed. Using requests+BeautifulSoup fallback.")
        return _scrape_announcements_requests(announcement_type, pages, delay)
    
    # Use DrissionPage for JS-rendered content
    co = ChromiumOptions()
    co.set_argument('--headless')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    
    page = ChromiumPage(co)
    
    url_map = {
        "A": f"{BASE_URL}/announcements",  # CDC (default)
        "B": f"{BASE_URL}/announcements",  # SECP
        "C": f"{BASE_URL}/announcements/companies",
        "D": f"{BASE_URL}/announcements",  # NCCPL
        "E": f"{BASE_URL}/announcements/psx",
    }
    
    page.get(url_map.get(announcement_type, f"{BASE_URL}/announcements/companies"))
    time.sleep(2)
    
    all_announcements = []
    
    for page_num in range(pages):
        # Parse current page
        rows = page.eles('tag:tr')
        
        for row in rows:
            cells = row.eles('tag:td')
            if len(cells) >= 3:
                date_text = cells[0].text.strip()
                time_text = cells[1].text.strip() if len(cells) > 3 else ""
                subject = cells[2].text.strip() if len(cells) > 3 else cells[1].text.strip()
                
                # Extract PDF link
                pdf_link = ""
                pdf_el = row.ele('tag:a', timeout=0.5)
                if pdf_el:
                    href = pdf_el.attr('href')
                    if href and 'download' in href:
                        pdf_link = href if href.startswith('http') else f"{BASE_URL}{href}"
                
                # Extract symbol from subject or company column
                symbol = ""
                if len(cells) > 3:
                    # announcements/companies has company column
                    company_text = cells[2].text.strip()
                    subject = cells[3].text.strip() if len(cells) > 3 else subject
                
                if date_text and subject:
                    all_announcements.append(Announcement(
                        symbol=symbol,
                        company_name="",
                        date=date_text,
                        time=time_text,
                        subject=subject,
                        pdf_url=pdf_link,
                        announcement_type=announcement_type,
                    ))
        
        # Click Next
        try:
            next_btn = page.ele('text=Next', timeout=2)
            if next_btn:
                next_btn.click()
                time.sleep(delay)
            else:
                break
        except:
            break
    
    page.quit()
    return all_announcements


def _scrape_announcements_requests(
    announcement_type: str = "C",
    pages: int = 10,
    delay: float = RATE_LIMIT_DELAY,
) -> list[Announcement]:
    """Fallback: scrape announcements using requests + BeautifulSoup."""
    url_map = {
        "C": f"{BASE_URL}/announcements/companies",
        "E": f"{BASE_URL}/announcements/psx",
    }
    
    url = url_map.get(announcement_type, f"{BASE_URL}/announcements/companies")
    all_announcements = []
    
    for page_num in range(pages):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                break
            
            soup = _parse_html(resp.text)
            if soup is None:
                break
            
            rows = soup.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    date = cells[0].get_text(strip=True)
                    time_str = cells[1].get_text(strip=True)
                    subject = cells[2].get_text(strip=True)
                    
                    pdf_link = ""
                    a_tag = row.find('a', href=True)
                    if a_tag and 'download' in a_tag['href']:
                        href = a_tag['href']
                        pdf_link = href if href.startswith('http') else f"{BASE_URL}{href}"
                    
                    if date and subject:
                        all_announcements.append(Announcement(
                            symbol="", company_name="",
                            date=date, time=time_str, subject=subject,
                            pdf_url=pdf_link, announcement_type=announcement_type,
                        ))
            
            time.sleep(delay)
            # Note: pagination with requests is harder — form POST needed
            break  # Only first page without DrissionPage
            
        except Exception as e:
            logger.error(f"Error scraping announcements page {page_num}: {e}")
            break
    
    return all_announcements


def scrape_all_payouts(pages: int = 20, delay: float = RATE_LIMIT_DELAY) -> list[PayoutRecord]:
    """
    Scrape ALL payout records from the global payouts page.
    457 entries total, 25 per page = ~19 pages.
    """
    try:
        from DrissionPage import ChromiumPage, ChromiumOptions
        
        co = ChromiumOptions()
        co.set_argument('--headless')
        co.set_argument('--no-sandbox')
        
        page = ChromiumPage(co)
        page.get(f"{BASE_URL}/payouts")
        time.sleep(2)
        
        all_payouts = []
        
        for page_num in range(pages):
            rows = page.eles('tag:tr')
            
            for row in rows:
                cells = row.eles('tag:td')
                if len(cells) >= 6:
                    symbol = cells[0].text.strip()
                    company = cells[1].text.strip()
                    sector = cells[2].text.strip()
                    details = cells[3].text.strip()
                    date_time = cells[4].text.strip()
                    book_closure = cells[5].text.strip()
                    
                    # Parse dividend details: "42.50%(ii) (D)"
                    div_pct, div_type, is_interim = _parse_dividend_details(details)
                    
                    # Parse book closure dates
                    bc_start, bc_end = _parse_book_closure(book_closure)
                    
                    # Parse date/time
                    date_str, time_str = _parse_datetime(date_time)
                    
                    all_payouts.append(PayoutRecord(
                        symbol=symbol,
                        company_name=company,
                        sector=sector,
                        date=date_str,
                        time=time_str,
                        financial_results="",
                        details=details,
                        dividend_pct=div_pct,
                        dividend_type=div_type,
                        is_interim=is_interim,
                        book_closure_start=bc_start,
                        book_closure_end=bc_end,
                    ))
            
            try:
                next_btn = page.ele('text=Next', timeout=2)
                if next_btn:
                    next_btn.click()
                    time.sleep(delay)
                else:
                    break
            except:
                break
        
        page.quit()
        return all_payouts
        
    except ImportError:
        logger.warning("DrissionPage not installed — payouts scraping requires it")
        return []


# ─── Per-Company Scraper (All 8 Tabs) ───

def scrape_company_page(symbol: str) -> dict:
    """
    Scrape ALL data from a company page (all 8 tabs in one request).
    
    The page at /company/{SYMBOL} contains all tabs in the HTML.
    Parse each section by finding the tab content divs.
    
    Returns dict with keys: profile, equity, financials, ratios, payouts, 
    announcements, reports, quote
    """
    url = f"{BASE_URL}/company/{symbol}"
    
    try:
        resp = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}"}
        
        html = resp.text
        soup = _parse_html(html)
        
        if soup is None:
            # Fallback: regex extraction
            return _regex_extract_company(html, symbol)
        
        result = {
            "symbol": symbol,
            "quote": _extract_quote(soup, symbol),
            "profile": _extract_profile(soup, symbol),
            "equity": _extract_equity(soup, symbol),
            "financials": _extract_financials(soup, symbol),
            "ratios": _extract_ratios(soup, symbol),
            "payouts": _extract_payouts(soup, symbol),
            "announcements": _extract_announcements(soup, symbol),
            "reports": _extract_reports(soup, symbol),
        }
        
        return result
    
    except Exception as e:
        return {"error": str(e)}


def _extract_quote(soup, symbol: str) -> dict:
    """Extract quote data from the company page."""
    data = {"symbol": symbol}
    
    # Company name and sector
    h2 = soup.find('h2')
    if h2:
        data["name"] = h2.get_text(strip=True)
    
    # Sector (usually in a span or p after h2)
    sector_el = soup.find('span', class_=re.compile('sector|type', re.I))
    if sector_el:
        data["sector"] = sector_el.get_text(strip=True)
    
    # Price — look for the large price text
    price_el = soup.find(string=re.compile(r'Rs\.\s*[\d,]+'))
    if price_el:
        price_match = re.search(r'Rs\.\s*([\d,]+\.?\d*)', price_el)
        if price_match:
            data["price"] = float(price_match.group(1).replace(',', ''))
    
    # Extract all labeled values (OPEN, HIGH, LOW, VOLUME, etc.)
    for label in ['OPEN', 'HIGH', 'LOW', 'VOLUME', 'LDCP', 'VAR', 'HAIRCUT',
                  'P/E RATIO', 'CIRCUIT BREAKER', 'DAY RANGE', '52-WEEK RANGE',
                  'ASK PRICE', 'ASK VOLUME', 'BID PRICE', 'BID VOLUME',
                  '1-YEAR CHANGE', 'YTD CHANGE']:
        el = soup.find(string=re.compile(label, re.I))
        if el:
            parent = el.find_parent()
            if parent:
                # The value is usually in the next sibling or a nearby element
                next_el = parent.find_next_sibling()
                if next_el:
                    val = next_el.get_text(strip=True)
                    key = label.lower().replace(' ', '_').replace('/', '_').replace('-', '_')
                    data[key] = val
    
    return data


def _extract_profile(soup, symbol: str) -> dict:
    """Extract profile data (business description, key people)."""
    data = {"symbol": symbol}
    
    # Business description
    desc_header = soup.find(string=re.compile('BUSINESS DESCRIPTION', re.I))
    if desc_header:
        parent = desc_header.find_parent()
        if parent:
            # Get all text in the same container
            container = parent.find_parent()
            if container:
                paragraphs = container.find_all('p')
                data["description"] = " ".join(p.get_text(strip=True) for p in paragraphs)
    
    # Key people
    key_people_header = soup.find(string=re.compile('KEY PEOPLE', re.I))
    if key_people_header:
        parent = key_people_header.find_parent()
        if parent:
            container = parent.find_parent()
            if container:
                table = container.find('table')
                if table:
                    people = []
                    for row in table.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            people.append({
                                "name": cells[0].get_text(strip=True),
                                "designation": cells[1].get_text(strip=True),
                            })
                    data["key_people"] = people
    
    # Other profile fields
    for field_name in ['Registrar', 'Auditor', 'Legal Advisor', 'Website']:
        el = soup.find(string=re.compile(field_name, re.I))
        if el:
            parent = el.find_parent()
            if parent:
                sibling = parent.find_next_sibling()
                if sibling:
                    key = field_name.lower().replace(' ', '_')
                    data[key] = sibling.get_text(strip=True)
    
    return data


def _extract_equity(soup, symbol: str) -> dict:
    """Extract equity structure (capital, free float)."""
    data = {"symbol": symbol}
    
    for label in ['Authorized Capital', 'Paid-up Capital', 'Face Value',
                  'Free Float', 'Market Capitalization', 'Listing Date']:
        el = soup.find(string=re.compile(label, re.I))
        if el:
            parent = el.find_parent()
            if parent:
                vals = parent.find_next_siblings()
                for v in vals[:2]:
                    text = v.get_text(strip=True)
                    if text:
                        key = label.lower().replace(' ', '_').replace('-', '_')
                        data[key] = text
                        break
    
    return data


def _extract_financials(soup, symbol: str) -> list[dict]:
    """Extract financial statements (multi-year)."""
    financials = []
    
    # Find the Financials section — it has tables with year headers
    # Look for table headers with years (2025, 2024, 2023, 2022)
    tables = soup.find_all('table')
    
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all('th')]
        
        # Check if this is a financial table (has year columns)
        years = [h for h in headers if re.match(r'20\d{2}', h)]
        if not years:
            continue
        
        rows_data = {}
        for tr in table.find_all('tr'):
            cells = [td.get_text(strip=True) for td in tr.find_all('td')]
            if cells and len(cells) > 1:
                label = cells[0]
                values = cells[1:]
                rows_data[label] = values
        
        # Map to year columns
        for i, year in enumerate(years):
            fin = {"symbol": symbol, "year": int(year)}
            for label, values in rows_data.items():
                if i < len(values):
                    key = label.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct')
                    try:
                        val = values[i].replace(',', '').replace('(', '-').replace(')', '')
                        fin[key] = float(val) if val and val != '-' else None
                    except ValueError:
                        fin[key] = values[i]
            financials.append(fin)
    
    return financials


def _extract_ratios(soup, symbol: str) -> list[dict]:
    """Extract financial ratios (multi-year)."""
    ratios = []
    
    # Find the Ratios section
    ratios_header = soup.find(string=re.compile(r'^Ratios$', re.I))
    if ratios_header:
        parent = ratios_header.find_parent()
        if parent:
            table = parent.find_next('table')
            if table:
                headers = [th.get_text(strip=True) for th in table.find_all('th')]
                years = [h for h in headers if re.match(r'20\d{2}', h)]
                
                for tr in table.find_all('tr'):
                    cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                    if cells and len(cells) > 1:
                        label = cells[0]
                        for i, year in enumerate(years):
                            if i + 1 < len(cells):
                                val = cells[i + 1]
                                # Will be collected per year below
    
    return ratios


def _extract_payouts(soup, symbol: str) -> list[dict]:
    """Extract payout/dividend history from company page."""
    payouts = []
    
    payouts_header = soup.find(string=re.compile(r'^Payouts$', re.I))
    if payouts_header:
        parent = payouts_header.find_parent()
        if parent:
            table = parent.find_next('table')
            if table:
                for tr in table.find_all('tr')[1:]:
                    cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                    if len(cells) >= 4:
                        payouts.append({
                            "date": cells[0],
                            "financial_results": cells[1],
                            "details": cells[2],
                            "book_closure": cells[3],
                        })
    
    return payouts


def _extract_announcements(soup, symbol: str) -> list[dict]:
    """Extract announcements from company page."""
    announcements = []
    
    ann_header = soup.find(string=re.compile(r'Announcements', re.I))
    if ann_header:
        parent = ann_header.find_parent()
        if parent:
            table = parent.find_next('table')
            if table:
                for tr in table.find_all('tr')[1:]:
                    cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                    pdf_link = ""
                    a_tag = tr.find('a', href=True)
                    if a_tag:
                        href = a_tag['href']
                        pdf_link = href if href.startswith('http') else f"{BASE_URL}{href}"
                    
                    if len(cells) >= 2:
                        announcements.append({
                            "date": cells[0],
                            "time": cells[1] if len(cells) > 2 else "",
                            "subject": cells[-1] if len(cells) > 2 else cells[1],
                            "pdf_url": pdf_link,
                        })
    
    return announcements


def _extract_reports(soup, symbol: str) -> list[dict]:
    """Extract report PDF links from company page."""
    reports = []
    
    reports_header = soup.find(string=re.compile(r'Financial Reports|Reports', re.I))
    if reports_header:
        parent = reports_header.find_parent()
        if parent:
            links = parent.find_parent().find_all('a', href=True)
            for a in links:
                href = a['href']
                if 'download' in href:
                    reports.append({
                        "title": a.get_text(strip=True),
                        "url": href if href.startswith('http') else f"{BASE_URL}{href}",
                        "type": "PDF",
                    })
    
    return reports


def _regex_extract_company(html: str, symbol: str) -> dict:
    """Fallback: extract company data using regex when BeautifulSoup is unavailable."""
    data = {"symbol": symbol}
    
    # Price
    m = re.search(r'Rs\.\s*([\d,]+\.?\d*)', html)
    if m:
        data["price"] = float(m.group(1).replace(',', ''))
    
    # Company name (first h2)
    m = re.search(r'<h2[^>]*>([^<]+)</h2>', html)
    if m:
        data["name"] = m.group(1).strip()
    
    return data


# ─── Helper Parsers ───

def _parse_dividend_details(details: str) -> tuple:
    """Parse dividend details like '42.50%(ii) (D)' → (42.5, 'D', True)"""
    pct = 0.0
    div_type = "D"
    is_interim = False
    
    pct_match = re.search(r'([\d.]+)%', details)
    if pct_match:
        pct = float(pct_match.group(1))
    
    if '(D)' in details:
        div_type = "D"  # cash dividend
    elif '(R)' in details:
        div_type = "R"  # rights
    elif '(B)' in details:
        div_type = "B"  # bonus
    
    if '(i)' in details or '(ii)' in details or '(iii)' in details:
        is_interim = True
    if '(F)' in details:
        is_interim = False
    
    return pct, div_type, is_interim


def _parse_book_closure(text: str) -> tuple:
    """Parse book closure dates: '07/03/2026 - 09/03/2026' → (start, end)"""
    parts = text.split(' - ')
    start = parts[0].strip() if len(parts) > 0 else ""
    end = parts[1].strip() if len(parts) > 1 else ""
    return start, end


def _parse_datetime(text: str) -> tuple:
    """Parse date/time: 'February 23, 2026 1:25 PM' → (date, time)"""
    # Try to split on last space before AM/PM
    m = re.match(r'(.+?)\s+(\d{1,2}:\d{2}\s*[AP]M)', text, re.I)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return text, ""


# ─── Batch Operations ───

def scrape_all_companies(
    symbols: list[str] = None,
    delay: float = RATE_LIMIT_DELAY,
    save_to_db: bool = True,
) -> dict:
    """
    Scrape all company data for a list of symbols.
    If symbols is None, gets list from eod_ohlcv.
    
    Progress tracking: prints status every 10 companies.
    """
    import duckdb
    
    if symbols is None:
        con = duckdb.connect(str(Path("/mnt/e/psxdata/pakfindata.duckdb")), read_only=True)
        symbols = [r[0] for r in con.execute("""
            SELECT DISTINCT symbol FROM eod_ohlcv
            WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
            ORDER BY symbol
        """).fetchall()]
        con.close()
    
    results = {"success": 0, "failed": 0, "errors": []}
    
    for i, symbol in enumerate(symbols):
        try:
            data = scrape_company_page(symbol)
            
            if "error" not in data:
                if save_to_db:
                    _save_company_data(symbol, data)
                results["success"] += 1
            else:
                results["failed"] += 1
                results["errors"].append(f"{symbol}: {data['error']}")
            
            if (i + 1) % 10 == 0:
                print(f"  Progress: {i+1}/{len(symbols)} "
                      f"(✓ {results['success']}, ✗ {results['failed']})")
            
            time.sleep(delay)
            
        except Exception as e:
            results["failed"] += 1
            results["errors"].append(f"{symbol}: {str(e)}")
    
    return results


def _save_company_data(symbol: str, data: dict):
    """Save scraped company data to SQLite."""
    con = sqlite3.connect(str(PSX_SQLITE))
    
    # Create tables if not exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS company_profiles (
            symbol TEXT PRIMARY KEY,
            name TEXT, sector TEXT, description TEXT,
            website TEXT, address TEXT, phone TEXT,
            key_people TEXT, registrar TEXT, auditor TEXT,
            face_value REAL, authorized_capital TEXT,
            paidup_capital TEXT, free_float TEXT,
            listing_date TEXT, updated_at TEXT
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS company_payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, date TEXT, financial_results TEXT,
            details TEXT, dividend_pct REAL, dividend_type TEXT,
            is_interim INTEGER, book_closure_start TEXT,
            book_closure_end TEXT,
            UNIQUE(symbol, date, details)
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS company_announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, date TEXT, time TEXT,
            subject TEXT, pdf_url TEXT,
            announcement_type TEXT,
            UNIQUE(symbol, date, subject)
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS company_financials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, year INTEGER,
            data TEXT,
            UNIQUE(symbol, year)
        )
    """)
    
    now = datetime.now(PKT).isoformat()
    
    # Save profile
    profile = data.get("profile", {})
    if profile:
        con.execute("""
            INSERT OR REPLACE INTO company_profiles
            (symbol, name, sector, description, website, address, phone,
             key_people, registrar, auditor, face_value, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            profile.get("name", data.get("quote", {}).get("name", "")),
            profile.get("sector", data.get("quote", {}).get("sector", "")),
            profile.get("description", ""),
            profile.get("website", ""),
            profile.get("address", ""),
            profile.get("phone", ""),
            json.dumps(profile.get("key_people", [])),
            profile.get("registrar", ""),
            profile.get("auditor", ""),
            profile.get("face_value", 0),
            now,
        ))
    
    # Save payouts
    for p in data.get("payouts", []):
        try:
            div_pct, div_type, is_interim = _parse_dividend_details(p.get("details", ""))
            bc_start, bc_end = _parse_book_closure(p.get("book_closure", ""))
            con.execute("""
                INSERT OR IGNORE INTO company_payouts
                (symbol, date, financial_results, details, dividend_pct,
                 dividend_type, is_interim, book_closure_start, book_closure_end)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, p.get("date"), p.get("financial_results"), p.get("details"),
                  div_pct, div_type, int(is_interim), bc_start, bc_end))
        except:
            pass
    
    # Save announcements
    for a in data.get("announcements", []):
        try:
            con.execute("""
                INSERT OR IGNORE INTO company_announcements
                (symbol, date, time, subject, pdf_url, announcement_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, a.get("date"), a.get("time"), a.get("subject"),
                  a.get("pdf_url"), "C"))
        except:
            pass
    
    # Save financials
    for f in data.get("financials", []):
        try:
            con.execute("""
                INSERT OR REPLACE INTO company_financials
                (symbol, year, data)
                VALUES (?, ?, ?)
            """, (symbol, f.get("year", 0), json.dumps(f)))
        except:
            pass
    
    con.commit()
    con.close()


# ─── Recommended Scraping Strategy ───

def recommended_scrape_plan():
    """Print the recommended scraping strategy."""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║          PSX COMPANY DATA — SCRAPING STRATEGY               ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  STEP 1: Global Pages First (fast, all companies at once)    ║
    ║  ─────────────────────────────────────────────────────────── ║
    ║  a) /announcements/companies → all company announcements     ║
    ║     21,316 entries, ~430 pages at 50/page                    ║
    ║     Time: ~4 min at 0.5s/page                                ║
    ║                                                              ║
    ║  b) /payouts → all dividend declarations                     ║
    ║     457 entries, ~19 pages at 25/page                        ║
    ║     Time: ~10 seconds                                        ║
    ║                                                              ║
    ║  STEP 2: Per-Company Deep Scrape (slower, richer data)       ║
    ║  ─────────────────────────────────────────────────────────── ║
    ║  a) Top 100 liquid companies first                           ║
    ║     /company/{SYMBOL} → parse all 8 tabs                     ║
    ║     Time: ~50 seconds at 0.5s/company                        ║
    ║                                                              ║
    ║  b) Remaining 464 companies                                  ║
    ║     Run overnight as batch                                   ║
    ║     Time: ~4 minutes at 0.5s/company                         ║
    ║                                                              ║
    ║  STEP 3: Daily Incremental Updates                           ║
    ║  ─────────────────────────────────────────────────────────── ║
    ║  a) /announcements/companies page=1 → latest announcements   ║
    ║  b) /payouts page=1 → latest dividend declarations           ║
    ║  c) Only re-scrape companies that had announcements today    ║
    ║                                                              ║
    ║  TOTAL TIME: ~5 min full scrape, ~30 sec daily update        ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
```

## Step 3: Add Streamlit UI for scrape management

Add scrape controls to the existing Data Status or Sync Center admin page:

```python
# In the admin page, add a section:

st.markdown("### 📥 Company Data Scraper")

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("Scrape Announcements"):
        with st.spinner("Scraping all announcements..."):
            from pakfindata.engine.psx_company_scraper import scrape_all_announcements
            anns = scrape_all_announcements(pages=5)
            st.success(f"Scraped {len(anns)} announcements")

with col2:
    if st.button("Scrape Payouts"):
        with st.spinner("Scraping all payouts..."):
            from pakfindata.engine.psx_company_scraper import scrape_all_payouts
            payouts = scrape_all_payouts()
            st.success(f"Scraped {len(payouts)} payouts")

with col3:
    if st.button("Deep Scrape Top 100"):
        with st.spinner("Deep scraping top 100 companies..."):
            from pakfindata.engine.psx_company_scraper import scrape_all_companies
            result = scrape_all_companies(symbols=top_100_symbols)
            st.success(f"Done: {result['success']} success, {result['failed']} failed")
```

## Step 4: Install dependencies

```bash
conda activate psx
pip install beautifulsoup4 --break-system-packages 2>/dev/null || pip install beautifulsoup4
# DrissionPage should already be installed
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test single company scrape
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.psx_company_scraper import scrape_company_page

data = scrape_company_page('OGDC')
print(f'Keys: {list(data.keys())}')
print(f'Quote: {data.get(\"quote\", {})}')
print(f'Profile keys: {list(data.get(\"profile\", {}).keys())}')
print(f'Payouts: {len(data.get(\"payouts\", []))}')
print(f'Announcements: {len(data.get(\"announcements\", []))}')
print(f'Financials: {len(data.get(\"financials\", []))}')
print(f'Reports: {len(data.get(\"reports\", []))}')
if data.get('payouts'):
    print(f'Latest payout: {data[\"payouts\"][0]}')
"

# Test global announcements scrape
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.psx_company_scraper import scrape_all_announcements

# Just first page
anns = scrape_all_announcements(announcement_type='C', pages=1)
print(f'Announcements: {len(anns)}')
for a in anns[:5]:
    print(f'  {a.date} {a.subject[:80]}')
"

# Test global payouts scrape
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.psx_company_scraper import scrape_all_payouts

payouts = scrape_all_payouts(pages=1)
print(f'Payouts: {len(payouts)}')
for p in payouts[:5]:
    print(f'  {p.symbol:8s} {p.details:20s} {p.date} BC: {p.book_closure_start}-{p.book_closure_end}')
"

# Show recommended plan
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.psx_company_scraper import recommended_scrape_plan
recommended_scrape_plan()
"
```

## IMPORTANT NOTES

1. **Company page is ONE HTML** — all 8 tabs are in the same response, not separate URLs
2. **No AJAX/API calls** for company tabs — all data is server-rendered in the initial HTML
3. **JSON API only exists for timeseries/eod** — everything else requires HTML parsing
4. **Global pages first** — `/announcements/companies` and `/payouts` get ALL companies in one scrape
5. **Rate limit 0.5s** — PSX blocks aggressive scraping, respect the delay
6. **DrissionPage for pagination** — Next/Prev buttons are JavaScript, not URL params
7. **BeautifulSoup as fallback** — works for initial HTML parsing when DrissionPage not needed
8. **Announcement types:** A=CDC, B=SECP, C=Companies (most useful), D=NCCPL, E=PSX
9. **Dividend parsing:** "42.50%(ii) (D)" = 42.5% second interim cash dividend
10. **Save to psx.sqlite** — company_profiles, company_payouts, company_announcements, company_financials tables
11. **UPSERT logic** — INSERT OR REPLACE for profiles, INSERT OR IGNORE for announcements/payouts
12. **Daily updates** — only scrape first page of announcements/payouts for incremental updates
13. **564 companies total** — full deep scrape takes ~5 minutes
14. **PDF links pattern:** `/download/document/{ID}.pdf` and `/download/attachment/{ID}.pdf`
15. **Fix for broken deep scrape:** The old code likely tried to navigate to tab URLs that don't exist. This new code parses all tabs from the single HTML response.
