"""Tier 4: Web scraper fetchers for live/daily commodity data.

Scrapers for sources that don't have APIs:
- PMEX Market Watch (Pakistan commodity futures, JS-rendered)
- SBP Exchange Rates (official interbank WAR for USD/PKR)
- Investing.com (Palm Oil, Coal, Steel — Cloudflare-protected)

These scrapers require maintenance as page structures may change.
"""

import logging
import re
from datetime import datetime

import requests

logger = logging.getLogger("pakfindata.commodities.scraper")


# ─────────────────────────────────────────────────────────────────────────────
# 4a. PMEX Market Watch (requires Selenium for JS-rendered content)
# ─────────────────────────────────────────────────────────────────────────────

PMEX_URL = "https://dportal.pmex.com.pk/mwatch"


def fetch_pmex_market_watch() -> list[dict] | None:
    """DEPRECATED: Use fetcher_pmex.fetch_pmex_snapshot() instead.

    The PMEX dportal has a direct JSON API that returns all 134 instruments
    with no Selenium dependency. See fetcher_pmex.py.

    This Selenium-based scraper is kept for backward compatibility.
    """
    import warnings
    warnings.warn(
        "fetch_pmex_market_watch() is deprecated. Use fetcher_pmex.fetch_pmex_snapshot() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.info("Selenium not installed — skipping PMEX scraper")
        return None

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.info("beautifulsoup4 not installed — skipping PMEX scraper")
        return None

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    try:
        driver = webdriver.Chrome(options=options)
        driver.get(PMEX_URL)

        # Wait for the market watch table to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )

        soup = BeautifulSoup(driver.page_source, "lxml")
        driver.quit()
    except Exception as e:
        logger.warning("PMEX scraper failed: %s", e)
        return None

    # Parse the market watch table
    results = []
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) >= 4:
                try:
                    name = cells[0].get_text(strip=True)
                    last_price = _parse_number(cells[1].get_text(strip=True))
                    change = _parse_number(cells[2].get_text(strip=True))
                    volume = _parse_number(cells[3].get_text(strip=True))

                    if name and last_price is not None:
                        results.append({
                            "name": name,
                            "last_price": last_price,
                            "change": change,
                            "volume": volume,
                            "source": "pmex",
                            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        })
                except (ValueError, IndexError):
                    continue

    logger.info("PMEX: scraped %d instruments", len(results))
    return results if results else None


# ─────────────────────────────────────────────────────────────────────────────
# 4e. SBP Exchange Rates (static HTML, no JS needed)
# ─────────────────────────────────────────────────────────────────────────────

SBP_WAR_URL = "https://www.sbp.org.pk/ecodata/rates/war/WAR-Current.asp"


def fetch_sbp_usd_pkr() -> dict | None:
    """Scrape the SBP WAR (Weighted Average Rate) for USD/PKR.

    Returns dict with: rate, date, source. None if scraping fails.
    Uses requests + lxml (no JS needed — static HTML).
    """
    try:
        from lxml import html as lxml_html
    except ImportError:
        logger.info("lxml not installed — skipping SBP scraper")
        return None

    try:
        resp = requests.get(SBP_WAR_URL, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("SBP WAR request failed: %s", e)
        return None

    try:
        tree = lxml_html.fromstring(resp.content)

        # Find table rows with USD data
        rows = tree.xpath("//table//tr")
        for row in rows:
            cells = row.xpath(".//td")
            cell_texts = [c.text_content().strip() for c in cells]

            # Look for "US Dollar" or "USD" in the row
            for i, text in enumerate(cell_texts):
                if "US Dollar" in text or text == "USD":
                    # The rate is typically in the next column(s)
                    for j in range(i + 1, len(cell_texts)):
                        rate = _parse_number(cell_texts[j])
                        if rate and rate > 100:  # USD/PKR should be > 100
                            return {
                                "rate": rate,
                                "date": datetime.now().strftime("%Y-%m-%d"),
                                "source": "sbp_war",
                            }
    except Exception as e:
        logger.warning("SBP WAR parse failed: %s", e)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# 4c. Investing.com scraper (for Palm Oil, Coal, Steel)
# ─────────────────────────────────────────────────────────────────────────────

INVESTING_URLS = {
    "PALM_OIL": "https://www.investing.com/commodities/crude-palm-oil",
    "COAL": "https://www.investing.com/commodities/newcastle-coal-futures",
    "STEEL_HRC": "https://www.investing.com/commodities/shanghai-rebar-futures",
    "NICKEL": "https://www.investing.com/commodities/nickel",
}


def fetch_investing_commodity(symbol: str) -> dict | None:
    """Scrape a commodity price from Investing.com.

    Requires Selenium due to Cloudflare protection.
    Returns dict with: symbol, price, change, change_pct, timestamp.
    Returns None if unavailable.
    """
    url = INVESTING_URLS.get(symbol)
    if not url:
        return None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.info("Selenium not installed — skipping Investing.com scraper")
        return None

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(options=options)
        driver.get(url)

        # Wait for price element
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-test='instrument-price-last']"))
        )

        price_el = driver.find_element(By.CSS_SELECTOR, "[data-test='instrument-price-last']")
        price = _parse_number(price_el.text)

        # Try to get change
        change = None
        try:
            change_el = driver.find_element(By.CSS_SELECTOR, "[data-test='instrument-price-change']")
            change = _parse_number(change_el.text)
        except Exception:
            pass

        driver.quit()

        if price is not None:
            return {
                "symbol": symbol,
                "price": price,
                "change": change,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source": "investing.com",
                "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
    except Exception as e:
        logger.warning("Investing.com scraper failed for %s: %s", symbol, e)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _parse_number(text: str) -> float | None:
    """Parse a number from text, handling commas and whitespace."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None
