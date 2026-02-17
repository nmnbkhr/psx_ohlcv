"""Company website financial statement scanner.

Fetches company websites (from PSX DPS profile data) and checks whether
they host financial statements / investor relations pages.

Usage:
    from psx_ohlcv.sources.website_scanner import run_website_scan
    results = run_website_scan(con, limit=10)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Callable

import aiohttp
from lxml import html

logger = logging.getLogger("psx_ohlcv")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Keyword matching
# ---------------------------------------------------------------------------

# Strong signals — high-confidence financial page indicators
STRONG_KEYWORDS = [
    "investor relation",
    "annual report",
    "financial statement",
    "financial report",
    "quarterly report",
    "half year report",
    "half-year report",
    "financial result",
    "annual result",
    "investor corner",
    "shareholder info",
]

# Medium signals — need context
MEDIUM_KEYWORDS = [
    "investor",
    "financial",
    "disclosure",
    "shareholder",
    "governance",
    "annual",
    "quarterly",
]

# URL path patterns (regex)
URL_PATTERNS = [
    re.compile(r"/investor", re.I),
    re.compile(r"/financial", re.I),
    re.compile(r"/annual.?report", re.I),
    re.compile(r"/ir(/|$)", re.I),
    re.compile(r"/disclosure", re.I),
    re.compile(r"/shareholder", re.I),
    re.compile(r"/report", re.I),
]


def normalize_url(url: str) -> str:
    """Ensure URL has a protocol prefix."""
    url = url.strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _score_link(text: str, href: str) -> tuple[int, list[str]]:
    """Score a link for financial relevance.

    Returns:
        (score, matched_keywords) — score >= 2 is strong match.
    """
    text_lower = text.lower()
    href_lower = href.lower()
    score = 0
    matched: list[str] = []

    for kw in STRONG_KEYWORDS:
        if kw in text_lower or kw in href_lower:
            score += 2
            matched.append(kw)

    for kw in MEDIUM_KEYWORDS:
        if kw in text_lower:
            score += 1
            matched.append(kw)

    for pat in URL_PATTERNS:
        if pat.search(href_lower):
            score += 1
            matched.append(pat.pattern)

    return score, matched


def scan_html_for_financials(html_content: str, base_url: str) -> dict[str, Any]:
    """Parse HTML and find financial statement links.

    Args:
        html_content: Raw HTML of the company website homepage.
        base_url: The website base URL (for resolving relative links).

    Returns:
        Dict with: has_financial_page, financial_urls, financial_keywords, link_count.
    """
    try:
        tree = html.fromstring(html_content)
    except Exception:
        return {"has_financial_page": False, "financial_urls": [], "financial_keywords": [], "link_count": 0}

    # Make links absolute
    try:
        tree.make_links_absolute(base_url)
    except Exception:
        pass

    anchors = tree.xpath("//a[@href]")
    financial_urls: list[str] = []
    all_keywords: list[str] = []
    best_score = 0

    for a in anchors:
        href = a.get("href", "").strip()
        text = a.text_content().strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue

        score, kws = _score_link(text, href)
        if score >= 1:
            if href not in financial_urls:
                financial_urls.append(href)
            for kw in kws:
                if kw not in all_keywords:
                    all_keywords.append(kw)
            if score > best_score:
                best_score = score

    has_financial = best_score >= 2 or len(financial_urls) >= 3

    return {
        "has_financial_page": has_financial,
        "financial_urls": financial_urls[:20],  # cap at 20
        "financial_keywords": all_keywords[:20],
        "link_count": len(anchors),
    }


# ---------------------------------------------------------------------------
# Async scanner
# ---------------------------------------------------------------------------

async def _fetch_one(
    session: aiohttp.ClientSession,
    symbol: str,
    url: str,
    timeout: int = 15,
    max_retries: int = 2,
) -> dict[str, Any]:
    """Fetch and scan a single company website.

    Returns:
        Result dict ready for upsert_website_scan().
    """
    result: dict[str, Any] = {
        "symbol": symbol,
        "dps_website_url": url,
        "website_reachable": False,
        "http_status": None,
        "has_financial_page": False,
        "financial_urls": [],
        "financial_keywords": [],
        "error_message": None,
        "scan_duration_ms": 0,
    }

    normalized = normalize_url(url)
    if not normalized:
        result["error_message"] = "empty URL"
        return result

    t0 = time.monotonic()

    for attempt in range(max_retries):
        try:
            async with session.get(
                normalized,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                result["http_status"] = resp.status
                if resp.status == 200:
                    result["website_reachable"] = True
                    content_type = resp.headers.get("Content-Type", "")
                    if "text/html" in content_type or "application/xhtml" in content_type:
                        body = await resp.text(errors="replace")
                        scan = scan_html_for_financials(body, normalized)
                        result["has_financial_page"] = scan["has_financial_page"]
                        result["financial_urls"] = scan["financial_urls"]
                        result["financial_keywords"] = scan["financial_keywords"]
                    else:
                        result["error_message"] = f"non-HTML content: {content_type[:60]}"
                else:
                    result["error_message"] = f"HTTP {resp.status}"
                break  # success, no retry

        except asyncio.TimeoutError:
            result["error_message"] = "timeout"
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
        except aiohttp.ClientError as e:
            result["error_message"] = str(e)[:200]
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
        except Exception as e:
            result["error_message"] = f"unexpected: {str(e)[:150]}"
            break

    result["scan_duration_ms"] = int((time.monotonic() - t0) * 1000)
    return result


async def scan_batch(
    symbols_urls: list[tuple[str, str]],
    max_concurrent: int = 10,
    rate_limit: float = 0.3,
    timeout: int = 15,
    progress_cb: Callable[[int, int, str], None] | None = None,
) -> list[dict[str, Any]]:
    """Scan multiple company websites concurrently.

    Args:
        symbols_urls: List of (symbol, website_url) tuples.
        max_concurrent: Max concurrent requests.
        rate_limit: Seconds between request dispatches.
        timeout: Per-request timeout in seconds.
        progress_cb: Called with (done_count, total, current_symbol).

    Returns:
        List of result dicts.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results: list[dict[str, Any]] = []
    total = len(symbols_urls)
    done = 0

    connector = aiohttp.TCPConnector(limit=max_concurrent * 2, ssl=False)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:

        async def _wrapped(sym: str, url: str) -> dict:
            nonlocal done
            async with semaphore:
                await asyncio.sleep(rate_limit)
                r = await _fetch_one(session, sym, url, timeout=timeout)
                done += 1
                if progress_cb:
                    try:
                        progress_cb(done, total, sym)
                    except Exception:
                        pass
                return r

        tasks = [_wrapped(sym, url) for sym, url in symbols_urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    return list(results)


# ---------------------------------------------------------------------------
# Main entry point (synchronous wrapper)
# ---------------------------------------------------------------------------

def run_website_scan(
    con,
    symbols: list[str] | None = None,
    limit: int | None = None,
    progress_cb: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    """Run the website scanner end-to-end.

    Steps:
    1. Query company_profile for website URLs.
    2. Async-scan each website for financial content.
    3. Upsert results to company_website_scan.

    Args:
        con: SQLite connection.
        symbols: Optional list of symbols to scan (default: all with websites).
        limit: Max number of symbols to scan.
        progress_cb: Progress callback (done, total, symbol).

    Returns:
        Summary dict: total, scanned, reachable, has_financial, errors.
    """
    from ..db.repositories.website_scan import init_website_scan_schema, upsert_website_scan

    init_website_scan_schema(con)

    # Phase A: Get website URLs from company_profile
    query = "SELECT symbol, website FROM company_profile WHERE website IS NOT NULL AND website != ''"
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        query += f" AND symbol IN ({placeholders})"
        rows = con.execute(query, [s.upper() for s in symbols]).fetchall()
    else:
        rows = con.execute(query).fetchall()

    symbols_urls = [(r[0], r[1]) for r in rows]

    if limit and len(symbols_urls) > limit:
        symbols_urls = symbols_urls[:limit]

    if not symbols_urls:
        return {"total": 0, "scanned": 0, "reachable": 0, "has_financial": 0, "errors": 0, "message": "No website URLs found in company_profile. Run deep scraper first."}

    logger.info("Website scan: %d symbols to scan", len(symbols_urls))

    # Phase B: Async scan
    results = asyncio.run(scan_batch(symbols_urls, progress_cb=progress_cb))

    # Phase C: Store results
    reachable = 0
    has_financial = 0
    errors = 0
    for r in results:
        upsert_website_scan(con, r)
        if r.get("website_reachable"):
            reachable += 1
        if r.get("has_financial_page"):
            has_financial += 1
        if r.get("error_message"):
            errors += 1

    summary = {
        "total": len(symbols_urls),
        "scanned": len(results),
        "reachable": reachable,
        "has_financial": has_financial,
        "errors": errors,
    }
    logger.info("Website scan complete: %s", json.dumps(summary))
    return summary
