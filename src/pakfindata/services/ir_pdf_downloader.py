"""IR Website PDF Downloader.

Downloads financial report PDFs from company investor relations websites.
Uses financial_links.csv for URL discovery, scrapes PDF links, downloads.

Priority: IR website PDFs (clean text) > PSX DPS PDFs (often scanned)

Usage:
    python -m pakfindata.services.ir_pdf_downloader --symbol 786
    python -m pakfindata.services.ir_pdf_downloader --all
    python -m pakfindata.services.ir_pdf_downloader --symbol OGDC --manual-url https://...
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from lxml import html

logger = logging.getLogger("pakfindata.ir_downloader")

PDF_ROOT = Path("/mnt/e/psxsymbolfin")
LINKS_CSV = PDF_ROOT / "financial_links.csv"
MANUAL_URLS_CSV = PDF_ROOT / "manual_ir_urls.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Keywords that suggest a PDF is a financial report
_REPORT_KEYWORDS = re.compile(
    r"annual|quarterly|half.?year|interim|financial.?statement|financial.?report"
    r"|q[1-4]|first.?quarter|second.?quarter|third.?quarter"
    r"|six.?month|nine.?month|condensed|audited",
    re.I,
)

# Keywords to skip (presentations, notices, proxies)
_SKIP_KEYWORDS = re.compile(
    r"presentation|proxy|notice|invitation|form|agenda|minutes|circular"
    r"|pattern|code.?of.?conduct|csr|sustainability|esg|compliance",
    re.I,
)


def load_financial_links() -> dict[str, dict]:
    """Load financial_links.csv into {symbol: row_dict}."""
    if not LINKS_CSV.exists():
        return {}
    result = {}
    with open(LINKS_CSV) as f:
        for row in csv.DictReader(f):
            result[row["symbol"]] = row
    return result


def load_manual_urls() -> dict[str, list[str]]:
    """Load manual_ir_urls.csv into {symbol: [url1, url2, ...]}."""
    if not MANUAL_URLS_CSV.exists():
        return {}
    result: dict[str, list[str]] = {}
    with open(MANUAL_URLS_CSV) as f:
        for row in csv.DictReader(f):
            sym = row.get("symbol", "")
            url = row.get("url", "")
            if sym and url:
                result.setdefault(sym, []).append(url)
    return result


def scrape_pdf_links(page_url: str, timeout: int = 15) -> list[dict]:
    """Scrape a webpage for PDF download links.

    Returns list of {url, title, is_financial} dicts.
    """
    try:
        resp = requests.get(page_url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", page_url, e)
        return []

    try:
        tree = html.fromstring(resp.content)
    except Exception:
        return []

    pdfs = []
    seen_urls = set()

    # Find all links to PDFs
    for a in tree.xpath("//a[@href]"):
        href = a.get("href", "")
        if not href:
            continue

        # Resolve relative URLs
        full_url = urljoin(page_url, href)

        # Only PDFs
        if not full_url.lower().endswith(".pdf"):
            continue

        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Get link text
        title = (a.text_content() or "").strip()
        if not title:
            title = full_url.split("/")[-1]

        # Classify
        combined = f"{title} {full_url}"
        is_financial = bool(_REPORT_KEYWORDS.search(combined))
        is_skip = bool(_SKIP_KEYWORDS.search(combined))

        if is_skip and not is_financial:
            continue

        pdfs.append({
            "url": full_url,
            "title": title[:200],
            "is_financial": is_financial,
        })

    return pdfs


def download_pdf(url: str, dest_path: Path, timeout: int = 30) -> bool:
    """Download a PDF to disk. Returns True on success."""
    if dest_path.exists():
        return True  # Already downloaded

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            return False

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True

    except Exception as e:
        logger.warning("Download failed %s: %s", url, e)
        return False


def _safe_filename(url: str, title: str) -> str:
    """Generate a safe filename from URL and title."""
    # Try to extract meaningful name from URL
    url_name = url.split("/")[-1]
    if url_name.endswith(".pdf"):
        # Clean up
        name = re.sub(r"[^\w\s.-]", "_", url_name)
        return name

    # Fall back to title
    name = re.sub(r"[^\w\s.-]", "_", title[:80])
    if not name.endswith(".pdf"):
        name += ".pdf"
    return name


def process_symbol(
    symbol: str,
    links_data: dict | None = None,
    manual_urls: list[str] | None = None,
) -> dict:
    """Download all financial PDFs for a symbol from IR website.

    Returns summary dict.
    """
    symbol_dir = PDF_ROOT / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    existing_pdfs = {p.name for p in symbol_dir.glob("*.pdf")}

    # Collect all pages to scrape
    pages_to_scrape: list[str] = []

    # From manual URLs
    if manual_urls:
        pages_to_scrape.extend(manual_urls)

    # From financial_links.csv
    if links_data:
        fin_urls = json.loads(links_data.get("financial_urls", "[]"))
        # Prioritize pages with report/financial/annual in URL
        for url in fin_urls:
            if any(k in url.lower() for k in ["annual", "report", "financial", "result", "statement"]):
                pages_to_scrape.append(url)
        # Add investor relations pages
        for url in fin_urls:
            if url not in pages_to_scrape and any(k in url.lower() for k in ["investor", "shareholder"]):
                pages_to_scrape.append(url)

    if not pages_to_scrape:
        return {"symbol": symbol, "error": "No IR URLs found", "downloaded": 0, "skipped": 0}

    # Scrape PDF links from all pages
    all_pdfs: list[dict] = []
    seen = set()
    for page_url in pages_to_scrape[:5]:  # Max 5 pages to scrape
        pdfs = scrape_pdf_links(page_url)
        for p in pdfs:
            if p["url"] not in seen:
                seen.add(p["url"])
                all_pdfs.append(p)
        time.sleep(0.5)  # Be polite

    # Filter to financial reports
    financial_pdfs = [p for p in all_pdfs if p["is_financial"]]
    if not financial_pdfs:
        financial_pdfs = all_pdfs  # Take all if no financial ones detected

    # Download
    downloaded = 0
    skipped = 0
    for pdf_info in financial_pdfs:
        filename = _safe_filename(pdf_info["url"], pdf_info["title"])

        # Skip if already exists (from PSX DPS or prior download)
        if filename in existing_pdfs:
            skipped += 1
            continue

        dest = symbol_dir / filename
        if download_pdf(pdf_info["url"], dest):
            downloaded += 1
            # Tag as IR source
            tag_file = symbol_dir / f".ir_source_{filename}"
            tag_file.write_text(pdf_info["url"])

    return {
        "symbol": symbol,
        "pages_scraped": len(pages_to_scrape),
        "pdfs_found": len(all_pdfs),
        "financial_pdfs": len(financial_pdfs),
        "downloaded": downloaded,
        "skipped": skipped,
        "existing": len(existing_pdfs),
    }


def run(
    symbols: list[str] | None = None,
    manual_url: str | None = None,
):
    """Run IR PDF downloader."""
    all_links = load_financial_links()
    all_manual = load_manual_urls()

    if symbols:
        target_symbols = symbols
    else:
        # All symbols with financial links
        target_symbols = sorted(all_links.keys())

    t0 = time.time()
    total_downloaded = 0
    total_found = 0
    no_urls = []

    print(f"IR PDF Downloader — {len(target_symbols)} symbols")
    print()

    for symbol in target_symbols:
        links_data = all_links.get(symbol)
        sym_manual = all_manual.get(symbol, [])

        # Add CLI manual URL
        if manual_url and len(target_symbols) == 1:
            sym_manual.append(manual_url)

        if not links_data and not sym_manual:
            no_urls.append(symbol)
            continue

        summary = process_symbol(symbol, links_data, sym_manual)

        if "error" in summary:
            print(f"  ✗ {symbol:8s} — {summary['error']}")
            no_urls.append(symbol)
            continue

        total_downloaded += summary["downloaded"]
        total_found += summary["financial_pdfs"]

        dl = summary["downloaded"]
        found = summary["financial_pdfs"]
        existing = summary["existing"]
        status = "✓" if dl > 0 else "·" if found > 0 else "✗"
        print(
            f"  {status} {symbol:8s} — {found:3d} financial PDFs found, "
            f"{dl:3d} new downloaded, {existing:3d} existing"
        )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Downloaded: {total_downloaded}")
    print(f"  Financial PDFs found: {total_found}")

    if no_urls:
        print(f"\n  No IR URLs for {len(no_urls)} symbols.")
        print(f"  Add manually to {MANUAL_URLS_CSV}:")
        print(f"  Format: symbol,url")
        for s in no_urls[:10]:
            print(f"    {s},https://...")
        if len(no_urls) > 10:
            print(f"    ... and {len(no_urls) - 10} more")


def main():
    parser = argparse.ArgumentParser(description="Download PDFs from company IR websites")
    parser.add_argument("--symbol", "-s", nargs="+", help="Specific symbols")
    parser.add_argument("--all", action="store_true", help="Process all symbols with IR links")
    parser.add_argument("--manual-url", "-u", help="Manual URL to scrape for PDFs")
    args = parser.parse_args()

    if args.all:
        run(symbols=None)
    elif args.symbol:
        run(symbols=args.symbol, manual_url=args.manual_url)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
