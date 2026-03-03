"""
SBP Primary Market Activities (PMA) document archive module.

This module fetches and archives SBP PMA documents for fixed income instruments:
- Market Treasury Bills (MTBs)
- Pakistan Investment Bonds (PIBs)
- Government of Pakistan Ijara Sukuk (GIS)

Source: https://www.sbp.org.pk/dfmd/pma.asp

Documents are stored as metadata in the database. PDF parsing is NOT performed;
only document links and metadata are archived for reference.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..config import DATA_ROOT

# SBP PMA page URL
SBP_PMA_URL = "https://www.sbp.org.pk/dfmd/pma.asp"

# Output directory for downloaded PDFs (metadata only in DB)
PMA_DOCS_DIR = DATA_ROOT / "fixed_income" / "raw" / "sbp_pma"

# Document category patterns for inference
CATEGORY_PATTERNS = {
    "MTB": [
        r"treasury.?bill",
        r"mtb",
        r"t-bill",
        r"tbill",
        r"3.?month",
        r"6.?month",
        r"12.?month",
    ],
    "PIB": [
        r"investment.?bond",
        r"pib",
        r"pakistan.?investment",
        r"fixed.?rate",
        r"floating.?rate",
        r"frr",
    ],
    "GOP_SUKUK": [
        r"sukuk",
        r"ijara",
        r"gis",
        r"islamic",
        r"shariah",
    ],
}


@dataclass
class PMADocument:
    """Represents a PMA document from SBP website."""

    title: str
    url: str
    category: str  # MTB, PIB, GOP_SUKUK, OTHER
    doc_date: str | None  # YYYY-MM-DD if parseable
    doc_type: str  # ANNOUNCEMENT, RESULT, CALENDAR, OTHER
    file_name: str | None
    section: str | None  # Section heading from page


def infer_category(text: str, section: str | None = None) -> str:
    """
    Infer document category from text content.

    Args:
        text: Document title or link text
        section: Section heading for additional context

    Returns:
        Category string: MTB, PIB, GOP_SUKUK, or OTHER
    """
    combined = f"{text} {section or ''}".lower()

    for category, patterns in CATEGORY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, combined, re.IGNORECASE):
                return category

    return "OTHER"


def infer_doc_type(text: str) -> str:
    """
    Infer document type from text content.

    Args:
        text: Document title or link text

    Returns:
        Document type: ANNOUNCEMENT, RESULT, CALENDAR, CIRCULAR, or OTHER
    """
    text_lower = text.lower()

    if any(w in text_lower for w in ["result", "auction result", "accepted"]):
        return "RESULT"
    if any(w in text_lower for w in ["calendar", "schedule", "tentative"]):
        return "CALENDAR"
    if any(w in text_lower for w in ["announcement", "notice", "invitation"]):
        return "ANNOUNCEMENT"
    if any(w in text_lower for w in ["circular", "directive", "guideline"]):
        return "CIRCULAR"
    if any(w in text_lower for w in ["target", "indicative"]):
        return "TARGET"

    return "OTHER"


def parse_date_from_text(text: str) -> str | None:
    """
    Try to parse a date from document title or URL.

    Args:
        text: Text that may contain a date

    Returns:
        Date string in YYYY-MM-DD format, or None
    """
    # Common date patterns in SBP documents
    def fmt_dmy(m):
        return f"{m[3]}-{m[2].zfill(2)}-{m[1].zfill(2)}"

    def fmt_ymd(m):
        return f"{m[1]}-{m[2].zfill(2)}-{m[3].zfill(2)}"

    patterns = [
        # DD-MM-YYYY or DD/MM/YYYY
        (r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", fmt_dmy),
        # YYYY-MM-DD
        (r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", fmt_ymd),
        # Month name patterns: "January 2024", "Jan 2024"
        (
            r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
            r"[\s,]+(\d{4})",
            None,  # Handle separately
        ),
    ]

    month_map = {
        "jan": "01", "january": "01",
        "feb": "02", "february": "02",
        "mar": "03", "march": "03",
        "apr": "04", "april": "04",
        "may": "05",
        "jun": "06", "june": "06",
        "jul": "07", "july": "07",
        "aug": "08", "august": "08",
        "sep": "09", "september": "09",
        "oct": "10", "october": "10",
        "nov": "11", "november": "11",
        "dec": "12", "december": "12",
    }

    text_lower = text.lower()

    # Try numeric date patterns
    for pattern, formatter in patterns[:2]:
        match = re.search(pattern, text)
        if match:
            try:
                date_str = formatter(match)
                # Validate date
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                continue

    # Try month name pattern
    month_pattern = patterns[2][0]
    match = re.search(month_pattern, text_lower)
    if match:
        month_name = match.group(1).lower()
        year = match.group(2)
        month_num = month_map.get(month_name[:3])
        if month_num:
            return f"{year}-{month_num}-01"

    return None


def fetch_pma_html(url: str = SBP_PMA_URL, timeout: int = 30) -> str | None:
    """
    Fetch HTML content from SBP PMA page.

    Args:
        url: PMA page URL
        timeout: Request timeout in seconds

    Returns:
        HTML content as string, or None on error
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0; +https://github.com/)"
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching PMA page: {e}")
        return None


def parse_pma_links(html: str, base_url: str = SBP_PMA_URL) -> list[PMADocument]:
    """
    Parse PMA document links from HTML content.

    Args:
        html: HTML content from SBP PMA page
        base_url: Base URL for resolving relative links

    Returns:
        List of PMADocument objects
    """
    documents = []
    soup = BeautifulSoup(html, "html.parser")

    current_section = None

    # Find all links in the page
    for element in soup.find_all(["h1", "h2", "h3", "h4", "strong", "b", "a"]):
        # Track section headings
        if element.name in ["h1", "h2", "h3", "h4", "strong", "b"]:
            text = element.get_text(strip=True)
            if len(text) > 5 and len(text) < 100:
                current_section = text
            continue

        # Process links
        if element.name == "a":
            href = element.get("href", "")
            if not href:
                continue

            # Only process PDF and document links
            href_lower = href.lower()
            doc_extensions = [".pdf", ".doc", ".docx", ".xls", ".xlsx"]
            if not any(ext in href_lower for ext in doc_extensions):
                continue

            # Get link text
            title = element.get_text(strip=True)
            if not title:
                title = href.split("/")[-1]

            # Resolve relative URL
            full_url = urljoin(base_url, href)

            # Extract filename
            file_name = href.split("/")[-1] if "/" in href else href

            # Infer metadata
            combined_text = f"{title} {file_name}"
            category = infer_category(combined_text, current_section)
            doc_type = infer_doc_type(combined_text)
            doc_date = parse_date_from_text(combined_text)

            doc = PMADocument(
                title=title,
                url=full_url,
                category=category,
                doc_date=doc_date,
                doc_type=doc_type,
                file_name=file_name,
                section=current_section,
            )
            documents.append(doc)

    return documents


def download_document(
    doc: PMADocument,
    output_dir: Path = PMA_DOCS_DIR,
    timeout: int = 60,
    skip_existing: bool = True,
) -> Path | None:
    """
    Download a single PMA document.

    Args:
        doc: PMADocument to download
        output_dir: Directory to save files
        timeout: Request timeout
        skip_existing: Skip if file already exists

    Returns:
        Path to downloaded file, or None on error
    """
    if not doc.file_name:
        return None

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Organize by category
    category_dir = output_dir / doc.category.lower()
    category_dir.mkdir(exist_ok=True)

    output_path = category_dir / doc.file_name

    # Skip if exists
    if skip_existing and output_path.exists():
        return output_path

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0; +https://github.com/)"
        }
        response = requests.get(doc.url, headers=headers, timeout=timeout, stream=True)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return output_path

    except requests.RequestException as e:
        print(f"Error downloading {doc.file_name}: {e}")
        return None


def download_documents(
    docs: list[PMADocument],
    output_dir: Path = PMA_DOCS_DIR,
    skip_existing: bool = True,
    progress_callback=None,
) -> dict:
    """
    Download multiple PMA documents.

    Args:
        docs: List of documents to download
        output_dir: Output directory
        skip_existing: Skip existing files
        progress_callback: Optional callback(current, total, doc)

    Returns:
        Summary dict with download counts
    """
    summary = {
        "total": len(docs),
        "downloaded": 0,
        "skipped": 0,
        "failed": 0,
        "errors": [],
    }

    for i, doc in enumerate(docs):
        if progress_callback:
            progress_callback(i + 1, len(docs), doc)

        if skip_existing:
            category_dir = output_dir / doc.category.lower()
            if doc.file_name and (category_dir / doc.file_name).exists():
                summary["skipped"] += 1
                continue

        result = download_document(doc, output_dir, skip_existing=skip_existing)

        if result:
            summary["downloaded"] += 1
        else:
            summary["failed"] += 1
            summary["errors"].append(doc.file_name or doc.url)

    return summary


def fetch_and_parse_pma() -> list[PMADocument]:
    """
    Fetch and parse PMA documents in one call.

    Returns:
        List of PMADocument objects, or empty list on error
    """
    html = fetch_pma_html()
    if not html:
        return []

    return parse_pma_links(html)


def get_sample_pma_documents() -> list[PMADocument]:
    """
    Get sample PMA documents for testing when SBP is unavailable.

    Returns:
        List of sample PMADocument objects
    """
    return [
        PMADocument(
            title="MTB Auction Result - 29 January 2026",
            url="https://www.sbp.org.pk/dfmd/MTB-Result-29-01-2026.pdf",
            category="MTB",
            doc_date="2026-01-29",
            doc_type="RESULT",
            file_name="MTB-Result-29-01-2026.pdf",
            section="Market Treasury Bills",
        ),
        PMADocument(
            title="PIB Auction Result - 28 January 2026",
            url="https://www.sbp.org.pk/dfmd/PIB-Result-28-01-2026.pdf",
            category="PIB",
            doc_date="2026-01-28",
            doc_type="RESULT",
            file_name="PIB-Result-28-01-2026.pdf",
            section="Pakistan Investment Bonds",
        ),
        PMADocument(
            title="GOP Ijara Sukuk Auction Calendar Q1 2026",
            url="https://www.sbp.org.pk/dfmd/GIS-Calendar-Q1-2026.pdf",
            category="GOP_SUKUK",
            doc_date="2026-01-01",
            doc_type="CALENDAR",
            file_name="GIS-Calendar-Q1-2026.pdf",
            section="Government of Pakistan Ijara Sukuk",
        ),
        PMADocument(
            title="MTB Auction Calendar January 2026",
            url="https://www.sbp.org.pk/dfmd/MTB-Calendar-Jan-2026.pdf",
            category="MTB",
            doc_date="2026-01-01",
            doc_type="CALENDAR",
            file_name="MTB-Calendar-Jan-2026.pdf",
            section="Market Treasury Bills",
        ),
        PMADocument(
            title="PIB Auction Calendar Q1 2026",
            url="https://www.sbp.org.pk/dfmd/PIB-Calendar-Q1-2026.pdf",
            category="PIB",
            doc_date="2026-01-01",
            doc_type="CALENDAR",
            file_name="PIB-Calendar-Q1-2026.pdf",
            section="Pakistan Investment Bonds",
        ),
    ]


def convert_doc_to_db_record(doc: PMADocument) -> dict:
    """
    Convert PMADocument to database record format.

    Args:
        doc: PMADocument object

    Returns:
        Dict matching sbp_pma_docs table schema
    """
    # Generate doc_id from URL hash
    import hashlib
    doc_id = hashlib.md5(doc.url.encode()).hexdigest()[:12]

    file_path = None
    if doc.file_name:
        file_path = str(PMA_DOCS_DIR / doc.category.lower() / doc.file_name)

    return {
        "doc_id": doc_id,
        "url": doc.url,
        "title": doc.title,
        "doc_date": doc.doc_date,
        "category": doc.category,
        "doc_type": doc.doc_type,
        "file_path": file_path,
        "parsed": 0,  # PDF parsing not implemented
    }
