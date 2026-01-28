"""
SBP Primary Market Document Archive Module.

This module handles archiving and indexing of SBP DFMD (Domestic Financial
Market Department) primary market documents including:
- T-Bill auction results
- PIB auction results
- GOP Ijarah Sukuk auction results
- Yield curve data

Source: https://easydata.sbp.org.pk/apex/f?p=10:210

Note: This module provides manual document management. Users must download
documents from SBP and place them in the data/sukuk/sbp_docs/ directory.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Default document directory
DOCS_DIR = Path(__file__).parent.parent.parent.parent / "data" / "sukuk" / "sbp_docs"


@dataclass
class SBPDocument:
    """Represents an SBP primary market document."""

    doc_id: str
    doc_type: str
    auction_date: str
    instrument_type: str
    file_path: str
    file_name: str
    file_hash: str | None = None
    title: str | None = None
    source_url: str | None = None
    notes: str | None = None
    indexed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for database storage."""
        return {
            "doc_id": self.doc_id,
            "doc_type": self.doc_type,
            "auction_date": self.auction_date,
            "instrument_type": self.instrument_type,
            "file_path": self.file_path,
            "file_name": self.file_name,
            "file_hash": self.file_hash,
            "title": self.title,
            "source_url": self.source_url,
            "notes": self.notes,
            "indexed_at": self.indexed_at or datetime.now().isoformat(),
        }


# Document type definitions
DOC_TYPES = {
    "AUCTION_RESULT": "Auction Result",
    "AUCTION_CALENDAR": "Auction Calendar",
    "YIELD_CURVE": "Yield Curve Data",
    "CUT_OFF_YIELD": "Cut-off Yield Announcement",
    "ISSUE_NOTICE": "Issue Notice",
}

# Instrument types
INSTRUMENT_TYPES = {
    "TBILL": "Treasury Bill",
    "PIB": "Pakistan Investment Bond",
    "GOP_SUKUK": "GOP Ijarah Sukuk",
    "FRB": "Floating Rate Bond",
}


def scan_document_directory(
    docs_dir: Path | str | None = None,
) -> list[SBPDocument]:
    """
    Scan directory for SBP documents and create document records.

    Supported file formats: PDF, XLS, XLSX, CSV

    File naming convention (recommended):
        {instrument_type}_{doc_type}_{auction_date}.{ext}
        Example: TBILL_AUCTION_RESULT_2026-01-15.pdf

    Args:
        docs_dir: Directory containing documents

    Returns:
        List of SBPDocument objects
    """
    if docs_dir is None:
        docs_dir = DOCS_DIR

    docs_dir = Path(docs_dir)
    if not docs_dir.exists():
        return []

    documents = []
    extensions = {".pdf", ".xls", ".xlsx", ".csv"}

    for file_path in docs_dir.iterdir():
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in extensions:
            continue

        # Parse document info from filename
        doc_info = parse_document_filename(file_path.name)

        # Calculate file hash for dedup
        file_hash = calculate_file_hash(file_path)

        # Generate doc_id
        doc_id = generate_doc_id(
            doc_info["instrument_type"],
            doc_info["doc_type"],
            doc_info["auction_date"],
        )

        doc = SBPDocument(
            doc_id=doc_id,
            doc_type=doc_info["doc_type"],
            auction_date=doc_info["auction_date"],
            instrument_type=doc_info["instrument_type"],
            file_path=str(file_path),
            file_name=file_path.name,
            file_hash=file_hash,
            title=doc_info.get("title"),
            indexed_at=datetime.now().isoformat(),
        )

        documents.append(doc)

    return documents


def parse_document_filename(filename: str) -> dict[str, Any]:
    """
    Parse document info from filename.

    Supports patterns:
        - TBILL_AUCTION_RESULT_2026-01-15.pdf
        - PIB_YIELD_CURVE_2026-01.xlsx
        - gop_sukuk_auction_2026-01-15.pdf

    Args:
        filename: Document filename

    Returns:
        Dict with doc_type, instrument_type, auction_date, title
    """
    name = Path(filename).stem.upper()

    # Default values
    result = {
        "doc_type": "AUCTION_RESULT",
        "instrument_type": "TBILL",
        "auction_date": datetime.now().date().isoformat(),
        "title": filename,
    }

    # Detect instrument type
    for inst_type in INSTRUMENT_TYPES:
        if inst_type in name or inst_type.replace("_", "") in name:
            result["instrument_type"] = inst_type
            break

    # Detect document type
    for doc_type in DOC_TYPES:
        if doc_type in name or doc_type.replace("_", "") in name:
            result["doc_type"] = doc_type
            break

    # Extract date (YYYY-MM-DD or YYYY-MM)
    date_match = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2})", name)
    if date_match:
        date_str = date_match.group(1).replace("_", "-")
        result["auction_date"] = date_str
    else:
        date_match = re.search(r"(\d{4}[-_]\d{2})", name)
        if date_match:
            date_str = date_match.group(1).replace("_", "-")
            result["auction_date"] = f"{date_str}-01"

    return result


def calculate_file_hash(file_path: Path | str) -> str:
    """Calculate SHA-256 hash of file for deduplication."""
    file_path = Path(file_path)

    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def generate_doc_id(
    instrument_type: str,
    doc_type: str,
    auction_date: str,
) -> str:
    """Generate unique document ID."""
    return f"SBP:{instrument_type}:{doc_type}:{auction_date}"


def index_documents(
    docs_dir: Path | str | None = None,
    index_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Scan and index all documents in directory.

    Creates/updates an index JSON file.

    Args:
        docs_dir: Directory containing documents
        index_path: Path for index file

    Returns:
        Summary dict with counts
    """
    if docs_dir is None:
        docs_dir = DOCS_DIR

    docs_dir = Path(docs_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)

    if index_path is None:
        index_path = docs_dir / "document_index.json"

    # Scan for documents
    documents = scan_document_directory(docs_dir)

    # Load existing index
    existing_index = {}
    if Path(index_path).exists():
        with open(index_path, encoding="utf-8") as f:
            existing_index = json.load(f)

    # Merge with existing
    new_count = 0
    updated_count = 0

    for doc in documents:
        if doc.doc_id not in existing_index:
            new_count += 1
        else:
            # Check if file changed
            if existing_index[doc.doc_id].get("file_hash") != doc.file_hash:
                updated_count += 1

        existing_index[doc.doc_id] = doc.to_dict()

    # Save index
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(existing_index, f, indent=2, default=str)

    return {
        "total_documents": len(existing_index),
        "new_documents": new_count,
        "updated_documents": updated_count,
        "index_path": str(index_path),
    }


def load_document_index(
    index_path: Path | str | None = None,
) -> dict[str, dict]:
    """
    Load document index from JSON file.

    Args:
        index_path: Path to index file

    Returns:
        Dict mapping doc_id to document info
    """
    if index_path is None:
        index_path = DOCS_DIR / "document_index.json"

    if not Path(index_path).exists():
        return {}

    with open(index_path, encoding="utf-8") as f:
        return json.load(f)


def get_documents_by_type(
    instrument_type: str | None = None,
    doc_type: str | None = None,
    index_path: Path | str | None = None,
) -> list[dict]:
    """
    Get documents filtered by type.

    Args:
        instrument_type: Filter by instrument (TBILL, PIB, etc.)
        doc_type: Filter by document type
        index_path: Path to index file

    Returns:
        List of document dicts
    """
    index = load_document_index(index_path)

    results = []
    for doc in index.values():
        if instrument_type and doc.get("instrument_type") != instrument_type:
            continue
        if doc_type and doc.get("doc_type") != doc_type:
            continue
        results.append(doc)

    # Sort by auction date descending
    results.sort(key=lambda x: x.get("auction_date", ""), reverse=True)

    return results


def get_latest_document(
    instrument_type: str,
    doc_type: str = "AUCTION_RESULT",
    index_path: Path | str | None = None,
) -> dict | None:
    """
    Get most recent document of specified type.

    Args:
        instrument_type: Instrument type (TBILL, PIB, etc.)
        doc_type: Document type
        index_path: Path to index file

    Returns:
        Document dict or None
    """
    docs = get_documents_by_type(instrument_type, doc_type, index_path)
    return docs[0] if docs else None


def create_sample_documents(docs_dir: Path | str | None = None) -> list[str]:
    """
    Create sample placeholder documents for testing.

    Args:
        docs_dir: Directory for documents

    Returns:
        List of created file paths
    """
    if docs_dir is None:
        docs_dir = DOCS_DIR

    docs_dir = Path(docs_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)

    # Sample documents
    samples = [
        ("TBILL_AUCTION_RESULT_2026-01-15.csv", "T-Bill auction result data"),
        ("TBILL_AUCTION_RESULT_2026-01-08.csv", "T-Bill auction result data"),
        ("PIB_AUCTION_RESULT_2026-01-10.csv", "PIB auction result data"),
        ("GOP_SUKUK_AUCTION_RESULT_2026-01-12.csv", "GOP Sukuk auction result data"),
        ("TBILL_YIELD_CURVE_2026-01.csv", "T-Bill yield curve data"),
    ]

    created = []
    for filename, content in samples:
        file_path = docs_dir / filename
        if not file_path.exists():
            # Create minimal CSV content
            csv_content = f"# {content}\n# Generated for testing\n"
            csv_content += "date,value\n2026-01-15,15.0\n"
            file_path.write_text(csv_content)
            created.append(str(file_path))

    return created


def get_sbp_document_urls() -> dict[str, str]:
    """
    Get known SBP URLs for document downloads.

    Note: These are informational - actual downloads require
    manual navigation due to session requirements.

    Returns:
        Dict mapping document type to URL
    """
    return {
        "primary_market": "https://easydata.sbp.org.pk/apex/f?p=10:210",
        "auction_calendar": "https://www.sbp.org.pk/dfmd/auction-calendar.asp",
        "t_bill_auction": "https://www.sbp.org.pk/dfmd/auction-results.asp",
        "pib_auction": "https://www.sbp.org.pk/dfmd/PIB-Auction-Results.asp",
        "sukuk_auction": "https://www.sbp.org.pk/dfmd/Sukuk-Auction-Results.asp",
    }
