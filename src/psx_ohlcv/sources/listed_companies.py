"""Listed Companies Master File - Authoritative Symbol & Sector Source.

Downloads and parses the official PSX listed companies file:
https://dps.psx.com.pk/download/text/listed_cmp.lst.Z

This file is the PRIMARY source of truth for:
- Symbol names
- Company names
- Sector codes and names
- Outstanding shares
"""

import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ..config import DATA_ROOT
from ..db import upsert_sectors
from ..models import now_iso

# Constants
LISTED_CMP_URL = "https://dps.psx.com.pk/download/text/listed_cmp.lst.Z"
MASTER_DIR = DATA_ROOT / "master"
RAW_DIR = MASTER_DIR / "raw"


def download_listed_companies(
    out_dir: Path | None = None,
    timeout: int = 60,
) -> Path:
    """Download the listed_cmp.lst.Z file from PSX.

    Args:
        out_dir: Output directory (default: data/master/raw/)
        timeout: Request timeout in seconds

    Returns:
        Path to the downloaded .Z file

    Raises:
        requests.RequestException: If download fails
    """
    if out_dir is None:
        out_dir = RAW_DIR

    out_dir.mkdir(parents=True, exist_ok=True)
    z_path = out_dir / "listed_cmp.lst.Z"

    response = requests.get(
        LISTED_CMP_URL,
        timeout=timeout,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)",
        },
    )
    response.raise_for_status()

    z_path.write_bytes(response.content)
    return z_path


def extract_listed_companies(z_path: Path) -> Path:
    """Extract the .Z compressed file using system uncompress.

    Args:
        z_path: Path to the .Z file

    Returns:
        Path to the extracted text file

    Raises:
        RuntimeError: If extraction fails
        FileNotFoundError: If uncompress command not found
    """
    # Output path (same name without .Z extension)
    out_path = z_path.with_suffix("")

    # Remove existing output file if it exists
    if out_path.exists():
        out_path.unlink()

    # Try uncompress first, fall back to gzip
    try:
        result = subprocess.run(
            ["uncompress", "-k", str(z_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            # Try gzip as fallback (can handle .Z files)
            result = subprocess.run(
                ["gzip", "-d", "-k", "-f", str(z_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Extraction failed: {result.stderr or result.stdout}"
                )
    except FileNotFoundError:
        # Try gzip as fallback
        result = subprocess.run(
            ["gzip", "-d", "-k", "-f", str(z_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Extraction failed (gzip): {result.stderr or result.stdout}"
            )

    if not out_path.exists():
        raise RuntimeError(f"Extraction produced no output file: {out_path}")

    return out_path


def parse_listed_companies(file_path: Path) -> pd.DataFrame:
    """Parse the listed companies text file.

    The file is pipe-delimited with format:
    |SYMBOL|COMPANY_NAME|SECTOR_CODE|SECTOR_NAME|OUTSTANDING_SHARES||

    Args:
        file_path: Path to the extracted .lst file

    Returns:
        DataFrame with columns:
        - symbol: Uppercase, stripped
        - company_name: Company long name
        - sector_code: Sector code (leading zeros preserved)
        - sector_name: Sector name
        - outstanding_shares: Numeric or None
    """
    rows = []

    with open(file_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Split by pipe
            # Format: |SYMBOL|COMPANY_NAME|SECTOR_CODE|SECTOR_NAME|OUTSTANDING_SHARES||
            # After split: ["", "SYMBOL", "COMPANY_NAME", "SECTOR_CODE", ...]
            parts = line.split("|")

            # Need at least 6 parts (empty + 5 data fields)
            if len(parts) < 6:
                continue

            # parts[0] is empty due to leading pipe, actual data starts at parts[1]
            symbol = parts[1].strip().upper() if len(parts) > 1 and parts[1] else ""
            company_name = parts[2].strip() if len(parts) > 2 else ""
            sector_code = parts[3].strip() if len(parts) > 3 else ""
            sector_name = parts[4].strip() if len(parts) > 4 else ""
            shares_str = parts[5].strip() if len(parts) > 5 else ""

            # Skip if no symbol
            if not symbol:
                continue

            # Parse outstanding shares
            outstanding_shares = None
            if shares_str:
                try:
                    outstanding_shares = float(shares_str.replace(",", ""))
                except ValueError:
                    pass

            rows.append({
                "symbol": symbol,
                "company_name": company_name,
                "sector_code": sector_code,
                "sector_name": sector_name,
                "outstanding_shares": outstanding_shares,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=[
            "symbol", "company_name", "sector_code",
            "sector_name", "outstanding_shares"
        ])

    # Drop duplicates by symbol, keep first
    df = df.drop_duplicates(subset=["symbol"], keep="first")

    # Sort by symbol
    df = df.sort_values("symbol").reset_index(drop=True)

    return df


def upsert_symbols_from_master(
    con: sqlite3.Connection,
    df: pd.DataFrame,
    deactivate_missing: bool = False,
) -> dict[str, int]:
    """Upsert symbols from master file into database.

    Args:
        con: Database connection
        df: DataFrame from parse_listed_companies
        deactivate_missing: If True, mark symbols not in df as inactive

    Returns:
        Dict with counts: inserted, updated, deactivated
    """
    if df.empty:
        return {"inserted": 0, "updated": 0, "deactivated": 0}

    now = now_iso()
    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        # Check if symbol exists
        existing = con.execute(
            "SELECT symbol FROM symbols WHERE symbol = ?",
            (row["symbol"],)
        ).fetchone()

        if existing:
            # Update existing
            con.execute(
                """
                UPDATE symbols SET
                    name = ?,
                    sector = ?,
                    sector_name = ?,
                    outstanding_shares = ?,
                    is_active = 1,
                    source = 'LISTED_CMP',
                    updated_at = ?
                WHERE symbol = ?
                """,
                (
                    row["company_name"],
                    row["sector_code"],
                    row["sector_name"],
                    row["outstanding_shares"],
                    now,
                    row["symbol"],
                ),
            )
            updated += 1
        else:
            # Insert new
            con.execute(
                """
                INSERT INTO symbols (
                    symbol, name, sector, sector_name, outstanding_shares,
                    is_active, source, discovered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, 'LISTED_CMP', ?, ?)
                """,
                (
                    row["symbol"],
                    row["company_name"],
                    row["sector_code"],
                    row["sector_name"],
                    row["outstanding_shares"],
                    now,
                    now,
                ),
            )
            inserted += 1

    deactivated = 0
    if deactivate_missing:
        # Get all symbols from master file
        master_symbols = set(df["symbol"].tolist())

        # Mark symbols not in master as inactive
        result = con.execute(
            """
            UPDATE symbols SET is_active = 0, updated_at = ?
            WHERE symbol NOT IN ({}) AND is_active = 1
            """.format(",".join("?" * len(master_symbols))),
            [now, *master_symbols],
        )
        deactivated = result.rowcount

    con.commit()

    return {"inserted": inserted, "updated": updated, "deactivated": deactivated}


def refresh_listed_companies(
    con: sqlite3.Connection,
    deactivate_missing: bool = False,
) -> dict[str, Any]:
    """Full pipeline: download, extract, parse, and upsert listed companies.

    Also populates the sectors table with unique sector_code -> sector_name mappings.

    Args:
        con: Database connection
        deactivate_missing: If True, mark symbols not in file as inactive

    Returns:
        Summary dict with keys:
        - fetched_at: ISO timestamp
        - symbols_found: Number of symbols in file
        - inserted: Number of new symbols
        - updated: Number of updated symbols
        - deactivated: Number of deactivated symbols
        - sectors_upserted: Number of sectors upserted
        - success: Boolean
        - error: Error message if failed
    """
    result: dict[str, Any] = {
        "fetched_at": now_iso(),
        "symbols_found": 0,
        "inserted": 0,
        "updated": 0,
        "deactivated": 0,
        "sectors_upserted": 0,
        "success": False,
        "error": None,
    }

    try:
        # Download
        z_path = download_listed_companies()

        # Extract
        lst_path = extract_listed_companies(z_path)

        # Parse
        df = parse_listed_companies(lst_path)
        result["symbols_found"] = len(df)

        # Upsert symbols
        if not df.empty:
            counts = upsert_symbols_from_master(
                con, df, deactivate_missing=deactivate_missing
            )
            result["inserted"] = counts["inserted"]
            result["updated"] = counts["updated"]
            result["deactivated"] = counts["deactivated"]

            # Also populate sectors table from unique sector_code/sector_name pairs
            sectors_df = df[["sector_code", "sector_name"]].drop_duplicates()
            sectors_df = sectors_df[
                (sectors_df["sector_code"].str.len() > 0) &
                (sectors_df["sector_name"].str.len() > 0)
            ]
            if not sectors_df.empty:
                result["sectors_upserted"] = upsert_sectors(con, sectors_df)

        result["success"] = True

    except requests.RequestException as e:
        result["error"] = f"Download error: {e}"
    except RuntimeError as e:
        result["error"] = f"Extraction error: {e}"
    except Exception as e:
        result["error"] = f"Error: {e}"

    return result


def get_master_symbols(con: sqlite3.Connection) -> pd.DataFrame:
    """Get all symbols from the database.

    Args:
        con: Database connection

    Returns:
        DataFrame with symbol details
    """
    return pd.read_sql_query(
        """
        SELECT symbol, name, sector, sector_name, outstanding_shares,
               is_active, source, discovered_at, updated_at
        FROM symbols
        ORDER BY symbol
        """,
        con,
    )


def export_master_csv(con: sqlite3.Connection, out_path: str) -> int:
    """Export symbols to CSV file.

    Args:
        con: Database connection
        out_path: Output file path

    Returns:
        Number of rows exported
    """
    df = get_master_symbols(con)
    df.to_csv(out_path, index=False, encoding="utf-8")
    return len(df)
