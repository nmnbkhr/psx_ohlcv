"""Admin endpoint response models — raw catalog introspection.

Distinct from ``schemas.common.FreshnessRow`` (registered datasets in
``data_freshness``). Admin endpoints inspect the underlying SQLite +
DuckDB catalogs directly to support pages that need to walk every
table (Schema Explorer, App Lineage, Data Quality) — not just the
ones registered in Phase 0.2's catalog.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class AdminTableRow(BaseModel):
    """One row from a sqlite_master enumeration with optional row_count."""

    name: str
    row_count: Optional[int] = None


class AdminDuckdbTableRow(BaseModel):
    """One row from a DuckDB catalog enumeration.

    Only DuckDB-native objects (catalog ``memory``) are returned — the
    SQLite ATTACH passthrough is excluded so callers don't see every
    SQLite table double-listed alongside ``/v1/admin/tables``.
    """

    name: str
    table_type: Optional[str] = None  # BASE TABLE | VIEW
    row_count: Optional[int] = None


class AdminLatestDate(BaseModel):
    """Latest date in a given table's date column."""

    table: str
    column: str
    latest_date: Optional[str] = None


class AdminDuplicateRow(BaseModel):
    """One duplicate group from a GROUP BY ... HAVING COUNT > 1 query.

    ``key`` carries the grouping columns as a dict so the schema is
    polymorphic per table (eod_ohlcv groups by symbol+date,
    company_fundamentals by symbol alone, etc.).
    """

    key: dict[str, Any]
    count: int


class AdminDuplicatesResponse(BaseModel):
    table: str
    by: list[str]
    total_groups: int
    rows: list[AdminDuplicateRow]
