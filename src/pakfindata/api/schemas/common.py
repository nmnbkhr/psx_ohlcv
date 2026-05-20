"""Shared Pydantic models + helpers used by every v1 route.

Field names in ``FreshnessRow`` match the ``data_freshness`` table
columns exactly (``domain`` is the PK, not ``dataset_id``).
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel


class FreshnessRow(BaseModel):
    """One row from the ``data_freshness`` table.

    Fields mirror the SQLite schema column-for-column.
    """

    domain: str
    display_name: str
    source_table: str
    date_column: Optional[str] = None
    last_sync_at: Optional[str] = None
    last_row_date: Optional[str] = None
    row_count: Optional[int] = None
    status: Optional[str] = None
    last_sync_error: Optional[str] = None
    source: Optional[str] = None
    schema_version: Optional[int] = None
    notes: Optional[str] = None
    updated_at: Optional[str] = None


class ErrorResponse(BaseModel):
    detail: str


def df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame to a JSON-safe list of dicts.

    Replaces NaN with None so the JSON encoder emits ``null`` instead of
    crashing on ``float('nan')``. Pandas' ``to_dict(orient='records')``
    on its own leaves NaN as a Python float NaN, which ``json.dumps``
    rejects.
    """
    if df is None or df.empty:
        return []
    return df.where(pd.notna(df), None).to_dict(orient="records")
