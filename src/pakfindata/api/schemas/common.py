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

    Replaces NaN / NaT / pd.NA with None so the JSON encoder emits
    ``null`` instead of crashing on ``float('nan')``. ``df.where`` alone
    is not enough: pandas keeps the column dtype, so float columns
    still emit NaN through ``to_dict``. Casting to object first forces
    Python-object cells everywhere.
    """
    if df is None or df.empty:
        return []
    records = df.astype(object).where(pd.notna(df), None).to_dict(orient="records")
    return records
