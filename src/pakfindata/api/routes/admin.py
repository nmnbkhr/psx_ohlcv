"""Admin endpoints — /v1/admin/* — raw catalog introspection.

Scope notes:
- These routes inspect the *underlying* SQLite and DuckDB catalogs
  directly (sqlite_master, SHOW TABLES). They are deliberately
  distinct from ``/v1/freshness`` which serves the curated
  ``data_freshness`` catalog from Phase 0.2:
    * /v1/freshness  → registered datasets only; rich metadata
      (display_name, status, last_sync_error, …); the canonical
      source for "is dataset X up to date?"
    * /v1/admin/*    → every physical table/view, including ones
      never registered (mutual_funds, etf_master, ad-hoc imports);
      coarser metadata; backs the Schema Explorer / App Lineage /
      Data Quality dashboards that need full coverage.
- Read-only. NO sync triggers, NO compute endpoints, NO bulk DDL.
- {table} path params are NOT bound as SQL parameters — SQLite
  doesn't allow identifier parameterization. Every endpoint that
  interpolates {table} into SQL first allowlists it against
  ``sqlite_master.name`` (or DuckDB's ``information_schema.tables``)
  and rejects unknown names with 404. Same goes for column names
  passed via query params — allowlisted against PRAGMA table_info.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_analytics_con, get_read_db
from pakfindata.api.schemas.admin import (
    AdminDuckdbTableRow,
    AdminDuplicateRow,
    AdminDuplicatesResponse,
    AdminLatestDate,
    AdminTableRow,
)

admin_router = APIRouter(prefix="/v1/admin", tags=["admin"])

# Identifier-quoting helper. SQLite identifiers go in double-quotes;
# double-quotes inside the identifier are escaped by doubling.
def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _resolve_table(con: sqlite3.Connection, table: str) -> str:
    """Return the verified table name if it exists in sqlite_master,
    else raise 404. Prevents SQL-injection-shaped use of the {table}
    path parameter (which CANNOT be parameterized at SQLite level)."""
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (table,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"unknown table/view {table!r}"
        )
    return row["name"]


def _resolve_columns(
    con: sqlite3.Connection, table: str, cols: list[str]
) -> list[str]:
    """Allowlist each requested column against the table's actual columns."""
    rows = con.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
    valid = {r["name"] for r in rows}
    bad = [c for c in cols if c not in valid]
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"unknown columns {bad!r} in table {table!r}",
        )
    return list(cols)


# ── /v1/admin/tables ────────────────────────────────────────────────


@admin_router.get("/tables", response_model=list[AdminTableRow])
def list_tables(
    include_counts: Annotated[
        bool, Query(description="Add per-table COUNT(*) — slower")
    ] = False,
    con: sqlite3.Connection = Depends(get_read_db),
) -> list[dict]:
    """List every user table in the SQLite catalog.

    Excludes SQLite internal tables (``sqlite_%``) and indexes.
    """
    rows = con.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        name = r["name"]
        entry: dict = {"name": name, "row_count": None}
        if include_counts:
            try:
                count_row = con.execute(
                    f"SELECT COUNT(*) AS c FROM {_quote_ident(name)}"
                ).fetchone()
                entry["row_count"] = count_row["c"] if count_row else None
            except sqlite3.OperationalError:
                # Table dropped between SELECT name and COUNT, or no read
                # permission — surface as None rather than 500 the page.
                entry["row_count"] = None
        out.append(entry)
    return out


@admin_router.get(
    "/tables/{table}/latest-date", response_model=AdminLatestDate
)
def get_table_latest_date(
    table: Annotated[str, Path(description="Table name (allowlisted)")],
    col: Annotated[
        str, Query(description="Date-column name (allowlisted)")
    ] = "date",
    con: sqlite3.Connection = Depends(get_read_db),
) -> AdminLatestDate:
    """``MAX(col)`` over a single table, for staleness dashboards."""
    real_table = _resolve_table(con, table)
    _resolve_columns(con, real_table, [col])
    row = con.execute(
        f"SELECT MAX({_quote_ident(col)}) AS m FROM {_quote_ident(real_table)}"
    ).fetchone()
    return AdminLatestDate(
        table=real_table, column=col, latest_date=row["m"] if row else None
    )


@admin_router.get(
    "/tables/{table}/duplicates", response_model=AdminDuplicatesResponse
)
def get_table_duplicates(
    table: Annotated[str, Path(description="Table name (allowlisted)")],
    by: Annotated[
        str,
        Query(description="Comma-separated grouping columns (allowlisted)"),
    ],
    limit: Annotated[int, Query(ge=1, le=500)] = 20,
    con: sqlite3.Connection = Depends(get_read_db),
) -> AdminDuplicatesResponse:
    """``GROUP BY ... HAVING COUNT(*) > 1`` for a single table.

    Backs the Data Quality page's duplicate detection. ``by`` columns
    are allowlisted before interpolation.
    """
    real_table = _resolve_table(con, table)
    raw_cols = [c.strip() for c in by.split(",") if c.strip()]
    if not raw_cols:
        raise HTTPException(status_code=400, detail="empty `by` parameter")
    cols = _resolve_columns(con, real_table, raw_cols)

    col_list = ", ".join(_quote_ident(c) for c in cols)
    cur = con.execute(
        f"""SELECT {col_list}, COUNT(*) AS cnt
              FROM {_quote_ident(real_table)}
             GROUP BY {col_list}
            HAVING COUNT(*) > 1
             ORDER BY cnt DESC
             LIMIT ?""",
        (limit,),
    )
    rows = cur.fetchall()
    out_rows = [
        AdminDuplicateRow(
            key={c: r[c] for c in cols},
            count=r["cnt"],
        )
        for r in rows
    ]
    return AdminDuplicatesResponse(
        table=real_table, by=cols, total_groups=len(out_rows), rows=out_rows
    )


# ── /v1/admin/duckdb ───────────────────────────────────────────────


@admin_router.get(
    "/duckdb/tables", response_model=list[AdminDuckdbTableRow]
)
def list_duckdb_tables(
    include_counts: Annotated[
        bool, Query(description="Add per-table COUNT(*) — slower")
    ] = False,
    con=Depends(get_analytics_con),
) -> list[dict]:
    """List DuckDB-native tables/views (catalog ``memory`` only).

    The SQLite ATTACH catalog (``sq``) is excluded — those tables are
    already served by ``/v1/admin/tables`` and would otherwise appear
    twice when both catalogs are walked.

    Second /v1 route to use ``get_analytics_con`` after
    ``/v1/tick-logs/dates/{symbol}`` from 1.7.F.0.
    """
    rows = con.execute(
        "SELECT table_name AS name, table_type "
        "  FROM information_schema.tables "
        " WHERE table_catalog = 'memory' AND table_schema = 'main' "
        " ORDER BY table_name"
    ).fetchall()
    out: list[dict] = []
    for (name, table_type) in rows:
        entry: dict = {
            "name": name,
            "table_type": table_type,
            "row_count": None,
        }
        if include_counts:
            try:
                count_row = con.execute(
                    f"SELECT COUNT(*) AS c FROM {_quote_ident(name)}"
                ).fetchone()
                entry["row_count"] = count_row[0] if count_row else None
            except Exception:
                entry["row_count"] = None
        out.append(entry)
    return out
