"""SQL identifier allowlist — one helper, called by check handlers
before interpolating ``{table}`` / ``{column}`` into SQL.

The regex blocks injection. The ``sqlite_master`` /
``pragma_table_info`` lookup blocks references to objects that don't
exist. Together they make the identifier-interpolation pattern safe.

Imported from :mod:`pakfindata.quality.engine` (which re-exports the
function under the same name so existing callers don't change).
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Optional

__all__ = ["validate_identifier"]


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(
    con: sqlite3.Connection,
    name: Any,
    *,
    kind: str,
    table: Optional[str] = None,
) -> None:
    """Regex + existence check on a SQL identifier before interpolation.

    ``kind`` is ``"table"`` or ``"column"``. For ``"column"``, ``table``
    must be the (already-validated) parent table. Raises ``ValueError``
    on any failure — handlers convert to a failed CheckResult.
    """
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"invalid {kind} identifier: {name!r}")
    if kind == "table":
        row = con.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type IN ('table', 'view') AND name = ?",
            (name,),
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown table: {name!r}")
    elif kind == "column":
        if not table:
            raise ValueError("column validation requires `table` arg")
        cols = {
            r[1]
            for r in con.execute(
                "SELECT * FROM pragma_table_info(?)", (table,)
            ).fetchall()
        }
        if name not in cols:
            raise ValueError(f"unknown column {table}.{name}")
    else:
        raise ValueError(f"unknown identifier kind: {kind!r}")
