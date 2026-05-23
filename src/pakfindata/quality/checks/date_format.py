"""date_format check — every non-null value in {column} matches a date regex.

Catches the ZUMA / TBILL / MUFAP / WTL pollution at write time.
"""

from __future__ import annotations

import re

from pakfindata.quality.engine import _validate_identifier, register_check

# YYYY-MM-DD, optionally followed by ISO-8601 time component (Phase 0.2
# convention — see data_freshness.last_row_date examples).
_DEFAULT_PATTERN = r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}.*)?$"


@register_check("date_format")
def check_date_format(con, domain, params):
    """Params: {table, column, pattern? (defaults to YYYY-MM-DD[Ttime])}."""
    table = params["table"]
    column = params["column"]
    pattern = params.get("pattern", _DEFAULT_PATTERN)

    _validate_identifier(con, table, kind="table")
    _validate_identifier(con, column, kind="column", table=table)

    rx = re.compile(pattern)
    rows = con.execute(
        f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
    ).fetchall()
    bad = [r[0] for r in rows if not rx.match(str(r[0]))]

    measured = {
        "total_distinct": len(rows),
        "bad_count": len(bad),
        "bad_samples": bad[:5],
    }
    if bad:
        return False, measured, (
            f"{len(bad)} non-matching values in {table}.{column}; "
            f"samples: {bad[:3]}"
        )
    return True, measured, None
