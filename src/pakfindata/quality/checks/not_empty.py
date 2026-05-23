"""not_empty — table has > 0 rows."""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("not_empty")
def check_not_empty(con, domain, params):
    """Params: {table}."""
    table = params["table"]
    _validate_identifier(con, table, kind="table")

    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    measured = {"row_count": n}
    if n == 0:
        return False, measured, f"{table} is empty"
    return True, measured, None
