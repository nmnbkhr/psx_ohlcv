"""row_count_min — table has at least {min} rows."""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("row_count_min")
def check_row_count_min(con, domain, params):
    """Params: {table, min}."""
    table = params["table"]
    min_rows = int(params["min"])

    _validate_identifier(con, table, kind="table")

    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    measured = {"row_count": n, "min_required": min_rows}
    if n < min_rows:
        return False, measured, (
            f"{table} has {n} rows, expected >= {min_rows}"
        )
    return True, measured, None
