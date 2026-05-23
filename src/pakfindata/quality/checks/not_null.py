"""not_null — {column} has no NULLs (or fraction below max_null_fraction)."""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("not_null")
def check_not_null(con, domain, params):
    """Params: {table, column, max_null_fraction? (default 0.0)}."""
    table = params["table"]
    column = params["column"]
    max_frac = float(params.get("max_null_fraction", 0.0))

    _validate_identifier(con, table, kind="table")
    _validate_identifier(con, column, kind="column", table=table)

    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if total == 0:
        # Vacuous pass — not_empty is the rule for "must have rows."
        return True, {"row_count": 0, "note": "empty table"}, None

    nulls = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
    ).fetchone()[0]
    frac = nulls / total

    measured = {
        "row_count": total,
        "null_count": nulls,
        "null_fraction": round(frac, 4),
        "max_null_fraction": max_frac,
    }
    if frac > max_frac:
        return False, measured, (
            f"{table}.{column} null fraction {frac:.3f} > {max_frac}"
        )
    return True, measured, None
