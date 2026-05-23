"""custom_sql — escape hatch for one-off rules that don't fit the named checks.

By design this check does NOT go through the identifier allowlist.
The rule author is responsible for the SQL they write. Reserve this
``check_type`` for genuinely odd cases. If you find yourself writing
five ``custom_sql`` rules with similar shape, that's a signal to
register a new named ``check_type`` instead.
"""

from __future__ import annotations

from pakfindata.quality.engine import register_check


@register_check("custom_sql")
def check_custom_sql(con, domain, params):
    """Params: {sql, expected_count? (default 0), description?}.

    Runs ``sql``; passes iff the returned row count equals
    ``expected_count``.
    """
    sql = params["sql"]
    expected = int(params.get("expected_count", 0))

    if not isinstance(sql, str):
        return False, {}, "custom_sql.sql must be a string"
    if ";" in sql.rstrip(";"):
        return False, {}, "custom_sql.sql must be a single statement"

    rows = con.execute(sql).fetchall()
    actual = len(rows)
    measured = {
        "actual_row_count": actual,
        "expected_row_count": expected,
        "sql_preview": sql[:80],
    }
    if actual != expected:
        suffix = (
            f" — {params['description']}"
            if params.get("description")
            else ""
        )
        return False, measured, (
            f"custom_sql: expected {expected} rows, got {actual}{suffix}"
        )
    return True, measured, None
