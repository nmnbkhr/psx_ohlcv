"""reference — every non-null {column} value exists in {ref_table}.{ref_column}.

Foreign-key-style integrity check for tables that don't have actual
FK constraints (most of psx.sqlite predates the post-Phase-0 schema
discipline).
"""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("reference")
def check_reference(con, domain, params):
    """Params: {table, column, ref_table, ref_column}."""
    table = params["table"]
    column = params["column"]
    ref_table = params["ref_table"]
    ref_column = params["ref_column"]

    _validate_identifier(con, table, kind="table")
    _validate_identifier(con, column, kind="column", table=table)
    _validate_identifier(con, ref_table, kind="table")
    _validate_identifier(con, ref_column, kind="column", table=ref_table)

    bad = con.execute(
        f"""
        SELECT DISTINCT {column} FROM {table}
        WHERE {column} IS NOT NULL
          AND {column} NOT IN (SELECT {ref_column} FROM {ref_table})
        LIMIT 5
        """
    ).fetchall()

    measured = {
        "orphan_count": len(bad),
        "samples": [r[0] for r in bad],
        "reference": f"{ref_table}.{ref_column}",
    }
    if bad:
        return False, measured, (
            f"{table}.{column} has {len(bad)} orphan values "
            f"not in {ref_table}.{ref_column}"
        )
    return True, measured, None
