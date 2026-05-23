"""source_coverage — {table}.{value_column} contains every expected value
at the latest {partition_column}.

Typical use: ``sovereign_curve`` — verify the latest date has every
expected source (PKRV, PKISRV, KIBOR, MTB, PIB, POLICY). Catches the
case where a single source's sync silently stopped without any other
domain noticing.
"""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("source_coverage")
def check_source_coverage(con, domain, params):
    """Params: {table, partition_column, value_column, expected_values}."""
    table = params["table"]
    partition_col = params["partition_column"]
    value_col = params["value_column"]
    expected = set(params["expected_values"])

    _validate_identifier(con, table, kind="table")
    _validate_identifier(con, partition_col, kind="column", table=table)
    _validate_identifier(con, value_col, kind="column", table=table)

    latest = con.execute(
        f"SELECT MAX({partition_col}) FROM {table}"
    ).fetchone()[0]
    if latest is None:
        return False, {"latest_partition": None}, f"{table} is empty"

    present = {
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT {value_col} FROM {table} "
            f"WHERE {partition_col} = ?",
            (latest,),
        ).fetchall()
    }
    missing = sorted(expected - present)

    measured = {
        "latest_partition": latest,
        "expected": sorted(expected),
        "present": sorted(present & expected),
        "missing": missing,
    }
    if missing:
        return False, measured, (
            f"{table}.{value_col} missing {missing} "
            f"at {partition_col}={latest}"
        )
    return True, measured, None
