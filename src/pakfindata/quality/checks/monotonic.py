"""monotonic — {value_column} is non-decreasing when rows are ordered by
{order_column} within each {partition_column} (optional).

Use cases: row_count over time, cumulative volume, ratchet metrics.
Not for OHLCV close (which legitimately decreases).
"""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("monotonic")
def check_monotonic(con, domain, params):
    """Params: {table, order_column, value_column, partition_column?}."""
    table = params["table"]
    order_col = params["order_column"]
    value_col = params["value_column"]
    partition_col = params.get("partition_column")

    _validate_identifier(con, table, kind="table")
    _validate_identifier(con, order_col, kind="column", table=table)
    _validate_identifier(con, value_col, kind="column", table=table)
    if partition_col:
        _validate_identifier(con, partition_col, kind="column", table=table)

    partition_sql = (
        f"PARTITION BY {partition_col} " if partition_col else ""
    )
    sql = f"""
        WITH ordered AS (
            SELECT {order_col} AS o,
                   {value_col} AS v,
                   LAG({value_col}) OVER (
                       {partition_sql}ORDER BY {order_col}
                   ) AS prev_v
            FROM {table}
            WHERE {value_col} IS NOT NULL
        )
        SELECT o, v, prev_v FROM ordered
        WHERE prev_v IS NOT NULL AND v < prev_v
        LIMIT 5
    """
    decreases = con.execute(sql).fetchall()
    measured = {
        "decrease_count": len(decreases),
        "samples": [(r[0], r[1], r[2]) for r in decreases],
    }
    if decreases:
        return False, measured, (
            f"{table}.{value_col} has decreases ordered by {order_col}"
        )
    return True, measured, None
