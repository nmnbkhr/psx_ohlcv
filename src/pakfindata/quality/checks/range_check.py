"""range — every non-null {column} value in {table} is within [min, max].

Either bound is optional; specify both, one, or neither (with neither,
the check passes trivially — useful to document an intent without an
enforcement target yet).

Filename is ``range_check`` to avoid shadowing the Python built-in
``range``; the registered check_type is still ``"range"``.
"""

from __future__ import annotations

from pakfindata.quality.engine import _validate_identifier, register_check


@register_check("range")
def check_range(con, domain, params):
    """Params: {table, column, min?, max?}."""
    table = params["table"]
    column = params["column"]
    lo = params.get("min")
    hi = params.get("max")

    _validate_identifier(con, table, kind="table")
    _validate_identifier(con, column, kind="column", table=table)

    conds: list[str] = []
    args: list = []
    if lo is not None:
        conds.append(f"{column} < ?")
        args.append(lo)
    if hi is not None:
        conds.append(f"{column} > ?")
        args.append(hi)
    if not conds:
        return True, {"note": "no bounds specified"}, None

    where = f"{column} IS NOT NULL AND ({' OR '.join(conds)})"
    bad = con.execute(
        f"SELECT {column} FROM {table} WHERE {where} LIMIT 5", args
    ).fetchall()

    measured = {
        "out_of_range_count": len(bad),
        "samples": [r[0] for r in bad],
        "bounds": {"min": lo, "max": hi},
    }
    if bad:
        return False, measured, (
            f"{table}.{column} has values outside [{lo}, {hi}]"
        )
    return True, measured, None
