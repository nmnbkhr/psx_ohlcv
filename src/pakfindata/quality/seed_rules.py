"""Seed initial ``data_quality_rules`` rows.

Phase 2.A.1 seeds the ``indices`` domain only — proof of pattern.
Phase 2.A.3 adds rules for the other ~38 registered domains as the
backfill work surfaces "this should never happen" cases.

Idempotent: ``INSERT OR REPLACE`` on ``rule_id``. Run via::

    python -m pakfindata.quality.seed_rules

Naming convention: ``rule_id`` and ``domain`` both use the
``data_freshness.domain`` key (e.g. ``"indices"``). The source-table
identifier (e.g. ``"psx_indices"``) lives in ``params.table`` where
check handlers need it.
"""

from __future__ import annotations

import json
from typing import Any

from pakfindata.db.safe_writer import safe_writer

# (rule_id, domain, check_type, params, severity, description)
INITIAL_RULES: list[dict[str, Any]] = [
    {
        "rule_id": "indices.date_format",
        "domain": "indices",
        "check_type": "date_format",
        "params": {"table": "psx_indices", "column": "index_date"},
        "severity": "error",
        "description": (
            "psx_indices.index_date must contain YYYY-MM-DD values "
            "(catches ZUMA/WTL/TBILL pollution at write time)"
        ),
    },
    {
        "rule_id": "indices.row_count_min",
        "domain": "indices",
        "check_type": "row_count_min",
        "params": {"table": "psx_indices", "min": 1},
        "severity": "error",
        "description": "psx_indices must have at least 1 row",
    },
    {
        "rule_id": "indices.index_code_not_null",
        "domain": "indices",
        "check_type": "not_null",
        "params": {"table": "psx_indices", "column": "index_code"},
        "severity": "error",
        "description": "every psx_indices row must have an index_code",
    },
    {
        "rule_id": "indices.value_range",
        "domain": "indices",
        "check_type": "range",
        "params": {
            "table": "psx_indices",
            "column": "value",
            "min": 0,
            "max": 10_000_000,
        },
        "severity": "warn",
        "description": (
            "psx_indices.value sanity bound — PSX indices roughly "
            "1k (junior) to 200k (KSE100); 10M cap leaves headroom "
            "for inflation without admitting obvious garbage"
        ),
    },
]


def seed() -> int:
    """Idempotently upsert the seed rules. Returns the rule count."""
    with safe_writer() as con:
        for r in INITIAL_RULES:
            con.execute(
                """
                INSERT INTO data_quality_rules
                (rule_id, domain, check_type, params, severity, description, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(rule_id) DO UPDATE SET
                    domain      = excluded.domain,
                    check_type  = excluded.check_type,
                    params      = excluded.params,
                    severity    = excluded.severity,
                    description = excluded.description,
                    enabled     = 1,
                    updated_at  = datetime('now')
                """,
                (
                    r["rule_id"],
                    r["domain"],
                    r["check_type"],
                    json.dumps(r["params"]),
                    r["severity"],
                    r["description"],
                ),
            )
    return len(INITIAL_RULES)


if __name__ == "__main__":
    n = seed()
    print(f"Seeded {n} rules")
