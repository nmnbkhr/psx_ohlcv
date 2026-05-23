"""Seed initial ``data_quality_rules`` rows.

Phase 2.A.1 seeds the ``indices`` domain only — proof of pattern.
Phase 2.A.2 adds one defensive
``instrument_membership.effective_date`` rule pinned to the 1,530-row
MUFAP cleanup. Phase 2.A.3 starts coverage on populated-but-rule-less
domains (``tbill_auctions`` first — sized to current floor, not the
invalidated 175-row memory). Wider coverage continues as backfill
work surfaces "this should never happen" cases.

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
    # Phase 2.A.2 defensive rule — pinned to the 1,530-row MUFAP cleanup.
    # Future writes that try to land 'MUFAP' (or any non-date string) in
    # instrument_membership.effective_date roll back via the date_format
    # check. Wider symbol-code pollution in this and other tables is
    # tracked as DEBT-PHASE2-FOLLOWUP-2 (Phase 2.A.3 investigation).
    {
        "rule_id": "instrument_membership.effective_date_format",
        "domain": "instrument_membership",
        "check_type": "date_format",
        "params": {
            "table": "instrument_membership",
            "column": "effective_date",
        },
        "severity": "error",
        "description": (
            "instrument_membership.effective_date must contain "
            "YYYY-MM-DD values (catches MUFAP/symbol-code pollution "
            "at write time; 1,530 MUFAP rows cleaned in 2.A.2.2)"
        ),
    },
    # Phase 2.A.3 tbill_auctions rules — populated domain (12 rows
    # post-recovery, growing) that lacked coverage. The 175-row state
    # referenced in CLAUDE.md / earlier memory was pre-2026-05-09 NTFS
    # corruption and isn't in any extant backup; row_count_min is
    # sized to the current floor (12), not the invalidated 175.
    {
        "rule_id": "tbill_auctions.date_format",
        "domain": "tbill_auctions",
        "check_type": "date_format",
        "params": {
            "table": "tbill_auctions",
            "column": "auction_date",
        },
        "severity": "error",
        "description": (
            "tbill_auctions.auction_date must contain YYYY-MM-DD "
            "values (defensive — same pollution class as ZUMA/MUFAP)"
        ),
    },
    {
        "rule_id": "tbill_auctions.row_count_min",
        "domain": "tbill_auctions",
        "check_type": "row_count_min",
        "params": {"table": "tbill_auctions", "min": 12},
        "severity": "warn",
        "description": (
            "tbill_auctions floor sized to post-recovery state "
            "(2.A.3 audit). CLAUDE.md's 175-row figure was pre-"
            "2026-05-09 NTFS corruption and not in any backup."
        ),
    },
    {
        "rule_id": "tbill_auctions.auction_date_not_null",
        "domain": "tbill_auctions",
        "check_type": "not_null",
        "params": {
            "table": "tbill_auctions",
            "column": "auction_date",
        },
        "severity": "error",
        "description": (
            "every tbill_auctions row must have an auction_date "
            "(PK column; defensive against future schema drift)"
        ),
    },
    # Phase 2.A.3 sbp_fx_interbank — Category 3 (sparse-by-upstream).
    # SBP publishes interbank for USD only as an official daily series.
    # Other currencies have 0 rows expected. The rule checks that USD
    # appears at the latest date (i.e. USD doesn't silently disappear
    # from the publisher). Recency-of-latest is not enforced by this
    # check type — see DEBT-PHASE2-FOLLOWUP-4 for the structural note.
    {
        "rule_id": "sbp_fx_interbank.usd_present",
        "domain": "sbp_fx_interbank",
        "check_type": "source_coverage",
        "params": {
            "table": "sbp_fx_interbank",
            "partition_column": "date",
            "value_column": "currency",
            "expected_values": ["USD"],
        },
        "severity": "warn",
        "description": (
            "USD must be present at the latest sbp_fx_interbank.date "
            "(SBP publishes USD as the canonical interbank series; "
            "other currencies are kerb-market only)"
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
