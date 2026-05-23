"""Data quality endpoints — read window over the data_quality_* tables.

The engine (pakfindata.quality) writes results from inside ETL
safe_writer transactions. The API just exposes them for UI banner
queries + future Phase 2.B dashboard scrapes.

Routes (Bearer-auth via global middleware):

    GET /v1/quality                       all domains' summaries
    GET /v1/quality/rules                 every enabled rule
    GET /v1/quality/rules/{domain}        rules for one domain
    GET /v1/quality/{domain}              one domain's summary
    GET /v1/quality/{domain}/history      recent results, bounded
"""

from __future__ import annotations

import json
import sqlite3
from typing import Annotated, Iterable

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from pakfindata.api.deps import get_read_db
from pakfindata.api.schemas.quality import (
    DomainQualitySummary,
    DomainStatus,
    QualityResult,
    QualityRule,
)

router = APIRouter(prefix="/v1/quality", tags=["quality"])


def derive_status(
    latest_results: Iterable[dict],
    rule_count: int,
) -> DomainStatus:
    """Map (rule_count, latest results per rule) → one of the four states.

    Exposed at module level so the unit test in
    ``tests/test_quality_status.py`` can cover each path independently
    of the DB.
    """
    latest = list(latest_results)
    if rule_count == 0 or not latest:
        return "no_rules"
    if any(
        r["severity"] == "error" and not r["passed"] for r in latest
    ):
        return "failing"
    if any(
        r["severity"] == "warn" and not r["passed"] for r in latest
    ):
        return "degraded"
    return "clean"


# --- Helpers ---------------------------------------------------------------


def _row_to_rule(row: sqlite3.Row) -> QualityRule:
    return QualityRule(
        rule_id=row["rule_id"],
        domain=row["domain"],
        check_type=row["check_type"],
        params=json.loads(row["params"] or "{}"),
        severity=row["severity"],
        enabled=bool(row["enabled"]),
        description=row["description"],
    )


def _row_to_result(row: sqlite3.Row) -> QualityResult:
    return QualityResult(
        rule_id=row["rule_id"],
        domain=row["domain"],
        check_type=row["check_type"],
        severity=row["severity"],
        passed=bool(row["passed"]),
        measured=json.loads(row["measured"]) if row["measured"] else None,
        expected=json.loads(row["expected"]) if row["expected"] else None,
        error_message=row["error_message"],
        duration_ms=row["duration_ms"],
        run_at=row["run_at"],
    )


def _summary_for_domain(
    con: sqlite3.Connection, domain: str
) -> DomainQualitySummary:
    rule_count = con.execute(
        "SELECT COUNT(*) FROM data_quality_rules "
        "WHERE domain = ? AND enabled = 1",
        (domain,),
    ).fetchone()[0]

    # Latest result per rule (uses MAX(result_id) since result_id is
    # AUTOINCREMENT and monotonic with run_at within a domain).
    latest_rows = con.execute(
        """
        WITH latest AS (
            SELECT rule_id, MAX(result_id) AS max_id
            FROM data_quality_results
            WHERE domain = ?
            GROUP BY rule_id
        )
        SELECT r.rule_id, r.severity, r.passed, r.run_at
        FROM data_quality_results r
        INNER JOIN latest l ON r.result_id = l.max_id
        """,
        (domain,),
    ).fetchall()

    latest = [
        {
            "rule_id": r["rule_id"],
            "severity": r["severity"],
            "passed": bool(r["passed"]),
            "run_at": r["run_at"],
        }
        for r in latest_rows
    ]

    passed = sum(1 for r in latest if r["passed"])
    failed = len(latest) - passed
    error_failed = sum(
        1 for r in latest
        if r["severity"] == "error" and not r["passed"]
    )
    warn_failed = sum(
        1 for r in latest
        if r["severity"] == "warn" and not r["passed"]
    )
    last_run_at = (
        max(r["run_at"] for r in latest) if latest else None
    )

    return DomainQualitySummary(
        domain=domain,
        last_run_at=last_run_at,
        rule_count=rule_count,
        passed=passed,
        failed=failed,
        error_severity_failures=error_failed,
        warn_severity_failures=warn_failed,
        status=derive_status(latest, rule_count),
    )


# --- Routes (order matters: /rules and /rules/{domain} before /{domain}) ---


@router.get("", response_model=list[DomainQualitySummary])
def list_quality_summaries(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> list[DomainQualitySummary]:
    """One summary per domain that has at least one rule registered."""
    domains = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT domain FROM data_quality_rules "
            "ORDER BY domain"
        ).fetchall()
    ]
    return [_summary_for_domain(con, d) for d in domains]


@router.get("/rules", response_model=list[QualityRule])
def list_all_rules(
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> list[QualityRule]:
    """Every enabled rule across all domains. Config introspection."""
    rows = con.execute(
        "SELECT * FROM data_quality_rules "
        "WHERE enabled = 1 ORDER BY rule_id"
    ).fetchall()
    return [_row_to_rule(r) for r in rows]


@router.get("/rules/{domain}", response_model=list[QualityRule])
def list_rules_for_domain(
    domain: Annotated[str, Path(description="data_freshness.domain key")],
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> list[QualityRule]:
    rows = con.execute(
        "SELECT * FROM data_quality_rules "
        "WHERE domain = ? ORDER BY rule_id",
        (domain,),
    ).fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"no rules registered for domain {domain!r}",
        )
    return [_row_to_rule(r) for r in rows]


@router.get("/{domain}", response_model=DomainQualitySummary)
def get_domain_summary(
    domain: Annotated[str, Path(description="data_freshness.domain key")],
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
) -> DomainQualitySummary:
    """Latest validation state for one domain."""
    return _summary_for_domain(con, domain)


@router.get("/{domain}/history", response_model=list[QualityResult])
def get_domain_history(
    domain: Annotated[str, Path(description="data_freshness.domain key")],
    con: Annotated[sqlite3.Connection, Depends(get_read_db)],
    limit: Annotated[
        int,
        Query(
            ge=1, le=500,
            description=(
                "Maximum rows to return. Phase 2.B dashboards will "
                "query this regularly; an unbounded query against "
                "data_quality_results becomes a tail-latency problem "
                "once domains accumulate hundreds of runs."
            ),
        ),
    ] = 50,
) -> list[QualityResult]:
    """Recent results for a domain, newest first. Bounded by ``limit``."""
    rows = con.execute(
        """
        SELECT * FROM data_quality_results
        WHERE domain = ?
        ORDER BY result_id DESC
        LIMIT ?
        """,
        (domain, limit),
    ).fetchall()
    return [_row_to_result(r) for r in rows]
