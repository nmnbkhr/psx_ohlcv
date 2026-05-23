"""Pydantic models for /v1/quality endpoints.

Reads only — the data quality engine writes results from inside ETL
safe_writer transactions (Phase 2.A.1.4); the API is a read window
over what the engine produced.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel


Severity = Literal["error", "warn", "info"]

DomainStatus = Literal["clean", "degraded", "failing", "no_rules"]


class QualityRule(BaseModel):
    """One row from data_quality_rules."""

    rule_id: str
    domain: str
    check_type: str
    params: dict[str, Any]
    severity: Severity
    enabled: bool
    description: Optional[str] = None


class QualityResult(BaseModel):
    """One row from data_quality_results."""

    rule_id: str
    domain: str
    check_type: str
    severity: Severity
    passed: bool
    measured: Optional[dict[str, Any]] = None
    expected: Optional[dict[str, Any]] = None
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None
    run_at: str


class DomainQualitySummary(BaseModel):
    """Roll-up over the latest result of each enabled rule for one domain.

    ``status`` derivation:
        - ``no_rules``: rule_count==0 OR no results yet
        - ``failing``: any error-severity rule's latest run failed
        - ``degraded``: any warn-severity rule's latest run failed
          (with no error-severity failures)
        - ``clean``: every latest result passed
    """

    domain: str
    last_run_at: Optional[str] = None
    rule_count: int
    passed: int
    failed: int
    error_severity_failures: int
    warn_severity_failures: int
    status: DomainStatus
