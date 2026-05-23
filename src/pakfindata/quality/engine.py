"""Validator engine — rule loader + dispatcher.

Loads rules from ``data_quality_rules``, dispatches to handlers
registered via ``@register_check``, returns ``CheckResult`` objects.
Engine is read-only — ETL inside ``safe_writer`` decides what to do
with results (write to data_quality_results, roll back on
error-severity failures).

Layering: handlers receive ``(con, domain, params)`` and return
``(passed, measured, error_message)``. They MUST NOT touch
``data_quality_*`` tables — that's the engine's job. Identifier
safety lives in :mod:`pakfindata.quality.identifiers` and is
re-exported here as ``_validate_identifier`` for handler convenience.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from pakfindata.quality.identifiers import validate_identifier as _validate_identifier

__all__ = [
    "CheckResult",
    "DataQualityError",
    "register_check",
    "run_checks_for_domain",
    "CHECK_REGISTRY",
    "_validate_identifier",
]


@dataclass(frozen=True)
class CheckResult:
    """One rule's outcome on one run. Written to data_quality_results by ETL."""

    rule_id: str
    domain: str
    check_type: str
    severity: str
    passed: bool
    measured: dict[str, Any]
    expected: dict[str, Any]
    error_message: Optional[str]
    duration_ms: int


class DataQualityError(Exception):
    """ETL raises this when error-severity checks fail; safe_writer
    rolls back the transaction, so pollution never persists."""


# Handler signature: (con, domain, params_dict) -> (passed, measured, error_message_or_None)
CheckHandler = Callable[
    [sqlite3.Connection, str, dict[str, Any]],
    tuple[bool, dict[str, Any], Optional[str]],
]

CHECK_REGISTRY: dict[str, CheckHandler] = {}


def register_check(check_type: str) -> Callable[[CheckHandler], CheckHandler]:
    """Decorator: register a handler under the given check_type string."""

    def decorator(fn: CheckHandler) -> CheckHandler:
        if check_type in CHECK_REGISTRY:
            raise ValueError(
                f"check_type {check_type!r} already registered by "
                f"{CHECK_REGISTRY[check_type].__module__}"
            )
        CHECK_REGISTRY[check_type] = fn
        return fn

    return decorator


def run_checks_for_domain(
    con: sqlite3.Connection,
    domain: str,
) -> list[CheckResult]:
    """Run all enabled rules for `domain`, in rule_id order.

    Handler exceptions are caught and converted to failed CheckResults;
    engine never propagates. ETL routes the results based on severity.
    """
    rules = con.execute(
        """
        SELECT rule_id, check_type, params, severity
        FROM data_quality_rules
        WHERE domain = ? AND enabled = 1
        ORDER BY rule_id
        """,
        (domain,),
    ).fetchall()

    results: list[CheckResult] = []
    for rule_id, check_type, params_json, severity in rules:
        try:
            params = json.loads(params_json or "{}")
        except json.JSONDecodeError as e:
            results.append(
                _failure(rule_id, domain, check_type, severity,
                         f"params JSON decode error: {e}", 0)
            )
            continue

        handler = CHECK_REGISTRY.get(check_type)
        if handler is None:
            results.append(
                _failure(rule_id, domain, check_type, severity,
                         f"unknown check_type: {check_type!r}", 0,
                         expected=params)
            )
            continue

        t0 = time.monotonic()
        try:
            passed, measured, err = handler(con, domain, params)
        except ValueError as e:
            passed, measured, err = False, {}, f"identifier/param error: {e}"
        except Exception as e:  # pragma: no cover (defensive)
            passed, measured, err = False, {}, (
                f"handler exception: {type(e).__name__}: {e}"
            )
        duration_ms = int((time.monotonic() - t0) * 1000)

        results.append(
            CheckResult(
                rule_id=rule_id,
                domain=domain,
                check_type=check_type,
                severity=severity,
                passed=passed,
                measured=measured,
                expected=params,
                error_message=err if not passed else None,
                duration_ms=duration_ms,
            )
        )

    return results


def _failure(
    rule_id: str,
    domain: str,
    check_type: str,
    severity: str,
    err: str,
    duration_ms: int,
    *,
    expected: Optional[dict[str, Any]] = None,
) -> CheckResult:
    return CheckResult(
        rule_id=rule_id,
        domain=domain,
        check_type=check_type,
        severity=severity,
        passed=False,
        measured={},
        expected=expected or {},
        error_message=err,
        duration_ms=duration_ms,
    )
