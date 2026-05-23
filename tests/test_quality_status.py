"""Unit tests for the quality-status derivation function.

The four states (clean / degraded / failing / no_rules) are user-facing
semantics — pages key off them for banners. One test per state path so
future drift gets caught.

The function under test is :func:`pakfindata.api.routes.quality.derive_status`
— pure: takes (latest_results, rule_count) → DomainStatus. No DB.
"""

from __future__ import annotations

import pytest

from pakfindata.api.routes.quality import derive_status


# --- "no_rules" path -------------------------------------------------------

def test_no_rules_no_results():
    """rule_count==0 and no results → no_rules."""
    assert derive_status([], rule_count=0) == "no_rules"


def test_no_rules_with_rules_but_no_results():
    """Rules registered but ETL hasn't run yet → still no_rules.

    Semantically: 'we cannot make a quality claim about this domain.'
    Whether the cause is no rules or no runs, the operational meaning
    is identical: no signal.
    """
    assert derive_status([], rule_count=4) == "no_rules"


# --- "clean" path ----------------------------------------------------------

def test_clean_all_passed():
    latest = [
        {"severity": "error", "passed": True},
        {"severity": "warn", "passed": True},
        {"severity": "info", "passed": True},
    ]
    assert derive_status(latest, rule_count=3) == "clean"


def test_clean_single_rule_passed():
    latest = [{"severity": "error", "passed": True}]
    assert derive_status(latest, rule_count=1) == "clean"


# --- "degraded" path -------------------------------------------------------

def test_degraded_warn_failed_no_error():
    """warn failed, no error failed → degraded."""
    latest = [
        {"severity": "error", "passed": True},
        {"severity": "warn", "passed": False},
    ]
    assert derive_status(latest, rule_count=2) == "degraded"


def test_degraded_info_failure_still_clean():
    """info-severity failure does NOT trigger degraded — info is logging only.

    This is a deliberate semantic: info-severity is below the user-facing
    threshold. A page should not banner on it.
    """
    latest = [
        {"severity": "error", "passed": True},
        {"severity": "info", "passed": False},
    ]
    assert derive_status(latest, rule_count=2) == "clean"


# --- "failing" path --------------------------------------------------------

def test_failing_error_severity_failed():
    """Any error-severity failure → failing (overrides clean/degraded)."""
    latest = [
        {"severity": "error", "passed": False},
        {"severity": "warn", "passed": True},
    ]
    assert derive_status(latest, rule_count=2) == "failing"


def test_failing_error_overrides_warn_failure():
    """If both error and warn fail, status is failing (error wins)."""
    latest = [
        {"severity": "error", "passed": False},
        {"severity": "warn", "passed": False},
    ]
    assert derive_status(latest, rule_count=2) == "failing"


# --- Boundary / iteration safety ------------------------------------------

def test_accepts_iterable_not_just_list():
    """derive_status signature is Iterable[dict] — generator works too."""
    def gen():
        yield {"severity": "error", "passed": True}
    assert derive_status(gen(), rule_count=1) == "clean"
