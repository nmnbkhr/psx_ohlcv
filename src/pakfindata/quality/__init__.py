"""Data quality layer (Phase 2.A.1).

Declarative validators driven by ``data_quality_rules`` rows; outcomes
written to ``data_quality_results`` from inside ETL ``safe_writer``
transactions. Pages query ``/v1/quality/{domain}`` to degrade gracefully.

Public surface:

    from pakfindata.quality import (
        CheckResult,
        DataQualityError,
        run_checks_for_domain,
        register_check,
        CHECK_REGISTRY,
    )

Importing this package side-effectively registers all 9 check handlers
via ``@register_check`` decorators on their module-level functions.
"""

from pakfindata.quality.engine import (
    CHECK_REGISTRY,
    CheckResult,
    DataQualityError,
    register_check,
    run_checks_for_domain,
)

# Side-effect import: populates CHECK_REGISTRY via @register_check decorators.
from pakfindata.quality import checks  # noqa: F401  (side-effect import)

__all__ = [
    "CheckResult",
    "DataQualityError",
    "register_check",
    "run_checks_for_domain",
    "CHECK_REGISTRY",
]
