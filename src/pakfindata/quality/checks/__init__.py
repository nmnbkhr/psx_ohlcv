"""Check handlers — one module per check_type.

Importing this package side-effectively populates
:data:`pakfindata.quality.engine.CHECK_REGISTRY` via the
``@register_check`` decorators on each module's handler function.

Handlers receive ``(con, domain, params)`` and return
``(passed, measured, error_message)``. They MUST NOT touch
``data_quality_rules`` or ``data_quality_results`` — the engine
owns those. They MUST call
:func:`pakfindata.quality.engine._validate_identifier` before
interpolating any ``{table}`` / ``{column}`` into SQL.
"""

from pakfindata.quality.checks import (  # noqa: F401  (side-effect imports)
    custom_sql,
    date_format,
    monotonic,
    not_empty,
    not_null,
    range_check,
    reference,
    row_count_min,
    source_coverage,
)
