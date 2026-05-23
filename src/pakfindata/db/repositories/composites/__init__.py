"""Composite-aggregator repository functions.

Cross-domain analytical views that combine reads from 3+ source tables
(or 2 with non-trivial joins). One module per view; the route handler
in ``api/routes/<domain>.py`` is thin and delegates here.

See ``docs/architecture/composite_aggregator_pattern.md`` for rules.
"""
