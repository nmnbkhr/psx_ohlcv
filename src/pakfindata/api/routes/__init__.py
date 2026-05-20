"""New-style routes for the pakfindata API.

This package hosts routes that follow the Phase 1 contract:
- structured logging
- Bearer auth (or explicitly listed as public)
- canonical pydantic response models

The legacy `pakfindata.api.routers.*` package continues to exist for
the 16 pre-Phase-1 routers; Phase 1 will gradually migrate them here.
"""
