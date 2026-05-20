"""Health endpoint. Public (no auth) — listed in PUBLIC_PATHS.

Returns service version, current timestamp, db_path reachability,
and a summary of the data_freshness catalog (Phase 0.2's single
source of truth).

Consumers:
    - systemd healthcheck (future)
    - Streamlit dashboard's status badge (1.3+)
    - External monitoring (Phase 2)
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter

from pakfindata.api.config import get_settings


router = APIRouter(tags=["health"])

API_VERSION = "0.1.0"


def _catalog_summary(db_path: str) -> tuple[str, dict]:
    """Return (db_status, status→count map) from data_freshness.

    Read-only connection (uri mode=ro) — never holds a writer lock.
    """
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2)
        try:
            rows = con.execute(
                "SELECT status, COUNT(*) FROM data_freshness GROUP BY status"
            ).fetchall()
            return "ok", {status: count for status, count in rows}
        finally:
            con.close()
    except sqlite3.OperationalError as exc:
        # Most common: file missing, schema not yet migrated, or DB
        # is being recovered. Don't raise — caller wants a single
        # JSON response, not a 500.
        return f"error: {exc}", {}
    except Exception as exc:  # noqa: BLE001 — health must never raise
        return f"error: {type(exc).__name__}", {}


@router.get("/health")
def health() -> dict:
    settings = get_settings()
    db_status, catalog = _catalog_summary(str(settings.db_path))
    return {
        "status": "ok" if db_status == "ok" else "degraded",
        "version": API_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "db_path": str(settings.db_path),
        "db_status": db_status,
        "catalog_summary": catalog,
    }
