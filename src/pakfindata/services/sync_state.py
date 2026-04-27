"""Sync state file — single source of truth for last sync dates.

Simple JSON file with one key per sync domain (eod, intraday).
Sync jobs overwrite it when they complete. Dashboard reads from it
instead of scanning 600K+ row tables with MAX(date) queries.

File: /mnt/e/psxdata/last_sync.json

Schema:
    {
      "last_eod_date":      "2026-04-17",
      "last_intraday_date": "2026-04-17",
      "updated_at":         "2026-04-17T15:30:00"
    }
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

STATE_FILE = Path("/mnt/e/psxdata/last_sync.json")

__all__ = [
    "read_sync_state",
    "set_last_eod_date",
    "set_last_intraday_date",
    "set_last_tick_date",
    "get_last_eod_date",
    "get_last_intraday_date",
    "get_last_tick_date",
    "STATE_FILE",
]


def read_sync_state() -> dict:
    """Read the full state file. Returns {} if missing."""
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_state(state: dict) -> None:
    """Atomic write of the state file."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(STATE_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(STATE_FILE))


def set_last_eod_date(date: str) -> None:
    """Called by EOD sync jobs on completion."""
    state = read_sync_state()
    state["last_eod_date"] = str(date)[:10]
    state["updated_at"] = datetime.now().isoformat()
    _write_state(state)


def set_last_intraday_date(date: str) -> None:
    """Called by intraday sync jobs on completion."""
    state = read_sync_state()
    state["last_intraday_date"] = str(date)[:10]
    state["updated_at"] = datetime.now().isoformat()
    _write_state(state)


def set_last_tick_date(date: str, count: int | None = None) -> None:
    """Called by tick sync / collector on completion."""
    state = read_sync_state()
    state["last_tick_date"] = str(date)[:10]
    if count is not None:
        state["last_tick_count"] = int(count)
    state["updated_at"] = datetime.now().isoformat()
    _write_state(state)


def get_last_eod_date() -> str | None:
    """Read last EOD date from file. Returns None if not set."""
    return read_sync_state().get("last_eod_date")


def get_last_intraday_date() -> str | None:
    """Read last intraday date from file. Returns None if not set."""
    return read_sync_state().get("last_intraday_date")


def get_last_tick_date() -> tuple[str | None, int | None]:
    """Read last tick date + count. Returns (date, count) or (None, None)."""
    state = read_sync_state()
    return state.get("last_tick_date"), state.get("last_tick_count")
