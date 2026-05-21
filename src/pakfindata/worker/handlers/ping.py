"""Demo handler — proves the worker dispatch pipeline works end-to-end.

Sleeps for ``sleep_seconds`` (default 2). Returns ``{"echo": message,
"slept_s": ...}``. Useful for:

- Smoke-testing the worker after deployment
- Measuring round-trip latency from enqueue to finish
- Validating job-status polling from clients
"""

from __future__ import annotations

import time


def handle_ping(sleep_seconds: float = 2.0, message: str = "pong") -> dict:
    """Sleep, then return an echo dict. Never raises."""
    secs = max(0.0, float(sleep_seconds))
    time.sleep(secs)
    return {"echo": str(message), "slept_s": secs}
