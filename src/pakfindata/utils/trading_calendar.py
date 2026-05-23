"""Trading-day calendar for the Pakistan Stock Exchange (PSX).

Public API:

    is_trading_day(date=None) -> bool
        Whether `date` (defaults to today in PKT) is a PSX trading day.
        Skips Saturdays, Sundays, and known PSX public holidays.

    last_trading_day(date) -> str
        Most recent trading day on or before `date`. ISO YYYY-MM-DD.

    next_trading_day(date) -> str
        Next trading day strictly after `date`. ISO YYYY-MM-DD.

The PSX_HOLIDAYS_YYYY sets are hardcoded per year. They are SOURCE-OF-TRUTH
overrides — the helper does not auto-detect holidays. Moon-sighting-dependent
Islamic holidays (Eid ul-Fitr, Eid ul-Adha, Ashura, Eid Milad un-Nabi) are
listed with the most likely civil date based on the PSX-published calendar
at the time of writing; verify against the official PSX holiday list at the
start of each year:

    https://www.psx.com.pk/psx/exchange/general/calendar-holidays

This helper is used by `scripts/daily_sync.sh` to early-exit on weekends
and holidays so cron does not fire 9 failing API calls every Saturday
morning. False-positives (treating a real trading day as a holiday) are
worse than false-negatives — when in doubt, leave the date out of the
set and let the sync attempt the call. Each downstream sync is already
idempotent.
"""

from __future__ import annotations

from datetime import date as _date
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

PKT = ZoneInfo("Asia/Karachi")

# ─────────────────────────────────────────────────────────────────────────
# PSX public holidays
# ─────────────────────────────────────────────────────────────────────────

# 2026 holidays — verify against PSX's official calendar each January.
# Moon-dependent dates carry a comment with the likely civil date.
PSX_HOLIDAYS_2026: frozenset[str] = frozenset({
    "2026-02-05",  # Kashmir Day (Thu)
    "2026-03-20",  # Eid ul-Fitr day 1 (Fri) — moon-dependent
    "2026-03-23",  # Pakistan Day (Mon)
    "2026-05-01",  # Labour Day (Fri)
    "2026-05-27",  # Eid ul-Adha day 1 (Wed) — moon-dependent
    "2026-05-28",  # Eid ul-Adha day 2 (Thu)
    "2026-05-29",  # Eid ul-Adha day 3 (Fri)
    "2026-07-06",  # Ashura 10th (Mon) — moon-dependent
    "2026-08-14",  # Independence Day (Fri)
    "2026-09-04",  # Eid Milad un-Nabi (Fri) — moon-dependent
    "2026-12-25",  # Quaid-e-Azam Day / Christmas (Fri)
})

# 2027 — placeholder; update each January before rolling over.
PSX_HOLIDAYS_2027: frozenset[str] = frozenset()

_HOLIDAYS_BY_YEAR: dict[int, frozenset[str]] = {
    2026: PSX_HOLIDAYS_2026,
    2027: PSX_HOLIDAYS_2027,
}


# ─────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────


def _coerce(d: str | _date | datetime | None) -> _date:
    """Normalize various date inputs to a `datetime.date` in PKT."""
    if d is None:
        return datetime.now(PKT).date()
    if isinstance(d, str):
        return _date.fromisoformat(d)
    if isinstance(d, datetime):
        return d.astimezone(PKT).date() if d.tzinfo else d.date()
    if isinstance(d, _date):
        return d
    raise TypeError(f"Unsupported date type: {type(d).__name__}")


def is_trading_day(d: str | _date | datetime | None = None) -> bool:
    """Return True if `d` is a PSX trading day.

    A PSX trading day is any Monday–Friday that is not on the PSX holiday
    list for that year. Saturdays, Sundays, and listed holidays return
    False. If the year has no entry in `_HOLIDAYS_BY_YEAR`, only the
    weekend filter applies — calls are still safe.
    """
    dt = _coerce(d)
    # Python's weekday: Mon=0, Sun=6. PSX trades Mon–Fri.
    if dt.weekday() >= 5:
        return False
    iso = dt.isoformat()
    holidays = _HOLIDAYS_BY_YEAR.get(dt.year, frozenset())
    return iso not in holidays


def last_trading_day(d: str | _date | datetime | None = None) -> str:
    """Most recent trading day on or before `d`. Returns ISO YYYY-MM-DD."""
    dt = _coerce(d)
    while not is_trading_day(dt):
        dt -= timedelta(days=1)
    return dt.isoformat()


def next_trading_day(d: str | _date | datetime | None = None) -> str:
    """Next trading day strictly after `d`. Returns ISO YYYY-MM-DD."""
    dt = _coerce(d) + timedelta(days=1)
    while not is_trading_day(dt):
        dt += timedelta(days=1)
    return dt.isoformat()
