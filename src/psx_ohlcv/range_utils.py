"""Date range utilities for PSX OHLCV data fetching.

Provides utilities for iterating over date ranges with optional weekend skipping,
useful for downloading historical market data files.
"""

from datetime import date, datetime, timedelta
from typing import Iterator


def parse_date(s: str) -> date:
    """Parse a date string in YYYY-MM-DD format.

    Args:
        s: Date string in YYYY-MM-DD format

    Returns:
        date object

    Raises:
        ValueError: If string is not in valid YYYY-MM-DD format
    """
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format '{s}', expected YYYY-MM-DD") from e


def iter_dates(
    start: date,
    end: date,
    skip_weekends: bool = True,
) -> Iterator[date]:
    """Iterate over dates in a range (inclusive).

    Args:
        start: Start date (inclusive)
        end: End date (inclusive)
        skip_weekends: If True, skip Saturday (5) and Sunday (6)

    Yields:
        date objects from start to end

    Raises:
        ValueError: If start > end
    """
    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")

    current = start
    while current <= end:
        # weekday(): Monday=0, ..., Saturday=5, Sunday=6
        if skip_weekends and current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        yield current
        current += timedelta(days=1)


def resolve_range(
    start: str | None = None,
    end: str | None = None,
    days: int | None = None,
    today: date | None = None,
    skip_weekends: bool = True,
) -> tuple[date, date]:
    """Resolve date range from various input combinations.

    Priority:
    1. If both start and end are provided, use them directly
    2. If only days is provided, compute start = today - days, end = today
    3. If start only, end defaults to today
    4. If end only, start defaults to end - 30 days

    Args:
        start: Start date string (YYYY-MM-DD) or None
        end: End date string (YYYY-MM-DD) or None
        days: Number of days to look back from today
        today: Reference date for relative calculations (default: date.today())
        skip_weekends: Not used in resolution, but passed through for consistency

    Returns:
        Tuple of (start_date, end_date)

    Raises:
        ValueError: If dates are invalid or no valid range can be determined
    """
    if today is None:
        today = date.today()

    # Parse provided dates
    start_date: date | None = None
    end_date: date | None = None

    if start is not None:
        start_date = parse_date(start)
    if end is not None:
        end_date = parse_date(end)

    # Resolution logic
    if start_date is not None and end_date is not None:
        # Both provided - use directly
        pass
    elif days is not None:
        # Days-based range
        end_date = today
        start_date = today - timedelta(days=days)
    elif start_date is not None:
        # Start only - end is today
        end_date = today
    elif end_date is not None:
        # End only - start is 30 days before
        start_date = end_date - timedelta(days=30)
    else:
        # Nothing provided - default to last 7 days
        end_date = today
        start_date = today - timedelta(days=7)

    # Validate range
    if start_date > end_date:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    return start_date, end_date


def count_weekdays(start: date, end: date) -> int:
    """Count weekdays (Mon-Fri) in a date range.

    Args:
        start: Start date (inclusive)
        end: End date (inclusive)

    Returns:
        Number of weekdays in range
    """
    if start > end:
        return 0

    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon-Fri
            count += 1
        current += timedelta(days=1)
    return count


def format_date(d: date) -> str:
    """Format date as YYYY-MM-DD string.

    Args:
        d: date object

    Returns:
        String in YYYY-MM-DD format
    """
    return d.strftime("%Y-%m-%d")
