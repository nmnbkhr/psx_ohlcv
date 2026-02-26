"""Tests for range_utils module."""

from datetime import date

import pytest

from pakfindata.range_utils import (
    count_weekdays,
    format_date,
    iter_dates,
    parse_date,
    resolve_range,
)


class TestParseDate:
    """Tests for parse_date function."""

    def test_valid_date(self):
        """Test parsing valid date string."""
        result = parse_date("2025-01-15")
        assert result == date(2025, 1, 15)

    def test_valid_date_leading_zeros(self):
        """Test parsing date with leading zeros."""
        result = parse_date("2025-01-05")
        assert result == date(2025, 1, 5)

    def test_invalid_format_no_dashes(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("20250115")

    def test_invalid_format_wrong_separator(self):
        """Test that wrong separator raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("2025/01/15")

    def test_invalid_format_incomplete(self):
        """Test that incomplete date raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("2025-01")

    def test_invalid_date_values(self):
        """Test that invalid date values raise ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("2025-13-45")

    def test_empty_string(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("")


class TestIterDates:
    """Tests for iter_dates function."""

    def test_single_day(self):
        """Test iterating over single day."""
        start = date(2025, 1, 15)  # Wednesday
        result = list(iter_dates(start, start))
        assert result == [date(2025, 1, 15)]

    def test_weekdays_only(self):
        """Test iterating over weekdays only (skip weekends)."""
        # Mon Jan 13 to Fri Jan 17, 2025
        start = date(2025, 1, 13)
        end = date(2025, 1, 17)
        result = list(iter_dates(start, end, skip_weekends=True))
        assert len(result) == 5
        assert result[0] == date(2025, 1, 13)  # Mon
        assert result[4] == date(2025, 1, 17)  # Fri

    def test_include_weekends(self):
        """Test iterating with weekends included."""
        # Mon Jan 13 to Sun Jan 19, 2025
        start = date(2025, 1, 13)
        end = date(2025, 1, 19)
        result = list(iter_dates(start, end, skip_weekends=False))
        assert len(result) == 7  # All 7 days

    def test_skip_weekends_across_weekend(self):
        """Test skipping weekends when range spans weekend."""
        # Fri Jan 17 to Mon Jan 20, 2025
        start = date(2025, 1, 17)
        end = date(2025, 1, 20)
        result = list(iter_dates(start, end, skip_weekends=True))
        assert len(result) == 2  # Friday and Monday only
        assert result[0] == date(2025, 1, 17)  # Fri
        assert result[1] == date(2025, 1, 20)  # Mon

    def test_start_on_weekend(self):
        """Test starting on weekend day with skip enabled."""
        # Sat Jan 18 to Mon Jan 20, 2025
        start = date(2025, 1, 18)  # Saturday
        end = date(2025, 1, 20)
        result = list(iter_dates(start, end, skip_weekends=True))
        assert len(result) == 1  # Only Monday
        assert result[0] == date(2025, 1, 20)

    def test_end_on_weekend(self):
        """Test ending on weekend day with skip enabled."""
        # Fri Jan 17 to Sat Jan 18, 2025
        start = date(2025, 1, 17)  # Friday
        end = date(2025, 1, 18)  # Saturday
        result = list(iter_dates(start, end, skip_weekends=True))
        assert len(result) == 1  # Only Friday
        assert result[0] == date(2025, 1, 17)

    def test_weekend_only_range(self):
        """Test range containing only weekend days with skip enabled."""
        # Sat Jan 18 to Sun Jan 19, 2025
        start = date(2025, 1, 18)
        end = date(2025, 1, 19)
        result = list(iter_dates(start, end, skip_weekends=True))
        assert len(result) == 0

    def test_start_after_end_raises(self):
        """Test that start after end raises ValueError."""
        with pytest.raises(ValueError, match="Start date .* is after end date"):
            list(iter_dates(date(2025, 1, 20), date(2025, 1, 15)))

    def test_long_range(self):
        """Test iterating over a month."""
        start = date(2025, 1, 1)
        end = date(2025, 1, 31)
        result = list(iter_dates(start, end, skip_weekends=True))
        # January 2025: 23 weekdays
        assert len(result) == 23


class TestResolveRange:
    """Tests for resolve_range function."""

    def test_explicit_start_and_end(self):
        """Test with explicit start and end dates."""
        start, end = resolve_range(
            start="2025-01-10",
            end="2025-01-15",
        )
        assert start == date(2025, 1, 10)
        assert end == date(2025, 1, 15)

    def test_days_from_today(self):
        """Test with days parameter."""
        today = date(2025, 1, 20)
        start, end = resolve_range(days=7, today=today)
        assert start == date(2025, 1, 13)
        assert end == date(2025, 1, 20)

    def test_start_only(self):
        """Test with start only (end defaults to today)."""
        today = date(2025, 1, 20)
        start, end = resolve_range(start="2025-01-10", today=today)
        assert start == date(2025, 1, 10)
        assert end == date(2025, 1, 20)

    def test_end_only(self):
        """Test with end only (start defaults to end - 30 days)."""
        start, end = resolve_range(end="2025-01-31")
        assert start == date(2025, 1, 1)
        assert end == date(2025, 1, 31)

    def test_no_parameters(self):
        """Test with no parameters (defaults to last 7 days)."""
        today = date(2025, 1, 20)
        start, end = resolve_range(today=today)
        assert start == date(2025, 1, 13)
        assert end == date(2025, 1, 20)

    def test_invalid_start_date(self):
        """Test with invalid start date."""
        with pytest.raises(ValueError, match="Invalid date format"):
            resolve_range(start="invalid")

    def test_invalid_end_date(self):
        """Test with invalid end date."""
        with pytest.raises(ValueError, match="Invalid date format"):
            resolve_range(end="invalid")

    def test_start_after_end_raises(self):
        """Test that start after end raises ValueError."""
        with pytest.raises(ValueError, match="Start date .* is after end date"):
            resolve_range(start="2025-01-20", end="2025-01-10")


class TestCountWeekdays:
    """Tests for count_weekdays function."""

    def test_single_weekday(self):
        """Test counting single weekday."""
        result = count_weekdays(date(2025, 1, 15), date(2025, 1, 15))
        assert result == 1

    def test_single_weekend(self):
        """Test counting single weekend day."""
        result = count_weekdays(date(2025, 1, 18), date(2025, 1, 18))  # Saturday
        assert result == 0

    def test_full_week(self):
        """Test counting full week (Mon-Sun)."""
        result = count_weekdays(date(2025, 1, 13), date(2025, 1, 19))
        assert result == 5

    def test_empty_range(self):
        """Test counting when start > end."""
        result = count_weekdays(date(2025, 1, 20), date(2025, 1, 10))
        assert result == 0

    def test_two_weeks(self):
        """Test counting two weeks."""
        result = count_weekdays(date(2025, 1, 13), date(2025, 1, 26))
        assert result == 10


class TestFormatDate:
    """Tests for format_date function."""

    def test_basic_format(self):
        """Test basic date formatting."""
        result = format_date(date(2025, 1, 15))
        assert result == "2025-01-15"

    def test_leading_zeros(self):
        """Test that leading zeros are preserved."""
        result = format_date(date(2025, 1, 5))
        assert result == "2025-01-05"

    def test_december(self):
        """Test December date formatting."""
        result = format_date(date(2025, 12, 31))
        assert result == "2025-12-31"
