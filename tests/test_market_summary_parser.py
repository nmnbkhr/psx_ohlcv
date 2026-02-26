"""Tests for market summary parser and range-based downloading."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pakfindata.sources.market_summary import (
    MARKET_SUMMARY_COLUMNS,
    _validate_date_format,
    fetch_day,
    fetch_range,
    fetch_range_summary,
    parse_market_summary,
)


class TestParseMarketSummary:
    """Tests for parse_market_summary function."""

    def test_parses_valid_pipe_delimited_data(self, tmp_path):
        """Test parsing valid pipe-delimited market summary data."""
        # Create sample data with exactly 10 pipe-delimited fields
        lines = [
            "2026-01-20|ABOT|01|Abbott Labs|100.50|102.00|99.50|101.25|50000|100.00",
            "2026-01-20|HBL|02|Habib Bank|150.75|155.00|149.00|153.50|100000|150.00",
            "2026-01-20|OGDC|03|Oil and Gas|85.25|87.00|84.50|86.00|75000|85.00",
        ]
        sample_data = "\n".join(lines)

        # Write to temp file
        test_file = tmp_path / "2026-01-20"
        test_file.write_text(sample_data)

        # Parse
        df = parse_market_summary(test_file)

        # Verify shape
        assert len(df) == 3
        assert list(df.columns) == MARKET_SUMMARY_COLUMNS + ["market_type"]

    def test_correct_column_names(self, tmp_path):
        """Test that parsed DataFrame has correct column names."""
        sample_data = "2026-01-20|TEST|01|Test Company|10.0|11.0|9.0|10.5|1000|10.0"
        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        expected_columns = [
            "date", "symbol", "sector_code", "company_name",
            "open", "high", "low", "close", "volume", "prev_close",
            "market_type",
        ]
        assert list(df.columns) == expected_columns

    def test_numeric_conversion(self, tmp_path):
        """Test that numeric fields are properly converted."""
        sample_data = (
            "2026-01-20|ABC|01|ABC Corp|100.50|105.75|98.25|103.00|50000|99.00"
        )
        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        assert df.iloc[0]["open"] == 100.50
        assert df.iloc[0]["high"] == 105.75
        assert df.iloc[0]["low"] == 98.25
        assert df.iloc[0]["close"] == 103.00
        assert df.iloc[0]["volume"] == 50000
        assert df.iloc[0]["prev_close"] == 99.00

    def test_skips_malformed_rows(self, tmp_path):
        """Test that rows with wrong number of fields are skipped."""
        sample_data = """2026-01-20|VALID|01|Valid Company|10.0|11.0|9.0|10.5|1000|10.0
2026-01-20|INVALID|only|three|fields
2026-01-20|ALSO_VALID|02|Also Valid|20.0|22.0|19.0|21.0|2000|20.0"""

        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        # Only 2 valid rows
        assert len(df) == 2
        assert "VALID" in df["symbol"].values
        assert "ALSO_VALID" in df["symbol"].values
        assert "INVALID" not in df["symbol"].values

    def test_handles_empty_file(self, tmp_path):
        """Test handling of empty file."""
        test_file = tmp_path / "empty"
        test_file.write_text("")

        df = parse_market_summary(test_file)

        assert df.empty
        assert list(df.columns) == MARKET_SUMMARY_COLUMNS + ["market_type"]

    def test_handles_whitespace_lines(self, tmp_path):
        """Test that whitespace-only lines are skipped."""
        sample_data = """2026-01-20|ABC|01|ABC Corp|100.0|110.0|90.0|105.0|1000|100.0


2026-01-20|XYZ|02|XYZ Corp|200.0|210.0|190.0|205.0|2000|200.0
"""
        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        assert len(df) == 2

    def test_symbol_uppercase_and_stripped(self, tmp_path):
        """Test that symbols are converted to uppercase and stripped."""
        sample_data = "2026-01-20| abc |01|Company|10.0|11.0|9.0|10.5|1000|10.0"
        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        assert df.iloc[0]["symbol"] == "ABC"

    def test_handles_coerce_errors(self, tmp_path):
        """Test that invalid numeric values are coerced to NaN."""
        sample_data = "2026-01-20|TEST|01|Test|invalid|11.0|9.0|10.5|1000|10.0"
        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        # Open should be NaN due to 'invalid' value
        assert pd.isna(df.iloc[0]["open"])
        # Other values should be valid
        assert df.iloc[0]["high"] == 11.0

    def test_sorted_by_symbol(self, tmp_path):
        """Test that results are sorted by symbol."""
        sample_data = """2026-01-20|ZZZ|03|Z Corp|30.0|31.0|29.0|30.5|3000|30.0
2026-01-20|AAA|01|A Corp|10.0|11.0|9.0|10.5|1000|10.0
2026-01-20|MMM|02|M Corp|20.0|21.0|19.0|20.5|2000|20.0"""

        test_file = tmp_path / "test"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file)

        assert list(df["symbol"]) == ["AAA", "MMM", "ZZZ"]

    def test_file_not_found(self, tmp_path):
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError):
            parse_market_summary(tmp_path / "nonexistent")


class TestValidateDateFormat:
    """Tests for date format validation."""

    def test_valid_date_format(self):
        """Test that valid dates pass validation."""
        # Should not raise
        _validate_date_format("2026-01-20")
        _validate_date_format("2025-12-31")
        _validate_date_format("2000-01-01")

    def test_invalid_date_format_raises(self):
        """Test that invalid date formats raise ValueError."""
        with pytest.raises(ValueError):
            _validate_date_format("01-20-2026")  # Wrong order

        with pytest.raises(ValueError):
            _validate_date_format("2026/01/20")  # Wrong separator

        with pytest.raises(ValueError):
            _validate_date_format("20260120")  # No separators

        with pytest.raises(ValueError):
            _validate_date_format("2026-1-20")  # Missing leading zero


class TestMarketSummaryIntegration:
    """Integration tests for market summary module."""

    def test_full_parsing_workflow(self, tmp_path):
        """Test complete parsing workflow with realistic data."""
        # Simulate realistic PSX market summary format
        lines = [
            "2026-01-20|ABOT|11|ABBOTT LAB|665.01|666.00|655.00|659.95|11800|665.01",
            "2026-01-20|ABL|22|ALLIED BANK|97.50|98.00|96.50|97.25|150000|97.50",
            "2026-01-20|ACPL|33|ATTOCK CEM|185.00|188.00|183.00|186.50|25000|185.00",
            "2026-01-20|AGTL|44|AL-GHAZI|575.00|580.00|570.00|577.50|5000|575.00",
            "2026-01-20|AIRLINK|55|AIR LINK|35.50|36.00|35.00|35.75|500000|35.50",
        ]
        sample_data = "\n".join(lines)

        test_file = tmp_path / "2026-01-20"
        test_file.write_text(sample_data)

        df = parse_market_summary(test_file, expected_date="2026-01-20")

        # Verify all rows parsed
        assert len(df) == 5

        # Verify specific values
        abot = df[df["symbol"] == "ABOT"].iloc[0]
        assert abot["company_name"] == "ABBOTT LAB"
        assert abot["open"] == 665.01
        assert abot["high"] == 666.00
        assert abot["low"] == 655.00
        assert abot["close"] == 659.95
        assert abot["volume"] == 11800

        # Verify sorting (alphabetically: ABL < ABOT < ACPL < AGTL < AIRLINK)
        assert df.iloc[0]["symbol"] == "ABL"
        assert df.iloc[-1]["symbol"] == "AIRLINK"


class TestFetchDay:
    """Tests for fetch_day function."""

    def test_skips_existing_csv(self, tmp_path):
        """Test that existing CSV is skipped without force flag."""
        # Create existing CSV
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        csv_file = csv_dir / "2025-01-15.csv"
        csv_file.write_text("symbol,sector_code\nTEST,01")

        result = fetch_day(
            date(2025, 1, 15),
            out_dir=tmp_path,
            force=False,
        )

        assert result["status"] == "skipped"
        assert result["date"] == "2025-01-15"
        assert result["csv_path"] == str(csv_file)

    def test_accepts_string_date(self, tmp_path):
        """Test that string date is accepted."""
        # Create existing CSV
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        csv_file = csv_dir / "2025-01-15.csv"
        csv_file.write_text("symbol,sector_code\nTEST,01\nABC,02")

        result = fetch_day(
            "2025-01-15",  # String instead of date object
            out_dir=tmp_path,
            force=False,
        )

        assert result["status"] == "skipped"
        assert result["row_count"] == 2


class TestFetchRange:
    """Tests for fetch_range function."""

    @patch("pakfindata.sources.market_summary.fetch_day")
    @patch("pakfindata.sources.market_summary.create_session")
    def test_iterates_over_dates(self, mock_session, mock_fetch_day):
        """Test that fetch_range iterates over correct dates."""
        mock_session.return_value = MagicMock()
        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "ok",
            "csv_path": "/tmp/test.csv",
            "raw_path": None,
            "extracted_path": None,
            "row_count": 100,
            "message": None,
        }

        # Wed Jan 15 to Fri Jan 17, 2025 (3 weekdays)
        results = list(fetch_range(
            date(2025, 1, 15),
            date(2025, 1, 17),
            skip_weekends=True,
        ))

        assert len(results) == 3
        assert mock_fetch_day.call_count == 3

    @patch("pakfindata.sources.market_summary.fetch_day")
    @patch("pakfindata.sources.market_summary.create_session")
    def test_skips_weekends(self, mock_session, mock_fetch_day):
        """Test that weekends are skipped by default."""
        mock_session.return_value = MagicMock()
        mock_fetch_day.return_value = {
            "date": "2025-01-17",
            "status": "ok",
            "csv_path": "/tmp/test.csv",
            "raw_path": None,
            "extracted_path": None,
            "row_count": 100,
            "message": None,
        }

        # Fri Jan 17 to Mon Jan 20, 2025
        results = list(fetch_range(
            date(2025, 1, 17),
            date(2025, 1, 20),
            skip_weekends=True,
        ))

        # Should only call for Fri (17) and Mon (20), skipping Sat (18) and Sun (19)
        assert len(results) == 2


class TestFetchRangeSummary:
    """Tests for fetch_range_summary function."""

    @patch("pakfindata.sources.market_summary.fetch_range")
    def test_aggregates_results(self, mock_fetch_range):
        """Test that fetch_range_summary aggregates results correctly."""
        mock_fetch_range.return_value = iter([
            {"date": "2025-01-15", "status": "ok", "message": None},
            {"date": "2025-01-16", "status": "skipped", "message": None},
            {"date": "2025-01-17", "status": "missing", "message": None},
            {"date": "2025-01-20", "status": "failed", "message": "Test error"},
        ])

        summary = fetch_range_summary(
            date(2025, 1, 15),
            date(2025, 1, 20),
        )

        assert summary["start"] == "2025-01-15"
        assert summary["end"] == "2025-01-20"
        assert summary["total"] == 4
        assert summary["ok"] == 1
        assert summary["skipped"] == 1
        assert summary["missing"] == 1
        assert len(summary["failed"]) == 1
        assert summary["failed"][0]["date"] == "2025-01-20"
        assert summary["failed"][0]["message"] == "Test error"

    @patch("pakfindata.sources.market_summary.fetch_range")
    def test_empty_range(self, mock_fetch_range):
        """Test with no dates (e.g., weekend-only range with skip_weekends)."""
        mock_fetch_range.return_value = iter([])

        summary = fetch_range_summary(
            date(2025, 1, 18),  # Saturday
            date(2025, 1, 19),  # Sunday
        )

        assert summary["total"] == 0
        assert summary["ok"] == 0
        assert summary["skipped"] == 0
        assert summary["missing"] == 0
        assert len(summary["failed"]) == 0
