"""Tests for market summary tracking table and retry logic."""

import sqlite3
from unittest.mock import patch

import pytest

from pakfindata.sources.market_summary import (
    fetch_day_with_tracking,
    get_download_record,
    get_failed_dates,
    get_missing_dates,
    init_market_summary_tracking,
    upsert_download_record,
)


@pytest.fixture
def con() -> sqlite3.Connection:
    """Create in-memory database with tracking table."""
    conn = sqlite3.connect(":memory:")
    init_market_summary_tracking(conn)
    return conn


class TestTrackingTable:
    """Tests for tracking table operations."""

    def test_init_creates_table(self, con):
        """Test that init_market_summary_tracking creates the table."""
        cur = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='downloaded_market_summary_dates'"
        )
        assert cur.fetchone() is not None

    def test_upsert_inserts_new_record(self, con):
        """Test that upsert_download_record inserts a new record."""
        upsert_download_record(
            con,
            "2025-01-15",
            "ok",
            csv_path="/data/csv/2025-01-15.csv",
            row_count=100,
            message="Downloaded 100 records",
        )

        record = get_download_record(con, "2025-01-15")
        assert record is not None
        assert record["date"] == "2025-01-15"
        assert record["status"] == "ok"
        assert record["csv_path"] == "/data/csv/2025-01-15.csv"
        assert record["row_count"] == 100
        assert record["message"] == "Downloaded 100 records"

    def test_upsert_updates_existing_record(self, con):
        """Test that upsert_download_record updates an existing record."""
        # First insert
        upsert_download_record(con, "2025-01-15", "failed", message="Error 1")

        # Second insert should update
        upsert_download_record(
            con,
            "2025-01-15",
            "ok",
            csv_path="/data/csv/2025-01-15.csv",
            row_count=100,
        )

        record = get_download_record(con, "2025-01-15")
        assert record["status"] == "ok"
        assert record["csv_path"] == "/data/csv/2025-01-15.csv"

    def test_get_download_record_returns_none_for_missing(self, con):
        """Test that get_download_record returns None for missing date."""
        record = get_download_record(con, "2025-01-15")
        assert record is None

    def test_get_failed_dates(self, con):
        """Test that get_failed_dates returns only failed dates."""
        upsert_download_record(con, "2025-01-15", "ok")
        upsert_download_record(con, "2025-01-16", "failed")
        upsert_download_record(con, "2025-01-17", "missing")
        upsert_download_record(con, "2025-01-18", "failed")

        failed = get_failed_dates(con)
        assert sorted(failed) == ["2025-01-16", "2025-01-18"]

    def test_get_missing_dates(self, con):
        """Test that get_missing_dates returns only missing dates."""
        upsert_download_record(con, "2025-01-15", "ok")
        upsert_download_record(con, "2025-01-16", "missing")
        upsert_download_record(con, "2025-01-17", "failed")
        upsert_download_record(con, "2025-01-18", "missing")

        missing = get_missing_dates(con)
        assert sorted(missing) == ["2025-01-16", "2025-01-18"]

    def test_get_all_tracking_records(self, con):
        """Test that get_all_tracking_records returns all records."""
        from pakfindata.sources.market_summary import get_all_tracking_records

        upsert_download_record(con, "2025-01-15", "ok", row_count=100)
        upsert_download_record(con, "2025-01-16", "failed", message="Error")
        upsert_download_record(con, "2025-01-17", "missing")

        records = get_all_tracking_records(con)
        assert len(records) == 3
        # Most recent first (by date DESC)
        assert records[0]["date"] == "2025-01-17"
        assert records[1]["date"] == "2025-01-16"
        assert records[2]["date"] == "2025-01-15"

    def test_get_tracking_stats(self, con):
        """Test that get_tracking_stats returns correct summary."""
        from pakfindata.sources.market_summary import get_tracking_stats

        upsert_download_record(con, "2025-01-15", "ok", row_count=100)
        upsert_download_record(con, "2025-01-16", "ok", row_count=150)
        upsert_download_record(con, "2025-01-17", "failed")
        upsert_download_record(con, "2025-01-18", "missing")

        stats = get_tracking_stats(con)
        assert stats["total"] == 4
        assert stats["ok"] == 2
        assert stats["failed"] == 1
        assert stats["missing"] == 1
        assert stats["total_rows"] == 250
        assert stats["min_date"] == "2025-01-15"
        assert stats["max_date"] == "2025-01-18"


class TestFirstAttemptInserts:
    """Test that first download attempt inserts a tracking record."""

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_first_attempt_ok_inserts_record(self, mock_fetch_day, con, tmp_path):
        """Test that successful first attempt inserts tracking record."""
        # Create a fake CSV file
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        csv_path = csv_dir / "2025-01-15.csv"
        csv_path.write_text("symbol,close\nTEST,100")

        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "ok",
            "csv_path": str(csv_path),
            "raw_path": None,
            "extracted_path": None,
            "row_count": 1,
            "message": "Downloaded 1 records",
        }

        result = fetch_day_with_tracking(con, "2025-01-15", out_dir=tmp_path)

        assert result["status"] == "ok"

        # Verify tracking record was created
        record = get_download_record(con, "2025-01-15")
        assert record is not None
        assert record["status"] == "ok"
        assert record["row_count"] == 1


class TestSecondAttemptSkips:
    """Test that second attempt without force is skipped."""

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_second_attempt_without_force_skips(self, mock_fetch_day, con, tmp_path):
        """Test that second attempt without force flag skips download."""
        # Insert initial OK record
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        csv_path = csv_dir / "2025-01-15.csv"
        csv_path.write_text("symbol,close\nTEST,100")

        upsert_download_record(
            con,
            "2025-01-15",
            "ok",
            csv_path=str(csv_path),
            row_count=1,
        )

        # Second attempt should skip (not call fetch_day)
        result = fetch_day_with_tracking(
            con, "2025-01-15", out_dir=tmp_path, force=False
        )

        assert result["status"] == "skipped"
        mock_fetch_day.assert_not_called()


class TestMissingDateHandling:
    """Test that missing dates are handled correctly."""

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_missing_date_sets_status_missing(self, mock_fetch_day, con, tmp_path):
        """Test that 404 response sets status to 'missing'."""
        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "missing",
            "csv_path": None,
            "raw_path": None,
            "extracted_path": None,
            "row_count": 0,
            "message": "No data available (404)",
        }

        result = fetch_day_with_tracking(con, "2025-01-15", out_dir=tmp_path)

        assert result["status"] == "missing"

        # Verify tracking record
        record = get_download_record(con, "2025-01-15")
        assert record is not None
        assert record["status"] == "missing"

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_missing_date_does_not_crash(self, mock_fetch_day, con, tmp_path):
        """Test that missing date does not cause crash."""
        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "missing",
            "csv_path": None,
            "raw_path": None,
            "extracted_path": None,
            "row_count": 0,
            "message": "No data available (404)",
        }

        # Should not raise
        result = fetch_day_with_tracking(con, "2025-01-15", out_dir=tmp_path)
        assert result is not None


class TestRetryFlags:
    """Test that retry flags allow re-attempt of failed/missing dates."""

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_retry_failed_allows_reattempt(self, mock_fetch_day, con, tmp_path):
        """Test that --retry-failed flag allows re-attempt."""
        # Insert failed record
        upsert_download_record(
            con,
            "2025-01-15",
            "failed",
            message="Previous error",
        )

        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "ok",
            "csv_path": "/tmp/2025-01-15.csv",
            "raw_path": None,
            "extracted_path": None,
            "row_count": 100,
            "message": "Downloaded 100 records",
        }

        # Without retry_failed, should skip
        result_skip = fetch_day_with_tracking(
            con, "2025-01-15", out_dir=tmp_path, retry_failed=False
        )
        assert result_skip["status"] == "skipped"

        # With retry_failed, should attempt download
        result_retry = fetch_day_with_tracking(
            con, "2025-01-15", out_dir=tmp_path, retry_failed=True
        )
        assert result_retry["status"] == "ok"
        mock_fetch_day.assert_called_once()

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_retry_missing_allows_reattempt(self, mock_fetch_day, con, tmp_path):
        """Test that --retry-missing flag allows re-attempt."""
        # Insert missing record
        upsert_download_record(
            con,
            "2025-01-15",
            "missing",
            message="No data available (404)",
        )

        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "ok",
            "csv_path": "/tmp/2025-01-15.csv",
            "raw_path": None,
            "extracted_path": None,
            "row_count": 100,
            "message": "Downloaded 100 records",
        }

        # Without retry_missing, should skip
        result_skip = fetch_day_with_tracking(
            con, "2025-01-15", out_dir=tmp_path, retry_missing=False
        )
        assert result_skip["status"] == "skipped"

        # With retry_missing, should attempt download
        result_retry = fetch_day_with_tracking(
            con, "2025-01-15", out_dir=tmp_path, retry_missing=True
        )
        assert result_retry["status"] == "ok"
        mock_fetch_day.assert_called_once()

    @patch("pakfindata.sources.market_summary.fetch_day")
    def test_force_overrides_all_statuses(self, mock_fetch_day, con, tmp_path):
        """Test that --force flag overrides all previous statuses."""
        # Insert OK record with existing CSV
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        csv_path = csv_dir / "2025-01-15.csv"
        csv_path.write_text("symbol,close\nTEST,100")

        upsert_download_record(
            con,
            "2025-01-15",
            "ok",
            csv_path=str(csv_path),
            row_count=1,
        )

        mock_fetch_day.return_value = {
            "date": "2025-01-15",
            "status": "ok",
            "csv_path": str(csv_path),
            "raw_path": None,
            "extracted_path": None,
            "row_count": 200,
            "message": "Downloaded 200 records",
        }

        # With force=True, should re-download even if already ok
        fetch_day_with_tracking(
            con, "2025-01-15", out_dir=tmp_path, force=True
        )

        mock_fetch_day.assert_called_once()
        # The record should be updated
        record = get_download_record(con, "2025-01-15")
        assert record["row_count"] == 200
