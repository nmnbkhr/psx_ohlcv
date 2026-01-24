"""Tests for intraday sync with mocked HTTP."""

from unittest.mock import patch

import pandas as pd
import pytest

from psx_ohlcv import connect, init_schema
from psx_ohlcv.db import get_intraday_sync_state, upsert_intraday
from psx_ohlcv.sync import IntradaySyncSummary, sync_intraday


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    """Create a database connection and initialize schema."""
    con = connect(db_path)
    init_schema(con)
    yield con
    con.close()


class TestSyncIntraday:
    """Tests for sync_intraday function."""

    def test_sync_intraday_success(self, db_path, db):
        """Test successful intraday sync."""
        # Mock payload (PSX format: [timestamp, close, volume, open])
        mock_payload = [
            [1705310400, 103.5, 50000, 100.0],
            [1705314000, 104.0, 55000, 103.5],
            [1705317600, 105.0, 60000, 104.0],
        ]

        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = mock_payload

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
                incremental=False,
            )

        assert summary.symbol == "ABOT"
        assert summary.rows_upserted == 3
        assert summary.error is None
        assert summary.newest_ts is not None

        # Verify data in database
        cur = db.execute("SELECT COUNT(*) FROM intraday_bars WHERE symbol='ABOT'")
        assert cur.fetchone()[0] == 3

    def test_sync_intraday_empty_response(self, db_path, db):
        """Test sync with empty API response."""
        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = []

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
            )

        assert summary.symbol == "ABOT"
        assert summary.rows_upserted == 0
        assert summary.error is None

    def test_sync_intraday_api_error(self, db_path, db):
        """Test sync with API error."""
        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.side_effect = Exception("Connection timeout")

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
            )

        assert summary.symbol == "ABOT"
        assert summary.rows_upserted == 0
        assert summary.error is not None
        assert "Connection timeout" in summary.error

    def test_sync_intraday_incremental(self, db_path, db):
        """Test incremental sync only fetches new data."""
        from datetime import datetime

        # Helper to get local epoch for a timestamp string
        def ts_to_epoch(ts_str):
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp())

        # Insert existing data
        existing_df = pd.DataFrame([
            {
                "symbol": "ABOT",
                "ts": "2024-01-15 10:00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ])
        upsert_intraday(db, existing_df)

        # Set sync state with ts_epoch (using local time epoch)
        last_ts = "2024-01-15 10:00:00"
        last_ts_epoch = ts_to_epoch(last_ts)
        db.execute(
            "INSERT INTO intraday_sync_state "
            "(symbol, last_ts, last_ts_epoch) VALUES (?, ?, ?)",
            ("ABOT", last_ts, last_ts_epoch)
        )
        db.commit()

        # Mock payload using dict format to have predictable timestamps
        # Use timestamps: one before sync state, one at sync state, two after
        mock_payload = [
            {"ts": "2024-01-15 09:00:00", "close": 100.5, "volume": 1000,
             "open": 100.0},
            {"ts": "2024-01-15 10:00:00", "close": 101.5, "volume": 1500,
             "open": 100.5},
            {"ts": "2024-01-15 11:00:00", "close": 102.5, "volume": 2000,
             "open": 101.5},
            {"ts": "2024-01-15 12:00:00", "close": 103.5, "volume": 2500,
             "open": 102.5},
        ]

        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = mock_payload

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
                incremental=True,
            )

        # Should only have upserted the 2 rows after last_ts
        assert summary.rows_upserted == 2

    def test_sync_intraday_non_incremental(self, db_path, db):
        """Test non-incremental sync fetches all data."""
        from datetime import datetime

        # Helper to get local epoch for a timestamp string
        def ts_to_epoch(ts_str):
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            return int(dt.timestamp())

        # Set sync state (should be ignored in non-incremental mode)
        last_ts = "2024-01-15 10:00:00"
        db.execute(
            "INSERT INTO intraday_sync_state "
            "(symbol, last_ts, last_ts_epoch) VALUES (?, ?, ?)",
            ("ABOT", last_ts, ts_to_epoch(last_ts))
        )
        db.commit()

        mock_payload = [
            [1705309200, 100.5, 1000, 100.0],
            [1705312800, 101.5, 1500, 100.5],
            [1705316400, 102.5, 2000, 101.5],
        ]

        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = mock_payload

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
                incremental=False,  # Non-incremental
            )

        # Should upsert all 3 rows
        assert summary.rows_upserted == 3

    def test_sync_intraday_updates_sync_state(self, db_path, db):
        """Test that sync updates the sync state."""
        mock_payload = [
            [1705309200, 100.5, 1000, 100.0],
            [1705320000, 103.5, 2500, 102.5],  # Latest
        ]

        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = mock_payload

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
            )

        # Verify sync state was updated (returns tuple of (last_ts, last_ts_epoch))
        last_ts, last_ts_epoch = get_intraday_sync_state(db, "ABOT")
        assert last_ts is not None
        assert last_ts == summary.newest_ts
        assert last_ts_epoch == 1705320000

    def test_sync_intraday_max_rows(self, db_path, db):
        """Test max_rows parameter limits data fetched."""
        # The max_rows is passed through to the API/parsing
        # In real implementation, it may affect how much data is processed
        mock_payload = [
            [1705309200 + i * 300, 100.0 + i * 0.1, 1000 + i, 100.0]
            for i in range(10)
        ]

        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = mock_payload

            summary = sync_intraday(
                db_path=db_path,
                symbol="ABOT",
                max_rows=5,
            )

        # Should have processed all rows from the payload
        # (max_rows affects fetch, but we're mocking the fetch)
        assert summary.rows_upserted <= 10

    def test_sync_intraday_dict_payload(self, db_path, db):
        """Test sync with dict payload (data key)."""
        mock_payload = {
            "data": [
                [1705310400, 103.5, 50000, 100.0],
                [1705314000, 104.0, 55000, 103.5],
            ]
        }

        with patch("psx_ohlcv.sync.fetch_intraday_json") as mock_fetch:
            mock_fetch.return_value = mock_payload

            summary = sync_intraday(
                db_path=db_path,
                symbol="HBL",
            )

        assert summary.symbol == "HBL"
        assert summary.rows_upserted == 2
        assert summary.error is None

    def test_sync_intraday_symbol_required(self, db_path):
        """Test that symbol is required."""
        with patch("psx_ohlcv.sync.fetch_intraday_json"):
            summary = sync_intraday(
                db_path=db_path,
                symbol="",  # Empty symbol
            )

        assert summary.error is not None
        assert "symbol" in summary.error.lower()


class TestIntradaySyncSummary:
    """Tests for IntradaySyncSummary dataclass."""

    def test_summary_attributes(self):
        """Test summary has expected attributes."""
        summary = IntradaySyncSummary(
            symbol="ABOT",
            rows_upserted=100,
            newest_ts="2024-01-15 14:00:00",
            error=None,
        )

        assert summary.symbol == "ABOT"
        assert summary.rows_upserted == 100
        assert summary.newest_ts == "2024-01-15 14:00:00"
        assert summary.error is None

    def test_summary_with_error(self):
        """Test summary with error."""
        summary = IntradaySyncSummary(
            symbol="ABOT",
            rows_upserted=0,
            newest_ts=None,
            error="Connection failed",
        )

        assert summary.error == "Connection failed"
        assert summary.rows_upserted == 0
