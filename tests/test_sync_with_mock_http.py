"""Integration tests for sync with mocked HTTP."""

from unittest.mock import MagicMock, patch

import pytest

from psx_ohlcv import connect, init_schema, upsert_symbols
from psx_ohlcv.sync import sync_all

# Sample EOD responses for two symbols
HBL_EOD_RESPONSE = [
    {
        "date": "2024-01-15",
        "open": 150.0,
        "high": 155.0,
        "low": 148.0,
        "close": 153.0,
        "volume": 100000,
    },
    {
        "date": "2024-01-16",
        "open": 153.0,
        "high": 158.0,
        "low": 152.0,
        "close": 156.0,
        "volume": 120000,
    },
]

OGDC_EOD_RESPONSE = {
    "data": [
        {
            "date": "2024-01-15",
            "open": 200.0,
            "high": 205.0,
            "low": 198.0,
            "close": 203.0,
            "volume": 50000,
        },
        {
            "date": "2024-01-16",
            "open": 203.0,
            "high": 210.0,
            "low": 201.0,
            "close": 208.0,
            "volume": 60000,
        },
        {
            "date": "2024-01-17",
            "open": 208.0,
            "high": 212.0,
            "low": 205.0,
            "close": 210.0,
            "volume": 55000,
        },
    ]
}


@pytest.fixture
def db():
    """Create an in-memory database with symbols for testing."""
    con = connect(":memory:")
    init_schema(con)
    # Pre-populate symbols
    upsert_symbols(con, [{"symbol": "HBL"}, {"symbol": "OGDC"}])
    yield con
    con.close()


@pytest.fixture
def mock_session():
    """Create a mock session that returns predetermined JSON."""
    session = MagicMock()

    def mock_get(url, **kwargs):
        response = MagicMock()
        response.raise_for_status = MagicMock()

        if "HBL" in url:
            response.json.return_value = HBL_EOD_RESPONSE
            response.text = str(HBL_EOD_RESPONSE)
        elif "OGDC" in url:
            response.json.return_value = OGDC_EOD_RESPONSE
            response.text = str(OGDC_EOD_RESPONSE)
        else:
            response.json.return_value = []
            response.text = "[]"

        return response

    session.get = mock_get
    return session


class TestSyncAllWithMockHttp:
    """Integration tests for sync_all with mocked HTTP."""

    def test_sync_all_with_explicit_symbols(self, mock_session):
        """Should sync explicit list of symbols."""
        with patch("psx_ohlcv.sync.create_session", return_value=mock_session):
            with patch("psx_ohlcv.http.time.sleep"):  # Skip polite delays
                summary = sync_all(
                    db_path=":memory:",
                    symbols_list=["HBL", "OGDC"],
                    session=mock_session,
                )

        assert summary.symbols_total == 2
        assert summary.symbols_ok == 2
        assert summary.symbols_failed == 0
        assert summary.rows_upserted == 5  # 2 HBL + 3 OGDC

    def test_sync_updates_database(self, mock_session, tmp_path):
        """Should update database with synced data."""
        db_path = tmp_path / "test.db"

        # Setup database with symbols
        con = connect(db_path)
        init_schema(con)
        upsert_symbols(con, [{"symbol": "HBL"}, {"symbol": "OGDC"}])
        con.close()

        with patch("psx_ohlcv.http.time.sleep"):
            sync_all(
                db_path=db_path,
                symbols_list=["HBL", "OGDC"],
                session=mock_session,
            )

        # Verify database contents
        con = connect(db_path)

        # Check EOD rows
        cur = con.execute("SELECT COUNT(*) as cnt FROM eod_ohlcv")
        assert cur.fetchone()["cnt"] == 5

        # Check HBL rows
        cur = con.execute("SELECT * FROM eod_ohlcv WHERE symbol='HBL' ORDER BY date")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0]["date"] == "2024-01-15"
        assert rows[0]["close"] == 153.0

        # Check OGDC rows
        cur = con.execute("SELECT * FROM eod_ohlcv WHERE symbol='OGDC' ORDER BY date")
        rows = cur.fetchall()
        assert len(rows) == 3

        con.close()

    def test_sync_records_run(self, mock_session, tmp_path):
        """Should record sync run in database."""
        db_path = tmp_path / "test.db"

        con = connect(db_path)
        init_schema(con)
        upsert_symbols(con, [{"symbol": "HBL"}])
        con.close()

        with patch("psx_ohlcv.http.time.sleep"):
            summary = sync_all(
                db_path=db_path,
                symbols_list=["HBL"],
                session=mock_session,
            )

        # Verify sync_runs table
        con = connect(db_path)
        cur = con.execute("SELECT * FROM sync_runs WHERE run_id = ?", (summary.run_id,))
        row = cur.fetchone()

        assert row is not None
        assert row["symbols_total"] == 1
        assert row["symbols_ok"] == 1
        assert row["symbols_failed"] == 0
        assert row["rows_upserted"] == 2
        assert row["ended_at"] is not None

        con.close()

    def test_sync_handles_failures(self, tmp_path):
        """Should handle and record failures."""
        db_path = tmp_path / "test.db"

        con = connect(db_path)
        init_schema(con)
        upsert_symbols(con, [{"symbol": "HBL"}, {"symbol": "BADONE"}])
        con.close()

        # Create mock that fails for BADONE
        mock_session = MagicMock()

        def mock_get(url, **kwargs):
            response = MagicMock()

            if "BADONE" in url:
                from requests import HTTPError

                response.raise_for_status.side_effect = HTTPError("404 Not Found")
            else:
                response.raise_for_status = MagicMock()
                response.json.return_value = HBL_EOD_RESPONSE

            return response

        mock_session.get = mock_get

        with patch("psx_ohlcv.http.time.sleep"):
            summary = sync_all(
                db_path=db_path,
                symbols_list=["HBL", "BADONE"],
                session=mock_session,
            )

        assert summary.symbols_ok == 1
        assert summary.symbols_failed == 1
        assert len(summary.failures) == 1
        assert summary.failures[0]["symbol"] == "BADONE"
        assert summary.failures[0]["error_type"] == "HTTP_ERROR"

        # Verify failure recorded in database
        con = connect(db_path)
        cur = con.execute(
            "SELECT * FROM sync_failures WHERE run_id = ?", (summary.run_id,)
        )
        failures = cur.fetchall()
        assert len(failures) == 1
        assert failures[0]["symbol"] == "BADONE"
        con.close()

    def test_sync_from_db_symbols(self, mock_session, tmp_path):
        """Should sync symbols from database when no explicit list."""
        db_path = tmp_path / "test.db"

        con = connect(db_path)
        init_schema(con)
        upsert_symbols(con, [{"symbol": "HBL"}, {"symbol": "OGDC"}])
        con.close()

        with patch("psx_ohlcv.http.time.sleep"):
            summary = sync_all(
                db_path=db_path,
                session=mock_session,
            )

        assert summary.symbols_total == 2
        assert summary.symbols_ok == 2
        assert summary.rows_upserted == 5

    def test_sync_with_limit(self, mock_session, tmp_path):
        """Should respect limit_symbols parameter."""
        db_path = tmp_path / "test.db"

        con = connect(db_path)
        init_schema(con)
        upsert_symbols(
            con, [{"symbol": "AAA"}, {"symbol": "BBB"}, {"symbol": "CCC"}]
        )
        con.close()

        with patch("psx_ohlcv.http.time.sleep"):
            summary = sync_all(
                db_path=db_path,
                limit_symbols=2,
                session=mock_session,
            )

        # Should only sync 2 symbols (AAA, BBB alphabetically)
        assert summary.symbols_total == 2

    def test_sync_empty_symbols_list(self, mock_session, tmp_path):
        """Should handle empty symbols gracefully."""
        db_path = tmp_path / "test.db"

        con = connect(db_path)
        init_schema(con)
        con.close()

        with patch("psx_ohlcv.http.time.sleep"):
            summary = sync_all(
                db_path=db_path,
                session=mock_session,
            )

        assert summary.symbols_total == 0
        assert summary.symbols_ok == 0
        assert summary.rows_upserted == 0

    def test_sync_upserts_existing_data(self, mock_session, tmp_path):
        """Should upsert (update) existing EOD data."""
        db_path = tmp_path / "test.db"

        con = connect(db_path)
        init_schema(con)
        upsert_symbols(con, [{"symbol": "HBL"}])

        # Insert initial data
        con.execute(
            """
            INSERT INTO eod_ohlcv
                (symbol, date, open, high, low, close, volume, ingested_at)
            VALUES ('HBL', '2024-01-15', 100, 100, 100, 100, 1000, '2024-01-01')
            """
        )
        con.commit()
        con.close()

        with patch("psx_ohlcv.http.time.sleep"):
            sync_all(
                db_path=db_path,
                symbols_list=["HBL"],
                session=mock_session,
            )

        # per_symbol_api uses INSERT OR IGNORE — existing row is NOT overwritten
        con = connect(db_path)
        cur = con.execute(
            "SELECT close FROM eod_ohlcv WHERE symbol='HBL' AND date='2024-01-15'"
        )
        row = cur.fetchone()
        assert row["close"] == 100.0  # Unchanged: INSERT OR IGNORE keeps original

        # Should have 2 rows (1 original kept + 1 new date inserted)
        cur = con.execute("SELECT COUNT(*) as cnt FROM eod_ohlcv WHERE symbol='HBL'")
        assert cur.fetchone()["cnt"] == 2

        con.close()
