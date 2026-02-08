"""Tests for FastAPI endpoints."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Check if fastapi and httpx are available
pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from psx_ohlcv.api.main import app
from psx_ohlcv.api.client import APIClient, APIError, APIConnectionError


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    # Create minimal schema
    con.execute("""
        CREATE TABLE IF NOT EXISTS eod_ohlcv (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            prev_close REAL,
            sector_code TEXT,
            company_name TEXT,
            ingested_at TEXT,
            source TEXT,
            processname TEXT,
            UNIQUE(symbol, date)
        )
    """)

    # Insert test data
    test_data = [
        ("OGDC", "2024-01-15", 100.0, 105.0, 99.0, 104.0, 1000000, 100.0, "OGM", "Oil & Gas", "market_summary", "eodfile"),
        ("OGDC", "2024-01-16", 104.0, 108.0, 103.0, 107.0, 1200000, 104.0, "OGM", "Oil & Gas", "market_summary", "eodfile"),
        ("HBL", "2024-01-15", 50.0, 52.0, 49.0, 51.0, 500000, 50.0, "BNK", "HBL", "market_summary", "eodfile"),
        ("HBL", "2024-01-16", 51.0, 53.0, 50.0, 52.0, 600000, 51.0, "market_summary", "eodfile", "per_symbol_api", "per_symbol_api"),
    ]

    for row in test_data:
        con.execute("""
            INSERT INTO eod_ohlcv
            (symbol, date, open, high, low, close, volume, prev_close, sector_code, company_name, source, processname)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    con.commit()
    con.close()

    yield db_path

    # Cleanup
    db_path.unlink(missing_ok=True)


# =============================================================================
# API Endpoint Tests
# =============================================================================

class TestHealthEndpoint:
    """Tests for health check endpoint."""

    def test_health_check(self, client):
        """Health endpoint should return healthy status."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_root_endpoint(self, client):
        """Root endpoint should return API info."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data


class TestEODStatsEndpoint:
    """Tests for EOD stats endpoint."""

    def test_get_stats(self, client, temp_db):
        """Get EOD stats should return statistics."""
        with patch("psx_ohlcv.api.routers.eod.get_db_path", return_value=temp_db):
            response = client.get("/api/eod/stats")
            assert response.status_code == 200
            data = response.json()
            assert "total_rows" in data
            assert "total_dates" in data
            assert "total_symbols" in data


class TestEODFilesEndpoint:
    """Tests for EOD files listing endpoint."""

    def test_list_files(self, client, temp_db):
        """List files endpoint should return file info."""
        with patch("psx_ohlcv.api.routers.eod.get_db_path", return_value=temp_db):
            with patch("psx_ohlcv.api.routers.eod.DATA_ROOT", Path("/tmp/test_data")):
                response = client.get("/api/eod/files")
                assert response.status_code == 200
                data = response.json()
                assert "total_csv_files" in data
                assert "files" in data


class TestTasksEndpoint:
    """Tests for background tasks endpoints."""

    def test_list_tasks(self, client):
        """List tasks should return empty list initially."""
        response = client.get("/api/tasks/list")
        assert response.status_code == 200
        data = response.json()
        assert "tasks" in data
        assert isinstance(data["tasks"], list)

    def test_get_nonexistent_task(self, client):
        """Getting non-existent task should return 404."""
        response = client.get("/api/tasks/status/nonexistent-task-id")
        assert response.status_code == 404


# =============================================================================
# API Client Tests
# =============================================================================

class TestAPIClient:
    """Tests for API client."""

    def test_client_initialization(self):
        """Client should initialize with default URL."""
        client = APIClient()
        assert client.base_url == "http://localhost:8000"
        assert client.timeout == 30

    def test_client_custom_url(self):
        """Client should accept custom URL."""
        client = APIClient(base_url="http://custom:9000")
        assert client.base_url == "http://custom:9000"

    def test_health_check_connection_error(self):
        """Health check should return False on connection error."""
        client = APIClient(base_url="http://nonexistent:9999")
        assert client.health_check() is False

    def test_get_eod_stats_with_mock(self):
        """Get EOD stats should parse response correctly."""
        client = APIClient()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "total_rows": 1000,
            "total_dates": 10,
            "total_symbols": 100,
            "min_date": "2024-01-01",
            "max_date": "2024-01-10",
            "by_source": {"market_summary": 900, "per_symbol_api": 100},
            "by_processname": {"eodfile": 900, "per_symbol_api": 100},
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_response):
            stats = client.get_eod_stats()
            assert stats.total_rows == 1000
            assert stats.total_dates == 10
            assert stats.total_symbols == 100
            assert stats.min_date == "2024-01-01"
            assert stats.max_date == "2024-01-10"

    def test_list_csv_files_with_mock(self):
        """List CSV files should parse response correctly."""
        client = APIClient()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "total_csv_files": 50,
            "total_in_db": 45,
            "total_not_loaded": 5,
            "files": [
                {"date": "2024-01-15", "source": "market_summary", "exists": True, "in_db": True},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_response):
            result = client.list_csv_files()
            assert result["total_csv_files"] == 50
            assert len(result["files"]) == 1

    def test_load_dates_with_mock(self):
        """Load dates should send correct request."""
        client = APIClient()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "ok_count": 2,
            "total_rows": 1500,
            "results": [
                {"date": "2024-01-15", "status": "ok", "rows": 750, "source": "market_summary"},
                {"date": "2024-01-16", "status": "ok", "rows": 750, "source": "market_summary"},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_response) as mock_request:
            result = client.load_dates(["2024-01-15", "2024-01-16"], force=True)
            assert result["ok_count"] == 2
            assert result["total_rows"] == 1500

            # Verify request was made correctly
            call_args = mock_request.call_args
            assert call_args[0][0] == "POST"
            assert "/api/eod/load" in call_args[0][1]

    def test_start_load_task_with_mock(self):
        """Start load task should return task info."""
        client = APIClient()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "task_id": "eod-load-20240115-123456",
            "status": "started",
            "message": "Task started",
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_response):
            result = client.start_load_task(
                start_date="2024-01-01",
                end_date="2024-01-15",
                skip_weekends=True,
                force=False,
                auto_download=True,
            )
            assert "task_id" in result
            assert result["status"] == "started"

    def test_get_task_status_with_mock(self):
        """Get task status should parse response correctly."""
        client = APIClient()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "task_id": "eod-load-20240115-123456",
            "task_type": "eod_load",
            "status": "running",
            "progress": 50.0,
            "progress_message": "Processing 2024-01-08",
            "started_at": "2024-01-15T10:00:00",
            "completed_at": None,
            "pid": 12345,
            "result": None,
            "error": None,
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_response):
            status = client.get_task_status("eod-load-20240115-123456")
            assert status.task_id == "eod-load-20240115-123456"
            assert status.status == "running"
            assert status.progress == 50.0

    def test_list_tasks_with_mock(self):
        """List tasks should return list of tasks."""
        client = APIClient()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "tasks": [
                {
                    "task_id": "eod-load-20240115-123456",
                    "task_type": "eod_load",
                    "status": "completed",
                    "progress": 100,
                    "started_at": "2024-01-15T10:00:00",
                    "completed_at": "2024-01-15T10:05:00",
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_response):
            tasks = client.list_tasks()
            assert len(tasks) == 1
            assert tasks[0]["status"] == "completed"


class TestAPIClientErrors:
    """Tests for API client error handling."""

    def test_connection_error(self):
        """Connection error should raise APIConnectionError."""
        import requests
        client = APIClient(base_url="http://nonexistent:9999", timeout=1)

        with patch.object(
            client._session,
            "request",
            side_effect=requests.exceptions.ConnectionError("Connection refused")
        ):
            with pytest.raises(APIConnectionError):
                client.get_eod_stats()

    def test_timeout_error(self):
        """Timeout should raise APITimeoutError."""
        import requests
        from psx_ohlcv.api.client import APITimeoutError

        client = APIClient(timeout=1)

        with patch.object(
            client._session,
            "request",
            side_effect=requests.exceptions.Timeout("Request timed out")
        ):
            with pytest.raises(APITimeoutError):
                client.get_eod_stats()

    def test_http_error(self):
        """HTTP error should raise APIHTTPError."""
        import requests
        from psx_ohlcv.api.client import APIHTTPError

        client = APIClient()

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )

        with patch.object(client._session, "request", return_value=mock_response):
            with pytest.raises(APIHTTPError):
                client.get_eod_stats()


# =============================================================================
# Integration Tests (require running API)
# =============================================================================

@pytest.mark.integration
class TestAPIIntegration:
    """Integration tests that require running API server.

    Run with: pytest -m integration tests/test_api.py

    These tests are skipped by default unless API server is running.
    """

    @pytest.fixture(autouse=True)
    def check_api_available(self):
        """Skip test if API is not available."""
        from psx_ohlcv.api.client import is_api_available
        if not is_api_available():
            pytest.skip("API server not running at localhost:8000")

    def test_full_workflow(self):
        """Test complete workflow: stats -> load -> verify."""
        from psx_ohlcv.api.client import get_client

        client = get_client()

        # Get initial stats
        stats = client.get_eod_stats()
        assert stats.total_rows >= 0

        # List files
        files = client.list_csv_files(limit=10)
        assert "files" in files
