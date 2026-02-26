"""
API Client for PakFinData Backend.

Provides a Python client for the FastAPI backend.
Used by Streamlit frontend to communicate with the API.
"""

import os
from typing import Optional
from dataclasses import dataclass

import requests


# Default API URL - can be overridden via environment variable
DEFAULT_API_URL = os.environ.get("PSX_API_URL", "http://localhost:8000")


@dataclass
class EODStats:
    """EOD table statistics."""
    total_rows: int
    total_dates: int
    total_symbols: int
    min_date: Optional[str]
    max_date: Optional[str]
    by_source: dict
    by_processname: dict


@dataclass
class TaskStatus:
    """Status of a background task."""
    task_id: str
    task_type: str
    status: str
    progress: float
    progress_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    pid: Optional[int] = None
    result: Optional[dict] = None
    error: Optional[str] = None


@dataclass
class LoadResult:
    """Result of loading a single date."""
    date: str
    status: str
    rows: int
    source: str
    message: Optional[str] = None


class APIClient:
    """Client for PakFinData API."""

    def __init__(self, base_url: str = DEFAULT_API_URL, timeout: int = 30):
        """
        Initialize API client.

        Args:
            base_url: Base URL of the API server.
            timeout: Request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make HTTP request to API."""
        url = f"{self.base_url}{path}"
        kwargs.setdefault("timeout", self.timeout)

        try:
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError:
            raise APIConnectionError(f"Cannot connect to API at {self.base_url}")
        except requests.exceptions.Timeout:
            raise APITimeoutError(f"Request timed out after {self.timeout}s")
        except requests.exceptions.HTTPError as e:
            raise APIHTTPError(f"HTTP error: {e.response.status_code} - {e.response.text}")

    def health_check(self) -> bool:
        """Check if API is healthy."""
        try:
            result = self._request("GET", "/health")
            return result.get("status") == "healthy"
        except Exception:
            return False

    # =========================================================================
    # EOD Endpoints
    # =========================================================================

    def get_eod_stats(self) -> EODStats:
        """Get EOD table statistics."""
        data = self._request("GET", "/api/eod/stats")
        return EODStats(
            total_rows=data["total_rows"],
            total_dates=data["total_dates"],
            total_symbols=data["total_symbols"],
            min_date=data.get("min_date"),
            max_date=data.get("max_date"),
            by_source=data.get("by_source", {}),
            by_processname=data.get("by_processname", {}),
        )

    def list_csv_files(
        self,
        not_loaded_only: bool = False,
        limit: int = 100,
    ) -> dict:
        """
        List available CSV files.

        Args:
            not_loaded_only: Only return files not in DB.
            limit: Max files to return.

        Returns:
            Dict with total_csv_files, total_in_db, total_not_loaded, files.
        """
        params = {
            "not_loaded_only": not_loaded_only,
            "limit": limit,
        }
        return self._request("GET", "/api/eod/files", params=params)

    def load_dates(self, dates: list[str], force: bool = False) -> dict:
        """
        Load specific dates into eod_ohlcv table.

        Args:
            dates: List of date strings (YYYY-MM-DD).
            force: If True, overwrite existing data.

        Returns:
            Dict with ok_count, total_rows, results.
        """
        payload = {"dates": dates, "force": force}
        return self._request("POST", "/api/eod/load", json=payload)

    def get_date_info(self, date_str: str) -> dict:
        """
        Get information about a specific date.

        Args:
            date_str: Date string (YYYY-MM-DD).

        Returns:
            Dict with date info (csv_exists, in_db, db_row_count, etc.)
        """
        return self._request("GET", f"/api/eod/date/{date_str}")

    # =========================================================================
    # Task Endpoints
    # =========================================================================

    def start_load_task(
        self,
        start_date: str,
        end_date: str,
        skip_weekends: bool = True,
        force: bool = False,
        auto_download: bool = True,
    ) -> dict:
        """
        Start a background EOD load task.

        Args:
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            skip_weekends: Skip Saturdays and Sundays.
            force: Force re-download even if files exist.
            auto_download: Download files if not present.

        Returns:
            Dict with task_id, status, message.
        """
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "skip_weekends": skip_weekends,
            "force": force,
            "auto_download": auto_download,
        }
        return self._request("POST", "/api/tasks/start-load", json=payload)

    def get_task_status(self, task_id: str) -> TaskStatus:
        """
        Get status of a background task.

        Args:
            task_id: Task ID.

        Returns:
            TaskStatus object.
        """
        data = self._request("GET", f"/api/tasks/status/{task_id}")
        return TaskStatus(
            task_id=data["task_id"],
            task_type=data["task_type"],
            status=data["status"],
            progress=data["progress"],
            progress_message=data.get("progress_message"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            pid=data.get("pid"),
            result=data.get("result"),
            error=data.get("error"),
        )

    def stop_task(self, task_id: str) -> dict:
        """
        Stop a running background task.

        Args:
            task_id: Task ID.

        Returns:
            Dict with message.
        """
        return self._request("POST", f"/api/tasks/stop/{task_id}")

    def list_tasks(self, limit: int = 20) -> list[dict]:
        """
        List recent tasks.

        Args:
            limit: Max tasks to return.

        Returns:
            List of task summaries.
        """
        data = self._request("GET", "/api/tasks/list", params={"limit": limit})
        return data.get("tasks", [])


# =============================================================================
# Exceptions
# =============================================================================

class APIError(Exception):
    """Base exception for API errors."""
    pass


class APIConnectionError(APIError):
    """Cannot connect to API server."""
    pass


class APITimeoutError(APIError):
    """Request timed out."""
    pass


class APIHTTPError(APIError):
    """HTTP error from API."""
    pass


# =============================================================================
# Singleton Instance
# =============================================================================

_client: Optional[APIClient] = None


def get_client(base_url: str = DEFAULT_API_URL) -> APIClient:
    """Get or create singleton API client."""
    global _client
    if _client is None or _client.base_url != base_url:
        _client = APIClient(base_url=base_url)
    return _client


def is_api_available(base_url: str = DEFAULT_API_URL, timeout: int = 2) -> bool:
    """Check if PakFinData API server is available.

    Args:
        base_url: API server URL.
        timeout: Quick timeout for availability check (default 2 seconds).

    Returns:
        True if our PakFinData API is responding, False otherwise.
    """
    try:
        # Create a temporary client with short timeout for quick check
        temp_client = APIClient(base_url=base_url, timeout=timeout)
        # Check health endpoint first
        if not temp_client.health_check():
            return False
        # Also verify the root endpoint returns our API name
        result = temp_client._request("GET", "/")
        return result.get("name") == "PakFinData API"
    except Exception:
        return False
