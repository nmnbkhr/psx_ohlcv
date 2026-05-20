"""
API Client for pakfindata Backend.

Provides a Python client for the FastAPI backend.
Used by Streamlit frontend to communicate with the API.

Phase 1.1 (1.1.3): auto-injects Bearer token from
~/.config/pakfindata/api.env when present. Backwards-compatible with
the Phase-0 unauthenticated mode — if no token file exists, requests
go out without an Authorization header (legacy /api/* on Phase-0
deployments will continue to accept them).

Note: this is the per-endpoint typed client. The lower-level "smart
client" at pakfindata.api_client (DO NOT TOUCH) auto-detects API vs
direct-SQLite mode and is what most Streamlit pages import.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

import requests


# Default API URL - PSX_API_URL env var overrides. The api.env file
# sets PSX_API_URL=http://127.0.0.1:8001 once the user has bootstrapped
# Milestone 1.1; until then the legacy 8000 default applies for
# backwards compat with Phase-0 deployments.
DEFAULT_API_URL = os.environ.get("PSX_API_URL", "http://localhost:8001")


_API_ENV_FILE = Path.home() / ".config" / "pakfindata" / "api.env"


def _read_api_token() -> Optional[str]:
    """Read PAKFINDATA_API_TOKEN from ~/.config/pakfindata/api.env.

    Returns None if the file doesn't exist or the token isn't set —
    in which case requests go out unauthenticated (legacy mode).
    """
    # Env var wins
    token = os.environ.get("PAKFINDATA_API_TOKEN")
    if token:
        return token.strip()
    if not _API_ENV_FILE.exists():
        return None
    try:
        for line in _API_ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            if key.strip() == "PAKFINDATA_API_TOKEN":
                return value.strip().strip('"').strip("'") or None
    except OSError:
        return None
    return None


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

    def __init__(
        self,
        base_url: str = DEFAULT_API_URL,
        timeout: int = 30,
        token: Optional[str] = None,
    ):
        """
        Initialize API client.

        Args:
            base_url: Base URL of the API server.
            timeout: Request timeout in seconds.
            token:   Bearer token. If None, auto-loaded from
                     ~/.config/pakfindata/api.env (or env var).
                     If still unresolved, requests go out without
                     an Authorization header — legacy Phase-0 mode.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.token = token if token is not None else _read_api_token()
        self._session = requests.Session()
        if self.token:
            self._session.headers["Authorization"] = f"Bearer {self.token}"

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
            raise APIHTTPError(
                f"HTTP error: {e.response.status_code} - {e.response.text}",
                status_code=e.response.status_code,
                body=e.response.text,
            )

    def get(self, path: str, params: dict | None = None):
        """Generic GET helper — returns the decoded JSON body.

        Used by the Streamlit-side wrapper (pakfindata.ui.api.client) to
        call /v1/* endpoints without per-endpoint shim methods. The
        return value is whatever the endpoint serializes — dict, list,
        list-of-dicts, etc.
        """
        return self._request("GET", path, params=params)

    def health_check(self) -> bool:
        """Check if API is healthy. Returns True iff /health returns ok-ish.

        Accepts both the Phase-0 status='healthy' and the Phase-1
        status='ok' / status='degraded' (the latter is still 'reachable'
        but signals a DB-side issue — return True so callers can still
        talk to the API; the detailed catalog_summary is available via
        `health()` below).
        """
        try:
            result = self._request("GET", "/health")
            return result.get("status") in ("ok", "healthy", "degraded")
        except Exception:
            return False

    def health(self) -> dict:
        """Return the full /health payload.

        Phase-1 format:
            {"status": "ok"|"degraded", "version": ..., "timestamp": ...,
             "db_path": ..., "db_status": ..., "catalog_summary": {...}}

        Phase-0 legacy format: {"status": "healthy"} (no catalog_summary).
        """
        return self._request("GET", "/health")

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
    """HTTP error from API.

    Carries ``status_code`` (int) and ``body`` (raw response text) so
    callers can distinguish 404 / 401 / 5xx without parsing the
    message string.
    """

    def __init__(self, message: str, status_code: int = 0, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


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
        # Also verify the root endpoint identifies as ours.
        # Phase-0 banner used "PakFinData API"; Phase-1 lowercased to
        # "pakfindata API". Accept either to keep callers happy across
        # the migration.
        result = temp_client._request("GET", "/")
        return result.get("name") in ("PakFinData API", "pakfindata API")
    except Exception:
        return False
