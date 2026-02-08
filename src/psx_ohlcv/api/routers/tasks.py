"""
Background Tasks API endpoints.

Provides endpoints for:
- Starting background load/sync tasks
- Checking task status
- Stopping running tasks
"""

import json
import os
import signal
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ...config import DATA_ROOT

router = APIRouter()


# =============================================================================
# Task Status File Paths
# =============================================================================

TASKS_DIR = DATA_ROOT / "services" / "tasks"
TASKS_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Pydantic Models
# =============================================================================

class TaskStatus(BaseModel):
    """Status of a background task."""
    task_id: str
    task_type: str
    status: str  # pending, running, completed, failed, cancelled
    progress: float  # 0-100
    progress_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    pid: Optional[int] = None
    result: Optional[dict] = None
    error: Optional[str] = None


class StartLoadTaskRequest(BaseModel):
    """Request to start EOD load task."""
    start_date: str
    end_date: str
    skip_weekends: bool = True
    force: bool = False
    auto_download: bool = True


class StartLoadTaskResponse(BaseModel):
    """Response from starting load task."""
    task_id: str
    status: str
    message: str


# =============================================================================
# Helper Functions
# =============================================================================

def get_task_file(task_id: str) -> Path:
    """Get path to task status file."""
    return TASKS_DIR / f"{task_id}.json"


def read_task_status(task_id: str) -> Optional[dict]:
    """Read task status from file."""
    task_file = get_task_file(task_id)
    if task_file.exists():
        try:
            with open(task_file) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def write_task_status(task_id: str, status: dict):
    """Write task status to file."""
    task_file = get_task_file(task_id)
    with open(task_file, "w") as f:
        json.dump(status, f, indent=2)


def is_process_running(pid: int) -> bool:
    """Check if process with given PID is running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def generate_task_id() -> str:
    """Generate unique task ID."""
    import uuid
    return f"eod-load-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/start-load", response_model=StartLoadTaskResponse)
def start_load_task(request: StartLoadTaskRequest):
    """
    Start a background EOD load task.

    Downloads and loads EOD data for the specified date range.
    Returns immediately with a task_id to check status.
    """
    task_id = generate_task_id()

    # Create initial status
    status = {
        "task_id": task_id,
        "task_type": "eod_load",
        "status": "pending",
        "progress": 0.0,
        "progress_message": "Starting...",
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "pid": None,
        "params": {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "skip_weekends": request.skip_weekends,
            "force": request.force,
            "auto_download": request.auto_download,
        },
        "result": None,
        "error": None,
    }
    write_task_status(task_id, status)

    # Fork to run in background
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait briefly for child to start
            time.sleep(0.5)
            return StartLoadTaskResponse(
                task_id=task_id,
                status="started",
                message=f"Task started with ID: {task_id}",
            )
    except OSError as e:
        status["status"] = "failed"
        status["error"] = f"Fork failed: {e}"
        write_task_status(task_id, status)
        raise HTTPException(status_code=500, detail=f"Failed to start task: {e}")

    # Child process - run the task
    try:
        os.setsid()  # Create new session

        # Second fork to prevent zombie
        pid = os.fork()
        if pid > 0:
            os._exit(0)

        # Run the actual task
        _run_eod_load_task(task_id, request)

    except Exception as e:
        status = read_task_status(task_id) or status
        status["status"] = "failed"
        status["error"] = str(e)
        status["completed_at"] = datetime.now().isoformat()
        write_task_status(task_id, status)
    finally:
        os._exit(0)


def _run_eod_load_task(task_id: str, request: StartLoadTaskRequest):
    """Run the EOD load task (called in background process)."""
    import sqlite3
    from datetime import datetime as dt

    from ...config import DATA_ROOT, get_db_path
    from ...db import ingest_market_summary_csv
    from ...sources.market_summary import fetch_day_with_tracking, init_market_summary_tracking
    from ...range_utils import iter_dates

    status = read_task_status(task_id) or {}
    status["status"] = "running"
    status["pid"] = os.getpid()
    write_task_status(task_id, status)

    # Parse dates
    start_date = dt.strptime(request.start_date, "%Y-%m-%d").date()
    end_date = dt.strptime(request.end_date, "%Y-%m-%d").date()

    # Get dates to process
    dates = list(iter_dates(start_date, end_date, skip_weekends=request.skip_weekends))
    total_dates = len(dates)

    if total_dates == 0:
        status["status"] = "completed"
        status["progress"] = 100.0
        status["progress_message"] = "No dates to process"
        status["completed_at"] = datetime.now().isoformat()
        status["result"] = {"ok": 0, "skipped": 0, "missing": 0, "failed": 0, "rows": 0}
        write_task_status(task_id, status)
        return

    # Connect to database
    db_path = get_db_path()
    con = sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = sqlite3.Row

    csv_dir = DATA_ROOT / "market_summary" / "csv"

    # Initialize tracking
    init_market_summary_tracking(con)

    ok_count = 0
    skip_count = 0
    missing_count = 0
    fail_count = 0
    total_rows = 0

    try:
        for i, d in enumerate(dates):
            date_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)

            # Update progress
            progress = ((i + 1) / total_dates) * 100
            status["progress"] = progress
            status["progress_message"] = f"Processing {date_str} ({i+1}/{total_dates})"
            write_task_status(task_id, status)

            try:
                if request.auto_download:
                    # Download and track
                    result = fetch_day_with_tracking(con, d, force=request.force)
                    dl_status = result["status"]

                    if dl_status == "ok":
                        ok_count += 1
                        # Ingest
                        if result.get("csv_path"):
                            try:
                                ingest_result = ingest_market_summary_csv(
                                    con, result["csv_path"],
                                    skip_existing=False,
                                    source="market_summary"
                                )
                                total_rows += ingest_result.get("rows_inserted", 0)
                            except Exception:
                                pass
                    elif dl_status == "skipped":
                        skip_count += 1
                        # Try to ingest existing CSV
                        csv_path = csv_dir / f"{date_str}.csv"
                        if csv_path.exists():
                            try:
                                ingest_result = ingest_market_summary_csv(
                                    con, str(csv_path),
                                    skip_existing=not request.force,
                                    source="market_summary"
                                )
                                if ingest_result.get("status") == "ok":
                                    total_rows += ingest_result.get("rows_inserted", 0)
                            except Exception:
                                pass
                    elif dl_status == "missing":
                        missing_count += 1
                    else:
                        fail_count += 1
                else:
                    # Just ingest from existing CSV
                    csv_path = csv_dir / f"{date_str}.csv"
                    if csv_path.exists():
                        ingest_result = ingest_market_summary_csv(
                            con, str(csv_path),
                            skip_existing=not request.force,
                            source="market_summary"
                        )
                        if ingest_result.get("status") == "ok":
                            ok_count += 1
                            total_rows += ingest_result.get("rows_inserted", 0)
                        else:
                            skip_count += 1
                    else:
                        missing_count += 1

            except Exception as e:
                fail_count += 1

        # Complete
        status["status"] = "completed"
        status["progress"] = 100.0
        status["progress_message"] = f"Completed: {ok_count} OK, {skip_count} skipped, {missing_count} missing, {fail_count} failed"
        status["completed_at"] = datetime.now().isoformat()
        status["result"] = {
            "ok": ok_count,
            "skipped": skip_count,
            "missing": missing_count,
            "failed": fail_count,
            "rows": total_rows,
        }
        write_task_status(task_id, status)

    except Exception as e:
        status["status"] = "failed"
        status["error"] = str(e)
        status["completed_at"] = datetime.now().isoformat()
        write_task_status(task_id, status)

    finally:
        con.close()


@router.get("/status/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str):
    """Get status of a background task."""
    status = read_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    # Verify process is still running
    if status.get("status") == "running" and status.get("pid"):
        if not is_process_running(status["pid"]):
            status["status"] = "failed"
            status["error"] = "Process terminated unexpectedly"
            status["completed_at"] = datetime.now().isoformat()
            write_task_status(task_id, status)

    return TaskStatus(**status)


@router.post("/stop/{task_id}")
def stop_task(task_id: str):
    """Stop a running background task."""
    status = read_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    if status.get("status") != "running":
        return {"message": f"Task is not running (status: {status.get('status')})"}

    pid = status.get("pid")
    if not pid:
        return {"message": "Task has no PID"}

    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.5)

        if is_process_running(pid):
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.5)

        status["status"] = "cancelled"
        status["completed_at"] = datetime.now().isoformat()
        status["progress_message"] = "Cancelled by user"
        write_task_status(task_id, status)

        return {"message": f"Task stopped: {task_id}"}

    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop task: {e}")


@router.get("/list")
def list_tasks(limit: int = 20):
    """List recent tasks."""
    tasks = []
    task_files = sorted(TASKS_DIR.glob("*.json"), reverse=True)

    for task_file in task_files[:limit]:
        try:
            with open(task_file) as f:
                task = json.load(f)
                tasks.append({
                    "task_id": task.get("task_id"),
                    "task_type": task.get("task_type"),
                    "status": task.get("status"),
                    "progress": task.get("progress", 0),
                    "started_at": task.get("started_at"),
                    "completed_at": task.get("completed_at"),
                })
        except (json.JSONDecodeError, IOError):
            continue

    return {"tasks": tasks}
