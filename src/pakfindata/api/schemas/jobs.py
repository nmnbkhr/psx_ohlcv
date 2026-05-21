"""Pydantic models for /v1/jobs endpoints.

Job lifecycle:
    pending → running → ok | failed | cancelled

Submission (``POST /v1/jobs/{job_type}``) takes only the params dict
plus optional priority + notes. The server fills in everything else.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


JobStatus = Literal["pending", "running", "ok", "failed", "cancelled"]


class JobSubmission(BaseModel):
    """Body of ``POST /v1/jobs/{job_type}``."""

    params: dict[str, Any] = Field(default_factory=dict)
    priority: int = 100
    notes: Optional[str] = None


class JobResponse(BaseModel):
    """Returned by ``POST /v1/jobs/{job_type}`` — the enqueue receipt."""

    job_id: int
    status: JobStatus
    job_type: str
    enqueued_at: str
    message: str = "queued"


class JobDetail(BaseModel):
    """Full row from the ``jobs`` table — used by GET endpoints."""

    id: int
    job_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    status: JobStatus
    enqueued_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    error_detail: Optional[str] = None
    worker_pid: Optional[int] = None
    priority: int = 100
    source: str = "api"
    notes: Optional[str] = None


class CancelResponse(BaseModel):
    """Body of ``POST /v1/jobs/{job_id}/cancel`` on success."""

    job_id: int
    status: Literal["cancelled"] = "cancelled"
