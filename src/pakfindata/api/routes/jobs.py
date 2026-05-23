"""Job submission and inspection endpoints under /v1/jobs.

Routes (all Bearer-auth gated via the global middleware):

    POST   /v1/jobs/{job_type}        — enqueue; 202 + {job_id, status}
    GET    /v1/jobs/{job_id}          — full job detail; 404 if unknown
    GET    /v1/jobs                   — filtered listing
    POST   /v1/jobs/{job_id}/cancel   — cancel pending; 409 if running/finished

The worker (``pakfindata-worker.service``) picks pending jobs up and
runs them. This route just records intent and reports state.

job_type values come from :func:`pakfindata.worker.registry.known_types`.
Unknown types return 400 with the registered list — so a client can
discover what's submittable without scanning the OpenAPI schema.
"""

from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, Path, Query

from pakfindata.api.schemas.jobs import (
    CancelResponse,
    JobDetail,
    JobResponse,
    JobSubmission,
    JobStatus,
)
from pakfindata.db.jobs import (
    cancel_pending,
    enqueue_job,
    get_job,
    list_jobs,
)
from pakfindata.worker.registry import known_types

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post(
    "/{job_type}",
    response_model=JobResponse,
    status_code=202,
)
def submit_job(
    body: JobSubmission,
    job_type: Annotated[str, Path(description="Registered worker handler key")],
    source: Annotated[
        str,
        Query(
            description=(
                "Submission origin tag stored on the jobs row. Defaults to "
                "'api' for UI / ad-hoc submissions; cron's daily_sync.sh "
                "appends ?source=cron so WHERE source='cron' queries in "
                "Jobs Monitor distinguish scheduled from manual runs."
            ),
        ),
    ] = "api",
) -> JobResponse:
    """Enqueue a job; returns 202 with the new job_id."""
    types = known_types()
    if job_type not in types:
        raise HTTPException(
            status_code=400,
            detail=f"unknown job_type {job_type!r}; registered: {types}",
        )
    job_id = enqueue_job(
        job_type=job_type,
        params=body.params,
        priority=body.priority,
        source=source,
        notes=body.notes,
    )
    job = get_job(job_id)
    if job is None:  # extremely unlikely race; defensive
        raise HTTPException(
            status_code=500,
            detail=f"job_id {job_id} not found immediately after enqueue",
        )
    return JobResponse(
        job_id=job_id,
        status=job["status"],
        job_type=job["job_type"],
        enqueued_at=job["enqueued_at"],
    )


@router.get("/{job_id}", response_model=JobDetail)
def get_job_status(
    job_id: Annotated[int, Path(ge=1, description="jobs.id")],
) -> JobDetail:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"job {job_id} not found")
    return JobDetail.model_validate(job)


@router.get("", response_model=list[JobDetail])
def list_recent_jobs(
    status: Annotated[
        Optional[JobStatus],
        Query(description="Filter by lifecycle status"),
    ] = None,
    job_type: Annotated[
        Optional[str], Query(description="Filter by registered handler key")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[JobDetail]:
    """Recent jobs, newest first; optional status / type filters."""
    rows = list_jobs(status=status, job_type=job_type, limit=limit)
    return [JobDetail.model_validate(r) for r in rows]


@router.post("/{job_id}/cancel", response_model=CancelResponse)
def cancel_job(
    job_id: Annotated[int, Path(ge=1)],
) -> CancelResponse:
    """Cancel a pending job. 404 if unknown; 409 if already running/finished."""
    if cancel_pending(job_id):
        return CancelResponse(job_id=job_id)
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"job {job_id} not found")
    raise HTTPException(
        status_code=409,
        detail=f"job {job_id} is {job['status']}; only 'pending' jobs can be cancelled",
    )
