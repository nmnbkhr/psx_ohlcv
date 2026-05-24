"""Phase 2.B observability namespace.

Primitives for inspecting the platform's operational state without
modifying it. Read-only by design; any write-side operations belong
in `pakfindata.worker` or the specific source module.

Modules:
  stuck_jobs  — detect long-running rows across worker queues and
                *_sync_runs tables. Time-threshold based; complement
                to `pakfindata.worker.sweep` (which is PID-based and
                actively marks dead jobs as failed at worker startup).
"""
