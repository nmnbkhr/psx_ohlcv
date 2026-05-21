# pakfindata-worker — systemd user service

Phase 1 Milestone 1.4 introduced the worker process and the `jobs`
queue. This runbook covers install, inspect, restart, and recovery for
the worker.

## Architecture

```
            ┌────────────────────────────────────────────────┐
            │ Client (UI, cron, curl, …)                     │
            │   POST /v1/jobs/{job_type}  body={params,...}  │
            └─────────────────────┬──────────────────────────┘
                                  │
              FastAPI (8001)      ▼  pakfindata.db.jobs.enqueue_job
            ┌──────────────────────────────────────────────┐
            │  jobs table  (status='pending', …)           │
            └──────────────────────────────────────────────┘
                                  │
                                  │  every 2s poll
                                  ▼
      ┌─────────────────────────────────────────────────────┐
      │ pakfindata-worker.service                           │
      │   while True:                                       │
      │     job = claim_next_job(worker_pid)                │
      │     handler = REGISTRY[job.job_type]                │
      │     result = handler(**job.params)                  │
      │     finish_job(job.id, status='ok', result=result)  │
      └─────────────────────────────────────────────────────┘
```

Single process. Single thread. Writes always via
[`pakfindata.db.safe_writer`](../../src/pakfindata/db/safe_writer.py).

## Install

```bash
mkdir -p ~/.config/systemd/user
cp ~/projects/pakfindata/deploy/systemd/pakfindata-worker.service \
   ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now pakfindata-worker.service
```

The API service (`pakfindata-api.service`) must already be installed —
the worker unit declares `After=pakfindata-api.service` /
`PartOf=pakfindata-api.service`, so stopping the API also stops the
worker.

## Verify

```bash
# Service is up
systemctl --user is-active pakfindata-worker.service     # → active

# Worker handlers registered
journalctl --user -u pakfindata-worker.service -n 5 --no-pager -o cat \
    | jq -r 'select(.message | startswith("worker starting"))'
# → {"message":"worker starting pid=… handlers=['ping']", …}

# Submit a ping and watch it run end-to-end
TOKEN=$(grep PAKFINDATA_API_TOKEN ~/.config/pakfindata/api.env | cut -d= -f2)
RESP=$(curl -sS -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -X POST "http://127.0.0.1:8001/v1/jobs/ping" \
    -d '{"params": {"sleep_seconds": 2, "message": "smoke"}}')
JID=$(echo "$RESP" | python -c "import json,sys; print(json.load(sys.stdin)['job_id'])")
sleep 5
curl -sS -H "Authorization: Bearer $TOKEN" \
    "http://127.0.0.1:8001/v1/jobs/$JID" | python -m json.tool
# Expect: status=ok, result={echo: smoke, slept_s: 2.0}, duration_ms ≈ 2000-2500
```

## Restart after code changes

```bash
systemctl --user restart pakfindata-worker.service
systemctl --user status pakfindata-worker.service --no-pager -l
```

Workers don't auto-reload. Restart on every handler change.

## Stop / disable

```bash
# One-shot stop
systemctl --user stop pakfindata-worker.service

# Persistent disable across reboots
systemctl --user disable pakfindata-worker.service
```

Note: stopping the API service (`pakfindata-api.service`) also stops
the worker because of `PartOf=`. Conversely, stopping the worker on
its own leaves the API running but no jobs will execute.

## Logs

Every line is a JSON object — pipe through `jq`:

```bash
journalctl --user -u pakfindata-worker.service -o cat -n 50 | jq -r .

# Just terminal job states
journalctl --user -u pakfindata-worker.service -o cat --since "1 hour ago" \
    | jq 'select(.message | test("status=(ok|failed)"))'
```

## Stale-job sweep

Worker hits `sweep_stale_jobs()` at startup. Marks every `running`
job whose `worker_pid` is gone as `failed` with
`error="worker died before completion"`. Required because SIGKILL
(or kernel OOM, or host reboot) leaves orphan `running` rows that
otherwise stay forever.

Manual sweep:

```bash
~/miniforge3/envs/psx/bin/python -m pakfindata.worker.sweep
# → "swept 0 stale jobs"
```

## SafeWriter contention with daily_sync.sh

Daily cron at 03:45 PKT (`scripts/daily_sync.sh`) holds the writer
lock for ~19 min during full E2E. During that window, worker
`claim_next_job` will block waiting for the lock — the worker logs a
warning and backs off for `BUSY_BACKOFF_SEC` (5s). Jobs enqueued
during the cron window queue up and run once the cron finishes.

This is acceptable for Phase 1. Phase 2 may add a "skip-during-cron"
flag if it becomes a problem.

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `systemctl --user start` → "Failed to start" | Unit file path typo or missing api.env | `systemctl --user status pakfindata-worker -l` reads the error |
| Worker keeps crashing right after startup | Schema bootstrap failed; check first log line | `journalctl … --since '5 minutes ago'`; if DB path is wrong, fix `PAKFINDATA_DB_PATH` in api.env |
| `worker_pid` field is null on every job | Old worker version (Phase 0); upgrade | Pull, restart service |
| Jobs stuck in `running` after worker restart | SIGKILL / host reboot orphaned them | Sweep auto-runs at startup — they should flip to `failed` within a few seconds |
| Many jobs in `pending` during 03:45 PKT cron | Worker waiting on daily_sync.sh writer lock | Expected; jobs drain once cron finishes (~19 min) |

## Coexistence with the API service

| | API | Worker |
|---|---|---|
| Port | 127.0.0.1:8001 | none — DB only |
| Auth | Bearer (token from `api.env`) | n/a — worker doesn't make HTTP calls |
| Writes | None (mode=ro reads) | All — via `safe_writer` |
| Restart trigger | code change to `api/*` | code change to `worker/*` or new handler |
| systemd unit | `pakfindata-api.service` | `pakfindata-worker.service` |
| Restart strategy | `on-failure` | `on-failure` |
| Stop coupling | independent | `PartOf=pakfindata-api.service` → stops with API |
