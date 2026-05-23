# pakfindata-api — systemd user service

Phase 1 Milestone 1.1 introduced the pakfindata FastAPI service. This
runbook documents how to install, run, inspect, and troubleshoot it.

## Prerequisites

- Conda env `psx` exists at `~/miniforge3/envs/psx/` with
  `fastapi`, `uvicorn`, and `httpx` installed (already in
  `pyproject.toml`).
- `~/.config/pakfindata/api.env` exists with a 32+ byte
  `PAKFINDATA_API_TOKEN`. Generate via:
  ```bash
  mkdir -p ~/.config/pakfindata
  chmod 700 ~/.config/pakfindata
  python -c "import secrets; print(secrets.token_urlsafe(32))" \
      > ~/.config/pakfindata/api_token
  chmod 600 ~/.config/pakfindata/api_token
  cat > ~/.config/pakfindata/api.env <<EOF
  PAKFINDATA_API_TOKEN=$(cat ~/.config/pakfindata/api_token)
  PAKFINDATA_API_HOST=127.0.0.1
  PAKFINDATA_API_PORT=8001
  PAKFINDATA_DB_PATH=$HOME/psxdata_rescue/psx.sqlite
  PAKFINDATA_LOG_LEVEL=INFO
  PSX_API_URL=http://127.0.0.1:8001
  EOF
  chmod 600 ~/.config/pakfindata/api.env
  ```

## Install

```bash
mkdir -p ~/.config/systemd/user
cp ~/projects/pakfindata/deploy/systemd/pakfindata-api.service \
   ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now pakfindata-api.service

# Optional: keep the service running across logout sessions.
loginctl enable-linger $USER
```

## Verify

```bash
# Service active?
systemctl --user is-active pakfindata-api.service
# → active

# Recent logs (structured JSON, one object per line)
journalctl --user -u pakfindata-api -n 30 --no-pager

# Live tail
journalctl --user -u pakfindata-api -f

# Health endpoint (public; no auth)
curl -s http://127.0.0.1:8001/health | python -m json.tool

# Auth check — must 401 without token
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8001/api/eod/

# Auth check — 4xx with valid token (whatever the underlying route returns)
TOKEN=$(grep PAKFINDATA_API_TOKEN ~/.config/pakfindata/api.env | cut -d= -f2)
curl -s -o /dev/null -w "%{http_code}\n" \
    -H "Authorization: Bearer $TOKEN" \
    http://127.0.0.1:8001/api/eod/
```

## Restart after code changes

```bash
systemctl --user restart pakfindata-api.service
systemctl --user status pakfindata-api.service --no-pager -l
```

uvicorn's `--reload` flag is NOT enabled in production — restart on
deploy.

## Disable / stop

```bash
# One-shot stop (returns after next start trigger)
systemctl --user stop pakfindata-api.service

# Persistent disable across reboots
systemctl --user disable pakfindata-api.service

# Re-enable later
systemctl --user enable --now pakfindata-api.service
```

## Logs

Every log line is a JSON object — pipe through `jq` for readability:

```bash
journalctl --user -u pakfindata-api -o cat -n 200 | jq -r .
```

Common queries:

```bash
# All errors in last hour
journalctl --user -u pakfindata-api --since "1 hour ago" -o cat \
    | jq -r 'select(.level == "ERROR")'

# Just the request paths
journalctl --user -u pakfindata-api -o cat -n 50 \
    | jq -r 'select(.logger == "uvicorn.access") | .message'

# Anything from the auth middleware
journalctl --user -u pakfindata-api -o cat \
    | jq 'select(.logger | startswith("pakfindata.api.auth"))'
```

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `systemctl --user start` → "Failed to start" | Unit file has a typo or refers to wrong path | `systemctl --user status pakfindata-api -l` and read the error |
| Service crashes immediately, log shows `RuntimeError: PAKFINDATA_API_TOKEN is required` | `~/.config/pakfindata/api.env` missing or unreadable | Recreate per "Prerequisites"; `chmod 600` the file |
| `address already in use` on port 8001 | Another service holds the port | `ss -tlnp | grep 8001` to find the squatter |
| `uvicorn: command not found` | Wrong conda env path in `ExecStart` | Verify `~/miniforge3/envs/psx/bin/uvicorn` exists; update the unit if env name differs |
| All requests return 401 even with a token | `api.env` has a different token than what the client uses | Compare token strings; remember the env file is the source of truth |
| Service won't auto-restart | `Restart=on-failure` only fires on non-zero exit; SIGKILL by user counts as zero exit | Use `systemctl --user kill -s SIGKILL pakfindata-api` to force a failure scenario for testing |

## Cron-environment test

`cron` and `systemd` both run with minimal `PATH` / env. The unit file
sources `api.env`; verify it works under truly minimal env:

```bash
env -i HOME="$HOME" PATH="/usr/bin:/bin" systemctl --user start \
    pakfindata-api.service
sleep 2
curl -s http://127.0.0.1:8001/health | python -m json.tool
```

## Coexistence with Streamlit

Streamlit runs on 8501; the API on 8001. They share no state. Either
can be restarted independently. Phase 1.3 starts wiring Streamlit
pages to call the API instead of opening SQLite directly; until then
the smart client (`pakfindata.api_client`) silently falls back to
direct-DB mode.

## Token rotation (manual)

```bash
# Generate new token
NEW=$(python -c "import secrets; print(secrets.token_urlsafe(32))")

# Update both files
echo "$NEW" > ~/.config/pakfindata/api_token
sed -i "s/^PAKFINDATA_API_TOKEN=.*/PAKFINDATA_API_TOKEN=$NEW/" \
    ~/.config/pakfindata/api.env

# Bounce the service
systemctl --user restart pakfindata-api.service

# Verify
TOKEN=$(grep PAKFINDATA_API_TOKEN ~/.config/pakfindata/api.env | cut -d= -f2)
curl -s -o /dev/null -w "%{http_code}\n" \
    -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8001/api/eod/
# Expect 4xx (auth passed), not 401.
```

Phase 3 will replace this with per-user tokens + rotation tooling.
