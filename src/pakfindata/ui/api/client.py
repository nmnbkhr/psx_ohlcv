"""Streamlit-side API client wrapper.

Every Phase-1.3-migrated UI page calls into this module. Never imports
the underlying ``pakfindata.api.client.APIClient`` directly.

Responsibilities:
- Lazy-cache a single APIClient per Streamlit session via st.cache_resource
- Wrap each /v1 endpoint with @st.cache_data using a TTL tuned to how
  often the data changes
- Translate APIError into a Streamlit-friendly result (None on transport
  failure; empty list on 404 "no data" cases) so pages can render a
  graceful empty state instead of a stack trace
- Expose a top-of-page banner helper for API-down state

NOT this module's job:
- Business logic — that stays in pages
- Manual cache invalidation — Streamlit handles ttl
- Auth — the underlying APIClient handles token loading from api.env
"""

from __future__ import annotations

import logging
from datetime import date as _date
from typing import Any, Optional

import streamlit as st

from pakfindata.api.client import (
    APIClient,
    APIError,
    APIHTTPError,
    DEFAULT_API_URL,
)

logger = logging.getLogger(__name__)


@st.cache_resource
def _client() -> APIClient:
    """One client per Streamlit session.

    `cache_resource` (not `cache_data`) because the client is a
    stateful object (open requests.Session + token-loaded headers).
    """
    return APIClient(timeout=5)


def _safe_get(path: str, params: Optional[dict] = None, *, on_404=None):
    """Call APIClient.get with consistent error translation.

    Returns the JSON body on success. On any transport/auth/5xx error,
    returns None and logs a warning. On 404, returns ``on_404`` (defaults
    to None — caller can pass ``[]`` or ``{}`` to mean "valid empty").
    """
    try:
        return _client().get(path, params=params)
    except APIHTTPError as exc:
        if exc.status_code == 404:
            return on_404
        logger.warning("API %s failed: %s", path, exc)
        return None
    except APIError as exc:
        logger.warning("API %s failed: %s", path, exc)
        return None


# ── Health & banner ────────────────────────────────────────────────────────


def health() -> Optional[dict]:
    """Probe the API. No cache — we want to know NOW if it's down."""
    return _safe_get("/health")


def render_api_status_banner_if_down() -> bool:
    """Call at the top of each migrated page.

    Shows a red error banner if the API is unreachable. Returns True
    if API is up, False if down — pages can branch on this to render
    a minimal "service degraded" view instead of empty widgets.
    """
    if health() is None:
        st.error(
            "pakfindata API is unreachable. Data on this page may be "
            "missing or stale. Check service: "
            "`systemctl --user status pakfindata-api`"
        )
        return False
    return True


# ── Freshness ──────────────────────────────────────────────────────────────


@st.cache_data(ttl=30)
def get_freshness() -> Optional[list[dict]]:
    """All datasets' freshness, newest first."""
    return _safe_get("/v1/freshness")


@st.cache_data(ttl=30)
def get_dataset_freshness(domain: str) -> Optional[dict]:
    """One dataset's freshness row; None if domain unknown."""
    return _safe_get(f"/v1/freshness/{domain}")


def get_data_freshness_tuple(
    domain: str = "equity_eod",
) -> tuple[Optional[int], Optional[str]]:
    """Return (days_old, last_row_date) for backwards compatibility with
    pages that previously called the smart client's ``get_data_freshness``.

    Computes days_old by parsing ``last_row_date`` against today's date.
    Returns (None, None) on failure so caller can render a 'no data'
    badge.
    """
    row = get_dataset_freshness(domain)
    if not row or not row.get("last_row_date"):
        return (None, None)
    try:
        last = _date.fromisoformat(row["last_row_date"])
        days_old = (_date.today() - last).days
        return (days_old, row["last_row_date"])
    except (ValueError, TypeError):
        return (None, row.get("last_row_date"))


# ── EOD ────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=60)
def get_latest_eod(as_of: Optional[str] = None) -> Optional[list[dict]]:
    """All symbols, latest trading day (or override via ``as_of``)."""
    params = {"as_of": as_of} if as_of else None
    return _safe_get("/v1/eod/latest", params=params)


@st.cache_data(ttl=60)
def get_eod_for_date(date: str) -> Optional[list[dict]]:
    return _safe_get("/v1/eod", params={"date": date})


@st.cache_data(ttl=300)
def get_symbol_history(
    symbol: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> Optional[list[dict]]:
    """Symbol OHLCV history; default last 90 days. 404 returns []."""
    params: dict[str, Any] = {}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    return _safe_get(f"/v1/eod/{symbol}", params=params, on_404=[])


@st.cache_data(ttl=60)
def get_breadth(date: Optional[str] = None) -> Optional[dict]:
    """Advancers / decliners / unchanged for a date (latest by default)."""
    params = {"date": date} if date else None
    return _safe_get("/v1/eod/breadth", params=params)


# ── Indices ────────────────────────────────────────────────────────────────


@st.cache_data(ttl=60)
def get_all_indices() -> Optional[list[dict]]:
    """Latest snapshot for every index code (KSE100, KSE30, KMI30, …)."""
    return _safe_get("/v1/indices")


@st.cache_data(ttl=300)
def get_index_history(
    code: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> Optional[list[dict]]:
    params: dict[str, Any] = {}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    return _safe_get(f"/v1/indices/{code}", params=params, on_404=[])


@st.cache_data(ttl=600)
def get_index_constituents(code: str) -> Optional[list[dict]]:
    return _safe_get(f"/v1/indices/{code}/constituents", on_404=[])


# ── Market overview ────────────────────────────────────────────────────────


@st.cache_data(ttl=60)
def get_kse100_hero(as_of: Optional[str] = None) -> Optional[dict]:
    """Denormalized KSE-100 hero: quote + 52w + breadth."""
    params = {"as_of": as_of} if as_of else None
    return _safe_get("/v1/market/kse100", params=params)


@st.cache_data(ttl=60)
def get_top_gainers(limit: int = 10) -> Optional[list[dict]]:
    return _safe_get("/v1/market/top-gainers", params={"limit": limit})


@st.cache_data(ttl=60)
def get_top_losers(limit: int = 10) -> Optional[list[dict]]:
    return _safe_get("/v1/market/top-losers", params={"limit": limit})


@st.cache_data(ttl=60)
def get_volume_leaders(limit: int = 10) -> Optional[list[dict]]:
    return _safe_get("/v1/market/volume-leaders", params={"limit": limit})


@st.cache_data(ttl=60)
def get_value_leaders(limit: int = 10) -> Optional[list[dict]]:
    return _safe_get("/v1/market/value-leaders", params={"limit": limit})


@st.cache_data(ttl=300)
def get_52w_extremes(limit: int = 5) -> Optional[dict]:
    return _safe_get("/v1/market/52w-extremes", params={"limit": limit})


@st.cache_data(ttl=60)
def get_sector_leaderboard() -> Optional[list[dict]]:
    return _safe_get("/v1/market/sector-leaderboard")


@st.cache_data(ttl=60)
def get_change_distribution(date: Optional[str] = None) -> Optional[list[dict]]:
    params = {"date": date} if date else None
    return _safe_get("/v1/market/change-distribution", params=params)


@st.cache_data(ttl=120)
def get_announcements(limit: int = 8) -> Optional[list[dict]]:
    return _safe_get("/v1/market/announcements", params={"limit": limit})


@st.cache_data(ttl=60)
def get_market_analytics() -> Optional[dict]:
    """Latest snapshot from analytics_market_snapshot. None if no
    snapshot has been computed yet."""
    return _safe_get("/v1/market/analytics")


# ── Rates ──────────────────────────────────────────────────────────────────


@st.cache_data(ttl=60)
def get_rates_strip() -> Optional[dict]:
    """Macro rates strip — policy + KIBOR-3M + T-Bill-3M + PKRV-10Y + FX."""
    return _safe_get("/v1/rates/strip")


# ── Sync observability ─────────────────────────────────────────────────────


@st.cache_data(ttl=30)
def get_sync_runs(limit: int = 10) -> Optional[list[dict]]:
    """Recent sync_runs rows for the Dashboard footer widget."""
    return _safe_get("/v1/sync/runs", params={"limit": limit})


# ── Jobs queue ─────────────────────────────────────────────────────────────


@st.cache_data(ttl=3)
def get_jobs(
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    limit: int = 50,
) -> Optional[list[dict]]:
    """Recent jobs rows for the Jobs Monitor page.

    Very short TTL (3s) — job state changes quickly; the page also
    auto-refreshes every 5s so a 3s TTL keeps the table visibly live
    without hammering the API.
    """
    params: dict[str, Any] = {"limit": limit}
    if status:
        params["status"] = status
    if job_type:
        params["job_type"] = job_type
    return _safe_get("/v1/jobs", params=params)


@st.cache_data(ttl=3)
def get_job_detail(job_id: int) -> Optional[dict]:
    """Single job row by id; None on transport error / 404 / unknown."""
    return _safe_get(f"/v1/jobs/{job_id}")


def submit_job(
    job_type: str,
    params: Optional[dict] = None,
    priority: int = 100,
    notes: Optional[str] = None,
) -> Optional[dict]:
    """Enqueue a job. Returns ``{job_id, status, …}`` or None on failure.

    Not cached — every call enqueues a new row by definition.
    """
    body = {
        "params": params or {},
        "priority": priority,
    }
    if notes:
        body["notes"] = notes
    try:
        return _client().post(f"/v1/jobs/{job_type}", json=body)
    except APIError as exc:
        logger.warning("submit_job(%s) failed: %s", job_type, exc)
        return None


def cancel_job(job_id: int) -> Optional[dict]:
    """Cancel a pending job. Returns ``{job_id, status: "cancelled"}`` or
    None if already running/finished/unknown."""
    try:
        return _client().post(f"/v1/jobs/{job_id}/cancel")
    except APIError as exc:
        logger.warning("cancel_job(%d) failed: %s", job_id, exc)
        return None


def run_job_with_progress(
    job_type: str,
    params: Optional[dict] = None,
    *,
    spinner_text: str = "Running...",
    success_label: Optional[str] = None,
    poll_interval_s: float = 1.5,
    timeout_s: float = 180.0,
) -> Optional[dict]:
    """Submit a job, poll until terminal, show progress in Streamlit.

    Used by sync buttons that need worker dispatch + inline progress
    rendering. Returns the terminal job-detail dict on success/failure,
    or None if the submission itself failed (e.g. API down).

    The function clears ``st.cache_data`` on success so the next page
    render picks up freshly-synced data without waiting for TTL expiry.

    Args:
        job_type:      Registered handler key (e.g. "sync_indices").
        params:        Body params dict for ``POST /v1/jobs/{job_type}``.
        spinner_text:  Initial message shown while the job is pending.
        success_label: Optional override for the toast on success;
                       defaults to a short summary of result keys.
        poll_interval_s: Seconds between status polls.
        timeout_s:     Total wall-time budget before we give up.

    Returns:
        Terminal job-detail dict, or None if submission failed.
    """
    import time
    submission = submit_job(job_type, params=params or {})
    if submission is None:
        st.error(
            "Could not enqueue job — API unreachable or auth misconfigured."
        )
        return None
    job_id = submission["job_id"]

    progress = st.empty()
    progress.info(f"Job #{job_id} ({job_type}) queued — {spinner_text}")

    deadline = time.time() + timeout_s
    last_status: Optional[str] = None
    last_job: Optional[dict] = None
    while time.time() < deadline:
        time.sleep(poll_interval_s)
        job = get_job_detail(job_id)
        if job is None:
            continue
        last_job = job
        status = job["status"]
        if status != last_status:
            progress.info(f"Job #{job_id} ({job_type}) status: {status}")
            last_status = status
        if status in ("ok", "failed", "cancelled"):
            progress.empty()
            st.cache_data.clear()
            if status == "ok":
                if success_label:
                    st.toast(f"OK: {success_label}")
                else:
                    result = job.get("result") or {}
                    summary = ", ".join(
                        f"{k}={v}"
                        for k, v in result.items()
                        if k not in ("as_of",)
                    )
                    st.toast(f"Job #{job_id} done — {summary}")
            elif status == "failed":
                err = job.get("error") or "(no error message)"
                st.error(f"Job #{job_id} failed: {err}")
            else:  # cancelled
                st.warning(f"Job #{job_id} was cancelled.")
            return job

    progress.empty()
    st.warning(
        f"Job #{job_id} still running after {timeout_s}s — "
        "check the Jobs Monitor page for status."
    )
    return last_job


def get_fx_latest(source: str = "interbank") -> Optional[list[dict]]:
    """All currencies @ latest date for one FX source."""
    return _safe_get("/v1/fx/latest", params={"source": source})


def get_fx_latest_one(currency: str, source: str = "interbank") -> Optional[dict]:
    """Latest row for one currency from one source (404 → None)."""
    return _safe_get(
        f"/v1/fx/latest/{currency}", params={"source": source}, on_404=None
    )


def get_fx_history(
    currency: str, source: str = "interbank", limit: int = 500
) -> Optional[list[dict]]:
    """Date-desc history for one currency in one source."""
    return _safe_get(
        "/v1/fx/history",
        params={"currency": currency, "source": source, "limit": limit},
    )


def get_fx_ohlcv(pair: str, limit: int = 1000) -> Optional[list[dict]]:
    """``fx_ohlcv`` history for one pair (e.g. USD/PKR)."""
    return _safe_get(f"/v1/fx/ohlcv/{pair}", params={"limit": limit})


def get_fx_global_pairs() -> Optional[list[str]]:
    """Distinct global pairs in ``commodity_fx_rates``."""
    return _safe_get("/v1/fx/global-pairs")


def get_fx_global_history(pair: str, limit: int = 1000) -> Optional[list[dict]]:
    """``commodity_fx_rates`` history for one global pair."""
    return _safe_get(f"/v1/fx/global-history/{pair}", params={"limit": limit})


def get_fx_spread_heatmap(limit: int = 150) -> Optional[list[dict]]:
    """Interbank-vs-kerb spread bundle."""
    return _safe_get("/v1/fx/spread-heatmap", params={"limit": limit})


def get_fx_sync_runs(limit: int = 10) -> Optional[list[dict]]:
    """Recent ``fx_sync_runs`` rows."""
    return _safe_get("/v1/fx/sync-runs", params={"limit": limit})


def get_konia(limit: int = 1) -> Optional[list[dict]]:
    """Latest KONIA rows."""
    return _safe_get("/v1/rates/konia", params={"limit": limit})


def get_npc_rates(limit: int = 200) -> Optional[list[dict]]:
    """``npc_rates`` rows (carry rates)."""
    return _safe_get("/v1/rates/npc", params={"limit": limit})


def get_global_reference_rates(rate_names: Optional[str] = None) -> Optional[list[dict]]:
    """Latest row per rate_name from ``global_reference_rates``.

    ``rate_names``: optional comma-separated set, e.g. ``"SOFR,SONIA,EUSTR,TONA"``.
    """
    params = {"rate_names": rate_names} if rate_names else None
    return _safe_get("/v1/rates/global", params=params)


def get_kibor_history(
    tenors: Optional[str] = None, days: int = 3000
) -> Optional[list[dict]]:
    """KIBOR history (ascending date order) for charting.

    ``tenors``: optional comma-separated set, e.g. ``"1M,3M,6M,1Y"``.
    """
    params: dict = {"days": days}
    if tenors:
        params["tenors"] = tenors
    return _safe_get("/v1/rates/kibor", params=params)


def use_worker_sync() -> bool:
    """Feature flag: should sync buttons enqueue worker jobs?

    Reads ``st.session_state['use_worker_sync']`` which the sidebar
    checkbox in ``ui/app.py`` writes. Defaults to True (worker mode).
    """
    return bool(st.session_state.get("use_worker_sync", True))


__all__ = [
    "DEFAULT_API_URL",
    "health",
    "render_api_status_banner_if_down",
    # freshness
    "get_freshness",
    "get_dataset_freshness",
    "get_data_freshness_tuple",
    # eod
    "get_latest_eod",
    "get_eod_for_date",
    "get_symbol_history",
    "get_breadth",
    # indices
    "get_all_indices",
    "get_index_history",
    "get_index_constituents",
    # market
    "get_kse100_hero",
    "get_top_gainers",
    "get_top_losers",
    "get_volume_leaders",
    "get_value_leaders",
    "get_52w_extremes",
    "get_sector_leaderboard",
    "get_change_distribution",
    "get_announcements",
    "get_market_analytics",
    # rates
    "get_rates_strip",
    # sync
    "get_sync_runs",
    # jobs
    "get_jobs",
    "get_job_detail",
    "submit_job",
    "cancel_job",
    "run_job_with_progress",
    # fx (1.7.C.1)
    "get_fx_latest",
    "get_fx_latest_one",
    "get_fx_history",
    "get_fx_ohlcv",
    "get_fx_global_pairs",
    "get_fx_global_history",
    "get_fx_spread_heatmap",
    "get_fx_sync_runs",
    # rates extras (1.7.C.1, .3, .4)
    "get_konia",
    "get_npc_rates",
    "get_global_reference_rates",
    "get_kibor_history",
    "use_worker_sync",
]
