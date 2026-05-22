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
    limit: Optional[int] = None,
) -> Optional[list[dict]]:
    """Symbol OHLCV history; default last 90 days. 404 returns [].

    When ``limit`` is supplied without ``from_date``, the lower-bound
    is dropped and the last N rows are returned date-descending.
    """
    params: dict[str, Any] = {}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    if limit is not None:
        params["limit"] = limit
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


@st.cache_data(ttl=3600)
def get_fx_pairs(active_only: bool = True) -> Optional[list[dict]]:
    """All rows from ``fx_pairs`` master."""
    return _safe_get("/v1/fx/pairs", params={"active_only": active_only})


@st.cache_data(ttl=300)
def get_fx_analytics(pair: str) -> Optional[dict]:
    """Returns + volatility + trend for one FX pair (light compute)."""
    if not pair:
        return None
    return _safe_get(f"/v1/fx/analytics/{pair}", on_404=None)


# ── NCCPL Flow Intelligence (1.7.G.4.5a) ──────────────────────────────────


@st.cache_data(ttl=60)
def get_nccpl_coverage() -> Optional[dict]:
    return _safe_get("/v1/nccpl/coverage")


@st.cache_data(ttl=300)
def get_nccpl_fipi(limit: int = 20) -> Optional[list[dict]]:
    return _safe_get("/v1/nccpl/fipi", params={"limit": limit})


@st.cache_data(ttl=300)
def get_nccpl_lipi(limit: int = 20) -> Optional[list[dict]]:
    return _safe_get("/v1/nccpl/lipi", params={"limit": limit})


@st.cache_data(ttl=600)
def get_nccpl_sector_dates(limit: int = 60) -> Optional[list[str]]:
    return _safe_get("/v1/nccpl/sector-dates", params={"limit": limit})


@st.cache_data(ttl=300)
def get_nccpl_sector(date: str) -> Optional[list[dict]]:
    if not date:
        return []
    return _safe_get("/v1/nccpl/sector", params={"date": date})


@st.cache_data(ttl=300)
def get_nccpl_sector_heatmap(days: int = 20) -> Optional[list[dict]]:
    return _safe_get("/v1/nccpl/sector-heatmap", params={"days": days})


@st.cache_data(ttl=300)
def get_nccpl_flows_derived(limit: int = 1000) -> Optional[list[dict]]:
    return _safe_get("/v1/nccpl/flows-derived", params={"limit": limit})


@st.cache_data(ttl=300)
def get_fx_normalized_performance(
    pairs: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    base: float = 100.0,
) -> Optional[list[dict]]:
    """Wide-format normalized performance across multiple FX pairs."""
    if not pairs:
        return []
    params: dict = {"pairs": ",".join(pairs), "base": base}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return _safe_get("/v1/fx/normalized-performance", params=params)


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


# ── Fixed Income (1.7.B.0) ─────────────────────────────────────────────────


def get_tbill_auctions(
    tenor: Optional[str] = None,
    from_: Optional[str] = None,
    to: Optional[str] = None,
    limit: int = 500,
) -> Optional[list[dict]]:
    params: dict = {"limit": limit}
    if tenor:
        params["tenor"] = tenor
    if from_:
        params["from"] = from_
    if to:
        params["to"] = to
    return _safe_get("/v1/treasury/tbill", params=params)


def get_tbill_latest_per_tenor() -> Optional[list[dict]]:
    return _safe_get("/v1/treasury/tbill/latest-per-tenor")


def get_pib_auctions(
    tenor: Optional[str] = None,
    from_: Optional[str] = None,
    to: Optional[str] = None,
    limit: int = 500,
) -> Optional[list[dict]]:
    params: dict = {"limit": limit}
    if tenor:
        params["tenor"] = tenor
    if from_:
        params["from"] = from_
    if to:
        params["to"] = to
    return _safe_get("/v1/treasury/pib", params=params)


def get_pib_latest_per_tenor() -> Optional[list[dict]]:
    return _safe_get("/v1/treasury/pib/latest-per-tenor")


def get_gis_auctions(limit: int = 100) -> Optional[list[dict]]:
    return _safe_get("/v1/treasury/gis", params={"limit": limit})


def get_pkrv(
    date: Optional[str] = None, days: int = 1
) -> Optional[list[dict]]:
    params: dict = {"days": days}
    if date:
        params["date"] = date
    return _safe_get("/v1/yield-curves/pkrv", params=params)


def get_pkisrv(
    date: Optional[str] = None, days: int = 1
) -> Optional[list[dict]]:
    params: dict = {"days": days}
    if date:
        params["date"] = date
    return _safe_get("/v1/yield-curves/pkisrv", params=params)


def get_pkfrv(
    date: Optional[str] = None, limit: int = 500
) -> Optional[list[dict]]:
    params: dict = {"limit": limit}
    if date:
        params["date"] = date
    return _safe_get("/v1/yield-curves/pkfrv", params=params)


def get_pkfrv_bond_history(
    bond_code: str, limit: int = 1000
) -> Optional[list[dict]]:
    return _safe_get(
        f"/v1/yield-curves/pkfrv/{bond_code}/history",
        params={"limit": limit},
    )


def get_sovereign_sources() -> Optional[list[str]]:
    return _safe_get("/v1/curve/sovereign/sources")


def get_sovereign_tenor_history(
    tenor: str, sources: Optional[str] = None, limit: int = 1000
) -> Optional[list[dict]]:
    params: dict = {"tenor": tenor, "limit": limit}
    if sources:
        params["sources"] = sources
    return _safe_get("/v1/curve/sovereign/tenor-history", params=params)


def get_sovereign_dates(
    source: Optional[str] = None, limit: int = 500
) -> Optional[list[str]]:
    params: dict = {"limit": limit}
    if source:
        params["source"] = source
    return _safe_get("/v1/curve/sovereign/dates", params=params)


def get_sovereign_curve(
    date: Optional[str] = None,
    source: Optional[str] = None,
    include_synthetic: bool = True,
) -> Optional[list[dict]]:
    params: dict = {"include_synthetic": include_synthetic}
    if date:
        params["date"] = date
    if source:
        params["source"] = source
    return _safe_get("/v1/curve/sovereign", params=params)


def get_bond_trading_daily(
    from_: Optional[str] = None,
    to: Optional[str] = None,
    security_type: Optional[str] = None,
    limit: int = 1000,
) -> Optional[list[dict]]:
    params: dict = {"limit": limit}
    if from_:
        params["from"] = from_
    if to:
        params["to"] = to
    if security_type:
        params["security_type"] = security_type
    return _safe_get("/v1/bonds/trading-daily", params=params)


def get_benchmark_snapshot() -> Optional[dict]:
    """Returns ``{date: str|None, metrics: {metric: value, ...}}``."""
    return _safe_get("/v1/benchmark/snapshot")


def get_benchmark_history(
    metric: str, from_: Optional[str] = None, to: Optional[str] = None
) -> Optional[list[dict]]:
    params: dict = {"metric": metric}
    if from_:
        params["from"] = from_
    if to:
        params["to"] = to
    return _safe_get("/v1/benchmark/snapshot/history", params=params)


def get_bond_market_status() -> Optional[dict]:
    return _safe_get("/v1/benchmark/status")


def get_policy_rate_history(limit: int = 100) -> Optional[list[dict]]:
    return _safe_get("/v1/rates/policy/history", params={"limit": limit})


def get_npc_vs_rfr_spread(
    currency: Optional[str] = None, from_: Optional[str] = None
) -> Optional[list[dict]]:
    params: dict = {}
    if currency:
        params["currency"] = currency
    if from_:
        params["from"] = from_
    return _safe_get("/v1/rates/npc/spread", params=params)


def get_npc_carry(
    currency: str = "USD", from_: Optional[str] = None
) -> Optional[list[dict]]:
    params: dict = {"currency": currency}
    if from_:
        params["from"] = from_
    return _safe_get("/v1/rates/npc/carry", params=params)


def get_npc_multicurrency(date: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if date:
        params["date"] = date
    return _safe_get("/v1/rates/npc/multicurrency", params=params)


def get_npc_yield_curve(
    currency: str = "USD", date: Optional[str] = None
) -> Optional[dict]:
    params: dict = {"currency": currency}
    if date:
        params["date"] = date
    return _safe_get("/v1/rates/npc/yield-curve", params=params)


def get_sofr_kibor_spread(from_: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if from_:
        params["from"] = from_
    return _safe_get("/v1/rates/global/spread/sofr-kibor", params=params)


def get_rate_comparison() -> Optional[dict]:
    return _safe_get("/v1/rates/global/comparison")


def get_global_rates_latest() -> Optional[list[dict]]:
    return _safe_get("/v1/rates/global/latest")


def get_global_rate_history(
    rate_name: str,
    tenor: str = "ON",
    from_: Optional[str] = None,
    limit: int = 0,
) -> Optional[list[dict]]:
    params: dict = {"rate_name": rate_name, "tenor": tenor, "limit": limit}
    if from_:
        params["from"] = from_
    return _safe_get("/v1/rates/global/history", params=params)


def get_fcy_instruments() -> Optional[list[dict]]:
    return _safe_get("/v1/fi/fcy-instruments")


def get_alm_products(
    active_only: bool = True, asset_liability: Optional[str] = None
) -> Optional[list[dict]]:
    params: dict = {"active_only": active_only}
    if asset_liability:
        params["asset_liability"] = asset_liability
    return _safe_get("/v1/alm/products", params=params)


def get_alm_positions(as_of: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if as_of:
        params["as_of"] = as_of
    return _safe_get("/v1/alm/positions", params=params)


def get_alm_repricing_gap(as_of: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if as_of:
        params["as_of"] = as_of
    return _safe_get("/v1/alm/repricing-gap", params=params)


def get_alm_ftp_rates(as_of: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if as_of:
        params["as_of"] = as_of
    return _safe_get("/v1/alm/ftp-rates", params=params)


def get_alm_sensitivity(as_of: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if as_of:
        params["as_of"] = as_of
    return _safe_get("/v1/alm/sensitivity", params=params)


def get_alm_liquidity_ladder(as_of: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if as_of:
        params["as_of"] = as_of
    return _safe_get("/v1/alm/liquidity-ladder", params=params)


def get_fi_instruments(
    active_only: bool = True,
    category: Optional[str] = None,
    limit: int = 500,
) -> Optional[list[dict]]:
    params: dict = {"active_only": active_only, "limit": limit}
    if category:
        params["category"] = category
    return _safe_get("/v1/fi/instruments", params=params)


def get_fi_quotes_latest() -> Optional[list[dict]]:
    return _safe_get("/v1/fi/quotes/latest")


def get_fi_quotes_history(
    instrument_id: str, days: int = 60
) -> Optional[list[dict]]:
    return _safe_get(
        f"/v1/fi/quotes/{instrument_id}/history",
        params={"days": days},
    )


def get_kibor_latest_per_tenor() -> Optional[list[dict]]:
    return _safe_get("/v1/rates/kibor/latest-per-tenor")


# ── Equities (1.7.D.0) ─────────────────────────────────────────────────────


def get_screener(
    sector: Optional[str] = None,
    min_pe: float = 0.0,
    max_pe: float = 1000.0,
    min_mcap_m: float = 0.0,
    min_volume: float = 0.0,
    limit: int = 200,
) -> Optional[list[dict]]:
    params: dict = {
        "min_pe": min_pe,
        "max_pe": max_pe,
        "min_mcap_m": min_mcap_m,
        "min_volume": min_volume,
        "limit": limit,
    }
    if sector and sector != "All":
        params["sector"] = sector
    return _safe_get("/v1/symbols/screener", params=params)


def get_symbol_sectors() -> Optional[list[str]]:
    return _safe_get("/v1/symbols/sectors")


def get_sector_performance(
    date: Optional[str] = None, min_stocks: int = 2
) -> Optional[list[dict]]:
    params: dict = {"min_stocks": min_stocks}
    if date:
        params["date"] = date
    return _safe_get("/v1/sectors/performance", params=params)


def get_sector_symbol_map(date: Optional[str] = None) -> Optional[list[dict]]:
    params: dict = {}
    if date:
        params["date"] = date
    return _safe_get("/v1/sectors/symbol-map", params=params)


def get_financial_symbols() -> Optional[list[str]]:
    return _safe_get("/v1/companies/financial-symbols")


def get_company_financials(
    symbol: str, period_type: Optional[str] = None
) -> Optional[dict]:
    """Returns ``{symbol, is_bank, rows: [...]}``."""
    params: dict = {}
    if period_type:
        params["period_type"] = period_type
    return _safe_get(
        f"/v1/companies/{symbol.upper()}/financials", params=params
    )


def get_sector_valuation(symbol: str) -> Optional[dict]:
    return _safe_get(f"/v1/companies/{symbol.upper()}/sector-valuation")


def get_company_profile_extras(symbol: str) -> Optional[dict]:
    """Returns ``{profile: {...}|None, key_people: [...]}``."""
    return _safe_get(f"/v1/companies/{symbol.upper()}/profile-extras")


def get_company_announcements(
    symbol: str, limit: int = 30
) -> Optional[list[dict]]:
    return _safe_get(
        f"/v1/companies/{symbol.upper()}/announcements",
        params={"limit": limit},
    )


def get_company_dividend_payouts(
    symbol: str, limit: int = 20
) -> Optional[list[dict]]:
    return _safe_get(
        f"/v1/companies/{symbol.upper()}/dividend-payouts",
        params={"limit": limit},
    )


def get_factor_raw_data() -> Optional[dict]:
    """Returns ``{rows: [...], snapshot_count: int}``."""
    return _safe_get("/v1/factors/raw-data")


def get_factor_risk_stats(
    symbols: list[str], days: int = 90
) -> Optional[list[dict]]:
    if not symbols:
        return []
    return _safe_get(
        "/v1/factors/risk-stats",
        params={"symbols": ",".join(symbols), "days": days},
    )


# ── Intraday + Turnover (1.7.E.0) ──────────────────────────────────────────


@st.cache_data(ttl=300)
def get_intraday_dates() -> Optional[list[str]]:
    return _safe_get("/v1/intraday/dates")


@st.cache_data(ttl=30)
def get_intraday_summary(
    date: str, market: str = "REG"
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/intraday/summary",
        params={"date": date, "market": market},
    )


@st.cache_data(ttl=30)
def get_intraday_bars(
    symbol: str,
    date: str,
    interval: str = "1s",
    limit: int = 20000,
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/intraday/bars",
        params={"symbol": symbol, "date": date, "interval": interval, "limit": limit},
    )


@st.cache_data(ttl=30)
def get_intraday_minute_breadth(
    date: str, market: str = "REG"
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/intraday/breadth/minute",
        params={"date": date, "market": market},
    )


@st.cache_data(ttl=30)
def get_intraday_hourly_breadth(
    date: str, market: str = "REG"
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/intraday/breadth/hourly",
        params={"date": date, "market": market},
    )


@st.cache_data(ttl=30)
def get_intraday_index_minute(
    date: str, symbols: list[str]
) -> Optional[list[dict]]:
    if not symbols:
        return []
    return _safe_get(
        "/v1/intraday/index-minute",
        params={"date": date, "symbols": ",".join(symbols)},
    )


@st.cache_data(ttl=60)
def get_turnover_stats() -> Optional[dict]:
    return _safe_get("/v1/turnover/stats")


@st.cache_data(ttl=60)
def get_turnover_dates() -> Optional[list[str]]:
    return _safe_get("/v1/turnover/dates")


@st.cache_data(ttl=60)
def get_turnover_missing(
    since: str = "2024-01-01", until: Optional[str] = None
) -> Optional[list[str]]:
    params: dict = {"since": since}
    if until:
        params["until"] = until
    return _safe_get("/v1/turnover/missing", params=params)


@st.cache_data(ttl=60)
def get_turnover(
    date: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = 2000,
) -> Optional[list[dict]]:
    params: dict = {"limit": limit}
    if date:
        params["date"] = date
    if symbol:
        params["symbol"] = symbol
    return _safe_get("/v1/turnover", params=params)


# ── Research / Strategies (1.7.F.0) ────────────────────────────────────────


@st.cache_data(ttl=600)
def get_symbols(active_only: bool = True) -> Optional[list[dict]]:
    return _safe_get("/v1/symbols", params={"active_only": active_only})


@st.cache_data(ttl=300)
def get_top_symbols_by_volume(
    n: int = 30, days: int = 20
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/symbols/top-by-volume", params={"n": n, "days": days}
    )


@st.cache_data(ttl=600)
def get_futures_symbols(min_data_days: int = 30) -> Optional[list[str]]:
    return _safe_get(
        "/v1/symbols/futures", params={"min_data_days": min_data_days}
    )


@st.cache_data(ttl=300)
def get_tick_logs_dates(symbol: str) -> Optional[list[str]]:
    if not symbol:
        return []
    return _safe_get(f"/v1/tick-logs/dates/{symbol}")


@st.cache_data(ttl=300)
def get_latest_futures(base_symbol: str) -> Optional[dict]:
    """Latest active futures contract for an underlying. 404 -> None."""
    if not base_symbol:
        return None
    return _safe_get(f"/v1/futures/{base_symbol}/latest", on_404=None)


# ── Admin (1.7.G.1.0) — raw catalog introspection ──────────────────────────


@st.cache_data(ttl=60)
def get_admin_tables(include_counts: bool = False) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/admin/tables", params={"include_counts": include_counts}
    )


@st.cache_data(ttl=60)
@st.cache_data(ttl=3600)
def get_admin_table_columns(table: str) -> Optional[list[dict]]:
    """Column metadata for one table (PRAGMA table_info)."""
    if not table:
        return []
    return _safe_get(f"/v1/admin/tables/{table}/columns", on_404=[])


def get_admin_table_latest_date(
    table: str, col: str = "date"
) -> Optional[dict]:
    return _safe_get(
        f"/v1/admin/tables/{table}/latest-date",
        params={"col": col},
        on_404=None,
    )


@st.cache_data(ttl=300)
def get_admin_table_duplicates(
    table: str, by: list[str], limit: int = 20
) -> Optional[dict]:
    if not by:
        return None
    return _safe_get(
        f"/v1/admin/tables/{table}/duplicates",
        params={"by": ",".join(by), "limit": limit},
        on_404=None,
    )


@st.cache_data(ttl=60)
def get_admin_duckdb_tables(
    include_counts: bool = False,
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/admin/duckdb/tables", params={"include_counts": include_counts}
    )


@st.cache_data(ttl=30)
def get_admin_sync_runs(
    limit: int = 20, completed_only: bool = False
) -> Optional[list[dict]]:
    return _safe_get(
        "/v1/admin/sync-runs",
        params={"limit": limit, "completed_only": completed_only},
    )


@st.cache_data(ttl=30)
def get_admin_sync_failures(limit: int = 50) -> Optional[list[dict]]:
    return _safe_get("/v1/admin/sync-runs/failures", params={"limit": limit})


@st.cache_data(ttl=300)
def get_admin_table_distinct_count(
    table: str, col: str
) -> Optional[dict]:
    return _safe_get(
        f"/v1/admin/tables/{table}/distinct-count",
        params={"col": col},
        on_404=None,
    )


@st.cache_data(ttl=300)
def get_admin_table_distinct(
    table: str, col: str, limit: int = 1000
) -> Optional[list[str]]:
    return _safe_get(
        f"/v1/admin/tables/{table}/distinct",
        params={"col": col, "limit": limit},
        on_404=[],
    )


@st.cache_data(ttl=60)
def get_admin_db_stats() -> Optional[dict]:
    return _safe_get("/v1/admin/db-stats")


# ── Funds + ETFs (1.7.G.2.0) ───────────────────────────────────────────────


@st.cache_data(ttl=300)
def get_funds(
    category: Optional[str] = None,
    amc_code: Optional[str] = None,
    fund_type: Optional[str] = None,
    is_shariah: Optional[int] = None,
    active_only: bool = True,
    limit: int = 2000,
) -> Optional[list[dict]]:
    params: dict = {"active_only": active_only, "limit": limit}
    if category:
        params["category"] = category
    if amc_code:
        params["amc_code"] = amc_code
    if fund_type:
        params["fund_type"] = fund_type
    if is_shariah is not None:
        params["is_shariah"] = is_shariah
    return _safe_get("/v1/funds", params=params)


@st.cache_data(ttl=300)
def get_fund_categories() -> Optional[list[str]]:
    return _safe_get("/v1/funds/categories")


@st.cache_data(ttl=300)
def get_fund_amcs() -> Optional[list[dict]]:
    return _safe_get("/v1/funds/amcs")


@st.cache_data(ttl=60)
def get_funds_nav_latest(limit: int = 2000) -> Optional[list[dict]]:
    return _safe_get("/v1/funds/nav-latest", params={"limit": limit})


@st.cache_data(ttl=300)
def get_fund_performance_leaders(
    metric: str = "return_365d",
    category: Optional[str] = None,
    limit: int = 50,
    direction: str = "top",
) -> Optional[list[dict]]:
    params: dict = {"metric": metric, "limit": limit, "direction": direction}
    if category:
        params["category"] = category
    return _safe_get("/v1/funds/performance/leaders", params=params)


@st.cache_data(ttl=300)
def get_fund(fund_id: str) -> Optional[dict]:
    if not fund_id:
        return None
    return _safe_get(f"/v1/funds/{fund_id}", on_404=None)


@st.cache_data(ttl=300)
def get_fund_nav(
    fund_id: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 2000,
) -> Optional[list[dict]]:
    if not fund_id:
        return []
    params: dict = {"limit": limit}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    return _safe_get(f"/v1/funds/{fund_id}/nav", params=params, on_404=[])


@st.cache_data(ttl=300)
def get_fund_performance(fund_id: str) -> Optional[dict]:
    if not fund_id:
        return None
    return _safe_get(f"/v1/funds/{fund_id}/performance", on_404=None)


@st.cache_data(ttl=300)
def get_fund_risk(fund_id: str) -> Optional[dict]:
    if not fund_id:
        return None
    return _safe_get(f"/v1/funds/{fund_id}/risk", on_404=None)


@st.cache_data(ttl=300)
def get_fund_calendar_returns(fund_id: str) -> Optional[list[dict]]:
    if not fund_id:
        return []
    return _safe_get(f"/v1/funds/{fund_id}/calendar-returns", on_404=[])


@st.cache_data(ttl=300)
def get_etfs() -> Optional[list[dict]]:
    return _safe_get("/v1/etfs")


@st.cache_data(ttl=300)
def get_etf(symbol: str) -> Optional[dict]:
    if not symbol:
        return None
    return _safe_get(f"/v1/etfs/{symbol}", on_404=None)


@st.cache_data(ttl=300)
def get_etf_nav(
    symbol: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 2000,
) -> Optional[list[dict]]:
    if not symbol:
        return []
    params: dict = {"limit": limit}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    return _safe_get(f"/v1/etfs/{symbol}/nav", params=params, on_404=[])


# ── Commodities + khistocks + PMEX-portal (1.7.G.3.0) ──────────────────


@st.cache_data(ttl=300)
def get_commodity_has_data() -> Optional[dict]:
    """Composite gate: does any of the 3 commodity tables have rows."""
    return _safe_get("/v1/commodities/has-data")


@st.cache_data(ttl=600)
def get_commodity_symbols(source: Optional[str] = None) -> Optional[list[str]]:
    params = {"source": source} if source else None
    return _safe_get("/v1/commodities/symbols", params=params)


@st.cache_data(ttl=600)
def get_commodity_fx_pairs() -> Optional[list[str]]:
    return _safe_get("/v1/commodities/fx-pairs")


@st.cache_data(ttl=60)
def get_commodity_latest(symbols: list[str]) -> Optional[list[dict]]:
    """Latest + previous close for a list of commodity symbols."""
    if not symbols:
        return []
    return _safe_get(
        "/v1/commodities/latest",
        params={"symbols": ",".join(symbols)},
    )


@st.cache_data(ttl=300)
def get_commodity_sector_performance() -> Optional[list[dict]]:
    return _safe_get("/v1/commodities/sector-performance")


@st.cache_data(ttl=300)
def get_commodity_pkr_latest() -> Optional[list[dict]]:
    return _safe_get("/v1/commodities/pkr-latest")


@st.cache_data(ttl=300)
def get_commodity_categories_latest() -> Optional[list[dict]]:
    return _safe_get("/v1/commodities/categories-latest")


@st.cache_data(ttl=60)
def get_commodity_sync_runs(limit: int = 10) -> Optional[list[dict]]:
    return _safe_get("/v1/commodities/sync-runs", params={"limit": limit})


@st.cache_data(ttl=60)
def get_commodity_export(dataset: str, limit: int = 5000) -> Optional[list[dict]]:
    """Bulk export wrapper. dataset ∈ {eod, monthly, pkr, fx, khistocks, pmex_market_watch}."""
    return _safe_get(
        f"/v1/commodities/export/{dataset}",
        params={"limit": limit},
        on_404=None,
    )


@st.cache_data(ttl=300)
def get_commodity_eod(symbol: str, limit: int = 365) -> Optional[list[dict]]:
    if not symbol:
        return []
    return _safe_get(
        f"/v1/commodities/{symbol}/eod",
        params={"limit": limit},
        on_404=[],
    )


@st.cache_data(ttl=300)
def get_commodity_pkr_history(symbol: str, limit: int = 90) -> Optional[list[dict]]:
    if not symbol:
        return []
    return _safe_get(
        f"/v1/commodities/{symbol}/pkr",
        params={"limit": limit},
        on_404=[],
    )


@st.cache_data(ttl=600)
def get_khistocks_feeds() -> Optional[list[str]]:
    return _safe_get("/v1/khistocks/feeds")


@st.cache_data(ttl=120)
def get_khistocks_latest(feed: Optional[str] = None) -> Optional[list[dict]]:
    params = {"feed": feed} if feed else None
    return _safe_get("/v1/khistocks/latest", params=params)


@st.cache_data(ttl=300)
def get_khistocks_history(symbol: str, limit: int = 90) -> Optional[list[dict]]:
    if not symbol:
        return []
    return _safe_get(
        f"/v1/khistocks/{symbol}/history",
        params={"limit": limit},
        on_404=[],
    )


@st.cache_data(ttl=600)
def get_pmex_portal_categories() -> Optional[list[str]]:
    return _safe_get("/v1/pmex-portal/categories")


@st.cache_data(ttl=120)
def get_pmex_portal_latest(category: Optional[str] = None) -> Optional[list[dict]]:
    params = {"category": category} if category else None
    return _safe_get("/v1/pmex-portal/latest", params=params)


@st.cache_data(ttl=300)
def get_pmex_portal_history(contract: str, limit: int = 90) -> Optional[list[dict]]:
    if not contract:
        return []
    return _safe_get(
        f"/v1/pmex-portal/{contract}/history",
        params={"limit": limit},
        on_404=[],
    )


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
    # fixed income (1.7.B.0)
    "get_tbill_auctions",
    "get_tbill_latest_per_tenor",
    "get_pib_auctions",
    "get_pib_latest_per_tenor",
    "get_gis_auctions",
    "get_pkrv",
    "get_pkisrv",
    "get_pkfrv",
    "get_pkfrv_bond_history",
    "get_sovereign_sources",
    "get_sovereign_dates",
    "get_sovereign_tenor_history",
    "get_sovereign_curve",
    "get_bond_trading_daily",
    "get_benchmark_snapshot",
    "get_benchmark_history",
    "get_bond_market_status",
    "get_policy_rate_history",
    "get_npc_vs_rfr_spread",
    "get_npc_carry",
    "get_npc_multicurrency",
    "get_npc_yield_curve",
    "get_sofr_kibor_spread",
    "get_rate_comparison",
    "get_global_rates_latest",
    "get_global_rate_history",
    "get_fcy_instruments",
    "get_alm_products",
    "get_alm_positions",
    "get_alm_repricing_gap",
    "get_alm_ftp_rates",
    "get_alm_sensitivity",
    "get_alm_liquidity_ladder",
    "get_fi_instruments",
    "get_fi_quotes_latest",
    "get_fi_quotes_history",
    "get_kibor_latest_per_tenor",
    # equities (1.7.D.0)
    "get_screener",
    "get_symbol_sectors",
    "get_sector_performance",
    "get_sector_symbol_map",
    "get_financial_symbols",
    "get_company_financials",
    "get_sector_valuation",
    "get_company_profile_extras",
    "get_company_announcements",
    "get_company_dividend_payouts",
    "get_factor_raw_data",
    "get_factor_risk_stats",
    # intraday + turnover (1.7.E.0)
    "get_intraday_dates",
    "get_intraday_summary",
    "get_intraday_bars",
    "get_intraday_minute_breadth",
    "get_intraday_hourly_breadth",
    "get_intraday_index_minute",
    "get_turnover_stats",
    "get_turnover_dates",
    "get_turnover_missing",
    "get_turnover",
    # research / strategies (1.7.F.0)
    "get_symbols",
    "get_top_symbols_by_volume",
    "get_futures_symbols",
    "get_tick_logs_dates",
    "get_latest_futures",
    # admin / catalog introspection (1.7.G.1.0)
    "get_admin_tables",
    "get_admin_table_columns",
    "get_admin_table_latest_date",
    "get_admin_table_duplicates",
    "get_admin_duckdb_tables",
    "get_admin_sync_runs",
    "get_admin_sync_failures",
    "get_admin_table_distinct_count",
    "get_admin_table_distinct",
    "get_admin_db_stats",
    # funds + etfs (1.7.G.2.0)
    "get_funds",
    "get_fund_categories",
    "get_fund_amcs",
    "get_funds_nav_latest",
    "get_fund_performance_leaders",
    "get_fund",
    "get_fund_nav",
    "get_fund_performance",
    "get_fund_risk",
    "get_fund_calendar_returns",
    "get_etfs",
    "get_etf",
    "get_etf_nav",
    # commodities + khistocks + pmex-portal (1.7.G.3.0)
    "get_commodity_has_data",
    "get_commodity_symbols",
    "get_commodity_fx_pairs",
    "get_commodity_latest",
    "get_commodity_sector_performance",
    "get_commodity_pkr_latest",
    "get_commodity_categories_latest",
    "get_commodity_sync_runs",
    "get_commodity_export",
    "get_commodity_eod",
    "get_commodity_pkr_history",
    "get_khistocks_feeds",
    "get_khistocks_latest",
    "get_khistocks_history",
    "get_pmex_portal_categories",
    "get_pmex_portal_latest",
    "get_pmex_portal_history",
    # fx pair/analytics extras (1.7.G.4.2a)
    "get_fx_pairs",
    "get_fx_analytics",
    "get_fx_normalized_performance",
    # nccpl flow intelligence (1.7.G.4.5a)
    "get_nccpl_coverage",
    "get_nccpl_fipi",
    "get_nccpl_lipi",
    "get_nccpl_sector_dates",
    "get_nccpl_sector",
    "get_nccpl_sector_heatmap",
    "get_nccpl_flows_derived",
    "use_worker_sync",
]
