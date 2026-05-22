"""Rates Overview — the Fixed Income Command Center.

SBP Policy Rate, KIBOR/KONIA money market rates, and latest
government securities auction cutoffs in a single view.

Reads through the /v1 API client (Phase 1.7.B.6).
"""

import pandas as pd
import streamlit as st

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer


# ── Cached data-loading helpers ───────────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def _load_benchmark_snapshot() -> dict:
    """Returns ``{metrics..., _date}`` matching the legacy dict shape."""
    payload = api_client.get_benchmark_snapshot() or {}
    metrics = dict(payload.get("metrics") or {})
    metrics["_date"] = payload.get("date")
    return metrics


@st.cache_data(ttl=3600, show_spinner=False)
def _load_policy_rate():
    """Fallback: latest policy rate from /v1/rates/policy/history."""
    rows = api_client.get_policy_rate_history(limit=1) or []
    if not rows:
        return None
    return rows[0].get("policy_rate")


@st.cache_data(ttl=3600, show_spinner=False)
def _load_kibor_rates():
    """Fallback: latest KIBOR latest-per-tenor."""
    rows = api_client.get_kibor_latest_per_tenor() or []
    formatted = []
    for r in rows:
        bid = r.get("bid")
        offer = r.get("offer")
        formatted.append({
            "Tenor": f"KIBOR {r.get('tenor')}",
            "Bid": f"{bid:.2f}%" if bid else "N/A",
            "Offer": f"{offer:.2f}%" if offer else "N/A",
            "Change": "",
        })
    return formatted


@st.cache_data(ttl=3600, show_spinner=False)
def _load_konia_rate():
    """Latest KONIA rate via /v1/rates/konia.

    Defensive guard (Group C corruption) — only return values in the
    0..50% band.
    """
    rows = api_client.get_konia(limit=1) or []
    if not rows:
        return None
    val = rows[0].get("rate_pct")
    if val is not None and 0 < val < 50:
        return val
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def _load_tbill_cutoffs():
    """Latest T-Bill cutoff per tenor via /v1/treasury/tbill/latest-per-tenor."""
    rows = api_client.get_tbill_latest_per_tenor() or []
    return [
        {
            "Tenor": r.get("tenor"),
            "Cutoff": f"{r['cutoff_yield']:.2f}%" if r.get("cutoff_yield") is not None else "—",
            "Date": r.get("auction_date"),
        }
        for r in rows
    ]


@st.cache_data(ttl=3600, show_spinner=False)
def _load_pib_cutoffs():
    """Latest PIB cutoff per tenor via /v1/treasury/pib/latest-per-tenor."""
    rows = api_client.get_pib_latest_per_tenor() or []
    return [
        {
            "Tenor": r.get("tenor"),
            "Cutoff": f"{r['cutoff_yield']:.2f}%" if r.get("cutoff_yield") is not None else "—",
            "Date": r.get("auction_date"),
        }
        for r in rows
    ]


# ── Page renderer ─────────────────────────────────────────────────


def render_rates_overview():
    """Render the Rates Overview command center page."""
    api_client.render_api_status_banner_if_down()

    st.markdown("## Rates Overview")
    st.caption("KIBOR, KONIA, policy rate — the big picture")

    snap = _load_benchmark_snapshot()
    snap = dict(snap)  # shallow copy so pop doesn't mutate cache
    snap_date = snap.pop("_date", None)

    # ── Section 1: SBP Policy Rate Hero ──────────────────────────
    _render_policy_rate_hero(snap)

    st.divider()

    # ── Section 2: Money Market Rates ────────────────────────────
    _render_money_market_rates(snap)

    st.divider()

    # ── Section 3: Latest Auction Cutoffs ────────────────────────
    _render_auction_cutoffs(snap)

    st.divider()

    # ── Section 4: Quick Navigation ──────────────────────────────
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Yield Curves", key="ro_nav_yc", width='stretch'):
            st.session_state.nav_to = "Yield Curves"
            st.rerun()
    with c2:
        if st.button("Auction History", key="ro_nav_auct", width='stretch'):
            st.session_state.nav_to = "Treasury Auctions"
            st.rerun()
    with c3:
        if st.button("Rate Trends", key="ro_nav_bm", width='stretch'):
            st.session_state.nav_to = "Benchmark Monitor"
            st.rerun()

    # ── Sync Controls ────────────────────────────────────────────
    st.divider()
    with st.expander("Sync Rates Data"):
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Sync KIBOR (EasyData)", key="ro_rates"):
                if api_client.use_worker_sync():
                    api_client.run_job_with_progress(
                        "sync_kibor_easydata",
                        spinner_text="syncing KIBOR daily from SBP EasyData",
                    )
                else:
                    with st.spinner("Syncing KIBOR from SBP EasyData..."):
                        from pakfindata.db.safe_writer import SafeWriterBusyError
                        from pakfindata.etl.rates import sync_kibor_easydata
                        try:
                            result = sync_kibor_easydata()
                            st.cache_data.clear()
                            st.success(f"KIBOR synced: {result['kibor_rows']} rows")
                        except SafeWriterBusyError:
                            st.error("Another sync is running. Wait a moment and retry.")
                        except Exception as e:
                            st.error(f"Failed: {e}")
        with col2:
            if st.button("Sync Benchmark Snapshot", key="ro_bench"):
                if api_client.use_worker_sync():
                    api_client.run_job_with_progress(
                        "sync_benchmark",
                        spinner_text="scraping SBP benchmark snapshot",
                    )
                else:
                    with st.spinner("Fetching SBP benchmark..."):
                        from pakfindata.db.safe_writer import SafeWriterBusyError
                        from pakfindata.etl.benchmark import sync as sync_benchmark
                        try:
                            result = sync_benchmark()
                            st.cache_data.clear()
                            if result["status"] == "ok":
                                st.success(f"Stored {result['metrics_stored']} metrics")
                            else:
                                st.error("Scrape returned a non-ok status")
                        except SafeWriterBusyError:
                            st.error("Another sync is running. Wait a moment and retry.")
                        except Exception as e:
                            st.error(f"Failed: {e}")
        with col3:
            if st.button("Sync Auctions", key="ro_auctions"):
                if api_client.use_worker_sync():
                    api_client.run_job_with_progress(
                        "sync_treasury_auctions",
                        spinner_text="syncing T-Bill / PIB auctions",
                    )
                else:
                    with st.spinner("Syncing T-Bill/PIB auctions..."):
                        from pakfindata.db.safe_writer import SafeWriterBusyError
                        from pakfindata.etl.treasury import sync_auctions
                        try:
                            result = sync_auctions()
                            st.cache_data.clear()
                            st.success(
                                f"Auctions synced: T-Bills {result['tbills_ok']}, "
                                f"PIBs {result['pibs_ok']} "
                                f"(auction date: {result.get('auction_date') or '—'})"
                            )
                        except SafeWriterBusyError:
                            st.error("Another sync is running. Wait a moment and retry.")
                        except Exception as e:
                            st.error(f"Failed: {e}")

    if snap_date:
        st.caption(f"Benchmark data as of: {snap_date}")

    render_footer()


def _render_policy_rate_hero(snap: dict):
    """SBP Policy Rate as a big hero number with historical range."""
    st.subheader("SBP Policy Rate")

    policy_rate = snap.get("policy_rate")

    # Fallback: try /v1/rates/policy/history
    if policy_rate is None:
        policy_rate = _load_policy_rate()

    if policy_rate is not None:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Policy Rate", f"{policy_rate}%")
        c2.metric("Ceiling (Rev. Repo)", f"{snap.get('ceiling_rate', 'N/A')}%")
        c3.metric("Floor (Repo)", f"{snap.get('floor_rate', 'N/A')}%")
        c4.metric("Overnight WA Repo", f"{snap.get('overnight_repo', 'N/A')}%")
    else:
        st.info("No policy rate data. Sync benchmark snapshot to populate.")


def _render_money_market_rates(snap: dict):
    """KIBOR and KONIA money market rates table."""
    st.subheader("Money Market Rates")

    rows = []

    # Try KIBOR from benchmark snapshot
    kibor_tenors = [
        ("1W", "kibor_1w"),
        ("1M", "kibor_1m"),
        ("3M", "kibor_3m"),
        ("6M", "kibor_6m"),
        ("1Y", "kibor_12m"),
    ]

    for label, key_prefix in kibor_tenors:
        bid = snap.get(f"{key_prefix}_bid")
        offer = snap.get(f"{key_prefix}_offer")
        if bid is not None or offer is not None:
            rows.append({
                "Tenor": f"KIBOR {label}",
                "Bid": f"{bid:.2f}%" if bid else "N/A",
                "Offer": f"{offer:.2f}%" if offer else "N/A",
                "Change": "",
            })

    # Fallback: latest-per-tenor from kibor_daily
    if not rows:
        rows = _load_kibor_rates()

    # KONIA
    konia_rate = _load_konia_rate()
    if konia_rate is not None:
        rows.append({
            "Tenor": "KONIA (Overnight)",
            "Bid": f"{konia_rate:.2f}%",
            "Offer": "—",
            "Change": "",
        })

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, width='stretch', hide_index=True)
    else:
        st.info("No money market rate data. Run `pfsync rates sync` to fetch.")


def _render_auction_cutoffs(snap: dict):
    """Latest government securities auction cutoffs."""
    st.subheader("Government Securities — Latest Auction Cutoffs")

    col1, col2 = st.columns(2)

    # MTB (T-Bills)
    with col1:
        st.markdown("**T-Bills (MTB)**")
        mtb_rows = []
        mtb_tenors = [("3M", "mtb_3m"), ("6M", "mtb_6m"), ("12M", "mtb_12m")]

        # Try from benchmark snapshot
        for label, key in mtb_tenors:
            val = snap.get(key)
            if val is not None:
                mtb_rows.append({"Tenor": label, "Cutoff": f"{val:.2f}%"})

        # Fallback: latest-per-tenor from tbill_auctions
        if not mtb_rows:
            mtb_rows = _load_tbill_cutoffs()

        if mtb_rows:
            st.dataframe(pd.DataFrame(mtb_rows), width='stretch', hide_index=True)
        else:
            st.info("No T-Bill auction data.")

    # PIB
    with col2:
        st.markdown("**PIBs**")
        pib_rows = []
        pib_tenors = [
            ("2Y", "pib_2y"), ("3Y", "pib_3y"), ("5Y", "pib_5y"),
            ("10Y", "pib_10y"), ("15Y", "pib_15y"),
        ]

        # Try from benchmark snapshot
        for label, key in pib_tenors:
            val = snap.get(key)
            if val is not None:
                pib_rows.append({"Tenor": label, "Cutoff": f"{val:.2f}%"})

        # Fallback: latest-per-tenor from pib_auctions
        if not pib_rows:
            pib_rows = _load_pib_cutoffs()

        if pib_rows:
            st.dataframe(pd.DataFrame(pib_rows), width='stretch', hide_index=True)
        else:
            st.info("No PIB auction data.")
