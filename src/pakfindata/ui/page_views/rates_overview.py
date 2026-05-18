"""Rates Overview — the Fixed Income Command Center.

SBP Policy Rate, KIBOR/KONIA money market rates, and latest
government securities auction cutoffs in a single view.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


# ── Cached data-loading helpers ───────────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def _load_benchmark_snapshot():
    from pakfindata.db.repositories.bond_market import (
        init_bond_market_schema,
        get_benchmark_snapshot,
    )
    con = get_connection()
    if con is None:
        return {}
    init_bond_market_schema(con)
    return get_benchmark_snapshot(con) or {}


@st.cache_data(ttl=3600, show_spinner=False)
def _load_policy_rate():
    """Fallback: latest policy rate from sbp_policy_rates table."""
    con = get_connection()
    if con is None:
        return None
    try:
        row = con.execute(
            "SELECT policy_rate, rate_date FROM sbp_policy_rates "
            "ORDER BY rate_date DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def _load_kibor_rates():
    """Fallback: latest KIBOR rates from kibor_daily table."""
    con = get_connection()
    if con is None:
        return []
    try:
        kibor_df = pd.read_sql_query(
            """SELECT tenor, bid, offer FROM kibor_daily
               WHERE date = (SELECT MAX(date) FROM kibor_daily)
               ORDER BY
                 CASE tenor
                   WHEN '1W' THEN 1 WHEN '2W' THEN 2
                   WHEN '1M' THEN 3 WHEN '3M' THEN 4
                   WHEN '6M' THEN 5 WHEN '9M' THEN 6
                   WHEN '1Y' THEN 7 ELSE 8
                 END""",
            con,
        )
        rows = []
        for _, r in kibor_df.iterrows():
            rows.append({
                "Tenor": f"KIBOR {r['tenor']}",
                "Bid": f"{r['bid']:.2f}%" if r["bid"] else "N/A",
                "Offer": f"{r['offer']:.2f}%" if r["offer"] else "N/A",
                "Change": "",
            })
        return rows
    except Exception:
        return []


@st.cache_data(ttl=3600, show_spinner=False)
def _load_konia_rate():
    """Latest KONIA overnight rate from konia_daily table."""
    con = get_connection()
    if con is None:
        return None
    try:
        konia = con.execute(
            "SELECT rate, date FROM konia_daily ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if konia and konia[0]:
            return konia[0]
        return None
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def _load_tbill_cutoffs():
    """Fallback: latest T-Bill auction cutoffs from tbill_auctions table."""
    con = get_connection()
    if con is None:
        return []
    try:
        df = pd.read_sql_query(
            """SELECT tenor, cutoff_yield, auction_date
               FROM tbill_auctions
               WHERE (tenor, auction_date) IN (
                   SELECT tenor, MAX(auction_date)
                   FROM tbill_auctions GROUP BY tenor
               )
               ORDER BY
                 CASE tenor
                   WHEN '3M' THEN 1 WHEN '6M' THEN 2
                   WHEN '12M' THEN 3 ELSE 4
                 END""",
            con,
        )
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "Tenor": r["tenor"],
                "Cutoff": f"{r['cutoff_yield']:.2f}%",
                "Date": r["auction_date"],
            })
        return rows
    except Exception:
        return []


@st.cache_data(ttl=3600, show_spinner=False)
def _load_pib_cutoffs():
    """Fallback: latest PIB auction cutoffs from pib_auctions table."""
    con = get_connection()
    if con is None:
        return []
    try:
        df = pd.read_sql_query(
            """SELECT tenor, cutoff_yield, auction_date
               FROM pib_auctions
               WHERE (tenor, auction_date) IN (
                   SELECT tenor, MAX(auction_date)
                   FROM pib_auctions GROUP BY tenor
               )
               ORDER BY
                 CASE tenor
                   WHEN '2Y' THEN 1 WHEN '3Y' THEN 2
                   WHEN '5Y' THEN 3 WHEN '10Y' THEN 4
                   WHEN '15Y' THEN 5 WHEN '20Y' THEN 6
                   WHEN '30Y' THEN 7 ELSE 8
                 END""",
            con,
        )
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "Tenor": r["tenor"],
                "Cutoff": f"{r['cutoff_yield']:.2f}%",
                "Date": r["auction_date"],
            })
        return rows
    except Exception:
        return []


# ── Page renderer ─────────────────────────────────────────────────


def render_rates_overview():
    """Render the Rates Overview command center page."""
    st.markdown("## Rates Overview")
    st.caption("KIBOR, KONIA, policy rate — the big picture")

    snap = _load_benchmark_snapshot()
    snap = dict(snap)  # shallow copy so pop doesn't mutate cache
    snap_date = snap.pop("_date", None)

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # ── Section 1: SBP Policy Rate Hero ──────────────────────────
    _render_policy_rate_hero(con, snap)

    st.divider()

    # ── Section 2: Money Market Rates ────────────────────────────
    _render_money_market_rates(con, snap)

    st.divider()

    # ── Section 3: Latest Auction Cutoffs ────────────────────────
    _render_auction_cutoffs(con, snap)

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
                with st.spinner("Syncing KIBOR from SBP EasyData..."):
                    from pakfindata.db.safe_writer import safe_writer, SafeWriterBusyError
                    from pakfindata.db.catalog import update_catalog_from_table, record_catalog_failure
                    from pakfindata.sources.sbp_easydata import sync_kibor_to_db
                    try:
                        with safe_writer() as wcon:
                            result = sync_kibor_to_db(wcon)
                            update_catalog_from_table(wcon, "kibor", source="sbp_easydata")
                        st.cache_data.clear()
                        st.success(f"KIBOR synced: {result.get('kibor_rows', 0)} rows")
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        st.error(f"Failed: {e}")
                        record_catalog_failure("kibor", source="sbp_easydata", error=e)
        with col2:
            if st.button("Sync Benchmark Snapshot", key="ro_bench"):
                with st.spinner("Fetching SBP benchmark..."):
                    from pakfindata.db.safe_writer import safe_writer, SafeWriterBusyError
                    from pakfindata.db.catalog import update_catalog_from_table, record_catalog_failure
                    from pakfindata.sources.sbp_bond_market import SBPBondMarketScraper
                    try:
                        scraper = SBPBondMarketScraper()  # init outside lock
                        with safe_writer() as wcon:
                            result = scraper.sync_benchmark(wcon)
                            update_catalog_from_table(wcon, "benchmark_snapshot", source="sbp_bond_market")
                        st.cache_data.clear()
                        if result["status"] == "ok":
                            st.success(f"Stored {result['metrics_stored']} metrics")
                        else:
                            st.error(result.get("error", "Unknown error"))
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        st.error(f"Failed: {e}")
                        record_catalog_failure("benchmark_snapshot", source="sbp_bond_market", error=e)
        with col3:
            if st.button("Sync Auctions", key="ro_auctions"):
                with st.spinner("Syncing T-Bill/PIB auctions..."):
                    from pakfindata.db.safe_writer import safe_writer, SafeWriterBusyError
                    from pakfindata.db.catalog import update_catalog_from_table, record_catalog_failure
                    from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
                    try:
                        scraper = SBPTreasuryScraper()  # init outside lock
                        with safe_writer() as wcon:
                            result = scraper.sync_treasury(wcon)
                            update_catalog_from_table(wcon, "treasury", source="sbp")
                            update_catalog_from_table(wcon, "pib", source="sbp")
                        st.cache_data.clear()
                        st.success(f"Auctions synced: {result}")
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        st.error(f"Failed: {e}")
                        for ds in ("treasury", "pib"):
                            record_catalog_failure(ds, source="sbp", error=e)

    if snap_date:
        st.caption(f"Benchmark data as of: {snap_date}")

    render_footer()


def _render_policy_rate_hero(con, snap: dict):
    """SBP Policy Rate as a big hero number with historical range."""
    st.subheader("SBP Policy Rate")

    policy_rate = snap.get("policy_rate")

    # Fallback: try sbp_policy_rates table directly
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


def _render_money_market_rates(con, snap: dict):
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

    # Fallback: try kibor_daily table
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


def _render_auction_cutoffs(con, snap: dict):
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

        # Fallback: query tbill_auctions table
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

        # Fallback: query pib_auctions table
        if not pib_rows:
            pib_rows = _load_pib_cutoffs()

        if pib_rows:
            st.dataframe(pd.DataFrame(pib_rows), width='stretch', hide_index=True)
        else:
            st.info("No PIB auction data.")
