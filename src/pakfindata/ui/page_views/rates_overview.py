"""Rates Overview — the Fixed Income Command Center.

SBP Policy Rate, KIBOR/KONIA money market rates, and latest
government securities auction cutoffs in a single view.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_rates_overview():
    """Render the Rates Overview command center page."""
    st.markdown("## Rates Overview")
    st.caption("KIBOR, KONIA, policy rate — the big picture")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # Try to load benchmark data first (most comprehensive source)
    from pakfindata.db.repositories.bond_market import (
        init_bond_market_schema,
        get_benchmark_snapshot,
    )
    init_bond_market_schema(con)
    snap = get_benchmark_snapshot(con) or {}
    snap_date = snap.pop("_date", None)

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
        if st.button("Yield Curves", key="ro_nav_yc", use_container_width=True):
            st.session_state.nav_to = "Yield Curves"
            st.rerun()
    with c2:
        if st.button("Auction History", key="ro_nav_auct", use_container_width=True):
            st.session_state.nav_to = "Treasury Auctions"
            st.rerun()
    with c3:
        if st.button("Rate Trends", key="ro_nav_bm", use_container_width=True):
            st.session_state.nav_to = "Benchmark Monitor"
            st.rerun()

    # ── Sync Controls ────────────────────────────────────────────
    st.divider()
    with st.expander("Sync Rates Data"):
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Sync KIBOR/KONIA", key="ro_rates"):
                with st.spinner("Syncing rates from SBP..."):
                    try:
                        from pakfindata.sources.sbp_rates import SBPRatesScraper
                        result = SBPRatesScraper().sync_rates(con)
                        st.success(f"Rates synced: {result}")
                    except Exception as e:
                        st.error(f"Failed: {e}")
        with col2:
            if st.button("Sync Benchmark Snapshot", key="ro_bench"):
                with st.spinner("Fetching SBP benchmark..."):
                    try:
                        from pakfindata.sources.sbp_bond_market import SBPBondMarketScraper
                        result = SBPBondMarketScraper().sync_benchmark(con)
                        if result["status"] == "ok":
                            st.success(f"Stored {result['metrics_stored']} metrics")
                        else:
                            st.error(result.get("error", "Unknown error"))
                    except Exception as e:
                        st.error(f"Failed: {e}")
        with col3:
            if st.button("Sync Auctions", key="ro_auctions"):
                with st.spinner("Syncing T-Bill/PIB auctions..."):
                    try:
                        from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
                        result = SBPTreasuryScraper().sync_treasury(con)
                        st.success(f"Auctions synced: {result}")
                    except Exception as e:
                        st.error(f"Failed: {e}")

    if snap_date:
        st.caption(f"Benchmark data as of: {snap_date}")

    render_footer()


def _render_policy_rate_hero(con, snap: dict):
    """SBP Policy Rate as a big hero number with historical range."""
    st.subheader("SBP Policy Rate")

    policy_rate = snap.get("policy_rate")

    # Fallback: try sbp_policy_rates table directly
    if policy_rate is None:
        try:
            row = con.execute(
                "SELECT policy_rate, rate_date FROM sbp_policy_rates "
                "ORDER BY rate_date DESC LIMIT 1"
            ).fetchone()
            if row:
                policy_rate = row[0]
        except Exception:
            pass

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
            for _, r in kibor_df.iterrows():
                rows.append({
                    "Tenor": f"KIBOR {r['tenor']}",
                    "Bid": f"{r['bid']:.2f}%" if r["bid"] else "N/A",
                    "Offer": f"{r['offer']:.2f}%" if r["offer"] else "N/A",
                    "Change": "",
                })
        except Exception:
            pass

    # KONIA
    try:
        konia = con.execute(
            "SELECT rate, date FROM konia_daily ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if konia and konia[0]:
            rows.append({
                "Tenor": "KONIA (Overnight)",
                "Bid": f"{konia[0]:.2f}%",
                "Offer": "—",
                "Change": "",
            })
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)
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
                for _, r in df.iterrows():
                    mtb_rows.append({
                        "Tenor": r["tenor"],
                        "Cutoff": f"{r['cutoff_yield']:.2f}%",
                        "Date": r["auction_date"],
                    })
            except Exception:
                pass

        if mtb_rows:
            st.dataframe(pd.DataFrame(mtb_rows), use_container_width=True, hide_index=True)
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
                for _, r in df.iterrows():
                    pib_rows.append({
                        "Tenor": r["tenor"],
                        "Cutoff": f"{r['cutoff_yield']:.2f}%",
                        "Date": r["auction_date"],
                    })
            except Exception:
                pass

        if pib_rows:
            st.dataframe(pd.DataFrame(pib_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No PIB auction data.")
