"""Treasury Market Dashboard — yield curves, auctions, and rate comparisons."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from psx_ohlcv.ui.components.helpers import get_connection, render_footer


def render_treasury_dashboard():
    """Treasury Market dashboard with yield curves, auctions, and rates."""
    st.markdown("## Treasury Market Dashboard")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        _render_rate_metrics(con)
        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            _render_yield_curve(con)
        with col2:
            _render_rate_history(con)

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            _render_tbill_auctions(con)
        with col2:
            _render_pib_auctions(con)

    except Exception as e:
        st.error(f"Error loading treasury data: {e}")

    render_footer()


def _render_rate_metrics(con):
    """Rate comparison metrics row."""
    cols = st.columns(4)

    # Policy Rate
    row = con.execute(
        "SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    with cols[0]:
        if row:
            st.metric("SBP Policy Rate", f"{row['policy_rate']:.1f}%", help=f"As of {row['rate_date']}")
        else:
            st.metric("SBP Policy Rate", "N/A")

    # KIBOR 3M
    kibor = con.execute(
        "SELECT bid, offer FROM kibor_daily WHERE tenor = '3M' ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with cols[1]:
        if kibor:
            st.metric("KIBOR 3M", f"{kibor['offer']:.2f}%", help="Offer rate")
        else:
            # Try any tenor
            kibor = con.execute(
                "SELECT tenor, bid, offer FROM kibor_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if kibor:
                st.metric(f"KIBOR {kibor['tenor']}", f"{kibor['offer']:.2f}%")
            else:
                st.metric("KIBOR", "N/A")

    # T-Bill 3M yield
    tbill = con.execute(
        "SELECT cutoff_yield FROM tbill_auctions WHERE tenor LIKE '%3M%' OR tenor LIKE '%3 M%' ORDER BY auction_date DESC LIMIT 1"
    ).fetchone()
    with cols[2]:
        if tbill:
            st.metric("T-Bill 3M Yield", f"{tbill['cutoff_yield']:.2f}%")
        else:
            tbill = con.execute(
                "SELECT tenor, cutoff_yield FROM tbill_auctions ORDER BY auction_date DESC LIMIT 1"
            ).fetchone()
            if tbill:
                st.metric(f"T-Bill {tbill['tenor']}", f"{tbill['cutoff_yield']:.2f}%")
            else:
                st.metric("T-Bill Yield", "N/A")

    # KONIA
    konia = con.execute(
        "SELECT rate_pct, date FROM konia_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with cols[3]:
        if konia:
            st.metric("KONIA", f"{konia['rate_pct']:.2f}%", help=f"As of {konia['date']}")
        else:
            st.metric("KONIA", "N/A")


def _render_yield_curve(con):
    """PKRV yield curve chart with comparison dates."""
    st.markdown("### PKRV Yield Curve")

    # Get available dates
    dates = con.execute(
        "SELECT DISTINCT date FROM pkrv_daily ORDER BY date DESC LIMIT 10"
    ).fetchall()

    if not dates:
        st.info("No PKRV yield curve data available. Run `psxsync rates yield-curve` to fetch.")
        return

    date_list = [r["date"] for r in dates]
    latest_date = date_list[0]

    # Current curve
    df = pd.read_sql_query(
        "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
        con, params=(latest_date,),
    )

    if df.empty:
        st.info("No yield curve points available")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["tenor_months"], y=df["yield_pct"],
        mode="lines+markers", name=f"Current ({latest_date})",
        line=dict(width=3, color="#FF6B35"),
    ))

    # Comparison dates from yield_curve_points table
    comp_dates = con.execute(
        "SELECT DISTINCT curve_date FROM yield_curve_points ORDER BY curve_date DESC LIMIT 5"
    ).fetchall()
    colors = ["#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD"]
    for i, row in enumerate(comp_dates):
        cdf = pd.read_sql_query(
            "SELECT tenor_months, yield_rate as yield_pct FROM yield_curve_points WHERE curve_date = ? ORDER BY tenor_months",
            con, params=(row["curve_date"],),
        )
        if not cdf.empty:
            fig.add_trace(go.Scatter(
                x=cdf["tenor_months"], y=cdf["yield_pct"],
                mode="lines+markers", name=row["curve_date"],
                line=dict(width=1, dash="dash", color=colors[i % len(colors)]),
            ))

    fig.update_layout(
        xaxis_title="Tenor (Months)", yaxis_title="Yield (%)",
        height=400, margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_rate_history(con):
    """Multi-line rate history chart."""
    st.markdown("### Rate History")

    fig = go.Figure()

    # Policy rate
    df = pd.read_sql_query(
        "SELECT rate_date as date, policy_rate as rate FROM sbp_policy_rates ORDER BY rate_date",
        con,
    )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="Policy Rate",
            line=dict(width=2, color="#FF6B35"),
        ))

    # KIBOR (pick one representative tenor)
    df = pd.read_sql_query(
        "SELECT date, offer as rate FROM kibor_daily WHERE tenor = '3M' ORDER BY date", con,
    )
    if df.empty:
        df = pd.read_sql_query(
            "SELECT date, offer as rate FROM kibor_daily ORDER BY date", con,
        )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="KIBOR",
            line=dict(width=2, color="#4ECDC4"),
        ))

    # KONIA
    df = pd.read_sql_query(
        "SELECT date, rate_pct as rate FROM konia_daily ORDER BY date", con,
    )
    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rate"],
            mode="lines+markers", name="KONIA",
            line=dict(width=2, color="#45B7D1"),
        ))

    if fig.data:
        fig.update_layout(
            xaxis_title="Date", yaxis_title="Rate (%)",
            height=400, margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No rate history data available")


def _render_tbill_auctions(con):
    """T-Bill auction results table."""
    st.markdown("### T-Bill Auctions")
    df = pd.read_sql_query(
        """SELECT auction_date, tenor, cutoff_yield, cutoff_price,
                  amount_accepted_billions, target_amount_billions
           FROM tbill_auctions ORDER BY auction_date DESC LIMIT 10""",
        con,
    )
    if df.empty:
        st.info("No T-Bill auction data. Run `psxsync treasury tbill-sync` to fetch.")
        return

    st.dataframe(
        df.rename(columns={
            "auction_date": "Date", "tenor": "Tenor",
            "cutoff_yield": "Yield (%)", "cutoff_price": "Price",
            "amount_accepted_billions": "Accepted (B)",
            "target_amount_billions": "Target (B)",
        }),
        use_container_width=True, hide_index=True,
    )


def _render_pib_auctions(con):
    """PIB auction results table."""
    st.markdown("### PIB Auctions")
    df = pd.read_sql_query(
        """SELECT auction_date, tenor, pib_type, cutoff_yield,
                  coupon_rate, amount_accepted_billions
           FROM pib_auctions ORDER BY auction_date DESC LIMIT 10""",
        con,
    )
    if df.empty:
        st.info("No PIB auction data. Run `psxsync treasury pib-sync` to fetch.")
        return

    st.dataframe(
        df.rename(columns={
            "auction_date": "Date", "tenor": "Tenor", "pib_type": "Type",
            "cutoff_yield": "Yield (%)", "coupon_rate": "Coupon (%)",
            "amount_accepted_billions": "Accepted (B)",
        }),
        use_container_width=True, hide_index=True,
    )
