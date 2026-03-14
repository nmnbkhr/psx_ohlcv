"""FTP Monitor — Daily Funds Transfer Pricing rates, margins, P&L attribution.

Shows dynamic FTP rates computed from live PKRV/KIBOR/KONIA curves,
margin decomposition, product profitability, and monthly P&L attribution.

Tabs:
  FTP Rates — Current FTP by product, curve used, margin
  Margin Decomposition — Base + liquidity + optionality breakdown
  Time Series — FTP rate & margin evolution over time
  P&L Attribution — Volume vs Rate vs Mix effects (monthly)
  Compute — Run FTP engine, backfill, settings
"""

from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_C = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "asset": "#4ECDC4", "liability": "#E74C3C",
    "base": "#3498DB", "liq": "#9B59B6", "credit": "#E67E22",
    "opt": "#FF6B35", "margin_pos": "#00E676", "margin_neg": "#FF5252",
    "pkrv": "#FF6B35", "kibor": "#4ECDC4", "konia": "#45B7D1", "sofr": "#2196F3",
    "bg": "#0e1117", "card_bg": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "text_dim": "#888888",
}

_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_C["text"], size=11),
    xaxis=dict(gridcolor=_C["grid"], zeroline=False),
    yaxis=dict(gridcolor=_C["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=10, r=10, t=40, b=10),
)

CURVE_COLORS = {
    "PKRV": _C["pkrv"], "PKISRV": "#2ECC71", "KIBOR": _C["kibor"],
    "KONIA": _C["konia"], "SOFR": _C["sofr"],
}


def _fig(height=400, **kw):
    return go.Figure(layout={**_LAYOUT, "height": height, **kw})


def _card(label, value, delta=None, color=None, suffix=""):
    border = color or _C["accent"]
    dim = _C["text_dim"]
    delta_html = ""
    if delta is not None:
        d_color = _C["up"] if delta >= 0 else _C["down"]
        d_sign = "+" if delta >= 0 else ""
        delta_html = f'<div style="font-size:12px;color:{d_color}">{d_sign}{delta:.1f}{suffix}</div>'
    st.markdown(f"""
    <div style="background:{_C['card_bg']};border-left:3px solid {border};
         padding:12px 16px;border-radius:6px;margin-bottom:8px">
      <div style="color:{dim};font-size:11px;text-transform:uppercase;letter-spacing:1px">{label}</div>
      <div style="font-size:22px;font-weight:700;color:{_C['text']}">{value}</div>
      {delta_html}
    </div>""", unsafe_allow_html=True)


def _section(title):
    st.markdown(f"""
    <div style="border-bottom:1px solid {_C['grid']};padding:6px 0;margin:16px 0 12px 0">
      <span style="color:{_C['accent']};font-weight:600;font-size:13px;
            text-transform:uppercase;letter-spacing:1px">{title}</span>
    </div>""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═════════════════════════════════════════════════════════════════════════════

def render_ftp_monitor():
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
      <span style="font-size:24px;font-weight:700;color:{_C['text']}">FTP MONITOR</span>
      <span style="color:{_C['text_dim']};font-size:13px">Funds Transfer Pricing &amp; Product Profitability</span>
    </div>""", unsafe_allow_html=True)

    con = get_connection()

    tabs = st.tabs(["FTP Rates", "Margin Decomposition", "Time Series", "P&L Attribution", "Compute"])

    with tabs[0]:
        _render_ftp_rates(con)
    with tabs[1]:
        _render_decomposition(con)
    with tabs[2]:
        _render_time_series(con)
    with tabs[3]:
        _render_pnl_attribution(con)
    with tabs[4]:
        _render_compute(con)

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB: FTP RATES
# ═════════════════════════════════════════════════════════════════════════════

def _render_ftp_rates(con):
    from pakfindata.db.repositories.alm import get_ftp_rates

    _section("CURRENT FTP RATES")

    ftp_df = get_ftp_rates(con)
    if ftp_df.empty:
        st.info("No FTP rates computed yet. Go to **Compute** tab to run the FTP engine.")
        return

    as_of = ftp_df["as_of_date"].iloc[0] if "as_of_date" in ftp_df.columns else "N/A"
    st.markdown(f'<div style="color:{_C["text_dim"]};font-size:12px;margin-bottom:12px">As of: {as_of}</div>',
                unsafe_allow_html=True)

    # Summary KPIs
    assets = ftp_df[ftp_df["asset_liability"] == "A"]
    liabs = ftp_df[ftp_df["asset_liability"] == "L"]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        avg_asset_ftp = assets["total_ftp_rate"].mean() if not assets.empty else 0
        _card("Avg Asset FTP", f"{avg_asset_ftp:.2f}%", color=_C["asset"])
    with c2:
        avg_liab_ftp = liabs["total_ftp_rate"].mean() if not liabs.empty else 0
        _card("Avg Liability FTP", f"{avg_liab_ftp:.2f}%", color=_C["liability"])
    with c3:
        spread = avg_asset_ftp - avg_liab_ftp
        _card("FTP Spread", f"{spread:.2f}%", delta=spread * 100, suffix=" bps",
              color=_C["up"] if spread > 0 else _C["down"])
    with c4:
        if ftp_df["daily_nii_mn"].notna().any():
            daily_nii = ftp_df["daily_nii_mn"].sum()
            _card("Daily NII", f"PKR {daily_nii:,.2f} Mn", color=_C["accent"])
        else:
            _card("Daily NII", "N/A", color=_C["accent"])

    # Asset FTP table
    _section("ASSET PRODUCTS — FTP RATES")
    if not assets.empty:
        display_cols = ["product_code", "product_name", "category", "ftp_curve",
                       "ftp_tenor_months", "ftp_base_rate", "total_ftp_rate",
                       "customer_rate", "ftp_margin_bps"]
        available_cols = [c for c in display_cols if c in assets.columns]
        st.dataframe(
            assets[available_cols].style.format({
                "ftp_base_rate": "{:.2f}%", "total_ftp_rate": "{:.2f}%",
                "customer_rate": "{:.2f}%", "ftp_margin_bps": "{:+.0f}",
                "ftp_tenor_months": "{:.1f}",
            }, na_rep="—"),
            use_container_width=True, hide_index=True,
        )

    # Liability FTP table
    _section("LIABILITY PRODUCTS — FTP RATES")
    if not liabs.empty:
        display_cols = ["product_code", "product_name", "category", "ftp_curve",
                       "ftp_tenor_months", "ftp_base_rate", "total_ftp_rate",
                       "customer_rate", "ftp_margin_bps"]
        available_cols = [c for c in display_cols if c in liabs.columns]
        st.dataframe(
            liabs[available_cols].style.format({
                "ftp_base_rate": "{:.2f}%", "total_ftp_rate": "{:.2f}%",
                "customer_rate": "{:.2f}%", "ftp_margin_bps": "{:+.0f}",
                "ftp_tenor_months": "{:.1f}",
            }, na_rep="—"),
            use_container_width=True, hide_index=True,
        )

    # Product profitability heatmap
    if ftp_df["ftp_margin_bps"].notna().any():
        _section("PRODUCT PROFITABILITY")
        margin_data = ftp_df[ftp_df["ftp_margin_bps"].notna()].sort_values("ftp_margin_bps", ascending=True)
        colors = [_C["margin_pos"] if v >= 0 else _C["margin_neg"] for v in margin_data["ftp_margin_bps"]]

        fig = go.Figure(go.Bar(
            y=margin_data["product_name"] if "product_name" in margin_data.columns else margin_data["product_code"],
            x=margin_data["ftp_margin_bps"],
            orientation="h",
            marker_color=colors,
            text=[f"{v:+.0f} bps" for v in margin_data["ftp_margin_bps"]],
            textposition="outside",
        ))
        fig.update_layout(**_LAYOUT, height=max(350, len(margin_data) * 28),
                         title="FTP Margin by Product (bps)", xaxis_title="Margin (bps)")
        st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: MARGIN DECOMPOSITION
# ═════════════════════════════════════════════════════════════════════════════

def _render_decomposition(con):
    from pakfindata.db.repositories.alm import get_ftp_rates

    _section("FTP MARGIN DECOMPOSITION")

    ftp_df = get_ftp_rates(con)
    if ftp_df.empty:
        st.info("No FTP data. Run the engine first.")
        return

    # Stacked bar: base + liquidity + credit + optionality for each product
    fig = go.Figure()

    products = ftp_df["product_code"].tolist()
    fig.add_trace(go.Bar(
        name="Base Rate", x=products, y=ftp_df["ftp_base_rate"],
        marker_color=_C["base"],
    ))
    if "liq_premium_bps" in ftp_df.columns:
        fig.add_trace(go.Bar(
            name="Liquidity Premium", x=products, y=ftp_df["liq_premium_bps"] / 100,
            marker_color=_C["liq"],
        ))
    if "credit_spread_bps" in ftp_df.columns:
        fig.add_trace(go.Bar(
            name="Credit Spread", x=products, y=ftp_df["credit_spread_bps"] / 100,
            marker_color=_C["credit"],
        ))
    if "optionality_bps" in ftp_df.columns:
        fig.add_trace(go.Bar(
            name="Optionality Cost", x=products, y=ftp_df["optionality_bps"] / 100,
            marker_color=_C["opt"],
        ))

    fig.update_layout(
        **_LAYOUT, height=500, barmode="stack",
        title="FTP Rate Components by Product",
        yaxis_title="Rate (%)", xaxis_tickangle=-45,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Curve usage breakdown
    _section("CURVE USAGE")
    if "ftp_curve" in ftp_df.columns:
        curve_counts = ftp_df["ftp_curve"].value_counts()
        fig2 = go.Figure(go.Pie(
            labels=curve_counts.index.tolist(),
            values=curve_counts.values.tolist(),
            marker_colors=[CURVE_COLORS.get(c, _C["neutral"]) for c in curve_counts.index],
            hole=0.4,
        ))
        fig2.update_layout(**_LAYOUT, height=300, title="Products by FTP Curve")
        st.plotly_chart(fig2, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: TIME SERIES
# ═════════════════════════════════════════════════════════════════════════════

def _render_time_series(con):
    from pakfindata.db.repositories.alm import get_ftp_history, get_alm_products

    _section("FTP RATE & MARGIN EVOLUTION")

    products = get_alm_products(con)
    if products.empty:
        st.info("No products configured.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        selected = st.multiselect(
            "Products",
            products["product_code"].tolist(),
            default=products["product_code"].tolist()[:5],
        )
    with col2:
        days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2)

    if not selected:
        return

    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    history = get_ftp_history(con, start_date=start)

    if history.empty:
        st.info("No FTP history. Run the engine for multiple days to see trends.")
        return

    history = history[history["product_code"].isin(selected)]
    if history.empty:
        return

    # FTP Rate time series
    fig = _fig(height=400, title="Total FTP Rate Over Time")
    for pc in selected:
        pc_data = history[history["product_code"] == pc]
        if not pc_data.empty:
            fig.add_trace(go.Scatter(
                x=pc_data["as_of_date"], y=pc_data["total_ftp_rate"],
                mode="lines", name=pc, line=dict(width=2),
            ))
    fig.update_layout(yaxis_title="FTP Rate (%)")
    st.plotly_chart(fig, use_container_width=True)

    # Margin time series
    margin_data = history[history["ftp_margin_bps"].notna()]
    if not margin_data.empty:
        fig2 = _fig(height=350, title="FTP Margin Evolution (bps)")
        for pc in selected:
            pc_data = margin_data[margin_data["product_code"] == pc]
            if not pc_data.empty:
                fig2.add_trace(go.Scatter(
                    x=pc_data["as_of_date"], y=pc_data["ftp_margin_bps"],
                    mode="lines", name=pc, line=dict(width=2),
                ))
        fig2.update_layout(yaxis_title="Margin (bps)")
        st.plotly_chart(fig2, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: P&L ATTRIBUTION
# ═════════════════════════════════════════════════════════════════════════════

def _render_pnl_attribution(con):
    from pakfindata.db.repositories.alm import get_ftp_pnl
    from pakfindata.engine.alm_engine import compute_monthly_pnl

    _section("MONTHLY FTP P&L ATTRIBUTION")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        month = st.text_input("Month (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
    with col2:
        prev = st.text_input("Previous Month", value=(datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m"))
    with col3:
        if st.button("Compute P&L", type="primary"):
            with st.spinner("Computing..."):
                results = compute_monthly_pnl(con, month, prev)
                st.success(f"Computed {len(results)} products")
                st.rerun()

    pnl = get_ftp_pnl(con, month=month)
    if pnl.empty:
        st.info("No P&L data for this month. Click **Compute P&L** to generate.")
        return

    # Summary
    total_nii = pnl["nii_contribution_mn"].sum()
    total_vol = pnl["volume_effect_mn"].sum()
    total_rate = pnl["rate_effect_mn"].sum()
    total_mix = pnl["mix_effect_mn"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _card("Total NII", f"PKR {total_nii:,.1f} Mn", color=_C["accent"])
    with c2:
        _card("Volume Effect", f"PKR {total_vol:+,.1f} Mn",
              color=_C["up"] if total_vol >= 0 else _C["down"])
    with c3:
        _card("Rate Effect", f"PKR {total_rate:+,.1f} Mn",
              color=_C["up"] if total_rate >= 0 else _C["down"])
    with c4:
        _card("Mix Effect", f"PKR {total_mix:+,.1f} Mn",
              color=_C["up"] if total_mix >= 0 else _C["down"])

    # Waterfall chart: NII by product
    fig = go.Figure()
    sorted_pnl = pnl.sort_values("nii_contribution_mn", ascending=True)
    labels = sorted_pnl["product_name"] if "product_name" in sorted_pnl.columns else sorted_pnl["product_code"]
    colors = [_C["asset"] if v >= 0 else _C["liability"] for v in sorted_pnl["nii_contribution_mn"]]

    fig.add_trace(go.Bar(
        y=labels, x=sorted_pnl["nii_contribution_mn"],
        orientation="h", marker_color=colors,
        text=[f"PKR {v:+,.1f}" for v in sorted_pnl["nii_contribution_mn"]],
        textposition="outside",
    ))
    fig.update_layout(**_LAYOUT, height=max(350, len(pnl) * 28),
                     title=f"NII Contribution by Product — {month}",
                     xaxis_title="PKR Millions")
    st.plotly_chart(fig, use_container_width=True)

    # Attribution breakdown
    if (pnl["volume_effect_mn"].abs().sum() + pnl["rate_effect_mn"].abs().sum()) > 0:
        _section("ATTRIBUTION BREAKDOWN")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(name="Volume", y=labels, x=sorted_pnl["volume_effect_mn"],
                             orientation="h", marker_color=_C["base"]))
        fig2.add_trace(go.Bar(name="Rate", y=labels, x=sorted_pnl["rate_effect_mn"],
                             orientation="h", marker_color=_C["liq"]))
        fig2.add_trace(go.Bar(name="Mix", y=labels, x=sorted_pnl["mix_effect_mn"],
                             orientation="h", marker_color=_C["opt"]))
        fig2.update_layout(**_LAYOUT, height=max(350, len(pnl) * 28),
                          barmode="group", title="Volume / Rate / Mix Attribution",
                          xaxis_title="PKR Millions")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("P&L Data"):
        st.dataframe(pnl.style.format({
            "avg_balance_mn": "{:,.0f}", "avg_customer_rate": "{:.2f}%",
            "avg_ftp_rate": "{:.2f}%", "avg_margin_bps": "{:+.0f}",
            "nii_contribution_mn": "{:+,.2f}", "volume_effect_mn": "{:+,.2f}",
            "rate_effect_mn": "{:+,.2f}", "mix_effect_mn": "{:+,.2f}",
        }, na_rep="—"), use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: COMPUTE
# ═════════════════════════════════════════════════════════════════════════════

def _render_compute(con):
    from pakfindata.engine.alm_engine import run_daily_ftp
    from pakfindata.db.repositories.alm import get_alm_products, seed_default_products

    _section("FTP ENGINE CONTROLS")

    # Status
    products = get_alm_products(con)
    n_products = len(products)

    # Check curve data availability
    pkrv_latest = con.execute("SELECT MAX(date) as d FROM pkrv_daily").fetchone()
    kibor_latest = con.execute("SELECT MAX(date) as d FROM kibor_daily").fetchone()
    konia_latest = con.execute("SELECT MAX(date) as d FROM konia_daily").fetchone()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _card("Products", str(n_products), color=_C["accent"])
    with c2:
        _card("PKRV Latest", pkrv_latest["d"] if pkrv_latest and pkrv_latest["d"] else "N/A",
              color=_C["pkrv"])
    with c3:
        _card("KIBOR Latest", kibor_latest["d"] if kibor_latest and kibor_latest["d"] else "N/A",
              color=_C["kibor"])
    with c4:
        _card("KONIA Latest", konia_latest["d"] if konia_latest and konia_latest["d"] else "N/A",
              color=_C["konia"])

    st.markdown("---")

    # Quick actions
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"**Run Daily FTP**")
        ftp_date = st.date_input("FTP Date", value=datetime.now().date(), key="ftp_date")
        if st.button("Compute FTP", type="primary", use_container_width=True):
            if n_products == 0:
                st.warning("No products. Seed first.")
            else:
                with st.spinner(f"Computing FTP for {ftp_date}..."):
                    results = run_daily_ftp(con, ftp_date.strftime("%Y-%m-%d"))
                    st.success(f"Computed FTP for {len(results)} products")

    with col2:
        st.markdown(f"**Seed Products**")
        st.markdown(f'<div style="color:{_C["text_dim"]};font-size:12px">Load typical Pakistani bank product set</div>',
                    unsafe_allow_html=True)
        if st.button("Seed Defaults", type="secondary", use_container_width=True):
            count = seed_default_products(con)
            st.success(f"Seeded {count} products")
            st.rerun()

    with col3:
        st.markdown(f"**Backfill FTP**")
        backfill_days = st.number_input("Days to backfill", value=30, min_value=1, max_value=365)
        if st.button("Backfill", type="secondary", use_container_width=True):
            if n_products == 0:
                st.warning("No products. Seed first.")
            else:
                progress = st.progress(0)
                total = 0
                for i in range(backfill_days):
                    d = (datetime.now() - timedelta(days=backfill_days - i)).strftime("%Y-%m-%d")
                    results = run_daily_ftp(con, d)
                    total += len(results)
                    progress.progress((i + 1) / backfill_days)
                st.success(f"Backfilled {total} FTP entries over {backfill_days} days")

    # FTP computation log
    _section("RECENT FTP COMPUTATIONS")
    recent = pd.read_sql_query("""
        SELECT as_of_date, COUNT(*) as products,
               ROUND(AVG(total_ftp_rate), 2) as avg_ftp,
               ROUND(AVG(ftp_margin_bps), 0) as avg_margin_bps,
               MAX(computed_at) as last_computed
        FROM alm_ftp_rates
        GROUP BY as_of_date
        ORDER BY as_of_date DESC
        LIMIT 20
    """, con)

    if not recent.empty:
        st.dataframe(recent, use_container_width=True, hide_index=True)
    else:
        st.info("No FTP computations yet.")
