"""ALM Dashboard — Repricing Gap, NII/EVE Sensitivity, Liquidity Ladder, Duration Gap.

Reads from alm_* tables and live market curves (PKRV, KIBOR, KONIA).
Separate from PSX equity pages — this is the bank balance sheet risk view.

Tabs:
  Overview — KPI cards, gap summary, rate environment
  Repricing Gap — Assets vs Liabilities by repricing bucket (waterfall)
  Sensitivity — NII/EVE parallel shift scenarios, SBP cut scenarios
  Liquidity — Maturity ladder, LCR proxy, HQLA composition
  Products — Product catalog manager, seed defaults
  Positions — Upload/manage balance sheet positions by bucket
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM — matching treasury_dashboard Bloomberg style
# ═════════════════════════════════════════════════════════════════════════════

_C = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "asset": "#4ECDC4", "liability": "#E74C3C",
    "gap_pos": "#00E676", "gap_neg": "#FF5252",
    "nii": "#3498DB", "eve": "#9B59B6",
    "hqla": "#2ECC71", "inflow": "#4ECDC4", "outflow": "#E74C3C",
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

BUCKETS_ORDER = ["ON", "1D-1M", "1M-3M", "3M-6M", "6M-1Y", "1Y-2Y", "2Y-3Y", "3Y-5Y", "5Y-10Y", "10Y+"]


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

def render_alm_dashboard():
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
      <span style="font-size:24px;font-weight:700;color:{_C['text']}">ALM DASHBOARD</span>
      <span style="color:{_C['text_dim']};font-size:13px">Asset-Liability Management &amp; Interest Rate Risk</span>
    </div>""", unsafe_allow_html=True)

    con = get_connection()

    tabs = st.tabs(["Overview", "Repricing Gap", "NII/EVE Sensitivity", "Liquidity", "Products", "Positions"])

    with tabs[0]:
        _render_overview(con)
    with tabs[1]:
        _render_repricing_gap(con)
    with tabs[2]:
        _render_sensitivity(con)
    with tabs[3]:
        _render_liquidity(con)
    with tabs[4]:
        _render_products(con)
    with tabs[5]:
        _render_positions(con)

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB: OVERVIEW
# ═════════════════════════════════════════════════════════════════════════════

def _render_overview(con):
    _section("RATE ENVIRONMENT")

    # Pull live rates for context
    konia_row = con.execute("SELECT rate_pct, date FROM konia_daily ORDER BY date DESC LIMIT 1").fetchone()
    policy_row = con.execute("SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1").fetchone()
    kibor_3m = con.execute("SELECT offer, date FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1").fetchone()
    kibor_6m = con.execute("SELECT offer, date FROM kibor_daily WHERE tenor='6M' ORDER BY date DESC LIMIT 1").fetchone()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _card("SBP Policy Rate", f"{policy_row['policy_rate']:.2f}%" if policy_row else "N/A",
              color=_C["liability"])
    with c2:
        _card("KONIA (ON)", f"{konia_row['rate_pct']:.2f}%" if konia_row else "N/A",
              color="#45B7D1")
    with c3:
        _card("KIBOR 3M Offer", f"{kibor_3m['offer']:.2f}%" if kibor_3m else "N/A",
              color=_C["asset"])
    with c4:
        _card("KIBOR 6M Offer", f"{kibor_6m['offer']:.2f}%" if kibor_6m else "N/A",
              color=_C["nii"])

    # ALM summary cards
    _section("BALANCE SHEET SUMMARY")

    from pakfindata.db.repositories.alm import get_repricing_gap, get_ftp_rates, get_sensitivity

    gap_df = get_repricing_gap(con)
    ftp_df = get_ftp_rates(con)
    sens_df = get_sensitivity(con)

    if gap_df.empty:
        st.info("No balance sheet positions loaded yet. Go to **Positions** tab to upload data, or **Products** tab to seed the product catalog.")
        return

    total_assets = gap_df["assets_mn"].sum()
    total_liabs = gap_df["liabilities_mn"].sum()
    total_gap = gap_df["gap_mn"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _card("Total Assets", f"PKR {total_assets:,.0f} Mn", color=_C["asset"])
    with c2:
        _card("Total Liabilities", f"PKR {total_liabs:,.0f} Mn", color=_C["liability"])
    with c3:
        gap_pct = total_gap / total_assets * 100 if total_assets > 0 else 0
        _card("Net Gap", f"PKR {total_gap:,.0f} Mn", delta=gap_pct, suffix="% of assets",
              color=_C["gap_pos"] if total_gap >= 0 else _C["gap_neg"])
    with c4:
        if not ftp_df.empty and "ftp_margin_bps" in ftp_df.columns:
            avg_margin = ftp_df["ftp_margin_bps"].mean()
            _card("Avg FTP Margin", f"{avg_margin:.0f} bps", color=_C["accent"])
        else:
            _card("Avg FTP Margin", "N/A", color=_C["accent"])

    # NII sensitivity summary
    if not sens_df.empty:
        _section("NII SENSITIVITY SNAPSHOT")
        key_scenarios = sens_df[sens_df["scenario"].isin(["+100bps", "-100bps", "+200bps", "-200bps", "SBP_CUT_150"])]
        if not key_scenarios.empty:
            cols = st.columns(len(key_scenarios))
            for i, (_, row) in enumerate(key_scenarios.iterrows()):
                with cols[i]:
                    impact = row["nii_impact_mn"]
                    _card(
                        row["scenario"],
                        f"PKR {impact:+,.0f} Mn",
                        delta=row["nii_pct_change"],
                        suffix="% NII",
                        color=_C["up"] if impact >= 0 else _C["down"],
                    )


# ═════════════════════════════════════════════════════════════════════════════
# TAB: REPRICING GAP
# ═════════════════════════════════════════════════════════════════════════════

def _render_repricing_gap(con):
    from pakfindata.db.repositories.alm import get_repricing_gap

    _section("REPRICING GAP ANALYSIS")

    gap_df = get_repricing_gap(con)
    if gap_df.empty:
        st.info("No positions loaded. Upload balance sheet data in the Positions tab.")
        return

    # Waterfall chart: assets vs liabilities by bucket
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Assets", x=gap_df["bucket"], y=gap_df["assets_mn"],
        marker_color=_C["asset"], opacity=0.85,
    ))
    fig.add_trace(go.Bar(
        name="Liabilities", x=gap_df["bucket"], y=gap_df["liabilities_mn"],
        marker_color=_C["liability"], opacity=0.85,
    ))
    fig.add_trace(go.Scatter(
        name="Net Gap", x=gap_df["bucket"], y=gap_df["gap_mn"],
        mode="lines+markers", line=dict(color=_C["accent"], width=2),
        marker=dict(size=8),
    ))

    # Cumulative gap line
    cum_gap = gap_df["gap_mn"].cumsum()
    fig.add_trace(go.Scatter(
        name="Cumulative Gap", x=gap_df["bucket"], y=cum_gap,
        mode="lines+markers", line=dict(color="#FF6B35", width=2, dash="dash"),
        marker=dict(size=6),
    ))

    fig.update_layout(
        **_LAYOUT, height=500, barmode="group",
        title="Repricing Gap — Assets vs Liabilities by Bucket",
        yaxis_title="PKR Millions",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Gap as % of total assets
    total_assets = gap_df["assets_mn"].sum()
    if total_assets > 0:
        gap_pct_df = gap_df.copy()
        gap_pct_df["gap_pct"] = gap_pct_df["gap_mn"] / total_assets * 100
        gap_pct_df["cum_gap_pct"] = gap_pct_df["gap_pct"].cumsum()

        fig2 = go.Figure()
        colors = [_C["gap_pos"] if v >= 0 else _C["gap_neg"] for v in gap_pct_df["gap_pct"]]
        fig2.add_trace(go.Bar(
            x=gap_pct_df["bucket"], y=gap_pct_df["gap_pct"],
            marker_color=colors, name="Gap % of Assets",
        ))
        fig2.add_trace(go.Scatter(
            x=gap_pct_df["bucket"], y=gap_pct_df["cum_gap_pct"],
            mode="lines+markers", name="Cumulative %",
            line=dict(color="#FF6B35", width=2),
        ))
        fig2.update_layout(**_LAYOUT, height=350, title="Gap as % of Total Assets",
                          yaxis_title="% of Assets")
        st.plotly_chart(fig2, use_container_width=True)

    # Data table
    with st.expander("Repricing Gap Data"):
        display_df = gap_df.copy()
        display_df["cum_gap_mn"] = display_df["gap_mn"].cumsum()
        st.dataframe(display_df.style.format({
            "assets_mn": "{:,.0f}", "liabilities_mn": "{:,.0f}",
            "gap_mn": "{:+,.0f}", "cum_gap_mn": "{:+,.0f}",
        }), use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: SENSITIVITY
# ═════════════════════════════════════════════════════════════════════════════

def _render_sensitivity(con):
    from pakfindata.db.repositories.alm import get_sensitivity
    from pakfindata.engine.alm_engine import run_sensitivity

    _section("NII / EVE SENSITIVITY ANALYSIS")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Run Sensitivity", type="primary", use_container_width=True):
            with st.spinner("Computing scenarios..."):
                results = run_sensitivity(con)
                st.success(f"Computed {len(results)} scenarios")
                st.rerun()

    sens_df = get_sensitivity(con)
    if sens_df.empty:
        st.info("No sensitivity results. Click **Run Sensitivity** to compute (requires positions).")
        return

    # NII Impact bar chart
    fig = make_subplots(rows=1, cols=2, subplot_titles=("NII Impact (PKR Mn)", "EVE Impact (PKR Mn)"))

    nii_colors = [_C["up"] if v >= 0 else _C["down"] for v in sens_df["nii_impact_mn"]]
    fig.add_trace(go.Bar(
        x=sens_df["scenario"], y=sens_df["nii_impact_mn"],
        marker_color=nii_colors, name="NII Impact",
        text=[f"{v:+,.0f}" for v in sens_df["nii_impact_mn"]], textposition="outside",
    ), row=1, col=1)

    eve_colors = [_C["up"] if v >= 0 else _C["down"] for v in sens_df["eve_impact_mn"]]
    fig.add_trace(go.Bar(
        x=sens_df["scenario"], y=sens_df["eve_impact_mn"],
        marker_color=eve_colors, name="EVE Impact",
        text=[f"{v:+,.0f}" for v in sens_df["eve_impact_mn"]], textposition="outside",
    ), row=1, col=2)

    fig.update_layout(**_LAYOUT, height=450, showlegend=False,
                     title="Interest Rate Sensitivity — Parallel Shift Scenarios")
    st.plotly_chart(fig, use_container_width=True)

    # Duration gap card
    if "duration_gap" in sens_df.columns:
        dur_gap = sens_df["duration_gap"].iloc[0]
        c1, c2, c3 = st.columns(3)
        with c1:
            _card("Duration Gap", f"{dur_gap:.2f} years",
                  color=_C["up"] if abs(dur_gap) < 1 else _C["down"])
        with c2:
            base_nii = sens_df["nii_base_mn"].iloc[0]
            _card("Base Annual NII", f"PKR {base_nii:,.0f} Mn", color=_C["nii"])
        with c3:
            base_eve = sens_df["eve_base_mn"].iloc[0]
            _card("Economic Value of Equity", f"PKR {base_eve:,.0f} Mn", color=_C["eve"])

    with st.expander("Sensitivity Data"):
        st.dataframe(sens_df.style.format({
            "nii_base_mn": "{:,.0f}", "nii_shocked_mn": "{:,.0f}",
            "nii_impact_mn": "{:+,.0f}", "nii_pct_change": "{:+.2f}%",
            "eve_base_mn": "{:,.0f}", "eve_shocked_mn": "{:,.0f}",
            "eve_impact_mn": "{:+,.0f}", "eve_pct_change": "{:+.2f}%",
            "duration_gap": "{:.4f}",
        }), use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: LIQUIDITY
# ═════════════════════════════════════════════════════════════════════════════

def _render_liquidity(con):
    from pakfindata.db.repositories.alm import get_liquidity_ladder
    from pakfindata.engine.alm_engine import compute_liquidity_ladder

    _section("LIQUIDITY MATURITY LADDER")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Compute Ladder", type="primary", use_container_width=True):
            with st.spinner("Computing liquidity ladder..."):
                results = compute_liquidity_ladder(con)
                st.success(f"Computed {len(results)} buckets")
                st.rerun()

    liq_df = get_liquidity_ladder(con)
    if liq_df.empty:
        st.info("No liquidity data. Click **Compute Ladder** (requires positions).")
        return

    # Inflows vs Outflows bar + cumulative gap line
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Inflows (Assets maturing)", x=liq_df["bucket"], y=liq_df["inflows_mn"],
        marker_color=_C["inflow"], opacity=0.85,
    ))
    fig.add_trace(go.Bar(
        name="Outflows (Liabs maturing)", x=liq_df["bucket"], y=liq_df["outflows_mn"],
        marker_color=_C["outflow"], opacity=0.85,
    ))
    fig.add_trace(go.Scatter(
        name="Cumulative Net Gap", x=liq_df["bucket"], y=liq_df["cumulative_gap_mn"],
        mode="lines+markers", line=dict(color=_C["accent"], width=3),
        marker=dict(size=8),
    ))
    fig.add_trace(go.Bar(
        name="HQLA", x=liq_df["bucket"], y=liq_df["hqla_mn"],
        marker_color=_C["hqla"], opacity=0.5,
    ))

    fig.update_layout(**_LAYOUT, height=450, barmode="group",
                     title="Maturity Ladder — Cash Inflows vs Outflows",
                     yaxis_title="PKR Millions")
    st.plotly_chart(fig, use_container_width=True)

    # LCR proxy
    if liq_df["lcr_pct"].notna().any():
        lcr = liq_df["lcr_pct"].iloc[0]
        c1, c2, c3 = st.columns(3)
        with c1:
            color = _C["up"] if lcr and lcr >= 100 else _C["down"]
            _card("LCR Proxy", f"{lcr:.0f}%" if lcr else "N/A", color=color)
        with c2:
            total_hqla = liq_df["hqla_mn"].sum()
            _card("Total HQLA", f"PKR {total_hqla:,.0f} Mn", color=_C["hqla"])
        with c3:
            cum_gap = liq_df["cumulative_gap_mn"].iloc[-1]
            _card("Total Cumulative Gap", f"PKR {cum_gap:+,.0f} Mn",
                  color=_C["up"] if cum_gap >= 0 else _C["down"])

    with st.expander("Liquidity Ladder Data"):
        st.dataframe(liq_df.style.format({
            "inflows_mn": "{:,.0f}", "outflows_mn": "{:,.0f}",
            "net_gap_mn": "{:+,.0f}", "cumulative_gap_mn": "{:+,.0f}",
            "hqla_mn": "{:,.0f}", "lcr_pct": "{:.0f}%",
        }), use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: PRODUCTS
# ═════════════════════════════════════════════════════════════════════════════

def _render_products(con):
    from pakfindata.db.repositories.alm import get_alm_products, seed_default_products, upsert_alm_product

    _section("ALM PRODUCT CATALOG")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Seed Default Products", type="secondary", use_container_width=True):
            count = seed_default_products(con)
            st.success(f"Seeded {count} products")
            st.rerun()

    products = get_alm_products(con, active_only=False)
    if products.empty:
        st.info("No products configured. Click **Seed Default Products** to load a typical Pakistani bank product set.")
        return

    # Summary cards
    assets = products[products["asset_liability"] == "A"]
    liabs = products[products["asset_liability"] == "L"]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _card("Total Products", str(len(products)), color=_C["accent"])
    with c2:
        _card("Asset Products", str(len(assets)), color=_C["asset"])
    with c3:
        _card("Liability Products", str(len(liabs)), color=_C["liability"])
    with c4:
        islamic = products[products["is_islamic"] == 1]
        _card("Islamic Products", str(len(islamic)), color="#2ECC71")

    # Product tables
    for al, label, color in [("A", "ASSET PRODUCTS", _C["asset"]), ("L", "LIABILITY PRODUCTS", _C["liability"])]:
        _section(label)
        df = products[products["asset_liability"] == al][[
            "product_code", "product_name", "product_type", "rate_type",
            "reference_rate", "spread_bps", "repricing_freq_months",
            "contractual_maturity_months", "category", "currency", "is_islamic",
        ]]
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Add product form
    with st.expander("Add / Edit Product"):
        with st.form("add_product"):
            c1, c2, c3 = st.columns(3)
            with c1:
                code = st.text_input("Product Code", placeholder="CORP_KIBOR3M")
                name = st.text_input("Product Name", placeholder="Corporate Loan KIBOR+3M")
                ptype = st.selectbox("Product Type", ["deposit", "loan", "investment", "borrowing", "equity"])
            with c2:
                al = st.selectbox("Asset/Liability", ["A", "L"])
                rtype = st.selectbox("Rate Type", ["fixed", "floating", "administered", "zero"])
                ref = st.text_input("Reference Rate", placeholder="KIBOR_3M, PKRV, KONIA, SOFR")
            with c3:
                spread = st.number_input("Spread (bps)", value=0)
                reprice = st.number_input("Repricing Freq (months)", value=0, min_value=0)
                mat = st.number_input("Contractual Maturity (months)", value=12, min_value=0)

            category = st.selectbox("Category", ["CASA", "TDR", "CORPORATE", "SME", "CONSUMER", "SLR", "INTERBANK"])
            submitted = st.form_submit_button("Save Product")

            if submitted and code and name:
                upsert_alm_product(con, {
                    "product_code": code, "product_name": name,
                    "product_type": ptype, "asset_liability": al,
                    "rate_type": rtype, "reference_rate": ref or None,
                    "spread_bps": spread,
                    "repricing_freq_months": reprice if reprice > 0 else None,
                    "contractual_maturity_months": mat if mat > 0 else None,
                    "category": category,
                })
                st.success(f"Saved product: {code}")
                st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# TAB: POSITIONS
# ═════════════════════════════════════════════════════════════════════════════

def _render_positions(con):
    from pakfindata.db.repositories.alm import get_alm_products, get_alm_positions, upsert_alm_position

    _section("BALANCE SHEET POSITIONS")

    products = get_alm_products(con)
    if products.empty:
        st.warning("No products configured. Go to Products tab first.")
        return

    # Upload CSV
    st.markdown(f"""
    <div style="background:{_C['card_bg']};border:1px solid {_C['grid']};
         padding:12px;border-radius:6px;margin-bottom:12px">
      <div style="color:{_C['accent']};font-weight:600;font-size:13px">CSV Upload Format</div>
      <div style="color:{_C['text_dim']};font-size:12px;margin-top:4px">
        Columns: <code>product_code, bucket, outstanding_mn, weighted_avg_rate</code><br>
        Buckets: ON, 1D-1M, 1M-3M, 3M-6M, 6M-1Y, 1Y-2Y, 2Y-3Y, 3Y-5Y, 5Y-10Y, 10Y+
      </div>
    </div>""", unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader("Upload Position CSV", type=["csv"])
    with col2:
        as_of = st.date_input("As-of Date", value=datetime.now().date())

    if uploaded:
        try:
            df = pd.read_csv(uploaded)
            required = {"product_code", "bucket", "outstanding_mn"}
            if not required.issubset(set(df.columns)):
                st.error(f"CSV must have columns: {required}")
            else:
                st.dataframe(df.head(20), use_container_width=True)
                if st.button("Load Positions", type="primary"):
                    count = 0
                    date_str = as_of.strftime("%Y-%m-%d")
                    for _, row in df.iterrows():
                        data = {
                            "as_of_date": date_str,
                            "product_code": row["product_code"],
                            "bucket": row["bucket"],
                            "outstanding_mn": row["outstanding_mn"],
                            "weighted_avg_rate": row.get("weighted_avg_rate"),
                            "num_accounts": row.get("num_accounts"),
                            "avg_remaining_mat_months": row.get("avg_remaining_mat_months"),
                        }
                        if upsert_alm_position(con, data):
                            count += 1
                    st.success(f"Loaded {count} position rows for {date_str}")
                    st.rerun()
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

    # Manual entry
    with st.expander("Manual Position Entry"):
        with st.form("manual_position"):
            c1, c2, c3 = st.columns(3)
            with c1:
                prod_code = st.selectbox("Product", products["product_code"].tolist())
                bucket = st.selectbox("Bucket", BUCKETS_ORDER)
            with c2:
                amt = st.number_input("Outstanding (PKR Mn)", value=0.0, step=100.0)
                rate = st.number_input("Weighted Avg Rate (%)", value=0.0, step=0.1)
            with c3:
                n_acc = st.number_input("# Accounts", value=0, step=1)
                rem_mat = st.number_input("Avg Remaining Maturity (months)", value=0.0, step=1.0)

            if st.form_submit_button("Add Position"):
                date_str = as_of.strftime("%Y-%m-%d")
                upsert_alm_position(con, {
                    "as_of_date": date_str,
                    "product_code": prod_code,
                    "bucket": bucket,
                    "outstanding_mn": amt,
                    "weighted_avg_rate": rate if rate > 0 else None,
                    "num_accounts": n_acc if n_acc > 0 else None,
                    "avg_remaining_mat_months": rem_mat if rem_mat > 0 else None,
                })
                st.success(f"Added {prod_code} / {bucket}: PKR {amt:,.0f} Mn")
                st.rerun()

    # Show current positions
    positions = get_alm_positions(con)
    if not positions.empty:
        _section("CURRENT POSITIONS")
        display = positions[[
            "product_code", "product_name", "asset_liability", "bucket",
            "outstanding_mn", "weighted_avg_rate", "category",
        ]]
        st.dataframe(display.style.format({
            "outstanding_mn": "{:,.0f}", "weighted_avg_rate": "{:.2f}",
        }), use_container_width=True, hide_index=True)

    # Sample data generator
    with st.expander("Generate Sample Positions (Demo Data)"):
        st.markdown("Generate realistic sample positions for demo/testing purposes.")
        if st.button("Generate Sample Data", type="secondary"):
            _generate_sample_positions(con, products, as_of.strftime("%Y-%m-%d"))
            st.success("Sample positions generated!")
            st.rerun()


def _generate_sample_positions(con, products: pd.DataFrame, as_of_date: str):
    """Generate realistic sample balance sheet positions for demo."""
    from pakfindata.db.repositories.alm import upsert_alm_position

    np.random.seed(42)
    bucket_weights = {
        "ON": 0.05, "1D-1M": 0.10, "1M-3M": 0.15, "3M-6M": 0.15,
        "6M-1Y": 0.20, "1Y-2Y": 0.12, "2Y-3Y": 0.08, "3Y-5Y": 0.07,
        "5Y-10Y": 0.05, "10Y+": 0.03,
    }

    # Assign base amounts per product (scaled to a mid-size Pakistani bank ~ PKR 1.5T total assets)
    product_amounts = {
        # Liabilities
        "CASA_CURRENT": 250000, "CASA_SAVINGS": 350000,
        "TDR_1M": 50000, "TDR_3M": 80000, "TDR_6M": 100000,
        "TDR_1Y": 120000, "COI_5Y": 30000,
        "REPO_ON": 40000, "INTERBANK_CALL": 20000,
        "FCY_DEPOSIT_USD": 15000,
        # Assets
        "CORP_KIBOR3M": 200000, "CORP_KIBOR6M": 150000,
        "SME_KIBOR3M": 80000, "CONSUMER_FIXED": 60000,
        "AGRI_KIBOR3M": 40000,
        "TBILL_3M": 100000, "TBILL_6M": 80000, "TBILL_12M": 60000,
        "PIB_3Y": 70000, "PIB_5Y": 50000, "PIB_10Y": 40000,
        "GIS_3Y": 20000,
        "INTERBANK_PLACE": 30000, "REPO_LEND_ON": 25000,
        "NPC_PKR_1Y": 10000,
    }

    # Simulated customer rates
    base_rate = 12.0  # approximate current KIBOR level
    product_rates = {
        "CASA_CURRENT": 0.0, "CASA_SAVINGS": 7.5,
        "TDR_1M": 10.5, "TDR_3M": 11.0, "TDR_6M": 11.5,
        "TDR_1Y": 12.0, "COI_5Y": 12.5,
        "REPO_ON": 11.8, "INTERBANK_CALL": 11.9,
        "FCY_DEPOSIT_USD": 4.5,
        "CORP_KIBOR3M": 14.0, "CORP_KIBOR6M": 14.5,
        "SME_KIBOR3M": 16.0, "CONSUMER_FIXED": 17.0,
        "AGRI_KIBOR3M": 15.5,
        "TBILL_3M": 11.5, "TBILL_6M": 11.8, "TBILL_12M": 12.0,
        "PIB_3Y": 12.5, "PIB_5Y": 13.0, "PIB_10Y": 13.5,
        "GIS_3Y": 12.0,
        "INTERBANK_PLACE": 11.9, "REPO_LEND_ON": 11.8,
        "NPC_PKR_1Y": 12.5,
    }

    for _, prod in products.iterrows():
        pc = prod["product_code"]
        total = product_amounts.get(pc, 10000)
        rate = product_rates.get(pc, base_rate)

        # Distribute across buckets based on product characteristics
        mat = prod.get("contractual_maturity_months") or prod.get("behavioral_maturity_months") or 12
        for bucket, weight in bucket_weights.items():
            # Concentrate amount near the product's maturity bucket
            bucket_mid = {"ON": 0.03, "1D-1M": 0.5, "1M-3M": 2, "3M-6M": 4.5,
                         "6M-1Y": 9, "1Y-2Y": 18, "2Y-3Y": 30, "3Y-5Y": 48,
                         "5Y-10Y": 90, "10Y+": 180}.get(bucket, 6)
            # Gaussian weighting around maturity
            dist = abs(bucket_mid - mat)
            w = np.exp(-dist**2 / (2 * (mat * 0.5 + 1)**2))
            amt = total * w * (1 + np.random.uniform(-0.1, 0.1))
            amt = max(0, round(amt, 0))

            if amt > 0:
                upsert_alm_position(con, {
                    "as_of_date": as_of_date,
                    "product_code": pc,
                    "bucket": bucket,
                    "outstanding_mn": amt,
                    "weighted_avg_rate": rate + np.random.uniform(-0.3, 0.3),
                    "num_accounts": int(amt * np.random.uniform(5, 50)),
                    "avg_remaining_mat_months": bucket_mid + np.random.uniform(-1, 1),
                })
