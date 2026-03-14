"""Symbol Financials — Balance Sheet + P&L viewer per symbol.

Browse imported financial statement data (from PDF parsing) for any PSX symbol.
Shows P&L trends, Balance Sheet composition, key ratios, and period comparison.
Banks get ALM-specific views (NII decomposition, asset mix, deposits).
"""

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM (Bloomberg-style)
# ═════════════════════════════════════════════════════════════════════════════

_C = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "revenue": "#4ECDC4", "profit": "#00E676",
    "assets": "#3498DB", "liabilities": "#E74C3C", "equity": "#2ECC71",
    "nii": "#FF6B35", "non_nii": "#9B59B6", "provisions": "#E67E22",
    "bg": "#0e1117", "card_bg": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "text_dim": "#888888",
}

_CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_C["text"], size=11),
    xaxis=dict(gridcolor=_C["grid"], zeroline=False),
    yaxis=dict(gridcolor=_C["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=10, r=10, t=40, b=10),
)


def _card(label, value, delta=None, color=_C["accent"]):
    delta_html = ""
    if delta is not None:
        dc = _C["up"] if delta > 0 else _C["down"]
        delta_html = f'<div style="color:{dc};font-size:0.75rem;">{delta:+.1f}%</div>'
    st.markdown(f"""
    <div style="background:{_C['card_bg']};border:1px solid {color}44;
    border-radius:6px;padding:0.8rem;text-align:center;">
    <div style="color:{_C['text_dim']};font-size:0.7rem;text-transform:uppercase;">{label}</div>
    <div style="color:{color};font-size:1.4rem;font-weight:700;">{value}</div>
    {delta_html}</div>""", unsafe_allow_html=True)


def _fmt_b(v):
    """Format number as Billions."""
    if v is None or v == 0:
        return "N/A"
    b = abs(v) / 1e9
    sign = "-" if v < 0 else ""
    if b >= 1:
        return f"{sign}{b:,.1f}B"
    return f"{sign}{abs(v)/1e6:,.0f}M"


def _fmt_m(v):
    """Format number as Millions."""
    if v is None or v == 0:
        return "N/A"
    return f"{v/1e6:,.0f}M"


def _pct_change(curr, prev):
    if not curr or not prev or prev == 0:
        return None
    return ((curr - prev) / abs(prev)) * 100


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def render():
    st.markdown(
        '<h2 style="color:#00D4AA;margin-bottom:0;">SYMBOL FINANCIALS</h2>'
        '<p style="color:#888;margin-top:0;">Balance Sheet &amp; P&amp;L from PDF Reports</p>',
        unsafe_allow_html=True,
    )

    con = get_connection()

    # Symbol selector
    symbols = [r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM company_financials ORDER BY symbol"
    ).fetchall()]

    if not symbols:
        st.warning("No financial data imported yet. Use `pfsync company import-financials --all` to import PDFs.")
        return

    col1, col2, col3 = st.columns([2, 2, 4])
    with col1:
        symbol = st.selectbox("Symbol", symbols, index=0)
    with col2:
        period_filter = st.selectbox("Period", ["All", "Annual", "Quarterly"])

    # Check if bank
    is_bank = False
    bank_row = con.execute(
        "SELECT markup_earned FROM company_financials WHERE symbol = ? AND markup_earned IS NOT NULL LIMIT 1",
        (symbol,),
    ).fetchone()
    if bank_row:
        is_bank = True

    with col3:
        if is_bank:
            st.markdown(f'<div style="background:#1a1a2e;border:1px solid #E74C3C44;border-radius:4px;'
                        f'padding:0.4rem 0.8rem;display:inline-block;margin-top:1.6rem;">'
                        f'<span style="color:#E74C3C;">■</span> '
                        f'<span style="color:{_C["text_dim"]};">BANK</span></div>',
                        unsafe_allow_html=True)

    # Load data
    where = "WHERE symbol = ?"
    params = [symbol]
    if period_filter == "Annual":
        where += " AND period_type = 'annual'"
    elif period_filter == "Quarterly":
        where += " AND period_type = 'quarterly'"

    df = pd.read_sql_query(
        f"SELECT * FROM company_financials {where} ORDER BY period_end DESC",
        con, params=params,
    )

    if df.empty:
        st.info(f"No financial data for {symbol}")
        return

    # Tabs
    if is_bank:
        tabs = st.tabs(["Overview", "P&L Trend", "Balance Sheet", "NII Decomposition", "Ratios", "Raw Data"])
    else:
        tabs = st.tabs(["Overview", "P&L Trend", "Balance Sheet", "Ratios", "Raw Data"])

    tab_idx = 0

    # ── Tab: Overview ───────────────────────────────────────────────────────
    with tabs[tab_idx]:
        _render_overview(df, is_bank, symbol)
    tab_idx += 1

    # ── Tab: P&L Trend ──────────────────────────────────────────────────────
    with tabs[tab_idx]:
        _render_pl_trend(df, is_bank)
    tab_idx += 1

    # ── Tab: Balance Sheet ──────────────────────────────────────────────────
    with tabs[tab_idx]:
        _render_balance_sheet(df, is_bank)
    tab_idx += 1

    # ── Tab: NII Decomposition (banks only) ─────────────────────────────────
    if is_bank:
        with tabs[tab_idx]:
            _render_nii_decomp(df)
        tab_idx += 1

    # ── Tab: Ratios ─────────────────────────────────────────────────────────
    with tabs[tab_idx]:
        _render_ratios(df, is_bank)
    tab_idx += 1

    # ── Tab: Raw Data ───────────────────────────────────────────────────────
    with tabs[tab_idx]:
        _render_raw(df, is_bank)

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB RENDERERS
# ═════════════════════════════════════════════════════════════════════════════

def _render_overview(df, is_bank, symbol):
    latest = df.iloc[0]
    prev = df.iloc[1] if len(df) > 1 else None

    st.markdown(f'<h4 style="color:{_C["accent"]};">{symbol} — Latest: {latest["period_end"]} ({latest["period_type"]})</h4>',
                unsafe_allow_html=True)

    if is_bank:
        c1, c2, c3, c4 = st.columns(4)
        nii = latest.get("net_interest_income") or 0
        pbt = latest.get("profit_before_tax") or 0
        pat = latest.get("profit_after_tax") or 0
        ta = latest.get("total_assets") or 0

        prev_nii = prev.get("net_interest_income") if prev is not None else None
        prev_pbt = prev.get("profit_before_tax") if prev is not None else None

        with c1:
            _card("Net Interest Income", _fmt_b(nii), _pct_change(nii, prev_nii), _C["nii"])
        with c2:
            _card("PBT", _fmt_b(pbt), _pct_change(pbt, prev_pbt), _C["profit"])
        with c3:
            _card("PAT", _fmt_b(pat), color=_C["profit"])
        with c4:
            _card("Total Assets", _fmt_b(ta), color=_C["assets"])

        c5, c6, c7, c8 = st.columns(4)
        me = latest.get("markup_earned") or 0
        mx = latest.get("markup_expensed") or 0
        te = latest.get("total_equity") or 0
        tl = latest.get("total_liabilities") or 0

        with c5:
            _card("Markup Earned", _fmt_b(me), color=_C["revenue"])
        with c6:
            _card("Markup Expensed", _fmt_b(mx), color=_C["liabilities"])
        with c7:
            _card("Total Equity", _fmt_b(te), color=_C["equity"])
        with c8:
            _card("Total Liabilities", _fmt_b(tl), color=_C["liabilities"])
    else:
        c1, c2, c3, c4 = st.columns(4)
        sales = latest.get("sales") or 0
        gp = latest.get("gross_profit") or 0
        pbt = latest.get("profit_before_tax") or 0
        pat = latest.get("profit_after_tax") or 0

        prev_sales = prev.get("sales") if prev is not None else None
        prev_pbt = prev.get("profit_before_tax") if prev is not None else None

        with c1:
            _card("Revenue", _fmt_b(sales), _pct_change(sales, prev_sales), _C["revenue"])
        with c2:
            _card("Gross Profit", _fmt_b(gp), color=_C["profit"])
        with c3:
            _card("PBT", _fmt_b(pbt), _pct_change(pbt, prev_pbt), _C["profit"])
        with c4:
            _card("PAT", _fmt_b(pat), color=_C["profit"])

        c5, c6, c7, c8 = st.columns(4)
        ta = latest.get("total_assets") or 0
        tl = latest.get("total_liabilities") or 0
        te = latest.get("total_equity") or 0
        eps = latest.get("eps") or 0

        with c5:
            _card("Total Assets", _fmt_b(ta), color=_C["assets"])
        with c6:
            _card("Total Liabilities", _fmt_b(tl), color=_C["liabilities"])
        with c7:
            _card("Total Equity", _fmt_b(te), color=_C["equity"])
        with c8:
            val = f"{eps:,.2f}" if eps else "N/A"
            _card("EPS", val, color=_C["accent"])


def _render_pl_trend(df, is_bank):
    dfc = df.sort_values("period_end")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    if is_bank:
        if dfc["markup_earned"].notna().any():
            fig.add_trace(go.Bar(
                x=dfc["period_end"], y=dfc["markup_earned"] / 1e9,
                name="Markup Earned", marker_color=_C["revenue"], opacity=0.7,
            ))
        if dfc["net_interest_income"].notna().any():
            fig.add_trace(go.Scatter(
                x=dfc["period_end"], y=dfc["net_interest_income"] / 1e9,
                name="NII", line=dict(color=_C["nii"], width=3),
            ))
        if dfc["profit_before_tax"].notna().any():
            fig.add_trace(go.Scatter(
                x=dfc["period_end"], y=dfc["profit_before_tax"] / 1e9,
                name="PBT", line=dict(color=_C["profit"], width=2, dash="dot"),
            ), secondary_y=True)
    else:
        if dfc["sales"].notna().any():
            fig.add_trace(go.Bar(
                x=dfc["period_end"], y=dfc["sales"] / 1e9,
                name="Revenue", marker_color=_C["revenue"], opacity=0.7,
            ))
        if dfc["gross_profit"].notna().any():
            fig.add_trace(go.Scatter(
                x=dfc["period_end"], y=dfc["gross_profit"] / 1e9,
                name="Gross Profit", line=dict(color=_C["profit"], width=2),
            ))
        if dfc["profit_before_tax"].notna().any():
            fig.add_trace(go.Scatter(
                x=dfc["period_end"], y=dfc["profit_before_tax"] / 1e9,
                name="PBT", line=dict(color=_C["accent"], width=2, dash="dot"),
            ), secondary_y=True)

    fig.update_layout(**_CHART, title="P&L Trend (PKR Billions)", height=450)
    fig.update_yaxes(title_text="PKR (B)", secondary_y=False)
    fig.update_yaxes(title_text="PBT (B)", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)


def _render_balance_sheet(df, is_bank):
    dfc = df[df["total_assets"].notna() & (df["total_assets"] > 0)].sort_values("period_end")

    if dfc.empty:
        st.info("No balance sheet data available for this symbol.")
        return

    # Stacked area: Assets vs Liabilities vs Equity
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=dfc["period_end"], y=dfc["total_equity"] / 1e9,
        name="Equity", marker_color=_C["equity"],
    ))
    fig.add_trace(go.Bar(
        x=dfc["period_end"], y=dfc["total_liabilities"] / 1e9,
        name="Liabilities", marker_color=_C["liabilities"],
    ))
    fig.update_layout(**_CHART, title="Balance Sheet Composition (PKR Billions)",
                      barmode="stack", height=400)
    st.plotly_chart(fig, use_container_width=True)

    # Current vs Non-current breakdown
    has_current = dfc["current_assets"].notna().any()
    if has_current:
        st.markdown(f'<h5 style="color:{_C["text_dim"]};">Asset Breakdown</h5>', unsafe_allow_html=True)
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=dfc["period_end"], y=dfc["current_assets"] / 1e9,
            name="Current Assets", marker_color=_C["assets"],
        ))
        fig2.add_trace(go.Bar(
            x=dfc["period_end"], y=dfc["non_current_assets"] / 1e9,
            name="Non-Current Assets", marker_color="#1a6fb5",
        ))
        fig2.update_layout(**_CHART, barmode="stack", height=350)
        st.plotly_chart(fig2, use_container_width=True)


def _render_nii_decomp(df):
    """NII decomposition for banks — markup earned vs expensed."""
    dfc = df[df["markup_earned"].notna()].sort_values("period_end")

    if dfc.empty:
        st.info("No NII decomposition data available.")
        return

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=dfc["period_end"], y=dfc["markup_earned"] / 1e9,
        name="Markup Earned", marker_color=_C["revenue"],
    ))
    fig.add_trace(go.Bar(
        x=dfc["period_end"], y=-(dfc["markup_expensed"].fillna(0)) / 1e9,
        name="Markup Expensed", marker_color=_C["liabilities"],
    ))
    fig.add_trace(go.Scatter(
        x=dfc["period_end"], y=dfc["net_interest_income"] / 1e9,
        name="NII", line=dict(color=_C["nii"], width=3),
    ))
    fig.update_layout(**_CHART, title="NII Decomposition (PKR Billions)",
                      barmode="relative", height=450)
    st.plotly_chart(fig, use_container_width=True)

    # NIM proxy
    has_ta = dfc["total_assets"].notna() & (dfc["total_assets"] > 0)
    if has_ta.any():
        nim_df = dfc[has_ta].copy()
        nim_df["nim_pct"] = (nim_df["net_interest_income"] / nim_df["total_assets"]) * 100

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=nim_df["period_end"], y=nim_df["nim_pct"],
            name="NIM %", line=dict(color=_C["accent"], width=2),
            fill="tozeroy", fillcolor="rgba(0,212,170,0.13)",
        ))
        fig2.update_layout(**_CHART, title="Net Interest Margin (NII/TA %)", height=300)
        st.plotly_chart(fig2, use_container_width=True)


def _render_ratios(df, is_bank):
    dfc = df.sort_values("period_end").copy()
    num_cols = ["total_assets", "total_equity", "total_liabilities",
                "sales", "gross_profit", "profit_after_tax", "net_interest_income"]
    for c in num_cols:
        if c in dfc.columns:
            dfc[c] = pd.to_numeric(dfc[c], errors="coerce")

    ratios = pd.DataFrame({"period": dfc["period_end"]})

    ta = dfc["total_assets"].replace(0, pd.NA)
    te = dfc["total_equity"].replace(0, pd.NA)
    tl = dfc["total_liabilities"].replace(0, pd.NA)

    if is_bank:
        pat = dfc["profit_after_tax"]
        ratios["ROA %"] = (pat / ta * 100).round(2)
        ratios["ROE %"] = (pat / te * 100).round(2)
        ratios["Leverage (TA/TE)"] = (ta / te).round(1)
        nii = dfc["net_interest_income"]
        ratios["NIM %"] = (nii / ta * 100).round(2)
    else:
        sales = dfc["sales"].replace(0, pd.NA)
        gp = dfc["gross_profit"]
        pat = dfc["profit_after_tax"]
        ratios["Gross Margin %"] = (gp / sales * 100).round(1)
        ratios["Net Margin %"] = (pat / sales * 100).round(1)
        ratios["ROA %"] = (pat / ta * 100).round(2)
        ratios["ROE %"] = (pat / te * 100).round(2)
        ratios["D/E"] = (tl / te).round(2)

    ratios = ratios.set_index("period")
    st.dataframe(ratios.T.style.format("{:.2f}", na_rep="—"), use_container_width=True)

    # Chart the key ratios
    fig = go.Figure()
    for col in ratios.columns[:3]:
        vals = ratios[col].dropna()
        if not vals.empty:
            fig.add_trace(go.Scatter(
                x=vals.index, y=vals.values,
                name=col, mode="lines+markers",
            ))
    fig.update_layout(**_CHART, title="Key Ratios Trend", height=350)
    st.plotly_chart(fig, use_container_width=True)


def _render_raw(df, is_bank):
    st.markdown(f'<p style="color:{_C["text_dim"]};">{len(df)} periods loaded</p>',
                unsafe_allow_html=True)

    # Select relevant columns
    if is_bank:
        cols = ["period_end", "period_type", "markup_earned", "markup_expensed",
                "net_interest_income", "non_markup_income", "total_income", "provisions",
                "profit_before_tax", "profit_after_tax", "eps",
                "total_assets", "total_liabilities", "total_equity",
                "current_assets", "non_current_assets",
                "current_liabilities", "non_current_liabilities",
                "cash_and_equivalents", "share_capital", "source"]
    else:
        cols = ["period_end", "period_type", "sales", "cost_of_sales", "gross_profit",
                "operating_expenses", "operating_profit", "finance_cost", "other_income",
                "profit_before_tax", "taxation", "profit_after_tax", "eps",
                "total_assets", "total_liabilities", "total_equity",
                "current_assets", "non_current_assets",
                "current_liabilities", "non_current_liabilities",
                "cash_and_equivalents", "share_capital", "source"]

    display_cols = [c for c in cols if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True, height=500)

    # CSV download
    csv = df[display_cols].to_csv(index=False)
    st.download_button("Download CSV", csv, f"{df.iloc[0]['symbol']}_financials.csv", "text/csv")
