"""Treasury Market Terminal — yield curves, auctions, KIBOR, spreads, policy rate.

Tabs:
  Overview — KPI cards (policy rate, KIBOR, T-Bill, KONIA), rate history overlay
  Yield Curves — PKRV with comparison, PKISRV Islamic, curve evolution 3D
  Auctions — T-Bill/PIB results with charts, bid-cover, yield evolution
  KIBOR — Term structure, history, bid-offer spread
  Spreads — T-Bill 6-12M, KIBOR vs Policy, curve steepness
  Bonds — PKFRV floating rate bonds, FMA prices, maturity schedule
  Sync — All sync/backfill controls
"""

from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.sources.sbp_gsp import GSPScraper
from pakfindata.sources.sbp_rates import SBPRatesScraper
from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
from pakfindata.ui.components.helpers import get_connection, render_ai_commentary, render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "policy": "#E74C3C", "kibor": "#4ECDC4",
    "tbill": "#3498DB", "pib": "#9B59B6", "konia": "#45B7D1",
    "pkrv": "#FF6B35", "pkisrv": "#2ECC71", "pkfrv": "#E67E22",
    "bg": "#0e1117", "card_bg": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "text_dim": "#888888",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    xaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    yaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=10, r=10, t=40, b=10),
)

_TENOR_DAYS = {
    "1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122,
    "6M": 182, "9M": 274, "1Y": 365, "2Y": 730, "3Y": 1095,
    "4Y": 1461, "5Y": 1826, "6Y": 2191, "7Y": 2557, "8Y": 2922,
    "9Y": 3287, "10Y": 3652, "12M": 365, "15Y": 5479, "20Y": 7305,
    "25Y": 9131, "30Y": 10957,
}
_MONTHS_TO_DAYS = {
    1: 30, 2: 60, 3: 91, 4: 122, 6: 182, 9: 274, 12: 365, 24: 730,
    36: 1095, 48: 1461, 60: 1826, 72: 2191, 84: 2557, 96: 2922,
    108: 3287, 120: 3652, 180: 5479, 240: 7305, 300: 9131, 360: 10957,
}
_TENOR_LABELS = {
    1: "1M", 2: "2M", 3: "3M", 4: "4M", 6: "6M", 9: "9M",
    12: "1Y", 24: "2Y", 36: "3Y", 48: "4Y", 60: "5Y",
    72: "6Y", 84: "7Y", 96: "8Y", 108: "9Y", 120: "10Y",
    180: "15Y", 240: "20Y", 300: "25Y", 360: "30Y",
}


def _styled_fig(height=400, **kw):
    return go.Figure(layout={**_CHART_LAYOUT, "height": height, **kw})


def _card(label, value, delta=None, color=None, suffix=""):
    card_bg = _COLORS["card_bg"]
    border = color or _COLORS["accent"]
    dim = _COLORS["text_dim"]
    delta_html = ""
    if delta is not None:
        dc = _COLORS["up"] if delta > 0 else _COLORS["down"] if delta < 0 else _COLORS["neutral"]
        sign = "+" if delta > 0 else ""
        delta_html = f"<span style='color:{dc};font-size:0.85em;'>{sign}{delta:.2f}{suffix}</span>"
    st.markdown(
        f"<div style='background:{card_bg};border-radius:8px;padding:12px 16px;"
        f"border-left:3px solid {border};'>"
        f"<div style='color:{dim};font-size:0.75em;'>{label}</div>"
        f"<div style='font-size:1.3em;font-weight:600;'>{value}</div>"
        f"{delta_html}</div>",
        unsafe_allow_html=True,
    )


def _days_ago(date_str):
    try:
        return (datetime.now() - datetime.strptime(str(date_str)[:10], "%Y-%m-%d")).days
    except (ValueError, TypeError):
        return -1


def _remaining_days(maturity_str):
    try:
        dt = datetime.strptime(str(maturity_str)[:10], "%Y-%m-%d")
        delta = (dt - datetime.now()).days
        return delta if delta > 0 else 0
    except (ValueError, TypeError):
        return None


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def render_treasury_dashboard():
    st.markdown("## Treasury Terminal")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    tabs = st.tabs([
        "Overview", "Yield Curves", "Auctions", "KIBOR", "Spreads", "Bonds", "Sync",
    ])
    renderers = [
        _render_overview, _render_yield_curves, _render_auctions,
        _render_kibor, _render_spreads, _render_bonds, _render_sync,
    ]

    for tab, renderer in zip(tabs, renderers):
        with tab:
            try:
                renderer(con)
            except Exception as e:
                st.error(f"Error: {e}")

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW
# ═════════════════════════════════════════════════════════════════════════════

def _render_overview(con):
    # ── KPI row ──
    mc = st.columns(5)

    policy = con.execute(
        "SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()
    with mc[0]:
        if policy:
            age = _days_ago(policy["rate_date"])
            _card("SBP Policy Rate", f"{policy['policy_rate']:.1f}%", color=_COLORS["policy"])
            st.caption(f"{policy['rate_date']} ({age}d ago)")
        else:
            _card("SBP Policy Rate", "N/A", color=_COLORS["policy"])

    kibor = con.execute(
        "SELECT date, bid, offer FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with mc[1]:
        if kibor:
            _card("KIBOR 3M", f"{kibor['offer']:.2f}%", color=_COLORS["kibor"])
            st.caption(f"Bid: {kibor['bid']:.2f}% | {kibor['date']}")
        else:
            _card("KIBOR 3M", "N/A", color=_COLORS["kibor"])

    tbill = con.execute(
        "SELECT cutoff_yield, auction_date FROM tbill_auctions"
        " WHERE tenor LIKE '%3M%' OR tenor LIKE '%3 M%'"
        " ORDER BY auction_date DESC LIMIT 1"
    ).fetchone()
    with mc[2]:
        if tbill:
            _card("T-Bill 3M", f"{tbill['cutoff_yield']:.2f}%", color=_COLORS["tbill"])
            st.caption(f"Auction: {tbill['auction_date']}")
        else:
            _card("T-Bill 3M", "N/A", color=_COLORS["tbill"])

    konia = con.execute(
        "SELECT rate_pct, date FROM konia_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with mc[3]:
        if konia:
            _card("KONIA (O/N)", f"{konia['rate_pct']:.2f}%", color=_COLORS["konia"])
            st.caption(f"{konia['date']}")
        else:
            _card("KONIA", "N/A", color=_COLORS["konia"])

    # PKRV 10Y
    pkrv10 = con.execute(
        "SELECT yield_pct, date FROM pkrv_daily WHERE tenor_months=120 ORDER BY date DESC LIMIT 1"
    ).fetchone()
    with mc[4]:
        if pkrv10:
            _card("PKRV 10Y", f"{pkrv10['yield_pct']:.2f}%", color=_COLORS["pkrv"])
            st.caption(f"{pkrv10['date']}")
        else:
            _card("PKRV 10Y", "N/A", color=_COLORS["pkrv"])

    # ── Rate history overlay ──
    st.markdown("### Rate History")
    fig = _styled_fig(height=420)
    rate_series = [
        ("sbp_policy_rates", "rate_date", "policy_rate", "Policy Rate", _COLORS["policy"], "lines+markers"),
        ("kibor_daily", "date", "offer", "KIBOR 3M", _COLORS["kibor"], "lines"),
        ("konia_daily", "date", "rate_pct", "KONIA", _COLORS["konia"], "lines"),
    ]
    for table, date_col, val_col, name, color, mode in rate_series:
        where = " WHERE tenor='3M'" if table == "kibor_daily" else ""
        df = pd.read_sql_query(
            f"SELECT {date_col} as date, {val_col} as rate FROM {table}{where} ORDER BY {date_col}",
            con,
        )
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["rate"], mode=mode, name=name,
                line=dict(width=2, color=color,
                          shape="hv" if table == "sbp_policy_rates" else "linear"),
            ))

    # T-Bill cutoff yield
    tb = pd.read_sql_query(
        "SELECT auction_date as date, cutoff_yield as rate FROM tbill_auctions"
        " WHERE tenor='3M' ORDER BY auction_date", con,
    )
    if not tb.empty:
        fig.add_trace(go.Scatter(
            x=tb["date"], y=tb["rate"], mode="markers",
            name="T-Bill 3M (Cutoff)", marker=dict(size=7, color=_COLORS["tbill"], symbol="diamond"),
        ))

    if fig.data:
        fig.update_layout(
            yaxis_title="Rate (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No rate history data available")

    # ── Policy rate timeline ──
    st.markdown("### SBP Policy Rate Timeline")
    pdf = pd.read_sql_query(
        "SELECT rate_date, policy_rate FROM sbp_policy_rates ORDER BY rate_date", con,
    )
    if not pdf.empty:
        fig = _styled_fig(height=300)
        fig.add_trace(go.Scatter(
            x=pdf["rate_date"], y=pdf["policy_rate"],
            mode="lines+markers", name="Policy Rate",
            line=dict(width=3, color=_COLORS["policy"], shape="hv"),
            marker=dict(size=6),
        ))
        for i in range(1, len(pdf)):
            prev, curr = pdf.iloc[i-1]["policy_rate"], pdf.iloc[i]["policy_rate"]
            if prev != curr:
                chg = (curr - prev) * 100
                color = _COLORS["down"] if chg > 0 else _COLORS["up"]
                fig.add_annotation(
                    x=pdf.iloc[i]["rate_date"], y=curr,
                    text=f"{chg:+.0f}bp", showarrow=True, arrowhead=2,
                    font=dict(size=9, color=color), arrowcolor=color,
                )
        fig.update_layout(yaxis_title="Rate (%)")
        st.plotly_chart(fig, use_container_width=True)

    # AI Commentary
    render_ai_commentary(con, "TREASURY")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: YIELD CURVES
# ═════════════════════════════════════════════════════════════════════════════

def _render_yield_curves(con):
    # ── PKRV ──
    st.markdown("### PKRV Yield Curve")
    dates = [r["date"] for r in con.execute(
        "SELECT DISTINCT date FROM pkrv_daily ORDER BY date DESC"
    ).fetchall()]
    if not dates:
        st.info("No PKRV yield curve data. Sync Yield Curves (MUFAP) to fetch.")
        return

    c1, c2 = st.columns(2)
    with c1:
        sel_date = st.selectbox("Curve date", dates, index=0, key="pkrv_d1")
    with c2:
        cmp_date = st.selectbox("Compare with", ["None"] + dates, index=0, key="pkrv_d2")

    df = pd.read_sql_query(
        "SELECT tenor_months, yield_pct, change_bps FROM pkrv_daily"
        " WHERE date=? ORDER BY tenor_months",
        con, params=(sel_date,),
    )
    if df.empty:
        st.info("No yield curve points for selected date")
        return

    fig = _styled_fig(height=420)
    fig.add_trace(go.Scatter(
        x=df["tenor_months"], y=df["yield_pct"],
        mode="lines+markers", name=sel_date,
        line=dict(width=3, color=_COLORS["pkrv"]),
        marker=dict(size=7),
        hovertemplate="%{text}<br>Yield: %{y:.4f}%<extra></extra>",
        text=[f"{_TENOR_LABELS.get(t, f'{t}M')}" for t in df["tenor_months"]],
    ))

    if cmp_date != "None":
        cdf = pd.read_sql_query(
            "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date=? ORDER BY tenor_months",
            con, params=(cmp_date,),
        )
        if not cdf.empty:
            fig.add_trace(go.Scatter(
                x=cdf["tenor_months"], y=cdf["yield_pct"],
                mode="lines+markers", name=cmp_date,
                line=dict(width=2, dash="dash", color=_COLORS["kibor"]),
            ))

    fig.update_layout(
        yaxis_title="Yield (%)",
        xaxis=dict(
            title="Tenor",
            tickmode="array",
            tickvals=df["tenor_months"].tolist(),
            ticktext=[_TENOR_LABELS.get(t, f"{t}M") for t in df["tenor_months"]],
        ),
        legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Change bps bar
    if "change_bps" in df.columns and df["change_bps"].notna().any():
        st.markdown("#### Daily Change (bps)")
        chg_df = df.dropna(subset=["change_bps"])
        if not chg_df.empty:
            fig2 = _styled_fig(height=220)
            bar_colors = [_COLORS["up"] if c >= 0 else _COLORS["down"] for c in chg_df["change_bps"]]
            fig2.add_trace(go.Bar(
                x=[_TENOR_LABELS.get(t, f"{t}M") for t in chg_df["tenor_months"]],
                y=chg_df["change_bps"],
                marker_color=bar_colors,
                text=[f"{c:+.1f}" for c in chg_df["change_bps"]],
                textposition="outside", textfont=dict(size=9),
            ))
            fig2.update_layout(yaxis_title="Change (bps)", showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Curve Data"):
        display = df.copy()
        display["Tenor"] = display["tenor_months"].map(lambda t: _TENOR_LABELS.get(t, f"{t}M"))
        st.dataframe(display[["Tenor", "tenor_months", "yield_pct", "change_bps"]].rename(columns={
            "tenor_months": "Months", "yield_pct": "Yield (%)", "change_bps": "Change (bps)",
        }), use_container_width=True, hide_index=True)

    # ── PKISRV (Islamic) ──
    st.markdown("### PKISRV (Islamic Yield Curve)")
    isrv_count = con.execute("SELECT COUNT(*) FROM pkisrv_daily").fetchone()[0]
    if isrv_count == 0:
        st.info("No PKISRV data. Sync Yield Curves (MUFAP) to fetch.")
        return

    isrv_dates = [r["date"] for r in con.execute(
        "SELECT DISTINCT date FROM pkisrv_daily ORDER BY date DESC"
    ).fetchall()]
    isrv_date = st.selectbox("Islamic curve date", isrv_dates, index=0, key="pkisrv_date")

    idf = pd.read_sql_query(
        "SELECT tenor, yield_pct FROM pkisrv_daily WHERE date=? ORDER BY tenor",
        con, params=(isrv_date,),
    )
    if not idf.empty:
        idf["days"] = idf["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), 9999))
        idf = idf.sort_values("days")

        fig = _styled_fig(height=350)
        fig.add_trace(go.Scatter(
            x=idf["tenor"], y=idf["yield_pct"],
            mode="lines+markers", name=f"PKISRV ({isrv_date})",
            line=dict(width=3, color=_COLORS["pkisrv"]),
            marker=dict(size=7),
        ))

        # Overlay PKRV for comparison
        pkrv_on_date = pd.read_sql_query(
            "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date=? ORDER BY tenor_months",
            con, params=(isrv_date,),
        )
        if not pkrv_on_date.empty:
            pkrv_on_date["tenor"] = pkrv_on_date["tenor_months"].map(
                lambda t: _TENOR_LABELS.get(t, f"{t}M")
            )
            merged = idf.merge(pkrv_on_date[["tenor", "yield_pct"]], on="tenor", suffixes=("_isrv", "_pkrv"))
            if not merged.empty:
                fig.add_trace(go.Scatter(
                    x=merged["tenor"], y=merged["yield_pct_pkrv"],
                    mode="lines+markers", name=f"PKRV ({isrv_date})",
                    line=dict(width=2, dash="dash", color=_COLORS["pkrv"]),
                ))

        fig.update_layout(
            yaxis_title="Yield (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"PKISRV: {isrv_count} records | {len(isrv_dates)} dates")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: AUCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def _render_auctions(con):
    ac1, ac2 = st.columns(2)

    # ── T-Bills ──
    with ac1:
        st.markdown("### T-Bill Auctions")
        tb_count = con.execute("SELECT COUNT(*) FROM tbill_auctions").fetchone()[0]
        if tb_count == 0:
            st.info("No T-Bill data. Sync below.")
        else:
            tenors = [r[0] for r in con.execute(
                "SELECT DISTINCT tenor FROM tbill_auctions ORDER BY tenor"
            ).fetchall()]
            sel_tenor = st.selectbox("Tenor", ["All"] + tenors, key="tb_tenor")
            where = " WHERE tenor=?" if sel_tenor != "All" else ""
            params = (sel_tenor,) if sel_tenor != "All" else ()

            df = pd.read_sql_query(
                f"SELECT auction_date, tenor, cutoff_yield, weighted_avg_yield,"
                f" target_amount_billions, amount_accepted_billions"
                f" FROM tbill_auctions{where} ORDER BY auction_date DESC LIMIT 40",
                con, params=params,
            )

            # Yield evolution chart
            if not df.empty:
                chart_df = df.sort_values("auction_date")
                fig = _styled_fig(height=280)
                fig.add_trace(go.Scatter(
                    x=chart_df["auction_date"], y=chart_df["cutoff_yield"],
                    mode="lines+markers", name="Cutoff Yield",
                    line=dict(width=2, color=_COLORS["tbill"]),
                    marker=dict(size=6),
                ))
                if chart_df["weighted_avg_yield"].notna().any():
                    fig.add_trace(go.Scatter(
                        x=chart_df["auction_date"], y=chart_df["weighted_avg_yield"],
                        mode="lines", name="WA Yield",
                        line=dict(width=1.5, dash="dot", color=_COLORS["konia"]),
                    ))
                fig.update_layout(
                    yaxis_title="Yield (%)",
                    legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)"),
                )
                st.plotly_chart(fig, use_container_width=True)

                st.caption(f"{tb_count} total records")
                st.dataframe(df.rename(columns={
                    "auction_date": "Date", "tenor": "Tenor",
                    "cutoff_yield": "Cutoff %", "weighted_avg_yield": "WA Yield %",
                    "target_amount_billions": "Target (B)", "amount_accepted_billions": "Accepted (B)",
                }), use_container_width=True, hide_index=True)

    # ── PIBs ──
    with ac2:
        st.markdown("### PIB Auctions")
        pib_count = con.execute("SELECT COUNT(*) FROM pib_auctions").fetchone()[0]
        if pib_count == 0:
            st.info("No PIB data. Sync below.")
        else:
            tenors = [r[0] for r in con.execute(
                "SELECT DISTINCT tenor FROM pib_auctions ORDER BY tenor"
            ).fetchall()]
            sel_tenor = st.selectbox("Tenor", ["All"] + tenors, key="pib_tenor")
            where = " WHERE tenor=?" if sel_tenor != "All" else ""
            params = (sel_tenor,) if sel_tenor != "All" else ()

            df = pd.read_sql_query(
                f"SELECT auction_date, tenor, pib_type, cutoff_yield,"
                f" coupon_rate, amount_accepted_billions"
                f" FROM pib_auctions{where} ORDER BY auction_date DESC LIMIT 40",
                con, params=params,
            )

            if not df.empty:
                chart_df = df.sort_values("auction_date")
                fig = _styled_fig(height=280)
                fig.add_trace(go.Scatter(
                    x=chart_df["auction_date"], y=chart_df["cutoff_yield"],
                    mode="lines+markers", name="Cutoff Yield",
                    line=dict(width=2, color=_COLORS["pib"]),
                    marker=dict(size=6),
                ))
                fig.update_layout(
                    yaxis_title="Yield (%)",
                    legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)"),
                )
                st.plotly_chart(fig, use_container_width=True)

                st.caption(f"{pib_count} total records")
                st.dataframe(df.rename(columns={
                    "auction_date": "Date", "tenor": "Tenor", "pib_type": "Type",
                    "cutoff_yield": "Yield %", "coupon_rate": "Coupon %",
                    "amount_accepted_billions": "Accepted (B)",
                }), use_container_width=True, hide_index=True)

    # ── Auction scatter (combined) ──
    st.markdown("### Auction Yield Map")
    combined = pd.read_sql_query(
        """SELECT 'T-Bill' as type, tenor, auction_date, cutoff_yield,
                  target_amount_billions, amount_accepted_billions
           FROM tbill_auctions
           UNION ALL
           SELECT 'PIB', tenor, auction_date, cutoff_yield,
                  target_amount_billions, amount_accepted_billions
           FROM pib_auctions
           ORDER BY auction_date DESC LIMIT 60""",
        con,
    )
    if not combined.empty:
        fig = _styled_fig(height=350)
        for atype, color, sym in [("T-Bill", _COLORS["tbill"], "circle"), ("PIB", _COLORS["pib"], "diamond")]:
            sub = combined[combined["type"] == atype]
            if not sub.empty:
                fig.add_trace(go.Scatter(
                    x=sub["auction_date"], y=sub["cutoff_yield"],
                    mode="markers+text", name=atype,
                    marker=dict(size=10, color=color, symbol=sym),
                    text=sub["tenor"], textposition="top center",
                    textfont=dict(size=8),
                ))
        fig.update_layout(
            yaxis_title="Cutoff Yield (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── GIS Sukuk ──
    gis = pd.read_sql_query(
        "SELECT * FROM gis_auctions ORDER BY auction_date DESC LIMIT 20", con,
    )
    if not gis.empty:
        st.markdown("### GIS Sukuk Auctions")
        st.dataframe(gis, use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4: KIBOR
# ═════════════════════════════════════════════════════════════════════════════

def _render_kibor(con):
    kb_count = con.execute("SELECT COUNT(*) FROM kibor_daily").fetchone()[0]
    if kb_count == 0:
        st.info("No KIBOR history. Use Sync tab to backfill.")
        return

    st.caption(f"{kb_count} total records")

    # ── Term structure chart ──
    st.markdown("### KIBOR Term Structure")
    kdf = pd.read_sql_query(
        "SELECT date, tenor, bid, offer FROM kibor_daily"
        " WHERE tenor IN ('1M','3M','6M','1Y') AND offer IS NOT NULL ORDER BY date",
        con,
    )
    if not kdf.empty:
        fig = _styled_fig(height=380)
        kibor_colors = {"1M": "#FF6B35", "3M": "#4ECDC4", "6M": "#45B7D1", "1Y": "#96CEB4"}
        for tenor in ["1M", "3M", "6M", "1Y"]:
            tdf = kdf[kdf["tenor"] == tenor]
            if not tdf.empty:
                fig.add_trace(go.Scatter(
                    x=tdf["date"], y=tdf["offer"], mode="lines",
                    name=f"KIBOR {tenor}",
                    line=dict(width=2, color=kibor_colors.get(tenor, "#999")),
                ))
        fig.update_layout(
            yaxis_title="Offer Rate (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Bid-Offer spread ──
    st.markdown("### Bid-Offer Spread")
    spread_df = pd.read_sql_query(
        "SELECT date, tenor, offer - bid as spread FROM kibor_daily"
        " WHERE tenor='3M' AND bid IS NOT NULL AND offer IS NOT NULL ORDER BY date",
        con,
    )
    if not spread_df.empty:
        fig = _styled_fig(height=250)
        fig.add_trace(go.Scatter(
            x=spread_df["date"], y=spread_df["spread"],
            mode="lines", name="3M Bid-Offer",
            line=dict(width=2, color="#E67E22"),
            fill="tozeroy", fillcolor="rgba(230,126,34,0.08)",
        ))
        fig.update_layout(yaxis_title="Spread (%)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # ── Latest rates table ──
    st.markdown("### Latest KIBOR Rates")
    latest = pd.read_sql_query(
        """SELECT k.date, k.tenor, k.bid, k.offer
           FROM kibor_daily k
           INNER JOIN (SELECT tenor, MAX(date) as md FROM kibor_daily GROUP BY tenor) m
             ON k.tenor=m.tenor AND k.date=m.md
           ORDER BY k.tenor""",
        con,
    )
    if not latest.empty:
        latest["days"] = latest["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), ""))
        latest["spread"] = (latest["offer"] - latest["bid"]).round(4)
        st.dataframe(latest.rename(columns={
            "date": "Date", "tenor": "Tenor", "days": "Days",
            "bid": "Bid %", "offer": "Offer %", "spread": "Spread",
        }), use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5: SPREADS
# ═════════════════════════════════════════════════════════════════════════════

def _render_spreads(con):
    sc1, sc2 = st.columns(2)

    with sc1:
        st.markdown("### T-Bill 6M vs 12M Spread")
        df = pd.read_sql_query(
            """SELECT a.auction_date as date, a.cutoff_yield as y6, b.cutoff_yield as y12,
                      ROUND(b.cutoff_yield - a.cutoff_yield, 4) as spread
               FROM tbill_auctions a
               JOIN tbill_auctions b ON a.auction_date=b.auction_date
               WHERE a.tenor='6M' AND b.tenor='12M' ORDER BY a.auction_date""",
            con,
        )
        if not df.empty:
            fig = _styled_fig(height=300)
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["spread"], mode="lines+markers",
                name="12M-6M", line=dict(width=2, color=_COLORS["pib"]),
                fill="tozeroy", fillcolor="rgba(155,89,182,0.08)",
            ))
            fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
            fig.update_layout(yaxis_title="Spread (%)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need both 6M and 12M auction data")

    with sc2:
        st.markdown("### KIBOR 3M vs Policy Rate")
        df = pd.read_sql_query(
            """SELECT k.date, k.offer as kibor,
                      (SELECT p.policy_rate FROM sbp_policy_rates p
                       WHERE p.rate_date <= k.date ORDER BY p.rate_date DESC LIMIT 1) as policy
               FROM kibor_daily k
               WHERE k.tenor='3M' AND k.offer IS NOT NULL ORDER BY k.date""",
            con,
        )
        if not df.empty and df["policy"].notna().any():
            df["spread"] = df["kibor"] - df["policy"]
            fig = _styled_fig(height=300)
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["spread"], mode="lines",
                name="KIBOR-Policy", line=dict(width=2, color=_COLORS["pkfrv"]),
                fill="tozeroy", fillcolor="rgba(230,126,34,0.08)",
            ))
            fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
            fig.update_layout(yaxis_title="Spread (%)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No KIBOR-Policy spread data")

    # ── Curve steepness ──
    st.markdown("### Yield Curve Steepness (2Y-10Y)")
    steep_dates = con.execute(
        "SELECT DISTINCT date FROM pkrv_daily ORDER BY date DESC LIMIT 90"
    ).fetchall()
    steepness = []
    for row in steep_dates:
        d = row["date"]
        y2 = con.execute(
            "SELECT yield_pct FROM pkrv_daily WHERE date=? AND tenor_months=24", (d,)
        ).fetchone()
        y10 = con.execute(
            "SELECT yield_pct FROM pkrv_daily WHERE date=? AND tenor_months=120", (d,)
        ).fetchone()
        if y2 and y10:
            steepness.append({"date": d, "steep": y10["yield_pct"] - y2["yield_pct"]})

    if steepness:
        sdf = pd.DataFrame(steepness).sort_values("date")
        fig = _styled_fig(height=280)
        bar_colors = [_COLORS["up"] if s >= 0 else _COLORS["down"] for s in sdf["steep"]]
        fig.add_trace(go.Bar(
            x=sdf["date"], y=sdf["steep"],
            marker_color=bar_colors,
        ))
        fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
        fig.update_layout(yaxis_title="10Y-2Y Spread (%)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        latest_steep = sdf.iloc[-1]["steep"]
        signal = "Normal (Positive)" if latest_steep > 0.1 else "Flat" if abs(latest_steep) <= 0.1 else "Inverted"
        _card("Curve Shape", signal, latest_steep, color=_COLORS["pkrv"], suffix="%")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 6: BONDS
# ═════════════════════════════════════════════════════════════════════════════

def _render_bonds(con):
    st.markdown("### PKFRV (Floating Rate Bonds)")
    frv_count = con.execute("SELECT COUNT(*) FROM pkfrv_daily").fetchone()[0]
    if frv_count == 0:
        st.info("No PKFRV data. Sync Yield Curves (MUFAP) to fetch.")
        return

    dates = [r["date"] for r in con.execute(
        "SELECT DISTINCT date FROM pkfrv_daily ORDER BY date DESC"
    ).fetchall()]
    sel_date = st.selectbox("Valuation date", dates, index=0, key="pkfrv_d")

    df = pd.read_sql_query(
        "SELECT bond_code, issue_date, maturity_date, coupon_frequency, fma_price"
        " FROM pkfrv_daily WHERE date=? ORDER BY bond_code",
        con, params=(sel_date,),
    )
    if df.empty:
        st.info("No bonds for selected date")
        return

    df["remaining_days"] = df["maturity_date"].apply(_remaining_days)

    st.caption(f"{len(df)} bonds on {sel_date} | {frv_count} total records")

    # ── FMA price bar ──
    df_chart = df[df["fma_price"].notna()].copy()
    if not df_chart.empty:
        fig = _styled_fig(height=380)
        # Color by price relative to par
        colors = [_COLORS["up"] if p >= 100 else _COLORS["down"] if p < 98 else _COLORS["accent"]
                  for p in df_chart["fma_price"]]
        fig.add_trace(go.Bar(
            x=df_chart["bond_code"], y=df_chart["fma_price"],
            marker_color=colors,
            customdata=df_chart[["maturity_date", "remaining_days"]].values,
            hovertemplate="<b>%{x}</b><br>FMA: %{y:.4f}<br>"
                          "Maturity: %{customdata[0]}<br>Rem: %{customdata[1]}d<extra></extra>",
        ))
        fig.add_hline(y=100, line_dash="dash", line_color=_COLORS["text_dim"],
                      annotation_text="Par", annotation_position="top right")
        fig.update_layout(
            yaxis_title="FMA Price", showlegend=False,
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Maturity schedule ──
    mat_df = df.dropna(subset=["remaining_days"]).copy()
    if not mat_df.empty and mat_df["remaining_days"].sum() > 0:
        st.markdown("#### Maturity Schedule")
        mat_df = mat_df.sort_values("remaining_days")
        fig = _styled_fig(height=250)
        fig.add_trace(go.Bar(
            y=mat_df["bond_code"], x=mat_df["remaining_days"],
            orientation="h", marker_color=_COLORS["pkfrv"],
            text=[f"{d:,.0f}d" for d in mat_df["remaining_days"]],
            textposition="outside", textfont=dict(size=9),
        ))
        fig.update_layout(xaxis_title="Days to Maturity", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Bond Data"):
        st.dataframe(df.rename(columns={
            "bond_code": "Bond", "issue_date": "Issue",
            "maturity_date": "Maturity", "remaining_days": "Rem. Days",
            "coupon_frequency": "Coupon Freq", "fma_price": "FMA Price",
        }), use_container_width=True, hide_index=True)

    # ── Bond price history ──
    bonds = df["bond_code"].tolist()
    if bonds:
        sel_bond = st.selectbox("Bond history", bonds, key="pkfrv_bond")
        if sel_bond:
            hist = pd.read_sql_query(
                "SELECT date, fma_price FROM pkfrv_daily"
                " WHERE bond_code=? AND fma_price IS NOT NULL ORDER BY date",
                con, params=(sel_bond,),
            )
            if len(hist) > 1:
                fig = _styled_fig(height=280)
                fig.add_trace(go.Scatter(
                    x=hist["date"], y=hist["fma_price"], mode="lines",
                    name=sel_bond, line=dict(width=2, color=_COLORS["pkfrv"]),
                ))
                fig.add_hline(y=100, line_dash="dash", line_color=_COLORS["text_dim"])
                fig.update_layout(yaxis_title="FMA Price")
                st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 7: SYNC
# ═════════════════════════════════════════════════════════════════════════════

def _render_sync(con):
    st.markdown("### Sync Treasury Data")

    st.markdown("##### Daily Sync (SBP PMA page)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Sync T-Bill / PIB", type="primary", key="tsy_sync_treasury"):
            with st.spinner("Syncing treasury auctions from SBP..."):
                try:
                    result = SBPTreasuryScraper().sync_treasury(con)
                    st.success(f"T-Bills: {result['tbills_ok']}, PIBs: {result['pibs_ok']}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")
    with c2:
        if st.button("Sync Rates (KIBOR/PKRV/KONIA)", key="tsy_sync_rates"):
            with st.spinner("Syncing rates from SBP..."):
                try:
                    result = SBPRatesScraper().sync_rates(con)
                    st.success(
                        f"KIBOR: {result['kibor_ok']}, PKRV: {result['pkrv_points']}, "
                        f"KONIA: {'OK' if result['konia_ok'] else 'N/A'}"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")
    with c3:
        if st.button("Sync GIS Sukuk", key="tsy_sync_gis"):
            with st.spinner("Syncing GIS auctions from SBP..."):
                try:
                    result = GSPScraper().sync_gis(con)
                    st.success(f"GIS: {result.get('ok', 0)} synced")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    st.markdown("##### MUFAP Yield Curves (PKRV/PKISRV/PKFRV)")
    if st.button("Sync Yield Curves (MUFAP)", key="tsy_sync_mufap"):
        with st.spinner("Downloading & parsing MUFAP rate files..."):
            try:
                from pakfindata.sources.mufap_rates import download_and_sync
                result = download_and_sync(con)
                st.success(
                    f"New: {result['downloaded']}, Skipped: {result['skipped']} | "
                    f"PKRV: {result['pkrv_records']}, PKISRV: {result['pkisrv_records']}, "
                    f"PKFRV: {result['pkfrv_records']}"
                )
                st.rerun()
            except Exception as e:
                st.error(f"MUFAP sync failed: {e}")

    st.markdown("##### Historical Backfill (SBP PDFs)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Backfill SIR (T-Bill+PIB+KIBOR)", key="tsy_backfill_sir"):
            with st.spinner("Downloading & parsing SIR PDF..."):
                try:
                    from pakfindata.sources.sbp_sir import SBPSirScraper
                    counts = SBPSirScraper().sync_sir(con)
                    st.success(
                        f"T-Bills: {counts['tbills']}, PIBs: {counts['pibs']}, "
                        f"KIBOR: {counts['kibor']}, GIS: {counts['gis']}"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"SIR backfill failed: {e}")
    with c2:
        if st.button("Backfill PIB (2000-Present)", key="tsy_backfill_pib"):
            with st.spinner("Downloading PIB archive PDF..."):
                try:
                    from pakfindata.sources.sbp_pib_archive import SBPPibArchiveScraper
                    counts = SBPPibArchiveScraper().sync_pib_archive(con)
                    st.success(f"PIB: {counts['inserted']}/{counts['total']} inserted")
                    st.rerun()
                except Exception as e:
                    st.error(f"PIB backfill failed: {e}")
    with c3:
        _render_kibor_backfill_button()


def _render_kibor_backfill_button():
    from pakfindata.sources.sbp_kibor_history import (
        is_kibor_history_sync_running,
        read_kibor_sync_progress,
        start_kibor_history_sync,
    )

    if is_kibor_history_sync_running():
        progress = read_kibor_sync_progress()
        if progress:
            pct = progress["current"] / max(progress["total_days"], 1)
            st.progress(pct, text=f"KIBOR: {progress['current_date']} ({progress['records_inserted']} records)")
        else:
            st.info("KIBOR history sync running...")
    else:
        progress = read_kibor_sync_progress()
        if progress and progress.get("status") == "completed":
            st.caption(f"Last: {progress['records_inserted']} records, {progress['dates_processed']} dates")

        start_year = st.number_input(
            "Start year", min_value=2008, max_value=2026, value=2024,
            key="kibor_start_year",
        )
        if st.button("Backfill KIBOR (2008-Present)", key="tsy_backfill_kibor"):
            started = start_kibor_history_sync(start_year=int(start_year))
            if started:
                st.success("KIBOR history sync started in background")
                st.rerun()
            else:
                st.warning("KIBOR sync already running")
