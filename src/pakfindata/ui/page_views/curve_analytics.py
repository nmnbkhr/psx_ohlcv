"""Yield Curve Analytics — sovereign curve with synthetic extension.

Reads: sovereign_curve table
Shows: PKISRV, PKRV, PKFRV curves with Linear / Cubic Spline / NSS methods
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import render_footer

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

from pakfindata.engine.curve_analytics import (
    CurveAnalytics,
    FULL_TENORS_LABELS,
    FULL_TENORS_YEARS,
    fit_nss_anchored,
    persist_synthetic_rates,
    rmse_rating,
    source_convergence,
    z_spread,
    fair_value_check,
)

PKT = timezone(timedelta(hours=5))
DB_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")

_C = {
    "bg": "#0B0E11", "card": "#141820", "border": "#1E2530",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "blue": "#2196F3", "purple": "#BB86FC",
    "gold": "#C8A96E",
}


def _con():
    return sqlite3.connect(str(DB_PATH))


def _load_curve(date_str: str, source: str) -> pd.DataFrame:
    con = _con()
    df = pd.read_sql_query(
        "SELECT tenor, days, yield_pct FROM sovereign_curve "
        "WHERE date = ? AND source = ? ORDER BY days",
        con, params=[date_str, source],
    )
    con.close()
    return df


def _available_dates(source: str) -> list[str]:
    con = _con()
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT date FROM sovereign_curve "
        "WHERE source = ? ORDER BY date DESC LIMIT 500",
        (source,),
    ).fetchall()]
    con.close()
    return dates


def _load_auction_anchors(date_str: str) -> dict:
    """Load latest PIB auction cutoffs as NSS anchors."""
    con = _con()
    rows = con.execute(
        "SELECT tenor, days, yield_pct FROM sovereign_curve "
        "WHERE source = 'PIB' AND date >= date(?, '-60 days') AND date <= ? "
        "ORDER BY date DESC",
        (date_str, date_str),
    ).fetchall()
    con.close()
    seen: set = set()
    anchors: dict = {}
    for tenor, days, yield_pct in rows:
        if tenor not in seen and yield_pct > 0:
            anchors[days / 365.25] = yield_pct
            seen.add(tenor)
    return anchors


def _build_excel(full: dict, ca: CurveAnalytics, metrics: dict,
                 source: str, date_str: str) -> bytes | None:
    """Build 3-sheet Excel export (Step 7)."""
    try:
        import io as _io
        official = full["official"]

        # Sheet 1: Yield Curve
        curve_rows = []
        for i, t in enumerate(full["targets"]):
            label = FULL_TENORS_LABELS[i] if i < len(FULL_TENORS_LABELS) else f"{t}Y"
            is_off = t in official
            row = {
                "Tenor": label,
                "Years": t,
                "Days": int(t * 365.25),
                "Type": "Official" if is_off else "Synthetic",
                "Official": official.get(t),
                "Linear": round(full["linear"][i], 4),
                "Spline": round(full["spline"][i], 4),
            }
            if full.get("nss"):
                row["NSS"] = round(full["nss"][i], 4)
            curve_rows.append(row)
        df_curve = pd.DataFrame(curve_rows)

        # Sheet 2: NSS Parameters
        nss_rows = []
        if full.get("nss_params"):
            names = ["b0 (Long-term level)", "b1 (Short-term slope)",
                     "b2 (Curvature 1)", "b3 (Curvature 2)",
                     "t1 (Decay 1)", "t2 (Decay 2)"]
            for name, val in zip(names, full["nss_params"]):
                nss_rows.append({"Parameter": name, "Value": round(val, 6)})
            if full.get("nss_rmse") is not None:
                nss_rows.append({"Parameter": "RMSE (%)", "Value": round(full["nss_rmse"], 6)})
                nss_rows.append({"Parameter": "RMSE (bps)", "Value": round(full["nss_rmse"] * 100, 2)})
        df_nss = pd.DataFrame(nss_rows) if nss_rows else pd.DataFrame({"Note": ["NSS not fitted"]})

        # Sheet 3: Curve Metrics
        metrics_rows = [{"Metric": k, "Value": v} for k, v in metrics.items()]
        metrics_rows.append({"Metric": "source", "Value": source})
        metrics_rows.append({"Metric": "date", "Value": date_str})
        df_metrics = pd.DataFrame(metrics_rows)

        buf = _io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_curve.to_excel(writer, sheet_name="Yield Curve", index=False)
            df_nss.to_excel(writer, sheet_name="NSS Parameters", index=False)
            df_metrics.to_excel(writer, sheet_name="Curve Metrics", index=False)
        return buf.getvalue()
    except Exception:
        return None


def _available_sources() -> list[str]:
    con = _con()
    sources = [r[0] for r in con.execute(
        "SELECT DISTINCT source FROM sovereign_curve ORDER BY source"
    ).fetchall()]
    con.close()
    return sources


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═══════════════════════════════════════════════════════════════════════════════

def render_curve_analytics():
    st.markdown("## Yield Curve Analytics")
    st.caption("Sovereign yield curve with synthetic extension — Linear, Cubic Spline, NSS")

    sources = _available_sources()
    if not sources:
        st.warning("No curve data. Run `python -m pakfindata.sources.sbp_rates_processor process` first.")
        render_footer()
        return

    # Controls
    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        default_src = "PKRV" if "PKRV" in sources else sources[0]
        source = st.selectbox(
            "Curve", sources,
            index=sources.index(default_src) if default_src in sources else 0,
            key="ca_source",
        )
    with c2:
        dates = _available_dates(source)
        if not dates:
            st.info(f"No data for {source}")
            render_footer()
            return
        date_str = st.selectbox("Date", dates, index=0, key="ca_date")
    with c3:
        method = st.radio(
            "Default Method", ["Spline", "NSS", "Linear"],
            index=0, key="ca_method", horizontal=True,
        )

    # Load official data
    df = _load_curve(date_str, source)
    if df.empty:
        st.warning(f"No {source} data for {date_str}")
        render_footer()
        return

    tenors_years = (df["days"] / 365.25).values
    yields = df["yield_pct"].values
    labels = df["tenor"].values.tolist()

    # Run engine
    ca = CurveAnalytics(tenors_years.tolist(), yields.tolist(), labels)

    # Auction anchoring (Feature 2)
    with st.expander("Advanced Options"):
        use_anchors = st.checkbox(
            "Anchor NSS to SBP auction cutoffs", value=True, key="ca_anchors",
            help="Weight PIB auction results 3x in NSS fitting to prevent tail divergence",
        )
        if use_anchors:
            anchors = _load_auction_anchors(date_str)
            if anchors:
                st.caption(
                    f"Using {len(anchors)} PIB auction anchors: "
                    + ", ".join(f"{k:.0f}Y={v:.2f}%" for k, v in sorted(anchors.items()))
                )
                ca._nss_params = fit_nss_anchored(ca.tenors, ca.yields, anchors)

    full = ca.full_curve()
    metrics = ca.curve_metrics()
    band = ca.confidence_band()

    # RMSE badge (Step 8)
    rmse_bps = round(full["nss_rmse"] * 100, 1) if full.get("nss_rmse") else None
    rmse_label, rmse_color = rmse_rating(rmse_bps) if rmse_bps is not None else ("N/A", _C["dim"])

    # Metrics bar
    cols = st.columns(6)
    cols[0].metric("Level", f"{metrics.get('level', 0):.2f}%")

    slope = metrics.get("slope_2s10s")
    if slope is not None:
        cols[1].metric("2s10s Slope", f"{slope:+.0f} bps",
                       delta_color="inverse" if slope < 0 else "normal")

    butterfly = metrics.get("butterfly_2_5_10")
    if butterfly is not None:
        cols[2].metric("2-5-10 Butterfly", f"{butterfly:+.0f} bps")

    if metrics.get("short_avg"):
        cols[3].metric("Short (<1Y)", f"{metrics['short_avg']:.2f}%")
    if metrics.get("long_avg"):
        cols[4].metric("Long (>10Y)", f"{metrics['long_avg']:.2f}%")

    # RMSE badge
    if rmse_bps is not None:
        cols[5].markdown(
            f"<div style='text-align:center;padding:4px'>"
            f"<span style='color:#666;font-size:10px'>NSS FIT</span><br>"
            f"<span style='color:{rmse_color};font-size:18px;font-weight:bold'>"
            f"{rmse_label}</span><br>"
            f"<span style='color:#888;font-size:11px'>{rmse_bps:.1f} bps</span></div>",
            unsafe_allow_html=True,
        )

    if metrics.get("inverted"):
        st.warning("Curve is INVERTED — short rates above long rates")

    # Illiquidity warning (Step 5)
    if band["max_spread_bps"] > 50:
        st.error(
            f"ILLIQUIDITY WARNING — synthetic rate uncertainty exceeds 50 bps "
            f"(max spread: {band['max_spread_bps']:.0f} bps). "
            f"Long-end estimates are unreliable."
        )

    # Decision-support warnings (Step 9d)
    conf_score = band.get("confidence_score", 100)
    if rmse_bps is not None and rmse_bps < 10 and band["max_spread_bps"] > 200:
        st.error(
            f"Model Divergence: The NSS estimate has decoupled from the Spline/Linear "
            f"trend (>{band['max_spread_bps']:.0f} bps). Long-end estimates are currently "
            f"based on the Cubic Spline method. Cross-reference with the PKRV conventional "
            f"proxy (ghost line) for validation."
        )
    elif rmse_bps is not None and rmse_bps > 20:
        st.error(
            f"NSS fit quality is POOR (RMSE = {rmse_bps:.1f} bps). "
            f"Market data may be too noisy for reliable curve fitting. "
            f"Use Cubic Spline for synthetic rates."
        )

    if conf_score < 40:
        st.warning(
            f"Synthetic Rate Confidence: {conf_score:.0f}/100 ({band['confidence']}). "
            f"The {band['max_spread_bps']:.0f} bps divergence between methods indicates "
            f"high model uncertainty at long tenors. For NAV calculations, consider using "
            f"the PKRV-anchored estimate or flagging the 30Y rate as 'indicative only'."
        )

    if any(band.get("nss_excluded", [])):
        excluded = [f"{t}Y" for t, exc in zip(band["targets"], band["nss_excluded"]) if exc]
        st.caption(
            f"NSS excluded from confidence band at {', '.join(excluded)} "
            f"(>200 bps deviation from Spline — unanchored tail)"
        )

    # Main chart with confidence band
    method_key = {"Spline": "spline", "NSS": "nss", "Linear": "linear"}[method]
    _render_curve_chart(full, ca, method_key, source, date_str, band)

    # Action buttons row
    btn_cols = st.columns([1, 1, 4])
    with btn_cols[0]:
        if st.button("Persist Synthetic", key="ca_persist"):
            result = persist_synthetic_rates(str(DB_PATH), source, date_str)
            if result["status"] == "ok":
                st.success(
                    f"Stored {result['inserted']} synthetic rates as {result['source']} "
                    f"(RMSE: {result.get('nss_rmse_bps', '?')} bps)"
                )
            else:
                st.warning(f"Persist: {result['status']}")
    with btn_cols[1]:
        xlsx_bytes = _build_excel(full, ca, metrics, source, date_str)
        if xlsx_bytes:
            st.download_button(
                "Export Excel", data=xlsx_bytes,
                file_name=f"{source}_curve_{date_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="ca_export",
            )

    st.divider()

    # Tabs
    tab_labels = ["Method Comparison", "Data Table", "Curve History",
                  "Source Convergence", "Z-Spread Calculator", "Sukuk Pricer"]
    if full.get("nss_params"):
        tab_labels.append("NSS Parameters")

    active_tab = st.radio(
        "View", tab_labels, horizontal=True, key="ca_tab",
        label_visibility="collapsed",
    )

    if active_tab == "Method Comparison":
        _render_comparison(full, ca)
    elif active_tab == "Data Table":
        _render_data_table(full, ca, source, date_str)
    elif active_tab == "Curve History":
        _render_history(source)
    elif active_tab == "Source Convergence":
        _render_source_convergence(date_str)
    elif active_tab == "Z-Spread Calculator":
        _render_z_spread(ca, source, date_str)
    elif active_tab == "Sukuk Pricer":
        _render_sukuk_pricer(ca, source, date_str)
    elif active_tab == "NSS Parameters":
        _render_nss_params(full, source)

    render_footer()


# ═══════════════════════════════════════════════════════════════════════════════
# CHART RENDERERS
# ═══════════════════════════════════════════════════════════════════════════════

def _render_curve_chart(full: dict, ca: CurveAnalytics, method: str,
                        source: str, date_str: str, band: dict | None = None):
    fig = go.Figure()

    targets = full["targets"]
    synthetic_yields = full.get(method) or full["spline"]
    official = full["official"]

    # Confidence band (Step 5) — shaded area between methods
    if band and any(s > 0 for s in band["spread_bps"]):
        synth_mask = [w > 0 for w in band["spread_bps"]]
        band_x = [t for t, m in zip(band["targets"], synth_mask) if m]
        band_upper = [u for u, m in zip(band["upper"], synth_mask) if m]
        band_lower = [lo for lo, m in zip(band["lower"], synth_mask) if m]
        band_w = [w for w, m in zip(band["spread_bps"], synth_mask) if m]

        fig.add_trace(go.Scatter(
            x=band_x, y=band_upper, mode="lines", line=dict(width=0),
            showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=band_x, y=band_lower, mode="lines", line=dict(width=0),
            fill="tonexty", fillcolor="rgba(33,150,243,0.15)",
            name="Model Uncertainty",
            hovertemplate="Uncertainty: %{customdata:.1f} bps<extra></extra>",
            customdata=band_w,
        ))

    # PKRV ghost overlay when viewing PKISRV (proxy anchor)
    if source in ("PKISRV", "PKFRV"):
        pkrv_df = _load_curve(date_str, "PKRV")
        if not pkrv_df.empty:
            pkrv_x = (pkrv_df["days"] / 365.25).values
            pkrv_y = pkrv_df["yield_pct"].values
            fig.add_trace(go.Scatter(
                x=pkrv_x, y=pkrv_y, mode="lines",
                name="PKRV (Conventional)",
                line=dict(color="rgba(200,169,110,0.35)", width=1.5, dash="dot"),
                hovertemplate="PKRV %{customdata}<br>%{y:.4f}%<extra></extra>",
                customdata=pkrv_df["tenor"].values,
            ))

    # Official points
    fig.add_trace(go.Scatter(
        x=list(official.keys()), y=list(official.values()),
        mode="markers+lines",
        name=f"{source} (Official)",
        line=dict(color=_C["gold"], width=2),
        marker=dict(size=8, color=_C["gold"]),
    ))

    # Split interpolated vs extrapolated
    min_t, max_t = min(official.keys()), max(official.keys())
    interp_x, interp_y, extrap_x, extrap_y = [], [], [], []

    for t, y in zip(targets, synthetic_yields):
        if min_t <= t <= max_t:
            interp_x.append(t)
            interp_y.append(y)
        else:
            extrap_x.append(t)
            extrap_y.append(y)

    if interp_x:
        fig.add_trace(go.Scatter(
            x=interp_x, y=interp_y, mode="lines",
            name=f"{method.title()} (Interpolated)",
            line=dict(color=_C["cyan"], width=1.5, dash="dot"),
        ))

    if extrap_x:
        fig.add_trace(go.Scatter(
            x=extrap_x, y=extrap_y, mode="lines+markers",
            name=f"{method.title()} (Synthetic)",
            line=dict(color=_C["blue"], width=2, dash="dash"),
            marker=dict(size=6, symbol="diamond", color=_C["blue"]),
        ))

    tick_vals = [0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30]
    tick_text = ["3M", "6M", "1Y", "2Y", "3Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]

    fig.update_layout(
        title=f"{source} Yield Curve — {date_str}",
        xaxis=dict(title="Tenor (Years)", tickvals=tick_vals, ticktext=tick_text,
                   gridcolor=_C["border"]),
        yaxis=dict(title="Yield (%)", gridcolor=_C["border"]),
        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
        font_color=_C["dim"], height=420,
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, width='stretch', key="ca_main_chart")

    # Confidence badge with numeric score (Step 9a)
    if band:
        conf_score = band.get("confidence_score", 0)
        conf_label = band.get("confidence", "N/A")
        conf_color = band.get("confidence_color", _C["dim"])
        st.markdown(
            f'<div style="display:inline-flex;align-items:center;gap:8px;padding:3px 10px;'
            f'border:1px solid {conf_color};border-radius:4px;">'
            f'<span style="color:{_C["dim"]};font-size:10px;">Synthetic Confidence</span>'
            f'<span style="color:{conf_color};font-weight:900;font-size:14px;">'
            f'{conf_score:.0f}</span>'
            f'<span style="color:{conf_color};font-size:11px;">{conf_label}</span>'
            f'<span style="color:{_C["dim"]};font-size:9px;">'
            f'(RMSE:{band.get("rmse_penalty",0):.0f} + Spread:{band.get("spread_penalty",0):.0f} penalty)</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # PKRV sanity bound when viewing PKISRV (Step 9b)
    if source in ("PKISRV", "PKFRV") and band:
        pkrv_df = _load_curve(date_str, "PKRV")
        if not pkrv_df.empty:
            pkrv_long = pkrv_df[pkrv_df["days"] >= 10000]
            if not pkrv_long.empty:
                pkrv_30y = pkrv_long.iloc[-1]["yield_pct"]
                idx_30 = full["targets"].index(30) if 30 in full["targets"] else None
                if idx_30 is not None:
                    synth_30y = (full.get(method_key) or full["spline"])[idx_30]
                    spread_to_pkrv = (synth_30y - pkrv_30y) * 100
                    if spread_to_pkrv > 150:
                        pkrv_anchored = pkrv_30y + 0.60
                        st.error(
                            f"Model Divergence: {source} synthetic 30Y ({synth_30y:.2f}%) "
                            f"exceeds PKRV 30Y ({pkrv_30y:.2f}%) by {spread_to_pkrv:.0f} bps. "
                            f"Historical Islamic-Conventional spread is +30 to +80 bps."
                        )
                        st.info(
                            f"PKRV-Anchored Estimate: PKRV 30Y ({pkrv_30y:.2f}%) "
                            f"+ typical Islamic spread (+60 bps) = **{pkrv_anchored:.2f}%**"
                        )
                    elif spread_to_pkrv < -50:
                        st.warning(
                            f"{source} synthetic 30Y ({synth_30y:.2f}%) is "
                            f"{abs(spread_to_pkrv):.0f} bps BELOW PKRV 30Y ({pkrv_30y:.2f}%). "
                            f"Islamic rates below conventional is unusual."
                        )


def _render_comparison(full: dict, ca: CurveAnalytics):
    fig = go.Figure()
    targets = full["targets"]
    official = full["official"]

    fig.add_trace(go.Scatter(
        x=list(official.keys()), y=list(official.values()),
        mode="markers", name="Official",
        marker=dict(size=10, color=_C["gold"], symbol="circle"),
    ))
    fig.add_trace(go.Scatter(
        x=targets, y=full["linear"], mode="lines", name="Linear",
        line=dict(color=_C["dim"], width=1, dash="dot"),
    ))
    fig.add_trace(go.Scatter(
        x=targets, y=full["spline"], mode="lines", name="Cubic Spline",
        line=dict(color=_C["cyan"], width=2),
    ))
    # NSS visual decoupling (Step 9c) — red where >200 bps from Spline
    if full.get("nss"):
        nss_ok_x, nss_ok_y = [], []
        nss_bad_x, nss_bad_y = [], []
        for t, nv, sv in zip(targets, full["nss"], full["spline"]):
            if abs(nv - sv) * 100 <= 200:
                nss_ok_x.append(t)
                nss_ok_y.append(nv)
            else:
                nss_bad_x.append(t)
                nss_bad_y.append(nv)
        if nss_ok_x:
            fig.add_trace(go.Scatter(
                x=nss_ok_x, y=nss_ok_y, mode="lines", name="NSS (Fitted)",
                line=dict(color=_C["purple"], width=1.5, dash="dash"),
            ))
        if nss_bad_x:
            fig.add_trace(go.Scatter(
                x=nss_bad_x, y=nss_bad_y, mode="lines+markers",
                name="NSS (DECOUPLED)",
                line=dict(color="#FF1744", width=1, dash="dot"),
                marker=dict(size=5, symbol="x", color="#FF1744"),
                hovertemplate="NSS DECOUPLED<br>%{y:.4f}% (unreliable)<extra></extra>",
            ))

    tick_vals = [0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30]
    tick_text = ["3M", "6M", "1Y", "2Y", "3Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]

    fig.update_layout(
        title="Method Comparison",
        xaxis=dict(tickvals=tick_vals, ticktext=tick_text, gridcolor=_C["border"]),
        yaxis=dict(title="Yield (%)", gridcolor=_C["border"]),
        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
        font_color=_C["dim"], height=380,
    )
    st.plotly_chart(fig, width='stretch', key="ca_comparison")

    # Synthetic tenors table
    st.markdown("**Synthetic Rate Estimates**")
    rows = []
    for i, t in enumerate(targets):
        if t not in official:
            label = FULL_TENORS_LABELS[i] if i < len(FULL_TENORS_LABELS) else f"{t}Y"
            row = {"Tenor": label, "Years": t}
            row["Linear"] = f"{full['linear'][i]:.4f}%"
            row["Spline"] = f"{full['spline'][i]:.4f}%"
            if full.get("nss"):
                row["NSS"] = f"{full['nss'][i]:.4f}%"
            spread = abs(full["spline"][i] - full["linear"][i]) * 100
            row["Spread (bps)"] = f"{spread:.1f}"
            rows.append(row)

    if rows:
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)


def _render_data_table(full: dict, ca: CurveAnalytics, source: str, date_str: str):
    official = full["official"]
    rows = []
    for i, t in enumerate(full["targets"]):
        label = FULL_TENORS_LABELS[i] if i < len(FULL_TENORS_LABELS) else f"{t}Y"
        is_off = t in official
        yield_val = official.get(t, full["spline"][i]) if full.get("spline") else official.get(t, 0)

        rows.append({
            "Tenor": label,
            "Years": t,
            "Days": int(t * 365.25),
            "Yield (%)": round(yield_val, 4),
            "Type": "Official" if is_off else "Synthetic*",
            "Method": "---" if is_off else "Spline",
        })

    st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)
    st.caption("*Synthetic rates — estimated via Cubic Spline interpolation/extrapolation")


def _render_history(source: str):
    con = _con()
    tenors = ["2Y", "5Y", "10Y", "15Y", "20Y", "25Y", "30Y"]
    colors = {"2Y": _C["cyan"], "5Y": _C["blue"], "10Y": _C["gold"],
              "15Y": _C["purple"], "20Y": _C["down"],
              "25Y": "#FF9800", "30Y": "#E91E63"}

    fig = go.Figure()
    syn_source = f"{source}_SYN"
    for tenor in tenors:
        # Query both official and synthetic sources
        df = pd.read_sql_query(
            "SELECT date, yield_pct FROM sovereign_curve "
            "WHERE (source = ? OR source = ?) AND tenor = ? "
            "ORDER BY date DESC LIMIT 500",
            con, params=[source, syn_source, tenor],
        )
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["yield_pct"], mode="lines", name=tenor,
            line=dict(color=colors.get(tenor, "#888"), width=1.5),
        ))

    con.close()
    fig.update_layout(
        title=f"{source} Rate History by Tenor",
        xaxis=dict(gridcolor=_C["border"]),
        yaxis=dict(title="Yield (%)", gridcolor=_C["border"]),
        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
        font_color=_C["dim"], height=380,
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, width='stretch', key="ca_history")


def _render_nss_params(full: dict, source: str = "PKRV"):
    params = full.get("nss_params")
    if not params:
        st.info("NSS model could not be fitted to this data")
        return

    b0, b1, b2, b3, tau1, tau2 = params
    rmse = full.get("nss_rmse", 0)
    rmse_bps = rmse * 100 if rmse else 0
    label, color = rmse_rating(rmse_bps)

    st.markdown("**Nelson-Siegel-Svensson Parameters**")
    p1, p2, p3 = st.columns(3)
    p1.metric("b0 (Long-term level)", f"{b0:.4f}%")
    p2.metric("b1 (Short-term slope)", f"{b1:.4f}")
    p3.markdown(
        f"<div style='text-align:center;padding:8px'>"
        f"<span style='color:#666;font-size:11px'>RMSE</span><br>"
        f"<span style='color:{color};font-size:22px;font-weight:bold'>"
        f"{rmse_bps:.1f} bps</span><br>"
        f"<span style='color:{color};font-size:12px'>{label}</span></div>",
        unsafe_allow_html=True,
    )

    p4, p5, p6 = st.columns(3)
    p4.metric("b2 (Curvature 1)", f"{b2:.4f}")
    p5.metric("b3 (Curvature 2)", f"{b3:.4f}")
    p6.metric("t1 / t2", f"{tau1:.2f} / {tau2:.2f}")

    st.markdown("""
    **Interpretation:**
    - **b0** = long-run asymptotic yield (where the curve flattens at very long tenors)
    - **b1** = short-end slope (negative = upward sloping, positive = inverted)
    - **b2, b3** = curvature humps (captures belly convexity)
    - **t1, t2** = decay speeds (smaller = faster decay, affects where humps peak)
    - **RMSE** = model fit error (lower = better, <5 bps is excellent)
    """)

    # RMSE History chart (Step 8)
    syn_source = f"{source}_SYN"
    con = _con()
    rmse_hist = pd.read_sql_query(
        "SELECT date, yield_pct as rmse_bps FROM sovereign_curve "
        "WHERE source = ? AND tenor = '_RMSE' ORDER BY date DESC LIMIT 365",
        con, params=[syn_source],
    )
    con.close()

    if not rmse_hist.empty:
        rmse_hist["date"] = pd.to_datetime(rmse_hist["date"])
        rmse_hist = rmse_hist.sort_values("date")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rmse_hist["date"], y=rmse_hist["rmse_bps"],
            mode="lines+markers", name="NSS RMSE",
            line=dict(color=_C["cyan"], width=1.5),
            marker=dict(size=4),
        ))
        # Threshold lines
        fig.add_hline(y=5, line_dash="dot", line_color=_C["up"],
                      annotation_text="Excellent", annotation_position="bottom right")
        fig.add_hline(y=20, line_dash="dot", line_color=_C["down"],
                      annotation_text="Poor", annotation_position="top right")

        fig.update_layout(
            title="NSS RMSE History (bps)",
            xaxis=dict(gridcolor=_C["border"]),
            yaxis=dict(title="RMSE (bps)", gridcolor=_C["border"]),
            paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
            font_color=_C["dim"], height=280,
        )
        st.plotly_chart(fig, width='stretch', key="ca_rmse_hist")


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE CONVERGENCE TAB
# ═══════════════════════════════════════════════════════════════════════════════

def _render_source_convergence(date_str: str):
    st.markdown("**Source Convergence**")
    st.caption("Cross-check: same tenor from MUFAP, SBP auctions, and synthetic models")

    tenor = st.selectbox(
        "Tenor", ["3M", "6M", "12M", "2Y", "3Y", "5Y", "10Y", "15Y", "20Y", "30Y"],
        index=6, key="sc_tenor",
    )

    sources = source_convergence(str(DB_PATH), date_str, tenor)
    if not sources:
        st.info(f"No data for {tenor}")
        return

    status_cfg = {
        "verified": ("OK", "#00E676"), "recent": ("OK", "#00E676"),
        "lagging": ("LAG", "#FFB300"), "stale": ("OLD", "#FF5252"),
        "calculated": ("SYN", "#2196F3"),
    }

    display_names = {
        "PKRV": "MUFAP (PKRV)", "PKISRV": "MUFAP (PKISRV)",
        "PKFRV": "MUFAP (PKFRV)", "MTB": "SBP T-Bill Auction",
        "PIB": "SBP PIB Auction", "KIBOR": "SBP KIBOR",
        "POLICY": "SBP Policy Rate",
    }

    for src in sources:
        icon, color = status_cfg.get(src["status"], ("?", _C["dim"]))
        name = display_names.get(src["source"], src["source"])
        if "_SYN" in src["source"]:
            name = f"Synthetic ({src['source'].replace('_SYN', '')})"
        date_note = f"({src['date']})" if src["days_old"] > 0 else ""

        st.markdown(
            f'<div style="display:flex;align-items:center;gap:12px;padding:6px 0;'
            f'border-bottom:1px solid #1E2530;">'
            f'<span style="width:200px;color:#E0E0E0;font-size:12px;">{name}</span>'
            f'<span style="width:80px;font-weight:700;font-size:14px;color:{color};">'
            f'{src["yield_pct"]:.4f}%</span>'
            f'<span style="font-size:11px;color:{color};">[{icon}] {src["status"].title()} '
            f'{date_note}</span></div>',
            unsafe_allow_html=True,
        )

    verified = [s for s in sources if s["status"] in ("verified", "recent")]
    if len(verified) >= 2:
        yields = [s["yield_pct"] for s in verified]
        spread = (max(yields) - min(yields)) * 100
        label = "Converged" if spread < 20 else "Divergent" if spread < 50 else "Wide divergence"
        st.markdown(
            f'<div style="margin-top:8px;padding:6px 10px;background:rgba(33,150,243,0.1);'
            f'border-radius:4px;font-size:11px;">'
            f'Cross-source spread: <b>{spread:.1f} bps</b> — {label}</div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Z-SPREAD CALCULATOR TAB
# ═══════════════════════════════════════════════════════════════════════════════

def _render_z_spread(ca: CurveAnalytics, source: str, date_str: str):
    st.markdown("**Z-Spread Calculator**")
    st.caption("Calculate the spread a specific bond trades over the sovereign curve")

    full = ca.full_curve()
    try:
        from scipy.interpolate import CubicSpline
        spline_fn = CubicSpline(
            np.array(full["targets"]), np.array(full["spline"]), bc_type="natural",
        )
    except ImportError:
        st.error("scipy required for Z-Spread calculation")
        return

    def sov_yield(t):
        return float(spline_fn(t))

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        bond_name = st.text_input("Bond/Sukuk Name", "Meezan Sukuk", key="zs_name")
    with c2:
        clean_price = st.number_input("Clean Price", value=98.50, step=0.01, key="zs_price")
    with c3:
        coupon = st.number_input("Coupon (%)", value=13.50, step=0.01, key="zs_coupon")
    with c4:
        maturity = st.number_input("Maturity (Yrs)", value=5.0, step=0.5, key="zs_mat")

    freq_opts = [("Semi-Annual", 2), ("Quarterly", 4), ("Annual", 1)]
    freq = st.selectbox("Coupon Frequency", freq_opts, format_func=lambda x: x[0], key="zs_freq")

    if st.button("Calculate Z-Spread", key="zs_calc"):
        z = z_spread(sov_yield, clean_price, coupon, maturity, coupon_freq=freq[1])
        ref_yield = sov_yield(maturity)
        implied_yield = ref_yield + z / 100

        z_color = _C["up"] if z < 100 else _C["amber"] if z < 300 else _C["down"]
        r1, r2, r3 = st.columns(3)
        r1.markdown(
            f'<div style="text-align:center;padding:10px;background:#141820;border-radius:8px;">'
            f'<div style="color:#6B7280;font-size:10px;">Z-SPREAD</div>'
            f'<div style="color:{z_color};font-weight:900;font-size:28px;">{z:.0f} bps</div>'
            f'</div>', unsafe_allow_html=True,
        )
        r2.metric(f"{source} {maturity:.0f}Y Yield", f"{ref_yield:.4f}%")
        r3.metric("Implied Bond Yield", f"{implied_yield:.4f}%")

        if z < 50:
            st.success(f"**{bond_name}** trades near sovereign — very low credit risk")
        elif z < 150:
            st.info(f"**{bond_name}** — moderate credit premium, typical for high-grade corporates")
        elif z < 300:
            st.warning(f"**{bond_name}** — elevated spread, market perceives meaningful credit risk")
        else:
            st.error(f"**{bond_name}** — distressed spread, significant credit concern")


# ═══════════════════════════════════════════════════════════════════════════════
# SUKUK PRICER TAB
# ═══════════════════════════════════════════════════════════════════════════════

def _render_sukuk_pricer(ca: CurveAnalytics, source: str, date_str: str):
    st.markdown("**Sukuk Fair Value Engine**")
    st.caption("Auto-price Islamic instruments using the PKISRV synthetic curve")

    from pakfindata.engine.sukuk_pricer import SukukPricer

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        isin = st.text_input("ISIN", "PK0129601156", key="sp_isin")
    with c2:
        coupon = st.number_input("Coupon/Rental (%)", value=13.50, step=0.01, key="sp_coupon")
    with c3:
        from datetime import datetime as _dt
        maturity = st.date_input("Maturity Date", value=_dt(2031, 5, 30), key="sp_maturity")
    with c4:
        freq_opts = {"Semi-Annual": 2, "Quarterly": 4, "Annual": 1}
        freq_label = st.selectbox("Frequency", list(freq_opts.keys()), key="sp_freq")
        frequency = freq_opts[freq_label]

    c5, c6 = st.columns(2)
    with c5:
        face_value = st.number_input("Face Value", value=100.0, key="sp_face")
    with c6:
        mufap = st.number_input("MUFAP Price (optional, 0=skip)", value=0.0, key="sp_mufap")

    if st.button("Price", key="sp_price_btn", type="primary"):
        try:
            pricer = SukukPricer(curve_date=date_str, curve_source=source)
            result = pricer.price_isin(
                isin=isin, coupon=coupon,
                maturity_date=maturity.strftime("%Y-%m-%d"),
                face_value=face_value, frequency=frequency,
                mufap_price=mufap if mufap > 0 else None,
            )

            st.markdown("---")

            # Status badge
            sc = {"Reliable": _C["up"], "Indicative": _C["amber"],
                  "Outlier": _C["down"]}.get(result.valuation_status, _C["dim"])
            st.markdown(
                f'<div style="display:inline-flex;align-items:center;gap:8px;padding:4px 12px;'
                f'border:1px solid {sc};border-radius:4px;margin-bottom:12px;">'
                f'<span style="color:{sc};font-weight:900;font-size:14px;">'
                f'{result.valuation_status.upper()}</span>'
                f'<span style="color:#6B7280;font-size:10px;">'
                f'Confidence: {result.curve_confidence}/100</span></div>',
                unsafe_allow_html=True,
            )

            r1, r2, r3, r4, r5 = st.columns(5)
            r1.metric("Clean Price", f"{result.clean_price:.4f}")
            r2.metric("Dirty Price", f"{result.dirty_price:.4f}")
            r3.metric("Accrued", f"{result.accrued_interest:.4f}")
            r4.metric("YTM", f"{result.ytm:.4f}%")
            r5.metric("Mod Duration", f"{result.modified_duration:.3f}")

            r6, r7, r8 = st.columns(3)
            r6.metric("Tenor", f"{result.tenor_years:.1f}Y")
            r7.metric("Spot Rate", f"{result.interpolated_yield:.4f}%")
            r8.metric("Cash Flows", result.n_cash_flows)

            if result.variance_bps is not None:
                var_color = _C["up"] if abs(result.variance_bps) < 50 else _C["down"]
                st.markdown(
                    f'<div style="padding:6px 10px;background:rgba(33,150,243,0.1);'
                    f'border-radius:4px;font-size:12px;">'
                    f'MUFAP Comparison: Fair Value {result.clean_price:.4f} vs '
                    f'MUFAP {result.mufap_price:.4f} -> '
                    f'<b style="color:{var_color}">{result.variance_bps:+.0f} bps</b>'
                    f'</div>', unsafe_allow_html=True,
                )

            st.caption(result.confidence_reason)

            with st.expander("Cash Flow Schedule"):
                cf_rows = [{
                    "Date": cf.date.strftime("%Y-%m-%d"),
                    "Years": round(cf.years_to_cf, 3),
                    "Amount": round(cf.amount, 2),
                    "Spot Rate": f"{cf.spot_rate:.4f}%",
                    "DF": f"{cf.discount_factor:.6f}",
                    "PV": round(cf.present_value, 4),
                    "Final": "Y" if cf.is_final else "",
                } for cf in result.cash_flows]
                st.dataframe(pd.DataFrame(cf_rows), width='stretch', hide_index=True)

        except Exception as e:
            st.error(f"Pricing failed: {e}")
