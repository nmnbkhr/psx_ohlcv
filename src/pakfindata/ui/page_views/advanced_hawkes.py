"""Hawkes Process -- tick event clustering & volatility burst detection."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
    "purple": "#BB86FC",
}
DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
    font_color="#c9d1d9", margin=dict(l=20, r=20, t=40, b=20),
)


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.markdown("### Hawkes Process -- Event Clustering")
    st.caption(
        "Self-exciting point process: tick bursts predict volatility spikes. "
        "Research-grade -- Bacry et al. (2015)"
    )

    tab1, tab2, tab3, tab4 = st.tabs([
        "Live Intensity", "Burst Backtest", "Cross-Symbol Scanner", "Methodology"
    ])

    # ------------------------------------------------------------------
    # TAB 1: Live Intensity
    # ------------------------------------------------------------------
    with tab1:
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            symbol = st.text_input("Symbol", value="OGDC", key="hawkes_sym")
        with c2:
            resolution = st.selectbox("Resolution (sec)", [0.5, 1.0, 2.0, 5.0], index=1)
        with c3:
            burst_thresh = st.slider("Burst threshold (x mu)", 2.0, 8.0, 3.0, 0.5)

        if st.button("Fit Hawkes Model", type="primary"):
            with st.spinner("Fitting Hawkes process..."):
                try:
                    from pakfindata.engine.hawkes_process import analyze_symbol
                    result = analyze_symbol(
                        symbol.upper(),
                        intensity_resolution=resolution,
                        burst_threshold=burst_thresh,
                    )
                except Exception as e:
                    st.error(f"Error: {e}")
                    return

            if "error" in result:
                st.error(result["error"])
                return

            s = result["summary"]
            params = result["params"]

            # KPI row
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            with k1:
                _kpi("mu (baseline)", f"{s['mu']:.3f}/s", _C["cyan"])
            with k2:
                _kpi("alpha (excite)", f"{s['alpha']:.3f}", _C["amber"])
            with k3:
                _kpi("beta (decay)", f"{s['beta']:.3f}", _C["text"])
            with k4:
                n = s["branching_ratio"]
                nc = _C["up"] if n < 0.5 else (_C["amber"] if n < 0.8 else _C["down"])
                _kpi("Branching n", f"{n:.2f}", nc)
            with k5:
                _kpi("Half-life", f"{s['half_life_seconds']:.1f}s", _C["purple"])
            with k6:
                _kpi("Bursts", str(s["n_bursts"]),
                     _C["down"] if s["n_bursts"] > 3 else _C["text"])

            # Stability warning
            if not s["is_stable"]:
                st.error("Process is SUPERCRITICAL (n >= 1). Model may be unreliable.")
            elif s["branching_ratio"] > 0.8:
                st.warning(f"Process is near-critical (n={s['branching_ratio']:.2f}). "
                           f"Bursts are highly clustered.")

            # Secondary KPIs
            k7, k8, k9, k10 = st.columns(4)
            with k7:
                _kpi("Ticks", f"{s['n_ticks']:,}")
            with k8:
                _kpi("Duration", f"{s['duration_seconds']:.0f}s")
            with k9:
                _kpi("Ticks/sec", f"{s['ticks_per_second']:.2f}")
            with k10:
                _kpi("Max Intensity", f"{s['max_intensity_ratio']:.1f}x", _C["gold"])

            # Intensity plot
            intensity = result["intensity"]
            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3],
                vertical_spacing=0.05,
                subplot_titles=["Conditional Intensity lambda(t)", "Regime"]
            )

            # Intensity line
            fig.add_trace(go.Scatter(
                x=intensity["time"], y=intensity["intensity"],
                mode="lines", name="lambda(t)",
                line=dict(color=_C["cyan"], width=1.5),
            ), row=1, col=1)

            # Baseline
            fig.add_hline(
                y=s["mu"], line_dash="dash", line_color=_C["dim"],
                annotation_text=f"mu = {s['mu']:.3f}", row=1, col=1,
            )

            # Burst threshold
            fig.add_hline(
                y=s["mu"] * burst_thresh, line_dash="dot", line_color=_C["down"],
                annotation_text=f"{burst_thresh}x mu", row=1, col=1,
            )

            # Color-coded regime bar
            regime_colors = {
                "CALM": _C["dim"], "ELEVATED": _C["amber"],
                "BURST": _C["down"], "EXPLOSIVE": "#FF1744",
            }
            for regime, color in regime_colors.items():
                mask = intensity["regime"] == regime
                if mask.any():
                    fig.add_trace(go.Scatter(
                        x=intensity.loc[mask, "time"],
                        y=[1] * mask.sum(),
                        mode="markers", name=regime,
                        marker=dict(color=color, size=6, symbol="square"),
                    ), row=2, col=1)

            fig.update_layout(**PLOT_LAYOUT, height=500, showlegend=True)
            fig.update_yaxes(title_text="lambda(t) events/sec", row=1, col=1)
            fig.update_yaxes(visible=False, row=2, col=1)
            st.plotly_chart(fig, width='stretch')

            # Burst table
            if result["bursts"]:
                st.markdown(f"**{len(result['bursts'])} burst(s) detected:**")
                bdf = pd.DataFrame(result["bursts"])
                display_cols = [c for c in [
                    "start_time", "duration_seconds", "peak_ratio",
                    "n_points", "price_change_pct", "volume_in_burst",
                ] if c in bdf.columns]
                st.dataframe(bdf[display_cols], width='stretch', hide_index=True)
            else:
                st.info("No bursts detected at current threshold.")

            # Regime distribution
            if s.get("regime_counts"):
                st.markdown("**Regime Distribution:**")
                rc = s["regime_counts"]
                total = sum(rc.values())
                cols = st.columns(len(rc))
                for i, (regime, count) in enumerate(rc.items()):
                    with cols[i]:
                        pct = count / total * 100
                        color = regime_colors.get(regime, _C["dim"])
                        _kpi(regime, f"{pct:.1f}%", color)

    # ------------------------------------------------------------------
    # TAB 2: Burst Backtest
    # ------------------------------------------------------------------
    with tab2:
        st.subheader("Burst Backtest")
        st.caption("Do Hawkes bursts predict subsequent volatility spikes?")

        c1, c2, c3 = st.columns(3)
        with c1:
            bt_sym = st.text_input("Symbol", value="OGDC", key="hawkes_bt_sym")
        with c2:
            bt_days = st.slider("Lookback days", 5, 60, 20, key="hawkes_bt_days")
        with c3:
            bt_thresh = st.slider("Burst threshold", 2.0, 8.0, 3.0, 0.5, key="hawkes_bt_thresh")

        if st.button("Run Backtest", key="hawkes_bt_run"):
            try:
                from pakfindata.engine.hawkes_process import analyze_symbol
                from pakfindata.db.connections import _duck_con

                sym_upper = bt_sym.upper()
                con = _duck_con()
                dates = con.execute(f"""
                    SELECT DISTINCT CAST(_ts AS DATE) as d
                    FROM tick_logs WHERE symbol = '{sym_upper}'
                    ORDER BY d DESC LIMIT {bt_days}
                """).fetchall()

                all_bursts = []
                progress = st.progress(0, text=f"Analyzing day 0/{len(dates)}...")
                for i, (d,) in enumerate(dates):
                    progress.progress((i + 1) / len(dates),
                                      text=f"Fitting {sym_upper} on {d} ({i+1}/{len(dates)})...")
                    analysis = analyze_symbol(sym_upper, str(d),
                                              intensity_resolution=10.0,
                                              burst_threshold=bt_thresh, fast=True)
                    if "error" in analysis:
                        continue
                    for burst in analysis["bursts"]:
                        burst["date"] = str(d)
                        ticks = analysis["ticks"]
                        t_start, t_end = burst["start_time"], burst["end_time"]
                        pre = ticks[(ticks["ts_seconds"] >= t_start - 300) &
                                    (ticks["ts_seconds"] < t_start)]
                        post = ticks[(ticks["ts_seconds"] > t_end) &
                                     (ticks["ts_seconds"] <= t_end + 300)]
                        burst["pre_vol"] = float(pre["price"].pct_change().dropna().std() *
                                                  np.sqrt(len(pre))) if len(pre) > 5 else 0
                        burst["post_vol"] = float(post["price"].pct_change().dropna().std() *
                                                   np.sqrt(len(post))) if len(post) > 5 else 0
                        burst["vol_amplification"] = (burst["post_vol"] / burst["pre_vol"]
                                                      if burst["pre_vol"] > 0 else 0)
                        all_bursts.append(burst)
                progress.empty()

                if not all_bursts:
                    st.warning(f"No bursts detected for {sym_upper} in {len(dates)} days.")
                    return

                bdf_raw = pd.DataFrame(all_bursts)
                m = {
                    "days_analyzed": len(dates),
                    "total_bursts": len(bdf_raw),
                    "bursts_per_day": len(bdf_raw) / len(dates) if dates else 0,
                    "avg_duration_sec": float(bdf_raw["duration_seconds"].mean()),
                    "avg_peak_ratio": float(bdf_raw["peak_ratio"].mean()),
                    "avg_vol_amplification": float(bdf_raw["vol_amplification"].mean()),
                    "pct_vol_increase": float((bdf_raw["vol_amplification"] > 1.0).mean() * 100),
                    "avg_price_move_pct": float(bdf_raw["price_change_pct"].abs().mean()) if "price_change_pct" in bdf_raw.columns else 0,
                }
                result = {"metrics": m, "bursts": bdf_raw}

            except Exception as e:
                st.error(f"Error: {e}")
                return

            m = result["metrics"]

            k1, k2, k3, k4 = st.columns(4)
            with k1:
                _kpi("Days Analyzed", str(m["days_analyzed"]))
            with k2:
                _kpi("Total Bursts", str(m["total_bursts"]))
            with k3:
                amp = m["avg_vol_amplification"]
                c = _C["up"] if amp > 1.5 else (_C["amber"] if amp > 1.0 else _C["dim"])
                _kpi("Avg Vol Amplification", f"{amp:.2f}x", c)
            with k4:
                pct = m["pct_vol_increase"]
                c = _C["up"] if pct > 60 else _C["dim"]
                _kpi("% Bursts -> Vol Up", f"{pct:.0f}%", c)

            st.markdown("---")

            if m["pct_vol_increase"] > 60:
                st.success(
                    f"Hawkes bursts ARE predictive for {bt_sym}: "
                    f"{m['pct_vol_increase']:.0f}% of bursts led to higher volatility. "
                    f"Average amplification: {m['avg_vol_amplification']:.2f}x"
                )
            elif m["pct_vol_increase"] > 50:
                st.info(
                    f"Marginal signal: {m['pct_vol_increase']:.0f}% of bursts "
                    f"led to higher volatility (need >60% for significance)."
                )
            else:
                st.warning(
                    f"Hawkes bursts are NOT predictive for {bt_sym}: "
                    f"only {m['pct_vol_increase']:.0f}% led to higher volatility."
                )

            # Extra KPIs
            k5, k6, k7 = st.columns(3)
            with k5:
                _kpi("Bursts/Day", f"{m['bursts_per_day']:.1f}")
            with k6:
                _kpi("Avg Duration", f"{m['avg_duration_sec']:.0f}s")
            with k7:
                _kpi("Avg Peak Ratio", f"{m['avg_peak_ratio']:.1f}x")

            # Burst detail table
            bdf = result["bursts"]
            if not bdf.empty:
                st.markdown("**Burst Details:**")
                display_cols = [c for c in [
                    "date", "start_time", "duration_seconds", "peak_ratio",
                    "price_change_pct", "pre_vol", "post_vol", "vol_amplification",
                ] if c in bdf.columns]
                st.dataframe(
                    bdf[display_cols].round(3),
                    width='stretch', hide_index=True,
                )

    # ------------------------------------------------------------------
    # TAB 3: Cross-Symbol Scanner
    # ------------------------------------------------------------------
    with tab3:
        st.subheader("Cross-Symbol Scanner")
        st.caption("Fit Hawkes to top symbols -- find the most self-exciting stocks")

        c1, c2 = st.columns(2)
        with c1:
            scan_n = st.slider("Top N symbols (by tick count)", 10, 100, 20, key="hawkes_scan_n")
        with c2:
            scan_btn = st.button("Scan Now", key="hawkes_scan_btn")

        if scan_btn:
            try:
                from pakfindata.engine.hawkes_process import analyze_symbol
                from pakfindata.db.connections import _duck_con

                con = _duck_con()
                date = str(con.execute("SELECT MAX(CAST(_ts AS DATE)) FROM tick_logs").fetchone()[0])
                rows = con.execute(f"""
                    SELECT symbol, COUNT(*) as n
                    FROM tick_logs WHERE CAST(_ts AS DATE) = '{date}'
                    GROUP BY symbol ORDER BY n DESC LIMIT {scan_n}
                """).fetchall()
                symbols = [r[0] for r in rows]

                results = []
                progress = st.progress(0, text=f"Scanning 0/{len(symbols)}...")
                for i, sym in enumerate(symbols):
                    progress.progress((i + 1) / len(symbols),
                                      text=f"Fitting {sym} ({i+1}/{len(symbols)})...")
                    try:
                        analysis = analyze_symbol(sym, date, intensity_resolution=5.0, fast=True)
                        if "error" not in analysis:
                            results.append(analysis["summary"])
                    except Exception:
                        continue
                progress.empty()

                if results:
                    df = pd.DataFrame(results).sort_values("branching_ratio", ascending=False)
                else:
                    df = pd.DataFrame()

            except Exception as e:
                st.error(f"Error: {e}")
                return

            if df.empty:
                st.info("No results.")
                return

            st.markdown(f"**{len(df)} symbols analyzed** -- sorted by branching ratio (most self-exciting first)")

            display_cols = [c for c in [
                "symbol", "n_ticks", "mu", "alpha", "beta",
                "branching_ratio", "half_life_seconds", "n_bursts",
                "time_in_burst_pct", "max_intensity_ratio",
            ] if c in df.columns]

            def _color_n(val):
                if isinstance(val, (int, float)):
                    if val > 0.8:
                        return "color: #FF5252; font-weight: bold"
                    elif val > 0.5:
                        return "color: #FFB300"
                    else:
                        return "color: #00E676"
                return ""

            styled = df[display_cols].style.map(
                _color_n, subset=["branching_ratio"] if "branching_ratio" in display_cols else []
            ).format({
                "mu": "{:.4f}", "alpha": "{:.3f}", "beta": "{:.3f}",
                "branching_ratio": "{:.3f}", "half_life_seconds": "{:.1f}",
                "time_in_burst_pct": "{:.1f}%", "max_intensity_ratio": "{:.1f}x",
            })
            st.dataframe(styled, width='stretch', hide_index=True, height=500)

            # Bar chart of branching ratios
            top20 = df.head(20)
            fig = go.Figure(data=[go.Bar(
                x=top20["symbol"], y=top20["branching_ratio"],
                marker_color=[
                    _C["down"] if n > 0.8 else (_C["amber"] if n > 0.5 else _C["up"])
                    for n in top20["branching_ratio"]
                ],
            )])
            fig.add_hline(y=0.5, line_dash="dash", line_color=_C["dim"],
                          annotation_text="Moderate excitation")
            fig.add_hline(y=0.8, line_dash="dot", line_color=_C["down"],
                          annotation_text="Near-critical")
            fig.update_layout(**PLOT_LAYOUT, height=350,
                              title_text="Branching Ratio by Symbol")
            st.plotly_chart(fig, width='stretch')

    # ------------------------------------------------------------------
    # TAB 4: Methodology
    # ------------------------------------------------------------------
    with tab4:
        st.subheader("Self-Exciting Hawkes Process")

        st.markdown("""
        A **Hawkes process** is a point process where each event increases the
        probability of future events. In financial markets, trades beget trades --
        a large buy triggers other buyers (momentum), market makers adjust quotes,
        and stop-loss orders cascade.

        **Conditional intensity:**

        `lambda(t) = mu + SUM alpha * exp(-beta * (t - t_i))`

        | Parameter | Meaning | PSX Typical |
        |-----------|---------|-------------|
        | **mu** | Baseline arrival rate (ticks/sec in calm market) | 0.1 -- 0.5 |
        | **alpha** | Excitation jump per event | 0.3 -- 0.8 |
        | **beta** | Decay rate (how fast excitation fades) | 0.5 -- 2.0 |
        | **n = alpha/beta** | Branching ratio (fraction caused by others) | 0.3 -- 0.8 |
        | **ln(2)/beta** | Half-life of excitation (seconds) | 30 -- 120 on PSX |

        **Key insight for PSX:** On NYSE, half-life is milliseconds (HFT arbitrages
        excitation instantly). On PSX, half-life is **30-120 seconds** because there
        are no HFT firms. Bursts are visible, persistent, and tradeable.

        ---

        ### Regime Classification

        | Regime | lambda(t) / mu | Meaning | Action |
        |--------|----------|---------|--------|
        | CALM | < 1.5x | Normal trading | Standard sizing |
        | ELEVATED | 1.5 -- 3x | Above-average activity | Watch closely |
        | BURST | 3 -- 5x | Activity burst -- likely news/flow | Widen stops, reduce size |
        | EXPLOSIVE | > 5x | Extreme -- possible circuit lock | Exit or hedge |

        ---

        ### Estimation

        Parameters are fitted via **Maximum Likelihood Estimation (MLE)** using
        L-BFGS-B optimization with multiple restarts. The log-likelihood is computed
        using the recursive O(n) algorithm (not naive O(n^2)).

        **Stability constraint:** n = alpha/beta < 1 (subcritical). If n >= 1,
        the process is explosive (intensity grows without bound).

        ---

        ### References

        - Hawkes, A.G. (1971). "Spectra of some self-exciting and mutually exciting point processes."
        - Bacry, E., Mastromatteo, I., & Muzy, J.F. (2015). "Hawkes processes in finance."
        - Filimonov, V. & Sornette, D. (2012). "Quantifying reflexivity in financial markets."
        """)

    render_footer()
