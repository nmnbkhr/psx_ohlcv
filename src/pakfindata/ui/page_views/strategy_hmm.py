"""Macro Regime HMM Strategy — cross-asset regime detection and allocation (v1 + v2)."""

from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
}
_CHART = dict(paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
              margin=dict(t=30, b=20, l=50, r=20))

# v2 regime colors keyed by name string
_RC2 = {
    "RISK_ON": "#22C55E",
    "RECOVERY": "#14B8A6",
    "TRANSITION": "#EAB308",
    "RISK_OFF": "#F97316",
    "CRISIS": "#EF4444",
}

# Source badge colors
_SRC_COLORS = {
    "hmm": "#2196F3",
    "HMM": "#2196F3",
    "override": "#EF4444",
    "HARD_OVERRIDE": "#EF4444",
    "gated": "#EAB308",
    "GATED": "#EAB308",
}


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def _badge(text, bg_color, text_color="#FFFFFF"):
    return (f'<span style="background:{bg_color};color:{text_color};'
            f'padding:4px 12px;border-radius:4px;font-size:0.8em;'
            f'font-weight:600;margin-left:8px;">{text}</span>')


def render_page():
    st.markdown("### Macro Regime Model (HMM v2)")
    st.caption("5-state Hidden Markov Model — cross-asset regime detection with confidence gating")

    tab_current, tab_bt, tab_train, tab_method = st.tabs(
        ["Current Regime", "Backtest", "Train Model", "Methodology"])

    with tab_current:
        _render_current()
    with tab_bt:
        _render_backtest()
    with tab_train:
        _render_train()
    with tab_method:
        _render_methodology()

    render_footer()


# ═══════════════════════════════════════════════════════
# TAB 1: Current Regime
# ═══════════════════════════════════════════════════════

def _render_current():
    try:
        from pakfindata.engine.macro_regime_hmm_v2 import get_current_regime_v2, MODEL_PATH_V2
        model_path = MODEL_PATH_V2
        get_regime = get_current_regime_v2
        is_v2 = True
    except ImportError:
        from pakfindata.engine.macro_regime_hmm import get_current_regime, MODEL_PATH
        model_path = MODEL_PATH
        get_regime = get_current_regime
        is_v2 = False

    if not model_path.exists():
        st.warning("No trained model. Go to **Train Model** tab first.")
        return

    with st.spinner("Loading regime..."):
        state = get_regime()

    if not state:
        st.error("Could not compute current regime")
        return

    regime = state["regime"]
    rc = _RC2.get(regime, _C["dim"])

    # Build source badge
    source = state.get("regime_source", "hmm").upper()
    src_color = _SRC_COLORS.get(source, _SRC_COLORS.get(source.lower(), _C["accent"]))
    source_badge = _badge(source, src_color)

    # Gated badge
    gated_badge = ""
    if state.get("gated", False):
        gated_badge = _badge("GATED", "#EAB308", "#000000")

    # Regime badge
    trained_str = str(state.get("model_trained", "unknown"))[:10]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:24px;border-radius:12px;border-left:6px solid {rc};margin-bottom:16px;">
        <div style="color:{_C['dim']};font-size:0.8em;text-transform:uppercase;">Current Macro Regime</div>
        <div style="color:{rc};font-size:2.5em;font-weight:700;display:inline-block;">
            {regime.replace('_',' ')}
        </div>
        {source_badge}{gated_badge}
        <div style="color:{_C['dim']};font-size:0.85em;margin-top:8px;">
            Confidence: {state['probability']:.0%} | Month: {state['date']} | Model: {trained_str}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # KPIs — 7 cards
    alloc = state["allocation"]
    mc = st.columns(7)
    with mc[0]:
        _kpi("KSE Momentum (3M)", f"{state.get('kse_momentum', 0):+.1%}")
    with mc[1]:
        _kpi("KIBOR 3M", f"{state.get('kibor', 0):.1f}%")
    with mc[2]:
        _kpi("PKR/USD", f"{state.get('pkr_usd', 0):.1f}")
    with mc[3]:
        _kpi("SBP Cycle", state.get("sbp_cycle", "HOLD"))
    with mc[4]:
        reserves = state.get("reserves", 0)
        _kpi("SBP Reserves (USD Bn)", f"{reserves:.1f}")
    with mc[5]:
        ca = state.get("ca_balance", 0)
        ca_color = _C["up"] if ca >= 0 else _C["down"]
        _kpi("CA Balance", f"{ca:+,.0f}mn", ca_color)
    with mc[6]:
        eq = alloc.get("equity", 0)
        _kpi("Equity Alloc", f"{eq:.0%}", _C["up"] if eq > 0.5 else _C["down"])

    # Regime probabilities — 5 bars
    st.markdown("#### Regime Probabilities")
    probs = state["probs"]
    fig = go.Figure(go.Bar(
        x=[k.replace("_", " ") for k in probs.keys()],
        y=list(probs.values()),
        marker_color=[_RC2.get(k, _C["dim"]) for k in probs.keys()],
        text=[f"{v:.0%}" for v in probs.values()],
        textposition="auto",
    ))
    fig.update_layout(**_CHART, height=260,
                      yaxis=dict(gridcolor=_C["grid"], range=[0, 1], title="Probability"))
    st.plotly_chart(fig, width='stretch')

    # Allocation pie
    st.markdown("#### Recommended Allocation")
    fig = go.Figure(go.Pie(
        labels=[k.title() for k in alloc.keys()],
        values=list(alloc.values()),
        hole=0.4,
        marker=dict(colors=[_C["accent"], _C["amber"], _C["dim"]]),
        textinfo="label+percent",
    ))
    fig.update_layout(**_CHART, height=260)
    st.plotly_chart(fig, width='stretch')

    # ── Flow Intelligence Panel ──
    _render_flow_intelligence()


def _render_flow_intelligence():
    """Investor Flow Intelligence panel — NCCPL FIPI/LIPI derived signals."""
    try:
        from pakfindata.db.connection import connect
        from pakfindata.db.repositories.nccpl_flows import (
            get_derived_latest,
            get_sector_flows_latest,
        )
        import plotly.express as px

        con = connect()

        derived = get_derived_latest(con)
        if not derived:
            return  # No flow data yet — silently skip

        st.markdown("---")
        st.markdown("#### Investor Flow Intelligence (4-Week Rolling)")

        fpi_4w = derived.get("fpi_net_4w", 0) or 0
        mf_4w = derived.get("mf_net_4w", 0) or 0
        retail_4w = derived.get("retail_net_4w", 0) or 0
        bank_4w = derived.get("bank_net_4w", 0) or 0
        smart_dumb_ratio = derived.get("smart_dumb_ratio", 0) or 0
        inst_consensus = derived.get("institutional_consensus", 0)
        flow_regime = derived.get("flow_regime_signal", "NEUTRAL")

        # 4-week rolling metrics
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            _kpi("Foreign (FPI) 4W", f"{fpi_4w:+,.0f} Mn",
                 _C["up"] if fpi_4w > 0 else _C["down"])
        with fc2:
            _kpi("Mutual Funds 4W", f"{mf_4w:+,.0f} Mn",
                 _C["up"] if mf_4w > 0 else _C["down"])
        with fc3:
            label = "Retail 4W"
            if retail_4w > 500 and fpi_4w < 0:
                label = "Retail 4W (WARN)"
            _kpi(label, f"{retail_4w:+,.0f} Mn",
                 _C["amber"] if (retail_4w > 500 and fpi_4w < 0) else _C["text"])
        with fc4:
            _kpi("Banks 4W", f"{bank_4w:+,.0f} Mn",
                 _C["up"] if bank_4w > 0 else _C["down"])

        # Smart/Dumb ratio + consensus
        sc1, sc2 = st.columns(2)
        with sc1:
            if smart_dumb_ratio > 0.5:
                ratio_color = _C["up"]
            elif smart_dumb_ratio < -0.5:
                ratio_color = _C["down"]
            else:
                ratio_color = _C["amber"]
            _kpi("Smart/Dumb Ratio", f"{smart_dumb_ratio:.2f}", ratio_color)
            st.caption("Positive = smart money leading | Negative = retail leading (contrarian warning)")
        with sc2:
            consensus_text = "YES" if inst_consensus else "NO"
            consensus_color = _C["up"] if inst_consensus else _C["down"]
            _kpi("Institutional Consensus", consensus_text, consensus_color)

        # Flow regime signal
        flow_colors = {
            "BULLISH": "#22C55E", "BEARISH": "#EF4444",
            "DIVERGENT": "#F97316", "NEUTRAL": "#6B7280",
        }
        fc = flow_colors.get(flow_regime, _C["dim"])
        st.markdown(f"""
        <div style="padding:10px;border-radius:5px;background:{fc}20;
                    border-left:4px solid {fc};margin:8px 0;">
            <b>Flow Regime Signal: {flow_regime}</b>
        </div>
        """, unsafe_allow_html=True)

        # Sector heatmap
        try:
            sector_df = get_sector_flows_latest(con)
            if not sector_df.empty:
                st.markdown("#### Foreign Flow by Sector (Latest)")
                fig = px.bar(
                    sector_df, x="sector", y="fpi_net",
                    color="fpi_net",
                    color_continuous_scale="RdYlGn",
                )
                fig.update_layout(
                    **_CHART, height=300,
                    xaxis=dict(tickangle=45, gridcolor=_C["grid"]),
                    yaxis=dict(gridcolor=_C["grid"], title="Net Flow"),
                    coloraxis_colorbar=dict(title="Net"),
                )
                st.plotly_chart(fig, width='stretch')
        except Exception:
            pass

    except Exception as e:
        st.caption(f"Flow intelligence unavailable: {e}")


# ═══════════════════════════════════════════════════════
# TAB 2: Backtest
# ═══════════════════════════════════════════════════════

def _render_backtest():
    try:
        from pakfindata.engine.macro_regime_hmm_v2 import (
            load_hmm_v2, load_macro_features_v2, predict_regime_v2,
            backtest_regime_allocation_v2, MODEL_PATH_V2,
        )
        model_path = MODEL_PATH_V2
        load_fn = load_hmm_v2
        features_fn = load_macro_features_v2
        predict_fn = predict_regime_v2
        backtest_fn = backtest_regime_allocation_v2
    except ImportError:
        from pakfindata.engine.macro_regime_hmm import (
            load_hmm, load_macro_features, predict_regime,
            backtest_regime_allocation, MODEL_PATH,
        )
        model_path = MODEL_PATH
        load_fn = load_hmm
        features_fn = load_macro_features
        predict_fn = predict_regime
        backtest_fn = backtest_regime_allocation

    if not model_path.exists():
        st.warning("Train model first.")
        return

    model_dict = load_fn()
    if not model_dict:
        st.error("Failed to load model")
        return

    df = features_fn()
    pred_df = predict_fn(model_dict, df)
    bt = backtest_fn(pred_df)

    if "error" in bt:
        st.error(bt["error"])
        return

    m = bt["metrics"]

    # Metrics comparison
    st.markdown("#### Strategy vs Buy & Hold")
    gated_months = m.get("gated_months", 0)
    override_months = m.get("override_months", 0)
    has_extra = gated_months > 0 or override_months > 0

    n_cols = 8 if has_extra else 6
    mc = st.columns(n_cols)

    labels = ["Strategy Ret", "B&H Ret", "Alpha", "Strategy Sharpe", "Strategy DD", "B&H DD"]
    values = [
        f"{m['strategy_return']:+.1%}", f"{m['bh_return']:+.1%}", f"{m['alpha']:+.1%}",
        f"{m['strategy_sharpe']:.2f}", f"{m['strategy_max_dd']:.1%}", f"{m['bh_max_dd']:.1%}",
    ]
    colors = [
        _C["up"] if m["strategy_return"] > 0 else _C["down"],
        _C["up"] if m["bh_return"] > 0 else _C["down"],
        _C["up"] if m["alpha"] > 0 else _C["down"],
        _C["up"] if m["strategy_sharpe"] > 0.5 else _C["down"],
        _C["down"], _C["down"],
    ]

    if has_extra:
        labels += ["Gated Months", "Override Months"]
        values += [str(gated_months), str(override_months)]
        colors += [_C["amber"], _C["down"]]

    for i, col in enumerate(mc):
        with col:
            _kpi(labels[i], values[i], colors[i])

    rdf = bt["df"]

    # Equity curves + regime overlay
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.6, 0.4], vertical_spacing=0.05)

    fig.add_trace(go.Scatter(
        x=rdf["month"], y=rdf["strategy_equity"], name="Strategy",
        line=dict(color=_C["accent"], width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=rdf["month"], y=rdf["bh_equity"], name="Buy & Hold",
        line=dict(color=_C["dim"], width=1, dash="dot")), row=1, col=1)

    # 5-regime overlay
    regime_id_col = "effective_regime_id" if "effective_regime_id" in rdf.columns else "regime_id"
    regime_col = "effective_regime" if "effective_regime" in rdf.columns else "regime"
    for r_name, r_color in _RC2.items():
        mask = rdf[regime_col] == r_name
        if mask.any():
            fig.add_trace(go.Scatter(
                x=rdf.loc[mask, "month"],
                y=rdf.loc[mask, regime_id_col],
                mode="markers", marker=dict(color=r_color, size=8),
                name=r_name.replace("_", " ").title(), showlegend=True,
            ), row=2, col=1)

    fig.update_layout(
        **_CHART, height=520,
        legend=dict(orientation="h", y=1.08, bgcolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor=_C["grid"], title="Equity"),
        yaxis2=dict(gridcolor=_C["grid"], title="Regime",
                    tickvals=[0, 1, 2, 3, 4],
                    ticktext=["Risk On", "Recovery", "Transition", "Risk Off", "Crisis"]),
    )
    st.plotly_chart(fig, width='stretch')

    # Regime durations
    durations = bt.get("regime_durations", {})
    if durations:
        st.markdown("#### Regime Duration (months)")
        fig = go.Figure(go.Bar(
            x=[k.replace("_", " ").title() for k in durations.keys()],
            y=list(durations.values()),
            marker_color=[_RC2.get(k, _C["dim"]) for k in durations.keys()],
            text=[str(v) for v in durations.values()], textposition="auto",
        ))
        fig.update_layout(**_CHART, height=260,
                          yaxis=dict(gridcolor=_C["grid"], title="Months"))
        st.plotly_chart(fig, width='stretch')


# ═══════════════════════════════════════════════════════
# TAB 3: Train Model
# ═══════════════════════════════════════════════════════

def _render_train():
    try:
        from pakfindata.engine.macro_regime_hmm_v2 import (
            train_and_save_v2, MODEL_PATH_V2, load_macro_features_v2,
            FEATURE_COLS_V2,
        )
        model_path = MODEL_PATH_V2
        train_fn = train_and_save_v2
        features_fn = load_macro_features_v2
        feature_cols = FEATURE_COLS_V2
        is_v2 = True
    except ImportError:
        from pakfindata.engine.macro_regime_hmm import train_and_save, MODEL_PATH
        model_path = MODEL_PATH
        train_fn = train_and_save
        features_fn = None
        feature_cols = []
        is_v2 = False

    st.markdown("#### Train / Retrain HMM Model (v2)" if is_v2 else "#### Train / Retrain HMM Model")
    st.caption(f"Model location: `{model_path}`")

    # Show existing model info
    if model_path.exists():
        import joblib
        m = joblib.load(model_path)
        trained_at = m.get("trained_at", "unknown")
        months = m.get("months_trained", "?")
        ll = m.get("log_likelihood", 0)
        st.success(f"Existing model: trained {trained_at}, {months} months, LL={ll:.1f}")
        if is_v2:
            cols_used = m.get("feature_cols", feature_cols)
            st.caption(f"Features: {', '.join(cols_used)}")

    # Sync HMM data button
    if is_v2:
        st.markdown("---")
        st.markdown("##### Data Sync")

        sc1, sc2 = st.columns(2)

        with sc1:
            if st.button("Sync Macro Sources", key="hmm_sync"):
                with st.spinner("Fetching T-bill/PIB spread, SBP reserves, CA balance..."):
                    try:
                        from pakfindata.sources.hmm_data_fetchers import sync_all_hmm_data
                        result = sync_all_hmm_data()
                        if isinstance(result, dict) and "error" not in result:
                            for source, info in result.items():
                                if isinstance(info, dict):
                                    rows = info.get("rows", info.get("count", "?"))
                                    st.write(f"- **{source}**: {rows} rows")
                                else:
                                    st.write(f"- **{source}**: {info}")
                            st.success("Macro data sync complete")
                        elif isinstance(result, dict):
                            st.error(result.get("error", "Sync failed"))
                        else:
                            st.success("Macro data sync complete")
                    except Exception as e:
                        st.error(f"Sync error: {e}")

        with sc2:
            if st.button("Sync NCCPL Flows", key="hmm_nccpl_sync"):
                with st.spinner("Fetching NCCPL FIPI/LIPI flows (khistocks)..."):
                    try:
                        from pakfindata.sources.khistocks_nccpl import sync_nccpl_flows
                        from pakfindata.db.connection import connect as _connect
                        _con = _connect()
                        res = sync_nccpl_flows(_con, days=90)
                        flow_count = _con.execute(
                            "SELECT COUNT(*) FROM nccpl_flows_derived"
                        ).fetchone()[0]
                        st.success(
                            f"NCCPL flows synced via **khistocks** — "
                            f"FIPI: {res['fipi']} days, LIPI: {res['lipi']} days, "
                            f"{flow_count} total derived signals."
                        )
                    except Exception as e:
                        st.error(f"NCCPL sync error: {e}")

    # Train button
    st.markdown("---")
    st.markdown("##### Train Model")
    if st.button("Train Model", type="primary", key="hmm_train"):
        with st.spinner("Training HMM on 10 macro features..." if is_v2
                        else "Training HMM on macro features..."):
            result = train_fn()

        if "error" in result:
            st.error(result["error"])
        else:
            st.success(
                f"Model trained on {result['months_trained']} months "
                f"and saved to `{result['model_path']}`"
            )
            st.markdown(f"**Current regime: {result['current_regime']}**")

            bt = result.get("backtest", {})
            if bt:
                st.markdown(
                    f"Strategy: {bt['strategy_return']:+.1%} | "
                    f"B&H: {bt['bh_return']:+.1%} | "
                    f"Sharpe: {bt['strategy_sharpe']:.2f} | "
                    f"DD: {bt['strategy_max_dd']:.1%}"
                )

            # v2-specific post-training analysis
            if is_v2 and "pred_df" in result:
                pred_df = result["pred_df"]
                _render_feature_analysis(pred_df, feature_cols)

    # Show data coverage if v2 features available
    if is_v2 and features_fn is not None:
        with st.expander("Data Coverage", expanded=False):
            try:
                df = features_fn()
                if not df.empty and "month" in df.columns:
                    coverage = []
                    for col in feature_cols:
                        if col in df.columns:
                            valid = df[col].dropna()
                            if len(valid) > 0:
                                start = df.loc[valid.index[0], "month"]
                                end = df.loc[valid.index[-1], "month"]
                                coverage.append({
                                    "Feature": col,
                                    "Start": start,
                                    "End": end,
                                    "Months": len(valid),
                                })
                    if coverage:
                        cdf = pd.DataFrame(coverage)
                        st.dataframe(
                            cdf.style.map(
                                lambda _: f"background-color:{_C['card']};color:{_C['text']}",
                            ),
                            width='stretch',
                            hide_index=True,
                        )
            except Exception as e:
                st.caption(f"Could not load features: {e}")


def _render_feature_analysis(pred_df: pd.DataFrame, feature_cols: list):
    """Show feature importance (mutual info) and correlation heatmap after training."""
    st.markdown("---")
    st.markdown("##### Feature Analysis")

    available_cols = [c for c in feature_cols if c in pred_df.columns]
    if not available_cols:
        return

    # Mutual information (feature importance)
    try:
        from sklearn.feature_selection import mutual_info_classif

        target_col = "effective_regime_id" if "effective_regime_id" in pred_df.columns else "regime_id"
        X = pred_df[available_cols].fillna(0).values
        y = pred_df[target_col].values

        mi = mutual_info_classif(X, y, random_state=42)
        mi_df = pd.DataFrame({"Feature": available_cols, "MI Score": mi})
        mi_df = mi_df.sort_values("MI Score", ascending=True)

        st.markdown("###### Feature Importance (Mutual Information)")
        fig = go.Figure(go.Bar(
            x=mi_df["MI Score"], y=mi_df["Feature"],
            orientation="h",
            marker_color=_C["gold"],
            text=[f"{v:.3f}" for v in mi_df["MI Score"]],
            textposition="auto",
        ))
        fig.update_layout(**_CHART, height=300,
                          xaxis=dict(gridcolor=_C["grid"], title="MI Score"),
                          yaxis=dict(gridcolor=_C["grid"]))
        st.plotly_chart(fig, width='stretch')
    except Exception as e:
        st.caption(f"Could not compute mutual info: {e}")

    # Correlation heatmap
    try:
        corr = pred_df[available_cols].corr()
        labels = [c.replace("_", " ").title() for c in available_cols]

        fig = go.Figure(go.Heatmap(
            z=corr.values, x=labels, y=labels,
            colorscale=[[0, _C["down"]], [0.5, _C["bg"]], [1, _C["up"]]],
            zmin=-1, zmax=1,
            text=corr.values.round(2).astype(str),
            texttemplate="%{text}",
            textfont=dict(size=10),
        ))
        st.markdown("###### Feature Correlation")
        fig.update_layout(**_CHART, height=400,
                          xaxis=dict(tickangle=45),
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, width='stretch')
    except Exception as e:
        st.caption(f"Could not compute correlation: {e}")


# ═══════════════════════════════════════════════════════
# TAB 4: Methodology
# ═══════════════════════════════════════════════════════

def _render_methodology():
    st.markdown("""
#### Hidden Markov Model v2 for Macro Regimes

**10 Observables** (what we see):
1. **KSE-100 monthly return** — equity momentum
2. **KSE-100 3M momentum** — medium-term trend
3. **KSE-100 6M volatility** — risk proxy
4. **KIBOR 3M direction** — interbank rate trend (-1/0/+1)
5. **PKR/USD monthly change** — currency pressure
6. **T-bill / PIB yield spread** — term premium / credit stress
7. **SBP FX reserves (USD bn)** — external buffer
8. **Current account balance (USD mn)** — external balance
9. **Foreign investor 4W net flow** — NCCPL FIPI rolling flow (smart money)
10. **Mutual fund 4W net flow** — NCCPL LIPI MF rolling flow (institutional)

---

#### 5 Hidden States (what we infer):

| Regime | Equity | Bonds | Cash | Characteristics |
|---|---|---|---|---|
| **Risk On** | 80% | 10% | 10% | KSE rising, rates falling, PKR stable, reserves high |
| **Recovery** | 60% | 25% | 15% | Early recovery, improving fundamentals, mixed signals |
| **Transition** | 40% | 30% | 30% | Mixed signals, regime changing |
| **Risk Off** | 20% | 50% | 30% | KSE falling, rates rising, PKR weak |
| **Crisis** | 0% | 30% | 70% | Sharp drawdowns, liquidity freeze, reserve depletion |

---

#### Confidence Gating

When the HMM confidence for the predicted regime is **below 75%**, the model
holds the **previous regime** instead of switching. This prevents noisy
transitions during ambiguous periods. Gated months are flagged in the backtest.

---

#### Hard Override Rules

The model forces **CRISIS** regime when **all three** conditions are met simultaneously:

1. **KIBOR 3M > 18%** — extreme monetary tightening
2. **SBP reserves < 8 USD bn** — critically low external buffer
3. **CA balance < -3,000 USD mn** — severe external deficit

This overrides HMM output regardless of model confidence, ensuring
the allocation goes defensive during genuine macro crises.

---

#### Why HMM?

The market regime is **not directly observable** — we can't measure "risk appetite" directly.
But we can observe its effects (returns, rates, FX, reserves). HMM infers the most likely
hidden state from the observable sequence.

**Gaussian HMM** assumes each regime generates observations from a multivariate Gaussian.
The Viterbi algorithm finds the most likely sequence of regimes given the observations.

---

#### Pakistan-Specific Drivers

- **SBP rate cycle** is THE dominant driver (~40% of KSE variance)
- **PKR depreciation** correlates with equity selloffs
- **KIBOR leads equity** by 2-3 months (monetary transmission lag)
- **SBP reserves** signal external vulnerability and capital flow pressure
- **Current account** drives PKR expectations and IMF program risk
- **T-bill/PIB spread** widens during stress (flight to short duration)
- **Model retraining**: monthly, after new SBP data is available
    """)
