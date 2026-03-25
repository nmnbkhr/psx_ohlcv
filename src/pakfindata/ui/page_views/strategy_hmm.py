"""Macro Regime HMM Strategy — cross-asset regime detection and allocation."""

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
    "cyan": "#00BCD4", "accent": "#2196F3",
}
_CHART = dict(paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
              margin=dict(t=30, b=20, l=50, r=20))

_RC = {"RISK_ON": "#22C55E", "TRANSITION": "#EAB308", "RISK_OFF": "#F97316", "CRISIS": "#EF4444"}


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.markdown("### Macro Regime Model (HMM)")
    st.caption("Hidden Markov Model — cross-asset regime detection for allocation")

    tab_current, tab_bt, tab_train, tab_method = st.tabs(["Current Regime", "Backtest", "Train Model", "Methodology"])

    with tab_current:
        _render_current()
    with tab_bt:
        _render_backtest()
    with tab_train:
        _render_train()
    with tab_method:
        _render_methodology()

    render_footer()


def _render_current():
    from pakfindata.engine.macro_regime_hmm import get_current_regime, MODEL_PATH

    if not MODEL_PATH.exists():
        st.warning("No trained model. Go to Train Model tab first.")
        return

    with st.spinner("Loading regime..."):
        state = get_current_regime()

    if not state:
        st.error("Could not compute current regime")
        return

    regime = state["regime"]
    rc = _RC.get(regime, _C["dim"])

    # Regime badge
    st.markdown(f"""
    <div style="background:{_C['card']};padding:24px;border-radius:12px;border-left:6px solid {rc};margin-bottom:16px;">
        <div style="color:{_C['dim']};font-size:0.8em;text-transform:uppercase;">Current Macro Regime</div>
        <div style="color:{rc};font-size:2.5em;font-weight:700;">{regime.replace('_',' ')}</div>
        <div style="color:{_C['dim']};font-size:0.85em;">Confidence: {state['probability']:.0%} | Month: {state['date']} | Model: {state['model_trained'][:10]}</div>
    </div>
    """, unsafe_allow_html=True)

    # KPIs
    mc = st.columns(5)
    with mc[0]:
        _kpi("KSE Momentum (3M)", f"{state['kse_momentum']:+.1%}")
    with mc[1]:
        _kpi("KIBOR 3M", f"{state['kibor']:.1f}%")
    with mc[2]:
        _kpi("PKR/USD", f"{state['pkr_usd']:.1f}")
    with mc[3]:
        _kpi("SBP Cycle", state["sbp_cycle"])
    with mc[4]:
        alloc = state["allocation"]
        _kpi("Equity Alloc", f"{alloc['equity']:.0%}", _C["up"] if alloc["equity"] > 0.5 else _C["down"])

    # Regime probabilities
    st.markdown("#### Regime Probabilities")
    probs = state["probs"]
    fig = go.Figure(go.Bar(
        x=list(probs.keys()), y=list(probs.values()),
        marker_color=[_RC.get(k, _C["dim"]) for k in probs.keys()],
        text=[f"{v:.0%}" for v in probs.values()],
        textposition="auto",
    ))
    fig.update_layout(**_CHART, height=250, yaxis=dict(gridcolor=_C["grid"], range=[0, 1], title="Probability"))
    st.plotly_chart(fig, use_container_width=True)

    # Allocation pie
    st.markdown("#### Recommended Allocation")
    alloc = state["allocation"]
    fig = go.Figure(go.Pie(
        labels=list(alloc.keys()), values=list(alloc.values()),
        hole=0.4, marker=dict(colors=[_C["accent"], _C["amber"], _C["dim"]]),
        textinfo="label+percent",
    ))
    fig.update_layout(**_CHART, height=250)
    st.plotly_chart(fig, use_container_width=True)


def _render_backtest():
    from pakfindata.engine.macro_regime_hmm import load_hmm, load_macro_features, predict_regime, backtest_regime_allocation, MODEL_PATH

    if not MODEL_PATH.exists():
        st.warning("Train model first.")
        return

    model_dict = load_hmm()
    if not model_dict:
        st.error("Failed to load model")
        return

    df = load_macro_features()
    pred_df = predict_regime(model_dict, df)
    bt = backtest_regime_allocation(pred_df)

    if "error" in bt:
        st.error(bt["error"])
        return

    m = bt["metrics"]

    # Metrics comparison
    st.markdown("#### Strategy vs Buy & Hold")
    mc = st.columns(6)
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
    for i, col in enumerate(mc):
        with col:
            _kpi(labels[i], values[i], colors[i])

    rdf = bt["df"]

    # Equity curves
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.6, 0.4], vertical_spacing=0.05)

    fig.add_trace(go.Scatter(x=rdf["month"], y=rdf["strategy_equity"], name="Strategy",
                             line=dict(color=_C["accent"], width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=rdf["month"], y=rdf["bh_equity"], name="Buy & Hold",
                             line=dict(color=_C["dim"], width=1, dash="dot")), row=1, col=1)

    # Regime overlay
    for r_name, r_color in _RC.items():
        mask = rdf["regime"] == r_name
        if mask.any():
            fig.add_trace(go.Scatter(
                x=rdf.loc[mask, "month"], y=rdf.loc[mask, "regime_id"],
                mode="markers", marker=dict(color=r_color, size=8),
                name=r_name, showlegend=True,
            ), row=2, col=1)

    fig.update_layout(**_CHART, height=500,
                      legend=dict(orientation="h", y=1.08, bgcolor="rgba(0,0,0,0)"),
                      yaxis=dict(gridcolor=_C["grid"], title="Equity"),
                      yaxis2=dict(gridcolor=_C["grid"], title="Regime", tickvals=[0, 1, 2, 3],
                                  ticktext=["Risk On", "Transition", "Risk Off", "Crisis"]))
    st.plotly_chart(fig, use_container_width=True)

    # Regime durations
    durations = bt.get("regime_durations", {})
    if durations:
        st.markdown("#### Regime Duration (months)")
        fig = go.Figure(go.Bar(
            x=list(durations.keys()), y=list(durations.values()),
            marker_color=[_RC.get(k, _C["dim"]) for k in durations.keys()],
            text=[str(v) for v in durations.values()], textposition="auto",
        ))
        fig.update_layout(**_CHART, height=250, yaxis=dict(gridcolor=_C["grid"]))
        st.plotly_chart(fig, use_container_width=True)


def _render_train():
    from pakfindata.engine.macro_regime_hmm import train_and_save, MODEL_PATH

    st.markdown("#### Train / Retrain HMM Model")
    st.caption(f"Model location: `{MODEL_PATH}`")

    if MODEL_PATH.exists():
        import joblib
        m = joblib.load(MODEL_PATH)
        st.success(f"Existing model: trained {m.get('trained_at', 'unknown')}, {m.get('months_trained', '?')} months, LL={m.get('log_likelihood', 0):.1f}")

    if st.button("Train Model", type="primary", key="hmm_train"):
        with st.spinner("Training HMM on macro features (KIBOR, FX, KSE, SBP rates)..."):
            result = train_and_save()

        if "error" in result:
            st.error(result["error"])
        else:
            st.success(f"Model trained on {result['months_trained']} months and saved to `{result['model_path']}`")
            st.markdown(f"**Current regime: {result['current_regime']}**")

            m = result["backtest"]
            if m:
                st.markdown(f"Strategy: {m['strategy_return']:+.1%} | B&H: {m['bh_return']:+.1%} | Sharpe: {m['strategy_sharpe']:.2f} | DD: {m['strategy_max_dd']:.1%}")


def _render_methodology():
    st.markdown("""
#### Hidden Markov Model for Macro Regimes

**Observables** (what we see):
1. KSE-100 momentum (monthly returns, 3M momentum, volatility)
2. KIBOR 3M direction (rate changes)
3. PKR/USD trend (depreciation rate)
4. SBP policy cycle (easing/tightening/hold)

**Hidden States** (what we infer):
| Regime | Equity | Bonds | Cash | Characteristics |
|---|---|---|---|---|
| Risk On | 80% | 10% | 10% | KSE rising, rates falling, PKR stable |
| Transition | 40% | 30% | 30% | Mixed signals, regime changing |
| Risk Off | 20% | 50% | 30% | KSE falling, rates rising, PKR weak |
| Crisis | 0% | 30% | 70% | Sharp drawdowns, liquidity freeze |

---

#### Why HMM?

The market regime is **not directly observable** — we can't measure "risk appetite" directly.
But we can observe its effects (returns, rates, FX). HMM infers the most likely hidden state
from the observable sequence.

**Gaussian HMM** assumes each regime generates observations from a multivariate Gaussian.
The Viterbi algorithm finds the most likely sequence of regimes given the observations.

---

#### Pakistan-Specific Drivers

- **SBP rate cycle** is THE dominant driver (~40% of KSE variance)
- **PKR depreciation** correlates with equity selloffs
- **KIBOR leads equity** by 2-3 months (monetary transmission lag)
- **Model retraining**: monthly, after new SBP data is available
    """)
