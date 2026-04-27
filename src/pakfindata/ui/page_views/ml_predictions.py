"""ML Price Predictions -- XGBoost/LightGBM direction prediction page."""

from __future__ import annotations

import time
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from pakfindata.engine.ml_features import (
    FEATURE_COLS,
    TICK_FEATURE_COLS,
    build_dataset,
    get_eod_features,
)
from pakfindata.engine.ml_model import (
    get_feature_importance,
    load_model,
    save_model,
    train_model,
    walk_forward_validate,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG,
    plot_bgcolor=DARK_BG,
    font_color="#c9d1d9",
    margin=dict(l=20, r=20, t=40, b=20),
)

TARGET_MAP = {
    "Next-day direction": 1,
    "5-day return": 5,
    "10-day return": 10,
}


@st.cache_data(ttl=3600, show_spinner=False)
def _build_dataset_cached(symbols_key: str, symbols: list[str] | None,
                          lookback: int, horizon: int, ticks: bool):
    return build_dataset(
        symbols=symbols,
        lookback_days=lookback,
        target_horizon=horizon,
        include_ticks=ticks,
    )


# ---------------------------------------------------------------------------
# Tab 1 — Train & Validate
# ---------------------------------------------------------------------------

def _render_train_tab():
    st.subheader("Model Configuration")

    c1, c2, c3 = st.columns(3)
    with c1:
        model_type = st.selectbox("Model", ["xgboost", "lightgbm", "random_forest"],
                                  format_func=lambda x: {"xgboost": "XGBoost",
                                                          "lightgbm": "LightGBM",
                                                          "random_forest": "Random Forest"}[x])
    with c2:
        target_label = st.selectbox("Target", list(TARGET_MAP.keys()))
        target_horizon = TARGET_MAP[target_label]
    with c3:
        sym_mode = st.selectbox("Symbols", ["Top 50 by volume", "Custom list", "Single symbol"])

    c4, c5, c6 = st.columns(3)
    with c4:
        lookback_map = {"1 year": 250, "2 years": 500, "All available": 2000}
        lookback_label = st.selectbox("Training period", list(lookback_map.keys()), index=1)
        lookback = lookback_map[lookback_label]
    with c5:
        include_ticks = st.checkbox("Include tick features", value=False,
                                    help="Adds 8 microstructure features (slower)")
    with c6:
        n_splits = st.slider("CV folds", 3, 10, 5)

    symbols = None
    if sym_mode == "Custom list":
        sym_text = st.text_input("Symbols (comma-separated)", "HUBC,OGDC,PPL,HBL,UBL")
        symbols = [s.strip().upper() for s in sym_text.split(",") if s.strip()]
    elif sym_mode == "Single symbol":
        sym_single = st.text_input("Symbol", "HUBC").strip().upper()
        symbols = [sym_single] if sym_single else None

    if st.button("Train Model", type="primary", width='stretch'):
        feature_cols = FEATURE_COLS + (TICK_FEATURE_COLS if include_ticks else [])
        symbols_key = str(symbols) + str(lookback) + str(target_horizon) + str(include_ticks)

        with st.spinner("Building dataset..."):
            t0 = time.time()
            df = _build_dataset_cached(symbols_key, symbols, lookback,
                                       target_horizon, include_ticks)
            build_time = time.time() - t0

        if df.empty:
            st.error("No data returned. Check that the symbols exist in DuckDB.")
            return

        st.info(f"Dataset: **{len(df):,}** rows, **{len(feature_cols)}** features "
                f"({build_time:.1f}s)")

        with st.spinner("Running walk-forward validation..."):
            t0 = time.time()
            results = walk_forward_validate(
                df, feature_cols, model_type=model_type, n_splits=n_splits,
            )
            train_time = time.time() - t0

        if "error" in results:
            st.error(results["error"])
            return

        overall = results["overall"]
        if "error" in overall:
            st.error(overall["error"])
            return

        st.success(f"Trained in **{train_time:.1f}s** on **{overall['total_predictions']:,}** "
                   f"test samples across {len(results['folds'])} folds")

        # -- Overall metrics --
        st.subheader("Overall Metrics")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Accuracy", f"{overall['accuracy']:.1%}")
        m2.metric("Precision", f"{overall['precision']:.1%}")
        m3.metric("Recall", f"{overall['recall']:.1%}")
        m4.metric("F1 Score", f"{overall['f1']:.3f}")
        m5.metric("AUC-ROC", f"{overall['auc']:.3f}")

        # -- Per-fold table --
        st.subheader("Per-Fold Results")
        fold_df = pd.DataFrame(results["folds"])
        for col in ["accuracy", "precision", "recall", "f1"]:
            fold_df[col] = fold_df[col].map(lambda x: f"{x:.1%}")
        st.dataframe(fold_df, width='stretch', hide_index=True)

        # -- Confusion matrix --
        st.subheader("Confusion Matrix")
        preds = np.array(results["predictions"])
        actuals = np.array(results["actuals"])
        cm = np.zeros((2, 2), dtype=int)
        for a, p in zip(actuals, preds):
            cm[int(a)][int(p)] += 1

        fig_cm = go.Figure(data=go.Heatmap(
            z=cm, x=["Pred Down", "Pred Up"], y=["Actual Down", "Actual Up"],
            text=cm, texttemplate="%{text}",
            colorscale=[[0, "#161b22"], [1, "#d4a017"]],
            showscale=False,
        ))
        fig_cm.update_layout(**PLOT_LAYOUT, height=300, title="Confusion Matrix")
        st.plotly_chart(fig_cm, width='stretch')

        # -- Equity curve --
        st.subheader("Equity Curve (following predictions)")
        probs = np.array(results["probabilities"])
        # Reconstruct returns from actuals: actual=1 means up, actual=0 means down
        # Assume equal-sized bets, +1% for correct, -1% for incorrect (simplified)
        correct = (preds == actuals).astype(float)
        strategy_returns = np.where(correct, 0.01, -0.01)
        equity = np.cumprod(1 + strategy_returns)

        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            y=equity, mode="lines", name="ML Strategy",
            line=dict(color="#d4a017", width=2),
        ))
        fig_eq.add_hline(y=1.0, line_dash="dash", line_color="#666")
        fig_eq.update_layout(**PLOT_LAYOUT, height=350, title="Cumulative Equity",
                             yaxis_title="Growth of $1", xaxis_title="Trade #")
        st.plotly_chart(fig_eq, width='stretch')

        # -- Train final model and save --
        with st.spinner("Training final model on full dataset..."):
            avail = [c for c in feature_cols if c in df.columns]
            X_full = df[avail].replace([np.inf, -np.inf], np.nan).fillna(0)
            y_full = df["target_direction"]
            final_model, final_scaler = train_model(X_full, y_full, model_type=model_type)

            metadata = {
                "model_type": model_type,
                "target_horizon": target_horizon,
                "feature_cols": avail,
                "n_samples": len(df),
                "accuracy": overall["accuracy"],
                "auc": overall["auc"],
                "trained_at": datetime.now().isoformat(),
                "cv_sample_predictions": results.get("sample_predictions", []),
            }
            path = save_model(final_model, final_scaler, metadata)

        st.success(f"Model saved to `{path}`")

        # Bootstrap credibility from CV results
        cv_preds = results.get("sample_predictions", [])
        if cv_preds:
            from pakfindata.engine.ml_model import BayesianCredibility
            tracker = BayesianCredibility()
            added = tracker.seed_from_cv(cv_preds)
            if added > 0:
                cred = tracker.get_credibility()
                st.success(
                    f"Credibility bootstrapped: **{added}** CV predictions seeded "
                    f"→ **{cred['assessment']}** ({cred['posterior_mean']:.1%} accuracy)"
                )

        # Store for other tabs
        st.session_state["ml_model"] = final_model
        st.session_state["ml_scaler"] = final_scaler
        st.session_state["ml_metadata"] = metadata
        st.session_state["ml_dataset"] = df
        st.session_state["ml_results"] = results


# ---------------------------------------------------------------------------
# Tab 2 — Live Predictions
# ---------------------------------------------------------------------------

def _render_predictions_tab():
    st.subheader("Today's Predictions")

    saved = load_model()
    if saved is None and "ml_model" not in st.session_state:
        st.warning("No trained model found. Train a model first in the **Train & Validate** tab.")
        return

    if "ml_model" in st.session_state:
        model = st.session_state["ml_model"]
        scaler = st.session_state["ml_scaler"]
        meta = st.session_state["ml_metadata"]
    else:
        model = saved["model"]
        scaler = saved["scaler"]
        meta = saved["metadata"]

    feature_cols = meta.get("feature_cols", FEATURE_COLS)

    st.caption(f"Model: **{meta.get('model_type', 'xgboost')}** | "
               f"Horizon: **{meta.get('target_horizon', 1)}-day** | "
               f"CV Accuracy: **{meta.get('accuracy', 0):.1%}** | "
               f"Trained: {meta.get('trained_at', 'unknown')[:16]}")

    # Bayesian credibility panel
    from pakfindata.engine.ml_model import BayesianCredibility, compute_expected_value
    from pakfindata.engine.ml_features import compute_return_targets
    from scipy import stats as sp_stats
    from datetime import datetime, timezone, timedelta

    tracker = BayesianCredibility()

    # Resolve pending predictions from previous days
    try:
        updated = tracker.update_actuals()
        if updated > 0:
            st.toast(f"Updated {updated} previous predictions with actual results")
    except Exception:
        pass

    cred = tracker.get_credibility()

    # Auto-seed from CV if credibility is empty and model has CV data
    if cred["assessment"] == "NO_DATA":
        try:
            _saved = load_model("latest")
            if _saved:
                cv_preds = _saved.get("metadata", {}).get("cv_sample_predictions", [])
                if cv_preds:
                    added = tracker.seed_from_cv(cv_preds)
                    if added > 0:
                        cred = tracker.get_credibility()
        except Exception:
            pass
    _render_credibility_panel(cred)

    sym_input = st.text_input(
        "Symbols (comma-separated, blank = top 30)",
        help="Leave blank to predict top 30 by volume",
    )

    if st.button("Run Predictions", type="primary"):
        if sym_input.strip():
            symbols = [s.strip().upper() for s in sym_input.split(",") if s.strip()]
        else:
            from pakfindata.db.connections import _duck_con
            con = _duck_con()
            symbols = [r[0] for r in con.execute("""
                SELECT symbol FROM eod_ohlcv
                WHERE date >= (SELECT MAX(date) FROM eod_ohlcv)
                GROUP BY symbol
                ORDER BY SUM(volume) DESC
                LIMIT 30
            """).fetchall()]

        pkt = timezone(timedelta(hours=5))
        today = datetime.now(pkt).strftime("%Y-%m-%d")

        rows = []
        progress = st.progress(0)
        for i, sym in enumerate(symbols):
            progress.progress((i + 1) / len(symbols))
            df = get_eod_features(sym, lookback_days=250)
            if df.empty or len(df) < 50:
                continue

            latest = df.iloc[[-1]].reindex(columns=feature_cols, fill_value=0)
            latest = latest.replace([np.inf, -np.inf], np.nan).fillna(0)
            X = scaler.transform(latest)

            pred = model.predict(X)[0]
            prob = (
                model.predict_proba(X)[0][1]
                if hasattr(model, "predict_proba")
                else float(pred)
            )

            signal = "BUY" if pred == 1 else "SELL"
            current_price = float(df.iloc[-1]["close"])

            # Compute expected PKR value
            rt_df = compute_return_targets(df)
            last_row = rt_df.iloc[-1]
            hist_up = float(last_row.get("hist_up_move_avg", 0.8) or 0.8)
            hist_dn = float(last_row.get("hist_dn_move_avg", 0.6) or 0.6)
            hist_vol = float(last_row.get("hist_volatility", 25.0) or 25.0)

            ev = compute_expected_value(prob, current_price, hist_up, hist_dn, hist_vol)

            # Get per-symbol credibility
            sym_cred = tracker.get_credibility(symbol=sym)

            rows.append({
                "Symbol": sym,
                "Direction": signal,
                "Probability": prob if pred == 1 else (1 - prob),
                "Close": current_price,
                "Predicted": ev["expected_price"],
                "Move (PKR)": ev["expected_move_pkr"],
                "EV (PKR)": ev["expected_move_pkr"],
                "R/R": ev["risk_reward"],
                "Credibility": sym_cred["assessment"],
            })

            # Log prediction
            try:
                tracker.log_prediction(
                    symbol=sym, date=today,
                    predicted_dir=signal, prob=float(prob),
                    predicted_price=ev["expected_price"],
                )
            except Exception:
                pass

        progress.empty()

        if not rows:
            st.warning("No predictions generated.")
            return

        pred_df = pd.DataFrame(rows).sort_values("EV (PKR)", ascending=False)

        # Color coding
        def _color_direction(val):
            return "color: #3fb950" if val == "BUY" else "color: #f85149"

        def _color_prob(val):
            if val >= 0.6:
                return "color: #3fb950"
            elif val <= 0.4:
                return "color: #f85149"
            return "color: #8b949e"

        def _color_ev(val):
            return "color: #3fb950" if val > 0 else "color: #f85149" if val < 0 else ""

        def _color_cred(val):
            colors = {"STRONG": "#00BCD4", "CREDIBLE": "#00E676", "MARGINAL": "#FFB300",
                       "NOT_CREDIBLE": "#FF5252", "INSUFFICIENT": "#6B7280", "NO_DATA": "#6B7280"}
            return f"color: {colors.get(val, '#6B7280')}"

        styled = pred_df.style.map(
            _color_direction, subset=["Direction"]
        ).map(
            _color_prob, subset=["Probability"]
        ).map(
            _color_ev, subset=["Move (PKR)", "EV (PKR)"]
        ).map(
            _color_cred, subset=["Credibility"]
        ).format({
            "Probability": "{:.1%}",
            "Close": "{:,.2f}",
            "Predicted": "{:,.2f}",
            "Move (PKR)": "{:+,.2f}",
            "EV (PKR)": "{:+,.2f}",
            "R/R": "{:.1f}x",
        })

        st.dataframe(styled, width='stretch', hide_index=True, height=600)

        # Summary
        buys = sum(1 for r in rows if r["Direction"] == "BUY")
        sells = len(rows) - buys
        pos_ev = sum(1 for r in rows if r["EV (PKR)"] > 0)
        st.caption(f"**{buys}** BUY | **{sells}** SELL | "
                   f"**{pos_ev}** positive EV | **{len(rows)}** total")

    # Per-symbol breakdown
    breakdown = tracker.get_symbol_breakdown()
    if not breakdown.empty:
        with st.expander("Per-Symbol Model Credibility", expanded=False):
            st.dataframe(breakdown, width='stretch', hide_index=True)

    # Posterior distribution
    if cred["n_predictions"] > 0:
        with st.expander("Bayesian Posterior Distribution", expanded=False):
            _render_posterior_chart(cred["alpha"], cred["beta_param"])


def _render_credibility_panel(c: dict):
    """Render Bayesian credibility assessment banner."""
    st.markdown(f"""
    <div style="background:#141820;border:1px solid #1E2530;border-radius:8px;padding:16px;margin-bottom:16px;">
      <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
        <div>
          <span style="color:#6B7280;font-size:10px;">MODEL CREDIBILITY</span><br>
          <span style="color:{c['color']};font-weight:900;font-size:18px;">{c['assessment']}</span>
        </div>
        <div>
          <span style="color:#6B7280;font-size:10px;">BAYESIAN ACCURACY</span><br>
          <span style="font-weight:700;font-size:16px;">{c['posterior_mean']:.1%}</span>
          <span style="color:#6B7280;font-size:10px;">
            ({c['credible_interval'][0]:.0%} – {c['credible_interval'][1]:.0%} 90% CI)
          </span>
        </div>
        <div>
          <span style="color:#6B7280;font-size:10px;">P(BETTER THAN RANDOM)</span><br>
          <span style="font-weight:700;font-size:16px;">{c['prob_better_than_random']:.0%}</span>
        </div>
        <div>
          <span style="color:#6B7280;font-size:10px;">TRACK RECORD</span><br>
          <span style="font-weight:700;">{c['n_correct']}/{c['n_predictions']}</span>
          <span style="color:{'#00E676' if c['streak_type']=='win' else '#FF5252' if c['streak_type']=='loss' else '#6B7280'};font-size:10px;">
            {c['streak']}{'W' if c['streak_type']=='win' else 'L' if c['streak_type']=='loss' else ''} streak
          </span>
        </div>
      </div>
      <div style="margin-top:8px;color:#6B7280;font-size:11px;">{c['assessment_text']}</div>
    </div>
    """, unsafe_allow_html=True)


def _render_posterior_chart(alpha: int, beta_param: int):
    """Show Beta posterior distribution."""
    from scipy import stats as sp_stats

    x = np.linspace(0, 1, 200)
    y = sp_stats.beta.pdf(x, alpha, beta_param)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x * 100, y=y, mode="lines", fill="tozeroy",
        line=dict(color="#00BCD4", width=2),
        fillcolor="rgba(0,188,212,0.15)",
        name="Posterior",
    ))
    fig.add_vline(x=50, line_dash="dot", line_color="#FF5252",
                  annotation_text="Random (50%)")
    fig.add_vline(x=55, line_dash="dot", line_color="#FFB300",
                  annotation_text="PSX Baseline (55%)")
    fig.add_vline(x=alpha / (alpha + beta_param) * 100, line_dash="solid",
                  line_color="#00E676", annotation_text="Model")

    fig.update_layout(
        paper_bgcolor="#0B0E11", plot_bgcolor="#0B0E11",
        font_color="#6B7280", height=200,
        margin=dict(l=10, r=10, t=10, b=30),
        xaxis_title="Accuracy %", yaxis_visible=False,
        showlegend=False,
    )
    st.plotly_chart(fig, width='stretch')


# ---------------------------------------------------------------------------
# Tab 3 — Feature Importance
# ---------------------------------------------------------------------------

def _render_features_tab():
    st.subheader("Feature Analysis")

    model = st.session_state.get("ml_model")
    meta = st.session_state.get("ml_metadata")
    dataset = st.session_state.get("ml_dataset")

    if model is None:
        saved = load_model()
        if saved:
            model = saved["model"]
            meta = saved["metadata"]
        else:
            st.warning("Train a model first.")
            return

    feature_cols = meta.get("feature_cols", FEATURE_COLS)
    imp_df = get_feature_importance(model, feature_cols)

    # Top 20 bar chart
    top20 = imp_df.head(20)
    fig = go.Figure(go.Bar(
        x=top20["importance"].values[::-1],
        y=top20["feature"].values[::-1],
        orientation="h",
        marker_color="#d4a017",
    ))
    fig.update_layout(**PLOT_LAYOUT, height=500, title="Top 20 Features by Importance",
                      xaxis_title="Importance", yaxis_title="")
    st.plotly_chart(fig, width='stretch')

    # Full table
    with st.expander("All features"):
        st.dataframe(imp_df.reset_index(drop=True), width='stretch', hide_index=True)

    # Feature correlation heatmap (if dataset available)
    if dataset is not None and not dataset.empty:
        st.subheader("Feature Correlation")
        avail = [c for c in feature_cols if c in dataset.columns]
        top_feats = imp_df.head(15)["feature"].tolist()
        top_avail = [c for c in top_feats if c in avail]

        if top_avail:
            corr = dataset[top_avail].corr()
            fig_corr = go.Figure(data=go.Heatmap(
                z=corr.values,
                x=corr.columns.tolist(),
                y=corr.index.tolist(),
                colorscale="RdYlGn",
                zmin=-1, zmax=1,
                text=corr.values.round(2),
                texttemplate="%{text}",
            ))
            fig_corr.update_layout(**PLOT_LAYOUT, height=500,
                                   title="Top 15 Feature Correlations")
            st.plotly_chart(fig_corr, width='stretch')

    # SHAP values
    try:
        import shap

        if dataset is not None and not dataset.empty:
            st.subheader("SHAP Feature Impact")
            avail = [c for c in feature_cols if c in dataset.columns]
            sample = dataset[avail].replace([np.inf, -np.inf], np.nan).fillna(0).sample(min(500, len(dataset)), random_state=42)

            with st.spinner("Computing SHAP values..."):
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(sample)

            if isinstance(shap_values, list):
                shap_values = shap_values[1]

            mean_shap = np.abs(shap_values).mean(axis=0)
            shap_df = pd.DataFrame({
                "feature": avail,
                "mean_|SHAP|": mean_shap,
            }).sort_values("mean_|SHAP|", ascending=False).head(20)

            fig_shap = go.Figure(go.Bar(
                x=shap_df["mean_|SHAP|"].values[::-1],
                y=shap_df["feature"].values[::-1],
                orientation="h",
                marker_color="#58a6ff",
            ))
            fig_shap.update_layout(**PLOT_LAYOUT, height=500,
                                   title="Top 20 Features by SHAP Impact",
                                   xaxis_title="Mean |SHAP value|")
            st.plotly_chart(fig_shap, width='stretch')

    except Exception:
        pass  # SHAP is optional


# ---------------------------------------------------------------------------
# Tab 4 — Backtest
# ---------------------------------------------------------------------------

def _render_backtest_tab():
    st.subheader("Strategy Backtest")

    results = st.session_state.get("ml_results")
    dataset = st.session_state.get("ml_dataset")

    if results is None or "error" in results:
        st.warning("Run training first to generate backtest data.")
        return

    probs = np.array(results["probabilities"])
    actuals = np.array(results["actuals"])

    c1, c2 = st.columns(2)
    with c1:
        threshold = st.slider("Probability threshold", 0.50, 0.80, 0.55, 0.01,
                              help="Only trade when P(up) > threshold or P(up) < 1-threshold")
    with c2:
        sizing = st.selectbox("Position sizing", ["Equal weight", "Probability-weighted"])

    # Simulate strategy
    trades = []
    for i in range(len(probs)):
        p = probs[i]
        actual_dir = actuals[i]  # 1=up, 0=down
        actual_ret = 0.01 if actual_dir == 1 else -0.01  # simplified

        if p >= threshold:
            # Go long
            weight = (p - 0.5) * 2 if sizing == "Probability-weighted" else 1.0
            trades.append({"type": "LONG", "prob": p, "return": actual_ret * weight,
                           "correct": actual_dir == 1})
        elif p <= (1 - threshold):
            # Go short
            weight = (0.5 - p) * 2 if sizing == "Probability-weighted" else 1.0
            trades.append({"type": "SHORT", "prob": p, "return": -actual_ret * weight,
                           "correct": actual_dir == 0})
        # else: no trade (neutral zone)

    if not trades:
        st.warning("No trades generated at this threshold. Lower the threshold.")
        return

    trade_df = pd.DataFrame(trades)
    rets = trade_df["return"].values
    equity = np.cumprod(1 + rets)
    buy_hold = np.cumprod(1 + np.where(actuals == 1, 0.01, -0.01))

    # Metrics
    total_return = equity[-1] - 1
    n_trades = len(trades)
    win_rate = trade_df["correct"].mean()
    sharpe = rets.mean() / rets.std() * np.sqrt(245) if rets.std() > 0 else 0

    # Max drawdown
    peak = np.maximum.accumulate(equity)
    drawdown = (equity - peak) / peak
    max_dd = drawdown.min()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Return", f"{total_return:.1%}")
    m2.metric("Trades", f"{n_trades:,}")
    m3.metric("Win Rate", f"{win_rate:.1%}")
    m4.metric("Sharpe Ratio", f"{sharpe:.2f}")
    m5.metric("Max Drawdown", f"{max_dd:.1%}")

    # Equity curve
    fig = go.Figure()
    fig.add_trace(go.Scatter(y=equity, mode="lines", name="ML Strategy",
                             line=dict(color="#d4a017", width=2)))
    fig.add_trace(go.Scatter(y=buy_hold, mode="lines", name="Buy & Hold",
                             line=dict(color="#8b949e", width=1, dash="dash")))
    fig.add_hline(y=1.0, line_dash="dot", line_color="#444")
    fig.update_layout(**PLOT_LAYOUT, height=400, title="ML Strategy vs Buy & Hold",
                      yaxis_title="Growth of $1", xaxis_title="Trade #",
                      legend=dict(x=0.02, y=0.98))
    st.plotly_chart(fig, width='stretch')

    # Monthly returns heatmap (simplified — use trade index as proxy)
    st.subheader("Return Distribution")
    fig_hist = go.Figure(go.Histogram(
        x=rets, nbinsx=50,
        marker_color="#d4a017",
    ))
    fig_hist.add_vline(x=0, line_dash="dash", line_color="#f85149")
    fig_hist.update_layout(**PLOT_LAYOUT, height=300, title="Trade Return Distribution",
                           xaxis_title="Return per Trade", yaxis_title="Count")
    st.plotly_chart(fig_hist, width='stretch')

    # Long vs Short breakdown
    st.subheader("Long vs Short Performance")
    for trade_type in ["LONG", "SHORT"]:
        subset = trade_df[trade_df["type"] == trade_type]
        if len(subset) > 0:
            wr = subset["correct"].mean()
            avg_ret = subset["return"].mean()
            st.caption(f"**{trade_type}**: {len(subset)} trades | "
                       f"Win rate: {wr:.1%} | Avg return: {avg_ret:.3%}")


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def render_ml_predictions():
    st.title("ML Price Predictions")
    st.caption("XGBoost / LightGBM direction prediction using 40+ technical + microstructure features")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Train & Validate", "Live Predictions", "Feature Importance", "Backtest",
    ])

    with tab1:
        _render_train_tab()
    with tab2:
        _render_predictions_tab()
    with tab3:
        _render_features_tab()
    with tab4:
        _render_backtest_tab()
