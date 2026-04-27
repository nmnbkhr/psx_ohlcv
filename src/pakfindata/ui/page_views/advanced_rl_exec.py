"""RL Execution Agent -- adaptive order execution via reinforcement learning."""

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
    st.markdown("### RL Execution Agent")
    st.caption(
        "Reinforcement learning for optimal order execution -- "
        "learns to beat VWAP/TWAP from historical tick replay"
    )

    tab1, tab2, tab3, tab4 = st.tabs([
        "Train Agent", "Live Execution", "Benchmark Comparison", "Methodology"
    ])

    # ------------------------------------------------------------------
    # TAB 1: Train Agent
    # ------------------------------------------------------------------
    with tab1:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            symbol = st.text_input("Symbol", value="OGDC", key="rle_sym")
        with c2:
            total_shares = st.number_input("Order size", 10_000, 1_000_000, 100_000, 10_000, key="rle_shares")
        with c3:
            side = st.selectbox("Side", ["BUY", "SELL"], key="rle_side")
        with c4:
            algorithm = st.selectbox("Algorithm", ["PPO", "SAC"], key="rle_algo")

        c5, c6 = st.columns(2)
        with c5:
            n_days = st.slider("Training days", 10, 60, 30, key="rle_days")
        with c6:
            timesteps = st.select_slider(
                "Training timesteps",
                options=[10_000, 50_000, 100_000, 200_000, 500_000],
                value=50_000, key="rle_steps",
            )

        if st.button("Train Agent", type="primary", key="rle_train"):
            progress = st.progress(0, text=f"Loading {n_days} days of {symbol} replay data...")

            try:
                from pakfindata.engine.rl_execution import train_execution_agent
                progress.progress(10, text=f"Training {algorithm} ({timesteps:,} steps, may take 1-5 min)...")
                result = train_execution_agent(
                    symbol=symbol.upper(), total_shares=total_shares,
                    side=side, n_episodes_data=n_days,
                    total_timesteps=timesteps, algorithm=algorithm,
                )
                progress.progress(100, text="Done!")
                progress.empty()
            except Exception as e:
                progress.empty()
                st.error(f"Error: {e}")
                return

            if "error" in result:
                st.error(result["error"])
                return

            st.success(f"Model saved to `{result['model_path']}`")

            # KPIs
            k1, k2, k3, k4, k5 = st.columns(5)
            with k1:
                v = result["rl_avg_slippage_bps"]
                _kpi("RL Avg Slip", f"{v:+.1f} bps", _C["up"] if v < 0 else _C["down"])
            with k2:
                v = result["vwap_avg_slippage_bps"]
                _kpi("VWAP Avg Slip", f"{v:+.1f} bps", _C["dim"])
            with k3:
                v = result["twap_avg_slippage_bps"]
                _kpi("TWAP Avg Slip", f"{v:+.1f} bps", _C["dim"])
            with k4:
                v = result["rl_beats_vwap_pct"]
                _kpi("RL Beats VWAP", f"{v:.0f}%", _C["up"] if v > 50 else _C["down"])
            with k5:
                _kpi("Device", result["device"], _C["cyan"])

            # Improvement
            improvement = result["vwap_avg_slippage_bps"] - result["rl_avg_slippage_bps"]
            if improvement > 0:
                st.success(f"RL agent saves **{improvement:.1f} bps** vs static VWAP on average")
            else:
                st.warning(f"RL agent is {abs(improvement):.1f} bps worse than static VWAP -- "
                           f"try more training steps or different hyperparams")

            # Extra KPIs
            k6, k7, k8 = st.columns(3)
            with k6:
                _kpi("Train Days", str(result["train_days"]))
            with k7:
                _kpi("Eval Days", str(result["eval_days"]))
            with k8:
                _kpi("Avg Reward", f"{result['rl_avg_reward']:.1f}")

            # Per-day breakdown
            if "per_day" in result and result["per_day"]:
                st.markdown("**Per-day evaluation:**")
                day_df = pd.DataFrame(result["per_day"])

                # Bar chart: RL vs VWAP vs TWAP
                if not day_df.empty:
                    vwap_slips = [r["slippage_bps"] for r in pd.DataFrame(
                        [{"slippage_bps": result["vwap_avg_slippage_bps"]}] * len(day_df)
                    ).to_dict("records")]

                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=day_df["date"], y=day_df["slippage_bps"],
                        name="RL Agent", marker_color=_C["accent"],
                    ))
                    fig.add_hline(y=result["vwap_avg_slippage_bps"],
                                  line_dash="dash", line_color=_C["amber"],
                                  annotation_text=f"VWAP avg: {result['vwap_avg_slippage_bps']:+.1f} bps")
                    fig.add_hline(y=0, line_dash="dot", line_color=_C["dim"])
                    fig.update_layout(**PLOT_LAYOUT, height=300,
                                      title_text="Implementation Shortfall (bps) -- lower is better")
                    st.plotly_chart(fig, width='stretch')

                st.dataframe(day_df.round(2), width='stretch', hide_index=True)

    # ------------------------------------------------------------------
    # TAB 2: Live Execution
    # ------------------------------------------------------------------
    with tab2:
        st.subheader("Simulate Execution on Latest Day")

        c1, c2 = st.columns(2)
        with c1:
            live_sym = st.text_input("Symbol", value="OGDC", key="rle_live_sym")
        with c2:
            live_algo = st.selectbox("Model", ["PPO", "SAC"], key="rle_live_algo")

        if st.button("Run Simulation", key="rle_live_run"):
            with st.spinner("Loading model and replaying..."):
                try:
                    from pakfindata.engine.rl_execution import (
                        load_trained_agent, load_replay_episodes, create_execution_env,
                    )
                    model = load_trained_agent(live_sym.upper(), live_algo)
                    if model is None:
                        st.error(f"No trained model for {live_sym}/{live_algo}. Train first in Tab 1.")
                        return

                    episodes = load_replay_episodes(live_sym.upper(), n_days=1)
                    if not episodes:
                        st.error("No replay data available.")
                        return

                    ep = episodes[0]
                    env = create_execution_env(live_sym.upper(), 100_000, "BUY", [ep])
                    obs, _ = env.reset()

                    trajectory = []
                    done = False
                    while not done:
                        action, _ = model.predict(obs, deterministic=True)
                        obs, reward, done, _, _ = env.step(action)
                        trajectory.append({
                            "step": env._step,
                            "remaining_pct": env._remaining / env.total_shares * 100,
                            "exec_rate": float(action[0]),
                            "aggression": float(action[1]),
                            "reward": reward,
                        })
                except Exception as e:
                    st.error(f"Error: {e}")
                    return

            traj_df = pd.DataFrame(trajectory)

            # Summary KPIs
            k1, k2, k3 = st.columns(3)
            with k1:
                _kpi("Steps Used", str(len(traj_df)))
            with k2:
                _kpi("Total Reward", f"{traj_df['reward'].sum():.1f}")
            with k3:
                _kpi("Avg Exec Rate", f"{traj_df['exec_rate'].mean()*100:.1f}%")

            # Trajectory plots
            fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                subplot_titles=["Inventory Remaining (%)", "Execution Rate (%)", "Price Aggression"],
                                vertical_spacing=0.08)

            fig.add_trace(go.Scatter(
                x=traj_df["step"], y=traj_df["remaining_pct"],
                mode="lines", name="Remaining %",
                line=dict(color=_C["cyan"], width=2),
            ), row=1, col=1)

            fig.add_trace(go.Bar(
                x=traj_df["step"], y=traj_df["exec_rate"] * 100,
                name="Exec Rate %", marker_color=_C["accent"],
            ), row=2, col=1)

            fig.add_trace(go.Scatter(
                x=traj_df["step"], y=traj_df["aggression"],
                mode="lines", name="Aggression",
                line=dict(color=_C["amber"], width=1.5),
            ), row=3, col=1)
            fig.add_hline(y=0, line_dash="dash", line_color=_C["dim"], row=3, col=1)

            fig.update_layout(**PLOT_LAYOUT, height=600, showlegend=False)
            st.plotly_chart(fig, width='stretch')

    # ------------------------------------------------------------------
    # TAB 3: Benchmark Comparison
    # ------------------------------------------------------------------
    with tab3:
        st.subheader("RL vs Static Baselines")
        st.markdown("""
        After training, the RL agent is compared against:

        | Metric | RL Agent | VWAP | TWAP |
        |--------|---------|------|------|
        | Adapts to spread | Yes | No | No |
        | Adapts to momentum | Yes | No | No |
        | Adapts to volume surges | Yes | No | No |
        | Handles illiquidity | Yes (goes passive) | No (same rate) | No (same rate) |
        | Completion guarantee | Learned (MOC penalty) | By design | By design |

        **Target:** RL should beat VWAP by 3-10 bps on PSX.
        - Liquid names (OGDC, HBL): expect 2-5 bps improvement
        - Mid-cap names: expect 5-15 bps (more inefficiency to exploit)
        """)

    # ------------------------------------------------------------------
    # TAB 4: Methodology
    # ------------------------------------------------------------------
    with tab4:
        st.subheader("RL for Optimal Execution")
        st.markdown("""
        **Problem:** Execute a large order (e.g. 100K shares) over a trading session
        while minimizing implementation shortfall (slippage vs arrival price).

        **Why RL?** Static VWAP/TWAP can't adapt to:
        - Spread widening (RL goes passive, waits)
        - Volume surges (RL executes more when liquidity is available)
        - Adverse momentum (RL slows down to avoid chasing)
        - Favorable momentum (RL speeds up to capture better prices)

        ---

        ### State Space (12 dimensions)

        | Feature | Range | Why |
        |---------|-------|-----|
        | Inventory remaining | 0-1 | How much left to execute |
        | Time remaining | 0-1 | Urgency increases as session ends |
        | VWAP distance | bps | Am I beating or lagging VWAP? |
        | Current spread | bps | Wide spread -> go passive |
        | Volume ratio | 0-5x | High volume -> execute more |
        | Momentum (5-bar) | % | Adverse = slow down |
        | Momentum (20-bar) | % | Trend context |
        | Realized vol | % | High vol -> smaller slices |
        | Volume profile % | % | Expected volume this period |
        | Participation rate | % | Am I too visible? |
        | Price vs arrival | bps | Current mark-to-market |
        | Time of day | 0-1 | Open/close dynamics |

        ### Action Space (2 continuous)

        | Action | Range | Meaning |
        |--------|-------|---------|
        | Execution rate | 0-30% | Fraction of remaining to trade now |
        | Price aggression | -1 to +1 | -1=passive limit, 0=mid, +1=cross spread |

        ### Reward Design

        | Component | Formula | Purpose |
        |-----------|---------|---------|
        | Per-step | -abs(slippage_bps) x (shares/total) | Penalize every bps of slippage |
        | Completion bonus | +2.0 | Reward finishing the order |
        | Incomplete penalty | -5.0 - MOC slippage | Harshly penalize not finishing |
        | Beat VWAP bonus | +0.5 x excess_bps | Reward outperforming VWAP |
        | Miss VWAP penalty | -3.0 (if >5bps worse) | Penalize badly missing VWAP |

        ---

        ### References

        - Almgren & Chriss (2001). "Optimal Execution of Portfolio Transactions."
        - Nevmyvaka, Feng & Kearns (2006). "RL for Optimized Trade Execution."
        - Ning, Lin & Jaimungal (2021). "Double Deep Q-Learning for Optimal Execution."
        """)

    render_footer()
