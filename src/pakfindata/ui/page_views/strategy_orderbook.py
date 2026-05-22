"""Order Book Simulation & RL Agent -- Streamlit page."""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import streamlit.components.v1 as components

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3",
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


# ---------------------------------------------------------------------------
# Tab 1: Book Visualization
# ---------------------------------------------------------------------------

def _render_book_tab():
    st.subheader("Reconstructed Order Book")
    st.caption("Approximate depth from Level 1 (top-of-book) data — auto-saved to Parquet")

    from pakfindata.engine.orderbook_sim import (
        reconstruct_book_history, analyze_book_quality, TICK_SIZE,
        save_book_snapshots, load_book_snapshots, list_book_snapshots,
    )
    from pathlib import Path

    # Symbol & date selectors
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        symbol = st.text_input("Symbol", "OGDC", key="ob_sym").strip().upper()
    with c2:
        from pakfindata.ui.api import client as api_client
        date_options = (api_client.get_tick_logs_dates(symbol) or [])[:30]
        date_str = st.selectbox("Date", date_options if date_options else ["N/A"], key="ob_date")
    with c3:
        max_ticks = st.number_input("Max ticks", 1000, 50000, 10000, 1000, key="ob_maxt")

    if date_str == "N/A":
        st.warning("No tick data found for this symbol.")
        return

    # Check for cached snapshot
    cached = load_book_snapshots(symbol, date_str)
    if cached is not None:
        st.info(f"Cached snapshot found ({len(cached):,} rows). Use 'Reconstruct' to refresh.")

    run = st.button("Reconstruct Book", type="primary", key="ob_run")
    if not run:
        if cached is None:
            st.info("Select a symbol and date, then click Reconstruct Book.")
        return

    with st.spinner("Reconstructing order book..."):
        books = reconstruct_book_history(symbol, date_str, max_ticks=max_ticks)

    if not books:
        st.error("No data returned.")
        return

    # Auto-save snapshot to Parquet
    save_path = save_book_snapshots(books, symbol, date_str)
    st.success(f"Saved {len(books):,} snapshots to `{save_path}`")

    # KPIs
    quality = analyze_book_quality(symbol, date_str)
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        _kpi("Ticks", f"{quality.get('ticks', 0):,}")
    with k2:
        _kpi("Avg Spread", f"{quality.get('avg_spread_ticks', 0):.1f} ticks")
    with k3:
        acc = quality.get("imbalance_predictive_accuracy", 0)
        _kpi("Imbalance Accuracy", f"{acc:.1%}", _C["up"] if acc > 0.55 else _C["amber"])
    with k4:
        _kpi("Avg Bid Depth", f"{quality.get('avg_bid_depth', 0):.1f} lvls")
    with k5:
        _kpi("Avg Ask Depth", f"{quality.get('avg_ask_depth', 0):.1f} lvls")

    # Latest book snapshot — bar chart
    last = books[-1]
    st.markdown("---")
    st.markdown(f"**Latest snapshot** — Mid: {last.mid_price:.2f} | Spread: {last.spread:.2f} | Imbalance: {last.imbalance:+.3f}")

    bid_prices = [f"{b.price:.2f}" for b in last.bids[:10]]
    bid_vols = [b.volume for b in last.bids[:10]]
    ask_prices = [f"{a.price:.2f}" for a in last.asks[:10]]
    ask_vols = [a.volume for a in last.asks[:10]]

    fig = make_subplots(rows=1, cols=2, subplot_titles=("Bids", "Asks"),
                        shared_yaxes=False)
    fig.add_trace(go.Bar(x=bid_vols, y=bid_prices, orientation="h",
                         marker_color="#22c55e", name="Bids"), row=1, col=1)
    fig.add_trace(go.Bar(x=ask_vols, y=ask_prices, orientation="h",
                         marker_color="#ef4444", name="Asks"), row=1, col=2)
    fig.update_layout(**PLOT_LAYOUT, height=400, showlegend=False,
                      title_text=f"{symbol} Reconstructed Book")
    fig.update_xaxes(title_text="Volume", row=1, col=1, autorange="reversed")
    fig.update_xaxes(title_text="Volume", row=1, col=2)
    st.plotly_chart(fig, width='stretch')

    # Imbalance over time
    step = max(1, len(books) // 2000)
    sampled = books[::step]
    imb_df = pd.DataFrame({
        "tick": range(len(sampled)),
        "imbalance": [b.imbalance for b in sampled],
        "spread": [b.spread / TICK_SIZE for b in sampled],
        "mid_price": [b.mid_price for b in sampled],
    })

    col1, col2 = st.columns(2)
    with col1:
        fig_imb = go.Figure()
        fig_imb.add_trace(go.Scatter(x=imb_df["tick"], y=imb_df["imbalance"],
                                     mode="lines", line=dict(color=_C["cyan"], width=1),
                                     name="Imbalance"))
        fig_imb.add_hline(y=0, line_dash="dash", line_color=_C["dim"])
        fig_imb.update_layout(**PLOT_LAYOUT, height=300, title_text="Book Imbalance")
        st.plotly_chart(fig_imb, width='stretch')

    with col2:
        fig_spread = go.Figure()
        fig_spread.add_trace(go.Scatter(x=imb_df["tick"], y=imb_df["spread"],
                                        mode="lines", line=dict(color=_C["amber"], width=1),
                                        name="Spread (ticks)"))
        fig_spread.update_layout(**PLOT_LAYOUT, height=300, title_text="Spread (ticks)")
        st.plotly_chart(fig_spread, width='stretch')

    # Book heatmap
    st.markdown("#### Book Depth Heatmap")
    n_samples = min(500, len(books))
    step_h = max(1, len(books) // n_samples)
    heatmap_books = books[::step_h][:n_samples]

    all_bid_prices = set()
    all_ask_prices = set()
    for b in heatmap_books:
        for lvl in b.bids[:5]:
            all_bid_prices.add(round(lvl.price, 2))
        for lvl in b.asks[:5]:
            all_ask_prices.add(round(lvl.price, 2))

    all_prices = sorted(all_bid_prices | all_ask_prices)
    if len(all_prices) > 50:
        mid_price = heatmap_books[len(heatmap_books)//2].mid_price
        all_prices = [p for p in all_prices if abs(p - mid_price) < mid_price * 0.02]
        all_prices = sorted(all_prices)

    if all_prices:
        heat = np.zeros((len(all_prices), len(heatmap_books)))
        price_idx = {p: i for i, p in enumerate(all_prices)}
        for j, b in enumerate(heatmap_books):
            for lvl in b.bids[:5]:
                rp = round(lvl.price, 2)
                if rp in price_idx:
                    heat[price_idx[rp], j] = lvl.volume
            for lvl in b.asks[:5]:
                rp = round(lvl.price, 2)
                if rp in price_idx:
                    heat[price_idx[rp], j] = -lvl.volume  # negative for asks

        fig_heat = go.Figure(data=go.Heatmap(
            z=heat, x=list(range(len(heatmap_books))),
            y=[f"{p:.2f}" for p in all_prices],
            colorscale=[[0, "#ef4444"], [0.5, "#1a1f2e"], [1, "#22c55e"]],
            zmid=0,
        ))
        fig_heat.update_layout(**PLOT_LAYOUT, height=700, title_text="Bid (green) / Ask (red) Volume",
                               xaxis_title="Tick", yaxis_title="Price")
        st.plotly_chart(fig_heat, width='stretch')


# ---------------------------------------------------------------------------
# Tab 2: Market Simulation
# ---------------------------------------------------------------------------

def _render_simulation_tab():
    st.subheader("Agent-Based Market Simulation")
    st.caption("Replay historical ticks with simulated agents — results auto-saved to Parquet")

    from pakfindata.engine.orderbook_sim import (
        PSXMarketSimulator, NoiseTrader, MomentumTrader, MarketMaker,
        save_simulation_results,
    )

    c1, c2 = st.columns(2)
    with c1:
        symbol = st.text_input("Symbol", "OGDC", key="sim_sym").strip().upper()
    with c2:
        max_ticks = st.number_input("Ticks to simulate", 1000, 50000, 5000, 1000, key="sim_ticks")

    st.markdown("**Agent Configuration**")
    a1, a2, a3 = st.columns(3)
    with a1:
        n_noise = st.slider("Noise traders", 1, 20, 5, key="sim_noise")
        noise_prob = st.slider("Trade probability", 0.01, 0.20, 0.05, 0.01, key="sim_nprob")
    with a2:
        n_momentum = st.slider("Momentum traders", 0, 5, 1, key="sim_mom")
        mom_threshold = st.slider("Momentum threshold %", 0.5, 5.0, 1.0, 0.5, key="sim_momth")
    with a3:
        use_mm = st.checkbox("Market Maker", value=True, key="sim_mm")
        mm_spread = st.slider("MM spread (ticks)", 1, 5, 2, key="sim_mmspd") if use_mm else 2

    run = st.button("Run Simulation", type="primary", key="sim_run")
    if not run:
        st.info("Configure agents and click Run Simulation.")
        return

    with st.spinner("Running simulation..."):
        try:
            sim = PSXMarketSimulator(symbol)
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

        for i in range(n_noise):
            sim.add_agent(NoiseTrader(f"noise_{i+1}", trade_prob=noise_prob))
        for i in range(n_momentum):
            sim.add_agent(MomentumTrader(f"momentum_{i+1}", threshold=mom_threshold/100))
        if use_mm:
            sim.add_agent(MarketMaker("mm_1", spread_ticks=mm_spread))

        result = sim.run(max_ticks=max_ticks)

    # Auto-save simulation results
    # Build book summary (sampled for size)
    step_s = max(1, len(sim.book_history) // 5000)
    book_summary = [
        {"tick": i * step_s, "mid_price": b.mid_price, "spread": b.spread,
         "imbalance": b.imbalance, "last_price": b.last_price}
        for i, b in enumerate(sim.book_history[::step_s])
    ]

    # Derive date from ticks
    from pakfindata.engine.orderbook_sim import PKT
    from datetime import datetime
    if not sim.ticks.empty and "timestamp" in sim.ticks.columns:
        ts0 = sim.ticks.iloc[0]["timestamp"]
        date_str = datetime.fromtimestamp(ts0, tz=PKT).strftime("%Y-%m-%d")
    else:
        date_str = "unknown"

    save_path = save_simulation_results(symbol, date_str, result,
                                        sim.trade_log, book_summary)
    st.success(f"Saved simulation to `{save_path.parent}`")

    # Results
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _kpi("Ticks", f"{result['ticks_processed']:,}")
    with k2:
        _kpi("Total Trades", f"{result['trades']:,}")
    with k3:
        _kpi("Book Snapshots", f"{result['book_snapshots']:,}")
    with k4:
        _kpi("Agents", f"{len(sim.agents)}")

    # Agent stats
    st.markdown("**Agent Trade Counts**")
    agent_df = pd.DataFrame([
        {"Agent": k, "Trades": v} for k, v in result["agent_stats"].items()
    ])
    if not agent_df.empty:
        st.dataframe(agent_df, width='stretch', hide_index=True)

    # Price path
    if sim.book_history:
        step = max(1, len(sim.book_history) // 2000)
        prices = [b.last_price for b in sim.book_history[::step]]
        mids = [b.mid_price for b in sim.book_history[::step]]

        fig = go.Figure()
        fig.add_trace(go.Scatter(y=prices, mode="lines", name="Last Price",
                                 line=dict(color=_C["accent"], width=1)))
        fig.add_trace(go.Scatter(y=mids, mode="lines", name="Mid Price",
                                 line=dict(color=_C["cyan"], width=1, dash="dot")))

        # Mark trades
        if sim.trade_log:
            buy_ticks = [t["tick"]//step for t in sim.trade_log if t["side"] == "BUY"]
            buy_prices = [t["price"] for t in sim.trade_log if t["side"] == "BUY"]
            sell_ticks = [t["tick"]//step for t in sim.trade_log if t["side"] == "SELL"]
            sell_prices = [t["price"] for t in sim.trade_log if t["side"] == "SELL"]

            if buy_ticks:
                fig.add_trace(go.Scatter(x=buy_ticks[:200], y=buy_prices[:200],
                                         mode="markers", name="Buy",
                                         marker=dict(color=_C["up"], size=4, symbol="triangle-up")))
            if sell_ticks:
                fig.add_trace(go.Scatter(x=sell_ticks[:200], y=sell_prices[:200],
                                         mode="markers", name="Sell",
                                         marker=dict(color=_C["down"], size=4, symbol="triangle-down")))

        fig.update_layout(**PLOT_LAYOUT, height=400, title_text=f"{symbol} Price + Agent Trades")
        st.plotly_chart(fig, width='stretch')

    # Trade log
    if sim.trade_log:
        with st.expander("Trade Log (first 100)"):
            st.dataframe(pd.DataFrame(sim.trade_log[:100]), width='stretch', hide_index=True)


# ---------------------------------------------------------------------------
# Tab 3: RL Agent
# ---------------------------------------------------------------------------

def _render_rl_tab():
    st.subheader("RL Limit Order Agent")
    st.caption("Train a PPO/DQN agent — training history auto-saved to Parquet for reporting")

    c1, c2, c3 = st.columns(3)
    with c1:
        symbol = st.text_input("Symbol", "OGDC", key="rl_sym").strip().upper()
    with c2:
        algorithm = st.selectbox("Algorithm", ["PPO", "DQN"], key="rl_algo")
    with c3:
        timesteps = st.select_slider("Timesteps", [5000, 10000, 50000, 100000, 500000],
                                     value=10000, key="rl_steps")

    # Test env first
    test_env = st.button("Test Environment", key="rl_test_env")
    if test_env:
        with st.spinner("Creating environment..."):
            try:
                from pakfindata.engine.orderbook_sim import create_rl_environment
                env = create_rl_environment(symbol)
                if env is None:
                    st.error("Failed (gymnasium not installed?)")
                    return

                obs, _ = env.reset()
                st.success(f"Env OK — obs shape: {obs.shape}, actions: {env.action_space.n}")

                # Random baseline
                total_reward = 0
                done = False
                steps = 0
                while not done and steps < 1000:
                    action = env.action_space.sample()
                    obs, reward, done, _, info = env.step(action)
                    total_reward += reward
                    steps += 1

                k1, k2, k3 = st.columns(3)
                with k1:
                    _kpi("Random Steps", f"{steps}")
                with k2:
                    _kpi("Random Reward", f"{total_reward:.4f}")
                with k3:
                    _kpi("Random Trades", f"{info.get('trades', 0)}")

            except Exception as e:
                st.error(f"Error: {e}")
                return

    st.markdown("---")
    train = st.button("Train Agent", type="primary", key="rl_train")
    if not train:
        return

    with st.spinner(f"Training {algorithm} for {timesteps:,} timesteps... This may take a while."):
        try:
            from pakfindata.engine.orderbook_sim import train_rl_agent
            result = train_rl_agent(symbol, total_timesteps=timesteps, algorithm=algorithm)
        except Exception as e:
            st.error(f"Training failed: {e}")
            return

    if "error" in result:
        st.error(result["error"])
        return

    st.success(f"Training complete! History saved to `/mnt/e/psxdata/simulation/rl_history/`")

    # Results
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _kpi("Total Reward", f"{result['total_reward']:.4f}")
    with k2:
        _kpi("Trades", f"{result['trades']}")
    with k3:
        _kpi("Limit Fills", f"{result['limit_order_fills']}", _C["up"])
    with k4:
        _kpi("Market Orders", f"{result['market_orders']}", _C["amber"])

    k5, k6 = st.columns(2)
    with k5:
        _kpi("Final Position", f"{result['final_position']}")
    with k6:
        _kpi("Model Path", result["model_path"])

    # Action distribution
    if result["trades"] > 0:
        fig = go.Figure(data=[go.Pie(
            labels=["Limit Orders", "Market Orders"],
            values=[result["limit_order_fills"], result["market_orders"]],
            marker=dict(colors=[_C["up"], _C["amber"]]),
            hole=0.4,
        )])
        fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Order Type Distribution")
        st.plotly_chart(fig, width='stretch')


# ---------------------------------------------------------------------------
# Tab 4: History & Reporting
# ---------------------------------------------------------------------------

def _render_history_tab():
    st.subheader("Saved Results & Reporting")
    st.caption("Browse all saved simulations, book snapshots, and RL training runs")

    from pakfindata.engine.orderbook_sim import (
        list_book_snapshots, list_simulation_runs, list_rl_runs,
        load_simulation_trades, load_rl_eval_trades, load_rl_training_log,
        load_book_snapshots,
    )

    hist_sub = st.radio("Category", ["Book Snapshots", "Simulations", "RL Training"],
                        horizontal=True, key="hist_cat")

    if hist_sub == "Book Snapshots":
        snaps = list_book_snapshots()
        if not snaps:
            st.info("No saved book snapshots yet. Run a reconstruction first.")
            return

        st.markdown(f"**{len(snaps)} saved snapshot(s)**")
        snap_df = pd.DataFrame(snaps)
        st.dataframe(snap_df, width='stretch', hide_index=True)

        # Load and preview one
        labels = [f"{s['symbol']} / {s['date']} ({s['size_mb']} MB)" for s in snaps]
        sel = st.selectbox("Preview snapshot", labels, key="hist_snap_sel")
        if sel:
            idx = labels.index(sel)
            s = snaps[idx]
            df = load_book_snapshots(s["symbol"], s["date"])
            if df is not None:
                st.markdown(f"**{len(df):,} rows** | Columns: {', '.join(df.columns[:10])}...")
                st.dataframe(df.head(50), width='stretch', hide_index=True)

                # Quick chart from cached data
                if "mid_price" in df.columns and "imbalance" in df.columns:
                    col1, col2 = st.columns(2)
                    with col1:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(y=df["mid_price"], mode="lines",
                                                 line=dict(color=_C["accent"], width=1)))
                        fig.update_layout(**PLOT_LAYOUT, height=250, title_text="Mid Price")
                        st.plotly_chart(fig, width='stretch')
                    with col2:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(y=df["imbalance"], mode="lines",
                                                 line=dict(color=_C["cyan"], width=1)))
                        fig.add_hline(y=0, line_dash="dash", line_color=_C["dim"])
                        fig.update_layout(**PLOT_LAYOUT, height=250, title_text="Imbalance")
                        st.plotly_chart(fig, width='stretch')

    elif hist_sub == "Simulations":
        runs = list_simulation_runs()
        if not runs:
            st.info("No saved simulation runs yet. Run a simulation first.")
            return

        st.markdown(f"**{len(runs)} simulation run(s)**")
        display_cols = ["symbol", "date", "saved_at", "ticks_processed", "trades"]
        run_df = pd.DataFrame(runs)
        show_cols = [c for c in display_cols if c in run_df.columns]
        st.dataframe(run_df[show_cols] if show_cols else run_df, width='stretch', hide_index=True)

        # Load trade log
        labels = [f"{r.get('symbol','?')} / {r.get('date','?')} @ {r.get('saved_at','?')}" for r in runs]
        sel = st.selectbox("View trade log", labels, key="hist_sim_sel")
        if sel and st.button("Load Trades", key="hist_sim_load"):
            idx = labels.index(sel)
            prefix = runs[idx]["prefix"]
            trades = load_simulation_trades(prefix)
            if trades is not None:
                st.markdown(f"**{len(trades):,} trades**")
                st.dataframe(trades.head(200), width='stretch', hide_index=True)

                # Buy/sell distribution
                if "side" in trades.columns:
                    counts = trades["side"].value_counts()
                    fig = go.Figure(data=[go.Pie(
                        labels=counts.index.tolist(), values=counts.values.tolist(),
                        marker=dict(colors=[_C["up"] if s == "BUY" else _C["down"] for s in counts.index]),
                        hole=0.4,
                    )])
                    fig.update_layout(**PLOT_LAYOUT, height=250, title_text="Buy/Sell Distribution")
                    st.plotly_chart(fig, width='stretch')
            else:
                st.warning("Trade log file not found.")

    elif hist_sub == "RL Training":
        runs = list_rl_runs()
        if not runs:
            st.info("No saved RL training runs yet. Train an agent first.")
            return

        st.markdown(f"**{len(runs)} RL training run(s)**")
        run_df = pd.DataFrame(runs)
        display_cols = ["symbol", "algorithm", "total_timesteps", "total_reward",
                        "trades", "limit_order_fills", "market_orders", "saved_at"]
        show_cols = [c for c in display_cols if c in run_df.columns]
        st.dataframe(run_df[show_cols] if show_cols else run_df, width='stretch', hide_index=True)

        # Compare runs
        if len(runs) > 1:
            st.markdown("#### Performance Comparison")
            rewards = [r.get("total_reward", 0) for r in runs]
            labels_r = [f"{r.get('algorithm','?')} {r.get('total_timesteps','?')}ts\n{r.get('saved_at','')}" for r in runs]

            fig = go.Figure(data=[go.Bar(
                x=labels_r, y=rewards,
                marker_color=[_C["up"] if r > 0 else _C["down"] for r in rewards],
            )])
            fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Total Reward by Run")
            st.plotly_chart(fig, width='stretch')

        # Drill into a run
        labels = [f"{r.get('symbol','?')} {r.get('algorithm','?')} {r.get('total_timesteps','?')}ts @ {r.get('saved_at','?')}" for r in runs]
        sel = st.selectbox("Inspect run", labels, key="hist_rl_sel")
        if sel:
            idx = labels.index(sel)
            prefix = runs[idx]["prefix"]

            col1, col2 = st.columns(2)

            # Eval trades
            with col1:
                if runs[idx].get("has_eval_trades"):
                    if st.button("Load Eval Trades", key="hist_rl_trades"):
                        trades = load_rl_eval_trades(prefix)
                        if trades is not None:
                            st.markdown(f"**{len(trades):,} eval trades**")
                            st.dataframe(trades, width='stretch', hide_index=True)

                            # Limit vs Market
                            if "type" in trades.columns:
                                counts = trades["type"].value_counts()
                                fig = go.Figure(data=[go.Pie(
                                    labels=counts.index.tolist(), values=counts.values.tolist(),
                                    marker=dict(colors=[_C["up"], _C["amber"]]),
                                    hole=0.4,
                                )])
                                fig.update_layout(**PLOT_LAYOUT, height=250,
                                                  title_text="Limit vs Market Orders")
                                st.plotly_chart(fig, width='stretch')

            # Training log
            with col2:
                if runs[idx].get("has_training_log"):
                    if st.button("Load Training Log", key="hist_rl_train"):
                        tlog = load_rl_training_log(prefix)
                        if tlog is not None:
                            st.markdown(f"**{len(tlog):,} iterations logged**")

                            # Plot training curves
                            if "timesteps" in tlog.columns:
                                numeric_cols = [c for c in tlog.columns
                                                if c not in ("iteration", "timesteps")
                                                and pd.api.types.is_numeric_dtype(tlog[c])]
                                for col_name in numeric_cols[:6]:
                                    fig = go.Figure()
                                    fig.add_trace(go.Scatter(
                                        x=tlog["timesteps"], y=tlog[col_name],
                                        mode="lines", line=dict(width=1),
                                        name=col_name,
                                    ))
                                    fig.update_layout(**PLOT_LAYOUT, height=200,
                                                      title_text=col_name)
                                    st.plotly_chart(fig, width='stretch')

                            st.dataframe(tlog, width='stretch', hide_index=True)


# ---------------------------------------------------------------------------
# Tab 5: Research
# ---------------------------------------------------------------------------

def _render_research_tab():
    st.subheader("Level 1 Sufficiency Analysis")
    st.caption("How much can we learn about order book shape from top-of-book data alone?")

    st.markdown("""
    **PSX Order Book Characteristics:**
    - **Tick size:** Rs 0.01 for most stocks
    - **Typical spread:** 1-5 ticks (very thin)
    - **Depth:** 5-10 meaningful price levels
    - **No HFT:** Book changes every 1-5 seconds
    - **Level 1 captures 60-80%** of information on PSX (thin book)

    **Reconstruction Method:**
    1. Track historical bid/ask levels over time
    2. When best bid/ask changes, old level becomes Level 2
    3. Volume at non-best levels decays (cancellation proxy, rate=0.95/tick)
    4. Infer depth using power law: volume at level k = best_vol x 0.5^k

    **Key Metrics:**
    - **Imbalance predictive accuracy >55%** = reconstruction has value
    - **Spread in ticks** = market thickness indicator
    - **Bid/Ask depth levels** = how many levels we can meaningfully reconstruct
    """)

    from pakfindata.engine.orderbook_sim import analyze_book_quality

    symbol = st.text_input("Analyze symbol", "OGDC", key="res_sym").strip().upper()
    if st.button("Run Analysis", key="res_run"):
        with st.spinner("Analyzing..."):
            quality = analyze_book_quality(symbol)

        if "error" in quality:
            st.error(quality["error"])
            return

        st.markdown("#### Results")
        for k, v in quality.items():
            if isinstance(v, float):
                st.markdown(f"- **{k}:** {v:.4f}")
            else:
                st.markdown(f"- **{k}:** {v}")

        acc = quality.get("imbalance_predictive_accuracy", 0)
        if acc > 0.55:
            st.success(f"Imbalance accuracy {acc:.1%} > 55% — the reconstructed book has predictive value!")
        elif acc > 0.50:
            st.warning(f"Imbalance accuracy {acc:.1%} — marginal. May need calibration.")
        else:
            st.error(f"Imbalance accuracy {acc:.1%} — below random. Check data quality.")

    st.markdown("---")
    st.markdown("""
    **Storage Layout** (`/mnt/e/psxdata/simulation/`):
    ```
    simulation/
      book_snapshots/     # {SYMBOL}_{DATE}.parquet
      sim_results/        # {SYMBOL}_{DATE}_{TS}_trades/summary/meta.parquet
      rl_history/         # {SYMBOL}_{ALGO}_{TS}_result/trades/training.parquet
    ```

    **Limitations:**
    - Reconstructed book is APPROXIMATE — not ground truth
    - Power law assumption may not hold for all stocks
    - Decay rate (0.95) needs calibration per stock
    - No Level 2 data to validate against

    **Future Upgrades:**
    - Cross-stock book inference (correlated names)
    - Integration with VPIN/OFI strategies
    - Multi-agent RL (competing agents)
    - If PSX ever provides Level 2, swap reconstruction with real depth
    """)


# ---------------------------------------------------------------------------
# Tab: Depth Heatmap
# ---------------------------------------------------------------------------

def _build_depth_heatmap_html(data: dict) -> str:
    """Build HTML Canvas depth heatmap from DepthHeatmapData dict."""
    bid_matrix = json.dumps(data["bid_matrix"])
    ask_matrix = json.dumps(data["ask_matrix"])
    trade_matrix = json.dumps(data["trade_matrix"])
    price_levels = json.dumps(data["price_levels"])
    time_bins = json.dumps(data["time_bins"])
    last_price = data["last_price"]
    symbol = data["symbol"]
    date = data["date"]
    stats = data["stats"]

    return f"""<!DOCTYPE html>
<html><head><style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0B0E11;font-family:'JetBrains Mono',monospace;color:#c9d1d9}}
.hdr{{display:flex;align-items:center;gap:12px;padding:6px 8px;font-size:12px;color:#8b949e}}
.hdr b{{color:#c9d1d9;font-size:14px}}
.stat{{background:rgba(128,128,128,0.08);border-radius:4px;padding:3px 8px}}
.ctrls{{display:flex;gap:10px;align-items:center;padding:4px 8px;font-size:11px;color:#8b949e}}
.ctrls select,.ctrls input{{background:#161b22;border:1px solid #30363d;color:#c9d1d9;
  padding:3px 6px;border-radius:4px;font-size:11px;font-family:inherit}}
#heatmap{{width:100%;border:1px solid rgba(200,169,110,0.15);border-radius:6px;cursor:crosshair;background:#0B0E11}}
.tip{{position:absolute;padding:6px 10px;background:#161b22;color:#c9d1d9;
  border:1px solid #C8A96E;border-radius:4px;font-size:10px;pointer-events:none;
  opacity:0;font-family:monospace;z-index:10;white-space:nowrap;box-shadow:0 4px 12px rgba(0,0,0,0.5)}}
.legend{{display:flex;gap:16px;padding:4px 8px;font-size:10px;color:#8b949e}}
.lg{{display:flex;align-items:center;gap:4px}}
.lg-bar{{width:60px;height:8px;border-radius:2px}}
.levels{{display:flex;gap:6px;flex-wrap:wrap;padding:4px 8px;font-size:10px}}
.lvl{{padding:2px 6px;border-radius:3px;font-family:monospace}}
</style></head><body>
<div class="hdr">
  <b>{symbol}</b><span>{date}</span>
  <span class="stat">Ticks: {stats['total_ticks']:,}</span>
  <span class="stat">Range: {stats['price_range']}</span>
  <span class="stat">{'L1 + Bid/Ask' if stats['has_bid_ask'] else 'L1 (no bid/ask)'}</span>
  <span style="flex:1"></span>
  <span class="stat">Last: {last_price:.2f}</span>
</div>
<div class="ctrls">
  <span>Layer:</span>
  <select id="layer">
    <option value="all" selected>Bid + Ask + Trades</option>
    <option value="bid">Bid only</option>
    <option value="ask">Ask only</option>
    <option value="trade">Trades only</option>
  </select>
  <span style="margin-left:8px">Intensity:</span>
  <input type="range" min="0.5" max="3" value="1" step="0.1" id="intensity" style="width:80px">
  <span id="int-val">1.0x</span>
</div>
<div class="legend">
  <div class="lg"><div class="lg-bar" style="background:linear-gradient(90deg,#0a1a0a,#00E676)"></div><span>Bid (buyers)</span></div>
  <div class="lg"><div class="lg-bar" style="background:linear-gradient(90deg,#1a0a0a,#FF5252)"></div><span>Ask (sellers)</span></div>
  <div class="lg"><div class="lg-bar" style="background:linear-gradient(90deg,#0a0a1a,#448AFF)"></div><span>Trades</span></div>
  <div class="lg" style="margin-left:12px"><span style="color:#FFB300">-- Last price</span></div>
</div>
<div style="position:relative">
  <canvas id="heatmap" height="400"></canvas>
  <div class="tip" id="tip"></div>
</div>
<div class="levels" id="levels"></div>
<script>
const bidM={bid_matrix};
const askM={ask_matrix};
const tradeM={trade_matrix};
const prices={price_levels};
const times={time_bins};
const lastP={last_price};
const nP=prices.length, nT=times.length;
const canvas=document.getElementById('heatmap');
const ctx=canvas.getContext('2d');
const tip=document.getElementById('tip');
let iMul=1.0, layer='all';

function resize(){{canvas.width=canvas.clientWidth||canvas.parentElement.clientWidth||800;canvas.height=400}}
resize();
// Redraw after layout settles (iframe may have 0 width initially)
setTimeout(()=>{{resize();draw()}},100);
const M={{l:60,r:20,t:10,b:30}};

function draw(){{
  const w=canvas.width,h=canvas.height;
  const cw=(w-M.l-M.r)/nT, ch=(h-M.t-M.b)/nP;
  ctx.clearRect(0,0,w,h);
  for(let pi=0;pi<nP;pi++){{
    const y=M.t+(nP-1-pi)*ch;
    for(let ti=0;ti<nT;ti++){{
      const x=M.l+ti*cw;
      let r=0,g=0,b=0;
      if(layer==='all'||layer==='bid'){{const v=Math.min(100,(bidM[pi]?.[ti]||0)*iMul);g+=v*2.3}}
      if(layer==='all'||layer==='ask'){{const v=Math.min(100,(askM[pi]?.[ti]||0)*iMul);r+=v*2.5}}
      if(layer==='all'||layer==='trade'){{const v=Math.min(100,(tradeM[pi]?.[ti]||0)*iMul);b+=v*2.5;r+=v*0.5}}
      if(r>0||g>0||b>0){{
        ctx.fillStyle=`rgb(${{Math.min(255,Math.round(r))}},${{Math.min(255,Math.round(g))}},${{Math.min(255,Math.round(b))}})`;
        ctx.fillRect(x,y,Math.ceil(cw),Math.ceil(ch));
      }}
    }}
  }}
  // Last price line
  const lIdx=nP-1-Math.round((lastP-prices[0])/(prices[1]-prices[0]));
  if(lIdx>=0&&lIdx<nP){{
    const ly=M.t+lIdx*ch;
    ctx.strokeStyle='#FFB300';ctx.lineWidth=1.5;ctx.setLineDash([4,3]);
    ctx.beginPath();ctx.moveTo(M.l,ly);ctx.lineTo(w-M.r,ly);ctx.stroke();ctx.setLineDash([]);
    ctx.fillStyle='#FFB300';ctx.font='10px monospace';ctx.textAlign='right';
    ctx.fillText(lastP.toFixed(2),M.l-4,ly+3);
  }}
  // Y labels
  ctx.fillStyle='#8b949e';ctx.font='9px monospace';ctx.textAlign='right';
  const ls=Math.max(1,Math.floor(nP/15));
  for(let i=0;i<nP;i+=ls){{ctx.fillText(prices[i].toFixed(2),M.l-4,M.t+(nP-1-i)*ch+ch/2+3)}}
  // X labels
  ctx.textAlign='center';const ts=Math.max(1,Math.floor(nT/12));
  for(let i=0;i<nT;i+=ts){{ctx.fillText(times[i],M.l+i*cw+cw/2,h-M.b+14)}}
  ctx.strokeStyle='rgba(128,128,128,0.2)';ctx.lineWidth=0.5;
  ctx.strokeRect(M.l,M.t,w-M.l-M.r,h-M.t-M.b);
}}
draw();

canvas.addEventListener('mousemove',e=>{{
  const rect=canvas.getBoundingClientRect();
  const mx=e.clientX-rect.left,my=e.clientY-rect.top;
  const w=canvas.width,h=canvas.height;
  const cw=(w-M.l-M.r)/nT, ch=(h-M.t-M.b)/nP;
  const ti=Math.floor((mx-M.l)/cw), pi=nP-1-Math.floor((my-M.t)/ch);
  if(ti>=0&&ti<nT&&pi>=0&&pi<nP){{
    const bv=(bidM[pi]?.[ti]||0).toFixed(1);
    const av=(askM[pi]?.[ti]||0).toFixed(1);
    const tv=(tradeM[pi]?.[ti]||0).toFixed(1);
    tip.innerHTML=`<b>${{prices[pi].toFixed(2)}}</b> @ ${{times[ti]}}<br>`
      +`<span style="color:#00E676">Bid: ${{bv}}</span> `
      +`<span style="color:#FF5252">Ask: ${{av}}</span> `
      +`<span style="color:#448AFF">Trade: ${{tv}}</span>`;
    tip.style.opacity=1;tip.style.left=(mx+12)+'px';tip.style.top=(my-10)+'px';
  }}else{{tip.style.opacity=0}}
}});
canvas.addEventListener('mouseout',()=>tip.style.opacity=0);

document.getElementById('layer').addEventListener('change',e=>{{layer=e.target.value;draw()}});
document.getElementById('intensity').addEventListener('input',e=>{{
  iMul=parseFloat(e.target.value);document.getElementById('int-val').textContent=iMul.toFixed(1)+'x';draw();
}});

const levelsEl=document.getElementById('levels');
const stats={json.dumps(stats)};
if(stats.top_bid_levels){{
  levelsEl.innerHTML='<span style="color:#8b949e">Support:</span> ';
  stats.top_bid_levels.forEach(l=>{{levelsEl.innerHTML+=`<span class="lvl" style="background:rgba(0,230,118,0.15);color:#00E676">${{l.price.toFixed(2)}}</span>`}});
  levelsEl.innerHTML+=' <span style="color:#8b949e;margin-left:8px">Resistance:</span> ';
  (stats.top_ask_levels||[]).forEach(l=>{{levelsEl.innerHTML+=`<span class="lvl" style="background:rgba(255,82,82,0.15);color:#FF5252">${{l.price.toFixed(2)}}</span>`}});
}}
window.addEventListener('resize',()=>{{resize();draw()}});
</script></body></html>"""


def _render_depth_heatmap_tab():
    st.subheader("Synthetic Depth Heatmap")
    st.caption("Reconstructed order book depth from L1 bid/ask tick data. Green = bid support, Red = ask resistance.")

    from pakfindata.engine.depth_heatmap import build_heatmap, get_available_dates

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        symbol = st.text_input("Symbol", "OGDC", key="hm_sym").strip().upper()
    with c2:
        dates = get_available_dates(symbol) if symbol else []
        date = st.selectbox("Date", dates if dates else ["No data"], key="hm_date")
    with c3:
        granularity = st.selectbox("Price step (PKR)", [0.10, 0.25, 0.50, 1.00], index=1, key="hm_gran")

    if not symbol or date == "No data":
        st.warning("No tick data available for this symbol.")
        return

    @st.cache_data(ttl=300, show_spinner="Building depth heatmap...")
    def _build_cached(sym, dt, gran):
        data = build_heatmap(sym, dt, price_granularity=gran)
        if data:
            return {
                "price_levels": data.price_levels,
                "time_bins": data.time_bins,
                "bid_matrix": data.bid_matrix,
                "ask_matrix": data.ask_matrix,
                "trade_matrix": data.trade_matrix,
                "last_price": data.last_price,
                "bid_price": data.bid_price,
                "ask_price": data.ask_price,
                "symbol": data.symbol,
                "date": data.date,
                "stats": data.stats,
            }
        return None

    hm_data = _build_cached(symbol, date, granularity)

    if hm_data:
        html = _build_depth_heatmap_html(hm_data)
        components.html(html, height=520, scrolling=False)

        stats = hm_data["stats"]
        if stats.get("top_bid_levels"):
            st.caption(
                "**Support:** " +
                ", ".join(f"{l['price']:.2f}" for l in stats["top_bid_levels"][:3]) +
                "  |  **Resistance:** " +
                ", ".join(f"{l['price']:.2f}" for l in stats.get("top_ask_levels", [])[:3])
            )
    else:
        st.warning(f"No tick data found for {symbol} on {date}")


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def render_page():
    st.markdown("### Order Book Simulation & RL Agent")
    st.caption("Reconstruct approximate order book from Level 1 data, simulate markets, train RL agents")

    tab_book, tab_depth, tab_sim, tab_rl, tab_hist, tab_research = st.tabs([
        "Book Visualization", "Depth Heatmap", "Market Simulation",
        "RL Agent", "History & Reporting", "Research",
    ])

    with tab_book:
        _render_book_tab()
    with tab_depth:
        _render_depth_heatmap_tab()
    with tab_sim:
        _render_simulation_tab()
    with tab_rl:
        _render_rl_tab()
    with tab_hist:
        _render_history_tab()
    with tab_research:
        _render_research_tab()

    render_footer()
