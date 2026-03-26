"""Stock Graph (GNN) -- PSX stock relationship graph & neural network predictions."""

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
}
DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
    font_color="#c9d1d9", margin=dict(l=20, r=20, t=40, b=20),
)

EDGE_COLORS = {
    "SECTOR": "#6B7280",
    "SUPPLY_CHAIN": "#3B82F6",
    "COMMON_DIRECTORS": "#C8A96E",
    "CORRELATION": "#22C55E",
}


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Tab 1: Graph Explorer
# ---------------------------------------------------------------------------

def _render_graph_explorer():
    st.subheader("PSX Stock Relationship Graph")
    st.caption("564 stocks connected by sector, supply chain, directors, and correlation")

    from pakfindata.engine.gnn_stock_graph import (
        build_stock_graph, graph_to_networkx, graph_statistics,
        detect_communities, PSX_BUSINESS_GROUPS,
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        corr_thresh = st.slider("Correlation threshold", 0.3, 0.9, 0.7, 0.05, key="gnn_corr")
    with c2:
        min_vol = st.number_input("Min daily volume", 10000, 500000, 50000, 10000, key="gnn_vol")
    with c3:
        pass

    run = st.button("Build Graph", type="primary", key="gnn_build")
    if not run:
        st.info("Click Build Graph to construct the PSX stock relationship graph.")
        return

    with st.spinner("Building graph (computing correlations, loading directors)..."):
        nodes, edges = build_stock_graph(
            correlation_threshold=corr_thresh, min_volume=min_vol,
        )

    if not nodes:
        st.error("No data. Check DuckDB connection.")
        return

    stats = graph_statistics(nodes, edges)

    # KPIs
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        _kpi("Nodes", f"{stats['num_nodes']}")
    with k2:
        _kpi("Edges", f"{stats['num_edges']:,}")
    with k3:
        _kpi("Density", f"{stats['density']:.4f}")
    with k4:
        _kpi("Avg Degree", f"{stats['avg_degree']:.1f}")
    with k5:
        _kpi("Components", f"{stats['num_components']}")
    with k6:
        _kpi("Clustering", f"{stats['avg_clustering']:.3f}")

    # Edge type breakdown
    col1, col2 = st.columns(2)
    with col1:
        et = stats["edge_types"]
        fig = go.Figure(data=[go.Pie(
            labels=list(et.keys()), values=list(et.values()),
            marker=dict(colors=[EDGE_COLORS.get(k, "#888") for k in et.keys()]),
            hole=0.4,
        )])
        fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Edge Type Distribution")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        top = stats["most_connected"][:15]
        fig = go.Figure(data=[go.Bar(
            x=[t[0] for t in top], y=[t[1] for t in top],
            marker_color=_C["accent"],
        )])
        fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Most Connected Stocks")
        st.plotly_chart(fig, use_container_width=True)

    # Network visualization
    st.markdown("#### Network Graph")
    import networkx as nx
    G = graph_to_networkx(nodes, edges)
    pos = nx.spring_layout(G, k=0.3, iterations=50, weight="weight", seed=42)

    fig = go.Figure()

    # Edge traces by type
    for edge_type, color in EDGE_COLORS.items():
        edge_x, edge_y = [], []
        for u, v, d in G.edges(data=True):
            if d.get("edge_type") == edge_type:
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])

        if edge_x:
            fig.add_trace(go.Scatter(
                x=edge_x, y=edge_y, mode="lines",
                line=dict(width=0.5, color=color),
                name=edge_type, opacity=0.4,
            ))

    # Node trace
    sectors = list(set(n.sector for n in nodes))
    sector_colors = {s: f"hsl({i * 360 // max(len(sectors), 1)}, 70%, 50%)" for i, s in enumerate(sectors)}

    node_x = [pos[n.symbol][0] for n in nodes if n.symbol in pos]
    node_y = [pos[n.symbol][1] for n in nodes if n.symbol in pos]
    node_text = [n.symbol for n in nodes if n.symbol in pos]
    node_colors = [sector_colors.get(n.sector, "#888") for n in nodes if n.symbol in pos]
    node_sizes = [max(5, G.degree(n.symbol) * 1.5) for n in nodes if n.symbol in pos]

    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        marker=dict(size=node_sizes, color=node_colors, line=dict(width=0.5, color="#333")),
        text=node_text, textposition="top center", textfont=dict(size=6),
        name="Stocks", hovertext=[f"{n.symbol} ({n.sector})" for n in nodes if n.symbol in pos],
    ))

    fig.update_layout(**PLOT_LAYOUT, height=700, showlegend=True,
                      title_text="PSX Stock Relationship Graph",
                      xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                      yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
    st.plotly_chart(fig, use_container_width=True)

    # Community detection
    with st.expander("Community Detection (Louvain)"):
        communities = detect_communities(nodes, edges)
        st.markdown(f"**{communities['num_communities']} communities detected**")
        for c in communities["communities"][:8]:
            syms_preview = ", ".join(c["symbols"][:8])
            if len(c["symbols"]) > 8:
                syms_preview += f"... (+{len(c['symbols'])-8})"
            st.markdown(f"- **Cluster {c['id']}** ({c['size']} stocks) | {c['dominant_sector']} | {syms_preview}")

    # Business groups
    with st.expander("Known Business Groups"):
        for group, syms in PSX_BUSINESS_GROUPS.items():
            in_graph = [s for s in syms if s in G]
            st.markdown(f"- **{group}:** {', '.join(in_graph)} ({len(in_graph)}/{len(syms)} in graph)")


# ---------------------------------------------------------------------------
# Tab 2: GNN Training
# ---------------------------------------------------------------------------

def _render_gnn_training():
    st.subheader("GNN Model Training")
    st.caption("Train GCN/GAT/GraphSAGE on temporal graph snapshots")

    c1, c2, c3 = st.columns(3)
    with c1:
        model_type = st.selectbox("Architecture", ["GAT", "GCN", "GraphSAGE"], key="gnn_arch")
        hidden_dim = st.selectbox("Hidden dim", [32, 64, 128], index=1, key="gnn_hdim")
    with c2:
        task = st.selectbox("Task", ["classification", "regression"], key="gnn_task")
        epochs = st.select_slider("Epochs", [20, 50, 100, 200], value=50, key="gnn_epochs")
    with c3:
        lookback = st.select_slider("Lookback (days)", [120, 250, 500, 750], value=500, key="gnn_lb")
        lr = st.select_slider("Learning rate", [0.0001, 0.0005, 0.001, 0.005], value=0.001, key="gnn_lr")

    train = st.button("Train Model", type="primary", key="gnn_train")
    if not train:
        # Show past runs
        from pakfindata.engine.gnn_stock_graph import list_gnn_runs
        runs = list_gnn_runs()
        if runs:
            st.markdown(f"**{len(runs)} previous run(s) saved**")
            run_df = pd.DataFrame(runs)
            cols = [c for c in ["model_type", "task", "test_accuracy", "epochs",
                                "num_nodes", "num_edges", "final_train_loss"] if c in run_df.columns]
            st.dataframe(run_df[cols] if cols else run_df, use_container_width=True, hide_index=True)
        return

    with st.spinner(f"Training {model_type} ({task}) for {epochs} epochs..."):
        try:
            from pakfindata.engine.gnn_stock_graph import train_gnn
            result = train_gnn(
                model_type=model_type, task=task, epochs=epochs,
                lookback_days=lookback, lr=lr, hidden_dim=hidden_dim,
            )
        except Exception as e:
            st.error(f"Training failed: {e}")
            return

    if "error" in result:
        st.error(result["error"])
        return

    st.success(f"Training complete! Saved to `/mnt/e/psxdata/simulation/gnn_results/`")

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        acc = result["test_accuracy"]
        _kpi("Test Accuracy", f"{acc:.1%}", _C["up"] if acc > 0.52 else _C["down"])
    with k2:
        _kpi("Train Loss", f"{result['final_train_loss']:.4f}")
    with k3:
        _kpi("Nodes", f"{result['num_nodes']}")
    with k4:
        _kpi("Edges", f"{result['num_edges']:,}")

    k5, k6, k7 = st.columns(3)
    with k5:
        _kpi("Train Graphs", f"{result['train_graphs']}")
    with k6:
        _kpi("Val Graphs", f"{result['val_graphs']}")
    with k7:
        _kpi("Test Graphs", f"{result['test_graphs']}")

    # Training curves
    col1, col2 = st.columns(2)
    with col1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(y=result["train_losses"], mode="lines",
                                 line=dict(color=_C["accent"], width=1), name="Train Loss"))
        fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Training Loss")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if result["val_accuracies"]:
            fig = go.Figure()
            fig.add_trace(go.Scatter(y=result["val_accuracies"], mode="lines+markers",
                                     line=dict(color=_C["up"], width=1), name="Val Accuracy"))
            fig.add_hline(y=0.5, line_dash="dash", line_color=_C["dim"],
                          annotation_text="Random baseline")
            fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Validation Accuracy")
            st.plotly_chart(fig, use_container_width=True)

    # Sector accuracy
    if result.get("sector_accuracy"):
        st.markdown("#### Per-Sector Accuracy")
        sa = sorted(result["sector_accuracy"].items(), key=lambda x: -x[1])
        fig = go.Figure(data=[go.Bar(
            x=[s[0][:20] for s in sa[:15]], y=[s[1] for s in sa[:15]],
            marker_color=[_C["up"] if s[1] > 0.5 else _C["down"] for s in sa[:15]],
        )])
        fig.add_hline(y=0.5, line_dash="dash", line_color=_C["dim"])
        fig.update_layout(**PLOT_LAYOUT, height=350, title_text="Accuracy by Sector")
        st.plotly_chart(fig, use_container_width=True)

    # Sample predictions
    if result.get("predictions_sample"):
        with st.expander("Sample Predictions (first 20)"):
            st.dataframe(pd.DataFrame(result["predictions_sample"]),
                         use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Tab 3: Influence Analysis
# ---------------------------------------------------------------------------

def _render_influence():
    st.subheader("Influence & Propagation Analysis")
    st.caption("Find the most influential PSX stocks and trace shock propagation")

    from pakfindata.engine.gnn_stock_graph import (
        build_stock_graph, find_most_influential_stocks, propagation_analysis,
    )

    if st.button("Build Graph & Analyze", type="primary", key="inf_run"):
        with st.spinner("Building graph and computing centrality..."):
            nodes, edges = build_stock_graph()

        if not nodes:
            st.error("No data.")
            return

        st.session_state["gnn_nodes"] = nodes
        st.session_state["gnn_edges"] = edges

        # Most influential
        top = find_most_influential_stocks(nodes, edges, top_n=20)
        st.session_state["gnn_top"] = top

    if "gnn_top" not in st.session_state:
        st.info("Click 'Build Graph & Analyze' first.")
        return

    top = st.session_state["gnn_top"]
    nodes = st.session_state["gnn_nodes"]
    edges = st.session_state["gnn_edges"]

    st.markdown("#### Most Influential Stocks (Composite Centrality)")
    top_df = pd.DataFrame(top)
    display_cols = ["symbol", "sector", "composite", "degree", "betweenness",
                    "pagerank", "eigenvector", "connections"]
    show = [c for c in display_cols if c in top_df.columns]
    st.dataframe(top_df[show], use_container_width=True, hide_index=True)

    # Bar chart
    fig = go.Figure(data=[go.Bar(
        x=[t["symbol"] for t in top[:15]],
        y=[t["composite"] for t in top[:15]],
        marker_color=_C["accent"],
    )])
    fig.update_layout(**PLOT_LAYOUT, height=300, title_text="Composite Centrality Score")
    st.plotly_chart(fig, use_container_width=True)

    # Propagation analysis
    st.markdown("---")
    st.markdown("#### Shock Propagation")
    source = st.text_input("Source symbol", "OGDC", key="inf_source").strip().upper()
    hops = st.slider("Hops", 1, 5, 3, key="inf_hops")

    if st.button("Trace Propagation", key="inf_prop"):
        result = propagation_analysis(nodes, edges, source, hops=hops)

        if "error" in result:
            st.error(result["error"])
            return

        for hop_data in result["hops"]:
            st.markdown(f"**Hop {hop_data['hop']}:** {hop_data['count']} stocks reached")
            if hop_data["stocks"]:
                hop_df = pd.DataFrame(hop_data["stocks"])
                st.dataframe(hop_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Tab 4: Research
# ---------------------------------------------------------------------------

def _render_research():
    st.subheader("Research Notes")

    st.markdown("""
    **Why GNN works on PSX:**
    - PSX is a SMALL market (564 nodes) -- GNNs excel on small, dense graphs
    - Director networks are REAL -- same families sit on multiple boards
    - Supply chains are VISIBLE -- Pakistan's industrial base is concentrated
    - Sector co-movement is STRONG -- 33 sectors, 5-20 stocks each
    - Information propagates SLOWLY -- low analyst coverage means graph edges = info channels

    **4 Edge Types:**
    1. **SECTOR** -- same PSX sector (auto-discovered from data)
    2. **SUPPLY_CHAIN** -- business relationships (manually curated domain knowledge)
    3. **COMMON_DIRECTORS** -- shared board members (from scraped company profiles)
    4. **CORRELATION** -- rolling 60-day price correlation > threshold

    **20-Dimension Node Features:**
    - Price: current, annualized return, volatility, vs SMA20, momentum
    - Volume: ratio, log average
    - Volatility: ratio (5d/20d), range ratio
    - Momentum: 5d, 10d returns
    - Microstructure: avg abs return, max/min daily, up-day ratio
    - Statistical: skewness, kurtosis, Sharpe-like ratio

    **Models:**
    - **GAT** (recommended) -- attention learns WHICH edges matter
    - **GCN** -- baseline graph convolution
    - **GraphSAGE** -- inductive, handles new nodes

    **Storage:** `/mnt/e/psxdata/simulation/gnn_results/`
    - `*_meta.parquet` -- run config & final metrics
    - `*_curves.parquet` -- training loss per epoch
    - `*_predictions.parquet` -- sample test predictions
    - `*_sector_acc.parquet` -- per-sector accuracy

    **Publication angle:**
    *"GNN for Emerging Market Stock Prediction: Evidence from PSX"*
    - Novelty: director network + supply chain edges on thin, info-poor market
    - Ablation: which edge types contribute most?
    - Temporal stability: does graph structure change over time?
    """)


# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def render_page():
    st.markdown("### Stock Graph (GNN)")
    st.caption("Graph Neural Network for PSX stock relationships -- sector, supply chain, directors, correlation")

    tab_graph, tab_train, tab_influence, tab_research = st.tabs([
        "Graph Explorer", "GNN Training", "Influence Analysis", "Research"
    ])

    with tab_graph:
        _render_graph_explorer()
    with tab_train:
        _render_gnn_training()
    with tab_influence:
        _render_influence()
    with tab_research:
        _render_research()

    render_footer()
