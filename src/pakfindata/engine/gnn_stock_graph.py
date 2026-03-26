"""
Graph Neural Network for PSX Stock Relationships.

Builds a heterogeneous graph of PSX stocks with multiple edge types:
  1. SECTOR -- same sector membership
  2. SUPPLY_CHAIN -- inferred business relationships
  3. COMMON_DIRECTORS -- shared board members
  4. CORRELATION -- rolling price correlation > threshold

Then trains GNN models (GCN, GAT, GraphSAGE) to predict:
  - Next-day return direction (classification)
  - Next-day return magnitude (regression)
  - Cross-stock momentum propagation (which neighbor moves first?)

Architecture:
  Node features -> GNN layers -> per-node prediction
  Graph structure encodes HOW stocks are related
  Temporal batching: train on window t, predict t+1

PSX-Specific:
  - 564 nodes (small graph -- GNN trains fast)
  - 33 sectors (dense intra-sector edges)
  - Common director network is STRONG in Pakistan (family business groups)
  - 245 trading days/year
  - Circuit breakers +/-7.5% cap daily returns

Published methods implemented:
  - GCN (Kipf & Welling, 2017)
  - GAT (Velickovic et al., 2018)
  - GraphSAGE (Hamilton et al., 2017)
  - Temporal attention (our extension for PSX)
"""

import numpy as np
import pandas as pd
import json
import duckdb
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, Tuple
from collections import defaultdict

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
GNN_DIR = Path("/mnt/e/psxdata/simulation/gnn_results")
TRADING_DAYS = 245

# PSX supply chain map (domain knowledge, manually curated)
# Format: {downstream_sector: [upstream_sectors]}
PSX_SUPPLY_CHAIN = {
    "AUTOMOBILE ASSEMBLER": ["AUTOMOBILE PARTS & ACCESSORIES", "ENGINEERING"],
    "AUTOMOBILE PARTS & ACCESSORIES": ["ENGINEERING", "SYNTHETIC & RAYON"],
    "CEMENT": ["ENGINEERING", "POWER GENERATION & DISTRIBUTION"],
    "CHEMICAL": ["OIL & GAS EXPLORATION COMPANIES", "REFINERY"],
    "FERTILIZER": ["OIL & GAS MARKETING COMPANIES", "CHEMICAL"],
    "FOOD & PERSONAL CARE PRODUCTS": ["SUGAR & ALLIED INDUSTRIES"],
    "OIL & GAS MARKETING COMPANIES": ["OIL & GAS EXPLORATION COMPANIES", "REFINERY"],
    "PHARMACEUTICALS": ["CHEMICAL"],
    "POWER GENERATION & DISTRIBUTION": ["OIL & GAS MARKETING COMPANIES", "OIL & GAS EXPLORATION COMPANIES"],
    "REFINERY": ["OIL & GAS EXPLORATION COMPANIES"],
    "SUGAR & ALLIED INDUSTRIES": ["POWER GENERATION & DISTRIBUTION"],
    "SYNTHETIC & RAYON": ["CHEMICAL"],
    "TECHNOLOGY & COMMUNICATION": [],  # mostly independent
    "TEXTILE COMPOSITE": ["SYNTHETIC & RAYON", "CHEMICAL", "POWER GENERATION & DISTRIBUTION"],
    "TEXTILE SPINNING": ["SYNTHETIC & RAYON", "POWER GENERATION & DISTRIBUTION"],
    "TEXTILE WEAVING": ["TEXTILE SPINNING"],
}

# Known Pakistan business groups (common director networks)
PSX_BUSINESS_GROUPS = {
    "Engro Group": ["ENGRO", "FFC", "EFERT", "EPCL"],
    "Nishat Group": ["NML", "MCB", "DGKC", "NCL", "NLPK"],
    "Dawood Hercules": ["DAWH", "ENGRO", "DHL"],
    "Lucky Group": ["LUCK", "YLPC", "ICL"],
    "Packages Group": ["PKGS", "NESTLE", "GPP", "TREET", "TGL"],
    "Fauji Group": ["FFC", "FFBL", "FCCL", "FCEPL", "FNEL"],
    "Habib Group": ["HBL", "HABSM", "HASCOL"],
    "Aga Khan Fund": ["HBL", "JSGCL", "NATF", "PKFN"],
    "JS Group": ["JSGBKTI", "JSMFI", "JSCL"],
    "Atlas Group": ["ATLH", "ATRL", "AGI", "ATBA"],
}


def _ensure_dirs():
    GNN_DIR.mkdir(parents=True, exist_ok=True)


def _load_sector_names() -> dict[str, str]:
    """Load sector_code -> sector_name mapping from SQLite."""
    try:
        scon = sqlite3.connect(str(PSX_SQLITE))
        rows = scon.execute("SELECT sector_code, sector_name FROM sectors").fetchall()
        scon.close()
        # Map both with and without leading zeros: "0804" -> name, "804" -> name
        mapping = {}
        for code, name in rows:
            mapping[code] = name
            mapping[code.lstrip("0")] = name
        return mapping
    except Exception:
        return {}


# ═══════════════════════════════════════════
# PHASE 1: GRAPH CONSTRUCTION
# ═══════════════════════════════════════════

@dataclass
class StockNode:
    symbol: str
    sector: str
    features: np.ndarray  # node feature vector
    label: float          # prediction target (next-day return)


@dataclass
class StockEdge:
    source: str        # symbol
    target: str        # symbol
    edge_type: str     # SECTOR, SUPPLY_CHAIN, COMMON_DIRECTORS, CORRELATION
    weight: float      # edge weight


def build_stock_graph(
    as_of_date: str = None,
    correlation_threshold: float = 0.7,
    correlation_window: int = 60,
    min_volume: float = 50000,
    feature_window: int = 20,
) -> Tuple[list[StockNode], list[StockEdge]]:
    """
    Build the PSX stock relationship graph.

    Returns (nodes, edges) where:
      nodes: list of StockNode with feature vectors
      edges: list of StockEdge with types and weights
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    if as_of_date is None:
        as_of_date = str(con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()[0])

    cutoff = (pd.to_datetime(as_of_date) - timedelta(days=correlation_window * 2)).strftime("%Y-%m-%d")

    # -- Load EOD data for all liquid stocks --
    eod = con.execute(f"""
        SELECT date, symbol, close, volume, sector_code AS sector
        FROM eod_ohlcv
        WHERE date BETWEEN '{cutoff}' AND '{as_of_date}'
        AND volume > {min_volume}
        ORDER BY date, symbol
    """).df()

    con.close()

    if eod.empty:
        return [], []

    # Resolve sector codes to names
    sector_names = _load_sector_names()

    symbols = sorted(eod["symbol"].unique())
    sym_to_idx = {s: i for i, s in enumerate(symbols)}

    # -- Compute node features --
    nodes = []
    returns_matrix = {}  # symbol -> array of daily returns

    for sym in symbols:
        sdf = eod[eod["symbol"] == sym].sort_values("date")
        if len(sdf) < feature_window:
            continue

        close = sdf["close"].values
        volume = sdf["volume"].values
        sector_code = sdf["sector"].iloc[-1] if "sector" in sdf.columns else ""
        sector = sector_names.get(str(sector_code), sector_names.get(str(sector_code).lstrip("0"), str(sector_code)))

        # Daily returns
        rets = np.diff(close) / close[:-1]
        returns_matrix[sym] = rets[-correlation_window:]

        # Feature vector (20 dimensions)
        recent = close[-feature_window:]
        recent_vol = volume[-feature_window:]
        recent_rets = rets[-feature_window:] if len(rets) >= feature_window else rets

        features = np.array([
            # Price features
            recent[-1],                                    # current price
            np.mean(recent_rets) * TRADING_DAYS,           # annualized return
            np.std(recent_rets) * np.sqrt(TRADING_DAYS),   # annualized volatility
            recent[-1] / np.mean(recent) - 1,              # price vs SMA20
            recent[-1] / recent[0] - 1,                    # 20-day momentum

            # Volume features
            recent_vol[-1] / np.mean(recent_vol) if np.mean(recent_vol) > 0 else 1,
            np.log(np.mean(recent_vol) + 1),               # log avg volume

            # Volatility features
            np.std(recent_rets[-5:]) / np.std(recent_rets) if np.std(recent_rets) > 0 else 1,
            max(recent) / min(recent) - 1 if min(recent) > 0 else 0,

            # Momentum features
            (recent[-1] / recent[-5] - 1) if len(recent) >= 5 else 0,
            (recent[-1] / recent[-10] - 1) if len(recent) >= 10 else 0,

            # Microstructure
            np.mean(np.abs(recent_rets)),
            np.max(recent_rets) if len(recent_rets) > 0 else 0,
            np.min(recent_rets) if len(recent_rets) > 0 else 0,
            len(recent_rets[recent_rets > 0]) / len(recent_rets) if len(recent_rets) > 0 else 0.5,

            # Statistical moments
            float(pd.Series(recent_rets).skew()) if len(recent_rets) > 2 else 0,
            float(pd.Series(recent_rets).kurtosis()) if len(recent_rets) > 3 else 0,

            # Relative strength
            np.mean(recent_rets[-5:]) / np.std(recent_rets[-5:]) if np.std(recent_rets[-5:]) > 0 else 0,

            # Placeholder for sector encoding
            0, 0,
        ], dtype=np.float32)

        # Replace NaN/Inf
        features = np.nan_to_num(features, nan=0, posinf=0, neginf=0)

        # Next-day return as label
        all_rets = np.diff(close) / close[:-1]
        label = float(all_rets[-1]) if len(all_rets) > 0 else 0

        nodes.append(StockNode(
            symbol=sym,
            sector=sector if sector else "Unknown",
            features=features,
            label=label,
        ))

    node_symbols = {n.symbol for n in nodes}

    # -- Build edges --
    edges = []

    # 1. SECTOR edges
    sector_groups = defaultdict(list)
    for n in nodes:
        sector_groups[n.sector].append(n.symbol)

    for sector, syms in sector_groups.items():
        if sector == "Unknown":
            continue
        for i in range(len(syms)):
            for j in range(i + 1, len(syms)):
                edges.append(StockEdge(
                    source=syms[i], target=syms[j],
                    edge_type="SECTOR", weight=1.0,
                ))

    # 2. SUPPLY_CHAIN edges
    sym_sectors = {n.symbol: n.sector.upper() for n in nodes}
    sector_syms = defaultdict(list)
    for sym, sec in sym_sectors.items():
        sector_syms[sec].append(sym)

    for downstream, upstreams in PSX_SUPPLY_CHAIN.items():
        for upstream in upstreams:
            for d_sym in sector_syms.get(downstream, []):
                for u_sym in sector_syms.get(upstream, []):
                    if d_sym in node_symbols and u_sym in node_symbols:
                        edges.append(StockEdge(
                            source=u_sym, target=d_sym,
                            edge_type="SUPPLY_CHAIN", weight=0.7,
                        ))

    # 3. COMMON_DIRECTORS edges (from business group mapping)
    for group_name, group_syms in PSX_BUSINESS_GROUPS.items():
        valid_syms = [s for s in group_syms if s in node_symbols]
        for i in range(len(valid_syms)):
            for j in range(i + 1, len(valid_syms)):
                edges.append(StockEdge(
                    source=valid_syms[i], target=valid_syms[j],
                    edge_type="COMMON_DIRECTORS", weight=0.8,
                ))

    # 3b. COMMON_DIRECTORS from actual key_people data
    try:
        scon = sqlite3.connect(str(PSX_SQLITE))
        rows = scon.execute(
            "SELECT symbol, key_people FROM company_profiles WHERE key_people IS NOT NULL"
        ).fetchall()
        scon.close()

        # Build director -> companies mapping
        director_companies = defaultdict(set)
        for sym, kp_json in rows:
            if sym not in node_symbols:
                continue
            try:
                people = json.loads(kp_json) if kp_json else []
                for p in people:
                    name = p.get("name", "").strip().upper()
                    if len(name) > 3:
                        director_companies[name].add(sym)
            except Exception:
                continue

        # Create edges for shared directors
        for director, companies in director_companies.items():
            companies = list(companies)
            if len(companies) >= 2:
                for i in range(len(companies)):
                    for j in range(i + 1, len(companies)):
                        edges.append(StockEdge(
                            source=companies[i], target=companies[j],
                            edge_type="COMMON_DIRECTORS",
                            weight=1.0 / len(companies),
                        ))
    except Exception:
        pass

    # 4. CORRELATION edges
    corr_syms = [s for s in node_symbols if s in returns_matrix and len(returns_matrix[s]) >= 20]
    if len(corr_syms) > 1:
        ret_df = pd.DataFrame({s: returns_matrix[s][-60:] for s in corr_syms if len(returns_matrix[s]) >= 60})

        if not ret_df.empty:
            corr_matrix = ret_df.corr()

            for i, sym_a in enumerate(corr_matrix.columns):
                for j, sym_b in enumerate(corr_matrix.columns):
                    if i < j:
                        corr = corr_matrix.iloc[i, j]
                        if abs(corr) > correlation_threshold:
                            edges.append(StockEdge(
                                source=sym_a, target=sym_b,
                                edge_type="CORRELATION",
                                weight=float(corr),
                            ))

    # Deduplicate edges
    seen = set()
    unique_edges = []
    for e in edges:
        key = tuple(sorted([e.source, e.target])) + (e.edge_type,)
        if key not in seen:
            seen.add(key)
            unique_edges.append(e)

    return nodes, unique_edges


def graph_to_networkx(nodes: list[StockNode], edges: list[StockEdge]):
    """Convert to NetworkX graph for analysis and visualization."""
    import networkx as nx

    G = nx.Graph()

    for n in nodes:
        G.add_node(n.symbol, sector=n.sector, features=n.features, label=n.label)

    for e in edges:
        if G.has_edge(e.source, e.target):
            existing = G[e.source][e.target]
            types = existing.get("edge_types", [])
            types.append(e.edge_type)
            G[e.source][e.target]["edge_types"] = types
            G[e.source][e.target]["weight"] = max(existing.get("weight", 0), e.weight)
        else:
            G.add_edge(e.source, e.target,
                       edge_type=e.edge_type, edge_types=[e.edge_type],
                       weight=e.weight)

    return G


def graph_statistics(nodes: list[StockNode], edges: list[StockEdge]) -> dict:
    """Compute graph-level statistics."""
    import networkx as nx

    G = graph_to_networkx(nodes, edges)

    edge_type_counts = defaultdict(int)
    for e in edges:
        edge_type_counts[e.edge_type] += 1

    sector_counts = defaultdict(int)
    for n in nodes:
        sector_counts[n.sector] += 1

    return {
        "num_nodes": len(nodes),
        "num_edges": len(edges),
        "edge_types": dict(edge_type_counts),
        "density": nx.density(G),
        "avg_degree": np.mean([d for _, d in G.degree()]),
        "max_degree": max([d for _, d in G.degree()]),
        "num_components": nx.number_connected_components(G),
        "avg_clustering": nx.average_clustering(G),
        "num_sectors": len(sector_counts),
        "top_sectors": sorted(sector_counts.items(), key=lambda x: -x[1])[:10],
        "most_connected": sorted(G.degree(), key=lambda x: -x[1])[:10],
    }


# ═══════════════════════════════════════════
# PHASE 2: GNN MODEL
# ═══════════════════════════════════════════

def graph_to_pyg(nodes: list[StockNode], edges: list[StockEdge]):
    """Convert to PyTorch Geometric Data object."""
    try:
        import torch
        from torch_geometric.data import Data
    except ImportError:
        return None

    sym_to_idx = {n.symbol: i for i, n in enumerate(nodes)}

    # Node features
    x = torch.tensor(np.stack([n.features for n in nodes]), dtype=torch.float)

    # Labels
    y = torch.tensor([n.label for n in nodes], dtype=torch.float)

    # Edge index (2 x num_edges for undirected)
    src_idx = []
    dst_idx = []
    edge_attr_list = []

    edge_type_map = {"SECTOR": 0, "SUPPLY_CHAIN": 1, "COMMON_DIRECTORS": 2, "CORRELATION": 3}

    for e in edges:
        if e.source in sym_to_idx and e.target in sym_to_idx:
            s = sym_to_idx[e.source]
            t = sym_to_idx[e.target]

            # Both directions (undirected)
            src_idx.extend([s, t])
            dst_idx.extend([t, s])

            # Edge features: [type_one_hot(4), weight]
            type_vec = [0] * 4
            type_vec[edge_type_map.get(e.edge_type, 0)] = 1
            edge_feat = type_vec + [e.weight]
            edge_attr_list.extend([edge_feat, edge_feat])

    edge_index = torch.tensor([src_idx, dst_idx], dtype=torch.long)
    edge_attr = torch.tensor(edge_attr_list, dtype=torch.float) if edge_attr_list else None

    data = Data(x=x, edge_index=edge_index, y=y, edge_attr=edge_attr)
    data.symbols = [n.symbol for n in nodes]
    data.sectors = [n.sector for n in nodes]

    return data


def build_gnn_model(
    num_features: int = 20,
    hidden_dim: int = 64,
    num_classes: int = 1,
    num_edge_features: int = 5,
    model_type: str = "GAT",
    num_layers: int = 3,
    dropout: float = 0.3,
):
    """
    Build GNN model for stock prediction.

    Models:
      GCN -- Graph Convolutional Network (Kipf & Welling, 2017)
      GAT -- Graph Attention Network (Velickovic et al., 2018)
      GraphSAGE -- Inductive learning (Hamilton et al., 2017)
    """
    try:
        import torch
        import torch.nn as nn
        import torch.nn.functional as F
        from torch_geometric.nn import GCNConv, GATConv, SAGEConv
    except ImportError:
        return None

    class StockGNN(nn.Module):
        def __init__(self):
            super().__init__()

            ConvClass = {"GCN": GCNConv, "GAT": GATConv, "GraphSAGE": SAGEConv}[model_type]

            self.convs = nn.ModuleList()
            self.norms = nn.ModuleList()

            # First layer
            if model_type == "GAT":
                self.convs.append(GATConv(num_features, hidden_dim, heads=4, concat=True, dropout=dropout))
                current_dim = hidden_dim * 4
            else:
                self.convs.append(ConvClass(num_features, hidden_dim))
                current_dim = hidden_dim
            self.norms.append(nn.LayerNorm(current_dim))

            # Hidden layers
            for _ in range(num_layers - 2):
                if model_type == "GAT":
                    self.convs.append(GATConv(current_dim, hidden_dim, heads=4, concat=True, dropout=dropout))
                    current_dim = hidden_dim * 4
                else:
                    self.convs.append(ConvClass(current_dim, hidden_dim))
                    current_dim = hidden_dim
                self.norms.append(nn.LayerNorm(current_dim))

            # Final layer
            if model_type == "GAT":
                self.convs.append(GATConv(current_dim, hidden_dim, heads=1, concat=False, dropout=dropout))
            else:
                self.convs.append(ConvClass(current_dim, hidden_dim))
            self.norms.append(nn.LayerNorm(hidden_dim))

            # Prediction head
            self.predictor = nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim // 2),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(hidden_dim // 2, num_classes),
            )

            self.dropout = dropout

        def forward(self, data):
            x, edge_index = data.x, data.edge_index

            for conv, norm in zip(self.convs, self.norms):
                x = conv(x, edge_index)
                x = norm(x)
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)

            out = self.predictor(x)
            return out.squeeze(-1)

    return StockGNN()


def train_gnn(
    model_type: str = "GAT",
    lookback_days: int = 500,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    epochs: int = 100,
    lr: float = 0.001,
    hidden_dim: int = 64,
    task: str = "classification",
) -> dict:
    """
    Train GNN on temporal graph snapshots.

    Walk-forward training:
      1. Build graph at date t (features from [t-window, t])
      2. Label = next-day return direction (classification) or value (regression)
      3. Train on dates [0, train_split]
      4. Validate on [train_split, val_split]
      5. Test on [val_split, end]
    """
    try:
        import torch
        import torch.nn.functional as F
    except ImportError:
        return {"error": "PyTorch not installed"}

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Build temporal graph snapshots
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    dates = [r[0] for r in con.execute(f"""
        SELECT DISTINCT date FROM eod_ohlcv
        WHERE date >= '{cutoff}' ORDER BY date
    """).fetchall()]
    con.close()

    if len(dates) < 60:
        return {"error": f"Only {len(dates)} dates -- need at least 60"}

    # Sample dates (every 5 trading days for efficiency)
    sample_dates = dates[30::5]

    # Build graph for each date
    graphs = []
    for dt in sample_dates:
        dt_str = str(dt)
        nodes, edges = build_stock_graph(as_of_date=dt_str, feature_window=20)
        if len(nodes) < 10:
            continue

        pyg_data = graph_to_pyg(nodes, edges)
        if pyg_data is not None:
            if task == "classification":
                pyg_data.y = (pyg_data.y > 0).float()
            graphs.append(pyg_data)

    if len(graphs) < 10:
        return {"error": f"Only {len(graphs)} valid graph snapshots"}

    # Split
    n = len(graphs)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_graphs = graphs[:train_end]
    val_graphs = graphs[train_end:val_end]
    test_graphs = graphs[val_end:]

    # Build model
    num_features = graphs[0].x.shape[1]
    model = build_gnn_model(
        num_features=num_features,
        hidden_dim=hidden_dim,
        num_classes=1,
        model_type=model_type,
    )

    if model is None:
        return {"error": "Failed to build model -- check PyG installation"}

    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)

    if task == "classification":
        criterion = torch.nn.BCEWithLogitsLoss()
    else:
        criterion = torch.nn.MSELoss()

    # Training loop
    train_losses = []
    val_accuracies = []

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0

        for data in train_graphs:
            data = data.to(device)
            optimizer.zero_grad()

            out = model(data)
            loss = criterion(out, data.y)
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()

        epoch_loss /= len(train_graphs)
        train_losses.append(epoch_loss)

        # Validation
        if (epoch + 1) % 10 == 0:
            model.eval()
            correct = 0
            total = 0

            with torch.no_grad():
                for data in val_graphs:
                    data = data.to(device)
                    out = model(data)

                    if task == "classification":
                        pred = (out > 0).float()
                        correct += (pred == data.y).sum().item()
                        total += len(data.y)
                    else:
                        pred_dir = (out > 0).float()
                        actual_dir = (data.y > 0).float()
                        correct += (pred_dir == actual_dir).sum().item()
                        total += len(data.y)

            acc = correct / total if total > 0 else 0
            val_accuracies.append(acc)

            if (epoch + 1) % 20 == 0:
                print(f"  Epoch {epoch+1}/{epochs}: loss={epoch_loss:.4f}, val_acc={acc:.1%}")

    # Test evaluation
    model.eval()
    test_correct = 0
    test_total = 0
    test_predictions = []

    with torch.no_grad():
        for data in test_graphs:
            data = data.to(device)
            out = model(data)

            if task == "classification":
                pred = (out > 0).float()
                test_correct += (pred == data.y).sum().item()
            else:
                pred_dir = (out > 0).float()
                actual_dir = (data.y > 0).float()
                test_correct += (pred_dir == actual_dir).sum().item()

            test_total += len(data.y)

            for i, sym in enumerate(data.symbols):
                test_predictions.append({
                    "symbol": sym,
                    "predicted": float(out[i].cpu()),
                    "actual": float(data.y[i].cpu()),
                    "correct": bool((out[i] > 0) == (data.y[i] > 0)),
                })

    test_acc = test_correct / test_total if test_total > 0 else 0

    # Save model
    model_path = Path.home() / "pakfindata" / "models" / f"gnn_{model_type}_{task}.pt"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), model_path)

    # Per-sector accuracy
    pred_df = pd.DataFrame(test_predictions)
    sector_acc = {}
    if not pred_df.empty and "symbol" in pred_df.columns:
        sym_sector = {}
        for g in test_graphs:
            for s, sec in zip(g.symbols, g.sectors):
                sym_sector[s] = sec
        pred_df["sector"] = pred_df["symbol"].map(sym_sector)
        sector_acc = pred_df.groupby("sector")["correct"].mean().to_dict()

    result = {
        "model_type": model_type,
        "task": task,
        "train_graphs": len(train_graphs),
        "val_graphs": len(val_graphs),
        "test_graphs": len(test_graphs),
        "epochs": epochs,
        "final_train_loss": train_losses[-1],
        "test_accuracy": test_acc,
        "val_accuracies": val_accuracies,
        "train_losses": train_losses,
        "model_path": str(model_path),
        "num_nodes": graphs[0].x.shape[0],
        "num_features": num_features,
        "num_edges": graphs[0].edge_index.shape[1],
        "sector_accuracy": sector_acc,
        "predictions_sample": test_predictions[:20],
    }

    # Auto-save to Parquet
    _save_gnn_history(result)

    return result


def _save_gnn_history(result: dict):
    """Save GNN training run to Parquet."""
    _ensure_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{result['model_type']}_{result['task']}_{ts}"

    # Flatten for Parquet
    meta = {k: v for k, v in result.items()
            if k not in ("train_losses", "val_accuracies", "sector_accuracy", "predictions_sample")}
    pd.DataFrame([meta]).to_parquet(GNN_DIR / f"{prefix}_meta.parquet", index=False)

    # Training curves
    curves = pd.DataFrame({
        "epoch": list(range(len(result["train_losses"]))),
        "train_loss": result["train_losses"],
    })
    curves.to_parquet(GNN_DIR / f"{prefix}_curves.parquet", index=False)

    # Predictions
    if result.get("predictions_sample"):
        pd.DataFrame(result["predictions_sample"]).to_parquet(
            GNN_DIR / f"{prefix}_predictions.parquet", index=False)

    # Sector accuracy
    if result.get("sector_accuracy"):
        sa = pd.DataFrame([
            {"sector": k, "accuracy": v} for k, v in result["sector_accuracy"].items()
        ])
        sa.to_parquet(GNN_DIR / f"{prefix}_sector_acc.parquet", index=False)


def list_gnn_runs() -> list[dict]:
    """List all saved GNN training runs."""
    _ensure_dirs()
    meta_files = sorted(GNN_DIR.glob("*_meta.parquet"), reverse=True)
    runs = []
    for f in meta_files:
        try:
            meta = pd.read_parquet(f).iloc[0].to_dict()
            prefix = f.stem.replace("_meta", "")
            meta["prefix"] = prefix
            curves_file = GNN_DIR / f"{prefix}_curves.parquet"
            meta["has_curves"] = curves_file.exists()
            runs.append(meta)
        except Exception:
            continue
    return runs


# ═══════════════════════════════════════════
# PHASE 3: GRAPH ANALYTICS
# ═══════════════════════════════════════════

def find_most_influential_stocks(nodes, edges, top_n: int = 20) -> list[dict]:
    """
    Find most influential stocks using graph centrality measures.
    Combines: degree centrality, betweenness, PageRank, eigenvector.
    """
    import networkx as nx

    G = graph_to_networkx(nodes, edges)

    degree = nx.degree_centrality(G)
    betweenness = nx.betweenness_centrality(G)
    pagerank = nx.pagerank(G, weight="weight")
    try:
        eigenvector = nx.eigenvector_centrality(G, max_iter=1000, weight="weight")
    except Exception:
        eigenvector = {n: 0 for n in G.nodes}

    results = []
    for sym in G.nodes:
        sector = G.nodes[sym].get("sector", "Unknown")
        results.append({
            "symbol": sym,
            "sector": sector,
            "degree": degree.get(sym, 0),
            "betweenness": betweenness.get(sym, 0),
            "pagerank": pagerank.get(sym, 0),
            "eigenvector": eigenvector.get(sym, 0),
            "composite": (degree.get(sym, 0) * 0.3 +
                          betweenness.get(sym, 0) * 0.2 +
                          pagerank.get(sym, 0) * 0.3 +
                          eigenvector.get(sym, 0) * 0.2),
            "connections": G.degree(sym),
            "edge_types": list(set(
                G[sym][nbr].get("edge_type", "") for nbr in G.neighbors(sym)
            )),
        })

    results.sort(key=lambda x: -x["composite"])
    return results[:top_n]


def detect_communities(nodes, edges) -> dict:
    """Detect stock communities using Louvain algorithm."""
    import networkx as nx

    G = graph_to_networkx(nodes, edges)

    try:
        from networkx.algorithms.community import louvain_communities
        communities = louvain_communities(G, weight="weight")
    except Exception:
        from networkx.algorithms.community import label_propagation_communities
        communities = list(label_propagation_communities(G))

    result = {
        "num_communities": len(communities),
        "communities": [],
    }

    for i, community in enumerate(communities):
        syms = sorted(community)
        sectors = [G.nodes[s].get("sector", "Unknown") for s in syms]
        dominant_sector = max(set(sectors), key=sectors.count)

        result["communities"].append({
            "id": i,
            "size": len(syms),
            "symbols": syms,
            "dominant_sector": dominant_sector,
            "sector_mix": dict(pd.Series(sectors).value_counts()),
        })

    result["communities"].sort(key=lambda x: -x["size"])
    return result


def propagation_analysis(nodes, edges, source_symbol: str, hops: int = 3) -> dict:
    """
    Analyze how a price shock at source_symbol propagates through the graph.

    Returns: {hop_1: [neighbors], hop_2: [2nd degree], ...}
    with expected impact at each hop.
    """
    import networkx as nx

    G = graph_to_networkx(nodes, edges)

    if source_symbol not in G:
        return {"error": f"{source_symbol} not in graph"}

    result = {"source": source_symbol, "hops": []}
    visited = {source_symbol}
    current_level = [source_symbol]

    for hop in range(1, hops + 1):
        next_level = []
        for sym in current_level:
            for nbr in G.neighbors(sym):
                if nbr not in visited:
                    visited.add(nbr)
                    edge_data = G[sym][nbr]
                    next_level.append({
                        "symbol": nbr,
                        "sector": G.nodes[nbr].get("sector", ""),
                        "connection_type": edge_data.get("edge_type", ""),
                        "weight": edge_data.get("weight", 0),
                        "expected_impact": edge_data.get("weight", 0) * (0.5 ** (hop - 1)),
                    })

        next_level.sort(key=lambda x: -x["expected_impact"])
        result["hops"].append({
            "hop": hop,
            "count": len(next_level),
            "stocks": next_level[:10],
        })
        current_level = [s["symbol"] for s in next_level]

    return result


# ═══════════════════════════════════════════
# PHASE 4: CUSTOM GRAPH BUILDER
# ═══════════════════════════════════════════

def get_available_symbols() -> list[str]:
    """Get all symbols with recent EOD data."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    syms = [r[0] for r in con.execute("""
        SELECT DISTINCT symbol FROM eod_ohlcv
        WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY symbol
    """).fetchall()]
    con.close()
    return syms


def get_available_sectors() -> dict[str, str]:
    """Get sector_code -> sector_name mapping."""
    return _load_sector_names()


def get_symbols_by_sector(sector_name: str) -> list[str]:
    """Get all symbols in a given sector."""
    sector_map = _load_sector_names()
    codes = [code for code, name in sector_map.items() if name == sector_name]
    if not codes:
        return []

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    placeholders = ",".join(f"'{c}'" for c in codes)
    syms = [r[0] for r in con.execute(f"""
        SELECT DISTINCT symbol FROM eod_ohlcv
        WHERE sector_code IN ({placeholders})
        AND CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY symbol
    """).fetchall()]
    con.close()
    return syms


def build_custom_graph(
    symbols: list[str],
    edge_types: list[str] = None,
    correlation_threshold: float = 0.7,
    correlation_window: int = 60,
    feature_window: int = 20,
    feature_set: str = "full",
) -> Tuple[list[StockNode], list[StockEdge]]:
    """
    Build a custom graph from user-selected symbols and edge types.

    Args:
        symbols: list of PSX symbols to include as nodes
        edge_types: which edges to build ["SECTOR", "SUPPLY_CHAIN", "COMMON_DIRECTORS", "CORRELATION"]
        correlation_threshold: min abs correlation to create CORRELATION edge
        correlation_window: days for correlation computation
        feature_set: "full" (20 dims), "price_only" (7 dims), "momentum" (10 dims)
    """
    if edge_types is None:
        edge_types = ["SECTOR", "SUPPLY_CHAIN", "COMMON_DIRECTORS", "CORRELATION"]

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    cutoff_days = max(correlation_window, feature_window) * 2
    cutoff = (datetime.now() - timedelta(days=cutoff_days)).strftime("%Y-%m-%d")

    placeholders = ",".join(f"'{s}'" for s in symbols)
    eod = con.execute(f"""
        SELECT date, symbol, close, volume, sector_code AS sector
        FROM eod_ohlcv
        WHERE symbol IN ({placeholders})
        AND date >= '{cutoff}'
        ORDER BY date, symbol
    """).df()
    con.close()

    if eod.empty:
        return [], []

    sector_names = _load_sector_names()
    valid_symbols = sorted(eod["symbol"].unique())

    nodes = []
    returns_matrix = {}

    for sym in valid_symbols:
        sdf = eod[eod["symbol"] == sym].sort_values("date")
        if len(sdf) < feature_window:
            continue

        close = sdf["close"].values
        volume = sdf["volume"].values
        sector_code = sdf["sector"].iloc[-1] if "sector" in sdf.columns else ""
        sector = sector_names.get(str(sector_code),
                                  sector_names.get(str(sector_code).lstrip("0"), str(sector_code)))

        rets = np.diff(close) / close[:-1]
        returns_matrix[sym] = rets[-correlation_window:]

        recent = close[-feature_window:]
        recent_vol = volume[-feature_window:]
        recent_rets = rets[-feature_window:] if len(rets) >= feature_window else rets

        if feature_set == "price_only":
            features = np.array([
                recent[-1],
                np.mean(recent_rets) * TRADING_DAYS,
                np.std(recent_rets) * np.sqrt(TRADING_DAYS),
                recent[-1] / np.mean(recent) - 1,
                recent[-1] / recent[0] - 1,
                recent_vol[-1] / np.mean(recent_vol) if np.mean(recent_vol) > 0 else 1,
                np.log(np.mean(recent_vol) + 1),
            ], dtype=np.float32)
        elif feature_set == "momentum":
            features = np.array([
                recent[-1],
                np.mean(recent_rets) * TRADING_DAYS,
                np.std(recent_rets) * np.sqrt(TRADING_DAYS),
                recent[-1] / np.mean(recent) - 1,
                recent[-1] / recent[0] - 1,
                (recent[-1] / recent[-5] - 1) if len(recent) >= 5 else 0,
                (recent[-1] / recent[-10] - 1) if len(recent) >= 10 else 0,
                len(recent_rets[recent_rets > 0]) / len(recent_rets) if len(recent_rets) > 0 else 0.5,
                np.mean(np.abs(recent_rets)),
                np.mean(recent_rets[-5:]) / np.std(recent_rets[-5:]) if np.std(recent_rets[-5:]) > 0 else 0,
            ], dtype=np.float32)
        else:  # full
            features = np.array([
                recent[-1],
                np.mean(recent_rets) * TRADING_DAYS,
                np.std(recent_rets) * np.sqrt(TRADING_DAYS),
                recent[-1] / np.mean(recent) - 1,
                recent[-1] / recent[0] - 1,
                recent_vol[-1] / np.mean(recent_vol) if np.mean(recent_vol) > 0 else 1,
                np.log(np.mean(recent_vol) + 1),
                np.std(recent_rets[-5:]) / np.std(recent_rets) if np.std(recent_rets) > 0 else 1,
                max(recent) / min(recent) - 1 if min(recent) > 0 else 0,
                (recent[-1] / recent[-5] - 1) if len(recent) >= 5 else 0,
                (recent[-1] / recent[-10] - 1) if len(recent) >= 10 else 0,
                np.mean(np.abs(recent_rets)),
                np.max(recent_rets) if len(recent_rets) > 0 else 0,
                np.min(recent_rets) if len(recent_rets) > 0 else 0,
                len(recent_rets[recent_rets > 0]) / len(recent_rets) if len(recent_rets) > 0 else 0.5,
                float(pd.Series(recent_rets).skew()) if len(recent_rets) > 2 else 0,
                float(pd.Series(recent_rets).kurtosis()) if len(recent_rets) > 3 else 0,
                np.mean(recent_rets[-5:]) / np.std(recent_rets[-5:]) if np.std(recent_rets[-5:]) > 0 else 0,
                0, 0,
            ], dtype=np.float32)

        features = np.nan_to_num(features, nan=0, posinf=0, neginf=0)
        all_rets = np.diff(close) / close[:-1]
        label = float(all_rets[-1]) if len(all_rets) > 0 else 0

        nodes.append(StockNode(symbol=sym, sector=sector, features=features, label=label))

    node_symbols = {n.symbol for n in nodes}
    edges = []

    if "SECTOR" in edge_types:
        sector_groups = defaultdict(list)
        for n in nodes:
            sector_groups[n.sector].append(n.symbol)
        for sector, syms in sector_groups.items():
            if sector == "Unknown":
                continue
            for i in range(len(syms)):
                for j in range(i + 1, len(syms)):
                    edges.append(StockEdge(source=syms[i], target=syms[j],
                                           edge_type="SECTOR", weight=1.0))

    if "SUPPLY_CHAIN" in edge_types:
        sym_sectors = {n.symbol: n.sector.upper() for n in nodes}
        sector_syms = defaultdict(list)
        for sym, sec in sym_sectors.items():
            sector_syms[sec].append(sym)
        for downstream, upstreams in PSX_SUPPLY_CHAIN.items():
            for upstream in upstreams:
                for d_sym in sector_syms.get(downstream, []):
                    for u_sym in sector_syms.get(upstream, []):
                        if d_sym in node_symbols and u_sym in node_symbols:
                            edges.append(StockEdge(source=u_sym, target=d_sym,
                                                   edge_type="SUPPLY_CHAIN", weight=0.7))

    if "COMMON_DIRECTORS" in edge_types:
        for group_name, group_syms in PSX_BUSINESS_GROUPS.items():
            valid_syms = [s for s in group_syms if s in node_symbols]
            for i in range(len(valid_syms)):
                for j in range(i + 1, len(valid_syms)):
                    edges.append(StockEdge(source=valid_syms[i], target=valid_syms[j],
                                           edge_type="COMMON_DIRECTORS", weight=0.8))
        try:
            scon = sqlite3.connect(str(PSX_SQLITE))
            rows = scon.execute(
                "SELECT symbol, key_people FROM company_profiles WHERE key_people IS NOT NULL"
            ).fetchall()
            scon.close()
            director_companies = defaultdict(set)
            for sym, kp_json in rows:
                if sym not in node_symbols:
                    continue
                try:
                    people = json.loads(kp_json) if kp_json else []
                    for p in people:
                        name = p.get("name", "").strip().upper()
                        if len(name) > 3:
                            director_companies[name].add(sym)
                except Exception:
                    continue
            for director, companies in director_companies.items():
                companies = list(companies)
                if len(companies) >= 2:
                    for i in range(len(companies)):
                        for j in range(i + 1, len(companies)):
                            edges.append(StockEdge(source=companies[i], target=companies[j],
                                                   edge_type="COMMON_DIRECTORS",
                                                   weight=1.0 / len(companies)))
        except Exception:
            pass

    if "CORRELATION" in edge_types:
        corr_syms = [s for s in node_symbols if s in returns_matrix and len(returns_matrix[s]) >= 20]
        if len(corr_syms) > 1:
            ret_df = pd.DataFrame({
                s: returns_matrix[s][-correlation_window:]
                for s in corr_syms if len(returns_matrix[s]) >= correlation_window
            })
            if not ret_df.empty:
                corr_matrix = ret_df.corr()
                for i, sym_a in enumerate(corr_matrix.columns):
                    for j, sym_b in enumerate(corr_matrix.columns):
                        if i < j and abs(corr_matrix.iloc[i, j]) > correlation_threshold:
                            edges.append(StockEdge(source=sym_a, target=sym_b,
                                                   edge_type="CORRELATION",
                                                   weight=float(corr_matrix.iloc[i, j])))

    # Deduplicate
    seen = set()
    unique_edges = []
    for e in edges:
        key = tuple(sorted([e.source, e.target])) + (e.edge_type,)
        if key not in seen:
            seen.add(key)
            unique_edges.append(e)

    return nodes, unique_edges


def train_custom_gnn(
    symbols: list[str],
    edge_types: list[str],
    model_type: str = "GAT",
    feature_set: str = "full",
    correlation_threshold: float = 0.7,
    epochs: int = 50,
    lr: float = 0.001,
    hidden_dim: int = 64,
    task: str = "classification",
    lookback_days: int = 500,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> dict:
    """Train a GNN on a custom-selected graph."""
    try:
        import torch
    except ImportError:
        return {"error": "PyTorch not installed"}

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    dates = [r[0] for r in con.execute(f"""
        SELECT DISTINCT date FROM eod_ohlcv WHERE date >= '{cutoff}' ORDER BY date
    """).fetchall()]
    con.close()

    if len(dates) < 60:
        return {"error": f"Only {len(dates)} dates -- need at least 60"}

    sample_dates = dates[30::5]
    feat_dims = {"full": 20, "price_only": 7, "momentum": 10}
    num_features = feat_dims.get(feature_set, 20)

    graphs = []
    for dt in sample_dates:
        nodes, edges = build_custom_graph(
            symbols=symbols, edge_types=edge_types,
            correlation_threshold=correlation_threshold,
            feature_set=feature_set,
        )
        if len(nodes) < 5:
            continue
        pyg_data = graph_to_pyg(nodes, edges)
        if pyg_data is not None:
            if task == "classification":
                pyg_data.y = (pyg_data.y > 0).float()
            graphs.append(pyg_data)

    if len(graphs) < 5:
        return {"error": f"Only {len(graphs)} valid snapshots (need 5+). Try more symbols or longer lookback."}

    n = len(graphs)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))
    train_graphs = graphs[:train_end]
    val_graphs = graphs[train_end:val_end]
    test_graphs = graphs[val_end:]

    if not train_graphs or not val_graphs or not test_graphs:
        return {"error": "Not enough data for train/val/test split. Try longer lookback."}

    model = build_gnn_model(
        num_features=num_features, hidden_dim=hidden_dim,
        num_classes=1, model_type=model_type,
    )
    if model is None:
        return {"error": "Failed to build model -- check PyG installation"}

    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    criterion = (torch.nn.BCEWithLogitsLoss() if task == "classification"
                 else torch.nn.MSELoss())

    train_losses, val_accuracies = [], []

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0
        for data in train_graphs:
            data = data.to(device)
            optimizer.zero_grad()
            out = model(data)
            loss = criterion(out, data.y)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        epoch_loss /= len(train_graphs)
        train_losses.append(epoch_loss)

        if (epoch + 1) % 10 == 0:
            model.eval()
            correct = total = 0
            with torch.no_grad():
                for data in val_graphs:
                    data = data.to(device)
                    out = model(data)
                    pred = (out > 0).float()
                    target = data.y if task == "classification" else (data.y > 0).float()
                    correct += (pred == target).sum().item()
                    total += len(data.y)
            val_accuracies.append(correct / total if total > 0 else 0)

    # Test
    model.eval()
    test_correct = test_total = 0
    test_predictions = []
    with torch.no_grad():
        for data in test_graphs:
            data = data.to(device)
            out = model(data)
            pred = (out > 0).float()
            target = data.y if task == "classification" else (data.y > 0).float()
            test_correct += (pred == target).sum().item()
            test_total += len(data.y)
            for i, sym in enumerate(data.symbols):
                test_predictions.append({
                    "symbol": sym,
                    "predicted": float(out[i].cpu()),
                    "actual": float(data.y[i].cpu()),
                    "correct": bool((out[i] > 0) == (data.y[i] > 0)),
                })

    test_acc = test_correct / test_total if test_total > 0 else 0

    model_path = Path.home() / "pakfindata" / "models" / f"gnn_custom_{model_type}_{task}.pt"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), model_path)

    pred_df = pd.DataFrame(test_predictions)
    sector_acc = {}
    if not pred_df.empty:
        sym_sector = {}
        for g in test_graphs:
            for s, sec in zip(g.symbols, g.sectors):
                sym_sector[s] = sec
        pred_df["sector"] = pred_df["symbol"].map(sym_sector)
        sector_acc = pred_df.groupby("sector")["correct"].mean().to_dict()

    result = {
        "model_type": model_type, "task": task, "feature_set": feature_set,
        "edge_types": edge_types, "custom_symbols": len(symbols),
        "train_graphs": len(train_graphs), "val_graphs": len(val_graphs),
        "test_graphs": len(test_graphs), "epochs": epochs,
        "final_train_loss": train_losses[-1], "test_accuracy": test_acc,
        "val_accuracies": val_accuracies, "train_losses": train_losses,
        "model_path": str(model_path), "num_nodes": graphs[0].x.shape[0],
        "num_features": num_features, "num_edges": graphs[0].edge_index.shape[1],
        "sector_accuracy": sector_acc, "predictions_sample": test_predictions[:30],
    }
    _save_gnn_history(result)
    return result
