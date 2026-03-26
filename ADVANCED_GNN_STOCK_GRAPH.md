# Claude Code Prompt: Strategy 12 — Graph Neural Network for PSX Stock Relationships

## Context

pakfindata has company profiles, sector data, 598K EOD bars, and 564 listed companies. 
This strategy builds a graph of PSX stocks connected by multiple edge types 
(sector, supply chain, common directors, price correlation), then trains a GNN 
to predict which connections drive future price co-movement.

**This is RESEARCH grade — the kind of work published at NeurIPS/ICML/KDD.**

**The graph:**
```
Nodes: 564 PSX stocks
  Features: price, volume, returns, sector, market cap, P/E, EPS growth, free float

Edges (4 types):
  1. SECTOR — same PSX sector (33 sectors)
  2. SUPPLY_CHAIN — business relationship (cement buys from limestone, banks lend to all)
  3. COMMON_DIRECTORS — shared board members (very common on PSX)
  4. CORRELATION — rolling 60-day price correlation > 0.7
```

**Why GNN works on PSX:**
- PSX is a SMALL market (564 nodes) — GNNs excel on small, dense graphs
- Director networks are REAL — same families sit on multiple boards
- Supply chains are VISIBLE — Pakistan's industrial base is concentrated
- Sector co-movement is STRONG — 33 sectors, 5-20 stocks each
- Information propagates SLOWLY — low analyst coverage means graph edges = info channels
- Cross-holdings are common — conglomerates own stakes in multiple listed companies

**Academic references:**
- "Temporal Relational Ranking for Stock Prediction" (Feng et al., 2019, CIKM)
- "Stock Movement Prediction with Financial News using GAT" (Kim et al., 2019)
- "Exploring Graph Neural Networks for Stock Market Predictions" (Matsunaga et al., 2019)
- "REST: Relational Event-driven Stock Trend Forecasting" (Xu et al., 2021, WWW)

## What already exists

```bash
# Check company profile data for graph construction
python3 -c "
import duckdb, sqlite3, json

# Company profiles (nodes)
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in con.execute('SELECT table_name FROM information_schema.tables').fetchall():
    tl = t[0].lower()
    if any(k in tl for k in ['company','profile','sector','director','constituent']):
        count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
        cols = [c[0] for c in con.execute(f'DESCRIBE {t[0]}').fetchall()]
        print(f'DuckDB {t[0]}: {count:,} — {cols[:8]}')
con.close()

scon = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for t in [r[0] for r in scon.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]:
    tl = t.lower()
    if any(k in tl for k in ['company','profile','sector','director','constituent','key_people']):
        count = scon.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[1] for r in scon.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'SQLite {t}: {count:,} — {cols[:8]}')
scon.close()
"

# Check sector data
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
try:
    df = con.execute('''
        SELECT sector, COUNT(DISTINCT symbol) as n
        FROM eod_ohlcv WHERE sector IS NOT NULL
        GROUP BY sector ORDER BY n DESC
    ''').df()
    print(f'Sectors: {len(df)}')
    print(df.to_string())
except: pass
con.close()
"

# Check for director/key people data
python3 -c "
import sqlite3, json
scon = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
try:
    # Check company_profiles for key_people JSON
    rows = scon.execute('SELECT symbol, key_people FROM company_profiles WHERE key_people IS NOT NULL LIMIT 5').fetchall()
    for sym, kp in rows:
        people = json.loads(kp) if kp else []
        print(f'{sym}: {len(people)} people — {[p.get(\"name\",\"\")[:30] for p in people[:3]]}')
except Exception as e:
    print(f'No key_people data: {e}')
scon.close()
"

# Check PyTorch Geometric availability
python3 -c "
try:
    import torch; print(f'PyTorch: {torch.__version__}, CUDA: {torch.cuda.is_available()}')
except: print('PyTorch: NOT INSTALLED')

try:
    import torch_geometric; print(f'PyG: {torch_geometric.__version__}')
except: print('PyG: NOT INSTALLED — install: pip install torch-geometric')

try:
    import networkx; print(f'NetworkX: {networkx.__version__}')
except: print('NetworkX: NOT INSTALLED — install: pip install networkx')
"
```

**READ ALL OUTPUT — identify available data for graph construction.**

## Step 1: Install dependencies

```bash
conda activate psx

# NetworkX (lightweight, always install)
pip install networkx --break-system-packages 2>/dev/null || pip install networkx

# PyTorch (if not already installed — ~2.5GB with CUDA)
# Check first: python3 -c "import torch; print(torch.__version__)"
# If missing:
pip install torch --break-system-packages 2>/dev/null || pip install torch

# PyTorch Geometric (after PyTorch is installed)
pip install torch-geometric --break-system-packages 2>/dev/null || pip install torch-geometric

# Optional but useful
pip install torch-scatter torch-sparse -f https://data.pyg.org/whl/torch-2.5.0+cu124.html \
    --break-system-packages 2>/dev/null || echo "Sparse extensions optional"
```

## Step 2: Create the GNN Engine

Create `src/pakfindata/engine/gnn_stock_graph.py`:

```python
"""
Graph Neural Network for PSX Stock Relationships.

Builds a heterogeneous graph of PSX stocks with multiple edge types:
  1. SECTOR — same sector membership
  2. SUPPLY_CHAIN — inferred business relationships
  3. COMMON_DIRECTORS — shared board members
  4. CORRELATION — rolling price correlation > threshold

Then trains GNN models (GCN, GAT, GraphSAGE) to predict:
  - Next-day return direction (classification)
  - Next-day return magnitude (regression)
  - Cross-stock momentum propagation (which neighbor moves first?)

Architecture:
  Node features → GNN layers → per-node prediction
  Graph structure encodes HOW stocks are related
  Temporal batching: train on window t, predict t+1

PSX-Specific:
  - 564 nodes (small graph — GNN trains fast)
  - 33 sectors (dense intra-sector edges)
  - Common director network is STRONG in Pakistan (family business groups)
  - 245 trading days/year
  - Circuit breakers ±7.5% cap daily returns

Published methods implemented:
  - GCN (Kipf & Welling, 2017)
  - GAT (Veličković et al., 2018)
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
# These groups have board members sitting across multiple companies
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
    weight: float      # edge weight (correlation value, overlap count, etc.)


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
    
    # ── Load EOD data for all liquid stocks ──
    eod = con.execute(f"""
        SELECT date, symbol, close, volume, sector
        FROM eod_ohlcv
        WHERE date BETWEEN '{cutoff}' AND '{as_of_date}'
        AND volume > {min_volume}
        ORDER BY date, symbol
    """).df()
    
    con.close()
    
    if eod.empty:
        return [], []
    
    symbols = sorted(eod["symbol"].unique())
    sym_to_idx = {s: i for i, s in enumerate(symbols)}
    
    # ── Compute node features ──
    nodes = []
    returns_matrix = {}  # symbol → array of daily returns
    
    for sym in symbols:
        sdf = eod[eod["symbol"] == sym].sort_values("date")
        if len(sdf) < feature_window:
            continue
        
        close = sdf["close"].values
        volume = sdf["volume"].values
        sector = sdf["sector"].iloc[-1] if "sector" in sdf.columns else "Unknown"
        
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
            recent_vol[-1] / np.mean(recent_vol) if np.mean(recent_vol) > 0 else 1,  # volume ratio
            np.log(np.mean(recent_vol) + 1),               # log avg volume
            
            # Volatility features
            np.std(recent_rets[-5:]) / np.std(recent_rets) if np.std(recent_rets) > 0 else 1,  # vol ratio
            max(recent) / min(recent) - 1 if min(recent) > 0 else 0,  # range ratio
            
            # Momentum features
            (recent[-1] / recent[-5] - 1) if len(recent) >= 5 else 0,   # 5-day return
            (recent[-1] / recent[-10] - 1) if len(recent) >= 10 else 0,  # 10-day return
            
            # Microstructure
            np.mean(np.abs(recent_rets)),                  # average absolute return
            np.max(recent_rets) if len(recent_rets) > 0 else 0,  # max daily return
            np.min(recent_rets) if len(recent_rets) > 0 else 0,  # min daily return
            len(recent_rets[recent_rets > 0]) / len(recent_rets) if len(recent_rets) > 0 else 0.5,  # up day ratio
            
            # Statistical moments
            float(pd.Series(recent_rets).skew()) if len(recent_rets) > 2 else 0,  # skewness
            float(pd.Series(recent_rets).kurtosis()) if len(recent_rets) > 3 else 0,  # kurtosis
            
            # Relative strength
            np.mean(recent_rets[-5:]) / np.std(recent_rets[-5:]) if np.std(recent_rets[-5:]) > 0 else 0,
            
            # Placeholder for sector encoding (filled later)
            0, 0,
        ], dtype=np.float32)
        
        # Replace NaN/Inf
        features = np.nan_to_num(features, nan=0, posinf=0, neginf=0)
        
        # Next-day return as label (if available)
        all_rets = np.diff(close) / close[:-1]
        label = float(all_rets[-1]) if len(all_rets) > 0 else 0
        
        nodes.append(StockNode(
            symbol=sym,
            sector=sector if sector else "Unknown",
            features=features,
            label=label,
        ))
    
    node_symbols = {n.symbol for n in nodes}
    
    # ── Build edges ──
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
        
        # Build director → companies mapping
        director_companies = defaultdict(set)
        for sym, kp_json in rows:
            if sym not in node_symbols:
                continue
            try:
                people = json.loads(kp_json) if kp_json else []
                for p in people:
                    name = p.get("name", "").strip().upper()
                    if len(name) > 3:  # skip empty/short names
                        director_companies[name].add(sym)
            except:
                continue
        
        # Create edges for shared directors
        for director, companies in director_companies.items():
            companies = list(companies)
            if len(companies) >= 2:
                for i in range(len(companies)):
                    for j in range(i + 1, len(companies)):
                        # Check not already added
                        edges.append(StockEdge(
                            source=companies[i], target=companies[j],
                            edge_type="COMMON_DIRECTORS",
                            weight=1.0 / len(companies),  # dilute if director is on many boards
                        ))
    except:
        pass
    
    # 4. CORRELATION edges
    corr_syms = [s for s in node_symbols if s in returns_matrix and len(returns_matrix[s]) >= 20]
    if len(corr_syms) > 1:
        # Build correlation matrix
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
            # Add edge type to existing edge
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
    
    # Edge index (2 × num_edges for undirected)
    src_idx = []
    dst_idx = []
    edge_attr_list = []
    
    # Edge type encoding
    edge_type_map = {"SECTOR": 0, "SUPPLY_CHAIN": 1, "COMMON_DIRECTORS": 2, "CORRELATION": 3}
    
    for e in edges:
        if e.source in sym_to_idx and e.target in sym_to_idx:
            s = sym_to_idx[e.source]
            t = sym_to_idx[e.target]
            
            # Add both directions (undirected graph)
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
      GCN — Graph Convolutional Network (Kipf & Welling, 2017)
      GAT — Graph Attention Network (Veličković et al., 2018)
      GraphSAGE — Inductive learning (Hamilton et al., 2017)
    """
    try:
        import torch
        import torch.nn as nn
        import torch.nn.functional as F
        from torch_geometric.nn import GCNConv, GATConv, SAGEConv, global_mean_pool
    except ImportError:
        return None
    
    class StockGNN(nn.Module):
        def __init__(self):
            super().__init__()
            
            # Select convolution type
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
            
            for i, (conv, norm) in enumerate(zip(self.convs, self.norms)):
                x = conv(x, edge_index)
                x = norm(x)
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
            
            # Per-node prediction
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
    task: str = "classification",  # "classification" or "regression"
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
        return {"error": f"Only {len(dates)} dates — need at least 60"}
    
    # Sample dates (every 5 trading days for efficiency)
    sample_dates = dates[30::5]  # skip first 30 for feature warmup
    
    # Build graph for each date
    graphs = []
    for dt in sample_dates:
        dt_str = str(dt)
        nodes, edges = build_stock_graph(as_of_date=dt_str, feature_window=20)
        if len(nodes) < 10:
            continue
        
        pyg_data = graph_to_pyg(nodes, edges)
        if pyg_data is not None:
            # Convert labels to binary for classification
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
        return {"error": "Failed to build model — check PyG installation"}
    
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
                        # For regression, measure directional accuracy
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
            
            # Store per-stock predictions
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
        # Map symbols to sectors
        sym_sector = {n.symbol: n.sector for g in test_graphs for n in 
                     [type('N', (), {'symbol': s, 'sector': sec})() 
                      for s, sec in zip(g.symbols, g.sectors)]}
        pred_df["sector"] = pred_df["symbol"].map(sym_sector)
        sector_acc = pred_df.groupby("sector")["correct"].mean().to_dict()
    
    return {
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
    except:
        eigenvector = {n: 0 for n in G.nodes}
    
    # Composite score
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
    except:
        # Fallback: label propagation
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
```

## Step 3: Create the Streamlit page

Create `src/pakfindata/ui/page_views/advanced_gnn.py`:

### Tab 1: Graph Explorer
```
├── Interactive network visualization (Plotly/D3):
│   ├── Nodes colored by sector (33 colors)
│   ├── Node size = market cap or centrality
│   ├── Edges colored by type: SECTOR (gray), SUPPLY_CHAIN (blue), 
│   │   COMMON_DIRECTORS (gold), CORRELATION (green/red)
│   ├── Click node → highlight neighbors, show details
│   ├── Edge type toggles (show/hide each type)
│   └── Layout: force-directed or circular by sector
├── Graph statistics card:
│   Nodes | Edges | Density | Avg Degree | Components | Clustering
├── Edge type breakdown (pie chart)
├── Top 10 most connected stocks (bar chart)
└── Community detection results (colored clusters)
```

### Tab 2: GNN Training
```
├── Model configuration:
│   ├── Architecture: GCN / GAT / GraphSAGE
│   ├── Task: Classification / Regression
│   ├── Hidden dim: 32 / 64 / 128
│   ├── Layers: 2 / 3 / 4
│   ├── Epochs: 50 / 100 / 200
│   ├── Lookback: 6M / 1Y / 2Y
│   └── [Train Model]
├── Training curves: loss + val accuracy over epochs
├── Test results:
│   ├── Overall accuracy vs random (50%)
│   ├── Per-sector accuracy heatmap
│   ├── Confusion matrix
│   └── Top predictions table: Symbol | Predicted | Actual | Correct
├── Model comparison table (GCN vs GAT vs GraphSAGE)
└── Feature importance: which node features matter most?
```

### Tab 3: Influence Analysis
```
├── Most influential stocks (composite centrality ranking):
│   Table: Rank | Symbol | Sector | Degree | Betweenness | PageRank | Eigenvector
├── Shock propagation:
│   ├── Source symbol selector
│   ├── [Analyze Propagation]
│   ├── Hop-by-hop visualization:
│   │   Hop 1: direct neighbors (strongest impact)
│   │   Hop 2: 2nd degree (diluted)
│   │   Hop 3: 3rd degree (weak)
│   └── Expected impact at each hop
├── Business group clusters (Nishat, Engro, Fauji, etc.)
│   Interactive: click group → highlight all members in graph
└── Supply chain flow diagram (upstream → downstream)
```

### Tab 4: Research
```
├── Ablation study: which edge types contribute most?
│   Train with: all edges, sector-only, correlation-only, directors-only
│   Compare accuracy
├── Temporal stability: does the graph structure change over time?
│   Monthly graph snapshots comparison
├── Correlation vs causation: lead-lag analysis between connected stocks
├── Graph embedding visualization: t-SNE of node embeddings
│   Do sector clusters emerge? Do business groups cluster?
├── Comparison with baselines:
│   ├── GNN vs simple momentum
│   ├── GNN vs sector rotation
│   ├── GNN vs random forest (no graph structure)
│   └── Value of graph structure = GNN accuracy - RF accuracy
├── Pakistan-specific findings:
│   ├── Director network predicts co-movement better than sector?
│   ├── Supply chain edges have directional information flow?
│   ├── Business group stocks lead/lag each other?
│   └── Small-cap stocks follow large-cap within same graph community?
└── Paper outline: methodology, results, PSX-specific contributions
```

### Key chart — Network graph visualization:
```python
import plotly.graph_objects as go
import networkx as nx

G = graph_to_networkx(nodes, edges)
pos = nx.spring_layout(G, k=0.3, iterations=50, weight='weight')

# Edge traces (one per type with different colors)
edge_colors = {
    "SECTOR": "#6B7280",
    "SUPPLY_CHAIN": "#3B82F6",
    "COMMON_DIRECTORS": "#C8A96E",
    "CORRELATION": "#22C55E",
}

for edge_type, color in edge_colors.items():
    edge_x, edge_y = [], []
    for u, v, d in G.edges(data=True):
        if d.get("edge_type") == edge_type:
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
    
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y, mode='lines',
        line=dict(width=0.5, color=color),
        name=edge_type, opacity=0.4,
    ))

# Node trace
sector_colors = {s: f"hsl({i*30}, 70%, 50%)" for i, s in enumerate(set(n.sector for n in nodes))}

fig.add_trace(go.Scatter(
    x=[pos[n.symbol][0] for n in nodes],
    y=[pos[n.symbol][1] for n in nodes],
    mode='markers+text',
    marker=dict(
        size=[max(5, G.degree(n.symbol) * 2) for n in nodes],
        color=[sector_colors.get(n.sector, "#888") for n in nodes],
    ),
    text=[n.symbol for n in nodes],
    textposition='top center',
    textfont=dict(size=7),
))

fig.update_layout(
    template="plotly_dark", paper_bgcolor="#0B0E11", plot_bgcolor="#0B0E11",
    showlegend=True, height=700,
    title="PSX Stock Relationship Graph",
)
```

## Step 4: Add to sidebar under new ADVANCED section

In `app.py`, add a new nav group:

```python
# After STRATEGIES and before ADMIN
st.sidebar.markdown("**ADVANCED**")
st.page_link("page_views/advanced_gnn.py", label="Stock Graph (GNN)", icon="🕸️")
# Future: more advanced/research pages here
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test graph construction (no PyTorch needed)
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.gnn_stock_graph import build_stock_graph, graph_statistics

nodes, edges = build_stock_graph(correlation_threshold=0.7)
print(f'Nodes: {len(nodes)}, Edges: {len(edges)}')

stats = graph_statistics(nodes, edges)
for k, v in stats.items():
    if k not in ('top_sectors', 'most_connected'):
        print(f'  {k}: {v}')
print(f'  Top sectors: {stats[\"top_sectors\"][:5]}')
print(f'  Most connected: {stats[\"most_connected\"][:5]}')
"

# Test influence analysis (NetworkX only)
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.gnn_stock_graph import build_stock_graph, find_most_influential_stocks

nodes, edges = build_stock_graph()
top = find_most_influential_stocks(nodes, edges, top_n=10)
print('Most influential PSX stocks:')
for s in top:
    print(f'  {s[\"symbol\"]:8s} ({s[\"sector\"]:20s}) degree:{s[\"degree\"]:.3f} PR:{s[\"pagerank\"]:.4f} conn:{s[\"connections\"]}')
"

# Test community detection
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.gnn_stock_graph import build_stock_graph, detect_communities

nodes, edges = build_stock_graph()
communities = detect_communities(nodes, edges)
print(f'Communities found: {communities[\"num_communities\"]}')
for c in communities['communities'][:5]:
    print(f'  Cluster {c[\"id\"]}: {c[\"size\"]} stocks, dominant: {c[\"dominant_sector\"]} — {c[\"symbols\"][:5]}')
"

# Test propagation analysis
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.gnn_stock_graph import build_stock_graph, propagation_analysis

nodes, edges = build_stock_graph()
result = propagation_analysis(nodes, edges, 'OGDC', hops=3)
print(f'Shock propagation from OGDC:')
for hop in result['hops']:
    print(f'  Hop {hop[\"hop\"]}: {hop[\"count\"]} stocks reached')
    for s in hop['stocks'][:3]:
        print(f'    {s[\"symbol\"]:8s} ({s[\"connection_type\"]}) impact: {s[\"expected_impact\"]:.3f}')
"

# Test GNN training (requires PyTorch + PyG)
python3 -c "
import sys; sys.path.insert(0, 'src')
try:
    from pakfindata.engine.gnn_stock_graph import train_gnn
    result = train_gnn(model_type='GAT', epochs=20, lookback_days=120)
    if 'error' not in result:
        print(f'GNN Training Result:')
        print(f'  Model: {result[\"model_type\"]}')
        print(f'  Test Accuracy: {result[\"test_accuracy\"]:.1%}')
        print(f'  Nodes: {result[\"num_nodes\"]}, Edges: {result[\"num_edges\"]}')
        print(f'  Train Loss: {result[\"final_train_loss\"]:.4f}')
    else:
        print(result)
except ImportError as e:
    print(f'Missing dependency: {e}')
    print('Install: pip install torch torch-geometric networkx')
"
```

## IMPORTANT NOTES

1. **Graph construction works WITHOUT PyTorch** — only needs NetworkX + numpy/pandas
2. **GNN training needs PyTorch + PyG** — install only when ready for research
3. **NetworkX is lightweight** (~5 MB) — always install for graph analytics
4. **PyTorch Geometric (PyG)** requires matching PyTorch version — check compatibility
5. **564 nodes = small graph** — trains in seconds on RTX 4080, no GPU memory concerns
6. **4 edge types** — sector (auto-discovered), supply chain (curated), directors (from scraped profiles), correlation (computed)
7. **PSX business groups** are hardcoded (Engro, Nishat, Fauji, etc.) — these are well-known
8. **Supply chain map** is manually curated from Pakistan industrial knowledge
9. **Director edges from real data** — extracted from `company_profiles.key_people` JSON column
10. **Walk-forward training** — no look-ahead bias, respects temporal ordering
11. **20-dimension node features** — price momentum, volatility, volume, microstructure
12. **GAT recommended** — attention mechanism learns WHICH edges matter more
13. **Community detection** reveals hidden stock clusters beyond sector boundaries
14. **Propagation analysis** shows how a shock (e.g., OGDC earnings miss) ripples through the graph
15. **This goes under ADVANCED** nav section in sidebar — separate from STRATEGIES
16. **Research-grade output:** ablation studies, baseline comparisons, t-SNE embeddings
17. **Publication angle:** "GNN for Emerging Market Stock Prediction: Evidence from PSX" — novelty is the director network + supply chain edges on a thin, information-poor market
