# Claude Code Prompt: ML Price Prediction Engine

## Context

Build an ML-powered price prediction page inside pakfindata. It reads from 
DuckDB (existing data — no new scraping needed) and runs XGBoost/LightGBM 
on the RTX 4080 GPU for training.

This is a NEW page: `src/pakfindata/ui/page_views/ml_predictions.py`
Add it to the sidebar under RESEARCH section in app.py.

## Step 1: Understand available features

All these already exist in DuckDB. Run this to confirm:

```bash
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

# Check what tables exist
for t in con.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=\"main\"').fetchall():
    count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
    print(f'  {t[0]}: {count:,}')

# Check EOD columns
print('\neod_ohlcv columns:')
for c in con.execute('DESCRIBE eod_ohlcv').fetchall():
    print(f'  {c[0]}: {c[1]}')

# Check tick_logs columns
print('\ntick_logs columns:')
for c in con.execute('DESCRIBE tick_logs').fetchall():
    print(f'  {c[0]}: {c[1]}')

# Check ohlcv_5s columns
print('\nohlcv_5s columns:')
for c in con.execute('DESCRIBE ohlcv_5s').fetchall():
    print(f'  {c[0]}: {c[1]}')

con.close()
"
```

**Read the output before proceeding.** Column names may differ from what's listed below.

## Step 2: Create the feature engineering engine

Create `src/pakfindata/engine/ml_features.py`:

```python
"""
ML Feature Engineering for PSX Price Prediction.

Extracts features from DuckDB tables, computes technical indicators,
and builds training/prediction datasets.

All features are computed in raw numpy/pandas — NO TA libraries.
"""

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, timedelta, timezone

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
TRADING_DAYS = 245  # PSX trading days per year


def get_eod_features(symbol: str, lookback_days: int = 500) -> pd.DataFrame:
    """
    Extract EOD features for a symbol from DuckDB.
    
    Returns DataFrame with columns:
      date, close, volume, returns, plus computed features
    """
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    df = con.execute("""
        SELECT date, open, high, low, close, volume
        FROM eod_ohlcv
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT ?
    """, [symbol, lookback_days]).df()
    
    con.close()
    
    if df.empty:
        return df
    
    df = df.sort_values("date").reset_index(drop=True)
    
    # ── Price features ──
    df["returns"] = df["close"].pct_change()
    df["log_returns"] = np.log(df["close"] / df["close"].shift(1))
    df["range_pct"] = (df["high"] - df["low"]) / df["close"]
    df["body_pct"] = abs(df["close"] - df["open"]) / df["close"]
    df["upper_shadow"] = (df["high"] - df[["close", "open"]].max(axis=1)) / df["close"]
    df["lower_shadow"] = (df[["close", "open"]].min(axis=1) - df["low"]) / df["close"]
    df["gap"] = (df["open"] - df["close"].shift(1)) / df["close"].shift(1)
    
    # ── Moving averages ──
    for window in [5, 10, 20, 50, 100, 200]:
        df[f"sma_{window}"] = df["close"].rolling(window).mean()
        df[f"close_vs_sma_{window}"] = (df["close"] - df[f"sma_{window}"]) / df[f"sma_{window}"]
    
    # ── EMA ──
    for window in [12, 26]:
        df[f"ema_{window}"] = df["close"].ewm(span=window, adjust=False).mean()
    
    # MACD
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    
    # ── RSI ──
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi_14"] = 100 - (100 / (1 + rs))
    
    # ── Bollinger Bands ──
    bb_sma = df["close"].rolling(20).mean()
    bb_std = df["close"].rolling(20).std()
    df["bb_upper"] = bb_sma + 2 * bb_std
    df["bb_lower"] = bb_sma - 2 * bb_std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / bb_sma
    df["bb_position"] = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])
    
    # ── Volatility ──
    for window in [5, 10, 20, 60]:
        df[f"vol_{window}d"] = df["log_returns"].rolling(window).std() * np.sqrt(TRADING_DAYS)
    
    df["vol_ratio"] = df["vol_5d"] / df["vol_20d"].replace(0, np.nan)
    
    # ── Volume features ──
    df["vol_sma_20"] = df["volume"].rolling(20).mean()
    df["vol_ratio_20"] = df["volume"] / df["vol_sma_20"].replace(0, np.nan)
    df["vol_change"] = df["volume"].pct_change()
    
    # ── Momentum ──
    for period in [1, 5, 10, 20, 60]:
        df[f"mom_{period}d"] = df["close"].pct_change(period)
    
    # ── Hurst exponent (simplified R/S) ──
    def hurst_rs(series, window=100):
        """Simplified Hurst via R/S analysis."""
        result = np.full(len(series), np.nan)
        for i in range(window, len(series)):
            s = series.iloc[i-window:i].values
            mean = np.mean(s)
            deviate = np.cumsum(s - mean)
            R = np.max(deviate) - np.min(deviate)
            S = np.std(s, ddof=1)
            if S > 0:
                result[i] = np.log(R / S) / np.log(window)
        return result
    
    df["hurst"] = hurst_rs(df["log_returns"], window=100)
    
    # ── Mean reversion signals ──
    df["dist_from_52w_high"] = df["close"] / df["high"].rolling(TRADING_DAYS).max() - 1
    df["dist_from_52w_low"] = df["close"] / df["low"].rolling(TRADING_DAYS).min() - 1
    
    # ── SMA crossover signals ──
    df["sma_20_50_cross"] = np.where(df["sma_20"] > df["sma_50"], 1, -1)
    df["sma_50_200_cross"] = np.where(df["sma_50"] > df["sma_200"], 1, -1)
    
    # ── Day of week ──
    df["day_of_week"] = pd.to_datetime(df["date"]).dt.dayofweek
    
    return df


def get_tick_features(symbol: str, date_str: str) -> dict:
    """
    Extract tick-level features for a symbol on a specific date.
    Returns a dict of features to be merged with EOD data.
    """
    try:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        
        df = con.execute("""
            SELECT price, volume, bid, ask, "bidVol", "askVol", 
                   change, trades, timestamp
            FROM tick_logs
            WHERE symbol = ? AND date = ?
            ORDER BY timestamp
        """, [symbol, date_str]).df()
        
        con.close()
        
        if df.empty or len(df) < 10:
            return {}
        
        features = {}
        
        # Tick count
        features["tick_count"] = len(df)
        
        # VPIN (simplified)
        n_buckets = min(50, len(df) // 10)
        if n_buckets > 5:
            bucket_size = len(df) // n_buckets
            buy_vol = 0
            sell_vol = 0
            for i in range(n_buckets):
                chunk = df.iloc[i*bucket_size:(i+1)*bucket_size]
                price_change = chunk["price"].iloc[-1] - chunk["price"].iloc[0]
                vol = chunk["volume"].iloc[-1] - chunk["volume"].iloc[0] if len(chunk) > 1 else 0
                if vol < 0: vol = 0
                if price_change >= 0:
                    buy_vol += vol
                else:
                    sell_vol += vol
            total = buy_vol + sell_vol
            features["vpin"] = abs(buy_vol - sell_vol) / total if total > 0 else 0
        
        # Bid-ask spread
        spreads = df["ask"] - df["bid"]
        spreads = spreads[spreads > 0]
        if len(spreads) > 0:
            features["median_spread"] = spreads.median()
            features["mean_spread"] = spreads.mean()
        
        # Order flow imbalance
        if "bidVol" in df.columns and "askVol" in df.columns:
            bid_v = df["bidVol"].dropna()
            ask_v = df["askVol"].dropna()
            if len(bid_v) > 0 and len(ask_v) > 0:
                total_bid = bid_v.sum()
                total_ask = ask_v.sum()
                features["ofi"] = (total_bid - total_ask) / (total_bid + total_ask) if (total_bid + total_ask) > 0 else 0
        
        # Price volatility intraday
        if len(df) > 1:
            intraday_returns = df["price"].pct_change().dropna()
            features["intraday_vol"] = intraday_returns.std()
            features["intraday_skew"] = intraday_returns.skew()
            features["intraday_kurt"] = intraday_returns.kurtosis()
        
        return features
    
    except Exception:
        return {}


def build_dataset(symbols: list[str] = None, 
                  lookback_days: int = 500,
                  target_horizon: int = 1,
                  include_ticks: bool = False) -> pd.DataFrame:
    """
    Build ML training dataset for multiple symbols.
    
    Args:
        symbols: list of symbols (None = top 50 by volume)
        lookback_days: days of history per symbol
        target_horizon: predict N-day forward return
        include_ticks: include tick-level features (slower)
    
    Returns:
        DataFrame with features + target column
    """
    if symbols is None:
        # Get top 50 by recent volume
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        symbols = [r[0] for r in con.execute("""
            SELECT symbol FROM eod_ohlcv
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY symbol
            ORDER BY SUM(volume) DESC
            LIMIT 50
        """).fetchall()]
        con.close()
    
    all_data = []
    
    for sym in symbols:
        df = get_eod_features(sym, lookback_days)
        if df.empty or len(df) < 100:
            continue
        
        # Target: N-day forward return
        df["target"] = df["close"].shift(-target_horizon) / df["close"] - 1
        
        # Binary target: 1 = positive return, 0 = negative
        df["target_direction"] = (df["target"] > 0).astype(int)
        
        # Symbol identifier
        df["symbol"] = sym
        
        # Add tick features if requested
        if include_ticks:
            for _, row in df.iterrows():
                tick_feats = get_tick_features(sym, str(row["date"]))
                for k, v in tick_feats.items():
                    df.loc[df["date"] == row["date"], k] = v
        
        all_data.append(df)
    
    if not all_data:
        return pd.DataFrame()
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # Drop rows with NaN target (last N rows per symbol)
    combined = combined.dropna(subset=["target"])
    
    return combined


# Feature columns for ML (exclude date, symbol, target, raw OHLCV)
FEATURE_COLS = [
    "returns", "log_returns", "range_pct", "body_pct", "upper_shadow", "lower_shadow", "gap",
    "close_vs_sma_5", "close_vs_sma_10", "close_vs_sma_20", "close_vs_sma_50", 
    "close_vs_sma_100", "close_vs_sma_200",
    "macd", "macd_signal", "macd_hist",
    "rsi_14", "bb_width", "bb_position",
    "vol_5d", "vol_10d", "vol_20d", "vol_60d", "vol_ratio",
    "vol_ratio_20", "vol_change",
    "mom_1d", "mom_5d", "mom_10d", "mom_20d", "mom_60d",
    "hurst", "dist_from_52w_high", "dist_from_52w_low",
    "sma_20_50_cross", "sma_50_200_cross", "day_of_week",
]

TICK_FEATURE_COLS = [
    "tick_count", "vpin", "median_spread", "mean_spread", 
    "ofi", "intraday_vol", "intraday_skew", "intraday_kurt",
]
```

## Step 3: Create the ML model engine

Create `src/pakfindata/engine/ml_model.py`:

```python
"""
ML Model for PSX Price Prediction.

Supports:
  - XGBoost (default, best for tabular data)
  - LightGBM (faster training)
  - Random Forest (baseline)

Walk-forward validation with expanding window.
"""

import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from datetime import datetime
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler

MODEL_DIR = Path.home() / "pakfindata" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    model_type: str = "xgboost",
    task: str = "classification",  # "classification" or "regression"
    **kwargs
) -> tuple:
    """
    Train a model. Returns (model, scaler).
    """
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train.fillna(0))
    
    if model_type == "xgboost":
        import xgboost as xgb
        
        if task == "classification":
            model = xgb.XGBClassifier(
                n_estimators=kwargs.get("n_estimators", 500),
                max_depth=kwargs.get("max_depth", 6),
                learning_rate=kwargs.get("learning_rate", 0.05),
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_weight=5,
                reg_alpha=0.1,
                reg_lambda=1.0,
                random_state=42,
                tree_method="hist",  # Use GPU if available: "gpu_hist"
                eval_metric="logloss",
                early_stopping_rounds=50,
                n_jobs=-1,
            )
        else:
            model = xgb.XGBRegressor(
                n_estimators=kwargs.get("n_estimators", 500),
                max_depth=kwargs.get("max_depth", 6),
                learning_rate=kwargs.get("learning_rate", 0.05),
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                tree_method="hist",
                n_jobs=-1,
            )
    
    elif model_type == "lightgbm":
        import lightgbm as lgb
        
        if task == "classification":
            model = lgb.LGBMClassifier(
                n_estimators=kwargs.get("n_estimators", 500),
                max_depth=kwargs.get("max_depth", 6),
                learning_rate=kwargs.get("learning_rate", 0.05),
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_samples=20,
                random_state=42,
                n_jobs=-1,
                verbose=-1,
            )
        else:
            model = lgb.LGBMRegressor(
                n_estimators=kwargs.get("n_estimators", 500),
                max_depth=kwargs.get("max_depth", 6),
                learning_rate=kwargs.get("learning_rate", 0.05),
                random_state=42,
                n_jobs=-1,
                verbose=-1,
            )
    
    elif model_type == "random_forest":
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        
        if task == "classification":
            model = RandomForestClassifier(
                n_estimators=kwargs.get("n_estimators", 300),
                max_depth=kwargs.get("max_depth", 10),
                min_samples_leaf=10,
                random_state=42,
                n_jobs=-1,
            )
        else:
            model = RandomForestRegressor(
                n_estimators=kwargs.get("n_estimators", 300),
                max_depth=kwargs.get("max_depth", 10),
                random_state=42,
                n_jobs=-1,
            )
    
    # Train with early stopping for XGBoost
    if model_type == "xgboost" and len(X_scaled) > 200:
        split_idx = int(len(X_scaled) * 0.85)
        model.fit(
            X_scaled[:split_idx], y_train.iloc[:split_idx],
            eval_set=[(X_scaled[split_idx:], y_train.iloc[split_idx:])],
            verbose=False
        )
    else:
        model.fit(X_scaled, y_train)
    
    return model, scaler


def walk_forward_validate(
    df: pd.DataFrame,
    feature_cols: list,
    target_col: str = "target_direction",
    model_type: str = "xgboost",
    n_splits: int = 5,
    train_min_size: int = 200,
) -> dict:
    """
    Walk-forward validation — train on past, predict future.
    Returns performance metrics per fold and overall.
    """
    df = df.dropna(subset=feature_cols + [target_col]).reset_index(drop=True)
    
    if len(df) < train_min_size + 50:
        return {"error": f"Not enough data: {len(df)} rows, need {train_min_size + 50}"}
    
    tscv = TimeSeriesSplit(n_splits=n_splits)
    
    all_preds = []
    all_actuals = []
    all_probs = []
    fold_results = []
    
    for fold, (train_idx, test_idx) in enumerate(tscv.split(df)):
        if len(train_idx) < train_min_size:
            continue
        
        X_train = df.loc[train_idx, feature_cols]
        y_train = df.loc[train_idx, target_col]
        X_test = df.loc[test_idx, feature_cols]
        y_test = df.loc[test_idx, target_col]
        
        model, scaler = train_model(X_train, y_train, model_type=model_type)
        
        X_test_scaled = scaler.transform(X_test.fillna(0))
        preds = model.predict(X_test_scaled)
        probs = model.predict_proba(X_test_scaled)[:, 1] if hasattr(model, "predict_proba") else preds
        
        acc = accuracy_score(y_test, preds)
        prec = precision_score(y_test, preds, zero_division=0)
        rec = recall_score(y_test, preds, zero_division=0)
        f1 = f1_score(y_test, preds, zero_division=0)
        
        fold_results.append({
            "fold": fold + 1,
            "train_size": len(train_idx),
            "test_size": len(test_idx),
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1": f1,
        })
        
        all_preds.extend(preds)
        all_actuals.extend(y_test.values)
        all_probs.extend(probs)
    
    # Overall metrics
    if len(all_preds) > 0:
        overall = {
            "accuracy": accuracy_score(all_actuals, all_preds),
            "precision": precision_score(all_actuals, all_preds, zero_division=0),
            "recall": recall_score(all_actuals, all_preds, zero_division=0),
            "f1": f1_score(all_actuals, all_preds, zero_division=0),
            "auc": roc_auc_score(all_actuals, all_probs) if len(set(all_actuals)) > 1 else 0,
            "total_predictions": len(all_preds),
        }
    else:
        overall = {"error": "No valid folds"}
    
    return {
        "overall": overall,
        "folds": fold_results,
        "predictions": all_preds,
        "actuals": all_actuals,
        "probabilities": all_probs,
    }


def get_feature_importance(model, feature_cols: list) -> pd.DataFrame:
    """Extract feature importance from trained model."""
    if hasattr(model, "feature_importances_"):
        imp = model.feature_importances_
    else:
        imp = np.zeros(len(feature_cols))
    
    return pd.DataFrame({
        "feature": feature_cols,
        "importance": imp
    }).sort_values("importance", ascending=False)


def save_model(model, scaler, metadata: dict, name: str = "latest"):
    """Save trained model + scaler + metadata."""
    path = MODEL_DIR / f"{name}.pkl"
    with open(path, "wb") as f:
        pickle.dump({"model": model, "scaler": scaler, "metadata": metadata}, f)
    return path


def load_model(name: str = "latest"):
    """Load a saved model."""
    path = MODEL_DIR / f"{name}.pkl"
    if not path.exists():
        return None
    with open(path, "rb") as f:
        return pickle.load(f)
```

## Step 4: Create the Streamlit page

Create `src/pakfindata/ui/page_views/ml_predictions.py`:

This page should have these sections:

### Tab 1: Train & Validate
```
Model Configuration
├── Model type: [XGBoost | LightGBM | Random Forest]
├── Target: [Next-day direction | 5-day return | 10-day return]
├── Symbols: [Top 50 by volume | Custom list | Single symbol]
├── Training period: [1 year | 2 years | All available]
├── Include tick features: [Yes | No] (slower but better)
└── [Train Model] button

Walk-Forward Validation Results
├── Overall metrics: Accuracy, Precision, Recall, F1, AUC-ROC
├── Per-fold results table
├── Equity curve chart (cumulative returns if following predictions)
└── Confusion matrix
```

### Tab 2: Live Predictions
```
Today's Predictions
├── Run predictions for all symbols using latest trained model
├── Table: Symbol | Prediction | Probability | Signal Strength
├── Sort by probability (highest conviction first)
├── Color: Green (buy) / Red (sell) / Gray (neutral)
├── Merge with Signal Analysis composite score for confluence
└── [Refresh Predictions] button
```

### Tab 3: Feature Importance
```
Feature Analysis
├── Bar chart: top 20 features by importance
├── SHAP values (if shap installed)
├── Feature correlation heatmap
└── Feature distribution for buy vs sell predictions
```

### Tab 4: Backtest
```
Strategy Backtest
├── Strategy: Go long when P(up) > threshold
├── Threshold slider: 0.5 to 0.8
├── Position sizing: Equal weight / Probability-weighted
├── Results: Total return, Sharpe ratio, max drawdown, win rate
├── Equity curve chart vs buy-and-hold
└── Monthly returns heatmap
```

### Implementation notes for the Streamlit page:

```python
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pakfindata.engine.ml_features import (
    build_dataset, get_eod_features, FEATURE_COLS, TICK_FEATURE_COLS
)
from pakfindata.engine.ml_model import (
    train_model, walk_forward_validate, get_feature_importance,
    save_model, load_model
)

def render_page():
    st.title("🤖 ML Price Predictions")
    st.caption("XGBoost/LightGBM direction prediction using 40+ technical + microstructure features")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Train & Validate", "Live Predictions", "Feature Importance", "Backtest"
    ])
    
    with tab1:
        render_train_tab()
    
    with tab2:
        render_predictions_tab()
    
    with tab3:
        render_features_tab()
    
    with tab4:
        render_backtest_tab()
```

Key UI elements:
- Use `st.progress()` during training (shows % complete)
- Cache the dataset with `@st.cache_data(ttl=3600)`
- Cache the model with `@st.cache_resource`
- Show training time ("Trained in 4.2 seconds on 25,000 samples")
- Feature importance as horizontal Plotly bar chart (gold bars on dark bg)
- Equity curve as Plotly line chart
- Confusion matrix as Plotly heatmap
- Live predictions table with `st.dataframe()` + conditional formatting

## Step 5: Add page to sidebar

In `app.py`, add under RESEARCH section:

```python
st.page_link("page_views/ml_predictions.py", label="ML Predictions", icon="🤖")
```

## Step 6: Install dependencies

```bash
conda activate psx
pip install xgboost lightgbm shap --break-system-packages
```

## Step 7: Test

```bash
# Test feature engineering
python -c "
from pakfindata.engine.ml_features import build_dataset, FEATURE_COLS
df = build_dataset(symbols=['HUBC'], lookback_days=500)
print(f'Dataset: {len(df)} rows, {len(FEATURE_COLS)} features')
print(f'Target balance: {df[\"target_direction\"].value_counts().to_dict()}')
print(f'Date range: {df[\"date\"].min()} → {df[\"date\"].max()}')
print(f'NaN ratio: {df[FEATURE_COLS].isna().mean().mean():.2%}')
"

# Test model training
python -c "
from pakfindata.engine.ml_features import build_dataset, FEATURE_COLS
from pakfindata.engine.ml_model import walk_forward_validate
import time

df = build_dataset(symbols=['HUBC','OGDC','PPL','HBL','UBL'], lookback_days=500)
print(f'Dataset: {len(df)} rows')

start = time.time()
results = walk_forward_validate(df, FEATURE_COLS, model_type='xgboost')
elapsed = time.time() - start

print(f'Training time: {elapsed:.1f}s')
print(f'Overall accuracy: {results[\"overall\"][\"accuracy\"]:.2%}')
print(f'Overall AUC: {results[\"overall\"][\"auc\"]:.3f}')
print(f'Overall F1: {results[\"overall\"][\"f1\"]:.3f}')
"
```

## IMPORTANT NOTES

1. **All computation in numpy/pandas** — NO TA-Lib or other external TA libraries
2. **Walk-forward validation only** — never train on future data
3. **XGBoost tree_method="hist"** — use "gpu_hist" if CUDA is configured
4. **40+ features** from EOD data alone, 48+ with tick features
5. **Target = next-day return direction** (binary classification, easier than regression)
6. **Cache aggressively** — dataset build takes 5-10 seconds, cache for 1 hour
7. **Model saves to ~/pakfindata/models/** — persists across sessions
8. **Live predictions merge with Signal Analysis score** — show both ML probability + composite score for confluence
9. **DO NOT use future-looking features** — all features use data up to date T only
10. **PSX has 539 symbols but only ~200 are liquid** — default to top 50 by volume
11. **Baseline accuracy ~52-55%** — anything above 55% with walk-forward is excellent for PSX
12. **Add the page to sidebar under RESEARCH section** — alongside Signal Analysis, Microstructure, etc.
