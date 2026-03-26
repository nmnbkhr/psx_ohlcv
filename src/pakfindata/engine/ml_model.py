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
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
)
from sklearn.preprocessing import StandardScaler

MODEL_DIR = Path.home() / "pakfindata" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    model_type: str = "xgboost",
    task: str = "classification",
    **kwargs,
) -> tuple:
    """Train a model. Returns (model, scaler)."""
    scaler = StandardScaler()
    X_filled = X_train.fillna(0)
    X_filled = X_filled.map(
        lambda x: 0.0 if not np.isfinite(x) else max(min(x, 1e10), -1e10)
    )
    X_scaled = pd.DataFrame(scaler.fit_transform(X_filled),
                            columns=X_filled.columns, index=X_filled.index)

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
                tree_method="hist",
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
    else:
        raise ValueError(f"Unknown model_type: {model_type}")

    # Train with early stopping for XGBoost
    if model_type == "xgboost" and task == "classification" and len(X_scaled) > 200:
        split_idx = int(len(X_scaled) * 0.85)
        model.fit(
            X_scaled[:split_idx], y_train.iloc[:split_idx],
            eval_set=[(X_scaled[split_idx:], y_train.iloc[split_idx:])],
            verbose=False,
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
    Walk-forward validation -- train on past, predict future.
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

        X_test_filled = X_test.fillna(0)
        X_test_filled = X_test_filled.map(
            lambda x: 0.0 if not np.isfinite(x) else max(min(x, 1e10), -1e10)
        )
        X_test_scaled = pd.DataFrame(scaler.transform(X_test_filled),
                                     columns=X_test_filled.columns, index=X_test_filled.index)
        X_test_scaled = X_test_scaled.fillna(0).clip(-10, 10)
        preds = model.predict(X_test_scaled)
        probs = (
            model.predict_proba(X_test_scaled)[:, 1]
            if hasattr(model, "predict_proba")
            else preds.astype(float)
        )

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

    if len(all_preds) > 0:
        overall = {
            "accuracy": accuracy_score(all_actuals, all_preds),
            "precision": precision_score(all_actuals, all_preds, zero_division=0),
            "recall": recall_score(all_actuals, all_preds, zero_division=0),
            "f1": f1_score(all_actuals, all_preds, zero_division=0),
            "auc": (
                roc_auc_score(all_actuals, all_probs)
                if len(set(all_actuals)) > 1
                else 0.0
            ),
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
        "importance": imp,
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
