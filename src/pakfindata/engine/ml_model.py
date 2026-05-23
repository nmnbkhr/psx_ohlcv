"""
ML Model for PSX Price Prediction.

Supports:
  - XGBoost (default, best for tabular data)
  - LightGBM (faster training)
  - Random Forest (baseline)

Walk-forward validation with expanding window.
"""

import json
import os
import numpy as np
import pandas as pd
import pickle
from pathlib import Path

from scipy import stats as sp_stats
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
    _sample_preds = []  # per-sample predictions for credibility bootstrap

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

        # Per-sample predictions for credibility bootstrapping
        test_rows = df.iloc[test_idx]
        for j in range(len(preds)):
            row = test_rows.iloc[j]
            prob_up = float(probs[j])
            _sample_preds.append({
                "symbol": str(row.get("symbol", "UNKNOWN")),
                "date": str(row.get("date", "")),
                "predicted_dir": "BUY" if preds[j] == 1 else "SELL",
                "prob": prob_up if prob_up > 0.5 else 1 - prob_up,
                "actual_dir": "BUY" if y_test.values[j] == 1 else "SELL",
                "correct": bool(preds[j] == y_test.values[j]),
                "fold": fold + 1,
                "source": "walk_forward_cv",
            })

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
        "sample_predictions": _sample_preds,
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


# ═══════════════════════════════════════════════════════
# EXPECTED VALUE — translate P(UP) into PKR move
# ═══════════════════════════════════════════════════════


def compute_expected_value(
    prob_up: float,
    current_price: float,
    hist_up_move_pct: float,
    hist_dn_move_pct: float,
    hist_volatility: float,
) -> dict:
    """Convert a direction probability into an expected PKR value.

    EV = P(UP) * avg_up_move - P(DOWN) * avg_down_move.
    A 60% UP prediction on a stock with +0.3% up / -1.5% down is NEGATIVE EV.

    Args:
        prob_up: model's P(next-day UP), 0.0 to 1.0
        current_price: today's closing price (PKR)
        hist_up_move_pct: avg % move on UP days (positive, e.g. 1.2)
        hist_dn_move_pct: avg % move on DOWN days (positive, e.g. 0.8)
        hist_volatility: annualized realized vol %
    """
    prob_dn = 1.0 - prob_up

    expected_return_pct = (prob_up * hist_up_move_pct) - (prob_dn * hist_dn_move_pct)
    expected_move_pkr = current_price * expected_return_pct / 100
    expected_price = current_price + expected_move_pkr

    upside_pkr = current_price * hist_up_move_pct / 100
    downside_pkr = current_price * hist_dn_move_pct / 100

    risk_reward = (
        (prob_up * upside_pkr) / (prob_dn * downside_pkr)
        if (prob_dn * downside_pkr) > 0 else 0
    )

    daily_vol = hist_volatility / np.sqrt(245) if hist_volatility > 0 else 1.0
    price_1sigma = current_price * daily_vol / 100

    return {
        "expected_return_pct": round(expected_return_pct, 3),
        "expected_price": round(expected_price, 2),
        "expected_move_pkr": round(expected_move_pkr, 2),
        "upside_pkr": round(upside_pkr, 2),
        "downside_pkr": round(downside_pkr, 2),
        "risk_reward": round(risk_reward, 2),
        "price_upper_1sigma": round(current_price + price_1sigma, 2),
        "price_lower_1sigma": round(current_price - price_1sigma, 2),
        "daily_vol_pct": round(daily_vol, 2),
    }


# ═══════════════════════════════════════════════════════
# BAYESIAN CREDIBILITY TRACKER
# ═══════════════════════════════════════════════════════

PREDICTION_LOG = Path("/mnt/e/psxdata/ml_prediction_log.json")


class BayesianCredibility:
    """Track model prediction accuracy with Bayesian updating.

    Uses Beta(alpha, beta) as conjugate prior for binomial success rate.
    Prior: Beta(1, 1) = uniform. After K correct out of N:
        Posterior: Beta(1 + K, 1 + N - K)
    """

    def __init__(self, log_path: Path = PREDICTION_LOG):
        self.log_path = log_path
        self.predictions: list[dict] = []
        self._load()

    def _load(self):
        if self.log_path.exists():
            try:
                data = json.loads(self.log_path.read_text())
                self.predictions = data.get("predictions", [])
            except (json.JSONDecodeError, IOError):
                self.predictions = []

    def _save(self):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(self.log_path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"predictions": self.predictions[-1000:]}, f, default=str)
        os.replace(tmp, str(self.log_path))

    def log_prediction(
        self, symbol: str, date: str, predicted_dir: str,
        prob: float, actual_dir: str = None,
        predicted_price: float = None, actual_price: float = None,
    ):
        """Log a prediction. Call with actual_dir=None first, update later."""
        entry = {
            "symbol": symbol, "date": date,
            "predicted_dir": predicted_dir, "prob": prob,
            "predicted_price": predicted_price,
            "actual_dir": actual_dir, "actual_price": actual_price,
            "correct": (predicted_dir == actual_dir) if actual_dir else None,
        }

        # Update existing entry if same symbol+date
        for i, p in enumerate(self.predictions):
            if p["symbol"] == symbol and p["date"] == date:
                self.predictions[i].update(entry)
                self._save()
                return

        self.predictions.append(entry)
        self._save()

    def update_actuals(self) -> int:
        """Resolve pending predictions from DuckDB eod_ohlcv. Returns count updated."""
        from pakfindata.db.connections import analytics_con

        pending = [p for p in self.predictions if p["correct"] is None]
        if not pending:
            return 0

        con = analytics_con()
        updated = 0

        for p in pending:
            try:
                rows = con.execute("""
                    SELECT close, LAG(close) OVER (ORDER BY date) as prev_close
                    FROM eod_ohlcv
                    WHERE symbol = ? AND date >= ?
                    ORDER BY date
                    LIMIT 2
                """, [p["symbol"], p["date"]]).fetchall()

                if len(rows) >= 2 and rows[1][1] is not None:
                    actual_close = rows[1][0]
                    prev_close = rows[1][1]
                    actual_dir = "BUY" if actual_close > prev_close else "SELL"
                    p["actual_dir"] = actual_dir
                    p["actual_price"] = actual_close
                    p["correct"] = (p["predicted_dir"] == actual_dir)
                    updated += 1
            except Exception:
                continue

        con.close()
        if updated > 0:
            self._save()
        return updated

    def seed_from_cv(self, sample_predictions: list[dict], replace: bool = False) -> int:
        """Seed tracker with walk-forward CV results for immediate credibility.

        Args:
            sample_predictions: from walk_forward_validate()["sample_predictions"]
            replace: if True, clear existing predictions first
        Returns:
            Number of predictions added.
        """
        if replace:
            self.predictions = []

        existing_keys = {(p["symbol"], p["date"]) for p in self.predictions}
        added = 0

        for sp in sample_predictions:
            key = (sp["symbol"], sp["date"])
            if key not in existing_keys:
                self.predictions.append({
                    "symbol": sp["symbol"],
                    "date": sp["date"],
                    "predicted_dir": sp["predicted_dir"],
                    "prob": sp["prob"],
                    "actual_dir": sp["actual_dir"],
                    "correct": sp["correct"],
                    "predicted_price": None,
                    "actual_price": None,
                    "source": "walk_forward_cv",
                })
                existing_keys.add(key)
                added += 1

        if added > 0:
            self._save()
        return added

    def get_credibility(self, symbol: str = None, last_n: int = None) -> dict:
        """Compute Bayesian credibility metrics."""
        resolved = [p for p in self.predictions if p["correct"] is not None]

        if symbol:
            resolved = [p for p in resolved if p["symbol"] == symbol]
        if last_n and len(resolved) > last_n:
            resolved = resolved[-last_n:]

        n = len(resolved)
        k = sum(1 for p in resolved if p["correct"])

        if n == 0:
            return {
                "n_predictions": 0, "n_correct": 0,
                "posterior_mean": 0.50,
                "credible_interval": (0.0, 1.0),
                "prob_better_than_random": 0.50,
                "prob_better_than_55": 0.25,
                "assessment": "NO_DATA",
                "assessment_text": "No resolved predictions yet.",
                "color": "#6B7280",
                "streak": 0, "streak_type": "none",
                "alpha": 1, "beta_param": 1,
            }

        alpha = 1 + k
        beta = 1 + n - k

        posterior_mean = alpha / (alpha + beta)
        ci_low = sp_stats.beta.ppf(0.05, alpha, beta)
        ci_high = sp_stats.beta.ppf(0.95, alpha, beta)
        prob_gt_50 = 1.0 - sp_stats.beta.cdf(0.50, alpha, beta)
        prob_gt_55 = 1.0 - sp_stats.beta.cdf(0.55, alpha, beta)
        prob_gt_60 = 1.0 - sp_stats.beta.cdf(0.60, alpha, beta)

        # Streak
        streak = 0
        if resolved:
            last_correct = resolved[-1]["correct"]
            for p in reversed(resolved):
                if p["correct"] == last_correct:
                    streak += 1
                else:
                    break
        streak_type = "win" if resolved and resolved[-1]["correct"] else "loss"

        # Assessment
        if n < 10:
            assessment, color = "INSUFFICIENT", "#6B7280"
            text = f"Only {n} resolved predictions. Need 10+ for meaningful assessment."
        elif prob_gt_50 < 0.70:
            assessment, color = "NOT_CREDIBLE", "#FF5252"
            text = f"Model likely NOT better than random. P(>50%) = {prob_gt_50:.0%}."
        elif prob_gt_55 < 0.50:
            assessment, color = "MARGINAL", "#FFB300"
            text = f"Model MAY be slightly better than random. Accuracy {posterior_mean:.1%}."
        elif prob_gt_60 < 0.30:
            assessment, color = "CREDIBLE", "#00E676"
            text = f"Model shows genuine edge. Accuracy {posterior_mean:.1%}, P(>55%) = {prob_gt_55:.0%}."
        else:
            assessment, color = "STRONG", "#00BCD4"
            text = f"Strong predictive power. Accuracy {posterior_mean:.1%}, P(>60%) = {prob_gt_60:.0%}."

        return {
            "n_predictions": n, "n_correct": k,
            "raw_accuracy": round(k / n, 4),
            "posterior_mean": round(posterior_mean, 4),
            "credible_interval": (round(ci_low, 3), round(ci_high, 3)),
            "prob_better_than_random": round(prob_gt_50, 3),
            "prob_better_than_55": round(prob_gt_55, 3),
            "prob_better_than_60": round(prob_gt_60, 3),
            "assessment": assessment, "assessment_text": text, "color": color,
            "streak": streak, "streak_type": streak_type,
            "alpha": alpha, "beta_param": beta,
        }

    def get_symbol_breakdown(self) -> pd.DataFrame:
        """Per-symbol credibility table."""
        resolved = [p for p in self.predictions if p["correct"] is not None]
        symbols = sorted(set(p["symbol"] for p in resolved))

        rows = []
        for sym in symbols:
            c = self.get_credibility(symbol=sym)
            rows.append({
                "Symbol": sym, "Predictions": c["n_predictions"],
                "Correct": c["n_correct"], "Accuracy": c["raw_accuracy"],
                "Bayesian": c["posterior_mean"],
                "P(>50%)": c["prob_better_than_random"],
                "Assessment": c["assessment"],
            })

        return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════
# REGRESSION MODEL — predict return MAGNITUDE
# ═══════════════════════════════════════════════════════


def train_regression_model(
    df: pd.DataFrame,
    feature_cols: list[str],
    target_col: str = "next_return_pct",
    model_type: str = "xgboost",
) -> tuple:
    """Train a regression model to predict return magnitude.

    Uses same features as classifier but predicts continuous returns.
    Returns (model, scaler, metrics_dict).
    """
    import xgboost as xgb
    from sklearn.metrics import mean_absolute_error, mean_squared_error

    df_clean = df.dropna(subset=feature_cols + [target_col]).copy()
    X = df_clean[feature_cols].values
    y = df_clean[target_col].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    tscv = TimeSeriesSplit(n_splits=5)
    fold_metrics = []
    all_sample_preds = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X_scaled)):
        X_train, X_test = X_scaled[train_idx], X_scaled[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        if model_type == "xgboost":
            reg = xgb.XGBRegressor(
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                tree_method="hist", random_state=42,
            )
        else:
            import lightgbm as lgb
            reg = lgb.LGBMRegressor(
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, verbose=-1,
            )

        reg.fit(X_train, y_train)
        y_pred = reg.predict(X_test)

        mae = mean_absolute_error(y_test, y_pred)
        rmse = float(mean_squared_error(y_test, y_pred) ** 0.5)
        dir_acc = float(((y_pred > 0) == (y_test > 0)).mean())

        fold_metrics.append({"fold": fold, "mae": mae, "rmse": rmse, "dir_accuracy": dir_acc})

        test_rows = df_clean.iloc[test_idx]
        for i, (_, row) in enumerate(test_rows.iterrows()):
            all_sample_preds.append({
                "symbol": str(row.get("symbol", "")),
                "date": str(row.get("date", "")),
                "predicted_return_pct": float(y_pred[i]),
                "actual_return_pct": float(y_test[i]),
            })

    # Final model on all data
    final_reg = xgb.XGBRegressor(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        tree_method="hist", random_state=42,
    ) if model_type == "xgboost" else lgb.LGBMRegressor(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1,
    )
    final_reg.fit(X_scaled, y)

    return final_reg, scaler, {
        "folds": fold_metrics,
        "overall_mae": float(np.mean([f["mae"] for f in fold_metrics])),
        "overall_rmse": float(np.mean([f["rmse"] for f in fold_metrics])),
        "overall_dir_acc": float(np.mean([f["dir_accuracy"] for f in fold_metrics])),
        "sample_predictions": all_sample_preds,
    }


def train_quantile_models(
    df: pd.DataFrame,
    feature_cols: list[str],
    target_col: str = "next_return_pct",
    quantiles: list[float] = None,
) -> dict:
    """Train XGBoost quantile regression for confidence intervals.

    Returns dict: {models: {q: model}, scaler, quantiles}.
    """
    import xgboost as xgb

    if quantiles is None:
        quantiles = [0.05, 0.25, 0.50, 0.75, 0.95]

    df_clean = df.dropna(subset=feature_cols + [target_col]).copy()
    X = df_clean[feature_cols].values
    y = df_clean[target_col].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    models = {}
    for q in quantiles:
        m = xgb.XGBRegressor(
            n_estimators=150, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            objective="reg:quantileerror", quantile_alpha=q,
            tree_method="hist", random_state=42,
        )
        m.fit(X_scaled, y)
        models[q] = m

    return {"models": models, "scaler": scaler, "quantiles": quantiles}
