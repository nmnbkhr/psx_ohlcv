"""
Cross-Asset Macro Regime Model using Hidden Markov Models.

Observes 4 signals: KSE-100 momentum, KIBOR direction, PKR/USD trend, SBP cycle.
Infers 4 regimes: RISK_ON, TRANSITION, RISK_OFF, CRISIS.
Allocates: equity/bonds/cash per regime.

Model saves to ~/pakfindata/models/hmm_regime.pkl
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import sqlite3
import joblib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from enum import Enum

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
PSX_SQLITE = Path("/home/smnb/psxdata_rescue/psx.sqlite")
MODEL_DIR = Path.home() / "pakfindata" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = MODEL_DIR / "hmm_regime.pkl"
TRADING_DAYS = 245


class MacroRegime(Enum):
    RISK_ON = 0
    TRANSITION = 1
    RISK_OFF = 2
    CRISIS = 3


REGIME_ALLOCATION = {
    MacroRegime.RISK_ON:    {"equity": 0.80, "bonds": 0.10, "cash": 0.10},
    MacroRegime.TRANSITION: {"equity": 0.40, "bonds": 0.30, "cash": 0.30},
    MacroRegime.RISK_OFF:   {"equity": 0.20, "bonds": 0.50, "cash": 0.30},
    MacroRegime.CRISIS:     {"equity": 0.00, "bonds": 0.30, "cash": 0.70},
}

REGIME_COLORS = {
    MacroRegime.RISK_ON: "#22C55E",
    MacroRegime.TRANSITION: "#EAB308",
    MacroRegime.RISK_OFF: "#F97316",
    MacroRegime.CRISIS: "#EF4444",
}

REGIME_NAMES = {r: r.name.replace("_", " ").title() for r in MacroRegime}


@dataclass
class RegimeState:
    date: str
    regime: str
    probability: float
    regime_probs: dict
    kse_momentum: float
    kibor_change: float
    pkr_change: float
    sbp_cycle: str
    allocation: dict

    def to_dict(self):
        return asdict(self)


# ═══════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════

def load_macro_features() -> pd.DataFrame:
    """Load and merge macro features into a monthly DataFrame."""
    scon = sqlite3.connect(str(PSX_SQLITE), timeout=10)
    scon.row_factory = sqlite3.Row

    # 1. KSE-100 from eod_ohlcv (use OGDC as proxy if no index)
    dcon = analytics_con()
    kse = dcon.execute("""
        SELECT SUBSTR(date,1,7) AS month,
               LAST(close ORDER BY date) AS kse_close,
               MAX(close) AS kse_high, MIN(close) AS kse_low,
               SUM(volume) AS kse_volume
        FROM eod_ohlcv WHERE symbol = 'OGDC'
        GROUP BY month ORDER BY month
    """).df()
    dcon.close()

    if kse.empty:
        return pd.DataFrame()

    kse["kse_return"] = kse["kse_close"].pct_change()
    kse["kse_mom_3m"] = kse["kse_close"].pct_change(3)
    kse["kse_vol"] = kse["kse_return"].rolling(6).std()

    # 2. KIBOR 3M
    kibor = pd.read_sql_query(
        """SELECT SUBSTR(date,1,7) AS month, AVG(offer) AS kibor_3m
           FROM kibor_daily WHERE tenor='3M' AND offer > 0
           GROUP BY month ORDER BY month""",
        scon,
    )
    kibor["kibor_change"] = kibor["kibor_3m"].diff()
    kibor["kibor_direction"] = np.where(kibor["kibor_change"] > 0.1, 1, np.where(kibor["kibor_change"] < -0.1, -1, 0))

    # 3. PKR/USD — use daily avg (EasyData historical) with interbank fallback
    fx = pd.read_sql_query(
        """SELECT SUBSTR(date,1,7) AS month, AVG(avg_rate) AS pkr_usd
           FROM sbp_fx_daily_avg WHERE currency='USD' AND avg_rate > 0
           GROUP BY month ORDER BY month""",
        scon,
    )
    if fx.empty:
        fx = pd.read_sql_query(
            """SELECT SUBSTR(date,1,7) AS month, AVG(mid) AS pkr_usd
               FROM sbp_fx_interbank WHERE currency='USD' AND mid > 0
               GROUP BY month ORDER BY month""",
            scon,
        )
    fx["pkr_change"] = fx["pkr_usd"].pct_change()

    # 4. SBP Policy Rate
    policy = pd.read_sql_query(
        """SELECT SUBSTR(rate_date,1,7) AS month, MAX(policy_rate) AS policy_rate
           FROM sbp_policy_rates WHERE policy_rate > 0
           GROUP BY month ORDER BY month""",
        scon,
    )
    policy["rate_change"] = policy["policy_rate"].diff()
    policy["sbp_cycle"] = np.where(policy["rate_change"] > 0, "TIGHTENING",
                                    np.where(policy["rate_change"] < 0, "EASING", "HOLD"))

    scon.close()

    # Merge all on month
    df = kse[["month", "kse_close", "kse_return", "kse_mom_3m", "kse_vol"]].copy()
    df = df.merge(kibor[["month", "kibor_3m", "kibor_change", "kibor_direction"]], on="month", how="left")
    df = df.merge(fx[["month", "pkr_usd", "pkr_change"]], on="month", how="left")
    df = df.merge(policy[["month", "policy_rate", "sbp_cycle"]], on="month", how="left")

    df = df.dropna(subset=["kse_return", "kibor_3m"]).reset_index(drop=True)
    df["sbp_cycle"] = df["sbp_cycle"].fillna("HOLD")

    # Forward fill remaining NaN
    df = df.ffill().bfill()

    return df


# ═══════════════════════════════════════════════════════
# HMM TRAINING
# ═══════════════════════════════════════════════════════

def train_hmm(df: pd.DataFrame, n_regimes: int = 4, n_iter: int = 100) -> dict:
    """Train Gaussian HMM on macro features. Returns model dict."""
    from hmmlearn.hmm import GaussianHMM

    feature_cols = ["kse_return", "kibor_change", "pkr_change", "kse_vol"]
    X = df[feature_cols].values

    # Standardize
    mu = X.mean(axis=0)
    sigma = X.std(axis=0)
    sigma[sigma == 0] = 1
    X_scaled = (X - mu) / sigma

    model = GaussianHMM(
        n_components=n_regimes,
        covariance_type="full",
        n_iter=n_iter,
        random_state=42,
        tol=0.01,
    )
    model.fit(X_scaled)

    # Predict states
    states = model.predict(X_scaled)
    probs = model.predict_proba(X_scaled)

    # Label regimes by average KSE return per state
    state_returns = {}
    for s in range(n_regimes):
        mask = states == s
        if mask.sum() > 0:
            state_returns[s] = df.loc[mask, "kse_return"].mean()
        else:
            state_returns[s] = 0

    # Sort: highest return = RISK_ON (0), lowest = CRISIS (3)
    sorted_states = sorted(state_returns.keys(), key=lambda s: state_returns[s], reverse=True)
    label_map = {sorted_states[i]: i for i in range(min(n_regimes, 4))}

    # Map states to regime labels
    labeled_states = np.array([label_map.get(s, 1) for s in states])

    result = {
        "model": model,
        "mu": mu,
        "sigma": sigma,
        "feature_cols": feature_cols,
        "label_map": label_map,
        "n_regimes": n_regimes,
        "trained_at": datetime.now(PKT).isoformat(),
        "months_trained": len(df),
        "log_likelihood": float(model.score(X_scaled)),
        "transition_matrix": model.transmat_.tolist(),
    }

    return result


def save_hmm(model_dict: dict) -> Path:
    """Save trained HMM to disk."""
    joblib.dump(model_dict, MODEL_PATH)
    return MODEL_PATH


def load_hmm() -> dict | None:
    """Load trained HMM from disk."""
    if MODEL_PATH.exists():
        return joblib.load(MODEL_PATH)
    return None


# ═══════════════════════════════════════════════════════
# PREDICTION
# ═══════════════════════════════════════════════════════

def predict_regime(model_dict: dict, df: pd.DataFrame) -> pd.DataFrame:
    """Predict regimes for a features DataFrame."""
    model = model_dict["model"]
    mu = model_dict["mu"]
    sigma = model_dict["sigma"]
    feature_cols = model_dict["feature_cols"]
    label_map = model_dict["label_map"]

    X = df[feature_cols].values
    X_scaled = (X - mu) / sigma

    raw_states = model.predict(X_scaled)
    probs = model.predict_proba(X_scaled)

    labeled = np.array([label_map.get(s, 1) for s in raw_states])
    regime_names = {0: "RISK_ON", 1: "TRANSITION", 2: "RISK_OFF", 3: "CRISIS"}

    df = df.copy()
    df["regime_id"] = labeled
    df["regime"] = [regime_names.get(r, "TRANSITION") for r in labeled]
    df["regime_prob"] = [float(probs[i, raw_states[i]]) for i in range(len(raw_states))]

    for r_id, r_name in regime_names.items():
        # Find which raw state maps to this regime
        raw_for_regime = [k for k, v in label_map.items() if v == r_id]
        if raw_for_regime:
            df[f"prob_{r_name.lower()}"] = probs[:, raw_for_regime[0]]
        else:
            df[f"prob_{r_name.lower()}"] = 0.0

    return df


# ═══════════════════════════════════════════════════════
# BACKTEST
# ═══════════════════════════════════════════════════════

def backtest_regime_allocation(df: pd.DataFrame) -> dict:
    """Backtest regime-based allocation vs buy-and-hold."""
    if "regime_id" not in df.columns or "kse_return" not in df.columns:
        return {"error": "Need regime predictions first"}

    df = df.copy()
    alloc_map = {0: 0.80, 1: 0.40, 2: 0.20, 3: 0.00}

    df["equity_alloc"] = df["regime_id"].map(alloc_map)
    df["strategy_return"] = df["equity_alloc"].shift(1).fillna(0.4) * df["kse_return"]
    df["bh_return"] = df["kse_return"]

    df["strategy_equity"] = (1 + df["strategy_return"]).cumprod()
    df["bh_equity"] = (1 + df["bh_return"]).cumprod()

    strat_total = float(df["strategy_equity"].iloc[-1] - 1)
    bh_total = float(df["bh_equity"].iloc[-1] - 1)

    strat_vol = float(df["strategy_return"].std() * np.sqrt(12))
    bh_vol = float(df["bh_return"].std() * np.sqrt(12))

    strat_sharpe = float(df["strategy_return"].mean() / df["strategy_return"].std() * np.sqrt(12)) if df["strategy_return"].std() > 0 else 0
    bh_sharpe = float(df["bh_return"].mean() / df["bh_return"].std() * np.sqrt(12)) if df["bh_return"].std() > 0 else 0

    strat_dd = float((df["strategy_equity"] / df["strategy_equity"].cummax() - 1).min())
    bh_dd = float((df["bh_equity"] / df["bh_equity"].cummax() - 1).min())

    # Regime durations
    durations = {}
    for r_id in range(4):
        mask = df["regime_id"] == r_id
        durations[MacroRegime(r_id).name] = int(mask.sum())

    return {
        "df": df,
        "metrics": {
            "strategy_return": strat_total,
            "bh_return": bh_total,
            "alpha": strat_total - bh_total,
            "strategy_sharpe": strat_sharpe,
            "bh_sharpe": bh_sharpe,
            "strategy_vol": strat_vol,
            "bh_vol": bh_vol,
            "strategy_max_dd": strat_dd,
            "bh_max_dd": bh_dd,
            "months": len(df),
        },
        "regime_durations": durations,
        "transition_matrix": df.groupby(["regime_id", df["regime_id"].shift(1)]).size().unstack(fill_value=0).values.tolist() if len(df) > 1 else [],
    }


# ═══════════════════════════════════════════════════════
# FULL PIPELINE
# ═══════════════════════════════════════════════════════

def train_and_save() -> dict:
    """Train HMM on all available data and save model."""
    df = load_macro_features()
    if len(df) < 24:
        return {"error": f"Not enough data ({len(df)} months)"}

    model_dict = train_hmm(df)
    path = save_hmm(model_dict)

    # Predict on training data for analysis
    pred_df = predict_regime(model_dict, df)
    bt = backtest_regime_allocation(pred_df)

    return {
        "model_path": str(path),
        "months_trained": len(df),
        "log_likelihood": model_dict["log_likelihood"],
        "trained_at": model_dict["trained_at"],
        "current_regime": pred_df["regime"].iloc[-1],
        "current_probs": {
            "RISK_ON": float(pred_df["prob_risk_on"].iloc[-1]),
            "TRANSITION": float(pred_df["prob_transition"].iloc[-1]),
            "RISK_OFF": float(pred_df["prob_risk_off"].iloc[-1]),
            "CRISIS": float(pred_df["prob_crisis"].iloc[-1]),
        },
        "backtest": bt["metrics"] if "metrics" in bt else {},
        "regime_durations": bt.get("regime_durations", {}),
        "pred_df": pred_df,
    }


def get_current_regime() -> dict | None:
    """Load saved model and predict current regime."""
    model_dict = load_hmm()
    if not model_dict:
        return None

    df = load_macro_features()
    if df.empty:
        return None

    pred_df = predict_regime(model_dict, df)
    latest = pred_df.iloc[-1]

    regime = MacroRegime(int(latest["regime_id"]))
    allocation = REGIME_ALLOCATION[regime]

    return {
        "date": str(latest["month"]),
        "regime": regime.name,
        "probability": float(latest["regime_prob"]),
        "probs": {
            "RISK_ON": float(latest.get("prob_risk_on", 0)),
            "TRANSITION": float(latest.get("prob_transition", 0)),
            "RISK_OFF": float(latest.get("prob_risk_off", 0)),
            "CRISIS": float(latest.get("prob_crisis", 0)),
        },
        "allocation": allocation,
        "kse_momentum": float(latest.get("kse_mom_3m", 0)),
        "kibor": float(latest.get("kibor_3m", 0)),
        "pkr_usd": float(latest.get("pkr_usd", 0)),
        "sbp_cycle": str(latest.get("sbp_cycle", "HOLD")),
        "model_trained": model_dict.get("trained_at", "unknown"),
    }
