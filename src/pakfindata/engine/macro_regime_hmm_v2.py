"""
Cross-Asset Macro Regime Model v2 — Enhanced Hidden Markov Model.

Upgrades from v1:
- 5 hidden states: RISK_ON, RECOVERY, TRANSITION, RISK_OFF, CRISIS
- 10 observable features (adds T-bill/PIB spread, SBP reserves, CA balance, NCCPL flows)
- Confidence gating: holds previous regime when prob < 0.75
- Hard override: forces CRISIS on extreme macro stress
- StandardScaler for robust normalization

Model saves to ~/pakfindata/models/hmm_regime_v2.pkl
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
import sqlite3
import joblib

logger = logging.getLogger(__name__)
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from enum import Enum

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
PSX_SQLITE = Path("/home/smnb/psxdata_rescue/psx.sqlite")
MODEL_DIR = Path.home() / "pakfindata" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH_V2 = MODEL_DIR / "hmm_regime_v2.pkl"
TRADING_DAYS = 245

# Confidence gating threshold
CONFIDENCE_THRESHOLD = 0.75

# Hard override thresholds
KIBOR_CRISIS_THRESHOLD = 18.0
RESERVES_CRISIS_THRESHOLD = 8.0  # USD bn
CA_GDP_CRISIS_THRESHOLD = -3.0   # proxy in USD mn (~ -3000)


class MacroRegimeV2(Enum):
    RISK_ON = 0
    RECOVERY = 1
    TRANSITION = 2
    RISK_OFF = 3
    CRISIS = 4


REGIME_ALLOCATION_V2 = {
    MacroRegimeV2.RISK_ON:    {"equity": 0.80, "bonds": 0.10, "cash": 0.10},
    MacroRegimeV2.RECOVERY:   {"equity": 0.60, "bonds": 0.25, "cash": 0.15},
    MacroRegimeV2.TRANSITION: {"equity": 0.40, "bonds": 0.30, "cash": 0.30},
    MacroRegimeV2.RISK_OFF:   {"equity": 0.20, "bonds": 0.50, "cash": 0.30},
    MacroRegimeV2.CRISIS:     {"equity": 0.00, "bonds": 0.30, "cash": 0.70},
}

REGIME_COLORS_V2 = {
    MacroRegimeV2.RISK_ON:    "#22C55E",   # green
    MacroRegimeV2.RECOVERY:   "#14B8A6",   # teal
    MacroRegimeV2.TRANSITION: "#EAB308",   # yellow
    MacroRegimeV2.RISK_OFF:   "#F97316",   # orange
    MacroRegimeV2.CRISIS:     "#EF4444",   # red
}

REGIME_NAMES_V2 = {r: r.name.replace("_", " ").title() for r in MacroRegimeV2}


@dataclass
class RegimeStateV2:
    date: str
    regime: str
    effective_regime: str
    probability: float
    regime_probs: dict
    kse_momentum: float
    kibor_change: float
    pkr_change: float
    sbp_cycle: str
    reserves: float
    ca_balance: float
    spread: float
    regime_source: str
    gated: bool
    allocation: dict

    def to_dict(self):
        return asdict(self)


# ═══════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════

def load_macro_features_v2() -> pd.DataFrame:
    """Load and merge 10 macro features into a monthly DataFrame.

    Features:
        1. kse_return          — monthly return from OGDC proxy
        2. kse_mom_3m          — 3-month momentum
        3. kse_vol             — 6-month rolling volatility
        4. kibor_direction     — KIBOR 3M direction (-1/0/1)
        5. pkr_change          — PKR/USD monthly pct change
        6. tbill_pib_spread    — T-bill vs PIB spread (SQLite)
        7. sbp_reserves_usd_bn — SBP FX reserves (SQLite)
        8. ca_balance_usd_mn   — Current account balance (SQLite)
        9. fpi_net_4w_mn       — NCCPL foreign investor 4-week net flow (monthly avg)
       10. mf_net_4w_mn        — NCCPL mutual fund 4-week net flow (monthly avg)

    Metadata (not model feature):
        sbp_cycle     — SBP policy stance string
        sbp_cycle_num — encoded as -1/0/1
    """
    scon = sqlite3.connect(str(PSX_SQLITE), timeout=10)
    scon.row_factory = sqlite3.Row

    # ── 1-3. KSE-100 from eod_ohlcv (OGDC proxy) ──
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
        scon.close()
        return pd.DataFrame()

    kse["kse_return"] = kse["kse_close"].pct_change()
    kse["kse_mom_3m"] = kse["kse_close"].pct_change(3)
    kse["kse_vol"] = kse["kse_return"].rolling(6).std()

    # ── 4. KIBOR 3M direction ──
    kibor = pd.read_sql_query(
        """SELECT SUBSTR(date,1,7) AS month, AVG(offer) AS kibor_3m
           FROM kibor_daily WHERE tenor='3M' AND offer > 0
           GROUP BY month ORDER BY month""",
        scon,
    )
    kibor["kibor_change"] = kibor["kibor_3m"].diff()
    kibor["kibor_direction"] = np.where(
        kibor["kibor_change"] > 0.1, 1,
        np.where(kibor["kibor_change"] < -0.1, -1, 0),
    )

    # ── 5. PKR/USD change — use daily avg (EasyData historical) with interbank fallback ──
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

    # ── 6. T-bill / PIB spread (monthly avg) ──
    try:
        spread = pd.read_sql_query(
            """SELECT SUBSTR(date,1,7) AS month,
                      AVG(spread) AS tbill_pib_spread
               FROM hmm_tbill_pib_spread
               GROUP BY month ORDER BY month""",
            scon,
        )
    except Exception:
        spread = pd.DataFrame(columns=["month", "tbill_pib_spread"])

    # ── 7. SBP reserves (monthly avg) ──
    try:
        reserves = pd.read_sql_query(
            """SELECT SUBSTR(date,1,7) AS month,
                      AVG(reserves_usd_bn) AS sbp_reserves_usd_bn
               FROM hmm_sbp_reserves
               GROUP BY month ORDER BY month""",
            scon,
        )
    except Exception:
        reserves = pd.DataFrame(columns=["month", "sbp_reserves_usd_bn"])

    # ── 8. Current account balance (monthly) ──
    try:
        ca = pd.read_sql_query(
            """SELECT SUBSTR(date,1,7) AS month,
                      AVG(ca_balance_usd_mn) AS ca_balance_usd_mn
               FROM sbp_ca_balance
               GROUP BY month ORDER BY month""",
            scon,
        )
    except Exception:
        ca = pd.DataFrame(columns=["month", "ca_balance_usd_mn"])

    # ── SBP Policy Rate (metadata, not model feature) ──
    policy = pd.read_sql_query(
        """SELECT SUBSTR(rate_date,1,7) AS month,
                  MAX(policy_rate) AS policy_rate
           FROM sbp_policy_rates WHERE policy_rate > 0
           GROUP BY month ORDER BY month""",
        scon,
    )
    policy["rate_change"] = policy["policy_rate"].diff()
    policy["sbp_cycle"] = np.where(
        policy["rate_change"] > 0, "TIGHTENING",
        np.where(policy["rate_change"] < 0, "EASING", "HOLD"),
    )
    policy["sbp_cycle_num"] = np.where(
        policy["rate_change"] > 0, 1,
        np.where(policy["rate_change"] < 0, -1, 0),
    )

    # ── 9-10. NCCPL investor flows (4-week rolling, monthly avg) ──
    try:
        nccpl_flows = pd.read_sql_query(
            """SELECT SUBSTR(date,1,7) AS month,
                      AVG(fpi_net_4w) AS fpi_net_4w_mn,
                      AVG(mf_net_4w) AS mf_net_4w_mn
               FROM nccpl_flows_derived
               GROUP BY month ORDER BY month""",
            scon,
        )
    except Exception:
        nccpl_flows = pd.DataFrame(columns=["month", "fpi_net_4w_mn", "mf_net_4w_mn"])

    scon.close()

    # ── Merge all on month ──
    df = kse[["month", "kse_close", "kse_return", "kse_mom_3m", "kse_vol"]].copy()
    df = df.merge(kibor[["month", "kibor_3m", "kibor_change", "kibor_direction"]],
                  on="month", how="left")
    df = df.merge(fx[["month", "pkr_usd", "pkr_change"]],
                  on="month", how="left")
    df = df.merge(spread, on="month", how="left")
    df = df.merge(reserves, on="month", how="left")
    df = df.merge(ca, on="month", how="left")
    df = df.merge(policy[["month", "policy_rate", "sbp_cycle", "sbp_cycle_num"]],
                  on="month", how="left")
    df = df.merge(nccpl_flows, on="month", how="left")

    # Drop rows missing core features
    df = df.dropna(subset=["kse_return", "kibor_3m"]).reset_index(drop=True)
    df["sbp_cycle"] = df["sbp_cycle"].fillna("HOLD")
    df["sbp_cycle_num"] = df["sbp_cycle_num"].fillna(0)

    # Forward-fill new features (max 4 months)
    new_features = ["tbill_pib_spread", "sbp_reserves_usd_bn", "ca_balance_usd_mn",
                    "fpi_net_4w_mn", "mf_net_4w_mn"]
    for col in new_features:
        if col in df.columns:
            df[col] = df[col].ffill(limit=4)

    # Fill remaining NaN
    df = df.ffill().bfill()

    # Final safety: fill any remaining NaN with 0
    for col in FEATURE_COLS_V2:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df


# ═══════════════════════════════════════════════════════
# HMM TRAINING
# ═══════════════════════════════════════════════════════

FEATURE_COLS_V2 = [
    "kse_return",
    "kse_mom_3m",
    "kse_vol",
    "kibor_direction",
    "pkr_change",
    "tbill_pib_spread",
    "sbp_reserves_usd_bn",
    "ca_balance_usd_mn",
    "fpi_net_4w_mn",
    "mf_net_4w_mn",
]


def train_hmm_v2(df: pd.DataFrame, n_regimes: int = 5, n_iter: int = 150) -> dict:
    """Train 5-state Gaussian HMM with sklearn StandardScaler."""
    from hmmlearn.hmm import GaussianHMM
    from sklearn.preprocessing import StandardScaler

    # Use only features present in the dataframe AND with variance
    available_cols = [c for c in FEATURE_COLS_V2 if c in df.columns]

    # Drop rows with any NaN in feature columns, then fill remaining
    df = df.dropna(subset=available_cols).reset_index(drop=True)

    # Remove near-constant features (std < 0.01) — they confuse HMM
    good_cols = [c for c in available_cols if df[c].std() > 0.01]
    if len(good_cols) < len(available_cols):
        dropped = set(available_cols) - set(good_cols)
        logger.warning("Dropped low-variance features: %s", dropped)
    available_cols = good_cols

    X = df[available_cols].values.astype(np.float64)

    # Safety: replace any remaining NaN/inf with 0
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Try requested n_regimes, fall back to fewer if data is insufficient
    model = None
    for n in range(n_regimes, 2, -1):
        candidate = GaussianHMM(
            n_components=n,
            covariance_type="diag",
            n_iter=n_iter,
            random_state=42,
            tol=0.01,
        )
        candidate.fit(X_scaled)

        # Check all transition rows sum to ~1 (no empty states)
        row_sums = candidate.transmat_.sum(axis=1)
        if np.all(row_sums > 0.5):
            model = candidate
            n_regimes = n
            break
        logger.warning("HMM with %d states had empty states (row sums: %s), trying %d",
                       n, row_sums.round(2), n - 1)

    if model is None:
        # Last resort: 3 states, fix degenerate rows
        model = GaussianHMM(
            n_components=3, covariance_type="diag",
            n_iter=n_iter, random_state=42, tol=0.01,
        )
        model.fit(X_scaled)
        n_regimes = 3
        # Fix any zero rows in transition matrix
        for i in range(model.transmat_.shape[0]):
            if model.transmat_[i].sum() < 0.5:
                model.transmat_[i] = 1.0 / model.transmat_.shape[1]

    logger.info("HMM trained with %d states on %d months", n_regimes, len(X_scaled))

    # Predict states
    states = model.predict(X_scaled)

    # Sort states by average KSE return (highest -> RISK_ON, lowest -> CRISIS)
    state_returns = {}
    for s in range(n_regimes):
        mask = states == s
        state_returns[s] = df.loc[mask, "kse_return"].mean() if mask.sum() > 0 else 0.0

    sorted_states = sorted(state_returns.keys(),
                           key=lambda s: state_returns[s], reverse=True)
    label_map = {sorted_states[i]: i for i in range(min(n_regimes, 5))}

    return {
        "model": model,
        "scaler": scaler,
        "feature_cols": available_cols,
        "label_map": label_map,
        "n_regimes": n_regimes,
        "trained_at": datetime.now(PKT).isoformat(),
        "months_trained": len(df),
        "log_likelihood": float(model.score(X_scaled)),
        "transition_matrix": model.transmat_.tolist(),
    }


def save_hmm_v2(model_dict: dict) -> Path:
    """Save trained HMM v2 to disk."""
    joblib.dump(model_dict, MODEL_PATH_V2)
    return MODEL_PATH_V2


def load_hmm_v2() -> dict | None:
    """Load trained HMM v2 from disk."""
    if MODEL_PATH_V2.exists():
        return joblib.load(MODEL_PATH_V2)
    return None


# ═══════════════════════════════════════════════════════
# HARD OVERRIDE
# ═══════════════════════════════════════════════════════

def _check_crisis_override(row: pd.Series) -> bool:
    """Force CRISIS when ALL conditions met:
    - KIBOR 3M > 18%
    - SBP reserves < 8 USD bn
    - CA balance / GDP < -3% (proxied as ca_balance_usd_mn < -3000)
    """
    kibor = row.get("kibor_3m", 0)
    reserves = row.get("sbp_reserves_usd_bn", 99)
    ca_balance = row.get("ca_balance_usd_mn", 0)

    # Pakistan GDP ~350bn -> -3% ~ -10.5bn annual ~ -875mn/month
    # Use -3000 mn as generous threshold for monthly data
    ca_threshold = CA_GDP_CRISIS_THRESHOLD * 1000  # -3000 mn

    return (
        kibor > KIBOR_CRISIS_THRESHOLD
        and reserves < RESERVES_CRISIS_THRESHOLD
        and ca_balance < ca_threshold
    )


# ═══════════════════════════════════════════════════════
# PREDICTION
# ═══════════════════════════════════════════════════════

def predict_regime_v2(model_dict: dict, df: pd.DataFrame) -> pd.DataFrame:
    """Predict regimes with confidence gating and hard override.

    Returns DataFrame with columns:
        regime_id, effective_regime_id, regime, effective_regime,
        regime_prob, gated, regime_source,
        prob_risk_on, prob_recovery, prob_transition, prob_risk_off, prob_crisis
    """
    model = model_dict["model"]
    scaler = model_dict["scaler"]
    feature_cols = model_dict["feature_cols"]
    label_map = model_dict["label_map"]

    X = df[feature_cols].values.astype(np.float64)
    X_scaled = scaler.transform(X)

    raw_states = model.predict(X_scaled)
    probs = model.predict_proba(X_scaled)

    # Map raw HMM states -> ordered regime labels
    labeled = np.array([label_map.get(s, MacroRegimeV2.TRANSITION.value)
                        for s in raw_states])
    regime_names = {r.value: r.name for r in MacroRegimeV2}

    df = df.copy()
    df["regime_id"] = labeled
    df["regime"] = [regime_names.get(r, "TRANSITION") for r in labeled]
    df["regime_prob"] = [float(probs[i, raw_states[i]])
                         for i in range(len(raw_states))]

    # Probability columns for all 5 states
    for r in MacroRegimeV2:
        raw_for_regime = [k for k, v in label_map.items() if v == r.value]
        if raw_for_regime:
            df[f"prob_{r.name.lower()}"] = probs[:, raw_for_regime[0]]
        else:
            df[f"prob_{r.name.lower()}"] = 0.0

    # ── Confidence gating + hard override ──
    df["gated"] = False
    df["regime_source"] = "hmm"
    df["effective_regime_id"] = df["regime_id"].copy()

    prev_regime = int(df["regime_id"].iloc[0]) if len(df) > 0 else MacroRegimeV2.TRANSITION.value
    effective_ids = []

    for i in range(len(df)):
        confidence = df["regime_prob"].iloc[i]
        raw_id = int(df["regime_id"].iloc[i])

        if confidence < CONFIDENCE_THRESHOLD and i > 0:
            effective_id = prev_regime
            df.iat[i, df.columns.get_loc("gated")] = True
            df.iat[i, df.columns.get_loc("regime_source")] = "gated"
        else:
            effective_id = raw_id

        # Hard override check
        if _check_crisis_override(df.iloc[i]):
            effective_id = MacroRegimeV2.CRISIS.value
            df.iat[i, df.columns.get_loc("regime_source")] = "override"
            df.iat[i, df.columns.get_loc("gated")] = False

        effective_ids.append(effective_id)
        prev_regime = effective_id

    df["effective_regime_id"] = effective_ids
    df["effective_regime"] = [regime_names.get(r, "TRANSITION")
                              for r in effective_ids]

    return df


# ═══════════════════════════════════════════════════════
# BACKTEST
# ═══════════════════════════════════════════════════════

def backtest_regime_allocation_v2(df: pd.DataFrame) -> dict:
    """Backtest 5-state regime allocation vs buy-and-hold."""
    if "effective_regime_id" not in df.columns or "kse_return" not in df.columns:
        return {"error": "Need regime predictions first"}

    df = df.copy()
    alloc_map = {r.value: REGIME_ALLOCATION_V2[r]["equity"]
                 for r in MacroRegimeV2}

    df["equity_alloc"] = df["effective_regime_id"].map(alloc_map)
    df["strategy_return"] = (df["equity_alloc"].shift(1).fillna(0.4)
                             * df["kse_return"])
    df["bh_return"] = df["kse_return"]

    df["strategy_equity"] = (1 + df["strategy_return"]).cumprod()
    df["bh_equity"] = (1 + df["bh_return"]).cumprod()

    strat_total = float(df["strategy_equity"].iloc[-1] - 1)
    bh_total = float(df["bh_equity"].iloc[-1] - 1)

    strat_vol = float(df["strategy_return"].std() * np.sqrt(12))
    bh_vol = float(df["bh_return"].std() * np.sqrt(12))

    strat_sharpe = (
        float(df["strategy_return"].mean() / df["strategy_return"].std()
              * np.sqrt(12))
        if df["strategy_return"].std() > 0 else 0
    )
    bh_sharpe = (
        float(df["bh_return"].mean() / df["bh_return"].std() * np.sqrt(12))
        if df["bh_return"].std() > 0 else 0
    )

    strat_dd = float(
        (df["strategy_equity"] / df["strategy_equity"].cummax() - 1).min()
    )
    bh_dd = float(
        (df["bh_equity"] / df["bh_equity"].cummax() - 1).min()
    )

    # Regime durations (effective)
    durations = {}
    for r in MacroRegimeV2:
        mask = df["effective_regime_id"] == r.value
        durations[r.name] = int(mask.sum())

    gated_count = int(df["gated"].sum())
    override_count = int((df["regime_source"] == "override").sum())

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
            "gated_months": gated_count,
            "override_months": override_count,
        },
        "regime_durations": durations,
        "transition_matrix": (
            df.groupby(["effective_regime_id",
                        df["effective_regime_id"].shift(1)])
            .size().unstack(fill_value=0).values.tolist()
            if len(df) > 1 else []
        ),
    }


# ═══════════════════════════════════════════════════════
# FULL PIPELINE
# ═══════════════════════════════════════════════════════

def train_and_save_v2() -> dict:
    """Train HMM v2 on all available data and save model."""
    df = load_macro_features_v2()
    if len(df) < 24:
        return {"error": f"Not enough data ({len(df)} months)"}

    model_dict = train_hmm_v2(df)
    path = save_hmm_v2(model_dict)

    # Predict on training data for analysis
    pred_df = predict_regime_v2(model_dict, df)
    bt = backtest_regime_allocation_v2(pred_df)

    latest = pred_df.iloc[-1]

    return {
        "model_path": str(path),
        "months_trained": len(df),
        "log_likelihood": model_dict["log_likelihood"],
        "trained_at": model_dict["trained_at"],
        "current_regime": str(latest["effective_regime"]),
        "current_probs": {
            "RISK_ON": float(latest.get("prob_risk_on", 0)),
            "RECOVERY": float(latest.get("prob_recovery", 0)),
            "TRANSITION": float(latest.get("prob_transition", 0)),
            "RISK_OFF": float(latest.get("prob_risk_off", 0)),
            "CRISIS": float(latest.get("prob_crisis", 0)),
        },
        "regime_source": str(latest.get("regime_source", "hmm")),
        "gated": bool(latest.get("gated", False)),
        "backtest": bt["metrics"] if "metrics" in bt else {},
        "regime_durations": bt.get("regime_durations", {}),
        "pred_df": pred_df,
    }


def get_current_regime_v2() -> dict | None:
    """Load saved v2 model and predict current regime."""
    model_dict = load_hmm_v2()
    if not model_dict:
        return None

    df = load_macro_features_v2()
    if df.empty:
        return None

    pred_df = predict_regime_v2(model_dict, df)
    latest = pred_df.iloc[-1]

    regime = MacroRegimeV2(int(latest["effective_regime_id"]))
    allocation = REGIME_ALLOCATION_V2[regime]

    return {
        "date": str(latest["month"]),
        "regime": regime.name,
        "raw_regime": str(latest.get("regime", regime.name)),
        "probability": float(latest["regime_prob"]),
        "probs": {
            "RISK_ON": float(latest.get("prob_risk_on", 0)),
            "RECOVERY": float(latest.get("prob_recovery", 0)),
            "TRANSITION": float(latest.get("prob_transition", 0)),
            "RISK_OFF": float(latest.get("prob_risk_off", 0)),
            "CRISIS": float(latest.get("prob_crisis", 0)),
        },
        "allocation": allocation,
        "kse_momentum": float(latest.get("kse_mom_3m", 0)),
        "kibor": float(latest.get("kibor_3m", 0)),
        "pkr_usd": float(latest.get("pkr_usd", 0)),
        "sbp_cycle": str(latest.get("sbp_cycle", "HOLD")),
        "reserves": float(latest.get("sbp_reserves_usd_bn", 0)),
        "ca_balance": float(latest.get("ca_balance_usd_mn", 0)),
        "spread": float(latest.get("tbill_pib_spread", 0)),
        "regime_source": str(latest.get("regime_source", "hmm")),
        "gated": bool(latest.get("gated", False)),
        "model_trained": model_dict.get("trained_at", "unknown"),
    }


# ═══════════════════════════════════════════════════════
# BACKWARD COMPATIBILITY — re-export v1 symbols
# ═══════════════════════════════════════════════════════

from pakfindata.engine.macro_regime_hmm import (  # noqa: E402, F401
    MacroRegime,
    REGIME_ALLOCATION,
    REGIME_COLORS,
    REGIME_NAMES,
    RegimeState,
    load_macro_features,
    train_hmm,
    save_hmm,
    load_hmm,
    predict_regime,
    backtest_regime_allocation,
    train_and_save,
    get_current_regime,
)
