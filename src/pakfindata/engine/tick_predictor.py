"""Tick-level prediction engine for replay overlay.

Takes raw tick data for a symbol+date, builds features at N-minute intervals,
runs the trained ML model (or momentum fallback), and returns timestamped
predictions with Bayesian credibility tracking.

Usage:
    from pakfindata.engine.tick_predictor import generate_replay_predictions

    predictions = generate_replay_predictions(
        ticks_df=ticks,          # raw tick DataFrame or list[dict]
        symbol="HUBC",
        interval_minutes=15,
    )
    # Returns dict with: predictions, bayesian, summary
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger("tick_predictor")

MODEL_DIR = Path.home() / "pakfindata" / "models"

_FEATURE_KEYS = [
    "ret_1m", "ret_5m", "ret_15m", "volatility", "vwap_dev",
    "range_pos", "spread_pct", "ofi", "volume_rate",
    "tick_velocity", "momentum",
]


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def _load_models(symbol: str | None = None):
    """Load a tick-compatible model.  Returns (classifier, scaler) or (None, None).

    Only returns a model if its expected feature count matches our tick
    features (len(_FEATURE_KEYS)).  EOD models trained on 37 daily features
    are *not* compatible and are skipped — momentum fallback is used instead.
    """
    import pickle

    n_tick_features = len(_FEATURE_KEYS)

    def _check(path):
        if not path.exists():
            return None, None
        with open(path, "rb") as f:
            data = pickle.load(f)
        if isinstance(data, dict):
            model = data.get("model") or data.get("classifier")
            scaler = data.get("scaler")
        else:
            model, scaler = data, None
        if model is None:
            return None, None
        # Validate feature count matches tick features
        n = getattr(model, "n_features_in_", None)
        if n is not None and n != n_tick_features:
            logger.info(
                f"Skipping {path.name}: expects {n} features, "
                f"tick engine provides {n_tick_features}"
            )
            return None, None
        return model, scaler

    # Try symbol-specific tick model, then generic tick model
    for prefix in ([f"{symbol}_tick_", "tick_", f"{symbol}_", ""] if symbol else ["tick_", ""]):
        m, s = _check(MODEL_DIR / f"{prefix}latest.pkl")
        if m is not None:
            return m, s

    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE ENGINEERING (from raw ticks at regular intervals)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_interval_features(
    ticks: pd.DataFrame, interval_minutes: int = 15
) -> list[dict]:
    """Build ML features from tick data at regular intervals.

    At each interval, compute features from all ticks up to that point.
    Returns list of feature dicts, one per interval.
    """
    if ticks.empty:
        return []

    ticks = ticks.sort_values("timestamp").reset_index(drop=True)

    ts_col = "timestamp"
    t_start = ticks[ts_col].iloc[0]
    t_end = ticks[ts_col].iloc[-1]
    interval_sec = interval_minutes * 60

    intervals: list[dict] = []
    t = t_start + interval_sec  # first prediction after one full interval

    while t <= t_end:
        mask = ticks[ts_col] <= t
        window = ticks[mask]

        if len(window) < 20:
            t += interval_sec
            continue

        prices = window["price"].values
        timestamps = window[ts_col].values

        # Skip intervals during trading gaps (lunch break, etc.)
        # If the most recent tick is >5 min old, we're in a gap
        last_tick_ts = timestamps[-1]
        if t - last_tick_ts > 300:
            t += interval_sec
            continue
        cur_price = prices[-1]
        cur_ts = timestamps[-1]

        # ── Price momentum (TIME-BASED lookback) ──
        def _ret_since(secs_ago):
            """Return % since N seconds ago."""
            cutoff = cur_ts - secs_ago
            idx = np.searchsorted(timestamps, cutoff)
            if idx < len(prices):
                return (cur_price / prices[idx] - 1) * 100
            return 0.0

        ret_1m = _ret_since(60)
        ret_5m = _ret_since(300)
        ret_15m = _ret_since(900)

        # ── Volatility (from last 5 min of price changes) ──
        cutoff_5m = cur_ts - 300
        recent_mask = timestamps >= cutoff_5m
        recent_prices = prices[recent_mask]
        if len(recent_prices) > 10:
            rets = np.diff(recent_prices) / recent_prices[:-1]
            vol = float(np.std(rets) * 100) if len(rets) > 0 else 0.0
        else:
            vol = 0.0

        # ── VWAP ──
        has_vol = "volume" in window.columns
        if has_vol:
            cum_pv = (window["price"] * window["volume"]).sum()
            cum_v = window["volume"].sum()
            vwap = cum_pv / cum_v if cum_v > 0 else cur_price
        else:
            vwap = window["price"].mean()
        vwap_dev = (cur_price / vwap - 1) * 100

        # ── Day range position (0 = low, 1 = high) ──
        day_high, day_low = prices.max(), prices.min()
        range_pos = (cur_price - day_low) / (day_high - day_low) if day_high != day_low else 0.5

        # ── Spread / order flow (last 2 minutes of ticks) ──
        has_bbo = "bid" in window.columns and "ask" in window.columns
        if has_bbo:
            recent_2m = window[window[ts_col] >= cur_ts - 120]
            if len(recent_2m) < 5:
                recent_2m = window.tail(30)
            avg_spread = (recent_2m["ask"] - recent_2m["bid"]).mean()
            spread_pct = float(avg_spread / cur_price * 100) if cur_price > 0 else 0.0
            bid_vol = recent_2m["bidVol"].sum() if "bidVol" in recent_2m.columns else 0
            ask_vol = recent_2m["askVol"].sum() if "askVol" in recent_2m.columns else 0
            ofi = float((bid_vol - ask_vol) / (bid_vol + ask_vol)) if (bid_vol + ask_vol) > 0 else 0.0
        else:
            spread_pct = 0.0
            ofi = 0.0

        # ── Volume rate ──
        volumes = window["volume"].values if has_vol else np.zeros(len(window))
        total_vol = float(volumes.sum())
        vol_rate = total_vol / len(window) if len(window) > 0 else 0.0

        # ── Tick velocity (ticks per minute in last 2 min) ──
        recent_2m_mask = timestamps >= cur_ts - 120
        n_recent = int(recent_2m_mask.sum())
        if n_recent > 5:
            ts_range = cur_ts - timestamps[recent_2m_mask][0]
            tick_velocity = float(n_recent / (ts_range / 60)) if ts_range > 0 else 0.0
        else:
            tick_velocity = 0.0

        # ── Composite momentum (ret_1m/5m/15m already in %) ──
        momentum = ret_1m * 0.5 + ret_5m * 0.3 + ret_15m * 0.2

        intervals.append({
            "timestamp": float(t),
            "price": float(cur_price),
            "tick_index": int(mask.sum()),
            "ret_1m": float(ret_1m),
            "ret_5m": float(ret_5m),
            "ret_15m": float(ret_15m),
            "volatility": float(vol),
            "vwap": float(vwap),
            "vwap_dev": float(vwap_dev),
            "range_pos": float(range_pos),
            "spread_pct": float(spread_pct),
            "ofi": float(ofi),
            "volume_rate": float(vol_rate),
            "tick_velocity": float(tick_velocity),
            "momentum": float(momentum),
            "day_high": float(day_high),
            "day_low": float(day_low),
        })
        t += interval_sec

    return intervals


# ═══════════════════════════════════════════════════════════════════════════════
# ML PREDICTION AT EACH INTERVAL
# ═══════════════════════════════════════════════════════════════════════════════


def _predict_at_intervals(
    intervals: list[dict], classifier, scaler=None
) -> list[dict]:
    """Run ML model on each interval's features."""
    if not intervals or classifier is None:
        return []

    predictions: list[dict] = []

    for iv in intervals:
        features = np.array([[iv.get(k, 0) for k in _FEATURE_KEYS]])
        if scaler is not None:
            try:
                features = scaler.transform(features)
            except Exception:
                pass

        pred: dict = {
            "timestamp": iv["timestamp"],
            "price": iv["price"],
            "tick_index": iv["tick_index"],
        }

        # Classification: UP/DOWN + probability
        try:
            if hasattr(classifier, "predict_proba"):
                proba = classifier.predict_proba(features)[0]
                pred["direction"] = "UP" if proba[1] > 0.5 else "DOWN"
                pred["probability"] = float(max(proba))
            else:
                direction = classifier.predict(features)[0]
                pred["direction"] = "UP" if direction > 0 else "DOWN"
                pred["probability"] = 0.6
        except Exception:
            pred["direction"] = "HOLD"
            pred["probability"] = 0.5

        # Estimate return from probability
        sign = 1 if pred["direction"] == "UP" else -1
        magnitude = (pred["probability"] - 0.5) * 4
        pred["predicted_return"] = sign * magnitude
        pred["predicted_price"] = iv["price"] * (1 + pred["predicted_return"] / 100)

        # Default CI: +/- scaled by volatility or 1%
        vol_scale = max(iv.get("volatility", 1.0), 0.3) * 0.5
        pred["ci_low"] = iv["price"] * (1 - vol_scale / 100)
        pred["ci_high"] = iv["price"] * (1 + vol_scale / 100)

        predictions.append(pred)

    return predictions


# ═══════════════════════════════════════════════════════════════════════════════
# RESOLVE PREDICTIONS AGAINST ACTUAL PRICES
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_predictions(
    predictions: list[dict],
    ticks: pd.DataFrame,
    lookahead_minutes: int = 15,
) -> list[dict]:
    """Mark each prediction hit/miss using actual future price."""
    if not predictions or ticks.empty:
        return predictions

    ts_col = "timestamp"
    lookahead_sec = lookahead_minutes * 60

    for pred in predictions:
        target_ts = pred["timestamp"] + lookahead_sec
        future_mask = ticks[ts_col] >= target_ts
        if future_mask.any():
            actual_price = float(ticks.loc[future_mask, "price"].iloc[0])
            actual_return = (actual_price / pred["price"] - 1) * 100

            pred["actual_price"] = actual_price
            pred["actual_return"] = actual_return
            pred["resolve_ts"] = float(target_ts)

            # Dead zone: if actual move < 0.02%, treat as neutral
            dead_zone = 0.02
            if abs(actual_return) < dead_zone:
                # Near-zero move: HOLD is correct, directional is neither
                pred["correct"] = pred["direction"] == "HOLD"
            elif pred["direction"] == "UP":
                pred["correct"] = actual_return > 0
            elif pred["direction"] == "DOWN":
                pred["correct"] = actual_return < 0
            else:
                # HOLD but price moved significantly
                pred["correct"] = False
        else:
            pred["actual_price"] = None
            pred["actual_return"] = None
            pred["resolve_ts"] = None
            pred["correct"] = None

    return predictions


# ═══════════════════════════════════════════════════════════════════════════════
# BAYESIAN CREDIBILITY (Beta-Binomial)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_bayesian_posterior(predictions: list[dict]) -> list[dict]:
    """Rolling Bayesian credibility using Beta-Binomial model.

    Prior: Beta(2, 2) — weakly informative, centered at 50%.
    """
    from scipy import stats as sp_stats

    alpha = 2.0
    beta_param = 2.0
    series: list[dict] = []

    for pred in predictions:
        if pred.get("correct") is None:
            continue

        if pred["correct"]:
            alpha += 1
        else:
            beta_param += 1

        posterior_mean = alpha / (alpha + beta_param)

        try:
            hdi_low, hdi_high = sp_stats.beta.ppf([0.025, 0.975], alpha, beta_param)
        except Exception:
            hdi_low, hdi_high = 0.3, 0.7

        if posterior_mean >= 0.60 and hdi_low >= 0.45:
            label = "CREDIBLE"
        elif posterior_mean >= 0.52:
            label = "MARGINAL"
        else:
            label = "NOT_CREDIBLE"

        series.append({
            "timestamp": pred.get("resolve_ts", pred["timestamp"]),
            "alpha": alpha,
            "beta": beta_param,
            "posterior_mean": round(posterior_mean, 4),
            "hdi_low": round(float(hdi_low), 4),
            "hdi_high": round(float(hdi_high), 4),
            "label": label,
            "n": int(alpha + beta_param - 4),
        })

    return series


# ═══════════════════════════════════════════════════════════════════════════════
# MOMENTUM FALLBACK (when no ML model is available)
# ═══════════════════════════════════════════════════════════════════════════════

def _momentum_fallback(
    ticks: pd.DataFrame, interval_min: int, lookahead_min: int
) -> list[dict]:
    """Simple momentum-based predictions when no ML model is available."""
    intervals = _build_interval_features(ticks, interval_min)
    predictions: list[dict] = []

    for iv in intervals:
        mom = iv.get("momentum", 0)  # already in % (e.g., +0.05 = 0.05%)
        ofi = iv.get("ofi", 0)       # -1 to +1
        vwap_dev = iv.get("vwap_dev", 0)  # in %

        # Direction from momentum (primary) with VWAP as tiebreaker
        # Threshold: 0.005% momentum triggers a directional call
        if abs(mom) > 0.005:
            direction = "UP" if mom > 0 else "DOWN"
        elif abs(vwap_dev) > 0.01:
            direction = "UP" if vwap_dev > 0 else "DOWN"
        else:
            direction = "HOLD"

        # Confidence: base from momentum magnitude, boosted if OFI agrees
        base_conf = min(0.5 + abs(mom) * 5, 0.85)  # 0.05% mom -> 0.75 conf
        ofi_agrees = (ofi > 0 and direction == "UP") or (ofi < 0 and direction == "DOWN")
        confidence = min(base_conf + (0.05 if ofi_agrees else 0), 0.95)

        sign = 1 if direction == "UP" else (-1 if direction == "DOWN" else 0)
        est_ret = sign * abs(mom) * 0.5

        vol_scale = max(iv.get("volatility", 1.0), 0.3) * 0.5
        predictions.append({
            "timestamp": iv["timestamp"],
            "price": iv["price"],
            "tick_index": iv["tick_index"],
            "direction": direction,
            "probability": confidence,
            "predicted_return": est_ret,
            "predicted_price": iv["price"] * (1 + est_ret / 100),
            "ci_low": iv["price"] * (1 - vol_scale / 100),
            "ci_high": iv["price"] * (1 + vol_scale / 100),
            "model": "momentum_fallback",
        })

    predictions = _resolve_predictions(predictions, ticks, lookahead_min)
    return predictions


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def generate_replay_predictions(
    ticks_df: pd.DataFrame | list[dict],
    symbol: str,
    interval_minutes: int = 15,
    lookahead_minutes: int | None = None,
) -> dict:
    """Generate predictions + Bayesian credibility for replay overlay.

    Args:
        ticks_df: Raw tick data (DataFrame or list[dict]) with timestamp, price, etc.
        symbol: Stock symbol for model loading.
        interval_minutes: How often to predict (default 15 min).
        lookahead_minutes: How far ahead to predict (default = interval_minutes).

    Returns:
        {"predictions": [...], "bayesian": [...], "summary": {...}}
    """
    if isinstance(ticks_df, list):
        ticks_df = pd.DataFrame(ticks_df)

    if ticks_df.empty or len(ticks_df) < 50:
        return {"predictions": [], "bayesian": [], "summary": {}}

    if lookahead_minutes is None:
        lookahead_minutes = interval_minutes

    # Load models
    classifier, scaler = _load_models(symbol)

    if classifier is None:
        logger.info(f"No trained model for {symbol} — using momentum fallback")
        predictions = _momentum_fallback(ticks_df, interval_minutes, lookahead_minutes)
    else:
        intervals = _build_interval_features(ticks_df, interval_minutes)
        if not intervals:
            return {"predictions": [], "bayesian": [], "summary": {}}
        predictions = _predict_at_intervals(intervals, classifier, scaler)
        predictions = _resolve_predictions(predictions, ticks_df, lookahead_minutes)

    # Bayesian posterior
    bayesian = _compute_bayesian_posterior(predictions)

    # Summary
    resolved = [p for p in predictions if p.get("correct") is not None]
    correct = sum(1 for p in resolved if p["correct"])
    total = len(resolved)

    summary = {
        "total_predictions": len(predictions),
        "resolved": total,
        "correct": correct,
        "accuracy": correct / total if total > 0 else 0,
        "final_credibility": bayesian[-1]["label"] if bayesian else "NO_DATA",
        "final_posterior": bayesian[-1]["posterior_mean"] if bayesian else 0.5,
    }

    return {
        "predictions": predictions,
        "bayesian": bayesian,
        "summary": summary,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LLM COMMENTARY (for key prediction moments)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_llm_commentary(
    predictions: list[dict], symbol: str, max_comments: int = 5
) -> list[dict]:
    """Generate LLM commentary for the most interesting prediction moments."""
    try:
        from pakfindata.services.llm_client import llm
    except ImportError:
        return []

    if not llm.is_running():
        return []

    resolved = [p for p in predictions if p.get("correct") is not None]
    if not resolved:
        return []

    # Select interesting moments
    interesting: list[dict] = []

    # Strongest correct predictions
    correct = sorted(
        [p for p in resolved if p["correct"]],
        key=lambda p: p["probability"], reverse=True,
    )
    interesting.extend(correct[:2])

    # Biggest misses (high confidence but wrong)
    wrong = sorted(
        [p for p in resolved if not p["correct"]],
        key=lambda p: p["probability"], reverse=True,
    )
    interesting.extend(wrong[:1])

    # Trend reversals
    for i in range(1, len(resolved)):
        if resolved[i]["direction"] != resolved[i - 1]["direction"]:
            interesting.append(resolved[i])
            if len(interesting) >= max_comments:
                break

    # Deduplicate
    seen_ts: set[float] = set()
    unique: list[dict] = []
    for p in interesting:
        if p["timestamp"] not in seen_ts:
            seen_ts.add(p["timestamp"])
            unique.append(p)
    unique = unique[:max_comments]

    comments: list[dict] = []
    for p in unique:
        prompt = (
            f"You are a PSX stock analyst. In ONE short sentence (max 12 words), "
            f"comment on this prediction for {symbol}:\n"
            f"- Predicted: {p['direction']} with {p['probability']:.0%} confidence\n"
            f"- Price at prediction: {p['price']:.2f}\n"
            f"- Actual outcome: {'correct' if p['correct'] else 'wrong'}, "
            f"actual return: {p.get('actual_return', 0):.2f}%\n"
            f"Be direct. No disclaimers."
        )

        resp = llm.complete(prompt, use_case="commentary", max_tokens=30, timeout=10)
        if resp.success and resp.text:
            comments.append({
                "timestamp": p["timestamp"],
                "comment": resp.text.strip()[:60],
                "correct": p["correct"],
            })

    return comments
