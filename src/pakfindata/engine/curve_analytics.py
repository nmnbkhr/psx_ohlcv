"""Sovereign Yield Curve Analytics Engine.

Three interpolation/extrapolation methods:
  1. Linear — simplest, manual-equivalent
  2. Cubic Spline — smooth, what Bloomberg FWCV uses
  3. Nelson-Siegel-Svensson (NSS) — central bank standard, parametric

Usage:
    from pakfindata.engine.curve_analytics import CurveAnalytics

    ca = CurveAnalytics(tenors_years, yields_pct)
    full = ca.full_curve()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger("curve_analytics")

STANDARD_TENORS = {
    "1W": 7 / 365, "2W": 14 / 365, "1M": 1 / 12, "2M": 2 / 12, "3M": 0.25,
    "4M": 4 / 12, "6M": 0.5, "9M": 0.75, "12M": 1.0,
    "2Y": 2, "3Y": 3, "4Y": 4, "5Y": 5, "6Y": 6, "7Y": 7,
    "8Y": 8, "9Y": 9, "10Y": 10, "15Y": 15, "20Y": 20, "25Y": 25, "30Y": 30,
}

TENOR_DAYS = {
    "1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122,
    "6M": 182, "9M": 274, "12M": 365,
    "2Y": 730, "3Y": 1095, "4Y": 1460, "5Y": 1825,
    "6Y": 2190, "7Y": 2555, "8Y": 2920, "9Y": 3285, "10Y": 3650,
    "15Y": 5475, "20Y": 7300, "25Y": 9125, "30Y": 10950,
}

FULL_TENORS_YEARS = [
    0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30,
]

FULL_TENORS_LABELS = [
    "3M", "6M", "1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y",
    "8Y", "9Y", "10Y", "15Y", "20Y", "25Y", "30Y",
]


# ═══════════════════════════════════════════════════════════════════════════════
# METHOD 1: LINEAR
# ═══════════════════════════════════════════════════════════════════════════════

def linear_extrapolate(
    tenors_y: np.ndarray, yields: np.ndarray, target_y: float,
) -> float:
    if len(tenors_y) < 2:
        return float(yields[0]) if len(yields) > 0 else 0.0

    if target_y <= tenors_y[0]:
        slope = (yields[1] - yields[0]) / (tenors_y[1] - tenors_y[0])
        return float(yields[0] + slope * (target_y - tenors_y[0]))
    elif target_y >= tenors_y[-1]:
        slope = (yields[-1] - yields[-2]) / (tenors_y[-1] - tenors_y[-2])
        return float(yields[-1] + slope * (target_y - tenors_y[-1]))
    else:
        idx = np.searchsorted(tenors_y, target_y)
        x0, x1 = tenors_y[idx - 1], tenors_y[idx]
        y0, y1 = yields[idx - 1], yields[idx]
        return float(y0 + (y1 - y0) * (target_y - x0) / (x1 - x0))


def linear_full_curve(
    tenors_y: np.ndarray, yields: np.ndarray, targets: list[float],
) -> list[float]:
    return [linear_extrapolate(tenors_y, yields, t) for t in targets]


# ═══════════════════════════════════════════════════════════════════════════════
# METHOD 2: CUBIC SPLINE
# ═══════════════════════════════════════════════════════════════════════════════

def spline_full_curve(
    tenors_y: np.ndarray, yields: np.ndarray, targets: list[float],
) -> list[float]:
    """Cubic spline with linear extrapolation beyond boundaries."""
    try:
        from scipy.interpolate import CubicSpline

        cs = CubicSpline(tenors_y, yields, bc_type="natural")
        results = []
        for t in targets:
            if tenors_y[0] <= t <= tenors_y[-1]:
                results.append(float(cs(t)))
            elif t > tenors_y[-1]:
                bv = float(cs(tenors_y[-1]))
                bd = float(cs(tenors_y[-1], 1))
                results.append(bv + bd * (t - tenors_y[-1]))
            else:
                bv = float(cs(tenors_y[0]))
                bd = float(cs(tenors_y[0], 1))
                results.append(bv + bd * (t - tenors_y[0]))
        return results
    except ImportError:
        logger.warning("scipy not installed — falling back to linear")
        return linear_full_curve(tenors_y, yields, targets)


# ═══════════════════════════════════════════════════════════════════════════════
# METHOD 3: NELSON-SIEGEL-SVENSSON (NSS)
# ═══════════════════════════════════════════════════════════════════════════════

def _nss_yield(t: float, params: np.ndarray) -> float:
    b0, b1, b2, b3, tau1, tau2 = params
    if t <= 0:
        return b0 + b1

    x1, x2 = t / tau1, t / tau2

    if x1 < 1e-10:
        term1, term2 = 1.0, 0.0
    else:
        exp1 = np.exp(-x1)
        term1 = (1 - exp1) / x1
        term2 = term1 - exp1

    if x2 < 1e-10:
        term3 = 0.0
    else:
        exp2 = np.exp(-x2)
        term3 = (1 - exp2) / x2 - exp2

    return b0 + b1 * term1 + b2 * term2 + b3 * term3


def fit_nss(tenors_y: np.ndarray, yields: np.ndarray) -> np.ndarray | None:
    """Fit NSS model. Returns 6 params or None."""
    try:
        from scipy.optimize import minimize
    except ImportError:
        return None

    if len(tenors_y) < 4:
        return None

    def objective(params):
        b0, b1, b2, b3, tau1, tau2 = params
        if tau1 <= 0.01 or tau2 <= 0.01:
            return 1e10
        return np.mean([(_nss_yield(t, params) - y) ** 2
                        for t, y in zip(tenors_y, yields)])

    long_rate = yields[-1]
    short_rate = yields[0]

    best_result, best_error = None, 1e10
    for tau1_init in [1.0, 2.0, 3.0, 5.0]:
        for tau2_init in [3.0, 5.0, 8.0, 15.0]:
            if tau2_init <= tau1_init:
                continue
            x0 = [long_rate, short_rate - long_rate, 0.0, 0.0, tau1_init, tau2_init]
            try:
                result = minimize(
                    objective, x0, method="Nelder-Mead",
                    options={"maxiter": 5000, "xatol": 1e-6, "fatol": 1e-8},
                )
                if result.fun < best_error:
                    best_error = result.fun
                    best_result = result
            except Exception:
                continue

    if best_result is None or best_error > 0.1:
        return None

    return best_result.x


def nss_full_curve(
    tenors_y: np.ndarray, yields: np.ndarray, targets: list[float],
) -> list[float] | None:
    params = fit_nss(tenors_y, yields)
    if params is None:
        return None
    return [_nss_yield(t, params) for t in targets]


# ═══════════════════════════════════════════════════════════════════════════════
# COMBINED ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════

class CurveAnalytics:
    """Full yield curve analytics with multiple interpolation methods."""

    def __init__(
        self,
        tenors_years: list[float],
        yields_pct: list[float],
        tenor_labels: list[str] | None = None,
    ):
        self.tenors = np.array(tenors_years, dtype=float)
        self.yields = np.array(yields_pct, dtype=float)
        self.labels = tenor_labels or [f"{t}Y" for t in tenors_years]

        idx = np.argsort(self.tenors)
        self.tenors = self.tenors[idx]
        self.yields = self.yields[idx]
        self.labels = [self.labels[i] for i in idx]

        self._nss_params = None
        if len(self.tenors) >= 4:
            self._nss_params = fit_nss(self.tenors, self.yields)

    def full_curve(self, targets: list[float] | None = None) -> dict:
        if targets is None:
            targets = FULL_TENORS_YEARS

        result = {
            "targets": targets,
            "linear": linear_full_curve(self.tenors, self.yields, targets),
            "spline": spline_full_curve(self.tenors, self.yields, targets),
            "nss": nss_full_curve(self.tenors, self.yields, targets),
            "official": {float(t): float(y) for t, y in zip(self.tenors, self.yields)},
            "nss_params": self._nss_params.tolist() if self._nss_params is not None else None,
        }

        if self._nss_params is not None:
            fitted = [_nss_yield(t, self._nss_params) for t in self.tenors]
            result["nss_rmse"] = float(np.sqrt(np.mean(
                [(f - a) ** 2 for f, a in zip(fitted, self.yields)]
            )))
        else:
            result["nss_rmse"] = None

        return result

    def curve_metrics(self) -> dict:
        metrics: dict = {}

        def y_at(t):
            idx = np.argmin(np.abs(self.tenors - t))
            return float(self.yields[idx]) if abs(self.tenors[idx] - t) < 0.5 else None

        y2, y5, y10 = y_at(2), y_at(5), y_at(10)

        if y2 is not None and y10 is not None:
            metrics["slope_2s10s"] = round((y10 - y2) * 100, 1)
        if y2 is not None and y5 is not None:
            metrics["slope_2s5s"] = round((y5 - y2) * 100, 1)
        if y5 is not None and y10 is not None:
            metrics["slope_5s10s"] = round((y10 - y5) * 100, 1)
        if y2 is not None and y5 is not None and y10 is not None:
            metrics["butterfly_2_5_10"] = round((2 * y5 - y2 - y10) * 100, 1)

        metrics["level"] = round(float(np.mean(self.yields)), 4)

        short = self.yields[self.tenors <= 1]
        long_end = self.yields[self.tenors >= 10]
        if len(short) > 0:
            metrics["short_avg"] = round(float(np.mean(short)), 4)
        if len(long_end) > 0:
            metrics["long_avg"] = round(float(np.mean(long_end)), 4)

        if y2 is not None and y10 is not None:
            metrics["inverted"] = y2 > y10

        return metrics

    def confidence_band(self, targets: list[float] | None = None) -> dict:
        """Compute confidence band from spread between reliable methods.

        - Official tenors: band = 0
        - Synthetic tenors: band = range of reliable methods
        - NSS excluded from band if >200 bps deviation from Spline
          (means NSS tail is unanchored)
        """
        if targets is None:
            targets = FULL_TENORS_YEARS

        full = self.full_curve(targets)
        official = full["official"]

        upper, lower, center, spread_bps, nss_excluded = [], [], [], [], []

        for i, t in enumerate(targets):
            if t in official:
                v = official[t]
                upper.append(v)
                lower.append(v)
                center.append(v)
                spread_bps.append(0.0)
                nss_excluded.append(False)
            else:
                reliable = [full["linear"][i], full["spline"][i]]
                excluded = False

                if full.get("nss") and full["nss"][i] is not None:
                    nss_val = full["nss"][i]
                    if abs(nss_val - full["spline"][i]) * 100 <= 200:
                        reliable.append(nss_val)
                    else:
                        excluded = True

                hi, lo = max(reliable), min(reliable)
                upper.append(hi)
                lower.append(lo)
                center.append(full["spline"][i])
                spread_bps.append(round((hi - lo) * 100, 1))
                nss_excluded.append(excluded)

        # Spread-weighted confidence score (Step 9a)
        rmse = None
        if self._nss_params is not None:
            fitted = [_nss_yield(t, self._nss_params) for t in self.tenors]
            rmse = float(np.sqrt(np.mean([(f - a) ** 2 for f, a in zip(fitted, self.yields)])))

        score_info = _compute_confidence_score(spread_bps, rmse)

        return {
            "targets": targets,
            "upper": upper,
            "lower": lower,
            "center": center,
            "spread_bps": spread_bps,
            "nss_excluded": nss_excluded,
            "confidence": score_info["label"],
            "confidence_score": score_info["score"],
            "confidence_color": score_info["color"],
            "rmse_penalty": score_info["rmse_penalty"],
            "spread_penalty": score_info["spread_penalty"],
            "max_spread_bps": score_info["max_spread_bps"],
        }


def _compute_confidence_score(
    band_width_bps: list[float], rmse: float | None = None,
) -> dict:
    """Spread-weighted confidence score (Step 9a).

    Score = 100 - RMSE_penalty(0-30) - Spread_penalty(0-70).
    """
    max_spread = max(band_width_bps) if band_width_bps else 0.0

    # RMSE penalty (0-30)
    if rmse is None or rmse > 0.5:
        rmse_penalty = 30.0
    else:
        rmse_penalty = min(30.0, rmse * 300)

    # Spread penalty (0-70) — non-linear
    if max_spread <= 0:
        spread_penalty = 0.0
    elif max_spread <= 50:
        spread_penalty = max_spread / 5
    elif max_spread <= 200:
        spread_penalty = 10 + (max_spread - 50) / 6
    elif max_spread <= 500:
        spread_penalty = 35 + (max_spread - 200) / 15
    else:
        spread_penalty = min(70.0, 55 + (max_spread - 500) / 33)

    score = max(0.0, 100 - rmse_penalty - spread_penalty)

    if score >= 80:
        label, color = "EXCELLENT", "#00E676"
    elif score >= 60:
        label, color = "GOOD", "#FFB300"
    elif score >= 40:
        label, color = "FAIR", "#FF9800"
    elif score >= 20:
        label, color = "LOW", "#FF5252"
    else:
        label, color = "UNRELIABLE", "#FF1744"

    return {
        "score": round(score, 1),
        "label": label,
        "color": color,
        "rmse_penalty": round(rmse_penalty, 1),
        "spread_penalty": round(spread_penalty, 1),
        "max_spread_bps": round(max_spread, 1),
    }


def rmse_rating(rmse_bps: float) -> tuple[str, str]:
    """Return (label, color) for RMSE quality badge."""
    if rmse_bps < 5:
        return "EXCELLENT", "#00E676"
    elif rmse_bps < 10:
        return "GOOD", "#C8A96E"
    elif rmse_bps < 20:
        return "FAIR", "#FFB300"
    else:
        return "POOR", "#FF5252"


def fit_nss_anchored(
    tenors_y: np.ndarray,
    yields: np.ndarray,
    anchors: dict | None = None,
) -> np.ndarray | None:
    """Fit NSS with auction anchor points (3x weighted).

    Anchors: {tenor_years: yield_pct}, e.g. {15: 12.40, 10: 12.50}.
    These prevent NSS tail from wandering when there's no data beyond 20Y.
    """
    try:
        from scipy.optimize import minimize
    except ImportError:
        return None

    all_t = list(tenors_y)
    all_y = list(yields)
    all_w = [1.0] * len(tenors_y)

    if anchors:
        for t, y in anchors.items():
            all_t.append(float(t))
            all_y.append(float(y))
            all_w.append(3.0)

    all_t = np.array(all_t)
    all_y = np.array(all_y)
    all_w = np.array(all_w)
    idx = np.argsort(all_t)
    all_t, all_y, all_w = all_t[idx], all_y[idx], all_w[idx]

    def objective(params):
        b0, b1, b2, b3, tau1, tau2 = params
        if tau1 <= 0.01 or tau2 <= 0.01:
            return 1e10
        return np.mean([w * (_nss_yield(t, params) - y) ** 2
                        for t, y, w in zip(all_t, all_y, all_w)])

    best_result, best_error = None, 1e10
    long_rate, short_rate = all_y[-1], all_y[0]

    for tau1_init in [1.0, 2.0, 3.0, 5.0]:
        for tau2_init in [3.0, 5.0, 8.0, 15.0]:
            if tau2_init <= tau1_init:
                continue
            x0 = [long_rate, short_rate - long_rate, 0, 0, tau1_init, tau2_init]
            try:
                result = minimize(objective, x0, method="Nelder-Mead",
                                  options={"maxiter": 5000, "xatol": 1e-6})
                if result.fun < best_error:
                    best_error = result.fun
                    best_result = result
            except Exception:
                continue

    return best_result.x if best_result is not None else None


def source_convergence(
    db_path: str | Path, date_str: str, tenor: str = "10Y",
) -> list[dict]:
    """Cross-check a single tenor across all sovereign_curve sources."""
    import sqlite3
    from datetime import datetime as _dt

    con = sqlite3.connect(str(db_path))
    rows = con.execute(
        "SELECT source, date, yield_pct FROM sovereign_curve "
        "WHERE tenor = ? AND date >= date(?, '-30 days') "
        "ORDER BY source, date DESC",
        (tenor, date_str),
    ).fetchall()
    con.close()

    seen: dict = {}
    for source, date, yield_pct in rows:
        if source in seen:
            continue
        days_old = (_dt.strptime(date_str, "%Y-%m-%d") - _dt.strptime(date, "%Y-%m-%d")).days
        if "_SYN" in source:
            status = "calculated"
        elif days_old == 0:
            status = "verified"
        elif days_old <= 3:
            status = "recent"
        elif days_old <= 7:
            status = "lagging"
        else:
            status = "stale"

        seen[source] = {
            "source": source, "yield_pct": yield_pct,
            "date": date, "days_old": days_old, "status": status,
        }

    return sorted(seen.values(), key=lambda x: x["source"])


def z_spread(
    sovereign_yields: callable,
    bond_price: float,
    coupon_rate: float,
    maturity_years: float,
    face_value: float = 100.0,
    coupon_freq: int = 2,
) -> float:
    """Z-Spread: constant spread over sovereign curve making DCF = price.

    Returns spread in basis points.
    """
    try:
        from scipy.optimize import brentq
    except ImportError:
        return 0.0

    periods = int(maturity_years * coupon_freq)
    coupon = face_value * (coupon_rate / 100) / coupon_freq

    def price_with_spread(z):
        pv = 0.0
        for i in range(1, periods + 1):
            t = i / coupon_freq
            r = sovereign_yields(t) / 100
            discount = (1 + (r + z / 100) / coupon_freq) ** i
            cf = coupon if i < periods else coupon + face_value
            pv += cf / discount
        return pv - bond_price

    try:
        z = brentq(price_with_spread, -5, 50, xtol=0.001)
        return round(z * 100, 1)
    except (ValueError, RuntimeError):
        return 0.0


def fair_value_check(
    instrument_yield: float,
    curve_yield: float,
    instrument_type: str = "sovereign",
) -> dict:
    """Check if instrument yield deviates from curve-implied fair value."""
    spread_bps = (instrument_yield - curve_yield) * 100

    if instrument_type == "sovereign":
        thresh = 10
    else:
        thresh = 30

    if abs(spread_bps) < thresh:
        return {"spread_bps": spread_bps, "signal": "FAIR", "severity": 0}
    elif spread_bps > 0:
        sev = 1 if spread_bps < thresh * 3 else 2
        return {"spread_bps": spread_bps, "signal": "CHEAP", "severity": sev}
    else:
        sev = 1 if abs(spread_bps) < thresh * 3 else 2
        return {"spread_bps": spread_bps, "signal": "RICH", "severity": sev}


def persist_synthetic_rates(
    db_path: str | Path,
    source: str = "PKRV",
    date_str: str | None = None,
) -> dict:
    """Compute and store synthetic rates for missing tenors.

    Stores with source='{SOURCE}_SYN' (e.g. PKRV_SYN, PKISRV_SYN)
    so synthetic values can be tracked over time without mixing with official.
    Also stores NSS RMSE as metadata row with tenor='_RMSE'.
    """
    import sqlite3

    con = sqlite3.connect(str(db_path), timeout=30)

    if date_str is None:
        row = con.execute(
            "SELECT MAX(date) FROM sovereign_curve WHERE source = ?", (source,)
        ).fetchone()
        date_str = row[0] if row and row[0] else None
        if not date_str:
            con.close()
            return {"status": "no_data"}

    # Load official curve
    rows = con.execute(
        "SELECT days, yield_pct, tenor FROM sovereign_curve "
        "WHERE date = ? AND source = ? ORDER BY days",
        (date_str, source),
    ).fetchall()

    if len(rows) < 3:
        con.close()
        return {"status": "insufficient_data", "points": len(rows)}

    tenors_y = [r[0] / 365.25 for r in rows]
    yields = [r[1] for r in rows]
    official_tenors = {r[0] / 365.25 for r in rows}

    ca = CurveAnalytics(tenors_y, yields)
    full = ca.full_curve()

    syn_source = f"{source}_SYN"
    inserted = 0

    # NOTE(market-sync-v1): engine writes synthetic (_SYN / _RMSE) rows into
    # the sovereign_curve base table — intentional layering exception because
    # the consolidated curve carries both observed and model-derived points.
    # Future cleanup: migrate to a dedicated sovereign_curve_synthetic table
    # so sovereign_curve contains only source-observed data.
    # Insert synthetic rates for missing tenors
    for i, t in enumerate(full["targets"]):
        if t in official_tenors:
            continue
        # Use spline as primary synthetic value
        yield_val = full["spline"][i]
        label = FULL_TENORS_LABELS[i] if i < len(FULL_TENORS_LABELS) else f"{t}Y"
        days = int(t * 365.25)

        con.execute(
            "INSERT OR REPLACE INTO sovereign_curve "
            "(date, source, tenor, days, yield_pct) VALUES (?, ?, ?, ?, ?)",
            (date_str, syn_source, label, days, round(yield_val, 4)),
        )
        inserted += 1

    # Store NSS RMSE as metadata
    if full.get("nss_rmse") is not None:
        con.execute(
            "INSERT OR REPLACE INTO sovereign_curve "
            "(date, source, tenor, days, yield_pct) VALUES (?, ?, '_RMSE', 0, ?)",
            (date_str, syn_source, round(full["nss_rmse"] * 100, 2)),
        )

    con.commit()
    con.close()

    return {
        "status": "ok",
        "date": date_str,
        "source": syn_source,
        "inserted": inserted,
        "nss_rmse_bps": round(full["nss_rmse"] * 100, 2) if full.get("nss_rmse") else None,
    }
