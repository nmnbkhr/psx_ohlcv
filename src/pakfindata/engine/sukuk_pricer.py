"""Sukuk Fair Value Engine — auto-pricing using PKISRV yield curve.

Discounts Islamic bond cash flows using the synthetic sovereign curve,
with confidence-aware valuation and MUFAP comparison.

Usage:
    from pakfindata.engine.sukuk_pricer import SukukPricer

    pricer = SukukPricer(curve_date="2026-04-14", curve_source="PKISRV")
    result = pricer.price_isin(isin="PK01296...", coupon=13.5, maturity_date="2031-05-30")
"""

from __future__ import annotations

import calendar
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

logger = logging.getLogger("sukuk_pricer")
PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

DB_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class CashFlow:
    date: datetime
    years_to_cf: float
    amount: float
    is_final: bool
    spot_rate: float = 0.0
    discount_factor: float = 1.0
    present_value: float = 0.0


@dataclass
class PricingResult:
    isin: str
    name: str
    valuation_date: str
    maturity_date: str
    tenor_years: float
    coupon_rate: float
    frequency: int
    interpolated_yield: float
    dirty_price: float
    accrued_interest: float
    clean_price: float
    ytm: float
    modified_duration: float
    mufap_price: float | None = None
    variance_bps: float | None = None
    curve_confidence: int = 0
    valuation_status: str = "N/A"
    confidence_reason: str = ""
    cash_flows: list = field(default_factory=list)
    n_cash_flows: int = 0

    def to_dict(self) -> dict:
        return {
            "isin": self.isin,
            "name": self.name,
            "tenor": f"{self.tenor_years:.1f}Y",
            "coupon": f"{self.coupon_rate:.2f}%",
            "yield": f"{self.interpolated_yield:.4f}%",
            "dirty_price": round(self.dirty_price, 4),
            "accrued": round(self.accrued_interest, 4),
            "clean_price": round(self.clean_price, 4),
            "ytm": f"{self.ytm:.4f}%",
            "mod_duration": round(self.modified_duration, 3),
            "mufap_price": self.mufap_price,
            "variance_bps": self.variance_bps,
            "confidence": self.curve_confidence,
            "status": self.valuation_status,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# DAY COUNT + CASH FLOW HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def year_fraction(d1: datetime, d2: datetime, convention: str = "ACT/365") -> float:
    days = (d2 - d1).days
    if convention == "ACT/360":
        return days / 360.0
    elif convention == "30/360":
        y1, m1, d1d = d1.year, d1.month, min(d1.day, 30)
        y2, m2, d2d = d2.year, d2.month, min(d2.day, 30)
        return (360 * (y2 - y1) + 30 * (m2 - m1) + (d2d - d1d)) / 360.0
    return days / 365.0


def _prev_date(d: datetime, period_months: int) -> datetime:
    month = d.month - period_months
    year = d.year
    while month <= 0:
        month += 12
        year -= 1
    last_day = calendar.monthrange(year, month)[1]
    return d.replace(year=year, month=month, day=min(d.day, last_day))


def generate_cash_flows(
    valuation_date: datetime,
    maturity_date: datetime,
    coupon_rate: float,
    face_value: float = 100.0,
    frequency: int = 2,
    day_count: str = "ACT/365",
) -> list[CashFlow]:
    period_months = 12 // frequency
    coupon_per_period = face_value * (coupon_rate / 100) / frequency

    coupon_dates: list[datetime] = []
    d = maturity_date
    while d > valuation_date:
        coupon_dates.append(d)
        d = _prev_date(d, period_months)

    coupon_dates.sort()

    cfs: list[CashFlow] = []
    for cd in coupon_dates:
        is_final = cd == maturity_date
        amount = coupon_per_period + (face_value if is_final else 0)
        yrs = year_fraction(valuation_date, cd, day_count)
        if yrs > 0:
            cfs.append(CashFlow(date=cd, years_to_cf=yrs, amount=amount, is_final=is_final))
    return cfs


def calc_accrued_interest(
    valuation_date: datetime,
    maturity_date: datetime,
    coupon_rate: float,
    face_value: float = 100.0,
    frequency: int = 2,
    day_count: str = "ACT/365",
) -> float:
    period_months = 12 // frequency
    coupon_per_period = face_value * (coupon_rate / 100) / frequency

    d = maturity_date
    last_coupon = None
    next_coupon = None
    while d > valuation_date - timedelta(days=400):
        if d <= valuation_date:
            last_coupon = d
            break
        next_coupon = d
        d = _prev_date(d, period_months)

    if last_coupon is None:
        return 0.0

    if next_coupon:
        period_days = (next_coupon - last_coupon).days
        accrued_days = (valuation_date - last_coupon).days
        if period_days > 0:
            return coupon_per_period * (accrued_days / period_days)

    accrued_days = (valuation_date - last_coupon).days
    denom = 365 if "365" in day_count else 360
    return face_value * (coupon_rate / 100) * accrued_days / denom


def calc_modified_duration(cash_flows: list[CashFlow], ytm: float, frequency: int = 2) -> float:
    if not cash_flows or ytm <= 0:
        return 0.0
    total_pv = sum(cf.present_value for cf in cash_flows)
    if total_pv <= 0:
        return 0.0
    mac_dur = sum(cf.years_to_cf * cf.present_value for cf in cash_flows) / total_pv
    return mac_dur / (1 + (ytm / 100) / frequency)


def calc_ytm(clean_price: float, cash_flows: list[CashFlow], face_value: float = 100.0) -> float:
    try:
        from scipy.optimize import brentq
    except ImportError:
        return 0.0

    def price_diff(y):
        return sum(cf.amount / (1 + y / 200) ** (cf.years_to_cf * 2) for cf in cash_flows) - clean_price

    try:
        return brentq(price_diff, -0.05, 0.50, xtol=0.00001) * 100
    except (ValueError, RuntimeError):
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# THE PRICER
# ═══════════════════════════════════════════════════════════════════════════════

class SukukPricer:
    """Auto-prices Sukuk using the PKISRV yield curve.

    Confidence-aware: uses PKRV-anchored rate for tenors >20Y,
    flags valuations where curve confidence <40/100.
    """

    def __init__(self, curve_date: str | None = None, curve_source: str = "PKISRV"):
        from pakfindata.engine.curve_analytics import CurveAnalytics

        self.curve_source = curve_source
        self.curve_date = curve_date or datetime.now(PKT).strftime("%Y-%m-%d")

        con = sqlite3.connect(str(DB_PATH))

        df = self._load_curve_data(con, self.curve_date, curve_source)

        self.pkrv_anchor: dict[float, float] = {}
        if curve_source != "PKRV":
            pkrv_df = self._load_curve_data(con, self.curve_date, "PKRV")
            if not pkrv_df.empty:
                for _, row in pkrv_df.iterrows():
                    self.pkrv_anchor[row["days"] / 365.25] = row["yield_pct"]

        con.close()

        if df.empty:
            raise ValueError(f"No {curve_source} data for {self.curve_date}")

        tenors_y = (df["days"] / 365.25).values
        yields = df["yield_pct"].values
        labels = df["tenor"].values.tolist()

        self.ca = CurveAnalytics(tenors_y.tolist(), yields.tolist(), labels)
        self.full = self.ca.full_curve()
        self.band = self.ca.confidence_band()

        try:
            from scipy.interpolate import CubicSpline
            self._spline = CubicSpline(
                np.array(self.full["targets"]),
                np.array(self.full["spline"]),
                bc_type="natural",
            )
        except ImportError:
            self._spline = None

    @staticmethod
    def _load_curve_data(con, date_str, source):
        import pandas as pd
        return pd.read_sql_query(
            "SELECT tenor, days, yield_pct FROM sovereign_curve "
            "WHERE date = ? AND source = ? ORDER BY days",
            con, params=[date_str, source],
        )

    def spot_rate(self, tenor_years: float) -> tuple[float, int, str]:
        """Get interpolated spot rate. Returns (yield, confidence, method)."""
        targets = self.band["targets"]
        confidence = 100

        nearest_idx = min(range(len(targets)), key=lambda i: abs(targets[i] - tenor_years))
        if abs(targets[nearest_idx] - tenor_years) < 0.5:
            width = self.band["spread_bps"][nearest_idx]
            confidence = max(0, int(100 - width * 0.4))

        max_official = max(self.full["official"].keys()) if self.full["official"] else 0

        if tenor_years <= max_official:
            if self._spline is not None:
                rate = float(self._spline(tenor_years))
            else:
                from pakfindata.engine.curve_analytics import linear_extrapolate
                rate = linear_extrapolate(
                    np.array(list(self.full["official"].keys())),
                    np.array(list(self.full["official"].values())),
                    tenor_years,
                )
            return rate, confidence, "spline"

        elif self.pkrv_anchor:
            nearest_pkrv = min(self.pkrv_anchor.keys(), key=lambda t: abs(t - tenor_years))
            if abs(nearest_pkrv - tenor_years) < 2:
                pkrv_rate = self.pkrv_anchor[nearest_pkrv]
                islamic_spread = self._calc_islamic_spread()
                rate = pkrv_rate + islamic_spread
                return rate, min(confidence, 50), "pkrv_anchored"

        # Fallback: spline extrapolation
        if self._spline is not None:
            boundary = max_official
            bv = float(self._spline(boundary))
            bd = float(self._spline(boundary, 1))
            rate = bv + bd * (tenor_years - boundary)
        else:
            from pakfindata.engine.curve_analytics import linear_extrapolate
            rate = linear_extrapolate(
                np.array(list(self.full["official"].keys())),
                np.array(list(self.full["official"].values())),
                tenor_years,
            )
        return rate, min(confidence, 30), "extrapolated"

    def _calc_islamic_spread(self) -> float:
        spreads = []
        for t, y in self.full["official"].items():
            if t in self.pkrv_anchor:
                spreads.append(y - self.pkrv_anchor[t])
        return float(np.median(spreads)) if spreads else 0.60

    def price_isin(
        self,
        isin: str,
        coupon: float,
        maturity_date: str,
        face_value: float = 100.0,
        frequency: int = 2,
        day_count: str = "ACT/365",
        name: str = "",
        mufap_price: float | None = None,
    ) -> PricingResult:
        val_date = datetime.strptime(self.curve_date, "%Y-%m-%d")
        mat_date = datetime.strptime(maturity_date, "%Y-%m-%d")
        tenor_years = year_fraction(val_date, mat_date, day_count)

        if tenor_years <= 0:
            return PricingResult(
                isin=isin, name=name, valuation_date=self.curve_date,
                maturity_date=maturity_date, tenor_years=0,
                coupon_rate=coupon, frequency=frequency,
                interpolated_yield=0, dirty_price=face_value,
                accrued_interest=0, clean_price=face_value,
                ytm=0, modified_duration=0, valuation_status="Matured",
            )

        spot, confidence, method = self.spot_rate(tenor_years)

        cfs = generate_cash_flows(val_date, mat_date, coupon, face_value, frequency, day_count)
        for cf in cfs:
            cf_spot, _, _ = self.spot_rate(cf.years_to_cf)
            cf.spot_rate = cf_spot
            cf.discount_factor = 1 / (1 + cf_spot / 100) ** cf.years_to_cf
            cf.present_value = cf.amount * cf.discount_factor

        dirty_price = sum(cf.present_value for cf in cfs)
        accrued = calc_accrued_interest(val_date, mat_date, coupon, face_value, frequency, day_count)
        clean_price = dirty_price - accrued

        ytm = calc_ytm(clean_price, cfs, face_value)
        mod_dur = calc_modified_duration(cfs, ytm, frequency)

        variance = None
        if mufap_price is not None and mufap_price > 0:
            variance = round((clean_price - mufap_price) / mufap_price * 10000, 1)

        if confidence >= 60:
            status = "Reliable"
            reason = f"Curve confidence {confidence}/100, method: {method}"
        elif confidence >= 40:
            status = "Indicative"
            reason = f"Moderate uncertainty ({confidence}/100), method: {method}"
        else:
            status = "Indicative"
            reason = f"High uncertainty ({confidence}/100), method: {method}"

        if variance is not None and abs(variance) > 200:
            status = "Outlier"
            reason += f", MUFAP variance: {variance:+.0f} bps"

        return PricingResult(
            isin=isin, name=name, valuation_date=self.curve_date,
            maturity_date=maturity_date, tenor_years=round(tenor_years, 2),
            coupon_rate=coupon, frequency=frequency,
            interpolated_yield=round(spot, 4),
            dirty_price=round(dirty_price, 4),
            accrued_interest=round(accrued, 4),
            clean_price=round(clean_price, 4),
            ytm=round(ytm, 4),
            modified_duration=round(mod_dur, 3),
            mufap_price=mufap_price, variance_bps=variance,
            curve_confidence=confidence, valuation_status=status,
            confidence_reason=reason, cash_flows=cfs, n_cash_flows=len(cfs),
        )

    def price_portfolio(self, instruments: list[dict]) -> list[PricingResult]:
        results = []
        for inst in instruments:
            try:
                results.append(self.price_isin(
                    isin=inst.get("isin", "UNKNOWN"),
                    coupon=inst.get("coupon", inst.get("rental_rate", 0)),
                    maturity_date=inst.get("maturity_date", "2030-01-01"),
                    face_value=inst.get("face_value", 100),
                    frequency=inst.get("frequency", 2),
                    day_count=inst.get("day_count", "ACT/365"),
                    name=inst.get("name", ""),
                    mufap_price=inst.get("mufap_price"),
                ))
            except Exception as e:
                logger.error("Failed to price %s: %s", inst.get("isin"), e)
        return results
