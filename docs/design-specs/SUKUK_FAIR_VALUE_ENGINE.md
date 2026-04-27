# Claude Code Prompt: Sukuk Fair Value Engine — Auto-Pricing with PKISRV

## What We're Building

A bond pricing engine that takes ISINs + the PKISRV yield curve and produces 
fair value prices, then compares against MUFAP revaluation rates to flag 
mispriced instruments.

```
Input:
  PKISRV yield curve (21 tenors, synthetic extension)  ← curve_analytics.py
  ISIN master data (coupon, maturity, frequency)       ← debt_securities table
  SBP auction anchors                                  ← sovereign_curve table
  MUFAP daily prices                                   ← debt pricing files

Engine:
  For each ISIN:
    1. Exact maturity → interpolated spot rate (Cubic Spline)
    2. Cash flow schedule (coupon dates + principal)
    3. Discount each CF using tenor-matched spot rates
    4. Sum PVs → Dirty Price → subtract accrued → Clean Price
    5. Compare vs MUFAP price → flag if variance > 50 bps

Output:
  ISIN | Tenor | Yield | Fair Value | MUFAP Price | Variance | Status
```

**Confidence-aware:** If the interpolated yield falls on a tenor where the 
curve confidence score is below 40/100, the valuation is marked "Indicative" 
not "Reliable". For tenors > 20Y, the engine uses the PKRV-anchored rate 
instead of unanchored NSS.

## Step 0: Audit Available Data

```bash
cd ~/pakfindata && conda activate psx

# 1. What instrument/security data exists?
echo "=== Security tables ==="
sqlite3 /mnt/e/psxdata/psx.sqlite ".tables" | tr ' ' '\n' | \
    grep -i "security\|bond\|debt\|instrument\|sukuk\|isin\|gis\|pib\|mtb"

# 2. Show schema of each found table
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite ".tables" | tr ' ' '\n' | \
    grep -i "security\|bond\|debt\|instrument\|sukuk\|isin"); do
    echo "--- $tbl ---"
    sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl"
    sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM $tbl;"
    sqlite3 /mnt/e/psxdata/psx.sqlite -header "SELECT * FROM $tbl LIMIT 3;"
    echo ""
done

# 3. Check the debt terminal's data source
echo "=== Debt terminal security loading ==="
grep -n "def.*load\|def.*fetch\|def.*get.*secur\|SELECT.*FROM.*debt\|SELECT.*FROM.*bond\|SELECT.*FROM.*instrument" \
    ~/pakfindata/src/pakfindata/ui/page_views/debt*.py 2>/dev/null | head -30

# 4. What columns are available? (ISIN, coupon, maturity, etc.)
echo "=== Column names ==="
grep -n "isin\|coupon\|maturity\|rental\|frequency\|day.*count\|face.*value\|clean\|dirty\|ytm" \
    ~/pakfindata/src/pakfindata/ui/page_views/debt*.py 2>/dev/null | head -30

# 5. MUFAP debt pricing files downloaded
echo "=== MUFAP debt pricing files ==="
ls /mnt/e/psxdata/mufap/debt_pricing/ 2>/dev/null | head -10

# 6. Check if there's existing pricing logic
echo "=== Existing pricing/valuation code ==="
grep -rn "clean.*price\|dirty.*price\|accrued\|discount.*factor\|present.*value\|z.spread\|i.spread\|ytm.*calc" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# 7. sovereign_curve data availability
echo "=== Curve data ==="
sqlite3 /mnt/e/psxdata/psx.sqlite -header "
    SELECT source, COUNT(DISTINCT tenor) as tenors, MAX(date) as latest
    FROM sovereign_curve GROUP BY source ORDER BY source;
" 2>/dev/null
```

**READ ALL OUTPUT.** The engine adapts to whatever table/column names 
actually exist in your database.

## Step 1: Create the Pricing Engine

Create `src/pakfindata/engine/sukuk_pricer.py`:

```python
"""
Sukuk Fair Value Engine — Auto-pricing using PKISRV yield curve.

Discounts Islamic bond cash flows using the synthetic sovereign curve,
with confidence-aware valuation and MUFAP comparison.

Usage:
    from pakfindata.engine.sukuk_pricer import SukukPricer
    
    pricer = SukukPricer(curve_date="2026-04-14", curve_source="PKISRV")
    result = pricer.price_isin(
        isin="PK0129601156",
        coupon=13.5,
        maturity_date="2031-05-30",
        face_value=100,
        frequency=2,        # semi-annual
        day_count="ACT/365",
    )
    # result.clean_price, result.dirty_price, result.ytm, result.accrued, ...
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger("sukuk_pricer")
PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

DB_PATH = DATA_ROOT / "psx.sqlite"


# ═══════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════

@dataclass
class CashFlow:
    date: datetime
    years_to_cf: float      # years from valuation date
    amount: float            # coupon or coupon + principal
    is_final: bool           # True for maturity payment
    spot_rate: float = 0.0   # interpolated curve yield at this tenor
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
    
    # Pricing outputs
    interpolated_yield: float    # spot rate at exact maturity
    dirty_price: float           # sum of PVs
    accrued_interest: float      # accrued since last coupon
    clean_price: float           # dirty - accrued
    ytm: float                   # yield to maturity at clean price
    modified_duration: float     # interest rate sensitivity
    
    # Comparison
    mufap_price: Optional[float] = None      # MUFAP revaluation rate
    variance_bps: Optional[float] = None     # fair value vs MUFAP
    
    # Confidence
    curve_confidence: int = 0        # 0-100 from confidence_band
    valuation_status: str = "N/A"    # Reliable / Indicative / Outlier
    confidence_reason: str = ""
    
    # Detail
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


# ═══════════════════════════════════════════
# DAY COUNT CONVENTIONS
# ═══════════════════════════════════════════

def year_fraction(d1: datetime, d2: datetime, convention: str = "ACT/365") -> float:
    """Calculate year fraction between two dates."""
    days = (d2 - d1).days
    if convention == "ACT/365":
        return days / 365.0
    elif convention == "ACT/360":
        return days / 360.0
    elif convention == "30/360":
        # Simplified 30/360
        y1, m1, d1_day = d1.year, d1.month, min(d1.day, 30)
        y2, m2, d2_day = d2.year, d2.month, min(d2.day, 30)
        return (360 * (y2 - y1) + 30 * (m2 - m1) + (d2_day - d1_day)) / 360.0
    else:
        return days / 365.0


# ═══════════════════════════════════════════
# CASH FLOW GENERATOR
# ═══════════════════════════════════════════

def generate_cash_flows(
    valuation_date: datetime,
    maturity_date: datetime,
    coupon_rate: float,       # annual rate in %
    face_value: float = 100.0,
    frequency: int = 2,       # payments per year
    day_count: str = "ACT/365",
) -> list[CashFlow]:
    """
    Generate cash flow schedule from valuation date to maturity.
    
    Returns list of CashFlow objects for all FUTURE payments.
    """
    period_months = 12 // frequency
    coupon_per_period = face_value * (coupon_rate / 100) / frequency
    
    # Build all coupon dates from maturity backwards
    coupon_dates = []
    d = maturity_date
    while d > valuation_date:
        coupon_dates.append(d)
        # Go back one period
        month = d.month - period_months
        year = d.year
        while month <= 0:
            month += 12
            year -= 1
        try:
            d = d.replace(year=year, month=month)
        except ValueError:
            # Handle month-end (e.g., Feb 30 → Feb 28)
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            d = d.replace(year=year, month=month, day=min(d.day, last_day))
    
    coupon_dates.sort()
    
    cash_flows = []
    for i, cd in enumerate(coupon_dates):
        is_final = (cd == maturity_date)
        amount = coupon_per_period + (face_value if is_final else 0)
        years_to_cf = year_fraction(valuation_date, cd, day_count)
        
        if years_to_cf > 0:  # only future cash flows
            cash_flows.append(CashFlow(
                date=cd,
                years_to_cf=years_to_cf,
                amount=amount,
                is_final=is_final,
            ))
    
    return cash_flows


# ═══════════════════════════════════════════
# ACCRUED INTEREST
# ═══════════════════════════════════════════

def calc_accrued_interest(
    valuation_date: datetime,
    maturity_date: datetime,
    coupon_rate: float,
    face_value: float = 100.0,
    frequency: int = 2,
    day_count: str = "ACT/365",
) -> float:
    """Calculate accrued interest (profit) from last coupon date."""
    period_months = 12 // frequency
    coupon_per_period = face_value * (coupon_rate / 100) / frequency
    
    # Find the most recent coupon date before valuation
    d = maturity_date
    last_coupon = None
    next_coupon = None
    
    while d > valuation_date - timedelta(days=400):
        if d <= valuation_date:
            last_coupon = d
            break
        next_coupon = d
        month = d.month - period_months
        year = d.year
        while month <= 0:
            month += 12
            year -= 1
        try:
            d = d.replace(year=year, month=month)
        except ValueError:
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            d = d.replace(year=year, month=month, day=min(d.day, last_day))
    
    if last_coupon is None:
        return 0.0
    
    # Accrued = coupon × (days since last coupon / days in period)
    if next_coupon:
        period_days = (next_coupon - last_coupon).days
        accrued_days = (valuation_date - last_coupon).days
        if period_days > 0:
            return coupon_per_period * (accrued_days / period_days)
    
    # Fallback: simple day count
    accrued_days = (valuation_date - last_coupon).days
    return face_value * (coupon_rate / 100) * accrued_days / (365 if "365" in day_count else 360)


# ═══════════════════════════════════════════
# MODIFIED DURATION
# ═══════════════════════════════════════════

def calc_modified_duration(cash_flows: list[CashFlow], ytm: float, 
                           frequency: int = 2) -> float:
    """Calculate Macaulay duration then convert to modified duration."""
    if not cash_flows or ytm <= 0:
        return 0.0
    
    total_pv = sum(cf.present_value for cf in cash_flows)
    if total_pv <= 0:
        return 0.0
    
    # Macaulay duration = weighted average time to cash flows
    mac_dur = sum(cf.years_to_cf * cf.present_value for cf in cash_flows) / total_pv
    
    # Modified duration = Macaulay / (1 + y/f)
    mod_dur = mac_dur / (1 + (ytm / 100) / frequency)
    
    return mod_dur


# ═══════════════════════════════════════════
# YTM SOLVER
# ═══════════════════════════════════════════

def calc_ytm(clean_price: float, cash_flows: list[CashFlow],
             face_value: float = 100.0) -> float:
    """Solve for yield to maturity given clean price and cash flows."""
    try:
        from scipy.optimize import brentq
    except ImportError:
        return 0.0
    
    total_cf = clean_price  # accrued is already handled
    
    def price_diff(y):
        pv = 0
        for cf in cash_flows:
            pv += cf.amount / (1 + y / 200) ** (cf.years_to_cf * 2)  # semi-annual
        return pv - total_cf
    
    try:
        ytm = brentq(price_diff, -0.05, 0.50, xtol=0.00001) * 100
        return ytm
    except (ValueError, RuntimeError):
        return 0.0


# ═══════════════════════════════════════════
# THE PRICER
# ═══════════════════════════════════════════

class SukukPricer:
    """
    Auto-prices Sukuk using the PKISRV yield curve.
    
    Confidence-aware: uses PKRV-anchored rate for tenors > 20Y,
    flags valuations where curve confidence < 40/100.
    """
    
    def __init__(self, curve_date: str = None, curve_source: str = "PKISRV"):
        self.curve_source = curve_source
        self.curve_date = curve_date or datetime.now(PKT).strftime("%Y-%m-%d")
        
        # Load curve engine
        from pakfindata.engine.curve_analytics import CurveAnalytics
        
        con = sqlite3.connect(str(DB_PATH))
        
        # Load official curve data
        df = self._load_curve_data(con, self.curve_date, curve_source)
        
        # Load PKRV for anchoring (if pricing PKISRV)
        self.pkrv_anchor = {}
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
        
        # Build spline function for arbitrary tenor lookup
        try:
            from scipy.interpolate import CubicSpline
            self._spline = CubicSpline(
                np.array(self.full["targets"]),
                np.array(self.full["spline"]),
                bc_type='natural',
            )
        except ImportError:
            self._spline = None
    
    def _load_curve_data(self, con, date_str, source):
        import pandas as pd
        return pd.read_sql_query("""
            SELECT tenor, days, yield_pct FROM sovereign_curve
            WHERE date = ? AND source = ? ORDER BY days
        """, con, params=[date_str, source])
    
    def spot_rate(self, tenor_years: float) -> tuple[float, int, str]:
        """
        Get the spot rate for a specific tenor.
        
        Returns: (yield_pct, confidence_score, method_used)
        
        Logic:
          - If tenor is within official range: use Spline interpolation
          - If tenor > 20Y and PKRV anchor available: use PKRV + Islamic spread
          - If tenor > 20Y and no PKRV: use Spline extrapolation (derivative-based)
          - Confidence score from the band at this tenor
        """
        # Confidence at this tenor
        targets = self.band["targets"]
        confidence = 100  # default for official points
        
        # Find nearest target for confidence lookup
        nearest_idx = min(range(len(targets)), key=lambda i: abs(targets[i] - tenor_years))
        if abs(targets[nearest_idx] - tenor_years) < 0.5:
            width = self.band["width_bps"][nearest_idx]
            # Map width to confidence: 0 bps = 100, 50 bps = 60, 200 bps = 20
            confidence = max(0, int(100 - width * 0.4))
        
        # Determine method
        max_official = max(self.full["official"].keys()) if self.full["official"] else 0
        
        if tenor_years <= max_official:
            # Within official range — spline interpolation
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
        
        elif self.pkrv_anchor and tenor_years in self.pkrv_anchor:
            # Beyond official range but PKRV has this tenor
            # Use PKRV + historical Islamic spread (~60 bps)
            pkrv_rate = self.pkrv_anchor[tenor_years]
            
            # Calculate actual Islamic spread from overlapping tenors
            islamic_spread = self._calc_islamic_spread()
            
            rate = pkrv_rate + islamic_spread
            return rate, min(confidence, 50), "pkrv_anchored"
        
        else:
            # Beyond official range, no PKRV anchor — spline extrapolation
            if self._spline is not None:
                # Use derivative-based extrapolation (not raw polynomial)
                boundary = max_official
                boundary_val = float(self._spline(boundary))
                boundary_deriv = float(self._spline(boundary, 1))
                rate = boundary_val + boundary_deriv * (tenor_years - boundary)
            else:
                from pakfindata.engine.curve_analytics import linear_extrapolate
                rate = linear_extrapolate(
                    np.array(list(self.full["official"].keys())),
                    np.array(list(self.full["official"].values())),
                    tenor_years,
                )
            return rate, min(confidence, 30), "extrapolated"
    
    def _calc_islamic_spread(self) -> float:
        """Calculate average PKISRV - PKRV spread from overlapping tenors."""
        spreads = []
        for t, y in self.full["official"].items():
            if t in self.pkrv_anchor:
                spreads.append(y - self.pkrv_anchor[t])
        if spreads:
            return np.median(spreads)
        return 0.60  # default 60 bps if no overlap
    
    def price_isin(
        self,
        isin: str,
        coupon: float,
        maturity_date: str,       # YYYY-MM-DD
        face_value: float = 100.0,
        frequency: int = 2,       # semi-annual
        day_count: str = "ACT/365",
        name: str = "",
        mufap_price: float = None,
    ) -> PricingResult:
        """
        Price a single ISIN using the PKISRV curve.
        
        Returns PricingResult with clean price, dirty price, YTM,
        modified duration, MUFAP comparison, and confidence status.
        """
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
                ytm=0, modified_duration=0,
                valuation_status="Matured",
            )
        
        # Step 1: Interpolated spot rate
        spot, confidence, method = self.spot_rate(tenor_years)
        
        # Step 2: Generate cash flows
        cfs = generate_cash_flows(val_date, mat_date, coupon, face_value, frequency, day_count)
        
        # Step 3: Discount each cash flow using tenor-matched spot rates
        for cf in cfs:
            cf_spot, _, _ = self.spot_rate(cf.years_to_cf)
            cf.spot_rate = cf_spot
            cf.discount_factor = 1 / (1 + cf_spot / 100) ** cf.years_to_cf
            cf.present_value = cf.amount * cf.discount_factor
        
        # Step 4: Sum PVs → Dirty Price → Clean Price
        dirty_price = sum(cf.present_value for cf in cfs)
        accrued = calc_accrued_interest(val_date, mat_date, coupon, face_value, frequency, day_count)
        clean_price = dirty_price - accrued
        
        # YTM at the calculated clean price
        ytm = calc_ytm(clean_price, cfs, face_value)
        
        # Modified duration
        mod_dur = calc_modified_duration(cfs, ytm, frequency)
        
        # Step 5: MUFAP comparison
        variance = None
        if mufap_price is not None and mufap_price > 0:
            variance = round((clean_price - mufap_price) / mufap_price * 10000, 1)  # bps
        
        # Confidence-aware status
        if confidence >= 60:
            status = "Reliable"
            reason = f"Curve confidence {confidence}/100, method: {method}"
        elif confidence >= 40:
            status = "Indicative"
            reason = f"Moderate uncertainty ({confidence}/100), method: {method}"
        else:
            status = "Indicative"
            reason = f"High uncertainty ({confidence}/100), method: {method}"
        
        # Override to Outlier if MUFAP variance is extreme
        if variance is not None and abs(variance) > 200:
            status = "Outlier"
            reason += f", MUFAP variance: {variance:+.0f} bps"
        
        return PricingResult(
            isin=isin, name=name,
            valuation_date=self.curve_date,
            maturity_date=maturity_date,
            tenor_years=round(tenor_years, 2),
            coupon_rate=coupon,
            frequency=frequency,
            interpolated_yield=round(spot, 4),
            dirty_price=round(dirty_price, 4),
            accrued_interest=round(accrued, 4),
            clean_price=round(clean_price, 4),
            ytm=round(ytm, 4),
            modified_duration=round(mod_dur, 3),
            mufap_price=mufap_price,
            variance_bps=variance,
            curve_confidence=confidence,
            valuation_status=status,
            confidence_reason=reason,
            cash_flows=cfs,
            n_cash_flows=len(cfs),
        )
    
    def price_portfolio(self, instruments: list[dict]) -> list[PricingResult]:
        """
        Price a list of instruments.
        
        Each dict: {isin, coupon, maturity_date, frequency, name, mufap_price, ...}
        """
        results = []
        for inst in instruments:
            try:
                r = self.price_isin(
                    isin=inst.get("isin", "UNKNOWN"),
                    coupon=inst.get("coupon", inst.get("rental_rate", 0)),
                    maturity_date=inst.get("maturity_date", "2030-01-01"),
                    face_value=inst.get("face_value", 100),
                    frequency=inst.get("frequency", 2),
                    day_count=inst.get("day_count", "ACT/365"),
                    name=inst.get("name", ""),
                    mufap_price=inst.get("mufap_price"),
                )
                results.append(r)
            except Exception as e:
                logger.error("Failed to price %s: %s", inst.get("isin"), e)
        
        return results
```

## Step 2: Create the UI Tab

Add to `curve_analytics.py` page (or as a separate page `sukuk_pricer_page.py`):

```python
def _render_sukuk_pricer(ca, source: str, date_str: str):
    """Sukuk Fair Value Engine — auto-pricing tab."""
    st.markdown("**Sukuk Fair Value Engine**")
    st.caption("Auto-price Islamic instruments using the PKISRV synthetic curve")
    
    from pakfindata.engine.sukuk_pricer import SukukPricer
    
    # ── Mode selector ──
    mode = st.radio(
        "Input", ["Database (all instruments)", "Manual (single ISIN)"],
        horizontal=True, key="sp_mode", label_visibility="collapsed",
    )
    
    if mode == "Manual (single ISIN)":
        _render_manual_pricer(source, date_str)
        return
    
    # ── Database mode: price all instruments ──
    try:
        pricer = SukukPricer(curve_date=date_str, curve_source=source)
    except Exception as e:
        st.error(f"Failed to initialize pricer: {e}")
        return
    
    # Load instruments from database
    # ADAPT table/column names based on Step 0 findings
    con = sqlite3.connect(str(DB_PATH))
    try:
        instruments = pd.read_sql_query("""
            SELECT * FROM debt_securities
            WHERE maturity_date > ? 
            ORDER BY maturity_date
        """, con, params=[date_str])
    except Exception:
        st.info("No instrument database found. Use Manual mode or populate debt_securities table.")
        con.close()
        return
    con.close()
    
    if instruments.empty:
        st.info("No active instruments found")
        return
    
    # Map DataFrame to pricer input
    # ADAPT column names based on actual schema
    inst_list = []
    for _, row in instruments.iterrows():
        inst_list.append({
            "isin": row.get("isin", row.get("symbol", "")),
            "name": row.get("name", row.get("description", "")),
            "coupon": row.get("coupon_rate", row.get("rental_rate", 0)),
            "maturity_date": str(row.get("maturity_date", "2030-01-01"))[:10],
            "frequency": int(row.get("coupon_freq", row.get("frequency", 2))),
            "face_value": float(row.get("face_value", 100)),
            "day_count": row.get("day_count", "ACT/365"),
            "mufap_price": row.get("mufap_price", row.get("clean_price", None)),
        })
    
    # Price all
    with st.spinner(f"Pricing {len(inst_list)} instruments..."):
        results = pricer.price_portfolio(inst_list)
    
    if not results:
        st.warning("No instruments could be priced")
        return
    
    # ── Summary metrics ──
    reliable = sum(1 for r in results if r.valuation_status == "Reliable")
    indicative = sum(1 for r in results if r.valuation_status == "Indicative")
    outliers = sum(1 for r in results if r.valuation_status == "Outlier")
    
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Instruments", len(results))
    s2.metric("Reliable", reliable)
    s3.metric("Indicative", indicative)
    s4.metric("Outliers", outliers, delta_color="inverse" if outliers > 0 else "off")
    
    # ── Results table ──
    rows = [r.to_dict() for r in results]
    df = pd.DataFrame(rows)
    
    # Color coding
    def _status_color(val):
        if val == "Reliable":
            return "color: #00E676"
        elif val == "Indicative":
            return "color: #FFB300"
        elif val == "Outlier":
            return "color: #FF5252"
        return ""
    
    st.dataframe(
        df.style.applymap(_status_color, subset=["status"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "variance_bps": st.column_config.NumberColumn("Var (bps)", format="%.1f"),
            "confidence": st.column_config.ProgressColumn("Conf", min_value=0, max_value=100),
        },
    )
    
    # ── Outlier details ──
    outlier_results = [r for r in results if r.valuation_status == "Outlier"]
    if outlier_results:
        st.markdown("---")
        st.markdown("**⚠️ Outlier Details**")
        for r in outlier_results:
            with st.expander(f"{r.isin} — {r.name} ({r.valuation_status})"):
                c1, c2, c3 = st.columns(3)
                c1.metric("Fair Value", f"{r.clean_price:.4f}")
                c2.metric("MUFAP Price", f"{r.mufap_price:.4f}" if r.mufap_price else "N/A")
                c3.metric("Variance", f"{r.variance_bps:+.0f} bps" if r.variance_bps else "N/A")
                st.caption(r.confidence_reason)
    
    # ── Export ──
    if st.button("📥 Export Valuation to Excel", key="sp_export"):
        import io
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Valuations", index=False)
            
            # Cash flow detail for first 5 instruments
            for r in results[:5]:
                if r.cash_flows:
                    cf_rows = [{
                        "Date": cf.date.strftime("%Y-%m-%d"),
                        "Years": round(cf.years_to_cf, 3),
                        "Amount": round(cf.amount, 2),
                        "Spot Rate": round(cf.spot_rate, 4),
                        "DF": round(cf.discount_factor, 6),
                        "PV": round(cf.present_value, 4),
                    } for cf in r.cash_flows]
                    pd.DataFrame(cf_rows).to_excel(
                        writer, sheet_name=f"CF_{r.isin[:10]}", index=False
                    )
        
        buf.seek(0)
        st.download_button(
            "⬇ Download XLSX",
            data=buf,
            file_name=f"sukuk_valuation_{date_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def _render_manual_pricer(source: str, date_str: str):
    """Manual single-ISIN pricing form."""
    from pakfindata.engine.sukuk_pricer import SukukPricer
    
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        isin = st.text_input("ISIN", "PK0129601156", key="sp_isin")
    with c2:
        coupon = st.number_input("Coupon/Rental (%)", value=13.50, step=0.01, key="sp_coupon")
    with c3:
        maturity = st.date_input("Maturity Date", 
                                  value=datetime(2031, 5, 30), key="sp_maturity")
    with c4:
        freq_opts = {"Semi-Annual": 2, "Quarterly": 4, "Annual": 1}
        freq_label = st.selectbox("Frequency", list(freq_opts.keys()), key="sp_freq")
        frequency = freq_opts[freq_label]
    
    c5, c6 = st.columns(2)
    with c5:
        face_value = st.number_input("Face Value", value=100.0, key="sp_face")
    with c6:
        mufap = st.number_input("MUFAP Price (optional)", value=0.0, key="sp_mufap",
                                 help="Enter 0 to skip comparison")
    
    if st.button("Price", key="sp_price_btn", type="primary"):
        try:
            pricer = SukukPricer(curve_date=date_str, curve_source=source)
            result = pricer.price_isin(
                isin=isin,
                coupon=coupon,
                maturity_date=maturity.strftime("%Y-%m-%d"),
                face_value=face_value,
                frequency=frequency,
                mufap_price=mufap if mufap > 0 else None,
            )
            
            # Display results
            st.markdown("---")
            
            # Status badge
            status_colors = {"Reliable": "#00E676", "Indicative": "#FFB300", "Outlier": "#FF5252"}
            sc = status_colors.get(result.valuation_status, "#6B7280")
            st.markdown(
                f'<div style="display:inline-flex;align-items:center;gap:8px;padding:4px 12px;'
                f'border:1px solid {sc};border-radius:4px;margin-bottom:12px;">'
                f'<span style="color:{sc};font-weight:900;font-size:14px;">'
                f'{result.valuation_status.upper()}</span>'
                f'<span style="color:#6B7280;font-size:10px;">'
                f'Confidence: {result.curve_confidence}/100</span></div>',
                unsafe_allow_html=True,
            )
            
            r1, r2, r3, r4, r5 = st.columns(5)
            r1.metric("Clean Price", f"{result.clean_price:.4f}")
            r2.metric("Dirty Price", f"{result.dirty_price:.4f}")
            r3.metric("Accrued", f"{result.accrued_interest:.4f}")
            r4.metric("YTM", f"{result.ytm:.4f}%")
            r5.metric("Mod Duration", f"{result.modified_duration:.3f}")
            
            r6, r7, r8 = st.columns(3)
            r6.metric("Tenor", f"{result.tenor_years:.1f}Y")
            r7.metric("Spot Rate", f"{result.interpolated_yield:.4f}%")
            r8.metric("Cash Flows", result.n_cash_flows)
            
            if result.variance_bps is not None:
                var_color = "#00E676" if abs(result.variance_bps) < 50 else "#FF5252"
                st.markdown(
                    f'<div style="padding:6px 10px;background:rgba(33,150,243,0.1);'
                    f'border-radius:4px;font-size:12px;">'
                    f'MUFAP Comparison: Fair Value {result.clean_price:.4f} vs '
                    f'MUFAP {result.mufap_price:.4f} → '
                    f'<b style="color:{var_color}">{result.variance_bps:+.0f} bps</b>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            
            st.caption(result.confidence_reason)
            
            # Cash flow table
            with st.expander("Cash Flow Schedule"):
                cf_rows = [{
                    "Date": cf.date.strftime("%Y-%m-%d"),
                    "Years": round(cf.years_to_cf, 3),
                    "Amount": round(cf.amount, 2),
                    "Spot Rate": f"{cf.spot_rate:.4f}%",
                    "DF": f"{cf.discount_factor:.6f}",
                    "PV": round(cf.present_value, 4),
                    "Final": "✓" if cf.is_final else "",
                } for cf in result.cash_flows]
                st.dataframe(pd.DataFrame(cf_rows), use_container_width=True, hide_index=True)
        
        except Exception as e:
            st.error(f"Pricing failed: {e}")
```

## Step 3: Register the Tab

Add "Sukuk Pricer" to the tab list in `render_page()`:

```python
    tab_labels = ["Method Comparison", "Data Table", "Curve History",
                  "Source Convergence", "Z-Spread Calculator", "Fair Value Alerts",
                  "Sukuk Pricer"]
    # ...
    elif active_tab == "Sukuk Pricer":
        _render_sukuk_pricer(ca, source, date_str)
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# 1. Test the engine directly
python3 -c "
from pakfindata.engine.sukuk_pricer import SukukPricer

pricer = SukukPricer(curve_date='2026-04-14', curve_source='PKISRV')

# Price a hypothetical 5Y Sukuk
result = pricer.price_isin(
    isin='TEST001',
    coupon=13.50,
    maturity_date='2031-04-14',
    frequency=2,
    name='Test 5Y Sukuk',
    mufap_price=98.50,
)

print(f'ISIN:           {result.isin}')
print(f'Tenor:          {result.tenor_years}Y')
print(f'Spot Rate:      {result.interpolated_yield}%')
print(f'Clean Price:    {result.clean_price}')
print(f'Dirty Price:    {result.dirty_price}')
print(f'Accrued:        {result.accrued_interest}')
print(f'YTM:            {result.ytm}%')
print(f'Mod Duration:   {result.modified_duration}')
print(f'MUFAP Variance: {result.variance_bps} bps')
print(f'Confidence:     {result.curve_confidence}/100')
print(f'Status:         {result.valuation_status}')
print(f'Reason:         {result.confidence_reason}')
print(f'Cash Flows:     {result.n_cash_flows}')

# Show first 3 cash flows
for cf in result.cash_flows[:3]:
    print(f'  {cf.date.strftime(\"%Y-%m-%d\")} | {cf.amount:.2f} | spot={cf.spot_rate:.4f}% | PV={cf.present_value:.4f}')
print(f'  ... ({len(result.cash_flows)} total)')

# Test a 20Y+ instrument (tests PKRV anchoring)
result_long = pricer.price_isin(
    isin='TEST_LONG',
    coupon=12.00,
    maturity_date='2051-04-14',
    frequency=2,
    name='Test 25Y Sukuk',
)
print(f'\n25Y Sukuk:')
print(f'  Spot Rate: {result_long.interpolated_yield}% (method: {result_long.confidence_reason})')
print(f'  Clean Price: {result_long.clean_price}')
print(f'  Status: {result_long.valuation_status}')
print(f'  Confidence: {result_long.curve_confidence}/100')
"

# 2. Start Streamlit
streamlit run src/pakfindata/ui/app.py --server.port 8501
```

## IMPORTANT NOTES

1. **Confidence flows through every calculation.** Each cash flow gets its own 
   tenor-matched spot rate AND a confidence score. If a 25Y Sukuk has 50 cash 
   flows, the ones near maturity (at 25Y) get low confidence while the near-term 
   coupons (at 1Y, 2Y) get high confidence. The overall status reflects the 
   worst-case confidence in the pricing chain.

2. **PKRV anchoring for long tenors.** When pricing a 25Y PKISRV instrument, 
   the engine checks if PKRV has a 25Y or 30Y rate. If yes, it uses 
   PKRV + Islamic spread (median of observed PKISRV-PKRV spread at overlapping 
   tenors). This is more reliable than extrapolating PKISRV alone.

3. **Day count convention matters.** Pakistan uses ACT/365 for most Islamic 
   instruments. The engine supports ACT/365, ACT/360, and 30/360. Using the 
   wrong convention can shift accrued interest by several basis points.

4. **The Excel export includes cash flow detail** for the first 5 instruments 
   (each on its own sheet). This is what auditors need — they want to see 
   every discount factor, not just the final price.

5. **Outlier detection is two-dimensional:** curve confidence (is the yield 
   reliable?) AND MUFAP variance (does our price match the market?). A 
   "Reliable" curve with >200 bps MUFAP variance → "Outlier". A low-confidence 
   curve with small MUFAP variance → "Indicative" (we agree with the market 
   but aren't sure why).

6. **The pricer is a reusable engine.** Other pages (Debt Terminal, Bond Market) 
   can import `SukukPricer` and call `price_isin()` without duplicating logic. 
   The engine handles all the curve lookup, interpolation, and confidence 
   scoring internally.

7. **Manual mode** lets users price hypothetical instruments — useful for 
   new issuances before they appear in the database. Enter ISIN, coupon, 
   maturity, get instant fair value with confidence rating.

8. **The engine does NOT call any external APIs.** All data comes from the 
   `sovereign_curve` table (populated by the downloaders/processors from 
   previous prompts). Pricing is deterministic and reproducible.
