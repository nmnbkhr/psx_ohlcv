"""ALM FTP Engine — Dynamic Funds Transfer Pricing using live market curves.

Reads PKRV, PKISRV, KIBOR, KONIA, SOFR from the existing pakfindata database
and computes matched-maturity FTP rates for each ALM product.

FTP Rate = Base Curve Rate + Liquidity Premium + Credit Spread + Optionality Cost
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

from pakfindata.db.repositories.alm import (
    get_alm_products,
    get_alm_positions,
    upsert_ftp_rate,
    upsert_sensitivity,
    upsert_liquidity_ladder,
    upsert_ftp_pnl,
)

log = logging.getLogger(__name__)

# Standard repricing buckets (SBP/Basel IRRBB)
BUCKETS = ["ON", "1D-1M", "1M-3M", "3M-6M", "6M-1Y", "1Y-2Y", "2Y-3Y", "3Y-5Y", "5Y-10Y", "10Y+"]
BUCKET_MID_MONTHS = {
    "ON": 0.03, "1D-1M": 0.5, "1M-3M": 2, "3M-6M": 4.5,
    "6M-1Y": 9, "1Y-2Y": 18, "2Y-3Y": 30, "3Y-5Y": 48,
    "5Y-10Y": 90, "10Y+": 180,
}

# Sensitivity shock scenarios (bps)
SCENARIOS = [
    ("BASE", 0),
    ("+100bps", 100), ("-100bps", -100),
    ("+200bps", 200), ("-200bps", -200),
    ("+300bps", 300), ("-300bps", -300),
    ("SBP_CUT_150", -150),
    ("SBP_CUT_250", -250),
]


# =============================================================================
# CURVE LOADERS — pull live rates from existing pakfindata tables
# =============================================================================

def _load_pkrv_curve(con: sqlite3.Connection, date: str | None = None) -> dict[float, float]:
    """Load PKRV as {tenor_months: yield_pct}."""
    if date is None:
        row = con.execute("SELECT MAX(date) as d FROM pkrv_daily").fetchone()
        if not row or not row["d"]:
            return {}
        date = row["d"]
    rows = con.execute(
        "SELECT tenor_months, yield_pct FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
        (date,),
    ).fetchall()
    return {float(r["tenor_months"]): float(r["yield_pct"]) for r in rows}


def _load_pkisrv_curve(con: sqlite3.Connection, date: str | None = None) -> dict[float, float]:
    """Load PKISRV as {tenor_months: yield_pct}."""
    if date is None:
        row = con.execute("SELECT MAX(date) as d FROM pkisrv_daily").fetchone()
        if not row or not row["d"]:
            return {}
        date = row["d"]
    rows = con.execute(
        "SELECT tenor, yield_pct FROM pkisrv_daily WHERE date = ?", (date,),
    ).fetchall()
    # tenor might be text like '3M', '6M', '1Y' — convert to months
    result = {}
    for r in rows:
        t = r["tenor"]
        months = _tenor_to_months(t) if isinstance(t, str) else float(t)
        if months is not None:
            result[months] = float(r["yield_pct"])
    return result


def _load_kibor_curve(con: sqlite3.Connection, date: str | None = None) -> dict[str, float]:
    """Load KIBOR as {tenor: offer_rate}. Tenor keys like '1W','3M','6M','1Y'."""
    if date is None:
        row = con.execute("SELECT MAX(date) as d FROM kibor_daily").fetchone()
        if not row or not row["d"]:
            return {}
        date = row["d"]
    rows = con.execute(
        "SELECT tenor, offer FROM kibor_daily WHERE date = ? AND offer IS NOT NULL",
        (date,),
    ).fetchall()
    return {r["tenor"]: float(r["offer"]) for r in rows}


def _load_konia(con: sqlite3.Connection, date: str | None = None) -> float | None:
    """Load KONIA overnight rate."""
    if date is None:
        row = con.execute("SELECT rate_pct FROM konia_daily ORDER BY date DESC LIMIT 1").fetchone()
    else:
        row = con.execute("SELECT rate_pct FROM konia_daily WHERE date <= ? ORDER BY date DESC LIMIT 1", (date,)).fetchone()
    return float(row["rate_pct"]) if row else None


def _load_policy_rate(con: sqlite3.Connection, date: str | None = None) -> float | None:
    """Load SBP policy rate."""
    if date is None:
        row = con.execute("SELECT policy_rate FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1").fetchone()
    else:
        row = con.execute(
            "SELECT policy_rate FROM sbp_policy_rates WHERE rate_date <= ? ORDER BY rate_date DESC LIMIT 1",
            (date,),
        ).fetchone()
    return float(row["policy_rate"]) if row else None


def _load_sofr(con: sqlite3.Connection, date: str | None = None) -> float | None:
    """Load latest SOFR rate."""
    try:
        if date is None:
            row = con.execute(
                "SELECT rate FROM global_reference_rates WHERE rate_name = 'SOFR' ORDER BY date DESC LIMIT 1"
            ).fetchone()
        else:
            row = con.execute(
                "SELECT rate FROM global_reference_rates WHERE rate_name = 'SOFR' AND date <= ? ORDER BY date DESC LIMIT 1",
                (date,),
            ).fetchone()
        return float(row["rate"]) if row else None
    except Exception:
        return None


# =============================================================================
# INTERPOLATION
# =============================================================================

def _tenor_to_months(tenor_str: str) -> float | None:
    """Convert tenor string like '3M', '1Y', '1W' to months."""
    s = tenor_str.strip().upper()
    if s.endswith("M"):
        try:
            return float(s[:-1])
        except ValueError:
            return None
    elif s.endswith("Y"):
        try:
            return float(s[:-1]) * 12
        except ValueError:
            return None
    elif s.endswith("W"):
        try:
            return float(s[:-1]) / 4.33
        except ValueError:
            return None
    elif s.endswith("D"):
        try:
            return float(s[:-1]) / 30.0
        except ValueError:
            return None
    return None


def _interpolate_curve(curve: dict[float, float], tenor_months: float) -> float | None:
    """Linear interpolation on a {tenor_months: rate} curve."""
    if not curve:
        return None
    tenors = sorted(curve.keys())
    rates = [curve[t] for t in tenors]

    if tenor_months <= tenors[0]:
        return rates[0]
    if tenor_months >= tenors[-1]:
        return rates[-1]

    # Find bracketing tenors
    for i in range(len(tenors) - 1):
        if tenors[i] <= tenor_months <= tenors[i + 1]:
            t0, t1 = tenors[i], tenors[i + 1]
            r0, r1 = rates[i], rates[i + 1]
            frac = (tenor_months - t0) / (t1 - t0) if t1 != t0 else 0
            return r0 + frac * (r1 - r0)
    return rates[-1]


def _kibor_tenor_to_months(kibor_dict: dict[str, float]) -> dict[float, float]:
    """Convert KIBOR tenor labels to months for interpolation."""
    result = {}
    for tenor, rate in kibor_dict.items():
        m = _tenor_to_months(tenor)
        if m is not None:
            result[m] = rate
    return result


# =============================================================================
# FTP COMPUTATION
# =============================================================================

def compute_ftp_for_product(
    product: dict,
    pkrv: dict[float, float],
    pkisrv: dict[float, float],
    kibor: dict[str, float],
    konia: float | None,
    policy_rate: float | None,
    sofr: float | None,
    customer_rate: float | None = None,
    outstanding_mn: float | None = None,
) -> dict:
    """
    Compute FTP rate for a single product using matched-maturity approach.

    Returns dict with all FTP components ready for upsert.
    """
    product_code = product["product_code"]
    rate_type = product["rate_type"]
    ref_rate = product.get("reference_rate") or ""
    is_islamic = product.get("is_islamic", 0)

    # Determine base curve and tenor
    ftp_curve = "PKRV"
    ftp_tenor = None
    base_rate = None

    if rate_type == "zero":
        # CASA current accounts: blended core + volatile
        core_pct = product.get("core_pct", 0.70) or 0.70
        core_tenor = product.get("core_tenor_months", 36) or 36
        vol_tenor = product.get("volatile_tenor_months", 0) or 0.03

        curve = pkisrv if is_islamic else pkrv
        ftp_curve = "PKISRV" if is_islamic else "PKRV"

        core_rate = _interpolate_curve(curve, core_tenor)
        vol_rate = konia if konia is not None else _interpolate_curve(curve, vol_tenor)

        if core_rate is not None and vol_rate is not None:
            base_rate = core_pct * core_rate + (1 - core_pct) * vol_rate
            ftp_tenor = core_pct * core_tenor + (1 - core_pct) * vol_tenor
        elif core_rate is not None:
            base_rate = core_rate
            ftp_tenor = core_tenor

    elif rate_type == "administered":
        # Savings accounts: use behavioral maturity on curve
        beh_mat = product.get("behavioral_maturity_months", 24) or 24
        curve = pkisrv if is_islamic else pkrv
        ftp_curve = "PKISRV" if is_islamic else "PKRV"
        base_rate = _interpolate_curve(curve, beh_mat)
        ftp_tenor = beh_mat

    elif rate_type == "floating":
        # Floating rate: FTP at repricing frequency tenor
        repricing = product.get("repricing_freq_months")
        if repricing is None or repricing == 0:
            repricing = 0.03  # overnight

        if "KONIA" in ref_rate:
            ftp_curve = "KONIA"
            base_rate = konia
            ftp_tenor = 0.03
        elif "SOFR" in ref_rate:
            ftp_curve = "SOFR"
            base_rate = sofr
            ftp_tenor = repricing
        elif "KIBOR" in ref_rate:
            # Match to KIBOR at repricing tenor
            ftp_curve = "KIBOR"
            kibor_months = _kibor_tenor_to_months(kibor)
            base_rate = _interpolate_curve(kibor_months, repricing)
            ftp_tenor = repricing
        else:
            # Default to PKRV at repricing tenor
            curve = pkisrv if is_islamic else pkrv
            ftp_curve = "PKISRV" if is_islamic else "PKRV"
            base_rate = _interpolate_curve(curve, repricing)
            ftp_tenor = repricing

    elif rate_type == "fixed":
        # Fixed rate: match to contractual/behavioral maturity
        mat = product.get("behavioral_maturity_months") or product.get("contractual_maturity_months") or 12
        if is_islamic:
            ftp_curve = "PKISRV"
            base_rate = _interpolate_curve(pkisrv, mat)
        else:
            ftp_curve = "PKRV"
            base_rate = _interpolate_curve(pkrv, mat)
        ftp_tenor = mat

    if base_rate is None:
        log.warning("No base rate for %s (curve=%s)", product_code, ftp_curve)
        return {}

    # Add-ons from product config
    liq_bps = product.get("liq_premium_bps", 0) or 0
    opt_bps = product.get("optionality_cost_bps", 0) or 0
    credit_bps = 0  # could be product-specific, kept at 0 for now

    total_ftp = base_rate + (liq_bps + credit_bps + opt_bps) / 100.0

    # Margin calculation
    margin_bps = None
    daily_nii = None
    if customer_rate is not None:
        al = product.get("asset_liability", "A")
        if al == "A":
            margin_bps = (customer_rate - total_ftp) * 100
        else:
            margin_bps = (total_ftp - customer_rate) * 100

        if outstanding_mn is not None and outstanding_mn > 0:
            daily_nii = (margin_bps / 10000) * outstanding_mn / 365

    return {
        "product_code": product_code,
        "ftp_curve": ftp_curve,
        "ftp_tenor_months": ftp_tenor,
        "ftp_base_rate": round(base_rate, 4),
        "liq_premium_bps": liq_bps,
        "credit_spread_bps": credit_bps,
        "optionality_bps": opt_bps,
        "total_ftp_rate": round(total_ftp, 4),
        "customer_rate": customer_rate,
        "ftp_margin_bps": round(margin_bps, 2) if margin_bps is not None else None,
        "outstanding_mn": outstanding_mn,
        "daily_nii_mn": round(daily_nii, 6) if daily_nii is not None else None,
    }


def run_daily_ftp(con: sqlite3.Connection, as_of_date: str | None = None) -> list[dict]:
    """
    Compute FTP rates for all active products using live market curves.

    Args:
        con: DB connection (must have yield curve data + ALM products)
        as_of_date: Date to compute FTP for (defaults to latest available curve date)

    Returns:
        List of FTP result dicts (also persisted to alm_ftp_rates table)
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    # Load all curves
    pkrv = _load_pkrv_curve(con, as_of_date)
    pkisrv = _load_pkisrv_curve(con, as_of_date)
    kibor = _load_kibor_curve(con, as_of_date)
    konia = _load_konia(con, as_of_date)
    policy = _load_policy_rate(con, as_of_date)
    sofr = _load_sofr(con, as_of_date)

    if not pkrv:
        log.warning("No PKRV data for %s — cannot compute FTP", as_of_date)
        return []

    log.info(
        "FTP curves loaded for %s: PKRV=%d pts, KIBOR=%d tenors, KONIA=%s, Policy=%s",
        as_of_date, len(pkrv), len(kibor),
        f"{konia:.2f}%" if konia else "N/A",
        f"{policy:.2f}%" if policy else "N/A",
    )

    # Load products
    products_df = get_alm_products(con)
    if products_df.empty:
        log.warning("No ALM products — seed with seed_default_products() first")
        return []

    # Load positions for customer rates & balances (if available)
    positions_df = get_alm_positions(con, as_of_date)
    pos_lookup: dict[str, dict] = {}
    if not positions_df.empty:
        for _, row in positions_df.groupby("product_code").agg({
            "outstanding_mn": "sum",
            "weighted_avg_rate": "mean",
        }).iterrows():
            pos_lookup[_] = {"customer_rate": row["weighted_avg_rate"], "outstanding_mn": row["outstanding_mn"]}

    results = []
    for _, prod in products_df.iterrows():
        product = prod.to_dict()
        pos = pos_lookup.get(product["product_code"], {})

        ftp = compute_ftp_for_product(
            product=product,
            pkrv=pkrv,
            pkisrv=pkisrv,
            kibor=kibor,
            konia=konia,
            policy_rate=policy,
            sofr=sofr,
            customer_rate=pos.get("customer_rate"),
            outstanding_mn=pos.get("outstanding_mn"),
        )
        if ftp:
            ftp["as_of_date"] = as_of_date
            upsert_ftp_rate(con, ftp)
            results.append(ftp)

    log.info("FTP computed for %d/%d products on %s", len(results), len(products_df), as_of_date)
    return results


# =============================================================================
# NII / EVE SENSITIVITY
# =============================================================================

def run_sensitivity(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
) -> list[dict]:
    """
    Run NII and EVE sensitivity analysis under parallel rate shock scenarios.

    Uses positions + FTP rates to estimate impact of rate changes.
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    positions = get_alm_positions(con, as_of_date)
    if positions.empty:
        log.warning("No positions for %s — cannot run sensitivity", as_of_date)
        return []

    # Load base curves
    pkrv = _load_pkrv_curve(con, as_of_date)
    if not pkrv:
        return []

    # Compute base NII from positions
    base_nii = 0.0
    base_eve = 0.0
    asset_dur_weighted = 0.0
    liab_dur_weighted = 0.0
    total_assets = 0.0
    total_liabs = 0.0

    for _, pos in positions.iterrows():
        bal = pos["outstanding_mn"] or 0
        rate = pos["weighted_avg_rate"] or 0
        al = pos["asset_liability"]
        mat = pos.get("avg_remaining_mat_months", 12) or 12
        mod_dur = mat / 12.0  # simplified duration ~ maturity in years

        if al == "A":
            base_nii += bal * rate / 100.0
            base_eve += bal  # simplified EVE
            asset_dur_weighted += bal * mod_dur
            total_assets += bal
        else:
            base_nii -= bal * rate / 100.0
            base_eve -= bal
            liab_dur_weighted += bal * mod_dur
            total_liabs += bal

    asset_dur = asset_dur_weighted / total_assets if total_assets > 0 else 0
    liab_dur = liab_dur_weighted / total_liabs if total_liabs > 0 else 0
    duration_gap = asset_dur - (total_liabs / total_assets * liab_dur) if total_assets > 0 else 0

    results = []
    for scenario_name, shock_bps in SCENARIOS:
        shock_pct = shock_bps / 100.0

        # NII impact: repricing gap * shock (simplified)
        nii_impact = 0.0
        eve_impact = 0.0

        for _, pos in positions.iterrows():
            bal = pos["outstanding_mn"] or 0
            rate_type = pos.get("rate_type", "fixed")
            al = pos["asset_liability"]
            mat = pos.get("avg_remaining_mat_months", 12) or 12
            mod_dur = mat / 12.0

            # Floating/administered rates reprice → NII changes
            if rate_type in ("floating", "administered", "zero"):
                if al == "A":
                    nii_impact += bal * shock_pct / 100.0
                else:
                    nii_impact -= bal * shock_pct / 100.0

            # EVE: all instruments affected by duration
            pv_change = -mod_dur * (shock_bps / 10000.0) * bal
            if al == "A":
                eve_impact += pv_change
            else:
                eve_impact -= pv_change

        nii_shocked = base_nii + nii_impact
        eve_base = total_assets - total_liabs  # simplified equity
        eve_shocked = eve_base + eve_impact

        result = {
            "as_of_date": as_of_date,
            "scenario": scenario_name,
            "shock_bps": shock_bps,
            "nii_base_mn": round(base_nii, 2),
            "nii_shocked_mn": round(nii_shocked, 2),
            "nii_impact_mn": round(nii_impact, 2),
            "nii_pct_change": round(nii_impact / base_nii * 100, 2) if base_nii != 0 else 0,
            "eve_base_mn": round(eve_base, 2),
            "eve_shocked_mn": round(eve_shocked, 2),
            "eve_impact_mn": round(eve_impact, 2),
            "eve_pct_change": round(eve_impact / eve_base * 100, 2) if eve_base != 0 else 0,
            "duration_gap": round(duration_gap, 4),
        }
        upsert_sensitivity(con, result)
        results.append(result)

    log.info("Sensitivity: %d scenarios computed for %s", len(results), as_of_date)
    return results


# =============================================================================
# LIQUIDITY LADDER
# =============================================================================

def compute_liquidity_ladder(con: sqlite3.Connection, as_of_date: str | None = None) -> list[dict]:
    """Compute liquidity maturity ladder from ALM positions."""
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    positions = get_alm_positions(con, as_of_date)
    if positions.empty:
        return []

    liq_buckets = ["ON", "1D-1M", "1M-3M", "3M-6M", "6M-1Y", "1Y+"]
    inflows = {b: 0.0 for b in liq_buckets}
    outflows = {b: 0.0 for b in liq_buckets}
    hqla = {b: 0.0 for b in liq_buckets}

    for _, pos in positions.iterrows():
        bal = pos["outstanding_mn"] or 0
        al = pos["asset_liability"]
        bucket = pos["bucket"]
        category = pos.get("category", "")

        # Map position buckets to liquidity buckets
        liq_bucket = _map_to_liq_bucket(bucket)

        if al == "A":
            inflows[liq_bucket] += bal
            # T-Bills and PIBs count as HQLA
            if category == "SLR":
                hqla[liq_bucket] += bal
        else:
            outflows[liq_bucket] += bal

    cum_gap = 0.0
    results = []
    for b in liq_buckets:
        net = inflows[b] - outflows[b]
        cum_gap += net
        thirty_day_outflows = outflows.get("ON", 0) + outflows.get("1D-1M", 0)
        total_hqla = sum(hqla.values())
        lcr = (total_hqla / thirty_day_outflows * 100) if thirty_day_outflows > 0 else None

        row = {
            "as_of_date": as_of_date,
            "bucket": b,
            "inflows_mn": round(inflows[b], 2),
            "outflows_mn": round(outflows[b], 2),
            "net_gap_mn": round(net, 2),
            "cumulative_gap_mn": round(cum_gap, 2),
            "hqla_mn": round(hqla[b], 2),
            "lcr_pct": round(lcr, 2) if lcr else None,
        }
        upsert_liquidity_ladder(con, row)
        results.append(row)

    return results


def _map_to_liq_bucket(bucket: str) -> str:
    """Map repricing bucket to liquidity bucket."""
    mapping = {
        "ON": "ON",
        "1D-1M": "1D-1M",
        "1M-3M": "1M-3M",
        "3M-6M": "3M-6M",
        "6M-1Y": "6M-1Y",
    }
    return mapping.get(bucket, "1Y+")


# =============================================================================
# MONTHLY P&L ATTRIBUTION
# =============================================================================

def compute_monthly_pnl(
    con: sqlite3.Connection,
    month: str,
    prev_month: str | None = None,
) -> list[dict]:
    """
    Compute monthly FTP P&L attribution (volume, rate, mix effects).

    Args:
        month: YYYY-MM for the current month
        prev_month: YYYY-MM for the previous month (for delta attribution)
    """
    # Get average daily FTP data for the month
    current = pd.read_sql_query("""
        SELECT product_code,
               AVG(outstanding_mn) as avg_balance_mn,
               AVG(customer_rate) as avg_customer_rate,
               AVG(total_ftp_rate) as avg_ftp_rate,
               AVG(ftp_margin_bps) as avg_margin_bps
        FROM alm_ftp_rates
        WHERE as_of_date LIKE ? || '%'
          AND outstanding_mn IS NOT NULL
        GROUP BY product_code
    """, con, params=(month,))

    if current.empty:
        return []

    # Get previous month for attribution
    prev = pd.DataFrame()
    if prev_month:
        prev = pd.read_sql_query("""
            SELECT product_code,
                   AVG(outstanding_mn) as avg_balance_mn,
                   AVG(customer_rate) as avg_customer_rate,
                   AVG(total_ftp_rate) as avg_ftp_rate,
                   AVG(ftp_margin_bps) as avg_margin_bps
            FROM alm_ftp_rates
            WHERE as_of_date LIKE ? || '%'
              AND outstanding_mn IS NOT NULL
            GROUP BY product_code
        """, con, params=(prev_month,))

    prev_lookup = {}
    if not prev.empty:
        prev_lookup = prev.set_index("product_code").to_dict("index")

    results = []
    for _, row in current.iterrows():
        pc = row["product_code"]
        bal = row["avg_balance_mn"] or 0
        margin = row["avg_margin_bps"] or 0
        nii = (margin / 10000) * bal  # annualized

        # Attribution vs previous month
        vol_eff = rate_eff = mix_eff = 0.0
        if pc in prev_lookup:
            p = prev_lookup[pc]
            prev_bal = p.get("avg_balance_mn", 0) or 0
            prev_margin = p.get("avg_margin_bps", 0) or 0
            # Volume effect: change in balance × old margin
            vol_eff = ((bal - prev_bal) * prev_margin / 10000)
            # Rate effect: change in margin × old balance
            rate_eff = ((margin - prev_margin) / 10000 * prev_bal)
            # Mix effect: residual
            mix_eff = nii - (prev_margin / 10000 * prev_bal) - vol_eff - rate_eff

        result = {
            "month": month,
            "product_code": pc,
            "avg_balance_mn": round(bal, 2),
            "avg_customer_rate": round(row["avg_customer_rate"], 4) if row["avg_customer_rate"] else None,
            "avg_ftp_rate": round(row["avg_ftp_rate"], 4) if row["avg_ftp_rate"] else None,
            "avg_margin_bps": round(margin, 2),
            "nii_contribution_mn": round(nii, 4),
            "volume_effect_mn": round(vol_eff, 4),
            "rate_effect_mn": round(rate_eff, 4),
            "mix_effect_mn": round(mix_eff, 4),
        }
        upsert_ftp_pnl(con, result)
        results.append(result)

    return results
