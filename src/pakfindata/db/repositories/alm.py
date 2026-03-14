"""ALM (Asset-Liability Management) repository — products, positions, FTP rates, sensitivity."""

from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "upsert_alm_product",
    "get_alm_products",
    "get_alm_product",
    "upsert_alm_position",
    "get_alm_positions",
    "get_repricing_gap",
    "upsert_ftp_rate",
    "get_ftp_rates",
    "get_ftp_history",
    "upsert_sensitivity",
    "get_sensitivity",
    "upsert_liquidity_ladder",
    "get_liquidity_ladder",
    "upsert_ftp_pnl",
    "get_ftp_pnl",
    "seed_default_products",
]


# =============================================================================
# PRODUCT CATALOG
# =============================================================================

def upsert_alm_product(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update an ALM product."""
    try:
        con.execute("""
            INSERT INTO alm_products (
                product_code, product_name, product_type, asset_liability,
                rate_type, reference_rate, spread_bps, repricing_freq_months,
                contractual_maturity_months, behavioral_maturity_months,
                currency, is_islamic, liq_premium_bps, optionality_cost_bps,
                core_pct, core_tenor_months, volatile_tenor_months,
                category, is_active, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
            ON CONFLICT(product_code) DO UPDATE SET
                product_name=excluded.product_name,
                product_type=excluded.product_type,
                asset_liability=excluded.asset_liability,
                rate_type=excluded.rate_type,
                reference_rate=excluded.reference_rate,
                spread_bps=excluded.spread_bps,
                repricing_freq_months=excluded.repricing_freq_months,
                contractual_maturity_months=excluded.contractual_maturity_months,
                behavioral_maturity_months=excluded.behavioral_maturity_months,
                currency=excluded.currency,
                is_islamic=excluded.is_islamic,
                liq_premium_bps=excluded.liq_premium_bps,
                optionality_cost_bps=excluded.optionality_cost_bps,
                core_pct=excluded.core_pct,
                core_tenor_months=excluded.core_tenor_months,
                volatile_tenor_months=excluded.volatile_tenor_months,
                category=excluded.category,
                is_active=excluded.is_active,
                updated_at=datetime('now')
        """, (
            data["product_code"], data["product_name"], data["product_type"],
            data["asset_liability"], data["rate_type"],
            data.get("reference_rate"), data.get("spread_bps", 0),
            data.get("repricing_freq_months"), data.get("contractual_maturity_months"),
            data.get("behavioral_maturity_months"), data.get("currency", "PKR"),
            data.get("is_islamic", 0), data.get("liq_premium_bps", 0),
            data.get("optionality_cost_bps", 0), data.get("core_pct", 1.0),
            data.get("core_tenor_months"), data.get("volatile_tenor_months", 0),
            data.get("category"), data.get("is_active", 1),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_alm_products(
    con: sqlite3.Connection,
    active_only: bool = True,
    asset_liability: str | None = None,
) -> pd.DataFrame:
    """Get ALM products as DataFrame."""
    query = "SELECT * FROM alm_products WHERE 1=1"
    params: list = []
    if active_only:
        query += " AND is_active = 1"
    if asset_liability:
        query += " AND asset_liability = ?"
        params.append(asset_liability)
    query += " ORDER BY asset_liability, category, product_code"
    return pd.read_sql_query(query, con, params=params)


def get_alm_product(con: sqlite3.Connection, product_code: str) -> dict | None:
    """Get a single ALM product."""
    row = con.execute(
        "SELECT * FROM alm_products WHERE product_code = ?", (product_code,)
    ).fetchone()
    return dict(row) if row else None


# =============================================================================
# BALANCE SHEET POSITIONS
# =============================================================================

def upsert_alm_position(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a balance sheet position."""
    try:
        con.execute("""
            INSERT INTO alm_positions (
                as_of_date, product_code, bucket, outstanding_mn,
                weighted_avg_rate, num_accounts, avg_remaining_mat_months,
                source, ingested_at
            ) VALUES (?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(as_of_date, product_code, bucket) DO UPDATE SET
                outstanding_mn=excluded.outstanding_mn,
                weighted_avg_rate=excluded.weighted_avg_rate,
                num_accounts=excluded.num_accounts,
                avg_remaining_mat_months=excluded.avg_remaining_mat_months,
                source=excluded.source,
                ingested_at=datetime('now')
        """, (
            data["as_of_date"], data["product_code"], data["bucket"],
            data["outstanding_mn"], data.get("weighted_avg_rate"),
            data.get("num_accounts"), data.get("avg_remaining_mat_months"),
            data.get("source", "MANUAL"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_alm_positions(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    """Get balance sheet positions. Latest date if None."""
    if as_of_date is None:
        row = con.execute("SELECT MAX(as_of_date) as d FROM alm_positions").fetchone()
        if not row or not row["d"]:
            return pd.DataFrame()
        as_of_date = row["d"]
    return pd.read_sql_query(
        """SELECT p.*, pr.product_name, pr.product_type, pr.asset_liability,
                  pr.rate_type, pr.category
           FROM alm_positions p
           JOIN alm_products pr ON p.product_code = pr.product_code
           WHERE p.as_of_date = ?
           ORDER BY pr.asset_liability, pr.category, p.bucket""",
        con, params=(as_of_date,),
    )


def get_repricing_gap(con: sqlite3.Connection, as_of_date: str | None = None) -> pd.DataFrame:
    """Get repricing gap analysis — assets vs liabilities by bucket."""
    if as_of_date is None:
        row = con.execute("SELECT MAX(as_of_date) as d FROM alm_positions").fetchone()
        if not row or not row["d"]:
            return pd.DataFrame()
        as_of_date = row["d"]
    return pd.read_sql_query("""
        SELECT
            p.bucket,
            SUM(CASE WHEN pr.asset_liability='A' THEN p.outstanding_mn ELSE 0 END) as assets_mn,
            SUM(CASE WHEN pr.asset_liability='L' THEN p.outstanding_mn ELSE 0 END) as liabilities_mn,
            SUM(CASE WHEN pr.asset_liability='A' THEN p.outstanding_mn ELSE 0 END) -
            SUM(CASE WHEN pr.asset_liability='L' THEN p.outstanding_mn ELSE 0 END) as gap_mn
        FROM alm_positions p
        JOIN alm_products pr ON p.product_code = pr.product_code
        WHERE p.as_of_date = ?
        GROUP BY p.bucket
        ORDER BY CASE p.bucket
            WHEN 'ON' THEN 1 WHEN '1D-1M' THEN 2 WHEN '1M-3M' THEN 3
            WHEN '3M-6M' THEN 4 WHEN '6M-1Y' THEN 5 WHEN '1Y-2Y' THEN 6
            WHEN '2Y-3Y' THEN 7 WHEN '3Y-5Y' THEN 8 WHEN '5Y-10Y' THEN 9
            WHEN '10Y+' THEN 10 ELSE 99 END
    """, con, params=(as_of_date,))


# =============================================================================
# FTP RATES
# =============================================================================

def upsert_ftp_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a daily FTP rate for a product."""
    try:
        con.execute("""
            INSERT INTO alm_ftp_rates (
                as_of_date, product_code, ftp_curve, ftp_tenor_months,
                ftp_base_rate, liq_premium_bps, credit_spread_bps,
                optionality_bps, total_ftp_rate, customer_rate,
                ftp_margin_bps, outstanding_mn, daily_nii_mn, computed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(as_of_date, product_code) DO UPDATE SET
                ftp_curve=excluded.ftp_curve,
                ftp_tenor_months=excluded.ftp_tenor_months,
                ftp_base_rate=excluded.ftp_base_rate,
                liq_premium_bps=excluded.liq_premium_bps,
                credit_spread_bps=excluded.credit_spread_bps,
                optionality_bps=excluded.optionality_bps,
                total_ftp_rate=excluded.total_ftp_rate,
                customer_rate=excluded.customer_rate,
                ftp_margin_bps=excluded.ftp_margin_bps,
                outstanding_mn=excluded.outstanding_mn,
                daily_nii_mn=excluded.daily_nii_mn,
                computed_at=datetime('now')
        """, (
            data["as_of_date"], data["product_code"],
            data.get("ftp_curve"), data.get("ftp_tenor_months"),
            data["ftp_base_rate"], data.get("liq_premium_bps", 0),
            data.get("credit_spread_bps", 0), data.get("optionality_bps", 0),
            data["total_ftp_rate"], data.get("customer_rate"),
            data.get("ftp_margin_bps"), data.get("outstanding_mn"),
            data.get("daily_nii_mn"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_ftp_rates(
    con: sqlite3.Connection, as_of_date: str | None = None
) -> pd.DataFrame:
    """Get FTP rates for a date. Latest if None."""
    if as_of_date is None:
        row = con.execute("SELECT MAX(as_of_date) as d FROM alm_ftp_rates").fetchone()
        if not row or not row["d"]:
            return pd.DataFrame()
        as_of_date = row["d"]
    return pd.read_sql_query("""
        SELECT f.*, p.product_name, p.product_type, p.asset_liability,
               p.rate_type, p.category
        FROM alm_ftp_rates f
        JOIN alm_products p ON f.product_code = p.product_code
        WHERE f.as_of_date = ?
        ORDER BY p.asset_liability, p.category, f.product_code
    """, con, params=(as_of_date,))


def get_ftp_history(
    con: sqlite3.Connection,
    product_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get FTP rate history for time series analysis."""
    query = """
        SELECT f.*, p.product_name, p.asset_liability, p.category
        FROM alm_ftp_rates f
        JOIN alm_products p ON f.product_code = p.product_code
        WHERE 1=1
    """
    params: list = []
    if product_code:
        query += " AND f.product_code = ?"
        params.append(product_code)
    if start_date:
        query += " AND f.as_of_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND f.as_of_date <= ?"
        params.append(end_date)
    query += " ORDER BY f.as_of_date, f.product_code"
    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# SENSITIVITY
# =============================================================================

def upsert_sensitivity(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a sensitivity scenario result."""
    try:
        con.execute("""
            INSERT INTO alm_sensitivity (
                as_of_date, scenario, shock_bps, nii_base_mn, nii_shocked_mn,
                nii_impact_mn, nii_pct_change, eve_base_mn, eve_shocked_mn,
                eve_impact_mn, eve_pct_change, duration_gap, computed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(as_of_date, scenario) DO UPDATE SET
                shock_bps=excluded.shock_bps,
                nii_base_mn=excluded.nii_base_mn,
                nii_shocked_mn=excluded.nii_shocked_mn,
                nii_impact_mn=excluded.nii_impact_mn,
                nii_pct_change=excluded.nii_pct_change,
                eve_base_mn=excluded.eve_base_mn,
                eve_shocked_mn=excluded.eve_shocked_mn,
                eve_impact_mn=excluded.eve_impact_mn,
                eve_pct_change=excluded.eve_pct_change,
                duration_gap=excluded.duration_gap,
                computed_at=datetime('now')
        """, (
            data["as_of_date"], data["scenario"], data.get("shock_bps"),
            data.get("nii_base_mn"), data.get("nii_shocked_mn"),
            data.get("nii_impact_mn"), data.get("nii_pct_change"),
            data.get("eve_base_mn"), data.get("eve_shocked_mn"),
            data.get("eve_impact_mn"), data.get("eve_pct_change"),
            data.get("duration_gap"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_sensitivity(
    con: sqlite3.Connection, as_of_date: str | None = None
) -> pd.DataFrame:
    """Get sensitivity scenarios for a date."""
    if as_of_date is None:
        row = con.execute("SELECT MAX(as_of_date) as d FROM alm_sensitivity").fetchone()
        if not row or not row["d"]:
            return pd.DataFrame()
        as_of_date = row["d"]
    return pd.read_sql_query(
        "SELECT * FROM alm_sensitivity WHERE as_of_date = ? ORDER BY shock_bps",
        con, params=(as_of_date,),
    )


# =============================================================================
# LIQUIDITY LADDER
# =============================================================================

def upsert_liquidity_ladder(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a liquidity ladder bucket."""
    try:
        con.execute("""
            INSERT INTO alm_liquidity_ladder (
                as_of_date, bucket, inflows_mn, outflows_mn, net_gap_mn,
                cumulative_gap_mn, hqla_mn, lcr_pct, computed_at
            ) VALUES (?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(as_of_date, bucket) DO UPDATE SET
                inflows_mn=excluded.inflows_mn,
                outflows_mn=excluded.outflows_mn,
                net_gap_mn=excluded.net_gap_mn,
                cumulative_gap_mn=excluded.cumulative_gap_mn,
                hqla_mn=excluded.hqla_mn,
                lcr_pct=excluded.lcr_pct,
                computed_at=datetime('now')
        """, (
            data["as_of_date"], data["bucket"],
            data.get("inflows_mn", 0), data.get("outflows_mn", 0),
            data.get("net_gap_mn"), data.get("cumulative_gap_mn"),
            data.get("hqla_mn"), data.get("lcr_pct"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_liquidity_ladder(
    con: sqlite3.Connection, as_of_date: str | None = None
) -> pd.DataFrame:
    """Get liquidity ladder for a date."""
    if as_of_date is None:
        row = con.execute("SELECT MAX(as_of_date) as d FROM alm_liquidity_ladder").fetchone()
        if not row or not row["d"]:
            return pd.DataFrame()
        as_of_date = row["d"]
    return pd.read_sql_query(
        """SELECT * FROM alm_liquidity_ladder WHERE as_of_date = ?
           ORDER BY CASE bucket
               WHEN 'ON' THEN 1 WHEN '1D-1M' THEN 2 WHEN '1M-3M' THEN 3
               WHEN '3M-6M' THEN 4 WHEN '6M-1Y' THEN 5 WHEN '1Y+' THEN 6
               ELSE 99 END""",
        con, params=(as_of_date,),
    )


# =============================================================================
# FTP P&L ATTRIBUTION
# =============================================================================

def upsert_ftp_pnl(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update monthly FTP P&L attribution."""
    try:
        con.execute("""
            INSERT INTO alm_ftp_pnl (
                month, product_code, avg_balance_mn, avg_customer_rate,
                avg_ftp_rate, avg_margin_bps, nii_contribution_mn,
                volume_effect_mn, rate_effect_mn, mix_effect_mn, computed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(month, product_code) DO UPDATE SET
                avg_balance_mn=excluded.avg_balance_mn,
                avg_customer_rate=excluded.avg_customer_rate,
                avg_ftp_rate=excluded.avg_ftp_rate,
                avg_margin_bps=excluded.avg_margin_bps,
                nii_contribution_mn=excluded.nii_contribution_mn,
                volume_effect_mn=excluded.volume_effect_mn,
                rate_effect_mn=excluded.rate_effect_mn,
                mix_effect_mn=excluded.mix_effect_mn,
                computed_at=datetime('now')
        """, (
            data["month"], data["product_code"],
            data.get("avg_balance_mn"), data.get("avg_customer_rate"),
            data.get("avg_ftp_rate"), data.get("avg_margin_bps"),
            data.get("nii_contribution_mn"), data.get("volume_effect_mn"),
            data.get("rate_effect_mn"), data.get("mix_effect_mn"),
        ))
        con.commit()
        return True
    except Exception:
        return False


def get_ftp_pnl(
    con: sqlite3.Connection,
    month: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
) -> pd.DataFrame:
    """Get FTP P&L attribution."""
    query = """
        SELECT pnl.*, p.product_name, p.asset_liability, p.category
        FROM alm_ftp_pnl pnl
        JOIN alm_products p ON pnl.product_code = p.product_code
        WHERE 1=1
    """
    params: list = []
    if month:
        query += " AND pnl.month = ?"
        params.append(month)
    if start_month:
        query += " AND pnl.month >= ?"
        params.append(start_month)
    if end_month:
        query += " AND pnl.month <= ?"
        params.append(end_month)
    query += " ORDER BY pnl.month, p.asset_liability, p.category"
    return pd.read_sql_query(query, con, params=params)


# =============================================================================
# DEFAULT PRODUCT SEED — typical Pakistani bank product mix
# =============================================================================

_DEFAULT_PRODUCTS = [
    # LIABILITIES — Deposits
    dict(product_code="CASA_CURRENT", product_name="Current Accounts (0%)",
         product_type="deposit", asset_liability="L", rate_type="zero",
         reference_rate=None, spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=None, behavioral_maturity_months=36,
         core_pct=0.70, core_tenor_months=36, volatile_tenor_months=0,
         category="CASA", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="CASA_SAVINGS", product_name="PLS Savings Accounts",
         product_type="deposit", asset_liability="L", rate_type="administered",
         reference_rate="SBP_POLICY", spread_bps=-500, repricing_freq_months=1,
         contractual_maturity_months=None, behavioral_maturity_months=24,
         core_pct=0.60, core_tenor_months=24, volatile_tenor_months=0,
         category="CASA", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="TDR_1M", product_name="Term Deposit 1 Month",
         product_type="deposit", asset_liability="L", rate_type="fixed",
         reference_rate="PKRV", spread_bps=-50, repricing_freq_months=None,
         contractual_maturity_months=1, behavioral_maturity_months=1,
         category="TDR", liq_premium_bps=5, optionality_cost_bps=10),

    dict(product_code="TDR_3M", product_name="Term Deposit 3 Months",
         product_type="deposit", asset_liability="L", rate_type="fixed",
         reference_rate="PKRV", spread_bps=-30, repricing_freq_months=None,
         contractual_maturity_months=3, behavioral_maturity_months=3,
         category="TDR", liq_premium_bps=10, optionality_cost_bps=15),

    dict(product_code="TDR_6M", product_name="Term Deposit 6 Months",
         product_type="deposit", asset_liability="L", rate_type="fixed",
         reference_rate="PKRV", spread_bps=-20, repricing_freq_months=None,
         contractual_maturity_months=6, behavioral_maturity_months=6,
         category="TDR", liq_premium_bps=15, optionality_cost_bps=20),

    dict(product_code="TDR_1Y", product_name="Term Deposit 1 Year",
         product_type="deposit", asset_liability="L", rate_type="fixed",
         reference_rate="PKRV", spread_bps=-10, repricing_freq_months=None,
         contractual_maturity_months=12, behavioral_maturity_months=12,
         category="TDR", liq_premium_bps=20, optionality_cost_bps=25),

    dict(product_code="COI_5Y", product_name="Certificate of Investment 5Y",
         product_type="deposit", asset_liability="L", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=60, behavioral_maturity_months=60,
         category="TDR", liq_premium_bps=30, optionality_cost_bps=30),

    # LIABILITIES — Borrowings
    dict(product_code="REPO_ON", product_name="Overnight Repo Borrowing",
         product_type="borrowing", asset_liability="L", rate_type="floating",
         reference_rate="KONIA", spread_bps=0, repricing_freq_months=0,
         contractual_maturity_months=0, behavioral_maturity_months=0,
         category="INTERBANK", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="INTERBANK_CALL", product_name="Interbank Call Borrowing",
         product_type="borrowing", asset_liability="L", rate_type="floating",
         reference_rate="KONIA", spread_bps=10, repricing_freq_months=0,
         contractual_maturity_months=0, behavioral_maturity_months=0,
         category="INTERBANK", liq_premium_bps=0, optionality_cost_bps=0),

    # ASSETS — Lending
    dict(product_code="CORP_KIBOR3M", product_name="Corporate Loan KIBOR+3M",
         product_type="loan", asset_liability="A", rate_type="floating",
         reference_rate="KIBOR_3M", spread_bps=200, repricing_freq_months=3,
         contractual_maturity_months=60, behavioral_maturity_months=60,
         category="CORPORATE", liq_premium_bps=0, optionality_cost_bps=20),

    dict(product_code="CORP_KIBOR6M", product_name="Corporate Loan KIBOR+6M",
         product_type="loan", asset_liability="A", rate_type="floating",
         reference_rate="KIBOR_6M", spread_bps=250, repricing_freq_months=6,
         contractual_maturity_months=84, behavioral_maturity_months=84,
         category="CORPORATE", liq_premium_bps=0, optionality_cost_bps=25),

    dict(product_code="SME_KIBOR3M", product_name="SME Loan KIBOR+3M",
         product_type="loan", asset_liability="A", rate_type="floating",
         reference_rate="KIBOR_3M", spread_bps=400, repricing_freq_months=3,
         contractual_maturity_months=36, behavioral_maturity_months=36,
         category="SME", liq_premium_bps=0, optionality_cost_bps=30),

    dict(product_code="CONSUMER_FIXED", product_name="Consumer Fixed Rate Loan",
         product_type="loan", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=500, repricing_freq_months=None,
         contractual_maturity_months=60, behavioral_maturity_months=48,
         category="CONSUMER", liq_premium_bps=0, optionality_cost_bps=50),

    dict(product_code="AGRI_KIBOR3M", product_name="Agriculture Loan KIBOR+3M",
         product_type="loan", asset_liability="A", rate_type="floating",
         reference_rate="KIBOR_3M", spread_bps=350, repricing_freq_months=3,
         contractual_maturity_months=24, behavioral_maturity_months=24,
         category="CORPORATE", liq_premium_bps=0, optionality_cost_bps=30),

    # ASSETS — Investments (SLR & non-SLR)
    dict(product_code="TBILL_3M", product_name="T-Bill 3 Month",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=3, behavioral_maturity_months=3,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="TBILL_6M", product_name="T-Bill 6 Month",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=6, behavioral_maturity_months=6,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="TBILL_12M", product_name="T-Bill 12 Month",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=12, behavioral_maturity_months=12,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="PIB_3Y", product_name="PIB 3 Year",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=36, behavioral_maturity_months=36,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="PIB_5Y", product_name="PIB 5 Year",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=60, behavioral_maturity_months=60,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="PIB_10Y", product_name="PIB 10 Year",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=120, behavioral_maturity_months=120,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="GIS_3Y", product_name="GoP Ijarah Sukuk 3Y",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKISRV", spread_bps=0, repricing_freq_months=None,
         contractual_maturity_months=36, behavioral_maturity_months=36,
         is_islamic=1, category="SLR", liq_premium_bps=0, optionality_cost_bps=0),

    # ASSETS — Interbank
    dict(product_code="INTERBANK_PLACE", product_name="Interbank Placement (ON)",
         product_type="loan", asset_liability="A", rate_type="floating",
         reference_rate="KONIA", spread_bps=5, repricing_freq_months=0,
         contractual_maturity_months=0, behavioral_maturity_months=0,
         category="INTERBANK", liq_premium_bps=0, optionality_cost_bps=0),

    dict(product_code="REPO_LEND_ON", product_name="Reverse Repo (ON lending)",
         product_type="investment", asset_liability="A", rate_type="floating",
         reference_rate="KONIA", spread_bps=0, repricing_freq_months=0,
         contractual_maturity_months=0, behavioral_maturity_months=0,
         category="INTERBANK", liq_premium_bps=0, optionality_cost_bps=0),

    # FCY Products
    dict(product_code="FCY_DEPOSIT_USD", product_name="USD Deposit Account",
         product_type="deposit", asset_liability="L", rate_type="floating",
         reference_rate="SOFR", spread_bps=-50, repricing_freq_months=3,
         contractual_maturity_months=12, behavioral_maturity_months=12,
         currency="USD", category="TDR", liq_premium_bps=10, optionality_cost_bps=10),

    dict(product_code="NPC_PKR_1Y", product_name="NPC PKR 1 Year",
         product_type="investment", asset_liability="A", rate_type="fixed",
         reference_rate="PKRV", spread_bps=50, repricing_freq_months=None,
         contractual_maturity_months=12, behavioral_maturity_months=12,
         category="SLR", liq_premium_bps=0, optionality_cost_bps=0),
]


def seed_default_products(con: sqlite3.Connection) -> int:
    """Seed default Pakistani bank product catalog. Returns count seeded."""
    count = 0
    for prod in _DEFAULT_PRODUCTS:
        if upsert_alm_product(con, prod):
            count += 1
    return count
