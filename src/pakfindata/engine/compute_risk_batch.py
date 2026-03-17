"""
Batch compute risk metrics for ALL mutual funds and store in fund_risk_metrics table.

Usage:
    python -m pakfindata.engine.compute_risk_batch
    python -m pakfindata.engine.compute_risk_batch --fund-id MUFAP:ABL-CSF
    python -m pakfindata.engine.compute_risk_batch --since 2025-01-01
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

from pakfindata.config import get_db_path
from pakfindata.engine.benchmark import get_benchmark_nav, get_risk_free_rate
from pakfindata.engine.fund_risk import (
    TRADING_DAYS,
    _log_returns,
    _simple_returns,
    calc_alpha_from_nav,
    capture_ratios,
    information_ratio,
    maximum_drawdown,
    rolling_beta,
    rolling_sharpe,
    rolling_sortino,
    value_at_risk,
    compute_treynor_ratio,
    compute_calendar_year_returns,
)
from pakfindata.engine.fund_factors import single_factor_regression


MIN_NAV_RECORDS = 30


def _period_return(nav: pd.Series, days: int | None = None) -> float | None:
    if days is None:
        if len(nav) < 2:
            return None
        return float((nav.iloc[-1] / nav.iloc[0]) - 1)
    if len(nav) < days:
        return None
    return float((nav.iloc[-1] / nav.iloc[-days]) - 1)


def _ytd_return(nav: pd.Series) -> float | None:
    if len(nav) < 2:
        return None
    year_start = nav.index[-1].replace(month=1, day=1)
    ytd_nav = nav[nav.index >= year_start]
    if len(ytd_nav) < 2:
        return None
    return float((ytd_nav.iloc[-1] / ytd_nav.iloc[0]) - 1)


def compute_fund_metrics(
    fund_id: str,
    fund_name: str,
    category: str,
    nav_series: pd.Series,
    benchmark_nav: pd.Series | None,
    risk_free_rate: float,
) -> dict:
    """Compute all risk metrics for a single fund."""
    nav = nav_series.dropna().sort_index()
    rets = _log_returns(nav)

    result = {
        "fund_id": fund_id,
        "fund_name": fund_name,
        "category": category,
        "nav_count": len(nav),
        "first_nav_date": str(nav.index[0].date()),
        "last_nav_date": str(nav.index[-1].date()),
        "computed_at": datetime.now().isoformat(timespec="seconds"),
    }

    # ── Returns ──
    result["return_1m"] = _period_return(nav, 21)
    result["return_3m"] = _period_return(nav, 63)
    result["return_6m"] = _period_return(nav, 126)
    result["return_1y"] = _period_return(nav, 252)
    result["return_2y"] = _period_return(nav, 504)
    result["return_3y"] = _period_return(nav, 756)
    result["return_5y"] = _period_return(nav, 1260)
    result["return_ytd"] = _ytd_return(nav)
    result["return_since_inception"] = _period_return(nav, None)

    # ── Volatility ──
    if len(rets) >= 252:
        result["volatility_1y"] = float(
            rets.iloc[-252:].std(ddof=1) * math.sqrt(TRADING_DAYS)
        )

    # ── Sharpe ──
    if len(rets) >= 252:
        sharpe_s = rolling_sharpe(nav, window=252, risk_free_rate=risk_free_rate)
        last = sharpe_s.iloc[-1] if len(sharpe_s) > 0 else np.nan
        if not np.isnan(last):
            result["sharpe_ratio"] = round(float(last), 4)

    # ── Sortino ──
    if len(rets) >= 252:
        sortino_s = rolling_sortino(nav, window=252, risk_free_rate=risk_free_rate)
        last = sortino_s.iloc[-1] if len(sortino_s) > 0 else np.nan
        if not np.isnan(last):
            result["sortino_ratio"] = round(float(last), 4)

    # ── Max Drawdown ──
    dd = maximum_drawdown(nav)
    result["max_drawdown"] = round(dd["max_drawdown"], 6)
    if dd["max_drawdown_start"] is not None:
        result["max_drawdown_start"] = str(dd["max_drawdown_start"])[:10]
    if dd["max_drawdown_end"] is not None:
        result["max_drawdown_end"] = str(dd["max_drawdown_end"])[:10]

    # ── VaR / CVaR ──
    var = value_at_risk(nav, window=min(252, len(nav)))
    if var["var_95"] is not None:
        result["var_95"] = round(var["var_95"], 6)
    if var["cvar_95"] is not None:
        result["cvar_95"] = round(var["cvar_95"], 6)

    # ── Benchmark-relative metrics ──
    if benchmark_nav is not None and len(benchmark_nav) >= 30:
        # Beta
        try:
            beta_s = rolling_beta(nav, benchmark_nav, window=min(252, len(nav) - 1))
            last_beta = beta_s.iloc[-1] if len(beta_s) > 0 else np.nan
            if not np.isnan(last_beta):
                result["beta"] = round(float(last_beta), 4)
        except Exception:
            pass

        # Alpha
        try:
            alpha_val = calc_alpha_from_nav(nav, benchmark_nav, risk_free_rate)
            if not np.isnan(alpha_val):
                result["alpha"] = round(alpha_val, 4)
        except Exception:
            pass

        # R-squared
        try:
            reg = single_factor_regression(nav, benchmark_nav, risk_free_rate)
            if reg["r_squared"] is not None:
                result["r_squared"] = round(reg["r_squared"], 4)
            if reg.get("residual_std") is not None:
                # Tracking error = annualized residual std
                fr = _log_returns(nav)
                br = _log_returns(benchmark_nav)
                aligned = pd.DataFrame({"fund": fr, "bench": br}).dropna()
                if len(aligned) > 1:
                    te = float(
                        aligned["fund"]
                        .sub(aligned["bench"])
                        .std(ddof=1)
                        * math.sqrt(TRADING_DAYS)
                    )
                    result["tracking_error"] = round(te, 4)
        except Exception:
            pass

        # Treynor Ratio
        try:
            fr = _log_returns(nav)
            br = _log_returns(benchmark_nav)
            treynor = compute_treynor_ratio(fr, br, risk_free_rate)
            if treynor is not None:
                result["treynor_ratio"] = round(treynor, 4)
        except Exception:
            pass

        # Information Ratio
        try:
            ir = information_ratio(nav, benchmark_nav)
            if not np.isnan(ir):
                result["information_ratio"] = round(ir, 4)
        except Exception:
            pass

        # Capture ratios
        try:
            cap = capture_ratios(nav, benchmark_nav)
            if cap["up_capture"] is not None:
                result["up_capture"] = round(cap["up_capture"], 2)
            if cap["down_capture"] is not None:
                result["down_capture"] = round(cap["down_capture"], 2)
        except Exception:
            pass

    return result


def run_batch(
    fund_id: str | None = None,
    since: str | None = None,
) -> None:
    db_path = str(get_db_path())
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # ── Get risk-free rate ──
    rf = get_risk_free_rate(con)
    print(f"Risk-free rate: {rf*100:.2f}%")

    # ── Get benchmark (KSE-100) ──
    benchmark_nav = get_benchmark_nav(con, "KSE-100")
    if benchmark_nav.empty:
        print("WARNING: No KSE-100 benchmark data — skipping relative metrics")
        benchmark_nav = None
    else:
        print(f"KSE-100 benchmark: {len(benchmark_nav)} days ({benchmark_nav.index[0].date()} to {benchmark_nav.index[-1].date()})")

    # ── Get fund list ──
    if fund_id:
        funds = con.execute(
            "SELECT fund_id, fund_name, category FROM mutual_funds WHERE fund_id = ?",
            (fund_id,),
        ).fetchall()
    else:
        funds = con.execute(
            "SELECT fund_id, fund_name, category FROM mutual_funds"
        ).fetchall()

    total = len(funds)
    print(f"Processing {total} funds...")

    # ── Bulk-load all NAV data sorted for efficiency ──
    if fund_id:
        all_nav = pd.read_sql_query(
            "SELECT fund_id, date, nav FROM mutual_fund_nav WHERE fund_id = ? AND nav > 0 ORDER BY fund_id, date",
            con,
            params=(fund_id,),
        )
    elif since:
        # Only funds with new NAV data since cutoff
        all_nav = pd.read_sql_query(
            f"SELECT fund_id, date, nav FROM mutual_fund_nav WHERE nav > 0 AND fund_id IN "
            f"(SELECT DISTINCT fund_id FROM mutual_fund_nav WHERE date >= ?) ORDER BY fund_id, date",
            con,
            params=(since,),
        )
    else:
        all_nav = pd.read_sql_query(
            "SELECT fund_id, date, nav FROM mutual_fund_nav WHERE nav > 0 ORDER BY fund_id, date",
            con,
        )

    all_nav["date"] = pd.to_datetime(all_nav["date"])
    nav_grouped = {fid: grp.set_index("date")["nav"] for fid, grp in all_nav.groupby("fund_id")}
    del all_nav  # free memory

    computed = 0
    skipped = 0
    errors = 0
    t0 = time.time()

    for i, fund in enumerate(funds):
        fid = fund["fund_id"]
        fname = fund["fund_name"]
        cat = fund["category"]

        nav_series = nav_grouped.get(fid)
        if nav_series is None or len(nav_series) < MIN_NAV_RECORDS:
            skipped += 1
            continue

        try:
            metrics = compute_fund_metrics(fid, fname, cat, nav_series, benchmark_nav, rf)

            # INSERT OR REPLACE into fund_risk_metrics
            cols = list(metrics.keys())
            placeholders = ", ".join(["?"] * len(cols))
            col_names = ", ".join(cols)
            con.execute(
                f"INSERT OR REPLACE INTO fund_risk_metrics ({col_names}) VALUES ({placeholders})",
                [metrics.get(c) for c in cols],
            )

            # Calendar year returns
            cal_returns = compute_calendar_year_returns(nav_series)
            now_str = datetime.now().isoformat(timespec="seconds")
            for year, ret_pct in cal_returns.items():
                year_nav = nav_series[nav_series.index.year == year]
                con.execute(
                    """INSERT OR REPLACE INTO fund_calendar_returns
                       (fund_id, year, return_pct, first_nav, last_nav, trading_days, computed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (fid, year, ret_pct, float(year_nav.iloc[0]), float(year_nav.iloc[-1]),
                     len(year_nav), now_str),
                )

            sharpe_str = f"Sharpe: {metrics.get('sharpe_ratio', 'N/A')}"
            dd_str = f"MaxDD: {metrics.get('max_drawdown', 0)*100:.1f}%" if metrics.get("max_drawdown") else "MaxDD: N/A"
            print(f"  [{i+1:4d}/{total}] {fname[:40]:<40s} — {sharpe_str}, {dd_str}")
            computed += 1

            if computed % 50 == 0:
                con.commit()

        except Exception as e:
            errors += 1
            print(f"  [{i+1:4d}/{total}] ERROR {fname}: {e}")

    con.commit()
    elapsed = time.time() - t0

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Computed: {computed} funds")
    print(f"  Skipped:  {skipped} (< {MIN_NAV_RECORDS} NAV records)")
    print(f"  Errors:   {errors}")

    final = con.execute("SELECT COUNT(*) FROM fund_risk_metrics").fetchone()[0]
    print(f"  fund_risk_metrics: {final} rows total")
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Batch compute fund risk metrics")
    parser.add_argument("--fund-id", help="Compute for a single fund ID")
    parser.add_argument("--since", help="Only recompute funds with NAV data since date (YYYY-MM-DD)")
    args = parser.parse_args()
    run_batch(fund_id=args.fund_id, since=args.since)


if __name__ == "__main__":
    main()
