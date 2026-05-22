"""PSX Debt Intelligence Terminal — Bloomberg-style bond analytics dashboard.

Merges PSX debt market data with SBP benchmark rates (KIBOR, PKRV/IFRV)
to provide real-time valuation, risk metrics, spread analysis, and rate
shock simulation for Pakistan's fixed income securities.

Features:
- Security blotter with persona-based column views (Treasury/Investor/Quant)
- Yield curve overlay (PKRV + IFRV + traded dots)
- Hike/Cut rate shock simulator with MTM impact
- Rich/Cheap/Fair signal badges
- Liquidity gauges
- Tax calculator (Filer/Non-Filer/Zakat)

Data Sources (live from DB with fallback):
- fi_instruments + fi_quotes → bond registry & prices
- pkrv_daily → PKRV yield curve
- pkisrv_daily → PKISRV (Islamic) curve
- kibor_daily → KIBOR rates
- sbp_benchmark_snapshot → policy rate & other benchmarks
"""

import calendar
import math
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer

# ── Terminal Theme CSS ──────────────────────────────────────────────────────

TERMINAL_CSS = """
<style>
.debt-terminal-header {
    background: rgba(0,0,0,0);
    padding: 16px 24px;
    border-bottom: 1px solid #30363D;
    border-radius: 8px;
    margin-bottom: 16px;
}
.debt-terminal-header h1 {
    margin: 0; font-size: 28px; font-weight: 800; letter-spacing: 2px;
}
.debt-terminal-header .pak { color: #22c55e; }
.debt-terminal-header .fin { color: #e2e8f0; }
.debt-terminal-header .data { color: #6B7280; }
.debt-terminal-header .subtitle {
    color: #6B7280; font-size: 12px; margin-top: 4px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
}

.badge {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 10px; font-weight: 700; letter-spacing: 1px;
    text-transform: uppercase;
}
.badge-cheap { background: rgba(34,197,94,0.15); color: #22c55e; }
.badge-fair { background: rgba(255,179,0,0.15); color: #FFB300; }
.badge-rich { background: rgba(255,82,82,0.15); color: #FF5252; }
.badge-islamic { background: rgba(255,179,0,0.15); color: #FFB300; }
.badge-conventional { background: rgba(47,129,247,0.15); color: #2F81F7; }
.badge-stale { background: rgba(255,82,82,0.15); color: #FF5252; }

.liquidity-bar {
    height: 6px; border-radius: 3px; background: #30363D; width: 48px;
    display: inline-block; vertical-align: middle;
}
.liquidity-fill {
    height: 100%; border-radius: 3px;
}
.liq-high { background: #22c55e; width: 90%; }
.liq-med { background: #FFB300; width: 55%; }
.liq-low { background: #FF5252; width: 20%; }

.terminal-footer {
    color: #6B7280; font-size: 11px; padding: 12px 0;
    border-top: 1px solid #30363D;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
}

.sim-alert {
    padding: 10px 16px; border-radius: 6px; margin-top: 10px;
    font-size: 13px;
}
.sim-alert-ease { background: rgba(34,197,94,0.1); color: #22c55e; border: 1px solid rgba(34,197,94,0.2); }
.sim-alert-tight { background: rgba(255,82,82,0.1); color: #FF5252; border: 1px solid rgba(255,82,82,0.2); }

.data-source-bar {
    font-size: 11px; color: #6B7280; padding: 6px 12px;
    background: rgba(0,0,0,0); border-radius: 4px;
    margin-bottom: 12px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.data-source-bar .live { color: #22c55e; }
.data-source-bar .fallback { color: #FFB300; }
</style>
"""

# ── Fallback Data ──────────────────────────────────────────────────────────
# Used when database tables are empty or unavailable.

_FALLBACK_PKRV = [
    {"tenor": "3M", "years": 0.25, "yield": 11.25},
    {"tenor": "6M", "years": 0.5, "yield": 11.40},
    {"tenor": "1Y", "years": 1, "yield": 11.65},
    {"tenor": "2Y", "years": 2, "yield": 11.85},
    {"tenor": "3Y", "years": 3, "yield": 12.10},
    {"tenor": "5Y", "years": 5, "yield": 12.45},
    {"tenor": "7Y", "years": 7, "yield": 12.65},
    {"tenor": "10Y", "years": 10, "yield": 12.80},
    {"tenor": "15Y", "years": 15, "yield": 12.95},
    {"tenor": "20Y", "years": 20, "yield": 13.05},
]

_FALLBACK_IFRV = [
    {"tenor": "3M", "years": 0.25, "yield": 11.05},
    {"tenor": "6M", "years": 0.5, "yield": 11.20},
    {"tenor": "1Y", "years": 1, "yield": 11.40},
    {"tenor": "2Y", "years": 2, "yield": 11.60},
    {"tenor": "3Y", "years": 3, "yield": 11.85},
    {"tenor": "5Y", "years": 5, "yield": 12.20},
    {"tenor": "7Y", "years": 7, "yield": 12.40},
    {"tenor": "10Y", "years": 10, "yield": 12.55},
]

_FALLBACK_KIBOR = {"6M": 12.08, "1Y": 12.18}

_FALLBACK_BONDS = [
    {
        "symbol": "P03FRR050329",
        "name": "3Y FRR Sukuk (mat. 2029-03-05)",
        "coupon_rate": 0.1225,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2029-03-05",
        "last_coupon_date": "2026-03-05",
        "is_islamic": True,
        "is_slr_eligible": True,
        "security_type": "FRR Sukuk",
    },
    {
        "symbol": "P05FRR280629",
        "name": "5Y FRR Sukuk (mat. 2029-06-28)",
        "coupon_rate": 0.1275,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2029-06-28",
        "last_coupon_date": "2025-12-28",
        "is_islamic": True,
        "is_slr_eligible": True,
        "security_type": "FRR Sukuk",
    },
    {
        "symbol": "P10PIB150735",
        "name": "10Y PIB Fixed (mat. 2035-07-15)",
        "coupon_rate": 0.1350,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2035-07-15",
        "last_coupon_date": "2026-01-15",
        "is_islamic": False,
        "is_slr_eligible": True,
        "security_type": "PIB",
    },
    {
        "symbol": "P03GIS120329",
        "name": "3Y Ijara Sukuk (mat. 2029-03-12)",
        "coupon_rate": 0.1200,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2029-03-12",
        "last_coupon_date": "2026-03-05",
        "is_islamic": True,
        "is_slr_eligible": True,
        "security_type": "GIS",
    },
    {
        "symbol": "P05GIS200630",
        "name": "5Y Ijara Sukuk (mat. 2030-06-20)",
        "coupon_rate": 0.1250,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2030-06-20",
        "last_coupon_date": "2025-12-20",
        "is_islamic": True,
        "is_slr_eligible": True,
        "security_type": "GIS",
    },
    {
        "symbol": "P07PIB100732",
        "name": "7Y PIB Fixed (mat. 2032-07-10)",
        "coupon_rate": 0.1300,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2032-07-10",
        "last_coupon_date": "2026-01-10",
        "is_islamic": False,
        "is_slr_eligible": True,
        "security_type": "PIB",
    },
    {
        "symbol": "P15PIB220940",
        "name": "15Y PIB Fixed (mat. 2040-09-22)",
        "coupon_rate": 0.1400,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2040-09-22",
        "last_coupon_date": "2025-09-22",
        "is_islamic": False,
        "is_slr_eligible": True,
        "security_type": "PIB",
    },
    {
        "symbol": "P20PIB011245",
        "name": "20Y PIB Fixed (mat. 2045-12-01)",
        "coupon_rate": 0.1425,
        "face_value": 100,
        "frequency": 2,
        "maturity_date": "2045-12-01",
        "last_coupon_date": "2025-12-01",
        "is_islamic": False,
        "is_slr_eligible": True,
        "security_type": "PIB",
    },
]

_FALLBACK_TRADES = {
    "P03FRR050329": {"price": 100.35, "volume": 1200, "weighted_avg": 100.30},
    "P05FRR280629": {"price": 100.70, "volume": 1800, "weighted_avg": 100.65},
    "P10PIB150735": {"price": 103.50, "volume": 600, "weighted_avg": 103.45},
    "P03GIS120329": {"price": 100.45, "volume": 800, "weighted_avg": 0},
    "P05GIS200630": {"price": 100.80, "volume": 0, "weighted_avg": 0},
    "P07PIB100732": {"price": 101.20, "volume": 2400, "weighted_avg": 101.15},
    "P15PIB220940": {"price": 104.80, "volume": 300, "weighted_avg": 104.75},
    "P20PIB011245": {"price": 105.80, "volume": 50, "weighted_avg": 105.70},
}


# ── Tenor & Date Helpers ──────────────────────────────────────────────────

_TENOR_MONTHS_TO_LABEL = {
    1: "1M", 3: "3M", 6: "6M", 9: "9M", 12: "1Y",
    24: "2Y", 36: "3Y", 60: "5Y", 84: "7Y", 120: "10Y",
    180: "15Y", 240: "20Y", 360: "30Y",
}


def _tenor_text_to_years(tenor: str) -> float:
    """Convert tenor text like '6M', '1Y', '3Y' to years."""
    tenor = tenor.upper().strip()
    if tenor.endswith("M"):
        try:
            return int(tenor[:-1]) / 12
        except ValueError:
            return 0.0
    elif tenor.endswith("Y"):
        try:
            return float(tenor[:-1])
        except ValueError:
            return 0.0
    elif tenor.endswith("W"):
        try:
            return int(tenor[:-1]) / 52
        except ValueError:
            return 0.0
    return 0.0


def _estimate_last_coupon_date(
    maturity_str: str, issue_str: str | None = None, frequency: int = 2
) -> str:
    """Estimate the most recent coupon payment date before today."""
    today = date.today()
    months_per_cpn = 12 // max(frequency, 1)

    # Try working forward from issue date
    if issue_str:
        try:
            cpn = datetime.strptime(issue_str, "%Y-%m-%d").date()
            last_cpn = cpn
            while cpn <= today:
                last_cpn = cpn
                m = cpn.month + months_per_cpn
                y = cpn.year + (m - 1) // 12
                m = (m - 1) % 12 + 1
                d = min(cpn.day, calendar.monthrange(y, m)[1])
                cpn = date(y, m, d)
            return last_cpn.strftime("%Y-%m-%d")
        except Exception:
            pass

    # Work backward from maturity
    try:
        mat = datetime.strptime(maturity_str, "%Y-%m-%d").date()
        cpn = mat
        while cpn > today:
            m = cpn.month - months_per_cpn
            y = cpn.year
            while m <= 0:
                m += 12
                y -= 1
            d = min(cpn.day, calendar.monthrange(y, m)[1])
            cpn = date(y, m, d)
        return cpn.strftime("%Y-%m-%d")
    except Exception:
        return today.strftime("%Y-%m-%d")


# ── Math Engine (pure functions, no UI) ─────────────────────────────────────

def _days_between(d1: str, d2: str) -> int:
    """Actual days between two YYYY-MM-DD date strings."""
    dt1 = datetime.strptime(d1, "%Y-%m-%d")
    dt2 = datetime.strptime(d2, "%Y-%m-%d")
    return (dt2 - dt1).days


def _accrued_interest(coupon_rate: float, last_coupon_date: str,
                      settlement_date: str, face_value: float = 100) -> float:
    """AI = (CouponRate / 2) * (DaysSinceLastCoupon / 182) * FaceValue."""
    days = _days_between(last_coupon_date, settlement_date)
    return (coupon_rate / 2) * (days / 182) * face_value


def _ytm_newton_raphson(dirty_price: float, coupon_rate: float, face_value: float,
                        n_periods: int, frequency: int = 2,
                        tol: float = 1e-4, max_iter: int = 100) -> float | None:
    """Solve YTM via Newton-Raphson with analytical derivative."""
    if n_periods <= 0 or dirty_price <= 0:
        return None

    coupon = (coupon_rate * face_value) / frequency
    y = coupon_rate  # initial guess

    for _ in range(max_iter):
        r = y / frequency
        if r <= -1:
            r = 0.001

        # f(y) = sum CF/(1+r)^t + FV/(1+r)^n - DirtyPrice
        pv = 0.0
        dpv = 0.0  # analytical derivative
        for t in range(1, n_periods + 1):
            df = (1 + r) ** t
            pv += coupon / df
            dpv += -t * coupon / (df * (1 + r))  # d/dr [C/(1+r)^t]

        pv += face_value / ((1 + r) ** n_periods)
        dpv += -n_periods * face_value / ((1 + r) ** (n_periods + 1))

        # Scale derivative by 1/freq since y = r * freq
        dpv /= frequency

        f_val = pv - dirty_price
        if abs(f_val) < tol:
            return y

        if abs(dpv) < 1e-12:
            break
        y = y - f_val / dpv

    return y if abs(pv - dirty_price) < tol * 100 else None


def _macaulay_duration(coupon_rate: float, face_value: float, ytm: float,
                       n_periods: int, frequency: int = 2,
                       price: float = 100) -> float | None:
    """Macaulay Duration in years."""
    if n_periods <= 0 or price <= 0:
        return None
    coupon = (coupon_rate * face_value) / frequency
    r = ytm / frequency
    if r <= -1:
        return None

    weighted = 0.0
    for t in range(1, n_periods + 1):
        df = (1 + r) ** t
        weighted += (t / frequency) * coupon / df

    weighted += (n_periods / frequency) * face_value / ((1 + r) ** n_periods)
    return weighted / price


def _modified_duration(mac_dur: float, ytm: float, frequency: int = 2) -> float:
    """Modified Duration = MacDur / (1 + YTM/freq)."""
    return mac_dur / (1 + ytm / frequency)


def _convexity(coupon_rate: float, face_value: float, ytm: float,
               n_periods: int, frequency: int = 2, price: float = 100) -> float | None:
    """Convexity = sum(t(t+1)*CF*DF) / (Price * freq^2 * (1+y/freq)^2)."""
    if n_periods <= 0 or price <= 0:
        return None
    coupon = (coupon_rate * face_value) / frequency
    r = ytm / frequency
    if r <= -1:
        return None

    weighted = 0.0
    for t in range(1, n_periods + 1):
        df = (1 + r) ** t
        weighted += t * (t + 1) * coupon / df

    weighted += n_periods * (n_periods + 1) * face_value / ((1 + r) ** n_periods)
    return weighted / (price * frequency ** 2 * (1 + r) ** 2)


def _price_sensitivity(mod_dur: float, convexity: float, delta_y: float) -> float:
    """dP% ~ -ModDur * dy + 0.5 * Convexity * dy^2."""
    return -mod_dur * delta_y + 0.5 * convexity * delta_y ** 2


def _interpolate_curve(curve: list[dict], target_years: float) -> float:
    """Linear interpolation on a yield curve."""
    if not curve:
        return 0.0
    sorted_c = sorted(curve, key=lambda p: p["years"])
    if target_years <= sorted_c[0]["years"]:
        return sorted_c[0]["yield"]
    if target_years >= sorted_c[-1]["years"]:
        return sorted_c[-1]["yield"]
    for i in range(len(sorted_c) - 1):
        y1, y2 = sorted_c[i]["years"], sorted_c[i + 1]["years"]
        if y1 <= target_years <= y2:
            w = (target_years - y1) / (y2 - y1) if y2 != y1 else 0
            return sorted_c[i]["yield"] + w * (sorted_c[i + 1]["yield"] - sorted_c[i]["yield"])
    return sorted_c[-1]["yield"]


def _spread_signal(spread_bps: float) -> str:
    """CHEAP / FAIR / RICH based on spread vs benchmark."""
    if spread_bps > 20:
        return "CHEAP"
    elif spread_bps < -20:
        return "RICH"
    return "FAIR"


def _liquidity_level(volume: float) -> str:
    """HIGH / MED / LOW based on volume."""
    if volume >= 2000:
        return "HIGH"
    elif volume >= 500:
        return "MED"
    return "LOW"


# ── Database Loaders ───────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def _load_pkrv_curve() -> tuple[list[dict], str | None]:
    """Load latest PKRV yield curve via /v1/yield-curves/pkrv."""
    try:
        rows = api_client.get_pkrv() or []
        if rows:
            curve_date = rows[0].get("date")
            result = []
            for row in rows:
                months = int(row.get("tenor_months") or 0)
                years = months / 12 if months else 0
                label = _TENOR_MONTHS_TO_LABEL.get(months, f"{months}M")
                yp = row.get("yield_pct")
                if yp is None:
                    continue
                result.append({
                    "tenor": label,
                    "years": round(years, 4),
                    "yield": float(yp),
                })
            if result:
                return result, curve_date
    except Exception:
        pass
    return list(_FALLBACK_PKRV), None


@st.cache_data(ttl=300)
def _load_ifrv_curve(pkrv_curve_json: str = "[]") -> tuple[list[dict], str | None]:
    """Load latest PKISRV (Islamic) curve from pkisrv_daily.

    Since PKISRV data typically covers only short tenors (1M-1Y),
    longer tenors are estimated from PKRV minus a spread derived from
    the 6M+ overlap zone (where Islamic & conventional rates converge).
    Very short tenors (<3M) are excluded because Islamic overnight/1M
    money market rates diverge significantly from conventional rates
    due to different liquidity dynamics.
    """
    import json
    pkrv_curve = json.loads(pkrv_curve_json)

    try:
        rows = api_client.get_pkisrv() or []
        if rows:
            curve_date = rows[0].get("date")
            result = []
            for row in rows:
                tenor = row.get("tenor")
                yp = row.get("yield_pct")
                if not tenor or yp is None:
                    continue
                years = _tenor_text_to_years(tenor)
                # Skip tenors below 6M — Islamic money market rates at the
                # very short end (1M, 3M) diverge 100-200bps from conventional
                # due to excess Islamic bank liquidity, which distorts the
                # yield curve chart y-axis and is not comparable to PKRV.
                if years >= 0.5:
                    result.append({
                        "tenor": tenor,
                        "years": round(years, 4),
                        "yield": float(yp),
                    })

            # Extend with PKRV-spread estimates for longer tenors
            if pkrv_curve and result:
                max_isrv_years = max(p["years"] for p in result)
                # Use only 6M+ tenors for spread calc — short-end Islamic
                # rates can be 100-200bps below conventional due to market
                # structure, but this spread narrows at 6M+ tenors.
                spreads = []
                for ip in result:
                    if ip["years"] >= 0.5:  # 6M+
                        pkrv_val = _interpolate_curve(pkrv_curve, ip["years"])
                        if pkrv_val > 0:
                            spreads.append(pkrv_val - ip["yield"])
                avg_spread = sum(spreads) / len(spreads) if spreads else 0.15

                for pp in pkrv_curve:
                    if pp["years"] > max_isrv_years:
                        result.append({
                            "tenor": pp["tenor"],
                            "years": pp["years"],
                            "yield": round(pp["yield"] - avg_spread, 2),
                        })

            return sorted(result, key=lambda x: x["years"]), curve_date
    except Exception:
        pass
    return list(_FALLBACK_IFRV), None


@st.cache_data(ttl=300)
def _load_kibor_rates() -> tuple[dict, str | None]:
    """Latest KIBOR offer rates via /v1/rates/kibor/latest-per-tenor."""
    try:
        rows = api_client.get_kibor_latest_per_tenor() or []
        if rows:
            kibor_date = rows[0].get("date")
            rates: dict[str, float] = {}
            for row in rows:
                offer = row.get("offer")
                tenor = row.get("tenor")
                if offer is not None and tenor:
                    rates[tenor] = float(offer)
            if rates:
                return rates, kibor_date
    except Exception:
        pass
    return dict(_FALLBACK_KIBOR), None


@st.cache_data(ttl=300)
def _load_securities_from_db() -> tuple[list[dict], dict[str, dict], str]:
    """Load FI securities and latest quotes from DB.

    Returns:
        (bond_list, quotes_dict, data_source)
        - bond_list: list of bond dicts with registry info
        - quotes_dict: maps instrument_id -> {price, volume, ...}
        - data_source: "db" or "fallback"
    """
    bonds = []
    quotes: dict[str, dict] = {}

    # Load instruments via /v1/fi/instruments. The API filters
    # active_only=1; the maturity/category/coupon filters that the
    # legacy SQL applied are now applied client-side below.
    try:
        from datetime import date as _date
        today_iso = _date.today().isoformat()
        all_rows = api_client.get_fi_instruments(active_only=True, limit=5000) or []
        rows = [
            r for r in all_rows
            if (r.get("maturity_date") or "0000-00-00") > today_iso
            and (r.get("category") or "") not in ("MTB",)
            and r.get("coupon_rate") is not None
            and (r.get("coupon_rate") or 0) > 0
        ]

        for row in rows:
            mat_str = row["maturity_date"]
            freq = row["coupon_frequency"] or 2
            coupon = float(row["coupon_rate"] or 0)
            issue_str = row["issue_date"]
            is_islamic = bool(row["shariah_compliant"])
            cat = row["category"] or ""

            # Determine security type from category
            if cat in ("GOP_SUKUK", "CORP_SUKUK"):
                sec_type = "Sukuk"
            elif cat == "PIB":
                sec_type = "PIB"
            else:
                sec_type = cat or "Bond"

            # Calculate last coupon date
            last_cpn = _estimate_last_coupon_date(mat_str, issue_str, freq)

            bonds.append({
                "symbol": row["instrument_id"],
                "name": row["name"] or row["instrument_id"],
                "coupon_rate": coupon,
                "face_value": float(row["face_value"] or 100),
                "frequency": freq,
                "maturity_date": mat_str,
                "last_coupon_date": last_cpn,
                "is_islamic": is_islamic,
                "is_slr_eligible": cat in ("PIB", "MTB", "GOP_SUKUK"),
                "security_type": sec_type,
            })
    except Exception:
        pass

    # Load latest quotes via /v1/fi/quotes/latest
    try:
        quote_rows = api_client.get_fi_quotes_latest() or []
        for row in quote_rows:
            iid = row.get("instrument_id")
            if not iid:
                continue
            quotes[iid] = {
                "price": float(row["clean_price"]) if row.get("clean_price") else 100,
                "volume": float(row.get("volume") or 0),
                "weighted_avg": 0,
                "quote_date": row.get("quote_date"),
            }
    except Exception:
        pass

    if bonds:
        return bonds, quotes, "db"
    return list(_FALLBACK_BONDS), dict(_FALLBACK_TRADES), "fallback"


@st.cache_data(ttl=300)
def _load_price_history(instrument_id: str, days: int = 60) -> list[dict]:
    """Price history for an instrument via /v1/fi/quotes/{id}/history."""
    try:
        rows = api_client.get_fi_quotes_history(instrument_id, days=days) or []
        return [
            {
                "date": r.get("quote_date"),
                "price": float(r.get("clean_price") or 0),
                "volume": float(r.get("volume") or 0),
            }
            for r in rows
            if r.get("clean_price")
        ]
    except Exception:
        pass
    return []


def _load_benchmark_from_db() -> dict:
    """SBP benchmark snapshot via /v1/benchmark/snapshot.

    Returns the flat ``metrics`` dict (legacy callers expect that
    shape — the snapshot date is dropped here; callers don't use it).
    """
    try:
        payload = api_client.get_benchmark_snapshot() or {}
        return payload.get("metrics") or {}
    except Exception:
        pass
    return {}


# ── Compute all analytics for a security ────────────────────────────────────

def _compute_security_analytics(
    bond: dict,
    trade: dict,
    pkrv_curve: list[dict],
    ifrv_curve: list[dict],
    curve_shift_bps: int = 0,
) -> dict:
    """Compute all analytics for a single security given registry + trade data."""
    today = date.today().strftime("%Y-%m-%d")
    mat = bond["maturity_date"]
    coupon = bond["coupon_rate"]
    face = bond["face_value"]
    freq = bond.get("frequency", 2)
    is_islamic = bond.get("is_islamic", False)

    # Effective clean price (Weighted_Avg=0 → use Price)
    wa = trade.get("weighted_avg", 0)
    clean_price = wa if wa and wa > 0 else trade.get("price", 100)
    volume = trade.get("volume", 0)

    # Time to maturity
    days_to_mat = _days_between(today, mat)
    years_to_mat = days_to_mat / 365.0
    n_periods = max(1, round(years_to_mat * freq))

    # Accrued interest
    last_cpn = bond.get("last_coupon_date", today)
    ai = _accrued_interest(coupon, last_cpn, today, face)
    dirty_price = clean_price + ai
    days_since_cpn = _days_between(last_cpn, today)

    # YTM
    ytm = _ytm_newton_raphson(dirty_price, coupon, face, n_periods, freq)
    ytm_pct = (ytm or 0) * 100

    # Duration & Convexity
    mac_dur = _macaulay_duration(coupon, face, ytm or coupon, n_periods, freq, dirty_price) if ytm else None
    mod_dur = _modified_duration(mac_dur, ytm or coupon, freq) if mac_dur else None
    cnvx = _convexity(coupon, face, ytm or coupon, n_periods, freq, dirty_price) if ytm else None

    # Benchmark & Spread — use appropriate curve
    curve = ifrv_curve if is_islamic else pkrv_curve
    shifted_curve = [
        {**p, "yield": p["yield"] + curve_shift_bps / 100}
        for p in curve
    ]
    bench_yield = _interpolate_curve(shifted_curve, years_to_mat)
    spread_bps = (ytm_pct - bench_yield) * 100 if ytm else 0
    signal = _spread_signal(spread_bps)

    # Price sensitivity to 1% rate shock
    price_impact_1pct = _price_sensitivity(mod_dur or 0, cnvx or 0, 0.01) * 100

    # Shifted price (for simulator)
    if curve_shift_bps != 0 and mod_dur and cnvx:
        dy = curve_shift_bps / 10000  # bps to decimal
        shifted_pct = _price_sensitivity(mod_dur, cnvx, dy)
        shifted_price = clean_price * (1 + shifted_pct)
        pnl_pct = shifted_pct * 100
    else:
        shifted_price = clean_price
        pnl_pct = 0.0

    # Tax (semi-annual coupon)
    gross_sa = (coupon / 2) * face
    net_sa_filer = gross_sa * (1 - 0.15)
    net_sa_nonfiler = gross_sa * (1 - 0.30)
    net_monthly_filer = net_sa_filer / 6
    net_monthly_nonfiler = net_sa_nonfiler / 6

    liq = _liquidity_level(volume)

    return {
        "symbol": bond["symbol"],
        "name": bond.get("name", bond["symbol"]),
        "security_type": bond.get("security_type", "PIB"),
        "is_islamic": is_islamic,
        "is_slr_eligible": bond.get("is_slr_eligible", False),
        "maturity_date": mat,
        "years_to_mat": round(years_to_mat, 2),
        "clean_price": round(clean_price, 2),
        "accrued_interest": round(ai, 4),
        "dirty_price": round(dirty_price, 4),
        "days_since_coupon": days_since_cpn,
        "ytm_pct": round(ytm_pct, 2) if ytm else None,
        "benchmark_yield": round(bench_yield, 2),
        "spread_bps": round(spread_bps, 1),
        "signal": signal,
        "mac_duration": round(mac_dur, 3) if mac_dur else None,
        "mod_duration": round(mod_dur, 3) if mod_dur else None,
        "convexity": round(cnvx, 3) if cnvx else None,
        "price_impact_1pct": round(price_impact_1pct, 2),
        "volume": volume,
        "liquidity": liq,
        "coupon_rate": coupon,
        "face_value": face,
        "gross_coupon_sa": round(gross_sa, 2),
        "net_filer_sa": round(net_sa_filer, 2),
        "net_nonfiler_sa": round(net_sa_nonfiler, 2),
        "net_monthly_filer": round(net_monthly_filer, 2),
        "net_monthly_nonfiler": round(net_monthly_nonfiler, 2),
        "shifted_price": round(shifted_price, 2),
        "pnl_pct": round(pnl_pct, 2),
    }


# ── UI Rendering ────────────────────────────────────────────────────────────

def render_debt_terminal():
    """Render the PSX Debt Intelligence Terminal page."""
    st.markdown(TERMINAL_CSS, unsafe_allow_html=True)

    # ── Header ──────────────────────────────────────────────
    today_str = date.today().strftime("%d %b %Y")
    st.markdown(
        f"""<div class="debt-terminal-header">
            <h1><span class="pak">PAK</span><span class="fin">FIN</span><span class="data">DATA</span></h1>
            <div class="subtitle">PSX Debt Intelligence Terminal &bull; {today_str}</div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ── Persona switcher ────────────────────────────────────
    persona = st.radio(
        "View Mode",
        ["TREASURY", "INVESTOR", "QUANT"],
        horizontal=True,
        key="dt_persona",
        label_visibility="collapsed",
    )

    # ── Rate Shock Slider (in session state) ────────────────
    curve_shift = st.session_state.get("dt_curve_shift", 0)

    # ── Load live data via /v1 ──────────────────────────────
    # Load PKRV curve
    pkrv_curve, pkrv_date = _load_pkrv_curve()

    # Load IFRV (Islamic) curve — pass PKRV as JSON for cache-safe arg
    import json
    ifrv_curve, ifrv_date = _load_ifrv_curve(json.dumps(pkrv_curve))

    # Load KIBOR rates
    kibor_rates, kibor_date = _load_kibor_rates()

    # Load securities + quotes from fi_instruments/fi_quotes
    bond_list, quotes_dict, data_source = _load_securities_from_db()

    # Load SBP benchmarks
    db_snap = _load_benchmark_from_db()

    # ── Data source indicator ───────────────────────────────
    source_parts = []
    if pkrv_date:
        source_parts.append(f'<span class="live">PKRV {pkrv_date}</span>')
    else:
        source_parts.append('<span class="fallback">PKRV (sample)</span>')
    if kibor_date:
        source_parts.append(f'<span class="live">KIBOR {kibor_date}</span>')
    else:
        source_parts.append('<span class="fallback">KIBOR (sample)</span>')
    if data_source == "db":
        source_parts.append(f'<span class="live">{len(bond_list)} securities (DB)</span>')
    else:
        source_parts.append(f'<span class="fallback">{len(bond_list)} securities (sample)</span>')

    st.markdown(
        f'<div class="data-source-bar">Data: {" &bull; ".join(source_parts)}</div>',
        unsafe_allow_html=True,
    )

    # ── Filters (for DB data with many securities) ──────────
    if len(bond_list) > 15:
        with st.expander("Filters", expanded=False):
            filter_cols = st.columns(3)

            # Category filter
            categories = sorted(set(b.get("security_type", "Bond") for b in bond_list))
            with filter_cols[0]:
                sel_cats = st.multiselect(
                    "Security Type",
                    categories,
                    default=categories,
                    key="dt_cat_filter",
                )

            # Islamic filter
            with filter_cols[1]:
                islamic_filter = st.radio(
                    "Shariah",
                    ["All", "Islamic Only", "Conventional Only"],
                    horizontal=True,
                    key="dt_islamic_filter",
                )

            # Maturity range filter
            with filter_cols[2]:
                mat_range = st.slider(
                    "Maturity (years)",
                    min_value=0.0,
                    max_value=30.0,
                    value=(0.0, 30.0),
                    step=0.5,
                    key="dt_mat_range",
                )

            # Apply filters
            filtered = []
            for b in bond_list:
                if b.get("security_type", "Bond") not in sel_cats:
                    continue
                if islamic_filter == "Islamic Only" and not b.get("is_islamic"):
                    continue
                if islamic_filter == "Conventional Only" and b.get("is_islamic"):
                    continue
                # Check maturity range
                try:
                    days = _days_between(
                        date.today().strftime("%Y-%m-%d"),
                        b["maturity_date"],
                    )
                    yrs = days / 365.0
                    if yrs < mat_range[0] or yrs > mat_range[1]:
                        continue
                except Exception:
                    pass
                filtered.append(b)
            bond_list = filtered

        # Limit display to top N by volume (for performance)
        if len(bond_list) > 50:
            # Sort by whether we have quotes, then by maturity
            bond_list_with_quotes = [
                b for b in bond_list if b["symbol"] in quotes_dict
            ]
            bond_list_no_quotes = [
                b for b in bond_list if b["symbol"] not in quotes_dict
            ]
            bond_list = bond_list_with_quotes[:40] + bond_list_no_quotes[:10]
            st.caption(f"Showing top {len(bond_list)} securities (sorted by data availability)")

    # ── Extract KIBOR display values ────────────────────────
    kibor_6m = kibor_rates.get("6M", kibor_rates.get("6m", 0))
    kibor_1y = kibor_rates.get("1Y", kibor_rates.get("12M", kibor_rates.get("1y", 0)))
    pkrv_5y = _interpolate_curve(pkrv_curve, 5) + curve_shift / 100

    # ── Compute analytics for all securities ────────────────
    analytics_list = []
    for bond in bond_list:
        trade = quotes_dict.get(
            bond["symbol"],
            {"price": 100, "volume": 0, "weighted_avg": 0},
        )
        a = _compute_security_analytics(
            bond, trade, pkrv_curve, ifrv_curve, curve_shift
        )
        analytics_list.append(a)

    traded_count = sum(1 for a in analytics_list if a["volume"] > 0)

    # ── Metric Cards Row ────────────────────────────────────
    cols = st.columns(6)
    metrics = [
        ("KIBOR 6M", f"{kibor_6m:.2f}%", ""),
        ("KIBOR 1Y", f"{kibor_1y:.2f}%", ""),
        ("PKRV 5Y", f"{pkrv_5y:.2f}%", "green" if curve_shift == 0 else "amber"),
        ("SECURITIES", str(len(analytics_list)), ""),
        ("TRADED", str(traded_count), "green"),
        ("CURVE SHIFT", f"{curve_shift:+d} bps",
         "green" if curve_shift == 0 else ("red" if curve_shift > 0 else "amber")),
    ]
    for col, (label, value, color) in zip(cols, metrics):
        cls = f' {color}' if color else ''
        col.markdown(
            f'<div class="metric-card"><div class="label">{label}</div>'
            f'<div class="value{cls}">{value}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    # ── Main Grid: Blotter + Detail ─────────────────────────
    left_col, right_col = st.columns([3, 2])

    with left_col:
        st.markdown("##### Security Blotter")
        _render_blotter(analytics_list, persona)

    with right_col:
        st.markdown("##### Security Detail")
        _render_detail_panel(analytics_list, persona, con)

    st.markdown("")

    # ── Bottom Grid: Yield Curve + Simulator ────────────────
    bottom_left, bottom_right = st.columns(2)

    with bottom_left:
        st.markdown("##### Yield Curve Overlay")
        _render_yield_curve(analytics_list, pkrv_curve, ifrv_curve, curve_shift)

    with bottom_right:
        st.markdown("##### Hike/Cut Simulator")
        _render_simulator(analytics_list, bond_list, quotes_dict, pkrv_curve, ifrv_curve)

    # ── Footer ──────────────────────────────────────────────
    pkrv_note = f"PKRV {pkrv_date}" if pkrv_date else "PKRV (sample)"
    st.markdown(
        f'<div class="terminal-footer">'
        f'PakFinData Terminal v2.0 &bull; PSX Secondary Market &bull; {pkrv_note}'
        f'<span style="float:right">Data as of {today_str} &bull; Settlement T+1</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_blotter(analytics_list: list[dict], persona: str):
    """Render the security blotter table based on persona."""
    if not analytics_list:
        st.info("No securities match the current filters.")
        return

    rows = []
    for a in analytics_list:
        type_badge = "ISLAMIC" if a["is_islamic"] else "CONV"
        sig = a["signal"]
        liq = a["liquidity"]

        row = {
            "Symbol": a["symbol"],
            "Type": type_badge,
            "Price": a["clean_price"],
            "YTM %": a["ytm_pct"],
        }

        if persona == "TREASURY":
            row["Spread (bps)"] = a["spread_bps"]
            row["Mod Dur"] = a["mod_duration"]
            row["Signal"] = sig
            row["Liquidity"] = liq
        elif persona == "INVESTOR":
            row["Net Monthly"] = a["net_monthly_filer"]
            row["Coupon %"] = round(a["coupon_rate"] * 100, 2)
            row["Signal"] = sig
        else:  # QUANT
            row["Spread (bps)"] = a["spread_bps"]
            row["Convexity"] = a["convexity"]
            row["Signal"] = sig
            row["Liquidity"] = liq

        # Mark untraded
        if a["volume"] == 0:
            row["Symbol"] = f"{a['symbol']} [STALE]"

        rows.append(row)

    df = pd.DataFrame(rows)

    st.dataframe(
        df,
        width='stretch',
        height=min(350, 35 * len(rows) + 38),
        column_config={
            "Price": st.column_config.NumberColumn(format="%.2f"),
            "YTM %": st.column_config.NumberColumn(format="%.2f"),
            "Spread (bps)": st.column_config.NumberColumn(format="%.1f"),
            "Mod Dur": st.column_config.NumberColumn(format="%.3f"),
            "Convexity": st.column_config.NumberColumn(format="%.3f"),
            "Net Monthly": st.column_config.NumberColumn(format="Rs. %.2f"),
            "Coupon %": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def _render_detail_panel(analytics_list: list[dict], persona: str, con=None):
    """Render the detail panel for selected security."""
    if not analytics_list:
        st.info("No security selected.")
        return

    symbols = [a["symbol"] for a in analytics_list]
    selected_idx = st.selectbox(
        "Select Security",
        range(len(symbols)),
        format_func=lambda i: f"{analytics_list[i]['symbol']} — {analytics_list[i]['name']}",
        key="dt_selected_security",
        label_visibility="collapsed",
    )
    a = analytics_list[selected_idx]

    # Header with badges
    badge_type = "badge-islamic" if a["is_islamic"] else "badge-conventional"
    type_label = "ISLAMIC" if a["is_islamic"] else "CONVENTIONAL"
    slr_html = ' <span class="badge badge-conventional">SLR ELIGIBLE</span>' if a["is_slr_eligible"] and persona == "TREASURY" else ""
    stale_html = ' <span class="badge badge-stale">STALE</span>' if a["volume"] == 0 else ""
    st.markdown(
        f'**{a["name"]}** <span class="badge {badge_type}">{type_label}</span>{slr_html}{stale_html}',
        unsafe_allow_html=True,
    )

    # Row 1: Price analytics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clean Price", f"{a['clean_price']:.2f}")
    c2.metric("Accrued Int", f"{a['accrued_interest']:.4f}")
    c3.metric("Dirty Price", f"{a['dirty_price']:.4f}")
    c4.metric("Days Since Cpn", str(a["days_since_coupon"]))

    # Row 2: Yield & Risk
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("YTM", f"{a['ytm_pct']:.2f}%" if a["ytm_pct"] else "N/A")
    spread_label = "I-Spread" if a["is_islamic"] else "G-Spread"
    c2.metric(spread_label, f"{a['spread_bps']:.1f} bps")
    c3.metric("Mod Duration", f"{a['mod_duration']:.3f}" if a["mod_duration"] else "N/A")
    c4.metric("1% Shock", f"{a['price_impact_1pct']:.2f}%")

    # Signal badge
    sig_cls = {"CHEAP": "badge-cheap", "FAIR": "badge-fair", "RICH": "badge-rich"}[a["signal"]]
    st.markdown(f'Signal: <span class="badge {sig_cls}">{a["signal"]}</span>', unsafe_allow_html=True)

    # Persona-specific sub-panel
    if persona == "TREASURY":
        st.markdown("---")
        st.caption("TREASURY VIEW")
        c1, c2 = st.columns(2)
        c1.metric("SLR Eligible", "Yes" if a["is_slr_eligible"] else "No")
        c2.metric("Benchmark", f"{'IFRV' if a['is_islamic'] else 'PKRV'} {a['benchmark_yield']:.2f}%")

    elif persona == "INVESTOR":
        st.markdown("---")
        st.caption("INVESTOR VIEW — Tax Calculator")
        tax_status = st.radio("Tax Status", ["Filer", "Non-Filer"], horizontal=True, key="dt_tax")
        cz50 = st.checkbox("CZ-50 Zakat Exemption", key="dt_cz50")

        if tax_status == "Filer":
            net_sa = a["net_filer_sa"]
            net_m = a["net_monthly_filer"]
            wht_rate = "15%"
        else:
            net_sa = a["net_nonfiler_sa"]
            net_m = a["net_monthly_nonfiler"]
            wht_rate = "30%"

        c1, c2, c3 = st.columns(3)
        c1.metric("Gross Coupon (SA)", f"Rs. {a['gross_coupon_sa']:.2f}")
        c2.metric(f"Net Coupon ({wht_rate} WHT)", f"Rs. {net_sa:.2f}")
        c3.metric("Net Monthly", f"Rs. {net_m:.2f}")

        if not cz50:
            zakat = a["face_value"] * 0.025
            st.caption(f"Zakat liability: Rs. {zakat:.2f} per Rs. {a['face_value']} face (2.5%)")

    else:  # QUANT
        st.markdown("---")
        st.caption("QUANT VIEW")
        c1, c2, c3 = st.columns(3)
        c1.metric("Convexity", f"{a['convexity']:.3f}" if a["convexity"] else "N/A")
        c2.metric("Benchmark Pt", f"{a['benchmark_yield']:.2f}%")
        c3.metric("Shifted Price", f"{a['shifted_price']:.2f}")

    # Price/Volume Chart
    st.markdown("---")
    st.caption("Price & Volume History")
    _render_price_volume_chart(a["symbol"], con)


def _render_price_volume_chart(instrument_id: str, con=None):
    """Render dual-axis price + volume chart via /v1 or empty state.

    The ``con`` parameter is kept for legacy callers (now unused) —
    history is fetched through /v1/fi/quotes/{id}/history regardless.
    """
    history = _load_price_history(instrument_id, days=60)

    if not history:
        st.caption("No price history available for this security.")
        return

    df = pd.DataFrame(history)

    fig = go.Figure()

    # Volume bars
    fig.add_trace(go.Bar(
        x=df["date"], y=df["volume"],
        name="Volume",
        marker_color="rgba(34,197,94,0.2)",
        yaxis="y2",
    ))

    # Price line
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["price"],
        name="Price",
        mode="lines+markers",
        line=dict(color="#22c55e", width=2),
        marker=dict(size=4),
    ))

    fig.update_layout(
        height=250,
        margin=dict(l=0, r=0, t=10, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="JetBrains Mono, Fira Code, monospace", size=10, color="#BDC1C6"),
        xaxis=dict(gridcolor="#30363D", showgrid=True),
        yaxis=dict(title="Price", gridcolor="#30363D", side="left"),
        yaxis2=dict(title="Volume", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=-0.2),
        showlegend=True,
    )

    st.plotly_chart(fig, width='stretch')


def _render_yield_curve(
    analytics_list: list[dict],
    pkrv_curve: list[dict],
    ifrv_curve: list[dict],
    curve_shift: int,
):
    """Render PKRV + IFRV yield curves with traded security dots."""
    fig = go.Figure()

    # Shifted PKRV
    pkrv_years = [p["years"] for p in pkrv_curve]
    pkrv_yields = [p["yield"] + curve_shift / 100 for p in pkrv_curve]
    fig.add_trace(go.Scatter(
        x=pkrv_years, y=pkrv_yields,
        mode="lines+markers",
        name="PKRV (Govt)",
        line=dict(color="#22c55e", width=2),
        marker=dict(size=5),
    ))

    # Shifted IFRV
    ifrv_years = [p["years"] for p in ifrv_curve]
    ifrv_yields = [p["yield"] + curve_shift / 100 for p in ifrv_curve]
    fig.add_trace(go.Scatter(
        x=ifrv_years, y=ifrv_yields,
        mode="lines+markers",
        name="IFRV (Islamic)",
        line=dict(color="#FFB300", width=2, dash="dash"),
        marker=dict(size=5),
    ))

    # Traded security dots
    traded_x, traded_y, traded_text = [], [], []
    for a in analytics_list:
        if a["ytm_pct"] and a["volume"] > 0:
            traded_x.append(a["years_to_mat"])
            traded_y.append(a["ytm_pct"])
            traded_text.append(f"{a['symbol']}<br>YTM: {a['ytm_pct']:.2f}%")

    if traded_x:
        fig.add_trace(go.Scatter(
            x=traded_x, y=traded_y,
            mode="markers",
            name="Traded Securities",
            marker=dict(color="#2F81F7", size=10, symbol="circle",
                       line=dict(width=1, color="#e2e8f0")),
            text=traded_text,
            hoverinfo="text",
        ))

    # Determine tick values from curve data
    all_years = sorted(set(pkrv_years + ifrv_years))
    tick_map = {0.25: "3M", 0.5: "6M", 0.75: "9M", 1: "1Y", 2: "2Y", 3: "3Y",
                5: "5Y", 7: "7Y", 10: "10Y", 15: "15Y", 20: "20Y", 30: "30Y"}
    tick_vals = [y for y in all_years if y in tick_map]
    tick_text = [tick_map.get(y, f"{y}Y") for y in tick_vals]

    fig.update_layout(
        height=350,
        margin=dict(l=0, r=0, t=10, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="JetBrains Mono, Fira Code, monospace", size=10, color="#BDC1C6"),
        xaxis=dict(title="Tenor (Years)", gridcolor="#30363D",
                   tickvals=tick_vals, ticktext=tick_text),
        yaxis=dict(title="Yield (%)", gridcolor="#30363D"),
        legend=dict(orientation="h", y=-0.2),
    )

    st.plotly_chart(fig, width='stretch')

    # Traded dots list
    with st.expander("Traded Yield Points"):
        for a in analytics_list:
            if a["ytm_pct"] and a["volume"] > 0:
                st.markdown(
                    f"`{a['symbol']}` — YTM: **{a['ytm_pct']:.2f}%** | "
                    f"Spread: {a['spread_bps']:.1f}bps | Vol: {a['volume']:,.0f}"
                )


def _render_simulator(
    analytics_list: list[dict],
    bond_list: list[dict],
    quotes_dict: dict[str, dict],
    pkrv_curve: list[dict],
    ifrv_curve: list[dict],
):
    """Render the hike/cut rate shock simulator."""
    shift = st.slider(
        "Curve Shift (bps)",
        min_value=-300, max_value=300, value=0, step=25,
        key="dt_curve_shift",
        help="Parallel shift to benchmark curves. Negative = easing, Positive = tightening.",
    )

    shift_color = "#FF5252" if shift > 0 else ("#22c55e" if shift < 0 else "#e2e8f0")
    st.markdown(
        f'<div style="text-align:center; font-size:36px; font-weight:800; '
        f'font-family: monospace; color:{shift_color}; margin:8px 0">'
        f'{shift:+d} bps</div>',
        unsafe_allow_html=True,
    )

    labels = st.columns(3)
    labels[0].caption("-300 (Easing)")
    labels[1].markdown("<div style='text-align:center'><small>0</small></div>", unsafe_allow_html=True)
    labels[2].markdown("<div style='text-align:right'><small>+300 (Tightening)</small></div>", unsafe_allow_html=True)

    # Recompute with shift
    if shift != 0:
        shifted_list = []
        for bond in bond_list:
            trade = quotes_dict.get(
                bond["symbol"],
                {"price": 100, "volume": 0, "weighted_avg": 0},
            )
            shifted_list.append(
                _compute_security_analytics(bond, trade, pkrv_curve, ifrv_curve, shift)
            )

        # MTM Impact Table
        st.markdown("**MTM Impact**")
        impact_rows = []
        for orig, shifted in zip(analytics_list, shifted_list):
            impact_rows.append({
                "Security": orig["symbol"],
                "Current": orig["clean_price"],
                "Shifted": shifted["shifted_price"],
                "P&L %": shifted["pnl_pct"],
            })

        impact_df = pd.DataFrame(impact_rows)
        st.dataframe(
            impact_df,
            width='stretch',
            column_config={
                "Current": st.column_config.NumberColumn(format="%.2f"),
                "Shifted": st.column_config.NumberColumn(format="%.2f"),
                "P&L %": st.column_config.NumberColumn(format="%+.2f%%"),
            },
            height=min(300, 35 * len(impact_rows) + 38),
        )

        # Alert box
        dur_list = [a for a in analytics_list if a["mod_duration"]]
        if dur_list:
            max_dur = max(dur_list, key=lambda x: x["mod_duration"] or 0)
            max_dur_idx = analytics_list.index(max_dur)
            if max_dur_idx < len(shifted_list):
                loss_gain = abs(shifted_list[max_dur_idx]["pnl_pct"])
                if shift > 0:
                    st.markdown(
                        f'<div class="sim-alert sim-alert-tight">'
                        f'{shift}bps tightening: Long duration bonds lose up to {loss_gain:.1f}% '
                        f'({max_dur["symbol"]} — {max_dur["mod_duration"]:.1f}yr duration)'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="sim-alert sim-alert-ease">'
                        f'{abs(shift)}bps easing: Portfolio gains on duration exposure — '
                        f'up to +{loss_gain:.1f}% ({max_dur["symbol"]})'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
    else:
        st.info("Move the slider to simulate rate changes and see MTM impact on all securities.")
