"""Treasury Market Terminal — yield curves, auctions, KIBOR, spreads, policy rate.

Tabs:
  Overview — KPI cards (policy rate, KIBOR, T-Bill, KONIA, SOFR), rate history overlay
  Yield Curves — PKRV with comparison, PKISRV Islamic, curve evolution 3D
  Auctions — T-Bill/PIB results with charts, bid-cover, yield evolution
  KIBOR — Term structure, history, bid-offer spread
  Spreads — T-Bill 6-12M, KIBOR vs Policy, curve steepness
  Global Rates — SOFR, EFFR, SONIA, EUSTR, TONA + SOFR-KIBOR spread
  Bonds — PKFRV floating rate bonds, FMA prices, maturity schedule
  Sync — All sync/backfill controls
"""

from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.sources.sbp_gsp import GSPScraper
from pakfindata.sources.sbp_rates import SBPRatesScraper
from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import get_connection, render_ai_commentary, render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "policy": "#E74C3C", "kibor": "#4ECDC4",
    "tbill": "#3498DB", "pib": "#9B59B6", "konia": "#45B7D1",
    "sofr": "#2196F3", "effr": "#FF9800", "sonia": "#AB47BC",
    "eustr": "#26A69A", "tona": "#EF5350",
    "pkrv": "#FF6B35", "pkisrv": "#2ECC71", "pkfrv": "#E67E22",
    "bg": "#0e1117", "card_bg": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "text_dim": "#888888",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    xaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    yaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=10, r=10, t=40, b=10),
)

_TENOR_DAYS = {
    "1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122,
    "6M": 182, "9M": 274, "1Y": 365, "2Y": 730, "3Y": 1095,
    "4Y": 1461, "5Y": 1826, "6Y": 2191, "7Y": 2557, "8Y": 2922,
    "9Y": 3287, "10Y": 3652, "12M": 365, "15Y": 5479, "20Y": 7305,
    "25Y": 9131, "30Y": 10957,
}
_MONTHS_TO_DAYS = {
    1: 30, 2: 60, 3: 91, 4: 122, 6: 182, 9: 274, 12: 365, 24: 730,
    36: 1095, 48: 1461, 60: 1826, 72: 2191, 84: 2557, 96: 2922,
    108: 3287, 120: 3652, 180: 5479, 240: 7305, 300: 9131, 360: 10957,
}
_TENOR_LABELS = {
    1: "1M", 2: "2M", 3: "3M", 4: "4M", 6: "6M", 9: "9M",
    12: "1Y", 24: "2Y", 36: "3Y", 48: "4Y", 60: "5Y",
    72: "6Y", 84: "7Y", 96: "8Y", 108: "9Y", 120: "10Y",
    180: "15Y", 240: "20Y", 300: "25Y", 360: "30Y",
}


def _styled_fig(height=400, **kw):
    return go.Figure(layout={**_CHART_LAYOUT, "height": height, **kw})


def _card(label, value, delta=None, color=None, suffix=""):
    card_bg = _COLORS["card_bg"]
    border = color or _COLORS["accent"]
    dim = _COLORS["text_dim"]
    delta_html = ""
    if delta is not None:
        dc = _COLORS["up"] if delta > 0 else _COLORS["down"] if delta < 0 else _COLORS["neutral"]
        sign = "+" if delta > 0 else ""
        delta_html = f"<span style='color:{dc};font-size:0.85em;'>{sign}{delta:.2f}{suffix}</span>"
    st.markdown(
        f"<div style='background:{card_bg};border-radius:8px;padding:12px 16px;"
        f"border-left:3px solid {border};'>"
        f"<div style='color:{dim};font-size:0.75em;'>{label}</div>"
        f"<div style='font-size:1.3em;font-weight:600;'>{value}</div>"
        f"{delta_html}</div>",
        unsafe_allow_html=True,
    )


def _days_ago(date_str):
    try:
        return (datetime.now() - datetime.strptime(str(date_str)[:10], "%Y-%m-%d")).days
    except (ValueError, TypeError):
        return -1


# ── Cached data loaders ──────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _load_overview_kpis() -> dict:
    """Load all overview KPI values (cached) via /v1."""
    kpis: dict = {}

    # Use /v1/rates/strip for policy + KIBOR 3M + tbill 3M + PKRV 10Y
    strip = api_client.get_rates_strip() or {}

    kpis["policy"] = (
        {"policy_rate": strip["sbp_policy_rate"], "rate_date": strip.get("sbp_policy_date")}
        if strip.get("sbp_policy_rate") is not None else None
    )
    kpis["kibor"] = (
        {
            "date": strip.get("kibor_3m_date"),
            "bid": strip.get("kibor_3m_bid"),
            "offer": strip.get("kibor_3m_offer"),
        }
        if strip.get("kibor_3m_offer") is not None else None
    )
    kpis["tbill"] = (
        {
            "cutoff_yield": strip["tbill_3m_cutoff"],
            "auction_date": strip.get("tbill_3m_date"),
        }
        if strip.get("tbill_3m_cutoff") is not None else None
    )
    kpis["pkrv10"] = (
        {"yield_pct": strip["pkrv_10y_yield"], "date": strip.get("pkrv_10y_date")}
        if strip.get("pkrv_10y_yield") is not None else None
    )

    # KONIA with Group C defensive guard
    konia_rows = api_client.get_konia(limit=1) or []
    if konia_rows and 0 < (konia_rows[0].get("rate_pct") or 0) < 50:
        kpis["konia"] = konia_rows[0]
    else:
        kpis["konia"] = None

    # SOFR latest via /v1/rates/global
    global_rows = api_client.get_global_reference_rates(rate_names="SOFR") or []
    sofr_on = next(
        (r for r in global_rows if r.get("rate_name") == "SOFR" and r.get("tenor") == "ON"),
        None,
    )
    kpis["sofr"] = (
        {"rate": sofr_on["rate"], "date": sofr_on.get("date")}
        if sofr_on and sofr_on.get("rate") is not None else None
    )

    return kpis


@st.cache_data(ttl=3600, show_spinner=False)
def _load_rate_history() -> tuple[dict, pd.DataFrame]:
    """Load rate history overlay data (cached). Last 3 years only."""
    cutoff = (pd.Timestamp.now() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    frames: dict = {}

    # Policy rate history
    pr_rows = api_client.get_policy_rate_history(limit=500) or []
    pr_df = pd.DataFrame([
        {"date": r.get("rate_date"), "rate": r.get("policy_rate")}
        for r in pr_rows if r.get("rate_date") and r["rate_date"] >= cutoff
    ])
    frames["sbp_policy_rates"] = pr_df.sort_values("date") if not pr_df.empty else pr_df

    # KIBOR 3M history
    kibor_rows = api_client.get_kibor_history(tenors="3M", days=3000) or []
    kibor_df = pd.DataFrame([
        {"date": r.get("date"), "rate": r.get("offer")}
        for r in kibor_rows if r.get("date") and r["date"] >= cutoff
    ])
    frames["kibor_daily"] = kibor_df.sort_values("date") if not kibor_df.empty else kibor_df

    # KONIA history (via /v1/rates/konia with a wide limit)
    konia_rows = api_client.get_konia(limit=3000) or []
    konia_df = pd.DataFrame([
        {"date": r.get("date"), "rate": r.get("rate_pct")}
        for r in konia_rows if r.get("date") and r["date"] >= cutoff
        and r.get("rate_pct") is not None and 0 < r["rate_pct"] < 50
    ])
    frames["konia_daily"] = konia_df.sort_values("date") if not konia_df.empty else konia_df

    # T-Bill 3M history
    tb_rows = api_client.get_tbill_auctions(tenor="3M", from_=cutoff, limit=2000) or []
    tb_df = pd.DataFrame([
        {"date": r.get("auction_date"), "rate": r.get("cutoff_yield")}
        for r in tb_rows if r.get("auction_date")
    ])
    if not tb_df.empty:
        tb_df = tb_df.sort_values("date")
    frames["tbill_auctions"] = tb_df
    return frames, tb_df


@st.cache_data(ttl=3600, show_spinner=False)
def _load_policy_rate_timeline() -> pd.DataFrame:
    """Load policy rate timeline data (cached). Last 3 years."""
    cutoff = (pd.Timestamp.now() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    pr_rows = api_client.get_policy_rate_history(limit=500) or []
    df = pd.DataFrame([
        {"rate_date": r.get("rate_date"), "policy_rate": r.get("policy_rate")}
        for r in pr_rows if r.get("rate_date") and r["rate_date"] >= cutoff
    ])
    return df.sort_values("rate_date") if not df.empty else df


def _load_pkrv_dates() -> list[str]:
    """Load distinct PKRV dates from manifest."""
    from pakfindata.db.date_manifest import get_dates
    return get_dates("pkrv_daily")


@st.cache_data(ttl=3600, show_spinner=False)
def _load_pkrv_curve(date: str) -> pd.DataFrame:
    """Load PKRV yield curve for a date (cached) via /v1."""
    rows = api_client.get_pkrv(date=date) or []
    if not rows:
        return pd.DataFrame(columns=["tenor_months", "yield_pct", "change_bps"])
    return pd.DataFrame(rows)[["tenor_months", "yield_pct", "change_bps"]]


def _load_pkisrv_data() -> tuple[int, list[str]]:
    """Load PKISRV row coverage + distinct dates."""
    from pakfindata.db.date_manifest import get_dates
    isrv_dates = get_dates("pkisrv_daily")
    # Probe count via latest-day fetch — non-empty implies the table
    # has data; len of probe is sufficient for the "no data yet" gate.
    isrv_count = len(api_client.get_pkisrv() or [])
    return isrv_count, isrv_dates


@st.cache_data(ttl=3600, show_spinner=False)
def _load_pkisrv_curve(date: str) -> pd.DataFrame:
    """Load PKISRV curve for a date (cached) via /v1."""
    rows = api_client.get_pkisrv(date=date) or []
    if not rows:
        return pd.DataFrame(columns=["tenor", "yield_pct"])
    return pd.DataFrame(rows)[["tenor", "yield_pct"]]


@st.cache_data(ttl=3600, show_spinner=False)
def _load_auction_data(auction_type: str, tenor: str) -> tuple[int, list[str], pd.DataFrame]:
    """Load auction data for T-Bills or PIBs (cached) via /v1."""
    fetch = (
        api_client.get_tbill_auctions if auction_type == "tbill"
        else api_client.get_pib_auctions
    )
    # Pull a generous slice; we'll filter + cap below to preserve
    # legacy "ORDER BY auction_date DESC LIMIT 40" semantics.
    all_rows = fetch(limit=2000) or []
    if not all_rows:
        return 0, [], pd.DataFrame()

    df = pd.DataFrame(all_rows)
    tenors = sorted({r for r in df["tenor"].dropna().tolist() if r})
    if tenor != "All":
        df = df[df["tenor"] == tenor]
    df = df.sort_values("auction_date", ascending=False).head(40)

    if auction_type == "tbill":
        keep = ["auction_date", "tenor", "cutoff_yield", "weighted_avg_yield",
                "target_amount_billions", "amount_accepted_billions"]
    else:
        keep = ["auction_date", "tenor", "pib_type", "cutoff_yield",
                "coupon_rate", "amount_accepted_billions"]
    keep = [c for c in keep if c in df.columns]
    return len(all_rows), tenors, df[keep].reset_index(drop=True)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_auction_yield_map() -> pd.DataFrame:
    """Load combined auction scatter data (cached) — T-Bills + PIBs."""
    cols = ["type", "tenor", "auction_date", "cutoff_yield",
            "target_amount_billions", "amount_accepted_billions"]
    tb = api_client.get_tbill_auctions(limit=200) or []
    pi = api_client.get_pib_auctions(limit=200) or []
    rows = []
    for r in tb:
        rows.append({"type": "T-Bill", **{k: r.get(k) for k in cols[1:]}})
    for r in pi:
        rows.append({"type": "PIB", **{k: r.get(k) for k in cols[1:]}})
    if not rows:
        return pd.DataFrame(columns=cols)
    return (
        pd.DataFrame(rows)
        .sort_values("auction_date", ascending=False)
        .head(60)
        .reset_index(drop=True)
    )


@st.cache_data(ttl=3600, show_spinner=False)
def _load_gis_auctions() -> pd.DataFrame:
    """Load GIS sukuk auction data (cached) via /v1."""
    rows = api_client.get_gis_auctions(limit=20) or []
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_kibor_data() -> tuple[int, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all KIBOR data (cached) via /v1.

    Returns ``(count, kdf, spread_df, latest_df)`` matching the legacy
    shape — count is the row count of the kibor history slice (proxy
    for table size), ``kdf`` is per-(date, tenor) history filtered to
    1M/3M/6M/12M, ``spread_df`` is 3M offer-bid, ``latest_df`` is
    latest row per tenor.
    """
    cutoff = (pd.Timestamp.now() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    history = api_client.get_kibor_history(
        tenors="1M,3M,6M,12M", days=10000,
    ) or []
    if not history:
        return 0, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    kdf = pd.DataFrame([r for r in history if r.get("date", "") >= cutoff])
    if kdf.empty:
        return len(history), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    kdf = kdf[["date", "tenor", "bid", "offer"]].sort_values("date")

    spread_df = (
        kdf[kdf["tenor"] == "3M"][["date", "bid", "offer"]]
        .dropna()
        .copy()
    )
    if not spread_df.empty:
        spread_df["tenor"] = "3M"
        spread_df["spread"] = spread_df["offer"] - spread_df["bid"]
        spread_df = spread_df[["date", "tenor", "spread"]]

    latest_rows = api_client.get_kibor_latest_per_tenor() or []
    latest = (
        pd.DataFrame(latest_rows)[["date", "tenor", "bid", "offer"]]
        if latest_rows else pd.DataFrame()
    )
    return len(history), kdf, spread_df, latest


@st.cache_data(ttl=3600, show_spinner=False)
def _load_spread_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load T-Bill and KIBOR-Policy spread data (cached) via /v1."""
    cutoff = (pd.Timestamp.now() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")

    # T-Bill 6M vs 12M spread (joined client-side on auction_date)
    tb6 = api_client.get_tbill_auctions(tenor="6M", from_=cutoff, limit=2000) or []
    tb12 = api_client.get_tbill_auctions(tenor="12M", from_=cutoff, limit=2000) or []
    if tb6 and tb12:
        df6 = pd.DataFrame(tb6)[["auction_date", "cutoff_yield"]].rename(
            columns={"auction_date": "date", "cutoff_yield": "y6"}
        )
        df12 = pd.DataFrame(tb12)[["auction_date", "cutoff_yield"]].rename(
            columns={"auction_date": "date", "cutoff_yield": "y12"}
        )
        tb_spread = df6.merge(df12, on="date", how="inner")
        tb_spread["spread"] = (tb_spread["y12"] - tb_spread["y6"]).round(4)
        tb_spread = tb_spread.sort_values("date")
    else:
        tb_spread = pd.DataFrame()

    # KIBOR 3M vs policy rate (as-of join client-side via merge_asof)
    kibor_hist = api_client.get_kibor_history(tenors="3M", days=10000) or []
    policy_hist = api_client.get_policy_rate_history(limit=500) or []
    if kibor_hist and policy_hist:
        kdf = pd.DataFrame([
            {"date": r.get("date"), "kibor": r.get("offer")}
            for r in kibor_hist if r.get("date", "") >= cutoff and r.get("offer") is not None
        ])
        pdf = pd.DataFrame([
            {"date": r.get("rate_date"), "policy": r.get("policy_rate")}
            for r in policy_hist if r.get("rate_date") and r.get("policy_rate") is not None
        ])
        if not kdf.empty and not pdf.empty:
            kdf["date"] = pd.to_datetime(kdf["date"])
            pdf["date"] = pd.to_datetime(pdf["date"])
            kdf = kdf.sort_values("date")
            pdf = pdf.sort_values("date")
            kibor_policy = pd.merge_asof(kdf, pdf, on="date", direction="backward")
            kibor_policy["date"] = kibor_policy["date"].dt.strftime("%Y-%m-%d")
        else:
            kibor_policy = pd.DataFrame()
    else:
        kibor_policy = pd.DataFrame()
    return tb_spread, kibor_policy


@st.cache_data(ttl=3600, show_spinner=False)
def _load_curve_steepness() -> list[dict]:
    """Load yield curve steepness data (cached) via /v1 history endpoints.

    Falls back to tenor-history on sovereign_curve for PKRV 2Y and
    10Y, then computes 10Y-2Y per overlapping date.
    """
    y2_hist = api_client.get_sovereign_tenor_history(
        tenor="2Y", sources="PKRV", limit=5000,
    ) or []
    y10_hist = api_client.get_sovereign_tenor_history(
        tenor="10Y", sources="PKRV", limit=5000,
    ) or []
    if not y2_hist or not y10_hist:
        return []
    y2_map = {r["date"]: r["yield_pct"] for r in y2_hist if r.get("yield_pct") is not None}
    y10_map = {r["date"]: r["yield_pct"] for r in y10_hist if r.get("yield_pct") is not None}
    overlap = sorted(set(y2_map) & set(y10_map), reverse=True)[:90]
    return [
        {"date": d, "steep": y10_map[d] - y2_map[d]}
        for d in overlap
    ]


def _load_pkfrv_data() -> tuple[int, list[str]]:
    """Load PKFRV bond row count + distinct dates."""
    from pakfindata.db.date_manifest import get_dates
    # Probe coverage via latest-day fetch
    probe = api_client.get_pkfrv() or []
    if not probe:
        return 0, []
    dates = get_dates("pkfrv_daily")
    return len(probe), dates


@st.cache_data(ttl=3600, show_spinner=False)
def _load_pkfrv_bonds(date: str) -> pd.DataFrame:
    """Load PKFRV bonds for a date (cached) via /v1."""
    rows = api_client.get_pkfrv(date=date, limit=2000) or []
    if not rows:
        return pd.DataFrame(columns=[
            "bond_code", "issue_date", "maturity_date",
            "coupon_frequency", "fma_price",
        ])
    return pd.DataFrame(rows)[[
        "bond_code", "issue_date", "maturity_date",
        "coupon_frequency", "fma_price",
    ]]


@st.cache_data(ttl=3600, show_spinner=False)
def _load_bond_history(bond_code: str) -> pd.DataFrame:
    """FMA price history for a single bond via /v1."""
    rows = api_client.get_pkfrv_bond_history(bond_code, limit=2000) or []
    if not rows:
        return pd.DataFrame(columns=["date", "fma_price"])
    return (
        pd.DataFrame(rows)[["date", "fma_price"]]
        .sort_values("date")
        .reset_index(drop=True)
    )


def _remaining_days(maturity_str):
    try:
        dt = datetime.strptime(str(maturity_str)[:10], "%Y-%m-%d")
        delta = (dt - datetime.now()).days
        return delta if delta > 0 else 0
    except (ValueError, TypeError):
        return None


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def render_treasury_dashboard():
    st.markdown("## Treasury Terminal")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    tabs = st.tabs([
        "Overview", "Yield Curves", "Auctions", "KIBOR", "Spreads",
        "Global Rates", "Bonds", "Sync",
    ])
    renderers = [
        _render_overview, _render_yield_curves, _render_auctions,
        _render_kibor, _render_spreads, _render_global_rates,
        _render_bonds, _render_sync,
    ]

    for tab, renderer in zip(tabs, renderers):
        with tab:
            try:
                renderer(con)
            except Exception as e:
                st.error(f"Error: {e}")

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW
# ═════════════════════════════════════════════════════════════════════════════

def _render_overview(con):
    # ── KPI row ──
    mc = st.columns(6)
    kpis = _load_overview_kpis()

    policy = kpis["policy"]
    with mc[0]:
        if policy:
            age = _days_ago(policy["rate_date"])
            _card("SBP Policy Rate", f"{policy['policy_rate']:.1f}%", color=_COLORS["policy"])
            st.caption(f"{policy['rate_date']} ({age}d ago)")
        else:
            _card("SBP Policy Rate", "N/A", color=_COLORS["policy"])

    kibor = kpis["kibor"]
    with mc[1]:
        if kibor and kibor.get("offer") is not None:
            _card("KIBOR 3M", f"{kibor['offer']:.2f}%", color=_COLORS["kibor"])
            bid_str = f"{kibor['bid']:.2f}%" if kibor.get("bid") is not None else "N/A"
            st.caption(f"Bid: {bid_str} | {kibor['date']}")
        else:
            _card("KIBOR 3M", "N/A", color=_COLORS["kibor"])

    tbill = kpis["tbill"]
    with mc[2]:
        if tbill:
            _card("T-Bill 3M", f"{tbill['cutoff_yield']:.2f}%", color=_COLORS["tbill"])
            st.caption(f"Auction: {tbill['auction_date']}")
        else:
            _card("T-Bill 3M", "N/A", color=_COLORS["tbill"])

    konia = kpis["konia"]
    with mc[3]:
        if konia:
            _card("KONIA (O/N)", f"{konia['rate_pct']:.2f}%", color=_COLORS["konia"])
            st.caption(f"{konia['date']}")
        else:
            _card("KONIA", "N/A", color=_COLORS["konia"])

    pkrv10 = kpis["pkrv10"]
    with mc[4]:
        if pkrv10:
            _card("PKRV 10Y", f"{pkrv10['yield_pct']:.2f}%", color=_COLORS["pkrv"])
            st.caption(f"{pkrv10['date']}")
        else:
            _card("PKRV 10Y", "N/A", color=_COLORS["pkrv"])

    sofr = kpis["sofr"]
    with mc[5]:
        if sofr:
            _card("SOFR (O/N)", f"{sofr['rate']:.4f}%", color=_COLORS["sofr"])
            st.caption(f"{sofr['date']}")
        else:
            _card("SOFR", "N/A", color=_COLORS["sofr"])

    # ── Rate history overlay ──
    st.markdown("### Rate History")
    fig = _styled_fig(height=420)
    frames, tb = _load_rate_history()
    rate_series = [
        ("sbp_policy_rates", "Policy Rate", _COLORS["policy"], "lines+markers"),
        ("kibor_daily", "KIBOR 3M", _COLORS["kibor"], "lines"),
        ("konia_daily", "KONIA", _COLORS["konia"], "lines"),
    ]
    for table, name, color, mode in rate_series:
        df = frames.get(table, pd.DataFrame())
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["rate"], mode=mode, name=name,
                line=dict(width=2, color=color,
                          shape="hv" if table == "sbp_policy_rates" else "linear"),
            ))

    if not tb.empty:
        fig.add_trace(go.Scatter(
            x=tb["date"], y=tb["rate"], mode="markers",
            name="T-Bill 3M (Cutoff)", marker=dict(size=7, color=_COLORS["tbill"], symbol="diamond"),
        ))

    if fig.data:
        fig.update_layout(
            yaxis_title="Rate (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No rate history data available")

    # ── Policy rate timeline ──
    st.markdown("### SBP Policy Rate Timeline")

    pdf = _load_policy_rate_timeline()
    if not pdf.empty:
        fig = _styled_fig(height=300)
        fig.add_trace(go.Scatter(
            x=pdf["rate_date"], y=pdf["policy_rate"],
            mode="lines+markers", name="Policy Rate",
            line=dict(width=3, color=_COLORS["policy"], shape="hv"),
            marker=dict(size=6),
        ))
        for i in range(1, len(pdf)):
            prev, curr = pdf.iloc[i-1]["policy_rate"], pdf.iloc[i]["policy_rate"]
            if prev != curr:
                chg = (curr - prev) * 100
                color = _COLORS["down"] if chg > 0 else _COLORS["up"]
                fig.add_annotation(
                    x=pdf.iloc[i]["rate_date"], y=curr,
                    text=f"{chg:+.0f}bp", showarrow=True, arrowhead=2,
                    font=dict(size=9, color=color), arrowcolor=color,
                )
        fig.update_layout(yaxis_title="Rate (%)")
        st.plotly_chart(fig, width='stretch')

    # AI Commentary
    render_ai_commentary(con, "TREASURY")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: YIELD CURVES
# ═════════════════════════════════════════════════════════════════════════════

def _render_yield_curves(con):
    # ── PKRV ──
    st.markdown("### PKRV Yield Curve")
    dates = _load_pkrv_dates()
    if not dates:
        st.info("No PKRV yield curve data. Sync Yield Curves (MUFAP) to fetch.")
        return

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        sel_date = st.selectbox("Curve date", dates, index=0, key="pkrv_d1")
    with c2:
        cmp_date = st.selectbox("Compare with", ["None"] + dates, index=0, key="pkrv_d2")
    with c3:
        from pakfindata.ui.components.helpers import render_date_refresh_button
        render_date_refresh_button(["pkrv_daily", "pkisrv_daily", "pkfrv_daily"], key="tsy_yc_refresh")

    df = _load_pkrv_curve(sel_date)
    if df.empty:
        st.info("No yield curve points for selected date")
        return

    fig = _styled_fig(height=420)
    fig.add_trace(go.Scatter(
        x=df["tenor_months"], y=df["yield_pct"],
        mode="lines+markers", name=sel_date,
        line=dict(width=3, color=_COLORS["pkrv"]),
        marker=dict(size=7),
        hovertemplate="%{text}<br>Yield: %{y:.4f}%<extra></extra>",
        text=[f"{_TENOR_LABELS.get(t, f'{t}M')}" for t in df["tenor_months"]],
    ))

    if cmp_date != "None":
        cdf = _load_pkrv_curve(cmp_date)
        if not cdf.empty:
            fig.add_trace(go.Scatter(
                x=cdf["tenor_months"], y=cdf["yield_pct"],
                mode="lines+markers", name=cmp_date,
                line=dict(width=2, dash="dash", color=_COLORS["kibor"]),
            ))

    fig.update_layout(
        yaxis_title="Yield (%)",
        xaxis=dict(
            title="Tenor",
            tickmode="array",
            tickvals=df["tenor_months"].tolist(),
            ticktext=[_TENOR_LABELS.get(t, f"{t}M") for t in df["tenor_months"]],
        ),
        legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
    )
    st.plotly_chart(fig, width='stretch')

    # Change bps bar
    if "change_bps" in df.columns and df["change_bps"].notna().any():
        st.markdown("#### Daily Change (bps)")
        chg_df = df.dropna(subset=["change_bps"])
        if not chg_df.empty:
            fig2 = _styled_fig(height=220)
            bar_colors = [_COLORS["up"] if c >= 0 else _COLORS["down"] for c in chg_df["change_bps"]]
            fig2.add_trace(go.Bar(
                x=[_TENOR_LABELS.get(t, f"{t}M") for t in chg_df["tenor_months"]],
                y=chg_df["change_bps"],
                marker_color=bar_colors,
                text=[f"{c:+.1f}" for c in chg_df["change_bps"]],
                textposition="outside", textfont=dict(size=9),
            ))
            fig2.update_layout(yaxis_title="Change (bps)", showlegend=False)
            st.plotly_chart(fig2, width='stretch')

    with st.expander("Curve Data"):
        display = df.copy()
        display["Tenor"] = display["tenor_months"].map(lambda t: _TENOR_LABELS.get(t, f"{t}M"))
        st.dataframe(display[["Tenor", "tenor_months", "yield_pct", "change_bps"]].rename(columns={
            "tenor_months": "Months", "yield_pct": "Yield (%)", "change_bps": "Change (bps)",
        }), width='stretch', hide_index=True)

    # ── PKISRV (Islamic) ──
    st.markdown("### PKISRV (Islamic Yield Curve)")
    isrv_count, isrv_dates = _load_pkisrv_data()
    if isrv_count == 0:
        st.info("No PKISRV data. Sync Yield Curves (MUFAP) to fetch.")
        return

    isrv_date = st.selectbox("Islamic curve date", isrv_dates, index=0, key="pkisrv_date")

    idf = _load_pkisrv_curve(isrv_date)
    if not idf.empty:
        idf["days"] = idf["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip(), 9999))
        idf = idf.sort_values("days")

        fig = _styled_fig(height=350)
        fig.add_trace(go.Scatter(
            x=idf["tenor"], y=idf["yield_pct"],
            mode="lines+markers", name=f"PKISRV ({isrv_date})",
            line=dict(width=3, color=_COLORS["pkisrv"]),
            marker=dict(size=7),
        ))

        # Overlay PKRV for comparison
        pkrv_on_date = _load_pkrv_curve(isrv_date)
        if not pkrv_on_date.empty:
            pkrv_on_date["tenor"] = pkrv_on_date["tenor_months"].map(
                lambda t: _TENOR_LABELS.get(t, f"{t}M")
            )
            merged = idf.merge(pkrv_on_date[["tenor", "yield_pct"]], on="tenor", suffixes=("_isrv", "_pkrv"))
            if not merged.empty:
                fig.add_trace(go.Scatter(
                    x=merged["tenor"], y=merged["yield_pct_pkrv"],
                    mode="lines+markers", name=f"PKRV ({isrv_date})",
                    line=dict(width=2, dash="dash", color=_COLORS["pkrv"]),
                ))

        fig.update_layout(
            yaxis_title="Yield (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, width='stretch')
        st.caption(f"PKISRV: {isrv_count} records | {len(isrv_dates)} dates")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: AUCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def _render_auctions(con):
    ac1, ac2 = st.columns(2)

    # ── T-Bills ──
    with ac1:
        st.markdown("### T-Bill Auctions")
        # Initial load to get tenors list
        tb_count_init, tb_tenors_init, _ = _load_auction_data("tbill", "All")
        if tb_count_init == 0:
            st.info("No T-Bill data. Sync below.")
        else:
            sel_tenor = st.selectbox("Tenor", ["All"] + tb_tenors_init, key="tb_tenor")
            tb_count, tenors, df = _load_auction_data("tbill", sel_tenor)

            # Yield evolution chart
            if not df.empty:
                chart_df = df.sort_values("auction_date")
                fig = _styled_fig(height=320)

                if sel_tenor == "All":
                    # Plot each tenor as a separate line to avoid zigzag
                    tb_colors = ["#3498DB", "#2ECC71", "#E67E22", "#9B59B6", "#E74C3C", "#1ABC9C"]
                    for idx, tenor in enumerate(sorted(chart_df["tenor"].unique())):
                        sub = chart_df[chart_df["tenor"] == tenor]
                        fig.add_trace(go.Scatter(
                            x=sub["auction_date"], y=sub["cutoff_yield"],
                            mode="lines+markers", name=tenor,
                            line=dict(width=2, color=tb_colors[idx % len(tb_colors)]),
                            marker=dict(size=5),
                        ))
                else:
                    fig.add_trace(go.Scatter(
                        x=chart_df["auction_date"], y=chart_df["cutoff_yield"],
                        mode="lines+markers", name="Cutoff Yield",
                        line=dict(width=2, color=_COLORS["tbill"]),
                        marker=dict(size=5),
                    ))
                    if chart_df["weighted_avg_yield"].notna().any():
                        fig.add_trace(go.Scatter(
                            x=chart_df["auction_date"], y=chart_df["weighted_avg_yield"],
                            mode="lines", name="WA Yield",
                            line=dict(width=1.5, dash="dot", color=_COLORS["konia"]),
                        ))

                ymin = chart_df["cutoff_yield"].min()
                ymax = chart_df["cutoff_yield"].max()
                pad = max((ymax - ymin) * 0.15, 0.1)
                fig.update_layout(
                    yaxis=dict(title="Yield (%)", range=[ymin - pad, ymax + pad],
                               gridcolor=_COLORS["grid"]),
                    legend=dict(orientation="h", y=-0.18, bgcolor="rgba(0,0,0,0)",
                                font=dict(size=10)),
                )
                st.plotly_chart(fig, width='stretch')

                st.caption(f"{tb_count} total records")
                st.dataframe(df.rename(columns={
                    "auction_date": "Date", "tenor": "Tenor",
                    "cutoff_yield": "Cutoff %", "weighted_avg_yield": "WA Yield %",
                    "target_amount_billions": "Target (B)", "amount_accepted_billions": "Accepted (B)",
                }), width='stretch', hide_index=True)

    # ── PIBs ──
    with ac2:
        st.markdown("### PIB Auctions")
        pib_count_init, pib_tenors_init, _ = _load_auction_data("pib", "All")
        if pib_count_init == 0:
            st.info("No PIB data. Sync below.")
        else:
            sel_tenor = st.selectbox("Tenor", ["All"] + pib_tenors_init, key="pib_tenor")
            pib_count, tenors, df = _load_auction_data("pib", sel_tenor)

            if not df.empty:
                chart_df = df.sort_values("auction_date")
                fig = _styled_fig(height=320)

                if sel_tenor == "All":
                    # Plot each tenor as a separate line to avoid zigzag
                    pib_colors = ["#9B59B6", "#3498DB", "#E67E22", "#2ECC71", "#E74C3C", "#1ABC9C", "#F39C12", "#00BCD4"]
                    for idx, tenor in enumerate(sorted(chart_df["tenor"].unique())):
                        sub = chart_df[chart_df["tenor"] == tenor]
                        fig.add_trace(go.Scatter(
                            x=sub["auction_date"], y=sub["cutoff_yield"],
                            mode="lines+markers", name=tenor,
                            line=dict(width=2, color=pib_colors[idx % len(pib_colors)]),
                            marker=dict(size=5),
                        ))
                else:
                    fig.add_trace(go.Scatter(
                        x=chart_df["auction_date"], y=chart_df["cutoff_yield"],
                        mode="lines+markers", name="Cutoff Yield",
                        line=dict(width=2, color=_COLORS["pib"]),
                        marker=dict(size=5),
                    ))

                ymin = chart_df["cutoff_yield"].min()
                ymax = chart_df["cutoff_yield"].max()
                pad = max((ymax - ymin) * 0.15, 0.1)
                fig.update_layout(
                    yaxis=dict(title="Yield (%)", range=[ymin - pad, ymax + pad],
                               gridcolor=_COLORS["grid"]),
                    legend=dict(orientation="h", y=-0.18, bgcolor="rgba(0,0,0,0)",
                                font=dict(size=10)),
                )
                st.plotly_chart(fig, width='stretch')

                st.caption(f"{pib_count} total records")
                st.dataframe(df.rename(columns={
                    "auction_date": "Date", "tenor": "Tenor", "pib_type": "Type",
                    "cutoff_yield": "Yield %", "coupon_rate": "Coupon %",
                    "amount_accepted_billions": "Accepted (B)",
                }), width='stretch', hide_index=True)

    # ── Auction scatter (combined) ──
    st.markdown("### Auction Yield Map")
    combined = _load_auction_yield_map()
    if not combined.empty:
        fig = _styled_fig(height=350)
        for atype, color, sym in [("T-Bill", _COLORS["tbill"], "circle"), ("PIB", _COLORS["pib"], "diamond")]:
            sub = combined[combined["type"] == atype]
            if not sub.empty:
                fig.add_trace(go.Scatter(
                    x=sub["auction_date"], y=sub["cutoff_yield"],
                    mode="markers+text", name=atype,
                    marker=dict(size=10, color=color, symbol=sym),
                    text=sub["tenor"], textposition="top center",
                    textfont=dict(size=8),
                ))
        fig.update_layout(
            yaxis_title="Cutoff Yield (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, width='stretch')

    # ── GIS Sukuk ──
    gis = _load_gis_auctions()
    if not gis.empty:
        st.markdown("### GIS Sukuk Auctions")
        st.dataframe(gis, width='stretch', hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4: KIBOR
# ═════════════════════════════════════════════════════════════════════════════

def _render_kibor(con):
    kb_count, kdf, spread_df, latest = _load_kibor_data()
    if kb_count == 0:
        st.info("No KIBOR history. Use Sync tab to backfill.")
        return

    st.caption(f"{kb_count} total records")

    # ── Term structure chart ──
    st.markdown("### KIBOR Term Structure")
    if not kdf.empty:
        fig = _styled_fig(height=380)
        kibor_colors = {"1M": "#FF6B35", "3M": "#4ECDC4", "6M": "#45B7D1", "12M": "#96CEB4"}
        for tenor in ["1M", "3M", "6M", "12M"]:
            tdf = kdf[kdf["tenor"] == tenor]
            if not tdf.empty:
                fig.add_trace(go.Scatter(
                    x=tdf["date"], y=tdf["offer"], mode="lines",
                    name=f"KIBOR {tenor}",
                    line=dict(width=2, color=kibor_colors.get(tenor, "#999")),
                ))
        fig.update_layout(
            yaxis_title="Offer Rate (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, width='stretch')

    # ── Bid-Offer spread ──
    st.markdown("### Bid-Offer Spread")
    if not spread_df.empty:
        fig = _styled_fig(height=250)
        fig.add_trace(go.Scatter(
            x=spread_df["date"], y=spread_df["spread"],
            mode="lines", name="3M Bid-Offer",
            line=dict(width=2, color="#E67E22"),
            fill="tozeroy", fillcolor="rgba(230,126,34,0.08)",
        ))
        fig.update_layout(yaxis_title="Spread (%)", showlegend=False)
        st.plotly_chart(fig, width='stretch')

    # ── Latest rates table ──
    st.markdown("### Latest KIBOR Rates")
    if not latest.empty:
        latest["days"] = latest["tenor"].map(lambda t: _TENOR_DAYS.get(t.strip())).astype("Int64")
        latest["spread"] = (latest["offer"] - latest["bid"]).round(4)
        st.dataframe(latest.rename(columns={
            "date": "Date", "tenor": "Tenor", "days": "Days",
            "bid": "Bid %", "offer": "Offer %", "spread": "Spread",
        }), width='stretch', hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5: SPREADS
# ═════════════════════════════════════════════════════════════════════════════

def _render_spreads(con):
    tb_spread, kibor_policy = _load_spread_data()
    sc1, sc2 = st.columns(2)

    with sc1:
        st.markdown("### T-Bill 6M vs 12M Spread")
        df = tb_spread
        if not df.empty:
            fig = _styled_fig(height=300)
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["spread"], mode="lines+markers",
                name="12M-6M", line=dict(width=2, color=_COLORS["pib"]),
                fill="tozeroy", fillcolor="rgba(155,89,182,0.08)",
            ))
            fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
            fig.update_layout(yaxis_title="Spread (%)")
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("Need both 6M and 12M auction data")

    with sc2:
        st.markdown("### KIBOR 3M vs Policy Rate")
        df = kibor_policy
        if not df.empty and df["policy"].notna().any():
            df["spread"] = df["kibor"] - df["policy"]
            fig = _styled_fig(height=300)
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["spread"], mode="lines",
                name="KIBOR-Policy", line=dict(width=2, color=_COLORS["pkfrv"]),
                fill="tozeroy", fillcolor="rgba(230,126,34,0.08)",
            ))
            fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
            fig.update_layout(yaxis_title="Spread (%)")
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No KIBOR-Policy spread data")

    # ── Curve steepness ──
    st.markdown("### Yield Curve Steepness (2Y-10Y)")
    steepness = _load_curve_steepness()

    if steepness:
        sdf = pd.DataFrame(steepness).sort_values("date")
        fig = _styled_fig(height=280)
        bar_colors = [_COLORS["up"] if s >= 0 else _COLORS["down"] for s in sdf["steep"]]
        fig.add_trace(go.Bar(
            x=sdf["date"], y=sdf["steep"],
            marker_color=bar_colors,
        ))
        fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
        fig.update_layout(yaxis_title="10Y-2Y Spread (%)", showlegend=False)
        st.plotly_chart(fig, width='stretch')

        latest_steep = sdf.iloc[-1]["steep"]
        signal = "Normal (Positive)" if latest_steep > 0.1 else "Flat" if abs(latest_steep) <= 0.1 else "Inverted"
        _card("Curve Shape", signal, latest_steep, color=_COLORS["pkrv"], suffix="%")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 6: GLOBAL RATES
# ═════════════════════════════════════════════════════════════════════════════

def _render_global_rates(con):
    from pakfindata.db.repositories.global_rates import (
        ensure_tables,
        get_all_latest_rates,
        get_rate_comparison,
        get_rate_history,
        get_sofr_kibor_spread,
    )

    ensure_tables(con)

    # ── Sync button ──
    sc1, sc2 = st.columns([3, 1])
    with sc2:
        if st.button("Sync Global Rates", type="primary", key="tsy_gr_sync"):
            with st.spinner("Fetching from NY Fed, BoE, ECB, BoJ..."):
                try:
                    from pakfindata.sources.global_rates_scraper import GlobalRatesScraper
                    stats = GlobalRatesScraper().sync_all(con)
                    parts = [f"{k}: {v}" for k, v in stats.items()]
                    st.success(" | ".join(parts))
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    # ── Rate cards ──
    st.markdown("### Global Benchmark Rates")
    comparison = get_rate_comparison(con)

    cols = st.columns(6)
    rate_labels = [
        ("SOFR", "USD", comparison.get("SOFR"), _COLORS["sofr"]),
        ("EFFR", "USD", comparison.get("EFFR"), _COLORS["effr"]),
        ("KIBOR 6M", "PKR", comparison.get("KIBOR_6M"), _COLORS["kibor"]),
        ("KONIA", "PKR", comparison.get("KONIA"), _COLORS["konia"]),
        ("SONIA", "GBP", comparison.get("SONIA"), _COLORS["sonia"]),
        ("EUSTR", "EUR", comparison.get("EUSTR"), _COLORS["eustr"]),
    ]
    for col, (name, ccy, val, color) in zip(cols, rate_labels):
        with col:
            _card(f"{name} ({ccy})", f"{val:.4f}%" if val is not None else "N/A", color=color)

    # ── SOFR + EFFR history chart ──
    st.markdown("### SOFR & EFFR History")
    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2, key="tsy_sofr_days")
    from datetime import timedelta
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    sofr_df = get_rate_history(con, rate_name="SOFR", tenor="ON", start_date=start, limit=0)
    effr_df = get_rate_history(con, rate_name="EFFR", tenor="ON", start_date=start, limit=0)

    if sofr_df.empty and effr_df.empty:
        st.info("No SOFR/EFFR data. Click **Sync Global Rates** above.")
    else:
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
            row_heights=[0.7, 0.3], subplot_titles=["Rate (%)", "SOFR Volume ($B)"],
        )
        if not sofr_df.empty:
            sofr_df = sofr_df.sort_values("date")
            fig.add_trace(go.Scatter(
                x=sofr_df["date"], y=sofr_df["rate"], name="SOFR",
                line=dict(color=_COLORS["sofr"], width=2),
            ), row=1, col=1)
            if "volume" in sofr_df.columns:
                vol = sofr_df.dropna(subset=["volume"])
                if not vol.empty:
                    fig.add_trace(go.Bar(
                        x=vol["date"], y=vol["volume"], name="Volume",
                        marker_color="rgba(33,150,243,0.3)",
                    ), row=2, col=1)
        if not effr_df.empty:
            effr_df = effr_df.sort_values("date")
            fig.add_trace(go.Scatter(
                x=effr_df["date"], y=effr_df["rate"], name="EFFR",
                line=dict(color=_COLORS["effr"], width=2, dash="dot"),
            ), row=1, col=1)
        _layout = {k: v for k, v in _CHART_LAYOUT.items() if k != "legend"}
        fig.update_layout(
            **_layout, height=450,
            legend=dict(orientation="h", y=-0.08, bgcolor="rgba(0,0,0,0)"),
        )
        fig.update_yaxes(title_text="Rate (%)", row=1, col=1)
        fig.update_yaxes(title_text="$B", row=2, col=1)
        st.plotly_chart(fig, width='stretch')

    # ── SOFR-KIBOR Spread ──
    st.markdown("### SOFR-KIBOR Spread")
    st.caption("Higher spread = wider FX forward points")
    spread_df = get_sofr_kibor_spread(con, start_date=start)

    if spread_df.empty:
        st.info("Need both KIBOR and SOFR synced for spread analysis.")
    else:
        tenors = sorted(spread_df["tenor"].unique().tolist())
        sel_tenor = st.selectbox(
            "KIBOR Tenor", tenors,
            index=tenors.index("6M") if "6M" in tenors else 0,
            key="tsy_spread_tenor",
        )
        tdf = spread_df[spread_df["tenor"] == sel_tenor].copy().sort_values("date")
        if not tdf.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            has_sofr = tdf.dropna(subset=["sofr_rate"])
            if not has_sofr.empty:
                fig.add_trace(go.Scatter(
                    x=has_sofr["date"], y=has_sofr["sofr_rate"], name="SOFR",
                    line=dict(color=_COLORS["sofr"], width=2),
                ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=tdf["date"], y=tdf["kibor_offer"],
                name=f"KIBOR {sel_tenor} (Offer)",
                line=dict(color=_COLORS["kibor"], width=2),
            ), secondary_y=False)
            if not has_sofr.empty:
                fig.add_trace(go.Bar(
                    x=has_sofr["date"], y=has_sofr["spread_over_sofr"],
                    name="Spread (ppts)", marker_color="rgba(76,175,80,0.4)",
                ), secondary_y=True)
            _layout = {k: v for k, v in _CHART_LAYOUT.items() if k != "legend"}
            fig.update_layout(
                **_layout, height=400,
                legend=dict(orientation="h", y=-0.08, bgcolor="rgba(0,0,0,0)"),
            )
            fig.update_yaxes(title_text="Rate (%)", secondary_y=False)
            fig.update_yaxes(title_text="Spread (ppts)", secondary_y=True)
            st.plotly_chart(fig, width='stretch')

    # ── All latest rates table ──
    with st.expander("All Global Rates"):
        df = get_all_latest_rates(con)
        if df.empty:
            st.info("No data yet.")
        else:
            display_cols = [c for c in ["date", "rate_name", "tenor", "currency", "rate", "volume", "source"] if c in df.columns]
            st.dataframe(
                df[display_cols], width='stretch', hide_index=True,
                column_config={
                    "rate": st.column_config.NumberColumn("Rate (%)", format="%.4f"),
                    "volume": st.column_config.NumberColumn("Volume ($B)", format="%.1f"),
                },
            )


# ═════════════════════════════════════════════════════════════════════════════
# TAB 7: BONDS (was 6)
# ═════════════════════════════════════════════════════════════════════════════

def _render_bonds(con):
    st.markdown("### PKFRV (Floating Rate Bonds)")
    frv_count, dates = _load_pkfrv_data()
    if frv_count == 0:
        st.info("No PKFRV data. Sync Yield Curves (MUFAP) to fetch.")
        return

    sel_date = st.selectbox("Valuation date", dates, index=0, key="pkfrv_d")

    df = _load_pkfrv_bonds(sel_date)
    if df.empty:
        st.info("No bonds for selected date")
        return

    df["remaining_days"] = df["maturity_date"].apply(_remaining_days)

    st.caption(f"{len(df)} bonds on {sel_date} | {frv_count} total records")

    # ── FMA price bar ──
    df_chart = df[df["fma_price"].notna()].copy()
    if not df_chart.empty:
        fig = _styled_fig(height=380)
        # Color by price relative to par
        colors = [_COLORS["up"] if p >= 100 else _COLORS["down"] if p < 98 else _COLORS["accent"]
                  for p in df_chart["fma_price"]]
        fig.add_trace(go.Bar(
            x=df_chart["bond_code"], y=df_chart["fma_price"],
            marker_color=colors,
            customdata=df_chart[["maturity_date", "remaining_days"]].values,
            hovertemplate="<b>%{x}</b><br>FMA: %{y:.4f}<br>"
                          "Maturity: %{customdata[0]}<br>Rem: %{customdata[1]}d<extra></extra>",
        ))
        fig.add_hline(y=100, line_dash="dash", line_color=_COLORS["text_dim"],
                      annotation_text="Par", annotation_position="top right")
        fig.update_layout(
            yaxis_title="FMA Price", showlegend=False,
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig, width='stretch')

    # ── Maturity schedule ──
    mat_df = df.dropna(subset=["remaining_days"]).copy()
    if not mat_df.empty and mat_df["remaining_days"].sum() > 0:
        st.markdown("#### Maturity Schedule")
        mat_df = mat_df.sort_values("remaining_days")
        fig = _styled_fig(height=250)
        fig.add_trace(go.Bar(
            y=mat_df["bond_code"], x=mat_df["remaining_days"],
            orientation="h", marker_color=_COLORS["pkfrv"],
            text=[f"{d:,.0f}d" for d in mat_df["remaining_days"]],
            textposition="outside", textfont=dict(size=9),
        ))
        fig.update_layout(xaxis_title="Days to Maturity", showlegend=False)
        st.plotly_chart(fig, width='stretch')

    with st.expander("Bond Data"):
        st.dataframe(df.rename(columns={
            "bond_code": "Bond", "issue_date": "Issue",
            "maturity_date": "Maturity", "remaining_days": "Rem. Days",
            "coupon_frequency": "Coupon Freq", "fma_price": "FMA Price",
        }), width='stretch', hide_index=True)

    # ── Bond price history ──
    bonds = df["bond_code"].tolist()
    if bonds:
        sel_bond = st.selectbox("Bond history", bonds, key="pkfrv_bond")
        if sel_bond:
            hist = _load_bond_history(sel_bond)
            if len(hist) > 1:
                fig = _styled_fig(height=280)
                fig.add_trace(go.Scatter(
                    x=hist["date"], y=hist["fma_price"], mode="lines",
                    name=sel_bond, line=dict(width=2, color=_COLORS["pkfrv"]),
                ))
                fig.add_hline(y=100, line_dash="dash", line_color=_COLORS["text_dim"])
                fig.update_layout(yaxis_title="FMA Price")
                st.plotly_chart(fig, width='stretch')


# ═════════════════════════════════════════════════════════════════════════════
# TAB 7: SYNC
# ═════════════════════════════════════════════════════════════════════════════

def _render_sync(con):
    st.markdown("### Sync Treasury Data")

    st.markdown("##### Daily Sync (SBP PMA page)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Sync T-Bill / PIB", type="primary", key="tsy_sync_treasury"):
            # Phase 1.6.2: sidebar feature flag picks the path.
            from pakfindata.ui.api import client as api_client
            if api_client.use_worker_sync():
                api_client.run_job_with_progress(
                    "sync_treasury_auctions",
                    spinner_text="syncing T-Bill / PIB auctions",
                )
                st.rerun()
            else:
                with st.spinner("Syncing treasury auctions from SBP..."):
                    from pakfindata.db.safe_writer import SafeWriterBusyError
                    from pakfindata.etl.treasury import sync_auctions
                    try:
                        result = sync_auctions()
                        st.cache_data.clear()
                        st.success(
                            f"T-Bills: {result['tbills_ok']}, PIBs: {result['pibs_ok']}"
                        )
                        st.rerun()
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        # sync_auctions() already recorded the catalog failures.
                        st.error(f"Sync failed: {e}")
    with c2:
        if st.button("Sync Rates (KIBOR/PKRV/KONIA)", key="tsy_sync_rates"):
            # Phase 1.6.5: sidebar feature flag picks the path.
            from pakfindata.ui.api import client as api_client
            if api_client.use_worker_sync():
                api_client.run_job_with_progress(
                    "sync_sbp_curve",
                    spinner_text="scraping KIBOR/PKRV/KONIA from SBP PMA",
                )
                st.rerun()
            else:
                with st.spinner("Syncing rates from SBP..."):
                    from pakfindata.db.safe_writer import SafeWriterBusyError
                    from pakfindata.etl.rates import sync_sbp_curve
                    try:
                        result = sync_sbp_curve()
                        st.cache_data.clear()
                        st.success(
                            f"KIBOR: {result['kibor_ok']}, PKRV: {result['pkrv_points']}, "
                            f"KONIA: {'OK' if result['konia_ok'] else 'N/A'}"
                        )
                        st.rerun()
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        # sync_sbp_curve() already recorded the catalog failures.
                        st.error(f"Sync failed: {e}")
    with c3:
        if st.button("Sync GIS Sukuk", key="tsy_sync_gis"):
            with st.spinner("Syncing GIS auctions from SBP..."):
                try:
                    result = GSPScraper().sync_gis(con)
                    st.success(f"GIS: {result.get('ok', 0)} synced")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    st.markdown("##### Global Rates (SOFR, EFFR, SONIA, EUSTR, TONA)")
    if st.button("Sync Global Rates (NY Fed / BoE / ECB)", key="tsy_sync_global"):
        with st.spinner("Fetching global rates..."):
            try:
                from pakfindata.sources.global_rates_scraper import GlobalRatesScraper
                stats = GlobalRatesScraper().sync_all(con)
                parts = [f"{k}: {v}" for k, v in stats.items()]
                st.success(" | ".join(parts))
                st.rerun()
            except Exception as e:
                st.error(f"Global rates sync failed: {e}")

    st.markdown("##### MUFAP Yield Curves (PKRV/PKISRV/PKFRV)")
    if st.button("Sync Yield Curves (MUFAP)", key="tsy_sync_mufap"):
        with st.spinner("Downloading & parsing MUFAP rate files..."):
            from pakfindata.db.safe_writer import safe_writer, SafeWriterBusyError
            from pakfindata.db.catalog import update_catalog_from_table, record_catalog_failure
            try:
                from pakfindata.sources.mufap_rates import download_and_sync
                with safe_writer() as wcon:
                    result = download_and_sync(wcon)
                    update_catalog_from_table(wcon, "yield_curve", source="mufap")
                    update_catalog_from_table(wcon, "pkisrv", source="mufap")
                    update_catalog_from_table(wcon, "pkfrv", source="mufap")
                st.cache_data.clear()
                st.success(
                    f"New: {result['downloaded']}, Skipped: {result['skipped']} | "
                    f"PKRV: {result['pkrv_records']}, PKISRV: {result['pkisrv_records']}, "
                    f"PKFRV: {result['pkfrv_records']}"
                )
                st.rerun()
            except SafeWriterBusyError:
                st.error("Another sync is running. Wait a moment and retry.")
            except Exception as e:
                st.error(f"MUFAP sync failed: {e}")
                for ds in ("yield_curve", "pkisrv", "pkfrv"):
                    record_catalog_failure(ds, source="mufap", error=e)

    st.markdown("##### SBP EasyData (KIBOR, Policy Rate, FX, BoP, CPI, Reserves)")
    from pakfindata.sources.sbp_easydata import (
        is_fetch_running, read_fetch_status, start_fetch_background, sync_all_to_db,
    )

    fetch_running = is_fetch_running()
    fetch_status = read_fetch_status()

    # Show live status if running or recently completed
    if fetch_running:
        detail = fetch_status.get("detail", "working...")
        prog = fetch_status.get("progress", 0)
        total = fetch_status.get("total", 1) or 1
        st.progress(min(prog / total, 1.0), text=f"EasyData: {detail}")
        st.caption(f"Series: {fetch_status.get('series', 0)} | Obs: {fetch_status.get('observations', 0):,}")
    elif fetch_status.get("status") == "done":
        st.success(f"EasyData: {fetch_status.get('detail', 'complete')}")

    ec1, ec2, ec3 = st.columns(3)
    with ec1:
        if st.button(
            "Update Recent (1Y)", type="primary", key="tsy_easydata_recent",
            disabled=fetch_running,
        ):
            ok, msg = start_fetch_background(months=12)
            if ok:
                st.toast(f"EasyData fetch started in background — {msg}")
            else:
                st.warning(msg)
            st.rerun()
    with ec2:
        if st.button(
            "Full History (slow)", key="tsy_easydata_fetch",
            disabled=fetch_running,
        ):
            ok, msg = start_fetch_background(months=600)  # ~50 years
            if ok:
                st.toast(f"Full history fetch started — {msg}")
            else:
                st.warning(msg)
            st.rerun()
    with ec3:
        if st.button("Sync CSVs to DB", key="tsy_easydata_sync"):
            # Phase 1.6.7: sidebar feature flag picks the path.
            from pakfindata.ui.api import client as api_client
            if api_client.use_worker_sync():
                api_client.run_job_with_progress(
                    "sync_easydata_csv",
                    spinner_text="syncing EasyData CSVs (KIBOR + FX + policy)",
                )
                st.rerun()
            else:
                with st.spinner("Syncing EasyData CSVs to local DB tables..."):
                    from pakfindata.db.safe_writer import SafeWriterBusyError
                    from pakfindata.etl.easydata import sync_csvs_to_db
                    try:
                        result = sync_csvs_to_db()
                        st.cache_data.clear()
                        parts = [
                            f"{k}: {v}" for k, v in result.items()
                            if k not in ("duration_ms", "as_of")
                        ]
                        st.success(" | ".join(parts))
                        st.rerun()
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        # sync_csvs_to_db() already recorded the catalog failures.
                        st.error(f"EasyData sync failed: {e}")

    st.markdown("##### Historical Backfill (SBP PDFs)")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Backfill PIB (2000-Present)", key="tsy_backfill_pib"):
            with st.spinner("Downloading PIB archive PDF..."):
                try:
                    from pakfindata.sources.sbp_pib_archive import SBPPibArchiveScraper
                    counts = SBPPibArchiveScraper().sync_pib_archive(con)
                    st.success(f"PIB: {counts['inserted']}/{counts['total']} inserted")
                    st.rerun()
                except Exception as e:
                    st.error(f"PIB backfill failed: {e}")
    with c2:
        _render_kibor_backfill_button()
    with c3:
        _render_konia_backfill_button()


def _render_kibor_backfill_button():
    from pakfindata.sources.sbp_kibor_history import (
        is_kibor_history_sync_running,
        read_kibor_sync_progress,
        start_kibor_history_sync,
    )

    if is_kibor_history_sync_running():
        progress = read_kibor_sync_progress()
        if progress:
            pct = progress["current"] / max(progress["total_days"], 1)
            st.progress(pct, text=f"KIBOR: {progress['current_date']} ({progress['records_inserted']} records)")
        else:
            st.info("KIBOR history sync running...")
    else:
        progress = read_kibor_sync_progress()
        if progress and progress.get("status") == "completed":
            st.caption(f"Last: {progress['records_inserted']} records, {progress['dates_processed']} dates")

        start_year = st.number_input(
            "Start year", min_value=2008, max_value=2026, value=2024,
            key="kibor_start_year",
        )
        if st.button("Backfill KIBOR (2008-Present)", key="tsy_backfill_kibor"):
            started = start_kibor_history_sync(start_year=int(start_year))
            if started:
                st.success("KIBOR history sync started in background")
                st.rerun()
            else:
                st.warning("KIBOR sync already running")


def _render_konia_backfill_button():
    from pakfindata.sources.sbp_konia_history import (
        is_konia_history_sync_running,
        read_konia_sync_progress,
        start_konia_history_sync,
    )

    if is_konia_history_sync_running():
        progress = read_konia_sync_progress()
        if progress:
            phase = progress.get("phase", "")
            inserted = progress.get("inserted", 0)
            total = progress.get("total_records", 0)
            if total > 0:
                pct = inserted / total
                st.progress(pct, text=f"KONIA: {inserted}/{total} records")
            else:
                st.info(f"KONIA: {phase}...")
        else:
            st.info("KONIA history sync running...")
    else:
        progress = read_konia_sync_progress()
        if progress and progress.get("status") == "completed":
            st.caption(f"Last: {progress.get('inserted', 0)} records")

        konia_count = 0
        try:
            # Coarse coverage probe — KONIA latest row count via /v1.
            # We can't get a row total without a dedicated endpoint, so
            # presence of a single row is what gates the caption.
            konia_count = len(api_client.get_konia(limit=1) or [])
        except Exception:
            pass
        st.caption(f"Has data" if konia_count else "No data yet")

        if st.button("Backfill KONIA (2015-Present)", key="tsy_backfill_konia"):
            started = start_konia_history_sync()
            if started:
                st.success("KONIA history sync started in background")
                st.rerun()
            else:
                st.warning("KONIA sync already running")
