"""Dashboard page -- quant-worthy market overview."""

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.api_client import get_client
from pakfindata.config import get_db_path
from pakfindata.sources.fx_client import FXClient

_fx = FXClient()
from pakfindata.services import (
    is_service_running,
    read_status as read_service_status,
    is_eod_sync_running,
    read_eod_status,
    start_eod_sync_background,
    stop_eod_sync,
    is_announcements_service_running,
    read_announcements_status,
    start_announcements_service,
    stop_announcements_service,
)
from pakfindata.ui.charts import (
    make_market_breadth_chart,
    make_top_movers_chart,
)
from pakfindata.ui.components.helpers import (
    check_data_staleness,
    DATA_QUALITY_NOTICE,
    format_volume,
    get_freshness_badge,
    render_data_info,
    render_data_warning,
    render_domain_freshness_bar,
    render_footer,
    render_market_status_badge,
)


# ── Sync helpers ──────────────────────────────────────────────────────────

def _sync_market_data(con):
    from pakfindata.sources.regular_market import (
        fetch_regular_market, get_all_current_hashes,
        init_regular_market_schema, insert_snapshots, upsert_current,
    )
    from pakfindata.analytics import compute_all_analytics
    from datetime import datetime
    init_regular_market_schema(con)
    df = fetch_regular_market()
    prev_hashes = get_all_current_hashes(con)
    insert_snapshots(con, df, prev_hashes=prev_hashes)
    n = upsert_current(con, df)
    ts = df["ts"].iloc[0] if not df.empty else datetime.now().isoformat()
    compute_all_analytics(con, ts)
    return n


def _sync_indices(con):
    from pakfindata.sources.indices import fetch_indices_data, save_index_data
    indices_data = fetch_indices_data()
    return sum(1 for d in indices_data if save_index_data(con, d))


def _sync_rates(con):
    from pakfindata.sources.sbp_rates import SBPRatesScraper
    from pakfindata.sources.sbp_treasury import SBPTreasuryScraper
    rates = SBPRatesScraper().sync_rates(con)
    treas = SBPTreasuryScraper().sync_treasury(con)
    return rates, treas


# ── Query helpers (cached) ────────────────────────────────────────────────

@st.cache_data(ttl=60)
def _get_rates_strip(_con):
    """Fetch macro rates for the top strip."""
    data = {}
    queries = {
        "policy": ("SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1", None),
        "kibor3m": ("SELECT bid, offer, date FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1", None),
        "tbill3m": ("SELECT cutoff_yield, auction_date FROM tbill_auctions WHERE tenor='3M' ORDER BY auction_date DESC LIMIT 1", None),
        "pkrv10y": ("SELECT yield_pct, date FROM pkrv_daily WHERE tenor_months=120 ORDER BY date DESC LIMIT 1", None),
    }
    for key, (sql, _) in queries.items():
        try:
            row = _con.execute(sql).fetchone()
            data[key] = tuple(row) if row else None
        except Exception:
            data[key] = None
    return data


@st.cache_data(ttl=60)
def _get_fx_rates(_con):
    """Fetch FX rates from DB."""
    fx_data = []
    for curr in ["USD", "EUR", "GBP", "AED", "SAR"]:
        try:
            row = _con.execute(
                "SELECT date, selling FROM sbp_fx_interbank WHERE UPPER(currency)=? ORDER BY date DESC LIMIT 1",
                (curr,),
            ).fetchone()
            if row:
                fx_data.append((curr, float(row[1]), str(row[0])))
                continue
        except Exception:
            pass
        try:
            row = _con.execute(
                "SELECT date, selling FROM forex_kerb WHERE UPPER(currency)=? ORDER BY date DESC LIMIT 1",
                (curr,),
            ).fetchone()
            if row:
                fx_data.append((curr, float(row[1]), str(row[0])))
        except Exception:
            pass
    return fx_data


@st.cache_data(ttl=60)
def _get_market_breadth(_con):
    """Get gainers/losers/volume from EOD data."""
    try:
        row = _con.execute("""
            WITH best_date AS (
                SELECT date FROM eod_ohlcv
                GROUP BY date HAVING COUNT(DISTINCT symbol) >= 100
                ORDER BY date DESC LIMIT 1
            ),
            today AS (
                SELECT symbol, close, volume FROM eod_ohlcv
                WHERE date = (SELECT date FROM best_date)
            ),
            prev AS (
                SELECT symbol, close as prev_close FROM eod_ohlcv
                WHERE date = (SELECT MAX(date) FROM eod_ohlcv WHERE date < (SELECT date FROM best_date))
            ),
            changes AS (
                SELECT t.symbol, t.volume,
                    CASE WHEN p.prev_close > 0
                        THEN ((t.close - p.prev_close) / p.prev_close) * 100 ELSE 0
                    END as change_percent
                FROM today t LEFT JOIN prev p ON t.symbol = p.symbol
            )
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN change_percent > 0.01 THEN 1 ELSE 0 END) as gainers,
                SUM(CASE WHEN change_percent < -0.01 THEN 1 ELSE 0 END) as losers,
                SUM(CASE WHEN change_percent BETWEEN -0.01 AND 0.01 THEN 1 ELSE 0 END) as unchanged,
                ROUND(AVG(change_percent), 2) as avg_change,
                SUM(volume) as total_volume
            FROM changes
        """).fetchone()
        if row:
            return dict(row)
        return None
    except Exception:
        return None


@st.cache_data(ttl=60)
def _get_volume_leaders(_con, limit=5):
    try:
        return pd.read_sql_query("""
            WITH best_date AS (
                SELECT date FROM eod_ohlcv
                GROUP BY date HAVING COUNT(DISTINCT symbol) >= 100
                ORDER BY date DESC LIMIT 1
            ),
            today AS (SELECT symbol, close, volume FROM eod_ohlcv WHERE date=(SELECT date FROM best_date)),
            prev AS (SELECT symbol, close as prev_close FROM eod_ohlcv
                      WHERE date=(SELECT MAX(date) FROM eod_ohlcv WHERE date < (SELECT date FROM best_date)))
            SELECT t.symbol, t.volume, t.close as price,
                   ROUND(((t.close - p.prev_close) / p.prev_close) * 100, 2) as change_pct
            FROM today t LEFT JOIN prev p ON t.symbol = p.symbol
            WHERE t.volume > 0 ORDER BY t.volume DESC LIMIT ?
        """, _con, params=(limit,))
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def _get_52w_extremes(_con):
    """Get stocks near 52-week high and low."""
    try:
        high_df = pd.read_sql_query("""
            WITH best_date AS (
                SELECT session_date FROM trading_sessions
                WHERE market_type='REG' AND week_52_high > 0 AND week_52_low > 0
                GROUP BY session_date HAVING COUNT(DISTINCT symbol) >= 100
                ORDER BY session_date DESC LIMIT 1
            )
            SELECT symbol,
                CASE WHEN (week_52_high - week_52_low) > 0
                    THEN ROUND((COALESCE(close,high,ldcp) - week_52_low) / (week_52_high - week_52_low) * 100, 1)
                    ELSE 50 END as pos_pct
            FROM trading_sessions
            WHERE session_date=(SELECT session_date FROM best_date)
                AND market_type='REG' AND week_52_high > 0 AND week_52_low > 0
                AND COALESCE(close,high,ldcp) > 0
            ORDER BY pos_pct DESC LIMIT 3
        """, _con)
    except Exception:
        high_df = pd.DataFrame()
    try:
        low_df = pd.read_sql_query("""
            WITH best_date AS (
                SELECT session_date FROM trading_sessions
                WHERE market_type='REG' AND week_52_high > 0 AND week_52_low > 0
                GROUP BY session_date HAVING COUNT(DISTINCT symbol) >= 100
                ORDER BY session_date DESC LIMIT 1
            )
            SELECT symbol,
                CASE WHEN (week_52_high - week_52_low) > 0
                    THEN ROUND((COALESCE(close,high,ldcp) - week_52_low) / (week_52_high - week_52_low) * 100, 1)
                    ELSE 50 END as pos_pct
            FROM trading_sessions
            WHERE session_date=(SELECT session_date FROM best_date)
                AND market_type='REG' AND week_52_high > 0 AND week_52_low > 0
                AND COALESCE(close,high,ldcp) > 0
            ORDER BY pos_pct ASC LIMIT 3
        """, _con)
    except Exception:
        low_df = pd.DataFrame()
    return high_df, low_df


# ── HTML building blocks ──────────────────────────────────────────────────

def _kse100_hero(kse, breadth):
    """Build the KSE-100 hero banner HTML."""
    value = kse.get("value", 0)
    change = kse.get("change", 0) or 0
    change_pct = kse.get("change_pct", 0) or 0
    high = kse.get("high")
    low = kse.get("low")
    volume = kse.get("volume")
    ytd_pct = kse.get("ytd_change_pct", 0) or 0
    idx_date = kse.get("index_date", "")

    if change > 0:
        color, arrow, sign = "#00C853", "&#9650;", "+"
    elif change < 0:
        color, arrow, sign = "#FF5252", "&#9660;", ""
    else:
        color, arrow, sign = "#6B7280", "&#9679;", ""

    ytd_color = "#00C853" if ytd_pct > 0 else "#FF5252" if ytd_pct < 0 else "#6B7280"
    vol_str = f"{volume/1e6:.0f}M" if volume and volume >= 1e6 else (f"{volume:,}" if volume else "---")
    high_str = f"{high:,.2f}" if high else "---"
    low_str = f"{low:,.2f}" if low else "---"

    # Breadth bar
    g = breadth["gainers"] or 0 if breadth else 0
    l = breadth["losers"] or 0 if breadth else 0
    total = g + l or 1
    g_pct = g / total * 100
    l_pct = l / total * 100

    return f"""
    <div style="background:#12161C;border:1px solid #1E2329;border-radius:4px;padding:16px 20px;margin-bottom:8px;">
      <div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;">
        <span style="font-size:13px;color:#6B7280;font-weight:600;letter-spacing:0.05em;">KSE-100</span>
        <span style="font-size:28px;font-weight:700;font-family:ui-monospace,monospace;color:#EAECEF;">
          {value:,.2f}
        </span>
        <span style="font-size:16px;font-weight:600;color:{color};font-family:ui-monospace,monospace;">
          {arrow} {sign}{change:,.2f} ({sign}{change_pct:.2f}%)
        </span>
        <span style="font-size:12px;color:#6B7280;margin-left:auto;">
          {idx_date}
        </span>
      </div>
      <div style="display:flex;gap:24px;margin-top:10px;font-size:12px;font-family:ui-monospace,monospace;color:#9AA4B2;">
        <span>H <span style="color:#EAECEF">{high_str}</span></span>
        <span>L <span style="color:#EAECEF">{low_str}</span></span>
        <span>Vol <span style="color:#EAECEF">{vol_str}</span></span>
        <span>YTD <span style="color:{ytd_color}">{ytd_pct:+.2f}%</span></span>
        <span style="margin-left:auto;">
          <span style="color:#00C853">{g}</span> /
          <span style="color:#FF5252">{l}</span> A/D
        </span>
      </div>
      <div style="display:flex;height:3px;margin-top:8px;border-radius:2px;overflow:hidden;">
        <div style="width:{g_pct:.0f}%;background:#00C853;"></div>
        <div style="width:{l_pct:.0f}%;background:#FF5252;"></div>
      </div>
    </div>"""


def _kse100_proxy_hero(breadth):
    """Fallback hero when no index data -- show breadth only."""
    if not breadth or not breadth["total"]:
        return ""
    g = breadth["gainers"] or 0
    l = breadth["losers"] or 0
    avg = breadth["avg_change"] or 0
    total = g + l or 1
    g_pct = g / total * 100
    l_pct = l / total * 100

    color = "#00C853" if avg > 0 else "#FF5252" if avg < 0 else "#6B7280"
    arrow = "&#9650;" if avg > 0 else "&#9660;" if avg < 0 else "&#9679;"
    sign = "+" if avg > 0 else ""
    vol = breadth["total_volume"] or 0
    vol_str = f"{vol/1e6:.0f}M" if vol >= 1e6 else f"{vol:,}" if vol else "---"

    return f"""
    <div style="background:#12161C;border:1px solid #1E2329;border-radius:4px;padding:16px 20px;margin-bottom:8px;">
      <div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;">
        <span style="font-size:13px;color:#6B7280;font-weight:600;letter-spacing:0.05em;">KSE-100 (PROXY)</span>
        <span style="font-size:24px;font-weight:700;font-family:ui-monospace,monospace;color:{color};">
          {arrow} {sign}{avg:.2f}%
        </span>
        <span style="font-size:12px;color:#6B7280;">
          Avg across {breadth['total']} stocks
        </span>
      </div>
      <div style="display:flex;gap:24px;margin-top:10px;font-size:12px;font-family:ui-monospace,monospace;color:#9AA4B2;">
        <span>Vol <span style="color:#EAECEF">{vol_str}</span></span>
        <span style="margin-left:auto;">
          <span style="color:#00C853">{g}</span> /
          <span style="color:#FF5252">{l}</span> A/D
        </span>
      </div>
      <div style="display:flex;height:3px;margin-top:8px;border-radius:2px;overflow:hidden;">
        <div style="width:{g_pct:.0f}%;background:#00C853;"></div>
        <div style="width:{l_pct:.0f}%;background:#FF5252;"></div>
      </div>
    </div>"""


def _rates_strip_html(rates, fx_data, fx_live=None):
    """Compact rates strip: Policy | KIBOR | T-Bill | PKRV | FX."""
    items = []

    # Policy Rate
    pr = rates.get("policy")
    if pr:
        items.append(f'<span class="rs-label">SBP</span><span class="rs-val">{pr[0]:.1f}%</span>')

    # KIBOR 3M
    kb = rates.get("kibor3m")
    if kb and (kb[0] or kb[1]):
        mid = ((kb[0] or 0) + (kb[1] or 0)) / 2 if kb[0] and kb[1] else (kb[0] or kb[1])
        items.append(f'<span class="rs-label">KIBOR 3M</span><span class="rs-val">{mid:.2f}%</span>')

    # T-Bill 3M
    tb = rates.get("tbill3m")
    if tb:
        items.append(f'<span class="rs-label">T-Bill 3M</span><span class="rs-val">{tb[0]:.2f}%</span>')

    # PKRV 10Y
    pv = rates.get("pkrv10y")
    if pv:
        items.append(f'<span class="rs-label">PKRV 10Y</span><span class="rs-val">{pv[0]:.2f}%</span>')

    # Separator
    if items and fx_data:
        items.append('<span class="rs-sep">|</span>')

    # FX rates -- prefer live microservice, fall back to DB
    if fx_live:
        for pair, idx in [("USD/PKR", 0), ("EUR/PKR", 1), ("GBP/PKR", 2)]:
            r = fx_live.get(pair, {})
            mid = r.get("mid")
            if mid:
                items.append(f'<span class="rs-label">{pair.split("/")[0]}</span><span class="rs-val">{mid:,.2f}</span>')
    elif fx_data:
        for curr, rate, _ in fx_data[:3]:
            items.append(f'<span class="rs-label">{curr}</span><span class="rs-val">{rate:,.2f}</span>')

    cells = "".join(items)
    return f"""
    <style>
    .rates-strip {{
        display:flex; align-items:center; gap:12px; padding:6px 12px;
        background:#0B0E11; border:1px solid #1E2329; border-radius:2px;
        font-family:ui-monospace,monospace; font-size:12px; margin-bottom:10px;
        overflow-x:auto; white-space:nowrap;
    }}
    .rs-label {{ color:#6B7280; margin-right:4px; }}
    .rs-val {{ color:#EAECEF; font-weight:600; margin-right:12px; }}
    .rs-sep {{ color:#1E2329; font-size:16px; }}
    </style>
    <div class="rates-strip">{cells}</div>"""


def _volume_leaders_html(df):
    """Compact volume leaders list."""
    if df.empty:
        return ""
    rows = []
    for _, r in df.iterrows():
        vol = r["volume"]
        vol_str = f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol:,.0f}"
        chg = r.get("change_pct", 0) or 0
        c = "#00C853" if chg > 0 else "#FF5252" if chg < 0 else "#6B7280"
        rows.append(
            f'<tr><td style="color:#EAECEF;font-weight:600">{r["symbol"]}</td>'
            f'<td style="text-align:right">{vol_str}</td>'
            f'<td style="text-align:right;color:{c}">{chg:+.2f}%</td></tr>'
        )
    tbody = "".join(rows)
    return f"""
    <div style="font-size:12px;font-family:ui-monospace,monospace;">
      <div style="color:#6B7280;font-size:11px;font-weight:600;letter-spacing:0.05em;margin-bottom:6px;">
        VOLUME LEADERS
      </div>
      <table style="width:100%;border-collapse:collapse;color:#9AA4B2;">
        {tbody}
      </table>
    </div>"""


def _52w_html(high_df, low_df):
    """52-week range extremes."""
    rows = []
    if not high_df.empty:
        rows.append('<div style="color:#6B7280;font-size:10px;margin-bottom:2px;">NEAR 52W HIGH</div>')
        for _, r in high_df.iterrows():
            rows.append(f'<div><span style="color:#00C853;font-weight:600">{r["symbol"]}</span>'
                        f' <span style="color:#6B7280">{r["pos_pct"]:.0f}%</span></div>')
    if not low_df.empty:
        rows.append('<div style="color:#6B7280;font-size:10px;margin-top:6px;margin-bottom:2px;">NEAR 52W LOW</div>')
        for _, r in low_df.iterrows():
            rows.append(f'<div><span style="color:#FF5252;font-weight:600">{r["symbol"]}</span>'
                        f' <span style="color:#6B7280">{r["pos_pct"]:.0f}%</span></div>')
    if not rows:
        return ""
    return f"""
    <div style="font-size:12px;font-family:ui-monospace,monospace;">
      <div style="color:#6B7280;font-size:11px;font-weight:600;letter-spacing:0.05em;margin-bottom:6px;">
        52-WEEK RANGE
      </div>
      {"".join(rows)}
    </div>"""


# ── Main render ───────────────────────────────────────────────────────────

def render_dashboard():
    """Quant-worthy market dashboard."""

    # Auto-refresh when sync service is running
    service_running, _ = is_service_running()
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=60000, limit=None, key="dashboard_autorefresh")

    try:
        client = get_client()
        con = client.connection

        # ══════════════════════════════════════════════════════════════
        # ROW 0: HEADER -- Title + Status + Refresh
        # ══════════════════════════════════════════════════════════════
        h1, h2, h3 = st.columns([3, 1, 0.5])
        with h1:
            st.markdown("### Market Dashboard")
        with h2:
            render_market_status_badge()
            days_old, latest_date = client.get_data_freshness()
            _, badge_text = get_freshness_badge(days_old)
            sync_dot = "ON" if service_running else "OFF"
            if latest_date:
                st.caption(f"Data: {badge_text} | Sync: {sync_dot}")
        with h3:
            refresh_all = st.button("Refresh", type="primary", key="dash_refresh",
                                    use_container_width=True)

        # ══════════════════════════════════════════════════════════════
        # ROW 1: KSE-100 HERO
        # ══════════════════════════════════════════════════════════════
        breadth = _get_market_breadth(con)
        try:
            kse100 = client.get_latest_kse100()
        except Exception:
            kse100 = None

        if kse100:
            st.markdown(_kse100_hero(kse100, breadth), unsafe_allow_html=True)
        elif breadth and breadth["total"]:
            st.markdown(_kse100_proxy_hero(breadth), unsafe_allow_html=True)

        # ══════════════════════════════════════════════════════════════
        # ROW 2: RATES STRIP
        # ══════════════════════════════════════════════════════════════
        rates = _get_rates_strip(con)
        fx_db = _get_fx_rates(con)
        fx_live = None
        try:
            if _fx.is_healthy():
                snap = _fx.get_snapshot()
                if snap:
                    fx_live = snap.get("rates", {})
        except Exception:
            pass
        st.markdown(_rates_strip_html(rates, fx_db, fx_live), unsafe_allow_html=True)

        # ══════════════════════════════════════════════════════════════
        # ROW 3: FRESHNESS BAR (compact, non-blocking)
        # ══════════════════════════════════════════════════════════════
        render_domain_freshness_bar(con)

        # Stale data warning
        is_stale, stale_msg = check_data_staleness(con)
        if is_stale:
            render_data_warning(f"{stale_msg}. Hit Refresh to update.")

        # ══════════════════════════════════════════════════════════════
        # REFRESH ALL HANDLER
        # ══════════════════════════════════════════════════════════════
        if refresh_all:
            with st.status("Refreshing...", expanded=True) as status:
                errors = []
                for label, fn in [
                    ("Market data", lambda: _sync_market_data(con)),
                    ("Indices", lambda: _sync_indices(con)),
                    ("Rates & Treasury", lambda: _sync_rates(con)),
                ]:
                    status.update(label=f"Fetching {label}...")
                    try:
                        fn()
                        st.write(f"{label}: OK")
                    except Exception as e:
                        errors.append(str(e))
                        st.write(f"{label}: FAILED - {e}")
                if errors:
                    status.update(label=f"Done with {len(errors)} error(s)", state="error")
                else:
                    status.update(label="All data refreshed", state="complete")
            st.rerun()

        # ══════════════════════════════════════════════════════════════
        # ROW 4: MARKET VIZ -- Breadth | Gainers | Losers
        # ══════════════════════════════════════════════════════════════
        try:
            from pakfindata.sources.regular_market import init_regular_market_schema
            if con:
                init_regular_market_schema(con)
            client.init_analytics()
            market_analytics = client.get_latest_market_analytics()

            if market_analytics:
                gainers = market_analytics.get("gainers_count", 0)
                losers = market_analytics.get("losers_count", 0)
                unchanged = market_analytics.get("unchanged_count", 0)

                col1, col2, col3 = st.columns([1, 1, 1])

                with col1:
                    fig = make_market_breadth_chart(gainers=gainers, losers=losers,
                                                    unchanged=unchanged, height=280)
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    top_g = client.get_top_list("gainers", limit=5)
                    if not top_g.empty:
                        fig = make_top_movers_chart(top_g[["symbol", "change_pct"]],
                                                    title="Top Gainers", chart_type="gainers", height=280)
                        st.plotly_chart(fig, use_container_width=True)

                with col3:
                    top_l = client.get_top_list("losers", limit=5)
                    if not top_l.empty:
                        fig = make_top_movers_chart(top_l[["symbol", "change_pct"]],
                                                    title="Top Losers", chart_type="losers", height=280)
                        st.plotly_chart(fig, use_container_width=True)
        except Exception:
            pass

        # ══════════════════════════════════════════════════════════════
        # ROW 5: DATA GRID -- Volume Leaders | 52W Range | Sector
        # ══════════════════════════════════════════════════════════════
        c1, c2, c3 = st.columns([1, 1, 2])

        with c1:
            vol_df = _get_volume_leaders(con)
            st.markdown(_volume_leaders_html(vol_df), unsafe_allow_html=True)

        with c2:
            high_df, low_df = _get_52w_extremes(con)
            st.markdown(_52w_html(high_df, low_df), unsafe_allow_html=True)

        with c3:
            try:
                sector_df = client.get_sector_leaderboard()
                if not sector_df.empty:
                    st.markdown(
                        '<div style="color:#6B7280;font-size:11px;font-weight:600;'
                        'letter-spacing:0.05em;margin-bottom:4px;">SECTOR PERFORMANCE</div>',
                        unsafe_allow_html=True
                    )
                    display_cols = [c for c in ["sector_name", "symbols_count", "avg_change_pct",
                                                 "sum_volume", "top_symbol"] if c in sector_df.columns]
                    st.dataframe(
                        sector_df[display_cols].head(8),
                        use_container_width=True,
                        hide_index=True,
                        height=260,
                        column_config={
                            "sector_name": st.column_config.TextColumn("Sector", width="medium"),
                            "symbols_count": st.column_config.NumberColumn("Stk", format="%d", width="small"),
                            "avg_change_pct": st.column_config.NumberColumn("Chg%", format="%.2f", width="small"),
                            "sum_volume": st.column_config.NumberColumn("Volume", format="%,.0f"),
                            "top_symbol": st.column_config.TextColumn("Top", width="small"),
                        }
                    )
            except Exception:
                pass

        # ══════════════════════════════════════════════════════════════
        # ROW 6: SYNC CONTROLS (collapsed)
        # ══════════════════════════════════════════════════════════════
        with st.expander("Sync & Data Management", expanded=False):
            sync_c1, sync_c2, sync_c3 = st.columns(3)

            with sync_c1:
                st.markdown("**EOD Sync**")
                eod_running, _ = is_eod_sync_running()
                if eod_running:
                    eod_st = read_eod_status()
                    pct = getattr(eod_st, "progress", 0) or 0
                    msg = getattr(eod_st, "progress_message", "") or "Syncing..."
                    st.progress(min(pct / 100.0, 1.0), text=msg)
                    if st.button("Stop EOD", key="dash_stop_eod"):
                        stop_eod_sync()
                        st.rerun()
                else:
                    eod_st = read_eod_status()
                    last = getattr(eod_st, "completed_at", None) or getattr(eod_st, "ended_at", None)
                    if last:
                        ok = getattr(eod_st, "symbols_ok", "?")
                        st.caption(f"Last: {ok} symbols ({last})")
                    if st.button("Sync EOD", key="dash_sync_eod", help="Background EOD sync"):
                        success, msg = start_eod_sync_background(incremental=True)
                        st.toast(msg)
                        st.rerun()

            with sync_c2:
                st.markdown("**Announcements**")
                ann_running, ann_pid = is_announcements_service_running()
                if ann_running:
                    st.info(f"Running (PID: {ann_pid})")
                    if st.button("Stop Ann", key="dash_stop_ann"):
                        stop_announcements_service()
                        st.rerun()
                else:
                    ann_st = read_announcements_status()
                    last = getattr(ann_st, "last_run_at", None)
                    if last:
                        st.caption(f"Last: {last}")
                    if st.button("Sync Announcements", key="dash_sync_ann"):
                        success, msg = start_announcements_service()
                        st.toast(msg)
                        st.rerun()

            with sync_c3:
                st.markdown("**Rates & Indices**")
                rc1, rc2 = st.columns(2)
                with rc1:
                    if st.button("Sync Rates", key="dash_sync_rates2"):
                        with st.spinner(""):
                            _sync_rates(con)
                        st.toast("Rates synced")
                        st.rerun()
                with rc2:
                    if st.button("Sync Indices", key="dash_sync_idx2"):
                        with st.spinner(""):
                            _sync_indices(con)
                        st.toast("Indices synced")
                        st.rerun()

            # Recent sync runs
            runs_df = client.get_sync_runs(limit=5)
            if not runs_df.empty:
                runs_df.columns = ["ID", "Started", "Ended", "Mode", "Total", "OK", "Failed", "Rows"]
                st.dataframe(runs_df, use_container_width=True, hide_index=True, height=180)

    except Exception as e:
        st.error(f"Database error: {e}")
        st.info(f"Expected database at: {get_db_path()}")

    render_footer()
