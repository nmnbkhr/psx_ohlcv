"""Dashboard page -- quant-worthy market overview.

Phase 1.3 migration: market-data reads go through the API client
wrapper at ``pakfindata.ui.api.client`` (HTTP-only, no SQLite touched
on the read path). The sync expander block still uses safe_writer for
admin writes (Phase 1.5+ moves those to worker jobs).
"""

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.config import get_db_path
from pakfindata.db.repositories.market_summary import (
    get_latest_full_trading_day,
    refresh_eod_summary,
    summary_coverage,
)
from pakfindata.services import (
    is_service_running,
    is_eod_sync_running,
    read_eod_status,
    start_eod_sync_background,
    stop_eod_sync,
    is_announcements_service_running,
    read_announcements_status,
    start_announcements_service,
    stop_announcements_service,
)
from pakfindata.ui.api import client as api_client
from pakfindata.ui.charts import (
    make_market_breadth_chart,
    make_top_movers_chart,
)
from pakfindata.ui.components.helpers import (
    DATA_QUALITY_NOTICE,
    format_volume,
    get_connection,
    get_freshness_badge,
    render_data_info,
    render_data_warning,
    render_domain_freshness_bar,
    render_footer,
    render_market_status_badge,
)


# ── Sync helpers (write paths — use safe_writer internally) ───────────────


def _sync_market_data():
    # Phase 1.6.6: delegate to the shared ETL function. Caller uses the
    # rows_upserted count for status reporting.
    from pakfindata.etl.regular_market import sync_snapshot

    result = sync_snapshot()
    return result["rows_upserted"]


def _sync_indices():
    from pakfindata.db.safe_writer import safe_writer
    from pakfindata.sources.indices import fetch_indices_data, save_index_data

    indices_data = fetch_indices_data()
    with safe_writer() as con:
        return sum(1 for d in indices_data if save_index_data(con, d))


def _sync_rates():
    from pakfindata.db.safe_writer import safe_writer
    from pakfindata.sources.sbp_easydata import (
        sync_kibor_to_db,
        sync_policy_rate_to_db,
    )
    from pakfindata.sources.sbp_treasury import SBPTreasuryScraper

    scraper = SBPTreasuryScraper()
    with safe_writer() as con:
        rates = sync_kibor_to_db(con)
        treas = scraper.sync_treasury(con)
        sync_policy_rate_to_db(con)
    return rates, treas


# ── HTML building blocks ──────────────────────────────────────────────────


def _kse100_hero(kse: dict, breadth: dict | None) -> str:
    """KSE-100 hero banner HTML.

    ``kse`` is the /v1/market/kse100 payload (denormalized hero):
    value/change/change_pct/ytd_change_pct/week_52_*/advancers/decliners.
    ``breadth`` is /v1/eod/breadth (date/gainers/losers/unchanged/total).
    """
    value = kse.get("value", 0)
    change = kse.get("change", 0) or 0
    change_pct = kse.get("change_pct", 0) or 0
    ytd_pct = kse.get("ytd_change_pct", 0) or 0
    w52_high = kse.get("week_52_high")
    w52_low = kse.get("week_52_low")
    as_of = kse.get("as_of", "")

    if change > 0:
        color, arrow, sign = "#00C853", "&#9650;", "+"
    elif change < 0:
        color, arrow, sign = "#FF5252", "&#9660;", ""
    else:
        color, arrow, sign = "#6B7280", "&#9679;", ""

    ytd_color = "#00C853" if ytd_pct > 0 else "#FF5252" if ytd_pct < 0 else "#6B7280"
    w52h_str = f"{w52_high:,.2f}" if w52_high else "---"
    w52l_str = f"{w52_low:,.2f}" if w52_low else "---"

    g = (breadth or {}).get("gainers") or 0
    l = (breadth or {}).get("losers") or 0
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
          {as_of}
        </span>
      </div>
      <div style="display:flex;gap:24px;margin-top:10px;font-size:12px;font-family:ui-monospace,monospace;color:#9AA4B2;">
        <span>52W H <span style="color:#EAECEF">{w52h_str}</span></span>
        <span>52W L <span style="color:#EAECEF">{w52l_str}</span></span>
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


def _kse100_proxy_hero(breadth: dict) -> str:
    """Fallback hero when no index data — show breadth only."""
    if not breadth or not breadth.get("total"):
        return ""
    g = breadth.get("gainers") or 0
    l = breadth.get("losers") or 0
    avg = breadth.get("avg_change") or 0
    total = g + l or 1
    g_pct = g / total * 100
    l_pct = l / total * 100

    color = "#00C853" if avg > 0 else "#FF5252" if avg < 0 else "#6B7280"
    arrow = "&#9650;" if avg > 0 else "&#9660;" if avg < 0 else "&#9679;"
    sign = "+" if avg > 0 else ""
    vol = breadth.get("total_volume") or 0
    vol_str = f"{vol/1e6:.0f}M" if vol >= 1e6 else f"{vol:,}" if vol else "---"

    return f"""
    <div style="background:#12161C;border:1px solid #1E2329;border-radius:4px;padding:16px 20px;margin-bottom:8px;">
      <div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;">
        <span style="font-size:13px;color:#6B7280;font-weight:600;letter-spacing:0.05em;">KSE-100 (PROXY)</span>
        <span style="font-size:24px;font-weight:700;font-family:ui-monospace,monospace;color:{color};">
          {arrow} {sign}{avg:.2f}%
        </span>
        <span style="font-size:12px;color:#6B7280;">
          Avg across {breadth.get('total', 0)} stocks
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


def _rates_strip_html(rates: dict) -> str:
    """Compact rates strip from /v1/rates/strip — flat fields + FX list.

    ``rates`` shape (post-1.3 API):
      sbp_policy_rate, kibor_3m_bid, kibor_3m_offer, tbill_3m_cutoff,
      pkrv_10y_yield, fx=[{currency, selling, as_of}, …]
    """
    if not rates:
        return ""
    items: list[str] = []

    # Policy
    if rates.get("sbp_policy_rate") is not None:
        items.append(
            f'<span class="rs-label">SBP</span>'
            f'<span class="rs-val">{rates["sbp_policy_rate"]:.1f}%</span>'
        )

    # KIBOR 3M — expose bid+offer if both present, else whichever leg is.
    bid = rates.get("kibor_3m_bid")
    offer = rates.get("kibor_3m_offer")
    if bid is not None and offer is not None:
        items.append(
            f'<span class="rs-label">KIBOR 3M</span>'
            f'<span class="rs-val">{bid:.2f}/{offer:.2f}%</span>'
        )
    elif bid is not None or offer is not None:
        leg = bid if bid is not None else offer
        items.append(
            f'<span class="rs-label">KIBOR 3M</span>'
            f'<span class="rs-val">{leg:.2f}%</span>'
        )

    # T-Bill 3M
    if rates.get("tbill_3m_cutoff") is not None:
        items.append(
            f'<span class="rs-label">T-Bill 3M</span>'
            f'<span class="rs-val">{rates["tbill_3m_cutoff"]:.2f}%</span>'
        )

    # PKRV 10Y
    if rates.get("pkrv_10y_yield") is not None:
        items.append(
            f'<span class="rs-label">PKRV 10Y</span>'
            f'<span class="rs-val">{rates["pkrv_10y_yield"]:.2f}%</span>'
        )

    fx_list = rates.get("fx") or []
    if items and fx_list:
        items.append('<span class="rs-sep">|</span>')

    for row in fx_list[:3]:
        if row.get("selling") is None:
            continue
        items.append(
            f'<span class="rs-label">{row["currency"]}</span>'
            f'<span class="rs-val">{row["selling"]:,.2f}</span>'
        )

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


def _volume_leaders_html(rows: list[dict]) -> str:
    """Volume leaders list from /v1/market/volume-leaders."""
    if not rows:
        return ""
    body: list[str] = []
    for r in rows:
        vol = r.get("volume") or 0
        vol_str = f"{vol/1e6:.1f}M" if vol >= 1e6 else f"{vol:,.0f}"
        chg = r.get("change_pct") or 0
        c = "#00C853" if chg > 0 else "#FF5252" if chg < 0 else "#6B7280"
        body.append(
            f'<tr><td style="color:#EAECEF;font-weight:600">{r["symbol"]}</td>'
            f'<td style="text-align:right">{vol_str}</td>'
            f'<td style="text-align:right;color:{c}">{chg:+.2f}%</td></tr>'
        )
    tbody = "".join(body)
    return f"""
    <div style="font-size:12px;font-family:ui-monospace,monospace;">
      <div style="color:#6B7280;font-size:11px;font-weight:600;letter-spacing:0.05em;margin-bottom:6px;">
        VOLUME LEADERS
      </div>
      <table style="width:100%;border-collapse:collapse;color:#9AA4B2;">
        {tbody}
      </table>
    </div>"""


def _52w_html(extremes: dict | None) -> str:
    """52-week range extremes from /v1/market/52w-extremes.

    ``extremes`` shape: ``{near_high: [{symbol, pos_pct}, …], near_low: [...]}``.
    """
    if not extremes:
        return ""
    high = extremes.get("near_high") or []
    low = extremes.get("near_low") or []
    parts: list[str] = []
    if high:
        parts.append(
            '<div style="color:#6B7280;font-size:10px;margin-bottom:2px;">NEAR 52W HIGH</div>'
        )
        for r in high:
            pos = r.get("pos_pct") or 0
            parts.append(
                f'<div><span style="color:#00C853;font-weight:600">{r["symbol"]}</span>'
                f' <span style="color:#6B7280">{pos:.0f}%</span></div>'
            )
    if low:
        parts.append(
            '<div style="color:#6B7280;font-size:10px;margin-top:6px;margin-bottom:2px;">NEAR 52W LOW</div>'
        )
        for r in low:
            pos = r.get("pos_pct") or 0
            parts.append(
                f'<div><span style="color:#FF5252;font-weight:600">{r["symbol"]}</span>'
                f' <span style="color:#6B7280">{pos:.0f}%</span></div>'
            )
    if not parts:
        return ""
    return f"""
    <div style="font-size:12px;font-family:ui-monospace,monospace;">
      <div style="color:#6B7280;font-size:11px;font-weight:600;letter-spacing:0.05em;margin-bottom:6px;">
        52-WEEK RANGE
      </div>
      {"".join(parts)}
    </div>"""


# ── Main render ───────────────────────────────────────────────────────────


def render_dashboard():
    """Quant-worthy market dashboard — reads via the v1 API."""

    # Auto-refresh when sync service is running
    service_running, _ = is_service_running()
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=60000, limit=None, key="dashboard_autorefresh")

    # API-down banner — if the service is unreachable, render the banner
    # and bail out before any data fetches.
    if not api_client.render_api_status_banner_if_down():
        render_footer()
        return

    # ══════════════════════════════════════════════════════════════
    # ROW 0: HEADER — Title + Status + Refresh
    # ══════════════════════════════════════════════════════════════
    h1, h2, h3 = st.columns([3, 1, 0.5])
    with h1:
        st.markdown("### Market Dashboard")
    with h2:
        render_market_status_badge()
        days_old, latest_date = api_client.get_data_freshness_tuple("equity_eod")
        _, badge_text = get_freshness_badge(days_old)
        sync_dot = "ON" if service_running else "OFF"
        if latest_date:
            st.caption(f"Data: {badge_text} | Sync: {sync_dot}")
    with h3:
        refresh_all = st.button(
            "Refresh", type="primary", key="dash_refresh", width="stretch"
        )

    # ══════════════════════════════════════════════════════════════
    # ROW 1: KSE-100 HERO
    # ══════════════════════════════════════════════════════════════
    breadth = api_client.get_breadth() or {}
    kse100 = api_client.get_kse100_hero()

    if kse100:
        st.markdown(_kse100_hero(kse100, breadth), unsafe_allow_html=True)
    elif breadth and breadth.get("total"):
        st.markdown(_kse100_proxy_hero(breadth), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════
    # ROW 2: RATES STRIP
    # ══════════════════════════════════════════════════════════════
    rates_strip = api_client.get_rates_strip()
    if rates_strip:
        st.markdown(_rates_strip_html(rates_strip), unsafe_allow_html=True)

    # Stale data warning — derived from the freshness payload we already loaded.
    if days_old is not None and days_old > 3:
        render_data_warning(
            f"Equity EOD is {days_old} days old (latest: {latest_date}). "
            "Hit Refresh to update."
        )

    # ══════════════════════════════════════════════════════════════
    # REFRESH ALL HANDLER — sync path; still uses safe_writer
    # ══════════════════════════════════════════════════════════════
    if refresh_all:
        with st.status("Refreshing...", expanded=True) as status:
            errors = []
            for label, fn in [
                ("Market data", _sync_market_data),
                ("Indices", _sync_indices),
                ("Rates & Treasury", _sync_rates),
            ]:
                status.update(label=f"Fetching {label}...")
                try:
                    fn()
                    st.write(f"{label}: OK")
                except Exception as e:
                    errors.append(str(e))
                    st.write(f"{label}: FAILED - {e}")
            # Rebuild EOD summary for the latest trading day so the next page
            # render sees fresh breadth/movers immediately.
            try:
                from pakfindata.db.safe_writer import safe_writer

                with safe_writer() as wcon:
                    latest = get_latest_full_trading_day(wcon, min_symbols=1)
                    if latest:
                        status.update(label=f"Building summary for {latest}...")
                        refresh_eod_summary(wcon, latest)
                        st.write(f"EOD summary ({latest}): OK")
            except Exception as e:
                errors.append(str(e))
                st.write(f"EOD summary: FAILED - {e}")
            if errors:
                status.update(label=f"Done with {len(errors)} error(s)", state="error")
            else:
                status.update(label="All data refreshed", state="complete")
        # Invalidate every @st.cache_data wrapper so the page re-reads the
        # just-updated data instead of the stale 60s cache.
        st.cache_data.clear()
        st.rerun()

    # ══════════════════════════════════════════════════════════════
    # ROW 4: MARKET VIZ — Breadth | Gainers | Losers
    # ══════════════════════════════════════════════════════════════
    try:
        market_analytics = api_client.get_market_analytics()
        if market_analytics:
            gainers = market_analytics.get("gainers_count", 0)
            losers = market_analytics.get("losers_count", 0)
            unchanged = market_analytics.get("unchanged_count", 0)

            col1, col2, col3 = st.columns([1, 1, 1])

            with col1:
                fig = make_market_breadth_chart(
                    gainers=gainers,
                    losers=losers,
                    unchanged=unchanged,
                    height=280,
                )
                st.plotly_chart(fig, width="stretch")

            with col2:
                top_g = api_client.get_top_gainers(5) or []
                if top_g:
                    df_g = pd.DataFrame(top_g)[["symbol", "change_pct"]]
                    fig = make_top_movers_chart(
                        df_g, title="Top Gainers", chart_type="gainers", height=280
                    )
                    st.plotly_chart(fig, width="stretch")

            with col3:
                top_l = api_client.get_top_losers(5) or []
                if top_l:
                    df_l = pd.DataFrame(top_l)[["symbol", "change_pct"]]
                    fig = make_top_movers_chart(
                        df_l, title="Top Losers", chart_type="losers", height=280
                    )
                    st.plotly_chart(fig, width="stretch")
    except Exception:
        pass

    # ══════════════════════════════════════════════════════════════
    # ROW 5: DATA GRID — Volume Leaders | 52W Range | Sector
    # ══════════════════════════════════════════════════════════════
    c1, c2, c3 = st.columns([1, 1, 2])

    with c1:
        vol_rows = api_client.get_volume_leaders(5) or []
        st.markdown(_volume_leaders_html(vol_rows), unsafe_allow_html=True)

    with c2:
        extremes = api_client.get_52w_extremes(3)
        st.markdown(_52w_html(extremes), unsafe_allow_html=True)

    with c3:
        try:
            sector_rows = api_client.get_sector_leaderboard() or []
            if sector_rows:
                st.markdown(
                    '<div style="color:#6B7280;font-size:11px;font-weight:600;'
                    'letter-spacing:0.05em;margin-bottom:4px;">SECTOR PERFORMANCE</div>',
                    unsafe_allow_html=True,
                )
                sector_df = pd.DataFrame(sector_rows)
                # Columns shipped by /v1/market/sector-leaderboard:
                # sector, stocks, avg_chg, total_vol, up, down
                display_cols = [
                    c for c in ["sector", "stocks", "avg_chg", "total_vol", "up", "down"]
                    if c in sector_df.columns
                ]
                st.dataframe(
                    sector_df[display_cols].head(8),
                    width="stretch",
                    hide_index=True,
                    height=260,
                    column_config={
                        "sector": st.column_config.TextColumn("Sector", width="medium"),
                        "stocks": st.column_config.NumberColumn("Stk", format="%d", width="small"),
                        "avg_chg": st.column_config.NumberColumn("Chg%", format="%.2f", width="small"),
                        "total_vol": st.column_config.NumberColumn("Volume", format="%,.0f"),
                        "up": st.column_config.NumberColumn("Up", format="%d", width="small"),
                        "down": st.column_config.NumberColumn("Dn", format="%d", width="small"),
                    },
                )
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════
    # ROW 6: SYNC CONTROLS — still direct-DB via safe_writer.
    # These migrate to worker jobs in Phase 1.5+. Hard Rule 7-8.
    # ══════════════════════════════════════════════════════════════
    with st.expander("Sync & Data Management", expanded=False):
        st.caption(
            "🕒 Automatic refresh runs nightly at 03:45 PKT via cron "
            "(weekends + PSX holidays skipped). The buttons below are "
            "for manual backfill, troubleshooting, or intraday refreshes."
        )
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
                    # Phase 1.6.1: sidebar feature flag picks the path.
                    # Both routes converge on etl.rates.sync_bundle().
                    if api_client.use_worker_sync():
                        api_client.run_job_with_progress(
                            "sync_rates_bundle",
                            spinner_text="syncing rates bundle (KIBOR + treasury + policy)",
                        )
                    else:
                        with st.spinner("Syncing rates..."):
                            from pakfindata.db.safe_writer import SafeWriterBusyError
                            from pakfindata.etl.rates import sync_bundle

                            try:
                                result = sync_bundle()
                                st.cache_data.clear()
                                st.toast(
                                    f"Rates synced: KIBOR {result['kibor_rows']} rows, "
                                    f"T-Bills {result['tbills_ok']}, "
                                    f"PIBs {result['pibs_ok']}"
                                )
                            except SafeWriterBusyError:
                                st.error("Another sync is running. Wait a moment and retry.")
                            except Exception as e:
                                # sync_bundle() already recorded the catalog failures.
                                st.error(f"Rates sync failed: {e}")
                    st.rerun()
            with rc2:
                if st.button("Sync Indices", key="dash_sync_idx2"):
                    # Phase 1.5: sidebar feature flag picks the path.
                    # ON (default): enqueue a worker job and poll until
                    #               terminal — progress shows inline.
                    # OFF (fallback): run inline via the shared
                    #               etl.indices.sync() function.
                    # Both paths converge on the same ETL code; only the
                    # execution context (Streamlit thread vs worker
                    # process) differs.
                    if api_client.use_worker_sync():
                        api_client.run_job_with_progress(
                            "sync_indices",
                            spinner_text="syncing 18 PSX indices",
                        )
                    else:
                        with st.spinner("Syncing indices..."):
                            from pakfindata.db.safe_writer import SafeWriterBusyError
                            from pakfindata.etl.indices import sync as sync_indices

                            try:
                                result = sync_indices()
                                st.cache_data.clear()
                                st.toast(
                                    f"Synced {result['indices_count']} indices "
                                    f"(latest: {result.get('latest_date') or '—'})"
                                )
                            except SafeWriterBusyError:
                                st.error("Another sync is running. Wait a moment and retry.")
                            except Exception as e:
                                # sync() already recorded the catalog failure.
                                st.error(f"Sync failed: {e}")
                    st.rerun()

        # ── EOD summary tables (admin read of catalog coverage) ──
        st.markdown("**EOD Summary Tables**")
        # Admin read — uses the read-side connection helper; not part of
        # the migrated market-data path. summary_coverage and
        # get_latest_full_trading_day take a con argument.
        try:
            admin_con = get_connection()
            cov = summary_coverage(admin_con)
            latest_for_sum = get_latest_full_trading_day(admin_con, min_symbols=1)
        except Exception:
            cov, latest_for_sum = {}, None
        mkt = cov.get("eod_market_summary", {})
        raw = cov.get("eod_ohlcv", {})
        built = mkt.get("rows") or 0
        total_dates = raw.get("dates") or 0
        missing = max(total_dates - built, 0)
        st.caption(
            f"**Source data** (`eod_ohlcv`): {total_dates:,} dates "
            f"({raw.get('min_date') or '—'} → {raw.get('max_date') or '—'})"
        )
        st.caption(
            f"**Summary built**: {built:,} dates "
            f"({mkt.get('min_date') or '—'} → {mkt.get('max_date') or '—'})"
            f"  •  {missing:,} dates not yet built"
        )
        if latest_for_sum:
            st.caption(f"**Rebuild Today** → `{latest_for_sum}` (latest date in source)")

        s1, s2, s3 = st.columns(3)
        with s1:
            if st.button("Rebuild Today", key="dash_sum_today",
                         help="Refresh summaries for the latest trading day only."):
                with st.spinner("Rebuilding today's summary…"):
                    from pakfindata.db.catalog import (
                        record_catalog_failure,
                        update_catalog_from_table,
                    )
                    from pakfindata.db.repositories.market_summary import (
                        init_eod_summary_schema,
                    )
                    from pakfindata.db.safe_writer import (
                        SafeWriterBusyError,
                        safe_writer,
                    )
                    try:
                        with safe_writer() as wcon:
                            d = get_latest_full_trading_day(wcon, min_symbols=1)
                            if d:
                                init_eod_summary_schema(wcon)
                                n = refresh_eod_summary(wcon, d)
                                update_catalog_from_table(wcon, "eod_market_summary", source="computed")
                                update_catalog_from_table(wcon, "eod_sector_summary", source="computed")
                                update_catalog_from_table(wcon, "eod_symbol_summary", source="computed")
                        if d:
                            st.cache_data.clear()
                            st.toast(f"Rebuilt {d}: {n} symbol rows")
                        else:
                            st.warning("No EOD data found.")
                    except SafeWriterBusyError:
                        st.error("Another sync is running. Wait a moment and retry.")
                    except Exception as e:
                        st.error(f"Rebuild failed: {e}")
                        for ds in ("eod_market_summary", "eod_sector_summary", "eod_symbol_summary"):
                            record_catalog_failure(ds, source="computed", error=e)
                    st.rerun()
        with s2:
            if st.button("Rebuild Missing", key="dash_sum_missing",
                         help="Populate summaries only for dates not yet built."):
                with st.spinner("Populating missing dates…"):
                    from pakfindata.db.repositories.market_summary import (
                        refresh_eod_summary_bulk,
                    )
                    try:
                        r = refresh_eod_summary_bulk(only_missing=True, batch_size=50)
                        st.cache_data.clear()
                        st.toast(
                            f"Processed {r['dates_processed']}/{r['dates_considered']} "
                            f"dates in {r['batches']} batches, {r['rows_written']:,} rows"
                        )
                    except Exception as e:
                        st.error(f"Rebuild failed: {e}")
                    st.rerun()
        with s3:
            if st.button("Rebuild All", key="dash_sum_all",
                         help="Full rebuild of every date in eod_ohlcv. Slow."):
                with st.spinner("Rebuilding all summaries…"):
                    from pakfindata.db.repositories.market_summary import (
                        refresh_eod_summary_bulk,
                    )
                    try:
                        r = refresh_eod_summary_bulk(only_missing=False, batch_size=50)
                        st.cache_data.clear()
                        st.toast(
                            f"Rebuilt {r['dates_processed']} dates in {r['batches']} batches, "
                            f"{r['rows_written']:,} rows"
                        )
                    except Exception as e:
                        st.error(f"Rebuild failed: {e}")
                    st.rerun()

        # Recent sync runs — now via /v1/sync/runs
        runs = api_client.get_sync_runs(5) or []
        if runs:
            runs_df = pd.DataFrame(runs)
            runs_df = runs_df.rename(
                columns={
                    "run_id": "ID",
                    "started_at": "Started",
                    "ended_at": "Ended",
                    "mode": "Mode",
                    "symbols_total": "Total",
                    "symbols_ok": "OK",
                    "symbols_failed": "Failed",
                    "rows_upserted": "Rows",
                }
            )
            st.dataframe(runs_df, width="stretch", hide_index=True, height=180)

    with st.expander("Database Health (Advanced)", expanded=False):
        st.caption(
            "Use before sleep / shutdown / backup to flush WAL into the main DB "
            "file. Prevents the May 9 2026 corruption pattern (long-lived WAL "
            "across overnight idle)."
        )
        if st.button("Force WAL Checkpoint", key="dash_wal_checkpoint"):
            from pakfindata.db.safe_writer import checkpoint_wal
            try:
                busy, log_frames, ckpt = checkpoint_wal()
                if busy == 0:
                    st.success(
                        f"WAL fully checkpointed ({ckpt}/{log_frames} frames flushed)."
                    )
                else:
                    st.warning(
                        f"Partial checkpoint — {busy} pending. Another writer active."
                    )
            except Exception as e:
                st.error(f"Checkpoint failed: {e}")

    render_footer()
