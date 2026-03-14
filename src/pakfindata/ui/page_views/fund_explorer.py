"""Mutual Fund + ETF Explorer — fund directory, NAV charts, rankings, quant analytics."""

import math

import numpy as np
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pakfindata.ui.components.helpers import get_connection, render_ai_commentary, render_footer
from pakfindata.ui.themes import get_plotly_layout, get_theme

# ── Bloomberg palette ────────────────────────────────────────────────────────
_T = get_theme("bloomberg")
C_UP = _T.color_positive
C_DN = _T.color_negative
C_NEU = _T.color_neutral
C_BG = _T.bg_card
C_BG_DARK = _T.bg_main
C_BORDER = _T.border_primary
C_TEXT = _T.text_primary
C_MUTED = _T.text_muted
C_SEC = _T.text_secondary
C_ACCENT = _T.color_accent
C_WARN = _T.color_warning
C_INFO = _T.color_info
MONO = _T.font_mono


def _pct_color(v) -> str:
    """Return green/red/neutral color for a percentage value."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return C_NEU
    return C_UP if v > 0 else C_DN if v < 0 else C_NEU


def _pct_html(v, decimals: int = 1) -> str:
    """Render a percentage as colored HTML span."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f'<span style="color:{C_NEU}">---</span>'
    c = _pct_color(v)
    sign = "+" if v > 0 else ""
    return f'<span style="color:{c};font-weight:600">{sign}{v:.{decimals}f}%</span>'


def _section_label(text: str) -> str:
    """Bloomberg-style section header HTML."""
    return (
        f'<div style="color:{C_MUTED};font-size:11px;font-weight:600;'
        f'letter-spacing:0.08em;margin:16px 0 6px 0;text-transform:uppercase;">'
        f'{text}</div>'
    )


def _plotly_base() -> dict:
    """Get plotly layout with common conflicting keys removed."""
    layout = get_plotly_layout()
    for k in ("margin", "title", "xaxis", "yaxis", "legend"):
        layout.pop(k, None)
    return layout


def render_fund_explorer():
    """Mutual fund and ETF explorer page."""
    st.markdown("## Fund & ETF Explorer")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # Ensure fund_performance schema exists
    try:
        from pakfindata.db.repositories.fixed_income import init_fund_performance_schema
        init_fund_performance_schema(con)
    except Exception:
        pass

    tab_mf, tab_etf, tab_vps, tab_top, tab_compare, tab_risk, tab_factors, tab_llm, tab_sync = st.tabs([
        "Mutual Funds", "ETFs", "VPS Pension", "Top Performers",
        "Compare Funds", "Risk Analytics", "Factor Analysis", "LLM Analysis",
        "Sync & Tools",
    ])

    with tab_mf:
        try:
            _render_category_summary(con)
            _render_fund_directory(con)
        except Exception as e:
            st.error(f"Error loading mutual funds: {e}")

    with tab_etf:
        try:
            _render_etf_section(con)
        except Exception as e:
            st.error(f"Error loading ETFs: {e}")

    with tab_vps:
        try:
            _render_vps_section(con)
        except Exception as e:
            st.error(f"Error loading VPS data: {e}")

    with tab_top:
        try:
            _render_top_performers(con)
        except Exception as e:
            st.error(f"Error loading top performers: {e}")

    with tab_compare:
        try:
            _render_fund_comparison(con)
        except Exception as e:
            st.error(f"Error loading comparison: {e}")

    with tab_risk:
        try:
            _render_risk_analytics(con)
        except Exception as e:
            st.error(f"Error loading risk analytics: {e}")

    with tab_factors:
        try:
            _render_factor_analysis(con)
        except Exception as e:
            st.error(f"Error loading factor analysis: {e}")

    with tab_llm:
        try:
            _render_llm_analysis(con)
        except Exception as e:
            st.error(f"Error loading LLM analysis: {e}")

    with tab_sync:
        try:
            _render_sync_tools(con)
        except Exception as e:
            st.error(f"Error loading sync tools: {e}")

    # AI Commentary (after sync buttons so buttons always render)
    try:
        st.divider()
        render_ai_commentary(con, "FUNDS")
    except Exception:
        pass

    render_footer()


# ─────────────────────────────────────────────────────────────────────────────
# Sync & Tools Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_sync_tools(con):
    """Render sync buttons and scraper tools inside the Sync & Tools tab."""
    st.markdown(_section_label("SYNC FUND DATA"), unsafe_allow_html=True)

    # Show latest NAV date so user knows how fresh the data is
    try:
        latest_nav = con.execute(
            "SELECT MAX(date) FROM fund_nav_latest"
        ).fetchone()[0]
        if latest_nav:
            st.caption(f"Latest NAV date in DB: **{latest_nav}**")
    except Exception:
        pass

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        if st.button("Seed Funds", type="primary", key="fexp_seed_funds"):
            with st.spinner("Seeding mutual funds from MUFAP..."):
                try:
                    from pakfindata.sync_mufap import seed_mutual_funds
                    result = seed_mutual_funds()
                    st.success(
                        f"Seeded {result.get('inserted', 0)} funds "
                        f"(Failed: {result.get('failed', 0)})"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    with c2:
        if st.button("Daily NAV", type="primary", key="fexp_daily_nav"):
            with st.spinner("Fetching today's NAV for all funds (single request)..."):
                try:
                    from pakfindata.sync_mufap import sync_daily_nav
                    summary = sync_daily_nav()
                    from pakfindata.db.repositories.fixed_income import refresh_fund_nav_latest
                    refresh_fund_nav_latest(con)
                    _load_fund_directory.clear()
                    st.success(
                        f"Daily sync: {summary.ok} funds updated, "
                        f"{summary.rows_upserted} NAV rows "
                        f"({summary.no_data} unmatched)"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Daily sync failed: {e}")

    with c3:
        if st.button("Sync NAV History", key="fexp_sync_nav"):
            try:
                progress_bar = st.progress(0, text="Starting parallel NAV sync (10 workers)...")
                def _nav_progress(current, total, fund_id):
                    pct = current / total if total else 0
                    progress_bar.progress(pct, text=f"Synced {current}/{total}: {fund_id}")
                from pakfindata.sync_mufap import sync_mutual_funds_parallel
                summary = sync_mutual_funds_parallel(
                    source="AUTO", max_workers=10,
                    progress_callback=_nav_progress,
                )
                progress_bar.progress(1.0, text="Refreshing summary tables...")
                from pakfindata.db.repositories.fixed_income import refresh_fund_nav_latest
                refresh_fund_nav_latest(con)
                _load_fund_directory.clear()
                progress_bar.empty()
                st.success(
                    f"Synced {summary.ok} funds, "
                    f"{summary.rows_upserted} NAV records "
                    f"(skipped {summary.no_data} up-to-date)"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")

    with c4:
        if st.button("Sync Performance", key="fexp_sync_perf"):
            with st.spinner("Fetching MUFAP performance data (tab=1)..."):
                try:
                    from pakfindata.sync_mufap import sync_performance
                    result = sync_performance()
                    if result.get("status") == "ok":
                        from pakfindata.db.repositories.fixed_income import refresh_fund_performance_latest
                        refresh_fund_performance_latest(con)
                        _load_fund_directory.clear()
                        st.success(
                            f"Stored {result['funds_synced']} fund returns "
                            f"({result['categories']} categories, {result['date']})"
                        )
                    else:
                        st.error(result.get("error", "Unknown error"))
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    with c5:
        if st.button("Sync Expense", key="fexp_sync_expense"):
            with st.spinner("Fetching expense ratios (tab=5)..."):
                try:
                    from pakfindata.sync_mufap import sync_expense_ratios
                    result = sync_expense_ratios()
                    if result.get("status") == "ok":
                        st.success(f"Updated {result['updated']} funds")
                    else:
                        st.error(result.get("error", "Unknown error"))
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    with c6:
        if st.button("Sync ETFs", key="fexp_sync_etfs"):
            with st.spinner("Syncing ETF data..."):
                try:
                    from pakfindata.sources.etf_scraper import ETFScraper
                    result = ETFScraper().sync_all_etfs(con)
                    st.success(
                        f"ETFs: {result.get('ok', 0)} synced, "
                        f"{result.get('failed', 0)} failed"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    # NAV CSV Export via browser (Highcharts scraper)
    st.markdown("---")
    st.caption("**NAV CSV Export** — Browser-based Highcharts scraper (undetected-chromedriver + Xvfb)")
    nav_c1, nav_c2, nav_c3 = st.columns([2, 1, 1])
    with nav_c1:
        nav_fund_id = st.text_input(
            "Fund ID (blank = all funds)",
            value="", key="fexp_nav_csv_fund_id",
            placeholder="e.g. 12768",
        )
    with nav_c2:
        nav_no_xvfb = st.checkbox("No Xvfb", key="fexp_nav_no_xvfb",
                                   help="Skip Xvfb if WSLg/X11 display available")
    with nav_c3:
        if st.button("Export NAV CSV", type="primary", key="fexp_nav_csv_export"):
            try:
                from mufap_nav_downloader import run_download

                fid = int(nav_fund_id.strip()) if nav_fund_id.strip() else None
                label = f"fund {fid}" if fid else "all funds"

                progress_bar = st.progress(0, text=f"Starting NAV CSV export ({label})...")

                def _nav_csv_progress(current, total, info):
                    pct = current / total if total else 0
                    progress_bar.progress(pct, text=f"[{current}/{total}] {info}")

                summary = run_download(
                    fund_id=fid,
                    no_xvfb=nav_no_xvfb,
                    progress_callback=_nav_csv_progress,
                )
                progress_bar.progress(1.0, text="Done!")
                st.success(
                    f"NAV CSV Export: {summary['saved']} saved, "
                    f"{summary['skipped']} skipped, "
                    f"{summary['errors']} errors"
                )
                if summary.get("api_endpoints"):
                    with st.expander("Discovered API Endpoints"):
                        for ep in summary["api_endpoints"]:
                            st.code(f"{ep['method']} {ep['url']}", language=None)
            except ImportError:
                st.error(
                    "mufap_nav_downloader.py not found in project root. "
                    "Ensure it exists and undetected-chromedriver is installed."
                )
            except Exception as e:
                st.error(f"NAV CSV export failed: {e}")

    # MUFAP NAV History Scraper (DrissionPage)
    st.markdown("---")
    st.caption(
        "**NAV History Scraper** — DrissionPage bulk scraper "
        "(reads fund_masters.csv, saves per-fund CSVs to nav_history/)"
    )
    scraper_c1, scraper_c2 = st.columns([3, 1])
    with scraper_c1:
        st.markdown(
            "`/mnt/e/psxdata/mufapnav/nav_history/` — "
            "Skips funds already scraped (CSV exists)"
        )
    with scraper_c2:
        if st.button("Scrape NAV History", type="primary", key="fexp_nav_history_scrape"):
            try:
                import importlib
                import sys as _sys
                scraper_dir = "/mnt/e/psxdata/mufapnav"
                if scraper_dir not in _sys.path:
                    _sys.path.insert(0, scraper_dir)

                import mufap_nav_scraper
                importlib.reload(mufap_nav_scraper)

                progress_bar = st.progress(0, text="Starting NAV history scraper...")

                def _scraper_progress(current, total, info):
                    pct = current / total if total else 0
                    progress_bar.progress(pct, text=info)

                summary = mufap_nav_scraper.main(progress_callback=_scraper_progress)
                progress_bar.progress(1.0, text="Done!")
                st.success(
                    f"NAV History Scraper: {summary['scraped']} scraped, "
                    f"{summary['skipped']} skipped, "
                    f"{summary['failed']} failed "
                    f"(total: {summary['total']})"
                )
            except ImportError as e:
                st.error(
                    f"mufap_nav_scraper.py not found at /mnt/e/psxdata/mufapnav/. "
                    f"Ensure DrissionPage is installed. ({e})"
                )
            except Exception as e:
                st.error(f"NAV history scraper failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Category Summary Cards
# ─────────────────────────────────────────────────────────────────────────────


def _render_category_summary(con):
    """Bloomberg-style category group cards from fund_performance."""
    try:
        perf = pd.read_sql_query(
            "SELECT sector, category, return_ytd, return_30d FROM fund_performance_latest",
            con,
        )
    except Exception:
        return

    if perf.empty:
        return

    cat_lower = perf["category"].str.lower()

    equity_mask = cat_lower.str.contains("equity", na=False) & ~cat_lower.str.contains("vps", na=False)
    income_mask = (
        cat_lower.str.contains("income|money market|fixed rate", na=False)
        & ~cat_lower.str.contains("shariah|islamic|vps", na=False)
    )
    islamic_mask = cat_lower.str.contains("shariah|islamic", na=False) & ~cat_lower.str.contains("vps", na=False)
    vps_mask = cat_lower.str.contains("vps", na=False)

    groups = [
        ("EQUITY", equity_mask),
        ("INCOME / MM", income_mask),
        ("ISLAMIC", islamic_mask),
        ("VPS PENSION", vps_mask),
    ]

    cards_html = []
    for label, mask in groups:
        sub_ytd = perf.loc[mask, "return_ytd"].dropna()
        sub_1m = perf.loc[mask, "return_30d"].dropna()
        count = len(sub_ytd) or mask.sum()
        avg_ytd = sub_ytd.mean() if len(sub_ytd) > 0 else 0
        avg_1m = sub_1m.mean() if len(sub_1m) > 0 else 0
        ytd_c = C_UP if avg_ytd > 0 else C_DN if avg_ytd < 0 else C_NEU
        m1_c = C_UP if avg_1m > 0 else C_DN if avg_1m < 0 else C_NEU

        cards_html.append(f"""
        <div style="background:{C_BG};border:1px solid {C_BORDER};border-radius:2px;
                    padding:12px 16px;flex:1;min-width:160px;">
          <div style="font-size:10px;color:{C_MUTED};font-weight:600;letter-spacing:0.08em;">{label}</div>
          <div style="font-size:22px;font-weight:700;font-family:{MONO};color:{C_TEXT};margin:4px 0 2px;">
            {count}
          </div>
          <div style="font-size:11px;font-family:{MONO};display:flex;gap:12px;">
            <span>YTD <span style="color:{ytd_c};font-weight:600">{avg_ytd:+.1f}%</span></span>
            <span>1M <span style="color:{m1_c};font-weight:600">{avg_1m:+.1f}%</span></span>
          </div>
        </div>""")

    st.markdown(
        f'<div style="display:flex;gap:8px;margin-bottom:12px;">{"".join(cards_html)}</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mutual Funds Tab
# ─────────────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300, show_spinner="Loading fund data...")
def _load_fund_directory(_con) -> pd.DataFrame:
    """Load all funds with latest NAV and performance from summary tables."""
    import sqlite3 as _sqlite3

    db_path = _con.execute("PRAGMA database_list").fetchone()[2]
    con = _sqlite3.connect(db_path)
    con.row_factory = _sqlite3.Row

    df = pd.read_sql_query(
        """SELECT f.fund_id, f.symbol, f.fund_name, f.category, f.amc_name,
                  f.is_shariah, f.fund_type, f.expense_ratio,
                  nl.nav AS latest_nav, nl.date AS nav_date,
                  pl.return_30d, pl.return_90d, pl.return_ytd,
                  pl.return_365d, pl.rating
           FROM mutual_funds f
           LEFT JOIN fund_nav_latest nl ON nl.fund_id = f.fund_id
           LEFT JOIN fund_performance_latest pl ON pl.fund_name = f.fund_name
           ORDER BY f.fund_name""",
        con,
    )
    con.close()
    return df


def _render_fund_directory(con):
    """Bloomberg-style fund listing with color-coded returns."""
    col1, col2, col3, col4, col5 = st.columns(5)

    categories = con.execute(
        "SELECT DISTINCT category FROM mutual_funds ORDER BY category"
    ).fetchall()
    cat_list = ["All"] + [r["category"] for r in categories]

    amcs = con.execute(
        "SELECT DISTINCT amc_name FROM mutual_funds WHERE amc_name IS NOT NULL ORDER BY amc_name"
    ).fetchall()
    amc_list = ["All"] + [r["amc_name"] for r in amcs]

    with col1:
        sel_category = st.selectbox("Category", cat_list, key="fund_cat")
    with col2:
        sel_amc = st.selectbox("AMC", amc_list, key="fund_amc")
    with col3:
        sel_shariah = st.selectbox("Shariah", ["All", "Yes", "No"], key="fund_shariah")
    with col4:
        sel_type = st.selectbox("Type", ["All", "OPEN_END", "VPS", "DEDICATED", "EMPLOYER_PENSION", "ETF"], key="fund_type")
    with col5:
        search_term = st.text_input("Search", key="fund_search", placeholder="Fund name...")

    df = _load_fund_directory(con)

    if sel_category != "All":
        df = df[df["category"] == sel_category]
    if sel_amc != "All":
        df = df[df["amc_name"] == sel_amc]
    if sel_shariah == "Yes":
        df = df[df["is_shariah"] == 1]
    elif sel_shariah == "No":
        df = df[df["is_shariah"] == 0]
    if sel_type != "All":
        df = df[df["fund_type"] == sel_type]
    if search_term:
        df = df[df["fund_name"].str.contains(search_term, case=False, na=False)]

    if df.empty:
        st.info("No funds match filters")
        return

    st.markdown(
        f'<span style="font-size:11px;color:{C_MUTED};font-family:{MONO};">'
        f'{len(df)} FUNDS</span>',
        unsafe_allow_html=True,
    )

    display_cols = ["symbol", "fund_name", "category", "latest_nav",
                    "return_30d", "return_90d", "return_ytd", "return_365d"]
    display_df = df[[c for c in display_cols if c in df.columns]].copy()
    display_df = display_df.rename(columns={
        "symbol": "Symbol", "fund_name": "Fund", "category": "Category",
        "latest_nav": "NAV",
        "return_30d": "1M %", "return_90d": "3M %",
        "return_ytd": "YTD %", "return_365d": "1Y %",
    })

    # Color-code return columns
    ret_cols = [c for c in ["1M %", "3M %", "YTD %", "1Y %"] if c in display_df.columns]

    def _color_ret(val):
        if pd.isna(val):
            return ""
        return f"color: {C_UP}" if val > 0 else f"color: {C_DN}" if val < 0 else ""

    styled = display_df.style.map(_color_ret, subset=ret_cols).format(
        {c: "{:+.1f}" for c in ret_cols}, na_rep="---",
    ).format({"NAV": "{:.4f}"}, na_rep="---")

    st.dataframe(styled, use_container_width=True, hide_index=True, height=480)

    # Fund detail selector
    fund_options = {
        row["fund_id"]: f"{row['symbol']} — {row['fund_name']}"
        for _, row in df.iterrows()
    }
    selected_fund = st.selectbox(
        "Select fund for detail view",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options.get(x, x),
        key="fund_detail",
    )

    if selected_fund:
        _render_fund_detail(con, selected_fund)


def _render_fund_detail(con, fund_id):
    """Bloomberg-style fund detail: header cards, returns bar, NAV chart."""
    fund = con.execute(
        "SELECT * FROM mutual_funds WHERE fund_id = ?", (fund_id,)
    ).fetchone()
    if not fund:
        return

    # Convert sqlite3.Row to dict for safe .get() access
    fund_d = dict(fund)

    # ── Fund header ──
    shariah_badge = (
        f'<span style="background:{C_UP};color:#000;padding:1px 6px;border-radius:2px;'
        f'font-size:10px;font-weight:700;margin-left:8px;">SHARIAH</span>'
        if fund_d.get("is_shariah") else ""
    )
    st.markdown(
        f'<div style="font-family:{MONO};font-size:18px;font-weight:700;'
        f'color:{C_TEXT};margin-bottom:4px;">'
        f'{fund_d.get("fund_name", "")}{shariah_badge}</div>',
        unsafe_allow_html=True,
    )

    er_val = fund_d.get("expense_ratio")
    er = f"{er_val:.2f}%" if er_val else "---"

    cards = [
        ("CATEGORY", fund_d.get("category") or "---"),
        ("AMC", (fund_d.get("amc_name") or "---")[:30]),
        ("TYPE", fund_d.get("fund_type") or "---"),
        ("EXPENSE RATIO", er),
    ]
    card_html = '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">'
    for label, val in cards:
        card_html += (
            f'<div style="background:{C_BG};border:1px solid {C_BORDER};border-radius:3px;'
            f'padding:6px 12px;min-width:120px;flex:1;">'
            f'<div style="color:{C_MUTED};font-size:9px;font-weight:600;'
            f'letter-spacing:0.06em;text-transform:uppercase;">{label}</div>'
            f'<div style="color:{C_TEXT};font-family:{MONO};font-size:13px;'
            f'font-weight:600;margin-top:2px;">{val}</div>'
            f'</div>'
        )
    card_html += '</div>'
    st.markdown(card_html, unsafe_allow_html=True)

    # ── Sync full history button ──
    mufap_int_id = fund_d.get("mufap_int_id")
    if mufap_int_id and st.button("SYNC FULL HISTORY", key=f"sync_hist_{fund_id}"):
        with st.spinner("Fetching full NAV history from MUFAP..."):
            try:
                from pakfindata.sync_mufap import sync_fund_nav
                rows, error = sync_fund_nav(fund_id, incremental=False)
                if error:
                    st.error(error)
                else:
                    st.success(f"Synced {rows} NAV records")
                    st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")

    # ── Performance returns bar chart ──
    try:
        perf = con.execute(
            """SELECT return_ytd, return_mtd, return_1d, return_15d, return_30d,
                      return_90d, return_180d, return_270d, return_365d, return_2y, return_3y
               FROM fund_performance_latest
               WHERE fund_name = ?""",
            (fund_d["fund_name"],),
        ).fetchone()
        if perf:
            st.markdown(_section_label("RETURNS BY PERIOD"), unsafe_allow_html=True)
            labels = ["1D", "15D", "1M", "3M", "6M", "9M", "YTD", "1Y", "2Y", "3Y"]
            vals = [perf["return_1d"], perf["return_15d"], perf["return_30d"],
                    perf["return_90d"], perf["return_180d"], perf["return_270d"],
                    perf["return_ytd"], perf["return_365d"], perf["return_2y"], perf["return_3y"]]
            valid = [(l, v) for l, v in zip(labels, vals) if v is not None]
            if valid:
                bar_labels, bar_vals = zip(*valid)
                colors = [C_UP if v >= 0 else C_DN for v in bar_vals]
                layout = _plotly_base()
                fig = go.Figure(go.Bar(
                    x=list(bar_labels), y=list(bar_vals),
                    marker_color=colors,
                    text=[f"{v:+.1f}%" for v in bar_vals],
                    textposition="outside",
                    textfont=dict(family=MONO, size=10),
                ))
                fig.add_hline(y=0, line_color=C_NEU, line_width=0.5, opacity=0.4)
                fig.update_layout(
                    **layout, height=260,
                    margin=dict(l=10, r=10, t=10, b=30),
                    yaxis=dict(title=dict(text="RETURN %"), showgrid=True,
                               gridcolor=C_BORDER, zeroline=False),
                    xaxis=dict(tickfont=dict(family=MONO, size=10)),
                    bargap=0.3,
                )
                st.plotly_chart(fig, use_container_width=True)
    except Exception:
        pass

    # ── NAV history chart ──
    df = pd.read_sql_query(
        "SELECT date, nav FROM mutual_fund_nav WHERE fund_id = ? ORDER BY date",
        con, params=(fund_id,),
    )
    if df.empty:
        st.info("No NAV history available. Click 'SYNC FULL HISTORY' to fetch.")
        return

    nav_count = len(df)
    date_range = f"{df.iloc[0]['date']} to {df.iloc[-1]['date']}" if nav_count > 1 else df.iloc[0]["date"]
    latest_nav = df.iloc[-1]["nav"]

    st.markdown(_section_label("NAV HISTORY"), unsafe_allow_html=True)
    # NAV summary strip
    nav_strip = (
        f'<div style="display:flex;gap:16px;font-family:{MONO};font-size:12px;'
        f'color:{C_SEC};margin-bottom:6px;">'
        f'<span>LAST NAV: <b style="color:{C_TEXT}">Rs. {latest_nav:,.4f}</b></span>'
        f'<span>RECORDS: <b style="color:{C_TEXT}">{nav_count:,}</b></span>'
        f'<span>RANGE: <b style="color:{C_TEXT}">{date_range}</b></span>'
        f'</div>'
    )
    st.markdown(nav_strip, unsafe_allow_html=True)

    layout = _plotly_base()
    fig = go.Figure()
    nav_min = df["nav"].min()
    nav_max = df["nav"].max()
    pad = max((nav_max - nav_min) * 0.1, nav_min * 0.01)

    # Area fill for NAV line
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["nav"],
        mode="lines", name="NAV",
        line=dict(width=1.5, color=C_ACCENT),
        fill="tozeroy",
        fillcolor=f"rgba(0,122,255,0.08)",
        hovertemplate="<b>%{x}</b><br>NAV: Rs. %{y:,.4f}<extra></extra>",
    ))
    fig.update_layout(
        **layout, height=350,
        margin=dict(l=10, r=10, t=10, b=30),
        yaxis=dict(title=dict(text="NAV (PKR)"), side="right",
                   range=[nav_min - pad, nav_max + pad],
                   showgrid=True, gridcolor=C_BORDER),
        xaxis=dict(showgrid=False),
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Compare Funds Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_fund_comparison(con):
    """Bloomberg-style fund comparison: NAV overlay, metrics table, correlation heatmap."""
    st.markdown(_section_label("FUND COMPARISON"), unsafe_allow_html=True)

    funds = con.execute(
        """SELECT fund_id, symbol, fund_name, category FROM mutual_funds
           WHERE fund_id IN (SELECT DISTINCT fund_id FROM mutual_fund_nav)
           ORDER BY fund_name"""
    ).fetchall()

    if not funds:
        st.info("No funds with NAV history. Sync NAV data first.")
        return

    fund_options = {r["fund_id"]: f"{r['symbol']} -- {r['fund_name']}" for r in funds}

    selected = st.multiselect(
        "Select funds to compare (2-5)",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options.get(x, x),
        max_selections=5,
        key="compare_funds_select",
    )

    show_benchmark = st.checkbox("Overlay KSE-100 Index", value=True, key="compare_benchmark")

    if len(selected) < 2:
        st.info("Select at least 2 funds to compare")
        return

    # ── Normalized NAV chart ──
    layout = _plotly_base()
    fig = go.Figure()
    colors = [C_ACCENT, C_UP, C_WARN, C_INFO, "#9B59B6"]
    nav_frames = {}

    for i, fund_id in enumerate(selected):
        df = pd.read_sql_query(
            "SELECT date, nav FROM mutual_fund_nav WHERE fund_id = ? AND nav > 0 ORDER BY date",
            con, params=(fund_id,),
        )
        if df.empty:
            continue

        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["nav"])
        base = df.iloc[0]["nav"]
        if not base or base <= 0:
            continue
        df["normalized"] = (df["nav"] / base) * 100

        label = fund_options.get(fund_id, str(fund_id))
        short = label[:35] + "..." if len(label) > 35 else label

        fig.add_trace(go.Scatter(
            x=df["date"], y=df["normalized"],
            mode="lines", name=short,
            line=dict(width=1.5, color=colors[i % len(colors)]),
            hovertemplate=f"{short}<br>%{{x}}<br>NAV (idx): %{{y:.1f}}<extra></extra>",
        ))

        df["date"] = pd.to_datetime(df["date"])
        nav_frames[fund_id] = df.set_index("date")["nav"]

    if show_benchmark:
        try:
            idx_df = pd.read_sql_query(
                "SELECT index_date as date, value FROM psx_indices WHERE symbol = 'KSE100' AND value > 0 ORDER BY index_date",
                con,
            )
            if not idx_df.empty:
                base = idx_df.iloc[0]["value"]
                if base and base > 0:
                    idx_df["normalized"] = (idx_df["value"] / base) * 100
                    fig.add_trace(go.Scatter(
                        x=idx_df["date"], y=idx_df["normalized"],
                        mode="lines", name="KSE-100",
                        line=dict(width=1.5, color=C_NEU, dash="dash"),
                    ))
        except Exception:
            pass

    fig.add_hline(y=100, line_dash="dot", line_color=C_NEU, opacity=0.3)
    fig.update_layout(
        **layout, height=400,
        margin=dict(l=10, r=10, t=10, b=40),
        yaxis=dict(title=dict(text="Indexed (Base=100)"), side="right"),
        legend=dict(orientation="h", y=-0.12, font=dict(size=10)),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Side-by-side metrics table ──
    if nav_frames:
        st.markdown(_section_label("SIDE-BY-SIDE METRICS"), unsafe_allow_html=True)
        metrics_rows = []
        for fid in selected:
            nav = nav_frames.get(fid)
            if nav is None or len(nav) < 10:
                continue
            rets = nav.pct_change().dropna()
            total_ret = (nav.iloc[-1] / nav.iloc[0] - 1) * 100
            ret_1m = (nav.iloc[-1] / nav.iloc[-min(21, len(nav))] - 1) * 100 if len(nav) > 21 else None
            vol = rets.std() * (252 ** 0.5) * 100 if len(rets) > 20 else None
            dd = ((nav / nav.cummax()) - 1).min() * 100
            metrics_rows.append({
                "Fund": fund_options.get(fid, fid)[:40],
                "NAV": round(float(nav.iloc[-1]), 4),
                "Total Ret%": round(total_ret, 1),
                "1M Ret%": round(ret_1m, 1) if ret_1m else None,
                "Vol% (ann)": round(vol, 1) if vol else None,
                "Max DD%": round(dd, 1),
            })

        if metrics_rows:
            mdf = pd.DataFrame(metrics_rows)
            ret_cols = [c for c in ["Total Ret%", "1M Ret%", "Max DD%"] if c in mdf.columns]
            styled = mdf.style.map(
                lambda v: f"color:{C_UP}" if isinstance(v, (int, float)) and v > 0
                else f"color:{C_DN}" if isinstance(v, (int, float)) and v < 0 else "",
                subset=ret_cols,
            ).format(na_rep="---")
            st.dataframe(styled, use_container_width=True, hide_index=True)

    # ── Correlation heatmap ──
    if len(nav_frames) >= 2:
        st.markdown(_section_label("RETURN CORRELATION MATRIX"), unsafe_allow_html=True)

        # Align returns on common dates
        all_rets = pd.DataFrame({
            fund_options.get(fid, fid)[:20]: nav_frames[fid].pct_change().dropna()
            for fid in selected if fid in nav_frames
        }).dropna()

        if len(all_rets) > 10 and len(all_rets.columns) >= 2:
            corr = all_rets.corr()
            hm_layout = _plotly_base()
            fig_hm = go.Figure(go.Heatmap(
                z=corr.values,
                x=corr.columns.tolist(),
                y=corr.index.tolist(),
                colorscale=[[0, C_DN], [0.5, C_BG_DARK], [1, C_UP]],
                zmin=-1, zmax=1,
                text=np.round(corr.values, 2),
                texttemplate="%{text}",
                textfont=dict(size=11, color=C_TEXT),
                hovertemplate="(%{x}, %{y}): %{z:.3f}<extra></extra>",
            ))
            fig_hm.update_layout(
                **hm_layout, height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(tickangle=-30),
            )
            st.plotly_chart(fig_hm, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Risk Metrics Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_risk_metrics(con):
    """Bloomberg-style risk metrics: styled table + risk-return scatter."""
    st.markdown(_section_label("RISK METRICS"), unsafe_allow_html=True)
    st.caption("Sharpe ratio, max drawdown, and volatility for funds with 90+ NAV records")

    funds = con.execute(
        """SELECT f.fund_id, f.symbol, f.fund_name, f.category,
                  COUNT(n.date) as nav_count
           FROM mutual_funds f
           INNER JOIN mutual_fund_nav n ON f.fund_id = n.fund_id
           GROUP BY f.fund_id
           HAVING nav_count >= 90
           ORDER BY f.fund_name"""
    ).fetchall()

    if not funds:
        st.info("Need funds with at least 90 NAV records for risk analysis. Sync more NAV history.")
        return

    categories = sorted(set(r["category"] for r in funds if r["category"]))
    sel_cat = st.selectbox("Category", ["All"] + categories, key="risk_cat_filter")

    risk_data = []
    for fund in funds:
        if sel_cat != "All" and fund["category"] != sel_cat:
            continue

        navs = pd.read_sql_query(
            "SELECT date, nav FROM mutual_fund_nav WHERE fund_id = ? ORDER BY date",
            con, params=(fund["fund_id"],),
        )

        if len(navs) < 90:
            continue

        navs["nav"] = pd.to_numeric(navs["nav"], errors="coerce")
        navs = navs.dropna(subset=["nav"])
        navs["return"] = navs["nav"].pct_change()
        returns = navs["return"].dropna()

        if len(returns) < 30:
            continue

        total_return = (navs.iloc[-1]["nav"] / navs.iloc[0]["nav"]) - 1
        n_years = len(navs) / 252
        ann_return = ((1 + total_return) ** (1 / max(n_years, 0.1)) - 1) * 100

        vol = returns.std() * (252 ** 0.5) * 100

        rf = 12.0
        try:
            kb = con.execute(
                "SELECT offer FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1"
            ).fetchone()
            if kb:
                rf = kb["offer"]
        except Exception:
            pass
        sharpe = (ann_return - rf) / vol if vol > 0 else 0

        navs["cummax"] = navs["nav"].cummax()
        navs["drawdown"] = (navs["nav"] / navs["cummax"] - 1) * 100
        max_dd = navs["drawdown"].min()

        risk_data.append({
            "Fund": fund["fund_name"][:50],
            "Category": fund["category"],
            "NAVs": fund["nav_count"],
            "Ann. Ret %": round(ann_return, 2),
            "Vol %": round(vol, 2),
            "Sharpe": round(sharpe, 2),
            "Max DD %": round(max_dd, 2),
        })

    if not risk_data:
        st.info("No funds match the filter criteria")
        return

    df = pd.DataFrame(risk_data)
    df = df.sort_values("Sharpe", ascending=False)

    st.markdown(
        f'<div style="color:{C_SEC};font-family:{MONO};font-size:11px;margin-bottom:4px;">'
        f'{len(df)} FUNDS ANALYZED</div>',
        unsafe_allow_html=True,
    )

    # Color-coded styled table
    def _color_ret(v):
        if isinstance(v, (int, float)):
            return f"color: {C_UP}" if v > 0 else f"color: {C_DN}" if v < 0 else ""
        return ""

    def _color_sharpe(v):
        if isinstance(v, (int, float)):
            if v >= 1.0:
                return f"color: {C_UP}; font-weight: 600"
            elif v < 0:
                return f"color: {C_DN}"
        return ""

    styled = (
        df.style
        .map(_color_ret, subset=["Ann. Ret %", "Max DD %"])
        .map(_color_sharpe, subset=["Sharpe"])
        .format({"Ann. Ret %": "{:+.2f}", "Vol %": "{:.2f}", "Sharpe": "{:.2f}", "Max DD %": "{:.2f}"})
    )
    st.dataframe(styled, use_container_width=True, hide_index=True, height=420)

    # ── Risk-return scatter ──
    st.markdown(_section_label("RISK-RETURN SCATTER"), unsafe_allow_html=True)
    layout = _plotly_base()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["Vol %"], y=df["Ann. Ret %"],
        mode="markers",
        text=df["Fund"].str[:20],
        marker=dict(
            size=8,
            color=df["Sharpe"],
            colorscale=[[0, C_DN], [0.5, C_NEU], [1, C_UP]],
            colorbar=dict(title=dict(text="SHARPE"), thickness=12, len=0.6),
            showscale=True,
            line=dict(width=0.5, color=C_BORDER),
        ),
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Return: %{y:+.1f}%<br>"
            "Vol: %{x:.1f}%<br>"
            "Sharpe: %{marker.color:.2f}<extra></extra>"
        ),
    ))
    fig.update_layout(
        **layout, height=420,
        margin=dict(l=10, r=10, t=10, b=40),
        xaxis=dict(title=dict(text="VOLATILITY %"), showgrid=True, gridcolor=C_BORDER),
        yaxis=dict(title=dict(text="ANN. RETURN %"), showgrid=True, gridcolor=C_BORDER, side="right"),
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# ETFs Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_etf_section(con):
    """ETF listing with NAV vs market price."""
    st.markdown(_section_label("LISTED ETFS"), unsafe_allow_html=True)

    df = pd.read_sql_query(
        """SELECT m.symbol, m.name, m.amc, m.benchmark_index,
                  m.shariah_compliant,
                  n.date, n.nav, n.market_price, n.premium_discount, n.aum_millions
           FROM etf_master m
           LEFT JOIN etf_nav n ON m.symbol = n.symbol
             AND n.date = (SELECT MAX(date) FROM etf_nav WHERE symbol = m.symbol)
           ORDER BY m.symbol""",
        con,
    )

    if df.empty:
        st.info("No ETF data. Run `pfsync etf sync` to fetch.")
        return

    st.dataframe(
        df.rename(columns={
            "symbol": "Symbol", "name": "Name", "amc": "AMC",
            "nav": "NAV", "market_price": "Market Price",
            "premium_discount": "Prem/Disc %", "aum_millions": "AUM (M)",
            "date": "Date", "shariah_compliant": "Shariah",
        }),
        use_container_width=True, hide_index=True,
    )

    etfs_with_pd = df.dropna(subset=["premium_discount"])
    if not etfs_with_pd.empty:
        st.markdown(_section_label("PREMIUM / DISCOUNT"), unsafe_allow_html=True)
        layout = _plotly_base()
        fig = go.Figure()
        colors = [C_UP if v >= 0 else C_DN for v in etfs_with_pd["premium_discount"]]
        fig.add_trace(go.Bar(
            x=etfs_with_pd["symbol"], y=etfs_with_pd["premium_discount"],
            marker_color=colors,
            text=[f"{v:+.1f}%" for v in etfs_with_pd["premium_discount"]],
            textposition="outside",
            textfont=dict(family=MONO, size=10),
        ))
        fig.add_hline(y=0, line_dash="dot", line_color=C_NEU, opacity=0.4)
        fig.update_layout(
            **layout, height=280,
            margin=dict(l=10, r=10, t=10, b=30),
            yaxis=dict(title=dict(text="PREMIUM / DISCOUNT %"), showgrid=True, gridcolor=C_BORDER),
            xaxis=dict(tickfont=dict(family=MONO, size=10)),
        )
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# VPS Pension Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_vps_section(con):
    """VPS pension fund comparison."""
    st.markdown(_section_label("VPS PENSION FUNDS"), unsafe_allow_html=True)
    st.caption("Compare pension fund performance across AMCs and sub-fund categories")

    try:
        df = pd.read_sql_query(
            """SELECT fund_name, category, nav, rating,
                      return_ytd, return_30d, return_90d,
                      return_365d, return_2y, return_3y
               FROM fund_performance_latest
               WHERE (sector LIKE '%VPS%' OR category LIKE 'VPS%')
               ORDER BY category, return_ytd DESC""",
            con,
        )
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        st.info("No VPS performance data. Click **Sync Performance** to fetch from MUFAP.")
        return

    validity = con.execute(
        "SELECT MAX(validity_date) FROM fund_performance_latest WHERE sector LIKE '%VPS%' OR category LIKE 'VPS%'"
    ).fetchone()[0]
    st.caption(f"Data as of: **{validity}** | {len(df)} VPS funds")

    # AMC filter
    # Extract AMC name from fund name (first word typically)
    vps_categories = sorted(df["category"].dropna().unique())
    sel_vps_cat = st.selectbox("VPS Category", ["All"] + list(vps_categories), key="vps_cat_filter")

    if sel_vps_cat != "All":
        df = df[df["category"] == sel_vps_cat]

    # Summary by sub-fund type
    if len(vps_categories) > 1:
        summary = df.groupby("category").agg(
            funds=("fund_name", "count"),
            avg_ytd=("return_ytd", "mean"),
            best_ytd=("return_ytd", "max"),
            worst_ytd=("return_ytd", "min"),
        ).round(2).reset_index()

        st.dataframe(
            summary.rename(columns={
                "category": "Sub-Fund Type", "funds": "Funds",
                "avg_ytd": "Avg YTD %", "best_ytd": "Best YTD %", "worst_ytd": "Worst YTD %",
            }),
            use_container_width=True, hide_index=True,
        )

    # Full fund table with color-coded returns
    vps_disp = df.rename(columns={
        "fund_name": "Fund", "category": "Category", "nav": "NAV",
        "rating": "Rating", "return_ytd": "YTD %", "return_30d": "1M %",
        "return_90d": "3M %", "return_365d": "1Y %",
        "return_2y": "2Y %", "return_3y": "3Y %",
    })
    ret_cols = [c for c in vps_disp.columns if "%" in c]
    styled_vps = vps_disp.style.map(
        lambda v: f"color: {C_UP}" if isinstance(v, (int, float)) and v > 0
        else f"color: {C_DN}" if isinstance(v, (int, float)) and v < 0 else "",
        subset=ret_cols,
    )
    st.dataframe(styled_vps, use_container_width=True, hide_index=True, height=420)

    # Gold/Commodity sub-funds highlight
    gold_mask = df["category"].str.contains("Commodit|Gold", case=False, na=False)
    gold_df = df[gold_mask]
    if not gold_df.empty:
        st.markdown(_section_label("GOLD / COMMODITY SUB-FUNDS"), unsafe_allow_html=True)
        gold_disp = gold_df[["fund_name", "category", "nav", "return_ytd", "return_365d"]].rename(columns={
            "fund_name": "Fund", "category": "Category", "nav": "NAV",
            "return_ytd": "YTD %", "return_365d": "1Y %",
        })
        gold_ret_cols = [c for c in gold_disp.columns if "%" in c]
        styled_gold = gold_disp.style.map(
            lambda v: f"color: {C_UP}" if isinstance(v, (int, float)) and v > 0
            else f"color: {C_DN}" if isinstance(v, (int, float)) and v < 0 else "",
            subset=gold_ret_cols,
        )
        st.dataframe(styled_gold, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Top Performers Tab
# ─────────────────────────────────────────────────────────────────────────────


def _render_top_performers(con):
    """Bloomberg-style top performers with bar chart + color-coded table."""
    st.markdown(_section_label("TOP PERFORMERS"), unsafe_allow_html=True)

    try:
        perf_count = con.execute("SELECT COUNT(*) FROM fund_performance_latest").fetchone()[0]
    except Exception:
        perf_count = 0

    if perf_count == 0:
        _render_top_performers_fallback(con)
        return

    period_map = {
        "YTD": "return_ytd",
        "MTD": "return_mtd",
        "1M": "return_30d",
        "3M": "return_90d",
        "6M": "return_180d",
        "1Y": "return_365d",
        "2Y": "return_2y",
        "3Y": "return_3y",
    }

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        sel_period = st.radio("Period", list(period_map.keys()), horizontal=True, key="top_perf_period")
    with c2:
        try:
            cats = pd.read_sql_query(
                "SELECT DISTINCT category FROM fund_performance_latest ORDER BY category", con
            )["category"].tolist()
        except Exception:
            cats = []
        sel_cat = st.selectbox("Category", ["All"] + cats, key="top_perf_cat")
    with c3:
        top_n = st.number_input("Top N", min_value=5, max_value=50, value=20, key="top_n")

    period_col = period_map[sel_period]

    query = f"""
        SELECT fund_name, category, sector, nav, rating,
               {period_col} as return_pct
        FROM fund_performance_latest
        WHERE {period_col} IS NOT NULL
    """
    params: list = []
    if sel_cat != "All":
        query += " AND category = ?"
        params.append(sel_cat)
    query += f" ORDER BY {period_col} DESC LIMIT ?"
    params.append(top_n)

    df = pd.read_sql_query(query, con, params=params)

    if df.empty:
        st.info("No performance data for selected filters")
        return

    validity = con.execute(
        "SELECT MAX(validity_date) FROM fund_performance_latest"
    ).fetchone()[0]
    st.markdown(
        f'<span style="font-size:11px;color:{C_MUTED};font-family:{MONO};">'
        f'MUFAP OFFICIAL RETURNS AS OF {validity}</span>',
        unsafe_allow_html=True,
    )

    # ── Horizontal bar chart ──
    chart_df = df.head(15).copy()
    chart_df["short_name"] = chart_df["fund_name"].str[:30]
    chart_df = chart_df.sort_values("return_pct", ascending=True)
    bar_colors = [C_UP if v >= 0 else C_DN for v in chart_df["return_pct"]]

    layout = _plotly_base()
    fig = go.Figure(go.Bar(
        x=chart_df["return_pct"],
        y=chart_df["short_name"],
        orientation="h",
        marker=dict(color=bar_colors),
        text=[f"{v:+.1f}%" for v in chart_df["return_pct"]],
        textposition="outside",
        textfont=dict(size=10, color=C_TEXT),
        hovertemplate="%{y}<br>Return: %{x:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        **layout, height=max(280, len(chart_df) * 22),
        margin=dict(l=10, r=40, t=10, b=10),
        xaxis=dict(title=dict(text=f"Return ({sel_period}) %"), zeroline=True,
                   zerolinecolor=C_NEU, zerolinewidth=1),
        yaxis=dict(tickfont=dict(size=10)),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Styled table ──
    tbl = df.rename(columns={
        "fund_name": "Fund", "category": "Category",
        "nav": "NAV", "rating": "Rating",
        "return_pct": f"Ret ({sel_period})%",
    })
    ret_col = f"Ret ({sel_period})%"
    styled = tbl[["Fund", "Category", "NAV", ret_col]].style.map(
        lambda v: f"color:{C_UP}" if isinstance(v, (int, float)) and v > 0
        else f"color:{C_DN}" if isinstance(v, (int, float)) and v < 0 else "",
        subset=[ret_col],
    ).format({ret_col: "{:+.2f}", "NAV": "{:.4f}"}, na_rep="---")
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Rate benchmarks
    try:
        bm_cols = st.columns(4)
        pr = con.execute("SELECT policy_rate FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1").fetchone()
        kb = con.execute("SELECT bid, offer FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1").fetchone()
        tb = con.execute("SELECT cutoff_yield FROM tbill_auctions WHERE tenor='3M' ORDER BY auction_date DESC LIMIT 1").fetchone()
        tb6 = con.execute("SELECT cutoff_yield FROM tbill_auctions WHERE tenor='6M' ORDER BY auction_date DESC LIMIT 1").fetchone()
        with bm_cols[0]:
            st.metric("Policy Rate", f"{pr[0]:.1f}%" if pr else "—", help="SBP benchmark")
        with bm_cols[1]:
            if kb and kb[0] and kb[1]:
                st.metric("KIBOR 3M", f"{(kb[0]+kb[1])/2:.2f}%", help="Money market benchmark")
            else:
                st.metric("KIBOR 3M", "—")
        with bm_cols[2]:
            st.metric("T-Bill 3M", f"{tb[0]:.2f}%" if tb else "—", help="Risk-free 3M")
        with bm_cols[3]:
            st.metric("T-Bill 6M", f"{tb6[0]:.2f}%" if tb6 else "—", help="Risk-free 6M")
    except Exception:
        pass


def _render_top_performers_fallback(con):
    """Fallback: NAV-computed returns when fund_performance table is empty."""
    st.caption("Performance data not synced — showing NAV-computed returns. Click Sync Performance for MUFAP official returns.")

    period = st.radio("Period", ["30 days", "90 days", "365 days"], horizontal=True, key="fund_perf_period_fb")
    days = {"30 days": 30, "90 days": 90, "365 days": 365}[period]

    df = pd.read_sql_query(
        """
        SELECT f.fund_name, f.category, f.amc_name,
               nl.nav as latest_nav,
               ROUND((nl.nav - older.nav) / older.nav * 100, 2) as return_pct
        FROM mutual_funds f
        INNER JOIN mutual_fund_nav nl ON nl.fund_id = f.fund_id
            AND nl.date = (SELECT MAX(date) FROM mutual_fund_nav
                           WHERE fund_id = f.fund_id)
        INNER JOIN mutual_fund_nav older ON older.fund_id = f.fund_id
            AND older.date = (SELECT MIN(date) FROM mutual_fund_nav
                              WHERE fund_id = f.fund_id
                              AND date >= date('now', ? || ' days'))
        WHERE older.nav > 0
        ORDER BY return_pct DESC LIMIT 20
        """,
        con, params=(f"-{days}",),
    )

    if df.empty:
        st.info("Insufficient NAV history for return calculations")
        return

    st.dataframe(
        df.rename(columns={
            "fund_name": "Fund", "category": "Category",
            "amc_name": "AMC", "latest_nav": "NAV",
            "return_pct": f"Return ({period})",
        }),
        use_container_width=True, hide_index=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Standalone page render functions (called from app.py for individual pages)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Risk Analytics Tab (Quant Upgrade)
# ─────────────────────────────────────────────────────────────────────────────


def _get_fund_nav_series(con, fund_id: str) -> pd.Series:
    """Load fund NAV as a pd.Series with DatetimeIndex."""
    df = pd.read_sql_query(
        "SELECT date, nav FROM mutual_fund_nav WHERE fund_id = ? AND nav > 0 ORDER BY date",
        con, params=(fund_id,),
    )
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"])
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna(subset=["nav"])
    return df.set_index("date")["nav"]


@st.cache_data(ttl=600, show_spinner=False)
def _get_fund_list_with_nav_count(_con, min_navs: int = 90):
    """Get funds with sufficient NAV history (cached — heavy GROUP BY on 1.9M rows)."""
    import sqlite3 as _sq
    db_path = _con.execute("PRAGMA database_list").fetchone()[2]
    c2 = _sq.connect(db_path)
    c2.row_factory = _sq.Row
    rows = c2.execute(
        """SELECT f.fund_id, f.symbol, f.fund_name, f.category,
                  COUNT(n.date) as nav_count
           FROM mutual_funds f
           INNER JOIN mutual_fund_nav n ON f.fund_id = n.fund_id
           GROUP BY f.fund_id
           HAVING nav_count >= ?
           ORDER BY f.fund_name""",
        (min_navs,),
    ).fetchall()
    result = [dict(r) for r in rows]
    c2.close()
    return result


def _render_risk_analytics(con):
    """Comprehensive risk analytics dashboard using the quant engine."""
    from pakfindata.engine.fund_risk import (
        generate_fund_analytics,
        rolling_sharpe,
        maximum_drawdown,
    )
    from pakfindata.engine.benchmark import get_benchmark_nav

    st.markdown(_section_label("RISK ANALYTICS"), unsafe_allow_html=True)

    funds = _get_fund_list_with_nav_count(con, 90)
    if not funds:
        st.info("Need funds with at least 90 NAV records. Sync more NAV history.")
        return

    fund_options = {r["fund_id"]: f"{r['symbol']} -- {r['fund_name']}" for r in funds}

    sel_fund = st.selectbox(
        "Select Fund",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options.get(x, x),
        key="risk_fund_select",
    )

    nav = _get_fund_nav_series(con, sel_fund)
    if nav.empty or len(nav) < 30:
        st.warning("Insufficient NAV data for this fund.")
        return

    # Load benchmark
    benchmark = get_benchmark_nav(con, "KSE-100")
    bm = benchmark if not benchmark.empty else None

    with st.spinner("Computing analytics..."):
        analytics = generate_fund_analytics(
            fund_options.get(sel_fund, sel_fund), nav, bm,
        )

    if "error" in analytics:
        st.warning(f"Analysis error: {analytics['error']}")
        return

    risk = analytics.get("risk", {})
    rel = analytics.get("relative", {})

    # ── KPI Cards ──
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Sharpe (1Y)", f"{risk.get('sharpe_1y', 0):.2f}" if risk.get("sharpe_1y") else "---")
    c2.metric("Sortino (1Y)", f"{risk.get('sortino_1y', 0):.2f}" if risk.get("sortino_1y") else "---")
    c3.metric("Max Drawdown", f"{risk.get('max_drawdown', 0)*100:.1f}%" if risk.get("max_drawdown") else "---")
    c4.metric("VaR 95%", f"{risk.get('var_95_daily', 0)*100:.2f}%" if risk.get("var_95_daily") else "---")
    c5.metric("Beta", f"{rel.get('beta', 0):.2f}" if rel.get("beta") else "---")

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Volatility (1Y)", f"{risk.get('volatility_1y_ann', 0)*100:.1f}%" if risk.get("volatility_1y_ann") else "---")
    c7.metric("Alpha", f"{rel.get('alpha', 0)*100:.2f}%" if rel.get("alpha") else "---")
    c8.metric("Info Ratio", f"{rel.get('information_ratio', 0):.2f}" if rel.get("information_ratio") else "---")
    c9.metric("Up Capture", f"{rel.get('up_capture', 0):.0f}%" if rel.get("up_capture") else "---")
    c10.metric("Down Capture", f"{rel.get('down_capture', 0):.0f}%" if rel.get("down_capture") else "---")

    # ── Rolling Sharpe Chart ──
    sharpe_s = rolling_sharpe(nav, window=63)
    if not sharpe_s.empty:
        layout = _plotly_base()

        fig = go.Figure()
        pos = sharpe_s.copy()
        neg = sharpe_s.copy()
        pos[pos < 0] = 0
        neg[neg > 0] = 0

        fig.add_trace(go.Scatter(
            x=pos.index, y=pos.values, fill="tozeroy",
            fillcolor="rgba(0,200,83,0.15)", line=dict(color="#00C853", width=1),
            name="Sharpe > 0",
        ))
        fig.add_trace(go.Scatter(
            x=neg.index, y=neg.values, fill="tozeroy",
            fillcolor="rgba(255,82,82,0.15)", line=dict(color="#FF5252", width=1),
            name="Sharpe < 0",
        ))
        fig.add_hline(y=0, line_dash="dot", line_color="#6B7280")
        fig.update_layout(
            **layout,
            title=dict(text="Rolling Sharpe Ratio (63-day)", font=dict(size=12)),
            height=280, margin=dict(l=10, r=10, t=40, b=30),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Drawdown Chart ──
    dd = maximum_drawdown(nav)
    dd_series = dd.get("drawdown_series")
    if dd_series is not None and not dd_series.empty:
        layout = _plotly_base()

        fig = go.Figure(go.Scatter(
            x=dd_series.index, y=dd_series.values * 100,
            fill="tozeroy", fillcolor="rgba(255,82,82,0.2)",
            line=dict(color="#FF5252", width=1),
            hovertemplate="Date: %{x}<br>Drawdown: %{y:.2f}%<extra></extra>",
        ))
        fig.update_layout(
            **layout,
            title=dict(text="Underwater Chart (Drawdown %)", font=dict(size=12)),
            height=250, margin=dict(l=10, r=10, t=40, b=30),
            yaxis=dict(ticksuffix="%"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Risk Scatter (all funds in category) ──
    with st.expander("Category Risk-Return Scatter"):
        _render_risk_metrics(con)


# ─────────────────────────────────────────────────────────────────────────────
# Factor Analysis Tab (Quant Upgrade)
# ─────────────────────────────────────────────────────────────────────────────


def _render_factor_analysis(con):
    """Factor analysis: regression, MA signals, volatility regime."""
    from pakfindata.engine.fund_factors import (
        single_factor_regression,
        nav_ma_signals,
        rolling_volatility,
        volatility_regime,
    )
    from pakfindata.engine.benchmark import get_benchmark_nav

    st.markdown(_section_label("FACTOR ANALYSIS"), unsafe_allow_html=True)

    funds = _get_fund_list_with_nav_count(con, 90)
    if not funds:
        st.info("Need funds with at least 90 NAV records.")
        return

    fund_options = {r["fund_id"]: f"{r['symbol']} -- {r['fund_name']}" for r in funds}
    sel_fund = st.selectbox(
        "Select Fund",
        options=list(fund_options.keys()),
        format_func=lambda x: fund_options.get(x, x),
        key="factor_fund_select",
    )

    nav = _get_fund_nav_series(con, sel_fund)
    if nav.empty or len(nav) < 50:
        st.warning("Insufficient NAV data.")
        return

    benchmark = get_benchmark_nav(con, "KSE-100")

    # ── CAPM Regression ──
    if not benchmark.empty:
        st.markdown(_section_label("CAPM SINGLE-FACTOR REGRESSION"), unsafe_allow_html=True)
        reg = single_factor_regression(nav, benchmark)
        r1, r2, r3 = st.columns(3)
        r1.metric("Alpha (ann.)", f"{reg['alpha']*100:.2f}%" if reg.get("alpha") else "---")
        r2.metric("Beta", f"{reg['beta']:.3f}" if reg.get("beta") else "---")
        r3.metric("R-squared", f"{reg['r_squared']:.3f}" if reg.get("r_squared") else "---")

        p1, p2, p3 = st.columns(3)
        p1.metric("Alpha p-value", f"{reg['alpha_pvalue']:.4f}" if reg.get("alpha_pvalue") else "---")
        p2.metric("Beta p-value", f"{reg['beta_pvalue']:.6f}" if reg.get("beta_pvalue") else "---")
        p3.metric("Residual Std", f"{reg['residual_std']:.6f}" if reg.get("residual_std") else "---")

        sig_alpha = reg.get("alpha_pvalue", 1) < 0.05 if reg.get("alpha_pvalue") else False
        if sig_alpha and reg.get("alpha", 0) > 0:
            st.success("Statistically significant positive alpha -- fund generates excess returns")
        elif sig_alpha and reg.get("alpha", 0) < 0:
            st.warning("Statistically significant negative alpha -- fund underperforms benchmark")
    else:
        st.info("KSE-100 benchmark data not available for regression analysis.")

    st.markdown("---")

    # ── MA Crossover Chart ──
    st.markdown(_section_label("MOVING AVERAGE CROSSOVER (20/50)"), unsafe_allow_html=True)
    ma = nav_ma_signals(nav, fast=20, slow=50)
    if not ma.empty:
        layout = _plotly_base()

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ma.index, y=ma["nav"], mode="lines", name="NAV", line=dict(color="#EAECEF", width=1.5)))
        fig.add_trace(go.Scatter(x=ma.index, y=ma["ma_fast"], mode="lines", name="MA 20", line=dict(color="#FFB300", width=1)))
        fig.add_trace(go.Scatter(x=ma.index, y=ma["ma_slow"], mode="lines", name="MA 50", line=dict(color="#00B8D4", width=1)))

        # Mark crossover points
        crosses = ma[ma["signal"] != 0]
        for _, row in crosses.iterrows():
            color = "#00C853" if row["signal"] > 0 else "#FF5252"
            fig.add_vline(x=row.name, line_dash="dot", line_color=color, opacity=0.5)

        fig.update_layout(
            **layout, height=350,
            margin=dict(l=10, r=10, t=30, b=30),
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Current signal
        last_pos = int(ma["position"].iloc[-1])
        days = ma["days_since_cross"].iloc[-1]
        signal_text = "BULLISH (Golden Cross)" if last_pos == 1 else "BEARISH (Death Cross)"
        signal_color = "#00C853" if last_pos == 1 else "#FF5252"
        days_str = f"{int(days)} days ago" if not pd.isna(days) else "---"
        st.markdown(
            f'<div style="padding:8px 14px;background:rgba({"0,200,83" if last_pos==1 else "255,82,82"},0.1);'
            f'border-left:3px solid {signal_color};border-radius:2px;font-family:ui-monospace,monospace;">'
            f'<span style="color:{signal_color};font-weight:700;">{signal_text}</span> '
            f'<span style="color:#9AA4B2;font-size:12px;">Last crossover: {days_str}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ── Volatility ──
    st.markdown(_section_label("ROLLING VOLATILITY"), unsafe_allow_html=True)
    vol = rolling_volatility(nav, windows=[21, 63, 252])
    if not vol.empty:
        layout = _plotly_base()

        fig = go.Figure()
        if "vol_21d" in vol.columns:
            fig.add_trace(go.Scatter(x=vol.index, y=vol["vol_21d"]*100, name="21-day", line=dict(color="#FFB300", width=1)))
        if "vol_63d" in vol.columns:
            fig.add_trace(go.Scatter(x=vol.index, y=vol["vol_63d"]*100, name="63-day", line=dict(color="#00B8D4", width=1)))
        if "vol_252d" in vol.columns:
            fig.add_trace(go.Scatter(x=vol.index, y=vol["vol_252d"]*100, name="252-day", line=dict(color="#2F81F7", width=1)))
        fig.update_layout(
            **layout, height=280,
            margin=dict(l=10, r=10, t=30, b=30),
            yaxis=dict(ticksuffix="%", title=dict(text="Ann. Volatility")),
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Volatility regime
    regime = volatility_regime(nav)
    regime_colors = {"LOW": "#00C853", "NORMAL": "#2F81F7", "HIGH": "#FFB300", "EXTREME": "#FF5252"}
    rc = regime_colors.get(regime, "#6B7280")
    st.markdown(
        f'<span style="color:{rc};font-weight:700;font-family:ui-monospace,monospace;">'
        f'Volatility Regime: {regime}</span>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# LLM Analysis Tab (Quant Upgrade)
# ─────────────────────────────────────────────────────────────────────────────


def _render_llm_analysis(con):
    """LLM-ready structured output for fund analysis."""
    import json
    from pakfindata.engine.fund_risk import generate_fund_analytics
    from pakfindata.engine.fund_llm import fund_summary_for_llm, generate_market_context
    from pakfindata.engine.benchmark import get_benchmark_nav

    st.markdown(_section_label("LLM ANALYSIS"), unsafe_allow_html=True)
    st.caption("Generate structured JSON for AI/LLM consumption")

    mode = st.radio("Mode", ["Single Fund", "Market Context"], horizontal=True, key="llm_mode")

    if mode == "Single Fund":
        funds = _get_fund_list_with_nav_count(con, 90)
        if not funds:
            st.info("No funds with sufficient NAV history.")
            return

        fund_options = {r["fund_id"]: f"{r['symbol']} -- {r['fund_name']}" for r in funds}
        sel_fund = st.selectbox(
            "Select Fund",
            options=list(fund_options.keys()),
            format_func=lambda x: fund_options.get(x, x),
            key="llm_fund_select",
        )

        if st.button("Generate Analysis", key="llm_generate"):
            nav = _get_fund_nav_series(con, sel_fund)
            if nav.empty:
                st.warning("No NAV data.")
                return

            benchmark = get_benchmark_nav(con, "KSE-100")
            bm = benchmark if not benchmark.empty else None

            with st.spinner("Computing full analytics..."):
                analytics = generate_fund_analytics(
                    fund_options.get(sel_fund, sel_fund), nav, bm,
                )

                # Get fund metadata
                meta_row = con.execute(
                    """SELECT fund_name, amc_name, category, fund_type, benchmark,
                              launch_date, expense_ratio, is_shariah
                       FROM mutual_funds WHERE fund_id = ?""",
                    (sel_fund,),
                ).fetchone()
                metadata = dict(meta_row) if meta_row else {}

                summary = fund_summary_for_llm(
                    fund_name=metadata.get("fund_name", sel_fund),
                    analytics=analytics,
                    nav_history=nav,
                    metadata=metadata,
                )

            # Display hints as bullet points
            hints = summary.get("llm_narrative_hints", [])
            if hints:
                st.markdown(_section_label("KEY INSIGHTS"), unsafe_allow_html=True)
                for h in hints:
                    st.markdown(f"- {h}")

            # Display JSON
            st.markdown(_section_label("STRUCTURED OUTPUT"), unsafe_allow_html=True)
            st.code(json.dumps(summary, indent=2, default=str), language="json")

    else:
        # Market Context
        cat_filter = st.text_input("Category filter (optional)", key="llm_cat_filter")
        top_n = st.slider("Top/Bottom N", 5, 30, 10, key="llm_top_n")

        if st.button("Generate Market Context", key="llm_mkt_generate"):
            with st.spinner("Generating market context..."):
                ctx = generate_market_context(
                    con,
                    top_n=top_n,
                    category=cat_filter if cat_filter else None,
                )

            if "error" in ctx:
                st.error(f"Error: {ctx['error']}")
            else:
                summary = ctx.get("market_summary", {})
                st.metric("Total Active Funds", summary.get("total_active_funds", "---"))
                if summary.get("avg_return_1m"):
                    st.metric("Avg 1M Return", f"{summary['avg_return_1m']:.2f}%")

                st.markdown(_section_label("FULL CONTEXT JSON"), unsafe_allow_html=True)
                st.code(json.dumps(ctx, indent=2, default=str), language="json")


def render_vps_standalone():
    """VPS Pension as a standalone page."""
    st.markdown("## VPS Pension Funds")
    st.caption("Voluntary Pension Scheme — fund comparison by AMC and category")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        from pakfindata.db.repositories.fixed_income import init_fund_performance_schema
        init_fund_performance_schema(con)
    except Exception:
        pass

    _render_vps_section(con)
    render_footer()


def render_top_performers_standalone():
    """Top Performers as a standalone page."""
    st.markdown("## Top Performers")
    st.caption("Fund rankings by period and category")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        from pakfindata.db.repositories.fixed_income import init_fund_performance_schema
        init_fund_performance_schema(con)
    except Exception:
        pass

    _render_top_performers(con)
    render_footer()


def render_etfs_standalone():
    """ETFs as a standalone page."""
    st.markdown("## Listed ETFs")
    st.caption("Exchange-traded funds — NAV, market price, premium/discount")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    _render_etf_section(con)
    render_footer()
