"""Company deep analytics page."""

import pandas as pd
import streamlit as st
import time

from psx_ohlcv.api_client import get_client
from psx_ohlcv.sources.deep_scraper import deep_scrape_symbol
from psx_ohlcv.ui.session_tracker import (
    track_button_click,
    track_page_visit,
    track_refresh,
    track_symbol_search,
)
from psx_ohlcv.ui.components.helpers import (
    format_volume,
    render_footer,
)


def render_company_deep():
    """Company Analytics page for deep-dive into individual stocks."""

    client = get_client()
    con = client.connection  # For session tracking and raw SQL
    track_page_visit(con, "Company Analytics")

    # =================================================================
    # SEARCH BAR - Bloomberg Terminal Style
    # =================================================================
    all_symbols = client.get_symbols()
    symbols_with_profiles = client.get_symbols_with_profiles()
    default_symbol = st.session_state.get("company_symbol", "")

    # Compact search bar
    search_col1, search_col2, search_col3 = st.columns([4, 1, 1])

    with search_col1:
        symbol = st.text_input(
            "🔍 Search Symbol",
            value=default_symbol,
            placeholder="Enter symbol (e.g., OGDC, HBL, ENGRO)",
            label_visibility="collapsed",
        ).strip().upper()

    with search_col2:
        refresh_data = st.button("🔄 Refresh", type="primary", use_container_width=True)

    with search_col3:
        st.caption(f"{len(symbols_with_profiles)} companies")

    # Symbol suggestions
    if symbol and len(symbol) >= 1:
        matching = [s for s in all_symbols if s.startswith(symbol)][:8]
        if matching and symbol not in matching:
            suggestion_html = " ".join([
                f'<span style="background: rgba(33,150,243,0.1); padding: 2px 8px; '
                f'border-radius: 4px; margin: 2px; font-size: 12px;">{s}</span>'
                for s in matching
            ])
            st.markdown(f"Suggestions: {suggestion_html}", unsafe_allow_html=True)

    if not symbol:
        # Welcome screen when no symbol
        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; padding: 60px 20px;">
            <h2 style="color: #2196F3;">🏢 Company Analytics</h2>
            <p style="color: #888; font-size: 16px;">
                Enter a symbol above to view comprehensive company data<br>
                including quotes, trading sessions, announcements, and financials.
            </p>
        </div>
        """, unsafe_allow_html=True)
        render_footer()
        return

    # Track search
    if st.session_state.get("last_searched_symbol") != symbol:
        track_symbol_search(con, symbol, "Company Analytics")
        st.session_state.last_searched_symbol = symbol

    # Handle refresh
    if refresh_data:
        track_button_click(con, "Refresh Data", "Company Analytics", symbol)
        with st.spinner(f"Fetching data for {symbol}..."):
            try:
                from psx_ohlcv.sources.deep_scraper import deep_scrape_symbol
                result = deep_scrape_symbol(con, symbol, save_raw_html=False)
                if result.get("success"):
                    parts = []
                    if result.get("snapshot_saved"):
                        parts.append("Quote")
                    if result.get("trading_sessions_saved", 0) > 0:
                        parts.append(f"{result['trading_sessions_saved']} markets")
                    if result.get("announcements_saved", 0) > 0:
                        parts.append(f"{result['announcements_saved']} announcements")
                    track_refresh(con, "deep_scrape", symbol, "Company Analytics", True, {})
                    st.success(f"✓ Updated: {', '.join(parts)}" if parts else "✓ Refreshed")
                    st.rerun()
                else:
                    st.error(f"Failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("---")

    # =================================================================
    # FETCH DATA
    # =================================================================
    unified_data = client.get_company_overview(symbol)
    signals = client.get_company_latest_signals(symbol)
    quote_stats = client.get_company_quote_stats(symbol)

    if not unified_data:
        st.markdown(f"""
        <div style="text-align: center; padding: 40px; background: rgba(255,193,7,0.1);
                    border: 1px solid rgba(255,193,7,0.3); border-radius: 8px;">
            <h3>No data for {symbol}</h3>
            <p>Click <b>Refresh</b> to fetch data from PSX</p>
        </div>
        """, unsafe_allow_html=True)
        render_footer()
        return

    data = unified_data

    # =================================================================
    # COMPANY HEADER - Prominent Display
    # =================================================================
    company_name = data.get("company_name", symbol)
    sector_name = data.get("sector_name", "")
    snapshot_date = data.get("snapshot_date", "")
    price = data.get("price")
    change = data.get("change") or 0
    change_pct = data.get("change_pct") or 0

    # Determine color based on change
    price_color = "#00C853" if change_pct >= 0 else "#FF1744"
    change_sign = "+" if change_pct >= 0 else ""
    arrow = "▲" if change_pct > 0 else "▼" if change_pct < 0 else "●"

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: flex-start;
                padding: 16px 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
        <div>
            <div style="font-size: 28px; font-weight: 700;">{symbol}</div>
            <div style="font-size: 14px; color: #888;">{company_name}</div>
            <div style="font-size: 12px; color: #666; margin-top: 4px;">
                {sector_name} • Data as of {snapshot_date}
            </div>
        </div>
        <div style="text-align: right;">
            <div style="font-size: 32px; font-weight: 700; font-family: monospace;">
                Rs. {price:,.2f}
            </div>
            <div style="font-size: 18px; color: {price_color}; font-family: monospace;">
                {arrow} {change_sign}{change:.2f} ({change_sign}{change_pct:.2f}%)
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")  # Spacing

    # Quick Stats Row
    qs_col1, qs_col2, qs_col3, qs_col4, qs_col5, qs_col6 = st.columns(6)

    with qs_col1:
        vol = data.get("volume") or 0
        st.metric("Volume", format_volume(vol))

    with qs_col2:
        ldcp = data.get("ldcp")
        st.metric("LDCP", f"Rs. {ldcp:,.2f}" if ldcp else "N/A")

    with qs_col3:
        pe = data.get("pe_ratio")
        # Get sector P/E for comparison
        sector_code = data.get("sector_code") or data.get("sector")
        sector_pe_delta = None
        if pe and sector_code:
            try:
                sector_pe_result = con.execute("""
                    SELECT AVG(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as avg_pe
                    FROM company_snapshots cs
                    JOIN symbols s ON cs.symbol = s.symbol
                    WHERE s.sector = ?
                    AND json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') > 0
                    AND cs.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs2 WHERE cs2.symbol = cs.symbol)
                """, (sector_code,)).fetchone()
                if sector_pe_result and sector_pe_result["avg_pe"]:
                    sector_avg_pe = sector_pe_result["avg_pe"]
                    pe_diff = pe - sector_avg_pe
                    # Negative delta is good (cheaper than sector)
                    sector_pe_delta = f"{pe_diff:+.1f} vs sector"
            except Exception:
                pass
        st.metric("P/E Ratio", f"{pe:.2f}" if pe else "N/A",
                  delta=sector_pe_delta, delta_color="inverse" if sector_pe_delta else "off",
                  help="Price-to-Earnings ratio. Lower may indicate undervaluation.")

    with qs_col4:
        mc = data.get("market_cap")
        if mc:
            mc_str = f"Rs. {mc/1e9:.1f}B" if mc >= 1e9 else f"Rs. {mc/1e6:.1f}M"
        else:
            mc_str = "N/A"
        st.metric("Market Cap", mc_str)

    with qs_col5:
        ytd = data.get("ytd_change_pct")
        if ytd:
            st.metric("YTD", f"{ytd:+.1f}%", delta=f"{ytd:+.1f}%")
        else:
            st.metric("YTD", "N/A")

    with qs_col6:
        y1 = data.get("one_year_change_pct")
        if y1:
            st.metric("1Y Change", f"{y1:+.1f}%", delta=f"{y1:+.1f}%")
        else:
            st.metric("1Y Change", "N/A")

    # =================================================================
    # VALUATION COMPARISON - Sector Context
    # =================================================================
    pe = data.get("pe_ratio")
    sector_code = data.get("sector_code") or data.get("sector")

    if pe and sector_code:
        try:
            # Get sector valuation metrics
            sector_valuation = con.execute("""
                SELECT
                    COUNT(*) as sector_count,
                    AVG(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as avg_pe,
                    MIN(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as min_pe,
                    MAX(CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL)) as max_pe
                FROM company_snapshots cs
                JOIN symbols s ON cs.symbol = s.symbol
                WHERE s.sector = ?
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) > 0
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) < 500
                AND cs.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs2 WHERE cs2.symbol = cs.symbol)
            """, (sector_code,)).fetchone()

            # Get percentile rank within sector
            pe_rank = con.execute("""
                SELECT
                    COUNT(*) as cheaper_count,
                    (SELECT COUNT(*) FROM company_snapshots cs2
                     JOIN symbols s2 ON cs2.symbol = s2.symbol
                     WHERE s2.sector = ?
                     AND CAST(json_extract(cs2.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) > 0
                     AND CAST(json_extract(cs2.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) < 500
                     AND cs2.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs3 WHERE cs3.symbol = cs2.symbol)
                    ) as total_count
                FROM company_snapshots cs
                JOIN symbols s ON cs.symbol = s.symbol
                WHERE s.sector = ?
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) > ?
                AND CAST(json_extract(cs.snapshot_json, '$.fundamentals.pe_ratio') AS REAL) < 500
                AND cs.snapshot_date = (SELECT MAX(snapshot_date) FROM company_snapshots cs2 WHERE cs2.symbol = cs.symbol)
            """, (sector_code, sector_code, pe)).fetchone()

            if sector_valuation and sector_valuation["sector_count"] >= 3:
                sector_name = data.get("sector_name") or sector_code

                with st.expander(f"📊 Valuation vs {sector_name} Sector", expanded=False):
                    val_col1, val_col2, val_col3, val_col4 = st.columns(4)

                    with val_col1:
                        st.metric("Your P/E", f"{pe:.1f}")

                    with val_col2:
                        avg_pe = sector_valuation["avg_pe"]
                        diff = pe - avg_pe
                        st.metric("Sector Avg P/E", f"{avg_pe:.1f}",
                                  delta=f"{diff:+.1f}", delta_color="inverse")

                    with val_col3:
                        st.metric("Sector Range",
                                  f"{sector_valuation['min_pe']:.0f} - {sector_valuation['max_pe']:.0f}")

                    with val_col4:
                        if pe_rank and pe_rank["total_count"] > 0:
                            cheaper = pe_rank["cheaper_count"]
                            total = pe_rank["total_count"]
                            percentile = (cheaper / total) * 100
                            st.metric("Cheaper Than", f"{percentile:.0f}% of sector",
                                      help="Percentage of sector stocks with higher P/E (more expensive)")

                    # Visual comparison
                    if sector_valuation["max_pe"] > sector_valuation["min_pe"]:
                        pe_position = (pe - sector_valuation["min_pe"]) / (sector_valuation["max_pe"] - sector_valuation["min_pe"])
                        pe_position = min(1.0, max(0.0, pe_position))
                        st.progress(pe_position)
                        if pe_position < 0.33:
                            st.caption("✅ **Value Zone** - P/E in lower third of sector range")
                        elif pe_position < 0.67:
                            st.caption("⚪ **Fair Value** - P/E in middle of sector range")
                        else:
                            st.caption("⚠️ **Premium Valuation** - P/E in upper third of sector range")
        except Exception:
            pass

    # =================================================================
    # DETAILED QUOTE SECTION
    # =================================================================
    st.markdown("---")
    st.markdown("#### 📊 Quote Details")

    if data:
        # Bid/Ask and Ranges in a cleaner layout
        detail_col1, detail_col2, detail_col3 = st.columns(3)

        with detail_col1:
            st.markdown("**Day Range**")
            day_low = data.get("day_range_low")
            day_high = data.get("day_range_high")
            if day_low and day_high:
                current = data.get("price", 0)
                if day_high > day_low:
                    pct = (current - day_low) / (day_high - day_low)
                    st.progress(min(1.0, max(0.0, pct)))
                st.caption(f"Rs. {day_low:,.2f} — Rs. {day_high:,.2f}")
            else:
                st.caption("N/A")

        with detail_col2:
            st.markdown("**52-Week Range**")
            wk52_low = data.get("wk52_low")
            wk52_high = data.get("wk52_high")
            if wk52_low and wk52_high:
                current = data.get("price", 0)
                if wk52_high > wk52_low:
                    pct = (current - wk52_low) / (wk52_high - wk52_low)
                    st.progress(min(1.0, max(0.0, pct)))
                st.caption(f"Rs. {wk52_low:,.2f} — Rs. {wk52_high:,.2f}")
            else:
                st.caption("N/A")

        with detail_col3:
            st.markdown("**Circuit Breaker**")
            circuit_low = data.get("circuit_low")
            circuit_high = data.get("circuit_high")
            if circuit_low and circuit_high:
                st.caption(f"Lower: Rs. {circuit_low:,.2f}")
                st.caption(f"Upper: Rs. {circuit_high:,.2f}")
            else:
                st.caption("N/A")

        # Equity Structure Row
        st.markdown("")
        eq_col1, eq_col2, eq_col3, eq_col4 = st.columns(4)

        with eq_col1:
            total_shares = data.get("total_shares")
            if total_shares:
                ts_str = f"{total_shares/1e9:.2f}B" if total_shares >= 1e9 else f"{total_shares/1e6:.0f}M"
                st.metric("Total Shares", ts_str)
            else:
                st.metric("Total Shares", "N/A")

        with eq_col2:
            ff = data.get("free_float_shares")
            ff_pct = data.get("free_float_pct")
            if ff:
                ff_str = f"{ff/1e6:.0f}M ({ff_pct:.1f}%)" if ff_pct else f"{ff/1e6:.0f}M"
                st.metric("Free Float", ff_str)
            else:
                st.metric("Free Float", "N/A")

        with eq_col3:
            haircut = data.get("haircut")
            st.metric("Haircut", f"{haircut:.1f}%" if haircut else "N/A")

        with eq_col4:
            var = data.get("variance")
            st.metric("VAR", f"{var:.1f}%" if var else "N/A")

    # ----- Company Profile -----
    profile = data.get("profile_data", {})
    if profile or data.get("company_name"):
        st.subheader("🏢 Company Profile")

        profile_cols = st.columns(2)
        with profile_cols[0]:
            st.markdown(f"**Company Name:** {data.get('company_name') or profile.get('company_name', 'N/A')}")
            st.markdown(f"**Sector:** {data.get('sector_name') or profile.get('sector', 'N/A')}")
            st.markdown(f"**Listed In:** {profile.get('listed_in', 'N/A')}")
            shares = data.get("total_shares") or profile.get("shares_outstanding")
            if shares:
                st.markdown(f"**Shares Outstanding:** {shares:,}")
            else:
                st.markdown("**Shares Outstanding:** N/A")

        with profile_cols[1]:
            paid_up = profile.get("paid_up_capital")
            if paid_up:
                st.markdown(f"**Paid-up Capital:** Rs. {paid_up:,}")
            else:
                st.markdown("**Paid-up Capital:** N/A")
            st.markdown(f"**Face Value:** {profile.get('face_value', 'N/A')}")
            st.markdown(f"**Market Lot:** {profile.get('market_lot', 'N/A')}")
            st.markdown(f"**Fiscal Year End:** {profile.get('fiscal_year_end', 'N/A')}")

        # Additional info in expander
        with st.expander("More Details"):
            st.markdown(f"**Registrar:** {profile.get('registrar', 'N/A')}")
            st.markdown(f"**Last Updated:** {data.get('scraped_at', 'N/A')}")

    # ----- Trading Sessions (Multi-Market) -----
    trading_sessions = data.get("trading_sessions", {})
    trading_data = data.get("trading_data", {})
    if trading_sessions or trading_data:
        st.markdown("---")
        st.subheader("📈 Trading Sessions")

        # Combine today's sessions and snapshot trading data
        all_markets = set(list(trading_sessions.keys()) + list(trading_data.keys()))

        if all_markets:
            market_tabs = st.tabs(sorted(all_markets))
            for i, market in enumerate(sorted(all_markets)):
                with market_tabs[i]:
                    session = trading_sessions.get(market, {}) or trading_data.get(market, {})
                    if session:
                        mcols = st.columns(5)
                        with mcols[0]:
                            st.metric("Open", f"Rs. {session.get('open', 0):,.2f}" if session.get('open') else "N/A")
                        with mcols[1]:
                            st.metric("High", f"Rs. {session.get('high', 0):,.2f}" if session.get('high') else "N/A")
                        with mcols[2]:
                            st.metric("Low", f"Rs. {session.get('low', 0):,.2f}" if session.get('low') else "N/A")
                        with mcols[3]:
                            st.metric("Close", f"Rs. {session.get('close', 0):,.2f}" if session.get('close') else "N/A")
                        with mcols[4]:
                            vol = session.get('volume', 0)
                            if vol:
                                vol_str = f"{vol:,.0f}" if vol < 1000000 else f"{vol/1000000:.2f}M"
                            else:
                                vol_str = "N/A"
                            st.metric("Volume", vol_str)
                    else:
                        st.info(f"No data for {market}")
        else:
            st.info("No trading session data available.")

    # ----- Recent Announcements -----
    announcements = data.get("announcements", [])
    if announcements:
        st.markdown("---")
        st.subheader("📣 Recent Announcements")

        # Show count
        st.caption(f"Showing {len(announcements)} most recent announcements")

        for ann in announcements[:5]:
            with st.expander(f"{ann.get('announcement_date', 'N/A')} - {ann.get('announcement_type', 'News')}"):
                st.markdown(f"**{ann.get('title', 'No title')}**")
                if ann.get("content"):
                    st.markdown(ann.get("content", "")[:500] + "..." if len(ann.get("content", "")) > 500 else ann.get("content", ""))

        if len(announcements) > 5:
            with st.expander(f"View all {len(announcements)} announcements"):
                ann_df = pd.DataFrame(announcements)
                display_cols = ["announcement_date", "announcement_type", "title"]
                available_cols = [c for c in display_cols if c in ann_df.columns]
                if available_cols:
                    st.dataframe(ann_df[available_cols], use_container_width=True, hide_index=True)

    # ----- Charts -----
    st.subheader("📈 Charts")

    # Get quote history for charts
    quotes_df = client.get_company_quotes(symbol, limit=100)

    if not quotes_df.empty and len(quotes_df) > 1:
        import plotly.graph_objects as go

        chart_tabs = st.tabs(["Price Trend", "Volume"])

        with chart_tabs[0]:
            # Price trend line chart with auto-scaling y-axis
            chart_df = quotes_df.sort_values("ts", ascending=True)
            fig_price = go.Figure()
            fig_price.add_trace(go.Scatter(
                x=chart_df["ts"],
                y=chart_df["price"],
                mode="lines",
                name="Price",
                line={"color": "#2196F3", "width": 2},
            ))
            # Auto-scale y-axis to data range with 5% padding
            price_min = chart_df["price"].min()
            price_max = chart_df["price"].max()
            price_range = price_max - price_min
            padding = price_range * 0.05 if price_range > 0 else price_max * 0.05
            fig_price.update_layout(
                xaxis_title="Time",
                yaxis_title="Price (Rs.)",
                height=400,
                hovermode="x unified",
                yaxis={"range": [price_min - padding, price_max + padding]},
                margin={"l": 60, "r": 20, "t": 20, "b": 60},
            )
            st.plotly_chart(fig_price, use_container_width=True)

        with chart_tabs[1]:
            # Volume bar chart
            chart_df = quotes_df.sort_values("ts", ascending=True)
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(
                x=chart_df["ts"],
                y=chart_df["volume"],
                name="Volume",
                marker_color="#673AB7",
            ))
            fig_vol.update_layout(
                xaxis_title="Time",
                yaxis_title="Volume",
                height=400,
                hovermode="x unified",
                margin={"l": 60, "r": 20, "t": 20, "b": 60},
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        # Stats
        if quote_stats:
            with st.expander("Quote Statistics"):
                stat_cols = st.columns(4)
                with stat_cols[0]:
                    total = quote_stats.get("total_snapshots", 0)
                    st.metric("Total Snapshots", total)
                with stat_cols[1]:
                    avg_p = quote_stats.get("avg_price", 0)
                    st.metric("Avg Price", f"Rs. {avg_p:,.2f}")
                with stat_cols[2]:
                    min_p = quote_stats.get("min_price", 0)
                    st.metric("Min Price", f"Rs. {min_p:,.2f}")
                with stat_cols[3]:
                    max_p = quote_stats.get("max_price", 0)
                    st.metric("Max Price", f"Rs. {max_p:,.2f}")
    else:
        st.info("Not enough quote history for charts. Take more snapshots over time.")

    # ----- Financial Data Tabs (from PSX tabs: FINANCIALS, RATIOS, PAYOUTS) -----
    st.markdown("---")
    st.subheader("📊 Financial Data")

    # Fetch financial data
    financials_df = client.get_company_financials(symbol)
    ratios_df = client.get_company_ratios(symbol)
    payouts_df = client.get_company_payouts(symbol)

    has_financial_data = (
        not financials_df.empty or
        not ratios_df.empty or
        not payouts_df.empty
    )

    if has_financial_data:
        fin_tabs = st.tabs(["📈 Financials", "📊 Ratios", "💰 Payouts"])

        # FINANCIALS Tab
        with fin_tabs[0]:
            if not financials_df.empty:
                st.markdown("*All numbers in thousands (000's) except EPS*")

                # Pivot for better display
                annual_df = financials_df[financials_df["period_type"] == "annual"]
                quarterly_df = financials_df[financials_df["period_type"] == "quarterly"]

                if not annual_df.empty:
                    st.markdown("**Annual Financials**")
                    display_cols = ["period_end", "sales", "profit_after_tax", "eps"]
                    available_cols = [c for c in display_cols if c in annual_df.columns]
                    col_config = {
                        "period_end": st.column_config.TextColumn("Year"),
                        "sales": st.column_config.NumberColumn("Sales (000s)", format="%,.0f"),
                        "profit_after_tax": st.column_config.NumberColumn("Profit After Tax (000s)", format="%,.0f"),
                        "eps": st.column_config.NumberColumn("EPS", format="%.2f"),
                    }
                    st.dataframe(
                        annual_df[available_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_config,
                    )

                if not quarterly_df.empty:
                    st.markdown("**Quarterly Financials**")
                    display_cols = ["period_end", "sales", "profit_after_tax", "eps"]
                    available_cols = [c for c in display_cols if c in quarterly_df.columns]
                    st.dataframe(
                        quarterly_df[available_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_config,
                    )
            else:
                st.info("No financial data available. Click 'Refresh Profile' to fetch data.")

        # RATIOS Tab
        with fin_tabs[1]:
            if not ratios_df.empty:
                annual_ratios = ratios_df[ratios_df["period_type"] == "annual"]

                if not annual_ratios.empty:
                    st.markdown("**Annual Ratios**")
                    display_cols = [
                        "period_end", "gross_profit_margin", "net_profit_margin",
                        "eps_growth", "peg_ratio"
                    ]
                    available_cols = [c for c in display_cols if c in annual_ratios.columns]
                    col_config = {
                        "period_end": st.column_config.TextColumn("Year"),
                        "gross_profit_margin": st.column_config.NumberColumn("Gross Margin %", format="%.2f%%"),
                        "net_profit_margin": st.column_config.NumberColumn("Net Margin %", format="%.2f%%"),
                        "eps_growth": st.column_config.NumberColumn("EPS Growth %", format="%.2f%%"),
                        "peg_ratio": st.column_config.NumberColumn("PEG", format="%.2f"),
                    }
                    st.dataframe(
                        annual_ratios[available_cols].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_config,
                    )

                # Show key ratios summary
                if len(annual_ratios) > 0:
                    latest = annual_ratios.iloc[0]
                    ratio_cols = st.columns(4)

                    with ratio_cols[0]:
                        gpm = latest.get("gross_profit_margin")
                        st.metric("Gross Margin", f"{gpm:.1f}%" if gpm else "N/A")

                    with ratio_cols[1]:
                        npm = latest.get("net_profit_margin")
                        st.metric("Net Margin", f"{npm:.1f}%" if npm else "N/A")

                    with ratio_cols[2]:
                        epsg = latest.get("eps_growth")
                        if epsg:
                            st.metric("EPS Growth", f"{epsg:+.1f}%", delta=f"{epsg:+.1f}%")
                        else:
                            st.metric("EPS Growth", "N/A")

                    with ratio_cols[3]:
                        peg = latest.get("peg_ratio")
                        st.metric("PEG Ratio", f"{peg:.2f}" if peg else "N/A")
            else:
                st.info("No ratio data available. Click 'Refresh Profile' to fetch data.")

        # PAYOUTS Tab
        with fin_tabs[2]:
            if not payouts_df.empty:
                st.markdown("**Dividend / Payout History**")
                display_cols = [
                    "ex_date", "payout_type", "amount", "fiscal_year",
                    "announcement_date"
                ]
                available_cols = [c for c in display_cols if c in payouts_df.columns]
                col_config = {
                    "ex_date": st.column_config.TextColumn("Ex-Date"),
                    "payout_type": st.column_config.TextColumn("Type"),
                    "amount": st.column_config.NumberColumn("Amount", format="%.2f"),
                    "fiscal_year": st.column_config.TextColumn("Fiscal Year"),
                    "announcement_date": st.column_config.TextColumn("Announced"),
                }
                st.dataframe(
                    payouts_df[available_cols].head(20),
                    use_container_width=True,
                    hide_index=True,
                    column_config=col_config,
                )

                # Summary metrics
                total_div = payouts_df[payouts_df["payout_type"] == "cash"]["amount"].sum()
                cash_count = len(payouts_df[payouts_df["payout_type"] == "cash"])
                bonus_count = len(payouts_df[payouts_df["payout_type"] == "bonus"])

                payout_cols = st.columns(3)
                with payout_cols[0]:
                    st.metric("Total Cash Dividends", f"Rs. {total_div:.2f}" if total_div else "N/A")
                with payout_cols[1]:
                    st.metric("Cash Payouts", cash_count)
                with payout_cols[2]:
                    st.metric("Bonus Issues", bonus_count)
            else:
                st.info("No payout history available.")
    else:
        st.info(
            "No financial data available yet. "
            "Click 'Refresh Profile' to fetch financial data from PSX."
        )

    render_footer()
