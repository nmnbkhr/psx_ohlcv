"""Stock Screener — filter by sector, P/E, market cap, and more."""

import pandas as pd
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_stock_screener():
    """Render the Stock Screener page with fundamental filters."""
    st.markdown("## Stock Screener")
    st.caption("Filter by sector, P/E, market cap, and other fundamentals")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # ── Check available data sources ──────────────────────────────
    has_rm = _table_has_rows(con, "regular_market_current")
    has_cf = _table_has_rows(
        con, "company_fundamentals", "pe_ratio IS NOT NULL OR market_cap IS NOT NULL"
    )

    if not has_rm and not has_cf:
        st.warning("**No market data available yet.**")
        st.info(
            "**To get started, run one of these:**\n"
            "- `pfsync regular-market snapshot` — fetches live price, volume & change for all stocks (fastest)\n"
            "- `pfsync company deep-scrape --all` — fetches P/E, market cap, free float per stock (slower, ~1 req/symbol)"
        )
        render_footer()
        return

    # ── Filters ──────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    # Get sectors
    sectors = ["All"]
    try:
        rows = con.execute(
            "SELECT DISTINCT sector_name FROM sectors ORDER BY sector_name"
        ).fetchall()
        sectors += [r[0] for r in rows if r[0]]
    except Exception:
        try:
            rows = con.execute(
                "SELECT DISTINCT sector_name FROM company_profile WHERE sector_name IS NOT NULL ORDER BY sector_name"
            ).fetchall()
            sectors += [r[0] for r in rows if r[0]]
        except Exception:
            pass

    with col1:
        sector = st.selectbox("Sector", sectors, key="scr_sector")

    with col2:
        min_pe = st.number_input("Min P/E", value=0.0, step=1.0, key="scr_min_pe")
        max_pe = st.number_input("Max P/E", value=100.0, step=1.0, key="scr_max_pe")

    with col3:
        min_mcap = st.number_input(
            "Min Market Cap (M)", value=0.0, step=100.0, key="scr_min_mcap"
        )

    with col4:
        min_vol = st.number_input(
            "Min Avg Volume", value=0.0, step=10000.0, key="scr_min_vol"
        )

    # ── Build Query ──────────────────────────────────────────────
    # Uses regular_market_current as primary source for price/volume/change
    # and company_fundamentals for P/E, market cap, free float when available.
    try:
        query = """
            SELECT
                s.symbol,
                COALESCE(cp.company_name, cf.company_name, s.name) AS name,
                COALESCE(cp.sector_name, s.sector_name) AS sector,
                COALESCE(rm.current, cf.price) AS price,
                cf.pe_ratio,
                cf.market_cap,
                cf.free_float_pct,
                COALESCE(rm.volume, e.volume) AS last_volume,
                ROUND(COALESCE(rm.current, cf.price) * COALESCE(rm.volume, e.volume), 0) AS turnover,
                COALESCE(rm.change_pct,
                    ROUND((e.close - e.prev_close) / NULLIF(e.prev_close, 0) * 100, 2)
                ) AS change_pct
            FROM symbols s
            LEFT JOIN regular_market_current rm ON s.symbol = rm.symbol
            LEFT JOIN company_fundamentals cf ON s.symbol = cf.symbol
            LEFT JOIN company_profile cp ON s.symbol = cp.symbol
            LEFT JOIN eod_ohlcv e ON s.symbol = e.symbol
                AND e.date = (SELECT MAX(date) FROM eod_ohlcv)
            WHERE s.is_active = 1
        """
        params = []

        if sector != "All":
            query += " AND COALESCE(cp.sector_name, s.sector_name) = ?"
            params.append(sector)

        if min_pe > 0:
            query += " AND cf.pe_ratio >= ?"
            params.append(min_pe)

        if max_pe < 100:
            query += " AND cf.pe_ratio <= ?"
            params.append(max_pe)

        if min_mcap > 0:
            query += " AND cf.market_cap >= ?"
            params.append(min_mcap * 1e6)

        if min_vol > 0:
            query += " AND COALESCE(rm.volume, e.volume) >= ?"
            params.append(min_vol)

        # Sort by market cap when available, fall back to turnover
        query += " ORDER BY cf.market_cap DESC NULLS LAST, turnover DESC NULLS LAST LIMIT 200"

        df = pd.read_sql_query(query, con, params=params)

        if df.empty:
            st.info("No stocks match your filters. Try adjusting criteria.")
            render_footer()
            return

        # ── Info banner for missing fundamentals ──────────────────
        if not has_cf:
            st.info(
                "Showing price, volume & change from market watch. "
                "Run `pfsync company deep-scrape --all` to add P/E, market cap & free float."
            )

        # ── Results Summary ──────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Matches", len(df))

        avg_pe = df["pe_ratio"].dropna().mean()
        c2.metric("Avg P/E", f"{avg_pe:.1f}" if pd.notna(avg_pe) else "N/A")

        total_mcap = df["market_cap"].dropna().sum()
        if total_mcap >= 1e12:
            c3.metric("Total Market Cap", f"Rs. {total_mcap / 1e12:.1f}T")
        elif total_mcap >= 1e9:
            c3.metric("Total Market Cap", f"Rs. {total_mcap / 1e9:.1f}B")
        elif total_mcap > 0:
            c3.metric("Total Market Cap", f"Rs. {total_mcap / 1e6:.0f}M")
        else:
            c3.metric("Total Market Cap", "N/A")

        total_turnover = df["turnover"].dropna().sum()
        if total_turnover >= 1e9:
            c4.metric("Total Turnover", f"Rs. {total_turnover / 1e9:.2f}B")
        elif total_turnover >= 1e6:
            c4.metric("Total Turnover", f"Rs. {total_turnover / 1e6:.0f}M")
        elif total_turnover > 0:
            c4.metric("Total Turnover", f"Rs. {total_turnover:,.0f}")
        else:
            c4.metric("Total Turnover", "N/A")

        st.divider()

        # ── Results Table ────────────────────────────────────────
        display_df = df.copy()
        display_df.columns = [
            "Symbol", "Name", "Sector", "Price", "P/E",
            "Market Cap", "Free Float %", "Volume", "Turnover", "Change %",
        ]

        # Format market cap
        display_df["Market Cap"] = display_df["Market Cap"].apply(
            lambda x: f"{x / 1e9:.1f}B" if pd.notna(x) and x >= 1e9
            else (f"{x / 1e6:.0f}M" if pd.notna(x) and x >= 1e6 else "—")
        )

        # Format free float
        display_df["Free Float %"] = display_df["Free Float %"].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) else "—"
        )

        # Format price
        display_df["Price"] = display_df["Price"].apply(
            lambda x: f"{x:,.2f}" if pd.notna(x) else "—"
        )

        # Format volume
        display_df["Volume"] = display_df["Volume"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) else "—"
        )

        # Format turnover
        display_df["Turnover"] = display_df["Turnover"].apply(
            lambda x: f"{x / 1e9:.2f}B" if pd.notna(x) and x >= 1e9
            else (f"{x / 1e6:.1f}M" if pd.notna(x) and x >= 1e6
            else (f"{x:,.0f}" if pd.notna(x) and x > 0 else "—"))
        )

        # Format change %
        display_df["Change %"] = display_df["Change %"].apply(
            lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
        )

        # Format P/E
        display_df["P/E"] = display_df["P/E"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "—"
        )

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Export button
        csv = df.to_csv(index=False)
        st.download_button(
            "Export CSV", csv, "screener_results.csv", "text/csv",
            key="scr_export",
        )

    except Exception as e:
        st.warning(f"Could not load screener data: {e}")
        st.info(
            "Run `pfsync regular-market snapshot` to populate market data, "
            "or `pfsync company deep-scrape --all` for full fundamentals."
        )

    render_footer()


def _table_has_rows(con, table: str, where: str = "1=1") -> bool:
    """Check if a table exists and has matching rows."""
    try:
        row = con.execute(
            f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {where})"
        ).fetchone()
        return bool(row[0])
    except Exception:
        return False
