"""Symbols listing page."""

import pandas as pd
import streamlit as st

from pakfindata.query import (
    get_symbols_list,
    get_symbols_string,
)
from pakfindata.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_symbols():
    """Browse and manage all symbols."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🧵 Symbols")
        st.caption("Master list of all PSX-listed securities")
    with header_col2:
        render_market_status_badge()

    try:
        con = get_connection()

        # Filters
        col1, col2 = st.columns([1, 3])
        with col1:
            show_inactive = st.checkbox(
                "Show inactive",
                value=False,
                help="Include symbols that are no longer actively traded"
            )
        with col2:
            search = st.text_input(
                "Search",
                placeholder="e.g. HBL, Bank",
                help="Filter by symbol or company name"
            )

        # Get active/inactive filter
        is_active_only = not show_inactive

        # Display count of active symbols
        active_count = len(get_symbols_list(con, is_active_only=True))
        st.markdown(f"**Active symbols: {active_count}**")

        # Build query for full symbol details
        # sector_name is now stored directly in symbols table from master file
        query = """
            SELECT symbol, name, sector as sector_code,
                   sector_name, outstanding_shares, is_active, source,
                   discovered_at, updated_at
            FROM symbols
        """
        conditions = []
        if not show_inactive:
            conditions.append("is_active = 1")
        if search:
            search_upper = search.upper()
            conditions.append(
                f"(symbol LIKE '%{search_upper}%' OR name LIKE '%{search}%')"
            )
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY symbol"

        df = pd.read_sql_query(query, con)

        st.markdown(f"**{len(df)} symbols found**")

        if df.empty:
            st.info("No symbols found. Run `pfsync master refresh` to fetch.")
        else:
            # Show symbols table from DB
            df["is_active"] = df["is_active"].map({1: "Yes", 0: "No"})
            # Fill empty sector_name with sector_code
            df["sector_name"] = df["sector_name"].fillna(df["sector_code"])
            # Select and rename columns - only show sector_name, not sector_code
            display_df = df[
                ["symbol", "name", "sector_name",
                 "is_active", "discovered_at", "updated_at"]
            ].copy()
            display_df.columns = [
                "Symbol", "Name", "Sector",
                "Active", "Discovered", "Updated"
            ]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # Actions
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "⬇️ Download CSV",
                    display_df.to_csv(index=False),
                    "psx_symbols.csv",
                    "text/csv",
                    help="Download symbols list to your computer"
                )
            with col2:
                # Copy comma-separated symbols string
                symbols_str = get_symbols_string(con, is_active_only=is_active_only)
                if len(symbols_str) > 100:
                    display_str = symbols_str[:100] + "..."
                else:
                    display_str = symbols_str
                st.code(display_str)
                st.caption("Copy symbols as comma-separated string")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()
