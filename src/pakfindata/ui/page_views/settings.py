"""Settings and configuration page."""

from pathlib import Path
import pandas as pd
import streamlit as st

from pakfindata.config import (
    DEFAULT_DB_PATH,
    DEFAULT_LOG_FILE,
    DEFAULT_SYNC_CONFIG,
)
from pakfindata.ui.components.helpers import (
    EXPORTS_DIR,
    get_connection,
    render_footer,
)


def render_settings():
    """Display configuration (read-only)."""
    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## ⚙️ Settings")
    st.caption("System configuration (read-only)")

    st.info("Settings are read-only. Use CLI flags or edit config to change.")

    # Database
    st.subheader("Database")
    st.code(f"Path: {DEFAULT_DB_PATH}")

    try:
        con = get_connection()
        tables = ["symbols", "eod_ohlcv", "sync_runs", "sync_failures"]

        # Check for regular market tables
        rm_tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "name LIKE 'regular_market%'"
        ).fetchall()
        if rm_tables:
            tables.extend([t[0] for t in rm_tables])

        # Check for intraday table
        intraday_table = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND "
            "name='intraday_ohlcv'"
        ).fetchone()
        if intraday_table:
            tables.append("intraday_ohlcv")

        sizes = []
        for table in tables:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                sizes.append({"Table": table, "Rows": count})
            except Exception:
                sizes.append({"Table": table, "Rows": "N/A"})
        st.dataframe(pd.DataFrame(sizes), hide_index=True)
    except Exception as e:
        st.warning(f"Cannot read database: {e}")

    st.markdown("---")

    # Sync configuration
    st.subheader("Sync Configuration")
    config = DEFAULT_SYNC_CONFIG
    st.markdown(f"""
    | Setting | Value |
    |---------|-------|
    | Max Retries | {config.max_retries} |
    | Delay Range | {config.delay_min}s - {config.delay_max}s |
    | Timeout | {config.timeout}s |
    | Incremental | {config.incremental} |
    """)

    st.markdown("---")

    # Logging
    st.subheader("Logging")
    st.code(f"Log Path: {DEFAULT_LOG_FILE}")
    st.markdown("- Max Size: 5 MB\n- Backups: 3 files")

    st.markdown("---")

    # Exports directory
    st.subheader("Exports")
    st.code(f"Export Path: {EXPORTS_DIR}")
    if EXPORTS_DIR.exists():
        exports = list(EXPORTS_DIR.glob("*.csv"))
        if exports:
            st.markdown(f"**{len(exports)} CSV files exported**")
            for f in exports[:10]:
                st.text(f"  - {f.name}")
        else:
            st.info("No exports yet.")
    else:
        st.info("Exports directory not created yet.")

    st.markdown("---")

    # Data source
    st.subheader("Data Source")
    st.markdown("""
    | Endpoint | URL |
    |----------|-----|
    | Market Watch | `https://dps.psx.com.pk/market-watch` |
    | EOD API | `https://dps.psx.com.pk/timeseries/eod/{SYMBOL}` |
    """)

    render_footer()
