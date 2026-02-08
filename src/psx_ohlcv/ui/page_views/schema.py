"""Database schema viewer page."""

from pathlib import Path
import pandas as pd
import streamlit as st
import time

from psx_ohlcv.config import get_db_path
from psx_ohlcv.ui.session_tracker import track_page_visit
from psx_ohlcv.ui.components.helpers import (
    get_connection,
)


def render_schema():
    """Display database schema documentation and SQL creation scripts."""
    from psx_ohlcv.db import SCHEMA_SQL

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📋 Database Schema")
    st.caption("Table structure, SQL scripts, and data dictionary")

    con = get_connection()
    track_page_visit(con, "Schema")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Table Overview",
        "📖 Glossary",
        "💾 SQL Scripts",
        "📈 Database Stats"
    ])

    # Tab 1: Table Overview
    with tab1:
        st.subheader("Table Categories")

        # Core Tables
        st.markdown("### Core Tables")
        core_tables = [
            ("symbols", "symbol", "Master symbol list with metadata"),
            ("eod_ohlcv", "(symbol, date)", "End-of-day OHLCV price data"),
            ("intraday_bars", "(symbol, ts)", "Intraday time series (1-min bars)"),
            ("intraday_sync_state", "symbol", "Last sync timestamp per symbol"),
            ("sectors", "sector_code", "Sector master list"),
        ]
        st.table({"Table": [t[0] for t in core_tables],
                  "Primary Key": [t[1] for t in core_tables],
                  "Description": [t[2] for t in core_tables]})

        # Company Data Tables
        st.markdown("### Company Data Tables")
        company_tables = [
            ("company_profile", "symbol", "Company profile information"),
            ("company_key_people", "(symbol, role, name)", "Directors, executives"),
            ("company_quote_snapshots", "(symbol, ts)", "Point-in-time quote captures"),
            ("company_fundamentals", "symbol", "Latest fundamentals (live)"),
            ("company_fundamentals_history", "(symbol, date)", "Historical fundamentals"),
            ("company_financials", "(symbol, period_end, period_type)", "Income statement data"),
            ("company_ratios", "(symbol, period_end, period_type)", "Financial ratios"),
            ("company_payouts", "(symbol, ex_date, payout_type)", "Dividends and bonuses"),
        ]
        st.table({"Table": [t[0] for t in company_tables],
                  "Primary Key": [t[1] for t in company_tables],
                  "Description": [t[2] for t in company_tables]})

        # Quant Tables
        st.markdown("### Quant/Bloomberg-Style Tables")
        quant_tables = [
            ("company_snapshots", "(symbol, snapshot_date)", "Full JSON document storage"),
            ("trading_sessions", "(symbol, session_date, market_type, contract_month)", "Market microstructure"),
            ("corporate_announcements", "id + unique constraint", "Company announcements"),
            ("equity_structure", "(symbol, as_of_date)", "Ownership and capital structure"),
            ("scrape_jobs", "job_id", "Scrape job tracking"),
        ]
        st.table({"Table": [t[0] for t in quant_tables],
                  "Primary Key": [t[1] for t in quant_tables],
                  "Description": [t[2] for t in quant_tables]})

        # System Tables
        st.markdown("### System Tables")
        system_tables = [
            ("sync_runs", "run_id", "Sync job runs"),
            ("sync_failures", "N/A", "Failed sync records"),
            ("downloaded_market_summary_dates", "date", "Market summary download tracking"),
            ("user_interactions", "id", "UI analytics tracking"),
        ]
        st.table({"Table": [t[0] for t in system_tables],
                  "Primary Key": [t[1] for t in system_tables],
                  "Description": [t[2] for t in system_tables]})

    # Tab 2: Glossary
    with tab2:
        st.subheader("Glossary")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Market Terms")
            market_terms = {
                "OHLCV": "Open, High, Low, Close, Volume - standard price bar data",
                "EOD": "End of Day - daily closing data",
                "LDCP": "Last Day Close Price - previous trading day's close",
                "VWAP": "Volume Weighted Average Price",
                "VAR": "Value at Risk - risk metric percentage",
                "Haircut": "Margin collateral discount percentage",
                "Circuit Breaker": "Price limit bands (upper/lower)",
                "Free Float": "Shares available for public trading",
            }
            for term, definition in market_terms.items():
                st.markdown(f"**{term}**: {definition}")

            st.markdown("#### Market Types")
            market_types = {
                "REG": "Regular Market - main trading board",
                "FUT": "Futures Market - derivatives",
                "CSF": "Cash Settled Futures",
                "ODL": "Odd Lot Market - small quantity trades",
            }
            for code, desc in market_types.items():
                st.markdown(f"**{code}**: {desc}")

        with col2:
            st.markdown("#### Data Sources")
            sources = {
                "Market Watch": "dps.psx.com.pk/market-watch (Real-time quotes)",
                "Company Page": "dps.psx.com.pk/company/{symbol} (Company details)",
                "Market Summary": "dps.psx.com.pk/download/mkt_summary/{date}.Z (EOD bulk)",
                "Listed Companies": "dps.psx.com.pk/listed-companies (Symbol master)",
            }
            for source, desc in sources.items():
                st.markdown(f"**{source}**: {desc}")

            st.markdown("#### Period Types")
            periods = {
                "annual": "Full fiscal year data",
                "quarterly": "Quarter-end data (Q1, Q2, Q3, Q4)",
                "ttm": "Trailing Twelve Months",
                "ytd": "Year to Date",
            }
            for period, desc in periods.items():
                st.markdown(f"**{period}**: {desc}")

            st.markdown("#### Payout Types")
            payouts = {
                "cash": "Cash dividend per share",
                "bonus": "Bonus shares (stock dividend)",
                "right": "Rights issue offering",
            }
            for ptype, desc in payouts.items():
                st.markdown(f"**{ptype}**: {desc}")

    # Tab 3: SQL Scripts
    with tab3:
        st.subheader("SQL Creation Scripts")

        st.markdown("Full schema SQL from `src/psx_ohlcv/db.py`:")

        # Show the full SQL
        st.code(SCHEMA_SQL, language="sql")

        # Download button
        st.download_button(
            label="📥 Download Schema SQL",
            data=SCHEMA_SQL,
            file_name="psx_ohlcv_schema.sql",
            mime="text/plain"
        )

        st.markdown("---")
        st.markdown("#### Quick Query Examples")

        examples = '''-- List all tables
.tables

-- Show table schema
.schema symbols
.schema eod_ohlcv

-- Recent EOD data
SELECT symbol, date, close, volume
FROM eod_ohlcv
WHERE date = (SELECT MAX(date) FROM eod_ohlcv)
ORDER BY volume DESC
LIMIT 10;

-- Company snapshot JSON extract
SELECT symbol, snapshot_date,
       json_extract(quote_data, '$.price') as price,
       json_extract(quote_data, '$.change_pct') as change_pct
FROM company_snapshots
WHERE snapshot_date = date('now');

-- Daily returns calculation
SELECT symbol, date, close,
       (close - prev_close) / prev_close * 100 AS return_pct
FROM eod_ohlcv
WHERE symbol = 'OGDC'
ORDER BY date DESC
LIMIT 30;
'''
        st.code(examples, language="sql")

    # Tab 4: Database Stats
    with tab4:
        st.subheader("Database Statistics")

        # Get all tables and their row counts
        try:
            tables_query = """
                SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name
            """
            tables = [row[0] for row in con.execute(tables_query).fetchall()]

            stats = []
            for table in tables:
                try:
                    count = con.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
                    stats.append({"Table": table, "Rows": count})
                except Exception:
                    stats.append({"Table": table, "Rows": "Error"})

            if stats:
                import pandas as pd
                df = pd.DataFrame(stats)
                df = df.sort_values("Rows", ascending=False, key=lambda x: pd.to_numeric(x, errors='coerce'))

                col1, col2 = st.columns([2, 1])
                with col1:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                with col2:
                    total_rows = sum(s["Rows"] for s in stats if isinstance(s["Rows"], int))
                    st.metric("Total Tables", len(stats))
                    st.metric("Total Rows", f"{total_rows:,}")

                    # Database file size
                    from psx_ohlcv.config import get_db_path
                    db_path = get_db_path()
                    if db_path.exists():
                        size_mb = db_path.stat().st_size / (1024 * 1024)
                        st.metric("Database Size", f"{size_mb:.2f} MB")

        except Exception as e:
            st.error(f"Error fetching stats: {e}")

        st.markdown("---")
        st.markdown("#### Connection Info")
        from psx_ohlcv.config import get_db_path
        st.code(f"Database Path: {get_db_path()}")
        st.code(f"SQLite Version: {con.execute('SELECT sqlite_version()').fetchone()[0]}")
