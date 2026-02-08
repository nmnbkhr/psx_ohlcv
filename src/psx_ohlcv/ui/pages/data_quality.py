"""Data quality dashboard — coverage, freshness, gaps, duplicates, maintenance."""

from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from psx_ohlcv.api_client import get_client
from psx_ohlcv.ui.components.helpers import render_footer


def render_data_quality():
    """Data quality dashboard: what data do I have, what's missing, what's stale?"""

    st.markdown("## 🩺 Data Quality Dashboard")
    st.caption("Coverage, freshness, gap detection, duplicates, and maintenance")

    client = get_client()
    con = client.connection

    if con is None:
        st.error("No database connection available.")
        render_footer()
        return

    st.markdown("---")

    # =================================================================
    # 1. COVERAGE SUMMARY
    # =================================================================
    st.subheader("1. Coverage Summary")

    try:
        # Total symbols registered
        total_symbols = con.execute(
            "SELECT COUNT(*) as cnt FROM symbols"
        ).fetchone()["cnt"]

        # Symbols with EOD data
        eod_symbols = con.execute(
            "SELECT COUNT(DISTINCT symbol) as cnt FROM eod_ohlcv"
        ).fetchone()["cnt"]

        # Symbols with intraday data
        try:
            intraday_symbols = con.execute(
                "SELECT COUNT(DISTINCT symbol) as cnt FROM intraday_bars"
            ).fetchone()["cnt"]
        except Exception:
            intraday_symbols = 0

        # Symbols with company profile
        try:
            profile_symbols = con.execute(
                "SELECT COUNT(DISTINCT symbol) as cnt FROM company_fundamentals"
            ).fetchone()["cnt"]
        except Exception:
            profile_symbols = 0

        # Symbols with regular market data
        try:
            rm_symbols = con.execute(
                "SELECT COUNT(DISTINCT symbol) as cnt FROM regular_market_current"
            ).fetchone()["cnt"]
        except Exception:
            rm_symbols = 0

        # v3 metrics
        try:
            mf_count = con.execute("SELECT COUNT(*) as cnt FROM mutual_funds").fetchone()["cnt"]
        except Exception:
            mf_count = 0
        try:
            etf_count = con.execute("SELECT COUNT(*) as cnt FROM etf_master").fetchone()["cnt"]
        except Exception:
            etf_count = 0

        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("Total Symbols", f"{total_symbols:,}")
        c2.metric("With EOD Data", f"{eod_symbols:,}")
        c3.metric("With Intraday", f"{intraday_symbols:,}")
        c4.metric("With Profile", f"{profile_symbols:,}")
        c5.metric("Live Market", f"{rm_symbols:,}")
        c6.metric("Mutual Funds", f"{mf_count:,}")
        c7.metric("ETFs", f"{etf_count:,}")

        # Symbols with NO data at all
        no_data = con.execute("""
            SELECT s.symbol FROM symbols s
            WHERE s.symbol NOT IN (SELECT DISTINCT symbol FROM eod_ohlcv)
              AND s.symbol NOT IN (SELECT DISTINCT symbol FROM regular_market_current)
            ORDER BY s.symbol
        """).fetchall()

        if no_data:
            with st.expander(f"Symbols with NO data ({len(no_data)})", expanded=False):
                no_data_list = [row["symbol"] for row in no_data]
                # Display in columns
                cols = st.columns(6)
                for i, sym in enumerate(no_data_list):
                    cols[i % 6].code(sym)
        else:
            st.success("All symbols have data.")

    except Exception as e:
        st.error(f"Coverage query failed: {e}")

    st.markdown("---")

    # =================================================================
    # 2. FRESHNESS TABLE
    # =================================================================
    st.subheader("2. Data Freshness")

    try:
        freshness_rows = []

        # EOD OHLCV
        try:
            row = con.execute(
                "SELECT MAX(date) as latest, COUNT(*) as cnt FROM eod_ohlcv"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "EOD OHLCV",
                    "Latest Date": str(row["latest"])[:10],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # Intraday Bars
        try:
            row = con.execute(
                "SELECT MAX(ts) as latest, COUNT(*) as cnt FROM intraday_bars"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "Intraday Bars",
                    "Latest Date": str(row["latest"])[:19],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # Regular Market Current
        try:
            row = con.execute(
                "SELECT MAX(ts) as latest, COUNT(*) as cnt FROM regular_market_current"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "Live Market",
                    "Latest Date": str(row["latest"])[:19],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # Company Fundamentals
        try:
            row = con.execute(
                "SELECT MAX(updated_at) as latest, COUNT(*) as cnt FROM company_fundamentals"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "Company Profiles",
                    "Latest Date": str(row["latest"])[:10],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # PSX Indices
        try:
            row = con.execute(
                "SELECT MAX(index_date) as latest, COUNT(*) as cnt FROM psx_indices"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "PSX Indices",
                    "Latest Date": str(row["latest"])[:10],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # FX Rates
        try:
            row = con.execute(
                "SELECT MAX(date) as latest, COUNT(*) as cnt FROM fx_ohlcv"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "FX Rates",
                    "Latest Date": str(row["latest"])[:10],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # Sync Runs
        try:
            row = con.execute(
                "SELECT MAX(start_time) as latest, COUNT(*) as cnt FROM sync_runs"
            ).fetchone()
            if row and row["latest"]:
                days_old = (datetime.now() - datetime.strptime(
                    str(row["latest"])[:10], "%Y-%m-%d"
                )).days
                freshness_rows.append({
                    "Data Type": "Sync Runs",
                    "Latest Date": str(row["latest"])[:19],
                    "Days Old": days_old,
                    "Row Count": f"{row['cnt']:,}",
                    "Status": _freshness_badge(days_old),
                })
        except Exception:
            pass

        # ── v3 Data Domains ─────────────────────────────────────
        _v3_domains = [
            ("ETF NAV", "etf_nav", "date"),
            ("Mutual Fund NAV", "mutual_fund_nav", "date"),
            ("T-Bill Auctions", "tbill_auctions", "auction_date"),
            ("PIB Auctions", "pib_auctions", "auction_date"),
            ("GIS Auctions", "gis_auctions", "auction_date"),
            ("PKRV Yield Curve", "pkrv_daily", "date"),
            ("KIBOR Rates", "kibor_daily", "date"),
            ("KONIA Rate", "konia_daily", "date"),
            ("SBP Policy Rate", "sbp_policy_rates", "rate_date"),
            ("SBP FX Interbank", "sbp_fx_interbank", "date"),
            ("SBP FX Open Market", "sbp_fx_open_market", "date"),
            ("Kerb FX", "forex_kerb", "date"),
            ("IPO Calendar", "ipo_listings", "listing_date"),
            ("Dividends", "company_payouts", "ex_date"),
            ("Sukuk Master", "sukuk_master", "created_at"),
        ]
        for label, table, date_col in _v3_domains:
            try:
                row = con.execute(
                    f"SELECT MAX({date_col}) as latest, COUNT(*) as cnt FROM {table}"
                ).fetchone()
                if row and row["latest"]:
                    try:
                        days_old = (datetime.now() - datetime.strptime(
                            str(row["latest"])[:10], "%Y-%m-%d"
                        )).days
                    except ValueError:
                        days_old = -1
                    freshness_rows.append({
                        "Data Type": label,
                        "Latest Date": str(row["latest"])[:10],
                        "Days Old": days_old,
                        "Row Count": f"{row['cnt']:,}",
                        "Status": _freshness_badge(days_old),
                    })
            except Exception:
                pass

        if freshness_rows:
            st.dataframe(
                pd.DataFrame(freshness_rows),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No data found in any table.")

    except Exception as e:
        st.error(f"Freshness check failed: {e}")

    st.markdown("---")

    # =================================================================
    # 3. GAP DETECTION
    # =================================================================
    st.subheader("3. EOD Gap Detection")

    try:
        # Get all unique trading dates
        dates_df = pd.read_sql_query(
            "SELECT DISTINCT date FROM eod_ohlcv ORDER BY date", con
        )

        if not dates_df.empty:
            dates_df["date"] = pd.to_datetime(dates_df["date"])

            # Find gaps: dates that are weekdays but missing from data
            min_date = dates_df["date"].min()
            max_date = dates_df["date"].max()

            # Generate all business days in range
            all_bdays = pd.bdate_range(start=min_date, end=max_date)
            existing_dates = set(dates_df["date"].dt.date)
            missing_dates = [d.date() for d in all_bdays if d.date() not in existing_dates]

            g1, g2, g3 = st.columns(3)
            g1.metric("Trading Days (data)", f"{len(dates_df):,}")
            g2.metric("Business Days (range)", f"{len(all_bdays):,}")
            g3.metric("Missing Days", f"{len(missing_dates):,}")

            if missing_dates:
                # Calendar heatmap — last 12 months
                twelve_months_ago = datetime.now() - timedelta(days=365)
                recent_missing = [
                    d for d in missing_dates if d >= twelve_months_ago.date()
                ]

                if recent_missing:
                    with st.expander(
                        f"Missing dates in last 12 months ({len(recent_missing)})",
                        expanded=False,
                    ):
                        for d in recent_missing[-30:]:  # Show last 30
                            st.text(f"  {d} ({d.strftime('%A')})")
                        if len(recent_missing) > 30:
                            st.caption(
                                f"... and {len(recent_missing) - 30} more"
                            )

                # Calendar heatmap using plotly
                # Build date->count mapping for last 6 months
                six_months_ago = datetime.now() - timedelta(days=180)
                cal_dates = pd.bdate_range(
                    start=six_months_ago, end=datetime.now()
                )
                cal_data = []
                for d in cal_dates:
                    has_data = d.date() in existing_dates
                    cal_data.append({
                        "date": d.date(),
                        "week": d.isocalendar()[1],
                        "weekday": d.weekday(),
                        "day_name": d.strftime("%a"),
                        "status": 1 if has_data else 0,
                        "label": "Data" if has_data else "Missing",
                    })

                cal_df = pd.DataFrame(cal_data)
                if not cal_df.empty:
                    fig = px.scatter(
                        cal_df,
                        x="week",
                        y="day_name",
                        color="label",
                        color_discrete_map={
                            "Data": "#00C853",
                            "Missing": "#FF1744",
                        },
                        title="EOD Data Calendar (last 6 months)",
                        hover_data=["date"],
                    )
                    fig.update_traces(marker=dict(size=12, symbol="square"))
                    fig.update_layout(
                        height=250,
                        margin=dict(t=40, l=10, r=10, b=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        yaxis=dict(
                            categoryorder="array",
                            categoryarray=["Mon", "Tue", "Wed", "Thu", "Fri"],
                        ),
                        xaxis_title="Week Number",
                        yaxis_title="",
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No gaps detected in EOD data.")
        else:
            st.info("No EOD data available.")

    except Exception as e:
        st.error(f"Gap detection failed: {e}")

    st.markdown("---")

    # =================================================================
    # 4. DUPLICATE DETECTION
    # =================================================================
    st.subheader("4. Duplicate Detection")

    try:
        dup_checks = []

        # EOD OHLCV duplicates
        try:
            dup_eod = con.execute("""
                SELECT symbol, date, COUNT(*) as cnt
                FROM eod_ohlcv
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                LIMIT 20
            """).fetchall()
            dup_checks.append(("EOD OHLCV (symbol+date)", len(dup_eod), dup_eod))
        except Exception:
            pass

        # Intraday duplicates
        try:
            dup_intra = con.execute("""
                SELECT symbol, ts, COUNT(*) as cnt
                FROM intraday_bars
                GROUP BY symbol, ts
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                LIMIT 20
            """).fetchall()
            dup_checks.append(("Intraday Bars (symbol+ts)", len(dup_intra), dup_intra))
        except Exception:
            pass

        # Company fundamentals duplicates
        try:
            dup_fund = con.execute("""
                SELECT symbol, COUNT(*) as cnt
                FROM company_fundamentals
                GROUP BY symbol
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                LIMIT 20
            """).fetchall()
            dup_checks.append(("Company Fundamentals (symbol)", len(dup_fund), dup_fund))
        except Exception:
            pass

        all_clean = True
        for table_name, dup_count, dup_rows in dup_checks:
            if dup_count > 0:
                all_clean = False
                st.warning(f"**{table_name}**: {dup_count} duplicate groups found")
                with st.expander(f"Show duplicates in {table_name}"):
                    dup_df = pd.DataFrame([dict(row) for row in dup_rows])
                    st.dataframe(dup_df, use_container_width=True, hide_index=True)
            else:
                st.success(f"**{table_name}**: No duplicates")

        if all_clean:
            st.success("All tables clean — no duplicates found.")

    except Exception as e:
        st.error(f"Duplicate check failed: {e}")

    st.markdown("---")

    # =================================================================
    # 5. QUICK ACTIONS & DB STATS
    # =================================================================
    st.subheader("5. Database Maintenance")

    try:
        from psx_ohlcv.db.maintenance import (
            analyze_database,
            backup_database,
            check_integrity,
            get_db_stats,
            vacuum_database,
        )

        # DB stats
        db_stats = get_db_stats(con)

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("DB Size", f"{db_stats.get('file_size_mb', 0):.1f} MB")
        s2.metric("WAL Size", f"{db_stats.get('wal_file_size_mb', 0):.1f} MB")
        s3.metric("Indexes", f"{db_stats.get('index_count', 0)}")
        s4.metric("Free Pages", f"{db_stats.get('free_page_count', 0)}")

        # Table row counts
        table_counts = db_stats.get("table_counts", {})
        if table_counts:
            with st.expander("Table Row Counts", expanded=False):
                tc_df = pd.DataFrame(
                    [{"Table": k, "Rows": f"{v:,}"} for k, v in
                     sorted(table_counts.items(), key=lambda x: x[1], reverse=True)]
                )
                st.dataframe(tc_df, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Quick action buttons
        a1, a2, a3, a4 = st.columns(4)

        with a1:
            if st.button("Run ANALYZE", help="Update query planner statistics"):
                with st.spinner("Running ANALYZE..."):
                    analyze_database(con)
                st.success("ANALYZE complete.")

        with a2:
            if st.button("Run VACUUM", help="Reclaim space and defragment"):
                with st.spinner("Running VACUUM (may take a minute)..."):
                    vacuum_database(con)
                st.success("VACUUM complete.")
                st.rerun()

        with a3:
            if st.button("Integrity Check", help="Verify database integrity"):
                with st.spinner("Checking integrity..."):
                    is_ok, msg = check_integrity(con)
                if is_ok:
                    st.success(f"Integrity OK: {msg}")
                else:
                    st.error(f"Integrity FAILED: {msg}")

        with a4:
            if st.button("Backup DB", help="Create a hot backup"):
                with st.spinner("Creating backup..."):
                    backup_path = backup_database(con)
                st.success(f"Backup saved: {backup_path}")

    except ImportError:
        st.warning("Maintenance module not available.")
    except Exception as e:
        st.error(f"Maintenance error: {e}")

    render_footer()


def _freshness_badge(days_old: int) -> str:
    """Return a text badge for freshness status."""
    if days_old <= 1:
        return "Fresh"
    elif days_old <= 3:
        return "Stale"
    else:
        return "Old"
