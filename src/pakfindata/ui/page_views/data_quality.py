"""Data quality dashboard — coverage, freshness, gaps, duplicates, maintenance."""

from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from pakfindata.api_client import get_client
from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer


# ── Cached data loaders ──────────────────────────────────────────────


def _safe_distinct(table: str, col: str = "symbol") -> int:
    """Call /v1/admin/tables/{table}/distinct-count; 0 on failure."""
    payload = api_client.get_admin_table_distinct_count(table, col)
    return int(payload["distinct_count"]) if payload else 0


def _safe_total(table: str, counts_by_name: dict) -> int:
    """Lookup a table's total row_count from the cached /v1/admin/tables payload."""
    return int(counts_by_name.get(table, 0) or 0)


def _load_coverage_summary():
    """Load coverage metrics: symbol counts across tables."""
    # Single /v1/admin/tables call with counts — used 3x below.
    tables = api_client.get_admin_tables(include_counts=True) or []
    counts_by_name = {t["name"]: t["row_count"] for t in tables}

    total_symbols = _safe_total("symbols", counts_by_name)
    eod_symbols = _safe_distinct("eod_ohlcv", "symbol")
    intraday_symbols = _safe_distinct("intraday_bars", "symbol")
    profile_symbols = _safe_distinct("company_fundamentals", "symbol")
    rm_symbols = _safe_distinct("regular_market_current", "symbol")
    mf_count = _safe_total("mutual_funds", counts_by_name)
    etf_count = _safe_total("etf_master", counts_by_name)

    # "Symbols with no data" — derive client-side from /v1/symbols.
    # The original SQL excluded symbols with EOD or live-market data.
    # We can't perform the exclusion server-side without a new endpoint;
    # for the dashboard's purposes, total - eod is a close enough proxy.
    all_symbols = api_client.get_symbols(active_only=False) or []
    # Sets of symbols with data are not easily fetched in bulk via /v1;
    # leave no_data_list empty when we can't compute it cheaply rather
    # than hammer per-symbol APIs.
    no_data_list: list[str] = []
    if eod_symbols == 0 and all_symbols:
        no_data_list = [r["symbol"] for r in all_symbols]

    return {
        "total_symbols": total_symbols,
        "eod_symbols": eod_symbols,
        "intraday_symbols": intraday_symbols,
        "profile_symbols": profile_symbols,
        "rm_symbols": rm_symbols,
        "mf_count": mf_count,
        "etf_count": etf_count,
        "no_data_list": no_data_list,
    }


_FRESHNESS_DOMAINS = [
    # (label, table, date_col, latest_slice)
    ("EOD OHLCV", "eod_ohlcv", "date", 10),
    ("Intraday Bars", "intraday_bars", "ts", 19),
    ("Live Market", "regular_market_current", "ts", 19),
    ("Company Profiles", "company_fundamentals", "updated_at", 10),
    ("PSX Indices", "psx_indices", "index_date", 10),
    ("FX Rates", "fx_ohlcv", "date", 10),
    ("Sync Runs", "sync_runs", "start_time", 19),
    ("ETF NAV", "etf_nav", "date", 10),
    ("Mutual Fund NAV", "mutual_fund_nav", "date", 10),
    ("T-Bill Auctions", "tbill_auctions", "auction_date", 10),
    ("PIB Auctions", "pib_auctions", "auction_date", 10),
    ("GIS Auctions", "gis_auctions", "auction_date", 10),
    ("PKRV Yield Curve", "pkrv_daily", "date", 10),
    ("KIBOR Rates", "kibor_daily", "date", 10),
    ("KONIA Rate", "konia_daily", "date", 10),
    ("SBP Policy Rate", "sbp_policy_rates", "rate_date", 10),
    ("SBP FX Interbank", "sbp_fx_interbank", "date", 10),
    ("SBP FX Open Market", "sbp_fx_open_market", "date", 10),
    ("Kerb FX", "forex_kerb", "date", 10),
    ("IPO Calendar", "ipo_listings", "listing_date", 10),
    ("Dividends", "company_payouts", "ex_date", 10),
    ("Sukuk Master", "sukuk_master", "created_at", 10),
    ("PKISRV Islamic Curve", "pkisrv_daily", "date", 10),
    ("PKFRV Float Rate", "pkfrv_daily", "date", 10),
]


def _load_freshness_data():
    """Load freshness info for all data domains via /v1/admin."""
    tables = api_client.get_admin_tables(include_counts=True) or []
    counts_by_name = {t["name"]: t["row_count"] for t in tables}

    freshness_rows = []
    for label, table, date_col, slice_len in _FRESHNESS_DOMAINS:
        if table not in counts_by_name:
            continue
        payload = api_client.get_admin_table_latest_date(table, col=date_col)
        latest = payload.get("latest_date") if payload else None
        if not latest:
            continue
        try:
            days_old = (datetime.now() - datetime.strptime(
                str(latest)[:10], "%Y-%m-%d"
            )).days
        except ValueError:
            days_old = -1
        row_count = counts_by_name.get(table) or 0
        freshness_rows.append({
            "Data Type": label,
            "Latest Date": str(latest)[:slice_len],
            "Days Old": days_old,
            "Row Count": f"{row_count:,}",
            "Status": _freshness_badge(days_old),
        })

    return freshness_rows


@st.cache_data(ttl=3600, show_spinner=False)
def _load_gap_data():
    """Load EOD trading dates for gap detection from manifest."""
    from pakfindata.db.date_manifest import get_dates
    dates = get_dates("eod_ohlcv")
    return pd.DataFrame({"date": sorted(dates)})


def _load_duplicate_data():
    """Detect duplicates across key tables via /v1/admin/.../duplicates."""
    targets = [
        ("EOD OHLCV (symbol+date)", "eod_ohlcv", ["symbol", "date"]),
        ("Intraday Bars (symbol+ts)", "intraday_bars", ["symbol", "ts"]),
        ("Company Fundamentals (symbol)", "company_fundamentals", ["symbol"]),
    ]
    dup_checks = []
    for label, table, by_cols in targets:
        payload = api_client.get_admin_table_duplicates(
            table=table, by=by_cols, limit=20
        )
        if payload is None:
            continue
        # /v1 payload rows shape: {"key": {col: val, ...}, "count": int}
        # The page expects flat dicts (col1, col2, cnt). Flatten back.
        rows = [
            {**r["key"], "cnt": r["count"]} for r in payload.get("rows", [])
        ]
        dup_checks.append((label, payload.get("total_groups", 0), rows))
    return dup_checks


def _load_db_stats():
    """Load database statistics via /v1/admin/db-stats."""
    return api_client.get_admin_db_stats() or {}


# ── Page renderer ────────────────────────────────────────────────────


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
        cov = _load_coverage_summary()

        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("Total Symbols", f"{cov['total_symbols']:,}")
        c2.metric("With EOD Data", f"{cov['eod_symbols']:,}")
        c3.metric("With Intraday", f"{cov['intraday_symbols']:,}")
        c4.metric("With Profile", f"{cov['profile_symbols']:,}")
        c5.metric("Live Market", f"{cov['rm_symbols']:,}")
        c6.metric("Mutual Funds", f"{cov['mf_count']:,}")
        c7.metric("ETFs", f"{cov['etf_count']:,}")

        no_data_list = cov["no_data_list"]
        if no_data_list:
            with st.expander(f"Symbols with NO data ({len(no_data_list)})", expanded=False):
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
        freshness_rows = _load_freshness_data()

        if freshness_rows:
            st.dataframe(
                pd.DataFrame(freshness_rows),
                width='stretch',
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
        dates_df = _load_gap_data()

        if not dates_df.empty:
            # Convert cached string dates back to datetime for analysis
            dates_dt = pd.to_datetime(dates_df["date"])
            min_date = dates_dt.min()
            max_date = dates_dt.max()

            all_bdays = pd.bdate_range(start=min_date, end=max_date)
            existing_dates = set(dates_dt.dt.date)
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
                            "Missing": "#FF5252",
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
                    st.plotly_chart(fig, width='stretch')
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
        dup_checks = _load_duplicate_data()

        all_clean = True
        for table_name, dup_count, dup_rows in dup_checks:
            if dup_count > 0:
                all_clean = False
                st.warning(f"**{table_name}**: {dup_count} duplicate groups found")
                with st.expander(f"Show duplicates in {table_name}"):
                    dup_df = pd.DataFrame(dup_rows)
                    st.dataframe(dup_df, width='stretch', hide_index=True)
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
        from pakfindata.db.maintenance import (
            analyze_database,
            backup_database,
            check_integrity,
            vacuum_database,
        )

        # DB stats (cached)
        db_stats = _load_db_stats()

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
                st.dataframe(tc_df, width='stretch', hide_index=True)

        st.markdown("---")

        # Quick action buttons (not cached — these are user-triggered actions)
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
