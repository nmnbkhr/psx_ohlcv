"""Futures, contracts, and odd-lot data page."""

import pandas as pd
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer

# ---------------------------------------------------------------------------
# Cached loaders — avoid re-running heavy queries on every interaction
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def _cached_futures_stats(_db_path: str) -> dict:
    from pakfindata.db.repositories.futures import get_futures_stats, init_futures_schema
    con = get_connection()
    init_futures_schema(con)
    return get_futures_stats(con)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_futures_dates(_db_path: str) -> list[str]:
    from pakfindata.db.repositories.futures import get_futures_dates
    con = get_connection()
    return get_futures_dates(con)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_base_symbols(_db_path: str) -> list[str]:
    con = get_connection()
    rows = con.execute(
        "SELECT DISTINCT base_symbol FROM futures_eod "
        "WHERE market_type IN ('FUT', 'CONT') ORDER BY base_symbol"
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300, show_spinner=False)
def _cached_odl_symbols(_db_path: str) -> pd.DataFrame:
    from pakfindata.db.repositories.futures import get_odl_symbols
    con = get_connection()
    return get_odl_symbols(con)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_odl_stats(_db_path: str) -> dict:
    from pakfindata.db.repositories.futures import get_odl_stats
    con = get_connection()
    return get_odl_stats(con)


# Use a constant key so cache invalidation is shared
_DB_KEY = "psx_futures"


def _clear_futures_cache():
    """Clear all futures-related caches after data changes."""
    _cached_futures_stats.clear()
    _cached_futures_dates.clear()
    _cached_base_symbols.clear()
    _cached_odl_symbols.clear()
    _cached_odl_stats.clear()


def render_futures():
    """Futures & Contracts dashboard with OHLCV data from daily market summary."""
    try:
        _render_futures_impl()
    except Exception as e:
        st.error(f"Error loading Futures page: {e}")
        import traceback
        with st.expander("Error Details"):
            st.code(traceback.format_exc())


def _render_futures_impl():
    st.markdown("## Futures & Contracts")
    st.caption(
        "Stock futures, deliverable contracts, index futures, "
        "and odd-lot bonds from daily market summary .Z files"
    )

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    from pakfindata.db.repositories.futures import (
        init_futures_schema,
        get_futures_eod,
        get_contract_comparison,
        get_most_active_futures,
        get_odl_history,
        migrate_from_eod_ohlcv,
    )

    init_futures_schema(con)

    # Load shared data once (cached)
    stats = _cached_futures_stats(_DB_KEY)
    dates = _cached_futures_dates(_DB_KEY)

    tab_overview, tab_compare, tab_odl, tab_sync = st.tabs([
        "Overview", "Contract Comparison", "Odd-Lot Bonds", "Sync & Migrate"
    ])

    with tab_overview:
        _render_overview(con, stats, dates,
                         get_futures_eod, get_most_active_futures)

    with tab_compare:
        _render_comparison(con, dates, get_contract_comparison)

    with tab_odl:
        _render_odd_lot(con, dates, get_futures_eod, get_odl_history)

    with tab_sync:
        _render_sync(con, stats, migrate_from_eod_ohlcv)

    render_footer()


# ---------------------------------------------------------------------------
# Tab 1: Overview
# ---------------------------------------------------------------------------

def _render_overview(con, stats, dates, get_futures_eod, get_most_active_futures):
    if stats["total_rows"] == 0:
        st.info(
            "No futures data yet. Use the **Sync & Migrate** tab to "
            "migrate existing data from eod_ohlcv or load new data."
        )
        return

    # Summary metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Records", f"{stats['total_rows']:,}")
    c2.metric("FUT", f"{stats['fut_rows']:,}")
    c3.metric("CONT", f"{stats['cont_rows']:,}")
    c4.metric("IDX_FUT", f"{stats['idx_fut_rows']:,}")
    c5.metric("ODL", f"{stats['odl_rows']:,}")

    c6, c7, c8 = st.columns(3)
    c6.metric("Trading Dates", f"{stats['total_dates']:,}")
    c7.metric("Base Symbols", stats["unique_base_symbols"])
    c8.metric("Date Range", f"{stats['min_date']} → {stats['max_date']}")

    st.markdown("---")

    if not dates:
        return

    # Reset date selection if stored value is stale (new data arrived)
    if "fut_date" in st.session_state and st.session_state["fut_date"] not in dates:
        del st.session_state["fut_date"]

    col_date, col_type = st.columns([1, 2])
    with col_date:
        sel_date = st.selectbox("Date", dates, key="fut_date")
    with col_type:
        type_options = ["All", "FUT", "CONT", "IDX_FUT", "ODL"]
        sel_type = st.selectbox("Market Type", type_options, key="fut_type")

    market_type = None if sel_type == "All" else sel_type
    df = get_futures_eod(con, date=sel_date, market_type=market_type, limit=5000)

    if df.empty:
        st.warning(f"No data for {sel_date}")
        return

    # Base symbol search
    search = st.text_input("Search base symbol", key="fut_search")
    if search:
        df = df[df["base_symbol"].str.contains(search.upper())]

    display_cols = [
        "symbol", "base_symbol", "market_type", "contract_month",
        "close", "change_pct", "volume", "turnover", "prev_close", "open", "high", "low",
    ]
    show_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[show_cols],
        use_container_width=True,
        hide_index=True,
        height=400,
    )
    st.caption(f"{len(df)} rows")

    # Most active
    with st.expander("Most Active (by volume)"):
        active = get_most_active_futures(con, sel_date, market_type=market_type, limit=20)
        if not active.empty:
            st.dataframe(active, use_container_width=True, hide_index=True)
        else:
            st.info("No active trading data.")


# ---------------------------------------------------------------------------
# Tab 2: Contract Comparison
# ---------------------------------------------------------------------------

def _render_comparison(con, dates, get_contract_comparison):
    if not dates:
        st.info("No futures data available.")
        return

    base_symbols = _cached_base_symbols(_DB_KEY)
    if not base_symbols:
        st.info("No futures/contract data to compare.")
        return

    col1, col2 = st.columns(2)
    if "cmp_date" in st.session_state and st.session_state["cmp_date"] not in dates:
        del st.session_state["cmp_date"]

    with col1:
        sel_sym = st.selectbox("Base Symbol", base_symbols, key="cmp_sym")
    with col2:
        sel_date = st.selectbox("Date", dates, key="cmp_date")

    df = get_contract_comparison(con, sel_sym, sel_date)
    if df.empty:
        st.warning(f"No data for {sel_sym} on {sel_date}")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)

    # Spot price from eod_ohlcv
    spot_row = con.execute(
        "SELECT close FROM eod_ohlcv WHERE symbol = ? AND date = ?",
        (sel_sym, sel_date),
    ).fetchone()

    if spot_row:
        spot = spot_row[0]
        st.metric(f"{sel_sym} Spot (REG)", f"{spot:.2f}")

        # Compute basis for each futures contract
        fut_rows = df[df["market_type"] == "FUT"]
        if not fut_rows.empty:
            st.markdown("**Basis (Futures - Spot):**")
            for _, row in fut_rows.iterrows():
                basis = row["close"] - spot if row["close"] else None
                if basis is not None:
                    st.write(
                        f"  {row['symbol']} ({row['contract_month']}): "
                        f"**{basis:+.2f}** ({basis / spot * 100:+.2f}%)"
                    )
    else:
        st.caption(f"No spot price found for {sel_sym} on {sel_date}")


# ---------------------------------------------------------------------------
# Tab 3: Odd-Lot Bonds
# ---------------------------------------------------------------------------

def _render_odd_lot(con, dates, get_futures_eod, get_odl_history):
    odl_stats = _cached_odl_stats(_DB_KEY)
    if odl_stats["distinct_symbols"] == 0:
        st.info("No odd-lot bond data. Load market summary data first.")
        return

    # --- Summary metrics ---
    odl_df = _cached_odl_symbols(_DB_KEY)
    odl_df = _decode_odl_df(odl_df)

    gov_df = odl_df[odl_df["is_government"] == True]  # noqa: E712
    corp_df = odl_df[odl_df["is_government"] == False]  # noqa: E712

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ODL Instruments", odl_stats["distinct_symbols"])
    c2.metric("Government", len(gov_df))
    c3.metric("Corporate", len(corp_df))
    c4.metric("Date Range", f"{odl_stats['min_date']} → {odl_stats['max_date']}")

    st.markdown("---")

    if "odl_date" in st.session_state and dates and st.session_state["odl_date"] not in dates:
        del st.session_state["odl_date"]

    col_date, col_view = st.columns([1, 3])
    with col_date:
        sel_date = st.selectbox("Date", dates, key="odl_date") if dates else None

    # --- Sub-tabs ---
    tab_gov, tab_corp, tab_all, tab_detail = st.tabs([
        "Government Bonds", "Corporate Bonds", "All", "Bond Detail",
    ])

    # Get daily data if date selected
    daily_df = pd.DataFrame()
    if sel_date:
        daily_df = get_futures_eod(con, date=sel_date, market_type="ODL")
        if not daily_df.empty:
            daily_df = _decode_odl_df(daily_df)

    # --- Government tab ---
    with tab_gov:
        _render_odl_gov_tab(con, daily_df, gov_df, sel_date)

    # --- Corporate tab ---
    with tab_corp:
        _render_odl_corp_tab(daily_df, corp_df, sel_date)

    # --- All tab ---
    with tab_all:
        src = daily_df if not daily_df.empty else odl_df
        if src.empty:
            st.info("No data")
        else:
            cols = ["symbol", "display_name", "security_type", "close",
                    "volume", "change_pct", "date"]
            show = [c for c in cols if c in src.columns]
            st.dataframe(src[show], use_container_width=True, hide_index=True,
                         height=500)
            st.caption(f"{len(src)} instruments")

    # --- Detail tab ---
    with tab_detail:
        _render_odl_detail(con, odl_df, get_odl_history)


def _decode_odl_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add decoded columns to an ODL DataFrame."""
    from pakfindata.sources.psx_debt import parse_symbol_info, build_display_name

    if df.empty:
        return df

    decoded = df["symbol"].apply(parse_symbol_info)
    df = df.copy()
    df["security_type"] = decoded.apply(lambda x: x.get("security_type") or "Unknown")
    df["tenor_years"] = decoded.apply(lambda x: x.get("tenor_years"))
    df["maturity_date"] = decoded.apply(lambda x: x.get("maturity_date"))
    df["is_government"] = decoded.apply(lambda x: x.get("is_government", True))
    df["is_islamic"] = decoded.apply(lambda x: x.get("is_islamic", False))
    df["issuer"] = decoded.apply(lambda x: x.get("issuer"))
    df["display_name"] = df.apply(
        lambda r: build_display_name(
            r["symbol"],
            decoded[r.name],
            r.get("company_name"),
        ),
        axis=1,
    )
    return df


def _render_odl_gov_tab(con, daily_df, latest_gov_df, sel_date):
    """Government bonds sub-tab with auction yield cross-link."""
    gov = pd.DataFrame()
    if not daily_df.empty:
        gov = daily_df[daily_df["is_government"] == True]  # noqa: E712

    # Fall back to latest prices if none traded on selected date
    if gov.empty:
        gov = latest_gov_df

    if gov.empty:
        st.info("No government bond data")
        return

    # Build auction yield lookup
    yield_map = _build_auction_yield_map(con)

    # Add auction yield column
    gov = gov.copy()
    gov["auction_yield"] = gov.apply(
        lambda r: yield_map.get(
            (r["security_type"], int(r["tenor_years"]) if r["tenor_years"] else None)
        ),
        axis=1,
    )

    # Group by security type
    for sec_type in sorted(gov["security_type"].unique()):
        subset = gov[gov["security_type"] == sec_type]
        st.markdown(f"**{sec_type}** ({len(subset)} instruments)")
        cols = ["symbol", "display_name", "tenor_years", "maturity_date",
                "close", "volume", "change_pct", "auction_yield"]
        show = [c for c in cols if c in subset.columns]
        st.dataframe(subset[show], use_container_width=True, hide_index=True)

    st.caption(
        f"{len(gov)} government bonds"
        + (f" on {sel_date}" if sel_date else " (latest prices)")
        + " | Auction Yield = latest SBP auction cutoff for same tenor"
    )


def _render_odl_corp_tab(daily_df, latest_corp_df, sel_date):
    """Corporate bonds (TFC/Sukuk) sub-tab."""
    corp = pd.DataFrame()
    used_daily = False
    if not daily_df.empty:
        corp = daily_df[daily_df["is_government"] == False]  # noqa: E712
        used_daily = not corp.empty

    # Fall back to latest prices if no corporate bonds traded on selected date
    if corp.empty:
        corp = latest_corp_df

    if corp.empty:
        st.info("No corporate bond data")
        return

    if not used_daily and sel_date:
        st.caption(f"No corporate bonds traded on {sel_date} — showing latest available prices")

    cols = ["symbol", "display_name", "security_type", "issuer",
            "close", "volume", "change_pct", "company_name", "date"]
    show = [c for c in cols if c in corp.columns]
    st.dataframe(corp[show], use_container_width=True, hide_index=True)
    st.caption(
        f"{len(corp)} corporate bonds"
        + (f" on {sel_date}" if used_daily else " (latest prices per instrument)")
    )


def _render_odl_detail(con, odl_df, get_odl_history):
    """Bond detail with price history chart."""
    if odl_df.empty:
        st.info("No ODL data")
        return

    symbols = sorted(odl_df["symbol"].unique())
    display_map = dict(zip(odl_df["symbol"], odl_df["display_name"]))
    sel = st.selectbox(
        "Select Bond",
        symbols,
        format_func=lambda s: f"{s} — {display_map.get(s, '')}",
        key="odl_detail_sym",
    )

    if not sel:
        return

    hist = get_odl_history(con, sel)
    if hist.empty:
        st.warning(f"No history for {sel}")
        return

    # Metadata
    info_row = odl_df[odl_df["symbol"] == sel].iloc[0] if sel in odl_df["symbol"].values else None
    if info_row is not None:
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Latest Close", f"{info_row['close']:.2f}" if info_row["close"] else "N/A")
        mc2.metric("Security Type", info_row.get("security_type", ""))
        mc3.metric("Maturity", info_row.get("maturity_date") or "N/A")
        mc4.metric("Volume", f"{info_row['volume']:,.0f}" if info_row["volume"] else "N/A")

    # Price chart
    import plotly.graph_objects as go

    hist_sorted = hist.sort_values("date")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist_sorted["date"], y=hist_sorted["close"],
        mode="lines+markers", name="Close Price",
        line=dict(color="#00d4aa", width=2),
        marker=dict(size=4),
    ))
    fig.update_layout(
        title=f"{sel} — {display_map.get(sel, '')}",
        xaxis_title="Date", yaxis_title="Price (PKR)",
        height=400, template="plotly_dark",
    )
    st.plotly_chart(fig, use_container_width=True)

    # History table
    with st.expander("Price History"):
        st.dataframe(hist, use_container_width=True, hide_index=True)


def _build_auction_yield_map(con) -> dict:
    """Build (security_type, tenor_years) → latest auction yield lookup."""
    yield_map = {}

    try:
        # PIB auctions → for FRR, VRR, FRZ, PIB types
        pib_rows = con.execute("""
            SELECT p.tenor, p.pib_type, p.cutoff_yield
            FROM pib_auctions p
            INNER JOIN (
                SELECT tenor, pib_type, MAX(auction_date) as max_date
                FROM pib_auctions GROUP BY tenor, pib_type
            ) latest ON p.tenor = latest.tenor
                     AND p.pib_type = latest.pib_type
                     AND p.auction_date = latest.max_date
            WHERE p.cutoff_yield IS NOT NULL
        """).fetchall()

        tenor_map = {"2Y": 2, "3Y": 3, "5Y": 5, "10Y": 10, "15Y": 15, "20Y": 20, "30Y": 30}
        for row in pib_rows:
            tenor_yrs = tenor_map.get(row[0])
            if tenor_yrs and row[2]:
                # PIB Fixed maps to FRR Sukuk, VRR Sukuk, FRZ, PIB
                for st_name in ["FRR Sukuk", "VRR Sukuk", "FRZ", "PIB", "Floating"]:
                    yield_map[(st_name, tenor_yrs)] = round(row[2], 4)

        # GIS auctions → for GIS and Variable GIS
        gis_rows = con.execute("""
            SELECT g.tenor, g.gis_type, g.cutoff_rental_rate
            FROM gis_auctions g
            INNER JOIN (
                SELECT tenor, gis_type, MAX(auction_date) as max_date
                FROM gis_auctions GROUP BY tenor, gis_type
            ) latest ON g.tenor = latest.tenor
                     AND g.gis_type = latest.gis_type
                     AND g.auction_date = latest.max_date
            WHERE g.cutoff_rental_rate IS NOT NULL
                AND g.cutoff_rental_rate < 30
        """).fetchall()

        for row in gis_rows:
            tenor_yrs = tenor_map.get(row[0])
            if tenor_yrs and row[2]:
                gis_type = row[1] or ""
                if "Variable" in gis_type:
                    yield_map[("Variable GIS", tenor_yrs)] = round(row[2], 4)
                else:
                    yield_map[("GIS", tenor_yrs)] = round(row[2], 4)

        # T-Bill auctions → for T-Bill type
        tbill_rows = con.execute("""
            SELECT t.tenor, t.cutoff_yield
            FROM tbill_auctions t
            INNER JOIN (
                SELECT tenor, MAX(auction_date) as max_date
                FROM tbill_auctions GROUP BY tenor
            ) latest ON t.tenor = latest.tenor AND t.auction_date = latest.max_date
            WHERE t.cutoff_yield IS NOT NULL
        """).fetchall()

        tbill_tenor_map = {"1M": 1/12, "3M": 0.25, "6M": 0.5, "12M": 1, "1Y": 1}
        for row in tbill_rows:
            tenor_yrs = tbill_tenor_map.get(row[0])
            if tenor_yrs and row[1]:
                yield_map[("T-Bill", tenor_yrs)] = round(row[1], 4)

    except Exception:
        pass  # Auction data may not exist yet

    return yield_map


# ---------------------------------------------------------------------------
# Tab 4: Sync & Migrate
# ---------------------------------------------------------------------------

def _render_sync(con, stats, migrate_from_eod_ohlcv):
    st.markdown("### Current Stats")
    c1, c2, c3 = st.columns(3)
    c1.metric("futures_eod rows", f"{stats['total_rows']:,}")
    c2.metric("Trading dates", f"{stats['total_dates']:,}")
    c3.metric("Base symbols", stats["unique_base_symbols"])

    # Check eod_ohlcv pollution
    eod_pollution = con.execute(
        "SELECT COUNT(*) FROM eod_ohlcv WHERE sector_code IN ('40', '41', '36')"
    ).fetchone()[0]

    st.markdown("### Migration from eod_ohlcv")
    st.metric("FUT/CONT/ODL rows still in eod_ohlcv", f"{eod_pollution:,}")

    if eod_pollution == 0:
        st.success("No derivative rows in eod_ohlcv — clean!")
    else:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Dry Run (preview)", key="fut_migrate_dry"):
                result = migrate_from_eod_ohlcv(con, dry_run=True)
                st.info(
                    f"Would migrate **{result['total_eligible']:,}** rows "
                    f"from eod_ohlcv → futures_eod"
                )
        with col2:
            if st.button("Execute Migration", key="fut_migrate_exec", type="primary"):
                with st.spinner("Migrating..."):
                    result = migrate_from_eod_ohlcv(con, dry_run=False)
                _clear_futures_cache()
                st.success(
                    f"Migrated {result['migrated']:,} rows to futures_eod, "
                    f"deleted {result['deleted_from_eod']:,} from eod_ohlcv"
                )
                st.rerun()

    # Incremental load — only new CSVs since last loaded date
    from pathlib import Path
    from pakfindata.config import DATA_ROOT
    from pakfindata.db.repositories.eod import ingest_market_summary_csv

    csv_dir = DATA_ROOT / "market_summary" / "csv"
    max_date = stats.get("max_date") or ""

    st.markdown("### Load New CSVs (Incremental)")

    if csv_dir.exists():
        all_csvs = sorted(csv_dir.glob("*.csv"))
        new_csvs = [f for f in all_csvs if f.stem > max_date] if max_date else all_csvs

        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.metric("Last loaded date", max_date or "None")
        with col_info2:
            st.metric("New CSVs available", len(new_csvs))

        if new_csvs:
            st.caption(
                f"Will process {len(new_csvs)} files: "
                f"{new_csvs[0].stem} → {new_csvs[-1].stem}"
            )
            if st.button(
                f"Load {len(new_csvs)} New Files",
                key="fut_load_new",
                type="primary",
            ):
                progress = st.progress(0, text="Processing new CSVs...")
                total_futures = 0
                total_reg = 0

                for i, csv_path in enumerate(new_csvs):
                    result = ingest_market_summary_csv(
                        con, csv_path, skip_existing=False, source="market_summary",
                    )
                    total_futures += result.get("futures_rows", 0)
                    total_reg += result.get("reg_rows", 0)
                    progress.progress(
                        (i + 1) / len(new_csvs),
                        text=f"Processing {csv_path.name}... ({i + 1}/{len(new_csvs)})",
                    )

                progress.empty()
                _clear_futures_cache()
                st.success(
                    f"Loaded {len(new_csvs)} new CSVs — "
                    f"REG: {total_reg:,}, FUT/CONT/ODL: {total_futures:,}"
                )
                st.rerun()
        else:
            st.success("Up to date — no new CSVs to load.")
    else:
        st.warning(f"No CSV directory: {csv_dir}")

    # Full reload from existing CSVs
    st.markdown("### Full Reload (all CSVs)")
    st.caption(
        "Re-ingest ALL historical CSVs to route FUT/CONT/ODL into futures_eod. "
        "Use only if incremental load missed data."
    )

    if st.button("Reload All CSVs", key="fut_populate_csv"):
        if not csv_dir.exists():
            st.warning(f"No CSV directory: {csv_dir}")
            return

        csvs = sorted(csv_dir.glob("*.csv"))
        if not csvs:
            st.warning("No CSV files found.")
            return

        progress = st.progress(0, text="Processing all CSVs...")
        total_futures = 0
        total_reg = 0

        for i, csv_path in enumerate(csvs):
            result = ingest_market_summary_csv(
                con, csv_path, skip_existing=False, source="market_summary",
            )
            total_futures += result.get("futures_rows", 0)
            total_reg += result.get("reg_rows", 0)
            progress.progress(
                (i + 1) / len(csvs),
                text=f"Processing {csv_path.name}... ({i + 1}/{len(csvs)})",
            )

        progress.empty()
        _clear_futures_cache()
        st.success(
            f"Processed {len(csvs)} CSVs — "
            f"REG: {total_reg:,}, FUT/CONT/ODL: {total_futures:,}"
        )
        st.rerun()
