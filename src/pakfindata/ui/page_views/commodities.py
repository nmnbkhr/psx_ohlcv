"""Commodities Dashboard — Global commodity prices with Pakistan context.

Tabs:
  Dashboard — KPIs: Gold PKR/Tola, Brent, Cotton, USD/PKR + sparklines
  Charts — Interactive candlestick/line for any commodity
  Categories — Browse by Metals/Energy/Agriculture/FX, daily change heatmap
  Pakistan View — All prices in PKR with local units
  Export — CSV download
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from pakfindata.ui.components.helpers import get_connection


# ─────────────────────────────────────────────────────────────────────────────
# Schema initialization
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_commodity_schema(con):
    """Initialize commodity tables if they don't exist."""
    from pakfindata.commodities.models import init_commodity_schema
    init_commodity_schema(con)


def _has_commodity_data(con) -> bool:
    """Check if any commodity data has been synced."""
    try:
        eod = con.execute("SELECT COUNT(*) as cnt FROM commodity_eod").fetchone()["cnt"]
        if eod > 0:
            return True
        khi = con.execute("SELECT COUNT(*) as cnt FROM khistocks_prices").fetchone()["cnt"]
        if khi > 0:
            return True
        pmex = con.execute("SELECT COUNT(*) as cnt FROM pmex_market_watch").fetchone()["cnt"]
        return pmex > 0
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Render entry point
# ─────────────────────────────────────────────────────────────────────────────

def render_commodities():
    """Main render function for the commodities page."""
    st.markdown("## Commodities Dashboard")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    _ensure_commodity_schema(con)

    # Show sync prompt if no data
    if not _has_commodity_data(con):
        _render_empty_state(con)
        return

    # Tabs
    tab_dash, tab_charts, tab_categories, tab_pk, tab_local, tab_pmex, tab_export = st.tabs([
        "Dashboard", "Charts", "Categories", "Pakistan View", "Local Markets", "PMEX Portal", "Export"
    ])

    with tab_dash:
        _render_dashboard(con)

    with tab_charts:
        _render_charts(con)

    with tab_categories:
        _render_categories(con)

    with tab_pk:
        _render_pakistan_view(con)

    with tab_local:
        _render_local_markets(con)

    with tab_pmex:
        _render_pmex_portal(con)

    with tab_export:
        _render_export(con)

    # Sync controls at bottom
    st.divider()
    _render_sync_controls(con)


# ─────────────────────────────────────────────────────────────────────────────
# Empty state — first-time setup
# ─────────────────────────────────────────────────────────────────────────────

def _render_empty_state(con):
    """Show setup instructions when no commodity data exists."""
    st.info(
        "No commodity data found. Run the initial sync to populate data.\n\n"
        "**CLI:** `pfsync commodity sync --all`\n\n"
        "Or use the sync button below."
    )

    if st.button("Seed Commodity Universe & Sync (yfinance)", type="primary"):
        with st.spinner("Seeding commodity universe and syncing from yfinance..."):
            try:
                from pakfindata.commodities.sync import seed_commodity_universe, sync_yfinance
                seed_commodity_universe()
                summary = sync_yfinance(incremental=False, period="1y")
                st.success(
                    f"Synced {summary.symbols_ok}/{summary.symbols_total} commodities, "
                    f"{summary.rows_upserted} rows upserted."
                )
                if summary.errors:
                    with st.expander("Errors"):
                        for sym, err in summary.errors:
                            st.text(f"{sym}: {err}")
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Tab: Dashboard — KPIs + sparklines
# ─────────────────────────────────────────────────────────────────────────────

def _render_dashboard(con):
    """Show headline KPIs for key commodities."""
    st.markdown("### Market Overview")

    # Key commodities for Pakistan context
    key_symbols = ["GOLD", "BRENT", "COTTON", "WHEAT", "NATURAL_GAS", "USD_PKR", "SUGAR", "COPPER"]

    # Fetch latest prices
    latest = {}
    for sym in key_symbols:
        row = con.execute(
            """
            SELECT symbol, date, close, open
            FROM commodity_eod WHERE symbol=? AND source='yfinance'
            ORDER BY date DESC LIMIT 1
            """,
            (sym,),
        ).fetchone()
        if not row:
            # Try FX table
            row = con.execute(
                """
                SELECT pair as symbol, date, close, open
                FROM commodity_fx_rates WHERE pair=?
                ORDER BY date DESC LIMIT 1
                """,
                (sym,),
            ).fetchone()
        if row:
            latest[sym] = dict(row)

    if not latest:
        st.warning("No recent commodity data. Run a sync first.")
        return

    # Load names from config
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    # KPI cards — 4 per row
    cols = st.columns(4)
    for i, sym in enumerate(key_symbols):
        if sym not in latest:
            continue
        data = latest[sym]
        cdef = COMMODITY_UNIVERSE.get(sym)
        name = cdef.name if cdef else sym
        unit = cdef.unit if cdef else ""

        price = data.get("close")
        prev = data.get("open")
        delta = None
        if price and prev and prev != 0:
            delta = f"{((price - prev) / prev) * 100:.2f}%"

        with cols[i % 4]:
            st.metric(
                label=f"{name} ({unit})",
                value=f"{price:,.2f}" if price else "N/A",
                delta=delta,
            )

    # Sparkline charts for key commodities
    st.markdown("### 30-Day Trends")
    spark_cols = st.columns(4)
    for i, sym in enumerate(key_symbols[:4]):
        with spark_cols[i]:
            _render_sparkline(con, sym)

    # PKR prices section
    pkr_rows = con.execute(
        """
        SELECT cp.symbol, cp.date, cp.pkr_price, cp.pk_unit, cp.usd_pkr
        FROM commodity_pkr cp
        INNER JOIN (
            SELECT symbol, MAX(date) as max_date FROM commodity_pkr GROUP BY symbol
        ) latest ON cp.symbol=latest.symbol AND cp.date=latest.max_date
        ORDER BY cp.symbol
        """
    ).fetchall()

    if pkr_rows:
        st.markdown("### Pakistan Prices (PKR)")
        pkr_df = pd.DataFrame([dict(r) for r in pkr_rows])
        # Add human-readable names
        pkr_df["name"] = pkr_df["symbol"].map(
            lambda s: COMMODITY_UNIVERSE[s].name if s in COMMODITY_UNIVERSE else s
        )
        pkr_df = pkr_df[["name", "symbol", "pkr_price", "pk_unit", "date", "usd_pkr"]]
        pkr_df.columns = ["Commodity", "Symbol", "PKR Price", "Unit", "Date", "USD/PKR"]
        st.dataframe(pkr_df, use_container_width=True, hide_index=True)


def _render_sparkline(con, symbol: str):
    """Render a mini sparkline chart for a commodity."""
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    rows = con.execute(
        """
        SELECT date, close FROM commodity_eod
        WHERE symbol=? AND source='yfinance' AND close IS NOT NULL
        ORDER BY date DESC LIMIT 30
        """,
        (symbol,),
    ).fetchall()

    if not rows:
        # Try FX table
        rows = con.execute(
            """
            SELECT date, close FROM commodity_fx_rates
            WHERE pair=? AND close IS NOT NULL
            ORDER BY date DESC LIMIT 30
            """,
            (symbol,),
        ).fetchall()

    if not rows:
        return

    df = pd.DataFrame([dict(r) for r in rows]).sort_values("date")
    cdef = COMMODITY_UNIVERSE.get(symbol)
    name = cdef.name if cdef else symbol

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["close"],
        mode="lines", fill="tozeroy",
        line=dict(width=1.5, color="#00d4aa"),
        fillcolor="rgba(0, 212, 170, 0.1)",
    ))
    fig.update_layout(
        title=dict(text=name, font=dict(size=12)),
        height=120, margin=dict(l=0, r=0, t=25, b=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Tab: Charts — Interactive commodity charts
# ─────────────────────────────────────────────────────────────────────────────

def _render_charts(con):
    """Interactive candlestick/line chart for any commodity."""
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    st.markdown("### Commodity Price Charts")

    # Symbol selector
    all_symbols = [r["symbol"] for r in con.execute(
        "SELECT DISTINCT symbol FROM commodity_eod ORDER BY symbol"
    ).fetchall()]
    fx_symbols = [r["pair"] for r in con.execute(
        "SELECT DISTINCT pair FROM commodity_fx_rates ORDER BY pair"
    ).fetchall()]

    all_available = sorted(set(all_symbols + fx_symbols))
    if not all_available:
        st.info("No commodity data available. Run a sync first.")
        return

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        selected = st.selectbox(
            "Commodity",
            all_available,
            format_func=lambda s: f"{COMMODITY_UNIVERSE[s].name} ({s})" if s in COMMODITY_UNIVERSE else s,
        )
    with col2:
        chart_type = st.radio("Chart Type", ["Line", "Candlestick"], horizontal=True)
    with col3:
        period = st.selectbox("Period", ["30d", "90d", "180d", "1y", "All"], index=3)

    if not selected:
        return

    # Fetch data
    limit_map = {"30d": 30, "90d": 90, "180d": 180, "1y": 365, "All": 10000}
    limit = limit_map.get(period, 365)

    # Try commodity_eod first, then fx
    rows = con.execute(
        """
        SELECT date, open, high, low, close, volume FROM commodity_eod
        WHERE symbol=? AND source='yfinance'
        ORDER BY date DESC LIMIT ?
        """,
        (selected, limit),
    ).fetchall()

    if not rows:
        rows = con.execute(
            """
            SELECT date, open, high, low, close, volume FROM commodity_fx_rates
            WHERE pair=?
            ORDER BY date DESC LIMIT ?
            """,
            (selected, limit),
        ).fetchall()

    if not rows:
        st.info(f"No data for {selected}")
        return

    df = pd.DataFrame([dict(r) for r in rows]).sort_values("date")
    cdef = COMMODITY_UNIVERSE.get(selected)
    title = f"{cdef.name} ({cdef.unit})" if cdef else selected

    if chart_type == "Candlestick" and all(col in df.columns for col in ["open", "high", "low", "close"]):
        fig = go.Figure(data=[go.Candlestick(
            x=df["date"],
            open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
        )])
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["close"],
            mode="lines", name="Close",
            line=dict(width=2, color="#00d4aa"),
        ))

    fig.update_layout(
        title=title, height=500,
        xaxis_title="Date", yaxis_title="Price",
        xaxis_rangeslider_visible=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    with st.expander("Raw Data"):
        st.dataframe(df.sort_values("date", ascending=False), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# Tab: Categories — Browse by category
# ─────────────────────────────────────────────────────────────────────────────

def _render_categories(con):
    """Browse commodities by category with daily change heatmap."""
    from pakfindata.commodities.config import COMMODITY_UNIVERSE, CATEGORIES

    st.markdown("### Browse by Category")

    selected_cat = st.selectbox("Category", ["All"] + CATEGORIES)

    # Get latest data for all commodities
    rows = con.execute(
        """
        SELECT e.symbol, e.date, e.close, e.open, e.volume,
               cs.name, cs.category, cs.unit, cs.pk_relevance
        FROM commodity_eod e
        INNER JOIN (
            SELECT symbol, MAX(date) as max_date FROM commodity_eod
            WHERE source='yfinance' GROUP BY symbol
        ) latest ON e.symbol=latest.symbol AND e.date=latest.max_date
        LEFT JOIN commodity_symbols cs ON e.symbol=cs.symbol
        WHERE e.source='yfinance'
        ORDER BY cs.category, cs.name
        """
    ).fetchall()

    if not rows:
        st.info("No commodity data. Run a sync first.")
        return

    df = pd.DataFrame([dict(r) for r in rows])

    # Filter by category
    if selected_cat != "All":
        df = df[df["category"] == selected_cat]

    if df.empty:
        st.info(f"No data for category: {selected_cat}")
        return

    # Compute daily change
    df["change_pct"] = ((df["close"] - df["open"]) / df["open"] * 100).round(2)
    df["change_pct"] = df["change_pct"].fillna(0)

    # Display as styled table
    display_df = df[["name", "symbol", "category", "close", "change_pct", "unit", "pk_relevance", "date"]].copy()
    display_df.columns = ["Commodity", "Symbol", "Category", "Price", "Change %", "Unit", "PK Relevance", "Date"]

    st.dataframe(
        display_df.style.applymap(
            lambda v: "color: #00d4aa" if isinstance(v, (int, float)) and v > 0
            else "color: #ff4444" if isinstance(v, (int, float)) and v < 0
            else "",
            subset=["Change %"],
        ),
        use_container_width=True,
        hide_index=True,
    )

    # Heatmap
    if len(df) > 3:
        st.markdown("### Daily Change Heatmap")
        heatmap_df = df[["name", "change_pct"]].set_index("name")

        fig = px.bar(
            df.sort_values("change_pct"),
            x="change_pct", y="name",
            orientation="h",
            color="change_pct",
            color_continuous_scale=["#ff4444", "#333333", "#00d4aa"],
            color_continuous_midpoint=0,
        )
        fig.update_layout(
            height=max(300, len(df) * 25),
            xaxis_title="Daily Change %",
            yaxis_title="",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Tab: Pakistan View — PKR prices in local units
# ─────────────────────────────────────────────────────────────────────────────

def _render_pakistan_view(con):
    """Show all commodity prices in PKR with Pakistan local units."""
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    st.markdown("### Pakistan Commodity Prices")
    st.caption("Prices converted to PKR using the latest USD/PKR exchange rate")

    # Get latest PKR data
    pkr_rows = con.execute(
        """
        SELECT cp.symbol, cp.date, cp.pkr_price, cp.pk_unit, cp.usd_price, cp.usd_pkr, cp.source
        FROM commodity_pkr cp
        INNER JOIN (
            SELECT symbol, MAX(date) as max_date FROM commodity_pkr GROUP BY symbol
        ) latest ON cp.symbol=latest.symbol AND cp.date=latest.max_date
        ORDER BY cp.symbol
        """
    ).fetchall()

    if not pkr_rows:
        st.info(
            "No PKR prices computed yet. These are generated after syncing commodity "
            "and FX data.\n\n**CLI:** `pfsync commodity sync --all`"
        )
        return

    df = pd.DataFrame([dict(r) for r in pkr_rows])
    df["name"] = df["symbol"].map(
        lambda s: COMMODITY_UNIVERSE[s].name if s in COMMODITY_UNIVERSE else s
    )

    # Display
    display = df[["name", "symbol", "pkr_price", "pk_unit", "usd_price", "usd_pkr", "date", "source"]].copy()
    display.columns = ["Commodity", "Symbol", "PKR Price", "Unit", "USD Price", "USD/PKR", "Date", "Source"]
    display["PKR Price"] = display["PKR Price"].apply(lambda x: f"{x:,.0f}" if x else "N/A")

    st.dataframe(display, use_container_width=True, hide_index=True)

    # Gold/silver focus
    gold_row = df[df["symbol"] == "GOLD"]
    silver_row = df[df["symbol"] == "SILVER"]

    if not gold_row.empty or not silver_row.empty:
        st.markdown("### Precious Metals (PKR/Tola)")
        m_cols = st.columns(2)
        if not gold_row.empty:
            with m_cols[0]:
                st.metric("Gold (per Tola)", f"PKR {gold_row.iloc[0]['pkr_price']:,.0f}")
        if not silver_row.empty:
            with m_cols[1]:
                st.metric("Silver (per Tola)", f"PKR {silver_row.iloc[0]['pkr_price']:,.0f}")


# ─────────────────────────────────────────────────────────────────────────────
# Tab: Local Markets — khistocks.com Pakistan data
# ─────────────────────────────────────────────────────────────────────────────

_FEED_LABELS = {
    "khistocks_pmex": "PMEX Commodity Exchange",
    "khistocks_sarafa": "Karachi Sarafa Bazaar",
    "khistocks_intl_bullion": "International Bullion",
    "khistocks_mandi": "Lahore Akbari Mandi",
    "khistocks_lme": "London Metal Exchange (LME)",
}


def _render_local_markets(con):
    """Show Pakistan local market data from khistocks.com (Business Recorder)."""
    st.markdown("### Pakistan Local Markets")
    st.caption("Data from khistocks.com — PMEX, Karachi Sarafa, Akbari Mandi, LME")

    # Check if khistocks data exists
    try:
        khi_count = con.execute("SELECT COUNT(*) as c FROM khistocks_prices").fetchone()["c"]
    except Exception:
        khi_count = 0

    if khi_count == 0:
        st.info(
            "No local market data found. Sync from khistocks.com first.\n\n"
            "**CLI:** `pfsync commodity sync --source khistocks`\n\n"
            "Or use the sync button in the controls below."
        )
        return

    # Feed selector
    feeds = con.execute(
        "SELECT DISTINCT feed FROM khistocks_prices ORDER BY feed"
    ).fetchall()
    feed_list = [r["feed"] for r in feeds]

    selected_feed = st.selectbox(
        "Market Feed",
        ["All Feeds"] + feed_list,
        format_func=lambda f: _FEED_LABELS.get(f, f) if f != "All Feeds" else "All Feeds",
        key="khi_feed_select",
    )

    # Get latest data (inline query to avoid import issues with Streamlit caching)
    if selected_feed == "All Feeds":
        rows = con.execute(
            """
            SELECT kp.* FROM khistocks_prices kp
            INNER JOIN (
                SELECT symbol, feed, MAX(date) as max_date FROM khistocks_prices
                GROUP BY symbol, feed
            ) latest ON kp.symbol=latest.symbol AND kp.feed=latest.feed AND kp.date=latest.max_date
            ORDER BY kp.feed, kp.symbol
            """
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT kp.* FROM khistocks_prices kp
            INNER JOIN (
                SELECT symbol, feed, MAX(date) as max_date FROM khistocks_prices
                WHERE feed=? GROUP BY symbol, feed
            ) latest ON kp.symbol=latest.symbol AND kp.feed=latest.feed AND kp.date=latest.max_date
            WHERE kp.feed=?
            ORDER BY kp.symbol
            """,
            (selected_feed, selected_feed),
        ).fetchall()
    rows = [dict(r) for r in rows]

    if not rows:
        st.info("No data for selected feed.")
        return

    df = pd.DataFrame(rows)
    df["feed_label"] = df["feed"].map(lambda f: _FEED_LABELS.get(f, f))

    # Show by feed group
    for feed_name, group in df.groupby("feed"):
        label = _FEED_LABELS.get(feed_name, feed_name)
        st.markdown(f"#### {label}")

        # Determine display columns based on feed type
        if "lme" in feed_name:
            cols = ["symbol", "name", "date", "cash_buyer", "cash_seller",
                    "three_month_buyer", "three_month_seller", "net_change", "change_pct"]
            col_names = ["Symbol", "Metal", "Date", "Cash Buyer", "Cash Seller",
                         "3M Buyer", "3M Seller", "Net Change", "Change %"]
        elif "sarafa" in feed_name or "bullion" in feed_name:
            cols = ["symbol", "name", "date", "open", "high", "low", "close",
                    "net_change", "change_pct"]
            col_names = ["Symbol", "Instrument", "Date", "Open", "High", "Low",
                         "Close", "Net Change", "Change %"]
        elif "mandi" in feed_name:
            cols = ["symbol", "name", "date", "rate", "quotation",
                    "net_change", "change_pct"]
            col_names = ["Symbol", "Commodity", "Date", "Rate", "Unit",
                         "Net Change", "Change %"]
        else:  # pmex
            cols = ["symbol", "name", "date", "open", "high", "low", "close",
                    "quotation", "net_change", "change_pct"]
            col_names = ["Symbol", "Commodity", "Date", "Open", "High", "Low",
                         "Close", "Unit", "Net Change", "Change %"]

        # Filter to available columns
        available = [c for c in cols if c in group.columns]
        display = group[available].copy()
        # Rename columns that exist
        rename_map = dict(zip(cols, col_names))
        display = display.rename(columns={c: rename_map[c] for c in available if c in rename_map})
        # Drop columns that are all NaN
        display = display.dropna(axis=1, how="all")

        st.dataframe(display, use_container_width=True, hide_index=True)

    # History drill-down
    st.markdown("---")
    st.markdown("#### Price History")
    all_symbols = sorted(df["symbol"].unique())
    selected_sym = st.selectbox("Select Symbol", all_symbols, key="khi_sym_history")

    if selected_sym:
        history = [dict(r) for r in con.execute(
            "SELECT * FROM khistocks_prices WHERE symbol=? ORDER BY date DESC LIMIT 90",
            (selected_sym,),
        ).fetchall()]
        if history:
            hist_df = pd.DataFrame(history).sort_values("date")
            # Plot close or rate
            price_col = "close" if hist_df["close"].notna().any() else "rate"
            if hist_df[price_col].notna().any():
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=hist_df["date"], y=hist_df[price_col],
                    mode="lines+markers",
                    line=dict(width=2, color="#00d4aa"),
                    name=selected_sym,
                ))
                fig.update_layout(
                    title=f"{selected_sym} — Price History",
                    height=350,
                    xaxis_title="Date",
                    yaxis_title="Price",
                )
                st.plotly_chart(fig, use_container_width=True)

            with st.expander("Raw Data"):
                st.dataframe(
                    hist_df.sort_values("date", ascending=False),
                    use_container_width=True, hide_index=True,
                )
        else:
            st.info(f"No history for {selected_sym}")


# ─────────────────────────────────────────────────────────────────────────────
# Tab: PMEX Portal — direct PMEX market watch data
# ─────────────────────────────────────────────────────────────────────────────

_PMEX_CATEGORY_LABELS = {
    "Indices": "PMEX Indices",
    "Metals": "Precious & Base Metals",
    "Oil": "Crude Oil & Petroleum",
    "Cots": "Cotton Contracts",
    "Energy": "Energy",
    "Agri": "Agriculture Futures",
    "Phy_Agri": "Physical Agriculture",
    "Phy_Gold": "Physical Gold",
    "Financials": "Financial Futures",
}


def _render_pmex_portal(con):
    """Show PMEX market watch data from the direct portal API."""
    st.markdown("### PMEX Market Watch")
    st.caption("Direct from PMEX Portal — 134 instruments across 9 categories (Bid/Ask/OHLCV)")

    try:
        pmex_count = con.execute("SELECT COUNT(*) as c FROM pmex_market_watch").fetchone()["c"]
    except Exception:
        pmex_count = 0

    if pmex_count == 0:
        st.info(
            "No PMEX data found. Sync from the PMEX portal first.\n\n"
            "**CLI:** `pfsync commodity sync --source pmex_portal`\n\n"
            "Or use the sync button in the controls below."
        )
        return

    # Category selector
    categories = con.execute(
        "SELECT DISTINCT category FROM pmex_market_watch ORDER BY category"
    ).fetchall()
    cat_list = [r["category"] for r in categories]

    selected_cat = st.selectbox(
        "Category",
        ["All Categories"] + cat_list,
        format_func=lambda c: _PMEX_CATEGORY_LABELS.get(c, c) if c != "All Categories" else "All Categories",
        key="pmex_cat_select",
    )

    # Get latest data (inline SQL to avoid import caching issues)
    if selected_cat == "All Categories":
        rows = con.execute("""
            SELECT p.* FROM pmex_market_watch p
            INNER JOIN (
                SELECT contract, MAX(snapshot_date) as max_date FROM pmex_market_watch
                GROUP BY contract
            ) latest ON p.contract=latest.contract AND p.snapshot_date=latest.max_date
            ORDER BY p.category, p.contract
        """).fetchall()
    else:
        rows = con.execute("""
            SELECT p.* FROM pmex_market_watch p
            INNER JOIN (
                SELECT contract, MAX(snapshot_date) as max_date FROM pmex_market_watch
                WHERE category=? GROUP BY contract
            ) latest ON p.contract=latest.contract AND p.snapshot_date=latest.max_date
            WHERE p.category=?
            ORDER BY p.contract
        """, (selected_cat, selected_cat)).fetchall()

    rows = [dict(r) for r in rows]
    if not rows:
        st.info("No data for selected category.")
        return

    df = pd.DataFrame(rows)

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Contracts", len(df))
    with col2:
        st.metric("Categories", df["category"].nunique())
    with col3:
        active = len(df[df["total_vol"].fillna(0) > 0]) if "total_vol" in df.columns else 0
        st.metric("With Volume", active)
    with col4:
        total_vol = df["total_vol"].fillna(0).sum() if "total_vol" in df.columns else 0
        st.metric("Total Volume", f"{total_vol:,.0f}")

    # Display by category
    for cat_name, group in df.groupby("category"):
        label = _PMEX_CATEGORY_LABELS.get(cat_name, cat_name)
        st.markdown(f"#### {label} ({len(group)} contracts)")

        cols = ["contract", "snapshot_date", "bid", "ask", "last_price",
                "open", "close", "high", "low", "change", "change_pct",
                "total_vol", "state"]
        available = [c for c in cols if c in group.columns]
        display = group[available].copy()
        display = display.rename(columns={
            "contract": "Contract", "snapshot_date": "Date",
            "bid": "Bid", "ask": "Ask", "last_price": "Last",
            "open": "Open", "close": "Close", "high": "High", "low": "Low",
            "change": "Change", "change_pct": "Chg%",
            "total_vol": "Volume", "state": "State",
        })
        display = display.dropna(axis=1, how="all")
        st.dataframe(display, use_container_width=True, hide_index=True)

    # Contract history drill-down
    st.markdown("---")
    st.markdown("#### Contract History")
    all_contracts = sorted(df["contract"].unique())
    selected_contract = st.selectbox("Select Contract", all_contracts, key="pmex_contract_history")

    if selected_contract:
        history = [dict(r) for r in con.execute(
            "SELECT * FROM pmex_market_watch WHERE contract=? ORDER BY snapshot_date DESC LIMIT 90",
            (selected_contract,),
        ).fetchall()]
        if history:
            hist_df = pd.DataFrame(history).sort_values("snapshot_date")
            price_col = "last_price" if hist_df["last_price"].notna().any() else "close"
            if hist_df[price_col].notna().any():
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=hist_df["snapshot_date"], y=hist_df[price_col],
                    mode="lines+markers",
                    line=dict(width=2, color="#ff6b35"),
                    name=selected_contract,
                ))
                if hist_df["bid"].notna().any() and hist_df["ask"].notna().any():
                    fig.add_trace(go.Scatter(
                        x=hist_df["snapshot_date"], y=hist_df["bid"],
                        mode="lines", line=dict(width=1, dash="dash", color="#00d4aa"),
                        name="Bid",
                    ))
                    fig.add_trace(go.Scatter(
                        x=hist_df["snapshot_date"], y=hist_df["ask"],
                        mode="lines", line=dict(width=1, dash="dash", color="#ff4444"),
                        name="Ask",
                    ))
                fig.update_layout(
                    title=f"{selected_contract} — Price History",
                    height=350,
                    xaxis_title="Date",
                    yaxis_title="Price",
                )
                st.plotly_chart(fig, use_container_width=True)

            with st.expander("Raw Data"):
                st.dataframe(
                    hist_df.sort_values("snapshot_date", ascending=False),
                    use_container_width=True, hide_index=True,
                )
        else:
            st.info(f"No history for {selected_contract}")


# ─────────────────────────────────────────────────────────────────────────────
# Tab: Export
# ─────────────────────────────────────────────────────────────────────────────

def _render_export(con):
    """CSV download for commodity data."""
    st.markdown("### Export Commodity Data")

    export_type = st.selectbox("Data Set", [
        "Daily OHLCV (yfinance)",
        "Monthly Benchmarks (FRED/WorldBank)",
        "PKR Prices",
        "FX Rates",
        "Local Markets (khistocks)",
        "PMEX Market Watch",
    ])

    if export_type == "Daily OHLCV (yfinance)":
        df = pd.read_sql("SELECT * FROM commodity_eod ORDER BY symbol, date DESC", con)
    elif export_type == "Monthly Benchmarks (FRED/WorldBank)":
        df = pd.read_sql("SELECT * FROM commodity_monthly ORDER BY symbol, date DESC", con)
    elif export_type == "PKR Prices":
        df = pd.read_sql("SELECT * FROM commodity_pkr ORDER BY symbol, date DESC", con)
    elif export_type == "Local Markets (khistocks)":
        df = pd.read_sql("SELECT * FROM khistocks_prices ORDER BY feed, symbol, date DESC", con)
    elif export_type == "PMEX Market Watch":
        try:
            df = pd.read_sql("SELECT * FROM pmex_market_watch ORDER BY category, contract, snapshot_date DESC", con)
        except Exception:
            df = pd.DataFrame()
    else:
        df = pd.read_sql("SELECT * FROM commodity_fx_rates ORDER BY pair, date DESC", con)

    if df.empty:
        st.info("No data available for this export.")
        return

    st.text(f"{len(df)} rows")
    st.dataframe(df.head(100), use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False)
    st.download_button(
        "Download CSV",
        csv,
        file_name=f"pakfindata_commodity_{export_type.split('(')[0].strip().lower().replace(' ', '_')}.csv",
        mime="text/csv",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sync controls
# ─────────────────────────────────────────────────────────────────────────────

def _render_sync_controls(con):
    """Sync buttons for commodity data sources."""
    with st.expander("Sync Commodity Data"):
        st.caption("Fetch latest commodity prices from free data sources.")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            if st.button("Sync yfinance (Daily)", type="primary", key="cmd_sync_yf"):
                with st.spinner("Syncing from yfinance..."):
                    try:
                        from pakfindata.commodities.sync import sync_yfinance
                        summary = sync_yfinance(incremental=True)
                        st.success(
                            f"yfinance: {summary.symbols_ok}/{summary.symbols_total} symbols, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"yfinance sync failed: {e}")

        with col2:
            if st.button("Sync FRED (Monthly)", key="cmd_sync_fred"):
                with st.spinner("Syncing from FRED..."):
                    try:
                        from pakfindata.commodities.sync import sync_fred
                        summary = sync_fred()
                        st.success(
                            f"FRED: {summary.symbols_ok}/{summary.symbols_total} series, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                    except ImportError:
                        st.warning("fredapi not installed. Run: `pip install fredapi`")
                    except ValueError as e:
                        st.warning(str(e))
                    except Exception as e:
                        st.error(f"FRED sync failed: {e}")

        with col3:
            if st.button("Sync World Bank", key="cmd_sync_wb"):
                with st.spinner("Downloading World Bank Pink Sheet..."):
                    try:
                        from pakfindata.commodities.sync import sync_worldbank
                        summary = sync_worldbank()
                        st.success(
                            f"World Bank: {summary.symbols_ok} commodities, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"World Bank sync failed: {e}")

        with col4:
            if st.button("Sync khistocks (PK)", key="cmd_sync_khi"):
                with st.spinner("Syncing from khistocks.com..."):
                    try:
                        from pakfindata.commodities.sync import sync_khistocks
                        summary = sync_khistocks()
                        st.success(
                            f"khistocks: {summary.symbols_ok}/{summary.symbols_total} symbols, "
                            f"{summary.rows_upserted} rows"
                        )
                        if summary.errors:
                            with st.expander("Errors"):
                                for sym, err in summary.errors:
                                    st.text(f"{sym}: {err}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"khistocks sync failed: {e}")

        with col5:
            if st.button("Sync PMEX Portal", key="cmd_sync_pmex"):
                with st.spinner("Fetching PMEX market data..."):
                    try:
                        from pakfindata.commodities.sync import sync_pmex
                        summary = sync_pmex()
                        st.success(
                            f"PMEX: {summary.symbols_ok}/{summary.symbols_total} contracts, "
                            f"{summary.rows_upserted} rows"
                        )
                        if summary.errors:
                            with st.expander("Errors"):
                                for sym, err in summary.errors:
                                    st.text(f"{sym}: {err}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"PMEX sync failed: {e}")

        # PKR conversion
        if st.button("Compute PKR Prices", key="cmd_pkr"):
            with st.spinner("Computing PKR prices..."):
                try:
                    from pakfindata.commodities.sync import compute_pkr_prices
                    n = compute_pkr_prices()
                    st.success(f"Computed {n} PKR price rows")
                    st.rerun()
                except Exception as e:
                    st.error(f"PKR computation failed: {e}")

        # Show sync history
        sync_rows = con.execute(
            "SELECT * FROM commodity_sync_runs ORDER BY started_at DESC LIMIT 10"
        ).fetchall()
        if sync_rows:
            st.markdown("#### Recent Sync Runs")
            sync_df = pd.DataFrame([dict(r) for r in sync_rows])
            st.dataframe(sync_df, use_container_width=True, hide_index=True)
