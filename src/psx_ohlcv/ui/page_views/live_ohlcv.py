"""Live Tick OHLCV Builder — polls market-watch and builds real OHLCV from ticks.

Solves the fake H/L problem: tick-collected OHLCV gives REAL high/low values.
"""

import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from psx_ohlcv.collectors.tick_collector import TickCollector
from psx_ohlcv.db.connection import connect, init_schema
from psx_ohlcv.db.repositories.tick import (
    cleanup_old_ticks,
    init_tick_schema,
    promote_tick_ohlcv_to_eod,
)
from psx_ohlcv.sync_timeseries import (
    is_intraday_sync_running,
    read_intraday_sync_progress,
    start_intraday_sync,
)
from psx_ohlcv.ui.components.helpers import get_connection, render_footer


# =====================================================================
# Session State Helpers
# =====================================================================

def _get_collector() -> TickCollector:
    """Get or create the TickCollector in session state."""
    if "tick_collector" not in st.session_state:
        st.session_state.tick_collector = TickCollector()
    return st.session_state.tick_collector


def _init_state():
    """Initialize session state keys."""
    defaults = {
        "tick_auto_poll": False,
        "tick_collector": TickCollector(),
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# =====================================================================
# Main Render
# =====================================================================

def render_live_ohlcv():
    """Live Tick OHLCV Builder page."""
    _init_state()
    collector = _get_collector()

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## Live Tick OHLCV Builder")
    st.caption(
        "Polls PSX market-watch every 5s and builds real OHLCV from ticks. "
        "Solves the fake H/L problem."
    )

    # =================================================================
    # CONTROLS ROW
    # =================================================================
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        auto_poll = st.toggle(
            "Auto-Poll (5s)",
            value=st.session_state.tick_auto_poll,
            key="tick_auto_poll_toggle",
        )
        st.session_state.tick_auto_poll = auto_poll

    with c2:
        if st.button("Poll Once", use_container_width=True):
            stats = collector.poll_once()
            st.toast(
                f"Polled: {stats['new_ticks']} new ticks, "
                f"{stats['skipped']} skipped"
            )

    with c3:
        if st.button("Save OHLCV to DB", use_container_width=True):
            n = collector.save_ohlcv_to_db()
            if n:
                st.toast(f"Saved {n} OHLCV rows to tick_ohlcv table")
            else:
                st.toast("No data to save")

    with c4:
        syncing = is_intraday_sync_running()
        if st.button(
            "Syncing..." if syncing else "Sync Intraday Tables",
            use_container_width=True,
            disabled=syncing,
        ):
            start_intraday_sync()
            st.toast("Intraday sync started (intraday_bars + tick_data)")
            st.rerun()

    with c5:
        if st.button("Promote to EOD", use_container_width=True):
            con = get_connection()
            if con:
                init_tick_schema(con)
                # First save running OHLCV
                collector.save_ohlcv_to_db()
                n = promote_tick_ohlcv_to_eod(con)
                st.toast(f"Promoted {n} rows to eod_ohlcv (source=tick_aggregation)")

    with c6:
        if st.button("Reset", use_container_width=True):
            collector.reset()
            st.session_state.tick_auto_poll = False
            st.toast("Collector reset")
            st.rerun()

    # Show sync progress if running
    if is_intraday_sync_running():
        prog = read_intraday_sync_progress()
        if prog and prog.get("total"):
            pct = prog["current"] / prog["total"]
            st.progress(pct, text=f"Syncing {prog.get('current_symbol', '')} ({prog['current']}/{prog['total']})")
    elif read_intraday_sync_progress():
        prog = read_intraday_sync_progress()
        if prog and prog.get("status") == "completed":
            st.success(
                f"Last sync: {prog['ok']}/{prog['total']} ok, "
                f"{prog['rows_total']:,} rows | {prog.get('finished_at', '')[:19]}"
            )

    st.divider()

    # =================================================================
    # STATUS METRICS
    # =================================================================
    stats = collector.get_stats()
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Polls", stats["poll_count"])
    m2.metric("Symbols", stats["symbols_tracked"])
    m3.metric(
        "Last Poll",
        stats["last_poll_time"][:19] if stats["last_poll_time"] else "—",
    )
    m4.metric(
        "Started",
        stats["started_at"][:19] if stats["started_at"] else "—",
    )
    m5.metric("Total Ticks", stats["total_ticks"])

    st.divider()

    # =================================================================
    # TABS
    # =================================================================
    tab1, tab2, tab3 = st.tabs([
        "Symbol Deep View",
        "Running OHLCV Table",
        "Raw Market Watch",
    ])

    with tab1:
        _render_symbol_deep_view(collector)

    with tab2:
        _render_running_ohlcv_table(collector)

    with tab3:
        _render_raw_market_watch(collector)

    # =================================================================
    # AUTO-POLL LOOP
    # =================================================================
    if st.session_state.tick_auto_poll:
        collector.poll_once()
        time.sleep(5)
        st.rerun()


# =====================================================================
# Tab 1: Symbol Deep View
# =====================================================================

def _render_symbol_deep_view(collector: TickCollector):
    """Symbol-level deep view with tick build log and chart."""
    ohlcv = collector.get_running_ohlcv()
    symbols = sorted(ohlcv.keys()) if ohlcv else []

    if not symbols:
        st.info("No data yet. Click 'Poll Once' or enable Auto-Poll to start collecting.")
        return

    # Symbol selector
    col1, col2 = st.columns([1, 3])
    with col1:
        default_idx = symbols.index("HBL") if "HBL" in symbols else 0
        selected = st.selectbox(
            "Symbol",
            symbols,
            index=default_idx,
            key="tick_symbol_select",
        )

    sym_ohlcv = ohlcv.get(selected, {})
    if not sym_ohlcv:
        st.warning(f"No OHLCV data for {selected}")
        return

    # -----------------------------------------------------------------
    # KPI Cards
    # -----------------------------------------------------------------
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("OPEN", f"{sym_ohlcv.get('open', 0):,.2f}")
    k2.metric("HIGH", f"{sym_ohlcv.get('high', 0):,.2f}")
    k3.metric("LOW", f"{sym_ohlcv.get('low', 0):,.2f}")
    k4.metric("CLOSE", f"{sym_ohlcv.get('close', 0):,.2f}")
    k5.metric("VOLUME", f"{sym_ohlcv.get('volume', 0):,}")
    k6.metric("TICKS", sym_ohlcv.get("tick_count", 0))

    # -----------------------------------------------------------------
    # Tick Build Log
    # -----------------------------------------------------------------
    st.markdown("#### Incremental Build Log")
    ticks = collector.get_tick_history(selected)

    if ticks:
        log_rows = _build_tick_log(ticks)
        if log_rows:
            log_df = pd.DataFrame(log_rows)

            # Show last 20 rows
            display_df = log_df.tail(20)
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=min(400, 35 * len(display_df) + 38),
            )

            if len(log_df) > 20:
                with st.expander(f"Full history ({len(log_df)} ticks)"):
                    st.dataframe(log_df, use_container_width=True, hide_index=True)

            # CSV download
            csv = log_df.to_csv(index=False)
            st.download_button(
                f"Download {selected} ticks CSV",
                data=csv,
                file_name=f"{selected}_ticks_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
    else:
        st.info(f"No tick history for {selected}")

    # -----------------------------------------------------------------
    # Live Chart
    # -----------------------------------------------------------------
    if ticks and len(ticks) >= 2:
        st.markdown("#### Price Chart")
        _render_price_chart(selected, ticks, sym_ohlcv)


def _build_tick_log(ticks: list[dict]) -> list[dict]:
    """Build the incremental OHLCV build log from raw ticks.

    For each tick, shows what action it triggered (SET OPEN, NEW HIGH, NEW LOW, CLOSE updated).
    """
    if not ticks:
        return []

    log = []
    running_o = running_h = running_l = running_c = None

    for i, t in enumerate(ticks):
        price = t["price"]
        ts = datetime.fromtimestamp(t["timestamp"]).strftime("%H:%M:%S")
        vol = t.get("cumulative_volume", 0)

        actions = []

        if running_o is None:
            # First tick
            running_o = running_h = running_l = running_c = price
            actions.append("SET as OPEN")
        else:
            if price > running_h:
                old_h = running_h
                running_h = price
                actions.append(f"NEW HIGH ({old_h:,.2f} -> {running_h:,.2f})")
            if price < running_l:
                old_l = running_l
                running_l = price
                actions.append(f"NEW LOW ({old_l:,.2f} -> {running_l:,.2f})")
            running_c = price
            actions.append("CLOSE updated")

        log.append({
            "Tick#": i + 1,
            "Time": ts,
            "Price": f"{price:,.2f}",
            "Action": " | ".join(actions),
            "O": f"{running_o:,.2f}",
            "H": f"{running_h:,.2f}",
            "L": f"{running_l:,.2f}",
            "C": f"{running_c:,.2f}",
            "Vol": f"{vol:,}",
        })

    return log


def _render_price_chart(symbol: str, ticks: list[dict], ohlcv: dict):
    """Render price line chart with HIGH/LOW band and OPEN reference."""
    times = [datetime.fromtimestamp(t["timestamp"]) for t in ticks]
    prices = [t["price"] for t in ticks]
    volumes = [t.get("cumulative_volume", 0) for t in ticks]

    fig = go.Figure()

    # Price line
    fig.add_trace(go.Scatter(
        x=times, y=prices,
        mode="lines+markers",
        name="Price",
        line=dict(color="#2196F3", width=2),
        marker=dict(size=4),
    ))

    # HIGH line
    fig.add_hline(
        y=ohlcv["high"],
        line_dash="dot",
        line_color="#00C853",
        annotation_text=f"HIGH {ohlcv['high']:,.2f}",
        annotation_position="top right",
    )

    # LOW line
    fig.add_hline(
        y=ohlcv["low"],
        line_dash="dot",
        line_color="#FF1744",
        annotation_text=f"LOW {ohlcv['low']:,.2f}",
        annotation_position="bottom right",
    )

    # OPEN reference
    fig.add_hline(
        y=ohlcv["open"],
        line_dash="dash",
        line_color="#FFC107",
        annotation_text=f"OPEN {ohlcv['open']:,.2f}",
        annotation_position="top left",
    )

    fig.update_layout(
        title=f"{symbol} — Live Price",
        xaxis_title="Time",
        yaxis_title="Price (PKR)",
        template="plotly_dark",
        height=400,
        margin=dict(l=60, r=20, t=40, b=40),
        showlegend=False,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Volume bars
    if any(v > 0 for v in volumes):
        # Compute per-tick volume deltas
        vol_deltas = [volumes[0]]
        for i in range(1, len(volumes)):
            delta = max(0, volumes[i] - volumes[i - 1])
            vol_deltas.append(delta)

        colors = [
            "#00C853" if prices[i] >= prices[max(0, i - 1)] else "#FF1744"
            for i in range(len(prices))
        ]

        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=times, y=vol_deltas,
            marker_color=colors,
            name="Volume",
        ))
        fig_vol.update_layout(
            title=f"{symbol} — Volume per Tick",
            xaxis_title="Time",
            yaxis_title="Volume",
            template="plotly_dark",
            height=200,
            margin=dict(l=60, r=20, t=30, b=30),
            showlegend=False,
        )
        st.plotly_chart(fig_vol, use_container_width=True)


# =====================================================================
# Tab 2: Running OHLCV Table
# =====================================================================

def _render_running_ohlcv_table(collector: TickCollector):
    """Full table of all symbols with running OHLCV."""
    ohlcv = collector.get_running_ohlcv()
    if not ohlcv:
        st.info("No data yet. Click 'Poll Once' or enable Auto-Poll.")
        return

    # Controls
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        sort_by = st.selectbox(
            "Sort by",
            ["Chg%", "Spread", "Volume", "Ticks", "Symbol"],
            index=0,
            key="tick_ohlcv_sort",
        )
    with c2:
        sort_order = st.selectbox(
            "Order",
            ["Descending", "Ascending"],
            index=0,
            key="tick_ohlcv_order",
        )
    with c3:
        search = st.text_input("Filter symbol", key="tick_ohlcv_search")

    # Build DataFrame
    rows = []
    for sym, o in ohlcv.items():
        chg = o.get("change", 0) or 0
        chg_pct = o.get("change_pct", 0) or 0
        spread = o.get("high", 0) - o.get("low", 0)
        first_ts = datetime.fromtimestamp(o["first_ts"]).strftime("%H:%M:%S") if o.get("first_ts") else ""
        last_ts = datetime.fromtimestamp(o["last_ts"]).strftime("%H:%M:%S") if o.get("last_ts") else ""

        rows.append({
            "Symbol": sym,
            "Open": o.get("open", 0),
            "High": o.get("high", 0),
            "Low": o.get("low", 0),
            "Close": o.get("close", 0),
            "Chg": chg,
            "Chg%": chg_pct,
            "Spread": spread,
            "Volume": o.get("volume", 0),
            "Ticks": o.get("tick_count", 0),
            "First": first_ts,
            "Last": last_ts,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return

    # Filter
    if search:
        df = df[df["Symbol"].str.contains(search.upper(), na=False)]

    # Sort
    sort_map = {
        "Chg%": "Chg%",
        "Spread": "Spread",
        "Volume": "Volume",
        "Ticks": "Ticks",
        "Symbol": "Symbol",
    }
    col = sort_map.get(sort_by, "Chg%")
    ascending = sort_order == "Ascending"

    # For Chg% default, sort by absolute value descending
    if col == "Chg%":
        df["_abs_chg"] = df["Chg%"].abs()
        df = df.sort_values("_abs_chg", ascending=ascending).drop(columns=["_abs_chg"])
    else:
        df = df.sort_values(col, ascending=ascending)

    st.markdown(f"**{len(df)} symbols tracked**")

    # Color formatting function
    def _color_chg(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return "color: #00C853"
            elif val < 0:
                return "color: #FF1744"
        return ""

    styled = df.style.map(_color_chg, subset=["Chg", "Chg%"]).format({
        "Open": "{:,.2f}",
        "High": "{:,.2f}",
        "Low": "{:,.2f}",
        "Close": "{:,.2f}",
        "Chg": "{:+,.2f}",
        "Chg%": "{:+,.2f}%",
        "Spread": "{:,.2f}",
        "Volume": "{:,.0f}",
    })

    st.dataframe(styled, use_container_width=True, hide_index=True, height=600)


# =====================================================================
# Tab 3: Raw Market Watch
# =====================================================================

def _render_raw_market_watch(collector: TickCollector):
    """Show last raw JSON response from market-watch."""
    raw = collector.last_raw_response
    if not raw:
        st.info("No raw data yet. Poll at least once.")
        return

    st.markdown(f"**Last response: {len(raw)} items**")

    # First 3 items in expander
    with st.expander("Sample (first 3 items)"):
        for item in raw[:3]:
            st.json(item)

    # Full DataFrame
    try:
        df = pd.DataFrame(raw)
        st.dataframe(df, use_container_width=True, hide_index=True, height=500)
    except Exception as e:
        st.error(f"Could not render DataFrame: {e}")
