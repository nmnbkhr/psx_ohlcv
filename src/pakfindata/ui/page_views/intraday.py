"""Intraday trend analysis page."""

import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pathlib import Path

from pakfindata.config import get_db_path
from pakfindata.query import (
    get_intraday_latest,
    get_intraday_stats,
    get_symbols_list,
)
from pakfindata.services import (
    is_service_running,
    read_status as read_service_status,
)
from pakfindata.sync import sync_intraday
from pakfindata.sync_timeseries import (
    is_intraday_sync_running,
    read_intraday_sync_progress,
    start_intraday_sync,
)
from pakfindata.ui.charts import (
    make_intraday_chart,
    make_volume_chart,
)
from pakfindata.ui.components.helpers import (
    EXPORTS_DIR,
    format_volume,
    get_connection,
    render_footer,
    render_market_status_badge,
)

INTRADAY_TEMP_DIR = Path("/mnt/e/psxdata/intradaytemp")


def render_intraday():
    """Intraday price trend visualization and sync."""
    # =================================================================
    # AUTO-REFRESH WHEN SERVICE IS RUNNING
    # =================================================================
    service_running, service_pid = is_service_running()
    service_status = read_service_status()

    # Auto-refresh every 60 seconds if service is running and autorefresh is available
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        # Refresh every 60 seconds (60000 ms)
        count = st_autorefresh(interval=60000, limit=None, key="intraday_autorefresh")

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2, header_col3 = st.columns([2, 1, 1])
    with header_col1:
        st.markdown("## ⏱ Intraday Trend")
        st.caption("Live intraday price movements and volume throughout the trading day")
    with header_col2:
        render_market_status_badge()
    with header_col3:
        # Show service status
        if service_running:
            st.success("🟢 Auto-Sync ON")
            if service_status.last_run_at:
                last_sync = service_status.last_run_at[:19]
                st.caption(f"Last: {last_sync}")
        else:
            st.info("🔴 Auto-Sync OFF")
            st.caption("Start service on Data Sync page")

    # =================================================================
    # BULK INTRADAY SYNC (all symbols)
    # =================================================================
    with st.expander("PSX Intraday Ticks — All Symbols (Bulk)", expanded=False):
        running = is_intraday_sync_running()
        progress = read_intraday_sync_progress()

        if running and progress:
            pct = progress["current"] / max(progress["total"], 1)
            st.progress(pct, text=f"{progress['current']}/{progress['total']} — {progress.get('current_symbol', '')}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("OK", progress["ok"])
            c2.metric("Failed", progress["failed"])
            c3.metric("Ticks", f"{progress['rows_total']:,}")
            c4.metric("JSON Saved", progress.get("json_saved", 0))
            if st.button("Refresh", key="int_bulk_refresh"):
                st.rerun()
        else:
            if progress and progress.get("status") == "completed":
                json_info = ""
                if progress.get("json_saved", 0) > 0:
                    json_info = f", {progress['json_saved']} JSON files"
                trade_date = progress.get("trading_date", "")
                date_info = f" (trading date: {trade_date})" if trade_date else ""
                st.success(
                    f"Last run: {progress['ok']}/{progress['total']} symbols, "
                    f"{progress['rows_total']:,} ticks{json_info}{date_info} — {progress.get('finished_at', '')[:19]}"
                )
                if progress.get("json_dir"):
                    st.caption(f"JSON files: {progress['json_dir']}")

            from datetime import date as _date
            _today = _date.today().isoformat()

            bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
            with bulk_col1:
                save_json = st.checkbox(
                    "Save JSON files",
                    value=False,
                    key="int_bulk_save_json",
                    help="Save raw PSX responses to /mnt/e/psxdata/intraday/{date}/{SYMBOL}.json",
                )
                if st.button(
                    "Fetch PSX Ticks → intraday_bars",
                    key="int_bulk_btn",
                    type="primary",
                    help=(
                        "Source: PSX /timeseries/int/{symbol} API\n"
                        "Destination: intraday_bars + tick_data tables\n"
                        "Fetches today's tick-level trades for all ~620 symbols."
                    ),
                ):
                    started = start_intraday_sync(save_json=save_json)
                    if started:
                        st.success("Fetching PSX ticks for all symbols → intraday_bars + tick_data")
                    else:
                        st.warning("Already running.")
                    st.rerun()

            with bulk_col2:
                if st.button(
                    f"intraday_bars → JSON Disk ({_today})",
                    key="int_bulk_export_btn",
                    help=(
                        f"Source: intraday_bars table (all symbols for {_today})\n"
                        f"Destination: /mnt/e/psxdata/intraday/{_today}/{{SYMBOL}}.json\n"
                        "Exports DB tick data to per-symbol JSON files on disk."
                    ),
                ):
                    try:
                        import json as _json
                        from collections import defaultdict
                        from pakfindata.config import DATA_ROOT

                        _con = get_connection()
                        rows = _con.execute(
                            "SELECT symbol, ts_epoch, close, volume "
                            "FROM intraday_bars WHERE DATE(ts) = ? "
                            "ORDER BY symbol, ts_epoch",
                            (_today,),
                        ).fetchall()
                        by_sym = defaultdict(list)
                        for r in rows:
                            by_sym[r["symbol"]].append([r["ts_epoch"], r["close"], r["volume"]])

                        _dir = DATA_ROOT / "intraday" / _today
                        _dir.mkdir(parents=True, exist_ok=True)
                        for sym, data in by_sym.items():
                            (_dir / f"{sym}.json").write_text(_json.dumps(data, indent=2))

                        st.success(
                            f"Exported {len(by_sym)} symbols ({len(rows):,} ticks) "
                            f"→ {_dir}"
                        )
                    except Exception as e:
                        st.error(f"Export failed: {e}")

            with bulk_col3:
                if st.button(
                    f"intraday_bars → eod_ohlcv ({_today})",
                    key="int_bulk_promote_btn",
                    help=(
                        "Aggregates intraday_bars into eod_ohlcv for today.\n"
                        "open=first tick, high=MAX, low=MIN, close=last tick, volume=MAX.\n"
                        "Run AFTER bulk fetch completes."
                    ),
                ):
                    try:
                        from pakfindata.db.repositories.intraday import promote_intraday_to_eod
                        _con = get_connection()
                        eod_count = promote_intraday_to_eod(_con, _today)
                        st.success(f"Promoted {eod_count} symbols to eod_ohlcv for {_today}")
                    except Exception as e:
                        st.error(f"Promote failed: {e}")

    # =================================================================
    # LATEST TICKS TABLE — all symbols, today only
    # =================================================================
    import pandas as pd
    try:
        con = get_connection()
        today_str = __import__("datetime").date.today().isoformat()
        latest_df = pd.read_sql_query(
            """SELECT symbol, MAX(ts) as last_tick,
                      COUNT(*) as ticks,
                      MIN(close) as low, MAX(close) as high,
                      (SELECT close FROM intraday_bars b2
                       WHERE b2.symbol = b.symbol AND b2.ts LIKE ? || '%'
                       ORDER BY b2.ts DESC LIMIT 1) as last_price,
                      SUM(volume) as total_vol
               FROM intraday_bars b
               WHERE ts LIKE ? || '%'
               GROUP BY symbol
               ORDER BY ticks DESC""",
            con, params=[today_str, today_str],
        )
        if not latest_df.empty:
            st.markdown(f"### Today's Intraday Summary ({today_str}) — {len(latest_df)} symbols")
            st.dataframe(
                latest_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "symbol": st.column_config.TextColumn("Symbol", width="small"),
                    "last_tick": st.column_config.TextColumn("Last Tick"),
                    "ticks": st.column_config.NumberColumn("Ticks", format="%d"),
                    "low": st.column_config.NumberColumn("Low", format="%.2f"),
                    "high": st.column_config.NumberColumn("High", format="%.2f"),
                    "last_price": st.column_config.NumberColumn("Last Price", format="%.2f"),
                    "total_vol": st.column_config.NumberColumn("Volume", format="%,.0f"),
                },
            )
        else:
            st.info("No intraday ticks for today. Click 'Sync All Intraday Now' above.")
    except Exception as e:
        st.warning(f"Could not load latest ticks: {e}")

    st.markdown("---")

    # Initialize session state for intraday sync
    if "intraday_sync_result" not in st.session_state:
        st.session_state.intraday_sync_result = None
    if "intraday_sync_running" not in st.session_state:
        st.session_state.intraday_sync_running = False

    try:
        con = get_connection()

        # Load symbols for suggestions
        symbols = get_symbols_list(con)

        if not symbols:
            st.warning("No symbols found. Run `pfsync symbols refresh` first.")
            render_footer()
            return

        st.markdown("---")

        # Symbol selection
        col1, col2 = st.columns([2, 1])

        with col1:
            symbol_input = st.text_input(
                "Enter Symbol",
                value="OGDC",
                placeholder="e.g., HBL, OGDC, MCB",
                help="Enter a stock symbol to view intraday data"
            ).strip().upper()

        with col2:
            selected_from_list = st.selectbox(
                "Or select from list",
                [""] + symbols,
                index=0,
                help="Select a symbol from the dropdown"
            )

        selected_symbol = selected_from_list if selected_from_list else symbol_input

        if not selected_symbol:
            st.info("Enter or select a symbol to view intraday data.")
            render_footer()
            return

        if selected_symbol not in symbols:
            st.warning(
                f"Symbol '{selected_symbol}' not found in database. "
                "It may be invalid or you need to refresh symbols."
            )

        st.markdown("---")

        # Sync controls for single symbol
        st.subheader(f"Single Symbol — {selected_symbol}")

        col1, col2 = st.columns([1, 1])

        with col1:
            incremental_mode = st.checkbox(
                "Incremental",
                value=True,
                help="Only fetch new data since last sync",
                disabled=st.session_state.intraday_sync_running
            )

        with col2:
            max_rows = st.number_input(
                "Max Rows",
                min_value=100,
                max_value=5000,
                value=2000,
                step=100,
                help="Maximum rows to fetch from API",
                disabled=st.session_state.intraday_sync_running
            )

        # ── Three separate action buttons ──
        btn_col1, btn_col2, btn_col3 = st.columns(3)
        with btn_col1:
            from datetime import date
            today_str = date.today().isoformat()
            fetch_btn = st.button(
                f"PSX API → Disk ({selected_symbol})",
                type="primary",
                disabled=st.session_state.intraday_sync_running,
                help=(
                    f"Source: PSX /timeseries/int/{selected_symbol}\n"
                    f"Saves to: intraday/{today_str}/{selected_symbol}.json + intradaytemp/{selected_symbol}.csv\n"
                    "Downloads tick data from PSX and saves to disk only."
                ),
            )
        with btn_col2:
            load_btn = st.button(
                f"Disk → intraday_bars ({selected_symbol})",
                disabled=st.session_state.intraday_sync_running,
                help=(
                    f"Source: intraday/{today_str}/{selected_symbol}.json (on disk)\n"
                    "Destination: intraday_bars table\n"
                    "Loads previously downloaded JSON into the database."
                ),
            )
        with btn_col3:
            promote_btn = st.button(
                f"intraday_bars → eod_ohlcv ({today_str})",
                disabled=st.session_state.intraday_sync_running,
                help=(
                    "Source: intraday_bars (all symbols for today)\n"
                    "Destination: eod_ohlcv table\n"
                    "Aggregates ticks: open=first, high=MAX, low=MIN, close=last, vol=MAX."
                ),
            )

        if st.session_state.intraday_sync_running:
            st.warning("Processing...")

        # ── Action 1: Download (fetch + save to disk only) ──
        if fetch_btn and not st.session_state.intraday_sync_running:
            st.session_state.intraday_sync_result = None
            st.session_state.intraday_sync_running = True

            with st.status(
                f"Fetching intraday data for {selected_symbol}...",
                expanded=True
            ) as status:
                st.write(f"Fetching from timeseries/int/{selected_symbol}...")

                try:
                    from pakfindata.sources.intraday import (
                        fetch_intraday_json,
                        parse_intraday_payload,
                    )
                    from pakfindata.http import create_session as create_http_session

                    session = create_http_session()
                    payload = fetch_intraday_json(selected_symbol, session)
                    df_fetched = parse_intraday_payload(selected_symbol, payload)

                    if df_fetched.empty:
                        st.session_state.intraday_sync_result = {
                            "action": "download", "success": True, "rows": 0,
                        }
                        status.update(label="No data returned", state="complete")
                    else:
                        import json
                        from pakfindata.config import DATA_ROOT

                        # Save JSON to intraday/{date}/{SYMBOL}.json
                        json_dir = DATA_ROOT / "intraday" / today_str
                        json_dir.mkdir(parents=True, exist_ok=True)
                        json_path = json_dir / f"{selected_symbol}.json"
                        json_path.write_text(json.dumps(payload, indent=2))

                        # Save CSV to intradaytemp/{SYMBOL}.csv
                        INTRADAY_TEMP_DIR.mkdir(parents=True, exist_ok=True)
                        csv_path = INTRADAY_TEMP_DIR / f"{selected_symbol}.csv"
                        save_cols = ["symbol", "ts", "open", "high", "low", "close", "volume"]
                        df_fetched[save_cols].to_csv(csv_path, index=False)

                        st.session_state.intraday_sync_result = {
                            "action": "download",
                            "success": True,
                            "rows": len(df_fetched),
                            "newest_ts": df_fetched["ts"].max(),
                            "json_path": str(json_path),
                        }
                        status.update(
                            label=f"Downloaded {len(df_fetched)} ticks → JSON + CSV saved",
                            state="complete",
                        )

                except Exception as e:
                    st.session_state.intraday_sync_result = {
                        "action": "download", "success": False, "error": str(e),
                    }
                    status.update(label="Download failed!", state="error")
                finally:
                    st.session_state.intraday_sync_running = False

        # ── Action 2: Load to DB (from saved JSON on disk) ──
        if load_btn and not st.session_state.intraday_sync_running:
            st.session_state.intraday_sync_result = None
            st.session_state.intraday_sync_running = True

            with st.status(
                f"Loading {selected_symbol} into intraday_bars...",
                expanded=True
            ) as status:
                try:
                    import json as _json
                    from pakfindata.config import DATA_ROOT
                    from pakfindata.sources.intraday import parse_intraday_payload
                    from pakfindata.db.repositories.intraday import upsert_intraday

                    # Find the most recent JSON file for this symbol
                    json_path = DATA_ROOT / "intraday" / today_str / f"{selected_symbol}.json"
                    if not json_path.exists():
                        # Try to find any date folder with this symbol
                        intraday_dir = DATA_ROOT / "intraday"
                        found = None
                        if intraday_dir.exists():
                            for d in sorted(intraday_dir.iterdir(), reverse=True):
                                candidate = d / f"{selected_symbol}.json"
                                if candidate.exists():
                                    found = candidate
                                    break
                        if found:
                            json_path = found
                        else:
                            raise FileNotFoundError(
                                f"No JSON file found for {selected_symbol}. Download first."
                            )

                    st.write(f"Loading from {json_path}")
                    payload = _json.loads(json_path.read_text())
                    df_load = parse_intraday_payload(selected_symbol, payload)

                    if df_load.empty:
                        st.session_state.intraday_sync_result = {
                            "action": "load", "success": True, "db_rows": 0,
                        }
                        status.update(label="No rows to load", state="complete")
                    else:
                        db_rows = upsert_intraday(con, df_load)
                        st.session_state.intraday_sync_result = {
                            "action": "load",
                            "success": True,
                            "db_rows": db_rows,
                            "total_ticks": len(df_load),
                            "source": str(json_path),
                        }
                        status.update(
                            label=f"Loaded {db_rows} rows into intraday_bars",
                            state="complete",
                        )

                except Exception as e:
                    st.session_state.intraday_sync_result = {
                        "action": "load", "success": False, "error": str(e),
                    }
                    status.update(label="Load failed!", state="error")
                finally:
                    st.session_state.intraday_sync_running = False

        # ── Action 3: Promote intraday_bars → eod_ohlcv ──
        if promote_btn and not st.session_state.intraday_sync_running:
            st.session_state.intraday_sync_result = None
            st.session_state.intraday_sync_running = True

            with st.status(
                f"Promoting intraday_bars → eod_ohlcv for {today_str}...",
                expanded=True
            ) as status:
                try:
                    from pakfindata.db.repositories.intraday import promote_intraday_to_eod

                    eod_promoted = promote_intraday_to_eod(con, today_str)
                    st.session_state.intraday_sync_result = {
                        "action": "promote",
                        "success": True,
                        "eod_promoted": eod_promoted,
                        "date": today_str,
                    }
                    status.update(
                        label=f"Promoted {eod_promoted} symbols to eod_ohlcv for {today_str}",
                        state="complete",
                    )

                except Exception as e:
                    st.session_state.intraday_sync_result = {
                        "action": "promote", "success": False, "error": str(e),
                    }
                    status.update(label="Promote failed!", state="error")
                finally:
                    st.session_state.intraday_sync_running = False

        # Display result
        if st.session_state.intraday_sync_result is not None:
            result = st.session_state.intraday_sync_result
            action = result.get("action", "")
            if result.get("success"):
                if action == "download":
                    rows = result.get("rows", 0)
                    if rows > 0:
                        st.success(f"Downloaded {rows} ticks for {selected_symbol}")
                        if result.get("json_path"):
                            st.caption(f"JSON: {result['json_path']}")
                        if result.get("newest_ts"):
                            st.caption(f"Latest: {result['newest_ts']}")
                    else:
                        st.info(f"No intraday data returned for {selected_symbol}")
                elif action == "load":
                    db_rows = result.get("db_rows", 0)
                    st.success(
                        f"Loaded {db_rows} rows into intraday_bars "
                        f"({result.get('total_ticks', 0)} ticks from file)"
                    )
                    if result.get("source"):
                        st.caption(f"Source: {result['source']}")
                elif action == "promote":
                    eod = result.get("eod_promoted", 0)
                    st.success(
                        f"Promoted {eod} symbols to eod_ohlcv for {result.get('date', today_str)}"
                    )
            else:
                st.error(f"Error: {result.get('error')}")

        st.markdown("---")

        # Display controls
        col1, col2 = st.columns(2)
        with col1:
            limit = st.slider(
                "Display Limit",
                min_value=200,
                max_value=5000,
                value=500,
                step=100,
                help="Number of rows to display (most recent)"
            )

        with col2:
            stats = get_intraday_stats(con, selected_symbol)
            if stats["row_count"] > 0:
                st.metric(
                    "Total Rows",
                    f"{stats['row_count']:,}",
                    help="Total intraday records for this symbol"
                )
                st.caption(f"Range: {stats['min_ts']} to {stats['max_ts']}")
            else:
                st.info("No intraday data yet. Click 'Fetch / Refresh Intraday'.")

        st.markdown("---")

        # Fetch and display intraday data
        # First try DB, then fall back to temp CSV on disk
        df = get_intraday_latest(con, selected_symbol, limit=limit)

        if df.empty:
            import pandas as pd
            temp_csv = INTRADAY_TEMP_DIR / f"{selected_symbol}.csv"
            if temp_csv.exists():
                df = pd.read_csv(temp_csv)
                df = df.sort_values("ts").tail(limit).reset_index(drop=True)

        if df.empty:
            st.info(
                f"No intraday data for {selected_symbol}. "
                "Click 'Download' to fetch data."
            )
            render_footer()
            return

        # Latest price stats
        st.subheader(f"{selected_symbol} - Intraday Stats")

        latest = df.iloc[-1]
        first = df.iloc[0]
        change = latest["close"] - first["open"] if first["open"] else 0
        change_pct = (change / first["open"]) * 100 if first["open"] else 0

        # Calculate VWAP (Volume Weighted Average Price)
        # VWAP = Σ(Typical Price × Volume) / Σ(Volume)
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tp_volume"] = df["typical_price"] * df["volume"]
        cumulative_tp_volume = df["tp_volume"].cumsum()
        cumulative_volume = df["volume"].cumsum()
        df["vwap"] = cumulative_tp_volume / cumulative_volume
        vwap = df["vwap"].iloc[-1] if not df["vwap"].empty else None

        # Session stats
        session_high = df["high"].max()
        session_low = df["low"].min()
        total_volume = df["volume"].sum()

        # First row: Price metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        change_str = f"{change:+.2f} ({change_pct:+.1f}%)"
        col1.metric(
            "Latest Close",
            f"PKR {latest['close']:.2f}" if latest["close"] else "N/A",
            change_str if first["open"] else None,
            help="Most recent close price"
        )
        col2.metric(
            "Session Open",
            f"PKR {first['open']:.2f}" if first["open"] else "N/A",
            help="Session opening price"
        )
        col3.metric(
            "Session High",
            f"PKR {session_high:.2f}" if session_high else "N/A",
            help="Highest price in session"
        )
        col4.metric(
            "Session Low",
            f"PKR {session_low:.2f}" if session_low else "N/A",
            help="Lowest price in session"
        )
        col5.metric(
            "📊 VWAP",
            f"PKR {vwap:.2f}" if vwap else "N/A",
            help="Volume Weighted Average Price - institutional benchmark"
        )
        col6.metric(
            "Total Volume",
            format_volume(total_volume) if total_volume else "N/A",
            help="Total session volume"
        )

        # VWAP context
        if vwap and latest["close"]:
            vwap_diff = latest["close"] - vwap
            vwap_pct = (vwap_diff / vwap) * 100
            if vwap_diff > 0:
                st.caption(f"📍 Latest: {latest['ts']} | Price **above** VWAP by Rs.{vwap_diff:.2f} ({vwap_pct:+.2f}%) - Bullish bias")
            else:
                st.caption(f"📍 Latest: {latest['ts']} | Price **below** VWAP by Rs.{abs(vwap_diff):.2f} ({vwap_pct:+.2f}%) - Bearish bias")
        else:
            st.caption(f"📍 Latest: {latest['ts']}")

        st.markdown("---")

        # Intraday chart using the helper
        fig = make_intraday_chart(
            df,
            title=f"{selected_symbol} - Intraday",
            ts_col="ts",
            height=650,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Close Price Trend with VWAP overlay
        st.subheader("📈 Price & VWAP")
        import plotly.graph_objects as go

        chart_df = df.sort_values("ts", ascending=True)
        fig_price = go.Figure()

        # Close price line
        fig_price.add_trace(go.Scatter(
            x=chart_df["ts"],
            y=chart_df["close"],
            mode="lines",
            name="Close",
            line={"color": "#2196F3", "width": 2},
        ))

        # VWAP line
        fig_price.add_trace(go.Scatter(
            x=chart_df["ts"],
            y=chart_df["vwap"],
            mode="lines",
            name="VWAP",
            line={"color": "#FF9800", "width": 2, "dash": "dash"},
        ))

        # Add horizontal line at current VWAP
        if vwap:
            fig_price.add_hline(
                y=vwap,
                line_dash="dot",
                line_color="rgba(255,152,0,0.5)",
                annotation_text=f"VWAP: {vwap:.2f}",
                annotation_position="right"
            )

        fig_price.update_layout(
            title=f"{selected_symbol} - Price vs VWAP",
            xaxis_title="Time",
            yaxis_title="Price (PKR)",
            height=400,
            hovermode="x unified",
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            margin={"l": 60, "r": 20, "t": 60, "b": 60},
        )
        st.plotly_chart(fig_price, use_container_width=True)

        st.caption("**VWAP** (Volume Weighted Average Price) = institutional benchmark. "
                   "Price above VWAP suggests bullish bias; below suggests bearish bias.")

        # Volume chart
        st.subheader("📊 Volume")
        fig_vol = make_volume_chart(df, date_col="ts", height=250)
        st.plotly_chart(fig_vol, use_container_width=True)

        st.markdown("---")

        # Data table
        st.subheader(f"Data Preview (last {min(50, len(df))} rows)")

        preview_df = df.sort_values("ts", ascending=False).head(50)
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": st.column_config.TextColumn("Symbol"),
                "ts": st.column_config.TextColumn("Timestamp"),
                "open": st.column_config.NumberColumn("Open", format="%.2f"),
                "high": st.column_config.NumberColumn("High", format="%.2f"),
                "low": st.column_config.NumberColumn("Low", format="%.2f"),
                "close": st.column_config.NumberColumn("Close", format="%.2f"),
                "volume": st.column_config.NumberColumn("Volume", format="%d"),
            }
        )

        st.markdown("---")

        # Export options
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                df.to_csv(index=False),
                f"{selected_symbol}_intraday.csv",
                "text/csv",
                help="Download intraday data to your computer"
            )

        with col2:
            if st.button(
                f"💾 Export to /exports/{selected_symbol}_intraday.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / f"{selected_symbol}_intraday.csv"
                df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()
