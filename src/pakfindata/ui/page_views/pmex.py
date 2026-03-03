"""PMEX Commodities page — OHLC API + Margins Excel.

Provides 6 independent action buttons:
  OHLC:    Get Data → Save to Disk → Populate to Table
  Margins: Get Data → Save to Disk → Populate to Table

Data stored in separate DB: /mnt/e/psxdata/commod/commod.db
Raw files: /mnt/e/psxdata/commod/pmex_ohlc/ and /mnt/e/psxdata/commod/pmex_margins/
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

COMMOD_DB_PATH = Path("/mnt/e/psxdata/commod/commod.db")
COMMOD_DATA_ROOT = Path("/mnt/e/psxdata/commod")
PMEX_OHLC_DIR = COMMOD_DATA_ROOT / "pmex_ohlc"
PMEX_MARGINS_DIR = COMMOD_DATA_ROOT / "pmex_margins"

COMMOD_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pmex_ohlc (
    trading_date     DATE NOT NULL,
    symbol           TEXT NOT NULL,
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    traded_volume    INTEGER DEFAULT 0,
    settlement_price REAL,
    fx_rate          REAL,
    fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trading_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_pmex_ohlc_sym  ON pmex_ohlc(symbol);
CREATE INDEX IF NOT EXISTS idx_pmex_ohlc_date ON pmex_ohlc(trading_date);

CREATE TABLE IF NOT EXISTS pmex_margins (
    report_date          DATE NOT NULL,
    sheet_name           TEXT NOT NULL,
    product_group        TEXT,
    contract_code        TEXT NOT NULL,
    reference_price      REAL,
    initial_margin_pct   REAL,
    initial_margin_value REAL,
    wcm                  REAL,
    maintenance_margin   REAL,
    lower_limit          REAL,
    upper_limit          REAL,
    fx_rate              REAL,
    is_active            BOOLEAN DEFAULT 1,
    fetched_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, contract_code)
);
CREATE INDEX IF NOT EXISTS idx_pmex_margins_date ON pmex_margins(report_date);
CREATE INDEX IF NOT EXISTS idx_pmex_margins_code ON pmex_margins(contract_code);
"""


# ─────────────────────────────────────────────────────────────────────────────
# DB Connection (inline — avoids Streamlit import caching issues)
# ─────────────────────────────────────────────────────────────────────────────


def _get_commod_con() -> sqlite3.Connection | None:
    """Get connection to commod.db, creating dirs/schema as needed."""
    try:
        for d in [COMMOD_DATA_ROOT, PMEX_OHLC_DIR, PMEX_MARGINS_DIR]:
            d.mkdir(parents=True, exist_ok=True)

        con = sqlite3.connect(str(COMMOD_DB_PATH), check_same_thread=False, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        con.executescript(COMMOD_SCHEMA_SQL)
        con.commit()
        return con
    except Exception as e:
        st.error(f"Failed to connect to commod.db: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main Render
# ─────────────────────────────────────────────────────────────────────────────


def render_pmex():
    """Main entry point for the PMEX Commodities page."""
    st.markdown("## PMEX Commodities")
    st.caption("OHLC API + Margins Excel | Separate DB: commod.db")

    con = _get_commod_con()
    if con is None:
        return

    # DB Stats at top
    _render_db_stats(con)

    st.divider()

    # OHLC Section
    _render_ohlc_section(con)

    st.divider()

    # Margins Section
    _render_margins_section(con)

    st.divider()

    # Backfill (expander)
    with st.expander("Backfill", expanded=False):
        _render_backfill(con)

    # View Database (expander)
    with st.expander("View Database", expanded=False):
        _render_view_db(con)


# ─────────────────────────────────────────────────────────────────────────────
# DB Stats
# ─────────────────────────────────────────────────────────────────────────────


def _render_db_stats(con: sqlite3.Connection):
    """Show DB summary metrics at the top of the page."""
    ohlc_stats = con.execute(
        """SELECT COUNT(*) as rows, COUNT(DISTINCT symbol) as symbols,
           MIN(trading_date) as min_dt, MAX(trading_date) as max_dt
           FROM pmex_ohlc"""
    ).fetchone()

    margins_stats = con.execute(
        """SELECT COUNT(*) as rows, COUNT(DISTINCT contract_code) as contracts,
           MIN(report_date) as min_dt, MAX(report_date) as max_dt
           FROM pmex_margins"""
    ).fetchone()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("OHLC Rows", f"{ohlc_stats['rows']:,}")
    with c2:
        st.metric("OHLC Symbols", f"{ohlc_stats['symbols']:,}")
    with c3:
        st.metric("Margins Rows", f"{margins_stats['rows']:,}")
    with c4:
        st.metric("Margins Contracts", f"{margins_stats['contracts']:,}")

    # Date ranges
    parts = []
    if ohlc_stats["min_dt"]:
        parts.append(f"OHLC: {ohlc_stats['min_dt']} → {ohlc_stats['max_dt']}")
    if margins_stats["min_dt"]:
        parts.append(f"Margins: {margins_stats['min_dt']} → {margins_stats['max_dt']}")
    if parts:
        st.caption(" | ".join(parts))


# ─────────────────────────────────────────────────────────────────────────────
# OHLC Section — 3 buttons
# ─────────────────────────────────────────────────────────────────────────────


def _render_ohlc_section(con: sqlite3.Connection):
    """OHLC data section with Get, Save, Populate buttons."""
    st.markdown("### OHLC Data")

    # Date range inputs
    col_from, col_to = st.columns(2)
    with col_from:
        ohlc_from = st.date_input(
            "From Date",
            value=date.today() - timedelta(days=30),
            key="ohlc_from_date",
        )
    with col_to:
        ohlc_to = st.date_input(
            "To Date",
            value=date.today(),
            key="ohlc_to_date",
        )

    # Active only checkbox
    active_only = st.checkbox("Active only (volume > 0)", key="ohlc_active_only")

    # 3 action buttons
    has_data = "pmex_ohlc_df" in st.session_state and st.session_state["pmex_ohlc_df"] is not None

    btn1, btn2, btn3 = st.columns(3)

    with btn1:
        if st.button("Get OHLC Data", type="primary", use_container_width=True):
            _do_ohlc_fetch(ohlc_from, ohlc_to)

    with btn2:
        if st.button(
            "Save OHLC to Disk",
            disabled=not has_data,
            use_container_width=True,
        ):
            _do_ohlc_save(ohlc_from, ohlc_to)

    with btn3:
        if st.button(
            "Populate OHLC to Table",
            disabled=not has_data,
            use_container_width=True,
        ):
            _do_ohlc_populate(con)

    # Preview
    if has_data:
        df = st.session_state["pmex_ohlc_df"]
        if df is not None and not df.empty:
            display_df = df.copy()
            if active_only:
                display_df = display_df[display_df["traded_volume"] > 0]

            # Summary metrics
            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1:
                st.metric("Total Rows", len(display_df))
            with mc2:
                st.metric("Active Contracts", len(display_df[display_df["traded_volume"] > 0]))
            with mc3:
                st.metric("Unique Symbols", display_df["symbol"].nunique())
            with mc4:
                if "trading_date" in display_df.columns and not display_df.empty:
                    dt_range = f"{display_df['trading_date'].min()} → {display_df['trading_date'].max()}"
                    st.metric("Date Range", dt_range)

            st.dataframe(display_df, use_container_width=True, hide_index=True)


def _do_ohlc_fetch(from_date: date, to_date: date):
    """Fetch OHLC data from API and store in session state."""
    with st.spinner("Fetching OHLC data from PMEX API..."):
        try:
            from pakfindata.commodities.fetcher_pmex_ohlc import fetch_ohlc

            df = fetch_ohlc(from_date, to_date)
            if df.empty:
                st.warning("No data returned for the selected date range.")
                st.session_state["pmex_ohlc_df"] = None
                st.session_state["pmex_ohlc_raw"] = None
            else:
                # Convert trading_date to string for display
                df["trading_date"] = df["trading_date"].dt.strftime("%Y-%m-%d")
                st.session_state["pmex_ohlc_df"] = df
                st.session_state["pmex_ohlc_raw"] = df.to_dict("records")
                st.success(
                    f"Fetched {len(df)} rows, "
                    f"{df['symbol'].nunique()} symbols, "
                    f"{len(df[df['traded_volume'] > 0])} active"
                )
                st.rerun()
        except Exception as e:
            st.error(f"OHLC fetch failed: {e}")


def _do_ohlc_save(from_date: date, to_date: date):
    """Save OHLC raw data to disk as JSON."""
    raw = st.session_state.get("pmex_ohlc_raw")
    if not raw:
        st.warning("No data to save. Fetch first.")
        return

    try:
        import json

        out_dir = PMEX_OHLC_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = f"ohlc_{from_date.isoformat()}_{to_date.isoformat()}.json"
        fpath = out_dir / fname

        with open(fpath, "w") as f:
            json.dump(raw, f, indent=2, default=str)

        size_kb = fpath.stat().st_size / 1024
        st.success(f"Saved: {fpath} ({size_kb:.1f} KB)")
    except Exception as e:
        st.error(f"Save failed: {e}")


def _do_ohlc_populate(con: sqlite3.Connection):
    """Upsert OHLC data into commod.db."""
    df = st.session_state.get("pmex_ohlc_df")
    if df is None or df.empty:
        st.warning("No data to populate. Fetch first.")
        return

    try:
        rows = df.to_dict("records")
        con.executemany(
            """INSERT OR REPLACE INTO pmex_ohlc
               (trading_date, symbol, open, high, low, close,
                traded_volume, settlement_price, fx_rate)
               VALUES (:trading_date, :symbol, :open, :high, :low, :close,
                        :traded_volume, :settlement_price, :fx_rate)""",
            rows,
        )
        con.commit()
        st.success(f"Populated {len(rows)} rows into pmex_ohlc table.")
        st.rerun()
    except Exception as e:
        st.error(f"Populate failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Margins Section — 3 buttons
# ─────────────────────────────────────────────────────────────────────────────


def _render_margins_section(con: sqlite3.Connection):
    """Margins data section with Get, Save, Populate buttons."""
    st.markdown("### Margins Data")

    margins_date = st.date_input(
        "Report Date",
        value=date.today(),
        key="margins_report_date",
    )

    active_only = st.checkbox("Active only (is_active = 1)", key="margins_active_only")

    has_data = (
        "pmex_margins_df" in st.session_state
        and st.session_state["pmex_margins_df"] is not None
    )

    btn4, btn5, btn6 = st.columns(3)

    with btn4:
        if st.button("Get Margins Data", type="primary", use_container_width=True):
            _do_margins_fetch(margins_date)

    with btn5:
        if st.button(
            "Save Margins to Disk",
            disabled=not has_data,
            use_container_width=True,
        ):
            _do_margins_save()

    with btn6:
        if st.button(
            "Populate Margins to Table",
            disabled=not has_data,
            use_container_width=True,
        ):
            _do_margins_populate(con)

    # Manual download helper + upload
    from pakfindata.commodities.fetcher_pmex_margins import margins_url

    file_url = margins_url(margins_date)
    st.caption(
        "Auto-download uses Chrome to bypass Cloudflare. "
        "If it fails, download the file in your browser and upload below."
    )
    st.code(file_url, language=None)

    uploaded = st.file_uploader(
        "Upload Margins Excel (.xlsx)",
        type=["xlsx"],
        key="margins_upload",
    )
    if uploaded is not None:
        _do_margins_upload(uploaded, margins_date)

    # Preview
    if has_data:
        df = st.session_state["pmex_margins_df"]
        if df is not None and not df.empty:
            display_df = df.copy()
            if active_only:
                display_df = display_df[display_df["is_active"] == True]  # noqa: E712

            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1:
                st.metric("Total Contracts", len(display_df))
            with mc2:
                st.metric("Active", int(display_df["is_active"].sum()))
            with mc3:
                groups = display_df["product_group"].dropna().nunique()
                st.metric("Product Groups", groups)
            with mc4:
                sheets = display_df["sheet_name"].nunique()
                st.metric("Sheets", sheets)

            st.dataframe(display_df, use_container_width=True, hide_index=True)


def _do_margins_fetch(target_date: date):
    """Fetch margins Excel via Playwright (Cloudflare bypass) and parse it."""
    with st.spinner("Downloading margins via Chrome (Cloudflare bypass)..."):
        try:
            from pakfindata.commodities.fetcher_pmex_margins import (
                fetch_margins_file,
                parse_margins_excel,
            )

            raw_bytes, actual_date = fetch_margins_file(target_date, walk_back_days=5)

            if raw_bytes is None:
                st.warning("No margins file found within 5 business days of the selected date.")
                st.session_state["pmex_margins_df"] = None
                st.session_state["pmex_margins_bytes"] = None
                st.session_state["pmex_margins_date"] = None
                return

            df = parse_margins_excel(raw_bytes, actual_date)

            if df.empty:
                st.warning(f"Margins file for {actual_date} downloaded but parsing returned no data.")
                st.session_state["pmex_margins_df"] = None
                st.session_state["pmex_margins_bytes"] = raw_bytes
                st.session_state["pmex_margins_date"] = actual_date
                return

            st.session_state["pmex_margins_df"] = df
            st.session_state["pmex_margins_bytes"] = raw_bytes
            st.session_state["pmex_margins_date"] = actual_date

            active_count = int(df["is_active"].sum())
            st.success(
                f"Fetched margins for {actual_date}: "
                f"{len(df)} contracts ({active_count} active)"
            )
            st.rerun()
        except Exception as e:
            st.error(f"Margins fetch failed: {e}")


def _do_margins_save():
    """Save raw margins Excel to disk."""
    raw_bytes = st.session_state.get("pmex_margins_bytes")
    actual_date = st.session_state.get("pmex_margins_date")

    if not raw_bytes or not actual_date:
        st.warning("No margins data to save. Fetch first.")
        return

    try:
        out_dir = PMEX_MARGINS_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = f"Margins-{actual_date.strftime('%d-%m-%Y')}.xlsx"
        fpath = out_dir / fname

        with open(fpath, "wb") as f:
            f.write(raw_bytes)

        size_kb = fpath.stat().st_size / 1024
        st.success(f"Saved: {fpath} ({size_kb:.1f} KB)")
    except Exception as e:
        st.error(f"Save failed: {e}")


def _do_margins_upload(uploaded_file, report_date: date):
    """Parse an uploaded margins Excel file."""
    try:
        from pakfindata.commodities.fetcher_pmex_margins import parse_margins_excel

        raw_bytes = uploaded_file.read()
        df = parse_margins_excel(raw_bytes, report_date)

        if df.empty:
            st.warning("Uploaded file could not be parsed (no valid data found).")
            return

        st.session_state["pmex_margins_df"] = df
        st.session_state["pmex_margins_bytes"] = raw_bytes
        st.session_state["pmex_margins_date"] = report_date

        active_count = int(df["is_active"].sum())
        st.success(
            f"Parsed uploaded margins: {len(df)} contracts ({active_count} active)"
        )
        st.rerun()
    except Exception as e:
        st.error(f"Upload parse failed: {e}")


def _do_margins_populate(con: sqlite3.Connection):
    """Upsert margins data into commod.db."""
    df = st.session_state.get("pmex_margins_df")
    if df is None or df.empty:
        st.warning("No data to populate. Fetch first.")
        return

    try:
        rows = df.to_dict("records")
        con.executemany(
            """INSERT OR REPLACE INTO pmex_margins
               (report_date, sheet_name, product_group, contract_code,
                reference_price, initial_margin_pct, initial_margin_value,
                wcm, maintenance_margin, lower_limit, upper_limit,
                fx_rate, is_active)
               VALUES (:report_date, :sheet_name, :product_group, :contract_code,
                        :reference_price, :initial_margin_pct, :initial_margin_value,
                        :wcm, :maintenance_margin, :lower_limit, :upper_limit,
                        :fx_rate, :is_active)""",
            rows,
        )
        con.commit()
        st.success(f"Populated {len(rows)} rows into pmex_margins table.")
        st.rerun()
    except Exception as e:
        st.error(f"Populate failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Backfill Section
# ─────────────────────────────────────────────────────────────────────────────


def _render_backfill(con: sqlite3.Connection):
    """Backfill section in an expander."""
    tab_ohlc, tab_margins = st.tabs(["OHLC Backfill", "Margins Backfill"])

    with tab_ohlc:
        _render_ohlc_backfill(con)

    with tab_margins:
        _render_margins_backfill(con)


def _render_ohlc_backfill(con: sqlite3.Connection):
    """OHLC backfill with date range and progress bar."""
    c1, c2 = st.columns(2)
    with c1:
        bf_from = st.date_input(
            "Start Date",
            value=date(2024, 1, 1),
            key="ohlc_bf_from",
        )
    with c2:
        bf_to = st.date_input(
            "End Date",
            value=date.today(),
            key="ohlc_bf_to",
        )

    bf_active = st.checkbox("Active only", key="ohlc_bf_active")
    bf_save = st.checkbox("Also save raw JSON files", key="ohlc_bf_save")

    if st.button("Start OHLC Backfill", type="primary", key="ohlc_bf_start"):
        from pakfindata.commodities.fetcher_pmex_ohlc import fetch_ohlc

        progress_bar = st.progress(0, text="Starting OHLC backfill...")
        status_text = st.empty()

        session = None
        try:
            import requests as _requests
            import time
            import json

            session = _requests.Session()
            all_chunks: list[pd.DataFrame] = []
            cur = bf_from
            total_days = (bf_to - bf_from).days
            total_chunks = max(1, (total_days + 89) // 90)
            chunk_num = 0

            while cur < bf_to:
                chunk_end = min(cur + timedelta(days=89), bf_to)
                chunk_num += 1

                status_text.text(f"Chunk {chunk_num}/{total_chunks}: {cur} → {chunk_end}")
                progress_bar.progress(chunk_num / total_chunks, text=f"Chunk {chunk_num}/{total_chunks}")

                df = fetch_ohlc(cur, chunk_end, session)

                if not df.empty:
                    if bf_active:
                        df = df[df["traded_volume"] > 0]

                    # Convert trading_date to string for DB
                    df["trading_date"] = df["trading_date"].dt.strftime("%Y-%m-%d")

                    if bf_save:
                        raw = df.to_dict("records")
                        out_dir = PMEX_OHLC_DIR
                        out_dir.mkdir(parents=True, exist_ok=True)
                        fname = f"ohlc_{cur.isoformat()}_{chunk_end.isoformat()}.json"
                        with open(out_dir / fname, "w") as f:
                            json.dump(raw, f, indent=2, default=str)

                    # Upsert to DB
                    rows = df.to_dict("records")
                    con.executemany(
                        """INSERT OR REPLACE INTO pmex_ohlc
                           (trading_date, symbol, open, high, low, close,
                            traded_volume, settlement_price, fx_rate)
                           VALUES (:trading_date, :symbol, :open, :high, :low, :close,
                                    :traded_volume, :settlement_price, :fx_rate)""",
                        rows,
                    )
                    con.commit()
                    all_chunks.append(df)

                cur = chunk_end + timedelta(days=1)
                if cur < bf_to:
                    time.sleep(2.0)

            progress_bar.progress(1.0, text="Done!")
            total_rows = sum(len(c) for c in all_chunks)
            st.success(f"Backfill complete: {total_rows} rows from {chunk_num} chunks")
            st.rerun()

        except Exception as e:
            st.error(f"Backfill failed: {e}")


def _render_margins_backfill(con: sqlite3.Connection):
    """Margins backfill with date range and progress bar."""
    c1, c2 = st.columns(2)
    with c1:
        bf_from = st.date_input(
            "Start Date",
            value=date.today() - timedelta(days=30),
            key="margins_bf_from",
        )
    with c2:
        bf_to = st.date_input(
            "End Date",
            value=date.today(),
            key="margins_bf_to",
        )

    bf_save = st.checkbox("Also save raw Excel files", key="margins_bf_save")

    if st.button("Start Margins Backfill", type="primary", key="margins_bf_start"):
        from pakfindata.commodities.fetcher_pmex_margins import backfill_margins

        progress_bar = st.progress(0, text="Launching Chrome for Cloudflare bypass...")
        status_text = st.empty()

        try:
            def _progress(cur, total, label):
                pct = min(cur / total, 1.0) if total > 0 else 0
                progress_bar.progress(pct, text=f"Day {cur}/{total}: {label}")
                status_text.text(f"Processing {label}...")

            df = backfill_margins(
                start_date=bf_from,
                end_date=bf_to,
                progress_callback=_progress,
            )

            if df.empty:
                progress_bar.progress(1.0, text="Done!")
                st.warning("No margins data found in the selected range.")
            else:
                # Save raw files if requested
                if bf_save:
                    status_text.text("Saving raw Excel files not available in backfill mode (data already parsed)")

                # Upsert to DB
                rows = df.to_dict("records")
                con.executemany(
                    """INSERT OR REPLACE INTO pmex_margins
                       (report_date, sheet_name, product_group, contract_code,
                        reference_price, initial_margin_pct, initial_margin_value,
                        wcm, maintenance_margin, lower_limit, upper_limit,
                        fx_rate, is_active)
                       VALUES (:report_date, :sheet_name, :product_group, :contract_code,
                                :reference_price, :initial_margin_pct, :initial_margin_value,
                                :wcm, :maintenance_margin, :lower_limit, :upper_limit,
                                :fx_rate, :is_active)""",
                    rows,
                )
                con.commit()

                progress_bar.progress(1.0, text="Done!")
                dates_found = df["report_date"].nunique()
                st.success(
                    f"Margins backfill complete: {len(rows)} rows from {dates_found} dates"
                )
                st.rerun()

        except Exception as e:
            st.error(f"Margins backfill failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# View Database Section
# ─────────────────────────────────────────────────────────────────────────────


def _render_view_db(con: sqlite3.Connection):
    """Browse data from commod.db."""
    tab_ohlc, tab_margins = st.tabs(["OHLC Data", "Margins Data"])

    with tab_ohlc:
        _render_view_ohlc(con)

    with tab_margins:
        _render_view_margins(con)


def _render_view_ohlc(con: sqlite3.Connection):
    """Browse OHLC data from commod.db."""
    # Get available symbols
    symbols = [
        r["symbol"]
        for r in con.execute(
            "SELECT DISTINCT symbol FROM pmex_ohlc ORDER BY symbol"
        ).fetchall()
    ]

    if not symbols:
        st.info("No OHLC data in database. Run a fetch or backfill first.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        selected_sym = st.selectbox("Symbol", ["All"] + symbols, key="view_ohlc_sym")
    with c2:
        view_active = st.checkbox("Active only", key="view_ohlc_active")
    with c3:
        view_limit = st.number_input("Limit", 50, 5000, 500, key="view_ohlc_limit")

    conditions = []
    params: list = []

    if selected_sym != "All":
        conditions.append("symbol = ?")
        params.append(selected_sym)
    if view_active:
        conditions.append("traded_volume > 0")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM pmex_ohlc {where} ORDER BY trading_date DESC, symbol LIMIT ?"
    params.append(view_limit)

    rows = con.execute(sql, params).fetchall()
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        st.dataframe(df, use_container_width=True, hide_index=True)

        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            file_name="pmex_ohlc_export.csv",
            mime="text/csv",
        )
    else:
        st.info("No matching rows.")


def _render_view_margins(con: sqlite3.Connection):
    """Browse margins data from commod.db."""
    dates = [
        r["report_date"]
        for r in con.execute(
            "SELECT DISTINCT report_date FROM pmex_margins ORDER BY report_date DESC LIMIT 30"
        ).fetchall()
    ]

    if not dates:
        st.info("No margins data in database. Run a fetch or backfill first.")
        return

    c1, c2 = st.columns(2)
    with c1:
        selected_date = st.selectbox("Report Date", dates, key="view_margins_date")
    with c2:
        view_active = st.checkbox("Active only", key="view_margins_active")

    conditions = ["report_date = ?"]
    params: list = [selected_date]

    if view_active:
        conditions.append("is_active = 1")

    where = f"WHERE {' AND '.join(conditions)}"
    sql = f"SELECT * FROM pmex_margins {where} ORDER BY product_group, contract_code"

    rows = con.execute(sql, params).fetchall()
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        st.dataframe(df, use_container_width=True, hide_index=True)

        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            file_name=f"pmex_margins_{selected_date}.csv",
            mime="text/csv",
        )
    else:
        st.info("No matching rows.")
