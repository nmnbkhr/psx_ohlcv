"""FX Rates Comparison Dashboard — interbank, open market, kerb rates."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pakfindata.ui.components.helpers import get_connection, render_footer
from pakfindata.sources.sbp_fx import SBPFXScraper
from pakfindata.sources.forex_scraper import ForexPKScraper
from pakfindata.sources.fx_client import FXClient

_fx = FXClient()


_KEY_CURRENCIES = ["USD", "EUR", "GBP", "SAR", "AED"]

_FX_TABLES = {
    "Interbank": "sbp_fx_interbank",
    "Open Market": "sbp_fx_open_market",
    "Kerb": "forex_kerb",
}


def render_fx_dashboard():
    """FX rates comparison dashboard."""
    st.markdown("## FX Rates Dashboard")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    try:
        _render_rate_cards(con)
        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            _render_history_chart(con)
        with col2:
            _render_spread_analysis(con)

        st.divider()
        _render_all_currencies(con)

        st.divider()
        col3, col4 = st.columns(2)
        with col3:
            _render_spread_heatmap(con)
        with col4:
            _render_volatility_chart(con)

        st.divider()
        _render_carry_calculator(con)

    except Exception as e:
        st.error(f"Error loading FX data: {e}")

    # ── FX Microservice Signals ─────────────────────────────────────
    st.divider()
    _render_fx_signals()

    # Sync section
    st.markdown("---")
    with st.expander("Sync FX Data"):
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Sync SBP Interbank", type="primary", key="fx_sync_interbank"):
                with st.spinner("Syncing SBP interbank rates..."):
                    try:
                        result = SBPFXScraper().sync_interbank(con)
                        st.success(f"Interbank: {result.get('ok', 0)} rates synced")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        with col2:
            if st.button("Sync Kerb (forex.pk)", key="fx_sync_kerb"):
                with st.spinner("Syncing kerb rates from forex.pk..."):
                    try:
                        result = ForexPKScraper().sync_kerb(con)
                        st.success(f"Kerb: {result.get('ok', 0)} rates synced")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Sync failed: {e}")

        # FX microservice sync
        if _fx.is_healthy():
            col3, col4 = st.columns(2)
            with col3:
                if st.button("Sync from FX Microservice", key="fx_sync_micro"):
                    with st.spinner("Syncing rates from FX microservice..."):
                        try:
                            from pakfindata.sources.fx_sync import sync_fx_rates
                            result = sync_fx_rates(con)
                            st.success(
                                f"FX micro: {result.get('rates_stored', 0)} rates, "
                                f"{result.get('kibor_stored', 0)} KIBOR tenors"
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"FX micro sync failed: {e}")
            with col4:
                if st.button("Backfill FX History", key="fx_backfill"):
                    with st.spinner("Backfilling FX history from microservice..."):
                        try:
                            from pakfindata.sources.fx_sync import backfill_fx_history
                            result = backfill_fx_history(con)
                            st.success(
                                f"Backfill: {result.get('inserted', 0)} inserted, "
                                f"{result.get('skipped', 0)} skipped"
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"Backfill failed: {e}")

    render_footer()


def _get_latest_rate(con, table, currency):
    """Get latest rate for a currency from a specific table."""
    row = con.execute(
        f"""SELECT date, buying, selling FROM {table}
            WHERE UPPER(currency) = ? ORDER BY date DESC LIMIT 1""",
        (currency.upper(),),
    ).fetchone()
    return dict(row) if row else None


def _render_rate_cards(con):
    """Rate cards for key currencies across all sources."""
    st.markdown("### Key Currency Rates")

    for currency in _KEY_CURRENCIES:
        cols = st.columns([1, 2, 2, 2, 1])
        cols[0].markdown(f"**{currency}/PKR**")

        for i, (src_name, table) in enumerate(_FX_TABLES.items()):
            rate = _get_latest_rate(con, table, currency)
            with cols[i + 1]:
                if rate:
                    st.metric(
                        src_name,
                        f"{rate['buying']:.2f} / {rate['selling']:.2f}",
                        help=f"Buy / Sell as of {rate['date']}",
                    )
                else:
                    st.metric(src_name, "N/A")

        # Spread (kerb premium over interbank)
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        with cols[4]:
            if ib and kerb and ib["selling"] and kerb["selling"]:
                spread = kerb["selling"] - ib["selling"]
                st.metric("Spread", f"{spread:+.2f}", help="Kerb premium")
            else:
                st.metric("Spread", "N/A")


def _render_history_chart(con):
    """Historical FX rate chart."""
    st.markdown("### Rate History")

    currency = st.selectbox("Currency", _KEY_CURRENCIES, key="fx_hist_currency")

    fig = go.Figure()
    colors = {"Interbank": "#FF6B35", "Open Market": "#4ECDC4", "Kerb": "#45B7D1"}

    for src_name, table in _FX_TABLES.items():
        df = pd.read_sql_query(
            f"""SELECT date, selling FROM {table}
                WHERE UPPER(currency) = ?
                ORDER BY date LIMIT 365""",
            con, params=(currency.upper(),),
        )
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["selling"],
                mode="lines", name=src_name,
                line=dict(width=2, color=colors.get(src_name, "#999")),
            ))

    if fig.data:
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=f"{currency}/PKR (Selling)",
            height=350, margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No historical data for {currency}. Run `pfsync fx-rates sync-all` to fetch.")


def _render_spread_analysis(con):
    """Bar chart of interbank vs kerb spread per currency."""
    st.markdown("### Spread Analysis")

    spreads = []
    for currency in _KEY_CURRENCIES:
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        if ib and kerb and ib["selling"] and kerb["selling"]:
            spreads.append({
                "Currency": currency,
                "Interbank": ib["selling"],
                "Kerb": kerb["selling"],
                "Spread": round(kerb["selling"] - ib["selling"], 2),
            })

    if not spreads:
        st.info("No spread data available. Sync interbank + kerb rates first.")
        return

    df = pd.DataFrame(spreads)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["Currency"], y=df["Interbank"],
        name="Interbank", marker_color="#FF6B35",
    ))
    fig.add_trace(go.Bar(
        x=df["Currency"], y=df["Kerb"],
        name="Kerb", marker_color="#45B7D1",
    ))
    fig.update_layout(
        barmode="group", height=350,
        yaxis_title="PKR Rate (Selling)",
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_all_currencies(con):
    """Table of all available currencies from interbank."""
    st.markdown("### All Currency Rates (Interbank)")

    df = pd.read_sql_query(
        """SELECT t.currency, t.date, t.buying, t.selling,
                  ROUND(t.selling - t.buying, 4) as spread
           FROM sbp_fx_interbank t
           INNER JOIN (
               SELECT currency, MAX(date) as max_date
               FROM sbp_fx_interbank GROUP BY currency
           ) mx ON t.currency = mx.currency AND t.date = mx.max_date
           ORDER BY t.currency""",
        con,
    )

    if df.empty:
        # Try kerb as fallback
        df = pd.read_sql_query(
            """SELECT t.currency, t.date, t.buying, t.selling,
                      ROUND(t.selling - t.buying, 4) as spread
               FROM forex_kerb t
               INNER JOIN (
                   SELECT currency, MAX(date) as max_date
                   FROM forex_kerb GROUP BY currency
               ) mx ON t.currency = mx.currency AND t.date = mx.max_date
               ORDER BY t.currency""",
            con,
        )
        if not df.empty:
            st.caption("Showing kerb market rates (interbank data not yet available)")

    if df.empty:
        st.info("No currency data available. Run `pfsync fx-rates sync-all` to fetch.")
        return

    st.dataframe(
        df.rename(columns={
            "currency": "Currency", "date": "Date",
            "buying": "Buying", "selling": "Selling", "spread": "Spread",
        }),
        use_container_width=True, hide_index=True,
    )


def _render_spread_heatmap(con):
    """Interbank vs kerb spread heatmap over time."""
    st.markdown("### Spread Heatmap")

    df = pd.read_sql_query(
        """SELECT i.currency, i.date,
                  ROUND(k.selling - i.selling, 2) as spread
           FROM sbp_fx_interbank i
           INNER JOIN forex_kerb k
             ON i.currency = k.currency AND i.date = k.date
           WHERE i.currency IN ('USD', 'EUR', 'GBP', 'SAR', 'AED')
           ORDER BY i.date DESC
           LIMIT 150""",
        con,
    )

    if df.empty:
        st.info("No spread data — sync both interbank and kerb rates first")
        return

    pivot = df.pivot_table(index="date", columns="currency", values="spread")
    if pivot.empty:
        st.info("Insufficient overlap between interbank and kerb dates")
        return

    # Sort dates ascending for display
    pivot = pivot.sort_index()

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[
            [0, "#00C853"],
            [0.5, "#FFEB3B"],
            [1, "#FF1744"],
        ],
        colorbar=dict(title="Spread (PKR)"),
        hovertemplate="Currency: %{x}<br>Date: %{y}<br>Spread: %{z:.2f}<extra></extra>",
    ))
    fig.update_layout(
        height=400, margin=dict(l=20, r=20, t=30, b=20),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_volatility_chart(con):
    """30-day rolling volatility of USD/PKR."""
    st.markdown("### USD/PKR Volatility")

    df = pd.read_sql_query(
        """SELECT date, selling FROM sbp_fx_interbank
           WHERE UPPER(currency) = 'USD'
           ORDER BY date""",
        con,
    )

    if len(df) < 10:
        st.info("Need at least 10 data points for volatility chart")
        return

    df["selling"] = pd.to_numeric(df["selling"], errors="coerce")
    df = df.dropna(subset=["selling"])
    df["return"] = df["selling"].pct_change()
    df["vol_30d"] = df["return"].rolling(window=30, min_periods=10).std() * (252 ** 0.5) * 100

    df_plot = df.dropna(subset=["vol_30d"])
    if df_plot.empty:
        st.info("Insufficient data for rolling volatility")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_plot["date"], y=df_plot["vol_30d"],
        mode="lines", name="30D Annualized Vol",
        line=dict(width=2, color="#E74C3C"),
        fill="tozeroy", fillcolor="rgba(231,76,60,0.1)",
        hovertemplate="Date: %{x}<br>Vol: %{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        yaxis_title="Annualized Vol (%)", height=400,
        margin=dict(l=20, r=20, t=30, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_carry_calculator(con):
    """Carry trade calculator — PKR deposit rate vs foreign rates."""
    st.markdown("### Carry Trade Calculator")

    # Get PKR rate (KIBOR 3M as proxy)
    kibor = con.execute(
        "SELECT offer FROM kibor_daily WHERE tenor = '3M' ORDER BY date DESC LIMIT 1"
    ).fetchone()

    if not kibor:
        st.info("No KIBOR data for carry calculation")
        return

    pkr_rate = kibor["offer"]

    # Get global rates
    global_rates = {}
    try:
        rows = con.execute(
            """SELECT rate_name, rate_value FROM global_rates
               WHERE rate_name IN ('SOFR', 'SONIA', 'EUSTR', 'SAIBOR')
               ORDER BY date DESC"""
        ).fetchall()
        for r in rows:
            if r["rate_name"] not in global_rates:
                global_rates[r["rate_name"]] = r["rate_value"]
    except Exception:
        pass

    # Fallback with common benchmarks
    rate_map = {
        "USD (SOFR)": global_rates.get("SOFR", 4.50),
        "GBP (SONIA)": global_rates.get("SONIA", 4.25),
        "EUR (EUSTR)": global_rates.get("EUSTR", 3.00),
        "SAR (SAIBOR)": global_rates.get("SAIBOR", 5.50),
    }

    st.metric("PKR Deposit Rate (KIBOR 3M)", f"{pkr_rate:.2f}%")

    carry_data = []
    for label, foreign_rate in rate_map.items():
        carry = pkr_rate - foreign_rate
        carry_data.append({
            "Currency": label,
            "Foreign Rate (%)": f"{foreign_rate:.2f}",
            "PKR Rate (%)": f"{pkr_rate:.2f}",
            "Carry Differential (%)": f"{carry:+.2f}",
            "Signal": "Positive Carry" if carry > 0 else "Negative Carry",
        })

    df = pd.DataFrame(carry_data)

    fig = go.Figure()
    colors = ["#00C853" if float(r["Carry Differential (%)"]) > 0 else "#FF1744"
              for r in carry_data]
    fig.add_trace(go.Bar(
        x=[r["Currency"] for r in carry_data],
        y=[float(r["Carry Differential (%)"]) for r in carry_data],
        marker_color=colors,
        text=[r["Carry Differential (%)"] + "%" for r in carry_data],
        textposition="outside",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.update_layout(
        yaxis_title="Carry Spread (%)", height=300,
        margin=dict(l=20, r=20, t=30, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════
# FX Microservice Signal Sections
# ═══════════════════════════════════════════════════════════════════

def _render_fx_signals():
    """Render FX microservice signal sections (regime, carry, intervention)."""
    if not _fx.is_healthy():
        st.info("FX microservice not running — showing DB-sourced rates only. "
                "Start it: `uvicorn api.service:app --port 8100`")
        return

    st.markdown("### FX Trading Signals")

    # ── KIBOR Live Rates ──────────────────────────────────────────
    _render_kibor_live()

    # ── FX-Equity Regime ──────────────────────────────────────────
    _render_regime()

    col1, col2 = st.columns(2)
    with col1:
        _render_carry_trade()
    with col2:
        _render_premium_spread()

    # ── SBP Intervention ──────────────────────────────────────────
    _render_intervention()


def _render_kibor_live():
    """KIBOR rates from FX microservice."""
    data = _fx.get_kibor()
    if not data:
        return

    rates = data.get("rates", data.get("kibor", []))
    if not rates:
        return

    st.markdown("#### KIBOR Rates (Live)")
    rows = []
    for r in rates:
        if isinstance(r, dict):
            rows.append({
                "Tenor": r.get("tenor", ""),
                "Bid": r.get("bid", ""),
                "Offer": r.get("offer", ""),
                "Mid": r.get("mid", ""),
            })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_regime():
    """FX-Equity regime signal."""
    data = _fx.get_regime()
    if not data:
        return

    st.markdown("#### FX-Equity Regime")

    regime = data.get("regime", "unknown")
    equity_signal = data.get("equity_signal", "")
    sector_bias = data.get("sector_bias", "")

    # Regime badge
    regime_colors = {
        "pkr_weakening": "red",
        "pkr_strengthening": "green",
        "stable": "blue",
    }
    color = regime_colors.get(regime, "gray")
    regime_label = regime.replace("_", " ").title()
    st.markdown(f"**Regime:** :{color}[{regime_label}]")

    cols = st.columns(3)
    cols[0].metric("Equity Signal", equity_signal or "N/A")
    cols[1].metric("Sector Bias", sector_bias or "N/A")

    # Metrics
    metrics = data.get("metrics", {})
    if metrics:
        cols[2].metric("USD/PKR", f"{metrics.get('last_close', 0):.2f}")

    # Sector exposures
    exposures = data.get("sector_exposures", {})
    if exposures:
        with st.expander("Sector FX Exposures"):
            exp_rows = [
                {"Sector": k, "Exposure": v}
                for k, v in sorted(exposures.items(), key=lambda x: x[1], reverse=True)
            ]
            st.dataframe(pd.DataFrame(exp_rows), use_container_width=True, hide_index=True)


def _render_carry_trade():
    """Carry trade signals."""
    report = _fx.get_signal_report()
    if not report:
        return

    carry = report.get("carry_trade", report.get("carry", {}))
    if not carry:
        return

    st.markdown("#### Carry Trade")

    best = carry.get("best_carry", carry.get("signal", {}))
    if isinstance(best, dict):
        st.metric("Best Carry", best.get("pair", "N/A"),
                  delta=f"{best.get('differential', 0):.1f}% spread" if best.get("differential") else None)

    signals = carry.get("signals", carry.get("pairs", []))
    if signals and isinstance(signals, list):
        rows = []
        for s in signals:
            if isinstance(s, dict):
                rows.append({
                    "Pair": s.get("pair", ""),
                    "PKR Rate": s.get("pkr_rate", s.get("local_rate", "")),
                    "Foreign Rate": s.get("foreign_rate", ""),
                    "Differential": s.get("differential", s.get("spread", "")),
                    "Signal": s.get("signal", ""),
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_premium_spread():
    """Premium spread (interbank vs open market gap)."""
    report = _fx.get_signal_report()
    if not report:
        return

    prem = report.get("premium_spread", report.get("premium", {}))
    if not prem:
        return

    st.markdown("#### Premium Spread")

    stress = prem.get("stress_level", prem.get("signal", ""))
    if stress:
        stress_colors = {"low": "green", "moderate": "orange", "high": "red", "elevated": "orange"}
        sc = stress_colors.get(stress.lower(), "gray")
        st.markdown(f"**Stress Level:** :{sc}[{stress.title()}]")

    pairs = prem.get("pairs", prem.get("spreads", []))
    if pairs and isinstance(pairs, list):
        rows = []
        for p in pairs:
            if isinstance(p, dict):
                rows.append({
                    "Pair": p.get("pair", ""),
                    "Interbank": p.get("interbank", p.get("official", "")),
                    "Open Market": p.get("open_market", p.get("kerb", "")),
                    "Gap (%)": p.get("gap_pct", p.get("premium_pct", "")),
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_intervention():
    """SBP intervention detection."""
    data = _fx.get_intervention()
    if not data:
        return

    st.markdown("#### SBP Intervention Detection")

    signal = data.get("signal", data)
    if isinstance(signal, dict):
        cols = st.columns(3)
        likely = signal.get("likely", signal.get("intervention_likely", False))
        conf = signal.get("confidence", 0)
        direction = signal.get("direction", signal.get("stance", "N/A"))

        cols[0].metric("Likely", "Yes" if likely else "No")
        if isinstance(conf, (int, float)):
            cols[1].metric("Confidence", f"{conf:.0%}" if conf <= 1 else f"{conf}%")
        else:
            cols[1].metric("Confidence", str(conf))
        cols[2].metric("Direction", str(direction).replace("_", " ").title())

    # FXIM data
    fxim = data.get("fxim", {})
    history = fxim.get("history", [])
    if history and isinstance(history, list):
        with st.expander("FXIM History"):
            st.dataframe(pd.DataFrame(history), use_container_width=True, hide_index=True)
