"""Factor analysis page."""

import pandas as pd
import streamlit as st

from psx_ohlcv.db import get_latest_kse100
from psx_ohlcv.ui.session_tracker import track_page_visit
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    get_sector_names,
    render_footer,
    render_market_status_badge,
)


def render_factor_analysis():
    """Quantitative factor analysis and stock rankings."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Factor Analysis")
        st.caption("Quantitative factor rankings • Value, Momentum, Quality & Volatility")
    with header_col2:
        render_market_status_badge()

    con = get_connection()
    track_page_visit(con, "Factor Analysis")

    # Check data availability
    snapshot_count = con.execute(
        "SELECT COUNT(DISTINCT symbol) FROM company_snapshots"
    ).fetchone()[0]

    if snapshot_count < 10:
        st.warning(
            f"Only {snapshot_count} companies with data. "
            "Go to **Data Acquisition** to scrape more companies for meaningful factor analysis."
        )

    st.markdown("---")

    # Tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Factor Rankings",
        "🔄 Factor Correlations",
        "📊 Sector Exposure",
        "⚠️ Risk Metrics"
    ])

    # =========================================================================
    # Tab 1: Factor Rankings
    # =========================================================================
    with tab1:
        st.subheader("Multi-Factor Stock Rankings")
        st.markdown("""
        Stocks ranked by composite factor score combining **Value**, **Momentum**,
        **Quality**, and **Volatility** factors.
        """)

        # Factor weights
        st.markdown("#### Factor Weights")
        weight_cols = st.columns(4)
        with weight_cols[0]:
            w_value = st.slider("Value", 0.0, 1.0, 0.25, 0.05, key="w_value")
        with weight_cols[1]:
            w_momentum = st.slider("Momentum", 0.0, 1.0, 0.25, 0.05, key="w_momentum")
        with weight_cols[2]:
            w_quality = st.slider("Quality", 0.0, 1.0, 0.25, 0.05, key="w_quality")
        with weight_cols[3]:
            w_volatility = st.slider("Low Volatility", 0.0, 1.0, 0.25, 0.05, key="w_volatility")

        # Normalize weights
        total_weight = w_value + w_momentum + w_quality + w_volatility
        if total_weight > 0:
            w_value /= total_weight
            w_momentum /= total_weight
            w_quality /= total_weight
            w_volatility /= total_weight

        st.caption(f"Normalized: Value={w_value:.0%}, Momentum={w_momentum:.0%}, Quality={w_quality:.0%}, LowVol={w_volatility:.0%}")

        st.markdown("---")

        # Build factor data
        try:
            # Get latest snapshot data for each company
            # Schema: quote_data, trading_data, equity_data, financials_data, ratios_data (all JSON)
            factor_query = """
                WITH latest_snapshots AS (
                    SELECT
                        cs.symbol,
                        cs.snapshot_date,
                        cs.company_name,
                        cs.sector_name as sector_code,
                        -- From quote_data: close price
                        json_extract(cs.quote_data, '$.close') as price,
                        -- From trading_data (REG segment): volume, high, low, 52-week, P/E
                        json_extract(cs.trading_data, '$.REG.ldcp') as ldcp,
                        json_extract(cs.trading_data, '$.REG.volume') as volume,
                        json_extract(cs.trading_data, '$.REG.high') as high,
                        json_extract(cs.trading_data, '$.REG.low') as low,
                        json_extract(cs.trading_data, '$.REG.week_52_low') as wk52_low,
                        json_extract(cs.trading_data, '$.REG.week_52_high') as wk52_high,
                        json_extract(cs.trading_data, '$.REG.pe_ratio_ttm') as pe_ratio,
                        json_extract(cs.trading_data, '$.REG.ytd_change') as ytd_change,
                        json_extract(cs.trading_data, '$.REG.year_1_change') as year_1_change,
                        -- From equity_data: market cap + shares
                        json_extract(cs.equity_data, '$.market_cap') as market_cap,
                        json_extract(cs.equity_data, '$.outstanding_shares') as outstanding_shares,
                        json_extract(cs.equity_data, '$.free_float_percent') as free_float_pct,
                        -- From financials_data: EPS (latest annual)
                        json_extract(cs.financials_data, '$.annual[0].eps') as eps,
                        -- From ratios_data: profit margins
                        json_extract(cs.ratios_data, '$.annual[0].net_profit_margin') as net_margin,
                        json_extract(cs.ratios_data, '$.annual[0].eps_growth') as eps_growth
                    FROM company_snapshots cs
                    WHERE cs.snapshot_date = (
                        SELECT MAX(snapshot_date) FROM company_snapshots cs2
                        WHERE cs2.symbol = cs.symbol
                    )
                ),
                price_history AS (
                    SELECT
                        symbol,
                        (SELECT close FROM eod_ohlcv e2 WHERE e2.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 1) as latest_close,
                        (SELECT close FROM eod_ohlcv e3 WHERE e3.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 1 OFFSET 20) as close_20d_ago,
                        (SELECT close FROM eod_ohlcv e4 WHERE e4.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 1 OFFSET 60) as close_60d_ago,
                        (SELECT AVG(close) FROM (SELECT close FROM eod_ohlcv e5 WHERE e5.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 20)) as sma_20,
                        (SELECT AVG(close) FROM (SELECT close FROM eod_ohlcv e6 WHERE e6.symbol = eod_ohlcv.symbol ORDER BY date DESC LIMIT 50)) as sma_50
                    FROM eod_ohlcv
                    GROUP BY symbol
                )
                SELECT
                    ls.*,
                    ph.latest_close,
                    ph.close_20d_ago,
                    ph.close_60d_ago,
                    ph.sma_20,
                    ph.sma_50,
                    CASE WHEN ph.close_20d_ago > 0
                        THEN (ph.latest_close - ph.close_20d_ago) / ph.close_20d_ago * 100
                        ELSE 0 END as return_20d,
                    CASE WHEN ph.close_60d_ago > 0
                        THEN (ph.latest_close - ph.close_60d_ago) / ph.close_60d_ago * 100
                        ELSE 0 END as return_60d
                FROM latest_snapshots ls
                LEFT JOIN price_history ph ON ls.symbol = ph.symbol
                WHERE ls.price > 0
            """

            with st.spinner("Calculating factor scores..."):
                factor_df = pd.read_sql_query(factor_query, con)

            if factor_df.empty:
                st.info("No factor data available. Scrape company data first.")
            else:
                # Convert numeric columns
                factor_df["price"] = pd.to_numeric(factor_df["price"], errors="coerce")
                factor_df["pe_ratio"] = pd.to_numeric(factor_df["pe_ratio"], errors="coerce")
                factor_df["return_20d"] = pd.to_numeric(factor_df["return_20d"], errors="coerce")
                factor_df["return_60d"] = pd.to_numeric(factor_df["return_60d"], errors="coerce")
                factor_df["market_cap"] = pd.to_numeric(factor_df["market_cap"], errors="coerce")
                factor_df["outstanding_shares"] = pd.to_numeric(factor_df["outstanding_shares"], errors="coerce")
                factor_df["eps"] = pd.to_numeric(factor_df["eps"], errors="coerce")
                factor_df["net_margin"] = pd.to_numeric(factor_df["net_margin"], errors="coerce")
                factor_df["ytd_change"] = pd.to_numeric(factor_df["ytd_change"], errors="coerce")

                # Compute market_cap from price × shares when scraper value is missing/zero
                missing_mcap = factor_df["market_cap"].fillna(0) == 0
                factor_df.loc[missing_mcap, "market_cap"] = (
                    factor_df.loc[missing_mcap, "price"] * factor_df.loc[missing_mcap, "outstanding_shares"]
                )

                # Value Score: Low P/E + High Net Margin (profitable at low valuation)
                factor_df["value_score"] = 0.0
                if factor_df["pe_ratio"].notna().sum() > 5:
                    # Invert P/E (lower is better)
                    pe_valid = factor_df["pe_ratio"] > 0
                    factor_df.loc[pe_valid, "value_score"] += (1 - factor_df.loc[pe_valid, "pe_ratio"].rank(pct=True)) * 0.6
                if factor_df["net_margin"].notna().sum() > 5:
                    # Higher net margin is better
                    margin_valid = factor_df["net_margin"] > 0
                    factor_df.loc[margin_valid, "value_score"] += factor_df.loc[margin_valid, "net_margin"].rank(pct=True) * 0.4

                # Momentum Score: 20-day, 60-day returns, and YTD change
                factor_df["momentum_score"] = 0.0
                if factor_df["return_20d"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["return_20d"].rank(pct=True).fillna(0) * 0.4
                if factor_df["return_60d"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["return_60d"].rank(pct=True).fillna(0) * 0.4
                if factor_df["ytd_change"].notna().sum() > 5:
                    factor_df["momentum_score"] += factor_df["ytd_change"].rank(pct=True).fillna(0) * 0.2

                # Quality Score: Higher EPS + larger market cap + higher margins
                factor_df["quality_score"] = 0.0
                if factor_df["eps"].notna().sum() > 5:
                    eps_positive = factor_df["eps"] > 0
                    factor_df.loc[eps_positive, "quality_score"] += factor_df.loc[eps_positive, "eps"].rank(pct=True) * 0.4
                if factor_df["market_cap"].notna().sum() > 5:
                    factor_df["quality_score"] += factor_df["market_cap"].rank(pct=True).fillna(0) * 0.3
                if factor_df["net_margin"].notna().sum() > 5:
                    margin_valid = factor_df["net_margin"] > 0
                    factor_df.loc[margin_valid, "quality_score"] += factor_df.loc[margin_valid, "net_margin"].rank(pct=True) * 0.3

                # Volatility Score: Lower 52-week range is better (inverted)
                factor_df["wk52_low"] = pd.to_numeric(factor_df["wk52_low"], errors="coerce")
                factor_df["wk52_high"] = pd.to_numeric(factor_df["wk52_high"], errors="coerce")
                factor_df["volatility_range"] = (factor_df["wk52_high"] - factor_df["wk52_low"]) / factor_df["wk52_low"]
                factor_df["volatility_score"] = 0.0
                if factor_df["volatility_range"].notna().sum() > 5:
                    # Invert - lower volatility is better
                    factor_df["volatility_score"] = 1 - factor_df["volatility_range"].rank(pct=True).fillna(0.5)

                # Composite Score
                factor_df["composite_score"] = (
                    w_value * factor_df["value_score"].fillna(0) +
                    w_momentum * factor_df["momentum_score"].fillna(0) +
                    w_quality * factor_df["quality_score"].fillna(0) +
                    w_volatility * factor_df["volatility_score"].fillna(0)
                )

                # Rank by composite score
                factor_df["rank"] = factor_df["composite_score"].rank(ascending=False, method="min")
                factor_df = factor_df.sort_values("composite_score", ascending=False)

                # Display top stocks
                st.markdown("#### Top Ranked Stocks")

                display_cols = [
                    "rank", "symbol", "company_name", "sector_code",
                    "composite_score", "value_score", "momentum_score",
                    "quality_score", "volatility_score",
                    "pe_ratio", "return_20d", "market_cap"
                ]
                display_df = factor_df[display_cols].head(30).copy()
                display_df.columns = [
                    "Rank", "Symbol", "Company", "Sector",
                    "Composite", "Value", "Momentum", "Quality", "LowVol",
                    "P/E", "20D Ret%", "Mkt Cap"
                ]

                # Format market cap
                display_df["Mkt Cap"] = display_df["Mkt Cap"].apply(
                    lambda x: f"Rs.{x/1e9:.1f}B" if pd.notna(x) and x >= 1e9
                    else f"Rs.{x/1e6:.0f}M" if pd.notna(x) else "N/A"
                )

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Rank": st.column_config.NumberColumn(format="%d"),
                        "Composite": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Value": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Momentum": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Quality": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "LowVol": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "P/E": st.column_config.NumberColumn(format="%.1f"),
                        "20D Ret%": st.column_config.NumberColumn(format="%.1f%%"),
                    }
                )

                # Quick stats
                st.markdown("---")
                stat_cols = st.columns(4)
                with stat_cols[0]:
                    st.metric("Total Stocks Analyzed", len(factor_df))
                with stat_cols[1]:
                    avg_pe = factor_df["pe_ratio"].median()
                    st.metric("Median P/E", f"{avg_pe:.1f}" if pd.notna(avg_pe) else "N/A")
                with stat_cols[2]:
                    avg_momentum = factor_df["return_20d"].median()
                    st.metric("Median 20D Return", f"{avg_momentum:+.1f}%" if pd.notna(avg_momentum) else "N/A")
                with stat_cols[3]:
                    value_stocks = len(factor_df[factor_df["value_score"] > 0.7])
                    st.metric("High Value Stocks", value_stocks)

        except Exception as e:
            st.error(f"Error calculating factors: {e}")
            import traceback
            st.code(traceback.format_exc())

    # =========================================================================
    # Tab 2: Factor Correlations
    # =========================================================================
    with tab2:
        st.subheader("Factor Correlation Matrix")
        st.markdown("""
        Correlation between different factors. Low correlation means
        factors provide diversified signals.
        """)

        try:
            if 'factor_df' in dir() and not factor_df.empty:
                corr_cols = ["value_score", "momentum_score", "quality_score", "volatility_score"]
                corr_matrix = factor_df[corr_cols].corr()

                # Rename for display
                corr_matrix.index = ["Value", "Momentum", "Quality", "LowVol"]
                corr_matrix.columns = ["Value", "Momentum", "Quality", "LowVol"]

                import plotly.express as px
                fig = px.imshow(
                    corr_matrix,
                    text_auto=".2f",
                    color_continuous_scale="RdBu_r",
                    zmin=-1, zmax=1,
                    title="Factor Correlation Matrix"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("""
                **Interpretation:**
                - Values close to **+1.0** = highly correlated (redundant signals)
                - Values close to **-1.0** = negatively correlated (hedging signals)
                - Values close to **0** = uncorrelated (diversifying signals)
                """)
            else:
                st.info("Run Factor Rankings first to see correlations.")
        except Exception as e:
            st.error(f"Error: {e}")

    # =========================================================================
    # Tab 3: Sector Exposure
    # =========================================================================
    with tab3:
        st.subheader("Factor Exposure by Sector")
        st.markdown("Average factor scores by sector to identify sector biases.")

        try:
            if 'factor_df' in dir() and not factor_df.empty:
                # Get sector names
                sector_map = get_sector_names(con)

                sector_exposure = factor_df.groupby("sector_code").agg({
                    "value_score": "mean",
                    "momentum_score": "mean",
                    "quality_score": "mean",
                    "volatility_score": "mean",
                    "symbol": "count"
                }).reset_index()
                sector_exposure.columns = ["Sector Code", "Value", "Momentum", "Quality", "LowVol", "Count"]
                sector_exposure["Sector"] = sector_exposure["Sector Code"].map(sector_map).fillna(sector_exposure["Sector Code"])
                sector_exposure = sector_exposure.sort_values("Count", ascending=False)

                st.dataframe(
                    sector_exposure[["Sector", "Count", "Value", "Momentum", "Quality", "LowVol"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Value": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Momentum": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "Quality": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        "LowVol": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                    }
                )

                # Bar chart
                import plotly.express as px
                melt_df = sector_exposure.melt(
                    id_vars=["Sector"],
                    value_vars=["Value", "Momentum", "Quality", "LowVol"],
                    var_name="Factor",
                    value_name="Score"
                )
                fig = px.bar(
                    melt_df.head(40),  # Top 10 sectors x 4 factors
                    x="Sector",
                    y="Score",
                    color="Factor",
                    barmode="group",
                    title="Factor Scores by Sector"
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Run Factor Rankings first to see sector exposure.")
        except Exception as e:
            st.error(f"Error: {e}")

    # =========================================================================
    # Tab 4: Risk Metrics
    # =========================================================================
    with tab4:
        st.subheader("Portfolio Risk Metrics")
        st.markdown("""
        Risk analysis for top-ranked stocks. Essential for position sizing
        and portfolio construction.
        """)

        try:
            if 'factor_df' in dir() and not factor_df.empty:
                # Get top 20 stocks
                top_stocks = factor_df.head(20)["symbol"].tolist()

                # Calculate volatility from EOD data
                risk_query = """
                    SELECT
                        symbol,
                        COUNT(*) as trading_days,
                        AVG(close) as avg_price,
                        MIN(close) as min_price,
                        MAX(close) as max_price,
                        (MAX(close) - MIN(close)) / AVG(close) * 100 as range_pct
                    FROM eod_ohlcv
                    WHERE symbol IN ({})
                    AND date >= date('now', '-90 days')
                    GROUP BY symbol
                """.format(",".join([f"'{s}'" for s in top_stocks]))

                risk_df = pd.read_sql_query(risk_query, con)

                if not risk_df.empty:
                    # Merge with factor data
                    risk_df = risk_df.merge(
                        factor_df[["symbol", "composite_score", "market_cap"]],
                        on="symbol",
                        how="left"
                    )

                    st.markdown("#### Top 20 Stocks - Risk Profile")
                    risk_df["market_cap_str"] = risk_df["market_cap"].apply(
                        lambda x: f"Rs.{x/1e9:.1f}B" if pd.notna(x) and x >= 1e9
                        else f"Rs.{x/1e6:.0f}M" if pd.notna(x) else "N/A"
                    )

                    st.dataframe(
                        risk_df[["symbol", "trading_days", "avg_price", "range_pct", "composite_score", "market_cap_str"]].rename(columns={
                            "symbol": "Symbol",
                            "trading_days": "Days",
                            "avg_price": "Avg Price",
                            "range_pct": "90D Range%",
                            "composite_score": "Score",
                            "market_cap_str": "Mkt Cap"
                        }),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Avg Price": st.column_config.NumberColumn(format="Rs.%.2f"),
                            "90D Range%": st.column_config.NumberColumn(format="%.1f%%"),
                            "Score": st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f"),
                        }
                    )

                    # Summary metrics
                    st.markdown("---")
                    st.markdown("#### Portfolio Summary (Equal-Weight Top 20)")

                    metric_cols = st.columns(4)
                    with metric_cols[0]:
                        avg_range = risk_df["range_pct"].mean()
                        st.metric("Avg 90D Range", f"{avg_range:.1f}%")
                    with metric_cols[1]:
                        total_mktcap = risk_df["market_cap"].sum()
                        st.metric("Total Mkt Cap", f"Rs.{total_mktcap/1e9:.0f}B" if total_mktcap else "N/A")
                    with metric_cols[2]:
                        avg_score = risk_df["composite_score"].mean()
                        st.metric("Avg Score", f"{avg_score:.2f}")
                    with metric_cols[3]:
                        st.metric("Stocks", len(risk_df))

                    st.markdown("""
                    ---
                    **Risk Notes:**
                    - 90D Range% shows price volatility - higher = riskier
                    - Consider position sizing inversely to volatility
                    - PSX circuit breakers limit daily moves to ±7.5%
                    - Thin liquidity stocks may have execution slippage
                    """)

                    # KSE-100 Benchmark Comparison
                    st.markdown("---")
                    st.markdown("#### Benchmark Comparison")

                    kse100 = get_latest_kse100(con)
                    if kse100:
                        bench_cols = st.columns([2, 1, 1])
                        with bench_cols[0]:
                            kse_value = kse100.get("value", 0)
                            kse_change = kse100.get("change_pct", 0) or 0
                            kse_color = "#00C853" if kse_change >= 0 else "#FF1744"

                            st.markdown(f"""
                            <div style="background: rgba(33,150,243,0.1); border-radius: 8px; padding: 12px;
                                        border: 1px solid rgba(33,150,243,0.2);">
                                <div style="font-size: 11px; color: #888;">KSE-100 Index</div>
                                <div style="font-family: monospace; font-size: 18px; font-weight: 600;">
                                    {kse_value:,.2f}
                                    <span style="color: {kse_color}; font-size: 14px;">({kse_change:+.2f}%)</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                        with bench_cols[1]:
                            ytd = kse100.get("ytd_change_pct")
                            if ytd:
                                ytd_color = "#00C853" if ytd >= 0 else "#FF1744"
                                st.metric("Index YTD", f"{ytd:+.2f}%", delta_color="off")
                        with bench_cols[2]:
                            one_yr = kse100.get("one_year_change_pct")
                            if one_yr:
                                st.metric("Index 1-Year", f"{one_yr:+.2f}%", delta_color="off")

                        st.caption("Compare factor portfolio performance against KSE-100 benchmark to measure alpha generation.")
                    else:
                        st.info("No KSE-100 benchmark data available. Scrape index data to enable benchmark comparison.")

                else:
                    st.info("Insufficient price history for risk analysis.")
            else:
                st.info("Run Factor Rankings first to see risk metrics.")
        except Exception as e:
            st.error(f"Error: {e}")

    render_footer()
