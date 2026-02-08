"""AI insights and analysis page."""

from datetime import datetime, timedelta
import streamlit as st
import time

from psx_ohlcv.query import get_symbols_list
from psx_ohlcv.ui.session_tracker import (
    track_button_click,
    track_page_visit,
)
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_ai_insights():
    """AI-powered market insights using OpenAI GPT-5.2.

    This page provides LLM-generated analysis for:
    - Company summaries (profile + quote + OHLCV)
    - Intraday commentary (time series + volume)
    - Market summaries (gainers/losers + sectors)
    - Historical analysis (OHLCV patterns)
    """
    import os

    # =================================================================
    # HEADER
    # =================================================================
    # Custom CSS for AI Insights page theme
    st.markdown("""
    <style>
    /* Page Header Styling */
    .ai-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(102, 126, 234, 0.3);
    }
    .ai-header h1 {
        color: white;
        margin: 0;
        font-size: 2em;
    }
    .ai-header p {
        color: rgba(255, 255, 255, 0.85);
        margin: 8px 0 0 0;
    }

    /* Mode Selection Cards */
    .mode-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        padding: 12px 16px;
        margin: 4px 0;
        transition: all 0.3s ease;
    }
    .mode-card:hover {
        background: rgba(102, 126, 234, 0.1);
        border-color: rgba(102, 126, 234, 0.3);
    }
    .mode-card.active {
        background: rgba(102, 126, 234, 0.15);
        border-color: #667eea;
        box-shadow: 0 2px 10px rgba(102, 126, 234, 0.2);
    }

    /* Generate Button Enhancement */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        border: none !important;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4) !important;
        transition: all 0.3s ease !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6) !important;
        transform: translateY(-2px);
    }

    /* Info Cards */
    .info-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 8px;
        padding: 16px;
        border-left: 3px solid #667eea;
    }
    </style>
    """, unsafe_allow_html=True)

    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🤖 AI Insights")
        st.caption("GPT-5.2 powered market analysis • Company, Intraday, Market & Historical insights")
    with header_col2:
        render_market_status_badge()

    con = get_connection()
    track_page_visit(con, "AI Insights")

    # =================================================================
    # API KEY CHECK
    # =================================================================
    api_key_set = bool(os.environ.get("OPENAI_API_KEY", "").strip())

    if not api_key_set:
        st.warning(
            "**OpenAI API Key Not Configured**\n\n"
            "To use AI Insights, set the `OPENAI_API_KEY` environment variable:\n"
            "```bash\n"
            "export OPENAI_API_KEY='your-api-key-here'\n"
            "```\n\n"
            "Then restart the Streamlit app."
        )
        st.info(
            "**Why is an API key needed?**\n"
            "AI Insights uses OpenAI's GPT-5.2 model to analyze your stock data "
            "and provide intelligent commentary. This requires an OpenAI API account."
        )
        render_footer()
        return

    st.markdown("---")

    # =================================================================
    # DATA CAVEAT WARNING (Always shown)
    # =================================================================
    with st.expander("⚠️ Important Data Caveats", expanded=False):
        st.warning(
            "**DERIVED HIGH/LOW WARNING**\n\n"
            "The daily high and low values in this application's EOD data are calculated as "
            "`max(open, close)` and `min(open, close)` respectively.\n\n"
            "**These are NOT true intraday highs and lows.** Actual intraday price "
            "extremes may differ significantly from what is shown.\n\n"
            "The AI analysis acknowledges this limitation."
        )
        st.info(
            "**Other Caveats:**\n"
            "- PSX circuit breakers: ±7.5% daily limits\n"
            "- Some stocks may have thin liquidity\n"
            "- Data may have slight delays from live market\n"
            "- Historical data subject to corporate actions"
        )

    # =================================================================
    # INSIGHT MODE SELECTION
    # =================================================================
    st.markdown("### 📊 Select Analysis Mode")

    # Mode details with icons and enhanced descriptions
    mode_details = {
        "Company": {
            "icon": "🏢",
            "title": "Company Summary",
            "desc": "Profile, latest quote, OHLCV history, financials & corporate news",
        },
        "Intraday": {
            "icon": "📈",
            "title": "Intraday Analysis",
            "desc": "Session price/volume patterns, trading activity & momentum",
        },
        "Market": {
            "icon": "🌐",
            "title": "Market Summary",
            "desc": "Gainers, losers, sector performance & market breadth",
        },
        "History": {
            "icon": "📜",
            "title": "Historical Analysis",
            "desc": "Long-term OHLCV patterns, trends & technical insights",
        },
    }

    # Create 4 columns for mode cards
    mode_cols = st.columns(4)
    modes = ["Company", "Intraday", "Market", "History"]

    # Use session state for mode selection
    if "ai_insight_mode" not in st.session_state:
        st.session_state.ai_insight_mode = "Company"

    for i, mode in enumerate(modes):
        with mode_cols[i]:
            details = mode_details[mode]
            is_selected = st.session_state.ai_insight_mode == mode
            if st.button(
                f"{details['icon']} {details['title']}",
                key=f"mode_{mode}",
                use_container_width=True,
                type="primary" if is_selected else "secondary",
            ):
                st.session_state.ai_insight_mode = mode
                st.rerun()

    insight_mode = st.session_state.ai_insight_mode

    # Show selected mode description
    selected_details = mode_details[insight_mode]
    st.info(f"{selected_details['icon']} **{selected_details['title']}**: {selected_details['desc']}")

    st.markdown("---")

    # =================================================================
    # MODE-SPECIFIC CONTROLS
    # =================================================================
    if insight_mode in ["Company", "Intraday", "History"]:
        # Symbol selection - get_symbols_list returns list of strings directly
        symbol_options = get_symbols_list(con)

        if not symbol_options:
            st.warning("No symbols available. Please sync data first.")
            render_footer()
            return

        selected_symbol = st.selectbox(
            "Select Symbol",
            options=symbol_options,
            index=0,
            help="Choose a stock symbol for analysis",
        )
    else:
        selected_symbol = None

    # Mode-specific parameters
    if insight_mode == "Company":
        ohlcv_days = st.slider(
            "OHLCV History (days)",
            min_value=5,
            max_value=90,
            value=30,
            help="Number of trading days of price history to include",
        )
        include_financials = st.checkbox("Include Financial Data", value=True)

    elif insight_mode == "Intraday":
        # Get available intraday dates
        try:
            cur = con.execute(
                """
                SELECT DISTINCT DATE(ts) as date
                FROM intraday_bars
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT 30
                """,
                (selected_symbol,),
            )
            available_dates = [row[0] for row in cur.fetchall()]
        except Exception:
            available_dates = []

        if available_dates:
            trading_date = st.selectbox(
                "Trading Date",
                options=available_dates,
                index=0,
                help="Select the trading day for intraday analysis",
            )
        else:
            st.warning(f"No intraday data available for {selected_symbol}")
            trading_date = None

    elif insight_mode == "Market":
        # Get available market dates
        try:
            cur = con.execute(
                """
                SELECT DISTINCT session_date
                FROM trading_sessions
                WHERE market_type = 'REG'
                ORDER BY session_date DESC
                LIMIT 30
                """
            )
            market_dates = [row[0] for row in cur.fetchall()]
        except Exception:
            market_dates = []

        if market_dates:
            market_date = st.selectbox(
                "Market Date",
                options=market_dates,
                index=0,
                help="Select the date for market summary",
            )
        else:
            st.warning("No market data available")
            market_date = None

        top_n = st.slider(
            "Top N Movers",
            min_value=5,
            max_value=20,
            value=10,
            help="Number of top gainers/losers to include",
        )

    elif insight_mode == "History":
        history_days = st.slider(
            "History Period (days)",
            min_value=30,
            max_value=365,
            value=90,
            help="Number of trading days to analyze",
        )

    st.markdown("---")

    # =================================================================
    # GENERATE BUTTON AND RESULTS
    # =================================================================
    st.markdown("### 🚀 Generate Analysis")

    gen_col1, gen_col2, gen_col3 = st.columns([2, 1, 1])

    with gen_col1:
        generate_clicked = st.button(
            "✨ Generate AI Insight",
            type="primary",
            use_container_width=True,
            help="Generate AI-powered analysis using GPT-5.2",
        )

    with gen_col2:
        use_cache = st.checkbox("💾 Use Cache", value=True, help="Use cached responses if available (6hr TTL)")

    with gen_col3:
        # Show estimated tokens
        est_tokens = {
            "Company": "~2-3k tokens",
            "Intraday": "~1.5-2.5k tokens",
            "Market": "~2.5-3.5k tokens",
            "History": "~3-4.5k tokens",
        }
        st.caption(f"Est: {est_tokens.get(insight_mode, '~2k tokens')}")

    # Generate insight when button clicked
    if generate_clicked:
        try:
            # Import from consolidated agents module
            from psx_ohlcv.agents.llm_client import LLMError, is_api_key_configured, create_client
            from psx_ohlcv.agents.config import get_active_config
            from psx_ohlcv.agents.prompts import PromptBuilder, InsightMode as LLMInsightMode
            from psx_ohlcv.agents.cache import LLMCache, init_llm_cache_schema, get_db_freshness_marker
            from psx_ohlcv.agents.data_loader import DataLoader, format_data_for_prompt

            # Initialize cache
            init_llm_cache_schema(con)
            cache = LLMCache(con, ttl_hours=6)
            loader = DataLoader(con)

            # Map UI mode to LLM mode
            mode_mapping = {
                "Company": LLMInsightMode.COMPANY,
                "Intraday": LLMInsightMode.INTRADAY,
                "Market": LLMInsightMode.MARKET,
                "History": LLMInsightMode.HISTORY,
            }
            llm_mode = mode_mapping[insight_mode]

            # Load data based on mode
            with st.spinner("Loading data..."):
                if insight_mode == "Company":
                    data = loader.load_company_data(
                        selected_symbol,
                        ohlcv_days=ohlcv_days,
                        include_financials=include_financials,
                    )
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = selected_symbol
                    date_range = prompt_data.get("date_range", "")

                elif insight_mode == "Intraday":
                    if not trading_date:
                        st.error("No trading date selected")
                        return
                    data = loader.load_intraday_data(selected_symbol, trading_date)
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = selected_symbol
                    date_range = trading_date

                elif insight_mode == "Market":
                    if not market_date:
                        st.error("No market date selected")
                        return
                    data = loader.load_market_data(market_date, top_n=top_n)
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = "MARKET"
                    date_range = market_date

                elif insight_mode == "History":
                    data = loader.load_company_data(
                        selected_symbol,
                        ohlcv_days=history_days,
                        include_financials=False,
                    )
                    prompt_data = format_data_for_prompt(data)
                    cache_symbol = selected_symbol
                    date_range = prompt_data.get("date_range", "")

            # Show data provenance
            with st.expander("📊 Data Used (click to expand)", expanded=False):
                st.markdown("**Tables Queried:**")
                if hasattr(data, 'provenance'):
                    st.write(data.provenance.tables_used)
                    st.markdown(f"**Row Count:** {data.provenance.row_count}")
                    st.markdown(f"**Date Range:** {data.provenance.date_range[0]} to {data.provenance.date_range[1]}")
                    if data.provenance.was_downsampled:
                        st.warning(f"Data was downsampled from {data.provenance.original_row_count} rows")
                st.markdown(f"**Generated At:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Build prompt
            builder = PromptBuilder(llm_mode)
            prompt = builder.build(**prompt_data)

            # Check cache
            db_freshness = get_db_freshness_marker(con, cache_symbol if cache_symbol != "MARKET" else None)
            cache_key = cache.compute_key(
                symbol=cache_symbol,
                mode=llm_mode.value,
                date_range=date_range,
                db_freshness=db_freshness,
            )

            cached_response = None
            if use_cache:
                cached_response = cache.get(cache_key)

            if cached_response:
                st.success("✅ Using cached response")
                response_text = cached_response.response_text
                was_cached = True
            else:
                # Generate with LLM (multi-provider via agents config)
                with st.spinner("🤖 Generating AI insight (this may take a moment)..."):
                    config = get_active_config()
                    client = create_client(config.agent_model)

                    response = client.create_message(
                        messages=[{"role": "user", "content": prompt}],
                        system=builder.system_prompt,
                    )

                    response_text = response.text
                    was_cached = False

                    # Extract token usage
                    prompt_tokens = response.usage.get("input_tokens", 0)
                    completion_tokens = response.usage.get("output_tokens", 0)
                    total_tokens = prompt_tokens + completion_tokens

                    # Cache the response
                    cache.set(
                        cache_key=cache_key,
                        response_text=response_text,
                        symbol=cache_symbol,
                        mode=llm_mode.value,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        model=config.agent_model.model_id,
                    )

                    # Show token usage in metrics
                    token_cols = st.columns(4)
                    with token_cols[0]:
                        st.metric("Prompt Tokens", f"{prompt_tokens:,}")
                    with token_cols[1]:
                        st.metric("Completion", f"{completion_tokens:,}")
                    with token_cols[2]:
                        st.metric("Total", f"{total_tokens:,}")
                    with token_cols[3]:
                        est_cost = (prompt_tokens * 0.01 + completion_tokens * 0.03) / 1000
                        st.metric("Est. Cost", f"${est_cost:.4f}")

            # Display response with enhanced styling
            st.markdown("---")

            # Custom CSS for AI Insights styling
            st.markdown("""
            <style>
            /* AI Insights Theme Styling */
            .ai-insights-container {
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                border-radius: 12px;
                padding: 20px;
                margin: 10px 0;
            }

            /* Assessment Box Styling */
            .assessment-box {
                background: linear-gradient(135deg, #0f3460 0%, #16213e 100%);
                border-left: 4px solid #00d9ff;
                border-radius: 8px;
                padding: 16px 20px;
                margin-bottom: 20px;
                box-shadow: 0 4px 15px rgba(0, 217, 255, 0.1);
            }
            .assessment-bullish {
                border-left-color: #00ff88;
                box-shadow: 0 4px 15px rgba(0, 255, 136, 0.15);
            }
            .assessment-bearish {
                border-left-color: #ff4757;
                box-shadow: 0 4px 15px rgba(255, 71, 87, 0.15);
            }
            .assessment-neutral {
                border-left-color: #ffa502;
                box-shadow: 0 4px 15px rgba(255, 165, 2, 0.15);
            }

            /* Section Styling */
            .ai-section {
                background: rgba(255, 255, 255, 0.03);
                border-radius: 8px;
                padding: 16px;
                margin: 12px 0;
                border: 1px solid rgba(255, 255, 255, 0.08);
            }
            .ai-section h2, .ai-section h3 {
                color: #00d9ff;
                margin-top: 0;
            }

            /* Action Items Styling */
            .action-items {
                background: linear-gradient(135deg, #1e3a5f 0%, #16213e 100%);
                border-radius: 8px;
                padding: 16px;
                margin-top: 16px;
                border: 1px solid rgba(0, 217, 255, 0.2);
            }
            .action-items li {
                padding: 8px 0;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }
            .action-items li:last-child {
                border-bottom: none;
            }

            /* Metrics Table Styling */
            .ai-insights-container table {
                width: 100%;
                border-collapse: collapse;
                margin: 12px 0;
            }
            .ai-insights-container th {
                background: rgba(0, 217, 255, 0.1);
                padding: 10px;
                text-align: left;
                border-bottom: 2px solid rgba(0, 217, 255, 0.3);
            }
            .ai-insights-container td {
                padding: 10px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }

            /* Blockquote Styling for Assessment */
            .ai-insights-container blockquote {
                background: linear-gradient(135deg, #0f3460 0%, #1a1a2e 100%);
                border-left: 4px solid #00d9ff;
                padding: 16px 20px;
                margin: 16px 0;
                border-radius: 0 8px 8px 0;
                font-size: 1.05em;
            }

            /* Disclaimer Styling */
            .ai-disclaimer {
                background: rgba(255, 165, 2, 0.1);
                border: 1px solid rgba(255, 165, 2, 0.3);
                border-radius: 8px;
                padding: 12px 16px;
                margin-top: 20px;
                font-size: 0.85em;
                color: #ffa502;
            }
            </style>
            """, unsafe_allow_html=True)

            # Display header with cache status
            header_cols = st.columns([3, 1])
            with header_cols[0]:
                st.markdown("### 🤖 AI Analysis")
            with header_cols[1]:
                if was_cached:
                    st.markdown("🔄 *Cached*")
                else:
                    st.markdown("✨ *Fresh*")

            # Wrap response in styled container
            st.markdown('<div class="ai-insights-container">', unsafe_allow_html=True)
            st.markdown(response_text)
            st.markdown('</div>', unsafe_allow_html=True)

            # Copy prompt button (in expander)
            with st.expander("🔧 Debug: View Full Prompt", expanded=False):
                st.text_area(
                    "Prompt sent to LLM",
                    value=prompt,
                    height=400,
                    disabled=True,
                )
                if st.button("📋 Copy Prompt"):
                    st.code(prompt)

            # Track the generation
            track_button_click(con, "AI Insights", f"Generate {insight_mode}")

        except ImportError as e:
            st.error(
                f"**LLM Module Import Error**\n\n"
                f"Could not import LLM modules: {e}\n\n"
                "Install missing dependencies: `pip install tabulate`"
            )

        except LLMError as e:
            st.error(f"**LLM Error**\n\n{e}")

        except Exception as e:
            st.error(f"**Error generating insight**\n\n{e}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())

    # =================================================================
    # CACHE MANAGEMENT (in sidebar or expander)
    # =================================================================
    st.markdown("---")
    st.markdown("### ⚙️ Settings & Cache")

    settings_cols = st.columns(2)

    with settings_cols[0]:
        with st.expander("💾 Cache Management", expanded=False):
            try:
                from psx_ohlcv.agents.cache import LLMCache, init_llm_cache_schema

                init_llm_cache_schema(con)
                cache = LLMCache(con)

                stats = cache.get_stats()

                cache_col1, cache_col2 = st.columns(2)
                with cache_col1:
                    st.metric("📦 Active", stats.get("active_entries", 0))
                    st.metric("⏰ Expired", stats.get("expired_entries", 0))
                with cache_col2:
                    total_tokens = stats.get("total_prompt_tokens", 0) + stats.get("total_completion_tokens", 0)
                    st.metric("🔢 Tokens Used", f"{total_tokens:,}")
                    est_savings = total_tokens * 0.00002  # rough estimate
                    st.metric("💰 Cache Savings", f"~${est_savings:.2f}")

                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    if st.button("🧹 Clear Expired", use_container_width=True):
                        cleared = cache.cleanup_expired()
                        st.success(f"Cleared {cleared} expired entries")
                with btn_col2:
                    if st.button("🗑️ Clear All", type="secondary", use_container_width=True):
                        cleared = cache.clear_all()
                        st.success(f"Cleared {cleared} entries")

            except Exception as e:
                st.warning(f"Cache management unavailable: {e}")

    with settings_cols[1]:
        with st.expander("💡 Cost Control Tips", expanded=False):
            st.markdown("""
            **🎯 Minimize API Costs:**

            | Tip | Impact |
            |-----|--------|
            | ✅ Use Caching | High |
            | 📅 Shorter time windows | Medium |
            | 📊 Fewer top movers | Low |
            | 🔄 Batch analysis | Medium |

            **📈 Token Estimates:**
            - Company (30d): ~2-3k tokens
            - Intraday: ~1.5-2.5k tokens
            - Market (10): ~2.5-3.5k tokens
            - History (90d): ~3-4.5k tokens

            *Cache TTL: 6 hours*
            """)

    render_footer()
