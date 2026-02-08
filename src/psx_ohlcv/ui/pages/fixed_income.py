"""Fixed income pages (bonds, sukuk, yield curves, SBP auctions)."""

import json
import pandas as pd
import streamlit as st
import time

from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
)


def render_bonds_screener():
    """Bonds Screener - Fixed income instruments."""
    import pandas as pd

    from psx_ohlcv.analytics_bonds import get_bond_full_analytics
    from psx_ohlcv.db import get_bond_data_summary, get_bonds
    from psx_ohlcv.sync_bonds import seed_bonds, sync_sample_quotes

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🧾 Bonds Screener")
        st.caption("Fixed Income Analytics (Phase 3 - Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # =================================================================
    # DATA SYNC CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        sync_col1, sync_col2, sync_col3 = st.columns(3)

        with sync_col1:
            if st.button("Initialize Bonds", key="bonds_init"):
                with st.spinner("Seeding default bonds..."):
                    result = seed_bonds()
                    if result.get("success"):
                        st.success(f"Seeded {result['inserted']} bonds")
                        st.rerun()
                    else:
                        st.error(f"Error: {result.get('error')}")

        with sync_col2:
            days = st.number_input("Sample Days", min_value=30, max_value=365, value=90)
            if st.button("Generate Sample Quotes", key="bonds_sample"):
                with st.spinner("Generating sample data..."):
                    summary = sync_sample_quotes(days=days)
                    st.success(f"Generated {summary.rows_upserted} quotes")
                    st.rerun()

        with sync_col3:
            summary = get_bond_data_summary(con)
            st.metric("Total Bonds", summary.get("total_bonds", 0))
            st.metric("Quote Rows", summary.get("total_quote_rows", 0))

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

    with filter_col1:
        bond_type = st.selectbox(
            "Bond Type",
            ["ALL", "PIB", "T-Bill", "Sukuk", "TFC", "Corporate"],
            key="bonds_type_filter"
        )

    with filter_col2:
        issuer = st.text_input("Issuer", "", key="bonds_issuer_filter")

    with filter_col3:
        islamic_only = st.checkbox("Islamic Only", key="bonds_islamic")

    with filter_col4:
        min_ytm = st.number_input(
            "Min YTM (%)", min_value=0.0, max_value=30.0, value=0.0,
            step=0.5, key="bonds_min_ytm"
        )

    # =================================================================
    # BONDS TABLE
    # =================================================================
    st.markdown("### Bond Universe")

    bonds = get_bonds(
        con,
        bond_type=None if bond_type == "ALL" else bond_type,
        issuer=issuer if issuer else None,
        is_islamic=True if islamic_only else None,
        active_only=True,
    )

    if not bonds:
        st.warning("No bonds found. Click 'Initialize Bonds' to seed default data.")
    else:
        # Build table with analytics
        table_data = []
        for bond in bonds:
            analytics = get_bond_full_analytics(con, bond["bond_id"])
            ytm = analytics.get("ytm")

            # Apply YTM filter
            if min_ytm > 0 and (ytm is None or ytm * 100 < min_ytm):
                continue

            coupon = bond.get("coupon_rate")
            coupon_str = f"{coupon * 100:.1f}%" if coupon else "Zero"
            price = analytics.get("price")
            price_str = f"{price:.2f}" if price else "N/A"
            mod_dur = analytics.get("modified_duration")
            dur_str = f"{mod_dur:.2f}" if mod_dur else "N/A"

            table_data.append({
                "Symbol": bond.get("symbol"),
                "Type": bond.get("bond_type"),
                "Issuer": bond.get("issuer"),
                "Coupon": coupon_str,
                "Maturity": bond.get("maturity_date"),
                "Price": price_str,
                "YTM": f"{ytm * 100:.2f}%" if ytm else "N/A",
                "Duration": dur_str,
                "Islamic": "Yes" if bond.get("is_islamic") else "No",
            })

        if table_data:
            df = pd.DataFrame(table_data)
            st.dataframe(df, use_container_width=True, hide_index=True)

            # =================================================================
            # BOND DETAILS
            # =================================================================
            st.markdown("### Bond Details")

            bond_options = {b["bond_id"]: b["symbol"] for b in bonds}
            selected_bond = st.selectbox(
                "Select Bond",
                options=list(bond_options.keys()),
                format_func=lambda x: bond_options[x],
                key="bonds_selected"
            )

            if selected_bond:
                analytics = get_bond_full_analytics(con, selected_bond)

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    price = analytics.get("price")
                    price_val = f"{price:.4f}" if price else "N/A"
                    st.metric("Price", price_val)

                with col2:
                    ytm = analytics.get("ytm")
                    ytm_val = f"{ytm * 100:.4f}%" if ytm else "N/A"
                    st.metric("YTM", ytm_val)

                with col3:
                    dur = analytics.get("duration")
                    dur_val = f"{dur:.4f} yrs" if dur else "N/A"
                    st.metric("Duration", dur_val)

                with col4:
                    conv = analytics.get("convexity")
                    conv_val = f"{conv:.4f}" if conv else "N/A"
                    st.metric("Convexity", conv_val)

                # Additional details
                st.markdown("#### Details")
                cpn = analytics.get("coupon_rate")
                cpn_str = f"{cpn * 100:.2f}%" if cpn else "Zero-coupon"
                detail_data = {
                    "Bond ID": analytics.get("bond_id"),
                    "Issuer": analytics.get("issuer"),
                    "Maturity Date": analytics.get("maturity_date"),
                    "Days to Maturity": analytics.get("days_to_maturity"),
                    "Coupon Rate": cpn_str,
                    "Face Value": analytics.get("face_value"),
                    "Accrued Interest": analytics.get("accrued_interest"),
                    "Dirty Price": analytics.get("dirty_price"),
                }
                st.json(detail_data)
        else:
            st.info("No bonds match the current filters.")

    render_footer()


def render_yield_curve():
    """Yield Curve - Interest rate term structure."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.analytics_bonds import (
        build_yield_curve,
        interpolate_yield,
    )
    from psx_ohlcv.db import get_latest_yield_curve, get_yield_curve

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📉 Yield Curve")
    st.caption("Government Securities Term Structure (Phase 3)")

    con = get_connection()

    # =================================================================
    # CONTROLS
    # =================================================================
    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 1])

    with ctrl_col1:
        bond_type = st.selectbox(
            "Curve Type",
            ["PIB", "T-Bill", "Sukuk", "ALL"],
            key="yc_bond_type"
        )

    with ctrl_col2:
        curve_date = st.date_input(
            "Curve Date",
            value=None,
            key="yc_date"
        )
        curve_date_str = curve_date.strftime("%Y-%m-%d") if curve_date else None

    with ctrl_col3:
        if st.button("Build Curve", key="yc_build"):
            with st.spinner("Building yield curve..."):
                points = build_yield_curve(con, curve_date_str, bond_type)
                if points:
                    st.success(f"Built curve with {len(points)} points")
                    st.rerun()
                else:
                    st.warning("No data to build curve")

    # =================================================================
    # YIELD CURVE CHART
    # =================================================================
    st.markdown("### Term Structure")

    if curve_date_str:
        points = get_yield_curve(con, curve_date_str, bond_type)
    else:
        curve_date_str, points = get_latest_yield_curve(con, bond_type)

    if not points:
        st.info("No yield curve data available. Click 'Build Curve' to generate.")
        st.markdown("""
        **To build a yield curve:**
        1. First initialize bonds: `psxsync bonds init`
        2. Generate sample quotes: `psxsync bonds load --sample`
        3. Compute analytics: `psxsync bonds compute --curve`
        """)
    else:
        st.caption(f"Curve Date: {curve_date_str}")

        # Prepare data
        df = pd.DataFrame(points)

        tenor_labels = {
            3: "3M", 6: "6M", 12: "1Y", 24: "2Y",
            36: "3Y", 60: "5Y", 84: "7Y", 120: "10Y",
            180: "15Y", 240: "20Y",
        }
        df["tenor_label"] = df["tenor_months"].apply(
            lambda x: tenor_labels.get(x, f"{x}M")
        )
        df["yield_pct"] = df["yield_rate"] * 100

        # Create chart
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df["tenor_months"],
            y=df["yield_pct"],
            mode="lines+markers",
            name=f"{bond_type} Curve",
            line=dict(width=3, color="#1f77b4"),
            marker=dict(size=10),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Yield: %{y:.2f}%<br>"
                "<extra></extra>"
            ),
            text=df["tenor_label"],
        ))

        fig.update_layout(
            title=f"Yield Curve - {bond_type}",
            xaxis_title="Tenor (Months)",
            yaxis_title="Yield (%)",
            height=400,
            showlegend=False,
            hovermode="x unified",
        )

        # Add tenor labels on x-axis
        fig.update_xaxes(
            tickmode="array",
            tickvals=df["tenor_months"].tolist(),
            ticktext=df["tenor_label"].tolist(),
        )

        st.plotly_chart(fig, use_container_width=True)

        # =================================================================
        # CURVE DATA TABLE
        # =================================================================
        st.markdown("### Curve Points")

        table_df = df[["tenor_label", "tenor_months", "yield_pct"]].copy()
        table_df.columns = ["Tenor", "Months", "Yield (%)"]
        table_df["Yield (%)"] = table_df["Yield (%)"].apply(lambda x: f"{x:.4f}%")

        st.dataframe(table_df, use_container_width=True, hide_index=True)

        # =================================================================
        # INTERPOLATION TOOL
        # =================================================================
        st.markdown("### Yield Interpolation")

        interp_col1, interp_col2 = st.columns([1, 2])

        with interp_col1:
            target_tenor = st.number_input(
                "Target Tenor (months)",
                min_value=1,
                max_value=360,
                value=48,
                key="yc_target_tenor"
            )

        with interp_col2:
            interp_yield = interpolate_yield(points, target_tenor, "LINEAR")
            if interp_yield:
                st.metric(
                    f"Interpolated Yield ({target_tenor}M)",
                    f"{interp_yield * 100:.4f}%"
                )
            else:
                st.info("Cannot interpolate for this tenor")

    render_footer()


def render_sukuk_screener():
    """Sukuk Screener - Shariah-compliant fixed income instruments."""
    import pandas as pd

    from psx_ohlcv.analytics_sukuk import (
        get_sukuk_analytics_full,
        get_analytics_by_category,
    )
    from psx_ohlcv.db import get_sukuk_data_summary, get_sukuk_list
    from psx_ohlcv.sync_sukuk import seed_sukuk, sync_sukuk_quotes

    con = get_connection()

    st.markdown("## 🕌 Sukuk Screener")
    st.caption("Shariah-compliant fixed income (GOP Sukuk, PIBs, T-Bills)")

    # =================================================================
    # ADMIN CONTROLS (collapsed)
    # =================================================================
    with st.expander("⚙️ Data Management", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed Sukuk Data", key="sukuk_seed"):
                with st.spinner("Seeding sukuk instruments..."):
                    result = seed_sukuk()
                    if result.get("success"):
                        st.success(f"Seeded {result['inserted']} instruments")
                    else:
                        st.error(f"Error: {result.get('error')}")
                st.rerun()

        with col2:
            if st.button("Generate Sample Quotes", key="sukuk_sample"):
                with st.spinner("Generating sample quote data..."):
                    summary = sync_sukuk_quotes(source="SAMPLE", days=90)
                    st.success(f"Generated {summary.rows_upserted} quotes")
                st.rerun()

        with col3:
            summary = get_sukuk_data_summary(con)
            st.metric("Total Instruments", summary.get("total_sukuk", 0))
            st.metric("With Quotes", summary.get("sukuk_with_quotes", 0))

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

    with filter_col1:
        category = st.selectbox(
            "Category",
            ["ALL", "GOP_SUKUK", "PIB", "TBILL", "CORPORATE_SUKUK", "TFC"],
            key="sukuk_category_filter"
        )

    with filter_col2:
        issuer = st.text_input("Issuer", "", key="sukuk_issuer_filter")

    with filter_col3:
        shariah_only = st.checkbox("Shariah Only", key="sukuk_shariah")

    with filter_col4:
        min_ytm = st.number_input(
            "Min YTM (%)", min_value=0.0, max_value=30.0, value=0.0,
            step=0.5, key="sukuk_min_ytm"
        )

    # =================================================================
    # SUKUK LIST
    # =================================================================
    sukuk_list = get_sukuk_list(
        con,
        active_only=True,
        category=None if category == "ALL" else category,
    )

    if not sukuk_list:
        st.warning("No sukuk found. Click 'Seed Sukuk Data' to initialize.")
        render_footer()
        return

    # Apply filters
    if issuer:
        sukuk_list = [
            s for s in sukuk_list
            if issuer.lower() in s.get("issuer", "").lower()
        ]
    if shariah_only:
        sukuk_list = [s for s in sukuk_list if s.get("shariah_compliant")]

    # Get analytics for each sukuk
    analytics_data = get_analytics_by_category(
        category=None if category == "ALL" else category,
        db_path=None
    )

    # Create lookup for analytics
    analytics_lookup = {a["instrument_id"]: a for a in analytics_data}

    # Apply YTM filter
    if min_ytm > 0:
        def check_ytm(s):
            ytm = analytics_lookup.get(s["instrument_id"], {}).get("ytm", 0)
            return (ytm or 0) >= min_ytm
        sukuk_list = [s for s in sukuk_list if check_ytm(s)]

    if sukuk_list:
        # Display table
        table_data = []
        for sukuk in sukuk_list:
            analytics = analytics_lookup.get(sukuk["instrument_id"], {})
            table_data.append({
                "ID": sukuk["instrument_id"],
                "Name": sukuk.get("name", "")[:40],
                "Category": sukuk.get("category", ""),
                "Issuer": sukuk.get("issuer", "")[:20],
                "Maturity": sukuk.get("maturity_date", ""),
                "Coupon": f"{sukuk.get('coupon_rate', 0) or 0:.2f}%",
                "YTM": f"{analytics.get('ytm', 0) or 0:.2f}%",
                "Duration": f"{analytics.get('duration', 0) or 0:.2f}",
                "Shariah": "✓" if sukuk.get("shariah_compliant") else "",
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(sukuk_list)} instruments**")

        # =================================================================
        # SUKUK DETAIL VIEW
        # =================================================================
        st.markdown("### Sukuk Details")

        sukuk_options = {
            s["instrument_id"]: s.get("name", s["instrument_id"])
            for s in sukuk_list
        }
        selected_id = st.selectbox(
            "Select Instrument",
            options=list(sukuk_options.keys()),
            format_func=lambda x: sukuk_options.get(x, x),
            key="sukuk_selected"
        )

        if selected_id:
            result = get_sukuk_analytics_full(selected_id)

            if not result.get("error"):
                sukuk = result.get("sukuk", {})
                quote = result.get("quote", {})
                analytics = result.get("analytics", {})

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown("**Instrument Info**")
                    st.write(f"**ID:** {sukuk.get('instrument_id')}")
                    st.write(f"**Issuer:** {sukuk.get('issuer')}")
                    st.write(f"**Category:** {sukuk.get('category')}")
                    st.write(f"**Maturity:** {sukuk.get('maturity_date')}")
                    shariah = "Yes ✓" if sukuk.get("shariah_compliant") else "No"
                    st.write(f"**Shariah:** {shariah}")

                with col2:
                    st.markdown("**Pricing**")
                    if quote:
                        st.write(f"**Date:** {quote.get('quote_date')}")
                        st.write(f"**Clean Price:** {quote.get('clean_price')}")
                        st.write(f"**Dirty Price:** {quote.get('dirty_price')}")
                        st.write(f"**YTM:** {quote.get('yield_to_maturity')}%")
                    else:
                        st.info("No quote data available")

                with col3:
                    st.markdown("**Analytics**")
                    if analytics.get("yield_to_maturity"):
                        ytm = analytics["yield_to_maturity"]
                        st.metric("YTM", f"{ytm:.4f}%")
                        if analytics.get("modified_duration"):
                            mod_dur = analytics["modified_duration"]
                            st.metric("Mod Duration", f"{mod_dur:.2f} yrs")
                        if analytics.get("convexity"):
                            convex = analytics["convexity"]
                            st.metric("Convexity", f"{convex:.2f}")
                    else:
                        st.info("No analytics computed")
    else:
        st.info("No sukuk match the current filters.")

    render_footer()


def render_sukuk_yield_curve():
    """Sukuk Yield Curve - Term structure for sukuk instruments."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.analytics_sukuk import (
        get_yield_curve_data,
        interpolate_yield_curve,
    )
    from psx_ohlcv.sync_sukuk import sync_sample_yield_curves

    st.markdown("## 📈 Sukuk Yield Curve")
    st.caption("Term structure of sukuk yields (GOP Sukuk, PIB, T-Bill)")

    # =================================================================
    # CONTROLS
    # =================================================================
    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        curve_name = st.selectbox(
            "Curve",
            ["GOP_SUKUK", "PIB", "TBILL"],
            key="sukuk_curve_name"
        )

    with col2:
        curve_date = st.date_input(
            "Date (blank for latest)",
            value=None,
            key="sukuk_curve_date"
        )

    with col3:
        if st.button("Generate Sample Curves", key="sukuk_gen_curves"):
            with st.spinner("Generating yield curves..."):
                summary = sync_sample_yield_curves(days=30)
                st.success(f"Generated {summary.rows_upserted} curve points")
            st.rerun()

    # =================================================================
    # YIELD CURVE CHART
    # =================================================================
    date_str = curve_date.isoformat() if curve_date else None
    curve_data = get_yield_curve_data(
        curve_name=curve_name,
        curve_date=date_str,
    )

    points = curve_data.get("points", [])

    if not points:
        st.info("No yield curve data. Click 'Generate Sample Curves' to create.")
        st.markdown("""
        **To build a yield curve:**
        1. Seed sukuk data: `psxsync sukuk seed`
        2. Sync sample quotes: `psxsync sukuk sync --include-curves`
        """)
        render_footer()
        return

    # Build DataFrame
    df = pd.DataFrame(points)
    df["yield_pct"] = df["yield_rate"]

    # Create chart
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["tenor_days"],
        y=df["yield_pct"],
        mode="lines+markers",
        name=curve_name,
        line=dict(width=2),
        marker=dict(size=8),
    ))

    fig.update_layout(
        title=f"Sukuk Yield Curve - {curve_name}",
        xaxis_title="Tenor (Days)",
        yaxis_title="Yield (%)",
        height=450,
        hovermode="x unified",
    )

    # Add tenor labels on x-axis
    fig.update_xaxes(
        tickmode="array",
        tickvals=df["tenor_days"].tolist(),
        ticktext=df["tenor_label"].tolist(),
    )

    st.plotly_chart(fig, use_container_width=True)

    # =================================================================
    # CURVE DATA TABLE
    # =================================================================
    st.markdown("### Curve Points")

    table_df = df[["tenor_label", "tenor_days", "yield_pct"]].copy()
    table_df.columns = ["Tenor", "Days", "Yield (%)"]
    table_df["Yield (%)"] = table_df["Yield (%)"].apply(lambda x: f"{x:.4f}%")

    st.dataframe(table_df, use_container_width=True, hide_index=True)

    # =================================================================
    # INTERPOLATION TOOL
    # =================================================================
    st.markdown("### Yield Interpolation")

    interp_col1, interp_col2 = st.columns([1, 2])

    with interp_col1:
        target_days = st.number_input(
            "Target Tenor (days)",
            min_value=1,
            max_value=3650,
            value=365,
            key="sukuk_target_tenor"
        )

    with interp_col2:
        interp_yield = interpolate_yield_curve(points, target_days)
        if interp_yield:
            st.metric(
                f"Interpolated Yield ({target_days} days)",
                f"{interp_yield:.4f}%"
            )
        else:
            st.info("Cannot interpolate for this tenor")

    render_footer()


def render_sbp_auction_archive():
    """SBP Auction Archive - Primary market document archive."""
    import pandas as pd

    from psx_ohlcv.sources.sbp_primary_market import (
        get_documents_by_type,
        get_sbp_document_urls,
        index_documents,
        create_sample_documents,
        DOC_TYPES,
        INSTRUMENT_TYPES,
        DOCS_DIR,
    )
    from psx_ohlcv.sync_sukuk import index_sbp_documents

    st.markdown("## 🏛️ SBP Auction Archive")
    st.caption("State Bank of Pakistan Primary Market Document Archive")

    # =================================================================
    # ADMIN CONTROLS
    # =================================================================
    with st.expander("⚙️ Document Management", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Create Sample Documents", key="sbp_create_samples"):
                with st.spinner("Creating sample documents..."):
                    created = create_sample_documents()
                    st.success(f"Created {len(created)} sample files")
                st.rerun()

        with col2:
            if st.button("Re-index Documents", key="sbp_reindex"):
                with st.spinner("Indexing documents..."):
                    result = index_sbp_documents()
                    st.success(f"Indexed {result.get('total_documents', 0)} documents")
                st.rerun()

        st.markdown(f"**Document Directory:** `{DOCS_DIR}`")

    # =================================================================
    # SBP URLS
    # =================================================================
    st.markdown("### Official SBP Data Sources")

    urls = get_sbp_document_urls()
    url_data = [
        {"Source": name.replace("_", " ").title(), "URL": url}
        for name, url in urls.items()
    ]
    st.dataframe(pd.DataFrame(url_data), use_container_width=True, hide_index=True)

    st.info("Download documents and place in the document directory.")

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Document Archive")

    filter_col1, filter_col2 = st.columns(2)

    def fmt_inst(x):
        return INSTRUMENT_TYPES.get(x, x) if x != "ALL" else "All Types"

    def fmt_doc(x):
        return DOC_TYPES.get(x, x) if x != "ALL" else "All Documents"

    with filter_col1:
        inst_type = st.selectbox(
            "Instrument Type",
            ["ALL"] + list(INSTRUMENT_TYPES.keys()),
            format_func=fmt_inst,
            key="sbp_inst_filter"
        )

    with filter_col2:
        doc_type = st.selectbox(
            "Document Type",
            ["ALL"] + list(DOC_TYPES.keys()),
            format_func=fmt_doc,
            key="sbp_doc_filter"
        )

    # =================================================================
    # DOCUMENT LIST
    # =================================================================
    documents = get_documents_by_type(
        instrument_type=None if inst_type == "ALL" else inst_type,
        doc_type=None if doc_type == "ALL" else doc_type,
    )

    if documents:
        table_data = []
        for doc in documents:
            doc_type_val = doc.get("doc_type")
            inst_type_val = doc.get("instrument_type")
            indexed_at = doc.get("indexed_at", "")
            table_data.append({
                "ID": doc.get("doc_id", "")[:30],
                "Type": DOC_TYPES.get(doc_type_val, doc_type_val),
                "Instrument": INSTRUMENT_TYPES.get(inst_type_val, inst_type_val),
                "Auction Date": doc.get("auction_date", ""),
                "File": doc.get("file_name", ""),
                "Indexed": indexed_at[:10] if indexed_at else "",
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(documents)} documents**")
    else:
        st.info("No documents found. Add SBP files and click 'Re-index'.")

    # =================================================================
    # DOCUMENT NAMING GUIDE
    # =================================================================
    with st.expander("📋 Document Naming Convention", expanded=False):
        st.markdown("""
        **Recommended file naming format:**
        ```
        {INSTRUMENT}_{DOCTYPE}_{DATE}.{ext}
        ```

        **Examples:**
        - `TBILL_AUCTION_RESULT_2026-01-15.pdf`
        - `PIB_AUCTION_RESULT_2026-01-10.xlsx`
        - `GOP_SUKUK_YIELD_CURVE_2026-01.csv`

        **Supported formats:** PDF, XLS, XLSX, CSV

        **Instrument types:** TBILL, PIB, GOP_SUKUK, FRB

        **Document types:** AUCTION_RESULT, AUCTION_CALENDAR, YIELD_CURVE, CUT_OFF_YIELD
        """)

    render_footer()


def render_govt_fixed_income():
    """Government Fixed Income Screener - MTB, PIB, GOP Sukuk."""
    import pandas as pd

    from psx_ohlcv.analytics_fixed_income import (
        compute_analytics_for_instrument,
        get_instruments_by_yield,
    )
    from psx_ohlcv.db import (
        get_fi_instrument,
        get_fi_instruments,
        get_fi_latest_quote,
    )
    from psx_ohlcv.sync_fixed_income import (
        get_fi_status_summary,
        seed_fi_instruments,
        sync_all_fixed_income,
        sync_fi_quotes,
    )
    from psx_ohlcv.services.fi_sync_service import (
        is_fi_sync_running,
        read_fi_status,
        start_fi_sync_background,
        stop_fi_sync,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 💰 Government Fixed Income")
        st.caption("MTB, PIB, GOP Sukuk Analytics (Phase 3.5 - Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Bond Analytics</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # =================================================================
    # DATA SYNC CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        sync_col1, sync_col2, sync_col3 = st.columns(3)

        with sync_col1:
            if st.button("Seed Instruments", key="fi_seed"):
                with st.spinner("Seeding fixed income instruments..."):
                    result = seed_fi_instruments()
                    if result.get("success"):
                        st.success(f"Seeded {result['inserted']} instruments")
                        st.rerun()
                    else:
                        st.error(f"Error: {result.get('errors', [])}")

        with sync_col2:
            if st.button("Sync All Data", key="fi_sync_all"):
                with st.spinner("Syncing all fixed income data..."):
                    results = sync_all_fixed_income()
                    st.success("Sync complete!")
                    for key, summary in results.items():
                        if isinstance(summary, dict):
                            ok_count = summary.get('ok', summary.get('inserted', 0))
                            st.write(f"**{key}**: {ok_count} OK")
                    st.rerun()

        with sync_col3:
            summary = get_fi_status_summary()
            st.metric("Total Instruments", summary.get("total_instruments", 0))
            st.metric("Quote Rows", summary.get("total_quote_rows", 0))

        # Background Service Controls
        st.markdown("---")
        st.markdown("#### 🔄 Background Sync Service")

        svc_running, svc_pid = is_fi_sync_running()
        svc_status = read_fi_status()

        svc_col1, svc_col2, svc_col3 = st.columns(3)

        with svc_col1:
            if svc_running:
                st.success(f"🟢 Service Running (PID: {svc_pid})")
                if st.button("Stop Service", key="fi_svc_stop"):
                    success, msg = stop_fi_sync()
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                    st.rerun()
            else:
                st.warning("🔴 Service Stopped")
                continuous = st.checkbox(
                    "Continuous Mode",
                    key="fi_svc_continuous",
                    help="Run sync every hour automatically"
                )
                if st.button("Start Service", key="fi_svc_start"):
                    success, msg = start_fi_sync_background(continuous=continuous)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                    st.rerun()

        with svc_col2:
            if svc_status.last_sync_at:
                st.metric("Last Sync", svc_status.last_sync_at[:16])
            if svc_status.sync_count:
                st.metric("Total Syncs", svc_status.sync_count)

        with svc_col3:
            if svc_status.docs_synced:
                st.metric("Docs Synced", svc_status.docs_synced)
            if svc_status.curves_synced:
                st.metric("Curves Synced", svc_status.curves_synced)

        if svc_status.progress_message:
            st.caption(f"Status: {svc_status.progress_message}")

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

    with filter_col1:
        category = st.selectbox(
            "Category",
            ["ALL", "MTB", "PIB", "GOP_SUKUK", "CORP_BOND", "CORP_SUKUK"],
            key="fi_category_filter"
        )

    with filter_col2:
        min_yield = st.number_input(
            "Min YTM (%)", min_value=0.0, max_value=30.0, value=0.0,
            step=0.5, key="fi_min_ytm"
        )

    with filter_col3:
        sort_by = st.selectbox(
            "Sort By",
            ["yield", "duration", "maturity"],
            key="fi_sort"
        )

    with filter_col4:
        shariah_only = st.checkbox("Shariah Only", key="fi_shariah")

    # =================================================================
    # INSTRUMENTS TABLE
    # =================================================================
    st.markdown("### Fixed Income Instruments")

    cat_filter = None if category == "ALL" else category
    min_yield_dec = min_yield / 100 if min_yield > 0 else None

    instruments = get_instruments_by_yield(
        con,
        category=cat_filter,
        min_yield=min_yield_dec,
        sort_by=sort_by,
        limit=100,
    )

    # Filter shariah if needed
    if shariah_only:
        instruments = [i for i in instruments if i.get("is_shariah")]

    if not instruments:
        st.warning("No instruments found. Click 'Seed Instruments' to load sample data.")
    else:
        # Build table
        table_data = []
        for inst in instruments:
            coupon = inst.get("coupon_rate")
            coupon_str = f"{coupon * 100:.2f}%" if coupon else "Zero"
            ytm = inst.get("yield_to_maturity")
            ytm_str = f"{ytm * 100:.2f}%" if ytm else "N/A"
            dur = inst.get("modified_duration")
            dur_str = f"{dur:.2f}" if dur else "N/A"
            price = inst.get("clean_price") or inst.get("dirty_price")
            price_str = f"{price:.2f}" if price else "N/A"

            table_data.append({
                "ISIN": inst.get("isin", "")[:15],
                "Symbol": inst.get("symbol"),
                "Category": inst.get("category"),
                "Coupon": coupon_str,
                "Maturity": inst.get("maturity_date"),
                "Price": price_str,
                "YTM": ytm_str,
                "Duration": dur_str,
                "Shariah": "✓" if inst.get("is_shariah") else "",
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(instruments)} instruments**")

        # =================================================================
        # INSTRUMENT DETAILS
        # =================================================================
        st.markdown("### Instrument Details")

        all_instruments = get_fi_instruments(con, category=cat_filter, active_only=True)
        if all_instruments:
            isin_options = {i["isin"]: f"{i['symbol']} ({i['category']})" for i in all_instruments}
            selected_isin = st.selectbox(
                "Select Instrument",
                options=list(isin_options.keys()),
                format_func=lambda x: isin_options.get(x, x),
                key="fi_selected_isin"
            )

            if selected_isin:
                inst = get_fi_instrument(con, selected_isin)
                quote = get_fi_latest_quote(con, selected_isin)
                analytics = compute_analytics_for_instrument(con, selected_isin)

                if inst:
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("**Instrument Info**")
                        st.write(f"**Symbol:** {inst.get('symbol')}")
                        st.write(f"**ISIN:** {inst.get('isin')}")
                        st.write(f"**Category:** {inst.get('category')}")
                        st.write(f"**Issuer:** {inst.get('issuer', 'N/A')}")
                        st.write(f"**Issue Date:** {inst.get('issue_date', 'N/A')}")
                        st.write(f"**Maturity:** {inst.get('maturity_date')}")
                        coupon = inst.get("coupon_rate")
                        st.write(f"**Coupon:** {coupon * 100:.2f}%" if coupon else "**Coupon:** Zero")
                        st.write(f"**Shariah:** {'Yes' if inst.get('is_shariah') else 'No'}")

                    with col2:
                        st.markdown("**Analytics**")
                        if quote:
                            st.write(f"**Quote Date:** {quote.get('date')}")
                            st.write(f"**Clean Price:** {quote.get('clean_price', 'N/A')}")
                            st.write(f"**Dirty Price:** {quote.get('dirty_price', 'N/A')}")
                            ytm = quote.get("yield_to_maturity")
                            st.write(f"**Quoted YTM:** {ytm * 100:.2f}%" if ytm else "**YTM:** N/A")

                        if analytics and "ytm" in analytics:
                            st.markdown("---")
                            st.write(f"**Computed YTM:** {analytics.get('ytm_pct', 'N/A')}%")
                            st.write(f"**Mac Duration:** {analytics.get('macaulay_duration', 'N/A')} years")
                            st.write(f"**Mod Duration:** {analytics.get('modified_duration', 'N/A')}")
                            st.write(f"**Convexity:** {analytics.get('convexity', 'N/A')}")
                            st.write(f"**PVBP:** {analytics.get('pvbp', 'N/A')}")

    render_footer()


def render_fi_yield_curve():
    """Fixed Income Yield Curve - Term structure visualization."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.analytics_fixed_income import (
        compare_yield_curves,
        get_yield_curve_analytics,
    )
    from psx_ohlcv.db import get_fi_curve, get_fi_curve_dates
    from psx_ohlcv.sync_fixed_income import sync_fi_curves

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📊 Fixed Income Yield Curve")
    st.caption("Government Securities Term Structure (Phase 3.5)")

    con = get_connection()

    # =================================================================
    # DATA CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Sync Yield Curves", key="fi_curves_sync"):
                with st.spinner("Syncing yield curve data..."):
                    summary = sync_fi_curves()
                    st.success(f"Synced {summary.rows_upserted} curve points")
                    st.rerun()

        with col2:
            # Show available curves
            curve_dates = get_fi_curve_dates(con)
            if curve_dates:
                st.write("**Available Curves:**")
                for cd in curve_dates[:5]:
                    st.write(f"- {cd.get('curve_name')}: {cd.get('latest_date')} ({cd.get('count')} points)")

    # =================================================================
    # CURVE SELECTION
    # =================================================================
    st.markdown("### Select Curve")

    col1, col2 = st.columns(2)

    with col1:
        curve_name = st.selectbox(
            "Curve Name",
            ["PKR_MTB", "PKR_PIB", "PKR_GOP_SUKUK"],
            key="fi_curve_name"
        )

    with col2:
        # Get available dates for selected curve
        curve_data = get_fi_curve(con, curve_name)
        if curve_data:
            available_dates = sorted(set(p.get("curve_date") for p in curve_data if p.get("curve_date")), reverse=True)
            curve_date = st.selectbox(
                "Curve Date",
                available_dates if available_dates else ["Latest"],
                key="fi_curve_date"
            )
        else:
            curve_date = None
            st.info("No curve data available")

    # =================================================================
    # YIELD CURVE CHART
    # =================================================================
    if curve_data:
        analytics = get_yield_curve_analytics(con, curve_name, curve_date)

        if not analytics.get("error"):
            points = analytics.get("points", [])
            if points:
                # Sort by tenor
                sorted_points = sorted(points, key=lambda x: x.get("tenor_months", 0))

                # Build chart data
                tenors = []
                yields = []
                tenor_labels = []

                for p in sorted_points:
                    tenor_m = p.get("tenor_months", 0)
                    yld = p.get("yield_value", 0)
                    if yld:
                        tenors.append(tenor_m)
                        yields.append(yld * 100)  # Convert to percentage
                        if tenor_m < 12:
                            tenor_labels.append(f"{tenor_m}M")
                        else:
                            tenor_labels.append(f"{tenor_m // 12}Y")

                # Create chart
                fig = go.Figure()

                fig.add_trace(go.Scatter(
                    x=tenors,
                    y=yields,
                    mode='lines+markers',
                    name=curve_name,
                    line=dict(width=2),
                    marker=dict(size=8),
                ))

                fig.update_layout(
                    title=f"Yield Curve - {curve_name}",
                    xaxis_title="Tenor (Months)",
                    yaxis_title="Yield (%)",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=tenors,
                        ticktext=tenor_labels,
                    ),
                    height=400,
                )

                st.plotly_chart(fig, use_container_width=True)

                # =================================================================
                # CURVE METRICS
                # =================================================================
                st.markdown("### Curve Metrics")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Curve Date", analytics.get("curve_date", "N/A"))

                with col2:
                    shape = analytics.get("shape", "N/A")
                    st.metric("Curve Shape", shape.title() if shape else "N/A")

                with col3:
                    steepness = analytics.get("steepness")
                    if steepness is not None:
                        st.metric("Steepness", f"{steepness * 100:.0f} bps")
                    else:
                        st.metric("Steepness", "N/A")

                with col4:
                    st.metric("Points", analytics.get("num_points", 0))

                # =================================================================
                # CURVE DATA TABLE
                # =================================================================
                st.markdown("### Curve Points")

                table_data = []
                for p in sorted_points:
                    tenor_m = p.get("tenor_months", 0)
                    if tenor_m < 12:
                        tenor_str = f"{tenor_m} Months"
                    else:
                        tenor_str = f"{tenor_m // 12} Years"

                    yld = p.get("yield_value", 0)
                    table_data.append({
                        "Tenor": tenor_str,
                        "Months": tenor_m,
                        "Yield (%)": f"{yld * 100:.4f}" if yld else "N/A",
                    })

                df = pd.DataFrame(table_data)
                st.dataframe(df, use_container_width=True, hide_index=True)

        else:
            st.warning(f"No data for curve: {analytics.get('error')}")

    else:
        st.info("No yield curve data. Click 'Sync Yield Curves' to load sample data.")

    render_footer()


def render_sbp_pma_archive():
    """SBP PMA Archive - Primary Market Activities document metadata."""
    import pandas as pd

    from psx_ohlcv.db import get_sbp_pma_docs
    from psx_ohlcv.sources.sbp_pma import (
        PMA_DOCS_DIR,
        SBP_PMA_URL,
        fetch_and_parse_pma,
        get_sample_pma_documents,
    )
    from psx_ohlcv.sync_fixed_income import sync_sbp_pma_docs

    # =================================================================
    # HEADER
    # =================================================================
    st.markdown("## 📝 SBP PMA Archive")
    st.caption("State Bank of Pakistan Primary Market Activities (Phase 3.5)")

    con = get_connection()

    # =================================================================
    # INFO BOX
    # =================================================================
    st.info(f"""
    **Source:** [SBP Primary Market Activities]({SBP_PMA_URL})

    This page archives document metadata from SBP's Primary Market Activities page.
    Documents include MTB, PIB, and GOP Sukuk auction results, calendars, and announcements.
    """)

    # =================================================================
    # DATA CONTROLS
    # =================================================================
    with st.expander("🔧 Data Management", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            source = st.selectbox(
                "Source",
                ["SBP", "SAMPLE"],
                key="pma_source"
            )

        with col2:
            download = st.checkbox("Download PDFs", key="pma_download")

        with col3:
            category = st.selectbox(
                "Category Filter",
                ["ALL", "MTB", "PIB", "GOP_SUKUK"],
                key="pma_category"
            )

        if st.button("Sync Documents", key="pma_sync"):
            with st.spinner("Syncing SBP PMA documents..."):
                cat_filter = None if category == "ALL" else category
                summary = sync_sbp_pma_docs(
                    source=source,
                    download=download,
                    category=cat_filter,
                )
                st.success(f"Synced {summary.ok} documents, {summary.rows_upserted} stored")
                st.rerun()

        st.markdown(f"**Local Storage:** `{PMA_DOCS_DIR}`")

    # =================================================================
    # FILTERS
    # =================================================================
    st.markdown("### Filters")
    filter_col1, filter_col2 = st.columns(2)

    with filter_col1:
        cat_filter = st.selectbox(
            "Category",
            ["ALL", "MTB", "PIB", "GOP_SUKUK", "OTHER"],
            key="pma_view_category"
        )

    with filter_col2:
        doc_type_filter = st.selectbox(
            "Document Type",
            ["ALL", "RESULT", "CALENDAR", "ANNOUNCEMENT", "CIRCULAR", "TARGET", "OTHER"],
            key="pma_doc_type"
        )

    # =================================================================
    # DOCUMENTS TABLE
    # =================================================================
    st.markdown("### Documents")

    docs = get_sbp_pma_docs(
        con,
        category=None if cat_filter == "ALL" else cat_filter,
        doc_type=None if doc_type_filter == "ALL" else doc_type_filter,
        limit=100,
    )

    if docs:
        table_data = []
        for doc in docs:
            table_data.append({
                "Title": doc.get("title", "")[:50],
                "Category": doc.get("category", "N/A"),
                "Type": doc.get("doc_type", "N/A"),
                "Date": doc.get("doc_date", "N/A"),
                "URL": doc.get("url", "")[:60] + "..." if len(doc.get("url", "")) > 60 else doc.get("url", ""),
            })

        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown(f"**Total: {len(docs)} documents**")

        # =================================================================
        # DOCUMENT DETAILS
        # =================================================================
        st.markdown("### Document Details")

        doc_options = {d.get("doc_id"): d.get("title", d.get("doc_id")) for d in docs}
        selected_doc = st.selectbox(
            "Select Document",
            options=list(doc_options.keys()),
            format_func=lambda x: doc_options.get(x, x)[:60],
            key="pma_selected_doc"
        )

        if selected_doc:
            doc = next((d for d in docs if d.get("doc_id") == selected_doc), None)
            if doc:
                col1, col2 = st.columns(2)

                with col1:
                    st.write(f"**Title:** {doc.get('title')}")
                    st.write(f"**Category:** {doc.get('category')}")
                    st.write(f"**Type:** {doc.get('doc_type')}")
                    st.write(f"**Date:** {doc.get('doc_date', 'N/A')}")

                with col2:
                    url = doc.get("url")
                    if url:
                        st.markdown(f"**URL:** [{url[:40]}...]({url})")
                    st.write(f"**Fetched:** {doc.get('fetched_at', 'N/A')}")
                    st.write(f"**Parsed:** {'Yes' if doc.get('parsed') else 'No'}")

    else:
        st.info("No documents found. Click 'Sync Documents' to fetch from SBP.")

        # Show preview of what would be fetched
        with st.expander("Preview: Sample Documents"):
            sample_docs = get_sample_pma_documents()
            preview_data = []
            for doc in sample_docs:
                preview_data.append({
                    "Title": doc.title,
                    "Category": doc.category,
                    "Type": doc.doc_type,
                    "Date": doc.doc_date,
                })
            st.dataframe(pd.DataFrame(preview_data), use_container_width=True, hide_index=True)

    render_footer()


def render_psx_debt_market():
    """PSX Debt Market - Live debt securities from PSX DPS with full metrics."""
    import pandas as pd
    import plotly.graph_objects as go

    from psx_ohlcv.sources.psx_debt import (
        DEBT_CATEGORIES,
        DebtSecurity,
        fetch_all_debt_securities,
        fetch_debt_ohlcv,
        fetch_debt_security_detail,
        get_securities_flat_list,
        get_securities_summary,
        parse_symbol_info,
    )
    from psx_ohlcv.fi_analytics import (
        analyze_security,
        build_yield_curve,
        FREQ_SEMI_ANNUAL,
        FREQ_ZERO,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📈 PSX Debt Market")
        st.caption("T-Bills, PIBs, Sukuk, TFCs from PSX Data Portal")
    with header_col2:
        st.markdown(
            '<div class="data-info">💹 Live Data</div>',
            unsafe_allow_html=True
        )

    # =================================================================
    # FETCH DATA FROM PSX
    # =================================================================
    with st.spinner("Loading debt securities from PSX..."):
        securities_by_cat = fetch_all_debt_securities()

    if not any(securities_by_cat.values()):
        st.error("Could not fetch debt securities from PSX. Please try again later.")
        render_footer()
        return

    # Get summary
    summary = get_securities_summary(securities_by_cat)
    all_securities = get_securities_flat_list(securities_by_cat)

    # =================================================================
    # MARKET OVERVIEW KPIs
    # =================================================================
    st.markdown("### 📊 Market Overview")

    kpi_cols = st.columns(5)
    with kpi_cols[0]:
        st.metric("Total Securities", summary["total"])
    with kpi_cols[1]:
        st.metric("Government", summary["government"])
    with kpi_cols[2]:
        st.metric("Corporate", summary["corporate"])
    with kpi_cols[3]:
        st.metric("Islamic/Sukuk", summary["islamic"])
    with kpi_cols[4]:
        # Show by type breakdown
        type_count = len(summary.get("by_type", {}))
        st.metric("Security Types", type_count)

    # Category breakdown
    cat_cols = st.columns(4)
    for i, (cat_code, cat_name) in enumerate(DEBT_CATEGORIES.items()):
        with cat_cols[i]:
            count = len(securities_by_cat.get(cat_code, []))
            st.metric(cat_name, count)

    # =================================================================
    # CATEGORY TABS (matching PSX page structure)
    # =================================================================
    st.markdown("---")

    # Create tabs for each PSX category
    tab_names = [f"{DEBT_CATEGORIES[cat]} ({len(securities_by_cat[cat])})" for cat in DEBT_CATEGORIES]
    tab_names.append("📈 Price Chart")
    tab_names.append("📊 Security Analytics")  # Enhanced with Bloomberg-style metrics
    tab_names.append("📉 Yield Curve")  # New yield curve tab

    tabs = st.tabs(tab_names)

    # --- Category Tabs (gop, pds, cds, gds) ---
    for idx, cat_code in enumerate(DEBT_CATEGORIES.keys()):
        with tabs[idx]:
            cat_securities = securities_by_cat.get(cat_code, [])
            cat_name = DEBT_CATEGORIES[cat_code]

            if not cat_securities:
                st.info(f"No {cat_name} securities available")
                continue

            st.markdown(f"### {cat_name}")

            # Build table with all metrics
            table_data = []
            for sec in cat_securities:
                row = {
                    "Security Code": sec.symbol,
                    "Security Name": (sec.name or "")[:40],
                    "Face Value": f"{sec.face_value:,.0f}" if sec.face_value else "N/A",
                    "Listing Date": sec.listing_date or "N/A",
                    "Issue Date": sec.issue_date or "N/A",
                    "Issue Size": sec.issue_size or "N/A",
                    "Maturity Date": sec.maturity_date or "N/A",
                }

                # Add coupon info if available
                if sec.coupon_rate is not None:
                    row["Coupon Rate"] = f"{sec.coupon_rate:.4f}%"
                if sec.prev_coupon_date:
                    row["Prev Coupon"] = sec.prev_coupon_date
                if sec.next_coupon_date:
                    row["Next Coupon"] = sec.next_coupon_date

                row["Outstanding Days"] = sec.outstanding_days if sec.outstanding_days else "N/A"
                row["Remaining Yrs"] = f"{sec.remaining_years:.1f}" if sec.remaining_years else "N/A"

                # Add derived fields
                if sec.is_islamic:
                    row["Islamic"] = "✓"

                table_data.append(row)

            if table_data:
                df = pd.DataFrame(table_data)
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {len(table_data)} securities")

    # --- Price Chart Tab ---
    with tabs[4]:
        st.markdown("### 📈 Price History")

        chart_col1, chart_col2 = st.columns([3, 1])

        # Get all symbols for selector
        all_symbols = [s.symbol for s in all_securities]

        with chart_col2:
            selected_symbol = st.selectbox(
                "Select Security",
                options=all_symbols[:150],
                key="debt_chart_symbol"
            )

            if selected_symbol:
                # Find the security in our data
                sec = next((s for s in all_securities if s.symbol == selected_symbol), None)
                if sec:
                    st.markdown("**Security Info:**")
                    st.write(f"**Name:** {sec.name or 'N/A'}")
                    st.write(f"**Type:** {sec.security_type or 'N/A'}")
                    st.write(f"**Category:** {sec.category_name or 'N/A'}")
                    if sec.face_value:
                        st.write(f"**Face Value:** Rs. {sec.face_value:,.0f}")
                    if sec.coupon_rate is not None:
                        st.write(f"**Coupon:** {sec.coupon_rate:.4f}%")
                    if sec.maturity_date:
                        st.write(f"**Maturity:** {sec.maturity_date}")
                    if sec.outstanding_days:
                        st.write(f"**Days Left:** {sec.outstanding_days}")
                    if sec.is_islamic:
                        st.success("✓ Shariah Compliant")

        with chart_col1:
            if selected_symbol:
                with st.spinner(f"Loading price data for {selected_symbol}..."):
                    ohlcv = fetch_debt_ohlcv(selected_symbol)

                if ohlcv:
                    df = pd.DataFrame(ohlcv)
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date")

                    # Price chart
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df["date"],
                        y=df["price"],
                        mode="lines",
                        name="Price",
                        line=dict(color="#00d4aa", width=2),
                    ))

                    fig.update_layout(
                        title=f"{selected_symbol} Price History",
                        xaxis_title="Date",
                        yaxis_title="Price (PKR)",
                        template="plotly_dark",
                        height=400,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Volume chart
                    if df["volume"].sum() > 0:
                        vol_fig = go.Figure()
                        vol_fig.add_trace(go.Bar(
                            x=df["date"],
                            y=df["volume"],
                            name="Volume",
                            marker_color="#ff6b6b",
                        ))
                        vol_fig.update_layout(
                            title="Trading Volume",
                            xaxis_title="Date",
                            yaxis_title="Volume",
                            template="plotly_dark",
                            height=200,
                        )
                        st.plotly_chart(vol_fig, use_container_width=True)

                    # Stats
                    stat_cols = st.columns(4)
                    with stat_cols[0]:
                        st.metric("Latest", f"{df['price'].iloc[-1]:.4f}")
                    with stat_cols[1]:
                        st.metric("High", f"{df['price'].max():.4f}")
                    with stat_cols[2]:
                        st.metric("Low", f"{df['price'].min():.4f}")
                    with stat_cols[3]:
                        if len(df) > 1:
                            chg = ((df['price'].iloc[-1] - df['price'].iloc[0]) / df['price'].iloc[0]) * 100
                            st.metric("Change", f"{chg:.2f}%")
                else:
                    st.warning(f"No price data available for {selected_symbol}")

    # --- Security Analytics Tab (Bloomberg-style) ---
    with tabs[5]:
        st.markdown("### 📊 Security Analytics")
        st.caption("Bloomberg-style yield and risk metrics")

        detail_symbol = st.selectbox(
            "Select Security",
            options=all_symbols[:150],
            key="debt_detail_symbol"
        )

        if detail_symbol:
            # First check our scraped data
            sec = next((s for s in all_securities if s.symbol == detail_symbol), None)

            if sec:
                # Fetch price data for analytics
                ohlcv = fetch_debt_ohlcv(sec.symbol)
                latest_price = ohlcv[0]['price'] if ohlcv else None

                # Basic Info Section
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown("#### Security Info")
                    st.write(f"**Symbol:** {sec.symbol}")
                    st.write(f"**Name:** {sec.name or 'N/A'}")
                    st.write(f"**Type:** {sec.security_type or 'N/A'}")
                    st.write(f"**Category:** {sec.category_name or 'N/A'}")
                    if sec.is_islamic:
                        st.success("✓ Shariah Compliant")

                with col2:
                    st.markdown("#### Issue Details")
                    if sec.face_value:
                        st.write(f"**Face Value:** Rs. {sec.face_value:,.0f}")
                    if sec.issue_size:
                        st.write(f"**Issue Size:** {sec.issue_size}")
                    if sec.issue_date:
                        st.write(f"**Issue Date:** {sec.issue_date}")
                    if sec.maturity_date:
                        st.write(f"**Maturity:** {sec.maturity_date}")

                with col3:
                    st.markdown("#### Coupon/Rental")
                    if sec.coupon_rate is not None and sec.coupon_rate > 0:
                        st.write(f"**Rate:** {sec.coupon_rate:.4f}%")
                        st.write(f"**Frequency:** Semi-Annual")
                    else:
                        st.write("**Type:** Zero Coupon/Discount")
                    if sec.prev_coupon_date:
                        st.write(f"**Last Coupon:** {sec.prev_coupon_date}")
                    if sec.next_coupon_date:
                        st.write(f"**Next Coupon:** {sec.next_coupon_date}")

                st.markdown("---")

                # Calculate Analytics if we have price
                if latest_price and sec.maturity_date and sec.outstanding_days and sec.outstanding_days > 0:
                    # Determine frequency
                    coupon_pct = sec.coupon_rate if sec.coupon_rate else 0
                    freq = FREQ_ZERO if coupon_pct == 0 else FREQ_SEMI_ANNUAL
                    coupon = coupon_pct / 100  # Convert to decimal

                    # Run analytics
                    analytics = analyze_security(
                        symbol=sec.symbol,
                        name=sec.name,
                        security_type=sec.security_type,
                        face_value=sec.face_value or 5000,
                        coupon_rate=coupon,
                        maturity_date=sec.maturity_date,
                        price=latest_price,
                        prev_coupon_date=sec.prev_coupon_date,
                        frequency=freq,
                        price_is_per_100=True,
                    )

                    st.markdown("### 📈 Yield & Risk Analytics (YAS)")
                    st.caption("Bloomberg-style Yield Analysis")

                    # Price & Yield Metrics
                    yield_cols = st.columns(4)
                    with yield_cols[0]:
                        st.metric("Clean Price", f"{latest_price:.4f}")
                    with yield_cols[1]:
                        if analytics.yield_metrics and analytics.yield_metrics.ytm is not None:
                            ytm_pct = analytics.yield_metrics.ytm * 100
                            st.metric("YTM", f"{ytm_pct:.2f}%")
                        else:
                            st.metric("YTM", "N/A")
                    with yield_cols[2]:
                        if analytics.yield_metrics and analytics.yield_metrics.discount_yield is not None:
                            dy_pct = analytics.yield_metrics.discount_yield * 100
                            st.metric("Discount Yield", f"{dy_pct:.2f}%")
                        elif analytics.yield_metrics and analytics.yield_metrics.current_yield is not None:
                            cy_pct = analytics.yield_metrics.current_yield * 100
                            st.metric("Current Yield", f"{cy_pct:.2f}%")
                        else:
                            st.metric("Current Yield", "N/A")
                    with yield_cols[3]:
                        if analytics.yield_metrics and analytics.yield_metrics.bey is not None:
                            bey_pct = analytics.yield_metrics.bey * 100
                            st.metric("BEY", f"{bey_pct:.2f}%")
                        else:
                            st.metric("BEY", "N/A")

                    # Duration & Risk Metrics
                    st.markdown("### 📊 Duration & Risk (DUR)")
                    st.caption("Bloomberg-style Duration Analysis")

                    dur_cols = st.columns(4)
                    with dur_cols[0]:
                        if analytics.years_to_maturity:
                            st.metric("Years to Mat", f"{analytics.years_to_maturity:.2f}")
                        else:
                            st.metric("Years to Mat", "N/A")
                    with dur_cols[1]:
                        if analytics.duration_metrics and analytics.duration_metrics.macaulay_dur is not None:
                            st.metric("Macaulay Dur", f"{analytics.duration_metrics.macaulay_dur:.2f}")
                        else:
                            st.metric("Macaulay Dur", "N/A")
                    with dur_cols[2]:
                        if analytics.duration_metrics and analytics.duration_metrics.modified_dur is not None:
                            st.metric("Modified Dur", f"{analytics.duration_metrics.modified_dur:.2f}")
                        else:
                            st.metric("Modified Dur", "N/A")
                    with dur_cols[3]:
                        if analytics.duration_metrics and analytics.duration_metrics.dv01 is not None:
                            st.metric("DV01", f"{analytics.duration_metrics.dv01:.4f}")
                        else:
                            st.metric("DV01", "N/A")

                    # Analytics Table
                    with st.expander("📋 Full Analytics Details"):
                        analytics_data = {
                            "Metric": [],
                            "Value": [],
                            "Description": [],
                        }

                        # Add all metrics
                        analytics_data["Metric"].append("Symbol")
                        analytics_data["Value"].append(sec.symbol)
                        analytics_data["Description"].append("Security identifier")

                        analytics_data["Metric"].append("Clean Price")
                        analytics_data["Value"].append(f"{latest_price:.4f}")
                        analytics_data["Description"].append("Price excluding accrued interest")

                        if analytics.yield_metrics:
                            ym = analytics.yield_metrics
                            if ym.ytm is not None:
                                analytics_data["Metric"].append("YTM")
                                analytics_data["Value"].append(f"{ym.ytm*100:.4f}%")
                                analytics_data["Description"].append("Yield to Maturity (annualized)")
                            if ym.discount_yield is not None:
                                analytics_data["Metric"].append("Discount Yield")
                                analytics_data["Value"].append(f"{ym.discount_yield*100:.4f}%")
                                analytics_data["Description"].append("Money market discount yield")
                            if ym.bey is not None:
                                analytics_data["Metric"].append("BEY")
                                analytics_data["Value"].append(f"{ym.bey*100:.4f}%")
                                analytics_data["Description"].append("Bond Equivalent Yield")
                            if ym.current_yield is not None:
                                analytics_data["Metric"].append("Current Yield")
                                analytics_data["Value"].append(f"{ym.current_yield*100:.4f}%")
                                analytics_data["Description"].append("Annual coupon / price")

                        if analytics.duration_metrics:
                            dm = analytics.duration_metrics
                            if dm.macaulay_dur is not None:
                                analytics_data["Metric"].append("Macaulay Duration")
                                analytics_data["Value"].append(f"{dm.macaulay_dur:.4f} years")
                                analytics_data["Description"].append("Weighted avg time to cash flows")
                            if dm.modified_dur is not None:
                                analytics_data["Metric"].append("Modified Duration")
                                analytics_data["Value"].append(f"{dm.modified_dur:.4f}")
                                analytics_data["Description"].append("Price sensitivity to yield changes")
                            if dm.dv01 is not None:
                                analytics_data["Metric"].append("DV01")
                                analytics_data["Value"].append(f"{dm.dv01:.6f}")
                                analytics_data["Description"].append("Dollar value of 1bp yield change")
                            if dm.convexity is not None:
                                analytics_data["Metric"].append("Convexity")
                                analytics_data["Value"].append(f"{dm.convexity:.4f}")
                                analytics_data["Description"].append("Second-order price sensitivity")

                        df_analytics = pd.DataFrame(analytics_data)
                        st.dataframe(df_analytics, use_container_width=True, hide_index=True)

                else:
                    st.info("Select a security with available price data to view analytics")

                # Maturity Progress
                st.markdown("---")
                st.markdown("#### Time to Maturity")
                if sec.outstanding_days is not None:
                    if sec.outstanding_days > 0:
                        st.write(f"**Outstanding Days:** {sec.outstanding_days}")
                        # Progress bar
                        if sec.tenor_years and sec.tenor_years > 0:
                            original_days = sec.tenor_years * 365
                            elapsed = original_days - sec.outstanding_days
                            pct_complete = min(1.0, max(0.0, elapsed / original_days))
                            st.progress(pct_complete)
                            st.caption(f"{pct_complete*100:.1f}% of original {sec.tenor_years}Y tenor elapsed")
                    else:
                        st.error("MATURED")
                if sec.remaining_years is not None:
                    st.write(f"**Remaining Years:** {sec.remaining_years:.2f}")
            else:
                st.warning("Security not found in data")

    # --- Yield Curve Tab ---
    with tabs[6]:
        st.markdown("### 📉 PKR Yield Curve")
        st.caption("Government Securities Yield Curve Analysis")

        # Build yield curves from available data
        curve_col1, curve_col2 = st.columns([3, 1])

        with curve_col2:
            curve_type = st.selectbox(
                "Curve Type",
                ["Government (PIB + T-Bill)", "Sukuk (GIS + FRR)", "All Securities"],
                key="yield_curve_type"
            )

        with curve_col1:
            # Filter securities based on curve type
            if curve_type == "Government (PIB + T-Bill)":
                curve_securities = [s for s in all_securities
                                  if s.security_type in ["PIB", "T-Bill", "Floating"]
                                  and s.outstanding_days and s.outstanding_days > 30]
            elif curve_type == "Sukuk (GIS + FRR)":
                curve_securities = [s for s in all_securities
                                  if s.is_islamic and s.outstanding_days and s.outstanding_days > 30]
            else:
                curve_securities = [s for s in all_securities
                                  if s.outstanding_days and s.outstanding_days > 30]

            st.info(f"Building curve from {len(curve_securities)} securities with price data...")

            # Calculate YTM for each security
            curve_data = []
            for sec in curve_securities:
                ohlcv = fetch_debt_ohlcv(sec.symbol)
                if ohlcv and sec.maturity_date:
                    price = ohlcv[0]['price']
                    coupon_pct = sec.coupon_rate if sec.coupon_rate else 0
                    freq = FREQ_ZERO if coupon_pct == 0 else FREQ_SEMI_ANNUAL

                    analytics = analyze_security(
                        symbol=sec.symbol,
                        name=sec.name,
                        security_type=sec.security_type,
                        face_value=sec.face_value or 5000,
                        coupon_rate=coupon_pct / 100,
                        maturity_date=sec.maturity_date,
                        price=price,
                        frequency=freq,
                        price_is_per_100=True,
                    )

                    if analytics.yield_metrics and analytics.yield_metrics.ytm is not None:
                        ytm = analytics.yield_metrics.ytm
                        # Filter out extreme values
                        if 0 < ytm < 0.5:  # 0-50% yield range
                            curve_data.append({
                                "symbol": sec.symbol,
                                "name": sec.name,
                                "type": sec.security_type,
                                "years_to_maturity": analytics.years_to_maturity,
                                "ytm": ytm,
                                "price": price,
                            })

            if curve_data:
                # Sort by maturity
                curve_data.sort(key=lambda x: x["years_to_maturity"])
                df_curve = pd.DataFrame(curve_data)

                # Plot yield curve
                fig = go.Figure()

                # Scatter plot with symbols
                fig.add_trace(go.Scatter(
                    x=df_curve["years_to_maturity"],
                    y=df_curve["ytm"] * 100,  # Convert to percentage
                    mode="markers+text",
                    text=df_curve["type"],
                    textposition="top center",
                    textfont=dict(size=8),
                    marker=dict(
                        size=10,
                        color=df_curve["ytm"] * 100,
                        colorscale="Viridis",
                        showscale=True,
                        colorbar=dict(title="YTM %"),
                    ),
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>" +
                        "Maturity: %{x:.2f}Y<br>" +
                        "YTM: %{y:.2f}%<br>" +
                        "Price: %{customdata[1]:.2f}<extra></extra>"
                    ),
                    customdata=list(zip(df_curve["symbol"], df_curve["price"])),
                ))

                # Add smoothed curve line
                if len(df_curve) > 2:
                    fig.add_trace(go.Scatter(
                        x=df_curve["years_to_maturity"],
                        y=df_curve["ytm"] * 100,
                        mode="lines",
                        line=dict(color="#00d4aa", width=2, dash="dash"),
                        name="Trend",
                    ))

                fig.update_layout(
                    title=f"PKR {curve_type} Yield Curve",
                    xaxis_title="Years to Maturity",
                    yaxis_title="Yield to Maturity (%)",
                    template="plotly_dark",
                    height=500,
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

                # Curve statistics
                stat_cols = st.columns(4)
                short_term = [d["ytm"] for d in curve_data if d["years_to_maturity"] < 1]
                med_term = [d["ytm"] for d in curve_data if 1 <= d["years_to_maturity"] < 5]
                long_term = [d["ytm"] for d in curve_data if d["years_to_maturity"] >= 5]

                with stat_cols[0]:
                    if short_term:
                        avg_st = sum(short_term) / len(short_term) * 100
                        st.metric("Short Term (<1Y)", f"{avg_st:.2f}%")
                with stat_cols[1]:
                    if med_term:
                        avg_mt = sum(med_term) / len(med_term) * 100
                        st.metric("Medium Term (1-5Y)", f"{avg_mt:.2f}%")
                with stat_cols[2]:
                    if long_term:
                        avg_lt = sum(long_term) / len(long_term) * 100
                        st.metric("Long Term (>5Y)", f"{avg_lt:.2f}%")
                with stat_cols[3]:
                    if short_term and long_term:
                        spread = (sum(long_term)/len(long_term) - sum(short_term)/len(short_term)) * 10000
                        st.metric("Curve Spread (bps)", f"{spread:.0f}")

                # Data table
                with st.expander("📋 Curve Points Data"):
                    display_df = df_curve.copy()
                    display_df["ytm_pct"] = display_df["ytm"] * 100
                    display_df = display_df[["symbol", "type", "years_to_maturity", "ytm_pct", "price"]]
                    display_df.columns = ["Symbol", "Type", "Years to Mat", "YTM %", "Price"]
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.warning("Not enough securities with price data to build yield curve")

    render_footer()
