"""Sentiment-Driven Signals (LLM) — PSX announcement sentiment scoring."""

from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3",
}
_CHART = dict(paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
              margin=dict(t=30, b=20, l=50, r=20))


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.markdown("### Sentiment-Driven Signals (LLM)")
    st.caption("GPT-4o-mini scores PSX announcements for trading signals")

    tab_scan, tab_sym, tab_cache, tab_method = st.tabs(["Recent Signals", "By Symbol", "Cache", "Methodology"])

    with tab_scan:
        _render_scanner()
    with tab_sym:
        _render_symbol()
    with tab_cache:
        _render_cache()
    with tab_method:
        _render_methodology()

    render_footer()


def _render_scanner():
    from pakfindata.engine.sentiment_strategy import score_recent_announcements

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        limit = st.slider("Announcements", 5, 50, 15, key="sent_limit")
    with c2:
        days = st.slider("Days back", 1, 30, 7, key="sent_days")
    with c3:
        run = st.button("Score Announcements", type="primary", key="sent_run")

    if not run:
        st.info("Click to score recent PSX announcements using GPT-4o-mini. Cached results are instant.")
        return

    with st.spinner(f"Scoring {limit} announcements (LLM calls for uncached)..."):
        signals = score_recent_announcements(limit=limit, days_back=days)

    if not signals:
        st.warning("No announcements found")
        return

    # Summary
    bullish = sum(1 for s in signals if s.sentiment_label == "BULLISH")
    bearish = sum(1 for s in signals if s.sentiment_label == "BEARISH")
    neutral = sum(1 for s in signals if s.sentiment_label == "NEUTRAL")
    cached = sum(1 for s in signals if s.cached)

    mc = st.columns(5)
    with mc[0]:
        _kpi("Total", str(len(signals)))
    with mc[1]:
        _kpi("Bullish", str(bullish), _C["up"])
    with mc[2]:
        _kpi("Bearish", str(bearish), _C["down"])
    with mc[3]:
        _kpi("Neutral", str(neutral))
    with mc[4]:
        _kpi("Cached", f"{cached}/{len(signals)}")

    # Sentiment distribution
    scores = [s.sentiment_score for s in signals]
    fig = go.Figure(go.Histogram(x=scores, nbinsx=20, marker_color=_C["cyan"]))
    fig.add_vline(x=0.3, line_dash="dash", line_color=_C["up"])
    fig.add_vline(x=-0.3, line_dash="dash", line_color=_C["down"])
    fig.update_layout(**_CHART, height=200, xaxis=dict(title="Sentiment Score", range=[-1.1, 1.1], gridcolor=_C["grid"]),
                      yaxis=dict(gridcolor=_C["grid"]))
    st.plotly_chart(fig, width='stretch')

    # Signals table
    df = pd.DataFrame([s.to_dict() for s in signals])
    show = df[["date", "symbol", "title", "sentiment_score", "sentiment_label", "signal", "confidence", "reason"]].copy()
    show["sentiment_score"] = show["sentiment_score"].map(lambda x: f"{x:+.2f}")
    show["confidence"] = show["confidence"].map(lambda x: f"{x:.0%}")
    show.columns = ["Date", "Symbol", "Title", "Score", "Sentiment", "Signal", "Conf", "Reason"]

    def _color_signal(val):
        c = {"BUY": "#1B5E20", "SELL": "#B71C1C", "HOLD": "#333"}
        return f"background-color: {c.get(val, '#333')}"

    styled = show.style.map(_color_signal, subset=["Signal"])
    st.dataframe(styled, width='stretch', hide_index=True, height=400)


def _render_symbol():
    from pakfindata.engine.sentiment_strategy import load_announcements, score_announcement, get_announcement_symbols

    symbols = get_announcement_symbols()
    symbol = st.selectbox("Symbol", symbols, key="sent_sym")

    if not symbol:
        return

    df = load_announcements(symbol=symbol, limit=20)
    if df.empty:
        st.info(f"No announcements for {symbol}")
        return

    st.markdown(f"**{len(df)} announcements** for {symbol}")

    if st.button(f"Score all {symbol} announcements", type="primary", key="sent_sym_score"):
        signals = []
        progress = st.progress(0)
        for i, (_, row) in enumerate(df.iterrows()):
            sig = score_announcement(
                str(row["symbol"]), str(row["announcement_date"]),
                str(row["title"]), str(row.get("category", "")),
            )
            signals.append(sig)
            progress.progress((i + 1) / len(df))

        if signals:
            sdf = pd.DataFrame([s.to_dict() for s in signals])
            show = sdf[["date", "title", "sentiment_score", "sentiment_label", "signal", "reason", "cached"]].copy()
            show["sentiment_score"] = show["sentiment_score"].map(lambda x: f"{x:+.2f}")
            show.columns = ["Date", "Title", "Score", "Sentiment", "Signal", "Reason", "Cached"]
            st.dataframe(show, width='stretch', hide_index=True)

            # Score timeline
            fig = go.Figure()
            colors = [_C["up"] if s > 0.3 else _C["down"] if s < -0.3 else _C["dim"]
                      for s in sdf["sentiment_score"]]
            fig.add_trace(go.Bar(x=sdf["date"], y=sdf["sentiment_score"],
                                 marker_color=colors, name="Sentiment"))
            fig.add_hline(y=0.3, line_dash="dash", line_color=_C["up"])
            fig.add_hline(y=-0.3, line_dash="dash", line_color=_C["down"])
            fig.update_layout(**_CHART, height=250, yaxis=dict(gridcolor=_C["grid"], range=[-1.1, 1.1]))
            st.plotly_chart(fig, width='stretch')
    else:
        # Show raw announcements
        show = df[["announcement_date", "title", "category"]].copy()
        show.columns = ["Date", "Title", "Category"]
        st.dataframe(show, width='stretch', hide_index=True)


def _render_cache():
    from pakfindata.engine.sentiment_strategy import get_cache_stats

    stats = get_cache_stats()
    mc = st.columns(3)
    with mc[0]:
        _kpi("Cached Scores", str(stats["cached_scores"]))
    with mc[1]:
        _kpi("Cache Size", f"{stats['cache_size_kb']:.1f} KB")
    with mc[2]:
        _kpi("Cache Dir", stats["cache_dir"].split("/")[-2] + "/...")

    st.caption(f"Full path: `{stats['cache_dir']}`")
    st.info("Sentiment scores are cached per announcement. Rescoring reuses cached results (free, instant).")

    if st.button("Clear Cache", type="secondary", key="sent_clear"):
        import shutil
        from pakfindata.engine.sentiment_strategy import CACHE_DIR
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        st.success("Cache cleared")
        st.rerun()


def _render_methodology():
    st.markdown("""
#### Sentiment-Driven Signals

**Pipeline:** PSX Announcement -> GPT-4o-mini -> Sentiment Score (-1 to +1) -> Signal

**LLM Prompt:** The model is given Pakistan market context:
- Cash dividends >20% are bullish
- Rights issues are bearish (dilution)
- Director buying is bullish, selling bearish
- Board meetings signal upcoming action
- Earnings surprises drive 3-10% moves

---

#### Signal Rules

| Score | Label | Signal |
|---|---|---|
| > +0.3 | BULLISH | BUY |
| < -0.3 | BEARISH | SELL |
| -0.3 to +0.3 | NEUTRAL | HOLD |

---

#### Why It Works on PSX

1. **Low analyst coverage** — only ~30 companies have active coverage, 534 are information deserts
2. **After-hours announcements** — many drop after close, price adjusts next morning
3. **Slow information diffusion** — retail investors react days after institutional
4. **Caching** — each announcement scored once, then cached (no repeated API costs)

#### Model

- **GPT-4o-mini** via OpenAI API (requires `OPENAI_API_KEY` in `.env`)
- Temperature: 0.3 (low randomness for consistent scoring)
- Response: JSON with score, label, reason
- Cache: `~/pakfindata/models/sentiment_cache/` (hash-keyed JSON files)
    """)
