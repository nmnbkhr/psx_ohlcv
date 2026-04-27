"""Signal Intelligence — Hidden Event Detection + Strategy Decisioning."""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import streamlit.components.v1 as components

DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
    font_color="#c9d1d9", margin=dict(l=20, r=20, t=40, b=20),
)

SECTOR_NAMES = {
    "804": "Cement", "805": "Sugar", "807": "Banking", "808": "Engineering",
    "809": "Auto", "810": "Chemical", "812": "Textile", "813": "Technology",
    "818": "Insurance", "820": "Refinery", "821": "Oil Mktg", "822": "Power",
    "823": "Pharma", "825": "Paper", "826": "Transport", "828": "Telecom",
    "829": "Misc", "830": "Fertilizer", "837": "Real Estate", "838": "E&P",
}


# ─── Cached engine calls ───────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner="Scanning lead-lag relationships...")
def _cached_lead_lag():
    from pakfindata.engine.lead_lag_detector import scan_lead_lag
    return scan_lead_lag(top_n=30)


@st.cache_data(ttl=600, show_spinner="Detecting correlation breakouts...")
def _cached_corr_breakout():
    from pakfindata.engine.correlation_breakout import compute_correlation_regime
    return compute_correlation_regime(min_sigma=1.5)


@st.cache_data(ttl=600, show_spinner="Scanning announcement contagion...")
def _cached_contagion():
    import pakfindata.engine.announcement_contagion as mod
    mod._ollama_available = None  # reset for fresh attempt
    return mod.scan_contagion(days_back=14, min_confidence=0.1)


# ─── Tab 1: Lead-Lag Network ───────────────────────────────────────────

def _build_lead_lag_html(edges_json: str, nodes_json: str) -> str:
    return f"""<!DOCTYPE html>
<html><head>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0B0E11;font-family:'JetBrains Mono',monospace;color:#c9d1d9;overflow:hidden}}
.tip{{position:absolute;padding:10px 14px;background:#161b22;color:#c9d1d9;
  border:1px solid #C8A96E;border-radius:6px;font-size:11px;pointer-events:none;
  opacity:0;transition:opacity 0.15s;z-index:10;min-width:200px;line-height:1.7;
  box-shadow:0 4px 12px rgba(0,0,0,0.5)}}
.tip b{{color:#C8A96E}}
.tip .m{{color:#8b949e}}
#net{{width:100%;border:1px solid rgba(200,169,110,0.15);border-radius:8px;background:#0B0E11}}
.legend{{font-size:11px;color:#8b949e;padding:4px 8px}}
marker{{fill:#C8A96E}}
</style>
</head><body>
<div class="legend">Arrows show leader -> follower | Thickness = confidence | Hover for details</div>
<div style="position:relative">
  <svg id="net" height="440"></svg>
  <div class="tip" id="tip"></div>
</div>
<script>
const edges={edges_json}, nodes={nodes_json};
const W=document.getElementById('net').clientWidth||700, H=440;
const svg=d3.select('#net').attr('viewBox',`0 0 ${{W}} ${{H}}`);
const tip=document.getElementById('tip');
const g=svg.append('g');

// Arrow marker
svg.append('defs').append('marker').attr('id','arrow').attr('viewBox','0 0 10 6')
  .attr('refX',22).attr('refY',3).attr('markerWidth',8).attr('markerHeight',6)
  .attr('orient','auto').append('path').attr('d','M0,0 L10,3 L0,6 Z').attr('fill','#C8A96E');

const sim=d3.forceSimulation(nodes)
  .force('link',d3.forceLink(edges).id(d=>d.id).distance(100))
  .force('charge',d3.forceManyBody().strength(-300))
  .force('center',d3.forceCenter(W/2,H/2))
  .force('collision',d3.forceCollide(24));

const link=g.append('g').selectAll('line').data(edges).join('line')
  .attr('stroke','#C8A96E').attr('stroke-width',d=>1+d.confidence*4)
  .attr('stroke-opacity',d=>0.3+d.confidence*0.5).attr('marker-end','url(#arrow)')
  .on('mouseover',(e,d)=>{{
    tip.style.opacity=1;
    tip.innerHTML=`<b>${{d.source.id||d.source}} -> ${{d.target.id||d.target}}</b><br>`+
      `<span class="m">Lag:</span> ${{d.lag_min}}min (${{d.lag}} bars)<br>`+
      `<span class="m">Corr:</span> ${{d.corr}} <span class="m">| Consistency:</span> ${{Math.round(d.consistency*100)}}%<br>`+
      `<span class="m">Confidence:</span> ${{d.confidence}}`;
    const r=document.getElementById('net').getBoundingClientRect();
    tip.style.left=(e.clientX-r.left+12)+'px';tip.style.top=(e.clientY-r.top-10)+'px';
  }}).on('mouseout',()=>tip.style.opacity=0);

const SC={{"804":"#D85A30","807":"#378ADD","810":"#7F77DD","813":"#1D9E75","828":"#BA7517","830":"#639922","838":"#E24B4A"}};
const node=g.append('g').selectAll('g').data(nodes).join('g').call(
  d3.drag().on('start',(e,d)=>{{if(!e.active)sim.alphaTarget(0.3).restart();d.fx=d.x;d.fy=d.y}})
  .on('drag',(e,d)=>{{d.fx=e.x;d.fy=e.y}}).on('end',(e,d)=>{{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null}}));
node.append('circle').attr('r',14).attr('fill',d=>SC[d.sector]||'#C8A96E').attr('opacity',0.85)
  .attr('stroke','rgba(200,169,110,0.25)').attr('stroke-width',0.5);
node.append('text').text(d=>d.id).attr('text-anchor','middle').attr('dy',-18)
  .attr('font-size','10px').attr('fill','#c9d1d9');

sim.on('tick',()=>{{
  link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y).attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
  node.attr('transform',d=>`translate(${{d.x}},${{d.y}})`);
}});
svg.call(d3.zoom().scaleExtent([0.3,4]).on('zoom',e=>g.attr('transform',e.transform)));
</script></body></html>"""


def _render_lead_lag_tab():
    signals = _cached_lead_lag()
    if not signals:
        st.info("No lead-lag signals detected for the latest date.")
        return

    # Summary metrics
    cols = st.columns(4)
    cols[0].metric("Signals", len(signals))
    cols[1].metric("Top Leader", signals[0].leader)
    cols[2].metric("Avg Lag", f"{np.mean([s.lag_minutes for s in signals]):.1f}min")
    cols[3].metric("Avg Confidence", f"{np.mean([s.confidence for s in signals]):.2f}")

    # Build graph
    from pakfindata.engine.lead_lag_detector import build_lead_lag_graph
    graph = build_lead_lag_graph(signals)
    html = _build_lead_lag_html(json.dumps(graph["edges"]), json.dumps(graph["nodes"]))
    components.html(html, height=500, scrolling=False)

    # Table
    st.divider()
    rows = [{"Leader": s.leader, "Follower": s.follower, "Lag": f"{s.lag_minutes}min",
             "Corr": s.correlation, "Consistency": f"{s.consistency:.0%}",
             "Confidence": s.confidence, "Dir": "SAME" if s.direction > 0 else "INV"}
            for s in signals]
    st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)


# ─── Tab 2: Lead-Lag Timeline ──────────────────────────────────────────

def _render_timeline_tab():
    signals = _cached_lead_lag()
    if not signals:
        st.info("No lead-lag signals to display.")
        return

    st.caption("Most recent lead-lag events")
    for i, s in enumerate(signals[:20]):
        color = "#22c55e" if s.direction > 0 else "#ef4444"
        st.markdown(
            f'<div style="padding:6px 12px;margin-bottom:4px;border-left:3px solid {color};'
            f'background:rgba(128,128,128,0.05);border-radius:0 4px 4px 0;font-size:12px">'
            f'<b style="color:#C8A96E">{s.leader} -> {s.follower}</b>'
            f' &nbsp; {s.lag_minutes}min lag &nbsp; r={s.correlation:.2f} &nbsp; '
            f'{s.consistency:.0%} consistent &nbsp; conf={s.confidence:.2f}'
            f'</div>', unsafe_allow_html=True,
        )


# ─── Tab 3: Correlation Breakouts ──────────────────────────────────────

def _render_corr_tab():
    alerts = _cached_corr_breakout()

    if not alerts:
        st.info("No significant correlation breakouts detected (sigma < 1.5). Market correlations are stable.")
        # Still show the heatmap
        _render_corr_heatmap()
        return

    st.success(f"**{len(alerts)}** correlation breakout(s) detected")
    for a in alerts:
        color = "#22c55e" if a.direction == "CONVERGING" else "#ef4444"
        sector_label = SECTOR_NAMES.get(a.sector, a.sector)
        st.markdown(
            f'<div style="padding:8px 14px;margin-bottom:6px;border-left:3px solid {color};'
            f'background:rgba(128,128,128,0.05);border-radius:0 4px 4px 0;font-size:12px">'
            f'<b style="color:{color}">{a.direction}</b> &mdash; '
            f'{len(a.cluster)} symbols in <b>{sector_label}</b> &nbsp; '
            f'({a.sigma:.1f}s) &nbsp; corr {a.normal_corr:.2f} -> {a.current_corr:.2f}<br>'
            f'<span style="color:#8b949e">Cluster: {", ".join(a.cluster[:6])} &nbsp; '
            f'Trigger: {a.trigger_symbol}</span></div>',
            unsafe_allow_html=True,
        )

    _render_corr_heatmap()


def _render_corr_heatmap():
    """Recent correlation heatmap of top symbols."""
    from datetime import datetime, timedelta
    from pakfindata.db.connections import analytics_con
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    con = analytics_con()
    sym_df = con.execute("""
        SELECT symbol, SUM(volume) as vol FROM eod_ohlcv
        WHERE date >= ? GROUP BY symbol ORDER BY vol DESC LIMIT 25
    """, [cutoff]).df()
    placeholders = ",".join(f"'{s}'" for s in sym_df["symbol"])
    prices = con.execute(f"""
        SELECT symbol, date, close FROM eod_ohlcv
        WHERE symbol IN ({placeholders}) AND date >= ?
        ORDER BY date
    """, [cutoff]).df()
    con.close()

    pivot = prices.pivot(index="date", columns="symbol", values="close")
    corr = pivot.pct_change().dropna().corr()

    fig = go.Figure(data=go.Heatmap(
        z=corr.values, x=corr.columns, y=corr.index,
        colorscale=[[0, "#ef4444"], [0.5, "#0B0E11"], [1, "#22c55e"]],
        zmin=-1, zmax=1,
    ))
    fig.update_layout(**PLOT_LAYOUT, height=500, title="30-Day Correlation Matrix (Top 25)")
    st.plotly_chart(fig, width='stretch')


# ─── Tab 4: Announcement Contagion ─────────────────────────────────────

def _render_contagion_tab():
    signals = _cached_contagion()

    if not signals:
        st.info("No contagion signals detected in recent announcements.")
        return

    st.success(f"**{len(signals)}** announcement contagion signal(s)")

    for s in signals:
        sent_color = "#22c55e" if s.sentiment > 0 else "#ef4444" if s.sentiment < 0 else "#8b949e"
        sector_label = SECTOR_NAMES.get(s.sector, s.sector)
        st.markdown(
            f'<div style="padding:8px 14px;margin-bottom:6px;border-left:3px solid {sent_color};'
            f'background:rgba(128,128,128,0.05);border-radius:0 4px 4px 0;font-size:12px">'
            f'<b style="color:#C8A96E">{s.source_symbol}</b> &mdash; '
            f'<span style="color:{sent_color}">{s.announcement_type}</span> &nbsp; '
            f'sentiment {s.sentiment:+.2f} &nbsp; {sector_label}<br>'
            f'<span style="color:#8b949e">{s.announcement_summary[:80]}</span><br>'
            f'<span style="color:#c9d1d9">Peers: {", ".join(p["symbol"] for p in s.affected_peers[:6])}</span>'
            f'</div>', unsafe_allow_html=True,
        )


# ─── Tab 5: Decision Panel ─────────────────────────────────────────────

def _render_decision_tab():
    from pakfindata.services.fusion_service import STRATEGY_CATALOG, _call

    st.subheader("Unified Strategy Decision")

    sym = st.text_input("Symbol", "OGDC", key="intel_sym").strip().upper()
    if not sym:
        return

    if st.button("Run All Strategies", type="primary", width='stretch'):
        results = {}
        prog = st.progress(0.0)
        names = list(STRATEGY_CATALOG.keys())
        for i, name in enumerate(names):
            cfg = STRATEGY_CATALOG[name]
            if not cfg["on"]:
                results[name] = {"direction": 0, "confidence": 0, "signal": "OFF"}
            else:
                r = _call(name, sym) or {"direction": 0, "confidence": 0, "signal": "no_data"}
                results[name] = r
            prog.progress((i + 1) / len(names))
        prog.empty()

        # Compute fusion
        weighted_sum = 0.0
        total_weight = 0.0
        for name, cfg in STRATEGY_CATALOG.items():
            if not cfg["on"]:
                continue
            r = results.get(name, {})
            d = r.get("direction", 0)
            c = r.get("confidence", 0)
            w = cfg["wt"]
            weighted_sum += d * c * w
            total_weight += w

        fusion_score = weighted_sum / total_weight if total_weight > 0 else 0
        if fusion_score > 0.15:
            decision, dec_color = "BUY", "#22c55e"
        elif fusion_score < -0.15:
            decision, dec_color = "SELL", "#ef4444"
        else:
            decision, dec_color = "HOLD", "#eab308"

        # Show decision
        st.markdown(
            f'<div style="text-align:center;padding:16px;border:2px solid {dec_color};'
            f'border-radius:8px;margin-bottom:16px">'
            f'<span style="font-size:28px;font-weight:bold;color:{dec_color}">{decision}</span>'
            f'<br><span style="color:#8b949e">Fusion Score: {fusion_score:+.3f} &nbsp; '
            f'Symbol: {sym}</span></div>', unsafe_allow_html=True,
        )

        # Strategy grid
        rows = []
        for name, cfg in STRATEGY_CATALOG.items():
            r = results.get(name, {})
            d = r.get("direction", 0)
            arrow = "+" if d > 0 else ("-" if d < 0 else "=")
            rows.append({
                "Strategy": cfg["label"],
                "Category": cfg["cat"],
                "Dir": arrow,
                "Confidence": r.get("confidence", 0),
                "Signal": r.get("signal", "")[:40],
                "Weight": cfg["wt"],
                "Enabled": cfg["on"],
            })

        df = pd.DataFrame(rows)

        def _color_dir(val):
            if val == "+":
                return "color: #22c55e"
            elif val == "-":
                return "color: #ef4444"
            return "color: #8b949e"

        styled = df.style.map(_color_dir, subset=["Dir"]).format({
            "Confidence": "{:.2f}", "Weight": "{:.2f}",
        })
        st.dataframe(styled, width='stretch', hide_index=True)

        # Evidence chain
        agree = [STRATEGY_CATALOG[n]["label"] for n, r in results.items()
                 if r.get("direction", 0) * (1 if fusion_score > 0 else -1) > 0 and r.get("confidence", 0) > 0.2]
        conflict = [STRATEGY_CATALOG[n]["label"] for n, r in results.items()
                    if r.get("direction", 0) * (1 if fusion_score > 0 else -1) < 0 and r.get("confidence", 0) > 0.2]

        if agree:
            st.caption(f"Agree: {', '.join(agree)}")
        if conflict:
            st.caption(f"Conflict: {', '.join(conflict)}")


# ─── Main ──────────────────────────────────────────────────────────────

def render_page():
    st.title("Signal Intelligence")
    st.caption("Hidden event detection across Lead-Lag, Correlation, Announcement engines + Unified Decisioning")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Lead-Lag Network", "Lead-Lag Timeline",
        "Correlation Breakouts", "Announcement Contagion", "Decision Panel",
    ])

    with tab1:
        _render_lead_lag_tab()
    with tab2:
        _render_timeline_tab()
    with tab3:
        _render_corr_tab()
    with tab4:
        _render_contagion_tab()
    with tab5:
        _render_decision_tab()
