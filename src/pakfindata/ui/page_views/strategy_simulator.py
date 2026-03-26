"""Strategy Fusion Simulator -- reads fusion_state.json, renders with lightweight-charts.

Same pattern as live_ticker.py / tick_replay.py: file-based, data injected as JSON into HTML.
"""

import json
import os
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.ui.components.helpers import render_footer

PKT = timezone(timedelta(hours=5))
DATA_ROOT = Path("/mnt/e/psxdata")
FUSION_STATE = DATA_ROOT / "fusion_state.json"

_C = {
    "bg": "#0B0E11", "card": "#141820", "border": "#1E2530",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "blue": "#2196F3", "purple": "#BB86FC",
}


def _load():
    if not FUSION_STATE.exists():
        return None
    for _ in range(2):
        try:
            return json.loads(FUSION_STATE.read_text())
        except (json.JSONDecodeError, IOError):
            _time.sleep(0.3)
    return None


def _age():
    try:
        return _time.time() - os.path.getmtime(FUSION_STATE)
    except OSError:
        return 999


def _build_simulator_panel(data: dict) -> str:
    """Build lightweight-charts HTML panel from fusion_state.json data."""
    candles = data.get("candles", [])
    mkrs = data.get("markers", [])
    score_history = data.get("score_history", [])
    decision = data.get("decision", {})
    portfolio = data.get("portfolio", {})
    votes = data.get("votes", [])
    symbol = data.get("symbol", "?")
    source = data.get("source", "?")
    replay = data.get("replay", {})

    candles_json = json.dumps(candles, default=str)
    markers_json = json.dumps(mkrs, default=str)
    scores_json = json.dumps(score_history, default=str)
    votes_json = json.dumps(votes, default=str)
    equity_curve = json.dumps(portfolio.get("equity_curve", [])[-200:], default=str)

    dec = decision.get("decision", "HOLD")
    conf = decision.get("confidence", 0)
    price = decision.get("price", 0)
    raw_score = decision.get("raw_score", 0)
    pnl = portfolio.get("pnl", 0)
    trades = portfolio.get("trades", 0)
    win_rate = portfolio.get("win_rate", 0)

    dec_color = "#00E676" if "BUY" in dec else "#FF5252" if "SELL" in dec else "#6B7280"
    pnl_color = "#00E676" if pnl >= 0 else "#FF5252"
    source_color = "#00BCD4" if source == "REPLAY" else "#00E676" if source == "LIVE" else "#6B7280"
    source_label = "REPLAY (" + replay.get("replay_date", "?") + ")" if source == "REPLAY" else source
    dec_bg = "rgba(0,230,118,0.15)" if "BUY" in dec else "rgba(255,82,82,0.15)" if "SELL" in dec else "rgba(107,114,128,0.1)"

    return (
        '<script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>'
        "<style>"
        "*{margin:0;padding:0;box-sizing:border-box}"
        "body{background:#0B0E11;color:#E0E0E0;font-family:'JetBrains Mono','Courier New',monospace;overflow:hidden}"
        ".header-bar{display:flex;align-items:center;gap:14px;padding:8px 12px;background:#141821;border-bottom:1px solid #1E2530}"
        ".hdr-symbol{font-weight:800;font-size:16px;color:#C8A96E}"
        ".hdr-price{font-size:15px;font-weight:700}"
        ".hdr-decision{font-size:14px;font-weight:900;letter-spacing:1px;padding:3px 10px;border-radius:3px}"
        ".hdr-stat{color:#6B7280;font-size:10px}.hdr-stat b{color:#E0E0E0;font-size:12px}"
        ".hdr-source{font-size:10px;padding:2px 8px;border-radius:3px;border:1px solid}"
        "#main-chart{width:100%;height:320px}"
        "#score-chart{width:100%;height:100px;border-top:1px solid #1E2530}"
        ".signal-strip{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;padding:8px 12px;background:#0F1318;border-top:1px solid #1E2530}"
        ".sig-cell{padding:8px 6px;border-radius:4px;text-align:center;font-size:11px}"
        ".sig-long{background:rgba(0,230,118,0.2);border:1px solid rgba(0,230,118,0.4);color:#00E676}"
        ".sig-short{background:rgba(255,82,82,0.2);border:1px solid rgba(255,82,82,0.4);color:#FF5252}"
        ".sig-neutral{background:rgba(107,114,128,0.12);border:1px solid #1E2530;color:#6B7280}"
        ".sig-off{background:#0B0E11;border:1px solid #111;color:#333}"
        ".sig-name{font-weight:700;font-size:11px;text-transform:uppercase}"
        ".sig-signal{font-size:9px;opacity:0.8;margin-top:2px}"
        ".bottom-bar{display:flex;gap:16px;padding:6px 12px;background:#141821;border-top:1px solid #1E2530;font-size:11px}"
        ".bb-stat{display:flex;flex-direction:column}.bb-label{color:#555;font-size:8px;text-transform:uppercase}.bb-value{font-weight:700}"
        "</style>"
        '<div class="header-bar">'
        f'<span class="hdr-source" style="color:{source_color};border-color:{source_color}">{source_label}</span>'
        f'<span class="hdr-symbol">{symbol}</span>'
        f'<span class="hdr-price" style="color:{dec_color}">{price:,.2f}</span>'
        f'<span class="hdr-decision" style="color:{dec_color};background:{dec_bg}">{dec}</span>'
        f'<span class="hdr-stat">Conf <b>{conf:.0f}%</b></span>'
        f'<span class="hdr-stat">Score <b>{raw_score*100:.0f}</b></span>'
        '<span style="flex:1"></span>'
        f'<span class="hdr-stat">P&L <b style="color:{pnl_color}">{"+" if pnl >= 0 else ""}{pnl:,.0f}</b></span>'
        f'<span class="hdr-stat">Trades <b>{trades}</b></span>'
        f'<span class="hdr-stat">Win <b>{win_rate:.0f}%</b></span>'
        '</div>'
        '<div id="main-chart"></div>'
        '<div id="score-chart"></div>'
        '<div class="signal-strip" id="signals"></div>'
        '<div class="bottom-bar">'
        f'<div class="bb-stat"><span class="bb-label">Regime</span><span class="bb-value" style="color:#2196F3">{decision.get("regime",0)*100:+.0f}</span></div>'
        f'<div class="bb-stat"><span class="bb-label">Flow</span><span class="bb-value" style="color:#00BCD4">{decision.get("flow",0)*100:+.0f}</span></div>'
        f'<div class="bb-stat"><span class="bb-label">Structure</span><span class="bb-value" style="color:#C8A96E">{decision.get("structure",0)*100:+.0f}</span></div>'
        f'<div class="bb-stat"><span class="bb-label">Alpha</span><span class="bb-value" style="color:#BB86FC">{decision.get("alpha",0)*100:+.0f}</span></div>'
        '<span style="flex:1"></span>'
        f'<div class="bb-stat"><span class="bb-label">Equity</span><span class="bb-value">{portfolio.get("equity",0):,.0f}</span></div>'
        f'<div class="bb-stat"><span class="bb-label">Drawdown</span><span class="bb-value">{portfolio.get("drawdown",0):.1f}%</span></div>'
        f'<div class="bb-stat"><span class="bb-label">Positions</span><span class="bb-value">{len(portfolio.get("positions",[]))}</span></div>'
        '</div>'
        "<script>"
        f"const candles={candles_json};"
        f"const markers={markers_json};"
        f"const scores={scores_json};"
        f"const votes={votes_json};"
        f"const equityCurve={equity_curve};"
        "const mainEl=document.getElementById('main-chart');"
        "const mainChart=LightweightCharts.createChart(mainEl,{"
        "width:mainEl.clientWidth,height:mainEl.clientHeight,"
        "layout:{background:{color:'#0B0E11'},textColor:'#6B7280'},"
        "grid:{vertLines:{color:'#1E2530'},horzLines:{color:'#1E2530'}},"
        "crosshair:{mode:LightweightCharts.CrosshairMode.Normal},"
        "rightPriceScale:{borderColor:'#1E2530'},"
        "timeScale:{borderColor:'#1E2530',timeVisible:true,secondsVisible:false},"
        "});"
        "if(candles.length>0){"
        "const cs=mainChart.addCandlestickSeries({upColor:'#00E676',downColor:'#FF5252',borderUpColor:'#00E676',borderDownColor:'#FF5252',wickUpColor:'#00E676',wickDownColor:'#FF5252'});"
        "cs.setData(candles.map(c=>({time:c.time,open:c.open,high:c.high,low:c.low,close:c.close})));"
        "const vs=mainChart.addHistogramSeries({priceScaleId:'vol',color:'#2A3445',priceFormat:{type:'volume'}});"
        "mainChart.priceScale('vol').applyOptions({scaleMargins:{top:0.85,bottom:0}});"
        "vs.setData(candles.map(c=>({time:c.time,value:c.volume||0,color:c.close>=c.open?'rgba(0,230,118,0.3)':'rgba(255,82,82,0.3)'})));"
        "if(markers.length>0){"
        "const cm=markers.map(m=>({time:m.time,position:m.decision&&m.decision.includes('BUY')?'belowBar':'aboveBar',color:m.decision&&m.decision.includes('BUY')?'#00E676':'#FF5252',shape:m.decision&&m.decision.includes('BUY')?'arrowUp':'arrowDown',text:(m.decision||'')+' '+(m.confidence?m.confidence.toFixed(0)+'%':'')})).sort((a,b)=>a.time-b.time);"
        "cs.setMarkers(cm);}"
        "if(equityCurve.length>2){"
        "const eq=mainChart.addLineSeries({color:'#C8A96E',lineWidth:1,priceScaleId:'equity',lastValueVisible:false,priceLineVisible:false});"
        "mainChart.priceScale('equity').applyOptions({scaleMargins:{top:0.05,bottom:0.6},visible:false});"
        "eq.setData(equityCurve.map(e=>({time:e.time,value:e.pnl||0})));}"
        "mainChart.timeScale().fitContent();}"
        "const scoreEl=document.getElementById('score-chart');"
        "const scoreChart=LightweightCharts.createChart(scoreEl,{"
        "width:scoreEl.clientWidth,height:scoreEl.clientHeight,"
        "layout:{background:{color:'#0B0E11'},textColor:'#555'},"
        "grid:{vertLines:{color:'#0F1318'},horzLines:{color:'#0F1318'}},"
        "rightPriceScale:{borderColor:'#1E2530'},"
        "timeScale:{borderColor:'#1E2530',visible:false},"
        "});"
        "if(scores.length>0){"
        "const ss=scoreChart.addAreaSeries({topColor:'rgba(0,188,212,0.3)',bottomColor:'rgba(0,188,212,0.0)',lineColor:'#00BCD4',lineWidth:1.5});"
        "ss.setData(scores.map(s=>({time:s.time,value:(s.score||0)*100})));"
        "const tR=scores.map(s=>s.time);"
        "const bl=scoreChart.addLineSeries({color:'#00E67633',lineWidth:1,lineStyle:2});"
        "bl.setData(tR.map(t=>({time:t,value:15})));"
        "const sl=scoreChart.addLineSeries({color:'#FF525233',lineWidth:1,lineStyle:2});"
        "sl.setData(tR.map(t=>({time:t,value:-15})));"
        "const zl=scoreChart.addLineSeries({color:'#333',lineWidth:1});"
        "zl.setData(tR.map(t=>({time:t,value:0})));"
        "scoreChart.timeScale().fitContent();}"
        "mainChart.timeScale().subscribeVisibleTimeRangeChange(r=>{if(r)scoreChart.timeScale().setVisibleRange(r);});"
        "const sigEl=document.getElementById('signals');"
        "votes.forEach(v=>{"
        "const cls=!v.enabled?'sig-off':v.direction>0?'sig-long':v.direction<0?'sig-short':'sig-neutral';"
        "const conf=v.enabled?(v.confidence*100).toFixed(0)+'%':'';"
        "sigEl.innerHTML+='<div class=\"sig-cell '+cls+'\">'+"
        "'<div class=\"sig-name\">'+(v.label||v.name||'?')+'</div>'+"
        "'<div class=\"sig-signal\">'+(v.signal||'').substring(0,25)+'</div>'+"
        "(conf?'<div style=\"font-size:10px;font-weight:600;margin-top:2px\">'+conf+'</div>':'')+"
        "'</div>';});"
        "const ro=new ResizeObserver(()=>{mainChart.applyOptions({width:mainEl.clientWidth});scoreChart.applyOptions({width:scoreEl.clientWidth});});"
        "ro.observe(mainEl);"
        "</script>"
    )


def _render_service_control():
    """START/STOP buttons for fusion_service."""
    try:
        from pakfindata.services.fusion_service import (
            is_fusion_running, start_fusion_background, stop_fusion_service,
        )
    except ImportError:
        st.caption("fusion_service not available")
        return

    running, pid = is_fusion_running()

    if running:
        if st.button(f"Stop (PID {pid})", key="fs_stop", type="secondary"):
            ok, msg = stop_fusion_service()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            sym = st.text_input("Symbol", "OGDC", key="fs_sym", label_visibility="collapsed")
        with c2:
            interval = st.selectbox("Interval", [5, 10, 15, 30], index=1, key="fs_int",
                                    label_visibility="collapsed")
        with c3:
            mode = st.selectbox("Mode", ["auto", "replay", "live"], index=0, key="fs_mode",
                                label_visibility="collapsed")
        with c4:
            if st.button("Start", key="fs_start", type="primary"):
                ok, msg = start_fusion_background(symbol=sym, interval=interval, mode=mode)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
                st.rerun()


def render_page():
    age = _age()
    if age < 30 and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=5000, limit=None, key="fusion_sim_refresh")

    # Header + controls
    h1, h2 = st.columns([3, 2])
    with h1:
        st.markdown("### Strategy Fusion Simulator")
    with h2:
        _render_service_control()

    data = _load()

    if data is None:
        st.info(
            "Fusion service is not running. Click **Start** above, or run:\n\n"
            "```\npython -m pakfindata.services.fusion_service --symbol OGDC\n```"
        )
        st.markdown("""
        ---
        #### Architecture

        | Component | Role |
        |-----------|------|
        | **tick_service** | Writes `live_snapshot.json` (live prices from PSX) |
        | **fusion_service** | Reads prices, runs strategies, writes `fusion_state.json` |
        | **This page** | Reads `fusion_state.json`, renders charts (same as Live Ticker) |

        No ports. No iframes. No WebSocket from JS. Just two JSON files on disk.
        """)
        render_footer()
        return

    # Parse state
    running = data.get("running", False)
    symbol = data.get("symbol", "?")
    decision = data.get("decision", {})
    portfolio = data.get("portfolio", {})
    votes = data.get("votes", [])
    candles = data.get("candles", [])
    score_history = data.get("score_history", [])
    markers = data.get("markers", [])

    # Status
    source = data.get("source", "?")
    replay = data.get("replay", {})

    if source == "REPLAY":
        status_text = f"REPLAY ({replay.get('replay_date', '?')})"
    elif running and age < 30:
        status_text = "LIVE"
    elif age < 60:
        status_text = "STALE"
    else:
        status_text = "DOWN"

    # Compact status bar — trading terminal style
    dec = decision.get("decision", "HOLD")
    pnl = portfolio.get("pnl", 0)
    dec_c = "#00E676" if "BUY" in dec else "#FF5252" if "SELL" in dec else "#6B7280"
    pnl_c = "#00E676" if pnl >= 0 else "#FF5252"
    status_c = "#00BCD4" if source == "REPLAY" else "#00E676" if status_text == "LIVE" else "#FFB300" if status_text == "STALE" else "#FF5252"

    dec_bg = "rgba(0,230,118,0.15)" if "BUY" in dec else "rgba(255,82,82,0.15)" if "SELL" in dec else "rgba(107,114,128,0.08)"
    st.markdown(
        '<div style="display:flex;align-items:center;gap:20px;padding:10px 16px;'
        'background:#141821;border-radius:6px;border:1px solid #1E2530;font-family:monospace;">'
        f'<span style="color:{status_c};font-weight:700;font-size:13px">{status_text}</span>'
        f'<span style="color:#C8A96E;font-weight:800;font-size:18px">{symbol}</span>'
        f'<span style="color:#E0E0E0;font-weight:700;font-size:17px">{decision.get("price",0):,.2f}</span>'
        f'<span style="color:{dec_c};font-weight:900;font-size:17px;padding:4px 12px;'
        f'background:{dec_bg};border-radius:4px;letter-spacing:1px">{dec}</span>'
        f'<span style="color:#6B7280;font-size:13px">Conf <b style="color:#E0E0E0;font-size:15px">{decision.get("confidence",0):.0f}%</b></span>'
        '<span style="flex:1"></span>'
        f'<span style="color:#6B7280;font-size:13px">P&L <b style="color:{pnl_c};font-size:16px">{"+" if pnl >= 0 else ""}{pnl:,.0f}</b></span>'
        f'<span style="color:#6B7280;font-size:13px">Trades <b style="color:#E0E0E0;font-size:15px">{portfolio.get("trades",0)}</b></span>'
        f'<span style="color:#6B7280;font-size:13px">Win <b style="color:#E0E0E0;font-size:15px">{portfolio.get("win_rate",0):.0f}%</b></span>'
        f'<span style="color:#6B7280;font-size:13px">DD <b style="color:#FFB300;font-size:15px">{portfolio.get("drawdown",0):.1f}%</b></span>'
        '</div>',
        unsafe_allow_html=True,
    )

    if decision.get("vetoed"):
        st.warning(f"VETOED: {decision.get('veto_reason', '')}")

    if source == "REPLAY":
        progress = replay.get("replay_progress", 0)
        st.progress(int(min(progress, 100)),
                    text=f"Replay: {replay.get('replay_date', '')} -- "
                    f"bar {replay.get('replay_idx', 0)}/{replay.get('replay_bars', 0)}")

    # Interactive panel (lightweight-charts, same tech as Tick Replay)
    if candles or score_history or votes:
        panel_html = _build_simulator_panel(data)
        components.html(panel_html, height=700, scrolling=False)
    else:
        st.info("Waiting for data... fusion_service will populate candles after a few ticks.")

    st.divider()

    # Positions + Trades
    p1, p2 = st.columns(2)

    with p1:
        st.markdown("**Open Positions**")
        positions = portfolio.get("positions", [])
        if positions:
            pos_df = pd.DataFrame(positions)
            show = [c for c in ["symbol", "side", "entry", "cur", "pnl", "pnl_pct", "shares"]
                    if c in pos_df.columns]
            st.dataframe(pos_df[show].round(2), use_container_width=True, hide_index=True)
        else:
            st.caption("No open positions")

    with p2:
        st.markdown("**Recent Trades**")
        closed = portfolio.get("closed", [])
        if closed:
            trade_df = pd.DataFrame(closed[-10:])
            show = [c for c in ["exit_time", "side", "symbol", "shares", "entry", "exit", "pnl", "exit_reason"]
                    if c in trade_df.columns]
            st.dataframe(trade_df[show].round(2), use_container_width=True, hide_index=True)
        else:
            st.caption("No trades yet")

    render_footer()
