"""Tick Replay — client-side HTML component with TradingView Lightweight Charts.

All animation runs in browser JavaScript at 60fps. Streamlit sends data once.
"""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from pakfindata.ui.components.helpers import render_footer

_CLOUD_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
_LOCAL_DIR = Path("/mnt/e/psxdata/tick_logs")
_MAX_TICKS = 50000


def _get_available_dates() -> list[str]:
    dates = set()
    for d in [_CLOUD_DIR, _LOCAL_DIR]:
        if d.exists():
            for f in d.glob("ticks_*.jsonl"):
                dates.add(f.stem.replace("ticks_", ""))
    return sorted(dates, reverse=True)


@st.cache_data(ttl=600, show_spinner=False)
def _get_symbols(date_str: str) -> list[str]:
    """Get symbols using DuckDB (fast) or fallback."""
    cloud = _CLOUD_DIR / f"ticks_{date_str}.jsonl"
    local = _LOCAL_DIR / f"ticks_{date_str}.jsonl"
    path = cloud if cloud.exists() else local
    if not path.exists():
        return []
    try:
        import duckdb
        con = duckdb.connect()
        df = con.execute(f"""
            SELECT DISTINCT symbol
            FROM read_json_auto('{path}',
                 format='newline_delimited', maximum_object_size=10485760)
            WHERE symbol IS NOT NULL AND market != 'IDX'
            ORDER BY symbol
        """).df()
        con.close()
        return df["symbol"].tolist()
    except Exception:
        # Fallback — scan first 5000 lines
        syms = set()
        with open(path) as f:
            for i, line in enumerate(f):
                if i > 5000:
                    break
                try:
                    rec = json.loads(line.strip())
                    s, m = rec.get("symbol", ""), rec.get("market", "")
                    if s and m != "IDX":
                        syms.add(s)
                except Exception:
                    continue
        return sorted(syms)


@st.cache_data(ttl=600, show_spinner="Loading ticks…")
def _load_ticks(date_str: str, symbol: str) -> list[dict]:
    """Load ticks using DuckDB (fast) or fallback."""
    cloud = _CLOUD_DIR / f"ticks_{date_str}.jsonl"
    local = _LOCAL_DIR / f"ticks_{date_str}.jsonl"
    path = cloud if cloud.exists() else local
    if not path.exists():
        return []
    try:
        import duckdb
        con = duckdb.connect()
        df = con.execute(f"""
            SELECT timestamp, price, volume, bid, ask,
                   "bidVol" as bidVol, "askVol" as askVol,
                   open, high, low, change, trades
            FROM read_json_auto('{path}',
                 format='newline_delimited', maximum_object_size=10485760)
            WHERE symbol = '{symbol}' AND market != 'IDX'
            ORDER BY timestamp
        """).df()
        con.close()
        # Replace NaN with 0
        df = df.fillna(0)
        return df.to_dict("records")
    except Exception:
        return _load_ticks_fallback(str(path), symbol)


def _load_ticks_fallback(path: str, symbol: str) -> list[dict]:
    ticks = []
    with open(path) as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec.get("symbol") != symbol or rec.get("market") == "IDX":
                    continue
                ticks.append({
                    "timestamp": rec.get("timestamp", 0),
                    "price": rec.get("price", 0),
                    "volume": rec.get("volume", 0),
                    "bid": rec.get("bid", 0),
                    "ask": rec.get("ask", 0),
                    "bidVol": rec.get("bidVol", 0),
                    "askVol": rec.get("askVol", 0),
                    "open": rec.get("open", 0),
                    "high": rec.get("high", 0),
                    "low": rec.get("low", 0),
                    "change": rec.get("change", 0),
                    "trades": rec.get("trades", 0),
                })
            except Exception:
                continue
    return ticks


def _build_replay_html(
    ticks_json: str,
    symbol: str,
    date_str: str,
    predictions_json: str = "[]",
    bayesian_json: str = "[]",
    comments_json: str = "[]",
    show_predictions: bool = False,
) -> str:
    return f"""<!DOCTYPE html>
<html><head>
<script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0B0E11;color:#E0E0E0;font-family:'JetBrains Mono','Courier New',monospace;overflow:hidden}}
.controls{{display:flex;align-items:center;gap:10px;padding:6px 12px;background:#141821;border-bottom:1px solid #1E2530}}
.controls button{{background:#1E2530;color:#C8A96E;border:1px solid #2A3445;padding:5px 12px;border-radius:4px;cursor:pointer;font-family:inherit;font-size:12px}}
.controls button:hover{{background:#2A3445}}
.controls button.active{{background:#C8A96E;color:#0B0E11}}
.speed-btn.active{{background:#C8A96E!important;color:#0B0E11!important}}
.controls label{{color:#888;font-size:11px}}
.stats{{display:flex;gap:20px;padding:6px 12px;background:#0F1318;border-bottom:1px solid #1E2530;font-size:12px;flex-wrap:wrap}}
.stat{{display:flex;flex-direction:column}}.stat-label{{color:#666;font-size:9px;text-transform:uppercase}}
.stat-value{{color:#E0E0E0;font-size:13px;font-weight:bold}}.stat-value.up{{color:#22c55e}}.stat-value.down{{color:#ef4444}}.stat-value.gold{{color:#C8A96E}}
#chart{{width:100%;height:400px}}
.timeline{{padding:6px 12px;background:#0F1318;border-top:1px solid #1E2530}}
.timeline input[type="range"]{{width:100%;height:5px;-webkit-appearance:none;background:#1E2530;border-radius:3px;outline:none}}
.timeline input[type="range"]::-webkit-slider-thumb{{-webkit-appearance:none;width:12px;height:12px;border-radius:50%;background:#C8A96E;cursor:pointer}}
.time-labels{{display:flex;justify-content:space-between;font-size:9px;color:#555;margin-top:2px}}
.orderbook{{display:flex;gap:20px;padding:4px 12px;background:#0F1318;font-size:11px}}
.ob-bid{{color:#22c55e}}.ob-ask{{color:#ef4444}}.ob-spread{{color:#C8A96E}}
.ticklog{{padding:2px 12px;background:#0B0E11;font-size:10px;max-height:70px;overflow-y:auto}}
.ticklog-row{{display:flex;gap:12px;padding:1px 0;border-bottom:1px solid #0F1318}}
.ticklog-row.buy{{color:#22c55e}}.ticklog-row.sell{{color:#ef4444}}
</style></head><body>
<div class="controls">
<button id="playBtn" onclick="togglePlay()">▶ Play</button>
<button onclick="stepBack()">◀ -1m</button>
<button onclick="stepForward()">+1m ▶</button>
<span style="color:#333">|</span><label>Speed:</label>
<button class="speed-btn" data-speed="0.1" onclick="setSpeed(0.1)">0.1x</button>
<button class="speed-btn" data-speed="0.5" onclick="setSpeed(0.5)">0.5x</button>
<button class="speed-btn" data-speed="1" onclick="setSpeed(1)">1x</button>
<button class="speed-btn" data-speed="5" onclick="setSpeed(5)">5x</button>
<button class="speed-btn active" data-speed="25" onclick="setSpeed(25)">25x</button>
<button class="speed-btn" data-speed="100" onclick="setSpeed(100)">100x</button>
<button class="speed-btn" data-speed="500" onclick="setSpeed(500)">500x</button>
<span style="color:#333">|</span>
<span id="tickCounter" style="color:#666;font-size:11px">0 / 0</span>
<span style="flex:1"></span>
<span style="color:#C8A96E;font-size:13px;font-weight:bold">{symbol}</span>
<span style="color:#555;font-size:11px;margin-left:6px">{date_str}</span>
</div>
<div class="stats">
<div class="stat"><span class="stat-label">Price</span><span id="s_price" class="stat-value gold">—</span></div>
<div class="stat"><span class="stat-label">Change</span><span id="s_change" class="stat-value">—</span></div>
<div class="stat"><span class="stat-label">High</span><span id="s_high" class="stat-value">—</span></div>
<div class="stat"><span class="stat-label">Low</span><span id="s_low" class="stat-value">—</span></div>
<div class="stat"><span class="stat-label">Volume</span><span id="s_vol" class="stat-value">—</span></div>
<div class="stat"><span class="stat-label">Trades</span><span id="s_trades" class="stat-value">—</span></div>
<div class="stat"><span class="stat-label">VWAP</span><span id="s_vwap" class="stat-value gold">—</span></div>
<div class="stat"><span class="stat-label">Time</span><span id="s_time" class="stat-value">—</span></div>
<div class="stat" id="predStats" style="display:{'flex' if show_predictions else 'none'}"><span class="stat-label">ML Accuracy</span><span class="stat-value gold" id="predAccuracy">—</span></div>
<div class="stat" id="predNext" style="display:{'flex' if show_predictions else 'none'}"><span class="stat-label">Next Pred</span><span class="stat-value" id="nextPred">—</span></div>
<div class="stat" id="predCred" style="display:{'flex' if show_predictions else 'none'}"><span class="stat-label">Credibility</span><span class="stat-value" id="credLabel" style="font-size:11px">—</span></div>
</div>
<div id="chart"></div>
<div id="pred-chart" style="width:100%;height:180px;border-top:1px solid #1E2530;display:{'block' if show_predictions else 'none'}"></div>
<div id="bayes-chart" style="width:100%;height:80px;border-top:1px solid #1E2530;display:{'block' if show_predictions else 'none'}"></div>
<div id="llmBar" style="display:{'block' if show_predictions else 'none'};padding:4px 16px;background:#0F1318;border-top:1px solid #1E2530;font-size:11px;color:#888;min-height:20px;transition:opacity 0.5s"><span id="llmComment" style="opacity:0.3">—</span></div>
<div class="orderbook">
<span>Bid: <span id="ob_bid" class="ob-bid">—</span> (<span id="ob_bidvol">—</span>)</span>
<span>Ask: <span id="ob_ask" class="ob-ask">—</span> (<span id="ob_askvol">—</span>)</span>
<span>Spread: <span id="ob_spread" class="ob-spread">—</span></span>
<span style="flex:1"></span>
<span>Imbalance: <span id="ob_imbalance">—</span></span>
</div>
<div class="timeline">
<input type="range" id="scrubber" min="0" max="100" value="0" step="0.1" oninput="seek(this.value)">
<div class="time-labels"><span id="tl_start">09:15</span><span id="tl_current">—</span><span id="tl_end">15:30</span></div>
</div>
<div id="ticklog" class="ticklog"></div>
<script>
const allTicks={ticks_json};
const predictions={predictions_json};
const bayesianData={bayesian_json};
const llmComments={comments_json};
const totalTicks=allTicks.length;
let currentIdx=0,playing=false,speed=25,animFrameId=null;
let cumPV=0,cumVol=0,prevVol=0,recentTrades=[];

// Pre-build deduplicated price data keyed by second
// Multiple ticks in same second → keep last price (OHLC-style)
const priceBySecond={{}};  // ts → price
const volBySecond={{}};    // ts → {{value, color}}
let priceData=[];  // sorted array for setData
let volData=[];

const chartEl=document.getElementById('chart');
const chart=LightweightCharts.createChart(chartEl,{{
width:chartEl.clientWidth,height:400,
layout:{{background:{{type:'solid',color:'#0B0E11'}},textColor:'#888'}},
grid:{{vertLines:{{color:'#1E2530'}},horzLines:{{color:'#1E2530'}}}},
crosshair:{{mode:LightweightCharts.CrosshairMode.Normal}},
timeScale:{{timeVisible:true,secondsVisible:true,borderColor:'#1E2530'}},
rightPriceScale:{{borderColor:'#1E2530'}}
}});
const lineSeries=chart.addLineSeries({{color:'#C8A96E',lineWidth:2,priceLineVisible:true,lastValueVisible:true}});
const volumeSeries=chart.addHistogramSeries({{priceFormat:{{type:'volume'}},priceScaleId:'vol',color:'#2A3445'}});
chart.priceScale('vol').applyOptions({{scaleMargins:{{top:0.85,bottom:0}}}});
window.addEventListener('resize',()=>chart.applyOptions({{width:chartEl.clientWidth}}));

let needsChartRebuild=false;

function advanceTo(targetIdx){{
if(targetIdx>totalTicks)targetIdx=totalTicks;
let changed=false;
while(currentIdx<targetIdx){{
const t=allTicks[currentIdx],ts=Math.floor(t.timestamp);
priceBySecond[ts]=t.price;
changed=true;
const tv=t.volume-prevVol;prevVol=t.volume;
if(tv>0){{
const c=t.price>=(currentIdx>0?allTicks[currentIdx-1].price:t.price)?'#22c55e55':'#ef444455';
volBySecond[ts]={{value:(volBySecond[ts]?volBySecond[ts].value:0)+tv,color:c}};
cumPV+=t.price*tv;cumVol+=tv;
}}
currentIdx++;
}}
if(changed)needsChartRebuild=true;
if(currentIdx>0){{updateStats(allTicks[currentIdx-1]);if(predictions.length>0)updatePredictions(allTicks[currentIdx-1].timestamp);}}
}}

// Rebuild chart data arrays from maps — called less frequently for performance
function rebuildChart(){{
if(!needsChartRebuild)return;
needsChartRebuild=false;
const keys=Object.keys(priceBySecond).map(Number).sort((a,b)=>a-b);
priceData=keys.map(k=>({{time:k,value:priceBySecond[k]}}));
volData=keys.filter(k=>volBySecond[k]).map(k=>({{time:k,...volBySecond[k]}}));
lineSeries.setData(priceData);
volumeSeries.setData(volData);
}}

function updateStats(t){{
const p=t.price,ch=t.change||0,pct=((ch/(p-ch))*100)||0,up=ch>=0;
document.getElementById('s_price').textContent=p.toFixed(2);
const ce=document.getElementById('s_change');ce.textContent=(up?'+':'')+ch.toFixed(2)+' ('+pct.toFixed(2)+'%)';ce.className='stat-value '+(up?'up':'down');
document.getElementById('s_high').textContent=t.high?t.high.toFixed(2):'—';
document.getElementById('s_low').textContent=t.low?t.low.toFixed(2):'—';
document.getElementById('s_vol').textContent=t.volume?(t.volume/1e6).toFixed(2)+'M':'—';
document.getElementById('s_trades').textContent=t.trades||'—';
const vwap=cumVol>0?(cumPV/cumVol):0;document.getElementById('s_vwap').textContent=vwap>0?vwap.toFixed(2):'—';
const dt=new Date(t.timestamp*1000);const ts=dt.toLocaleTimeString('en-GB',{{hour12:false,timeZone:'Asia/Karachi'}});
document.getElementById('s_time').textContent=ts;document.getElementById('tl_current').textContent=ts;
document.getElementById('ob_bid').textContent=t.bid?t.bid.toFixed(2):'—';
document.getElementById('ob_ask').textContent=t.ask?t.ask.toFixed(2):'—';
document.getElementById('ob_bidvol').textContent=t.bidVol?t.bidVol.toLocaleString():'—';
document.getElementById('ob_askvol').textContent=t.askVol?t.askVol.toLocaleString():'—';
if(t.bid>0&&t.ask>0){{
document.getElementById('ob_spread').textContent=(t.ask-t.bid).toFixed(2);
const imb=((t.bidVol-t.askVol)/(t.bidVol+t.askVol)*100);const ie=document.getElementById('ob_imbalance');
ie.textContent=(imb>0?'+':'')+imb.toFixed(0)+'%';ie.style.color=imb>0?'#22c55e':'#ef4444';
}}
document.getElementById('tickCounter').textContent=currentIdx.toLocaleString()+' / '+totalTicks.toLocaleString()+' ticks';
document.getElementById('scrubber').value=(currentIdx/totalTicks*100);
const side=currentIdx>1&&t.price>=allTicks[currentIdx-2].price?'buy':'sell';
recentTrades.unshift({{time:ts,price:p.toFixed(2),bid:t.bid?t.bid.toFixed(2):'—',ask:t.ask?t.ask.toFixed(2):'—',side}});
if(recentTrades.length>5)recentTrades.pop();
document.getElementById('ticklog').innerHTML=recentTrades.map(r=>
`<div class="ticklog-row ${{r.side}}"><span style="width:65px">${{r.time}}</span><span style="width:65px">${{r.price}}</span><span style="width:55px">${{r.bid}}</span><span style="width:55px">${{r.ask}}</span><span style="width:40px">${{r.side.toUpperCase()}}</span></div>`).join('');
}}

let frameCount=0;
let tickAccum=0;
function animationLoop(){{
if(!playing)return;
tickAccum+=speed;
if(tickAccum>=1){{
const step=Math.floor(tickAccum);
tickAccum-=step;
advanceTo(Math.min(currentIdx+step,totalTicks));
}}
frameCount++;
if(frameCount%3===0)rebuildChart();
if(currentIdx>=totalTicks){{rebuildChart();togglePlay();return;}}
animFrameId=requestAnimationFrame(animationLoop);
}}

function togglePlay(){{
playing=!playing;const b=document.getElementById('playBtn');
if(playing){{b.textContent='⏸ Pause';b.classList.add('active');if(currentIdx>=totalTicks)resetReplay();animFrameId=requestAnimationFrame(animationLoop);}}
else{{b.textContent='▶ Play';b.classList.remove('active');if(animFrameId)cancelAnimationFrame(animFrameId);}}
}}

function setSpeed(s){{speed=s;tickAccum=0;document.querySelectorAll('.speed-btn').forEach(b=>b.classList.toggle('active',parseFloat(b.dataset.speed)===s));}}

function seek(pct){{
const wp=playing;if(playing)togglePlay();
resetReplay();
const ti=Math.floor(pct/100*totalTicks);
// Rebuild state up to target
for(let i=0;i<ti;i++){{
const t=allTicks[i],ts=Math.floor(t.timestamp);
priceBySecond[ts]=t.price;
const tv=i>0?t.volume-allTicks[i-1].volume:0;
if(tv>0){{
const c=t.price>=(i>0?allTicks[i-1].price:t.price)?'#22c55e55':'#ef444455';
volBySecond[ts]={{value:(volBySecond[ts]?volBySecond[ts].value:0)+tv,color:c}};
cumPV+=t.price*tv;cumVol+=tv;
}}
prevVol=t.volume;
}}
currentIdx=ti;needsChartRebuild=true;rebuildChart();
if(ti>0)updateStats(allTicks[ti-1]);
if(wp)togglePlay();
}}

function stepForward(){{const ct=currentIdx>0?allTicks[currentIdx-1].timestamp:allTicks[0].timestamp;let t=currentIdx;while(t<totalTicks&&allTicks[t].timestamp<ct+60)t++;advanceTo(t);}}
function stepBack(){{if(currentIdx<=0)return;const ct=allTicks[currentIdx-1].timestamp;let t=currentIdx-1;while(t>0&&allTicks[t].timestamp>ct-60)t--;seek(t/totalTicks*100);}}
function resetReplay(){{currentIdx=0;cumPV=0;cumVol=0;prevVol=0;recentTrades=[];frameCount=0;
Object.keys(priceBySecond).forEach(k=>delete priceBySecond[k]);
Object.keys(volBySecond).forEach(k=>delete volBySecond[k]);
priceData=[];volData=[];needsChartRebuild=false;
lineSeries.setData([]);volumeSeries.setData([]);
// Reset prediction state
nextPredIdx=0;nextBayesIdx=0;predCorrect=0;predTotal=0;predMarkers=[];
revealedForecasts=[];revealedActuals=[];revealedCITop=[];revealedCIBot=[];
revealedBayesMean=[];revealedBayesHDITop=[];revealedBayesHDIBot=[];
predictions.forEach(p=>delete p._resolved);
llmComments.forEach(c=>delete c._shown);
if(predForecastSeries){{predForecastSeries.setData([]);predForecastSeries.setMarkers([]);}}
if(predActualSeries)predActualSeries.setData([]);
if(predCITopSeries)predCITopSeries.setData([]);
if(predCIBotSeries)predCIBotSeries.setData([]);
if(bayesMeanSeries)bayesMeanSeries.setData([]);
if(bayesHDITopSeries)bayesHDITopSeries.setData([]);
if(bayesHDIBotSeries)bayesHDIBotSeries.setData([]);
lineSeries.setMarkers([]);}}

if(totalTicks>0){{
const fmt=ts=>new Date(ts*1000).toLocaleTimeString('en-GB',{{hour12:false,timeZone:'Asia/Karachi'}});
document.getElementById('tl_start').textContent=fmt(allTicks[0].timestamp);
document.getElementById('tl_end').textContent=fmt(allTicks[totalTicks-1].timestamp);
document.getElementById('tickCounter').textContent='0 / '+totalTicks.toLocaleString()+' — Ready';
updateStats(allTicks[0]);
}}

// ══════════════════════════════════════════════════════════
// CHART 2 — PREDICTION ACCURACY (predicted vs actual returns)
// ══════════════════════════════════════════════════════════

const predEl=document.getElementById('pred-chart');
let predChart=null,predActualSeries=null,predForecastSeries=null;
let predCITopSeries=null,predCIBotSeries=null;

if(predEl&&predictions.length>0){{
  predChart=LightweightCharts.createChart(predEl,{{
    width:predEl.clientWidth,height:180,
    layout:{{background:{{type:'solid',color:'#0B0E11'}},textColor:'#555'}},
    grid:{{vertLines:{{color:'#0F1318'}},horzLines:{{color:'#0F1318'}}}},
    rightPriceScale:{{borderColor:'#1E2530'}},
    timeScale:{{borderColor:'#1E2530',timeVisible:true,secondsVisible:false}},
  }});
  predCITopSeries=predChart.addAreaSeries({{
    topColor:'rgba(255,179,0,0.12)',bottomColor:'rgba(255,179,0,0.0)',
    lineColor:'rgba(255,179,0,0.25)',lineWidth:1,lineStyle:2,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  predCIBotSeries=predChart.addLineSeries({{
    color:'rgba(255,179,0,0.25)',lineWidth:1,lineStyle:2,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  predForecastSeries=predChart.addLineSeries({{
    color:'#FFB300',lineWidth:2,lineStyle:2,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  predActualSeries=predChart.addLineSeries({{
    color:'#E0E0E0',lineWidth:2,
    priceScaleId:'right',lastValueVisible:true,priceLineVisible:false,
  }});
  const zeroS=predChart.addLineSeries({{
    color:'#333',lineWidth:1,priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  const allPredTimes=predictions.map(p=>Math.floor(p.timestamp));
  if(allPredTimes.length>=2)zeroS.setData(allPredTimes.map(t=>({{time:t,value:0}})));
}}

// ══════════════════════════════════════════════════════════
// CHART 3 — BAYESIAN CREDIBILITY
// ══════════════════════════════════════════════════════════

const bayesEl=document.getElementById('bayes-chart');
let bayesChart=null,bayesMeanSeries=null,bayesHDITopSeries=null,bayesHDIBotSeries=null;

if(bayesEl&&bayesianData.length>0){{
  bayesChart=LightweightCharts.createChart(bayesEl,{{
    width:bayesEl.clientWidth,height:80,
    layout:{{background:{{type:'solid',color:'#0B0E11'}},textColor:'#555'}},
    grid:{{vertLines:{{color:'#0F1318'}},horzLines:{{color:'#0F1318'}}}},
    rightPriceScale:{{borderColor:'#1E2530',scaleMargins:{{top:0.1,bottom:0.1}}}},
    timeScale:{{borderColor:'#1E2530',visible:false}},
  }});
  bayesHDITopSeries=bayesChart.addAreaSeries({{
    topColor:'rgba(0,188,212,0.15)',bottomColor:'rgba(0,188,212,0.02)',
    lineColor:'rgba(0,188,212,0.3)',lineWidth:1,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  bayesHDIBotSeries=bayesChart.addLineSeries({{
    color:'rgba(0,188,212,0.3)',lineWidth:1,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  bayesMeanSeries=bayesChart.addLineSeries({{
    color:'#00BCD4',lineWidth:2,
    priceScaleId:'right',lastValueVisible:true,priceLineVisible:false,
  }});
  const chanceS=bayesChart.addLineSeries({{
    color:'#FF525255',lineWidth:1,lineStyle:2,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  const credThreshS=bayesChart.addLineSeries({{
    color:'#00E67633',lineWidth:1,lineStyle:2,
    priceScaleId:'right',lastValueVisible:false,priceLineVisible:false,
  }});
  if(bayesianData.length>=2){{
    const bTimes=bayesianData.map(b=>Math.floor(b.timestamp));
    chanceS.setData(bTimes.map(t=>({{time:t,value:50}})));
    credThreshS.setData(bTimes.map(t=>({{time:t,value:60}})));
  }}
}}

// ══════════════════════════════════════════════════════════
// SYNC ALL 3 CHARTS
// ══════════════════════════════════════════════════════════

function syncFromMain(range){{
  if(range&&predChart)predChart.timeScale().setVisibleRange(range);
  if(range&&bayesChart)bayesChart.timeScale().setVisibleRange(range);
}}
chart.timeScale().subscribeVisibleTimeRangeChange(syncFromMain);
if(predChart){{
  predChart.timeScale().subscribeVisibleTimeRangeChange(function(range){{
    if(range)chart.timeScale().setVisibleRange(range);
    if(range&&bayesChart)bayesChart.timeScale().setVisibleRange(range);
  }});
}}

// ══════════════════════════════════════════════════════════
// RESIZE ALL CHARTS
// ══════════════════════════════════════════════════════════

const _ro=new ResizeObserver(function(){{
  chart.applyOptions({{width:chartEl.clientWidth}});
  if(predChart)predChart.applyOptions({{width:predEl.clientWidth}});
  if(bayesChart)bayesChart.applyOptions({{width:bayesEl.clientWidth}});
}});
_ro.observe(chartEl);

// ══════════════════════════════════════════════════════════
// PROGRESSIVE PREDICTION REVEAL
// ══════════════════════════════════════════════════════════

let nextPredIdx=0,nextBayesIdx=0,predCorrect=0,predTotal=0;
let predMarkers=[];
let revealedForecasts=[],revealedActuals=[];
let revealedCITop=[],revealedCIBot=[];
let revealedBayesMean=[],revealedBayesHDITop=[],revealedBayesHDIBot=[];

function updatePredictions(currentTs){{
  if(!predictions.length)return;

  // Reveal new predictions as replay crosses their timestamp
  while(nextPredIdx<predictions.length&&predictions[nextPredIdx].timestamp<=currentTs){{
    const pred=predictions[nextPredIdx];
    const t=Math.floor(pred.timestamp);
    predMarkers.push({{
      time:t,
      position:pred.direction==='UP'?'belowBar':'aboveBar',
      color:'#FFB300',
      shape:pred.direction==='UP'?'arrowUp':'arrowDown',
      text:pred.direction+' '+Math.round(pred.probability*100)+'%',
    }});
    revealedForecasts.push({{time:t,value:(pred.predicted_return||0)}});
    revealedCITop.push({{time:t,value:pred.ci_high?((pred.ci_high/pred.price-1)*100):1}});
    revealedCIBot.push({{time:t,value:pred.ci_low?((pred.ci_low/pred.price-1)*100):-1}});
    if(predForecastSeries)predForecastSeries.setData([...revealedForecasts]);
    if(predCITopSeries)predCITopSeries.setData([...revealedCITop]);
    if(predCIBotSeries)predCIBotSeries.setData([...revealedCIBot]);
    nextPredIdx++;
  }}

  // Resolve predictions (replay passed their target time)
  for(const pred of predictions){{
    if(pred.resolve_ts&&currentTs>=pred.resolve_ts&&!pred._resolved){{
      pred._resolved=true;
      const t=Math.floor(pred.resolve_ts);
      if(pred.actual_return!==null){{
        revealedActuals.push({{time:t,value:pred.actual_return}});
        if(predActualSeries)predActualSeries.setData([...revealedActuals]);
      }}
      if(pred.correct!==null){{
        predTotal++;
        if(pred.correct)predCorrect++;
        const idx=predMarkers.findIndex(m=>m.time===Math.floor(pred.timestamp));
        if(idx>=0)predMarkers[idx].color=pred.correct?'#00E676':'#FF5252';
        // Hit/miss dot markers
        if(predForecastSeries){{
          const dots=predictions.filter(p=>p._resolved&&p.correct!==null)
            .map(p=>({{time:Math.floor(p.timestamp),position:'inBar',
              color:p.correct?'#00E676':'#FF5252',
              shape:p.correct?'circle':'square',
              text:p.correct?'OK':'X'}}))
            .sort((a,b)=>a.time-b.time);
          predForecastSeries.setMarkers(dots);
        }}
      }}
      const accEl=document.getElementById('predAccuracy');
      if(accEl&&predTotal>0){{
        const pct=(predCorrect/predTotal*100).toFixed(0);
        accEl.innerHTML=predCorrect+'/'+predTotal+' ('+pct+'%)';
        accEl.style.color=predCorrect/predTotal>0.55?'#00E676':'#FF5252';
      }}
    }}
  }}

  // Update Bayesian chart progressively
  while(nextBayesIdx<bayesianData.length&&bayesianData[nextBayesIdx].timestamp<=currentTs){{
    const b=bayesianData[nextBayesIdx];
    const t=Math.floor(b.timestamp);
    revealedBayesMean.push({{time:t,value:b.posterior_mean*100}});
    revealedBayesHDITop.push({{time:t,value:b.hdi_high*100}});
    revealedBayesHDIBot.push({{time:t,value:b.hdi_low*100}});
    if(bayesMeanSeries)bayesMeanSeries.setData([...revealedBayesMean]);
    if(bayesHDITopSeries)bayesHDITopSeries.setData([...revealedBayesHDITop]);
    if(bayesHDIBotSeries)bayesHDIBotSeries.setData([...revealedBayesHDIBot]);
    const credEl=document.getElementById('credLabel');
    if(credEl){{
      const clr=b.label==='CREDIBLE'?'#00E676':b.label==='MARGINAL'?'#FFB300':'#FF5252';
      credEl.innerHTML='<span style="color:'+clr+'">'+b.label+'</span> ('+
        (b.posterior_mean*100).toFixed(0)+'% n='+b.n+')';
    }}
    nextBayesIdx++;
  }}

  // Update markers on price chart
  if(predMarkers.length>0){{
    lineSeries.setMarkers([...predMarkers].sort((a,b)=>a.time-b.time));
  }}

  // Show next prediction info
  if(nextPredIdx<predictions.length){{
    const next=predictions[nextPredIdx];
    const nextEl=document.getElementById('nextPred');
    if(nextEl){{
      const arrow=next.direction==='UP'?'A':'V';
      const clr=next.direction==='UP'?'#00E676':'#FF5252';
      nextEl.innerHTML='<span style="color:'+clr+'">'+arrow+' '+
        next.direction+' '+Math.round(next.probability*100)+'%</span>'+
        ' -> '+(next.predicted_price?next.predicted_price.toFixed(2):'?');
    }}
  }}

  // LLM commentary
  for(const comment of llmComments){{
    if(Math.abs(currentTs-comment.timestamp)<5&&!comment._shown){{
      comment._shown=true;
      const el=document.getElementById('llmComment');
      if(el){{
        const clr=comment.correct?'#00E676':'#FF5252';
        el.innerHTML='<span style="color:'+clr+'">*</span> '+comment.comment;
        el.style.opacity=1;
        setTimeout(function(){{el.style.opacity=0.3;}},8000);
      }}
    }}
  }}
}}
</script></body></html>"""


def render_tick_replay():
    """Main entry point for the Tick Replay page."""
    st.markdown("## Tick Replay")
    st.caption("60fps client-side replay with TradingView charts — zero server round-trips")

    dates = _get_available_dates()
    if not dates:
        st.warning("No tick data found.")
        render_footer()
        return

    col1, col2 = st.columns(2)
    with col1:
        sel_date = st.selectbox("Date", dates, key="replay_date")
    symbols = _get_symbols(sel_date) if sel_date else []
    with col2:
        if not symbols:
            st.warning("No symbols found")
            render_footer()
            return
        default = "HUBC" if "HUBC" in symbols else symbols[0]
        sel_sym = st.selectbox("Symbol", symbols, index=symbols.index(default) if default in symbols else 0, key="replay_sym")

    # ── ML Prediction overlay controls ──
    pred_col1, pred_col2, pred_col3 = st.columns([1, 1, 1])
    with pred_col1:
        show_predictions = st.checkbox("Show ML Predictions", value=False, key="tr_ml")
    with pred_col2:
        pred_interval = st.selectbox(
            "Predict every", [5, 10, 15, 30], index=2,
            key="tr_pred_int", disabled=not show_predictions,
        )
    with pred_col3:
        show_llm = st.checkbox(
            "LLM Commentary", value=False, key="tr_llm",
            disabled=not show_predictions,
        )

    if st.button("Load Replay", type="primary"):
        ticks = _load_ticks(sel_date, sel_sym)
        if not ticks:
            st.error(f"No ticks for {sel_sym} on {sel_date}")
            return

        if len(ticks) > _MAX_TICKS:
            step = len(ticks) // _MAX_TICKS
            ticks = ticks[::step]
            st.info(f"Downsampled to {len(ticks):,} ticks for performance")
        else:
            st.success(f"Loaded **{len(ticks):,}** ticks for **{sel_sym}**")

        # Generate predictions if enabled
        predictions_json = "[]"
        bayesian_json = "[]"
        comments_json = "[]"

        if show_predictions and len(ticks) > 100:
            with st.spinner(f"Running ML predictions every {pred_interval} min..."):
                from pakfindata.engine.tick_predictor import (
                    generate_replay_predictions,
                    generate_llm_commentary,
                )
                result = generate_replay_predictions(
                    ticks, sel_sym, interval_minutes=pred_interval,
                )
                predictions_json = json.dumps(result["predictions"], default=str)
                bayesian_json = json.dumps(result["bayesian"], default=str)

                s = result.get("summary", {})
                if s.get("resolved", 0) > 0:
                    st.caption(
                        f"Generated {s['total_predictions']} predictions — "
                        f"{s['correct']}/{s['resolved']} correct "
                        f"({s['accuracy']:.0%}) — "
                        f"Credibility: **{s['final_credibility']}** "
                        f"(posterior: {s['final_posterior']:.0%})"
                    )

                if show_llm:
                    with st.spinner("Generating LLM commentary..."):
                        comments = generate_llm_commentary(
                            result["predictions"], sel_sym,
                        )
                        comments_json = json.dumps(comments, default=str)

        ticks_json = json.dumps(ticks, default=str)
        html = _build_replay_html(
            ticks_json, sel_sym, sel_date,
            predictions_json=predictions_json,
            bayesian_json=bayesian_json,
            comments_json=comments_json,
            show_predictions=show_predictions,
        )
        panel_height = 700
        if show_predictions:
            panel_height += 280  # prediction (180) + Bayesian (80) + borders (20)
        components.html(html, height=panel_height, scrolling=False)

    render_footer()
