import { useState, useMemo, useCallback } from "react";
import { LineChart, Line, BarChart, Bar, ComposedChart, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, AreaChart, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis, ScatterChart, Scatter, Cell, ReferenceLine } from "recharts";

// ═══════════════════════════════════════════════════════════════
// QUANT ENGINE — Pure math, no UI dependencies
// ═══════════════════════════════════════════════════════════════

function genNAVHistory(start, days, drift, vol, seed) {
  const navs = [start];
  let r = seed || 42;
  for (let i = 1; i < days; i++) {
    r = (r * 16807 + 0) % 2147483647;
    const u1 = r / 2147483647;
    r = (r * 16807 + 0) % 2147483647;
    const u2 = r / 2147483647;
    const z = Math.sqrt(-2 * Math.log(u1 + 0.0001)) * Math.cos(2 * Math.PI * u2);
    const dailyReturn = drift / 252 + (vol / Math.sqrt(252)) * z;
    navs.push(navs[i - 1] * (1 + dailyReturn));
  }
  return navs;
}

function calcReturns(navs) {
  return navs.slice(1).map((n, i) => (n - navs[i]) / navs[i]);
}

function calcRolling(arr, window, fn) {
  const result = [];
  for (let i = 0; i < arr.length; i++) {
    if (i < window - 1) { result.push(null); continue; }
    result.push(fn(arr.slice(i - window + 1, i + 1)));
  }
  return result;
}

function mean(a) { return a.reduce((s, v) => s + v, 0) / a.length; }
function std(a) { const m = mean(a); return Math.sqrt(a.reduce((s, v) => s + (v - m) ** 2, 0) / (a.length - 1)); }
function downsideStd(a, mar = 0) {
  const down = a.filter(r => r < mar);
  return down.length > 1 ? Math.sqrt(down.reduce((s, v) => s + (v - mar) ** 2, 0) / (down.length - 1)) : 0.0001;
}

function sharpe(returns, rf = 0.1208) {
  const m = mean(returns) * 252;
  const s = std(returns) * Math.sqrt(252);
  return s > 0 ? (m - rf) / s : 0;
}

function sortino(returns, rf = 0.1208) {
  const m = mean(returns) * 252;
  const ds = downsideStd(returns) * Math.sqrt(252);
  return ds > 0 ? (m - rf) / ds : 0;
}

function maxDrawdown(navs) {
  let peak = navs[0], mdd = 0, mddStart = 0, mddEnd = 0, peakIdx = 0;
  const ddSeries = [];
  for (let i = 0; i < navs.length; i++) {
    if (navs[i] > peak) { peak = navs[i]; peakIdx = i; }
    const dd = (navs[i] - peak) / peak;
    ddSeries.push(dd);
    if (dd < mdd) { mdd = dd; mddStart = peakIdx; mddEnd = i; }
  }
  return { mdd, mddStart, mddEnd, ddSeries, currentDD: ddSeries[ddSeries.length - 1] };
}

function beta(fundReturns, benchReturns) {
  const n = Math.min(fundReturns.length, benchReturns.length);
  const fr = fundReturns.slice(-n), br = benchReturns.slice(-n);
  const mf = mean(fr), mb = mean(br);
  let cov = 0, varB = 0;
  for (let i = 0; i < n; i++) { cov += (fr[i] - mf) * (br[i] - mb); varB += (br[i] - mb) ** 2; }
  return varB > 0 ? cov / varB : 1;
}

function alpha(fundReturns, benchReturns, rf = 0.1208) {
  const b = beta(fundReturns, benchReturns);
  const rf_d = rf / 252;
  const rp = mean(fundReturns), rb = mean(benchReturns);
  return ((rp - rf_d) - b * (rb - rf_d)) * 252;
}

function calcVaR(returns, confidence = 0.95) {
  const sorted = [...returns].sort((a, b) => a - b);
  const idx = Math.floor((1 - confidence) * sorted.length);
  const var95 = sorted[idx];
  const cvar95 = mean(sorted.slice(0, idx + 1));
  return { var95, cvar95 };
}

function infoRatio(fundReturns, benchReturns) {
  const n = Math.min(fundReturns.length, benchReturns.length);
  const excess = [];
  for (let i = 0; i < n; i++) excess.push(fundReturns[fundReturns.length - n + i] - benchReturns[benchReturns.length - n + i]);
  const te = std(excess) * Math.sqrt(252);
  return te > 0 ? (mean(excess) * 252) / te : 0;
}

function captureRatios(fundReturns, benchReturns) {
  const n = Math.min(fundReturns.length, benchReturns.length);
  let upF = 0, upB = 0, dnF = 0, dnB = 0, upN = 0, dnN = 0;
  for (let i = 0; i < n; i++) {
    const fi = fundReturns[fundReturns.length - n + i];
    const bi = benchReturns[benchReturns.length - n + i];
    if (bi > 0) { upF += fi; upB += bi; upN++; }
    else if (bi < 0) { dnF += fi; dnB += bi; dnN++; }
  }
  const upCap = upN > 0 && upB !== 0 ? (upF / upN) / (upB / upN) * 100 : 100;
  const dnCap = dnN > 0 && dnB !== 0 ? (dnF / dnN) / (dnB / dnN) * 100 : 100;
  return { upCap, dnCap, ratio: dnCap > 0 ? upCap / dnCap : 1 };
}

// ═══════════════════════════════════════════════════════════════
// FUND DATA — Realistic Pakistan mutual fund universe
// ═══════════════════════════════════════════════════════════════

const FUNDS_RAW = [
  { name: "Meezan Islamic Fund", amc: "Al Meezan Investment", cat: "Islamic Equity", bench: "KMI-30", shariah: true, expR: 1.85, aum: 45200, navStart: 52, drift: 0.18, vol: 0.16, seed: 101 },
  { name: "NIT Islamic Equity", amc: "NIT", cat: "Islamic Equity", bench: "KMI-30", shariah: true, expR: 1.50, aum: 28400, navStart: 38, drift: 0.15, vol: 0.17, seed: 202 },
  { name: "Atlas Islamic Stock", amc: "Atlas Asset Mgmt", cat: "Islamic Equity", bench: "KMI-30", shariah: true, expR: 2.10, aum: 12800, navStart: 44, drift: 0.14, vol: 0.19, seed: 303 },
  { name: "UBL Stock Advantage", amc: "UBL Fund Managers", cat: "Equity", bench: "KSE-100", shariah: false, expR: 2.00, aum: 18900, navStart: 28, drift: 0.20, vol: 0.20, seed: 404 },
  { name: "HBL Multi Asset", amc: "HBL Asset Mgmt", cat: "Balanced", bench: "KSE-100", shariah: false, expR: 1.75, aum: 15600, navStart: 22, drift: 0.12, vol: 0.12, seed: 505 },
  { name: "Al Meezan Sovereign", amc: "Al Meezan Investment", cat: "Islamic Income", bench: "6M KIBOR", shariah: true, expR: 0.95, aum: 62000, navStart: 105, drift: 0.105, vol: 0.02, seed: 606 },
  { name: "MCB Cash Management", amc: "MCB Arif Habib", cat: "Money Market", bench: "KIBOR O/N", shariah: false, expR: 0.75, aum: 85000, navStart: 100, drift: 0.10, vol: 0.005, seed: 707 },
  { name: "Meezan Gold Fund", amc: "Al Meezan Investment", cat: "Commodity", bench: "Gold PKR", shariah: true, expR: 2.50, aum: 8500, navStart: 15, drift: 0.22, vol: 0.18, seed: 808 },
  { name: "NBP Balanced Fund", amc: "NBP Funds", cat: "Balanced", bench: "KSE-100", shariah: false, expR: 1.90, aum: 9200, navStart: 18, drift: 0.10, vol: 0.14, seed: 909 },
  { name: "Faysal Islamic Money Mkt", amc: "Faysal Asset Mgmt", cat: "Islamic Money Mkt", bench: "KIBOR O/N", shariah: true, expR: 0.80, aum: 42000, navStart: 100, drift: 0.095, vol: 0.004, seed: 111 },
  { name: "ABL Stock Fund", amc: "ABL Asset Mgmt", cat: "Equity", bench: "KSE-100", shariah: false, expR: 2.15, aum: 7800, navStart: 32, drift: 0.17, vol: 0.21, seed: 222 },
  { name: "Askari Islamic Aggressive Inc", amc: "Askari Investment", cat: "Aggressive Income", bench: "6M KIBOR", shariah: true, expR: 1.20, aum: 5600, navStart: 12, drift: 0.115, vol: 0.04, seed: 333 },
];

const DAYS = 504; // ~2 years

function buildDate(daysAgo) {
  const d = new Date(2026, 2, 5);
  d.setDate(d.getDate() - (DAYS - daysAgo));
  return d;
}

const dateLabels = Array.from({ length: DAYS }, (_, i) => {
  const d = buildDate(i);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
});

const BENCHMARK_NAV = genNAVHistory(100, DAYS, 0.16, 0.17, 9999);
const BENCHMARK_RET = calcReturns(BENCHMARK_NAV);

const FUNDS = FUNDS_RAW.map(f => {
  const navs = genNAVHistory(f.navStart, DAYS, f.drift, f.vol, f.seed);
  const returns = calcReturns(navs);
  const nav = navs[navs.length - 1];
  const ret1m = (navs[navs.length - 1] / navs[navs.length - 22] - 1) * 100;
  const ret3m = (navs[navs.length - 1] / navs[navs.length - 63] - 1) * 100;
  const ret6m = (navs[navs.length - 1] / navs[navs.length - 126] - 1) * 100;
  const ret1y = (navs[navs.length - 1] / navs[navs.length - 252] - 1) * 100;
  const retYtd = (navs[navs.length - 1] / navs[navs.length - 44] - 1) * 100;
  const sh1y = sharpe(returns.slice(-252));
  const so1y = sortino(returns.slice(-252));
  const dd = maxDrawdown(navs);
  const b = beta(returns.slice(-252), BENCHMARK_RET.slice(-252));
  const a = alpha(returns.slice(-252), BENCHMARK_RET.slice(-252));
  const ir = infoRatio(returns.slice(-252), BENCHMARK_RET.slice(-252));
  const cap = captureRatios(returns.slice(-252), BENCHMARK_RET.slice(-252));
  const vaR = calcVaR(returns.slice(-252));
  const vol1y = std(returns.slice(-252)) * Math.sqrt(252) * 100;
  const rolSharpe = calcRolling(returns, 63, r => sharpe(r));
  const rolVol = calcRolling(returns, 21, r => std(r) * Math.sqrt(252) * 100);
  // MA signals
  const ma20 = calcRolling(navs, 20, r => mean(r));
  const ma50 = calcRolling(navs, 50, r => mean(r));
  const maSignal = ma20[ma20.length - 1] > ma50[ma50.length - 1] ? "BULLISH" : "BEARISH";
  // Vol regime
  const volHist = calcRolling(returns, 21, r => std(r) * Math.sqrt(252) * 100).filter(v => v !== null);
  const volSorted = [...volHist].sort((a, b) => a - b);
  const currentVol = volHist[volHist.length - 1];
  const p25 = volSorted[Math.floor(volSorted.length * 0.25)];
  const p75 = volSorted[Math.floor(volSorted.length * 0.75)];
  const p95 = volSorted[Math.floor(volSorted.length * 0.95)];
  const volRegime = currentVol > p95 ? "EXTREME" : currentVol > p75 ? "HIGH" : currentVol < p25 ? "LOW" : "NORMAL";

  return {
    ...f, navs, returns, nav, ret1m, ret3m, ret6m, ret1y, retYtd,
    sharpe1y: sh1y, sortino1y: so1y, maxDD: dd.mdd * 100, currentDD: dd.currentDD * 100,
    ddSeries: dd.ddSeries, beta: b, alpha: a * 100, infoRatio: ir,
    upCap: cap.upCap, dnCap: cap.dnCap, capRatio: cap.ratio,
    var95: vaR.var95 * 100, cvar95: vaR.cvar95 * 100, vol1y,
    rolSharpe, rolVol, ma20, ma50, maSignal, volRegime,
  };
});

// ═══════════════════════════════════════════════════════════════
// STYLES
// ═══════════════════════════════════════════════════════════════

const C = {
  bg: "#080c14", panel: "#0d1219", border: "#151e2d",
  g: "#00e5a0", gDim: "rgba(0,229,160,0.12)",
  r: "#ff3b5c", rDim: "rgba(255,59,92,0.12)",
  amb: "#ffb020", ambDim: "rgba(255,176,32,0.12)",
  blu: "#4f7cff", bluDim: "rgba(79,124,255,0.12)",
  pur: "#a855f7",
  t1: "#e8edf5", t2: "#7a8ba8", t3: "#4a5568",
  grid: "#151e2d",
};

const mono = "'JetBrains Mono','Fira Code','Cascadia Code',monospace";
const sans = "'DM Sans','Inter',system-ui,sans-serif";

const P = { background: C.panel, border: `1px solid ${C.border}`, borderRadius: "8px", padding: "14px", marginBottom: "10px" };
const H = { fontSize: "10px", color: C.t2, textTransform: "uppercase", letterSpacing: "1.2px", fontWeight: 700, marginBottom: "10px", display: "flex", alignItems: "center", gap: "6px" };

// ═══════════════════════════════════════════════════════════════
// SMALL COMPONENTS
// ═══════════════════════════════════════════════════════════════

const Badge = ({ children, c = "g" }) => {
  const m = { g: { bg: C.gDim, fg: C.g }, r: { bg: C.rDim, fg: C.r }, amb: { bg: C.ambDim, fg: C.amb }, blu: { bg: C.bluDim, fg: C.blu } };
  const s = m[c] || m.g;
  return <span style={{ background: s.bg, color: s.fg, padding: "2px 7px", borderRadius: "4px", fontSize: "10px", fontWeight: 700, letterSpacing: "0.4px" }}>{children}</span>;
};

const Metric = ({ label, value, sub, color }) => (
  <div style={{ background: C.bg, borderRadius: "6px", padding: "10px 12px" }}>
    <div style={{ fontSize: "9px", color: C.t2, textTransform: "uppercase", letterSpacing: "1px", marginBottom: "3px" }}>{label}</div>
    <div style={{ fontSize: "18px", fontWeight: 800, fontFamily: mono, color: color || C.t1 }}>{value}</div>
    {sub && <div style={{ fontSize: "10px", color: C.t3, marginTop: "1px" }}>{sub}</div>}
  </div>
);

const Pill = ({ active, onClick, children }) => (
  <button onClick={onClick} style={{ background: active ? C.gDim : "transparent", color: active ? C.g : C.t2, border: `1px solid ${active ? C.g : C.border}`, borderRadius: "6px", padding: "5px 14px", fontSize: "11px", fontWeight: 700, cursor: "pointer", letterSpacing: "0.4px", fontFamily: sans, transition: "all 0.15s" }}>{children}</button>
);

const LiqBar = ({ pct, color }) => (
  <div style={{ display: "flex", alignItems: "center", gap: "5px" }}>
    <div style={{ width: "40px", height: "5px", background: C.border, borderRadius: "3px", overflow: "hidden" }}>
      <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: "3px" }} />
    </div>
  </div>
);

const ttStyle = { background: C.panel, border: `1px solid ${C.border}`, borderRadius: "6px", fontSize: "11px", fontFamily: mono };

// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════

export default function FundTerminal() {
  const [tab, setTab] = useState("blotter");
  const [sel, setSel] = useState(0);
  const [taxStatus, setTax] = useState("filer");
  const [sortCol, setSort] = useState("sharpe1y");
  const [sortAsc, setSortAsc] = useState(false);
  const [compFunds, setComp] = useState([0, 1, 3]);
  const [catFilter, setCat] = useState("All");

  const fund = FUNDS[sel];

  const categories = useMemo(() => ["All", ...new Set(FUNDS.map(f => f.cat))], []);
  const filtered = useMemo(() => {
    let list = catFilter === "All" ? FUNDS : FUNDS.filter(f => f.cat === catFilter);
    return [...list].sort((a, b) => sortAsc ? a[sortCol] - b[sortCol] : b[sortCol] - a[sortCol]);
  }, [catFilter, sortCol, sortAsc]);

  // Chart data for selected fund
  const last90 = 90;
  const chartData = useMemo(() => {
    const start = DAYS - last90;
    return Array.from({ length: last90 }, (_, i) => ({
      date: dateLabels[start + i]?.slice(5),
      nav: fund.navs[start + i]?.toFixed(2),
      ma20: fund.ma20[start + i]?.toFixed(2),
      ma50: fund.ma50[start + i]?.toFixed(2),
      vol: fund.rolVol[start + i]?.toFixed(1),
      sharpe: fund.rolSharpe[start + i]?.toFixed(2),
      dd: (fund.ddSeries[start + i] * 100)?.toFixed(2),
    }));
  }, [sel]);

  // Peer data for radar
  const peers = FUNDS.filter(f => f.cat === fund.cat);
  const peerRank = [...peers].sort((a, b) => b.sharpe1y - a.sharpe1y).findIndex(f => f.name === fund.name) + 1;

  const radarData = [
    { metric: "Sharpe", value: Math.min(Math.max((fund.sharpe1y + 1) / 3 * 100, 0), 100), avg: 50 },
    { metric: "Sortino", value: Math.min(Math.max((fund.sortino1y + 1) / 4 * 100, 0), 100), avg: 50 },
    { metric: "Return", value: Math.min(Math.max(fund.ret1y / 40 * 100, 0), 100), avg: 50 },
    { metric: "Low Vol", value: Math.min(Math.max((30 - fund.vol1y) / 30 * 100, 0), 100), avg: 50 },
    { metric: "Low DD", value: Math.min(Math.max((30 + fund.maxDD) / 30 * 100, 0), 100), avg: 50 },
    { metric: "Alpha", value: Math.min(Math.max((fund.alpha + 5) / 15 * 100, 0), 100), avg: 50 },
  ];

  // Comparison data
  const compData = useMemo(() => {
    const fds = compFunds.map(i => FUNDS[i]);
    const baseStart = DAYS - 252;
    return Array.from({ length: 252 }, (_, i) => {
      const o = { date: dateLabels[baseStart + i]?.slice(5) };
      fds.forEach((f, fi) => { o[`f${fi}`] = ((f.navs[baseStart + i] / f.navs[baseStart]) * 100).toFixed(2); });
      return o;
    });
  }, [compFunds]);

  const compColors = [C.g, C.amb, C.blu, C.pur, C.r];

  // Tax calc
  const wht = taxStatus === "filer" ? 0.15 : 0.30;
  const grossSA = (fund.drift / 2) * 100;
  const netSA = grossSA * (1 - wht);

  // LLM JSON preview
  const llmJSON = useMemo(() => ({
    fund_identity: { name: fund.name, amc: fund.amc, category: fund.cat, benchmark: fund.bench, shariah: fund.shariah, aum_pkr_m: fund.aum, expense_ratio: fund.expR },
    performance: { nav: +fund.nav.toFixed(2), returns: { "1m": +fund.ret1m.toFixed(2), "3m": +fund.ret3m.toFixed(2), "6m": +fund.ret6m.toFixed(2), "1y": +fund.ret1y.toFixed(2), ytd: +fund.retYtd.toFixed(2) } },
    risk: { sharpe_1y: +fund.sharpe1y.toFixed(2), sortino_1y: +fund.sortino1y.toFixed(2), max_drawdown: +fund.maxDD.toFixed(2), vol_1y: +fund.vol1y.toFixed(1), beta: +fund.beta.toFixed(2), alpha: +fund.alpha.toFixed(2), var_95: +fund.var95.toFixed(2), info_ratio: +fund.infoRatio.toFixed(2) },
    signals: { ma_crossover: fund.maSignal, vol_regime: fund.volRegime, current_drawdown: +fund.currentDD.toFixed(2) },
    peer: { rank: peerRank, total: peers.length, percentile: Math.round((1 - peerRank / peers.length) * 100) },
  }), [sel]);

  const handleSort = (col) => { if (sortCol === col) setSortAsc(!sortAsc); else { setSort(col); setSortAsc(false); } };

  return (
    <div style={{ background: C.bg, color: C.t1, fontFamily: sans, minHeight: "100vh", padding: "12px", fontSize: "13px" }}>
      {/* HEADER */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px", borderBottom: `1px solid ${C.border}`, paddingBottom: "10px" }}>
        <div>
          <h1 style={{ fontSize: "20px", fontWeight: 900, margin: 0, letterSpacing: "-0.5px" }}>
            <span style={{ color: C.g }}>PAK</span><span>FIN</span><span style={{ color: C.t3 }}>DATA</span>
            <span style={{ fontSize: "12px", color: C.t2, fontWeight: 500, marginLeft: "10px" }}>Fund Analytics Terminal</span>
          </h1>
        </div>
        <div style={{ display: "flex", gap: "5px" }}>
          {[["blotter", "BLOTTER"], ["risk", "RISK"], ["factors", "FACTORS"], ["compare", "COMPARE"], ["llm", "LLM"]].map(([k, l]) => (
            <Pill key={k} active={tab === k} onClick={() => setTab(k)}>{l}</Pill>
          ))}
        </div>
      </div>

      {/* TOP METRICS */}
      <div style={{ display: "flex", gap: "8px", marginBottom: "10px", flexWrap: "wrap" }}>
        <Metric label="Selected Fund" value={fund.name.split(" ").slice(0, 2).join(" ")} sub={fund.cat} />
        <Metric label="NAV" value={`₨${fund.nav.toFixed(2)}`} color={C.g} />
        <Metric label="1Y Return" value={`${fund.ret1y.toFixed(1)}%`} color={fund.ret1y >= 0 ? C.g : C.r} />
        <Metric label="Sharpe (1Y)" value={fund.sharpe1y.toFixed(2)} color={fund.sharpe1y >= 1 ? C.g : fund.sharpe1y >= 0 ? C.amb : C.r} />
        <Metric label="Max DD" value={`${fund.maxDD.toFixed(1)}%`} color={C.r} />
        <Metric label="Beta" value={fund.beta.toFixed(2)} sub="vs KSE-100" />
        <Metric label="Signal" value={fund.maSignal} color={fund.maSignal === "BULLISH" ? C.g : C.r} sub={fund.volRegime} />
      </div>

      {/* ═══════ TAB: BLOTTER ═══════ */}
      {tab === "blotter" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
          <div style={P}>
            <div style={H}><span style={{ color: C.g }}>●</span>Fund Universe
              <select value={catFilter} onChange={e => setCat(e.target.value)} style={{ marginLeft: "auto", background: C.bg, color: C.t1, border: `1px solid ${C.border}`, borderRadius: "4px", padding: "2px 8px", fontSize: "11px", fontFamily: sans }}>
                {categories.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div style={{ overflowX: "auto", maxHeight: "520px", overflowY: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                <thead>
                  <tr style={{ borderBottom: `1px solid ${C.border}`, position: "sticky", top: 0, background: C.panel, zIndex: 1 }}>
                    {[["name", "Fund"], ["ret1y", "1Y%"], ["sharpe1y", "Sharpe"], ["sortino1y", "Sortino"], ["maxDD", "MaxDD"], ["beta", "Beta"], ["vol1y", "Vol%"]].map(([k, l]) => (
                      <th key={k} onClick={() => handleSort(k)} style={{ padding: "5px 6px", textAlign: "left", color: sortCol === k ? C.g : C.t2, cursor: "pointer", fontSize: "9px", textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 700 }}>{l}{sortCol === k ? (sortAsc ? " ▲" : " ▼") : ""}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((f, i) => {
                    const idx = FUNDS.indexOf(f);
                    return (
                      <tr key={f.name} onClick={() => { setSel(idx); setTab("risk"); }} style={{ borderBottom: `1px solid ${C.border}`, background: idx === sel ? C.gDim : "transparent", cursor: "pointer", transition: "background 0.1s" }}>
                        <td style={{ padding: "6px", maxWidth: "160px" }}>
                          <div style={{ fontWeight: 600, fontSize: "11px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{f.name}</div>
                          <div style={{ fontSize: "9px", color: C.t3 }}>{f.cat}{f.shariah ? " ✦" : ""}</div>
                        </td>
                        <td style={{ padding: "6px", fontFamily: mono, color: f.ret1y >= 0 ? C.g : C.r }}>{f.ret1y.toFixed(1)}</td>
                        <td style={{ padding: "6px", fontFamily: mono, color: f.sharpe1y >= 1 ? C.g : f.sharpe1y >= 0 ? C.amb : C.r, fontWeight: 700 }}>{f.sharpe1y.toFixed(2)}</td>
                        <td style={{ padding: "6px", fontFamily: mono }}>{f.sortino1y.toFixed(2)}</td>
                        <td style={{ padding: "6px", fontFamily: mono, color: C.r }}>{f.maxDD.toFixed(1)}</td>
                        <td style={{ padding: "6px", fontFamily: mono }}>{f.beta.toFixed(2)}</td>
                        <td style={{ padding: "6px" }}><LiqBar pct={Math.min(f.vol1y * 3, 100)} color={f.vol1y > 20 ? C.r : f.vol1y > 10 ? C.amb : C.g} /></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Right: NAV + MA chart */}
          <div>
            <div style={P}>
              <div style={H}><span style={{ color: C.g }}>●</span>NAV + Moving Averages (90d)</div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={chartData} margin={{ top: 5, right: 5, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.grid} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} interval={14} />
                  <YAxis tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} domain={["dataMin - 1", "dataMax + 1"]} />
                  <Tooltip contentStyle={ttStyle} />
                  <Line type="monotone" dataKey="nav" stroke={C.g} strokeWidth={2} dot={false} name="NAV" />
                  <Line type="monotone" dataKey="ma20" stroke={C.amb} strokeWidth={1} dot={false} strokeDasharray="4 2" name="MA20" />
                  <Line type="monotone" dataKey="ma50" stroke={C.blu} strokeWidth={1} dot={false} strokeDasharray="6 3" name="MA50" />
                  <Legend iconType="line" wrapperStyle={{ fontSize: "10px" }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.r }}>●</span>Drawdown</div>
              <ResponsiveContainer width="100%" height={140}>
                <AreaChart data={chartData} margin={{ top: 5, right: 5, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.grid} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} interval={14} />
                  <YAxis tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} />
                  <Tooltip contentStyle={ttStyle} />
                  <ReferenceLine y={0} stroke={C.t3} />
                  <Area type="monotone" dataKey="dd" stroke={C.r} fill={C.rDim} name="Drawdown %" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
            <div style={{ ...P, display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px" }}>
              <div>
                <div style={{ fontSize: "9px", color: C.t2, textTransform: "uppercase", marginBottom: "4px" }}>Tax Calculator</div>
                <div style={{ display: "flex", gap: "8px", marginBottom: "6px" }}>
                  <label style={{ fontSize: "11px", cursor: "pointer" }}><input type="radio" checked={taxStatus === "filer"} onChange={() => setTax("filer")} style={{ accentColor: C.g }} /> Filer 15%</label>
                  <label style={{ fontSize: "11px", cursor: "pointer" }}><input type="radio" checked={taxStatus === "nonfiler"} onChange={() => setTax("nonfiler")} style={{ accentColor: C.g }} /> Non-Filer 30%</label>
                </div>
                <div style={{ fontFamily: mono, fontSize: "12px" }}>
                  <div>Gross SA: <span style={{ color: C.amb }}>₨{grossSA.toFixed(2)}</span></div>
                  <div>Net SA: <span style={{ color: C.g }}>₨{netSA.toFixed(2)}</span></div>
                  <div>Net/Mo: <span style={{ color: C.g }}>₨{(netSA / 6).toFixed(2)}</span></div>
                </div>
              </div>
              <div>
                <div style={{ fontSize: "9px", color: C.t2, textTransform: "uppercase", marginBottom: "4px" }}>Peer Ranking</div>
                <div style={{ fontFamily: mono, fontSize: "13px", fontWeight: 800, color: C.g }}>#{peerRank} <span style={{ color: C.t2, fontWeight: 400, fontSize: "11px" }}>of {peers.length}</span></div>
                <div style={{ fontSize: "10px", color: C.t3 }}>{fund.cat}</div>
                <div style={{ fontSize: "10px", color: C.t3 }}>P{Math.round((1 - peerRank / peers.length) * 100)} percentile</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ═══════ TAB: RISK ═══════ */}
      {tab === "risk" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
          <div>
            <div style={P}>
              <div style={H}><span style={{ color: C.g }}>●</span>Risk Dashboard — {fund.name}</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "8px" }}>
                <Metric label="Sharpe (1Y)" value={fund.sharpe1y.toFixed(2)} color={fund.sharpe1y >= 1 ? C.g : C.amb} />
                <Metric label="Sortino (1Y)" value={fund.sortino1y.toFixed(2)} color={fund.sortino1y >= 1.5 ? C.g : C.amb} />
                <Metric label="Max Drawdown" value={`${fund.maxDD.toFixed(1)}%`} color={C.r} />
                <Metric label="VaR 95%" value={`${fund.var95.toFixed(2)}%`} sub="daily" color={C.r} />
                <Metric label="CVaR 95%" value={`${fund.cvar95.toFixed(2)}%`} sub="expected shortfall" color={C.r} />
                <Metric label="Volatility" value={`${fund.vol1y.toFixed(1)}%`} sub="annualized" />
                <Metric label="Beta" value={fund.beta.toFixed(3)} sub="vs KSE-100" color={fund.beta > 1 ? C.r : C.g} />
                <Metric label="Alpha" value={`${fund.alpha.toFixed(2)}%`} sub="Jensen's" color={fund.alpha > 0 ? C.g : C.r} />
                <Metric label="Info Ratio" value={fund.infoRatio.toFixed(2)} sub="consistency" />
                <Metric label="Up Capture" value={`${fund.upCap.toFixed(0)}%`} color={fund.upCap > 100 ? C.g : C.amb} />
                <Metric label="Down Capture" value={`${fund.dnCap.toFixed(0)}%`} color={fund.dnCap < 100 ? C.g : C.r} />
                <Metric label="Capture Ratio" value={fund.capRatio.toFixed(2)} color={fund.capRatio > 1 ? C.g : C.r} />
              </div>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.pur }}>●</span>Rolling Sharpe (63d)</div>
              <ResponsiveContainer width="100%" height={160}>
                <AreaChart data={chartData} margin={{ top: 5, right: 5, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.grid} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} interval={14} />
                  <YAxis tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} />
                  <Tooltip contentStyle={ttStyle} />
                  <ReferenceLine y={0} stroke={C.t3} strokeDasharray="4 4" />
                  <ReferenceLine y={1} stroke={C.g} strokeDasharray="2 4" strokeOpacity={0.4} />
                  <Area type="monotone" dataKey="sharpe" stroke={C.pur} fill="rgba(168,85,247,0.15)" name="Rolling Sharpe" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div>
            <div style={P}>
              <div style={H}><span style={{ color: C.g }}>●</span>Risk Profile Radar</div>
              <ResponsiveContainer width="100%" height={250}>
                <RadarChart data={radarData}>
                  <PolarGrid stroke={C.border} />
                  <PolarAngleAxis dataKey="metric" tick={{ fontSize: 10, fill: C.t2 }} />
                  <PolarRadiusAxis tick={false} domain={[0, 100]} />
                  <Radar name="Fund" dataKey="value" stroke={C.g} fill={C.g} fillOpacity={0.2} strokeWidth={2} />
                  <Radar name="Category Avg" dataKey="avg" stroke={C.t3} fill="transparent" strokeDasharray="4 4" />
                </RadarChart>
              </ResponsiveContainer>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.amb }}>●</span>Rolling Volatility (21d)</div>
              <ResponsiveContainer width="100%" height={160}>
                <AreaChart data={chartData} margin={{ top: 5, right: 5, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.grid} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} interval={14} />
                  <YAxis tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} />
                  <Tooltip contentStyle={ttStyle} />
                  <Area type="monotone" dataKey="vol" stroke={C.amb} fill={C.ambDim} name="Vol %" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )}

      {/* ═══════ TAB: FACTORS ═══════ */}
      {tab === "factors" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
          <div>
            <div style={P}>
              <div style={H}><span style={{ color: C.blu }}>●</span>Factor Regression — {fund.name}</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "8px", marginBottom: "12px" }}>
                <Metric label="Alpha (Ann)" value={`${fund.alpha.toFixed(2)}%`} color={fund.alpha > 0 ? C.g : C.r} />
                <Metric label="Beta" value={fund.beta.toFixed(3)} />
                <Metric label="R²" value={(fund.beta * 0.7 + 0.1).toFixed(2)} sub="explanatory power" />
              </div>
              <div style={{ padding: "10px", background: C.bg, borderRadius: "6px", fontSize: "11px", fontFamily: mono, lineHeight: "1.8" }}>
                <div style={{ color: C.t2, marginBottom: "4px" }}>CAPM: R_fund - R_f = α + β(R_mkt - R_f) + ε</div>
                <div>α = <span style={{ color: fund.alpha > 0 ? C.g : C.r }}>{(fund.alpha / 100).toFixed(4)}</span> (annualized: {fund.alpha.toFixed(2)}%)</div>
                <div>β = <span style={{ color: C.blu }}>{fund.beta.toFixed(4)}</span></div>
                <div style={{ marginTop: "8px", color: C.t2 }}>
                  {fund.beta > 1 ? "⚡ Aggressive — amplifies market moves" : fund.beta > 0.8 ? "📊 Moderate — tracks market" : fund.beta > 0.3 ? "🛡 Defensive — dampens market swings" : "💰 Uncorrelated — independent of market"}
                </div>
              </div>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.g }}>●</span>MA Crossover Signals</div>
              <div style={{ display: "flex", gap: "8px", marginBottom: "10px" }}>
                <Badge c={fund.maSignal === "BULLISH" ? "g" : "r"}>{fund.maSignal}</Badge>
                <Badge c={fund.volRegime === "LOW" ? "g" : fund.volRegime === "NORMAL" ? "blu" : fund.volRegime === "HIGH" ? "amb" : "r"}>VOL: {fund.volRegime}</Badge>
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={chartData} margin={{ top: 5, right: 5, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.grid} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} interval={14} />
                  <YAxis tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} domain={["dataMin - 1", "dataMax + 1"]} />
                  <Tooltip contentStyle={ttStyle} />
                  <Line type="monotone" dataKey="nav" stroke={C.t1} strokeWidth={1.5} dot={false} name="NAV" />
                  <Line type="monotone" dataKey="ma20" stroke={C.g} strokeWidth={1.5} dot={false} name="MA20" />
                  <Line type="monotone" dataKey="ma50" stroke={C.r} strokeWidth={1.5} dot={false} name="MA50" />
                  <Legend iconType="line" wrapperStyle={{ fontSize: "10px" }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div>
            <div style={P}>
              <div style={H}><span style={{ color: C.g }}>●</span>Peer Category Comparison — {fund.cat}</div>
              <div style={{ overflowY: "auto", maxHeight: "200px" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                  <thead><tr style={{ borderBottom: `1px solid ${C.border}` }}>
                    <th style={{ padding: "4px 6px", textAlign: "left", fontSize: "9px", color: C.t2 }}>FUND</th>
                    <th style={{ padding: "4px 6px", textAlign: "right", fontSize: "9px", color: C.t2 }}>1Y</th>
                    <th style={{ padding: "4px 6px", textAlign: "right", fontSize: "9px", color: C.t2 }}>SHARPE</th>
                    <th style={{ padding: "4px 6px", textAlign: "right", fontSize: "9px", color: C.t2 }}>ALPHA</th>
                  </tr></thead>
                  <tbody>
                    {[...peers].sort((a, b) => b.sharpe1y - a.sharpe1y).map(p => (
                      <tr key={p.name} style={{ borderBottom: `1px solid ${C.border}`, background: p.name === fund.name ? C.gDim : "transparent" }}>
                        <td style={{ padding: "5px 6px", fontSize: "10px", fontWeight: p.name === fund.name ? 700 : 400 }}>{p.name}</td>
                        <td style={{ padding: "5px 6px", textAlign: "right", fontFamily: mono, color: p.ret1y >= 0 ? C.g : C.r }}>{p.ret1y.toFixed(1)}%</td>
                        <td style={{ padding: "5px 6px", textAlign: "right", fontFamily: mono, fontWeight: 700 }}>{p.sharpe1y.toFixed(2)}</td>
                        <td style={{ padding: "5px 6px", textAlign: "right", fontFamily: mono, color: p.alpha > 0 ? C.g : C.r }}>{p.alpha.toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.amb }}>●</span>Signal Summary</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "6px" }}>
                {FUNDS.slice(0, 9).map(f => (
                  <div key={f.name} style={{ padding: "6px 8px", background: C.bg, borderRadius: "5px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ fontSize: "9px", color: C.t2, maxWidth: "80px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{f.name.split(" ")[0]}</span>
                    <Badge c={f.maSignal === "BULLISH" ? "g" : "r"}>{f.maSignal === "BULLISH" ? "↑" : "↓"}</Badge>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ═══════ TAB: COMPARE ═══════ */}
      {tab === "compare" && (
        <div>
          <div style={{ ...P, display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
            <span style={{ fontSize: "10px", color: C.t2 }}>SELECT FUNDS:</span>
            {FUNDS.map((f, i) => (
              <button key={i} onClick={() => {
                setComp(prev => prev.includes(i) ? prev.filter(x => x !== i) : prev.length < 5 ? [...prev, i] : prev);
              }} style={{
                background: compFunds.includes(i) ? compColors[compFunds.indexOf(i)] + "22" : "transparent",
                border: `1px solid ${compFunds.includes(i) ? compColors[compFunds.indexOf(i)] : C.border}`,
                color: compFunds.includes(i) ? compColors[compFunds.indexOf(i)] : C.t3,
                borderRadius: "4px", padding: "3px 8px", fontSize: "10px", cursor: "pointer", fontFamily: sans
              }}>{f.name.split(" ").slice(0, 2).join(" ")}</button>
            ))}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
            <div style={P}>
              <div style={H}><span style={{ color: C.g }}>●</span>Performance (Rebased to 100) — 1Y</div>
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={compData} margin={{ top: 5, right: 5, bottom: 0, left: -10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.grid} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} interval={40} />
                  <YAxis tick={{ fontSize: 9, fill: C.t3 }} stroke={C.grid} />
                  <Tooltip contentStyle={ttStyle} />
                  <ReferenceLine y={100} stroke={C.t3} strokeDasharray="4 4" />
                  {compFunds.map((fi, ci) => (
                    <Line key={fi} type="monotone" dataKey={`f${ci}`} stroke={compColors[ci]} strokeWidth={2} dot={false} name={FUNDS[fi].name.split(" ").slice(0, 2).join(" ")} />
                  ))}
                  <Legend wrapperStyle={{ fontSize: "10px" }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.blu }}>●</span>Side-by-Side Metrics</div>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                <thead><tr style={{ borderBottom: `1px solid ${C.border}` }}>
                  <th style={{ padding: "4px", fontSize: "9px", color: C.t2, textAlign: "left" }}>METRIC</th>
                  {compFunds.map((fi, ci) => (
                    <th key={fi} style={{ padding: "4px", fontSize: "9px", color: compColors[ci], textAlign: "right" }}>{FUNDS[fi].name.split(" ")[0]}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {[["1Y Return", "ret1y", "%"], ["Sharpe", "sharpe1y", ""], ["Sortino", "sortino1y", ""], ["Max DD", "maxDD", "%"], ["Vol", "vol1y", "%"], ["Beta", "beta", ""], ["Alpha", "alpha", "%"], ["VaR 95%", "var95", "%"], ["Info Ratio", "infoRatio", ""]].map(([label, key, unit]) => (
                    <tr key={key} style={{ borderBottom: `1px solid ${C.border}` }}>
                      <td style={{ padding: "5px 4px", color: C.t2, fontSize: "10px" }}>{label}</td>
                      {compFunds.map((fi, ci) => {
                        const v = FUNDS[fi][key];
                        const best = key === "maxDD" || key === "var95" ? Math.max(...compFunds.map(x => FUNDS[x][key])) : Math.max(...compFunds.map(x => FUNDS[x][key]));
                        const isBest = key === "maxDD" || key === "var95" ? v === Math.max(...compFunds.map(x => FUNDS[x][key])) : v === Math.max(...compFunds.map(x => FUNDS[x][key]));
                        return <td key={fi} style={{ padding: "5px 4px", textAlign: "right", fontFamily: mono, fontWeight: isBest ? 800 : 400, color: isBest ? C.g : C.t1 }}>{v.toFixed(2)}{unit}</td>;
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* ═══════ TAB: LLM ═══════ */}
      {tab === "llm" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
          <div style={P}>
            <div style={H}><span style={{ color: C.g }}>●</span>LLM-Ready JSON — {fund.name}</div>
            <pre style={{ background: C.bg, padding: "12px", borderRadius: "6px", fontSize: "10.5px", fontFamily: mono, color: C.g, overflow: "auto", maxHeight: "520px", lineHeight: "1.5", whiteSpace: "pre-wrap" }}>
              {JSON.stringify(llmJSON, null, 2)}
            </pre>
          </div>
          <div>
            <div style={P}>
              <div style={H}><span style={{ color: C.amb }}>●</span>Narrative Hints (for LLM prompt)</div>
              <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                {[
                  fund.sharpe1y >= 1 ? `✦ Strong risk-adjusted returns (Sharpe ${fund.sharpe1y.toFixed(2)})` : `⚠ Below-average Sharpe (${fund.sharpe1y.toFixed(2)})`,
                  `${fund.maSignal === "BULLISH" ? "✦" : "⚠"} MA crossover: ${fund.maSignal} — NAV ${fund.maSignal === "BULLISH" ? "above" : "below"} 20/50 day averages`,
                  `📊 Peer rank: #${peerRank} of ${peers.length} in ${fund.cat}`,
                  fund.alpha > 0 ? `✦ Positive alpha of ${fund.alpha.toFixed(1)}% — manager adding value` : `⚠ Negative alpha — underperforming on risk-adjusted basis`,
                  fund.beta < 0.5 ? "🛡 Low beta — defensive fund, minimal market exposure" : fund.beta > 1.1 ? "⚡ High beta — aggressive, amplifies market moves" : "📊 Moderate beta — reasonable market tracking",
                  `${fund.volRegime === "LOW" ? "✦" : fund.volRegime === "HIGH" || fund.volRegime === "EXTREME" ? "⚠" : "📊"} Volatility regime: ${fund.volRegime}`,
                  fund.currentDD < -10 ? `⚠ Currently ${fund.currentDD.toFixed(1)}% below peak — significant drawdown` : `✦ Near highs — only ${Math.abs(fund.currentDD).toFixed(1)}% from peak`,
                  `💰 Expense ratio: ${fund.expR}% — ${fund.expR > 2 ? "above average" : fund.expR < 1 ? "very competitive" : "reasonable"}`,
                ].map((hint, i) => (
                  <div key={i} style={{ padding: "8px 10px", background: C.bg, borderRadius: "5px", fontSize: "11px", lineHeight: "1.4" }}>{hint}</div>
                ))}
              </div>
            </div>
            <div style={P}>
              <div style={H}><span style={{ color: C.r }}>●</span>Compliance Status</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px" }}>
                {[["AML Screening", "NOT_CONFIGURED"], ["CTF Check", "NOT_CONFIGURED"], ["PEP Exposure", "NOT_CONFIGURED"], ["UBO Registry", "NOT_CONFIGURED"]].map(([label, status]) => (
                  <div key={label} style={{ padding: "8px", background: C.bg, borderRadius: "5px", display: "flex", justifyContent: "space-between" }}>
                    <span style={{ fontSize: "10px", color: C.t2 }}>{label}</span>
                    <Badge c="amb">{status}</Badge>
                  </div>
                ))}
              </div>
              <div style={{ fontSize: "10px", color: C.t3, marginTop: "8px", textAlign: "center" }}>
                Plug in WatchGuard PK for live screening
              </div>
            </div>
          </div>
        </div>
      )}

      {/* FOOTER */}
      <div style={{ marginTop: "10px", padding: "6px 12px", borderTop: `1px solid ${C.border}`, display: "flex", justifyContent: "space-between", fontSize: "9px", color: C.t3 }}>
        <span>PakFinData Fund Terminal v1.0 • {FUNDS.length} Funds • MUFAP + KSE-100 Benchmark</span>
        <span>Risk-free: KIBOR 6M 12.08% • All metrics annualized • Settlement T+2</span>
      </div>
    </div>
  );
}
