# Claude Code Prompt: Strategy Fusion Simulator v3 — Bulletproof Architecture

## Why v1 and v2 Failed

| Version | Approach | Why It Failed |
|---------|----------|---------------|
| v1 | Flask on port 8766 + HTTP poll from embedded HTML | Redundant server. Polls DuckDB for stale prices. No chart. |
| v2 | WebSocket from embedded HTML to ws_relay :8765 | **Iframe sandboxing blocks WebSocket to different port.** Browser security, corporate proxies, WSL port forwarding all break it. |

**The working pattern is already in the codebase:** `live_ticker.py` reads `live_snapshot.json` written by `tick_service.py`. No ports. No WebSocket from JS. No iframe. Just file → Streamlit → Plotly. It works everywhere.

## v3 Architecture — Copy What Works

```
tick_service.py (ALREADY RUNNING)
  ├── connects to wss://psxterminal.com/
  ├── writes /mnt/e/psxdata/live_snapshot.json every 2 seconds
  │     Contains: ALL symbol prices, bid/ask, volume
  │
  └── THIS is the price source. Sub-second. Already working.

fusion_service.py (NEW — runs as background process)
  ├── Reads live_snapshot.json every interval (5-15s)
  ├── Extracts price for selected symbol
  ├── Builds 1-minute candles from price updates
  ├── Runs enabled strategy engines
  ├── Fuses signals → BUY/SELL/HOLD decision
  ├── Updates virtual portfolio (stops, TP, P&L)
  ├── Writes /mnt/e/psxdata/fusion_state.json ← THE OUTPUT
  └── PID file + start/stop like tick_service

strategy_simulator.py (Streamlit page)
  ├── Reads fusion_state.json (same pattern as live_ticker reads live_snapshot.json)
  ├── st_autorefresh every 5 seconds
  ├── Plotly candlestick chart with signal markers (go.Candlestick — already used in 5+ pages)
  ├── Plotly fusion score line chart (signal sub-chart)
  ├── Strategy heatmap (colored st.columns — same as live_ticker top movers)
  ├── Portfolio P&L, positions table, trade blotter
  ├── START/STOP button (launches fusion_service in background)
  └── NO iframe. NO components.v1.html. NO WebSocket from JS. NO ports.

Data flow:
  psxterminal.com → tick_service → live_snapshot.json → fusion_service → fusion_state.json → Streamlit
```

**Zero new ports. Zero iframes. Zero JS WebSocket. Just two JSON files on disk.**

## Prerequisite — Check What Exists

```bash
cd ~/pakfindata && conda activate psx

# 1. Verify tick_service writes live_snapshot.json
ls -la /mnt/e/psxdata/live_snapshot.json
python3 -c "
import json
data = json.load(open('/mnt/e/psxdata/live_snapshot.json'))
print(f'Symbols: {data.get(\"symbol_count\", 0)}')
print(f'Ticks: {data.get(\"tick_count\", 0)}')
# Show one symbol's data structure
syms = data.get('symbols', [])
if syms:
    print(f'Sample: {json.dumps(syms[0], indent=2)[:300]}')
"

# 2. Verify Plotly candlestick already used
grep -c "go.Candlestick" ~/pakfindata/src/pakfindata/ui/page_views/*.py

# 3. Check strategy engine function signatures
for f in vpin_strategy ofi_strategy cvd_strategy basis_strategy macro_regime_hmm sector_rotation oi_strategy pairs_trading sentiment_strategy ml_model hawkes_process; do
    echo "=== $f ==="
    grep "^def \|^class " ~/pakfindata/src/pakfindata/engine/${f}.py 2>/dev/null | head -5
done

# 4. Check existing strategy_fusion.py (if any from v1)
wc -l ~/pakfindata/src/pakfindata/engine/strategy_fusion.py 2>/dev/null || echo "DOES NOT EXIST"
ls ~/pakfindata/src/pakfindata/ui/page_views/strategy_simulator.py 2>/dev/null || echo "DOES NOT EXIST"

# 5. Check live_ticker.py pattern (this is what we're copying)
head -70 ~/pakfindata/src/pakfindata/ui/page_views/live_ticker.py
```

**READ ALL OUTPUT.** Adapt function names in Step 1 based on actual signatures.

## Step 1: Create the Fusion Service

Create `src/pakfindata/services/fusion_service.py`:

This is a standalone background process (like tick_service). It reads prices from 
`live_snapshot.json`, runs strategies, writes `fusion_state.json`.

```python
"""
Strategy Fusion Service — reads live ticks, runs all strategies, writes decisions.

Reads:  /mnt/e/psxdata/live_snapshot.json  (written by tick_service every 2s)
Writes: /mnt/e/psxdata/fusion_state.json   (read by Streamlit every 5s)

Usage:
    python -m pakfindata.services.fusion_service                      # foreground
    python -m pakfindata.services.fusion_service --symbol HUBC        # specific symbol
    python -m pakfindata.services.fusion_service --interval 10        # every 10s
    python -m pakfindata.services.fusion_service --daemon             # background

Start/stop from Streamlit: same pattern as tick_service.
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

logger = logging.getLogger("fusion_service")

PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

# Paths — same directory as live_snapshot.json
LIVE_SNAPSHOT = DATA_ROOT / "live_snapshot.json"
FUSION_STATE = DATA_ROOT / "fusion_state.json"
PID_FILE = DATA_ROOT / "services" / "fusion_service.pid"
LOG_FILE = DATA_ROOT / "services" / "fusion_service.log"

# Default config
DEFAULT_SYMBOL = "OGDC"
DEFAULT_INTERVAL = 10  # seconds
DEFAULT_CAPITAL = 1_000_000


# ═══════════════════════════════════════════════════════
# CANDLE BUILDER — 1-minute from price snapshots
# ═══════════════════════════════════════════════════════

class CandleBuilder:
    """Builds 1-minute candles from periodic price reads."""

    def __init__(self):
        self.candles: list[dict] = []
        self._minute: int = -1
        self._cur: dict | None = None

    def update(self, price: float, volume: int = 0) -> dict | None:
        now = int(time.time())
        minute = now // 60
        completed = None

        if minute != self._minute:
            if self._cur is not None:
                completed = self._cur.copy()
                self.candles.append(completed)
                # Keep last 500 candles
                if len(self.candles) > 500:
                    self.candles = self.candles[-400:]
            self._minute = minute
            self._cur = {"time": minute * 60, "open": price, "high": price,
                         "low": price, "close": price, "volume": volume}
            return completed

        if self._cur:
            self._cur["high"] = max(self._cur["high"], price)
            self._cur["low"] = min(self._cur["low"], price)
            self._cur["close"] = price
            self._cur["volume"] += volume
        return None

    def all_candles(self) -> list[dict]:
        out = list(self.candles)
        if self._cur:
            out.append(self._cur)
        return out


# ═══════════════════════════════════════════════════════
# STRATEGY CALLER — safely calls each engine
# ═══════════════════════════════════════════════════════

STRATEGY_CATALOG = {
    "macro_hmm":       {"wt": 0.15, "cat": "REGIME",    "on": True,  "label": "Macro HMM"},
    "sector_rotation": {"wt": 0.15, "cat": "REGIME",    "on": True,  "label": "Sector Rot"},
    "vpin":            {"wt": 0.10, "cat": "FLOW",      "on": True,  "label": "VPIN"},
    "ofi":             {"wt": 0.08, "cat": "FLOW",      "on": True,  "label": "OFI"},
    "cvd":             {"wt": 0.07, "cat": "FLOW",      "on": False, "label": "CVD"},
    "oi_buildup":      {"wt": 0.05, "cat": "FLOW",      "on": True,  "label": "OI"},
    "basis_arb":       {"wt": 0.10, "cat": "STRUCTURE",  "on": True,  "label": "Basis"},
    "pairs_trading":   {"wt": 0.10, "cat": "STRUCTURE",  "on": False, "label": "Pairs"},
    "ml_predictions":  {"wt": 0.08, "cat": "ALPHA",     "on": False, "label": "ML"},
    "sentiment":       {"wt": 0.07, "cat": "ALPHA",     "on": False, "label": "Sentiment"},
    "hawkes":          {"wt": 0.03, "cat": "RESEARCH",   "on": False, "label": "Hawkes"},
}


def _call(name: str, symbol: str) -> dict:
    """Call one strategy. Returns {direction, confidence, signal}. Never throws."""
    try:
        # ─── ADAPT THESE to match YOUR actual function signatures ───
        # Run the prereq discovery to find exact function names and return types.

        if name == "vpin":
            from pakfindata.engine.vpin_strategy import compute_live_signal
            r = compute_live_signal(symbol)
            if r:
                d = {"BUY": 1, "SELL": -1, "EXIT": -1, "REDUCE": 0, "HOLD": 0}
                return {"direction": d.get(r.signal, 0), "confidence": r.confidence,
                        "signal": f"{r.vpin_state.value} ({r.vpin:.2f})"}

        elif name == "ofi":
            from pakfindata.engine.ofi_strategy import scan_current_ofi
            df = scan_current_ofi([symbol])
            if df is not None and not df.empty:
                row = df.iloc[0]
                d = {"LONG": 1, "SHORT": -1, "FLAT": 0}
                return {"direction": d.get(row.get("signal", ""), 0),
                        "confidence": float(row.get("confidence", 0)),
                        "signal": f"OFI={row.get('ofi', 0):.2f}"}

        elif name == "cvd":
            from pakfindata.engine.cvd_strategy import analyze_cvd
            r = analyze_cvd(symbol)
            if r and r.get("divergences"):
                div = r["divergences"][0]
                d = {"BUY": 1, "SELL": -1}
                return {"direction": d.get(div.get("signal", ""), 0),
                        "confidence": float(div.get("confidence", 0)),
                        "signal": div.get("type", "none")}

        elif name == "basis_arb":
            from pakfindata.engine.basis_strategy import scan_basis_signals
            for r in (scan_basis_signals() or []):
                if r.get("symbol") == symbol:
                    d = {"SELL_BASIS": -1, "BUY_BASIS": 1, "HOLD": 0}
                    return {"direction": d.get(r.get("signal", ""), 0),
                            "confidence": float(r.get("confidence", 0)),
                            "signal": f"z={r.get('basis_zscore', 0):.1f}"}

        elif name == "macro_hmm":
            from pakfindata.engine.macro_regime_hmm import get_current_regime
            r = get_current_regime()
            if r:
                regime = r.get("regime", "TRANSITION")
                d = {"RISK_ON": 1, "TRANSITION": 0, "RISK_OFF": -1, "CRISIS": -1}
                return {"direction": d.get(regime, 0),
                        "confidence": float(r.get("probability", 0.5)),
                        "signal": regime}

        elif name == "sector_rotation":
            from pakfindata.engine.sector_rotation import rank_sectors
            r = rank_sectors()
            if r:
                return {"direction": 0, "confidence": 0.3, "signal": "sector_ctx"}

        elif name == "oi_buildup":
            from pakfindata.engine.oi_strategy import scan_oi_signals
            df = scan_oi_signals([symbol])
            if df is not None and not df.empty:
                row = df.iloc[0]
                d = {"BUY": 1, "SELL": -1}
                return {"direction": d.get(row.get("signal", ""), 0),
                        "confidence": float(row.get("confidence", 0)),
                        "signal": row.get("state", "NEUTRAL")}

        elif name == "pairs_trading":
            from pakfindata.engine.pairs_trading import scan_pair_opportunities
            df = scan_pair_opportunities()
            if df is not None and not df.empty and "symbol_a" in df.columns:
                match = df[df["symbol_a"] == symbol]
                if not match.empty:
                    row = match.iloc[0]
                    return {"direction": 1 if row.get("direction", 0) > 0 else -1,
                            "confidence": min(abs(float(row.get("zscore", 0))) / 3, 1.0),
                            "signal": f"z={row.get('zscore', 0):.1f}"}

        elif name == "sentiment":
            from pakfindata.engine.sentiment_strategy import score_recent_announcements
            for r in (score_recent_announcements() or []):
                if r.get("symbol") == symbol:
                    s = float(r.get("score", 0))
                    return {"direction": 1 if s > 0.2 else (-1 if s < -0.2 else 0),
                            "confidence": abs(s), "signal": f"{s:+.2f}"}

        elif name == "ml_predictions":
            from pakfindata.engine.ml_model import predict_live
            r = predict_live(symbol)
            if r:
                return {"direction": 1 if r.get("direction", 0) > 0 else -1,
                        "confidence": float(r.get("probability", 0.5)),
                        "signal": f"ML {r.get('probability', 0):.0%}"}

        elif name == "hawkes":
            from pakfindata.engine.hawkes_process import analyze_symbol
            r = analyze_symbol(symbol, intensity_resolution=5.0)
            if r and "summary" in r:
                s = r["summary"]
                if s.get("n_bursts", 0) > 0:
                    return {"direction": 0,
                            "confidence": min(float(s.get("max_intensity_ratio", 1)) / 5, 1.0),
                            "signal": f"BURST {s['n_bursts']}x"}
                return {"direction": 0, "confidence": 0, "signal": "CALM"}

    except Exception as e:
        logger.debug("Strategy %s error: %s", name, e)

    return {"direction": 0, "confidence": 0, "signal": "no_data"}


# ═══════════════════════════════════════════════════════
# VIRTUAL PORTFOLIO
# ═══════════════════════════════════════════════════════

class Portfolio:
    def __init__(self, capital: float):
        self.initial = capital
        self.cash = capital
        self.positions: list[dict] = []
        self.closed: list[dict] = []
        self.equity_curve: list[dict] = []
        self.peak = capital

    def equity(self) -> float:
        return self.cash + sum(p["shares"] * p["cur"] for p in self.positions)

    def pnl(self) -> float:
        return self.equity() - self.initial

    def drawdown(self) -> float:
        eq = self.equity()
        self.peak = max(self.peak, eq)
        return (self.peak - eq) / self.peak * 100 if self.peak > 0 else 0

    def win_rate(self) -> float:
        if not self.closed:
            return 0
        return sum(1 for t in self.closed if t["pnl"] > 0) / len(self.closed) * 100

    def open_pos(self, sym, side, price, shares, reason):
        cost = shares * price
        if cost > self.cash or shares <= 0:
            return
        self.cash -= cost
        sl = price * (0.98 if side == "LONG" else 1.02)
        tp = price * (1.04 if side == "LONG" else 0.96)
        self.positions.append({
            "symbol": sym, "side": side, "entry": price, "cur": price,
            "shares": shares, "sl": sl, "tp": tp, "reason": reason,
            "time": datetime.now(PKT).strftime("%H:%M:%S"),
            "pnl": 0, "pnl_pct": 0,
        })

    def close_pos(self, pos, price, reason):
        pnl = ((price - pos["entry"]) if pos["side"] == "LONG" else (pos["entry"] - price)) * pos["shares"]
        self.cash += pos["shares"] * price
        self.closed.append({**pos, "exit": price, "pnl": pnl, "exit_reason": reason,
                            "exit_time": datetime.now(PKT).strftime("%H:%M:%S")})
        self.positions.remove(pos)
        # Keep last 100 closed
        if len(self.closed) > 100:
            self.closed = self.closed[-80:]

    def check_stops(self, sym, price):
        for p in list(self.positions):
            if p["symbol"] != sym:
                continue
            p["cur"] = price
            if p["side"] == "LONG":
                p["pnl"] = (price - p["entry"]) * p["shares"]
                p["pnl_pct"] = (price / p["entry"] - 1) * 100
                if price <= p["sl"]:
                    self.close_pos(p, price, "stop_loss")
                elif price >= p["tp"]:
                    self.close_pos(p, price, "take_profit")
            else:
                p["pnl"] = (p["entry"] - price) * p["shares"]
                p["pnl_pct"] = (1 - price / p["entry"]) * 100
                if price >= p["sl"]:
                    self.close_pos(p, price, "stop_loss")
                elif price <= p["tp"]:
                    self.close_pos(p, price, "take_profit")

    def record(self):
        self.equity_curve.append({
            "time": int(time.time()), "equity": self.equity(),
            "pnl": self.pnl(), "dd": self.drawdown(),
        })
        if len(self.equity_curve) > 2000:
            self.equity_curve = self.equity_curve[-1500:]

    def to_dict(self) -> dict:
        return {
            "capital": self.initial, "cash": round(self.cash),
            "equity": round(self.equity()), "pnl": round(self.pnl()),
            "drawdown": round(self.drawdown(), 1),
            "trades": len(self.closed), "win_rate": round(self.win_rate(), 1),
            "positions": self.positions,
            "closed": self.closed[-30:],
            "equity_curve": self.equity_curve[-300:],
        }


# ═══════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════

class FusionService:
    def __init__(self, symbol: str, interval: int, capital: float, enabled: dict):
        self.symbol = symbol.upper()
        self.interval = interval
        self.enabled = enabled  # {name: bool}
        self.portfolio = Portfolio(capital)
        self.candles = CandleBuilder()
        self.threshold = 0.15
        self.running = False

        # History for signal sub-chart
        self.score_history: list[dict] = []
        self.markers: list[dict] = []

    def read_price(self) -> tuple[float, int]:
        """Read latest price for self.symbol from live_snapshot.json."""
        try:
            data = json.loads(LIVE_SNAPSHOT.read_text())
            for sym in data.get("symbols", []):
                if sym.get("symbol") == self.symbol:
                    return float(sym.get("price", 0)), int(sym.get("volume", 0))
        except (json.JSONDecodeError, IOError, KeyError):
            pass
        return 0, 0

    def run_strategies(self) -> list[dict]:
        votes = []
        for name, cfg in STRATEGY_CATALOG.items():
            on = self.enabled.get(name, cfg["on"])
            if not on:
                votes.append({"name": name, "label": cfg["label"], "cat": cfg["cat"],
                              "wt": cfg["wt"], "direction": 0, "confidence": 0,
                              "signal": "off", "enabled": False})
                continue
            r = _call(name, self.symbol)
            votes.append({"name": name, "label": cfg["label"], "cat": cfg["cat"],
                          "wt": cfg["wt"], "enabled": True, **r})
        return votes

    def fuse(self, votes: list[dict], price: float) -> dict:
        on = [v for v in votes if v["enabled"]]
        tw = sum(v["wt"] for v in on) or 1.0
        raw = sum(v["direction"] * v["confidence"] * v["wt"] for v in on) / tw

        def cat_score(c):
            cv = [v for v in on if v["cat"] == c]
            w = sum(v["wt"] for v in cv) or 1
            return sum(v["direction"] * v["confidence"] * v["wt"] for v in cv) / w

        # VPIN veto
        vpin = next((v for v in votes if v["name"] == "vpin" and v["enabled"]), None)
        vetoed = vpin and "TOXIC" in str(vpin.get("signal", ""))
        veto_reason = "VPIN TOXIC" if vetoed else ""

        if vetoed:
            decision, conf = "HOLD", 0
        elif raw > 0.30:
            decision, conf = "STRONG_BUY", min(abs(raw) * 100, 100)
        elif raw > 0.15:
            decision, conf = "BUY", min(abs(raw) * 100, 100)
        elif raw < -0.30:
            decision, conf = "STRONG_SELL", min(abs(raw) * 100, 100)
        elif raw < -0.15:
            decision, conf = "SELL", min(abs(raw) * 100, 100)
        else:
            decision, conf = "HOLD", 50

        agree = sum(1 for v in on if v["direction"] != 0 and (v["direction"] > 0) == (raw > 0))
        conflict = sum(1 for v in on if v["direction"] != 0 and (v["direction"] > 0) != (raw > 0))

        # Marker for chart
        if decision in ("BUY", "STRONG_BUY", "SELL", "STRONG_SELL"):
            self.markers.append({
                "time": int(time.time()),
                "price": price,
                "decision": decision,
                "confidence": conf,
            })
            if len(self.markers) > 200:
                self.markers = self.markers[-150:]

        return {
            "decision": decision, "raw_score": round(raw, 4),
            "confidence": round(conf, 1), "price": price,
            "regime": round(cat_score("REGIME"), 3),
            "flow": round(cat_score("FLOW"), 3),
            "structure": round(cat_score("STRUCTURE"), 3),
            "alpha": round(cat_score("ALPHA"), 3),
            "agree": agree, "conflict": conflict,
            "vetoed": vetoed, "veto_reason": veto_reason,
        }

    def execute(self, decision: dict, price: float):
        if decision.get("vetoed"):
            return
        d = decision["decision"]
        existing = next((p for p in self.portfolio.positions if p["symbol"] == self.symbol), None)
        score = abs(decision.get("raw_score", 0))
        shares = int((self.portfolio.cash * score * 0.05) / price) if price > 0 else 0

        if d in ("BUY", "STRONG_BUY"):
            if existing and existing["side"] == "SHORT":
                self.portfolio.close_pos(existing, price, "flip")
            if not existing or existing["side"] == "SHORT":
                if len(self.portfolio.positions) < 10:
                    self.portfolio.open_pos(self.symbol, "LONG", price, shares, d)
        elif d in ("SELL", "STRONG_SELL"):
            if existing and existing["side"] == "LONG":
                self.portfolio.close_pos(existing, price, "flip")
            if not existing or existing["side"] == "LONG":
                if len(self.portfolio.positions) < 10:
                    self.portfolio.open_pos(self.symbol, "SHORT", price, shares, d)

    def write_state(self, votes, decision):
        """Write fusion_state.json — read by Streamlit."""
        state = {
            "timestamp": datetime.now(PKT).isoformat(),
            "symbol": self.symbol,
            "running": self.running,
            "interval": self.interval,
            "decision": decision,
            "votes": votes,
            "portfolio": self.portfolio.to_dict(),
            "candles": self.candles.all_candles()[-300:],
            "score_history": self.score_history[-300:],
            "markers": self.markers[-100:],
            "catalog": {k: {"label": v["label"], "cat": v["cat"],
                            "enabled": self.enabled.get(k, v["on"])}
                        for k, v in STRATEGY_CATALOG.items()},
        }
        try:
            tmp = str(FUSION_STATE) + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f, default=str)
            os.replace(tmp, str(FUSION_STATE))  # atomic write
        except OSError as e:
            logger.warning("Write failed: %s", e)

    def tick(self):
        """One cycle: price → candle → strategies → fuse → execute → write."""
        price, volume = self.read_price()
        if price <= 0:
            return

        # Update candle
        self.candles.update(price, volume)

        # Check stops on existing positions
        self.portfolio.check_stops(self.symbol, price)

        # Run strategies
        t0 = time.time()
        votes = self.run_strategies()
        elapsed = time.time() - t0

        # Fuse
        decision = self.fuse(votes, price)

        # Record score history
        self.score_history.append({
            "time": int(time.time()),
            "score": decision["raw_score"],
            "regime": decision["regime"],
            "flow": decision["flow"],
        })
        if len(self.score_history) > 2000:
            self.score_history = self.score_history[-1500:]

        # Execute
        self.execute(decision, price)
        self.portfolio.record()

        # Write state file
        self.write_state(votes, decision)

        # Console status
        pnl = self.portfolio.pnl()
        pnl_str = f"+{pnl:,.0f}" if pnl >= 0 else f"{pnl:,.0f}"
        now = datetime.now(PKT).strftime("%H:%M:%S")
        print(f"  {now} | {self.symbol} {price:,.2f} | {decision['decision']:12s} "
              f"({decision['confidence']:.0f}%) | P&L {pnl_str} | "
              f"{sum(1 for v in votes if v['enabled'])} strats in {elapsed:.1f}s")

    def run(self):
        self.running = True
        print(f"\n{'='*60}")
        print(f"  Fusion Service started")
        print(f"  Symbol:   {self.symbol}")
        print(f"  Interval: {self.interval}s")
        print(f"  Capital:  {self.portfolio.initial:,.0f} PKR")
        print(f"  Enabled:  {sum(1 for k, v in self.enabled.items() if v)}/{len(STRATEGY_CATALOG)}")
        print(f"  Reading:  {LIVE_SNAPSHOT}")
        print(f"  Writing:  {FUSION_STATE}")
        print(f"{'='*60}\n")

        while self.running:
            try:
                self.tick()
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error("Tick error: %s", e)
            time.sleep(self.interval)

        self.running = False
        print("\nFusion Service stopped.")


# ═══════════════════════════════════════════════════════
# START / STOP (same pattern as tick_service)
# ═══════════════════════════════════════════════════════

def _write_pid(pid: int):
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))

def _remove_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()

def is_fusion_running() -> tuple[bool, int | None]:
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, 0)  # check if alive
            return True, pid
        except (ValueError, ProcessLookupError, PermissionError):
            _remove_pid()
    return False, None

def stop_fusion_service() -> tuple[bool, str]:
    running, pid = is_fusion_running()
    if not running:
        return False, "Not running"
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)
        _remove_pid()
        return True, f"Stopped (PID {pid})"
    except Exception as e:
        return False, str(e)

def start_fusion_background(
    symbol: str = DEFAULT_SYMBOL,
    interval: int = DEFAULT_INTERVAL,
    capital: float = DEFAULT_CAPITAL,
) -> tuple[bool, str]:
    running, pid = is_fusion_running()
    if running:
        return False, f"Already running (PID {pid})"

    # Fork a subprocess
    import subprocess
    cmd = [
        sys.executable, "-m", "pakfindata.services.fusion_service",
        "--symbol", symbol,
        "--interval", str(interval),
        "--capital", str(int(capital)),
        "--daemon",
    ]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    time.sleep(1)
    if proc.poll() is None:
        _write_pid(proc.pid)
        return True, f"Started (PID {proc.pid})"
    return False, "Failed to start"


# ═══════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Strategy Fusion Service")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL)
    parser.add_argument("--capital", type=float, default=DEFAULT_CAPITAL)
    parser.add_argument("--daemon", action="store_true")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if args.daemon:
        # Detach
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            filename=str(LOG_FILE), level=logging.INFO,
            format="%(asctime)s %(message)s",
        )

    _write_pid(os.getpid())

    def _shutdown(signum, frame):
        svc.running = False
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    enabled = {k: v["on"] for k, v in STRATEGY_CATALOG.items()}
    svc = FusionService(args.symbol, args.interval, args.capital, enabled)

    try:
        svc.run()
    finally:
        _remove_pid()


if __name__ == "__main__":
    main()
```

## Step 2: Create the Streamlit Page

Create `src/pakfindata/ui/page_views/strategy_simulator.py`:

This follows the **exact same pattern** as `live_ticker.py`: read JSON file → render with Plotly → autorefresh.

```python
"""Strategy Fusion Simulator — reads fusion_state.json, renders with Plotly.

Same pattern as live_ticker.py: file-based, no ports, no iframes.
"""

import json
import os
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

PKT = timezone(timedelta(hours=5))
FUSION_STATE = DATA_ROOT / "fusion_state.json"

_C = {
    "bg": "#0B0E11", "card": "#141820", "border": "#1E2530",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "blue": "#2196F3", "purple": "#BB86FC",
}


def _load() -> dict | None:
    if not FUSION_STATE.exists():
        return None
    for _ in range(2):
        try:
            return json.loads(FUSION_STATE.read_text())
        except (json.JSONDecodeError, IOError):
            _time.sleep(0.3)
    return None


def _age() -> float:
    try:
        return _time.time() - os.path.getmtime(FUSION_STATE)
    except OSError:
        return 999


def render_page():
    age = _age()
    if age < 30 and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=5000, limit=None, key="fusion_sim_refresh")

    # ── HEADER + CONTROLS ──
    h1, h2, h3, h4 = st.columns([3, 1, 1, 0.5])
    with h1:
        st.markdown("## 🎯 Strategy Fusion Simulator")
    with h2:
        _render_service_control()
    with h3:
        pass
    with h4:
        if st.button("🔄", key="fs_refresh"):
            st.rerun()

    data = _load()

    if data is None:
        st.info(
            "Fusion service is not running. Click **Start** above, or run:\n\n"
            "```\npython -m pakfindata.services.fusion_service --symbol OGDC\n```"
        )
        return

    # Status
    running = data.get("running", False)
    symbol = data.get("symbol", "?")
    decision = data.get("decision", {})
    portfolio = data.get("portfolio", {})
    votes = data.get("votes", [])
    candles = data.get("candles", [])
    score_history = data.get("score_history", [])
    markers = data.get("markers", [])

    if running and age < 30:
        status_color, status_text = "#22c55e", "LIVE"
    elif age < 60:
        status_color, status_text = "#f59e0b", "STALE"
    else:
        status_color, status_text = "#ef4444", "DOWN"

    # ── STATUS BAR ──
    s1, s2, s3, s4, s5, s6, s7 = st.columns(7)
    s1.markdown(f'<span style="color:{status_color};font-weight:bold;font-size:14px">'
                f'● {status_text}</span>', unsafe_allow_html=True)
    s2.metric("Symbol", symbol)
    s3.metric("Price", f"{decision.get('price', 0):,.2f}")

    dec = decision.get("decision", "HOLD")
    dec_color = "#22c55e" if "BUY" in dec else "#ef4444" if "SELL" in dec else "#6b7280"
    s4.markdown(f'<div style="text-align:center"><span style="color:{_C["dim"]};font-size:10px">DECISION</span>'
                f'<br><span style="color:{dec_color};font-weight:900;font-size:18px">{dec}</span></div>',
                unsafe_allow_html=True)
    s5.metric("Confidence", f"{decision.get('confidence', 0):.0f}%")

    pnl = portfolio.get("pnl", 0)
    pnl_color = "#22c55e" if pnl >= 0 else "#ef4444"
    s6.markdown(f'<div style="text-align:center"><span style="color:{_C["dim"]};font-size:10px">P&L</span>'
                f'<br><span style="color:{pnl_color};font-weight:700;font-size:16px">'
                f'{"+" if pnl >= 0 else ""}{pnl:,.0f}</span></div>',
                unsafe_allow_html=True)
    s7.metric("Trades", portfolio.get("trades", 0))

    if decision.get("vetoed"):
        st.error(f"⚠️ VETOED: {decision.get('veto_reason', '')}")

    # ── CANDLESTICK CHART + SIGNAL SUB-CHART ──
    if candles and len(candles) > 2:
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            row_heights=[0.55, 0.20, 0.25],
            vertical_spacing=0.03,
        )

        # Row 1: Candlestick
        times = [datetime.fromtimestamp(c["time"], PKT) for c in candles]
        fig.add_trace(go.Candlestick(
            x=times,
            open=[c["open"] for c in candles],
            high=[c["high"] for c in candles],
            low=[c["low"] for c in candles],
            close=[c["close"] for c in candles],
            increasing_line_color=_C["up"], decreasing_line_color=_C["down"],
            increasing_fillcolor=_C["up"], decreasing_fillcolor=_C["down"],
            name="Price",
        ), row=1, col=1)

        # Signal markers on candlestick chart
        buy_markers = [m for m in markers if "BUY" in m.get("decision", "")]
        sell_markers = [m for m in markers if "SELL" in m.get("decision", "")]

        if buy_markers:
            fig.add_trace(go.Scatter(
                x=[datetime.fromtimestamp(m["time"], PKT) for m in buy_markers],
                y=[m["price"] * 0.998 for m in buy_markers],
                mode="markers", name="BUY",
                marker=dict(symbol="triangle-up", size=12, color=_C["up"]),
                hovertext=[f"{m['decision']} ({m['confidence']:.0f}%)" for m in buy_markers],
            ), row=1, col=1)
        if sell_markers:
            fig.add_trace(go.Scatter(
                x=[datetime.fromtimestamp(m["time"], PKT) for m in sell_markers],
                y=[m["price"] * 1.002 for m in sell_markers],
                mode="markers", name="SELL",
                marker=dict(symbol="triangle-down", size=12, color=_C["down"]),
                hovertext=[f"{m['decision']} ({m['confidence']:.0f}%)" for m in sell_markers],
            ), row=1, col=1)

        # Row 2: Volume
        fig.add_trace(go.Bar(
            x=times,
            y=[c.get("volume", 0) for c in candles],
            marker_color=[_C["up"] if c["close"] >= c["open"] else _C["down"] for c in candles],
            opacity=0.4, name="Volume",
        ), row=2, col=1)

        # Row 3: Fusion score
        if score_history:
            sh_times = [datetime.fromtimestamp(s["time"], PKT) for s in score_history]
            sh_scores = [s["score"] * 100 for s in score_history]
            fig.add_trace(go.Scatter(
                x=sh_times, y=sh_scores,
                mode="lines", name="Fusion Score",
                line=dict(color=_C["cyan"], width=1.5),
                fill="tozeroy", fillcolor="rgba(0,188,212,0.1)",
            ), row=3, col=1)
            fig.add_hline(y=15, line_dash="dot", line_color=_C["dim"],
                          annotation_text="BUY", row=3, col=1)
            fig.add_hline(y=-15, line_dash="dot", line_color=_C["dim"],
                          annotation_text="SELL", row=3, col=1)
            fig.add_hline(y=0, line_dash="solid", line_color="#333", row=3, col=1)

        fig.update_layout(
            paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
            font_color=_C["dim"], height=550,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False, xaxis_rangeslider_visible=False,
        )
        for ax in ["yaxis", "yaxis2", "yaxis3"]:
            fig.update_layout(**{ax: dict(gridcolor=_C["border"])})
        for ax in ["xaxis", "xaxis2", "xaxis3"]:
            fig.update_layout(**{ax: dict(gridcolor=_C["border"])})

        fig.update_yaxes(title_text=symbol, row=1, col=1)
        fig.update_yaxes(title_text="Vol", row=2, col=1)
        fig.update_yaxes(title_text="Score", row=3, col=1)

        st.plotly_chart(fig, use_container_width=True, key="fusion_chart")

    st.divider()

    # ── STRATEGY HEATMAP + CATEGORY SCORES ──
    left, right = st.columns([3, 2])

    with left:
        st.markdown("**Strategy Signals**")
        # Render as colored cards in a grid — same pattern as live_ticker top movers
        cols = st.columns(4)
        for i, v in enumerate(votes):
            with cols[i % 4]:
                if not v.get("enabled"):
                    bg, border_c = "#0B0E11", "#1E2530"
                    text_c = "#374151"
                elif v.get("direction", 0) > 0:
                    bg, border_c = "rgba(0,230,118,0.15)", "rgba(0,230,118,0.4)"
                    text_c = _C["up"]
                elif v.get("direction", 0) < 0:
                    bg, border_c = "rgba(255,82,82,0.15)", "rgba(255,82,82,0.4)"
                    text_c = _C["down"]
                else:
                    bg, border_c = "rgba(107,114,128,0.1)", _C["border"]
                    text_c = _C["dim"]

                conf = v.get("confidence", 0)
                st.markdown(
                    f'<div style="background:{bg};border:1px solid {border_c};'
                    f'border-radius:4px;padding:6px;margin-bottom:4px;text-align:center;">'
                    f'<div style="color:{text_c};font-weight:700;font-size:10px;">'
                    f'{v.get("label", v.get("name", "?"))}</div>'
                    f'<div style="color:{_C["dim"]};font-size:9px;">'
                    f'{str(v.get("signal", ""))[:18]}</div>'
                    f'<div style="color:{text_c};font-size:9px;">'
                    f'{conf:.0%}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    with right:
        st.markdown("**Category Scores**")
        for label, key, color in [
            ("REGIME", "regime", _C["blue"]),
            ("FLOW", "flow", _C["cyan"]),
            ("STRUCTURE", "structure", "#C8A96E"),
            ("ALPHA", "alpha", _C["purple"]),
        ]:
            score = decision.get(key, 0)
            pct = max(0, min(100, (score + 1) / 2 * 100))
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
                f'<span style="width:65px;text-align:right;color:{_C["dim"]};font-size:10px;">{label}</span>'
                f'<div style="flex:1;height:10px;background:{_C["border"]};border-radius:5px;overflow:hidden;">'
                f'<div style="height:100%;width:{pct:.0f}%;background:{color};border-radius:5px;'
                f'transition:width 0.3s;"></div></div>'
                f'<span style="width:35px;font-size:10px;">{score * 100:.0f}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown("**Portfolio**")
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("Equity", f"{portfolio.get('equity', 0):,.0f}")
        pc2.metric("Win Rate", f"{portfolio.get('win_rate', 0):.0f}%")
        pc3.metric("Drawdown", f"{portfolio.get('drawdown', 0):.1f}%")
        pc4.metric("Positions", len(portfolio.get("positions", [])))

    st.divider()

    # ── POSITIONS + TRADE LOG ──
    p1, p2 = st.columns(2)

    with p1:
        st.markdown("**Open Positions**")
        positions = portfolio.get("positions", [])
        if positions:
            for pos in positions:
                pnl_c = _C["up"] if pos.get("pnl", 0) >= 0 else _C["down"]
                side_c = _C["up"] if pos.get("side") == "LONG" else _C["down"]
                st.markdown(
                    f'<div style="display:flex;gap:12px;padding:3px 0;border-bottom:1px solid {_C["border"]};">'
                    f'<b>{pos.get("symbol")}</b>'
                    f'<span style="color:{side_c}">{pos.get("side")}</span>'
                    f'<span>Entry: {pos.get("entry", 0):.2f}</span>'
                    f'<span>Now: {pos.get("cur", 0):.2f}</span>'
                    f'<span style="color:{pnl_c}">{"+" if pos.get("pnl",0)>=0 else ""}{pos.get("pnl",0):,.0f}</span>'
                    f'<span style="color:{pnl_c}">{pos.get("pnl_pct",0):+.2f}%</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No open positions")

    with p2:
        st.markdown("**Recent Trades**")
        closed = portfolio.get("closed", [])
        if closed:
            for t in reversed(closed[-10:]):
                pnl_c = _C["up"] if t.get("pnl", 0) >= 0 else _C["down"]
                st.markdown(
                    f'<div style="font-size:10px;padding:2px 0;border-bottom:1px solid {_C["border"]};">'
                    f'<span style="color:{_C["dim"]}">{t.get("exit_time", "")}</span> '
                    f'{t.get("side")} {t.get("symbol")} ×{t.get("shares")} '
                    f'<span style="color:{pnl_c}">{"+" if t.get("pnl",0)>=0 else ""}{t.get("pnl",0):,.0f}</span> '
                    f'({t.get("exit_reason", "")})'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No trades yet")


# ── SERVICE CONTROL (START / STOP) ──

def _render_service_control():
    try:
        from pakfindata.services.fusion_service import (
            is_fusion_running, start_fusion_background, stop_fusion_service,
        )
    except ImportError:
        st.caption("fusion_service not available")
        return

    running, pid = is_fusion_running()

    if running:
        if st.button(f"⏹ Stop (PID {pid})", key="fs_stop", type="primary"):
            ok, msg = stop_fusion_service()
            st.success(msg) if ok else st.error(msg)
            st.rerun()
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            sym = st.text_input("Symbol", "OGDC", key="fs_sym", label_visibility="collapsed")
        with c2:
            interval = st.selectbox("Interval", [5, 10, 15, 30], index=1, key="fs_int",
                                    label_visibility="collapsed")
        with c3:
            if st.button("▶ Start", key="fs_start", type="primary"):
                ok, msg = start_fusion_background(symbol=sym, interval=interval)
                st.success(msg) if ok else st.error(msg)
                st.rerun()
```

## Step 3: Register in app.py

Add page function:
```python
def strategy_simulator_page():
    from pakfindata.ui.page_views.strategy_simulator import render_page
    render_page()
```

Add to page dict:
```python
        # SIMULATOR
        "Strategy Simulator": st.Page(strategy_simulator_page, title="Strategy Simulator", url_path="simulator"),
```

Add to nav_groups (FIRST section):
```python
    nav_groups = {
        "SIMULATOR":       ["Strategy Simulator"],
        "MARKET OVERVIEW": ["Dashboard", "Market Pulse", "Index Monitor"],
        ...
    }
```

## Step 4: Test

```bash
cd ~/pakfindata && conda activate psx

# 1. Ensure tick_service is running (writes live_snapshot.json)
python -m pakfindata.services.tick_service &
sleep 5
ls -la /mnt/e/psxdata/live_snapshot.json

# 2. Start fusion_service
python -m pakfindata.services.fusion_service --symbol OGDC --interval 10

# You should see output like:
#   15:30:01 | OGDC 279.50 | BUY          (42%) | P&L +0 | 6 strats in 2.1s
#   15:30:11 | OGDC 279.75 | BUY          (45%) | P&L +125 | 6 strats in 1.9s

# 3. In another terminal, check fusion_state.json
python3 -c "
import json
data = json.load(open('/mnt/e/psxdata/fusion_state.json'))
print(f'Symbol: {data[\"symbol\"]}')
print(f'Decision: {data[\"decision\"][\"decision\"]}')
print(f'Confidence: {data[\"decision\"][\"confidence\"]}')
print(f'Price: {data[\"decision\"][\"price\"]}')
print(f'Candles: {len(data[\"candles\"])}')
print(f'Votes:')
for v in data['votes']:
    status = '✓' if v['enabled'] else '✗'
    dir_str = {1: '▲', -1: '▼', 0: '—'}[v['direction']]
    print(f'  {status} {v[\"label\"]:12s} {dir_str} {v[\"confidence\"]:.0%} → {v[\"signal\"]}')
"

# 4. Start Streamlit
streamlit run src/pakfindata/ui/app.py --server.port 8501
# Navigate to Strategy Simulator — should show candlestick chart + signals

# 5. Background mode
python -m pakfindata.services.fusion_service --symbol HUBC --interval 15 --daemon
```

## IMPORTANT NOTES

1. **ZERO ports from browser.** No WebSocket from JS. No iframe to another port. No components.v1.html. Just Streamlit reading a JSON file.
2. **Follows live_ticker.py pattern EXACTLY.** tick_service writes JSON → Streamlit reads it. Proven, works everywhere.
3. **fusion_service.py is a standalone process** with PID file, start/stop, daemon mode — same as tick_service.
4. **Plotly candlestick** — `go.Candlestick` already used in 5+ pages in the app. No new rendering tech.
5. **Signal markers on the chart** — BUY ▲ green triangles below candles, SELL ▼ red triangles above candles. Shows WHERE on the price chart each decision was made.
6. **Fusion score sub-chart** — line chart showing the composite score (-100 to +100) over time, with BUY/SELL threshold lines. Shows how the score TRENDS.
7. **Strategy heatmap** — colored cards in a 4-column grid. Green = LONG, Red = SHORT, Gray = neutral, Dark = disabled. Same visual pattern as live_ticker top movers.
8. **st_autorefresh at 5 seconds** — Streamlit reruns every 5s and re-reads the JSON. Not 60fps smooth, but functional and reliable.
9. **Atomic writes** — fusion_state.json is written to .tmp then os.replace() to prevent reading partial data.
10. **Strategy adapters** — the `_call()` function must match YOUR actual engine function signatures. Run the prereq discovery step and adapt.
11. **Portfolio auto-managed** — 2% stop loss, 4% take profit, max 10 positions, checked on every tick.
12. **Add to SIMULATOR section** at the TOP of sidebar nav.
13. **Delete old files** — remove any Flask-based `strategy_simulator.py`, `simulator_panel.html` (v1/v2 approach). Clean start.
14. **The standalone TradingView page** (from v2 prompt) can still be added to ws_relay later as a BONUS for power users who want 60fps. But the PRIMARY experience is this Streamlit page — works everywhere, no port issues.
