"""
Strategy Fusion Service -- reads live ticks, runs all strategies, writes decisions.

Reads:  /mnt/e/psxdata/live_snapshot.json  (written by tick_service every 2s)
Writes: /mnt/e/psxdata/fusion_state.json   (read by Streamlit every 5s)

Usage:
    python -m pakfindata.services.fusion_service
    python -m pakfindata.services.fusion_service --symbol HUBC
    python -m pakfindata.services.fusion_service --interval 10
    python -m pakfindata.services.fusion_service --daemon
"""

import argparse
import json
import logging
import os
import signal as _signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

logger = logging.getLogger("fusion_service")

PKT = timezone(timedelta(hours=5))
DATA_ROOT = Path("/mnt/e/psxdata")

LIVE_SNAPSHOT = DATA_ROOT / "live_snapshot.json"
FUSION_STATE = DATA_ROOT / "fusion_state.json"
PID_FILE = DATA_ROOT / "services" / "fusion_service.pid"
LOG_FILE = DATA_ROOT / "services" / "fusion_service.log"

DEFAULT_SYMBOL = "OGDC"
DEFAULT_INTERVAL = 10
DEFAULT_CAPITAL = 1_000_000


# ═══════════════════════════════════════════════════════
# CANDLE BUILDER
# ═══════════════════════════════════════════════════════

class CandleBuilder:
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
# STRATEGY CATALOG & CALLER
# ═══════════════════════════════════════════════════════

STRATEGY_CATALOG = {
    "macro_hmm":       {"wt": 0.15, "cat": "REGIME",    "on": True,  "label": "Macro HMM"},
    "sector_rotation": {"wt": 0.15, "cat": "REGIME",    "on": True,  "label": "Sector Rot"},
    "vpin":            {"wt": 0.10, "cat": "FLOW",      "on": True,  "label": "VPIN"},
    "ofi":             {"wt": 0.08, "cat": "FLOW",      "on": True,  "label": "OFI"},
    "cvd":             {"wt": 0.07, "cat": "FLOW",      "on": False, "label": "CVD"},
    "oi_buildup":      {"wt": 0.05, "cat": "FLOW",      "on": True,  "label": "OI"},
    "basis_arb":       {"wt": 0.10, "cat": "STRUCTURE", "on": True,  "label": "Basis"},
    "pairs_trading":   {"wt": 0.10, "cat": "STRUCTURE", "on": False, "label": "Pairs"},
    "ml_predictions":  {"wt": 0.08, "cat": "ALPHA",     "on": False, "label": "ML"},
    "sentiment":       {"wt": 0.07, "cat": "ALPHA",     "on": False, "label": "Sentiment"},
    "hawkes":          {"wt": 0.03, "cat": "RESEARCH",  "on": False, "label": "Hawkes"},
    "lead_lag":        {"wt": 0.08, "cat": "INTELLIGENCE", "on": True,  "label": "Lead-Lag"},
    "corr_breakout":   {"wt": 0.07, "cat": "INTELLIGENCE", "on": True,  "label": "CorrBreak"},
    "contagion":       {"wt": 0.05, "cat": "INTELLIGENCE", "on": True,  "label": "Contagion"},
}


def _call(name: str, symbol: str) -> dict:
    """Call one strategy engine. Never throws."""
    try:
        if name == "vpin":
            from pakfindata.engine.vpin_strategy import compute_live_signal
            r = compute_live_signal(symbol)
            if r:
                d = {"BUY": 1, "SELL": -1, "EXIT": -1, "REDUCE": 0, "HOLD": 0}
                return {"direction": d.get(r.signal, 0), "confidence": r.confidence,
                        "signal": str(getattr(r, 'vpin_state', '')) + f" ({r.vpin:.2f})"}

        elif name == "ofi":
            from pakfindata.engine.ofi_strategy import scan_current_ofi
            df = scan_current_ofi([symbol])
            if df is not None and not df.empty:
                row = df.iloc[0]
                d = {"LONG": 1, "SHORT": -1, "FLAT": 0}
                return {"direction": d.get(row.get("signal", ""), 0),
                        "confidence": float(row.get("confidence", 0)),
                        "signal": "OFI=" + str(round(row.get("ofi", 0), 2))}

        elif name == "cvd":
            from pakfindata.engine.cvd_strategy import scan_divergences
            for r in (scan_divergences(top_n=50) or []):
                if r.get("symbol") == symbol:
                    d = {"BUY": 1, "SELL": -1}
                    return {"direction": d.get(r.get("signal", ""), 0),
                            "confidence": float(r.get("confidence", 0)),
                            "signal": r.get("signal", "none")}

        elif name == "basis_arb":
            from pakfindata.engine.basis_strategy import scan_basis_signals
            for r in (scan_basis_signals() or []):
                if symbol in str(r.get("symbol", "")):
                    sig = r.get("signal", "HOLD")
                    d = -1 if "SELL" in sig else (1 if "BUY" in sig else 0)
                    return {"direction": d, "confidence": float(r.get("confidence", 0)),
                            "signal": "z=" + str(round(r.get("basis_zscore", 0), 1))}

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
                match = df[(df["symbol_a"] == symbol) | (df["symbol_b"] == symbol)]
                if not match.empty:
                    row = match.iloc[0]
                    return {"direction": 1 if "LONG" in str(row.get("direction", "")) else -1,
                            "confidence": min(abs(float(row.get("zscore", 0))) / 3, 1.0),
                            "signal": "z=" + str(round(row.get("zscore", 0), 1))}

        elif name == "sentiment":
            from pakfindata.engine.sentiment_strategy import score_recent_announcements
            for r in (score_recent_announcements(limit=20) or []):
                sym = getattr(r, "symbol", None) or r.get("symbol", "") if isinstance(r, dict) else r.symbol
                if sym == symbol:
                    score = getattr(r, "sentiment_score", 0) if not isinstance(r, dict) else r.get("score", 0)
                    return {"direction": 1 if score > 0.2 else (-1 if score < -0.2 else 0),
                            "confidence": abs(score),
                            "signal": f"{score:+.2f}"}

        elif name == "ml_predictions":
            from pakfindata.engine.ml_features import get_eod_features
            df = get_eod_features(symbol, lookback_days=100)
            if df is not None and not df.empty and "target_direction" in df.columns:
                last_dir = df["target_direction"].iloc[-1]
                return {"direction": 1 if last_dir > 0 else -1,
                        "confidence": 0.55, "signal": "ML " + ("UP" if last_dir > 0 else "DOWN")}

        elif name == "hawkes":
            from pakfindata.engine.hawkes_process import analyze_symbol
            r = analyze_symbol(symbol, intensity_resolution=10.0, fast=True)
            if r and "summary" in r:
                s = r["summary"]
                if s.get("n_bursts", 0) > 0:
                    return {"direction": 0,
                            "confidence": min(float(s.get("max_intensity_ratio", 1)) / 5, 1.0),
                            "signal": f"BURST {s['n_bursts']}x"}
                return {"direction": 0, "confidence": 0, "signal": "CALM"}

        elif name == "lead_lag":
            from pakfindata.engine.lead_lag_detector import scan_lead_lag
            signals = scan_lead_lag(symbols=[symbol], top_n=5)
            if signals:
                for s in signals:
                    if s.follower == symbol and s.confidence > 0.3:
                        return {"direction": s.direction, "confidence": s.confidence,
                                "signal": f"follows {s.leader} ({s.lag_minutes}min lag)"}
            return {"direction": 0, "confidence": 0, "signal": "no_lead_lag"}

        elif name == "corr_breakout":
            from pakfindata.engine.correlation_breakout import compute_correlation_regime
            alerts = compute_correlation_regime()
            for a in alerts:
                if symbol in a.cluster:
                    d = 1 if a.direction == "CONVERGING" else -1
                    return {"direction": d, "confidence": a.confidence,
                            "signal": f"{a.direction} {a.sigma:.1f}s ({len(a.cluster)} syms)"}
            return {"direction": 0, "confidence": 0, "signal": "normal_corr"}

        elif name == "contagion":
            from pakfindata.engine.announcement_contagion import scan_contagion
            signals = scan_contagion(days_back=3)
            for s in signals:
                for peer in s.affected_peers:
                    if peer["symbol"] == symbol:
                        return {"direction": peer["direction"],
                                "confidence": peer["confidence"],
                                "signal": f"{s.announcement_type} at {s.source_symbol}"}
                if s.source_symbol == symbol:
                    d = 1 if s.sentiment > 0 else -1
                    return {"direction": d, "confidence": abs(s.sentiment),
                            "signal": f"own {s.announcement_type}"}
            return {"direction": 0, "confidence": 0, "signal": "no_contagion"}

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

    def equity(self):
        return self.cash + sum(p["shares"] * p["cur"] for p in self.positions)

    def pnl(self):
        return self.equity() - self.initial

    def drawdown(self):
        eq = self.equity()
        self.peak = max(self.peak, eq)
        return (self.peak - eq) / self.peak * 100 if self.peak > 0 else 0

    def win_rate(self):
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
            "time": datetime.now(PKT).strftime("%H:%M:%S"), "pnl": 0, "pnl_pct": 0,
        })

    def close_pos(self, pos, price, reason):
        pnl = ((price - pos["entry"]) if pos["side"] == "LONG" else (pos["entry"] - price)) * pos["shares"]
        self.cash += pos["shares"] * price
        self.closed.append({**pos, "exit": price, "pnl": pnl, "exit_reason": reason,
                            "exit_time": datetime.now(PKT).strftime("%H:%M:%S")})
        self.positions.remove(pos)
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

    def to_dict(self):
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
# REPLAY ENGINE
# ═══════════════════════════════════════════════════════

class ReplayEngine:
    """Replays historical ohlcv_5s bars when live data is unavailable."""

    def __init__(self, symbol: str, mode: str = "auto"):
        self.symbol = symbol
        self.mode = mode
        self._replay_bars: list[dict] = []
        self._replay_idx: int = 0
        self._replay_date: str = ""
        self._replay_loaded = False

    def _load_replay_data(self):
        try:
            from pakfindata.db.connections import analytics_con
            con = analytics_con()
            row = con.execute(
                "SELECT MAX(SUBSTR(ts,1,10)) FROM ohlcv_5s WHERE symbol=?",
                [self.symbol]).fetchone()
            if not row or not row[0]:
                con.close()
                return
            self._replay_date = row[0]
            df = con.execute(
                "SELECT ts, o, h, l, c, v FROM ohlcv_5s WHERE symbol=? AND SUBSTR(ts,1,10)=? ORDER BY ts",
                [self.symbol, self._replay_date]).df()
            con.close()
            if df.empty:
                return
            self._replay_bars = df.to_dict("records")
            self._replay_idx = 0
            self._replay_loaded = True
            logger.info("Replay loaded: %s %s — %d bars", self.symbol, self._replay_date, len(self._replay_bars))
        except Exception as e:
            logger.warning("Replay load failed: %s", e)

    def _live_price(self):
        try:
            age = time.time() - os.path.getmtime(LIVE_SNAPSHOT)
            if age > 30:
                return 0, 0, False
            data = json.loads(LIVE_SNAPSHOT.read_text())
            for sym in data.get("symbols", []):
                if sym.get("symbol") == self.symbol:
                    return float(sym.get("price", 0)), int(sym.get("volume", 0)), True
        except (json.JSONDecodeError, IOError, OSError):
            pass
        return 0, 0, False

    def next_price(self):
        """Returns (price, volume, source). source: LIVE, REPLAY, NONE."""
        if self.mode in ("live", "auto"):
            price, vol, fresh = self._live_price()
            if fresh and price > 0:
                return price, vol, "LIVE"

        if self.mode in ("replay", "auto"):
            if not self._replay_loaded:
                self._load_replay_data()
            if self._replay_bars:
                bar = self._replay_bars[self._replay_idx]
                self._replay_idx = (self._replay_idx + 1) % len(self._replay_bars)
                if self._replay_idx == 0:
                    logger.info("Replay wrapped — restarting %s %s", self.symbol, self._replay_date)
                return float(bar["c"]), int(bar.get("v", 0)), "REPLAY"

        return 0, 0, "NONE"

    @property
    def replay_info(self):
        return {
            "mode": self.mode,
            "replay_date": self._replay_date,
            "replay_bars": len(self._replay_bars),
            "replay_idx": self._replay_idx,
            "replay_progress": round(self._replay_idx / max(len(self._replay_bars), 1) * 100, 1),
        }


# ═══════════════════════════════════════════════════════
# MAIN SERVICE
# ═══════════════════════════════════════════════════════

class FusionService:
    def __init__(self, symbol, interval, capital, enabled, mode="auto"):
        self.symbol = symbol.upper()
        self.interval = interval
        self.enabled = enabled
        self.portfolio = Portfolio(capital)
        self.candles = CandleBuilder()
        self.running = False
        self.mode = mode
        self.price_source = ReplayEngine(self.symbol, mode=mode)
        self.score_history: list[dict] = []
        self.markers: list[dict] = []

    def run_strategies(self):
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

    def fuse(self, votes, price):
        on = [v for v in votes if v["enabled"]]
        tw = sum(v["wt"] for v in on) or 1.0
        raw = sum(v["direction"] * v["confidence"] * v["wt"] for v in on) / tw

        def cat_score(c):
            cv = [v for v in on if v["cat"] == c]
            w = sum(v["wt"] for v in cv) or 1
            return sum(v["direction"] * v["confidence"] * v["wt"] for v in cv) / w

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

        if decision in ("BUY", "STRONG_BUY", "SELL", "STRONG_SELL"):
            self.markers.append({"time": int(time.time()), "price": price,
                                 "decision": decision, "confidence": conf})
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

    def execute(self, decision, price):
        if decision.get("vetoed"):
            return
        d = decision["decision"]
        existing = next((p for p in self.portfolio.positions if p["symbol"] == self.symbol), None)
        score = abs(decision.get("raw_score", 0))
        shares = int((self.portfolio.cash * score * 0.05) / price) if price > 0 else 0

        if d in ("BUY", "STRONG_BUY"):
            if existing and existing["side"] == "SHORT":
                self.portfolio.close_pos(existing, price, "flip")
            if (not existing or existing["side"] == "SHORT") and len(self.portfolio.positions) < 10:
                self.portfolio.open_pos(self.symbol, "LONG", price, shares, d)
        elif d in ("SELL", "STRONG_SELL"):
            if existing and existing["side"] == "LONG":
                self.portfolio.close_pos(existing, price, "flip")
            if (not existing or existing["side"] == "LONG") and len(self.portfolio.positions) < 10:
                self.portfolio.open_pos(self.symbol, "SHORT", price, shares, d)

    def write_state(self, votes, decision, source="LIVE"):
        state = {
            "timestamp": datetime.now(PKT).isoformat(),
            "symbol": self.symbol,
            "running": self.running,
            "interval": self.interval,
            "source": source,
            "replay": self.price_source.replay_info,
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
            os.replace(tmp, str(FUSION_STATE))
        except OSError as e:
            logger.warning("Write failed: %s", e)

    def tick(self):
        price, volume, source = self.price_source.next_price()
        if price <= 0:
            return
        self.candles.update(price, volume)
        self.portfolio.check_stops(self.symbol, price)

        t0 = time.time()
        votes = self.run_strategies()
        elapsed = time.time() - t0

        decision = self.fuse(votes, price)

        self.score_history.append({
            "time": int(time.time()), "score": decision["raw_score"],
            "regime": decision["regime"], "flow": decision["flow"],
        })
        if len(self.score_history) > 2000:
            self.score_history = self.score_history[-1500:]

        self.execute(decision, price)
        self.portfolio.record()
        self.write_state(votes, decision, source)

        pnl = self.portfolio.pnl()
        pnl_str = f"+{pnl:,.0f}" if pnl >= 0 else f"{pnl:,.0f}"
        now = datetime.now(PKT).strftime("%H:%M:%S")
        src_tag = f" [{source}]" if source != "LIVE" else ""
        print(f"  {now} | {self.symbol} {price:,.2f}{src_tag} | {decision['decision']:12s} "
              f"({decision['confidence']:.0f}%) | P&L {pnl_str} | "
              f"{sum(1 for v in votes if v['enabled'])} strats in {elapsed:.1f}s")

    def run(self):
        self.running = True
        print(f"\n{'='*60}")
        print(f"  Fusion Service started")
        print(f"  Symbol:   {self.symbol}")
        print(f"  Interval: {self.interval}s")
        print(f"  Capital:  {self.portfolio.initial:,.0f} PKR")
        print(f"  Enabled:  {sum(1 for v in self.enabled.values() if v)}/{len(STRATEGY_CATALOG)}")
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
# START / STOP
# ═══════════════════════════════════════════════════════

def _write_pid(pid):
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))

def _remove_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()

def is_fusion_running():
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, 0)
            return True, pid
        except (ValueError, ProcessLookupError, PermissionError):
            _remove_pid()
    return False, None

def stop_fusion_service():
    running, pid = is_fusion_running()
    if not running:
        return False, "Not running"
    try:
        os.kill(pid, _signal.SIGTERM)
        time.sleep(1)
        _remove_pid()
        return True, f"Stopped (PID {pid})"
    except Exception as e:
        return False, str(e)

def start_fusion_background(symbol=DEFAULT_SYMBOL, interval=DEFAULT_INTERVAL,
                            capital=DEFAULT_CAPITAL, mode="auto"):
    running, pid = is_fusion_running()
    if running:
        return False, f"Already running (PID {pid})"
    import subprocess
    cmd = [sys.executable, "-m", "pakfindata.services.fusion_service",
           "--symbol", symbol, "--interval", str(interval),
           "--capital", str(int(capital)), "--mode", mode, "--daemon"]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)
    time.sleep(1)
    if proc.poll() is None:
        _write_pid(proc.pid)
        return True, f"Started (PID {proc.pid}) mode={mode}"
    return False, "Failed to start"


def main():
    parser = argparse.ArgumentParser(description="Strategy Fusion Service")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL)
    parser.add_argument("--capital", type=float, default=DEFAULT_CAPITAL)
    parser.add_argument("--mode", choices=["live", "replay", "auto"], default="auto",
                        help="Price source: live, replay (DuckDB history), auto (try live, fallback replay)")
    parser.add_argument("--daemon", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    if args.daemon:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(filename=str(LOG_FILE), level=logging.INFO, format="%(asctime)s %(message)s")

    _write_pid(os.getpid())

    def _shutdown(signum, frame):
        svc.running = False
    _signal.signal(_signal.SIGTERM, _shutdown)
    _signal.signal(_signal.SIGINT, _shutdown)

    enabled = {k: v["on"] for k, v in STRATEGY_CATALOG.items()}
    svc = FusionService(args.symbol, args.interval, args.capital, enabled, mode=args.mode)

    try:
        svc.run()
    finally:
        _remove_pid()


if __name__ == "__main__":
    main()
