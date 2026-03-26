"""
Multi-Symbol Portfolio Simulator v2 — fast scanning with cached regime signals.

Key design: regime strategies (Macro HMM, Sector Rotation) are computed ONCE per tick,
not per symbol. Only fast per-symbol strategies (OFI, OI, VPIN) run per symbol.

Reads:  /mnt/e/psxdata/live_snapshot.json or DuckDB tick_logs
Writes: /mnt/e/psxdata/portfolio_state.json (read by Streamlit)

Usage:
    python -m pakfindata.services.portfolio_simulator
    python -m pakfindata.services.portfolio_simulator --top 20 --interval 30
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

logger = logging.getLogger("portfolio_sim")

PKT = timezone(timedelta(hours=5))
DATA_ROOT = Path("/mnt/e/psxdata")

LIVE_SNAPSHOT = DATA_ROOT / "live_snapshot.json"
PORTFOLIO_STATE = DATA_ROOT / "portfolio_state.json"
PID_FILE = DATA_ROOT / "services" / "portfolio_sim.pid"

DEFAULT_INTERVAL = 30
DEFAULT_CAPITAL = 5_000_000
DEFAULT_TOP_N = 20
DEFAULT_MAX_POSITIONS = 8

try:
    from pakfindata.engine.execution_model import simulate_execution
except ImportError:
    simulate_execution = None


# ═══════════════════════════════════════════════════════
# FAST STRATEGY CALLS — cached regime, per-symbol flow
# ═══════════════════════════════════════════════════════

_regime_cache = {"ts": 0, "macro": None, "sector": None}


def _call_regime_once():
    """Compute regime strategies ONCE per tick (they don't vary per symbol)."""
    now = time.time()
    if now - _regime_cache["ts"] < 30:
        return _regime_cache["macro"], _regime_cache["sector"]

    macro = {"direction": 0, "confidence": 0, "signal": "no_data"}
    sector = {"direction": 0, "confidence": 0, "signal": "no_data"}

    try:
        from pakfindata.engine.macro_regime_hmm import get_current_regime
        r = get_current_regime()
        if r:
            regime = r.get("regime", "TRANSITION")
            d = {"RISK_ON": 1, "TRANSITION": 0, "RISK_OFF": -1, "CRISIS": -1}
            macro = {"direction": d.get(regime, 0),
                     "confidence": float(r.get("probability", 0.5)),
                     "signal": regime}
    except Exception as e:
        logger.debug("macro_hmm: %s", e)

    try:
        from pakfindata.engine.sector_rotation import rank_sectors
        r = rank_sectors()
        if r:
            sector = {"direction": 0, "confidence": 0.3, "signal": "sector_ctx"}
    except Exception as e:
        logger.debug("sector_rot: %s", e)

    _regime_cache["ts"] = now
    _regime_cache["macro"] = macro
    _regime_cache["sector"] = sector
    return macro, sector


def _call_fast(name, symbol):
    """Call a FAST per-symbol strategy. Skip slow ones entirely."""
    try:
        if name == "ofi":
            from pakfindata.engine.ofi_strategy import scan_current_ofi
            df = scan_current_ofi([symbol])
            if df is not None and not df.empty:
                row = df.iloc[0]
                d = {"LONG": 1, "SHORT": -1, "FLAT": 0}
                return {"direction": d.get(row.get("signal", ""), 0),
                        "confidence": float(row.get("confidence", 0)),
                        "signal": "OFI=" + str(round(row.get("ofi", 0), 2))}

        elif name == "oi_buildup":
            from pakfindata.engine.oi_strategy import scan_oi_signals
            df = scan_oi_signals([symbol])
            if df is not None and not df.empty:
                row = df.iloc[0]
                d = {"BUY": 1, "SELL": -1}
                return {"direction": d.get(row.get("signal", ""), 0),
                        "confidence": float(row.get("confidence", 0)),
                        "signal": row.get("state", "NEUTRAL")}

        elif name == "vpin":
            from pakfindata.engine.vpin_strategy import compute_live_signal
            r = compute_live_signal(symbol)
            if r:
                d = {"BUY": 1, "SELL": -1, "EXIT": -1, "REDUCE": 0, "HOLD": 0}
                return {"direction": d.get(r.signal, 0), "confidence": r.confidence,
                        "signal": f"{r.vpin:.2f}"}

        elif name == "basis_arb":
            from pakfindata.engine.basis_strategy import scan_basis_signals
            for r in (scan_basis_signals() or []):
                if symbol in str(r.get("symbol", "")):
                    sig = r.get("signal", "HOLD")
                    d = -1 if "SELL" in sig else (1 if "BUY" in sig else 0)
                    return {"direction": d, "confidence": float(r.get("confidence", 0)),
                            "signal": "z=" + str(round(r.get("basis_zscore", 0), 1))}

    except Exception as e:
        logger.debug("Strategy %s/%s: %s", name, symbol, e)

    return {"direction": 0, "confidence": 0, "signal": "no_data"}


STRATEGY_WEIGHTS = {
    "macro_hmm":       {"wt": 0.20, "cat": "REGIME",    "label": "Macro HMM"},
    "sector_rotation": {"wt": 0.10, "cat": "REGIME",    "label": "Sector Rot"},
    "vpin":            {"wt": 0.15, "cat": "FLOW",      "label": "VPIN"},
    "ofi":             {"wt": 0.15, "cat": "FLOW",      "label": "OFI"},
    "oi_buildup":      {"wt": 0.15, "cat": "FLOW",      "label": "OI"},
    "basis_arb":       {"wt": 0.25, "cat": "STRUCTURE", "label": "Basis"},
}


def _scan_one(symbol, macro_vote, sector_vote):
    """Scan one symbol — uses cached regime + fast per-symbol strategies."""
    votes = []

    # Regime (cached, instant)
    votes.append({"name": "macro_hmm", "label": "Macro HMM", "cat": "REGIME",
                  "wt": 0.20, "enabled": True, **macro_vote})
    votes.append({"name": "sector_rotation", "label": "Sector Rot", "cat": "REGIME",
                  "wt": 0.10, "enabled": True, **sector_vote})

    # Per-symbol strategies — only fast ones
    # VPIN ~0.4s, OI ~0.6s per symbol. OFI ~0.5s. Basis ~13s (SKIP).
    for name in ["vpin", "ofi", "oi_buildup"]:
        cfg = STRATEGY_WEIGHTS[name]
        r = _call_fast(name, symbol)
        votes.append({"name": name, "label": cfg["label"], "cat": cfg["cat"],
                      "wt": cfg["wt"], "enabled": True, **r})

    # Fuse
    tw = sum(v["wt"] for v in votes)
    raw = sum(v["direction"] * v["confidence"] * v["wt"] for v in votes) / (tw or 1)

    vpin_vote = next((v for v in votes if v["name"] == "vpin"), None)
    vetoed = vpin_vote and "TOXIC" in str(vpin_vote.get("signal", ""))

    if vetoed:
        dec, conf = "HOLD", 0
    elif raw > 0.30:
        dec, conf = "STRONG_BUY", min(abs(raw) * 100, 100)
    elif raw > 0.15:
        dec, conf = "BUY", min(abs(raw) * 100, 100)
    elif raw < -0.30:
        dec, conf = "STRONG_SELL", min(abs(raw) * 100, 100)
    elif raw < -0.15:
        dec, conf = "SELL", min(abs(raw) * 100, 100)
    else:
        dec, conf = "HOLD", 50

    bulls = [v for v in votes if v["direction"] > 0]
    bears = [v for v in votes if v["direction"] < 0]
    agree = max(len(bulls), len(bears))
    conflict = min(len(bulls), len(bears))

    # Explanation
    if raw > 0 and bulls:
        tip = max(bulls, key=lambda v: v["confidence"] * v["wt"])
        tip_text = f"{tip['label']} ({tip['signal']}) at {tip['confidence']:.0%}"
    elif raw < 0 and bears:
        tip = max(bears, key=lambda v: v["confidence"] * v["wt"])
        tip_text = f"{tip['label']} ({tip['signal']}) at {tip['confidence']:.0%}"
    else:
        tip_text = "No dominant signal"

    if "BUY" in dec:
        summary = f"{len(bulls)} bullish, {len(bears)} bearish. Tipped by {tip_text}"
    elif "SELL" in dec:
        summary = f"{len(bears)} bearish, {len(bulls)} bullish. Tipped by {tip_text}"
    else:
        summary = f"No consensus: {len(bulls)} bullish, {len(bears)} bearish"

    return {
        "decision": {"decision": dec, "raw_score": round(raw, 4),
                     "confidence": round(conf, 1), "agree": agree, "conflict": conflict,
                     "vetoed": vetoed, "veto_reason": "VPIN TOXIC" if vetoed else ""},
        "votes": votes,
        "explanation": {"summary": summary, "tipping_factor": tip_text,
                        "bull_case": [{"name": v["label"], "signal": v["signal"],
                                       "confidence": v["confidence"]} for v in bulls],
                        "bear_case": [{"name": v["label"], "signal": v["signal"],
                                       "confidence": v["confidence"]} for v in bears]},
        "score_abs": abs(raw),
    }


# ═══════════════════════════════════════════════════════
# PORTFOLIO
# ═══════════════════════════════════════════════════════

class Portfolio:
    def __init__(self, capital, max_pos=8):
        self.initial = capital
        self.cash = capital
        self.max_pos = max_pos
        self.positions = []
        self.closed = []
        self.equity_curve = []
        self.peak = capital

    def equity(self):
        return self.cash + sum(p["shares"] * p["cur"] for p in self.positions)

    def open_pos(self, symbol, side, price, shares, reason, explanation=""):
        cost = shares * price
        if cost > self.cash or shares <= 0 or len(self.positions) >= self.max_pos:
            return None
        if any(p["symbol"] == symbol for p in self.positions):
            return None

        # Realistic execution
        if simulate_execution:
            r = simulate_execution(symbol, "BUY" if side == "LONG" else "SELL",
                                   shares, 0, 0, 0, 0, 100000, price)
            fill_price = r.fill_price
            exec_info = f"spread={r.spread_bps}bps slip={r.slippage_bps}bps"
        else:
            fill_price = price
            exec_info = ""

        cost = shares * fill_price
        if cost > self.cash:
            shares = int(self.cash / fill_price)
            cost = shares * fill_price
        if shares <= 0:
            return None

        self.cash -= cost
        sl = fill_price * (0.98 if side == "LONG" else 1.02)
        tp = fill_price * (1.04 if side == "LONG" else 0.96)
        pos = {"symbol": symbol, "side": side, "entry": round(fill_price, 2),
               "cur": round(price, 2), "shares": shares,
               "sl": round(sl, 2), "tp": round(tp, 2),
               "time": datetime.now(PKT).strftime("%H:%M:%S"),
               "reason": reason, "explanation": explanation, "exec": exec_info,
               "pnl": 0, "pnl_pct": 0}
        self.positions.append(pos)
        return pos

    def close_pos(self, pos, price, reason):
        if pos["side"] == "LONG":
            pnl = (price - pos["entry"]) * pos["shares"]
        else:
            pnl = (pos["entry"] - price) * pos["shares"]
        self.cash += pos["shares"] * price
        self.closed.append({**pos, "exit": round(price, 2), "pnl": round(pnl),
                            "exit_time": datetime.now(PKT).strftime("%H:%M:%S"),
                            "exit_reason": reason})
        self.positions.remove(pos)
        if len(self.closed) > 200:
            self.closed = self.closed[-150:]

    def check_stops(self, prices):
        for pos in list(self.positions):
            p = prices.get(pos["symbol"], pos["cur"])
            pos["cur"] = p
            if pos["side"] == "LONG":
                pos["pnl"] = round((p - pos["entry"]) * pos["shares"])
                pos["pnl_pct"] = round((p / pos["entry"] - 1) * 100, 2)
                if p <= pos["sl"]:
                    self.close_pos(pos, p, "stop_loss")
                elif p >= pos["tp"]:
                    self.close_pos(pos, p, "take_profit")
            else:
                pos["pnl"] = round((pos["entry"] - p) * pos["shares"])
                pos["pnl_pct"] = round((1 - p / pos["entry"]) * 100, 2)
                if p >= pos["sl"]:
                    self.close_pos(pos, p, "stop_loss")
                elif p <= pos["tp"]:
                    self.close_pos(pos, p, "take_profit")

    def record(self):
        eq = self.equity()
        self.peak = max(self.peak, eq)
        dd = (self.peak - eq) / self.peak * 100 if self.peak > 0 else 0
        self.equity_curve.append({"time": int(time.time()), "equity": round(eq),
                                  "pnl": round(eq - self.initial), "dd": round(dd, 1)})
        if len(self.equity_curve) > 2000:
            self.equity_curve = self.equity_curve[-1500:]

    def to_dict(self):
        eq = self.equity()
        wins = sum(1 for t in self.closed if t["pnl"] > 0)
        total = len(self.closed) or 1
        return {
            "capital": self.initial, "cash": round(self.cash),
            "equity": round(eq), "pnl": round(eq - self.initial),
            "drawdown": round(self.equity_curve[-1]["dd"] if self.equity_curve else 0, 1),
            "trades": len(self.closed), "win_rate": round(wins / total * 100, 1),
            "positions": self.positions,
            "closed": self.closed[-30:],
            "equity_curve": self.equity_curve[-500:],
        }


# ═══════════════════════════════════════════════════════
# MAIN SERVICE
# ═══════════════════════════════════════════════════════

class PortfolioSimulator:
    def __init__(self, interval, capital, top_n, max_pos):
        self.interval = interval
        self.top_n = top_n
        self.portfolio = Portfolio(capital, max_pos)
        self.running = False
        self.scan_results = []
        self.capital_per_trade = capital / max_pos

    def read_symbols(self):
        """Read symbols from live_snapshot or DuckDB."""
        try:
            data = json.loads(LIVE_SNAPSHOT.read_text())
            syms = data.get("symbols", [])
            if syms:
                return syms
        except Exception:
            pass

        try:
            import duckdb
            con = duckdb.connect(str(DATA_ROOT / "pakfindata.duckdb"), read_only=True)
            max_ts = con.execute("SELECT MAX(CAST(_ts AS DATE)) FROM tick_logs").fetchone()[0]
            rows = con.execute("""
                SELECT symbol,
                       LAST(price ORDER BY timestamp) as price,
                       MAX(volume) as volume,
                       LAST(bid ORDER BY timestamp) as bid,
                       LAST(ask ORDER BY timestamp) as ask
                FROM tick_logs
                WHERE price > 0 AND market = 'REG'
                AND CAST(_ts AS DATE) = ?
                GROUP BY symbol
                ORDER BY volume DESC LIMIT ?
            """, [str(max_ts), self.top_n * 3]).fetchall()
            con.close()
            return [{"symbol": r[0], "price": r[1], "volume": r[2] or 0,
                     "bid": r[3] or 0, "ask": r[4] or 0,
                     "market": "REG", "changePercent": 0}
                    for r in rows if r[1] > 0]
        except Exception as e:
            logger.warning("DuckDB fallback failed: %s", e)
        return []

    def filter(self, symbols):
        skip = ("ALL", "KSE", "KMI", "P0", "P1", "P2", "P3", "MII", "ACI",
                "NBPPGI", "NITPGI", "MZNPI", "BKTI", "JSMFI")
        return [s for s in symbols
                if s.get("price", 0) > 0
                and s.get("price", 0) < 10000
                and s.get("volume", 0) > 50000
                and not s.get("symbol", "").startswith(skip)
                and "-" not in s.get("symbol", "")]

    def tick(self):
        t0 = time.time()

        # 1. Read all symbols
        all_syms = self.read_symbols()
        if not all_syms:
            logger.warning("No symbols available")
            return

        tradeable = self.filter(all_syms)
        by_vol = sorted(tradeable, key=lambda s: s.get("volume", 0), reverse=True)
        candidates = by_vol[:self.top_n]

        # 2. Compute regime ONCE (cached)
        macro_vote, sector_vote = _call_regime_once()
        regime_time = time.time() - t0

        # 3. Scan each symbol with fast strategies
        prices = {s["symbol"]: s["price"] for s in all_syms}
        self.portfolio.check_stops(prices)

        results = []
        for sym_data in candidates:
            symbol = sym_data["symbol"]
            try:
                scan = _scan_one(symbol, macro_vote, sector_vote)
                scan["symbol"] = symbol
                scan["price"] = sym_data["price"]
                scan["volume"] = sym_data.get("volume", 0)
                scan["bid"] = sym_data.get("bid", 0)
                scan["ask"] = sym_data.get("ask", 0)
                scan["change_pct"] = sym_data.get("changePercent", 0)
                spread = (sym_data.get("ask", 0) - sym_data.get("bid", 0))
                scan["spread_bps"] = round(spread / sym_data["price"] * 10000, 1) if sym_data["price"] > 0 and spread > 0 else 0
                results.append(scan)
            except Exception as e:
                logger.debug("Scan %s: %s", symbol, e)

        results.sort(key=lambda r: r["score_abs"], reverse=True)
        self.scan_results = results

        # 4. Pick trades
        holding = {p["symbol"] for p in self.portfolio.positions}
        for r in results:
            if r["symbol"] in holding:
                continue
            dec = r["decision"]["decision"]
            if dec in ("BUY", "STRONG_BUY", "SELL", "STRONG_SELL") and not r["decision"]["vetoed"]:
                side = "LONG" if "BUY" in dec else "SHORT"
                shares = int(self.capital_per_trade / r["price"]) if r["price"] > 0 else 0
                pos = self.portfolio.open_pos(r["symbol"], side, r["price"], shares,
                                              dec, r["explanation"]["summary"])
                if pos:
                    logger.info("OPEN %s %s %d @ %.2f", side, r["symbol"], shares, r["price"])

        self.portfolio.record()
        elapsed = time.time() - t0

        # 5. Write state
        self._write_state(elapsed)

        # Console
        pnl = self.portfolio.equity() - self.portfolio.initial
        pnl_s = f"+{pnl:,.0f}" if pnl >= 0 else f"{pnl:,.0f}"
        active = len([r for r in results if r["decision"]["decision"] != "HOLD"])
        now = datetime.now(PKT).strftime("%H:%M:%S")
        print(f"  {now} | {len(results)} scanned ({active} active) | "
              f"Pos: {len(self.portfolio.positions)} | P&L {pnl_s} | "
              f"regime={regime_time:.1f}s total={elapsed:.1f}s")

    def _write_state(self, elapsed):
        state = {
            "timestamp": datetime.now(PKT).isoformat(),
            "running": self.running,
            "interval": self.interval,
            "elapsed": round(elapsed, 1),
            "scan_results": [
                {"symbol": r["symbol"], "price": r["price"],
                 "bid": r.get("bid", 0), "ask": r.get("ask", 0),
                 "spread_bps": r.get("spread_bps", 0),
                 "volume": r.get("volume", 0),
                 "change_pct": r.get("change_pct", 0),
                 "decision": r["decision"],
                 "explanation": r["explanation"],
                 "top_votes": sorted(
                     [v for v in r["votes"] if v["enabled"] and v["confidence"] > 0],
                     key=lambda v: abs(v["direction"] * v["confidence"]),
                     reverse=True)[:3]}
                for r in self.scan_results[:50]
            ],
            "portfolio": self.portfolio.to_dict(),
        }
        try:
            tmp = str(PORTFOLIO_STATE) + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f, default=str)
            os.replace(tmp, str(PORTFOLIO_STATE))
        except OSError as e:
            logger.warning("Write: %s", e)

    def run(self):
        self.running = True
        print(f"\n{'='*60}")
        print(f"  Portfolio Simulator v2")
        print(f"  Top {self.top_n} stocks | {self.interval}s interval")
        print(f"  Capital: {self.portfolio.initial:,.0f} PKR")
        print(f"  Regime cached | Fast per-symbol strategies only")
        print(f"{'='*60}\n")
        while self.running:
            try:
                self.tick()
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error("Tick: %s", e)
            time.sleep(self.interval)
        self.running = False
        print("\nStopped.")


# ═══════════════════════════════════════════════════════
# START / STOP
# ═══════════════════════════════════════════════════════

def _write_pid(pid):
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))

def _remove_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()

def is_portfolio_sim_running():
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, 0)
            return True, pid
        except (ValueError, ProcessLookupError, PermissionError):
            _remove_pid()
    return False, None

def stop_portfolio_sim():
    running, pid = is_portfolio_sim_running()
    if not running:
        return False, "Not running"
    try:
        os.kill(pid, _signal.SIGTERM)
        time.sleep(1)
        _remove_pid()
        return True, f"Stopped (PID {pid})"
    except Exception as e:
        return False, str(e)

def start_portfolio_sim_background(interval=30, capital=5_000_000, top_n=20, max_pos=8):
    running, pid = is_portfolio_sim_running()
    if running:
        return False, f"Already running (PID {pid})"
    import subprocess
    cmd = [sys.executable, "-m", "pakfindata.services.portfolio_simulator",
           "--interval", str(interval), "--capital", str(int(capital)),
           "--top", str(top_n), "--max-positions", str(max_pos), "--daemon"]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)
    time.sleep(1)
    if proc.poll() is None:
        _write_pid(proc.pid)
        return True, f"Started (PID {proc.pid})"
    return False, "Failed to start"


def main():
    parser = argparse.ArgumentParser(description="Portfolio Simulator v2")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL)
    parser.add_argument("--capital", type=float, default=DEFAULT_CAPITAL)
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--max-positions", type=int, default=DEFAULT_MAX_POSITIONS)
    parser.add_argument("--daemon", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    _write_pid(os.getpid())

    def _shutdown(signum, frame):
        sim.running = False
    _signal.signal(_signal.SIGTERM, _shutdown)
    _signal.signal(_signal.SIGINT, _shutdown)

    sim = PortfolioSimulator(args.interval, args.capital, args.top, args.max_positions)
    try:
        sim.run()
    finally:
        _remove_pid()


if __name__ == "__main__":
    main()
