"""
Multi-Symbol Portfolio Simulator — scans all symbols, picks best opportunities,
executes with realistic bid/ask pricing, manages multi-position portfolio.

Reads:  /mnt/e/psxdata/live_snapshot.json  (ALL symbols, written by tick_service)
Writes: /mnt/e/psxdata/portfolio_state.json (read by Streamlit)

Usage:
    python -m pakfindata.services.portfolio_simulator
    python -m pakfindata.services.portfolio_simulator --interval 30
    python -m pakfindata.services.portfolio_simulator --top 30
    python -m pakfindata.services.portfolio_simulator --daemon
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
DEFAULT_TOP_N = 30
DEFAULT_MAX_POSITIONS = 8

try:
    from pakfindata.services.fusion_service import STRATEGY_CATALOG, _call
except ImportError:
    STRATEGY_CATALOG = {
        "macro_hmm":       {"wt": 0.15, "cat": "REGIME",   "on": True,  "label": "Macro HMM"},
        "vpin":            {"wt": 0.10, "cat": "FLOW",     "on": True,  "label": "VPIN"},
        "ofi":             {"wt": 0.08, "cat": "FLOW",     "on": True,  "label": "OFI"},
        "oi_buildup":      {"wt": 0.05, "cat": "FLOW",     "on": True,  "label": "OI"},
        "basis_arb":       {"wt": 0.10, "cat": "STRUCTURE", "on": True,  "label": "Basis"},
    }
    def _call(name, symbol):
        return {"direction": 0, "confidence": 0, "signal": "not_available"}

try:
    from pakfindata.engine.execution_model import simulate_execution
except ImportError:
    simulate_execution = None


def _build_explanation(votes, decision, symbol):
    enabled = [v for v in votes if v.get("enabled")]
    bulls = [v for v in enabled if v.get("direction", 0) > 0]
    bears = [v for v in enabled if v.get("direction", 0) < 0]
    neutrals = [v for v in enabled if v.get("direction", 0) == 0]
    dec = decision.get("decision", "HOLD")
    score = decision.get("raw_score", 0)

    if score > 0 and bulls:
        tip = max(bulls, key=lambda v: v["confidence"] * v["wt"])
        tip_text = f"{tip['label']} ({tip['signal']}) at {tip['confidence']:.0%}"
    elif score < 0 and bears:
        tip = max(bears, key=lambda v: v["confidence"] * v["wt"])
        tip_text = f"{tip['label']} ({tip['signal']}) at {tip['confidence']:.0%}"
    else:
        tip_text = "No dominant signal"

    conflicts = []
    if bulls and bears:
        for b in bulls:
            for s in bears:
                if b["confidence"] > 0.3 and s["confidence"] > 0.3:
                    conflicts.append(f"{b['label']} (LONG {b['confidence']:.0%}) vs {s['label']} (SHORT {s['confidence']:.0%})")

    if decision.get("vetoed"):
        summary = f"VETOED by {decision.get('veto_reason')}"
    elif "BUY" in dec:
        summary = f"{len(bulls)} bullish, {len(bears)} bearish. Tipped by {tip_text}"
    elif "SELL" in dec:
        summary = f"{len(bears)} bearish, {len(bulls)} bullish. Tipped by {tip_text}"
    else:
        summary = f"No consensus: {len(bulls)} bullish, {len(bears)} bearish, {len(neutrals)} neutral"

    return {
        "summary": summary,
        "bull_case": [{"name": v["label"], "signal": v["signal"], "confidence": v["confidence"],
                       "contribution": round(v["direction"] * v["confidence"] * v["wt"] * 100, 1)}
                      for v in bulls],
        "bear_case": [{"name": v["label"], "signal": v["signal"], "confidence": v["confidence"],
                       "contribution": round(v["direction"] * v["confidence"] * v["wt"] * 100, 1)}
                      for v in bears],
        "neutral": [v["label"] for v in neutrals],
        "tipping_factor": tip_text,
        "conflicts": conflicts,
        "vetoed": decision.get("vetoed", False),
        "veto_reason": decision.get("veto_reason", ""),
    }


def _scan_symbol(sym_data, enabled):
    symbol = sym_data["symbol"]
    price = sym_data.get("price", 0)
    if price <= 0:
        return None

    votes = []
    for name, cfg in STRATEGY_CATALOG.items():
        on = enabled.get(name, cfg["on"])
        if not on:
            votes.append({"name": name, "label": cfg["label"], "cat": cfg["cat"],
                          "wt": cfg["wt"], "direction": 0, "confidence": 0,
                          "signal": "off", "enabled": False})
            continue
        r = _call(name, symbol)
        votes.append({"name": name, "label": cfg["label"], "cat": cfg["cat"],
                      "wt": cfg["wt"], "enabled": True, **r})

    on_votes = [v for v in votes if v["enabled"]]
    tw = sum(v["wt"] for v in on_votes) or 1.0
    raw = sum(v["direction"] * v["confidence"] * v["wt"] for v in on_votes) / tw

    def cat_score(c):
        cv = [v for v in on_votes if v["cat"] == c]
        w = sum(v["wt"] for v in cv) or 1
        return sum(v["direction"] * v["confidence"] * v["wt"] for v in cv) / w

    vpin = next((v for v in votes if v["name"] == "vpin" and v["enabled"]), None)
    vetoed = vpin and "TOXIC" in str(vpin.get("signal", ""))

    if vetoed:
        dec_str, conf = "HOLD", 0
    elif raw > 0.30:
        dec_str, conf = "STRONG_BUY", min(abs(raw) * 100, 100)
    elif raw > 0.15:
        dec_str, conf = "BUY", min(abs(raw) * 100, 100)
    elif raw < -0.30:
        dec_str, conf = "STRONG_SELL", min(abs(raw) * 100, 100)
    elif raw < -0.15:
        dec_str, conf = "SELL", min(abs(raw) * 100, 100)
    else:
        dec_str, conf = "HOLD", 50

    agree = sum(1 for v in on_votes if v["direction"] != 0 and (v["direction"] > 0) == (raw > 0))
    conflict = sum(1 for v in on_votes if v["direction"] != 0 and (v["direction"] > 0) != (raw > 0))

    bid = sym_data.get("bid", 0)
    ask = sym_data.get("ask", 0)
    spread_bps = (ask - bid) / bid * 10000 if bid > 0 and ask > 0 else 0

    decision = {
        "decision": dec_str, "raw_score": round(raw, 4),
        "confidence": round(conf, 1), "price": price,
        "regime": round(cat_score("REGIME"), 3),
        "flow": round(cat_score("FLOW"), 3),
        "structure": round(cat_score("STRUCTURE"), 3),
        "alpha": round(cat_score("ALPHA"), 3),
        "agree": agree, "conflict": conflict,
        "vetoed": vetoed, "veto_reason": "VPIN TOXIC" if vetoed else "",
    }

    explanation = _build_explanation(votes, decision, symbol)

    return {
        "symbol": symbol, "market": sym_data.get("market", "REG"),
        "price": price, "bid": bid, "ask": ask,
        "bid_vol": sym_data.get("bidVol", 0), "ask_vol": sym_data.get("askVol", 0),
        "volume": sym_data.get("volume", 0),
        "spread_bps": round(spread_bps, 1),
        "change_pct": sym_data.get("changePercent", 0),
        "decision": decision, "votes": votes,
        "explanation": explanation, "score_abs": abs(raw),
    }


class RealisticPortfolio:
    def __init__(self, capital, max_positions=8):
        self.initial = capital
        self.cash = capital
        self.max_positions = max_positions
        self.positions = []
        self.closed = []
        self.equity_curve = []
        self.peak = capital

    def equity(self, prices=None):
        total = self.cash
        for p in self.positions:
            cur = (prices or {}).get(p["symbol"], p["current_price"])
            total += p["shares"] * cur
        return total

    def open_position(self, scan_result, capital_per_trade):
        symbol = scan_result["symbol"]
        dec = scan_result["decision"]["decision"]
        if any(p["symbol"] == symbol for p in self.positions):
            return None
        if len(self.positions) >= self.max_positions:
            return None

        side = "LONG" if "BUY" in dec else "SHORT"
        price = scan_result["price"]
        shares = int(capital_per_trade / price) if price > 0 else 0
        if shares <= 0:
            return None

        if simulate_execution:
            result = simulate_execution(
                symbol=symbol, side="BUY" if side == "LONG" else "SELL",
                shares=shares, bid=scan_result["bid"], ask=scan_result["ask"],
                bid_vol=scan_result.get("bid_vol", 0), ask_vol=scan_result.get("ask_vol", 0),
                daily_volume=scan_result["volume"], price=price,
            )
            fill_price = result.fill_price
            filled = result.filled_shares
            exec_detail = {
                "fill_price": result.fill_price, "mid_price": result.mid_price,
                "spread_bps": result.spread_bps, "slippage_bps": result.slippage_bps,
                "total_cost_bps": result.total_cost_bps, "commission": result.commission,
                "fill_rate": result.fill_rate, "reason": result.reason,
            }
        else:
            fill_price = scan_result["ask"] if side == "LONG" else scan_result["bid"]
            if fill_price <= 0:
                fill_price = price
            filled = shares
            exec_detail = {"fill_price": fill_price, "reason": "no_model"}

        cost = filled * fill_price
        if cost > self.cash:
            filled = int(self.cash / fill_price)
            cost = filled * fill_price
        if filled <= 0:
            return None

        self.cash -= cost
        sl = fill_price * (0.98 if side == "LONG" else 1.02)
        tp = fill_price * (1.04 if side == "LONG" else 0.96)

        pos = {
            "symbol": symbol, "side": side,
            "entry_price": fill_price, "current_price": price,
            "shares": filled, "cost": cost,
            "stop_loss": round(sl, 2), "take_profit": round(tp, 2),
            "entry_time": datetime.now(PKT).strftime("%H:%M:%S"),
            "entry_reason": dec,
            "entry_confidence": scan_result["decision"]["confidence"],
            "execution": exec_detail,
            "explanation_summary": scan_result["explanation"]["summary"],
            "pnl": 0, "pnl_pct": 0,
        }
        self.positions.append(pos)
        return pos

    def close_position(self, pos, bid, ask, price, reason):
        side = pos["side"]
        if simulate_execution:
            result = simulate_execution(
                symbol=pos["symbol"], side="SELL" if side == "LONG" else "BUY",
                shares=pos["shares"], bid=bid, ask=ask, bid_vol=0, ask_vol=0,
                daily_volume=100000, price=price,
            )
            exit_price = result.fill_price
            exit_detail = {"fill_price": result.fill_price, "spread_bps": result.spread_bps,
                           "slippage_bps": result.slippage_bps, "commission": result.commission}
        else:
            exit_price = bid if side == "LONG" else ask
            if exit_price <= 0:
                exit_price = price
            exit_detail = {"fill_price": exit_price}

        pnl = ((exit_price - pos["entry_price"]) if side == "LONG" else (pos["entry_price"] - exit_price)) * pos["shares"]
        self.cash += pos["shares"] * exit_price
        self.closed.append({**pos, "exit_price": exit_price, "pnl": round(pnl, 2),
                            "exit_time": datetime.now(PKT).strftime("%H:%M:%S"),
                            "exit_reason": reason, "exit_execution": exit_detail})
        self.positions.remove(pos)
        if len(self.closed) > 200:
            self.closed = self.closed[-150:]

    def update_prices(self, all_symbols):
        for pos in list(self.positions):
            sym_data = all_symbols.get(pos["symbol"])
            if not sym_data:
                continue
            price = sym_data.get("price", pos["current_price"])
            bid = sym_data.get("bid", 0) or price
            ask = sym_data.get("ask", 0) or price
            pos["current_price"] = price
            if pos["side"] == "LONG":
                pos["pnl"] = round((price - pos["entry_price"]) * pos["shares"], 2)
                pos["pnl_pct"] = round((price / pos["entry_price"] - 1) * 100, 2)
                if price <= pos["stop_loss"]:
                    self.close_position(pos, bid, ask, price, "stop_loss")
                elif price >= pos["take_profit"]:
                    self.close_position(pos, bid, ask, price, "take_profit")
            else:
                pos["pnl"] = round((pos["entry_price"] - price) * pos["shares"], 2)
                pos["pnl_pct"] = round((1 - price / pos["entry_price"]) * 100, 2)
                if price >= pos["stop_loss"]:
                    self.close_position(pos, bid, ask, price, "stop_loss")
                elif price <= pos["take_profit"]:
                    self.close_position(pos, bid, ask, price, "take_profit")

    def record_equity(self):
        eq = self.equity()
        self.peak = max(self.peak, eq)
        dd = (self.peak - eq) / self.peak * 100 if self.peak > 0 else 0
        self.equity_curve.append({"time": int(time.time()), "equity": round(eq),
                                  "pnl": round(eq - self.initial), "drawdown": round(dd, 1),
                                  "positions": len(self.positions)})
        if len(self.equity_curve) > 2000:
            self.equity_curve = self.equity_curve[-1500:]

    def to_dict(self):
        eq = self.equity()
        wins = sum(1 for t in self.closed if t["pnl"] > 0)
        total = len(self.closed) or 1
        return {
            "capital": self.initial, "cash": round(self.cash),
            "equity": round(eq), "pnl": round(eq - self.initial),
            "drawdown": round(self.equity_curve[-1]["drawdown"] if self.equity_curve else 0, 1),
            "trades": len(self.closed), "win_rate": round(wins / total * 100, 1),
            "positions": self.positions,
            "closed": self.closed[-50:],
            "equity_curve": self.equity_curve[-500:],
        }


class PortfolioSimulator:
    def __init__(self, interval, capital, top_n, max_positions, enabled):
        self.interval = interval
        self.top_n = top_n
        self.enabled = enabled
        self.portfolio = RealisticPortfolio(capital, max_positions)
        self.running = False
        self.scan_results = []
        self.capital_per_trade = capital / max_positions

    def read_snapshot(self):
        """Read symbols from live_snapshot.json. Falls back to DuckDB tick_logs."""
        try:
            data = json.loads(LIVE_SNAPSHOT.read_text())
            syms = data.get("symbols", [])
            if syms:
                return syms
        except (json.JSONDecodeError, IOError):
            pass

        # Fallback: build synthetic snapshot from DuckDB tick_logs
        try:
            import duckdb
            con = duckdb.connect(str(DATA_ROOT / "pakfindata.duckdb"), read_only=True)
            # Get latest price per symbol — simple GROUP BY approach
            max_ts = con.execute("SELECT MAX(CAST(_ts AS DATE)) FROM tick_logs").fetchone()[0]
            rows = con.execute("""
                SELECT symbol,
                       LAST(price ORDER BY timestamp) as price,
                       MAX(volume) as volume,
                       LAST(bid ORDER BY timestamp) as bid,
                       LAST(ask ORDER BY timestamp) as ask,
                       LAST(bid_vol ORDER BY timestamp) as bid_vol,
                       LAST(ask_vol ORDER BY timestamp) as ask_vol
                FROM tick_logs
                WHERE price > 0 AND market = 'REG'
                AND CAST(_ts AS DATE) = ?
                GROUP BY symbol
                ORDER BY volume DESC
                LIMIT 100
            """, [str(max_ts)]).fetchall()
            con.close()
            return [{"symbol": r[0], "price": r[1], "volume": r[2] or 0,
                     "bid": r[3] or 0, "ask": r[4] or 0,
                     "bidVol": r[5] or 0, "askVol": r[6] or 0,
                     "market": "REG", "changePercent": 0}
                    for r in rows if r[1] > 0]
        except Exception:
            pass
        return []

    def filter_tradeable(self, symbols):
        skip_prefixes = ("ALL", "KSE", "KMI", "P0", "P1", "P2", "P3")
        return [s for s in symbols
                if s.get("price", 0) > 0
                and s.get("price", 0) < 50000  # skip indices (price > 50K)
                and s.get("volume", 0) > 10000
                and not s.get("symbol", "").startswith(skip_prefixes)
                and "-" not in s.get("symbol", "")  # skip futures
                ]

    def scan_and_rank(self, symbols):
        by_vol = sorted(symbols, key=lambda s: s.get("volume", 0), reverse=True)
        candidates = by_vol[:self.top_n]
        results = []
        for sym_data in candidates:
            try:
                result = _scan_symbol(sym_data, self.enabled)
                if result:
                    results.append(result)
            except Exception as e:
                logger.debug("Scan error %s: %s", sym_data.get("symbol"), e)
        results.sort(key=lambda r: r["score_abs"], reverse=True)
        return results

    def pick_trades(self, scan_results):
        holding = {p["symbol"] for p in self.portfolio.positions}
        picks = []
        for r in scan_results:
            if r["symbol"] in holding:
                continue
            dec = r["decision"]["decision"]
            if dec in ("STRONG_BUY", "BUY", "STRONG_SELL", "SELL") and not r["decision"].get("vetoed"):
                picks.append(r)
            if len(picks) >= (self.portfolio.max_positions - len(holding)):
                break
        return picks

    def tick(self):
        t0 = time.time()
        all_syms = self.read_snapshot()
        if not all_syms:
            return
        tradeable = self.filter_tradeable(all_syms)
        sym_dict = {s["symbol"]: s for s in all_syms}
        self.portfolio.update_prices(sym_dict)
        self.scan_results = self.scan_and_rank(tradeable)
        picks = self.pick_trades(self.scan_results)
        for pick in picks:
            pos = self.portfolio.open_position(pick, self.capital_per_trade)
            if pos:
                logger.info("OPENED %s %s %d @ %.2f (%s)",
                            pos["side"], pos["symbol"], pos["shares"],
                            pos["entry_price"], pos["entry_reason"])
        self.portfolio.record_equity()
        elapsed = time.time() - t0
        self.write_state(elapsed)

        pnl = self.portfolio.equity() - self.portfolio.initial
        pnl_str = f"+{pnl:,.0f}" if pnl >= 0 else f"{pnl:,.0f}"
        now = datetime.now(PKT).strftime("%H:%M:%S")
        active = len([r for r in self.scan_results if r["decision"]["decision"] != "HOLD"])
        print(f"  {now} | Scanned {len(self.scan_results)} | Active: {active} | "
              f"Positions: {len(self.portfolio.positions)} | P&L {pnl_str} | {elapsed:.1f}s")

    def write_state(self, elapsed):
        state = {
            "timestamp": datetime.now(PKT).isoformat(),
            "running": self.running, "interval": self.interval,
            "elapsed": round(elapsed, 1),
            "scan_results": [
                {"symbol": r["symbol"], "price": r["price"], "bid": r["bid"], "ask": r["ask"],
                 "spread_bps": r["spread_bps"], "volume": r["volume"],
                 "change_pct": r["change_pct"], "decision": r["decision"],
                 "explanation": r["explanation"],
                 "top_votes": sorted(
                     [v for v in r["votes"] if v["enabled"] and v["confidence"] > 0],
                     key=lambda v: abs(v["direction"] * v["confidence"]), reverse=True)[:3]}
                for r in self.scan_results[:50]
            ],
            "portfolio": self.portfolio.to_dict(),
            "catalog": {k: {"label": v["label"], "cat": v["cat"],
                            "enabled": self.enabled.get(k, v["on"])}
                        for k, v in STRATEGY_CATALOG.items()},
        }
        try:
            tmp = str(PORTFOLIO_STATE) + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f, default=str)
            os.replace(tmp, str(PORTFOLIO_STATE))
        except OSError as e:
            logger.warning("Write failed: %s", e)

    def run(self):
        self.running = True
        print(f"\n{'='*60}")
        print(f"  Portfolio Simulator started")
        print(f"  Scanning top {self.top_n} symbols every {self.interval}s")
        print(f"  Capital: {self.portfolio.initial:,.0f} PKR | Max: {self.portfolio.max_positions} positions")
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
        print("\nPortfolio Simulator stopped.")


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

def start_portfolio_sim_background(interval=30, capital=5_000_000, top_n=30, max_pos=8):
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
    parser = argparse.ArgumentParser(description="Portfolio Simulator")
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

    enabled = {k: v["on"] for k, v in STRATEGY_CATALOG.items()}
    sim = PortfolioSimulator(args.interval, args.capital, args.top,
                             args.max_positions, enabled)
    try:
        sim.run()
    finally:
        _remove_pid()


if __name__ == "__main__":
    main()
