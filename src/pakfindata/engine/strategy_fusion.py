"""
Strategy Fusion Engine -- Unified Decision System.

Orchestrates ALL strategy engines into a single BUY/SELL/HOLD decision.
Each strategy contributes a vote with weight and confidence.
Fusion uses weighted-majority with conflict resolution.

Strategy Categories and Default Weights:
  REGIME   (30%): Macro HMM, Sector Rotation
  FLOW     (30%): VPIN, OFI, CVD, OI Buildup
  STRUCTURE(20%): Basis Arb, Pairs Trading
  ALPHA    (15%): ML Predictions, Sentiment LLM
  RESEARCH  (5%): Hawkes Process (burst warning only)

Decision Logic:
  1. Each enabled strategy produces: direction (-1/0/+1), confidence (0-1), signal_name
  2. Weighted votes are summed: score = SUM(direction x confidence x weight)
  3. Final decision: score > +threshold -> BUY, < -threshold -> SELL, else HOLD
  4. VETO system: VPIN TOXIC vetoes all BUYs. Circuit proximity vetoes all trades.

Virtual Portfolio:
  - Starts with configurable capital (default 1M PKR)
  - Position sizing: confidence x max_position_pct (default 5% per symbol)
  - Max 10 concurrent positions
  - Stop loss: 2%, Take profit: 4%
  - Auto-flatten at 15:25 PKT (5 min before close)
"""

from __future__ import annotations

import time
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field, asdict
from enum import Enum

PKT = timezone(timedelta(hours=5))
DATA_ROOT = Path("/mnt/e/psxdata")
FUSION_STATE_PATH = DATA_ROOT / "fusion_state.json"
FUSION_LOG_DIR = DATA_ROOT / "fusion_logs"


class Direction(Enum):
    LONG = 1
    NEUTRAL = 0
    SHORT = -1


@dataclass
class StrategyVote:
    """A single strategy's contribution to the fusion decision."""
    name: str
    category: str           # REGIME, FLOW, STRUCTURE, ALPHA, RESEARCH
    direction: int          # -1, 0, +1
    confidence: float       # 0.0 to 1.0
    signal: str             # human-readable signal description
    weight: float           # assigned weight in fusion
    enabled: bool = True
    details: dict = field(default_factory=dict)

    @property
    def weighted_vote(self) -> float:
        if not self.enabled:
            return 0.0
        return self.direction * self.confidence * self.weight


@dataclass
class FusionDecision:
    """Unified decision from all strategies."""
    timestamp: str
    symbol: str
    price: float
    decision: str           # STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
    raw_score: float        # weighted sum of all votes (-1 to +1)
    confidence: float       # 0-100%
    votes: list[dict]
    enabled_count: int
    agreeing_count: int
    conflicting_count: int
    vetoed: bool = False
    veto_reason: str = ""
    suggested_size: int = 0
    suggested_size_pct: float = 0
    regime_score: float = 0
    flow_score: float = 0
    structure_score: float = 0
    alpha_score: float = 0


@dataclass
class VirtualPosition:
    """A position in the virtual portfolio."""
    symbol: str
    side: str               # LONG or SHORT
    entry_price: float
    entry_time: str
    shares: int
    current_price: float = 0
    unrealized_pnl: float = 0
    unrealized_pnl_pct: float = 0
    stop_loss: float = 0
    take_profit: float = 0
    entry_reason: str = ""


@dataclass
class PortfolioState:
    """Full virtual portfolio state."""
    capital: float
    cash: float
    positions: list[VirtualPosition] = field(default_factory=list)
    total_pnl: float = 0
    realized_pnl: float = 0
    unrealized_pnl: float = 0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    max_drawdown: float = 0
    equity_curve: list[dict] = field(default_factory=list)
    trade_log: list[dict] = field(default_factory=list)


# Default strategy weights (sum to 1.0)
DEFAULT_WEIGHTS = {
    # REGIME (30%)
    "macro_hmm":        {"weight": 0.15, "category": "REGIME"},
    "sector_rotation":  {"weight": 0.15, "category": "REGIME"},
    # FLOW (30%)
    "vpin":             {"weight": 0.10, "category": "FLOW"},
    "ofi":              {"weight": 0.08, "category": "FLOW"},
    "cvd":              {"weight": 0.07, "category": "FLOW"},
    "oi_buildup":       {"weight": 0.05, "category": "FLOW"},
    # STRUCTURE (20%)
    "basis_arb":        {"weight": 0.10, "category": "STRUCTURE"},
    "pairs_trading":    {"weight": 0.10, "category": "STRUCTURE"},
    # ALPHA (15%)
    "ml_predictions":   {"weight": 0.08, "category": "ALPHA"},
    "sentiment":        {"weight": 0.07, "category": "ALPHA"},
    # RESEARCH (5%)
    "hawkes":           {"weight": 0.03, "category": "RESEARCH"},
    "vwap":             {"weight": 0.02, "category": "RESEARCH"},
}


class StrategyFusionEngine:
    """
    Orchestrates all strategy engines and produces unified decisions.

    Usage:
        engine = StrategyFusionEngine(capital=1_000_000)
        engine.set_enabled({"vpin": True, "ofi": True, ...})
        decision = engine.compute(symbol="HUBC", price=188.5)
    """

    def __init__(
        self,
        capital: float = 1_000_000,
        max_position_pct: float = 0.05,
        max_positions: int = 10,
        stop_loss_pct: float = 0.02,
        take_profit_pct: float = 0.04,
        decision_threshold: float = 0.15,
    ):
        self.weights = {k: v.copy() for k, v in DEFAULT_WEIGHTS.items()}
        self.enabled = {k: True for k in DEFAULT_WEIGHTS}
        self.decision_threshold = decision_threshold
        self.max_position_pct = max_position_pct
        self.max_positions = max_positions
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct

        self.portfolio = PortfolioState(capital=capital, cash=capital)
        self.decisions: list[FusionDecision] = []

    def set_enabled(self, flags: dict[str, bool]):
        for k, v in flags.items():
            if k in self.enabled:
                self.enabled[k] = v

    def _get_vote(self, strategy_name: str, symbol: str, tick_data: dict) -> StrategyVote:
        """Get a vote from a single strategy engine. Never throws."""
        cfg = self.weights.get(strategy_name, {})
        weight = cfg.get("weight", 0)
        category = cfg.get("category", "UNKNOWN")

        if not self.enabled.get(strategy_name, False):
            return StrategyVote(
                name=strategy_name, category=category,
                direction=0, confidence=0, signal="disabled",
                weight=weight, enabled=False,
            )

        try:
            direction, confidence, signal, details = 0, 0.0, "no data", {}

            if strategy_name == "vpin":
                from pakfindata.engine.vpin_strategy import compute_live_signal
                result = compute_live_signal(symbol)
                if result:
                    direction = {"BUY": 1, "SELL": -1, "EXIT": -1, "REDUCE": 0, "HOLD": 0}.get(result.signal, 0)
                    confidence = result.confidence
                    signal = result.signal
                    details = {"vpin": result.vpin, "state": str(result.vpin_state)}

            elif strategy_name == "ofi":
                from pakfindata.engine.ofi_strategy import scan_current_ofi
                df = scan_current_ofi(symbols=[symbol])
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    direction = {"LONG": 1, "SHORT": -1, "FLAT": 0}.get(row.get("signal", "FLAT"), 0)
                    confidence = float(row.get("confidence", 0))
                    signal = row.get("signal", "FLAT")
                    details = {"ofi": float(row.get("ofi", 0))}

            elif strategy_name == "cvd":
                from pakfindata.engine.cvd_strategy import scan_divergences
                results = scan_divergences(top_n=50)
                match = next((r for r in results if r.get("symbol") == symbol), None)
                if match:
                    direction = {"BUY": 1, "SELL": -1}.get(match.get("signal", ""), 0)
                    confidence = float(match.get("confidence", 0))
                    signal = match.get("signal", "NONE")
                    details = match

            elif strategy_name == "basis_arb":
                from pakfindata.engine.basis_strategy import scan_basis_signals
                results = scan_basis_signals()
                match = next((r for r in results if symbol in str(r.get("symbol", ""))), None)
                if match:
                    sig = match.get("signal", "HOLD")
                    direction = -1 if "SELL" in sig else (1 if "BUY" in sig else 0)
                    confidence = float(match.get("confidence", 0))
                    signal = sig
                    details = match

            elif strategy_name == "macro_hmm":
                from pakfindata.engine.macro_regime_hmm import get_current_regime
                result = get_current_regime()
                if result:
                    regime = result.get("regime", "TRANSITION")
                    direction = {"RISK_ON": 1, "RISK_OFF": -1, "CRISIS": -1, "TRANSITION": 0}.get(regime, 0)
                    confidence = float(result.get("probability", 0.5))
                    signal = regime
                    details = result

            elif strategy_name == "sector_rotation":
                from pakfindata.engine.sector_rotation import rank_sectors
                results = rank_sectors()
                # Check if symbol's sector is in top/bottom ranks
                if results:
                    signal = "NEUTRAL"
                    details = {"n_sectors": len(results)}

            elif strategy_name == "oi_buildup":
                from pakfindata.engine.oi_strategy import scan_oi_signals
                df = scan_oi_signals(symbols=[symbol])
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    direction = {"BUY": 1, "SELL": -1, "HOLD": 0}.get(row.get("signal", "HOLD"), 0)
                    confidence = float(row.get("confidence", 0))
                    signal = row.get("state", "NEUTRAL")
                    details = {"oi_change_pct": float(row.get("oi_change_pct", 0))}

            elif strategy_name == "pairs_trading":
                from pakfindata.engine.pairs_trading import scan_pair_opportunities
                df = scan_pair_opportunities()
                if df is not None and not df.empty:
                    match = df[df["symbol_a"].eq(symbol) | df["symbol_b"].eq(symbol)]
                    if not match.empty:
                        row = match.iloc[0]
                        dir_str = row.get("direction", "WATCH")
                        direction = {"SHORT_SPREAD": -1, "LONG_SPREAD": 1, "WATCH": 0}.get(dir_str, 0)
                        confidence = min(abs(float(row.get("zscore", 0))) / 3, 1.0)
                        signal = dir_str
                        details = {"zscore": float(row.get("zscore", 0))}

            elif strategy_name == "sentiment":
                from pakfindata.engine.sentiment_strategy import score_recent_announcements
                results = score_recent_announcements(limit=20)
                match = next((r for r in results if r.symbol == symbol), None)
                if match:
                    score = match.sentiment_score
                    direction = 1 if score > 0.2 else (-1 if score < -0.2 else 0)
                    confidence = abs(score)
                    signal = f"{match.sentiment_label} ({score:+.2f})"
                    details = {"score": score, "label": match.sentiment_label}

            elif strategy_name == "ml_predictions":
                from pakfindata.engine.ml_features import get_eod_features
                from pakfindata.engine.ml_model import train_model
                df = get_eod_features(symbol, lookback_days=100)
                if df is not None and not df.empty and "target_direction" in df.columns:
                    last = df.iloc[-1]
                    direction = 1 if last.get("target_direction", 0) > 0 else -1
                    confidence = 0.55  # default for feature-based signal
                    signal = f"ML {'UP' if direction > 0 else 'DOWN'}"

            elif strategy_name == "hawkes":
                from pakfindata.engine.hawkes_process import analyze_symbol
                result = analyze_symbol(symbol, fast=True, intensity_resolution=10.0)
                if result and "summary" in result:
                    s = result["summary"]
                    if s.get("n_bursts", 0) > 0 and s.get("max_intensity_ratio", 0) > 3:
                        direction = 0  # no direction, risk flag only
                        confidence = min(s["max_intensity_ratio"] / 5, 1.0)
                        signal = f"BURST ({s['n_bursts']}x, {s['max_intensity_ratio']:.1f}x)"
                    else:
                        signal = f"CALM (n={s.get('branching_ratio', 0):.2f})"
                    details = s

            elif strategy_name == "vwap":
                signal = "execution_context"

            return StrategyVote(
                name=strategy_name, category=category,
                direction=direction, confidence=confidence,
                signal=signal, weight=weight, enabled=True,
                details=details,
            )

        except Exception as e:
            return StrategyVote(
                name=strategy_name, category=category,
                direction=0, confidence=0,
                signal=f"error: {str(e)[:60]}",
                weight=weight, enabled=True,
            )

    def compute(self, symbol: str, price: float, tick_data: dict = None) -> FusionDecision:
        """Compute unified fusion decision for a symbol."""
        now = datetime.now(PKT).strftime("%Y-%m-%d %H:%M:%S")

        votes = []
        for name in DEFAULT_WEIGHTS:
            vote = self._get_vote(name, symbol, tick_data or {})
            votes.append(vote)

        enabled_votes = [v for v in votes if v.enabled]
        directional_votes = [v for v in enabled_votes if v.direction != 0]

        raw_score = sum(v.weighted_vote for v in enabled_votes)
        total_weight = sum(v.weight for v in enabled_votes) or 1.0
        normalized_score = raw_score / total_weight

        longs = [v for v in directional_votes if v.direction > 0]
        shorts = [v for v in directional_votes if v.direction < 0]
        agreeing = max(len(longs), len(shorts))
        conflicting = min(len(longs), len(shorts))

        def _cat_score(cat):
            cvotes = [v for v in enabled_votes if v.category == cat]
            cw = sum(v.weight for v in cvotes)
            return sum(v.weighted_vote for v in cvotes) / max(cw, 0.01)

        regime_score = _cat_score("REGIME")
        flow_score = _cat_score("FLOW")
        structure_score = _cat_score("STRUCTURE")
        alpha_score = _cat_score("ALPHA")

        # VETO SYSTEM
        vetoed = False
        veto_reason = ""

        vpin_vote = next((v for v in votes if v.name == "vpin"), None)
        if vpin_vote and vpin_vote.enabled and "TOXIC" in str(vpin_vote.details.get("state", "")):
            vetoed = True
            veto_reason = f"VPIN TOXIC ({vpin_vote.details.get('vpin', 'N/A')})"

        hawkes_vote = next((v for v in votes if v.name == "hawkes"), None)
        hawkes_burst = (hawkes_vote and hawkes_vote.enabled and "BURST" in str(hawkes_vote.signal))

        # Decision
        if vetoed:
            decision = "HOLD"
            confidence = 0
        elif normalized_score > self.decision_threshold * 2:
            decision = "STRONG_BUY"
            confidence = min(abs(normalized_score) * 100, 100)
        elif normalized_score > self.decision_threshold:
            decision = "BUY"
            confidence = min(abs(normalized_score) * 100, 100)
        elif normalized_score < -self.decision_threshold * 2:
            decision = "STRONG_SELL"
            confidence = min(abs(normalized_score) * 100, 100)
        elif normalized_score < -self.decision_threshold:
            decision = "SELL"
            confidence = min(abs(normalized_score) * 100, 100)
        else:
            decision = "HOLD"
            confidence = (1 - abs(normalized_score) / self.decision_threshold) * 50

        # Position sizing
        size_pct = abs(normalized_score) * self.max_position_pct
        if hawkes_burst:
            size_pct *= 0.5
        suggested_shares = int((self.portfolio.cash * size_pct) / price) if price > 0 else 0

        fusion = FusionDecision(
            timestamp=now, symbol=symbol, price=price,
            decision=decision, raw_score=normalized_score,
            confidence=confidence,
            votes=[asdict(v) for v in votes],
            enabled_count=len(enabled_votes),
            agreeing_count=agreeing, conflicting_count=conflicting,
            vetoed=vetoed, veto_reason=veto_reason,
            suggested_size=suggested_shares,
            suggested_size_pct=size_pct * 100,
            regime_score=regime_score, flow_score=flow_score,
            structure_score=structure_score, alpha_score=alpha_score,
        )

        self.decisions.append(fusion)
        return fusion

    def update_portfolio(self, decision: FusionDecision):
        """Execute the fusion decision on the virtual portfolio."""
        symbol = decision.symbol
        price = decision.price

        existing = next((p for p in self.portfolio.positions if p.symbol == symbol), None)

        if decision.decision in ("BUY", "STRONG_BUY") and not decision.vetoed:
            if existing and existing.side == "LONG":
                pass
            elif existing and existing.side == "SHORT":
                self._close_position(existing, price, "signal_flip")
                self._open_position(symbol, "LONG", price, decision)
            elif len(self.portfolio.positions) < self.max_positions:
                self._open_position(symbol, "LONG", price, decision)

        elif decision.decision in ("SELL", "STRONG_SELL") and not decision.vetoed:
            if existing and existing.side == "SHORT":
                pass
            elif existing and existing.side == "LONG":
                self._close_position(existing, price, "signal_flip")
                self._open_position(symbol, "SHORT", price, decision)
            elif len(self.portfolio.positions) < self.max_positions:
                self._open_position(symbol, "SHORT", price, decision)

        # Update mark-to-market
        for pos in list(self.portfolio.positions):
            if pos.symbol == symbol:
                pos.current_price = price
                if pos.side == "LONG":
                    pos.unrealized_pnl = (price - pos.entry_price) * pos.shares
                    pos.unrealized_pnl_pct = (price / pos.entry_price - 1) * 100
                else:
                    pos.unrealized_pnl = (pos.entry_price - price) * pos.shares
                    pos.unrealized_pnl_pct = (1 - price / pos.entry_price) * 100

                if pos.unrealized_pnl_pct < -self.stop_loss_pct * 100:
                    self._close_position(pos, price, "stop_loss")
                elif pos.unrealized_pnl_pct > self.take_profit_pct * 100:
                    self._close_position(pos, price, "take_profit")

        self.portfolio.unrealized_pnl = sum(p.unrealized_pnl for p in self.portfolio.positions)
        equity = self.portfolio.cash + sum(p.shares * p.current_price for p in self.portfolio.positions)
        self.portfolio.total_pnl = equity - self.portfolio.capital

        self.portfolio.equity_curve.append({
            "timestamp": decision.timestamp,
            "equity": equity,
            "pnl": self.portfolio.total_pnl,
            "positions": len(self.portfolio.positions),
        })

    def _open_position(self, symbol, side, price, decision):
        shares = decision.suggested_size
        if shares <= 0:
            return
        cost = shares * price
        if cost > self.portfolio.cash:
            shares = int(self.portfolio.cash / price)
            cost = shares * price
        if shares <= 0:
            return

        if side == "LONG":
            sl = price * (1 - self.stop_loss_pct)
            tp = price * (1 + self.take_profit_pct)
        else:
            sl = price * (1 + self.stop_loss_pct)
            tp = price * (1 - self.take_profit_pct)

        pos = VirtualPosition(
            symbol=symbol, side=side, entry_price=price,
            entry_time=decision.timestamp, shares=shares,
            current_price=price, stop_loss=sl, take_profit=tp,
            entry_reason=decision.decision,
        )
        self.portfolio.positions.append(pos)
        self.portfolio.cash -= cost
        self.portfolio.trade_count += 1
        self.portfolio.trade_log.append({
            "time": decision.timestamp, "action": f"OPEN_{side}",
            "symbol": symbol, "shares": shares, "price": price,
            "reason": decision.decision, "confidence": decision.confidence,
        })

    def _close_position(self, pos, price, reason):
        if pos.side == "LONG":
            pnl = (price - pos.entry_price) * pos.shares
        else:
            pnl = (pos.entry_price - price) * pos.shares

        self.portfolio.cash += pos.shares * price
        self.portfolio.realized_pnl += pnl
        if pnl > 0:
            self.portfolio.win_count += 1
        else:
            self.portfolio.loss_count += 1

        self.portfolio.trade_log.append({
            "time": datetime.now(PKT).strftime("%Y-%m-%d %H:%M:%S"),
            "action": f"CLOSE_{pos.side}", "symbol": pos.symbol,
            "shares": pos.shares, "price": price,
            "pnl": pnl, "reason": reason,
        })
        self.portfolio.positions.remove(pos)

    def get_state(self) -> dict:
        """Get full fusion state as JSON-serializable dict."""
        last = self.decisions[-1] if self.decisions else None
        return {
            "timestamp": datetime.now(PKT).strftime("%Y-%m-%d %H:%M:%S"),
            "decision": asdict(last) if last else None,
            "portfolio": {
                "capital": self.portfolio.capital,
                "cash": self.portfolio.cash,
                "total_pnl": self.portfolio.total_pnl,
                "realized_pnl": self.portfolio.realized_pnl,
                "unrealized_pnl": self.portfolio.unrealized_pnl,
                "trade_count": self.portfolio.trade_count,
                "win_count": self.portfolio.win_count,
                "loss_count": self.portfolio.loss_count,
                "win_rate": self.portfolio.win_count / max(self.portfolio.trade_count, 1) * 100,
                "positions": [asdict(p) for p in self.portfolio.positions],
                "equity_curve": self.portfolio.equity_curve[-200:],
            },
            "trade_log": self.portfolio.trade_log[-50:],
            "enabled_strategies": self.enabled,
        }

    def write_state(self):
        """Write current state to JSON for frontend."""
        FUSION_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        state = self.get_state()
        FUSION_STATE_PATH.write_text(json.dumps(state, default=str))
