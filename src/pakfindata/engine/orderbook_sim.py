"""
Order Book Reconstruction & RL Limit Order Agent.

Phase 1: Reconstruct approximate order book from Level 1 data
Phase 2: Agent-based market simulation
Phase 3: RL agent for optimal limit order placement

PSX Order Book Characteristics:
  - Tick size: Rs 0.01 for most stocks
  - Typical spread: 1-5 ticks (0.01-0.05 Rs for liquid names)
  - Depth: 5-10 meaningful price levels
  - No HFT: book changes every 1-5 seconds (slow by global standards)
  - Auction periods: 09:15-09:30, 15:28-15:30
  - Circuit breakers: +/-7.5%
"""

import numpy as np
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, Tuple
from collections import deque

from pakfindata.db.connections import analytics_con

PKT = timezone(timedelta(hours=5))
JSONL_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
SIM_DIR = Path("/mnt/e/psxdata/simulation")
BOOK_CACHE_DIR = SIM_DIR / "book_snapshots"
SIM_RESULTS_DIR = SIM_DIR / "sim_results"
RL_HISTORY_DIR = SIM_DIR / "rl_history"

# PSX market microstructure constants
TICK_SIZE = 0.01       # minimum price increment
MAX_BOOK_LEVELS = 10   # reconstructed depth levels
CIRCUIT_LIMIT_PCT = 0.075  # +/-7.5%


def _ensure_dirs():
    """Create simulation output directories if they don't exist."""
    for d in [SIM_DIR, BOOK_CACHE_DIR, SIM_RESULTS_DIR, RL_HISTORY_DIR]:
        d.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════
# PHASE 1: ORDER BOOK RECONSTRUCTION
# ═══════════════════════════════════════════

@dataclass
class BookLevel:
    price: float
    volume: int
    order_count: int = 1    # estimated
    last_seen: float = 0    # timestamp


@dataclass
class ReconstructedBook:
    """Approximate order book reconstructed from Level 1 data."""
    symbol: str
    timestamp: float
    bids: list[BookLevel]   # sorted high -> low (best bid first)
    asks: list[BookLevel]   # sorted low -> high (best ask first)
    last_price: float
    last_volume: int
    spread: float
    mid_price: float
    imbalance: float        # (bidVol - askVol) / (bidVol + askVol)


class OrderBookReconstructor:
    """
    Reconstruct approximate order book depth from Level 1 snapshots.

    Method: Track historical bid/ask levels over time. When the best bid/ask
    changes, the old level doesn't disappear -- it moves to Level 2.

    Assumptions:
    1. When best bid drops from 100.50 to 100.49, the volume at 100.50
       was likely consumed by a sell market order.
    2. When best bid rises from 100.49 to 100.50, new buy orders arrived.
    3. Volume at non-best levels decays over time (orders get cancelled).
    4. The book shape follows a power law: volume decreases with distance
       from the best price.

    This gives us an APPROXIMATE book -- not exact, but useful for simulation.
    """

    def __init__(self, symbol: str, max_levels: int = MAX_BOOK_LEVELS,
                 decay_rate: float = 0.95):
        self.symbol = symbol
        self.max_levels = max_levels
        self.decay_rate = decay_rate  # volume decay per tick for non-best levels

        # Historical bid/ask levels: {price: BookLevel}
        self.bid_levels: dict[float, BookLevel] = {}
        self.ask_levels: dict[float, BookLevel] = {}

        self.last_price = 0
        self.last_volume = 0
        self.tick_count = 0

    def update(self, tick: dict) -> ReconstructedBook:
        """
        Update the reconstructed book with a new tick.

        tick: {price, volume, bid, ask, bidVol, askVol, timestamp, ...}
        """
        bid = tick.get("bid", 0)
        ask = tick.get("ask", 0)
        bid_vol = tick.get("bidVol", 0) or 0
        ask_vol = tick.get("askVol", 0) or 0
        price = tick.get("price", 0)
        volume = tick.get("volume", 0)
        timestamp = tick.get("timestamp", 0)

        self.tick_count += 1
        self.last_price = price
        self.last_volume = volume

        # -- Update bid side --
        if bid > 0 and bid_vol > 0:
            # Apply decay to existing non-best levels
            for p in list(self.bid_levels.keys()):
                if p != bid:
                    self.bid_levels[p].volume = int(self.bid_levels[p].volume * self.decay_rate)
                    if self.bid_levels[p].volume <= 0:
                        del self.bid_levels[p]

            # Update/add best bid
            self.bid_levels[bid] = BookLevel(
                price=bid, volume=int(bid_vol),
                last_seen=timestamp
            )

            # Infer nearby levels using power law
            self._infer_levels(bid, bid_vol, "bid", timestamp)

        # -- Update ask side --
        if ask > 0 and ask_vol > 0:
            for p in list(self.ask_levels.keys()):
                if p != ask:
                    self.ask_levels[p].volume = int(self.ask_levels[p].volume * self.decay_rate)
                    if self.ask_levels[p].volume <= 0:
                        del self.ask_levels[p]

            self.ask_levels[ask] = BookLevel(
                price=ask, volume=int(ask_vol),
                last_seen=timestamp
            )

            self._infer_levels(ask, ask_vol, "ask", timestamp)

        # -- Build sorted book --
        bids = sorted(self.bid_levels.values(), key=lambda x: -x.price)[:self.max_levels]
        asks = sorted(self.ask_levels.values(), key=lambda x: x.price)[:self.max_levels]

        spread = (asks[0].price - bids[0].price) if bids and asks else 0
        mid = (bids[0].price + asks[0].price) / 2 if bids and asks else price

        total_bid_vol = sum(b.volume for b in bids)
        total_ask_vol = sum(a.volume for a in asks)
        imbalance = ((total_bid_vol - total_ask_vol) /
                     (total_bid_vol + total_ask_vol)) if (total_bid_vol + total_ask_vol) > 0 else 0

        return ReconstructedBook(
            symbol=self.symbol,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
            last_price=price,
            last_volume=volume,
            spread=spread,
            mid_price=mid,
            imbalance=imbalance,
        )

    def _infer_levels(self, best_price: float, best_vol: int,
                      side: str, timestamp: float):
        """
        Infer order book depth beyond best level using power law.

        Volume at level k ~ best_vol * alpha^k where alpha in (0.3, 0.7)
        Price at level k = best_price +/- k * tick_size
        """
        alpha = 0.5  # power law decay factor
        levels = self.bid_levels if side == "bid" else self.ask_levels

        for k in range(1, self.max_levels):
            if side == "bid":
                level_price = round(best_price - k * TICK_SIZE, 2)
            else:
                level_price = round(best_price + k * TICK_SIZE, 2)

            if level_price <= 0:
                continue

            # Only add if we don't already have a real observation at this level
            if level_price not in levels:
                inferred_vol = int(best_vol * (alpha ** k))
                if inferred_vol > 0:
                    levels[level_price] = BookLevel(
                        price=level_price,
                        volume=inferred_vol,
                        order_count=max(1, inferred_vol // 500),  # estimate
                        last_seen=timestamp,
                    )


def reconstruct_book_history(
    symbol: str,
    date_str: str = None,
    max_ticks: int = 50000,
) -> list[ReconstructedBook]:
    """
    Reconstruct order book history for a symbol on a given date.
    Returns list of ReconstructedBook snapshots.
    """
    con = analytics_con()

    if date_str is None:
        # Derive latest date from timestamp (epoch seconds -> date)
        date_str = str(con.execute(
            "SELECT CAST(to_timestamp(MAX(timestamp)) AS DATE) FROM tick_logs WHERE symbol = ?",
            [symbol]
        ).fetchone()[0])

    ticks = con.execute("""
        SELECT price, volume, bid, ask,
               bid_vol AS "bidVol", ask_vol AS "askVol",
               timestamp, change, trades
        FROM tick_logs
        WHERE symbol = ?
          AND CAST(to_timestamp(timestamp) AS DATE) = CAST(? AS DATE)
        ORDER BY timestamp
        LIMIT ?
    """, [symbol, date_str, max_ticks]).df()

    con.close()

    if ticks.empty:
        return []

    reconstructor = OrderBookReconstructor(symbol)
    books = []

    for _, tick in ticks.iterrows():
        book = reconstructor.update(tick.to_dict())
        books.append(book)

    return books


# ═══════════════════════════════════════════
# PHASE 2: AGENT-BASED MARKET SIMULATION
# ═══════════════════════════════════════════

class MarketAgent:
    """Base class for simulated market participants."""

    def __init__(self, agent_id: str, initial_cash: float = 10_000_000,
                 initial_shares: int = 0):
        self.agent_id = agent_id
        self.cash = initial_cash
        self.shares = initial_shares
        self.orders: list[dict] = []  # pending limit orders
        self.trades: list[dict] = []  # executed trades

    def decide(self, book: ReconstructedBook, t: int) -> Optional[dict]:
        """Return an order dict or None."""
        raise NotImplementedError


class NoiseTrader(MarketAgent):
    """
    Random noise trader -- submits random market orders.
    Represents retail PSX participants.
    """
    def __init__(self, agent_id: str, trade_prob: float = 0.05,
                 avg_size: int = 500):
        super().__init__(agent_id)
        self.trade_prob = trade_prob
        self.avg_size = avg_size

    def decide(self, book: ReconstructedBook, t: int) -> Optional[dict]:
        if np.random.random() > self.trade_prob:
            return None

        side = "BUY" if np.random.random() > 0.5 else "SELL"
        size = max(1, int(np.random.exponential(self.avg_size)))

        if side == "BUY" and book.asks:
            return {"type": "MARKET", "side": "BUY", "size": size,
                    "price": book.asks[0].price, "agent": self.agent_id}
        elif side == "SELL" and book.bids:
            return {"type": "MARKET", "side": "SELL", "size": size,
                    "price": book.bids[0].price, "agent": self.agent_id}
        return None


class MomentumTrader(MarketAgent):
    """
    Momentum trader -- buys on price increases, sells on decreases.
    Common on PSX due to retail herding behavior.
    """
    def __init__(self, agent_id: str, lookback: int = 20, threshold: float = 0.01):
        super().__init__(agent_id)
        self.lookback = lookback
        self.threshold = threshold
        self.price_history = deque(maxlen=lookback)

    def decide(self, book: ReconstructedBook, t: int) -> Optional[dict]:
        self.price_history.append(book.last_price)

        if len(self.price_history) < self.lookback:
            return None

        returns = (self.price_history[-1] / self.price_history[0]) - 1

        if returns > self.threshold and book.asks:
            size = max(1, int(500 * abs(returns) / self.threshold))
            return {"type": "MARKET", "side": "BUY", "size": size,
                    "price": book.asks[0].price, "agent": self.agent_id}
        elif returns < -self.threshold and book.bids:
            size = max(1, int(500 * abs(returns) / self.threshold))
            return {"type": "MARKET", "side": "SELL", "size": size,
                    "price": book.bids[0].price, "agent": self.agent_id}
        return None


class MarketMaker(MarketAgent):
    """
    Simple market maker -- quotes bid/ask around mid price.
    PSX has a few informal market makers in liquid stocks.
    """
    def __init__(self, agent_id: str, spread_ticks: int = 2,
                 quote_size: int = 1000):
        super().__init__(agent_id)
        self.spread_ticks = spread_ticks
        self.quote_size = quote_size
        self.inventory = 0
        self.max_inventory = 10000

    def decide(self, book: ReconstructedBook, t: int) -> Optional[dict]:
        if not book.bids or not book.asks:
            return None

        mid = book.mid_price
        half_spread = self.spread_ticks * TICK_SIZE / 2

        # Skew quotes based on inventory
        skew = -self.inventory / self.max_inventory * TICK_SIZE * 2

        bid_price = round(mid - half_spread + skew, 2)
        ask_price = round(mid + half_spread + skew, 2)

        # Return both sides as a single "quote"
        return {
            "type": "LIMIT",
            "bid_price": bid_price, "bid_size": self.quote_size,
            "ask_price": ask_price, "ask_size": self.quote_size,
            "agent": self.agent_id,
        }


class PSXMarketSimulator:
    """
    Agent-based PSX market simulation.

    Replays historical tick data while allowing agents to interact.
    The order book is reconstructed from real data, then agents submit
    orders that are matched against it.
    """

    def __init__(self, symbol: str, date_str: str = None):
        self.symbol = symbol
        self.agents: list[MarketAgent] = []
        self.order_book = OrderBookReconstructor(symbol)
        self.trade_log: list[dict] = []
        self.book_history: list[ReconstructedBook] = []

        # Load real tick data
        con = analytics_con()
        if date_str is None:
            date_str = str(con.execute(
                "SELECT CAST(to_timestamp(MAX(timestamp)) AS DATE) FROM tick_logs WHERE symbol = ?",
                [symbol]
            ).fetchone()[0])

        self.ticks = con.execute("""
            SELECT price, volume, bid, ask,
                   bid_vol AS "bidVol", ask_vol AS "askVol",
                   timestamp, change, trades
            FROM tick_logs
            WHERE symbol = ?
              AND CAST(to_timestamp(timestamp) AS DATE) = CAST(? AS DATE)
            ORDER BY timestamp
        """, [symbol, date_str]).df()
        con.close()

    def add_agent(self, agent: MarketAgent):
        self.agents.append(agent)

    def run(self, max_ticks: int = None) -> dict:
        """Run the simulation."""
        n = len(self.ticks) if max_ticks is None else min(max_ticks, len(self.ticks))

        for t in range(n):
            tick = self.ticks.iloc[t].to_dict()

            # Update book with real data
            book = self.order_book.update(tick)
            self.book_history.append(book)

            # Let agents decide
            for agent in self.agents:
                order = agent.decide(book, t)
                if order:
                    self._match_order(order, book, t)

        return {
            "ticks_processed": n,
            "trades": len(self.trade_log),
            "book_snapshots": len(self.book_history),
            "agent_stats": {a.agent_id: len(a.trades) for a in self.agents},
        }

    def _match_order(self, order: dict, book: ReconstructedBook, t: int):
        """Simple order matching against reconstructed book."""
        if order["type"] == "MARKET":
            if order["side"] == "BUY" and book.asks:
                fill_price = book.asks[0].price
                self.trade_log.append({
                    "tick": t, "agent": order["agent"],
                    "side": "BUY", "price": fill_price,
                    "size": order["size"], "type": "MARKET",
                })
            elif order["side"] == "SELL" and book.bids:
                fill_price = book.bids[0].price
                self.trade_log.append({
                    "tick": t, "agent": order["agent"],
                    "side": "SELL", "price": fill_price,
                    "size": order["size"], "type": "MARKET",
                })


# ═══════════════════════════════════════════
# PHASE 3: RL LIMIT ORDER AGENT
# ═══════════════════════════════════════════

def create_rl_environment(symbol: str, date_str: str = None):
    """
    Create a Gymnasium environment for limit order placement.

    State space (14 dimensions):
      - Mid price (normalized)
      - Spread (ticks)
      - Book imbalance (-1 to 1)
      - Bid volume at best (normalized)
      - Ask volume at best (normalized)
      - Last trade direction (1=buy, -1=sell)
      - Price momentum (5-tick return)
      - Price momentum (20-tick return)
      - Volatility (20-tick)
      - Time of day (0-1, normalized)
      - Current position (shares held, normalized)
      - Unrealized PnL (normalized)
      - Distance to VWAP (normalized)
      - Trade intensity (ticks with trades / total ticks, 20-tick window)

    Action space (discrete, 7 actions):
      0: Hold (do nothing)
      1: Market buy
      2: Market sell
      3: Limit buy at best bid
      4: Limit buy at bid - 1 tick (passive)
      5: Limit sell at best ask
      6: Limit sell at ask + 1 tick (passive)

    Reward:
      - PnL from executed trades (main component)
      - Penalty for market impact (spread cost)
      - Bonus for limit order fills (captured spread)
      - Penalty for holding inventory overnight
    """
    try:
        import gymnasium as gym
        from gymnasium import spaces
    except ImportError:
        print("gymnasium not installed. pip install gymnasium")
        return None

    class PSXLimitOrderEnv(gym.Env):
        """PSX Limit Order Placement Environment."""

        metadata = {"render_modes": ["human"]}

        def __init__(self, sym: str, dt: str = None):
            super().__init__()

            self.symbol = sym
            self.books = reconstruct_book_history(sym, dt)

            if not self.books:
                raise ValueError(f"No book data for {sym}")

            # Spaces
            self.observation_space = spaces.Box(
                low=-np.inf, high=np.inf,
                shape=(14,), dtype=np.float32
            )
            self.action_space = spaces.Discrete(7)

            # Episode state
            self.current_step = 0
            self.position = 0          # shares held
            self.cash = 1_000_000      # starting cash
            self.entry_price = 0
            self.trades_executed = []
            self.vwap_num = 0
            self.vwap_den = 0
            self.price_history = deque(maxlen=50)
            self.trade_flags = deque(maxlen=20)

            # Pending limit orders
            self.pending_bid = None    # {price, size, tick}
            self.pending_ask = None

        def reset(self, seed=None, options=None):
            super().reset(seed=seed)

            self.current_step = 50  # skip first 50 ticks for warmup
            self.position = 0
            self.cash = 1_000_000
            self.entry_price = 0
            self.trades_executed = []
            self.vwap_num = 0
            self.vwap_den = 0
            self.price_history.clear()
            self.trade_flags.clear()
            self.pending_bid = None
            self.pending_ask = None

            # Warm up price history
            for i in range(50):
                self.price_history.append(self.books[i].last_price)
                self.trade_flags.append(1 if self.books[i].last_volume > 0 else 0)

            return self._get_obs(), {}

        def step(self, action: int):
            if self.current_step >= len(self.books) - 1:
                return self._get_obs(), 0, True, False, {}

            book = self.books[self.current_step]
            prev_book = self.books[self.current_step - 1]

            reward = 0

            # -- Check pending limit order fills --
            if self.pending_bid and book.last_price <= self.pending_bid["price"]:
                # Bid filled!
                fill_price = self.pending_bid["price"]
                self.position += self.pending_bid["size"]
                self.cash -= fill_price * self.pending_bid["size"]
                self.entry_price = fill_price
                reward += (book.mid_price - fill_price) * self.pending_bid["size"] * 0.001
                self.trades_executed.append({
                    "tick": self.current_step, "side": "BUY",
                    "price": fill_price, "type": "LIMIT"
                })
                self.pending_bid = None

            if self.pending_ask and book.last_price >= self.pending_ask["price"]:
                # Ask filled!
                fill_price = self.pending_ask["price"]
                self.position -= self.pending_ask["size"]
                self.cash += fill_price * self.pending_ask["size"]
                reward += (fill_price - book.mid_price) * self.pending_ask["size"] * 0.001
                self.trades_executed.append({
                    "tick": self.current_step, "side": "SELL",
                    "price": fill_price, "type": "LIMIT"
                })
                self.pending_ask = None

            # -- Execute action --
            trade_size = 500  # lot size

            if action == 1:  # Market buy
                if book.asks:
                    fill_price = book.asks[0].price
                    self.position += trade_size
                    self.cash -= fill_price * trade_size
                    self.entry_price = fill_price
                    reward -= book.spread * trade_size * 0.0005  # spread cost penalty
                    self.trades_executed.append({
                        "tick": self.current_step, "side": "BUY",
                        "price": fill_price, "type": "MARKET"
                    })

            elif action == 2:  # Market sell
                if book.bids:
                    fill_price = book.bids[0].price
                    self.position -= trade_size
                    self.cash += fill_price * trade_size
                    reward -= book.spread * trade_size * 0.0005
                    self.trades_executed.append({
                        "tick": self.current_step, "side": "SELL",
                        "price": fill_price, "type": "MARKET"
                    })

            elif action == 3:  # Limit buy at best bid
                if book.bids:
                    self.pending_bid = {"price": book.bids[0].price,
                                       "size": trade_size, "tick": self.current_step}

            elif action == 4:  # Limit buy at bid - 1 tick (passive)
                if book.bids:
                    self.pending_bid = {"price": book.bids[0].price - TICK_SIZE,
                                       "size": trade_size, "tick": self.current_step}

            elif action == 5:  # Limit sell at best ask
                if book.asks:
                    self.pending_ask = {"price": book.asks[0].price,
                                       "size": trade_size, "tick": self.current_step}

            elif action == 6:  # Limit sell at ask + 1 tick (passive)
                if book.asks:
                    self.pending_ask = {"price": book.asks[0].price + TICK_SIZE,
                                       "size": trade_size, "tick": self.current_step}

            # action == 0: Hold

            # -- Cancel stale limit orders (older than 100 ticks) --
            if self.pending_bid and self.current_step - self.pending_bid["tick"] > 100:
                self.pending_bid = None
            if self.pending_ask and self.current_step - self.pending_ask["tick"] > 100:
                self.pending_ask = None

            # -- Compute reward --
            # Mark-to-market PnL change
            if self.position != 0:
                mtm_change = (book.mid_price - prev_book.mid_price) * self.position
                reward += mtm_change * 0.001  # scaled

            # Inventory penalty (encourage flat position)
            reward -= abs(self.position) * 0.00001

            # Update tracking
            self.price_history.append(book.last_price)
            self.trade_flags.append(1 if book.last_volume > 0 else 0)
            self.vwap_num += book.last_price * book.last_volume
            self.vwap_den += book.last_volume

            self.current_step += 1
            done = self.current_step >= len(self.books) - 1

            # End-of-day penalty for open position
            if done and self.position != 0:
                reward -= abs(self.position) * book.spread * 0.001

            return self._get_obs(), float(reward), done, False, {
                "position": self.position,
                "cash": self.cash,
                "trades": len(self.trades_executed),
            }

        def _get_obs(self) -> np.ndarray:
            """Build observation vector."""
            book = self.books[min(self.current_step, len(self.books) - 1)]
            prices = list(self.price_history) if self.price_history else [book.last_price]

            # Normalize features
            mid = book.mid_price if book.mid_price > 0 else 1

            # Momentum
            mom_5 = (prices[-1] / prices[-5] - 1) if len(prices) >= 5 else 0
            mom_20 = (prices[-1] / prices[-20] - 1) if len(prices) >= 20 else 0

            # Volatility
            if len(prices) >= 20:
                returns = np.diff(prices[-20:]) / np.array(prices[-20:-1])
                vol = np.std(returns) if len(returns) > 0 else 0
            else:
                vol = 0

            # Time of day (0-1)
            if book.timestamp > 0:
                dt = datetime.fromtimestamp(book.timestamp, tz=PKT)
                minutes_since_open = (dt.hour - 9) * 60 + dt.minute - 30
                time_frac = max(0, min(1, minutes_since_open / 360))
            else:
                time_frac = 0.5

            # VWAP distance
            vwap = self.vwap_num / self.vwap_den if self.vwap_den > 0 else mid
            vwap_dist = (mid - vwap) / mid

            # Trade intensity
            trade_intensity = sum(self.trade_flags) / max(1, len(self.trade_flags))

            obs = np.array([
                mid / 100,                     # normalized mid price
                book.spread / TICK_SIZE,        # spread in ticks
                book.imbalance,                 # book imbalance
                (book.bids[0].volume if book.bids else 0) / 10000,  # bid vol
                (book.asks[0].volume if book.asks else 0) / 10000,  # ask vol
                np.sign(book.last_price - (prices[-2] if len(prices) >= 2 else book.last_price)),
                mom_5 * 100,
                mom_20 * 100,
                vol * 100,
                time_frac,
                self.position / 5000,           # normalized position
                ((self.position * (mid - self.entry_price)) / mid) if self.entry_price > 0 else 0,
                vwap_dist * 100,
                trade_intensity,
            ], dtype=np.float32)

            return obs

    return PSXLimitOrderEnv(symbol, date_str)


def train_rl_agent(
    symbol: str = "OGDC",
    date_str: str = None,
    total_timesteps: int = 100_000,
    algorithm: str = "PPO",
) -> dict:
    """
    Train an RL agent for limit order placement.

    Uses Stable-Baselines3 with PPO or DQN.
    """
    try:
        from stable_baselines3 import PPO, DQN
        from stable_baselines3.common.vec_env import DummyVecEnv
    except ImportError:
        return {"error": "stable-baselines3 not installed. pip install stable-baselines3"}

    env = create_rl_environment(symbol, date_str)
    if env is None:
        return {"error": "Failed to create environment"}

    vec_env = DummyVecEnv([lambda: env])

    # Detect device
    try:
        import torch
        device = "cuda" if torch.cuda.is_available() else "cpu"
    except ImportError:
        device = "cpu"

    if algorithm == "PPO":
        model = PPO(
            "MlpPolicy", vec_env,
            learning_rate=3e-4,
            n_steps=2048,
            batch_size=64,
            n_epochs=10,
            gamma=0.99,
            gae_lambda=0.95,
            clip_range=0.2,
            verbose=1,
            device=device,
        )
    elif algorithm == "DQN":
        model = DQN(
            "MlpPolicy", vec_env,
            learning_rate=1e-4,
            buffer_size=50000,
            batch_size=32,
            gamma=0.99,
            exploration_fraction=0.3,
            verbose=1,
            device=device,
        )
    else:
        return {"error": f"Unknown algorithm: {algorithm}"}

    print(f"Training {algorithm} on {symbol} for {total_timesteps} timesteps (device={device})...")

    # Capture per-iteration metrics via callback
    training_log = []

    class _LogCallback:
        def __init__(self):
            self.n_calls = 0
        def __call__(self, _locals, _globals):
            self.n_calls += 1
            if "self" in _locals:
                m = _locals["self"]
                entry = {"iteration": self.n_calls,
                         "timesteps": getattr(m, "num_timesteps", 0)}
                if hasattr(m, "logger") and hasattr(m.logger, "name_to_value"):
                    for k, v in m.logger.name_to_value.items():
                        entry[k.replace("/", "_")] = v
                training_log.append(entry)
            return True

    cb = _LogCallback()
    model.learn(total_timesteps=total_timesteps, callback=cb)

    # Save model
    model_path = Path.home() / "pakfindata" / "models" / f"rl_orderbook_{symbol}_{algorithm}.zip"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save(str(model_path))

    # Evaluate
    obs, _ = env.reset()
    total_reward = 0
    done = False
    while not done:
        action, _ = model.predict(obs, deterministic=True)
        obs, reward, done, _, info = env.step(action)
        total_reward += reward

    result = {
        "model_path": str(model_path),
        "algorithm": algorithm,
        "total_timesteps": total_timesteps,
        "total_reward": total_reward,
        "trades": len(env.trades_executed),
        "final_position": env.position,
        "limit_order_fills": sum(1 for t in env.trades_executed if t["type"] == "LIMIT"),
        "market_orders": sum(1 for t in env.trades_executed if t["type"] == "MARKET"),
    }

    # Auto-save RL history to Parquet
    save_rl_training_history(
        symbol=symbol,
        algorithm=algorithm,
        total_timesteps=total_timesteps,
        result=result,
        eval_trades=env.trades_executed if env.trades_executed else None,
        training_log=training_log if training_log else None,
    )

    return result


# --- Book Analytics ---

def analyze_book_quality(symbol: str, date_str: str = None) -> dict:
    """
    Analyze how much information we can extract from Level 1 data.
    Measures: spread distribution, imbalance predictiveness, fill rates.
    """
    books = reconstruct_book_history(symbol, date_str)
    if not books:
        return {"error": "No data"}

    spreads = [b.spread for b in books if b.spread > 0]
    imbalances = [b.imbalance for b in books]
    prices = [b.last_price for b in books if b.last_price > 0]

    # Does imbalance predict next-tick direction?
    correct = 0
    total = 0
    for i in range(1, len(books)):
        if abs(books[i-1].imbalance) > 0.2:
            direction = np.sign(books[i].last_price - books[i-1].last_price)
            predicted = np.sign(books[i-1].imbalance)
            if direction != 0:
                total += 1
                if direction == predicted:
                    correct += 1

    return {
        "ticks": len(books),
        "avg_spread_ticks": np.mean(spreads) / TICK_SIZE if spreads else 0,
        "median_spread_ticks": np.median(spreads) / TICK_SIZE if spreads else 0,
        "avg_imbalance": np.mean(np.abs(imbalances)),
        "imbalance_predictive_accuracy": correct / total if total > 0 else 0,
        "imbalance_predictions": total,
        "price_range": max(prices) - min(prices) if prices else 0,
        "avg_bid_depth": np.mean([len(b.bids) for b in books]),
        "avg_ask_depth": np.mean([len(b.asks) for b in books]),
    }


# ═══════════════════════════════════════════
# PERSISTENCE — Parquet storage
# ═══════════════════════════════════════════

def save_book_snapshots(books: list[ReconstructedBook], symbol: str,
                        date_str: str) -> Path:
    """
    Save reconstructed book snapshots to Parquet.
    File: /mnt/e/psxdata/simulation/book_snapshots/{symbol}_{date}.parquet

    Columns: timestamp, mid_price, spread, imbalance, last_price, last_volume,
             bid_0_price..bid_4_price, bid_0_vol..bid_4_vol,
             ask_0_price..ask_4_price, ask_0_vol..ask_4_vol
    """
    _ensure_dirs()
    rows = []
    for b in books:
        row = {
            "timestamp": b.timestamp,
            "mid_price": b.mid_price,
            "spread": b.spread,
            "imbalance": b.imbalance,
            "last_price": b.last_price,
            "last_volume": b.last_volume,
            "n_bid_levels": len(b.bids),
            "n_ask_levels": len(b.asks),
        }
        for i in range(5):
            if i < len(b.bids):
                row[f"bid_{i}_price"] = b.bids[i].price
                row[f"bid_{i}_vol"] = b.bids[i].volume
            else:
                row[f"bid_{i}_price"] = None
                row[f"bid_{i}_vol"] = None
            if i < len(b.asks):
                row[f"ask_{i}_price"] = b.asks[i].price
                row[f"ask_{i}_vol"] = b.asks[i].volume
            else:
                row[f"ask_{i}_price"] = None
                row[f"ask_{i}_vol"] = None
        rows.append(row)

    df = pd.DataFrame(rows)
    path = BOOK_CACHE_DIR / f"{symbol}_{date_str}.parquet"
    df.to_parquet(path, index=False)
    return path


def load_book_snapshots(symbol: str, date_str: str) -> Optional[pd.DataFrame]:
    """Load cached book snapshots from Parquet. Returns None if not cached."""
    path = BOOK_CACHE_DIR / f"{symbol}_{date_str}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def list_book_snapshots() -> list[dict]:
    """List all saved book snapshot files."""
    _ensure_dirs()
    files = sorted(BOOK_CACHE_DIR.glob("*.parquet"), reverse=True)
    result = []
    for f in files:
        parts = f.stem.rsplit("_", 1)
        symbol = parts[0] if len(parts) == 2 else f.stem
        date = parts[1] if len(parts) == 2 else "unknown"
        size_mb = f.stat().st_size / (1024 * 1024)
        result.append({"symbol": symbol, "date": date, "file": str(f),
                        "size_mb": round(size_mb, 2)})
    return result


def save_simulation_results(symbol: str, date_str: str, result: dict,
                            trade_log: list[dict],
                            book_summary: list[dict]) -> Path:
    """
    Save simulation results to Parquet.
    Files:
      sim_results/{symbol}_{date}_{timestamp}_trades.parquet  — trade log
      sim_results/{symbol}_{date}_{timestamp}_summary.parquet — book summary per tick
      sim_results/{symbol}_{date}_{timestamp}_meta.parquet    — run metadata
    """
    _ensure_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{symbol}_{date_str}_{ts}"

    # Trade log
    trades_path = SIM_RESULTS_DIR / f"{prefix}_trades.parquet"
    if trade_log:
        pd.DataFrame(trade_log).to_parquet(trades_path, index=False)

    # Book summary (sampled — every Nth tick for manageable size)
    summary_path = SIM_RESULTS_DIR / f"{prefix}_summary.parquet"
    if book_summary:
        pd.DataFrame(book_summary).to_parquet(summary_path, index=False)

    # Metadata
    meta_path = SIM_RESULTS_DIR / f"{prefix}_meta.parquet"
    meta = {**result, "symbol": symbol, "date": date_str, "saved_at": ts}
    # Flatten agent_stats dict for parquet
    agent_stats = meta.pop("agent_stats", {})
    for k, v in agent_stats.items():
        meta[f"agent_{k}_trades"] = v
    pd.DataFrame([meta]).to_parquet(meta_path, index=False)

    return trades_path


def list_simulation_runs() -> list[dict]:
    """List all saved simulation runs."""
    _ensure_dirs()
    meta_files = sorted(SIM_RESULTS_DIR.glob("*_meta.parquet"), reverse=True)
    result = []
    for f in meta_files:
        try:
            meta = pd.read_parquet(f).iloc[0].to_dict()
            prefix = f.stem.replace("_meta", "")
            meta["prefix"] = prefix
            meta["meta_file"] = str(f)
            trades_file = SIM_RESULTS_DIR / f"{prefix}_trades.parquet"
            meta["has_trades"] = trades_file.exists()
            result.append(meta)
        except Exception:
            continue
    return result


def load_simulation_trades(prefix: str) -> Optional[pd.DataFrame]:
    """Load trade log for a specific simulation run."""
    path = SIM_RESULTS_DIR / f"{prefix}_trades.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def save_rl_training_history(symbol: str, algorithm: str,
                             total_timesteps: int, result: dict,
                             eval_trades: list[dict] = None,
                             training_log: list[dict] = None) -> Path:
    """
    Save RL training run to Parquet for reporting.
    Files:
      rl_history/{symbol}_{algo}_{timestamp}_result.parquet  — final metrics
      rl_history/{symbol}_{algo}_{timestamp}_trades.parquet   — evaluation trades
      rl_history/{symbol}_{algo}_{timestamp}_training.parquet — per-iteration log
    """
    _ensure_dirs()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{symbol}_{algorithm}_{ts}"

    # Result metrics
    result_path = RL_HISTORY_DIR / f"{prefix}_result.parquet"
    meta = {
        **result,
        "symbol": symbol,
        "algorithm": algorithm,
        "total_timesteps": total_timesteps,
        "saved_at": ts,
        "model_path": result.get("model_path", ""),
    }
    pd.DataFrame([meta]).to_parquet(result_path, index=False)

    # Evaluation trades
    if eval_trades:
        trades_path = RL_HISTORY_DIR / f"{prefix}_trades.parquet"
        pd.DataFrame(eval_trades).to_parquet(trades_path, index=False)

    # Training log (reward per iteration, loss, etc.)
    if training_log:
        train_path = RL_HISTORY_DIR / f"{prefix}_training.parquet"
        pd.DataFrame(training_log).to_parquet(train_path, index=False)

    return result_path


def list_rl_runs() -> list[dict]:
    """List all saved RL training runs for reporting."""
    _ensure_dirs()
    result_files = sorted(RL_HISTORY_DIR.glob("*_result.parquet"), reverse=True)
    runs = []
    for f in result_files:
        try:
            meta = pd.read_parquet(f).iloc[0].to_dict()
            prefix = f.stem.replace("_result", "")
            meta["prefix"] = prefix
            meta["result_file"] = str(f)
            trades_file = RL_HISTORY_DIR / f"{prefix}_trades.parquet"
            meta["has_eval_trades"] = trades_file.exists()
            train_file = RL_HISTORY_DIR / f"{prefix}_training.parquet"
            meta["has_training_log"] = train_file.exists()
            runs.append(meta)
        except Exception:
            continue
    return runs


def load_rl_eval_trades(prefix: str) -> Optional[pd.DataFrame]:
    """Load evaluation trades for a specific RL run."""
    path = RL_HISTORY_DIR / f"{prefix}_trades.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def load_rl_training_log(prefix: str) -> Optional[pd.DataFrame]:
    """Load training log for a specific RL run."""
    path = RL_HISTORY_DIR / f"{prefix}_training.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None
