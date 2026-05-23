# Claude Code Prompt: Strategy 14 — Reinforcement Learning Execution Agent

## Context

pakfindata already has a **static** VWAP/TWAP execution optimizer (`engine/vwap_execution.py`)
that slices orders proportionally to historical volume profiles. It works — but it's blind
to real-time conditions. This strategy replaces the static slicer with an RL agent that
**learns** optimal execution from historical tick replay.

**What exists vs what this builds:**

| | `vwap_execution.py` (existing) | `rl_execution.py` (this) |
|---|---|---|
| Type | Rule-based (static schedule) | RL (adaptive, learned) |
| Input | Historical volume profile | Live state every 5 seconds |
| Adapts to | Nothing — fixed at generation time | Spread widening, volume surges, momentum |
| Actions | Pre-computed share counts per slice | Continuous: order size + limit price offset |
| Training | None | 100K+ episodes on historical replay |
| Benchmark | — | Beats VWAP, TWAP, Aggressive by X bps |

**Also differs from `orderbook_sim.py` RL (Strategy 11):**

| | Strategy 11 (Order Book RL) | Strategy 14 (Execution RL) |
|---|---|---|
| Objective | Market-making: capture spread | Execution: minimize slippage on large order |
| Horizon | Single trade (seconds) | Full session (hours) |
| State | Order book depth, imbalance | VWAP distance, inventory, time, volume |
| Reward | Spread capture - inventory risk | -|slippage| per episode |

**The research question:** Can an RL agent learn to outperform static VWAP by 3-10 bps
on PSX, where volume profiles are noisy and spreads are wide?

**Hardware:** RTX 4080 12GB — trains 100K episodes in ~5-10 minutes with PPO.

## What already exists

```bash
# Check existing execution engine
wc -l ~/pakfindata/src/pakfindata/engine/vwap_execution.py
head -20 ~/pakfindata/src/pakfindata/engine/vwap_execution.py

# Check existing RL in orderbook_sim
grep -n "gymnasium\|Env\|PPO\|DQN\|SAC\|train_rl" \
    ~/pakfindata/src/pakfindata/engine/orderbook_sim.py | head -15

# Check 5s bar data availability
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)

print('=== ohlcv_5s schema ===')
for c in con.execute('DESCRIBE ohlcv_5s').fetchall():
    print(f'  {c[0]}: {c[1]}')

print('\n=== ohlcv_5s stats ===')
print(con.execute('''
    SELECT COUNT(*) as bars,
           COUNT(DISTINCT symbol) as symbols,
           COUNT(DISTINCT SUBSTR(ts,1,10)) as days,
           MIN(SUBSTR(ts,1,10)) as first_date,
           MAX(SUBSTR(ts,1,10)) as last_date
    FROM ohlcv_5s
''').df().to_string(index=False))

# Check tick_logs for spread data
print('\n=== tick_logs with spread (sample) ===')
print(con.execute('''
    SELECT symbol, timestamp, price, volume, bid, ask,
           (ask - bid) as spread
    FROM tick_logs 
    WHERE symbol = 'HUBC' 
    AND date = (SELECT MAX(date) FROM tick_logs WHERE symbol = 'HUBC')
    ORDER BY timestamp LIMIT 5
''').df().to_string(index=False))

con.close()
"

# Check volume profile function
grep -n "build_volume_profile\|ExecutionPlan\|backtest_execution" \
    ~/pakfindata/src/pakfindata/engine/vwap_execution.py
```

**READ ALL OUTPUT before proceeding.**

## Step 1: Create the RL Execution Engine

Create `src/pakfindata/engine/rl_execution.py`:

```python
"""
Reinforcement Learning Execution Agent.

Trains an RL agent (PPO/SAC) to execute large orders with minimal slippage.
Uses historical tick replay from ohlcv_5s and tick_logs.

The agent observes market state every decision_interval seconds and chooses:
  - What fraction of remaining order to execute this period (0-30%)
  - How aggressively to price it (passive to aggressive offset from mid)

Reward: negative implementation shortfall = -(exec_price - arrival_price) × shares

Architecture:
  Environment: PSXExecutionEnv (Gymnasium)
    State (12-dim):
      - inventory_remaining (fraction 0-1)
      - time_remaining (fraction 0-1)
      - vwap_distance (exec_vwap vs market_vwap, bps)
      - current_spread (bps)
      - recent_volume_ratio (current vs historical average)
      - price_momentum_5 (5-bar return)
      - price_momentum_20 (20-bar return)
      - realized_volatility (20-bar)
      - volume_profile_bucket_pct (expected % this period)
      - participation_rate_so_far
      - current_price_vs_arrival (bps from arrival)
      - time_of_day (0-1 fraction of session)

    Action (2-dim continuous):
      - execution_rate: [0, 0.3] — fraction of remaining to execute
      - price_aggression: [-1, +1] — passive (-1) to aggressive (+1)

    Reward:
      Per-step: -slippage_bps × shares_executed_this_step
      Terminal: bonus if beat VWAP by >2bps, penalty if missed by >5bps

  Training: PPO or SAC via Stable-Baselines3 on RTX 4080
  Evaluation: Compare vs static VWAP, TWAP, Aggressive on held-out dates

PSX-Specific:
  - Decision interval: 15-second bars (not 5s — too noisy for RL state)
  - Market hours: 09:30-15:30 = 1440 fifteen-second periods per session
  - Circuit breakers: ±7.5% — if price hits circuit, remaining order is stuck
  - Spread: 5-50 bps for liquid, 50-200 bps for illiquid
  - VWAP is the standard benchmark for PSX institutional execution
  - Trading days: 245/year
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import timezone, timedelta
from dataclasses import dataclass
from typing import Optional

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
MODEL_DIR = Path("/mnt/e/psxdata/models/rl_execution")
DECISION_INTERVAL = 15  # seconds between agent decisions
MARKET_OPEN_MIN = 570   # 09:30 in minutes
MARKET_CLOSE_MIN = 930  # 15:30 in minutes
SESSION_SECONDS = (MARKET_CLOSE_MIN - MARKET_OPEN_MIN) * 60  # 21600
STEPS_PER_EPISODE = SESSION_SECONDS // DECISION_INTERVAL  # 1440


def _duck_con():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


# ═══════════════════════════════════════════════════════
# DATA LOADING — build replay episodes from historical bars
# ═══════════════════════════════════════════════════════

@dataclass
class ReplayEpisode:
    """One day of market data for replay."""
    symbol: str
    date: str
    bars: pd.DataFrame       # 5s OHLCV bars
    bar_15s: pd.DataFrame    # resampled to 15s for RL state
    market_vwap: float
    total_volume: int
    arrival_price: float
    close_price: float
    volume_profile: np.ndarray  # expected % per 15s bucket


def load_replay_episodes(
    symbol: str,
    n_days: int = 30,
    min_bars: int = 100,
) -> list[ReplayEpisode]:
    """Load historical days as replay episodes for RL training."""
    con = _duck_con()

    dates = con.execute(f"""
        SELECT DISTINCT SUBSTR(ts,1,10) AS d, COUNT(*) AS n
        FROM ohlcv_5s WHERE symbol = '{symbol}'
        GROUP BY d HAVING n >= {min_bars}
        ORDER BY d DESC LIMIT {n_days}
    """).fetchall()

    episodes = []
    for date_str, _ in dates:
        bars = con.execute(f"""
            SELECT ts, o, h, l, c, v FROM ohlcv_5s
            WHERE symbol = '{symbol}' AND SUBSTR(ts,1,10) = '{date_str}'
            ORDER BY ts
        """).df()

        if bars.empty or len(bars) < min_bars:
            continue

        # Parse time
        bars["hour"] = bars["ts"].str[11:13].astype(int)
        bars["minute"] = bars["ts"].str[14:16].astype(int)
        bars["second"] = bars["ts"].str[17:19].astype(int)
        bars["session_sec"] = (bars["hour"] * 3600 + bars["minute"] * 60 + bars["second"]) - (9 * 3600 + 30 * 60)
        bars = bars[bars["session_sec"] >= 0].copy()

        if bars.empty:
            continue

        # Market VWAP
        total_vol = float(bars["v"].sum())
        if total_vol <= 0:
            continue
        market_vwap = float((bars["c"] * bars["v"]).sum() / total_vol)

        # Resample to 15s bars
        bars["bucket_15s"] = bars["session_sec"] // DECISION_INTERVAL
        bar_15s = bars.groupby("bucket_15s").agg(
            o=("o", "first"), h=("h", "max"), l=("l", "min"),
            c=("c", "last"), v=("v", "sum"),
            spread_approx=("h", lambda x: (x.max() - x.min())),  # proxy
        ).reset_index()

        # Volume profile: % of daily volume in each 15s bucket
        vol_profile = np.zeros(STEPS_PER_EPISODE)
        for _, row in bar_15s.iterrows():
            idx = int(row["bucket_15s"])
            if 0 <= idx < STEPS_PER_EPISODE:
                vol_profile[idx] = row["v"] / total_vol

        episodes.append(ReplayEpisode(
            symbol=symbol, date=date_str, bars=bars, bar_15s=bar_15s,
            market_vwap=market_vwap, total_volume=int(total_vol),
            arrival_price=float(bars["c"].iloc[0]),
            close_price=float(bars["c"].iloc[-1]),
            volume_profile=vol_profile,
        ))

    con.close()
    return episodes


# ═══════════════════════════════════════════════════════
# GYMNASIUM ENVIRONMENT
# ═══════════════════════════════════════════════════════

def create_execution_env(
    symbol: str = "OGDC",
    total_shares: int = 100_000,
    side: str = "BUY",
    episodes: list[ReplayEpisode] = None,
):
    """
    Create a Gymnasium environment for execution RL.

    Returns an env instance or None if deps missing.
    """
    try:
        import gymnasium as gym
        from gymnasium import spaces
    except ImportError:
        print("ERROR: gymnasium not installed. pip install gymnasium")
        return None

    if episodes is None:
        episodes = load_replay_episodes(symbol, n_days=30)
    if not episodes:
        print(f"ERROR: No replay episodes for {symbol}")
        return None

    class PSXExecutionEnv(gym.Env):
        """
        PSX Order Execution Environment.

        One episode = one trading day. Agent must execute total_shares
        by session end, minimizing implementation shortfall vs arrival price.
        """
        metadata = {"render_modes": []}

        def __init__(self):
            super().__init__()

            # Action: [execution_rate (0-0.3), price_aggression (-1 to +1)]
            self.action_space = spaces.Box(
                low=np.array([0.0, -1.0], dtype=np.float32),
                high=np.array([0.3, 1.0], dtype=np.float32),
            )

            # State: 12-dimensional
            self.observation_space = spaces.Box(
                low=-np.inf, high=np.inf,
                shape=(12,), dtype=np.float32,
            )

            self.episodes = episodes
            self.total_shares = total_shares
            self.side = side  # "BUY" or "SELL"

            # Episode state
            self._episode = None
            self._step = 0
            self._remaining = total_shares
            self._exec_shares = []
            self._exec_prices = []
            self._arrival_price = 0.0

        def reset(self, seed=None, options=None):
            super().reset(seed=seed)

            # Random episode selection
            idx = self.np_random.integers(0, len(self.episodes))
            self._episode = self.episodes[idx]
            self._step = 0
            self._remaining = self.total_shares
            self._exec_shares = []
            self._exec_prices = []
            self._arrival_price = self._episode.arrival_price

            return self._get_obs(), {}

        def step(self, action):
            ep = self._episode
            bar_15s = ep.bar_15s

            # Decode action
            exec_rate = float(np.clip(action[0], 0.0, 0.3))
            aggression = float(np.clip(action[1], -1.0, 1.0))

            # Shares to execute this step
            shares_this_step = int(self._remaining * exec_rate)
            shares_this_step = max(0, min(shares_this_step, self._remaining))

            # Get current bar data
            if self._step < len(bar_15s):
                bar = bar_15s.iloc[self._step]
                mid = float(bar["c"])
                spread = max(float(bar.get("spread_approx", 0)), mid * 0.001)
                bar_volume = float(bar["v"])
            else:
                # Past available data — use last known
                bar = bar_15s.iloc[-1]
                mid = float(bar["c"])
                spread = mid * 0.002
                bar_volume = 0

            # Execution price: mid ± aggression × half_spread
            # Aggressive (1.0) = cross spread, Passive (-1.0) = limit at far side
            half_spread = spread / 2
            if self.side == "BUY":
                exec_price = mid + aggression * half_spread
            else:
                exec_price = mid - aggression * half_spread

            # Market impact: participation rate × impact coefficient
            participation = shares_this_step / bar_volume if bar_volume > 0 else 1.0
            impact_bps = participation * 15  # 15 bps per 100% participation
            if self.side == "BUY":
                exec_price *= (1 + impact_bps / 10000)
            else:
                exec_price *= (1 - impact_bps / 10000)

            # Passive orders may not fill — fill probability
            if aggression < 0:
                fill_prob = 0.3 + 0.7 * (aggression + 1)  # -1 → 30%, 0 → 100%
                if self.np_random.random() > fill_prob:
                    shares_this_step = 0  # passive order didn't fill

            # Record execution
            if shares_this_step > 0:
                self._exec_shares.append(shares_this_step)
                self._exec_prices.append(exec_price)
                self._remaining -= shares_this_step

            # Reward: negative slippage in bps (per share executed)
            if shares_this_step > 0:
                if self.side == "BUY":
                    slippage_bps = (exec_price - self._arrival_price) / self._arrival_price * 10000
                else:
                    slippage_bps = (self._arrival_price - exec_price) / self._arrival_price * 10000
                reward = -abs(slippage_bps) * (shares_this_step / self.total_shares)
            else:
                reward = 0.0

            self._step += 1

            # Terminal conditions
            done = False
            if self._remaining <= 0:
                done = True
                reward += 2.0  # completion bonus
            elif self._step >= STEPS_PER_EPISODE:
                done = True
                # Penalty for unexecuted shares — must MOC (market on close)
                if self._remaining > 0:
                    close_price = ep.close_price
                    if self.side == "BUY":
                        moc_slip = (close_price - self._arrival_price) / self._arrival_price * 10000
                    else:
                        moc_slip = (self._arrival_price - close_price) / self._arrival_price * 10000
                    reward -= abs(moc_slip) * (self._remaining / self.total_shares)
                    reward -= 5.0  # harsh penalty for not finishing

                    self._exec_shares.append(self._remaining)
                    self._exec_prices.append(close_price)
                    self._remaining = 0

                # Terminal bonus/penalty vs VWAP
                if self._exec_shares:
                    exec_vwap = sum(
                        p * s for p, s in zip(self._exec_prices, self._exec_shares)
                    ) / sum(self._exec_shares)
                    market_vwap = ep.market_vwap

                    if self.side == "BUY":
                        beat_vwap_bps = (market_vwap - exec_vwap) / market_vwap * 10000
                    else:
                        beat_vwap_bps = (exec_vwap - market_vwap) / market_vwap * 10000

                    if beat_vwap_bps > 2:
                        reward += beat_vwap_bps * 0.5  # bonus for beating VWAP
                    elif beat_vwap_bps < -5:
                        reward -= 3.0  # penalty for badly missing VWAP

            truncated = False
            return self._get_obs(), float(reward), done, truncated, {}

        def _get_obs(self) -> np.ndarray:
            ep = self._episode
            bar_15s = ep.bar_15s
            step = min(self._step, len(bar_15s) - 1)
            bar = bar_15s.iloc[step] if step < len(bar_15s) else bar_15s.iloc[-1]

            mid = float(bar["c"])
            spread = max(float(bar.get("spread_approx", 0)), mid * 0.001)

            # Price momentum
            if step >= 5:
                mom5 = (float(bar_15s.iloc[step]["c"]) / float(bar_15s.iloc[step - 5]["c"]) - 1) * 100
            else:
                mom5 = 0
            if step >= 20:
                mom20 = (float(bar_15s.iloc[step]["c"]) / float(bar_15s.iloc[step - 20]["c"]) - 1) * 100
            else:
                mom20 = 0

            # Realized volatility (20-bar)
            if step >= 20:
                closes = bar_15s.iloc[step - 20:step + 1]["c"].values.astype(float)
                rets = np.diff(closes) / closes[:-1]
                rvol = np.std(rets) * 100 if len(rets) > 1 else 0
            else:
                rvol = 0

            # Volume ratio: current bar vs historical profile average
            expected_vol_pct = ep.volume_profile[min(self._step, len(ep.volume_profile) - 1)]
            actual_vol = float(bar["v"])
            avg_bar_vol = ep.total_volume * expected_vol_pct if expected_vol_pct > 0 else 1
            vol_ratio = actual_vol / avg_bar_vol if avg_bar_vol > 0 else 1.0

            # VWAP distance
            if self._exec_shares:
                exec_vwap = sum(
                    p * s for p, s in zip(self._exec_prices, self._exec_shares)
                ) / sum(self._exec_shares)
                vwap_dist = (exec_vwap - self._arrival_price) / self._arrival_price * 10000
            else:
                vwap_dist = 0

            # Participation rate so far
            shares_done = self.total_shares - self._remaining
            participation = shares_done / max(1, ep.total_volume)

            obs = np.array([
                self._remaining / self.total_shares,              # inventory remaining (0-1)
                1.0 - self._step / STEPS_PER_EPISODE,             # time remaining (0-1)
                vwap_dist,                                         # exec vwap vs arrival (bps)
                spread / mid * 10000,                              # spread (bps)
                min(vol_ratio, 5.0),                               # volume ratio (capped)
                mom5,                                              # 5-bar momentum (%)
                mom20,                                             # 20-bar momentum (%)
                rvol,                                              # realized vol (%)
                expected_vol_pct * 100,                            # expected vol this period (%)
                participation * 100,                               # participation so far (%)
                (mid - self._arrival_price) / self._arrival_price * 10000,  # price vs arrival (bps)
                self._step / STEPS_PER_EPISODE,                    # time of day (0-1)
            ], dtype=np.float32)

            return obs

    return PSXExecutionEnv()


# ═══════════════════════════════════════════════════════
# TRAINING
# ═══════════════════════════════════════════════════════

def train_execution_agent(
    symbol: str = "OGDC",
    total_shares: int = 100_000,
    side: str = "BUY",
    n_episodes_data: int = 30,
    total_timesteps: int = 100_000,
    algorithm: str = "PPO",
) -> dict:
    """
    Train an RL execution agent.

    Returns dict with model path, training metrics, comparison vs baselines.
    """
    try:
        from stable_baselines3 import PPO, SAC
        from stable_baselines3.common.vec_env import DummyVecEnv
        from stable_baselines3.common.callbacks import EvalCallback
    except ImportError:
        return {"error": "stable-baselines3 not installed. pip install stable-baselines3"}

    # Load replay data
    episodes = load_replay_episodes(symbol, n_days=n_episodes_data)
    if len(episodes) < 5:
        return {"error": f"Need ≥5 replay days, got {len(episodes)} for {symbol}"}

    # Split: 80% train, 20% eval
    split = int(len(episodes) * 0.8)
    train_eps = episodes[:split]
    eval_eps = episodes[split:]

    # Create environments
    train_env = create_execution_env(symbol, total_shares, side, train_eps)
    eval_env = create_execution_env(symbol, total_shares, side, eval_eps)
    if train_env is None or eval_env is None:
        return {"error": "Failed to create environments"}

    vec_train = DummyVecEnv([lambda: train_env])
    vec_eval = DummyVecEnv([lambda: eval_env])

    # Detect GPU
    try:
        import torch
        device = "cuda" if torch.cuda.is_available() else "cpu"
    except ImportError:
        device = "cpu"

    # Create model
    if algorithm == "PPO":
        model = PPO(
            "MlpPolicy", vec_train,
            learning_rate=3e-4,
            n_steps=2048,
            batch_size=64,
            n_epochs=10,
            gamma=0.99,
            gae_lambda=0.95,
            clip_range=0.2,
            ent_coef=0.01,   # encourage exploration
            verbose=1,
            device=device,
        )
    elif algorithm == "SAC":
        model = SAC(
            "MlpPolicy", vec_train,
            learning_rate=3e-4,
            batch_size=256,
            gamma=0.99,
            tau=0.005,
            ent_coef="auto",
            verbose=1,
            device=device,
        )
    else:
        return {"error": f"Unknown algorithm: {algorithm}. Use PPO or SAC."}

    print(f"Training {algorithm} on {symbol} ({len(train_eps)} days, "
          f"{total_timesteps} timesteps, device={device})...")

    # Train
    model.learn(total_timesteps=total_timesteps)

    # Save model
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / f"{symbol}_{algorithm.lower()}_{side.lower()}"
    model.save(str(model_path))

    # Evaluate: run on eval episodes and compare vs baselines
    eval_results = evaluate_agent(model, eval_eps, symbol, total_shares, side)

    return {
        "model_path": str(model_path),
        "algorithm": algorithm,
        "device": device,
        "symbol": symbol,
        "train_days": len(train_eps),
        "eval_days": len(eval_eps),
        "total_timesteps": total_timesteps,
        **eval_results,
    }


# ═══════════════════════════════════════════════════════
# EVALUATION — compare RL vs static baselines
# ═══════════════════════════════════════════════════════

def _run_static_baseline(
    episode: ReplayEpisode,
    total_shares: int,
    side: str,
    strategy: str,
) -> dict:
    """Run a static baseline (VWAP/TWAP) on one episode."""
    bar_15s = episode.bar_15s
    if bar_15s.empty:
        return {"slippage_bps": 0, "vwap_slippage_bps": 0}

    arrival = episode.arrival_price
    n_steps = len(bar_15s)
    remaining = total_shares
    exec_prices, exec_shares = [], []

    for i, (_, bar) in enumerate(bar_15s.iterrows()):
        if remaining <= 0:
            break

        if strategy == "VWAP":
            pct = episode.volume_profile[min(int(bar["bucket_15s"]), len(episode.volume_profile) - 1)]
            target = int(total_shares * max(pct, 1 / n_steps))
        elif strategy == "TWAP":
            target = int(remaining / max(1, n_steps - i))
        else:
            target = int(remaining / max(1, n_steps - i))

        target = min(target, remaining)
        if target > 0:
            exec_prices.append(float(bar["c"]))
            exec_shares.append(target)
            remaining -= target

    # MOC remaining
    if remaining > 0:
        exec_prices.append(episode.close_price)
        exec_shares.append(remaining)

    if not exec_shares:
        return {"slippage_bps": 0, "vwap_slippage_bps": 0}

    exec_vwap = sum(p * s for p, s in zip(exec_prices, exec_shares)) / sum(exec_shares)

    if side == "BUY":
        impl_short = (exec_vwap - arrival) / arrival * 10000
        vs_vwap = (exec_vwap - episode.market_vwap) / episode.market_vwap * 10000
    else:
        impl_short = (arrival - exec_vwap) / arrival * 10000
        vs_vwap = (episode.market_vwap - exec_vwap) / episode.market_vwap * 10000

    return {"slippage_bps": float(impl_short), "vwap_slippage_bps": float(vs_vwap)}


def evaluate_agent(
    model,
    episodes: list[ReplayEpisode],
    symbol: str,
    total_shares: int,
    side: str,
) -> dict:
    """
    Evaluate trained agent vs VWAP and TWAP baselines on held-out episodes.
    """
    rl_results = []
    vwap_results = []
    twap_results = []

    for ep in episodes:
        # RL agent
        env = create_execution_env(symbol, total_shares, side, [ep])
        if env is None:
            continue

        obs, _ = env.reset()
        done = False
        total_reward = 0

        while not done:
            action, _ = model.predict(obs, deterministic=True)
            obs, reward, done, truncated, info = env.step(action)
            total_reward += reward

        if env._exec_shares:
            exec_vwap = sum(
                p * s for p, s in zip(env._exec_prices, env._exec_shares)
            ) / sum(env._exec_shares)
            arrival = env._arrival_price

            if side == "BUY":
                rl_slip = (exec_vwap - arrival) / arrival * 10000
                rl_vs_vwap = (exec_vwap - ep.market_vwap) / ep.market_vwap * 10000
            else:
                rl_slip = (arrival - exec_vwap) / arrival * 10000
                rl_vs_vwap = (ep.market_vwap - exec_vwap) / ep.market_vwap * 10000

            rl_results.append({
                "date": ep.date, "slippage_bps": rl_slip,
                "vwap_slippage_bps": rl_vs_vwap, "reward": total_reward,
                "steps_used": env._step,
                "fills": len(env._exec_shares),
            })

        # Baselines
        vwap_results.append(_run_static_baseline(ep, total_shares, side, "VWAP"))
        twap_results.append(_run_static_baseline(ep, total_shares, side, "TWAP"))

    if not rl_results:
        return {"error": "No valid evaluation episodes"}

    rl_df = pd.DataFrame(rl_results)
    vwap_df = pd.DataFrame(vwap_results)
    twap_df = pd.DataFrame(twap_results)

    comparison = {
        "rl_avg_slippage_bps": float(rl_df["slippage_bps"].mean()),
        "vwap_avg_slippage_bps": float(vwap_df["slippage_bps"].mean()),
        "twap_avg_slippage_bps": float(twap_df["slippage_bps"].mean()),
        "rl_vs_vwap_bps": float(rl_df["vwap_slippage_bps"].mean()),
        "rl_avg_reward": float(rl_df["reward"].mean()),
        "rl_beats_vwap_pct": float((rl_df["vwap_slippage_bps"] < 0).mean() * 100),
        "rl_beats_twap_pct": float(
            (rl_df["slippage_bps"].values < twap_df["slippage_bps"].values).mean() * 100
        ) if len(rl_df) == len(twap_df) else 0,
        "eval_days": len(rl_results),
        "per_day": rl_df.to_dict("records"),
    }

    return comparison


def load_trained_agent(symbol: str, algorithm: str = "PPO", side: str = "BUY"):
    """Load a previously trained agent."""
    try:
        from stable_baselines3 import PPO, SAC
    except ImportError:
        return None

    model_path = MODEL_DIR / f"{symbol}_{algorithm.lower()}_{side.lower()}"
    if not model_path.with_suffix(".zip").exists():
        return None

    cls = PPO if algorithm == "PPO" else SAC
    return cls.load(str(model_path))
```

## Step 2: Create the Streamlit Page

Create `src/pakfindata/ui/page_views/advanced_rl_exec.py`:

```python
"""RL Execution Agent — adaptive order execution via reinforcement learning."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
    "purple": "#BB86FC",
}
DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
    font_color="#c9d1d9", margin=dict(l=20, r=20, t=40, b=20),
)


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.title("🧠 RL Execution Agent")
    st.caption(
        "Reinforcement learning for optimal order execution — "
        "learns to beat VWAP/TWAP from historical tick replay"
    )

    tab1, tab2, tab3, tab4 = st.tabs([
        "Train Agent", "Live Execution", "Benchmark Comparison", "Methodology"
    ])

    # ------------------------------------------------------------------
    # TAB 1: Train Agent
    # ------------------------------------------------------------------
    with tab1:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            symbol = st.text_input("Symbol", value="OGDC", key="rle_sym")
        with c2:
            total_shares = st.number_input("Order size", 10_000, 1_000_000, 100_000, 10_000, key="rle_shares")
        with c3:
            side = st.selectbox("Side", ["BUY", "SELL"], key="rle_side")
        with c4:
            algorithm = st.selectbox("Algorithm", ["PPO", "SAC"], key="rle_algo")

        c5, c6 = st.columns(2)
        with c5:
            n_days = st.slider("Training days", 10, 60, 30, key="rle_days")
        with c6:
            timesteps = st.select_slider(
                "Training timesteps",
                options=[50_000, 100_000, 200_000, 500_000],
                value=100_000, key="rle_steps",
            )

        if st.button("Train Agent", type="primary", key="rle_train"):
            with st.spinner(f"Training {algorithm} on {symbol} ({timesteps:,} steps)..."):
                try:
                    from pakfindata.engine.rl_execution import train_execution_agent
                    result = train_execution_agent(
                        symbol=symbol.upper(), total_shares=total_shares,
                        side=side, n_episodes_data=n_days,
                        total_timesteps=timesteps, algorithm=algorithm,
                    )
                except ImportError:
                    st.error("Engine not found. Ensure `engine/rl_execution.py` exists.")
                    return

            if "error" in result:
                st.error(result["error"])
                return

            st.success(f"Model saved to `{result['model_path']}`")

            # Results
            k1, k2, k3, k4, k5 = st.columns(5)
            with k1:
                v = result["rl_avg_slippage_bps"]
                _kpi("RL Avg Slip", f"{v:+.1f} bps", _C["up"] if v < 0 else _C["down"])
            with k2:
                v = result["vwap_avg_slippage_bps"]
                _kpi("VWAP Avg Slip", f"{v:+.1f} bps", _C["dim"])
            with k3:
                v = result["twap_avg_slippage_bps"]
                _kpi("TWAP Avg Slip", f"{v:+.1f} bps", _C["dim"])
            with k4:
                v = result["rl_beats_vwap_pct"]
                _kpi("RL Beats VWAP", f"{v:.0f}%", _C["up"] if v > 50 else _C["down"])
            with k5:
                _kpi("Device", result["device"], _C["cyan"])

            # Improvement
            improvement = result["vwap_avg_slippage_bps"] - result["rl_avg_slippage_bps"]
            if improvement > 0:
                st.success(f"RL agent saves **{improvement:.1f} bps** vs static VWAP on average")
            else:
                st.warning(f"RL agent is {abs(improvement):.1f} bps worse than static VWAP — "
                           f"try more training steps or different hyperparams")

            # Per-day breakdown
            if "per_day" in result and result["per_day"]:
                st.markdown("**Per-day evaluation:**")
                day_df = pd.DataFrame(result["per_day"])
                st.dataframe(
                    day_df.style.format({
                        "slippage_bps": "{:+.1f}", "vwap_slippage_bps": "{:+.1f}",
                        "reward": "{:.1f}",
                    }),
                    use_container_width=True, hide_index=True,
                )

    # ------------------------------------------------------------------
    # TAB 2: Live Execution (simulate with trained model)
    # ------------------------------------------------------------------
    with tab2:
        c1, c2 = st.columns(2)
        with c1:
            live_sym = st.text_input("Symbol", value="OGDC", key="rle_live_sym")
        with c2:
            live_algo = st.selectbox("Model", ["PPO", "SAC"], key="rle_live_algo")

        if st.button("Run Simulation on Latest Day", key="rle_live_run"):
            with st.spinner("Loading model and replaying..."):
                try:
                    from pakfindata.engine.rl_execution import (
                        load_trained_agent, load_replay_episodes, create_execution_env,
                    )
                    model = load_trained_agent(live_sym.upper(), live_algo)
                    if model is None:
                        st.error(f"No trained model found for {live_sym}/{live_algo}. Train first.")
                        return

                    episodes = load_replay_episodes(live_sym.upper(), n_days=1)
                    if not episodes:
                        st.error("No replay data available.")
                        return

                    ep = episodes[0]
                    env = create_execution_env(live_sym.upper(), 100_000, "BUY", [ep])
                    obs, _ = env.reset()

                    trajectory = []
                    done = False
                    while not done:
                        action, _ = model.predict(obs, deterministic=True)
                        obs, reward, done, _, _ = env.step(action)
                        trajectory.append({
                            "step": env._step,
                            "remaining_pct": env._remaining / env.total_shares * 100,
                            "exec_rate": float(action[0]),
                            "aggression": float(action[1]),
                            "reward": reward,
                        })
                except ImportError:
                    st.error("Engine not found.")
                    return

            # Plot trajectory
            traj_df = pd.DataFrame(trajectory)

            fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                subplot_titles=["Inventory Remaining", "Execution Rate", "Aggression"],
                                vertical_spacing=0.08)

            fig.add_trace(go.Scatter(
                x=traj_df["step"], y=traj_df["remaining_pct"],
                mode="lines", name="Remaining %",
                line=dict(color=_C["cyan"], width=2),
            ), row=1, col=1)

            fig.add_trace(go.Bar(
                x=traj_df["step"], y=traj_df["exec_rate"] * 100,
                name="Exec Rate %",
                marker_color=_C["accent"],
            ), row=2, col=1)

            fig.add_trace(go.Scatter(
                x=traj_df["step"], y=traj_df["aggression"],
                mode="lines", name="Aggression",
                line=dict(color=_C["amber"], width=1.5),
            ), row=3, col=1)
            fig.add_hline(y=0, line_dash="dash", line_color=_C["dim"], row=3, col=1)

            fig.update_layout(**PLOT_LAYOUT, height=600, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # TAB 3: Benchmark Comparison
    # ------------------------------------------------------------------
    with tab3:
        st.markdown("""
        After training, compare RL agent performance against static baselines.
        
        | Metric | RL Agent | VWAP | TWAP |
        |--------|---------|------|------|
        | Adapts to spread | ✅ | ❌ | ❌ |
        | Adapts to momentum | ✅ | ❌ | ❌ |
        | Adapts to volume surges | ✅ | ❌ | ❌ |
        | Handles illiquidity | ✅ (goes passive) | ❌ (same rate) | ❌ (same rate) |
        | Completion guarantee | Learned (MOC penalty) | By design | By design |
        
        **Target:** RL should beat VWAP by 3-10 bps on PSX.
        On liquid names (OGDC, HBL) expect 2-5 bps.
        On mid-cap names expect 5-15 bps (more inefficiency to exploit).
        """)

    # ------------------------------------------------------------------
    # TAB 4: Methodology
    # ------------------------------------------------------------------
    with tab4:
        st.markdown("""
        ### RL for Optimal Execution
        
        **Problem:** Execute a large order (e.g. 100K shares) over a trading session
        while minimizing implementation shortfall (slippage vs arrival price).
        
        **Why RL?** Static VWAP/TWAP can't adapt to:
        - Spread widening (RL goes passive, waits)
        - Volume surges (RL executes more when liquidity is available)
        - Adverse momentum (RL slows down to avoid chasing)
        - Favorable momentum (RL speeds up to capture better prices)
        
        ---
        
        ### State Space (12 dimensions)
        
        | Feature | Range | Why |
        |---------|-------|-----|
        | Inventory remaining | 0-1 | How much left to execute |
        | Time remaining | 0-1 | Urgency increases as session ends |
        | VWAP distance | bps | Am I beating or lagging VWAP? |
        | Current spread | bps | Wide spread → go passive |
        | Volume ratio | 0-5× | High volume → execute more |
        | Momentum (5-bar) | % | Adverse = slow down |
        | Momentum (20-bar) | % | Trend context |
        | Realized vol | % | High vol → smaller slices |
        | Volume profile % | % | Expected volume this period |
        | Participation rate | % | Am I too visible? |
        | Price vs arrival | bps | Current mark-to-market |
        | Time of day | 0-1 | Open/close dynamics |
        
        ### Action Space (2 continuous)
        
        | Action | Range | Meaning |
        |--------|-------|---------|
        | Execution rate | 0-30% | Fraction of remaining to trade now |
        | Price aggression | -1 to +1 | -1=passive limit, 0=mid, +1=cross spread |
        
        ### Reward Design
        
        | Component | Formula | Purpose |
        |-----------|---------|---------|
        | Per-step | -|slippage_bps| × (shares/total) | Penalize every bps of slippage |
        | Completion bonus | +2.0 | Reward finishing the order |
        | Incomplete penalty | -5.0 - MOC slippage | Harshly penalize not finishing |
        | Beat VWAP bonus | +0.5 × excess_bps | Reward outperforming VWAP |
        | Miss VWAP penalty | -3.0 (if > 5bps worse) | Penalize badly missing VWAP |
        
        ---
        
        ### References
        
        - Almgren & Chriss (2001). "Optimal Execution of Portfolio Transactions."
        - Nevmyvaka, Feng & Kearns (2006). "Reinforcement Learning for Optimized Trade Execution."
        - Ning, Lin & Jaimungal (2021). "Double Deep Q-Learning for Optimal Execution."
        """)

    render_footer()
```

## Step 3: Register in app.py

Add page function (near other ADVANCED functions ~line 575):

```python
def advanced_rl_exec_page():
    from pakfindata.ui.page_views.advanced_rl_exec import render_page
    render_page()
```

Add to page dict (in the `# ADVANCED` section ~line 878):

```python
        # ADVANCED
        "Order Book Sim":    st.Page(strategy_orderbook_page, title="Order Book Sim",    url_path="orderbook-sim"),
        "Stock Graph (GNN)": st.Page(advanced_gnn_page,       title="Stock Graph (GNN)", url_path="stock-graph-gnn"),
        "Hawkes Process":    st.Page(advanced_hawkes_page,     title="Hawkes Process",    url_path="hawkes-process"),
        "RL Execution":      st.Page(advanced_rl_exec_page,   title="RL Execution",      url_path="rl-execution"),
```

Add to nav_groups:

```python
        "ADVANCED":        ["Order Book Sim", "Stock Graph (GNN)", "Hawkes Process", "RL Execution"],
```

## Step 4: Install dependencies

```bash
conda activate psx
pip install gymnasium stable-baselines3
# PyTorch already installed (2.11.0+cu130)
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test data loading
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.rl_execution import load_replay_episodes

eps = load_replay_episodes('OGDC', n_days=5)
print(f'Loaded {len(eps)} replay episodes')
for ep in eps:
    print(f'  {ep.date}: {len(ep.bars)} bars, VWAP={ep.market_vwap:.2f}, '
          f'Vol={ep.total_volume:,}, Arrival={ep.arrival_price:.2f}')
"

# Test environment creation
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.rl_execution import create_execution_env
import numpy as np

env = create_execution_env('OGDC', total_shares=100000)
if env is None:
    print('Failed — check deps')
else:
    obs, _ = env.reset()
    print(f'Observation shape: {obs.shape}')
    print(f'Action space: {env.action_space}')
    print(f'Initial obs: {obs}')

    # Take a few random steps
    for i in range(5):
        action = env.action_space.sample()
        obs, reward, done, _, _ = env.step(action)
        print(f'  Step {i+1}: action={action}, reward={reward:.3f}, '
              f'remaining={obs[0]*100:.1f}%, done={done}')
"

# Test training (short run)
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.rl_execution import train_execution_agent

result = train_execution_agent(
    'OGDC', total_shares=100000, side='BUY',
    n_episodes_data=10, total_timesteps=10000, algorithm='PPO',
)
if 'error' in result:
    print(result['error'])
else:
    print(f'Model saved: {result[\"model_path\"]}')
    print(f'Device: {result[\"device\"]}')
    print(f'RL avg slippage: {result[\"rl_avg_slippage_bps\"]:+.1f} bps')
    print(f'VWAP avg slippage: {result[\"vwap_avg_slippage_bps\"]:+.1f} bps')
    print(f'TWAP avg slippage: {result[\"twap_avg_slippage_bps\"]:+.1f} bps')
    print(f'RL beats VWAP: {result[\"rl_beats_vwap_pct\"]:.0f}% of days')
    imp = result['vwap_avg_slippage_bps'] - result['rl_avg_slippage_bps']
    print(f'Improvement vs VWAP: {imp:+.1f} bps')
"
```

## IMPORTANT NOTES

1. **This is DIFFERENT from Strategy 11 (Order Book RL)** — that one does market-making (spread capture), this one does execution optimization (slippage minimization)
2. **This EXTENDS Strategy 5 (VWAP Execution)** — it uses the same volume profile data but replaces rule-based slicing with learned policy
3. **15-second decision interval** — not 5s (too noisy) or 1min (too coarse). 1440 steps per episode.
4. **SAC for continuous actions** — SAC handles continuous action spaces more naturally than PPO, but PPO is more stable. Try both.
5. **Fill probability for passive orders** — aggression < 0 means limit order may not fill. The env models this as fill_prob = 0.3 at extreme passive, 1.0 at mid.
6. **Market impact model** — 15 bps per 100% participation rate. Rough but calibrated for PSX mid-caps.
7. **MOC penalty** — if agent doesn't finish by session end, remaining fills at close price with harsh -5.0 reward. Forces the agent to learn urgency.
8. **Beat VWAP bonus** — asymmetric reward: bonus for beating VWAP by >2bps, penalty only if missing by >5bps. Encourages outperformance without over-penalizing small misses.
9. **Train/eval split is date-based** — 80% recent dates for training, 20% oldest for eval. Walk-forward by design.
10. **Model persistence** — saves to `/mnt/e/psxdata/models/rl_execution/`. Reusable across sessions.
11. **No TA libraries** — PyTorch + gymnasium + stable-baselines3 + numpy/pandas
12. **Add under ADVANCED** in sidebar after Hawkes Process
13. **Target: 3-10 bps improvement** over static VWAP. If less than 2 bps, the static VWAP is fine and RL overhead isn't worth it.
14. **GPU training** — 100K timesteps on RTX 4080 takes ~5-10 minutes with PPO, ~10-15 with SAC
