"""
Reinforcement Learning Execution Agent.

Trains an RL agent (PPO/SAC) to execute large orders with minimal slippage.
Uses historical tick replay from ohlcv_5s and tick_logs.

The agent observes market state every decision_interval seconds and chooses:
  - What fraction of remaining order to execute this period (0-30%)
  - How aggressively to price it (passive to aggressive offset from mid)

Reward: negative implementation shortfall = -(exec_price - arrival_price) x shares

Architecture:
  Environment: PSXExecutionEnv (Gymnasium)
    State (12-dim): inventory, time, vwap_distance, spread, volume, momentum, vol
    Action (2-dim continuous): execution_rate [0,0.3], price_aggression [-1,+1]
    Reward: -|slippage_bps| per step, terminal bonuses

PSX-Specific:
  - Decision interval: 15-second bars (not 5s -- too noisy for RL state)
  - Market hours: 09:30-15:30 = 1440 fifteen-second periods per session
  - Circuit breakers: +/-7.5%
  - Spread: 5-50 bps for liquid, 50-200 bps for illiquid
  - VWAP is the standard benchmark for PSX institutional execution

References:
  - Almgren & Chriss (2001). "Optimal Execution of Portfolio Transactions."
  - Nevmyvaka, Feng & Kearns (2006). "RL for Optimized Trade Execution."
  - Ning, Lin & Jaimungal (2021). "Double Deep Q-Learning for Optimal Execution."
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import timezone, timedelta
from dataclasses import dataclass

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
# DATA LOADING
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

    dates = con.execute("""
        SELECT SUBSTR(ts,1,10) AS d, COUNT(*) AS n
        FROM ohlcv_5s WHERE symbol = ?
        GROUP BY d HAVING n >= ?
        ORDER BY d DESC LIMIT ?
    """, [symbol, min_bars, n_days]).fetchall()

    episodes = []
    for date_str, _ in dates:
        bars = con.execute("""
            SELECT ts, o, h, l, c, v FROM ohlcv_5s
            WHERE symbol = ? AND SUBSTR(ts,1,10) = ?
            ORDER BY ts
        """, [symbol, date_str]).df()

        if bars.empty or len(bars) < min_bars:
            continue

        # Parse time from ISO ts
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
            spread_approx=("h", lambda x: (x.max() - x.min())),
        ).reset_index()

        # Volume profile
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
    """Create a Gymnasium environment for execution RL."""
    try:
        import gymnasium as gym
        from gymnasium import spaces
    except ImportError:
        return None

    if episodes is None:
        episodes = load_replay_episodes(symbol, n_days=30)
    if not episodes:
        return None

    class PSXExecutionEnv(gym.Env):
        """PSX Order Execution Environment."""
        metadata = {"render_modes": []}

        def __init__(self):
            super().__init__()
            self.action_space = spaces.Box(
                low=np.array([0.0, -1.0], dtype=np.float32),
                high=np.array([0.3, 1.0], dtype=np.float32),
            )
            self.observation_space = spaces.Box(
                low=-np.inf, high=np.inf, shape=(12,), dtype=np.float32,
            )
            self.episodes = episodes
            self.total_shares = total_shares
            self.side = side
            self._episode = None
            self._step = 0
            self._remaining = total_shares
            self._exec_shares = []
            self._exec_prices = []
            self._arrival_price = 0.0

        def reset(self, seed=None, options=None):
            super().reset(seed=seed)
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

            exec_rate = float(np.clip(action[0], 0.0, 0.3))
            aggression = float(np.clip(action[1], -1.0, 1.0))

            shares_this_step = int(self._remaining * exec_rate)
            shares_this_step = max(0, min(shares_this_step, self._remaining))

            if self._step < len(bar_15s):
                bar = bar_15s.iloc[self._step]
                mid = float(bar["c"])
                spread = max(float(bar.get("spread_approx", 0)), mid * 0.001)
                bar_volume = float(bar["v"])
            else:
                bar = bar_15s.iloc[-1]
                mid = float(bar["c"])
                spread = mid * 0.002
                bar_volume = 0

            half_spread = spread / 2
            if self.side == "BUY":
                exec_price = mid + aggression * half_spread
            else:
                exec_price = mid - aggression * half_spread

            # Market impact
            participation = shares_this_step / bar_volume if bar_volume > 0 else 1.0
            impact_bps = participation * 15
            if self.side == "BUY":
                exec_price *= (1 + impact_bps / 10000)
            else:
                exec_price *= (1 - impact_bps / 10000)

            # Passive fill probability
            if aggression < 0:
                fill_prob = 0.3 + 0.7 * (aggression + 1)
                if self.np_random.random() > fill_prob:
                    shares_this_step = 0

            if shares_this_step > 0:
                self._exec_shares.append(shares_this_step)
                self._exec_prices.append(exec_price)
                self._remaining -= shares_this_step

            # Reward
            if shares_this_step > 0:
                if self.side == "BUY":
                    slippage_bps = (exec_price - self._arrival_price) / self._arrival_price * 10000
                else:
                    slippage_bps = (self._arrival_price - exec_price) / self._arrival_price * 10000
                reward = -abs(slippage_bps) * (shares_this_step / self.total_shares)
            else:
                reward = 0.0

            self._step += 1

            done = False
            if self._remaining <= 0:
                done = True
                reward += 2.0
            elif self._step >= STEPS_PER_EPISODE:
                done = True
                if self._remaining > 0:
                    close_price = ep.close_price
                    if self.side == "BUY":
                        moc_slip = (close_price - self._arrival_price) / self._arrival_price * 10000
                    else:
                        moc_slip = (self._arrival_price - close_price) / self._arrival_price * 10000
                    reward -= abs(moc_slip) * (self._remaining / self.total_shares)
                    reward -= 5.0
                    self._exec_shares.append(self._remaining)
                    self._exec_prices.append(close_price)
                    self._remaining = 0

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
                        reward += beat_vwap_bps * 0.5
                    elif beat_vwap_bps < -5:
                        reward -= 3.0

            truncated = False
            return self._get_obs(), float(reward), done, truncated, {}

        def _get_obs(self) -> np.ndarray:
            ep = self._episode
            bar_15s = ep.bar_15s
            step = min(self._step, len(bar_15s) - 1)
            bar = bar_15s.iloc[step] if step < len(bar_15s) else bar_15s.iloc[-1]

            mid = float(bar["c"])
            spread = max(float(bar.get("spread_approx", 0)), mid * 0.001)

            mom5 = (float(bar_15s.iloc[step]["c"]) / float(bar_15s.iloc[step - 5]["c"]) - 1) * 100 if step >= 5 else 0
            mom20 = (float(bar_15s.iloc[step]["c"]) / float(bar_15s.iloc[step - 20]["c"]) - 1) * 100 if step >= 20 else 0

            if step >= 20:
                closes = bar_15s.iloc[step - 20:step + 1]["c"].values.astype(float)
                rets = np.diff(closes) / closes[:-1]
                rvol = np.std(rets) * 100 if len(rets) > 1 else 0
            else:
                rvol = 0

            expected_vol_pct = ep.volume_profile[min(self._step, len(ep.volume_profile) - 1)]
            actual_vol = float(bar["v"])
            avg_bar_vol = ep.total_volume * expected_vol_pct if expected_vol_pct > 0 else 1
            vol_ratio = actual_vol / avg_bar_vol if avg_bar_vol > 0 else 1.0

            if self._exec_shares:
                exec_vwap = sum(p * s for p, s in zip(self._exec_prices, self._exec_shares)) / sum(self._exec_shares)
                vwap_dist = (exec_vwap - self._arrival_price) / self._arrival_price * 10000
            else:
                vwap_dist = 0

            shares_done = self.total_shares - self._remaining
            participation = shares_done / max(1, ep.total_volume)

            obs = np.array([
                self._remaining / self.total_shares,
                1.0 - self._step / STEPS_PER_EPISODE,
                vwap_dist,
                spread / mid * 10000,
                min(vol_ratio, 5.0),
                mom5, mom20, rvol,
                expected_vol_pct * 100,
                participation * 100,
                (mid - self._arrival_price) / self._arrival_price * 10000,
                self._step / STEPS_PER_EPISODE,
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
    """Train an RL execution agent. Returns metrics + model path."""
    try:
        from stable_baselines3 import PPO, SAC
        from stable_baselines3.common.vec_env import DummyVecEnv
    except ImportError:
        return {"error": "stable-baselines3 not installed"}

    episodes = load_replay_episodes(symbol, n_days=n_episodes_data)
    if len(episodes) < 5:
        return {"error": f"Need >=5 replay days, got {len(episodes)} for {symbol}"}

    split = int(len(episodes) * 0.8)
    train_eps = episodes[:split]
    eval_eps = episodes[split:]

    train_env = create_execution_env(symbol, total_shares, side, train_eps)
    eval_env = create_execution_env(symbol, total_shares, side, eval_eps)
    if train_env is None or eval_env is None:
        return {"error": "Failed to create environments"}

    vec_train = DummyVecEnv([lambda: train_env])

    try:
        import torch
        device = "cuda" if torch.cuda.is_available() else "cpu"
    except ImportError:
        device = "cpu"

    if algorithm == "PPO":
        model = PPO(
            "MlpPolicy", vec_train,
            learning_rate=3e-4, n_steps=2048, batch_size=64,
            n_epochs=10, gamma=0.99, gae_lambda=0.95,
            clip_range=0.2, ent_coef=0.01, verbose=0, device=device,
        )
    elif algorithm == "SAC":
        model = SAC(
            "MlpPolicy", vec_train,
            learning_rate=3e-4, batch_size=256,
            gamma=0.99, tau=0.005, ent_coef="auto",
            verbose=0, device=device,
        )
    else:
        return {"error": f"Unknown algorithm: {algorithm}"}

    model.learn(total_timesteps=total_timesteps)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / f"{symbol}_{algorithm.lower()}_{side.lower()}"
    model.save(str(model_path))

    eval_results = evaluate_agent(model, eval_eps, symbol, total_shares, side)

    return {
        "model_path": str(model_path),
        "algorithm": algorithm, "device": device,
        "symbol": symbol, "train_days": len(train_eps),
        "eval_days": len(eval_eps), "total_timesteps": total_timesteps,
        **eval_results,
    }


# ═══════════════════════════════════════════════════════
# EVALUATION
# ═══════════════════════════════════════════════════════

def _run_static_baseline(
    episode: ReplayEpisode, total_shares: int, side: str, strategy: str,
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
        else:  # TWAP
            target = int(remaining / max(1, n_steps - i))
        target = min(target, remaining)
        if target > 0:
            exec_prices.append(float(bar["c"]))
            exec_shares.append(target)
            remaining -= target

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
    model, episodes: list[ReplayEpisode],
    symbol: str, total_shares: int, side: str,
) -> dict:
    """Evaluate trained agent vs VWAP and TWAP baselines."""
    rl_results, vwap_results, twap_results = [], [], []

    for ep in episodes:
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
                "steps_used": env._step, "fills": len(env._exec_shares),
            })

        vwap_results.append(_run_static_baseline(ep, total_shares, side, "VWAP"))
        twap_results.append(_run_static_baseline(ep, total_shares, side, "TWAP"))

    if not rl_results:
        return {"error": "No valid evaluation episodes"}

    rl_df = pd.DataFrame(rl_results)
    vwap_df = pd.DataFrame(vwap_results)
    twap_df = pd.DataFrame(twap_results)

    return {
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
