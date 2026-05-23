"""
Sentiment-Driven Signals (LLM) Strategy.

Pipeline: PSX Announcement -> LLM sentiment score (-1 to +1) -> trade signal.
Uses GPT-4o-mini to score announcement sentiment for Pakistan market context.

Announcement types that move prices:
  - Dividend: +2-5% if above consensus
  - Earnings: +/-3-10% on surprise
  - Rights issue: -5-10% (dilution)
  - Director buying/selling: insider signal
  - Board meetings: front-run signal
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict

PSX_SQLITE = Path("/home/smnb/psxdata_rescue/psx.sqlite")
CACHE_DIR = Path.home() / "pakfindata" / "models" / "sentiment_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class SentimentSignal:
    symbol: str
    date: str
    title: str
    category: str
    sentiment_score: float   # -1 to +1
    sentiment_label: str     # BULLISH, BEARISH, NEUTRAL
    confidence: float        # 0-1
    signal: str              # BUY, SELL, HOLD
    reason: str
    cached: bool = False

    def to_dict(self):
        return asdict(self)


def _sqlite_con():
    con = sqlite3.connect(str(PSX_SQLITE), timeout=10)
    con.row_factory = sqlite3.Row
    return con


# ═══════════════════════════════════════════════════════
# LLM SENTIMENT SCORING
# ═══════════════════════════════════════════════════════

def _llm_score(title: str, category: str = "", symbol: str = "") -> dict:
    """Score announcement sentiment via central LLM client.

    Uses Ollama (local, free) with automatic model selection and fallback.
    """
    from pakfindata.services.llm_client import llm

    result = llm.score_announcement(
        text=title,
        ann_type=category,
        symbol=symbol,
    )

    return {
        "score": result.get("sentiment", 0),
        "label": (
            "BULLISH" if result.get("sentiment", 0) > 0.15
            else "BEARISH" if result.get("sentiment", 0) < -0.15
            else "NEUTRAL"
        ),
        "confidence": result.get("confidence", 0),
        "reason": result.get("reason", ""),
        "provider": result.get("method", "none"),
    }


def _cache_key(symbol: str, date: str, title: str) -> str:
    import hashlib
    return hashlib.md5(f"{symbol}:{date}:{title}".encode()).hexdigest()


def _get_cached(symbol: str, date: str, title: str) -> dict | None:
    key = _cache_key(symbol, date, title)
    path = CACHE_DIR / f"{key}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None


def _set_cache(symbol: str, date: str, title: str, result: dict):
    key = _cache_key(symbol, date, title)
    path = CACHE_DIR / f"{key}.json"
    path.write_text(json.dumps(result))


# ═══════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════

def load_announcements(symbol: str = None, limit: int = 50, days_back: int = 30) -> pd.DataFrame:
    """Load recent announcements."""
    con = _sqlite_con()
    if symbol:
        df = pd.read_sql_query(
            "SELECT id, symbol, announcement_date, announcement_type, category, title, document_url "
            "FROM corporate_announcements WHERE symbol = ? "
            "ORDER BY announcement_date DESC LIMIT ?",
            con, params=[symbol, limit],
        )
    else:
        df = pd.read_sql_query(
            f"SELECT id, symbol, announcement_date, announcement_type, category, title, document_url "
            f"FROM corporate_announcements "
            f"WHERE announcement_date >= date('now', '-{days_back} days') "
            f"ORDER BY announcement_date DESC LIMIT ?",
            con, params=[limit],
        )
    con.close()
    return df


def get_announcement_symbols() -> list[str]:
    con = _sqlite_con()
    rows = con.execute(
        "SELECT DISTINCT symbol FROM corporate_announcements ORDER BY symbol"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════
# SIGNAL GENERATION
# ═══════════════════════════════════════════════════════

def score_announcement(symbol: str, date: str, title: str, category: str = "") -> SentimentSignal:
    """Score a single announcement using LLM with caching."""
    cached = _get_cached(symbol, date, title)
    if cached:
        score = cached.get("score", 0)
        label = cached.get("label", "NEUTRAL")
        conf = cached.get("confidence", 0)
        reason = cached.get("reason", "")
        is_cached = True
    else:
        result = _llm_score(title, category, symbol)
        score = result["score"]
        label = result["label"]
        conf = result["confidence"]
        reason = result["reason"]
        _set_cache(symbol, date, title, result)
        is_cached = False

    # Signal: combine sentiment with confidence threshold
    if score > 0.3 and conf > 0.3:
        signal = "BUY"
    elif score < -0.3 and conf > 0.3:
        signal = "SELL"
    else:
        signal = "HOLD"

    return SentimentSignal(
        symbol=symbol, date=date, title=title, category=category or "",
        sentiment_score=score, sentiment_label=label, confidence=conf,
        signal=signal, reason=reason, cached=is_cached,
    )


def score_recent_announcements(limit: int = 20, days_back: int = 7) -> list[SentimentSignal]:
    """Score recent announcements across all symbols."""
    df = load_announcements(limit=limit, days_back=days_back)
    if df.empty:
        return []

    signals = []
    for _, row in df.iterrows():
        sig = score_announcement(
            symbol=str(row["symbol"]),
            date=str(row["announcement_date"]),
            title=str(row["title"]),
            category=str(row.get("category", "")),
        )
        signals.append(sig)

    signals.sort(key=lambda s: abs(s.sentiment_score), reverse=True)
    return signals


def get_cache_stats() -> dict:
    """Get sentiment cache statistics."""
    files = list(CACHE_DIR.glob("*.json"))
    return {
        "cached_scores": len(files),
        "cache_size_kb": sum(f.stat().st_size for f in files) / 1024,
        "cache_dir": str(CACHE_DIR),
    }
