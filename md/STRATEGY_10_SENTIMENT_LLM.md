# Claude Code Prompt: Strategy 10 — Sentiment-Driven Signals (LLM)

## Context

pakfindata already scrapes PSX company announcements for 564 listed companies. 
This strategy builds an NLP pipeline that scores each announcement's sentiment, 
then combines it with technical signals for trade decisions.

**The pipeline:**
```
PSX Announcement → LLM/FinBERT sentiment score (-1 to +1) → 
merge with Signal Analysis composite → trade if both agree
```

**Why it works on PSX:**
- PSX announcements are poorly parsed by the market (low analyst coverage)
- Many announcements drop after-hours → price adjusts next morning
- Dividend, earnings, and rights issue announcements drive 3-5% moves
- Board meeting dates leak intent before the announcement
- Only ~30 companies have active analyst coverage → 534 are information deserts

**Announcement types that move PSX prices:**
- Dividend declarations (cash + bonus) → +2-5% if above consensus
- Earnings (quarterly/annual) → ±3-10% depending on surprise
- Rights issues → usually -5-10% (dilution fear)
- Board meeting dates → front-run signal (what will they announce?)
- Director buying/selling → insider signal
- Merger/acquisition → +10-30% for target
- Debt restructuring → mixed
- Plant shutdowns/expansions → sector-specific

## What already exists

```bash
# Find existing announcement/news code
grep -rn "announcement\|news\|sentiment\|nlp\|gpt\|openai\|llm\|company_ann" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | head -20

# Check announcement data
python3 -c "
import duckdb, sqlite3

# DuckDB
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for t in con.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=\"main\"').fetchall():
    tl = t[0].lower()
    if any(k in tl for k in ['announce','news','event','corporate','filing']):
        count = con.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
        cols = [c[0] for c in con.execute(f'DESCRIBE {t[0]}').fetchall()]
        print(f'DuckDB {t[0]}: {count:,} — {cols[:10]}')
con.close()

# SQLite
scon = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
for t in [r[0] for r in scon.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()]:
    tl = t.lower()
    if any(k in tl for k in ['announce','news','event','corporate','filing']):
        count = scon.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        cols = [r[1] for r in scon.execute(f'PRAGMA table_info({t})').fetchall()]
        print(f'SQLite {t}: {count:,} — {cols[:10]}')
scon.close()
"

# Sample announcements
python3 -c "
import sqlite3, duckdb

# Try both DBs
for db, connect in [
    ('DuckDB', lambda: duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)),
    ('SQLite', lambda: sqlite3.connect('/mnt/e/psxdata/psx.sqlite')),
]:
    try:
        con = connect()
        for t in ['announcements','company_announcements','corporate_events','psx_announcements']:
            try:
                if db == 'DuckDB':
                    df = con.execute(f'SELECT * FROM {t} ORDER BY ROWID DESC LIMIT 5').df()
                else:
                    import pandas as pd
                    df = pd.read_sql(f'SELECT * FROM {t} ORDER BY ROWID DESC LIMIT 5', con)
                print(f'\n{db} → {t}:')
                print(df.to_string())
                break
            except: pass
        con.close()
    except: pass
"

# Check OpenAI API availability
python3 -c "
import os
key = os.environ.get('OPENAI_API_KEY', '')
print(f'OPENAI_API_KEY: {\"set\" if key else \"NOT SET\"} ({len(key)} chars)')

try:
    import openai; print('openai package: OK')
except: print('openai: MISSING')

try:
    from transformers import pipeline; print('transformers (FinBERT): OK')
except: print('transformers: MISSING (optional)')

# Check Ollama Docker
import requests
try:
    r = requests.get('http://localhost:11434/api/tags', timeout=3)
    if r.status_code == 200:
        models = [m['name'] for m in r.json().get('models', [])]
        print(f'Ollama: RUNNING — models: {models}')
    else:
        print(f'Ollama: ERROR {r.status_code}')
except:
    print('Ollama: NOT RUNNING')
"
```

**READ ALL OUTPUT — identify announcement table structure, column names, and available LLM packages.**

## Step 1: Create the Sentiment Engine

Create `src/pakfindata/engine/sentiment_signals.py`:

```python
"""
Sentiment-Driven Trading Signals.

NLP pipeline for PSX announcements:
  1. Load announcements from DB
  2. Classify announcement type (dividend, earnings, rights, board meeting, etc.)
  3. Score sentiment using LLM (GPT-4o) or FinBERT
  4. Combine with technical signal (composite score from Signal Analysis)
  5. Generate trade signal if sentiment + technicals agree

Four scoring modes:
  A. Ollama (Docker, local) — free, private, llama3.1:8b on RTX 4080, ~2s/announcement
  B. GPT-4o-mini API — best quality, costs ~$0.01/announcement
  C. FinBERT (local) — free, good for financial text, runs on RTX 4080
  D. Rule-based — free, instant, keyword matching (fallback)

Ollama is already running in Docker with GPU passthrough. Models available locally.

PSX-Specific:
  - Announcements in English (some Urdu — skip or translate)
  - Board meeting dates = leading indicator (announce within 7 days)
  - Dividend > 20% = bullish signal for PSX stocks
  - Rights issue = almost always bearish short-term
  - 245 trading days/year
"""

import numpy as np
import pandas as pd
import duckdb
import sqlite3
import json
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

PKT = timezone(timedelta(hours=5))
DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")
PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")
TRADING_DAYS = 245


class AnnouncementType(Enum):
    DIVIDEND = "DIVIDEND"
    EARNINGS = "EARNINGS"
    RIGHTS_ISSUE = "RIGHTS_ISSUE"
    BOARD_MEETING = "BOARD_MEETING"
    DIRECTOR_TRADE = "DIRECTOR_TRADE"
    MERGER_ACQUISITION = "MERGER_ACQUISITION"
    DEBT = "DEBT"
    PLANT_EXPANSION = "PLANT_EXPANSION"
    SHUTDOWN = "SHUTDOWN"
    AGM_EGM = "AGM_EGM"
    BUYBACK = "BUYBACK"
    OTHER = "OTHER"


@dataclass
class SentimentScore:
    symbol: str
    date: str
    announcement_text: str
    announcement_type: AnnouncementType
    sentiment: float                # -1 (very bearish) to +1 (very bullish)
    confidence: float               # 0-1
    scoring_method: str             # "GPT4O", "CLAUDE", "FINBERT", "RULES"
    key_phrases: list[str]          # extracted key terms
    price_impact_estimate: float    # expected % move
    signal: str                     # "BUY", "SELL", "HOLD"
    reason: str


@dataclass
class CombinedSignal:
    symbol: str
    date: str
    sentiment_score: float          # from NLP
    technical_score: float          # from Signal Analysis composite
    combined_score: float           # weighted average
    sentiment_signal: str
    technical_signal: str
    final_signal: str               # "STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"
    confidence: float
    announcement_summary: str
    reason: str


# ─── Announcement Type Classification ───

DIVIDEND_KEYWORDS = [
    "dividend", "interim dividend", "final dividend", "cash dividend",
    "bonus share", "bonus issue", "stock dividend", "payout",
]

EARNINGS_KEYWORDS = [
    "quarterly", "half year", "annual", "profit", "loss", "earnings",
    "EPS", "revenue", "turnover", "net income", "financial results",
    "un-audited", "audited accounts", "financial statements",
]

RIGHTS_KEYWORDS = [
    "right issue", "rights issue", "right shares", "entitlement",
    "subscription", "pre-emptive",
]

BOARD_MEETING_KEYWORDS = [
    "board meeting", "meeting of the board", "board of directors",
    "agenda", "consider", "approve",
]

DIRECTOR_KEYWORDS = [
    "director", "CEO", "chairman", "purchase", "sale of shares",
    "acquisition of shares", "disposal", "insider", "pattern of shareholding",
]

MERGER_KEYWORDS = [
    "merger", "acquisition", "takeover", "amalgamation", "scheme of arrangement",
    "joint venture", "strategic alliance",
]

DEBT_KEYWORDS = [
    "sukuk", "bond", "TFC", "credit rating", "borrowing", "loan",
    "debenture", "commercial paper", "refinancing",
]

EXPANSION_KEYWORDS = [
    "expansion", "new plant", "capacity", "capex", "investment",
    "commissioning", "production increase", "new line",
]

SHUTDOWN_KEYWORDS = [
    "shutdown", "closure", "suspension", "halted", "force majeure",
    "ceased operations", "winding up",
]


def classify_announcement_type(text: str) -> AnnouncementType:
    """Classify announcement by keyword matching."""
    text_lower = text.lower()
    
    scores = {
        AnnouncementType.DIVIDEND: sum(1 for k in DIVIDEND_KEYWORDS if k in text_lower),
        AnnouncementType.EARNINGS: sum(1 for k in EARNINGS_KEYWORDS if k in text_lower),
        AnnouncementType.RIGHTS_ISSUE: sum(1 for k in RIGHTS_KEYWORDS if k in text_lower),
        AnnouncementType.BOARD_MEETING: sum(1 for k in BOARD_MEETING_KEYWORDS if k in text_lower),
        AnnouncementType.DIRECTOR_TRADE: sum(1 for k in DIRECTOR_KEYWORDS if k in text_lower),
        AnnouncementType.MERGER_ACQUISITION: sum(1 for k in MERGER_KEYWORDS if k in text_lower),
        AnnouncementType.DEBT: sum(1 for k in DEBT_KEYWORDS if k in text_lower),
        AnnouncementType.PLANT_EXPANSION: sum(1 for k in EXPANSION_KEYWORDS if k in text_lower),
        AnnouncementType.SHUTDOWN: sum(1 for k in SHUTDOWN_KEYWORDS if k in text_lower),
    }
    
    best = max(scores, key=scores.get)
    if scores[best] > 0:
        return best
    
    if "agm" in text_lower or "egm" in text_lower or "general meeting" in text_lower:
        return AnnouncementType.AGM_EGM
    if "buyback" in text_lower or "buy back" in text_lower or "repurchase" in text_lower:
        return AnnouncementType.BUYBACK
    
    return AnnouncementType.OTHER


# ─── Sentiment Scoring Methods ───

def score_with_rules(text: str, ann_type: AnnouncementType) -> dict:
    """
    Rule-based sentiment scoring (fastest, free, always available).
    Uses keyword polarity + announcement type bias.
    """
    text_lower = text.lower()
    
    positive_words = [
        "profit", "increase", "growth", "dividend", "bonus", "record",
        "highest", "improved", "expansion", "strong", "surplus",
        "acquisition", "buyback", "upgrade", "outperform", "beat",
        "exceeded", "remarkable", "robust", "positive", "recovery",
    ]
    negative_words = [
        "loss", "decline", "decrease", "shutdown", "closure", "suspension",
        "default", "penalty", "fine", "litigation", "downgrade",
        "disappointing", "weak", "deteriorated", "impairment", "write-off",
        "rights issue", "dilution", "delay", "postpone", "concerns",
    ]
    
    pos_count = sum(1 for w in positive_words if w in text_lower)
    neg_count = sum(1 for w in negative_words if w in text_lower)
    total = pos_count + neg_count
    
    if total == 0:
        base_score = 0.0
    else:
        base_score = (pos_count - neg_count) / total
    
    # Type-specific bias
    type_bias = {
        AnnouncementType.DIVIDEND: 0.3,       # dividends are positive
        AnnouncementType.EARNINGS: 0.0,        # neutral — depends on content
        AnnouncementType.RIGHTS_ISSUE: -0.4,   # usually bearish
        AnnouncementType.BOARD_MEETING: 0.1,   # slight positive (something happening)
        AnnouncementType.DIRECTOR_TRADE: 0.0,  # depends on buy/sell
        AnnouncementType.MERGER_ACQUISITION: 0.2,
        AnnouncementType.BUYBACK: 0.4,         # bullish signal
        AnnouncementType.PLANT_EXPANSION: 0.2,
        AnnouncementType.SHUTDOWN: -0.5,
        AnnouncementType.AGM_EGM: 0.0,
        AnnouncementType.DEBT: -0.1,
        AnnouncementType.OTHER: 0.0,
    }
    
    bias = type_bias.get(ann_type, 0)
    sentiment = np.clip(base_score * 0.6 + bias * 0.4, -1, 1)
    
    # Extract dividend amount if present
    div_match = re.search(r'(\d+\.?\d*)%?\s*(cash|interim|final)?\s*dividend', text_lower)
    if div_match:
        div_pct = float(div_match.group(1))
        if div_pct > 30:  # >30% dividend is very bullish for PSX
            sentiment = min(1.0, sentiment + 0.3)
        elif div_pct > 20:
            sentiment = min(1.0, sentiment + 0.2)
    
    # Director buying vs selling
    if ann_type == AnnouncementType.DIRECTOR_TRADE:
        if "purchase" in text_lower or "acquired" in text_lower or "bought" in text_lower:
            sentiment = 0.4  # insider buying = bullish
        elif "sold" in text_lower or "disposal" in text_lower or "sale" in text_lower:
            sentiment = -0.3  # insider selling = bearish
    
    # Extract key phrases
    key_phrases = []
    for w in positive_words + negative_words:
        if w in text_lower:
            key_phrases.append(w)
    
    return {
        "sentiment": float(sentiment),
        "confidence": min(0.7, total * 0.1 + 0.2),  # max 0.7 for rules
        "method": "RULES",
        "key_phrases": key_phrases[:5],
    }


def score_with_gpt4o(text: str, ann_type: AnnouncementType, symbol: str = "") -> dict:
    """
    Score sentiment using GPT-4o API.
    Best quality but costs ~$0.01 per announcement.
    """
    try:
        import openai
        import os
        
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return score_with_rules(text, ann_type)
        
        client = openai.OpenAI(api_key=api_key)
        
        prompt = f"""Analyze this Pakistan Stock Exchange (PSX) corporate announcement and provide a sentiment score.

Symbol: {symbol}
Announcement type: {ann_type.value}
Text: {text[:2000]}

Respond ONLY with a JSON object (no markdown, no backticks):
{{
    "sentiment": <float -1.0 to 1.0, where -1=very bearish, 0=neutral, 1=very bullish>,
    "confidence": <float 0 to 1>,
    "impact_estimate": <float, expected % price move, e.g. 0.03 for +3%>,
    "key_phrases": [<list of 3-5 key terms driving the sentiment>],
    "reasoning": "<one sentence explaining the score>"
}}

PSX context:
- Cash dividend >20% is considered good
- Rights issues are usually bearish (-5 to -10%)
- Director buying is a strong bullish signal
- EPS growth >20% YoY is significant
- Board meetings often precede major announcements"""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # cheaper, good enough for sentiment
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.1,
        )
        
        result_text = response.choices[0].message.content.strip()
        # Clean any markdown fences
        result_text = re.sub(r'```json\s*|\s*```', '', result_text)
        result = json.loads(result_text)
        
        return {
            "sentiment": float(np.clip(result.get("sentiment", 0), -1, 1)),
            "confidence": float(np.clip(result.get("confidence", 0.5), 0, 1)),
            "method": "GPT4O",
            "key_phrases": result.get("key_phrases", [])[:5],
            "impact_estimate": float(result.get("impact_estimate", 0)),
            "reasoning": result.get("reasoning", ""),
        }
    
    except Exception as e:
        # Fallback to rules
        result = score_with_rules(text, ann_type)
        result["error"] = str(e)
        return result


def score_with_finbert(text: str) -> dict:
    """
    Score sentiment using FinBERT (local, free, runs on GPU).
    Requires: pip install transformers torch
    """
    try:
        from transformers import pipeline
        
        # Cache the pipeline
        if not hasattr(score_with_finbert, "_pipe"):
            score_with_finbert._pipe = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",
                device=0,  # GPU (RTX 4080)
            )
        
        pipe = score_with_finbert._pipe
        
        # FinBERT max 512 tokens — truncate
        truncated = text[:500]
        result = pipe(truncated)[0]
        
        label = result["label"]  # "positive", "negative", "neutral"
        score = result["score"]  # confidence 0-1
        
        if label == "positive":
            sentiment = score * 0.8  # scale to -1..1
        elif label == "negative":
            sentiment = -score * 0.8
        else:
            sentiment = 0.0
        
        return {
            "sentiment": float(sentiment),
            "confidence": float(score),
            "method": "FINBERT",
            "key_phrases": [],
            "label": label,
        }
    
    except Exception as e:
        result = score_with_rules(text, AnnouncementType.OTHER)
        result["error"] = str(e)
        return result


# ─── Ollama (Docker, Local GPU) ───

OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.1:8b"  # or "mistral:7b" or "phi3:mini"


def check_ollama_status() -> dict:
    """Check if Ollama is running and which models are available."""
    import requests
    try:
        r = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if r.status_code == 200:
            models = [m["name"] for m in r.json().get("models", [])]
            return {"status": "running", "models": models, "url": OLLAMA_BASE_URL}
        return {"status": "error", "code": r.status_code}
    except requests.ConnectionError:
        return {"status": "not_running", "hint": "docker compose up ollama"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def score_with_ollama(text: str, ann_type: AnnouncementType, symbol: str = "") -> dict:
    """
    Score sentiment using local Ollama (Docker with GPU passthrough).
    Free, private, no API key needed. Runs on RTX 4080.
    
    Models ranked by quality for PSX sentiment:
      1. llama3.1:8b  — best reasoning, ~5GB VRAM, ~2s/announcement
      2. mistral:7b   — good balance, ~4GB VRAM, ~1.5s/announcement
      3. phi3:mini    — fastest, ~2.5GB VRAM, ~0.8s/announcement
    """
    import requests
    
    prompt = f"""Analyze this Pakistan Stock Exchange (PSX) corporate announcement.

Symbol: {symbol}
Type: {ann_type.value}
Text: {text[:1500]}

Respond ONLY with JSON (no explanation, no markdown):
{{"sentiment": <-1.0 to 1.0>, "confidence": <0 to 1>, "impact": <expected % move>, "phrases": [<3 key terms>], "reason": "<one sentence>"}}

PSX context: dividend >20% = bullish, rights issue = bearish, director buying = very bullish, EPS growth >20% = significant."""

    try:
        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 200,
                }
            },
            timeout=30,
        )
        
        if response.status_code != 200:
            return score_with_rules(text, ann_type)
        
        result_text = response.json().get("response", "")
        
        # Extract JSON from response (Ollama sometimes wraps it in text)
        json_match = re.search(r'\{[^{}]+\}', result_text)
        if json_match:
            result = json.loads(json_match.group())
            return {
                "sentiment": float(np.clip(result.get("sentiment", 0), -1, 1)),
                "confidence": float(np.clip(result.get("confidence", 0.5), 0, 1)),
                "method": f"OLLAMA_{OLLAMA_MODEL.upper().replace(':', '_')}",
                "key_phrases": result.get("phrases", [])[:5],
                "impact_estimate": float(result.get("impact", 0)),
                "reasoning": result.get("reason", ""),
            }
        
        # JSON parse failed — extract sentiment from free text
        text_lower = result_text.lower()
        if "bullish" in text_lower or "positive" in text_lower:
            sentiment = 0.4
        elif "bearish" in text_lower or "negative" in text_lower:
            sentiment = -0.4
        else:
            sentiment = 0.0
        
        return {
            "sentiment": sentiment,
            "confidence": 0.4,
            "method": f"OLLAMA_{OLLAMA_MODEL.upper().replace(':', '_')}_FREETEXT",
            "key_phrases": [],
            "reasoning": result_text[:200],
        }
    
    except Exception as e:
        result = score_with_rules(text, ann_type)
        result["error"] = str(e)
        return result


# ─── Data Loading ───

def load_announcements(
    symbol: str = None,
    days: int = 30,
    limit: int = 500,
) -> pd.DataFrame:
    """
    Load PSX announcements from available database.
    Auto-discovers the correct table and columns.
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    result = pd.DataFrame()
    
    # Try DuckDB
    try:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        tables = [t[0] for t in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()]
        
        for t in tables:
            tl = t.lower()
            if any(k in tl for k in ['announce', 'news', 'event', 'corporate', 'filing', 'notification']):
                try:
                    result = con.execute(f"SELECT * FROM {t} ORDER BY ROWID DESC LIMIT {limit}").df()
                    if not result.empty:
                        break
                except:
                    continue
        con.close()
    except:
        pass
    
    # Try SQLite
    if result.empty:
        try:
            scon = sqlite3.connect(str(PSX_SQLITE))
            for t in [r[0] for r in scon.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
                tl = t.lower()
                if any(k in tl for k in ['announce', 'news', 'event', 'corporate', 'filing', 'notification']):
                    try:
                        result = pd.read_sql(f"SELECT * FROM {t} ORDER BY ROWID DESC LIMIT {limit}", scon)
                        if not result.empty:
                            break
                    except:
                        continue
            scon.close()
        except:
            pass
    
    if result.empty:
        return result
    
    # Normalize columns
    col_map = {}
    for c in result.columns:
        cl = c.lower()
        if 'date' in cl and 'date' not in col_map: col_map['date'] = c
        elif any(k in cl for k in ['symbol', 'company_code', 'ticker']): col_map['symbol'] = c
        elif any(k in cl for k in ['subject', 'title', 'heading', 'description', 'text', 'content', 'announcement']):
            if 'text' not in col_map: col_map['text'] = c
        elif any(k in cl for k in ['company', 'name', 'company_name']): col_map['company'] = c
    
    # Rename to standard
    rename = {v: k for k, v in col_map.items() if v in result.columns}
    result = result.rename(columns=rename)
    
    # Filter by date and symbol
    if 'date' in result.columns:
        result['date'] = pd.to_datetime(result['date'], errors='coerce')
        result = result.dropna(subset=['date'])
        result = result[result['date'] >= cutoff]
    
    if symbol and 'symbol' in result.columns:
        result = result[result['symbol'].str.upper() == symbol.upper()]
    
    return result.head(limit)


# ─── Signal Generation ───

def score_announcements(
    announcements_df: pd.DataFrame,
    method: str = "rules",  # "rules", "gpt4o", "finbert"
) -> list[SentimentScore]:
    """
    Score all announcements using the selected method.
    """
    if announcements_df.empty:
        return []
    
    text_col = 'text' if 'text' in announcements_df.columns else None
    if text_col is None:
        # Find any text-like column
        for c in announcements_df.columns:
            if announcements_df[c].dtype == 'object' and announcements_df[c].str.len().mean() > 20:
                text_col = c
                break
    
    if text_col is None:
        return []
    
    scores = []
    
    for _, row in announcements_df.iterrows():
        text = str(row.get(text_col, ''))
        if len(text) < 10:
            continue
        
        symbol = str(row.get('symbol', ''))
        date = str(row.get('date', ''))[:10]
        
        # Classify type
        ann_type = classify_announcement_type(text)
        
        # Score sentiment
        if method == "ollama":
            result = score_with_ollama(text, ann_type, symbol)
        elif method == "gpt4o":
            result = score_with_gpt4o(text, ann_type, symbol)
        elif method == "finbert":
            result = score_with_finbert(text)
            result["key_phrases"] = []  # FinBERT doesn't extract phrases
        else:
            result = score_with_rules(text, ann_type)
        
        # Estimate price impact
        impact = _estimate_price_impact(ann_type, result["sentiment"])
        
        # Generate signal
        sentiment = result["sentiment"]
        if sentiment > 0.3:
            signal = "BUY"
        elif sentiment < -0.3:
            signal = "SELL"
        else:
            signal = "HOLD"
        
        scores.append(SentimentScore(
            symbol=symbol,
            date=date,
            announcement_text=text[:500],
            announcement_type=ann_type,
            sentiment=result["sentiment"],
            confidence=result["confidence"],
            scoring_method=result["method"],
            key_phrases=result.get("key_phrases", []),
            price_impact_estimate=impact,
            signal=signal,
            reason=result.get("reasoning", f"{ann_type.value}: sentiment {sentiment:+.2f}"),
        ))
    
    return scores


def _estimate_price_impact(ann_type: AnnouncementType, sentiment: float) -> float:
    """Estimate expected price impact based on announcement type and sentiment."""
    base_impact = {
        AnnouncementType.DIVIDEND: 0.025,
        AnnouncementType.EARNINGS: 0.04,
        AnnouncementType.RIGHTS_ISSUE: -0.07,
        AnnouncementType.BOARD_MEETING: 0.01,
        AnnouncementType.DIRECTOR_TRADE: 0.02,
        AnnouncementType.MERGER_ACQUISITION: 0.10,
        AnnouncementType.BUYBACK: 0.03,
        AnnouncementType.PLANT_EXPANSION: 0.02,
        AnnouncementType.SHUTDOWN: -0.05,
        AnnouncementType.DEBT: -0.01,
        AnnouncementType.AGM_EGM: 0.005,
        AnnouncementType.OTHER: 0.01,
    }
    
    base = base_impact.get(ann_type, 0.01)
    return base * np.sign(sentiment) if sentiment != 0 else base


def combine_with_technicals(
    sentiment_scores: list[SentimentScore],
    sentiment_weight: float = 0.4,
    technical_weight: float = 0.6,
) -> list[CombinedSignal]:
    """
    Combine sentiment scores with technical composite scores from Signal Analysis.
    
    Final score = sentiment_weight × sentiment + technical_weight × technical
    
    Strong signals: both sentiment AND technicals agree.
    """
    if not sentiment_scores:
        return []
    
    # Load technical scores
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    tech_scores = {}
    symbols = list(set(s.symbol for s in sentiment_scores if s.symbol))
    
    if symbols:
        placeholders = ",".join(f"'{s}'" for s in symbols)
        
        # Try to get composite signal scores
        for query in [
            f"SELECT symbol, score, signal FROM signal_scores WHERE symbol IN ({placeholders})",
            f"SELECT symbol, composite_score as score FROM batch_signals WHERE symbol IN ({placeholders})",
        ]:
            try:
                df = con.execute(query).df()
                if not df.empty:
                    for _, row in df.iterrows():
                        # Normalize to -1..1 (from 0..100)
                        score = (row.get('score', 50) - 50) / 50
                        tech_scores[row['symbol']] = score
                    break
            except:
                continue
        
        # Fallback: compute basic technical score from price action
        if not tech_scores:
            for sym in symbols:
                try:
                    eod = con.execute(f"""
                        SELECT close FROM eod_ohlcv 
                        WHERE symbol = '{sym}' ORDER BY date DESC LIMIT 60
                    """).df()
                    if len(eod) >= 20:
                        close = eod['close'].values
                        mom_20 = close[0] / close[19] - 1  # 20-day return
                        sma_50 = np.mean(close[:50]) if len(close) >= 50 else np.mean(close)
                        above_sma = 1 if close[0] > sma_50 else -1
                        tech_scores[sym] = np.clip(mom_20 * 5 + above_sma * 0.2, -1, 1)
                except:
                    pass
    
    con.close()
    
    # Combine
    combined = []
    for s in sentiment_scores:
        tech = tech_scores.get(s.symbol, 0)
        
        # Weighted combination
        combined_score = sentiment_weight * s.sentiment + technical_weight * tech
        combined_score = np.clip(combined_score, -1, 1)
        
        # Final signal
        if combined_score > 0.5:
            final = "STRONG_BUY"
        elif combined_score > 0.2:
            final = "BUY"
        elif combined_score < -0.5:
            final = "STRONG_SELL"
        elif combined_score < -0.2:
            final = "SELL"
        else:
            final = "HOLD"
        
        # Confidence boosted when both agree
        agreement = (s.sentiment > 0 and tech > 0) or (s.sentiment < 0 and tech < 0)
        confidence = s.confidence * (1.3 if agreement else 0.7)
        confidence = min(1.0, confidence)
        
        tech_signal = "BUY" if tech > 0.2 else "SELL" if tech < -0.2 else "HOLD"
        
        reason_parts = [s.reason]
        if agreement:
            reason_parts.append(f"✅ Sentiment + technicals AGREE (combined: {combined_score:+.2f})")
        else:
            reason_parts.append(f"⚠️ Sentiment vs technicals DISAGREE (sent: {s.sentiment:+.2f}, tech: {tech:+.2f})")
        
        combined.append(CombinedSignal(
            symbol=s.symbol,
            date=s.date,
            sentiment_score=s.sentiment,
            technical_score=tech,
            combined_score=combined_score,
            sentiment_signal=s.signal,
            technical_signal=tech_signal,
            final_signal=final,
            confidence=confidence,
            announcement_summary=s.announcement_text[:200],
            reason=" | ".join(reason_parts),
        ))
    
    # Sort by absolute combined score
    combined.sort(key=lambda x: abs(x.combined_score), reverse=True)
    
    return combined


def backtest_sentiment_strategy(
    method: str = "rules",
    lookback_days: int = 180,
    hold_days: int = 5,            # hold for N days after signal
    sentiment_threshold: float = 0.3,
    require_technical: bool = True, # require technical agreement
) -> dict:
    """
    Backtest sentiment strategy: buy/sell on announcement sentiment,
    hold for N trading days, measure actual price change.
    """
    announcements = load_announcements(days=lookback_days, limit=2000)
    if announcements.empty:
        return {"error": "No announcements found"}
    
    scores = score_announcements(announcements, method=method)
    if not scores:
        return {"error": "No scores generated"}
    
    # Get EOD data for measuring price impact
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    trades = []
    
    for s in scores:
        if abs(s.sentiment) < sentiment_threshold:
            continue
        if not s.symbol:
            continue
        
        try:
            # Get price after announcement
            eod = con.execute(f"""
                SELECT date, close FROM eod_ohlcv
                WHERE symbol = '{s.symbol}' AND date >= '{s.date}'
                ORDER BY date
                LIMIT {hold_days + 1}
            """).df()
            
            if len(eod) < 2:
                continue
            
            entry_price = eod.iloc[0]["close"]
            
            # Exit price after hold_days
            exit_idx = min(hold_days, len(eod) - 1)
            exit_price = eod.iloc[exit_idx]["close"]
            
            # Actual return
            if s.signal == "BUY":
                actual_return = exit_price / entry_price - 1
            elif s.signal == "SELL":
                actual_return = 1 - exit_price / entry_price
            else:
                continue
            
            trades.append({
                "symbol": s.symbol,
                "date": s.date,
                "type": s.announcement_type.value,
                "sentiment": s.sentiment,
                "signal": s.signal,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "actual_return": actual_return,
                "hold_days": exit_idx,
                "correct": (s.sentiment > 0 and actual_return > 0) or (s.sentiment < 0 and actual_return < 0),
                "method": s.scoring_method,
            })
        except:
            continue
    
    con.close()
    
    if not trades:
        return {"error": "No trades generated", "scores_count": len(scores)}
    
    trades_df = pd.DataFrame(trades)
    
    winning = trades_df[trades_df["actual_return"] > 0]
    
    # By type breakdown
    type_stats = trades_df.groupby("type").agg(
        count=("actual_return", "count"),
        win_rate=("correct", "mean"),
        avg_return=("actual_return", "mean"),
    ).to_dict("index")
    
    return {
        "trades": trades_df,
        "metrics": {
            "total_signals": len(scores),
            "total_trades": len(trades_df),
            "win_rate": trades_df["correct"].mean(),
            "avg_return": trades_df["actual_return"].mean(),
            "total_return": (1 + trades_df["actual_return"]).prod() - 1,
            "directional_accuracy": trades_df["correct"].mean(),
            "avg_sentiment": trades_df["sentiment"].mean(),
            "method": method,
            "hold_days": hold_days,
            "by_type": type_stats,
            "by_signal": trades_df.groupby("signal")["actual_return"].mean().to_dict(),
        },
    }


def scan_recent_sentiment(
    days: int = 7,
    method: str = "rules",
    min_sentiment: float = 0.2,
) -> pd.DataFrame:
    """
    Scan recent announcements and score them.
    Returns table sorted by absolute sentiment.
    """
    announcements = load_announcements(days=days, limit=200)
    if announcements.empty:
        return pd.DataFrame()
    
    scores = score_announcements(announcements, method=method)
    combined = combine_with_technicals(scores)
    
    results = []
    for c in combined:
        if abs(c.sentiment_score) >= min_sentiment:
            results.append({
                "symbol": c.symbol,
                "date": c.date,
                "sentiment": c.sentiment_score,
                "technical": c.technical_score,
                "combined": c.combined_score,
                "sent_signal": c.sentiment_signal,
                "tech_signal": c.technical_signal,
                "final": c.final_signal,
                "confidence": c.confidence,
                "summary": c.announcement_summary[:100],
            })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results).sort_values("combined", key=abs, ascending=False).reset_index(drop=True)
```

## Step 3: Create the Streamlit page

Create `src/pakfindata/ui/page_views/strategy_sentiment.py`:

### Tab 1: Sentiment Feed
```
├── Recent announcements with sentiment scores:
│   Table: Date | Symbol | Type | Sentiment (-1..+1) | Signal | Confidence | Summary
│   Color: green rows (bullish), red (bearish), gray (neutral)
│   Sentiment column as colored bars (-1 red ← 0 gray → +1 green)
├── Scoring method selector: [Ollama (local) | Rules | GPT-4o | FinBERT]
│   Auto-detects Ollama status + available models via check_ollama_status()
│   Shows 🟢/🔴 indicator in sidebar for Ollama Docker status
│   If Ollama running: shows model dropdown (llama3.1:8b, mistral:7b, etc.)
├── Days lookback: slider 1-30 (default 7)
├── Min sentiment: slider 0.0-0.5 (default 0.2)
├── Announcement type filter (multi-select)
├── Click row → expander with full announcement text + scoring details
└── Auto-refresh toggle (re-score every 30 min)
```

### Tab 2: Combined Signals
```
├── Sentiment + Technicals combined view:
│   Table: Symbol | Sentiment | Technical | Combined | Sent Signal | Tech Signal | Final | Confidence
├── Agreement indicator: ✅ AGREE / ⚠️ DISAGREE for each
├── Scatter plot: Sentiment (X) vs Technical Score (Y)
│   Quadrants: top-right = STRONG BUY, bottom-left = STRONG SELL
│   top-left / bottom-right = CONFLICTING
├── Weight sliders: Sentiment weight / Technical weight (default 40/60)
└── Only show symbols where both signals agree
```

### Tab 3: Backtest
```
├── Parameters:
│   ├── Method: Ollama / Rules / GPT-4o / FinBERT
│   ├── Lookback: 30d / 90d / 180d / 365d
│   ├── Hold period: 1d / 3d / 5d / 10d
│   ├── Sentiment threshold: 0.1-0.5
│   └── Require technical agreement: toggle
├── [Run Backtest]
├── Metrics: Signals, Trades, Win Rate, Avg Return, Total Return, Accuracy
├── By announcement type: which types predict best?
├── Sentiment vs Actual Return scatter
├── Win rate by sentiment decile
└── Time-of-day analysis: do morning announcements predict better?
```

### Tab 4: Announcement Research
```
├── Single symbol deep dive:
│   ├── All announcements for selected symbol (timeline)
│   ├── Price chart with announcement markers (vertical lines)
│   ├── Pre/post announcement return analysis
│   ├── Typical reaction by announcement type for this stock
│   └── Announcement frequency chart (are they announcing more/less?)
├── Cross-company analysis:
│   ├── Which announcement types predict best across all stocks?
│   ├── Sector-level sentiment aggregation
│   ├── Announcement clustering (similar announcements grouped)
│   └── Time-to-reaction: how fast does price adjust?
├── LLM cost tracker (if using GPT-4o):
│   ├── API calls made, tokens used, estimated cost
│   └── Cost per signal generated
└── Methodology explanation
```

### Key chart — Sentiment vs Technical scatter:
```python
import plotly.express as px

fig = px.scatter(
    df, x="sentiment", y="technical", color="final",
    size="confidence", hover_data=["symbol", "summary"],
    color_discrete_map={
        "STRONG_BUY": "#22C55E", "BUY": "#86EFAC",
        "HOLD": "#6B7280",
        "SELL": "#FCA5A5", "STRONG_SELL": "#EF4444",
    },
    labels={"sentiment": "Sentiment Score", "technical": "Technical Score"},
)

# Quadrant lines
fig.add_hline(y=0, line_dash="dot", line_color="#6B7280")
fig.add_vline(x=0, line_dash="dot", line_color="#6B7280")

# Quadrant labels
fig.add_annotation(x=0.7, y=0.7, text="STRONG BUY", showarrow=False, font=dict(color="#22C55E"))
fig.add_annotation(x=-0.7, y=-0.7, text="STRONG SELL", showarrow=False, font=dict(color="#EF4444"))
fig.add_annotation(x=-0.7, y=0.7, text="CONFLICTING", showarrow=False, font=dict(color="#EAB308"))
fig.add_annotation(x=0.7, y=-0.7, text="CONFLICTING", showarrow=False, font=dict(color="#EAB308"))

fig.update_layout(template="plotly_dark", paper_bgcolor="#0B0E11",
                  plot_bgcolor="#0B0E11", height=500)
```

## Step 4: Add to sidebar

```python
st.page_link("page_views/strategy_sentiment.py", label="Sentiment Signals", icon="💬")
```

## Step 5: Test

```bash
cd ~/pakfindata && conda activate psx

# Test announcement loading
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import load_announcements, classify_announcement_type

df = load_announcements(days=30, limit=50)
print(f'Announcements: {len(df)}')
print(f'Columns: {list(df.columns)}')
if not df.empty:
    print(df[['date','symbol','text']].head(5).to_string())
    
    # Test classification
    for _, row in df.head(10).iterrows():
        text = str(row.get('text', ''))
        if text:
            atype = classify_announcement_type(text)
            print(f'  {row.get(\"symbol\",\"\"):8s} → {atype.value:20s} | {text[:80]}...')
"

# Test rule-based scoring
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import load_announcements, score_announcements

df = load_announcements(days=30, limit=50)
scores = score_announcements(df, method='rules')
print(f'Scored: {len(scores)}')
for s in scores[:10]:
    emoji = '🟢' if s.sentiment > 0.2 else '🔴' if s.sentiment < -0.2 else '⚪'
    print(f'  {emoji} {s.symbol:8s} {s.sentiment:+.2f} {s.announcement_type.value:20s} {s.signal:5s} | {s.announcement_text[:60]}...')
"

# Test Ollama scoring (preferred — free, local, GPU)
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import check_ollama_status, score_with_ollama, AnnouncementType

status = check_ollama_status()
print(f'Ollama status: {status}')

if status['status'] == 'running':
    # Test with a sample announcement
    result = score_with_ollama(
        'HBL declares 25% cash dividend for Q4 2025. EPS Rs 15.23 versus Rs 12.10 last year, up 26% YoY.',
        AnnouncementType.DIVIDEND,
        'HBL'
    )
    print(f'Sentiment: {result[\"sentiment\"]:+.2f}')
    print(f'Confidence: {result[\"confidence\"]:.0%}')
    print(f'Method: {result[\"method\"]}')
    print(f'Phrases: {result.get(\"key_phrases\", [])}')
    print(f'Reasoning: {result.get(\"reasoning\", \"\")}')
    
    # Test negative announcement
    result2 = score_with_ollama(
        'LUCK announces rights issue of 1 share for every 5 held at Rs 500. Board approved PKR 10B capex for new line.',
        AnnouncementType.RIGHTS_ISSUE,
        'LUCK'
    )
    print(f'\nRights issue: {result2[\"sentiment\"]:+.2f} ({result2[\"method\"]})')
else:
    print('Ollama not running — skipping test')
"

# Test Ollama batch scoring on real announcements
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import load_announcements, score_announcements, check_ollama_status

status = check_ollama_status()
if status['status'] == 'running':
    df = load_announcements(days=14, limit=20)
    scores = score_announcements(df, method='ollama')
    print(f'Ollama scored: {len(scores)} announcements')
    for s in scores[:10]:
        emoji = '🟢' if s.sentiment > 0.2 else '🔴' if s.sentiment < -0.2 else '⚪'
        print(f'  {emoji} {s.symbol:8s} {s.sentiment:+.2f} {s.scoring_method:30s} | {s.announcement_text[:50]}...')
else:
    print('Ollama not running — using rules fallback')
    df = load_announcements(days=14, limit=20)
    scores = score_announcements(df, method='rules')
    print(f'Rules scored: {len(scores)}')
"

# Test combined signals
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import load_announcements, score_announcements, combine_with_technicals

df = load_announcements(days=14, limit=100)
scores = score_announcements(df, method='rules')
combined = combine_with_technicals(scores)
print(f'Combined signals: {len(combined)}')
for c in combined[:10]:
    print(f'  {c.symbol:8s} sent:{c.sentiment_score:+.2f} tech:{c.technical_score:+.2f} → {c.final_signal:12s} (conf:{c.confidence:.0%})')
"

# Test backtest
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import backtest_sentiment_strategy

result = backtest_sentiment_strategy(method='rules', lookback_days=180, hold_days=5)
if 'error' not in result:
    m = result['metrics']
    print(f'=== SENTIMENT BACKTEST ===')
    print(f'Signals: {m[\"total_signals\"]}')
    print(f'Trades: {m[\"total_trades\"]}')
    print(f'Win Rate: {m[\"win_rate\"]:.0%}')
    print(f'Avg Return: {m[\"avg_return\"]:.2%}')
    print(f'Total Return: {m[\"total_return\"]:.1%}')
    print(f'Accuracy: {m[\"directional_accuracy\"]:.0%}')
    print(f'Method: {m[\"method\"]}')
    print(f'\nBy type:')
    for t, stats in m['by_type'].items():
        print(f'  {t:20s}: {stats[\"count\"]} trades, {stats[\"win_rate\"]:.0%} WR, {stats[\"avg_return\"]:+.2%} avg')
else:
    print(result)
"

# Test scanner
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.sentiment_signals import scan_recent_sentiment

df = scan_recent_sentiment(days=7, method='rules')
print(f'Recent signals: {len(df)}')
if not df.empty:
    print(df[['symbol','sentiment','technical','combined','final','summary']].head(10).to_string())
"
```

## IMPORTANT NOTES

1. **Four scoring methods** — Ollama (free/local/GPU), rules (free/fast/always), GPT-4o-mini (best/$0.01), FinBERT (free/GPU)
2. **Ollama is already running in Docker** with GPU passthrough — use it as default scoring method
3. **Ollama Docker URL: `http://localhost:11434`** — no API key needed, models already pulled
4. **Ollama model preference:** llama3.1:8b (best) > mistral:7b (faster) > phi3:mini (fastest)
5. **RTX 4080 12GB budget:** Ollama llama3.1:8b ~5GB → 7GB free for DuckDB/Streamlit
6. **check_ollama_status()** auto-detects running + available models — use in Streamlit sidebar
7. **JSON extraction from Ollama** uses regex — handles Ollama wrapping JSON in explanation text
8. **Announcement table discovery is dynamic** — scans both DuckDB and SQLite
9. **Type classification uses keywords** — catches dividend, earnings, rights, board meeting, director trades, M&A
10. **Dividend >20% = bullish for PSX** — hardcoded in both rules and LLM prompt
11. **Director buying = strong bullish** — insider trading disclosure is mandatory on PSX
12. **Rights issue = almost always bearish** — dilution fear dominates PSX retail
13. **Combined signal weights: 40% sentiment + 60% technicals** — technicals get more weight
14. **Agreement boost** — sentiment + technicals agree → confidence ×1.3
15. **Hold period = 5 days** — PSX digests announcements in 3-5 trading days
16. **Recommended:** Ollama for daily batch (free), GPT-4o-mini only for high-conviction signals
17. **No TA libraries** — all in numpy/pandas
18. **Add under STRATEGIES** in sidebar
19. **PSX edge:** 534 of 564 companies have ZERO analyst coverage. LLM is the FIRST systematic analysis.
