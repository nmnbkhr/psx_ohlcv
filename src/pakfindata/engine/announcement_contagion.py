"""
Announcement Contagion — predicts peer stock reactions to announcements.

Approach:
1. Read recent announcements from company_announcements table
2. Classify each via Ollama LLM (with keyword fallback)
3. Map source company -> sector peers
4. Score peer reaction probability based on type + historical patterns
5. Output: which peer stocks will be affected, in which direction

Requires: Ollama running with a model for best results (keyword fallback otherwise).
"""

import json
import logging
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("contagion")

PKT = timezone(timedelta(hours=5))
OLLAMA_URL = "http://localhost:11434/api/generate"

try:
    from pakfindata.db.connections import analytics_con
except ImportError:
    analytics_con = None


@dataclass
class ContagionSignal:
    source_symbol: str
    announcement_type: str      # DIVIDEND, EARNINGS, RIGHTS, BONUS, OTHER
    announcement_summary: str
    sentiment: float            # -1 to +1
    affected_peers: list[dict]  # [{symbol, direction, confidence, reason}]
    sector: str
    detected_at: str
    evidence: str


# Announcement types and their typical peer impacts
CONTAGION_RULES = {
    "DIVIDEND": {
        "peer_direction": "SAME",
        "magnitude": 0.6,
        "decay": 0.8,
    },
    "EARNINGS_POSITIVE": {
        "peer_direction": "SAME",
        "magnitude": 0.7,
        "decay": 0.6,
    },
    "EARNINGS_NEGATIVE": {
        "peer_direction": "MIXED",
        "magnitude": 0.5,
        "decay": 0.4,
    },
    "RIGHTS_ISSUE": {
        "peer_direction": "NEUTRAL",
        "magnitude": 0.3,
        "decay": 0.2,
    },
    "BONUS_SHARES": {
        "peer_direction": "SAME",
        "magnitude": 0.5,
        "decay": 0.5,
    },
    "REGULATORY": {
        "peer_direction": "SAME",
        "magnitude": 0.8,
        "decay": 0.9,
    },
}


_ollama_available: bool | None = None  # cache Ollama status for session


def _classify_announcement(subject: str, category: str = "") -> dict:
    """Classify an announcement. Tries Ollama first, falls back to keywords."""
    global _ollama_available

    # Try Ollama (skip if already known to be down)
    if _ollama_available is not False:
        try:
            prompt = f"""Classify this Pakistan Stock Exchange announcement.
Subject: {subject}
Category: {category}

Return ONLY a JSON object with these fields:
- type: one of DIVIDEND, EARNINGS_POSITIVE, EARNINGS_NEGATIVE, RIGHTS_ISSUE, BONUS_SHARES, REGULATORY, OTHER
- sentiment: float -1.0 (very negative) to +1.0 (very positive)
- summary: one sentence summary
- sector_impact: float 0.0 (company-specific) to 1.0 (entire sector affected)

JSON only, no other text:"""

            resp = requests.post(OLLAMA_URL, json={
                "model": "llama3.1:8b",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 200},
            }, timeout=5)

            if resp.status_code == 200:
                _ollama_available = True
                text = resp.json().get("response", "")
                start = text.find("{")
                end = text.rfind("}") + 1
                if start >= 0 and end > start:
                    return json.loads(text[start:end])
        except Exception as e:
            _ollama_available = False
            logger.debug(f"LLM classification failed: {e}")

    # Keyword fallback
    subject_lower = subject.lower()
    cat_lower = (category or "").lower()

    if "dividend" in subject_lower or "interim" in subject_lower or cat_lower == "dividend":
        return {"type": "DIVIDEND", "sentiment": 0.5, "summary": subject[:100], "sector_impact": 0.3}
    elif "bonus" in subject_lower or cat_lower == "bonus":
        return {"type": "BONUS_SHARES", "sentiment": 0.4, "summary": subject[:100], "sector_impact": 0.2}
    elif "right" in subject_lower:
        return {"type": "RIGHTS_ISSUE", "sentiment": -0.1, "summary": subject[:100], "sector_impact": 0.1}
    elif "loss" in subject_lower or "decline" in subject_lower:
        return {"type": "EARNINGS_NEGATIVE", "sentiment": -0.5, "summary": subject[:100], "sector_impact": 0.4}
    elif "profit" in subject_lower or "earning" in subject_lower:
        return {"type": "EARNINGS_POSITIVE", "sentiment": 0.5, "summary": subject[:100], "sector_impact": 0.5}
    elif "secp" in subject_lower or "regulation" in subject_lower or cat_lower == "regulatory":
        return {"type": "REGULATORY", "sentiment": 0.0, "summary": subject[:100], "sector_impact": 0.8}
    elif cat_lower in ("book_closure", "agm"):
        return {"type": "DIVIDEND", "sentiment": 0.2, "summary": subject[:100], "sector_impact": 0.1}
    else:
        return {"type": "OTHER", "sentiment": 0.0, "summary": subject[:100], "sector_impact": 0.1}


def scan_contagion(
    days_back: int = 7,
    min_confidence: float = 0.2,
) -> list[ContagionSignal]:
    """Scan recent announcements for peer contagion signals."""
    con = analytics_con()

    # Get recent announcements
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    try:
        ann_df = con.execute("""
            SELECT symbol, announcement_date, title, category
            FROM company_announcements
            WHERE announcement_date >= ?
            ORDER BY announcement_date DESC
        """, [cutoff]).df()
    except Exception:
        con.close()
        return []

    if ann_df.empty:
        con.close()
        return []

    # Get sector mapping from eod_ohlcv (most reliable source)
    try:
        sec_df = con.execute("""
            SELECT DISTINCT symbol, sector_code FROM eod_ohlcv
            WHERE sector_code IS NOT NULL AND sector_code != ''
        """).df()
        symbol_to_sector: dict[str, str] = dict(zip(sec_df["symbol"], sec_df["sector_code"]))
        sector_to_symbols: dict[str, list[str]] = {}
        for sym, sec in zip(sec_df["symbol"], sec_df["sector_code"]):
            sector_to_symbols.setdefault(sec, []).append(sym)
    except Exception:
        symbol_to_sector = {}
        sector_to_symbols = {}

    con.close()

    signals = []

    for _, row in ann_df.iterrows():
        symbol = row["symbol"]
        title = row.get("title", "")
        category = row.get("category", "")

        if not title:
            continue

        # Classify
        classification = _classify_announcement(title, category)
        ann_type = classification.get("type", "OTHER")
        sentiment = classification.get("sentiment", 0)
        sector_impact = classification.get("sector_impact", 0.1)

        if ann_type not in CONTAGION_RULES:
            continue

        rules = CONTAGION_RULES[ann_type]
        sector = symbol_to_sector.get(symbol, "Unknown")

        # Find sector peers
        peers = [s for s in sector_to_symbols.get(sector, []) if s != symbol]
        if not peers:
            continue

        # Score peer impact
        affected = []
        for peer in peers[:10]:
            peer_confidence = abs(sentiment) * sector_impact * rules["decay"]

            if peer_confidence < min_confidence:
                continue

            if rules["peer_direction"] == "SAME":
                peer_direction = 1 if sentiment > 0 else -1
            else:
                peer_direction = 0

            affected.append({
                "symbol": peer,
                "direction": peer_direction,
                "confidence": round(peer_confidence, 3),
                "reason": f"{ann_type} at {symbol} -> {sector} peer",
            })

        if not affected:
            continue

        signals.append(ContagionSignal(
            source_symbol=symbol,
            announcement_type=ann_type,
            announcement_summary=classification.get("summary", title[:100]),
            sentiment=round(sentiment, 2),
            affected_peers=affected,
            sector=sector,
            detected_at=datetime.now(PKT).isoformat(),
            evidence=f"{ann_type} at {symbol} ({sector}): "
                     f"sentiment {sentiment:+.2f}, "
                     f"{len(affected)} peers affected",
        ))

    signals.sort(key=lambda s: abs(s.sentiment) * len(s.affected_peers), reverse=True)
    return signals
