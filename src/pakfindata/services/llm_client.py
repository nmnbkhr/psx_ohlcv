"""Central LLM client for pakfindata.

All Ollama LLM calls in the app go through this module.
Ollama (native Ubuntu, http://localhost:11434) is the primary backend.
Rule-based fallback always works — app never breaks if Ollama is down.

Usage:
    from pakfindata.services.llm_client import llm, LLMClient

    # Simple call
    result = llm.complete("Summarize this PSX announcement: ...")

    # Structured JSON response
    result = llm.complete_json(prompt, schema_hint={"sentiment": float})

    # Check status (for Streamlit sidebar)
    status = llm.status()
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger("pakfindata.llm_client")

# ─── Config ─────────────────────────────────────────────────────────────────

import os as _os
OLLAMA_BASE_URL = _os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

# Model registry — ordered by preference for each use case
MODEL_REGISTRY = {
    "sentiment": [
        "gemma4:e2b",
        "llama3.1:8b",
        "deepseek-r1:7b",
    ],
    "scoring": [
        "deepseek-r1:7b",
        "llama3.1:8b",
        "gemma4:e2b",
    ],
    "summary": [
        "llama3.1:8b",
        "gemma4:e2b",
        "deepseek-r1:7b",
    ],
    "general": [
        "gemma4:e2b",
        "llama3.1:8b",
        "deepseek-r1:7b",
    ],
    "commentary": [
        "llama3.1:8b",
        "gemma4:e2b",
        "deepseek-r1:7b",
    ],
}

# Explicit model identifiers for UI selectors and direct model pinning
OLLAMA_MODEL_FAST = "llama3.1:8b"
OLLAMA_MODEL_DEEP = "deepseek-r1:7b"
OLLAMA_MODEL_GEMMA = "gemma4:e2b"

PSX_SYSTEM_PROMPT = """You are a financial analyst specializing in Pakistan Stock Exchange (PSX).

PSX-specific context:
- Cash dividend >20% = bullish (PSX retail loves high yields)
- Rights issue = almost always bearish (dilution fear dominates)
- Director buying = very bullish (insider signal, mandatory disclosure on PSX)
- EPS growth >20% YoY = significant beat
- 534 of 564 PSX companies have ZERO analyst coverage
- PKR amounts — dividends quoted as % of par value (Rs10 par)
- Bonus shares = neutral to slightly bullish on PSX

Always respond ONLY with the requested JSON. No markdown, no explanation, no preamble."""


# ─── Response dataclass ──────────────────────────────────────────────────────

@dataclass
class LLMResponse:
    text: str = ""
    parsed: dict = field(default_factory=dict)
    model: str = ""
    elapsed: float = 0.0
    success: bool = False
    error: str = ""
    fallback_used: bool = False


def _is_reasoning_model(model: str) -> bool:
    """Check if model is a reasoning model that produces <think> blocks."""
    name = model.lower()
    return "deepseek" in name or "r1" in name


# ─── Main client ─────────────────────────────────────────────────────────────

class LLMClient:
    """Central Ollama client for pakfindata. Singleton via module-level `llm`."""

    def __init__(self, base_url: str = OLLAMA_BASE_URL):
        self.base_url = base_url
        self._available_models: list[str] = []
        self._last_status_check: float = 0
        self._is_running: bool = False

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self, force_refresh: bool = False) -> dict:
        """Check Ollama status. Cached for 30s."""
        now = time.time()
        if not force_refresh and (now - self._last_status_check) < 30:
            return {
                "running": self._is_running,
                "models": self._available_models,
                "url": self.base_url,
            }

        try:
            r = requests.get(f"{self.base_url}/api/tags", timeout=3)
            if r.status_code == 200:
                self._available_models = [m["name"] for m in r.json().get("models", [])]
                self._is_running = True
            else:
                self._is_running = False
                self._available_models = []
        except Exception:
            self._is_running = False
            self._available_models = []

        self._last_status_check = now
        return {
            "running": self._is_running,
            "models": self._available_models,
            "url": self.base_url,
        }

    def is_running(self) -> bool:
        return self.status()["running"]

    def best_model(self, use_case: str = "general") -> str | None:
        """Return best available model for a use case."""
        st = self.status()
        if not st["running"]:
            return None
        available = st["models"]
        for candidate in MODEL_REGISTRY.get(use_case, MODEL_REGISTRY["general"]):
            for m in available:
                if m.startswith(candidate.split(":")[0]):
                    return m
        return available[0] if available else None

    # ── Core completion (generate) ──────────────────────────────────────────

    def complete(
        self,
        prompt: str,
        model: str | None = None,
        use_case: str = "general",
        system: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 400,
        timeout: int = 60,
    ) -> LLMResponse:
        """Call Ollama with a prompt. Returns LLMResponse."""
        if model is None:
            model = self.best_model(use_case)

        if model is None:
            return LLMResponse(
                success=False,
                error="Ollama not running or no models available",
                fallback_used=True,
            )

        t0 = time.time()
        try:
            payload: dict[str, Any] = {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                },
            }
            if system:
                payload["system"] = system

            r = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=timeout,
            )
            elapsed = time.time() - t0

            if r.status_code != 200:
                return LLMResponse(
                    success=False,
                    error=f"HTTP {r.status_code}",
                    elapsed=elapsed,
                    model=model,
                )

            text = r.json().get("response", "").strip()
            return LLMResponse(
                text=text,
                model=model,
                elapsed=elapsed,
                success=True,
            )

        except requests.Timeout:
            return LLMResponse(
                success=False,
                error=f"Timeout after {timeout}s",
                elapsed=time.time() - t0,
                model=model or "",
            )
        except requests.ConnectionError:
            return LLMResponse(
                success=False,
                error="Ollama not reachable — run: sudo systemctl start ollama",
                elapsed=time.time() - t0,
                fallback_used=True,
            )
        except Exception as e:
            return LLMResponse(
                success=False,
                error=str(e),
                elapsed=time.time() - t0,
                model=model or "",
            )

    def complete_json(
        self,
        prompt: str,
        model: str | None = None,
        use_case: str = "general",
        system: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 400,
        timeout: int = 60,
    ) -> LLMResponse:
        """Like complete() but extracts JSON from the response."""
        resp = self.complete(
            prompt=prompt, model=model, use_case=use_case,
            system=system, temperature=temperature,
            max_tokens=max_tokens, timeout=timeout,
        )
        if not resp.success:
            return resp

        text = resp.text

        # Method 1: direct parse
        try:
            resp.parsed = json.loads(text)
            return resp
        except json.JSONDecodeError:
            pass

        # Method 2: find first {...} block
        match = re.search(r'\{[^{}]+\}', text, re.DOTALL)
        if match:
            try:
                resp.parsed = json.loads(match.group())
                return resp
            except json.JSONDecodeError:
                pass

        # Method 3: ```json ... ``` block
        match = re.search(r'```(?:json)?\s*(\{.+?\})\s*```', text, re.DOTALL)
        if match:
            try:
                resp.parsed = json.loads(match.group(1))
                return resp
            except json.JSONDecodeError:
                pass

        resp.error = "JSON parse failed — raw text returned in .text"
        return resp

    # ── Chat completion (system + user messages) ────────────────────────────

    def complete_chat(
        self,
        system: str,
        user: str,
        model: str | None = None,
        use_case: str = "general",
        temperature: float = 0.4,
        max_tokens: int = 600,
        timeout: int = 60,
    ) -> LLMResponse:
        """Call Ollama /api/chat with system + user messages.

        Auto-handles reasoning models (deepseek/r1):
        - Triples token budget (think blocks consume output tokens)
        - Extends timeout to 120s
        - Strips <think>...</think> blocks from response
        """
        if model is None:
            model = self.best_model(use_case)

        if model is None:
            return LLMResponse(
                success=False,
                error="Ollama not running or no models available",
                fallback_used=True,
            )

        is_reasoning = _is_reasoning_model(model)
        actual_tokens = max_tokens * 3 if is_reasoning else max_tokens
        actual_timeout = max(timeout, 120) if is_reasoning else timeout

        t0 = time.time()
        try:
            payload: dict[str, Any] = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "stream": False,
                "options": {
                    "num_predict": actual_tokens,
                    "temperature": temperature,
                },
            }

            r = requests.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=actual_timeout,
            )
            elapsed = time.time() - t0

            if r.status_code != 200:
                return LLMResponse(
                    success=False, error=f"HTTP {r.status_code}",
                    elapsed=elapsed, model=model,
                )

            content = r.json().get("message", {}).get("content", "").strip()

            # Strip <think> blocks from reasoning models
            if is_reasoning and "<think>" in content:
                content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
                content = re.sub(r"<think>.*", "", content, flags=re.DOTALL).strip()

            return LLMResponse(
                text=content, model=model, elapsed=elapsed, success=True,
            )

        except requests.Timeout:
            return LLMResponse(
                success=False, error=f"Timeout after {actual_timeout}s",
                elapsed=time.time() - t0, model=model or "",
            )
        except requests.ConnectionError:
            return LLMResponse(
                success=False,
                error="Ollama not reachable — run: sudo systemctl start ollama",
                elapsed=time.time() - t0, fallback_used=True,
            )
        except Exception as e:
            return LLMResponse(
                success=False, error=str(e),
                elapsed=time.time() - t0, model=model or "",
            )

    def complete_chat_text(
        self,
        system: str,
        user: str,
        model: str | None = None,
        use_case: str = "general",
        temperature: float = 0.4,
        max_tokens: int = 600,
        timeout: int = 60,
    ) -> str | None:
        """Like complete_chat() but returns plain text or None on failure.

        Drop-in replacement for the old _ollama_call() signature.
        """
        resp = self.complete_chat(
            system=system, user=user, model=model, use_case=use_case,
            temperature=temperature, max_tokens=max_tokens, timeout=timeout,
        )
        if resp.success and resp.text:
            return resp.text
        return None

    # ── Domain-specific helpers ───────────────────────────────────────────────

    def score_announcement(
        self,
        text: str,
        ann_type: str = "GENERAL",
        symbol: str = "",
        model: str | None = None,
    ) -> dict:
        """Score a PSX corporate announcement for sentiment.

        Returns dict with sentiment, confidence, impact, phrases, reason.
        Falls back to neutral if Ollama is unavailable.
        """
        prompt = f"""Analyze this Pakistan Stock Exchange (PSX) corporate announcement.

Symbol: {symbol}
Type: {ann_type}
Announcement: {text[:2000]}

Respond ONLY with JSON (no explanation):
{{"sentiment": <-1.0 to 1.0>, "confidence": <0.0 to 1.0>, "impact_pct": <expected % price move>, "phrases": [<3 key terms>], "reason": "<one sentence>"}}"""

        resp = self.complete_json(
            prompt=prompt,
            use_case="sentiment",
            system=PSX_SYSTEM_PROMPT,
            model=model,
            max_tokens=250,
        )

        if resp.success and resp.parsed:
            p = resp.parsed
            return {
                "sentiment": float(max(-1, min(1, p.get("sentiment", 0)))),
                "confidence": float(max(0, min(1, p.get("confidence", 0.5)))),
                "impact_estimate": float(p.get("impact_pct", 0)),
                "key_phrases": p.get("phrases", [])[:5],
                "reason": str(p.get("reason", "")),
                "method": f"OLLAMA:{resp.model}",
                "elapsed": resp.elapsed,
            }

        return {
            "sentiment": 0.0,
            "confidence": 0.0,
            "impact_estimate": 0.0,
            "key_phrases": [],
            "reason": "",
            "method": "OLLAMA_FAILED",
            "error": resp.error,
        }

    def score_strategy_signal(
        self,
        symbol: str,
        signals: list[dict],
        price: float,
        model: str | None = None,
    ) -> dict:
        """Ask the LLM to score/validate fused strategy signals."""
        signal_text = "\n".join(
            f"- {s.get('label', s.get('name', '?'))}: {s.get('signal', 'n/a')} "
            f"({'▲' if s.get('direction', 0) > 0 else '▼' if s.get('direction', 0) < 0 else '—'}, "
            f"conf={s.get('confidence', 0):.0%})"
            for s in signals if s.get("enabled")
        )

        prompt = f"""You are a quant analyst reviewing algorithmic trading signals for {symbol} on PSX.

Current price: {price:,.2f} PKR
Active strategy signals:
{signal_text}

Based on these signals, provide a trading assessment.

Respond ONLY with JSON:
{{"direction": <1=BUY, 0=HOLD, -1=SELL>, "confidence": <0.0 to 1.0>, "reasoning": "<one sentence>", "risk": "<LOW|MEDIUM|HIGH>"}}"""

        resp = self.complete_json(
            prompt=prompt, use_case="scoring", model=model, max_tokens=200,
        )

        if resp.success and resp.parsed:
            p = resp.parsed
            return {
                "direction": int(p.get("direction", 0)),
                "confidence": float(max(0, min(1, p.get("confidence", 0.5)))),
                "reasoning": str(p.get("reasoning", "")),
                "risk": str(p.get("risk", "MEDIUM")),
                "method": f"OLLAMA:{resp.model}",
            }

        return {
            "direction": 0, "confidence": 0,
            "reasoning": "LLM unavailable", "risk": "MEDIUM",
            "method": "OLLAMA_FAILED",
        }

    def summarize_company(self, symbol: str, context: str, model: str | None = None) -> str:
        """Generate a brief company/situation summary."""
        prompt = f"""Summarize the current investment situation for {symbol} (PSX-listed company).

Context:
{context[:3000]}

Write 2-3 sentences covering: recent performance, key risk/opportunity, PSX-specific factors.
Be direct and factual. No fluff."""

        resp = self.complete(
            prompt=prompt, use_case="summary", model=model,
            max_tokens=300, temperature=0.3,
        )
        return resp.text if resp.success else ""


# ── Module-level singleton ─────────────────────────────────────────────────────
llm = LLMClient()
