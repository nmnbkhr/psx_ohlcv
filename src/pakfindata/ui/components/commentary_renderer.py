"""Styled LLM commentary renderer — parses markdown into colored signal cards.

Splits LLM markdown by ### headings, classifies each section's sentiment/type,
and renders as color-coded cards with signal badges.

Falls back to plain st.markdown() if no headings are detected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

import streamlit as st


# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL STYLES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class SectionStyle:
    color: str       # border + badge color
    bg: str          # card background (color at ~6% opacity)
    icon: str        # unicode badge icon
    label: str       # short badge label


_STYLES = {
    "bullish":  SectionStyle("#00E676", "rgba(0,230,118,0.06)",  "\u25B2", "BULLISH"),
    "bearish":  SectionStyle("#FF5252", "rgba(255,82,82,0.06)",  "\u25BC", "BEARISH"),
    "neutral":  SectionStyle("#FFD600", "rgba(255,214,0,0.06)",  "\u25C6", "NEUTRAL"),
    "risk":     SectionStyle("#FF5252", "rgba(255,82,82,0.06)",  "\u26A0", "RISK"),
    "trade":    SectionStyle("#00D4AA", "rgba(0,212,170,0.06)",  "\u26A1", "TRADE"),
    "invest":   SectionStyle("#00E676", "rgba(0,230,118,0.06)",  "\u2191", "INVEST"),
    "macro":    SectionStyle("#2196F3", "rgba(33,150,243,0.06)", "\u2139", "MACRO"),
    "sector":   SectionStyle("#FFD600", "rgba(255,214,0,0.06)",  "\u25CF", "SECTOR"),
    "breadth":  SectionStyle("#2196F3", "rgba(33,150,243,0.06)", "\u2139", "BREADTH"),
    "info":     SectionStyle("#2196F3", "rgba(33,150,243,0.06)", "\u2139", "INFO"),
}

# LLM-provided explicit tags → style key
_EXPLICIT_TAGS = {
    "BULLISH": "bullish",
    "BEARISH": "bearish",
    "NEUTRAL": "neutral",
    "CAUTION": "risk",
    "INFO":    "info",
}

# Heading substring → style key (checked in order)
_HEADING_MAP: list[tuple[str, str]] = [
    ("risk",        "risk"),
    ("warning",     "risk"),
    ("caution",     "risk"),
    ("trading",     "trade"),
    ("trade idea",  "trade"),
    ("investment",  "invest"),
    ("opportunit",  "invest"),
    ("macro",       "macro"),
    ("rates context", "macro"),
    ("rate",        "macro"),
    ("sector",      "sector"),
    ("rotation",    "sector"),
    ("index",       "breadth"),
    ("breadth",     "breadth"),
    ("fixed income", "macro"),
    ("bond",        "macro"),
    ("t-bill",      "macro"),
    ("yield",       "macro"),
    ("mutual fund", "invest"),
    ("fund",        "invest"),
    ("commodit",    "trade"),
    ("pmex",        "trade"),
    ("gold",        "trade"),
    ("oil",         "trade"),
    ("asset alloc", "info"),
    ("diversif",    "info"),
    ("portfolio",   "info"),
]

# Keyword sets for dynamic verdict classification
_BULLISH_KW = {"bullish", "rally", "uptrend", "strong", "positive", "buy", "accumulate", "upside", "breakout", "surge"}
_BEARISH_KW = {"bearish", "selloff", "sell-off", "downtrend", "weak", "negative", "decline", "correction", "risk-off", "crash"}


# ═══════════════════════════════════════════════════════════════════════════
# PARSER
# ═══════════════════════════════════════════════════════════════════════════

def _parse_sections(text: str) -> tuple[str, list[dict]]:
    """Split markdown by ## or ### headings.

    Returns (preamble, sections) where each section is
    {"heading": str, "body": str, "tag": str|None}.
    """
    # Split on lines starting with ## or ### (but not #### or deeper)
    parts = re.split(r"^#{2,3}\s+", text, flags=re.MULTILINE)

    preamble = parts[0].strip() if parts else ""
    sections: list[dict] = []

    for part in parts[1:]:
        lines = part.split("\n", 1)
        raw_heading = lines[0].strip()
        body = lines[1].strip() if len(lines) > 1 else ""

        # Extract explicit tag like [BULLISH]
        tag = None
        tag_match = re.search(r"\[([A-Z]+)\]\s*$", raw_heading)
        if tag_match:
            tag = tag_match.group(1)
            raw_heading = raw_heading[:tag_match.start()].strip().rstrip("—-").strip()

        sections.append({"heading": raw_heading, "body": body, "tag": tag})

    return preamble, sections


# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════

def _classify_section(heading: str, body: str, tag: str | None) -> SectionStyle:
    """Determine visual style for a section."""
    h_lower = heading.lower()

    # 1) Verdict heading → use LLM tag if available, else detect from body
    if "verdict" in h_lower:
        if tag and tag in _EXPLICIT_TAGS:
            return _STYLES[_EXPLICIT_TAGS[tag]]
        return _detect_verdict_sentiment(body)

    # 2) Heading name mapping (takes priority over LLM tag for known sections)
    for substring, style_key in _HEADING_MAP:
        if substring in h_lower:
            return _STYLES[style_key]

    # 3) Explicit LLM tag (for unknown/custom sections)
    if tag and tag in _EXPLICIT_TAGS:
        return _STYLES[_EXPLICIT_TAGS[tag]]

    # 4) Keyword scan on heading + first 300 chars of body
    sample = (h_lower + " " + body[:300]).lower()
    bull_hits = sum(1 for kw in _BULLISH_KW if kw in sample)
    bear_hits = sum(1 for kw in _BEARISH_KW if kw in sample)
    if bear_hits > bull_hits:
        return _STYLES["risk"]
    if bull_hits > bear_hits:
        return _STYLES["invest"]

    # 5) Default
    return _STYLES["info"]


def _detect_verdict_sentiment(body: str) -> SectionStyle:
    """Classify Market Verdict body as bullish/bearish/neutral."""
    text = body[:500].lower()
    bull = sum(1 for kw in _BULLISH_KW if kw in text)
    bear = sum(1 for kw in _BEARISH_KW if kw in text)

    if bull > bear:
        return _STYLES["bullish"]
    if bear > bull:
        return _STYLES["bearish"]
    return _STYLES["neutral"]


# ═══════════════════════════════════════════════════════════════════════════
# HTML CARD RENDERER
# ═══════════════════════════════════════════════════════════════════════════

def _render_card_header(heading: str, style: SectionStyle) -> str:
    """Return HTML for the card header with badge."""
    return (
        f'<div style="'
        f"border-left:4px solid {style.color};"
        f"background:{style.bg};"
        f"border-radius:0 8px 8px 0;"
        f"padding:16px 20px 4px 20px;"
        f"margin:14px 0 0 0;"
        f'">'
        # Badge
        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
        f'<span style="'
        f"display:inline-flex;align-items:center;justify-content:center;"
        f"width:24px;height:24px;border-radius:6px;"
        f"background:{style.color}22;color:{style.color};"
        f"font-size:13px;font-weight:700;"
        f'">{style.icon}</span>'
        f'<span style="'
        f"color:{style.color};font-weight:700;font-size:10px;"
        f"text-transform:uppercase;letter-spacing:0.1em;"
        f"background:{style.color}18;padding:2px 8px;border-radius:4px;"
        f'">{style.label}</span>'
        # Heading text
        f'<span style="'
        f"color:#e0e0e0;font-weight:600;font-size:15px;"
        f"margin-left:4px;"
        f'">{heading}</span>'
        f"</div>"
        f"</div>"
    )


def _render_card_body_wrapper(style: SectionStyle) -> tuple[str, str]:
    """Return (open_html, close_html) for the body wrapper that continues the card."""
    open_html = (
        f'<div style="'
        f"border-left:4px solid {style.color};"
        f"background:{style.bg};"
        f"border-radius:0 0 8px 0;"
        f"padding:0 20px 14px 20px;"
        f"margin:0 0 2px 0;"
        f"font-size:14px;line-height:1.7;color:#d0d0d0;"
        f'">'
    )
    close_html = "</div>"
    return open_html, close_html


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════

def render_styled_commentary(text: str, page_label: str = "Research Note") -> None:
    """Render LLM commentary as color-coded signal cards with download button.

    Parses the response by ### headings, classifies each section,
    and renders styled HTML cards. Falls back to plain markdown
    if no headings are found.
    """
    if not text or not text.strip():
        return

    preamble, sections = _parse_sections(text)

    # Fallback: no headings detected
    if not sections:
        st.markdown(text)
        return

    # Render preamble (text before first heading)
    if preamble:
        st.markdown(preamble)

    # Render each section as a styled card
    for sec in sections:
        style = _classify_section(sec["heading"], sec["body"], sec.get("tag"))

        # Card header (HTML)
        header_html = _render_card_header(sec["heading"], style)
        st.markdown(header_html, unsafe_allow_html=True)

        # Card body — use body wrapper for the colored border continuation,
        # but render markdown content natively for proper formatting
        if sec["body"]:
            body_open, body_close = _render_card_body_wrapper(style)
            st.markdown(body_open, unsafe_allow_html=True)
            st.markdown(sec["body"])
            st.markdown(body_close, unsafe_allow_html=True)

    # Download button after commentary
    render_download_button(text, page_label)


# ═══════════════════════════════════════════════════════════════════════════
# DOWNLOAD BUTTON
# ═══════════════════════════════════════════════════════════════════════════

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  body {{
    background: #0e1117; color: #d0d0d0; font-family: 'Segoe UI', system-ui, sans-serif;
    max-width: 900px; margin: 40px auto; padding: 0 24px; line-height: 1.7;
  }}
  h1 {{ color: #e0e0e0; border-bottom: 2px solid #2196F3; padding-bottom: 10px; }}
  .meta {{ color: #888; font-size: 13px; margin-bottom: 24px; }}
  .card {{
    border-radius: 0 8px 8px 0; padding: 16px 20px; margin: 14px 0;
  }}
  .card-header {{
    display: flex; align-items: center; gap: 8px; margin-bottom: 8px;
  }}
  .badge {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 24px; height: 24px; border-radius: 6px;
    font-size: 13px; font-weight: 700;
  }}
  .label {{
    font-weight: 700; font-size: 10px; text-transform: uppercase;
    letter-spacing: 0.1em; padding: 2px 8px; border-radius: 4px;
  }}
  .heading {{ color: #e0e0e0; font-weight: 600; font-size: 15px; margin-left: 4px; }}
  .body {{ font-size: 14px; line-height: 1.7; }}
  .body p {{ margin: 6px 0; }}
  .body ul, .body ol {{ margin: 6px 0 6px 20px; }}
  .preamble {{ margin-bottom: 16px; font-size: 14px; }}
  strong {{ color: #e0e0e0; }}
  a {{ color: #2196F3; }}
  .footer {{
    margin-top: 40px; padding-top: 16px; border-top: 1px solid #333;
    color: #666; font-size: 12px; text-align: center;
  }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="meta">Generated: {timestamp} &nbsp;|&nbsp; Source: PakFinData Research</div>
{content}
<div class="footer">Generated by PakFinData AI Research Engine</div>
</body>
</html>"""


def _markdown_to_html_simple(md: str) -> str:
    """Minimal markdown → HTML for body text (bold, italic, lists, paragraphs)."""
    html = md
    # Bold
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    # Italic
    html = re.sub(r"\*(.+?)\*", r"<em>\1</em>", html)
    # Unordered list items
    html = re.sub(r"^[-*]\s+(.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)
    # Numbered list items
    html = re.sub(r"^\d+\.\s+(.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)
    # Wrap consecutive <li> in <ul>
    html = re.sub(r"((?:<li>.*?</li>\n?)+)", r"<ul>\1</ul>", html)
    # Paragraphs (double newline)
    parts = re.split(r"\n{2,}", html)
    result = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if p.startswith("<ul>") or p.startswith("<ol>") or p.startswith("<li>"):
            result.append(p)
        else:
            result.append(f"<p>{p}</p>")
    return "\n".join(result)


def _build_download_html(text: str, title: str) -> str:
    """Build a styled HTML document from LLM commentary text."""
    preamble, sections = _parse_sections(text)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    parts: list[str] = []
    if preamble:
        parts.append(f'<div class="preamble">{_markdown_to_html_simple(preamble)}</div>')

    if sections:
        for sec in sections:
            style = _classify_section(sec["heading"], sec["body"], sec.get("tag"))
            body_html = _markdown_to_html_simple(sec["body"]) if sec["body"] else ""
            parts.append(
                f'<div class="card" style="border-left:4px solid {style.color};background:{style.bg};">'
                f'<div class="card-header">'
                f'<span class="badge" style="background:{style.color}22;color:{style.color};">{style.icon}</span>'
                f'<span class="label" style="color:{style.color};background:{style.color}18;">{style.label}</span>'
                f'<span class="heading">{sec["heading"]}</span>'
                f'</div>'
                f'<div class="body">{body_html}</div>'
                f'</div>'
            )
    else:
        parts.append(f'<div class="body">{_markdown_to_html_simple(text)}</div>')

    return _HTML_TEMPLATE.format(
        title=title,
        timestamp=timestamp,
        content="\n".join(parts),
    )


def render_download_button(
    text: str,
    page_label: str = "Research Note",
    key: str | None = None,
) -> None:
    """Render a styled download button for AI commentary.

    Args:
        text: Raw LLM markdown text
        page_label: Label for the document title / filename
        key: Streamlit widget key (auto-generated if None)
    """
    if not text or not text.strip():
        return

    btn_key = key or f"dl_{page_label.lower().replace(' ', '_')}"
    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{page_label.lower().replace(' ', '_')}_{date_str}.html"

    html_doc = _build_download_html(text, page_label)

    st.download_button(
        label=f"Download {page_label}",
        data=html_doc,
        file_name=filename,
        mime="text/html",
        key=btn_key,
    )
