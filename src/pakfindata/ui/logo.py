"""PakFinData brand elements — logo, powered-by, disclaimer."""

import streamlit as st

# Brand colors (matches Bloomberg theme tokens)
_ACCENT = "#2F81F7"
_GREEN = "#00C853"
_TEXT = "#EAECEF"
_MUTED = "#6B7280"
_BG = "#12161C"
_BORDER = "#1E2329"


def _logo_svg(size: int = 28) -> str:
    """Inline SVG mark — stylized 'P' with chart bars."""
    return f"""<svg width="{size}" height="{size}" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
  <rect width="32" height="32" rx="4" fill="{_ACCENT}"/>
  <rect x="6" y="18" width="4" height="8" rx="1" fill="#fff" opacity="0.6"/>
  <rect x="12" y="12" width="4" height="14" rx="1" fill="#fff" opacity="0.8"/>
  <rect x="18" y="8" width="4" height="18" rx="1" fill="#fff"/>
  <rect x="24" y="14" width="4" height="12" rx="1" fill="{_GREEN}" opacity="0.9"/>
</svg>"""


def render_logo(variant: str = "sidebar") -> None:
    """Render the PakFinData logo.

    Args:
        variant: 'sidebar' for compact sidebar logo, 'header' for page header.
    """
    svg = _logo_svg(28 if variant == "sidebar" else 32)

    if variant == "sidebar":
        st.markdown(
            f"""<div style="display:flex;align-items:center;gap:10px;padding:4px 0 8px 0;">
  {svg}
  <div>
    <div style="font-size:16px;font-weight:700;font-family:ui-monospace,monospace;
                color:{_TEXT};letter-spacing:-0.02em;line-height:1.1;">
      PakFinData
    </div>
    <div style="font-size:9px;font-weight:500;color:{_MUTED};letter-spacing:0.12em;
                text-transform:uppercase;line-height:1;">
      TERMINAL
    </div>
  </div>
</div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
  {svg}
  <div style="font-size:22px;font-weight:700;font-family:ui-monospace,monospace;
              color:{_TEXT};letter-spacing:-0.02em;">
    PakFinData <span style="font-weight:400;color:{_MUTED};font-size:14px;">Terminal</span>
  </div>
</div>""",
            unsafe_allow_html=True,
        )


def render_powered_by() -> None:
    """Render 'Powered by PakFinData' attribution line."""
    svg = _logo_svg(14)
    st.markdown(
        f"""<div style="display:flex;align-items:center;justify-content:center;gap:6px;
                    padding:8px 0;font-size:11px;color:{_MUTED};font-family:ui-monospace,monospace;">
  {svg}
  <span>Powered by <span style="color:{_TEXT};font-weight:600;">PakFinData</span></span>
</div>""",
        unsafe_allow_html=True,
    )


def render_disclaimer() -> None:
    """Render regulatory / data disclaimer."""
    st.markdown(
        f"""<div style="text-align:center;padding:6px 16px;font-size:10px;color:{_MUTED};
                    font-family:ui-monospace,monospace;line-height:1.5;
                    border-top:1px solid {_BORDER};margin-top:4px;">
  Market data sourced from PSX, SBP, MUFAP, PMEX and other public sources.
  Provided for informational and research purposes only — not investment advice.
  Verify all data independently before making financial decisions.
</div>""",
        unsafe_allow_html=True,
    )
