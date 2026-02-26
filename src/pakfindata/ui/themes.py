"""
Theme System for PSX OHLCV Explorer.

Centralized theme tokens supporting multiple themes:
- default: Original trading terminal look
- bloomberg: Bloomberg Terminal-inspired high-contrast dark theme

Usage:
    from pakfindata.ui.themes import get_theme, get_theme_css, THEME_NAMES

    # Get theme tokens
    theme = get_theme("bloomberg")

    # Get CSS string for injection
    css = get_theme_css("bloomberg")
"""

from dataclasses import dataclass
from typing import Literal

# Available theme names
ThemeName = Literal["default", "bloomberg"]
THEME_NAMES: list[str] = ["default", "bloomberg"]


@dataclass(frozen=True)
class ThemeTokens:
    """Theme color and style tokens."""

    # Theme identifier
    name: str
    display_name: str

    # Backgrounds
    bg_main: str
    bg_panel: str
    bg_elevated: str
    bg_card: str

    # Borders & Gridlines
    border_primary: str
    border_subtle: str
    border_focus: str

    # Text
    text_primary: str
    text_secondary: str
    text_muted: str

    # Semantic Colors - Trading
    color_positive: str       # Buy / Gain / Up
    color_negative: str       # Sell / Loss / Down
    color_neutral: str        # Unchanged
    color_warning: str        # Alerts / Caution
    color_info: str           # Information
    color_accent: str         # Focus / Highlight / CTA

    # Semantic Colors - Light variants (for fills/backgrounds)
    color_positive_light: str
    color_negative_light: str
    color_warning_light: str
    color_info_light: str

    # Charts
    chart_bg: str
    chart_grid: str
    chart_axis: str
    chart_sma_20: str
    chart_sma_50: str
    chart_volume: str

    # Inputs & Buttons
    input_bg: str
    input_border: str
    input_text: str
    button_primary_bg: str
    button_primary_text: str
    button_secondary_bg: str
    button_secondary_text: str

    # Tables
    table_header_bg: str
    table_row_bg: str
    table_row_alt_bg: str
    table_row_hover_bg: str
    table_border: str

    # Typography
    font_mono: str
    font_sans: str

    # Borders & Radius
    border_radius_sm: str
    border_radius_md: str
    border_radius_lg: str


# =============================================================================
# DEFAULT THEME (Original Trading Terminal)
# =============================================================================

DEFAULT_THEME = ThemeTokens(
    name="default",
    display_name="Default Trading",

    # Backgrounds - Streamlit dark defaults with subtle modifications
    bg_main="transparent",
    bg_panel="rgba(255, 255, 255, 0.02)",
    bg_elevated="rgba(255, 255, 255, 0.04)",
    bg_card="rgba(255, 255, 255, 0.02)",

    # Borders
    border_primary="rgba(255, 255, 255, 0.1)",
    border_subtle="rgba(255, 255, 255, 0.05)",
    border_focus="#2196F3",

    # Text
    text_primary="#FAFAFA",
    text_secondary="#B0B0B0",
    text_muted="#888888",

    # Semantic Colors
    color_positive="#00C853",
    color_negative="#FF1744",
    color_neutral="#78909C",
    color_warning="#FFC107",
    color_info="#2196F3",
    color_accent="#2196F3",

    # Light variants
    color_positive_light="rgba(0, 200, 83, 0.15)",
    color_negative_light="rgba(255, 23, 68, 0.15)",
    color_warning_light="rgba(255, 193, 7, 0.15)",
    color_info_light="rgba(33, 150, 243, 0.15)",

    # Charts
    chart_bg="transparent",
    chart_grid="rgba(128, 128, 128, 0.15)",
    chart_axis="#888888",
    chart_sma_20="#FF9800",
    chart_sma_50="#9C27B0",
    chart_volume="#2196F3",

    # Inputs
    input_bg="rgba(255, 255, 255, 0.05)",
    input_border="rgba(255, 255, 255, 0.1)",
    input_text="#FAFAFA",
    button_primary_bg="#2196F3",
    button_primary_text="#FFFFFF",
    button_secondary_bg="rgba(255, 255, 255, 0.1)",
    button_secondary_text="#FAFAFA",

    # Tables
    table_header_bg="rgba(255, 255, 255, 0.05)",
    table_row_bg="transparent",
    table_row_alt_bg="rgba(255, 255, 255, 0.02)",
    table_row_hover_bg="rgba(255, 255, 255, 0.05)",
    table_border="rgba(255, 255, 255, 0.08)",

    # Typography
    font_mono="'JetBrains Mono', 'SF Mono', 'Consolas', monospace",
    font_sans="'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",

    # Border Radius
    border_radius_sm="4px",
    border_radius_md="6px",
    border_radius_lg="8px",
)


# =============================================================================
# BLOOMBERG THEME (Bloomberg Terminal-Inspired)
# High-contrast, data-dense, monospace-forward
# =============================================================================

BLOOMBERG_THEME = ThemeTokens(
    name="bloomberg",
    display_name="Bloomberg Terminal",

    # Backgrounds - Deep dark with subtle panel differentiation
    bg_main="#0B0E11",
    bg_panel="#12161C",
    bg_elevated="#161B22",
    bg_card="#12161C",

    # Borders - Sharp, visible gridlines
    border_primary="#1E2329",
    border_subtle="#1E2329",
    border_focus="#2F81F7",

    # Text - High contrast
    text_primary="#EAECEF",
    text_secondary="#9AA4B2",
    text_muted="#6B7280",

    # Semantic Colors - Bloomberg palette
    color_positive="#00C853",   # Bright green for gains/buy
    color_negative="#FF5252",   # Red for losses/sell
    color_neutral="#6B7280",    # Gray for unchanged
    color_warning="#FFB300",    # Amber for warnings
    color_info="#00B8D4",       # Cyan for info
    color_accent="#2F81F7",     # Bloomberg blue for focus/highlight

    # Light variants (for fills - more subtle than default)
    color_positive_light="rgba(0, 200, 83, 0.12)",
    color_negative_light="rgba(255, 82, 82, 0.12)",
    color_warning_light="rgba(255, 179, 0, 0.12)",
    color_info_light="rgba(0, 184, 212, 0.12)",

    # Charts - Bloomberg style with muted grids
    chart_bg="#0B0E11",
    chart_grid="rgba(30, 35, 41, 0.8)",
    chart_axis="#6B7280",
    chart_sma_20="#FFB300",     # Amber for SMA 20
    chart_sma_50="#00B8D4",     # Cyan for SMA 50
    chart_volume="#2F81F7",     # Bloomberg blue for volume

    # Inputs - Sharp, minimal
    input_bg="#12161C",
    input_border="#1E2329",
    input_text="#EAECEF",
    button_primary_bg="#2F81F7",
    button_primary_text="#FFFFFF",
    button_secondary_bg="#1E2329",
    button_secondary_text="#EAECEF",

    # Tables - Tight, data-dense
    table_header_bg="#161B22",
    table_row_bg="#0B0E11",
    table_row_alt_bg="#12161C",
    table_row_hover_bg="#1E2329",
    table_border="#1E2329",

    # Typography - Monospace-forward
    font_mono="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
    font_sans="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",

    # Border Radius - Sharp/minimal (Bloomberg aesthetic)
    border_radius_sm="2px",
    border_radius_md="2px",
    border_radius_lg="4px",
)


# Theme registry
THEMES: dict[str, ThemeTokens] = {
    "default": DEFAULT_THEME,
    "bloomberg": BLOOMBERG_THEME,
}


def get_theme(name: ThemeName = "bloomberg") -> ThemeTokens:
    """Get theme tokens by name.

    Args:
        name: Theme name ('default' or 'bloomberg')

    Returns:
        ThemeTokens instance
    """
    return THEMES.get(name, BLOOMBERG_THEME)


def get_css_variables(theme: ThemeTokens) -> str:
    """Generate CSS custom properties from theme tokens.

    Args:
        theme: ThemeTokens instance

    Returns:
        CSS string with :root variables
    """
    return f"""
:root {{
    /* Backgrounds */
    --bg-main: {theme.bg_main};
    --bg-panel: {theme.bg_panel};
    --bg-elevated: {theme.bg_elevated};
    --bg-card: {theme.bg_card};

    /* Borders */
    --border-primary: {theme.border_primary};
    --border-subtle: {theme.border_subtle};
    --border-focus: {theme.border_focus};

    /* Text */
    --text-primary: {theme.text_primary};
    --text-secondary: {theme.text_secondary};
    --text-muted: {theme.text_muted};

    /* Semantic Colors */
    --color-positive: {theme.color_positive};
    --color-negative: {theme.color_negative};
    --color-neutral: {theme.color_neutral};
    --color-warning: {theme.color_warning};
    --color-info: {theme.color_info};
    --color-accent: {theme.color_accent};

    /* Light Variants */
    --color-positive-light: {theme.color_positive_light};
    --color-negative-light: {theme.color_negative_light};
    --color-warning-light: {theme.color_warning_light};
    --color-info-light: {theme.color_info_light};

    /* Legacy aliases for backward compatibility */
    --gain-color: {theme.color_positive};
    --loss-color: {theme.color_negative};
    --neutral-color: {theme.color_neutral};
    --accent-color: {theme.color_accent};
    --warning-color: {theme.color_warning};
    --border-color: {theme.border_primary};

    /* Charts */
    --chart-bg: {theme.chart_bg};
    --chart-grid: {theme.chart_grid};
    --chart-axis: {theme.chart_axis};
    --chart-sma-20: {theme.chart_sma_20};
    --chart-sma-50: {theme.chart_sma_50};
    --chart-volume: {theme.chart_volume};

    /* Inputs */
    --input-bg: {theme.input_bg};
    --input-border: {theme.input_border};
    --input-text: {theme.input_text};
    --button-primary-bg: {theme.button_primary_bg};
    --button-primary-text: {theme.button_primary_text};
    --button-secondary-bg: {theme.button_secondary_bg};
    --button-secondary-text: {theme.button_secondary_text};

    /* Tables */
    --table-header-bg: {theme.table_header_bg};
    --table-row-bg: {theme.table_row_bg};
    --table-row-alt-bg: {theme.table_row_alt_bg};
    --table-row-hover-bg: {theme.table_row_hover_bg};
    --table-border: {theme.table_border};

    /* Typography */
    --font-mono: {theme.font_mono};
    --font-sans: {theme.font_sans};

    /* Border Radius */
    --radius-sm: {theme.border_radius_sm};
    --radius-md: {theme.border_radius_md};
    --radius-lg: {theme.border_radius_lg};
}}
"""


def get_theme_css(theme_name: ThemeName = "bloomberg") -> str:
    """Generate complete CSS for the specified theme.

    Args:
        theme_name: Theme name ('default' or 'bloomberg')

    Returns:
        Complete CSS string for injection via st.markdown
    """
    theme = get_theme(theme_name)

    # Bloomberg-specific overrides
    bloomberg_specific = ""
    if theme_name == "bloomberg":
        bloomberg_specific = f"""
/* === BLOOMBERG TERMINAL OVERRIDES === */

/* Main app background */
.stApp {{
    background-color: {theme.bg_main} !important;
}}

/* Sidebar - Bloomberg panel style */
[data-testid="stSidebar"] {{
    background-color: {theme.bg_panel} !important;
    border-right: 1px solid {theme.border_primary} !important;
}}

[data-testid="stSidebar"] > div:first-child {{
    background-color: {theme.bg_panel} !important;
}}

/* Header area */
[data-testid="stHeader"] {{
    background-color: {theme.bg_main} !important;
}}

/* All text defaults */
.stApp, .stApp p, .stApp span, .stApp div {{
    color: {theme.text_primary};
}}

/* Muted text */
.stApp small, .stApp .stCaption {{
    color: {theme.text_muted} !important;
}}
"""

    return f"""<style>
/* ============================================================================
   PSX OHLCV EXPLORER - {theme.display_name.upper()} THEME
   Generated theme: {theme.name}
   ============================================================================ */

{get_css_variables(theme)}

{bloomberg_specific}

/* === TYPOGRAPHY === */

/* Monospace for all numeric data */
.stMetric [data-testid="stMetricValue"],
.stDataFrame,
[data-testid="stTable"],
.ticker-item,
.price-value,
.volume-value,
code {{
    font-family: var(--font-mono) !important;
}}

.stMetric [data-testid="stMetricValue"] {{
    font-weight: 600;
    letter-spacing: -0.02em;
}}

/* === METRIC CARDS === */
[data-testid="stMetric"] {{
    background: var(--bg-card);
    border: 1px solid var(--border-primary);
    border-radius: var(--radius-sm);
    padding: 12px 16px;
}}

[data-testid="stMetric"]:hover {{
    border-color: var(--border-focus);
}}

/* Metric labels */
[data-testid="stMetricLabel"] {{
    color: var(--text-secondary) !important;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

/* Metric delta (change values) */
[data-testid="stMetricDelta"] svg {{
    display: none;
}}

[data-testid="stMetricDelta"][data-testid-delta="positive"] {{
    color: var(--color-positive) !important;
}}

[data-testid="stMetricDelta"][data-testid-delta="negative"] {{
    color: var(--color-negative) !important;
}}

/* === PRICE CHANGE COLORS === */
.price-up {{ color: var(--color-positive) !important; }}
.price-down {{ color: var(--color-negative) !important; }}
.price-neutral {{ color: var(--color-neutral) !important; }}

/* === DATA TABLES (Bloomberg-style) === */
.stDataFrame {{
    font-size: 13px !important;
    border: 1px solid var(--table-border) !important;
}}

.stDataFrame table {{
    border-collapse: collapse !important;
}}

.stDataFrame th {{
    background: var(--table-header-bg) !important;
    color: var(--text-secondary) !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    font-size: 11px !important;
    letter-spacing: 0.05em !important;
    padding: 8px 12px !important;
    border-bottom: 2px solid var(--border-primary) !important;
    text-align: left !important;
}}

.stDataFrame td {{
    padding: 6px 12px !important;
    border-bottom: 1px solid var(--table-border) !important;
    color: var(--text-primary) !important;
}}

.stDataFrame tr:nth-child(even) {{
    background: var(--table-row-alt-bg) !important;
}}

.stDataFrame tr:hover {{
    background: var(--table-row-hover-bg) !important;
}}

/* Right-align numeric columns (using nth-child for common positions) */
.stDataFrame td:nth-child(n+2),
.stDataFrame th:nth-child(n+2) {{
    text-align: right !important;
}}

/* First column left-aligned (usually symbol/name) */
.stDataFrame td:first-child,
.stDataFrame th:first-child {{
    text-align: left !important;
}}

/* === LOADING SKELETON === */
@keyframes shimmer {{
    0% {{ background-position: -200% 0; }}
    100% {{ background-position: 200% 0; }}
}}

.skeleton {{
    background: linear-gradient(90deg,
        var(--bg-panel) 25%,
        var(--bg-elevated) 50%,
        var(--bg-panel) 75%);
    background-size: 200% 100%;
    animation: shimmer 1.5s infinite;
    border-radius: var(--radius-sm);
}}

.skeleton-text {{
    height: 16px;
    margin: 8px 0;
}}

.skeleton-metric {{
    height: 32px;
    width: 80%;
    margin: 8px 0;
}}

/* === DATA FRESHNESS BADGES === */
.data-fresh {{
    color: var(--color-positive);
    background: var(--color-positive-light);
    padding: 2px 8px;
    border-radius: var(--radius-sm);
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

.data-stale {{
    color: var(--color-warning);
    background: var(--color-warning-light);
    padding: 2px 8px;
    border-radius: var(--radius-sm);
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

.data-old {{
    color: var(--color-negative);
    background: var(--color-negative-light);
    padding: 2px 8px;
    border-radius: var(--radius-sm);
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

/* === ALERT BANNERS (No gradients for Bloomberg) === */
.data-warning {{
    background: var(--color-warning-light);
    border: 1px solid var(--color-warning);
    border-left: 3px solid var(--color-warning);
    border-radius: var(--radius-sm);
    padding: 10px 14px;
    margin: 8px 0;
    font-size: 13px;
    color: var(--text-primary);
}}

.data-info {{
    background: var(--color-info-light);
    border: 1px solid var(--color-info);
    border-left: 3px solid var(--color-info);
    border-radius: var(--radius-sm);
    padding: 10px 14px;
    margin: 8px 0;
    font-size: 13px;
    color: var(--text-primary);
}}

.data-error {{
    background: var(--color-negative-light);
    border: 1px solid var(--color-negative);
    border-left: 3px solid var(--color-negative);
    border-radius: var(--radius-sm);
    padding: 10px 14px;
    margin: 8px 0;
    font-size: 13px;
    color: var(--text-primary);
}}

/* === EMPTY STATE === */
.empty-state {{
    text-align: center;
    padding: 40px 20px;
    color: var(--text-muted);
}}

.empty-state-icon {{
    font-size: 48px;
    margin-bottom: 16px;
    opacity: 0.5;
}}

/* === SECTION HEADERS === */
.section-header {{
    border-left: 3px solid var(--color-accent);
    padding-left: 12px;
    margin: 24px 0 16px 0;
    color: var(--text-primary);
}}

/* === KPI ROW === */
.kpi-row {{
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
}}

/* === TICKER TAPE === */
.ticker-item {{
    display: inline-block;
    padding: 4px 10px;
    margin: 2px 4px;
    border-radius: var(--radius-sm);
    font-size: 12px;
    font-weight: 500;
}}

.ticker-up {{
    background: var(--color-positive-light);
    border: 1px solid var(--color-positive);
    color: var(--color-positive);
}}

.ticker-down {{
    background: var(--color-negative-light);
    border: 1px solid var(--color-negative);
    color: var(--color-negative);
}}

/* === MARKET STATUS BADGE === */
.market-status {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: var(--radius-sm);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

.market-open {{
    background: var(--color-positive-light);
    color: var(--color-positive);
    border: 1px solid var(--color-positive);
}}

.market-closed {{
    background: var(--color-negative-light);
    color: var(--color-negative);
    border: 1px solid var(--color-negative);
}}

/* === INFO CARDS === */
.info-card {{
    background: var(--bg-card);
    border: 1px solid var(--border-primary);
    border-radius: var(--radius-sm);
    padding: 14px;
    margin-bottom: 12px;
}}

/* === BUTTONS === */
.stButton > button {{
    border-radius: var(--radius-sm) !important;
    font-weight: 500;
    transition: all 0.15s ease;
    border: 1px solid var(--border-primary) !important;
    background: var(--button-secondary-bg) !important;
    color: var(--button-secondary-text) !important;
}}

.stButton > button:hover {{
    border-color: var(--border-focus) !important;
    background: var(--bg-elevated) !important;
}}

.stButton > button:focus {{
    border-color: var(--border-focus) !important;
    box-shadow: 0 0 0 2px var(--color-info-light) !important;
}}

/* Primary button style */
.stButton > button[kind="primary"] {{
    background: var(--button-primary-bg) !important;
    color: var(--button-primary-text) !important;
    border-color: var(--button-primary-bg) !important;
}}

/* === INPUTS & SELECT BOXES === */
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stSelectbox > div > div,
.stMultiSelect > div > div {{
    background: var(--input-bg) !important;
    border: 1px solid var(--input-border) !important;
    border-radius: var(--radius-sm) !important;
    color: var(--input-text) !important;
}}

.stTextInput > div > div > input:focus,
.stNumberInput > div > div > input:focus {{
    border-color: var(--border-focus) !important;
    box-shadow: 0 0 0 2px var(--color-info-light) !important;
}}

/* === PROGRESS BARS === */
.stProgress > div > div {{
    border-radius: var(--radius-sm);
    background: var(--color-accent) !important;
}}

.stProgress > div {{
    background: var(--bg-panel) !important;
}}

/* === EXPANDERS === */
.streamlit-expanderHeader {{
    font-weight: 600;
    font-size: 13px;
    color: var(--text-primary) !important;
    background: var(--bg-card) !important;
    border: 1px solid var(--border-primary) !important;
    border-radius: var(--radius-sm) !important;
}}

.streamlit-expanderContent {{
    border: 1px solid var(--border-primary) !important;
    border-top: none !important;
    border-radius: 0 0 var(--radius-sm) var(--radius-sm) !important;
    background: var(--bg-panel) !important;
}}

/* === TABS === */
.stTabs [data-baseweb="tab-list"] {{
    gap: 4px;
    background: var(--bg-panel);
    padding: 4px;
    border-radius: var(--radius-sm);
}}

.stTabs [data-baseweb="tab"] {{
    border-radius: var(--radius-sm);
    padding: 8px 16px;
    font-weight: 500;
    font-size: 13px;
    color: var(--text-secondary) !important;
    background: transparent !important;
}}

.stTabs [data-baseweb="tab"]:hover {{
    color: var(--text-primary) !important;
    background: var(--bg-elevated) !important;
}}

.stTabs [aria-selected="true"] {{
    color: var(--text-primary) !important;
    background: var(--bg-elevated) !important;
    border-bottom: 2px solid var(--color-accent) !important;
}}

/* === ANNOUNCEMENT CARDS === */
.announcement-card {{
    border-left: 3px solid var(--color-warning);
    padding-left: 12px;
    margin: 8px 0;
    background: var(--bg-card);
    padding: 12px;
    border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
}}

/* === SCROLLBARS (Bloomberg-style) === */
::-webkit-scrollbar {{
    width: 8px;
    height: 8px;
}}

::-webkit-scrollbar-track {{
    background: var(--bg-main);
}}

::-webkit-scrollbar-thumb {{
    background: var(--border-primary);
    border-radius: var(--radius-sm);
}}

::-webkit-scrollbar-thumb:hover {{
    background: var(--text-muted);
}}

/* === HIDE STREAMLIT BRANDING === */
#MainMenu {{visibility: hidden;}}
footer {{visibility: hidden;}}
header[data-testid="stHeader"] {{visibility: hidden;}}

/* === SELECTBOX DROPDOWN === */
[data-baseweb="select"] > div {{
    background: var(--input-bg) !important;
    border-color: var(--input-border) !important;
}}

[data-baseweb="popover"] {{
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border-primary) !important;
}}

[data-baseweb="menu"] {{
    background: var(--bg-elevated) !important;
}}

[data-baseweb="menu"] li {{
    background: var(--bg-elevated) !important;
}}

[data-baseweb="menu"] li:hover {{
    background: var(--bg-panel) !important;
}}

/* === CHECKBOX & RADIO === */
.stCheckbox label span,
.stRadio label span {{
    color: var(--text-primary) !important;
}}

/* === DIVIDERS === */
hr {{
    border-color: var(--border-primary) !important;
}}

/* === FOCUS STATES (Accessibility) === */
*:focus-visible {{
    outline: 2px solid var(--border-focus) !important;
    outline-offset: 2px;
}}

/* === PLOTLY CHART CONTAINER === */
.stPlotlyChart {{
    background: var(--chart-bg) !important;
    border: 1px solid var(--border-primary);
    border-radius: var(--radius-sm);
}}

/* === MARKDOWN LINKS === */
a {{
    color: var(--color-accent) !important;
}}

a:hover {{
    color: var(--color-info) !important;
}}

/* === CODE BLOCKS === */
code {{
    background: var(--bg-elevated) !important;
    color: var(--text-primary) !important;
    padding: 2px 6px;
    border-radius: var(--radius-sm);
    font-size: 13px;
}}

pre {{
    background: var(--bg-panel) !important;
    border: 1px solid var(--border-primary) !important;
    border-radius: var(--radius-sm) !important;
}}

/* === DATAFRAME SPECIFIC (for Streamlit's st.dataframe) === */
[data-testid="stDataFrame"] {{
    border: 1px solid var(--border-primary);
    border-radius: var(--radius-sm);
    overflow: hidden;
}}

[data-testid="stDataFrame"] > div {{
    background: var(--bg-main) !important;
}}

/* === COLUMN LAYOUT === */
[data-testid="column"] {{
    padding: 0 8px;
}}

/* === STREAMLIT ELEMENTS OVERRIDE === */
.element-container {{
    margin-bottom: 8px;
}}

/* === PLOTLY MODEBAR === */
.modebar {{
    background: transparent !important;
}}

.modebar-btn {{
    color: var(--text-muted) !important;
}}

.modebar-btn:hover {{
    color: var(--text-primary) !important;
}}
</style>
"""


def get_chart_colors(theme_name: ThemeName = "bloomberg") -> dict:
    """Get chart color configuration for Plotly.

    Args:
        theme_name: Theme name ('default' or 'bloomberg')

    Returns:
        Dictionary with chart color configuration
    """
    theme = get_theme(theme_name)
    return {
        "bullish": theme.color_positive,
        "bearish": theme.color_negative,
        "bullish_light": theme.color_positive_light,
        "bearish_light": theme.color_negative_light,
        "neutral": theme.color_neutral,
        "sma_20": theme.chart_sma_20,
        "sma_50": theme.chart_sma_50,
        "volume": theme.chart_volume,
        "grid": theme.chart_grid,
        "axis": theme.chart_axis,
        "background": theme.chart_bg,
        "paper": theme.bg_main,
        "text": theme.text_primary,
        "text_secondary": theme.text_secondary,
        "accent": theme.color_accent,
        "info": theme.color_info,
        "info_light": theme.color_info_light,
        "warning": theme.color_warning,
        "warning_light": theme.color_warning_light,
    }


def get_plotly_layout(theme_name: ThemeName = "bloomberg") -> dict:
    """Get Plotly layout configuration for the theme.

    Args:
        theme_name: Theme name

    Returns:
        Dictionary with Plotly layout settings
    """
    theme = get_theme(theme_name)
    colors = get_chart_colors(theme_name)

    return {
        "paper_bgcolor": colors["paper"],
        "plot_bgcolor": colors["background"],
        "font": {
            "family": theme.font_mono,
            "color": colors["text"],
            "size": 12,
        },
        "title": {
            "font": {
                "family": theme.font_mono,
                "color": colors["text"],
                "size": 14,
            },
            "x": 0,
            "xanchor": "left",
        },
        "xaxis": {
            "gridcolor": colors["grid"],
            "linecolor": theme.border_primary,
            "tickfont": {"color": colors["text_secondary"], "size": 10},
            "title": {"font": {"color": colors["text_secondary"], "size": 11}},
            "zerolinecolor": colors["grid"],
            "showgrid": True,
            "gridwidth": 1,
        },
        "yaxis": {
            "gridcolor": colors["grid"],
            "linecolor": theme.border_primary,
            "tickfont": {"color": colors["text_secondary"], "size": 10},
            "title": {"font": {"color": colors["text_secondary"], "size": 11}},
            "zerolinecolor": colors["grid"],
            "showgrid": True,
            "gridwidth": 1,
            "side": "right",  # Bloomberg style: price axis on right
        },
        "legend": {
            "bgcolor": "rgba(0,0,0,0)",
            "font": {"color": colors["text_secondary"], "size": 11},
            "bordercolor": theme.border_primary,
            "borderwidth": 1,
        },
        "margin": {"l": 10, "r": 60, "t": 40, "b": 40},
        "hoverlabel": {
            "bgcolor": theme.bg_elevated,
            "bordercolor": theme.border_primary,
            "font": {"family": theme.font_mono, "color": colors["text"], "size": 12},
        },
    }
