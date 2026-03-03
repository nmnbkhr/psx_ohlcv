# PSX OHLCV Explorer - Theme System

## Overview

The PSX OHLCV Explorer supports multiple UI themes with a centralized token-based design system. The default theme is **Bloomberg Terminal** - a professional, high-contrast dark theme optimized for trading applications.

## Available Themes

| Theme | Description |
|-------|-------------|
| `bloomberg` | Bloomberg Terminal-inspired dark theme (default) |
| `default` | Original trading terminal theme |

## Theme Architecture

```
src/pakfindata/ui/
├── themes.py          # Theme tokens, CSS generation, chart colors
├── charts.py          # Plotly charts (uses theme colors)
└── app.py             # Main app (theme toggle, CSS injection)

.streamlit/
└── config.toml        # Streamlit base dark theme config
```

## Bloomberg Theme Colors

### Base Colors

| Token | Value | Usage |
|-------|-------|-------|
| `bg_main` | `#0B0E11` | Main background |
| `bg_panel` | `#12161C` | Panel/sidebar background |
| `bg_elevated` | `#161B22` | Elevated panels, dropdowns |
| `border_primary` | `#1E2329` | Borders, gridlines |

### Text Colors

| Token | Value | Usage |
|-------|-------|-------|
| `text_primary` | `#EAECEF` | Primary text |
| `text_secondary` | `#9AA4B2` | Labels, secondary text |
| `text_muted` | `#6B7280` | Disabled, hints |

### Semantic Colors

| Token | Value | Usage |
|-------|-------|-------|
| `color_positive` | `#00C853` | Gains, Buy, Up |
| `color_negative` | `#FF5252` | Losses, Sell, Down |
| `color_neutral` | `#6B7280` | Unchanged |
| `color_warning` | `#FFB300` | Alerts, caution |
| `color_info` | `#00B8D4` | Information |
| `color_accent` | `#2F81F7` | Focus, highlight, CTA |

### Chart Colors

| Token | Value | Usage |
|-------|-------|-------|
| `chart_sma_20` | `#FFB300` | 20-period SMA |
| `chart_sma_50` | `#00B8D4` | 50-period SMA |
| `chart_volume` | `#2F81F7` | Volume bars |
| `chart_grid` | `rgba(30, 35, 41, 0.8)` | Chart gridlines |

### Typography

```
Font Stack (Monospace):
ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
"Liberation Mono", "Courier New", monospace
```

### Border Radius

| Token | Value | Note |
|-------|-------|------|
| `radius_sm` | `2px` | Sharp/minimal |
| `radius_md` | `2px` | Consistent |
| `radius_lg` | `4px` | Subtle rounding |

## Using the Theme System

### Accessing Theme in Python

```python
from pakfindata.ui.themes import (
    get_theme,
    get_theme_css,
    get_chart_colors,
    get_plotly_layout,
    THEME_NAMES,
)

# Get theme tokens
theme = get_theme("bloomberg")
print(theme.color_positive)  # "#00C853"

# Get chart colors dict
colors = get_chart_colors("bloomberg")
print(colors["bullish"])  # "#00C853"

# Get complete CSS for injection
css = get_theme_css("bloomberg")
st.markdown(css, unsafe_allow_html=True)
```

### Accessing Theme in CSS

Theme tokens are available as CSS custom properties:

```css
/* Use theme colors */
.my-element {
    color: var(--color-positive);
    background: var(--bg-card);
    border: 1px solid var(--border-primary);
    font-family: var(--font-mono);
}

/* Semantic classes (pre-defined) */
.price-up { color: var(--color-positive); }
.price-down { color: var(--color-negative); }
.price-neutral { color: var(--color-neutral); }
```

### Available CSS Variables

```css
/* Backgrounds */
--bg-main, --bg-panel, --bg-elevated, --bg-card

/* Borders */
--border-primary, --border-subtle, --border-focus

/* Text */
--text-primary, --text-secondary, --text-muted

/* Semantic Colors */
--color-positive, --color-negative, --color-neutral
--color-warning, --color-info, --color-accent

/* Light Variants (for backgrounds) */
--color-positive-light, --color-negative-light
--color-warning-light, --color-info-light

/* Legacy Aliases */
--gain-color, --loss-color, --neutral-color
--accent-color, --warning-color, --border-color

/* Charts */
--chart-bg, --chart-grid, --chart-axis
--chart-sma-20, --chart-sma-50, --chart-volume

/* Inputs */
--input-bg, --input-border, --input-text
--button-primary-bg, --button-primary-text
--button-secondary-bg, --button-secondary-text

/* Tables */
--table-header-bg, --table-row-bg, --table-row-alt-bg
--table-row-hover-bg, --table-border

/* Typography */
--font-mono, --font-sans

/* Border Radius */
--radius-sm, --radius-md, --radius-lg
```

## Adding Themed Components

### Step 1: Use CSS Variables

When adding new styled elements, use CSS variables instead of hardcoded colors:

```python
# In app.py
st.markdown("""
<div style="
    background: var(--bg-card);
    border: 1px solid var(--border-primary);
    border-radius: var(--radius-sm);
    padding: 16px;
">
    <span style="color: var(--color-positive);">+5.2%</span>
</div>
""", unsafe_allow_html=True)
```

### Step 2: Add to Theme CSS (if reusable)

For reusable components, add styles to `get_theme_css()` in `themes.py`:

```python
# In themes.py, inside get_theme_css()
"""
/* === MY NEW COMPONENT === */
.my-component {{
    background: var(--bg-card);
    border: 1px solid var(--border-primary);
    border-radius: var(--radius-sm);
    padding: 16px;
}}
"""
```

### Step 3: Use Theme Colors in Charts

```python
from pakfindata.ui.themes import get_chart_colors

colors = get_chart_colors("bloomberg")

fig = go.Figure()
fig.add_trace(go.Bar(
    marker_color=colors["bullish"],  # Theme-aware color
))
fig.update_layout(
    paper_bgcolor=colors["paper"],
    plot_bgcolor=colors["background"],
    font=dict(color=colors["text"]),
)
```

## Adjusting Bloomberg Colors

### Quick Adjustments

To adjust colors, edit the `BLOOMBERG_THEME` constant in `themes.py`:

```python
# In themes.py
BLOOMBERG_THEME = ThemeTokens(
    name="bloomberg",
    display_name="Bloomberg Terminal",

    # Modify these values:
    color_positive="#00E676",   # Brighter green
    color_negative="#FF1744",   # Different red
    # ...
)
```

### Creating a Custom Theme

```python
# In themes.py

# 1. Create your theme tokens
MY_CUSTOM_THEME = ThemeTokens(
    name="custom",
    display_name="My Custom Theme",
    bg_main="#1a1a2e",
    bg_panel="#16213e",
    # ... all other tokens
)

# 2. Add to registry
THEMES["custom"] = MY_CUSTOM_THEME
THEME_NAMES.append("custom")

# 3. Update ThemeName type hint if using strict typing
ThemeName = Literal["default", "bloomberg", "custom"]
```

### Updating Streamlit Config

The `.streamlit/config.toml` sets the base Streamlit theme. Update if needed:

```toml
[theme]
base = "dark"
primaryColor = "#2F81F7"           # Bloomberg blue
backgroundColor = "#0B0E11"         # Match bg_main
secondaryBackgroundColor = "#12161C" # Match bg_panel
textColor = "#EAECEF"               # Match text_primary
font = "monospace"
```

## Design Principles

### Bloomberg Terminal Aesthetic

1. **No gradients** - Flat colors only
2. **Minimal shadows** - Borders define hierarchy
3. **Sharp borders** - 2px radius max
4. **Color = Information** - Not decoration
5. **Data-dense** - Tight spacing, small fonts
6. **Monospace typography** - For all numbers

### Accessibility

- Minimum 4.5:1 contrast ratio for text
- Visible focus states (`:focus-visible`)
- Color not sole indicator of meaning
- Keyboard navigable components

## Running the App

```bash
# Start the Streamlit app
streamlit run src/pakfindata/ui/app.py

# Or use the Makefile
make ui
```

The theme toggle is in the sidebar dropdown below the app title.

## Troubleshooting

### Theme not applying

1. Clear browser cache
2. Restart Streamlit server
3. Check `.streamlit/config.toml` exists
4. Verify `inject_theme_css()` is called in app.py

### Charts not using theme colors

Ensure charts call `get_chart_colors()`:

```python
from pakfindata.ui.themes import get_chart_colors
colors = get_chart_colors("bloomberg")
```

### CSS variables not working

CSS variables require the theme CSS to be injected first. Ensure `st.markdown(css, unsafe_allow_html=True)` runs before any styled content.
