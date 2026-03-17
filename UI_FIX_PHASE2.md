# Claude Code Prompt: UI Fix Phase 2 — Theme Standardization

## Context

UI audit found 4 pages using rogue color schemes that break the Bloomberg 
terminal aesthetic. 267 hardcoded hex colors across 30 files. 
The app already has a global theme in app.py:1149 — these pages ignore it.

## The Bloomberg Theme (already defined in app.py)

First, read `src/pakfindata/ui/app.py` and find the global CSS injection 
(around line 1149). Extract the theme tokens. They should look something like:

```
Background: #0f1117 / #1a1b2e type dark
Accent blue: #2F81F7
Green (up): #22c55e / #4ade80
Red (down): #FF5252 / #ef4444
Text primary: #FAFAFA / #E8EAED
Text secondary: #9AA0A6 / #BDC1C6
Border: #30363D / #2D333B
Card bg: #161B22 / #1C2128
```

Also read `src/pakfindata/ui/helpers.py` to find any shared theme constants.

## Pages to fix (4 rogue pages)

### Page 1: ai_insights.py — Purple gradient → Bloomberg blue

File: `src/pakfindata/ui/page_views/ai_insights.py`

PROBLEMS:
- Lines 34-94: Custom CSS with #667eea/#764ba2 purple gradient on buttons
- Lines 484-583: Custom table CSS with cyan accents
- Purple doesn't exist anywhere else in the app

FIX:
- Remove ALL `st.markdown(unsafe_allow_html=True)` CSS blocks that define colors
- Replace purple gradient buttons with standard Bloomberg blue (#2F81F7)
- Replace cyan table accents with the app's standard table styling
- Use the same card/metric patterns as dashboard.py or fund_explorer.py
- Keep functionality identical — only change visual styling

Specifically:
- `#667eea` → `#2F81F7` (accent blue)
- `#764ba2` → `#2F81F7` (no gradients — flat color)
- Remove `linear-gradient()` — use solid `background: #2F81F7`
- Cyan table headers → same muted header style as other pages

### Page 2: signal_dashboard.py — Gold accent → Bloomberg blue

File: `src/pakfindata/ui/page_views/signal_dashboard.py`

PROBLEMS:
- Lines 80-136: Custom CSS with #C8A96E gold accent, #12151A/#1A1D23 backgrounds
- Redefines `.metric-card` class (collides with debt_terminal.py)

FIX:
- Remove ALL custom CSS blocks
- `#C8A96E` gold → `#2F81F7` blue or `#22c55e` green (for positive signals)
- `#12151A/#1A1D23` → use app's standard card background
- Remove `.metric-card` custom class — use st.metric or the app's standard card pattern
- If custom HTML metric cards are needed, use the SAME class name and styling 
  as defined in the global theme (app.py), don't redefine them

### Page 3: debt_terminal.py — Custom dark → Bloomberg standard

File: `src/pakfindata/ui/page_views/debt_terminal.py`

PROBLEMS:
- Lines 36-119: Heavy custom CSS — #0a0e17/#0f1520 (darker than app bg), 
  #00d4aa green (different from app's #22c55e)
- Also redefines `.metric-card` (COLLISION with signal_dashboard.py)

FIX:
- Remove ALL custom CSS blocks
- `#0a0e17/#0f1520` → use app's standard background (don't go darker)
- `#00d4aa` → `#22c55e` (app's standard green)
- Remove `.metric-card` redefinition — use global definition
- Keep the Plotly charts but update their template colors to match

### Page 4: chat.py — Purple/orange → Bloomberg standard

File: `src/pakfindata/ui/page_views/chat.py`

PROBLEMS:
- #1a1a2e/#16213e backgrounds (different from app)
- #ff9800 orange accent (exists nowhere else)
- Purple gradient elements

FIX:
- Remove custom background colors → inherit from app theme
- `#ff9800` orange → `#2F81F7` blue (for interactive elements)
- Remove purple gradients → flat blue
- Chat messages can keep subtle bg differentiation (user vs assistant) 
  but use the app's color palette, not rogue colors

## Rules for ALL 4 pages

1. **NO new st.markdown(unsafe_allow_html=True) CSS injections** for colors.
   If the page needs custom CSS, it should ONLY be for layout (flexbox, grid), 
   NOT for colors. Colors come from the global theme.

2. **NO hardcoded hex colors** except:
   - Green up: #22c55e or #4ade80
   - Red down: #FF5252 or #ef4444
   - Blue accent: #2F81F7
   - These are the app's standard palette

3. **NO linear-gradient()** anywhere. Flat colors only. Bloomberg doesn't do gradients.

4. **Plotly chart themes** should match:
   ```python
   template="plotly_dark",
   paper_bgcolor="rgba(0,0,0,0)",
   plot_bgcolor="rgba(0,0,0,0)",
   font_color="#BDC1C6",
   ```

5. **Metric display** should use st.metric() or the globally-defined card pattern.
   Do NOT create new .metric-card classes.

## VERIFY

```bash
# Count hardcoded colors BEFORE fix
echo "BEFORE:"
grep -rn "#667eea\|#764ba2\|#C8A96E\|#00d4aa\|#ff9800\|#1a1a2e\|#16213e\|#12151A\|#1A1D23\|#0a0e17\|#0f1520" \
  src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

# After fix — should be 0
echo "AFTER:"
grep -rn "#667eea\|#764ba2\|#C8A96E\|#00d4aa\|#ff9800\|#1a1a2e\|#16213e\|#12151A\|#1A1D23\|#0a0e17\|#0f1520" \
  src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

# No linear-gradient in pages
grep -rn "linear-gradient" src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

# No .metric-card redefinitions (should only be in app.py or helpers.py)
grep -rn "\.metric-card" src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__

# Run app — check no crashes
cd ~/psx_ohlcv && streamlit run src/pakfindata/ui/app.py --server.headless true 2>&1 | head -5
```
