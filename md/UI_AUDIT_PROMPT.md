# Claude Code Prompt: Streamlit Frontend — UI Audit (READ ONLY)

## DO NOT CHANGE ANY CODE. THIS IS AN AUDIT ONLY.

Read everything, run the app if possible, report all findings. No edits.

## Context

The app is pakfindata (formerly psx_ohlcv) — a Streamlit-based Pakistan financial 
data platform with Bloomberg Terminal-style UI. Multiple pages were built at 
different times by different Claude Code sessions, causing inconsistencies.

Known issue: Streamlit "page bleeding" — where one page's CSS/state/components 
leak into another page when navigating via st.navigation() API.

## Step 1: Find all UI files

```bash
echo "=== App entry point ==="
find ~/psx_ohlcv -name "app.py" -path "*/ui/*" -not -path "*/.venv/*"

echo ""
echo "=== All page files ==="
find ~/psx_ohlcv -path "*/ui/page_views/*.py" -not -path "*/.venv/*" -not -path "*__pycache__*" | sort

echo ""
echo "=== Shared components/utils ==="
find ~/psx_ohlcv -path "*/ui/*.py" -not -path "*/page_views/*" -not -path "*/.venv/*" -not -path "*__pycache__*" | sort

echo ""
echo "=== Line counts per page ==="
find ~/psx_ohlcv -path "*/ui/page_views/*.py" -not -path "*/.venv/*" -not -path "*__pycache__*" -exec wc -l {} \; | sort -rn
```

## Step 2: Audit app.py — Navigation structure

Read the full app.py. Report:
- How is navigation implemented? (st.navigation, st.sidebar, manual routing?)
- What pages are registered and in what groups/order?
- Is there a shared layout (header, footer, sidebar)?
- Is there st.set_page_config() called? Where? (must be FIRST Streamlit command)
- Are there any global CSS injections (st.markdown with unsafe_allow_html)?
- Any session_state initialization that could leak between pages?

## Step 3: Check for page bleeding causes

For EACH page file, check these specific bleeding vectors:

### A. st.set_page_config() calls
```bash
# Must only be called ONCE, in app.py, not in individual pages
grep -rn "set_page_config" ~/psx_ohlcv/src/pakfindata/ui/ --include="*.py" | grep -v __pycache__
```
RULE: Only app.py should call st.set_page_config(). If ANY page calls it, 
that's a bleeding risk.

### B. Global CSS injection
```bash
# Find all st.markdown with HTML/CSS
grep -rn "st\.markdown.*unsafe_allow_html\|st\.markdown.*<style\|st\.html\|st\.components" \
  ~/psx_ohlcv/src/pakfindata/ui/ --include="*.py" | grep -v __pycache__
```
RULE: Custom CSS in one page affects ALL pages. Each injection should be 
scoped or conditional.

### C. Session state pollution
```bash
# Find all session_state writes
grep -rn "st\.session_state\[" ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | head -30

# Find session_state keys that might collide
grep -rn "session_state\[" ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | \
  grep -v __pycache__ | sed "s/.*session_state\[//" | sed "s/\].*//" | sort | uniq -c | sort -rn | head -20
```
RULE: Page-specific state keys should be prefixed with page name 
(e.g., "fund_explorer_selected" not "selected").

### D. Sidebar content
```bash
# Find all sidebar writes
grep -rn "st\.sidebar" ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | head -20
```
RULE: If pages write to sidebar independently, navigating between them 
leaves stale sidebar content.

### E. Callbacks and buttons
```bash
# Find all st.button, st.form, on_click, on_change
grep -rn "st\.button\|st\.form\|on_click\|on_change\|st\.rerun" \
  ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

echo "Pages with rerun calls:"
grep -rln "st\.rerun\|experimental_rerun" ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__
```

### F. Auto-refresh widgets
```bash
# Find streamlit-autorefresh or time.sleep patterns
grep -rn "autorefresh\|st_autorefresh\|time\.sleep" \
  ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__
```
RULE: Auto-refresh on one page continues running when you navigate away, 
causing background reruns.

## Step 4: Theme consistency audit

For EACH page, extract and compare:

### A. Color schemes used
```bash
# Find all color hex codes and rgba values
grep -rn "#[0-9a-fA-F]\{3,6\}\|rgba\|rgb(" \
  ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | head -40
```

### B. Common UI patterns
For each page, document:
- Header style (st.title vs st.header vs st.markdown with HTML)
- Table rendering (st.dataframe vs st.table vs custom HTML)
- Chart library (plotly, altair, matplotlib, st.line_chart)
- Card/metric display (st.metric vs custom HTML cards)
- Footer presence (yes/no, consistent?)
- Loading indicators (st.spinner vs none)
- Error handling (st.error vs st.warning vs try/except with nothing)

```bash
# Header patterns
echo "=== HEADER PATTERNS ==="
for f in $(find ~/psx_ohlcv/src/pakfindata/ui/page_views/ -name "*.py" -not -path "*__pycache__*"); do
    echo "--- $(basename $f) ---"
    head -50 "$f" | grep -n "st\.title\|st\.header\|st\.subheader\|st\.markdown.*#\|page_title\|set_page_config"
done

# Chart libraries
echo ""
echo "=== CHART LIBRARIES PER PAGE ==="
for f in $(find ~/psx_ohlcv/src/pakfindata/ui/page_views/ -name "*.py" -not -path "*__pycache__*"); do
    libs=""
    grep -q "plotly\|px\.\|go\." "$f" && libs="$libs plotly"
    grep -q "altair\|alt\." "$f" && libs="$libs altair"
    grep -q "matplotlib\|plt\." "$f" && libs="$libs matplotlib"
    grep -q "st\.line_chart\|st\.bar_chart\|st\.area_chart" "$f" && libs="$libs st_native"
    echo "  $(basename $f):$libs"
done

# Table rendering
echo ""
echo "=== TABLE RENDERING PER PAGE ==="
for f in $(find ~/psx_ohlcv/src/pakfindata/ui/page_views/ -name "*.py" -not -path "*__pycache__*"); do
    methods=""
    grep -q "st\.dataframe" "$f" && methods="$methods dataframe"
    grep -q "st\.table" "$f" && methods="$methods table"
    grep -q "<table\|<tr\|<td" "$f" && methods="$methods html_table"
    grep -q "AgGrid\|aggrid" "$f" && methods="$methods aggrid"
    echo "  $(basename $f):$methods"
done
```

### C. Metric/KPI display patterns
```bash
echo "=== METRIC DISPLAY PATTERNS ==="
for f in $(find ~/psx_ohlcv/src/pakfindata/ui/page_views/ -name "*.py" -not -path "*__pycache__*"); do
    methods=""
    grep -q "st\.metric" "$f" && methods="$methods st.metric"
    grep -q "st\.columns" "$f" && methods="$methods columns"
    grep -q "card\|Card\|<div.*card" "$f" && methods="$methods custom_card"
    echo "  $(basename $f):$methods"
done
```

## Step 5: Check for common Streamlit anti-patterns

### A. Expensive computations without caching
```bash
grep -rn "@st\.cache_data\|@st\.cache_resource\|st\.cache\|@cache" \
  ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | wc -l

echo "Pages WITHOUT any caching:"
for f in $(find ~/psx_ohlcv/src/pakfindata/ui/page_views/ -name "*.py" -not -path "*__pycache__*"); do
    if ! grep -q "cache_data\|cache_resource\|@cache" "$f"; then
        echo "  $(basename $f)"
    fi
done
```

### B. DB connections not properly managed
```bash
grep -rn "sqlite3\.connect\|get_connection\|engine\|Session" \
  ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | head -15
```

### C. Large data loads on every rerun
```bash
# Find SELECT * or large queries without LIMIT
grep -rn "SELECT \*\|fetchall\|read_sql\|pd\.read_sql" \
  ~/psx_ohlcv/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | head -15
```

## Step 6: Check shared components and theming

```bash
# Is there a shared theme/style module?
find ~/psx_ohlcv/src/pakfindata/ui/ -name "*theme*" -o -name "*style*" -o -name "*common*" -o -name "*shared*" -o -name "*layout*" | grep -v __pycache__

# Is there a shared header/footer?
grep -rn "render_footer\|render_header\|page_header\|common_layout" \
  ~/psx_ohlcv/src/pakfindata/ui/ --include="*.py" | grep -v __pycache__

# Streamlit config
cat ~/psx_ohlcv/.streamlit/config.toml 2>/dev/null || echo "No config.toml"
cat ~/psx_ohlcv/src/pakfindata/.streamlit/config.toml 2>/dev/null || echo "No config.toml in src"
```

---

## OUTPUT FORMAT

Produce a report with this EXACT structure:

```
═══════════════════════════════════════════════════════
  STREAMLIT FRONTEND — UI AUDIT REPORT
═══════════════════════════════════════════════════════

1. APP STRUCTURE
   Entry point: [path]
   Navigation method: [st.navigation / sidebar / manual]
   Total pages: [N]
   Page groups: [list]
   Total lines of UI code: [N]

2. PAGE BLEEDING ISSUES
   
   🔴 CRITICAL (causes visible bugs):
   - [issue + file + line number]
   
   🟡 WARNING (potential problems):
   - [issue + file + line number]
   
   🟢 OK:
   - [what's done correctly]

3. THEME CONSISTENCY MATRIX

   | Page | Header | Charts | Tables | Colors | Footer | Cache |
   |------|--------|--------|--------|--------|--------|-------|
   | fund_explorer | st.title | plotly | dataframe | #1e293b | yes | yes |
   | live_market | markdown | none | html | #0f172a | no | no |
   | ... | ... | ... | ... | ... | ... | ... |

   INCONSISTENCIES:
   - [specific mismatches]

4. CSS/STYLE INJECTIONS
   [list every st.markdown(unsafe_allow_html=True) with what it injects]
   [note which ones conflict with each other]

5. SESSION STATE AUDIT
   Total keys used: [N]
   Keys used by multiple pages (COLLISION RISK):
   - [key]: used in [page1, page2]
   Orphaned keys (set but never read):
   - [key]: set in [page] but never read

6. PERFORMANCE ISSUES
   Pages without caching: [list]
   Pages with uncached DB queries: [list]
   Pages loading large datasets on every rerun: [list]

7. ANTI-PATTERNS FOUND
   - [pattern + file + fix suggestion]

8. RECOMMENDED FIX PRIORITY

   Phase 1 (Quick wins — fix bleeding):
   - [specific fix + file + what to change]
   
   Phase 2 (Theme standardization):
   - [specific fix]
   
   Phase 3 (Performance):
   - [specific fix]

═══════════════════════════════════════════════════════
```

## IMPORTANT

- Report LINE NUMBERS for every issue found
- Show EXACT code snippets for bleeding/conflict issues
- Compare colors ACROSS pages — list every unique color used
- Be specific about which pages conflict with which
- Don't suggest vague "improvements" — list concrete file:line fixes
