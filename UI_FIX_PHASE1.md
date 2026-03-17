# Claude Code Prompt: UI Fix Phase 1 — Quick Bleeding Fixes

## Context

UI audit found minor bleeding issues and inconsistencies. These are quick fixes 
that won't break anything. DO NOT touch theme or performance — those are Phase 2 and 3.

## Fix 1: Remove sidebar writes from chat.py

File: `src/pakfindata/ui/page_views/chat.py` lines 279-296

The chat page writes "Clear Chat" and "Reset Agents" buttons to st.sidebar.
When user navigates away from chat, these buttons persist (stale sidebar content).

Move these buttons INTO the main page content area instead of sidebar.
Put them in a row at the top of the chat page:

```python
# Instead of st.sidebar.button("Clear Chat")
col1, col2, _ = st.columns([1, 1, 4])
with col1:
    if st.button("🗑 Clear Chat"):
        # existing clear logic
with col2:
    if st.button("🔄 Reset Agents"):
        # existing reset logic
```

## Fix 2: Fix red color inconsistency

File: `src/pakfindata/ui/app.py` line 274
File: `src/pakfindata/ui/helpers.py` line 130

Change `#FF1744` to `#FF5252` for consistency across the app.
Search the ENTIRE ui/ directory for `#FF1744` and replace all with `#FF5252`.

```bash
grep -rn "FF1744" src/pakfindata/ui/ --include="*.py"
```

Replace all occurrences.

## Fix 3: Fix get_connection() timeout inconsistency

File: `src/pakfindata/ui/app.py` line 483

Check the get_connection() function. If it has a different timeout value 
than other connection calls, standardize to 30 seconds:

```python
sqlite3.connect(str(db_path), timeout=30)
```

## Fix 4: Remove dead import in live_ohlcv.py

File: `src/pakfindata/ui/page_views/live_ohlcv.py` line 29

Remove the unused `render_footer` import (imported but never called).

## Fix 5: Fix legacy nav_to references

File: `src/pakfindata/ui/page_views/regular_market.py` lines 223, 247

Check these lines for legacy navigation names and update to current page names.

## VERIFY

```bash
# No sidebar writes outside app.py
grep -rn "st\.sidebar" src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__

# No FF1744 remaining
grep -rn "FF1744" src/pakfindata/ui/ --include="*.py" | grep -v __pycache__

# Dead imports
python3 -c "
import ast, sys
# Quick check for unused imports in live_ohlcv
with open('src/pakfindata/ui/page_views/live_ohlcv.py') as f:
    print('render_footer' in f.read())
"
```
