# Claude Code Prompt: pakfindata Rename — EXECUTION (Phase 2)

## Prerequisites

- Phase 1 analysis prompt has been run (rename-psx-ohlcv-to-pakfindata.md)
- You have the full impact analysis output
- You know EXACTLY what files, imports, configs, and external projects are affected
- This prompt does the ACTUAL work — broken into safe, individually committed steps

---

## STEP 0 — PRE-FLIGHT: Commit and Push ALL Existing Work

**NOTHING starts until every last change is saved.**

```bash
cd ~/psx_ohlcv

echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 0: SAVE EVERYTHING BEFORE RENAME               ║"
echo "╚══════════════════════════════════════════════════════╝"

# 0A — Check current branch and status
echo "━━━ Current state ━━━"
git branch --show-current
git status
git stash list

# 0B — If there's a stash, pop it
git stash pop 2>/dev/null

# 0C — Stage EVERYTHING
git add -A

# 0D — Show what's being committed
git diff --cached --stat

# 0E — Commit all pending work
git commit -m "chore: save all pending work before pakfindata rename

All uncommitted changes preserved:
$(git diff --cached --stat 2>/dev/null | tail -1)

Next step: rename psx_ohlcv → pakfindata on a dedicated branch"

# 0F — Push to remote
git push origin $(git branch --show-current)

# 0G — Verify clean working tree
echo ""
echo "━━━ Post-commit state ━━━"
git status
echo ""
echo "Working tree must be CLEAN before proceeding."
echo "If 'git status' shows anything, fix it now."
```

**STOP if working tree is not clean. Fix any issues before proceeding.**

---

## STEP 1 — Create Rename Branch

```bash
cd ~/psx_ohlcv

# 1A — Create dedicated branch from current HEAD
git checkout -b refactor/rename-pakfindata

# 1B — Verify
git branch --show-current
# Expected: refactor/rename-pakfindata

# 1C — Push branch (so it exists on remote even if empty)
git push -u origin refactor/rename-pakfindata
```

**Commit: none (branch creation only)**

---

## STEP 2 — PATH ANALYSIS: What Changes vs What Stays

Before touching anything, document every path decision.

```bash
echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 2: PATH DECISION MATRIX                        ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "━━━ PATHS THAT CHANGE ━━━"
echo ""
echo "  ~/psx_ohlcv/                    → ~/pakfindata/"
echo "  ~/psx_ohlcv/src/psx_ohlcv/      → ~/pakfindata/src/pakfindata/"
echo "  Every 'from psx_ohlcv.' import  → 'from pakfindata.'"
echo "  Every 'import psx_ohlcv'        → 'import pakfindata'"
echo "  CLI: psxsync                    → pfsync"
echo "  pyproject.toml package name     → pakfindata"
echo "  .egg-info directory name        → pakfindata.egg-info"
echo ""

echo "━━━ PATHS THAT DO NOT CHANGE ━━━"
echo ""
echo "  /mnt/e/psxdata/psx.sqlite       — database location (data path, not package)"
echo "  /mnt/e/psxdata/                  — data directory"
echo "  Conda env 'psx'                 — just a label, independent of package name"
echo "  dps.psx.com.pk                  — external URL"
echo "  psx.com.pk                      — external URL"
echo "  PSXClient class name            — describes the exchange"
echo "  PSXScraper class name           — describes the exchange"
echo "  PSXAdapter class name           — describes the exchange"
echo "  psx_symbols variable            — 'psx' means the exchange here"
echo "  psx_data variable               — 'psx' means the exchange here"
echo "  Table names in SQLite           — no rename needed"
echo "  Git remote URL                  — rename repo separately if desired"
echo ""

echo "━━━ VERIFY: Show all hardcoded paths in the project ━━━"
echo ""

# Find every hardcoded path reference
echo "--- Database path references ---"
grep -rn "/mnt/e/psxdata\|psx\.sqlite\|PSX_DB_PATH\|DB_PATH\|db_path" \
  --include="*.py" --include="*.toml" --include="*.yaml" --include="*.sh" --include="*.env*" \
  ~/psx_ohlcv/ | grep -v __pycache__ | grep -v .git

echo ""
echo "--- Home directory path references ---"
grep -rn "~/psx_ohlcv\|/home/.*/psx_ohlcv\|\$HOME/psx_ohlcv" \
  --include="*.py" --include="*.toml" --include="*.yaml" --include="*.sh" --include="*.env*" --include="*.json" \
  ~/psx_ohlcv/ | grep -v __pycache__ | grep -v .git

echo ""
echo "--- Data directory references ---"
grep -rn "data/smtv\|data/cache\|data/raw\|data/pdf" \
  --include="*.py" --include="*.sh" \
  ~/psx_ohlcv/ | grep -v __pycache__ | grep -v .git

echo ""
echo "--- Config file path references ---"
grep -rn "config.*path\|CONFIG_DIR\|CACHE_DIR\|LOG_DIR\|DATA_DIR" \
  --include="*.py" --include="*.toml" \
  ~/psx_ohlcv/src/ | grep -v __pycache__

echo ""
echo "--- Streamlit command references ---"
grep -rn "streamlit run\|app\.py" \
  --include="*.sh" --include="*.md" --include="*.yaml" \
  ~/psx_ohlcv/ | grep -v __pycache__ | grep -v .git

echo ""
echo "--- MCP / API server path references ---"
grep -rn "mcp\|server.*path\|endpoint.*path" \
  --include="*.py" --include="*.json" --include="*.yaml" \
  ~/psx_ohlcv/ | grep -v __pycache__ | grep -v .git | grep -i "psx_ohlcv\|path" | head -20

echo ""
echo "━━━ DECISION: Which paths need updating? ━━━"
echo ""
echo "From the output above, identify:"
echo "1. Paths containing 'psx_ohlcv' as PROJECT directory → MUST change"
echo "2. Paths containing 'psxdata' as DATA directory → DO NOT change"
echo "3. Paths containing 'psx' as exchange abbreviation → DO NOT change"
```

**Review this output carefully. Any path with `psx_ohlcv` as the project directory changes.
Any path with `psxdata` as the data directory stays.**

---

## STEP 3 — Remove Old Package Installation

```bash
cd ~/psx_ohlcv

echo "━━━ STEP 3: Clean old installation ━━━"

# 3A — Uninstall the old package from pip
pip uninstall psx_ohlcv -y 2>/dev/null || echo "psx_ohlcv not installed via pip"
pip uninstall psx-ohlcv -y 2>/dev/null || echo "psx-ohlcv not installed via pip"

# 3B — Remove egg-info (stale metadata)
rm -rf src/psx_ohlcv.egg-info/ 2>/dev/null
rm -rf src/psx_ohlcv/*.egg-info/ 2>/dev/null
find . -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null
find . -name "*.egg" -type f -delete 2>/dev/null

# 3C — Clear any __pycache__ to avoid import confusion
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete 2>/dev/null

# 3D — Verify old package is gone
python -c "import psx_ohlcv" 2>&1 && echo "⚠️ psx_ohlcv still importable!" || echo "✅ psx_ohlcv uninstalled"

# 3E — Verify psxsync CLI removed
which psxsync 2>/dev/null && echo "⚠️ psxsync still in PATH: $(which psxsync)" || echo "✅ psxsync removed from PATH"
```

**Commit:**
```bash
git add -A
git commit -m "chore: remove old psx_ohlcv package installation and stale metadata

- pip uninstall psx_ohlcv
- Removed .egg-info directories
- Cleared __pycache__ to prevent stale imports"
```

---

## STEP 4 — Rename Project Root Directory

```bash
echo "━━━ STEP 4: Rename project root ━━━"

cd ~

# 4A — Rename the project directory
mv psx_ohlcv pakfindata

# 4B — Verify
ls -la ~/pakfindata/
ls -la ~/pakfindata/src/psx_ohlcv/  # still old name inside

# 4C — cd into new location
cd ~/pakfindata
pwd
# Expected: /home/<user>/pakfindata
```

**No commit yet — we're mid-rename.**

---

## STEP 5 — Rename Python Package Directory

```bash
cd ~/pakfindata

echo "━━━ STEP 5: Rename Python package directory ━━━"

# 5A — Rename src/psx_ohlcv → src/pakfindata
mv src/psx_ohlcv src/pakfindata

# 5B — Verify directory structure
ls -la src/pakfindata/
ls -la src/pakfindata/__init__.py

echo ""
echo "Package directory renamed. Git will track this as rename."
```

**Commit:**
```bash
git add -A
git commit -m "refactor: rename directories psx_ohlcv → pakfindata

- Project root: ~/psx_ohlcv → ~/pakfindata
- Python package: src/psx_ohlcv → src/pakfindata
- No code changes yet — just directory moves"
```

---

## STEP 6 — Fix All Python Imports

This is the biggest step. Do it file-by-file with verification.

```bash
cd ~/pakfindata

echo "━━━ STEP 6: Fix Python imports ━━━"

# 6A — Count what needs fixing
echo "Files to fix:"
grep -rln "psx_ohlcv" --include="*.py" src/ tests/ | grep -v __pycache__ | sort
echo ""
echo "Total files: $(grep -rln "psx_ohlcv" --include="*.py" src/ tests/ | grep -v __pycache__ | wc -l)"
echo "Total lines: $(grep -rn "psx_ohlcv" --include="*.py" src/ tests/ | grep -v __pycache__ | wc -l)"

# 6B — Do the replacement
# SAFE because 'psx_ohlcv' is a unique string — no false positives possible
find src/ tests/ -name "*.py" -not -path "*/__pycache__/*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 6C — Verify: zero remaining references
echo ""
echo "━━━ Verification ━━━"
remaining=$(grep -rn "psx_ohlcv" --include="*.py" src/ tests/ | grep -v __pycache__)
if [ -z "$remaining" ]; then
  echo "✅ All Python imports updated — zero psx_ohlcv references remain"
else
  echo "❌ REMAINING REFERENCES (fix manually):"
  echo "$remaining"
fi

# 6D — Quick syntax check: can we import the package?
python -c "import pakfindata; print('✅ pakfindata imports OK')" 2>&1

# 6E — If import fails, find the broken file
if ! python -c "import pakfindata" 2>/dev/null; then
  echo "❌ Import failed. Finding broken module..."
  python -c "
import importlib, pkgutil
import pakfindata
for importer, modname, ispkg in pkgutil.walk_packages(pakfindata.__path__, prefix='pakfindata.'):
    try:
        importlib.import_module(modname)
    except Exception as e:
        print(f'  BROKEN: {modname} → {e}')
        break
"
  echo ""
  echo "Fix the broken module above, then re-run Step 6C-6D"
fi
```

**Commit:**
```bash
git add -A
git commit -m "refactor: update all Python imports psx_ohlcv → pakfindata

- Updated $(grep -rn 'pakfindata' --include='*.py' src/ tests/ | grep -v __pycache__ | wc -l) import lines
- All 'from psx_ohlcv.' → 'from pakfindata.'
- All 'import psx_ohlcv' → 'import pakfindata'
- All internal references updated"
```

---

## STEP 7 — Fix pyproject.toml + CLI Entry Point

```bash
cd ~/pakfindata

echo "━━━ STEP 7: Fix pyproject.toml ━━━"

# 7A — Show current state
echo "--- Before ---"
cat pyproject.toml

# 7B — Replace package name
sed -i 's/psx_ohlcv/pakfindata/g' pyproject.toml
sed -i 's/psx-ohlcv/pakfindata/g' pyproject.toml

# 7C — Replace CLI entry point: psxsync → pfsync
sed -i 's/psxsync/pfsync/g' pyproject.toml

# 7D — Show result
echo ""
echo "--- After ---"
cat pyproject.toml

# 7E — Verify the critical sections
echo ""
echo "━━━ Verification ━━━"
echo "Package name:"
grep -E "^name\s*=" pyproject.toml
echo "CLI entry point:"
grep -E "pfsync|psxsync" pyproject.toml
echo "Package dir:"
grep -E "packages|find" pyproject.toml

# 7F — Check for setup.cfg too
if [ -f setup.cfg ]; then
  echo ""
  echo "--- setup.cfg found, updating ---"
  sed -i 's/psx_ohlcv/pakfindata/g' setup.cfg
  sed -i 's/psx-ohlcv/pakfindata/g' setup.cfg
  sed -i 's/psxsync/pfsync/g' setup.cfg
  cat setup.cfg
fi

# 7G — Check for setup.py too
if [ -f setup.py ]; then
  echo ""
  echo "--- setup.py found, updating ---"
  sed -i 's/psx_ohlcv/pakfindata/g' setup.py
  sed -i 's/psx-ohlcv/pakfindata/g' setup.py
  sed -i 's/psxsync/pfsync/g' setup.py
fi

# 7H — Check for MANIFEST.in
if [ -f MANIFEST.in ]; then
  echo ""
  echo "--- MANIFEST.in found, updating ---"
  sed -i 's/psx_ohlcv/pakfindata/g' MANIFEST.in
  cat MANIFEST.in
fi
```

**Commit:**
```bash
git add -A
git commit -m "refactor: update pyproject.toml — package name + CLI entry point

- Package: psx_ohlcv → pakfindata
- CLI: psxsync → pfsync
- Updated setup.cfg/setup.py/MANIFEST.in if present"
```

---

## STEP 8 — Fix Shell Scripts + Cron References

```bash
cd ~/pakfindata

echo "━━━ STEP 8: Fix shell scripts ━━━"

# 8A — Find all shell scripts with references
echo "Shell scripts to update:"
find . -name "*.sh" -o -name "*.bash" | while read f; do
  cnt=$(grep -c "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null)
  [ "$cnt" -gt 0 ] && echo "  $f ($cnt references)"
done

# 8B — Fix package name in scripts
find . -name "*.sh" -o -name "*.bash" | while read f; do
  if grep -q "psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing: $f"
    sed -i 's/psx_ohlcv/pakfindata/g' "$f"
  fi
done

# 8C — Fix CLI command in scripts
find . -name "*.sh" -o -name "*.bash" | while read f; do
  if grep -q "psxsync" "$f" 2>/dev/null; then
    echo "Fixing CLI in: $f"
    sed -i 's/psxsync/pfsync/g' "$f"
  fi
done

# 8D — Fix project directory path in scripts
find . -name "*.sh" -o -name "*.bash" | while read f; do
  if grep -q "~/psx_ohlcv\|/psx_ohlcv/" "$f" 2>/dev/null; then
    echo "Fixing path in: $f"
    sed -i 's|~/psx_ohlcv|~/pakfindata|g' "$f"
    sed -i 's|/psx_ohlcv/|/pakfindata/|g' "$f"
  fi
done

# 8E — Fix cron-related files inside project
find . -name "*cron*" -o -name "*schedule*" | while read f; do
  [ -f "$f" ] && sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f" && echo "Fixed: $f"
done

# 8F — Verify
echo ""
echo "━━━ Verification ━━━"
remaining=$(find . -name "*.sh" -o -name "*.bash" | xargs grep -l "psx_ohlcv\|psxsync\|~/psx_ohlcv" 2>/dev/null)
if [ -z "$remaining" ]; then
  echo "✅ All shell scripts updated"
else
  echo "❌ Still has references:"
  echo "$remaining"
fi

# 8G — Show crontab (can't auto-fix — user must do manually)
echo ""
echo "━━━ CRONTAB (manual fix needed) ━━━"
crontab -l 2>/dev/null | grep -n "psx_ohlcv\|psxsync" && \
  echo "⚠️ Run 'crontab -e' and replace:" && \
  echo "   ~/psx_ohlcv → ~/pakfindata" && \
  echo "   psxsync → pfsync" || \
  echo "✅ No crontab references to fix"
```

**Commit:**
```bash
git add -A
git commit -m "refactor: update shell scripts — paths, CLI references

- Script paths: ~/psx_ohlcv → ~/pakfindata
- CLI calls: psxsync → pfsync
- Package refs: psx_ohlcv → pakfindata
- NOTE: crontab requires manual update (crontab -e)"
```

---

## STEP 9 — Fix Config Files (YAML, JSON, TOML, INI, ENV)

```bash
cd ~/pakfindata

echo "━━━ STEP 9: Fix config files ━━━"

# 9A — YAML files
find . \( -name "*.yml" -o -name "*.yaml" \) -not -path "./.git/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing YAML: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 9B — JSON configs (not node_modules, not .git)
find . -name "*.json" -not -path "./.git/*" -not -path "*/node_modules/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing JSON: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 9C — INI / CFG files
find . \( -name "*.ini" -o -name "*.cfg" \) -not -path "./.git/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync" "$f" 2>/dev/null; then
    echo "Fixing INI/CFG: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 9D — ENV files
find . -name ".env*" -not -path "./.git/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing ENV: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 9E — VS Code settings
find .vscode/ -name "*.json" 2>/dev/null | while read f; do
  if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing VS Code: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g; s|/psx_ohlcv/|/pakfindata/|g' "$f"
  fi
done

# 9F — Docker files
for f in Dockerfile Dockerfile.* docker-compose.yml docker-compose*.yml .dockerignore; do
  if [ -f "$f" ] && grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing Docker: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g; s|/psx_ohlcv|/pakfindata|g' "$f"
  fi
done

# 9G — Verify
echo ""
echo "━━━ Verification ━━━"
remaining=$(find . \( -name "*.yml" -o -name "*.yaml" -o -name "*.json" -o -name "*.ini" -o -name "*.cfg" -o -name ".env*" \) -not -path "./.git/*" -not -path "*/node_modules/*" | xargs grep -l "psx_ohlcv\|psxsync" 2>/dev/null)
if [ -z "$remaining" ]; then
  echo "✅ All config files updated"
else
  echo "❌ Remaining:"
  for f in $remaining; do
    echo "  $f:"
    grep -n "psx_ohlcv\|psxsync" "$f"
  done
fi
```

**Commit:**
```bash
git add -A
git commit -m "refactor: update config files — YAML, JSON, VS Code, Docker, ENV

- All config references: psx_ohlcv → pakfindata
- VS Code workspace settings updated
- Docker configs updated if present
- Environment files updated"
```

---

## STEP 10 — Fix Documentation

```bash
cd ~/pakfindata

echo "━━━ STEP 10: Fix documentation ━━━"

# 10A — Markdown files
find . -name "*.md" -not -path "./.git/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 10B — RST files
find . -name "*.rst" -not -path "./.git/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync" "$f" 2>/dev/null; then
    echo "Fixing: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 10C — TXT files (README, CHANGELOG, etc.)
find . -name "*.txt" -not -path "./.git/*" -not -path "*/node_modules/*" | while read f; do
  if grep -q "psx_ohlcv\|psxsync" "$f" 2>/dev/null; then
    echo "Fixing: $f"
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done

# 10D — Verify
echo ""
remaining=$(find . \( -name "*.md" -o -name "*.rst" -o -name "*.txt" \) -not -path "./.git/*" | xargs grep -l "psx_ohlcv\|psxsync" 2>/dev/null)
if [ -z "$remaining" ]; then
  echo "✅ All documentation updated"
else
  echo "❌ Remaining: $remaining"
fi
```

**Commit:**
```bash
git add -A
git commit -m "refactor: update documentation — README, CHANGELOG, docs

- All doc references: psx_ohlcv → pakfindata, psxsync → pfsync
- Path references updated"
```

---

## STEP 11 — Fix __init__.py Metadata + Logger Names

```bash
cd ~/pakfindata

echo "━━━ STEP 11: Fix package metadata ━━━"

# 11A — Check __init__.py for hardcoded package name
echo "--- __init__.py content ---"
cat src/pakfindata/__init__.py

# 11B — Fix any hardcoded name/version strings
# (These should already be fixed by Step 6's sed, but verify)
grep -n "psx_ohlcv" src/pakfindata/__init__.py && \
  sed -i 's/psx_ohlcv/pakfindata/g' src/pakfindata/__init__.py || \
  echo "✅ __init__.py already clean"

# 11C — Check __main__.py
if [ -f src/pakfindata/__main__.py ]; then
  echo ""
  echo "--- __main__.py content ---"
  cat src/pakfindata/__main__.py
  grep -n "psx_ohlcv\|psxsync" src/pakfindata/__main__.py && \
    sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g' src/pakfindata/__main__.py || \
    echo "✅ __main__.py already clean"
fi

# 11D — Check logger names that were hardcoded (not __name__)
echo ""
echo "--- Hardcoded logger names ---"
grep -rn "getLogger.*['\"]psx" --include="*.py" src/pakfindata/ | grep -v __pycache__
# Fix any found
grep -rln "getLogger.*['\"]psx_ohlcv" --include="*.py" src/pakfindata/ | grep -v __pycache__ | while read f; do
  echo "Fixing logger in: $f"
  sed -i "s/getLogger('psx_ohlcv/getLogger('pakfindata/g" "$f"
  sed -i 's/getLogger("psx_ohlcv/getLogger("pakfindata/g' "$f"
done
echo "Note: files using getLogger(__name__) auto-fix — no action needed"

# 11E — Check for any __package__ or __app_name__ style constants
grep -rn "__package__\|APP_NAME\|PACKAGE_NAME\|PROJECT_NAME" --include="*.py" src/pakfindata/ | grep -v __pycache__ | head -10
```

**Commit:**
```bash
git add -A
git commit -m "refactor: fix package metadata, __init__.py, logger names

- __init__.py package name updated
- __main__.py entry point updated
- Hardcoded logger names: psx_ohlcv → pakfindata
- Note: getLogger(__name__) loggers auto-updated via package rename"
```

---

## STEP 12 — Reinstall Package + Verify Core

```bash
cd ~/pakfindata

echo "━━━ STEP 12: Reinstall and verify ━━━"

# 12A — Install in editable mode
pip install -e . 2>&1 | tail -10

# 12B — Verify package is installed
pip show pakfindata 2>&1 | head -8

# 12C — Verify old package is NOT installed
pip show psx_ohlcv 2>/dev/null && echo "❌ OLD PACKAGE STILL INSTALLED" || echo "✅ Old package gone"
pip show psx-ohlcv 2>/dev/null && echo "❌ OLD PACKAGE STILL INSTALLED (hyphen)" || echo "✅ Old package gone (hyphen)"

# 12D — Test core import
python -c "
import pakfindata
print(f'✅ Package: {pakfindata.__name__}')
print(f'   Location: {pakfindata.__file__}')
"

# 12E — Test CLI
which pfsync && echo "✅ pfsync found at: $(which pfsync)" || echo "❌ pfsync not in PATH"
pfsync --help 2>&1 | head -5

# 12F — Test old CLI is GONE
which psxsync 2>/dev/null && echo "⚠️ psxsync still exists — run: pip uninstall psx_ohlcv" || echo "✅ psxsync removed"

# 12G — If install failed, diagnose
if ! python -c "import pakfindata" 2>/dev/null; then
  echo ""
  echo "❌ INSTALL FAILED — Diagnosing..."
  echo ""
  echo "Check pyproject.toml:"
  grep -A5 "\[project\]" pyproject.toml
  echo ""
  echo "Check package structure:"
  ls -la src/pakfindata/__init__.py
  echo ""
  echo "Try direct install:"
  pip install -e . --verbose 2>&1 | grep -i "error\|warning" | head -20
fi
```

**Commit:**
```bash
git add -A
git commit -m "chore: reinstall pakfindata package — verify imports and CLI

- pip install -e . successful
- pakfindata imports OK
- pfsync CLI registered and working"
```

---

## STEP 13 — Deep Import Test (Every Submodule)

```bash
cd ~/pakfindata

echo "━━━ STEP 13: Deep import test — every module ━━━"

python -c "
import importlib, pkgutil, sys

pkg = importlib.import_module('pakfindata')
total = 0
errors = []

for importer, modname, ispkg in pkgutil.walk_packages(pkg.__path__, prefix='pakfindata.'):
    total += 1
    try:
        importlib.import_module(modname)
    except Exception as e:
        errors.append((modname, str(e)))

print(f'Tested {total} modules')
if errors:
    print(f'❌ {len(errors)} FAILURES:')
    for mod, err in errors:
        print(f'  {mod}: {err}')
    print()
    print('FIX EACH FAILURE before continuing.')
    print('Common causes:')
    print('  - Missed psx_ohlcv reference in that file')
    print('  - Circular import exposed by rename')
    print('  - Missing dependency')
else:
    print('✅ All modules import successfully')
"
```

**If failures found, fix each one:**

```bash
# For each broken module, check for remaining old references
for mod in <BROKEN_MODULE_LIST>; do
  filepath=$(python -c "import importlib; m='$mod'; parts=m.split('.'); print('/'.join(['src'] + parts) + '.py')")
  echo "=== $filepath ==="
  grep -n "psx_ohlcv" "$filepath" 2>/dev/null
  # Fix if found
  [ -f "$filepath" ] && sed -i 's/psx_ohlcv/pakfindata/g' "$filepath"
done
```

**Commit (only if fixes were needed):**
```bash
git add -A
git commit -m "fix: resolve import failures found during deep module test

- Fixed remaining psx_ohlcv references in: <list files>"
```

---

## STEP 14 — Database Connectivity Test

```bash
cd ~/pakfindata

echo "━━━ STEP 14: Database connectivity ━━━"

python -c "
from pakfindata.db import connect

# Test connection
con = connect('/mnt/e/psxdata/psx.sqlite')

# Count tables
tables = con.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()
print(f'✅ Connected to DB: {len(tables)} tables')

# Sample key tables
for tbl in ['mutual_funds', 'mutual_fund_nav', 'eod_ohlcv', 'kibor_daily', 'tbill_auctions', 'pib_auctions', 'pkrv_daily']:
    try:
        count = con.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
        print(f'  {tbl}: {count:,} rows')
    except Exception as e:
        print(f'  {tbl}: ⚠️ {e}')

con.close()
print()
print('✅ Database path unchanged — /mnt/e/psxdata/psx.sqlite works')
"
```

---

## STEP 15 — CLI Command Test

```bash
cd ~/pakfindata

echo "━━━ STEP 15: CLI commands ━━━"

# 15A — Help
pfsync --help 2>&1 | head -20
echo ""

# 15B — Status (non-destructive)
pfsync status 2>&1 | head -20
echo ""

# 15C — List available subcommands
echo "Available subcommands:"
pfsync --help 2>&1 | grep -E "^\s+\w" | head -20

# 15D — Test a read-only fund command
pfsync funds status 2>&1 | head -10 || echo "(funds status not available or different syntax)"
```

---

## STEP 16 — Run Full Test Suite

```bash
cd ~/pakfindata

echo "━━━ STEP 16: Full pytest suite ━━━"

# 16A — Check for remaining old references in tests
remaining=$(grep -rn "psx_ohlcv\|psxsync" tests/ --include="*.py" | grep -v __pycache__)
if [ -n "$remaining" ]; then
  echo "❌ Old references in tests — fixing..."
  find tests/ -name "*.py" -exec sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g' {} +
  echo "Fixed. Re-checking..."
  grep -rn "psx_ohlcv" tests/ --include="*.py" | grep -v __pycache__
fi

# 16B — Run tests
pytest tests/ -x -q --tb=short 2>&1

# 16C — If tests fail, show details
if [ $? -ne 0 ]; then
  echo ""
  echo "━━━ Detailed failure output ━━━"
  pytest tests/ -x --tb=long 2>&1 | tail -50
  echo ""
  echo "Fix each failing test before continuing."
  echo "Common causes after rename:"
  echo "  - Fixture paths referencing old package name"
  echo "  - conftest.py with old imports"
  echo "  - Test data files with hardcoded paths"
fi
```

**Commit (if test fixes were needed):**
```bash
git add -A
git commit -m "fix: update test suite for pakfindata rename

- Fixed remaining psx_ohlcv references in tests/
- All tests passing"
```

---

## STEP 17 — Streamlit UI Test

```bash
cd ~/pakfindata

echo "━━━ STEP 17: Streamlit UI ━━━"

# 17A — Verify app.py exists at new path
ls -la src/pakfindata/ui/app.py && echo "✅ app.py found" || echo "❌ app.py NOT found at new path"

# 17B — Check all page files import correctly
python -c "
import importlib, glob, os
errors = []
for f in sorted(glob.glob('src/pakfindata/ui/pages/*.py')):
    modname = f.replace('src/', '').replace('/', '.').replace('.py', '')
    try:
        importlib.import_module(modname)
    except Exception as e:
        errors.append(f'{os.path.basename(f)}: {e}')

if errors:
    print('❌ Page import failures:')
    for e in errors:
        print(f'  {e}')
else:
    print('✅ All Streamlit pages import successfully')
"

# 17C — Check for any psx_ohlcv in UI files
remaining=$(grep -rn "psx_ohlcv\|psxsync" src/pakfindata/ui/ --include="*.py" | grep -v __pycache__)
if [ -n "$remaining" ]; then
  echo "❌ Old references in UI:"
  echo "$remaining"
  echo "Fixing..."
  find src/pakfindata/ui/ -name "*.py" -exec sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g' {} +
fi

# 17D — Dry-run Streamlit (check it can start)
timeout 10 streamlit run src/pakfindata/ui/app.py --server.headless true 2>&1 | head -10
echo ""
echo "(Streamlit should show 'You can now view your Streamlit app' or similar)"
```

**Commit (if fixes needed):**
```bash
git add -A
git commit -m "fix: update Streamlit UI pages for pakfindata rename

- All page imports updated
- App launches successfully at new path"
```

---

## STEP 18 — Update External Projects

### 18A — qp-mono

```bash
echo "━━━ STEP 18A: Update qp-mono ━━━"

# Find qp-mono
QP_DIR=""
for candidate in ~/qp-mono ~/projects/qp-mono ~/repos/qp-mono; do
  [ -d "$candidate" ] && QP_DIR="$candidate" && break
done

if [ -n "$QP_DIR" ]; then
  echo "Found: $QP_DIR"
  
  # Show what needs changing
  echo "--- References found ---"
  grep -rn "psx_ohlcv\|psxsync\|~/psx_ohlcv" --include="*.py" --include="*.toml" --include="*.yaml" --include="*.sh" --include="*.json" "$QP_DIR" | grep -v __pycache__ | grep -v node_modules | grep -v .git
  
  # Count
  cnt=$(grep -rn "psx_ohlcv\|psxsync" --include="*.py" --include="*.toml" "$QP_DIR" | grep -v __pycache__ | grep -v .git | wc -l)
  echo ""
  echo "Total references: $cnt"
  
  if [ "$cnt" -gt 0 ]; then
    echo ""
    echo "Fixing..."
    
    cd "$QP_DIR"
    git stash 2>/dev/null
    
    # Fix Python files
    find . -name "*.py" -not -path "*/__pycache__/*" -not -path "./.git/*" | while read f; do
      if grep -q "psx_ohlcv" "$f" 2>/dev/null; then
        echo "  Fixing: $f"
        sed -i 's/psx_ohlcv/pakfindata/g' "$f"
      fi
      if grep -q "psxsync" "$f" 2>/dev/null; then
        sed -i 's/psxsync/pfsync/g' "$f"
      fi
    done
    
    # Fix configs
    find . \( -name "*.toml" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.cfg" \) -not -path "./.git/*" | while read f; do
      if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
        echo "  Fixing config: $f"
        sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
      fi
    done
    
    # Fix shell scripts
    find . -name "*.sh" -not -path "./.git/*" | while read f; do
      if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
        echo "  Fixing script: $f"
        sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g' "$f"
      fi
    done
    
    # Verify
    echo ""
    echo "--- After fix ---"
    remaining=$(grep -rn "psx_ohlcv\|psxsync" --include="*.py" --include="*.toml" . | grep -v __pycache__ | grep -v .git)
    if [ -z "$remaining" ]; then
      echo "✅ qp-mono fully updated"
    else
      echo "❌ Remaining:"
      echo "$remaining"
    fi
    
    # Test import
    echo ""
    python -c "
try:
    # Try the adapter import — the actual module path may vary
    import importlib
    # Common patterns for PSX adapter
    for mod in ['qsconnect.adapters.psx_adapter', 'adapters.psx_adapter', 'qpmono.adapters.psx_adapter']:
        try:
            m = importlib.import_module(mod)
            print(f'✅ {mod} imports OK')
            break
        except ImportError:
            continue
    else:
        print('⚠️ Could not find PSX adapter module — check path manually')
except Exception as e:
    print(f'❌ Import error: {e}')
"
    
    # Commit qp-mono changes
    git add -A
    git diff --cached --stat
    git commit -m "refactor: update psx_ohlcv → pakfindata references

Companion change for pakfindata rename.
- Import paths: psx_ohlcv → pakfindata
- CLI references: psxsync → pfsync
- Project paths: ~/psx_ohlcv → ~/pakfindata"
    git push origin $(git branch --show-current) 2>/dev/null
    
    cd ~/pakfindata
  else
    echo "✅ qp-mono has no psx_ohlcv references"
  fi
else
  echo "⚠️ qp-mono not found — check manually at these locations:"
  echo "  ~/qp-mono, ~/projects/qp-mono, ~/repos/qp-mono"
fi
```

### 18B — Any other external projects

```bash
echo ""
echo "━━━ STEP 18B: Scan ALL projects for references ━━━"

# Broad scan
for dir in ~/projects/*/  ~/repos/*/  ~/*/; do
  [ -d "$dir" ] || continue
  [[ "$dir" == *"pakfindata"* ]] && continue  # skip our own project
  [[ "$dir" == *".cache"* ]] && continue
  [[ "$dir" == *".local"* ]] && continue
  
  cnt=$(grep -rn "psx_ohlcv\|psxsync" --include="*.py" --include="*.toml" --include="*.yaml" --include="*.sh" "$dir" 2>/dev/null | grep -v __pycache__ | grep -v .git | grep -v node_modules | wc -l)
  [ "$cnt" -gt 0 ] && echo "⚠️ $dir: $cnt references" && grep -rn "psx_ohlcv\|psxsync" --include="*.py" --include="*.toml" "$dir" 2>/dev/null | grep -v __pycache__ | grep -v .git | head -5
done

echo ""
echo "(Fix any found references in external projects before finalizing)"
```

---

## STEP 19 — Fix Environment (Shell, MCP, Windows)

```bash
echo "━━━ STEP 19: Environment cleanup ━━━"

# 19A — Shell configs
for f in ~/.bashrc ~/.bash_profile ~/.zshrc ~/.profile ~/.bash_aliases ~/.zprofile; do
  if [ -f "$f" ] && grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
    echo "Fixing: $f"
    echo "  Before:"
    grep -n "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f"
    sed -i 's|~/psx_ohlcv|~/pakfindata|g; s/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g' "$f"
    echo "  After:"
    grep -n "pakfindata\|pfsync" "$f"
  fi
done

# 19B — MCP server configs
echo ""
echo "--- MCP configs ---"
for search_dir in ~/.config/claude ~/.claude /mnt/c/Users/*/AppData/Roaming/Claude; do
  find "$search_dir" -name "*.json" -maxdepth 3 2>/dev/null | while read f; do
    if grep -q "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null; then
      echo "Fixing MCP config: $f"
      echo "  Before:"
      grep -n "psx_ohlcv\|psxsync" "$f"
      sed -i 's/psx_ohlcv/pakfindata/g; s/psxsync/pfsync/g; s|~/psx_ohlcv|~/pakfindata|g; s|\\\\psx_ohlcv|\\\\pakfindata|g' "$f"
      echo "  After:"
      grep -n "pakfindata\|pfsync" "$f"
    fi
  done
done

# 19C — Reminder for manual fixes
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  MANUAL FIXES REQUIRED                                   ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║                                                          ║"
echo "║  1. CRONTAB:                                             ║"
echo "║     Run: crontab -e                                      ║"
echo "║     Replace: ~/psx_ohlcv → ~/pakfindata                 ║"
echo "║     Replace: psxsync → pfsync                           ║"
echo "║                                                          ║"
echo "║  2. VS CODE:                                             ║"
echo "║     Reopen workspace: ~/pakfindata/                      ║"
echo "║     Update any launch.json / tasks.json paths            ║"
echo "║                                                          ║"
echo "║  3. WINDOWS TASK SCHEDULER (if applicable):              ║"
echo "║     Check for any scheduled tasks referencing psx_ohlcv  ║"
echo "║                                                          ║"
echo "║  4. BROWSER BOOKMARKS:                                   ║"
echo "║     Update any Streamlit URLs if port/path changed       ║"
echo "║                                                          ║"
echo "║  5. CLAUDE CODE / AI PROMPTS:                            ║"
echo "║     Future prompts should reference pakfindata not       ║"
echo "║     psx_ohlcv — update any saved prompt templates        ║"
echo "║                                                          ║"
echo "║  6. GIT REMOTE (optional):                               ║"
echo "║     If you want to rename the GitHub/GitLab repo too:    ║"
echo "║     Do it on the web UI, then:                           ║"
echo "║     git remote set-url origin <new-url>                  ║"
echo "║                                                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
```

---

## STEP 20 — FINAL VERIFICATION SWEEP

```bash
cd ~/pakfindata

echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 20: FINAL VERIFICATION                         ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "━━━ 20A: Zero remaining references in pakfindata ━━━"
found=$(grep -rn "psx_ohlcv" . --include="*.py" --include="*.toml" --include="*.cfg" --include="*.yaml" --include="*.yml" --include="*.json" --include="*.sh" --include="*.md" --include="*.rst" --include="*.txt" --include="*.ini" --include=".env*" | grep -v __pycache__ | grep -v .egg | grep -v ".git/" | grep -v "CHANGELOG\|HISTORY\|migration")
if [ -z "$found" ]; then
  echo "✅ ZERO psx_ohlcv references — completely clean"
else
  echo "❌ FOUND $(echo "$found" | wc -l) remaining references:"
  echo "$found"
  echo ""
  echo "Fix these before finalizing."
fi

echo ""
echo "━━━ 20B: Zero remaining psxsync references ━━━"
found_cli=$(grep -rn "psxsync" . --include="*.py" --include="*.toml" --include="*.sh" --include="*.yaml" --include="*.md" | grep -v __pycache__ | grep -v .git | grep -v "CHANGELOG\|HISTORY")
if [ -z "$found_cli" ]; then
  echo "✅ ZERO psxsync references — completely clean"
else
  echo "❌ FOUND remaining psxsync:"
  echo "$found_cli"
fi

echo ""
echo "━━━ 20C: Package health ━━━"
pip show pakfindata | head -5
python -c "import pakfindata; print(f'✅ import pakfindata OK: {pakfindata.__file__}')"
pfsync --help 2>&1 | head -3 && echo "✅ pfsync CLI works"

echo ""
echo "━━━ 20D: Database health ━━━"
python -c "
from pakfindata.db import connect
con = connect('/mnt/e/psxdata/psx.sqlite')
t = con.execute(\"SELECT COUNT(*) FROM sqlite_master WHERE type='table'\").fetchone()[0]
print(f'✅ DB OK: {t} tables at /mnt/e/psxdata/psx.sqlite')
con.close()
"

echo ""
echo "━━━ 20E: Full test suite ━━━"
pytest tests/ -q --tb=line 2>&1 | tail -5

echo ""
echo "━━━ 20F: Streamlit check ━━━"
python -c "from pakfindata.ui import app; print('✅ Streamlit app module OK')" 2>&1

echo ""
echo "━━━ 20G: External projects ━━━"
for dir in ~/qp-mono ~/projects/qp-mono; do
  [ -d "$dir" ] && echo "qp-mono: $(grep -rn 'psx_ohlcv' --include='*.py' "$dir" 2>/dev/null | grep -v __pycache__ | wc -l) remaining psx_ohlcv refs (should be 0)"
done

echo ""
echo "━━━ 20H: Pip registry ━━━"
pip show psx_ohlcv 2>/dev/null && echo "❌ OLD PACKAGE still registered" || echo "✅ Old package fully removed"
pip show psx-ohlcv 2>/dev/null && echo "❌ OLD PACKAGE still registered (hyphen)" || echo "✅ Old package fully removed (hyphen)"

echo ""
echo "━━━ SUMMARY ━━━"
echo "Project root:  ~/pakfindata/"
echo "Package:       pakfindata"
echo "CLI:           pfsync"
echo "Database:      /mnt/e/psxdata/psx.sqlite (unchanged)"
echo "Conda env:     psx (unchanged)"
```

---

## STEP 21 — FINAL COMMIT + PUSH

```bash
cd ~/pakfindata

# Catch any last fixes
git add -A
git status

# If there are changes, commit
git diff --cached --quiet || git commit -m "fix: final cleanup from pakfindata rename verification

- Resolved any issues found during comprehensive testing"

# Tag for reference
git tag -a v3.1.0-pakfindata -m "Renamed psx_ohlcv → pakfindata

Package now reflects its scope as a comprehensive Pakistan financial data platform:
- Mutual funds (MUFAP): 1,190 funds, 1.9M NAV rows
- Treasury auctions: T-Bill, PIB, GIS (25 years)
- Yield curves: PKRV, PKISRV, PKFRV
- Interest rates: KIBOR, KONIA, SBP policy rate
- FX rates: interbank, open market, kerb
- Bond trading: SBP SMTV volumes
- Equities: EOD, intraday, company data
- ETFs, REITs, company fundamentals
- 10+ Streamlit dashboard pages

CLI renamed: psxsync → pfsync
Database path unchanged: /mnt/e/psxdata/psx.sqlite"

# Push everything
git push origin refactor/rename-pakfindata
git push origin v3.1.0-pakfindata

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✅ RENAME COMPLETE                                   ║"
echo "║                                                      ║"
echo "║  Next steps:                                         ║"
echo "║  1. Merge refactor/rename-pakfindata → dev → main   ║"
echo "║  2. Update crontab manually                          ║"
echo "║  3. Reopen VS Code at ~/pakfindata/                  ║"
echo "║  4. Update any Claude Code saved prompts             ║"
echo "║  5. Optionally rename GitHub repo                    ║"
echo "╚══════════════════════════════════════════════════════╝"
```

---

## CRITICAL RULES

1. **STEP 0 is NON-NEGOTIABLE** — commit and push ALL existing work before ANY rename operation.
2. **Work on branch** — `refactor/rename-pakfindata`, never directly on main/dev.
3. **Commit after EACH step** — if anything goes wrong, you can revert to any step.
4. **Fix-as-you-go** — if ANY step finds remaining references or broken imports, FIX THEM in that step before moving to the next. Don't accumulate debt.
5. **Test after EACH step** — run `python -c "import pakfindata"` after every change to catch breaks immediately.
6. **`psx_ohlcv` is unique** — no false positives from sed. But `psx` alone IS ambiguous (exchange name), so NEVER do `sed 's/psx/pakfin/g'` — that would destroy PSXClient, dps.psx.com.pk, psx.sqlite, etc.
7. **Database path stays** — `/mnt/e/psxdata/psx.sqlite` does NOT change. The `psxdata` directory name is fine.
8. **External projects get their own commits** — qp-mono changes are committed in qp-mono's repo, not pakfindata's.
9. **Manual fixes documented** — crontab, VS Code workspace, Windows configs can't be auto-fixed. The prompt lists them explicitly.
10. **Tag the release** — v3.1.0-pakfindata marks the rename in git history.
