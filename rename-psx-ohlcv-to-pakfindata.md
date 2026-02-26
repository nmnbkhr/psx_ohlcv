# Claude Code Prompt: Rename psx_ohlcv → pakfindata (Full Refactor)

## Context

Renaming the entire `psx_ohlcv` project to `pakfindata`. The project has grown far beyond
PSX OHLCV data — it now covers mutual funds, bonds, yield curves, FX rates, treasury auctions,
ETFs, and more. The name no longer fits.

- **Current project root:** `~/psx_ohlcv/`
- **Current Python package:** `src/psx_ohlcv/`
- **Current CLI command:** `psxsync`
- **Conda environment:** `psx` (will NOT be renamed — just a label)
- **Database:** `/mnt/e/psxdata/psx.sqlite` (path will NOT change)
- **New project root:** `~/pakfindata/`
- **New Python package:** `src/pakfindata/`
- **New CLI command:** `pfsync`

### What DOES NOT change:
- Conda env name `psx`
- SQLite database path `/mnt/e/psxdata/psx.sqlite`
- SQLite table names inside the database
- Class names that describe data sources: `PSXClient`, `PSXScraper`, `SBPTreasuryScraper`, `MUFAPScraper`, etc.
- Variable names like `psx_symbols`, `psx_data` where "psx" means the exchange, not the package
- Git remote URL (stays the same repo)
- Any string "PSX" that refers to Pakistan Stock Exchange the entity (not our package)

### What DOES change:
- Every `import psx_ohlcv` and `from psx_ohlcv.xxx import yyy`
- Every `psx_ohlcv` in pyproject.toml, setup.cfg, MANIFEST.in, etc.
- Directory name `src/psx_ohlcv/` → `src/pakfindata/`
- Project root `~/psx_ohlcv/` → `~/pakfindata/`
- CLI entry point `psxsync` → `pfsync`
- References in shell scripts, cron jobs, docker configs, .vscode configs
- References in external projects that import from psx_ohlcv

---

## PHASE 1 — COMPREHENSIVE IMPACT ANALYSIS (DO NOT CHANGE ANYTHING YET)

### Step 1 — Map EVERY reference to "psx_ohlcv" in the project

```bash
echo "╔══════════════════════════════════════════════════════╗"
echo "║  PHASE 1: FULL IMPACT ANALYSIS — psx_ohlcv rename   ║"
echo "╚══════════════════════════════════════════════════════╝"

cd ~/psx_ohlcv

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1A. Python imports (the bulk of changes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "--- import psx_ohlcv ---"
grep -rn "import psx_ohlcv" --include="*.py" . | grep -v __pycache__ | grep -v .egg
echo ""
echo "--- from psx_ohlcv ---"
grep -rn "from psx_ohlcv" --include="*.py" . | grep -v __pycache__ | grep -v .egg
echo ""
echo "--- Total import lines ---"
grep -rn "psx_ohlcv" --include="*.py" . | grep -v __pycache__ | grep -v .egg | wc -l

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1B. String references (logging, comments, docstrings, error messages)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -rn "psx_ohlcv" --include="*.py" . | grep -v __pycache__ | grep -v .egg | grep -v "^.*import " | grep -v "^.*from " | head -50
echo "..."
grep -rn "psx_ohlcv" --include="*.py" . | grep -v __pycache__ | grep -v .egg | grep -v "^.*import " | grep -v "^.*from " | wc -l
echo "(string references excluding imports)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1C. Config files"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for f in pyproject.toml setup.cfg setup.py MANIFEST.in tox.ini .flake8 .pylintrc .pre-commit-config.yaml Makefile Dockerfile docker-compose.yml docker-compose*.yml; do
  [ -f "$f" ] && echo "--- $f ---" && grep -n "psx_ohlcv\|psxsync" "$f" 2>/dev/null
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1D. Shell scripts"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find . -name "*.sh" -o -name "*.bash" | while read f; do
  matches=$(grep -c "psx_ohlcv\|psxsync" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references" && grep -n "psx_ohlcv\|psxsync" "$f"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1E. VS Code / IDE configs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find .vscode/ -name "*.json" 2>/dev/null | while read f; do
  matches=$(grep -c "psx_ohlcv\|psxsync" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references" && grep -n "psx_ohlcv\|psxsync" "$f"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1F. Documentation / Markdown files"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find . -name "*.md" -o -name "*.rst" -o -name "*.txt" | while read f; do
  matches=$(grep -c "psx_ohlcv\|psxsync" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1G. YAML / TOML / INI / JSON configs (non-IDE)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find . \( -name "*.yml" -o -name "*.yaml" -o -name "*.toml" -o -name "*.ini" -o -name "*.cfg" -o -name "*.json" \) ! -path "./.vscode/*" ! -path "./.git/*" ! -path "*/node_modules/*" | while read f; do
  matches=$(grep -c "psx_ohlcv\|psxsync" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references" && grep -n "psx_ohlcv\|psxsync" "$f"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1H. Docker / containerization"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find . -name "Dockerfile*" -o -name "docker-compose*" -o -name ".dockerignore" | while read f; do
  [ -f "$f" ] && echo "--- $f ---" && grep -n "psx_ohlcv\|psxsync" "$f" 2>/dev/null
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1I. Cron / systemd / task scheduler"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
crontab -l 2>/dev/null | grep -n "psx_ohlcv\|psxsync" || echo "(no cron references found)"
find /etc/systemd/ -name "*psx*" 2>/dev/null || echo "(no systemd units found)"
find . -name "cron*" -o -name "*crontab*" -o -name "*scheduler*" | while read f; do
  [ -f "$f" ] && grep -n "psx_ohlcv\|psxsync" "$f" 2>/dev/null
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1J. Git hooks"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find .git/hooks/ -type f 2>/dev/null | while read f; do
  matches=$(grep -c "psx_ohlcv\|psxsync" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1K. Package metadata and egg-info"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find . -name "*.egg-info" -type d 2>/dev/null
pip show psx_ohlcv 2>/dev/null | head -10
pip show psx-ohlcv 2>/dev/null | head -10

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1L. __init__.py package name / version / metadata"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cat src/psx_ohlcv/__init__.py 2>/dev/null
grep -rn "__package__\|__name__.*psx" src/psx_ohlcv/ --include="*.py" | head -20

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1M. Logging / logger names"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -rn "getLogger.*psx_ohlcv\|logging.*psx_ohlcv\|logger.*psx_ohlcv" --include="*.py" . | grep -v __pycache__
grep -rn 'getLogger(__name__)' --include="*.py" src/psx_ohlcv/ | wc -l
echo "files using getLogger(__name__) — these auto-fix when package renames"
```

### Step 2 — Map EXTERNAL project references

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  EXTERNAL PROJECTS — Cross-repo references           ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2A. qp-mono (quantitative trading platform)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
QP_DIR=$(find ~/projects ~/qp-mono ~ -maxdepth 2 -name "qp-mono" -type d 2>/dev/null | head -1)
if [ -n "$QP_DIR" ]; then
  echo "Found qp-mono at: $QP_DIR"
  echo "--- Python imports ---"
  grep -rn "psx_ohlcv\|from psx_ohlcv\|import psx_ohlcv" --include="*.py" "$QP_DIR" | grep -v __pycache__
  echo "--- Config files ---"
  grep -rn "psx_ohlcv\|psxsync" --include="*.toml" --include="*.cfg" --include="*.yaml" --include="*.yml" --include="*.json" "$QP_DIR" | grep -v __pycache__ | grep -v node_modules
  echo "--- Shell scripts ---"
  grep -rn "psx_ohlcv\|psxsync" --include="*.sh" "$QP_DIR"
  echo "--- Requirements/deps ---"
  find "$QP_DIR" -name "requirements*.txt" -o -name "pyproject.toml" | xargs grep "psx_ohlcv" 2>/dev/null
else
  echo "qp-mono not found — check manually"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2B. psx-live (React trading terminal)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
PSX_LIVE=$(find ~/projects ~ -maxdepth 2 -name "psx-live" -type d 2>/dev/null | head -1)
if [ -n "$PSX_LIVE" ]; then
  echo "Found psx-live at: $PSX_LIVE"
  grep -rn "psx_ohlcv\|psxsync\|pakfindata" --include="*.js" --include="*.ts" --include="*.jsx" --include="*.tsx" --include="*.json" --include="*.env*" "$PSX_LIVE" | grep -v node_modules | grep -v __pycache__
else
  echo "psx-live not found — check manually"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2C. WatchGuard PK (AML platform)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
WG_DIR=$(find ~/projects ~ -maxdepth 2 -name "watchguard*" -type d 2>/dev/null | head -1)
if [ -n "$WG_DIR" ]; then
  echo "Found WatchGuard at: $WG_DIR"
  grep -rn "psx_ohlcv\|psxsync" --include="*.py" --include="*.toml" --include="*.yaml" "$WG_DIR" | grep -v __pycache__
else
  echo "WatchGuard not found or no references expected"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2D. STEM Buddy AI"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SB_DIR=$(find ~/projects ~ -maxdepth 2 -name "stem*buddy*" -type d 2>/dev/null | head -1)
if [ -n "$SB_DIR" ]; then
  echo "Found STEM Buddy at: $SB_DIR"
  grep -rn "psx_ohlcv" --include="*.py" "$SB_DIR" | grep -v __pycache__
else
  echo "STEM Buddy not found or no references expected"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2E. Broad scan — any project referencing psx_ohlcv"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Scanning ~/projects/ for any psx_ohlcv references..."
grep -rn "psx_ohlcv\|psxsync" ~/projects/ --include="*.py" --include="*.toml" --include="*.yaml" --include="*.yml" --include="*.json" --include="*.sh" --include="*.md" 2>/dev/null | grep -v __pycache__ | grep -v node_modules | grep -v ".git/" | head -50
echo ""
echo "Scanning ~/ (depth 1) for standalone scripts..."
grep -l "psx_ohlcv\|psxsync" ~/*.py ~/*.sh 2>/dev/null

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2F. Pip / conda package registry"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
pip list 2>/dev/null | grep -i "psx"
conda list 2>/dev/null | grep -i "psx"
echo "--- Editable installs ---"
pip show psx_ohlcv 2>/dev/null || pip show psx-ohlcv 2>/dev/null || echo "Not installed as pip package"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2G. MCP server configs (Claude Desktop, etc.)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find ~/.config/claude/ ~/AppData/ ~/.claude/ /mnt/c/Users/ -name "*.json" -maxdepth 4 2>/dev/null | while read f; do
  matches=$(grep -c "psx_ohlcv\|psxsync" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references" && grep -n "psx_ohlcv\|psxsync" "$f"
done
echo "(Also check Windows-side Claude Desktop config if using MCP server)"
```

### Step 3 — Map environment / PATH / alias references

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ENVIRONMENT — Shell, PATH, aliases                  ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3A. Shell config files"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for f in ~/.bashrc ~/.bash_profile ~/.zshrc ~/.profile ~/.bash_aliases ~/.zprofile; do
  [ -f "$f" ] && matches=$(grep -c "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f" 2>/dev/null) && [ "$matches" -gt 0 ] && echo "--- $f ---" && grep -n "psx_ohlcv\|psxsync\|~/psx_ohlcv" "$f"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3B. PATH entries"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo $PATH | tr ':' '\n' | grep "psx_ohlcv"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3C. Current aliases"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
alias 2>/dev/null | grep "psx_ohlcv\|psxsync"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3D. Entry points installed in conda env"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
which psxsync 2>/dev/null && echo "psxsync is installed at: $(which psxsync)"
ls -la $(dirname $(which python 2>/dev/null))/*psx* 2>/dev/null
ls -la $(dirname $(which python 2>/dev/null))/*psxsync* 2>/dev/null

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3E. Windows-side references (WSL2)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
# Check if psx_ohlcv is referenced in Windows paths
find /mnt/c/Users/ -maxdepth 3 -name "*.json" -path "*/claude*" 2>/dev/null | while read f; do
  matches=$(grep -c "psx_ohlcv" "$f" 2>/dev/null)
  [ "$matches" -gt 0 ] && echo "$f: $matches references"
done
# Check Windows Task Scheduler exports if any
find /mnt/c/Users/ -maxdepth 4 -name "*.xml" 2>/dev/null | xargs grep -l "psx_ohlcv" 2>/dev/null | head -5
```

### Step 4 — Analyze tricky rename edge cases

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  EDGE CASES — Things that look like psx_ohlcv but    ║"
echo "║  should NOT be renamed                                ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4A. 'PSX' as exchange name (should NOT change)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "These class/variable names stay as-is:"
grep -rn "class PSX\|PSXClient\|PSXScraper\|PSXAdapter\|psx_symbols\|psx_data\|psx\.com\|dps\.psx\|psx\.sqlite" --include="*.py" ~/psx_ohlcv/src/ | grep -v __pycache__ | head -30

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4B. Database path '/mnt/e/psxdata/' (should NOT change)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -rn "psxdata\|psx\.sqlite\|psx_data_dir\|PSX_DB" --include="*.py" --include="*.toml" --include="*.yaml" --include="*.sh" ~/psx_ohlcv/ | grep -v __pycache__ | head -20

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4C. URL strings containing 'psx' (should NOT change)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -rn "psx.com\|dps.psx\|psxterminal" --include="*.py" ~/psx_ohlcv/src/ | grep -v __pycache__ | head -20

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4D. Variable names with 'psx' meaning the exchange"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -rn "psx_" --include="*.py" ~/psx_ohlcv/src/ | grep -v __pycache__ | grep -v "psx_ohlcv" | head -30
echo "(These are 'psx' as exchange prefix, NOT package name — don't rename)"
```

### Step 5 — Generate summary report

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  SUMMARY REPORT                                      ║"
echo "╚══════════════════════════════════════════════════════╝"

cd ~/psx_ohlcv

echo ""
echo "FILES with 'psx_ohlcv' references (total unique files):"
grep -rln "psx_ohlcv" . --include="*.py" --include="*.toml" --include="*.cfg" --include="*.yaml" --include="*.yml" --include="*.json" --include="*.sh" --include="*.md" | grep -v __pycache__ | grep -v .egg | grep -v .git | sort | wc -l

echo ""
echo "TOTAL LINES to change:"
grep -rn "psx_ohlcv" . --include="*.py" --include="*.toml" --include="*.cfg" --include="*.yaml" --include="*.yml" --include="*.json" --include="*.sh" --include="*.md" | grep -v __pycache__ | grep -v .egg | grep -v .git | wc -l

echo ""
echo "FILES with 'psxsync' CLI references:"
grep -rln "psxsync" . --include="*.py" --include="*.toml" --include="*.sh" --include="*.md" | grep -v __pycache__ | grep -v .git | sort

echo ""
echo "EXTERNAL PROJECTS affected:"
echo "(list from Step 2 above)"

echo ""
echo "RENAME SAFETY CHECK — lines where 'psx_ohlcv' appears next to 'PSX' exchange context:"
grep -rn "psx_ohlcv" --include="*.py" ~/psx_ohlcv/src/ | grep -v __pycache__ | grep -i "Pakistan Stock\|dps.psx\|PSXClient" | head -10
echo "(verify these are pure import lines, not exchange-name references)"
```

**════════════════════════════════════════════════════════════════**
**STOP HERE — Show me the COMPLETE output of Phase 1 (Steps 1-5)**
**before proceeding to Phase 2.**
**════════════════════════════════════════════════════════════════**

---

## PHASE 2 — EXECUTE THE RENAME

Only proceed after Phase 1 output is reviewed.

### Step 6 — Git safety: create rename branch

```bash
cd ~/psx_ohlcv
git stash  # save any uncommitted work
git checkout -b refactor/rename-pakfindata
```

### Step 7 — Uninstall old package

```bash
pip uninstall psx_ohlcv -y 2>/dev/null
pip uninstall psx-ohlcv -y 2>/dev/null
rm -rf src/psx_ohlcv.egg-info/ 2>/dev/null
rm -rf src/psx_ohlcv/*.egg-info 2>/dev/null
```

### Step 8 — Rename directory structure

```bash
# Rename project root
cd ~
mv psx_ohlcv pakfindata
cd ~/pakfindata

# Rename Python package directory
mv src/psx_ohlcv src/pakfindata
```

### Step 9 — Global find-replace (SAFE — only exact matches)

```bash
cd ~/pakfindata

# 9A — Python files: replace all psx_ohlcv imports and references
find . -name "*.py" -not -path "./.git/*" -not -path "*/__pycache__/*" -not -path "*/.egg*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 9B — Config files
find . \( -name "*.toml" -o -name "*.cfg" -o -name "*.ini" \) -not -path "./.git/*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 9C — Shell scripts
find . -name "*.sh" -not -path "./.git/*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 9D — YAML configs
find . \( -name "*.yml" -o -name "*.yaml" \) -not -path "./.git/*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 9E — Documentation
find . \( -name "*.md" -o -name "*.rst" -o -name "*.txt" \) -not -path "./.git/*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 9F — JSON configs (careful — not package-lock.json or node_modules)
find . -name "*.json" -not -path "./.git/*" -not -path "*/node_modules/*" \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +

# 9G — Docker files
find . \( -name "Dockerfile*" -o -name "docker-compose*" -o -name ".dockerignore" \) \
  -exec sed -i 's/psx_ohlcv/pakfindata/g' {} +
```

### Step 10 — Rename CLI entry point: psxsync → pfsync

```bash
# In pyproject.toml, find the [project.scripts] or [tool.setuptools.scripts] section
# Replace: psxsync = "psx_ohlcv.cli:main"  →  pfsync = "pakfindata.cli:main"
# Also keep backward compat alias if desired:
#   psxsync = "pakfindata.cli:main"    # deprecated alias

# Find the exact line
grep -n "psxsync" pyproject.toml

# Do the replacement
sed -i 's/psxsync/pfsync/g' pyproject.toml

# Also replace in any shell scripts that call psxsync
find . -name "*.sh" -exec sed -i 's/psxsync/pfsync/g' {} +

# And in cron-related files
find . -name "*cron*" -exec sed -i 's/psxsync/pfsync/g' {} +

# Check __main__.py if it references psxsync
grep -n "psxsync" src/pakfindata/__main__.py 2>/dev/null && \
  sed -i 's/psxsync/pfsync/g' src/pakfindata/__main__.py
```

### Step 11 — Rename directory path references

```bash
# Any hardcoded paths like ~/psx_ohlcv/ in scripts
find . -name "*.sh" -name "*.py" -name "*.yaml" -name "*.toml" -exec grep -l "~/psx_ohlcv\|/home/.*/psx_ohlcv" {} + 2>/dev/null | while read f; do
  echo "Fixing path in: $f"
  sed -i 's|~/psx_ohlcv|~/pakfindata|g' "$f"
  sed -i 's|/psx_ohlcv/|/pakfindata/|g' "$f"
done
```

### Step 12 — Reinstall package

```bash
cd ~/pakfindata
pip install -e . 2>&1 | tail -5

# Verify import works
python -c "import pakfindata; print(f'Package: {pakfindata.__name__}')"
python -c "from pakfindata.db import connect; print('DB module OK')"
python -c "from pakfindata.sources import mufap; print('MUFAP module OK')"

# Verify CLI works
pfsync --help 2>&1 | head -5
```

### Step 13 — Verify NO remaining psx_ohlcv references

```bash
echo "════════════════════════════════════════════════════════"
echo "VERIFICATION: Scanning for remaining psx_ohlcv references"
echo "════════════════════════════════════════════════════════"

cd ~/pakfindata

# Should return ZERO results (excluding git history and false positives)
remaining=$(grep -rn "psx_ohlcv" . --include="*.py" --include="*.toml" --include="*.cfg" --include="*.yaml" --include="*.yml" --include="*.json" --include="*.sh" --include="*.md" | grep -v __pycache__ | grep -v .egg | grep -v ".git/" | grep -v "CHANGELOG\|HISTORY\|migration_notes")
if [ -z "$remaining" ]; then
  echo "✅ CLEAN — no psx_ohlcv references remain"
else
  echo "❌ REMAINING REFERENCES:"
  echo "$remaining"
  echo ""
  echo "Fix these manually — they may be in comments/docs that need updating"
fi

# Check for broken psxsync references
remaining_cli=$(grep -rn "psxsync" . --include="*.py" --include="*.toml" --include="*.sh" | grep -v __pycache__ | grep -v .git)
if [ -z "$remaining_cli" ]; then
  echo "✅ CLEAN — no psxsync references remain"
else
  echo "❌ REMAINING psxsync:"
  echo "$remaining_cli"
fi
```

---

## PHASE 3 — UPDATE EXTERNAL PROJECTS

### Step 14 — Update qp-mono

```bash
# Based on Phase 1 Step 2A findings, update qp-mono references
QP_DIR=$(find ~/projects ~/qp-mono ~ -maxdepth 2 -name "qp-mono" -type d 2>/dev/null | head -1)
if [ -n "$QP_DIR" ]; then
  echo "Updating qp-mono at: $QP_DIR"
  
  # Show what needs changing
  echo "--- Before ---"
  grep -rn "psx_ohlcv" --include="*.py" --include="*.toml" --include="*.yaml" "$QP_DIR" | grep -v __pycache__
  
  # Replace imports
  find "$QP_DIR" -name "*.py" -not -path "*/__pycache__/*" -exec grep -l "psx_ohlcv" {} + 2>/dev/null | while read f; do
    echo "Updating: $f"
    sed -i 's/psx_ohlcv/pakfindata/g' "$f"
  done
  
  # Replace in configs
  find "$QP_DIR" \( -name "*.toml" -o -name "*.yaml" -o -name "*.yml" -o -name "*.cfg" \) -exec grep -l "psx_ohlcv" {} + 2>/dev/null | while read f; do
    echo "Updating config: $f"
    sed -i 's/psx_ohlcv/pakfindata/g' "$f"
  done
  
  # Verify
  echo "--- After ---"
  grep -rn "psx_ohlcv" --include="*.py" --include="*.toml" "$QP_DIR" | grep -v __pycache__
  echo "(should be empty)"
else
  echo "qp-mono not found — update manually"
fi
```

### Step 15 — Update psx-live (if applicable)

```bash
PSX_LIVE=$(find ~/projects ~ -maxdepth 2 -name "psx-live" -type d 2>/dev/null | head -1)
if [ -n "$PSX_LIVE" ]; then
  echo "Checking psx-live at: $PSX_LIVE"
  grep -rn "psx_ohlcv" "$PSX_LIVE" --include="*.js" --include="*.ts" --include="*.json" --include="*.env*" | grep -v node_modules
  # psx-live probably doesn't import the Python package — it connects via API/WebSocket
  # But check for any config references
fi
```

### Step 16 — Update environment references

```bash
# Update crontab
echo "Current crontab entries with psx_ohlcv:"
crontab -l 2>/dev/null | grep "psx_ohlcv\|psxsync"
echo ""
echo "To fix, run: crontab -e"
echo "Replace: ~/psx_ohlcv → ~/pakfindata"
echo "Replace: psxsync → pfsync"

# Update shell configs
for f in ~/.bashrc ~/.bash_profile ~/.zshrc ~/.profile; do
  if [ -f "$f" ] && grep -q "psx_ohlcv\|psxsync" "$f"; then
    echo "Updating: $f"
    sed -i 's|~/psx_ohlcv|~/pakfindata|g' "$f"
    sed -i 's/psxsync/pfsync/g' "$f"
    sed -i 's/psx_ohlcv/pakfindata/g' "$f"
  fi
done

# Update MCP server configs (if any)
find ~/.config/claude/ ~/.claude/ 2>/dev/null -name "*.json" | while read f; do
  if grep -q "psx_ohlcv" "$f"; then
    echo "Updating MCP config: $f"
    sed -i 's/psx_ohlcv/pakfindata/g' "$f"
    sed -i 's|~/psx_ohlcv|~/pakfindata|g' "$f"
  fi
done
```

---

## PHASE 4 — COMPREHENSIVE TESTING

### Step 17 — Package import tests

```bash
cd ~/pakfindata

echo "╔══════════════════════════════════════════════════════╗"
echo "║  TESTING — Full verification suite                    ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "━━━━ 17A: Core imports ━━━━"
python -c "
import pakfindata
print(f'✅ pakfindata imported: {pakfindata.__name__}')
"

python -c "
from pakfindata.db import connect
print('✅ db.connect imported')
"

python -c "
from pakfindata.sources.mufap import MUFAPScraper
print('✅ MUFAPScraper imported')
" 2>/dev/null || python -c "
# Try alternative import paths based on actual module structure
import pakfindata.sources
print('✅ sources module imported')
"

echo ""
echo "━━━━ 17B: Import every submodule ━━━━"
python -c "
import importlib, pkgutil, sys
pkg = importlib.import_module('pakfindata')
errors = []
for importer, modname, ispkg in pkgutil.walk_packages(pkg.__path__, prefix='pakfindata.'):
    try:
        importlib.import_module(modname)
    except Exception as e:
        errors.append(f'  ❌ {modname}: {e}')
if errors:
    print('Import failures:')
    for e in errors:
        print(e)
else:
    print('✅ All submodules imported successfully')
"
```

### Step 18 — CLI tests

```bash
echo ""
echo "━━━━ 18: CLI commands ━━━━"
pfsync --help 2>&1 | head -3 && echo "✅ pfsync --help works" || echo "❌ pfsync --help failed"

# Test a non-destructive command
pfsync status 2>&1 | head -10 && echo "✅ pfsync status works" || echo "❌ pfsync status failed"

# Verify old CLI is gone
which psxsync 2>/dev/null && echo "⚠️ psxsync still exists at $(which psxsync) — remove manually" || echo "✅ psxsync no longer in PATH"
```

### Step 19 — Database connectivity

```bash
echo ""
echo "━━━━ 19: Database access ━━━━"
python -c "
from pakfindata.db import connect
con = connect('/mnt/e/psxdata/psx.sqlite')
tables = con.execute('SELECT COUNT(*) FROM sqlite_master WHERE type=\"table\"').fetchone()[0]
print(f'✅ Connected to DB: {tables} tables')
# Quick sanity check on a known table
rows = con.execute('SELECT COUNT(*) FROM mutual_fund_nav').fetchone()[0]
print(f'✅ mutual_fund_nav: {rows} rows')
con.close()
"
```

### Step 20 — Run full test suite

```bash
echo ""
echo "━━━━ 20: Test suite ━━━━"
cd ~/pakfindata
pytest tests/ -x -q --tb=short 2>&1 | tail -20

echo ""
echo "━━━━ 20B: Check for test files still referencing old name ━━━━"
grep -rn "psx_ohlcv" tests/ | grep -v __pycache__
```

### Step 21 — Streamlit UI test

```bash
echo ""
echo "━━━━ 21: Streamlit UI ━━━━"
# Verify Streamlit can find the app
ls -la src/pakfindata/ui/app.py && echo "✅ app.py exists" || echo "❌ app.py not found"

# Quick syntax check — import the page modules
python -c "
import sys
sys.path.insert(0, 'src')
from pakfindata.ui import app
print('✅ Streamlit app module imports OK')
" 2>&1
```

### Step 22 — External project tests

```bash
echo ""
echo "━━━━ 22: External projects ━━━━"

# Test qp-mono imports (if it references pakfindata)
QP_DIR=$(find ~/projects ~/qp-mono ~ -maxdepth 2 -name "qp-mono" -type d 2>/dev/null | head -1)
if [ -n "$QP_DIR" ]; then
  echo "Testing qp-mono imports..."
  cd "$QP_DIR"
  python -c "
from qsconnect.adapters.psx_adapter import PSXAdapter
print('✅ qp-mono PSXAdapter imports OK')
" 2>&1 || echo "❌ qp-mono import failed — check adapter path"
  cd ~/pakfindata
fi
```

### Step 23 — Final verification scan

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  FINAL SCAN — No orphaned references                 ║"
echo "╚══════════════════════════════════════════════════════╝"

echo ""
echo "In pakfindata project:"
grep -rn "psx_ohlcv" ~/pakfindata/ --include="*.py" --include="*.toml" --include="*.sh" --include="*.yaml" --include="*.json" | grep -v __pycache__ | grep -v .git | grep -v .egg
echo "(should be empty or only in CHANGELOG/docs referencing old name)"

echo ""
echo "In qp-mono:"
grep -rn "psx_ohlcv" ~/projects/qp-mono/ --include="*.py" --include="*.toml" 2>/dev/null | grep -v __pycache__ | grep -v .git
echo "(should be empty)"

echo ""
echo "Pip installed packages:"
pip show pakfindata 2>/dev/null | head -5
pip show psx_ohlcv 2>/dev/null && echo "⚠️ OLD PACKAGE STILL INSTALLED" || echo "✅ Old package removed"
```

---

## PHASE 5 — GIT COMMIT

```bash
cd ~/pakfindata

git add -A
git status

git commit -m "refactor: rename psx_ohlcv → pakfindata

  The project has grown beyond PSX OHLCV data to cover:
  - 1,190 mutual funds (MUFAP) with 1.9M NAV rows
  - Treasury auctions (T-Bill, PIB, GIS — 25 years of history)
  - Yield curves (PKRV, PKISRV, PKFRV)
  - Interest rates (KIBOR, KONIA, SBP policy rate)
  - FX rates (interbank, open market, kerb)
  - Bond trading volumes (SBP SMTV)
  - ETFs, REITs, company fundamentals
  - 10+ Streamlit dashboard pages
  
  RENAMED:
  - Package: psx_ohlcv → pakfindata
  - Project root: ~/psx_ohlcv → ~/pakfindata
  - CLI: psxsync → pfsync
  
  UNCHANGED:
  - Conda env: psx (just a label)
  - Database: /mnt/e/psxdata/psx.sqlite
  - Class names: PSXClient, PSXScraper, etc. (describe the exchange)
  - Table names in SQLite
  - All data, all functionality preserved"

git push origin refactor/rename-pakfindata
```

---

## CRITICAL RULES

1. **Phase 1 FIRST** — do NOT start renaming until the full impact analysis is complete and shown.
2. **Branch safety** — work on `refactor/rename-pakfindata` branch, not main/dev.
3. **Exact match only** — `sed 's/psx_ohlcv/pakfindata/g'` is safe because `psx_ohlcv` is unique. No false positives.
4. **DO NOT rename** PSXClient, PSXScraper, psx_symbols, psx.sqlite, psxdata/, dps.psx.com.pk — these are "PSX the exchange".
5. **DO rename** every import, package path, CLI command, project directory, config reference.
6. **psxsync → pfsync** everywhere — shell scripts, cron, CLI entry point, docs.
7. **Test EVERY external project** — qp-mono especially, since it imports from the package.
8. **pip install -e .** after rename — this registers the new package name.
9. **Remove old egg-info** before renaming to avoid stale metadata.
10. **Check Windows side** — WSL2 means MCP configs, task scheduler, etc. may be on the Windows filesystem.
