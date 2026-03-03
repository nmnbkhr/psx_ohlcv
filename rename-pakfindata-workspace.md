# Claude Code Prompt: pakfindata Rename — WORKSPACE UPDATE (Addendum)

## Context

Insert this between Step 9 and Step 10 of `rename-pakfindata-execution.md`.

The development environment uses a VS Code multi-root workspace:
- **Workspace file:** `pakfindata.code-workspace` (needs renaming)
- **Folder 1:** pakfindata → pakfindata (THE rename target)
- **Folder 2:** psx-live (React trading terminal — PSX only, does NOT rename)
- **Conda env:** `psx` (shared by both, does NOT change)

### Key distinction:
- `pakfindata` = our Python package → becomes `pakfindata`
- `psx-live` = React frontend for PSX exchange specifically → stays `psx-live`
- psx-live may call pakfindata's API/MCP endpoints — those references need updating

---

## STEP 9.1 — Audit Workspace File

```bash
echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 9.1: WORKSPACE FILE AUDIT                      ║"
echo "╚══════════════════════════════════════════════════════╝"

# 9.1A — Find the workspace file
echo "━━━ Workspace file location ━━━"
find ~ -maxdepth 3 -name "*.code-workspace" 2>/dev/null | grep -i psx
# Also check common locations
ls -la ~/pakfindata.code-workspace 2>/dev/null
ls -la ~/pakfindata/*.code-workspace 2>/dev/null
ls -la ~/projects/*.code-workspace 2>/dev/null
ls -la ~/*.code-workspace 2>/dev/null

# 9.1B — Show current workspace content
echo ""
echo "━━━ Current workspace file ━━━"
WS_FILE=$(find ~ -maxdepth 3 -name "pakfindata.code-workspace" 2>/dev/null | head -1)
if [ -z "$WS_FILE" ]; then
  WS_FILE=$(find ~ -maxdepth 3 -name "*psx*.code-workspace" 2>/dev/null | head -1)
fi

if [ -n "$WS_FILE" ]; then
  echo "Found: $WS_FILE"
  echo ""
  cat "$WS_FILE"
else
  echo "⚠️ Workspace file not found — search manually"
fi

# 9.1C — Check what paths/settings reference pakfindata
echo ""
echo "━━━ References in workspace file ━━━"
if [ -n "$WS_FILE" ]; then
  grep -n "pakfindata\|pfsync" "$WS_FILE"
  echo ""
  echo "Lines referencing psx-live (should NOT change):"
  grep -n "psx-live\|psx_live" "$WS_FILE"
fi
```

---

## STEP 9.2 — Audit psx-live for Cross-References

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 9.2: psx-live CROSS-REFERENCE AUDIT            ║"
echo "╚══════════════════════════════════════════════════════╝"

# Find psx-live directory
PSX_LIVE=""
for candidate in ~/psx-live ~/projects/psx-live ~/psx_live; do
  [ -d "$candidate" ] && PSX_LIVE="$candidate" && break
done

# Also check workspace file for the path
if [ -z "$PSX_LIVE" ] && [ -n "$WS_FILE" ]; then
  PSX_LIVE=$(python3 -c "
import json
with open('$WS_FILE') as f:
    ws = json.load(f)
for folder in ws.get('folders', []):
    p = folder.get('path', '')
    if 'psx-live' in p.lower() or 'psx_live' in p.lower():
        print(p)
        break
" 2>/dev/null)
fi

if [ -n "$PSX_LIVE" ]; then
  echo "Found psx-live at: $PSX_LIVE"
  
  echo ""
  echo "━━━ 9.2A: Python package imports (unlikely but check) ━━━"
  grep -rn "pakfindata\|pakfindata\|from pakfindata\|import pakfindata" \
    --include="*.py" --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" \
    "$PSX_LIVE" 2>/dev/null | grep -v node_modules | grep -v __pycache__ | grep -v .git
  
  echo ""
  echo "━━━ 9.2B: API endpoint URLs referencing pakfindata ━━━"
  grep -rn "pakfindata\|pfsync\|pakfindata\|pfsync" \
    --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" --include="*.json" --include="*.env*" \
    "$PSX_LIVE" 2>/dev/null | grep -v node_modules | grep -v .git
  
  echo ""
  echo "━━━ 9.2C: Package.json scripts referencing pakfindata ━━━"
  if [ -f "$PSX_LIVE/package.json" ]; then
    grep -n "pakfindata\|pfsync" "$PSX_LIVE/package.json"
  fi
  
  echo ""
  echo "━━━ 9.2D: Environment/config files ━━━"
  for f in "$PSX_LIVE/.env" "$PSX_LIVE/.env.local" "$PSX_LIVE/.env.development" "$PSX_LIVE/.env.production"; do
    if [ -f "$f" ]; then
      echo "--- $(basename $f) ---"
      grep -n "pakfindata\|pfsync\|pakfindata\|pfsync" "$f" 2>/dev/null
      # Also check API base URLs that might point to our backend
      grep -n "API_URL\|BACKEND\|BASE_URL\|WS_URL\|SOCKET_URL" "$f" 2>/dev/null
    fi
  done
  
  echo ""
  echo "━━━ 9.2E: Docker/compose references ━━━"
  find "$PSX_LIVE" -maxdepth 2 \( -name "Dockerfile*" -o -name "docker-compose*" -o -name "nginx*" \) | while read f; do
    if grep -q "pakfindata\|pfsync\|~/pakfindata" "$f" 2>/dev/null; then
      echo "$f:"
      grep -n "pakfindata\|pfsync\|~/pakfindata" "$f"
    fi
  done
  
  echo ""
  echo "━━━ 9.2F: Proxy/API config (vite, webpack, next.config, etc.) ━━━"
  find "$PSX_LIVE" -maxdepth 2 \( -name "vite.config*" -o -name "webpack.config*" -o -name "next.config*" -o -name "proxy.conf*" \) | while read f; do
    echo "--- $f ---"
    grep -n "pakfindata\|pfsync\|localhost\|127.0.0.1\|api.*psx" "$f" 2>/dev/null
  done
  
  echo ""
  echo "━━━ 9.2G: MCP/WebSocket connection configs ━━━"
  grep -rn "mcp\|websocket\|ws://\|wss://\|socket" \
    --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" --include="*.json" --include="*.env*" \
    "$PSX_LIVE" 2>/dev/null | grep -v node_modules | grep -v .git | grep -i "pakfindata\|pakfindata\|pfsync\|pfsync" | head -20
  
  echo ""
  echo "━━━ DECISION MATRIX ━━━"
  echo ""
  total=$(grep -rn "pakfindata\|pfsync" "$PSX_LIVE" --include="*.py" --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" --include="*.json" --include="*.env*" --include="*.yml" --include="*.yaml" 2>/dev/null | grep -v node_modules | grep -v .git | wc -l)
  echo "Total pakfindata/pfsync references in psx-live: $total"
  echo ""
  if [ "$total" -eq 0 ]; then
    echo "✅ psx-live has NO cross-references to pakfindata — no changes needed"
    echo "   (psx-live likely connects to PSX directly via DPS/WebSocket, not through our package)"
  else
    echo "⚠️ psx-live has $total references that need updating"
    echo "   These are likely API endpoint configs or backend connection strings"
  fi
  
else
  echo "⚠️ psx-live directory not found"
  echo "   Check workspace file for exact path, or search:"
  echo "   find ~ -maxdepth 3 -name 'psx-live' -type d 2>/dev/null"
fi
```

---

## STEP 9.3 — Update Workspace File

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 9.3: UPDATE WORKSPACE FILE                     ║"
echo "╚══════════════════════════════════════════════════════╝"

# Re-find workspace file (we're now in ~/pakfindata after Step 4)
WS_FILE=$(find ~ -maxdepth 3 -name "pakfindata.code-workspace" 2>/dev/null | head -1)
if [ -z "$WS_FILE" ]; then
  WS_FILE=$(find ~ -maxdepth 3 -name "*psx*.code-workspace" 2>/dev/null | head -1)
fi

if [ -n "$WS_FILE" ]; then
  echo "Workspace file: $WS_FILE"
  echo ""
  echo "--- Before ---"
  cat "$WS_FILE"
  echo ""
  
  # 9.3A — Update folder paths inside the workspace file
  # Change pakfindata folder reference → pakfindata
  # Keep psx-live folder reference UNCHANGED
  python3 -c "
import json, sys, os

ws_path = '$WS_FILE'
with open(ws_path) as f:
    ws = json.load(f)

changes = []

# Update folder paths
for i, folder in enumerate(ws.get('folders', [])):
    old_path = folder.get('path', '')
    # Only change the pakfindata project folder, NOT psx-live
    if 'pakfindata' in old_path and 'psx-live' not in old_path and 'psx_live' not in old_path:
        new_path = old_path.replace('pakfindata', 'pakfindata')
        folder['path'] = new_path
        changes.append(f'  Folder {i}: {old_path} → {new_path}')
    
    # Update folder name/label if present
    old_name = folder.get('name', '')
    if 'pakfindata' in old_name:
        new_name = old_name.replace('pakfindata', 'pakfindata')
        folder['name'] = new_name
        changes.append(f'  Name {i}: {old_name} → {new_name}')

# Update settings that reference pakfindata
def update_dict(d, depth=0):
    if depth > 10:
        return
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, str):
                if 'pakfindata' in v and 'psx-live' not in v:
                    new_v = v.replace('pakfindata', 'pakfindata')
                    d[k] = new_v
                    changes.append(f'  Setting {k}: ...pakfindata... → ...pakfindata...')
                if 'pfsync' in v:
                    new_v = v.replace('pfsync', 'pfsync')
                    d[k] = new_v
                    changes.append(f'  Setting {k}: pfsync → pfsync')
            elif isinstance(v, (dict, list)):
                update_dict(v, depth + 1)
    elif isinstance(d, list):
        for i, item in enumerate(d):
            if isinstance(item, str):
                if 'pakfindata' in item and 'psx-live' not in item:
                    d[i] = item.replace('pakfindata', 'pakfindata')
                    changes.append(f'  List item: {item} → {d[i]}')
                if 'pfsync' in item:
                    d[i] = item.replace('pfsync', 'pfsync')
            elif isinstance(item, (dict, list)):
                update_dict(item, depth + 1)

update_dict(ws.get('settings', {}))
update_dict(ws.get('launch', {}))
update_dict(ws.get('tasks', {}))
update_dict(ws.get('extensions', {}))

if changes:
    print('Changes made:')
    for c in changes:
        print(c)
else:
    print('No pakfindata references found in workspace settings')

# Write updated file
with open(ws_path, 'w') as f:
    json.dump(ws, f, indent=2)
    f.write('\n')

print(f'\nUpdated: {ws_path}')
"
  
  # 9.3B — Rename the workspace file itself
  WS_DIR=$(dirname "$WS_FILE")
  NEW_WS_FILE="$WS_DIR/pakfindata.code-workspace"
  
  echo ""
  echo "━━━ Rename workspace file ━━━"
  echo "  From: $WS_FILE"
  echo "  To:   $NEW_WS_FILE"
  mv "$WS_FILE" "$NEW_WS_FILE"
  
  echo ""
  echo "--- After ---"
  cat "$NEW_WS_FILE"
  
  # 9.3C — Verify psx-live path is UNCHANGED
  echo ""
  echo "━━━ Verify psx-live path preserved ━━━"
  python3 -c "
import json
with open('$NEW_WS_FILE') as f:
    ws = json.load(f)
for folder in ws.get('folders', []):
    p = folder.get('path', '')
    n = folder.get('name', '')
    print(f'  Folder: path={p}  name={n}')
    if 'psx-live' in p or 'psx_live' in p:
        print('    → psx-live path PRESERVED ✅')
    if 'pakfindata' in p:
        print('    → pakfindata path UPDATED ✅')
"

else
  echo "⚠️ Workspace file not found — create manually:"
  echo ""
  echo "Create ~/pakfindata.code-workspace with content:"
  echo '{'
  echo '  "folders": ['
  echo '    { "path": "pakfindata", "name": "pakfindata" },'
  echo '    { "path": "psx-live", "name": "psx-live" }'
  echo '  ],'
  echo '  "settings": {}'
  echo '}'
  echo ""
  echo "Adjust paths based on your actual directory layout."
fi
```

---

## STEP 9.4 — Fix psx-live Cross-References (if any found in 9.2)

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 9.4: FIX psx-live CROSS-REFERENCES             ║"
echo "╚══════════════════════════════════════════════════════╝"

PSX_LIVE=""
for candidate in ~/psx-live ~/projects/psx-live; do
  [ -d "$candidate" ] && PSX_LIVE="$candidate" && break
done

if [ -n "$PSX_LIVE" ]; then
  # Count references
  total=$(grep -rn "pakfindata\|pfsync" "$PSX_LIVE" --include="*.py" --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" --include="*.json" --include="*.env*" --include="*.yml" --include="*.yaml" --include="*.sh" 2>/dev/null | grep -v node_modules | grep -v .git | wc -l)
  
  if [ "$total" -eq 0 ]; then
    echo "✅ psx-live has zero pakfindata references — nothing to do"
    echo "   psx-live connects directly to PSX (dps.psx.com.pk), not through pakfindata"
  else
    echo "Found $total references in psx-live — fixing..."
    echo ""
    
    cd "$PSX_LIVE"
    
    # Show all references before fixing
    echo "━━━ References to fix ━━━"
    grep -rn "pakfindata\|pfsync" \
      --include="*.py" --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" \
      --include="*.json" --include="*.env*" --include="*.yml" --include="*.yaml" --include="*.sh" \
      . 2>/dev/null | grep -v node_modules | grep -v .git
    
    echo ""
    echo "━━━ Fixing... ━━━"
    
    # Fix JS/TS files (NOT in node_modules)
    find . \( -name "*.ts" -o -name "*.tsx" -o -name "*.js" -o -name "*.jsx" \) \
      -not -path "*/node_modules/*" -not -path "./.git/*" | while read f; do
      if grep -q "pakfindata" "$f" 2>/dev/null; then
        echo "  Fixing JS/TS: $f"
        sed -i 's/pakfindata/pakfindata/g' "$f"
      fi
      if grep -q "pfsync" "$f" 2>/dev/null; then
        sed -i 's/pfsync/pfsync/g' "$f"
      fi
    done
    
    # Fix env files
    find . -name ".env*" -maxdepth 2 | while read f; do
      if grep -q "pakfindata\|pfsync\|~/pakfindata" "$f" 2>/dev/null; then
        echo "  Fixing env: $f"
        sed -i 's/pakfindata/pakfindata/g; s/pfsync/pfsync/g; s|~/pakfindata|~/pakfindata|g' "$f"
      fi
    done
    
    # Fix configs (not node_modules)
    find . \( -name "*.json" -o -name "*.yml" -o -name "*.yaml" \) \
      -not -path "*/node_modules/*" -not -path "./.git/*" | while read f; do
      if grep -q "pakfindata\|pfsync" "$f" 2>/dev/null; then
        echo "  Fixing config: $f"
        sed -i 's/pakfindata/pakfindata/g; s/pfsync/pfsync/g; s|~/pakfindata|~/pakfindata|g' "$f"
      fi
    done
    
    # Fix Python files if any
    find . -name "*.py" -not -path "*/node_modules/*" -not -path "./.git/*" | while read f; do
      if grep -q "pakfindata" "$f" 2>/dev/null; then
        echo "  Fixing Python: $f"
        sed -i 's/pakfindata/pakfindata/g' "$f"
      fi
    done
    
    # Fix shell scripts
    find . -name "*.sh" | while read f; do
      if grep -q "pakfindata\|pfsync\|~/pakfindata" "$f" 2>/dev/null; then
        echo "  Fixing script: $f"
        sed -i 's/pakfindata/pakfindata/g; s/pfsync/pfsync/g; s|~/pakfindata|~/pakfindata|g' "$f"
      fi
    done
    
    # Verify
    echo ""
    echo "━━━ Verification ━━━"
    remaining=$(grep -rn "pakfindata\|pfsync" --include="*.py" --include="*.ts" --include="*.tsx" --include="*.js" --include="*.jsx" --include="*.json" --include="*.env*" --include="*.yml" --include="*.yaml" --include="*.sh" . 2>/dev/null | grep -v node_modules | grep -v .git)
    if [ -z "$remaining" ]; then
      echo "✅ psx-live fully cleaned of pakfindata references"
    else
      echo "❌ Remaining:"
      echo "$remaining"
    fi
    
    # Commit in psx-live repo
    if git rev-parse --git-dir > /dev/null 2>&1; then
      git add -A
      git diff --cached --stat
      if ! git diff --cached --quiet; then
        git commit -m "refactor: update pakfindata → pakfindata backend references

Companion change: backend package renamed from pakfindata to pakfindata.
- API/config references updated
- CLI references: pfsync → pfsync
- psx-live itself is NOT renamed (it's specifically for PSX exchange)"
        git push origin $(git branch --show-current) 2>/dev/null
        echo "✅ psx-live changes committed and pushed"
      else
        echo "✅ No changes to commit in psx-live"
      fi
    fi
    
    cd ~/pakfindata
  fi
else
  echo "⚠️ psx-live not found — check workspace file for actual path"
fi
```

---

## STEP 9.5 — Verify Workspace Opens Correctly

```bash
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  STEP 9.5: WORKSPACE VERIFICATION                    ║"
echo "╚══════════════════════════════════════════════════════╝"

# Find the new workspace file
NEW_WS=$(find ~ -maxdepth 3 -name "pakfindata.code-workspace" 2>/dev/null | head -1)

if [ -n "$NEW_WS" ]; then
  echo "Workspace file: $NEW_WS"
  echo ""
  
  # Verify JSON is valid
  python3 -c "
import json
with open('$NEW_WS') as f:
    ws = json.load(f)
print('✅ Valid JSON')

# Check all folder paths exist
for folder in ws.get('folders', []):
    import os
    path = folder.get('path', '')
    name = folder.get('name', path)
    
    # Resolve relative paths (relative to workspace file location)
    ws_dir = os.path.dirname('$NEW_WS')
    if not os.path.isabs(path):
        full_path = os.path.join(ws_dir, path)
    else:
        full_path = os.path.expanduser(path)
    
    exists = os.path.isdir(full_path)
    status = '✅' if exists else '❌ NOT FOUND'
    print(f'  {status} {name}: {path} → {full_path}')

# Check for any remaining pakfindata in settings
import re
ws_str = json.dumps(ws)
psx_refs = re.findall(r'pakfindata', ws_str)
pfsync_refs = re.findall(r'pfsync', ws_str)
if psx_refs or pfsync_refs:
    print(f'  ⚠️ Still has {len(psx_refs)} pakfindata + {len(pfsync_refs)} pfsync references')
else:
    print('  ✅ No old references in workspace settings')
"
  
  echo ""
  echo "━━━ To open in VS Code ━━━"
  echo "  code $NEW_WS"
  echo ""
  echo "Or from Windows:"
  echo "  code $(wslpath -w $NEW_WS 2>/dev/null || echo $NEW_WS)"
  
else
  echo "❌ pakfindata.code-workspace not found"
  echo "   Old file was at: $(find ~ -maxdepth 3 -name '*.code-workspace' 2>/dev/null)"
fi
```

**Commit (in pakfindata repo):**
```bash
cd ~/pakfindata
git add -A
git commit -m "refactor: update VS Code workspace — pakfindata.code-workspace → pakfindata.code-workspace

- Workspace file renamed
- pakfindata folder path updated
- psx-live folder path PRESERVED (unchanged)
- Workspace settings updated (python paths, launch configs, tasks)
- psx-live cross-references updated if any existed"
```

---

## SUMMARY: What Changes vs What Stays in Workspace

```
╔════════════════════════════════════════════════════════════════╗
║  WORKSPACE CHANGE MATRIX                                       ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  CHANGES:                                                      ║
║  ├── pakfindata.code-workspace → pakfindata.code-workspace     ║
║  ├── Folder 1 path: .../pakfindata → .../pakfindata            ║
║  ├── Python interpreter path in settings (if hardcoded)        ║
║  ├── Launch configs referencing pakfindata paths                ║
║  ├── Task configs referencing pfsync                          ║
║  └── psx-live cross-refs to pakfindata backend (if any)        ║
║                                                                ║
║  DOES NOT CHANGE:                                              ║
║  ├── Folder 2: psx-live path/name stays as-is                 ║
║  ├── Conda env 'psx' — shared by both projects                ║
║  ├── psx-live internal code (PSX exchange URLs, etc.)          ║
║  ├── Python interpreter binary path (still in psx env)         ║
║  ├── /mnt/e/psxdata/psx.sqlite                                ║
║  └── Any psx-live configs pointing to dps.psx.com.pk           ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
```

## CRITICAL RULES FOR WORKSPACE STEP

1. **psx-live does NOT rename** — it's a PSX exchange frontend, "psx" means the exchange there.
2. **Workspace file itself renames** — `pakfindata.code-workspace` → `pakfindata.code-workspace`.
3. **Only update psx-live if it imports pakfindata Python package** — if it just calls PSX APIs directly (dps.psx.com.pk), no changes needed.
4. **Conda env stays `psx`** — both projects share it, the env name is independent.
5. **psx-live gets its own git commit** in its own repo — don't mix pakfindata and psx-live commits.
6. **JSON must stay valid** — use Python's json module to update workspace file, not sed (sed can break JSON).
7. **Verify folder paths resolve** — after updating workspace, check that both folder paths point to real directories.
8. **Tell the user to reopen VS Code** — VS Code caches the workspace, a restart is needed.
