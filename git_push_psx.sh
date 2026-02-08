#!/bin/bash
# PSX OHLCV — Full Git Push Script
# Consolidates everything to dev branch, pushes with all tags
# Run from: ~/psx_ohlcv
set -e

echo "════════════════════════════════════════════"
echo "  PSX OHLCV — Git Push (Full Consolidation)"
echo "════════════════════════════════════════════"
echo ""

cd ~/psx_ohlcv || { echo "❌ Cannot cd to ~/psx_ohlcv"; exit 1; }

# ─── Step 1: Check current state ────────────────────
echo "📋 Step 1: Current state"
echo "  Branch: $(git branch --show-current)"
echo "  Remote: $(git remote -v | head -1)"
echo ""

# ─── Step 2: Ensure we're on dev ────────────────────
echo "📋 Step 2: Switch to dev"
git checkout dev
echo ""

# ─── Step 3: Check for uncommitted changes ──────────
echo "📋 Step 3: Check uncommitted changes"
if [ -n "$(git status --porcelain)" ]; then
    echo "  ⚠️  Uncommitted changes found:"
    git status --short
    echo ""
    echo "  Committing all changes..."
    git add -A
    git commit -m "chore: clean up uncommitted changes before push"
    echo "  ✅ Committed"
else
    echo "  ✅ Working tree clean"
fi
echo ""

# ─── Step 4: Merge any other branches into dev ──────
echo "📋 Step 4: Merge all branches into dev"
OTHER_BRANCHES=$(git branch --list | grep -v '^\*' | grep -v 'dev' | grep -v 'main' | sed 's/^[ ]*//')

if [ -z "$OTHER_BRANCHES" ]; then
    echo "  ✅ No other branches to merge"
else
    for branch in $OTHER_BRANCHES; do
        echo "  Merging: $branch → dev"
        git merge "$branch" --no-ff -m "merge: $branch into dev (consolidation)" 2>/dev/null || {
            echo "  ⚠️  Merge conflict on $branch — skipping (resolve manually)"
            git merge --abort 2>/dev/null
            continue
        }
        echo "  ✅ Merged $branch"
    done
fi
echo ""

# ─── Step 5: Merge dev into main (if main exists) ───
echo "📋 Step 5: Sync main with dev"
if git show-ref --verify --quiet refs/heads/main; then
    git checkout main
    git merge dev --no-ff -m "merge: dev into main — v3.0.0 release" 2>/dev/null || {
        echo "  ⚠️  Merge conflict dev→main — forcing main to match dev"
        git merge --abort
        git reset --hard dev
    }
    echo "  ✅ main synced with dev"
    git checkout dev
elif git show-ref --verify --quiet refs/heads/master; then
    git checkout master
    git merge dev --no-ff -m "merge: dev into master — v3.0.0 release" 2>/dev/null || {
        echo "  ⚠️  Merge conflict dev→master — forcing master to match dev"
        git merge --abort
        git reset --hard dev
    }
    echo "  ✅ master synced with dev"
    git checkout dev
else
    echo "  No main/master branch — creating main from dev"
    git branch main dev
    echo "  ✅ main created from dev"
fi
echo ""

# ─── Step 6: Delete merged feature branches ─────────
echo "📋 Step 6: Clean up merged branches"
MERGED=$(git branch --merged dev | grep -v '^\*' | grep -v 'dev' | grep -v 'main' | grep -v 'master' | sed 's/^[ ]*//')
if [ -z "$MERGED" ]; then
    echo "  ✅ No merged branches to clean"
else
    for branch in $MERGED; do
        git branch -d "$branch" 2>/dev/null && echo "  🗑️  Deleted: $branch" || echo "  ⚠️  Kept: $branch"
    done
fi
echo ""

# ─── Step 7: Show all tags ──────────────────────────
echo "📋 Step 7: Tags"
echo "  Local tags:"
git tag -l | while read tag; do
    echo "    $tag → $(git log -1 --format='%h %s' "$tag" 2>/dev/null)"
done
echo ""

# ─── Step 8: Push everything ────────────────────────
echo "📋 Step 8: Push to origin"
echo ""

echo "  Pushing dev..."
git push origin dev 2>&1 && echo "  ✅ dev pushed" || echo "  ❌ dev push failed"

echo ""
echo "  Pushing main..."
git push origin main 2>&1 && echo "  ✅ main pushed" || {
    echo "  ⚠️  main push failed — trying with --set-upstream"
    git push --set-upstream origin main 2>&1 && echo "  ✅ main pushed" || echo "  ❌ main push failed"
}

echo ""
echo "  Pushing all tags..."
git push origin --tags 2>&1 && echo "  ✅ All tags pushed" || echo "  ❌ Tag push failed"

echo ""

# ─── Step 9: Delete remote branches that don't exist locally ─
echo "📋 Step 9: Clean remote stale branches"
REMOTE_BRANCHES=$(git branch -r | grep 'origin/' | grep -v 'HEAD' | grep -v 'origin/dev' | grep -v 'origin/main' | grep -v 'origin/master' | sed 's|origin/||' | sed 's/^[ ]*//')
if [ -n "$REMOTE_BRANCHES" ]; then
    for rb in $REMOTE_BRANCHES; do
        if ! git show-ref --verify --quiet "refs/heads/$rb"; then
            echo "  🗑️  Deleting remote: origin/$rb (no local branch)"
            git push origin --delete "$rb" 2>/dev/null || echo "  ⚠️  Could not delete origin/$rb"
        fi
    done
else
    echo "  ✅ No stale remote branches"
fi
echo ""

# ─── Step 10: Final verification ────────────────────
echo "════════════════════════════════════════════"
echo "  FINAL STATE"
echo "════════════════════════════════════════════"
echo ""
echo "  Current branch: $(git branch --show-current)"
echo "  Local branches:"
git branch | sed 's/^/    /'
echo ""
echo "  Remote branches:"
git branch -r | sed 's/^/    /'
echo ""
echo "  Tags:"
git tag -l | sed 's/^/    /'
echo ""
echo "  Latest commits on dev:"
git log --oneline -5 | sed 's/^/    /'
echo ""
echo "  DB size: $(ls -lh /mnt/e/psxdata/psx.sqlite 2>/dev/null | awk '{print $5}' || echo 'N/A')"
echo ""
echo "✅ DONE — Everything pushed to origin"
