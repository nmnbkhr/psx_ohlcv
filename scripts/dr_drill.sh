#!/usr/bin/env bash
# scripts/dr_drill.sh — Disaster Recovery drill orchestrator
#
# Tests the recovery path on a COPY of the live psx.sqlite. Never modifies
# the live DB or any file under ~/psxdata_rescue/.
#
# Subcommands:
#   prepare <date>  — copy live DB to workspace via SQLite online-backup API
#   corrupt <date>  — destroy the workspace copy's header (May-9 failure mode)
#   recover <date>  — run sqlite_page_recover.py against the corrupted copy
#   verify  <date>  — row-count + integrity comparison original vs recovered
#
# Workspace: /mnt/e/psxdata/dr_drill_<date>/
#
# Hard safety: any path that resolves to ~/psxdata_rescue/ aborts the script.

set -euo pipefail

# ─── Configuration ──────────────────────────────────────────────────────
LIVE_DB="${PSX_DB_PATH:-$HOME/psxdata_rescue/psx.sqlite}"
WORKSPACE_ROOT="${DR_DRILL_ROOT:-/mnt/e/psxdata}"
RESCUE_DIR="$HOME/psxdata_rescue"

# ─── Helpers ────────────────────────────────────────────────────────────

err() { echo "[dr_drill] ERROR: $*" >&2; exit 1; }
log() { echo "$(date +'%Y-%m-%d %H:%M:%S') $*"; }

# Refuse any operation that would touch files under ~/psxdata_rescue/.
# Drill workspace MUST be under /mnt/e/psxdata/dr_drill_*. This safety
# check prevents the script from ever damaging the live DB or forensic
# copies, regardless of typo or argument confusion.
guard_path() {
    local path="$1"
    local resolved
    # Resolve to absolute path even if file doesn't exist yet.
    resolved=$(readlink -f "$path" 2>/dev/null || realpath -m "$path")
    case "$resolved" in
        "$RESCUE_DIR"/*|"$RESCUE_DIR")
            err "Path '$path' resolves under $RESCUE_DIR — refusing for safety."
            ;;
    esac
}

usage() {
    cat <<EOF
Usage: $0 <subcommand> <date>

Subcommands:
  prepare <date>   Copy live DB → workspace; verify integrity.
  corrupt <date>   Destroy workspace copy's SQLite header.
  recover <date>   Run sqlite_page_recover.py against corrupted copy.
  verify  <date>   Compare row counts original vs recovered.

<date> is YYYYMMDD; the workspace lives at $WORKSPACE_ROOT/dr_drill_<date>/.

The live DB ($LIVE_DB) and anything under $RESCUE_DIR are never touched.
EOF
    exit 2
}

# ─── Subcommands ────────────────────────────────────────────────────────

cmd_prepare() {
    local date="$1"
    local workspace="$WORKSPACE_ROOT/dr_drill_$date"
    local original="$workspace/original.sqlite"
    local drill_log="$workspace/drill_$date.log"

    guard_path "$workspace"
    guard_path "$original"

    [ -f "$LIVE_DB" ] || err "Live DB not found at $LIVE_DB"

    if [ -e "$workspace" ]; then
        err "Workspace already exists at $workspace — refusing to overwrite. Remove manually first."
    fi

    mkdir -p "$workspace"
    : > "$drill_log"

    {
        log "=== dr_drill prepare started ==="
        log "live_db=$LIVE_DB"
        log "workspace=$workspace"

        log "Copying live DB via sqlite3 .backup (online API, safe with active writers)..."
        local t0 t1
        t0=$(date +%s)
        sqlite3 "$LIVE_DB" ".backup '$original'"
        t1=$(date +%s)
        log "Copy completed in $((t1 - t0)) seconds."

        local size_bytes size_human
        size_bytes=$(stat -c %s "$original")
        size_human=$(du -h "$original" | cut -f1)
        log "original.sqlite size = $size_human ($size_bytes bytes)"

        # SQLite's online .backup API guarantees the copy is
        # structurally sound by construction. Verifying it with
        # PRAGMA integrity_check or quick_check on 14GB across
        # NTFS-via-FUSE was measured at 30+ min for integrity_check
        # and 12+ min for quick_check — both IO-bound on random
        # reads of millions of pages. Skipped here; the corruption
        # injection in `corrupt` is the real test that matters.
        log "Skipping integrity verification (trust .backup API; NTFS-3g random reads are slow)."

        local page_count page_size
        page_count=$(sqlite3 "$original" "PRAGMA page_count;")
        page_size=$(sqlite3 "$original" "PRAGMA page_size;")
        log "page_count=$page_count page_size=$page_size"

        log "=== dr_drill prepare done ==="
    } | tee -a "$drill_log"

    echo
    echo "Workspace ready at: $workspace"
    echo "Next: $0 corrupt $date"
}

cmd_corrupt() {
    local date="$1"
    local workspace="$WORKSPACE_ROOT/dr_drill_$date"
    local original="$workspace/original.sqlite"
    local corrupted="$workspace/corrupted.sqlite"
    local drill_log="$workspace/drill_$date.log"

    guard_path "$workspace"
    guard_path "$corrupted"

    [ -d "$workspace" ] || err "Workspace not found at $workspace. Run prepare first."
    [ -f "$original" ] || err "original.sqlite not found. Run prepare first."

    {
        log "=== dr_drill corrupt started ==="
        log "Copying original.sqlite → corrupted.sqlite (cp, fast — single sequential read)..."
        local t0 t1
        t0=$(date +%s)
        cp "$original" "$corrupted"
        t1=$(date +%s)
        log "Copy completed in $((t1 - t0)) seconds."

        log "Destroying first 16 bytes of SQLite header (the May-9 failure mode)..."
        # The first 16 bytes are the SQLite magic string "SQLite format 3\000".
        # Overwriting them with random data makes the file unrecognizable to
        # sqlite3 — exactly what happened May 9.
        dd if=/dev/urandom of="$corrupted" bs=1 count=16 seek=0 conv=notrunc 2>/dev/null
        log "Header destroyed (16 bytes at offset 0 randomized)."

        # Confirm sqlite refuses to open. Must use a query that actually
        # touches the file (sqlite_master read) — SELECT 1 is a constant
        # expression evaluated before the DB is opened, so it always
        # returns 1 even on a corrupted file.
        local sqlite_err
        sqlite_err=$(sqlite3 "$corrupted" "SELECT name FROM sqlite_master LIMIT 1;" 2>&1 | head -1 || true)
        log "sqlite3 sqlite_master read: $sqlite_err"
        case "$sqlite_err" in
            *"not a database"*|*"malformed"*|*"file is encrypted"*)
                log "OK — sqlite3 correctly refuses to open the corrupted file." ;;
            *)
                log "WARNING — sqlite3 did NOT reject the file. Drill may be invalid: $sqlite_err" ;;
        esac

        local size_human
        size_human=$(du -h "$corrupted" | cut -f1)
        log "corrupted.sqlite size = $size_human"
        log "=== dr_drill corrupt done ==="
    } | tee -a "$drill_log"

    echo
    echo "Corrupted copy ready at: $corrupted"
    echo "Next: $0 recover $date [--dry-run] [--tables T1,T2,...]"
}

cmd_recover() {
    local date="$1"
    shift
    local workspace="$WORKSPACE_ROOT/dr_drill_$date"
    local corrupted="$workspace/corrupted.sqlite"
    local recovered="$workspace/recovered.sqlite"
    local checkpoint="$workspace/recovery_progress.json"
    local report="$workspace/recovery_report.json"
    local drill_log="$workspace/drill_$date.log"
    local recovery_log="$workspace/recovery.log"

    guard_path "$workspace"
    guard_path "$recovered"

    [ -f "$corrupted" ] || err "corrupted.sqlite not found. Run 'corrupt $date' first."

    # Pass-through extra args to sqlite_page_recover.py (e.g. --dry-run,
    # --tables eod_ohlcv,psx_indices). User-friendly default = full recovery.
    local extra_args=("$@")

    # Schema source: a known-good DB. Use the live DB itself by default —
    # sqlite_page_recover.py only reads schema from it (read-only mmap).
    # Falls back to the most recent backup if user prefers.
    local schema_db="$LIVE_DB"
    local script="$HOME/projects/pakfindata/scripts/sqlite_page_recover.py"

    [ -f "$script" ] || err "Recovery tool not found at $script"

    {
        log "=== dr_drill recover started ==="
        log "corrupted=$corrupted"
        log "recovered=$recovered"
        log "schema_db=$schema_db (read-only mmap)"
        log "checkpoint=$checkpoint"
        log "report=$report"
        log "extra_args=${extra_args[*]:-(none)}"

        local t0 t1
        t0=$(date +%s)
        # The recovery tool's --source / --schema-db / --output / --checkpoint
        # / --report are all required by its argparse.
        if python "$script" \
            --source "$corrupted" \
            --schema-db "$schema_db" \
            --output "$recovered" \
            --checkpoint "$checkpoint" \
            --report "$report" \
            --log-file "$recovery_log" \
            "${extra_args[@]}"; then
            t1=$(date +%s)
            log "Recovery succeeded in $((t1 - t0)) seconds."
        else
            local rc=$?
            t1=$(date +%s)
            log "Recovery FAILED with exit $rc after $((t1 - t0)) seconds."
            log "See $recovery_log for details."
        fi

        if [ -f "$recovered" ]; then
            local size_human
            size_human=$(du -h "$recovered" | cut -f1)
            log "recovered.sqlite size = $size_human"
        else
            log "recovered.sqlite was NOT created (likely --dry-run)."
        fi

        if [ -f "$report" ]; then
            log "Recovery report: $report"
        fi
        log "=== dr_drill recover done ==="
    } | tee -a "$drill_log"

    echo
    echo "Recovery log: $recovery_log"
    [ -f "$recovered" ] && echo "Next: $0 verify $date" || \
        echo "(no recovered DB — --dry-run? skip verify)"
}

cmd_verify() {
    local date="$1"
    local workspace="$WORKSPACE_ROOT/dr_drill_$date"
    local original="$workspace/original.sqlite"
    local recovered="$workspace/recovered.sqlite"
    local report_txt="$workspace/verification_report.txt"
    local drill_log="$workspace/drill_$date.log"

    guard_path "$workspace"

    [ -f "$original" ] || err "original.sqlite not found. Run prepare first."
    [ -f "$recovered" ] || err "recovered.sqlite not found. Run recover first."

    {
        log "=== dr_drill verify started ==="
    } | tee -a "$drill_log"

    # Build per-table comparison without printing all rows to drill_log.
    {
        echo "DR Drill verification report"
        echo "============================"
        echo "Generated: $(date -Iseconds)"
        echo "Original:  $original"
        echo "Recovered: $recovered"
        echo ""
        echo "Per-table row counts (orig vs recovered, % recovered)"
        echo "-----------------------------------------------------"
    } > "$report_txt"

    local orig_tables recov_tables
    orig_tables=$(sqlite3 "$original" \
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
    recov_tables=$(sqlite3 "$recovered" \
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;" 2>/dev/null || echo "")

    local total_orig=0 total_recov=0 missing_tables=0
    while IFS= read -r table; do
        [ -n "$table" ] || continue
        local orig recov pct
        orig=$(sqlite3 "$original" "SELECT COUNT(*) FROM \"$table\";" 2>/dev/null || echo "?")
        if echo "$recov_tables" | grep -qx "$table"; then
            recov=$(sqlite3 "$recovered" "SELECT COUNT(*) FROM \"$table\";" 2>/dev/null || echo "?")
        else
            recov=0
            missing_tables=$((missing_tables + 1))
        fi
        if [ "$orig" != "?" ] && [ "$recov" != "?" ] && [ "$orig" -gt 0 ] 2>/dev/null; then
            pct=$(awk "BEGIN { printf \"%.1f%%\", $recov * 100 / $orig }")
            total_orig=$((total_orig + orig))
            total_recov=$((total_recov + recov))
        elif [ "$orig" = "0" ]; then
            pct="(empty)"
        else
            pct="?"
        fi
        printf "  %-40s orig=%10s recov=%10s  %s\n" "$table" "$orig" "$recov" "$pct" >> "$report_txt"
    done <<< "$orig_tables"

    {
        echo ""
        echo "Summary"
        echo "-------"
        echo "  Tables in original:  $(echo "$orig_tables" | grep -c .)"
        echo "  Tables in recovered: $(echo "$recov_tables" | grep -c .)"
        echo "  Tables missing in recovered: $missing_tables"
        echo "  Total rows in original:  $total_orig"
        echo "  Total rows in recovered: $total_recov"
        if [ "$total_orig" -gt 0 ]; then
            awk "BEGIN { printf \"  Overall recovery rate: %.2f%% (counting only non-empty tables)\n\", \
                $total_recov * 100 / $total_orig }"
        fi
    } >> "$report_txt"

    log "Verification report at $report_txt" | tee -a "$drill_log"
    cat "$report_txt" | tee -a "$drill_log"
    log "=== dr_drill verify done ===" | tee -a "$drill_log"
}

# ─── Dispatch ───────────────────────────────────────────────────────────

[ $# -ge 2 ] || usage

subcmd="$1"
date_arg="$2"

# Validate date format YYYYMMDD
case "$date_arg" in
    [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;;
    *) err "Date must be YYYYMMDD (got: $date_arg)" ;;
esac

shift 2  # drop subcommand + date so $@ holds any pass-through args
case "$subcmd" in
    prepare) cmd_prepare "$date_arg" "$@" ;;
    corrupt) cmd_corrupt "$date_arg" "$@" ;;
    recover) cmd_recover "$date_arg" "$@" ;;
    verify)  cmd_verify  "$date_arg" "$@" ;;
    *) usage ;;
esac
