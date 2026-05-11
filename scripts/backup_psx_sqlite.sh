#!/usr/bin/env bash
# Daily auto-backup of psx.sqlite using sqlite3's `.backup` command (safe to
# run while writers are active — uses the online backup API, no file copy
# races). Keeps last 7 daily snapshots at $BACKUP_DIR.
#
# Install as a cron job (daily at 02:00):
#   (crontab -l 2>/dev/null | grep -v backup_psx_sqlite; \
#    echo "0 2 * * * $HOME/projects/pakfindata/scripts/backup_psx_sqlite.sh \
#         >> $HOME/.cron_backup.log 2>&1") | crontab -

set -e

SRC="${PSX_DB_PATH:-$HOME/psxdata_rescue/psx.sqlite}"
BACKUP_DIR="${PSX_BACKUP_DIR:-/mnt/e/psxdata/backups}"
TS=$(date +%Y%m%d)
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR"

if [ ! -f "$SRC" ]; then
    echo "[backup] ERROR: source DB not found at $SRC" >&2
    exit 1
fi

OUT="$BACKUP_DIR/psx_$TS.sqlite"
echo "[backup] $(date -Iseconds) — backing up $SRC -> $OUT"
sqlite3 "$SRC" ".backup '$OUT'"

PRUNED=$(find "$BACKUP_DIR" -maxdepth 1 -name "psx_*.sqlite" -mtime +$RETENTION_DAYS -print -delete | wc -l)
SIZE=$(du -h "$OUT" | cut -f1)
echo "[backup] saved $OUT ($SIZE), pruned $PRUNED snapshots older than ${RETENTION_DAYS}d"
