#!/bin/bash
# =============================================================================
# PSX OHLCV Backup Script
# =============================================================================
#
# Usage:
#   ./backup.sh              # Full backup
#   ./backup.sh --db-only    # Database only
#   ./backup.sh --code-only  # Code only
#   ./backup.sh --quick      # Quick backup (no compression)
#
# Cron setup (daily at 2 AM):
#   0 2 * * * /home/adnoman/psx_ohlcv/scripts/backup.sh >> /mnt/e/psxdata/logs/backup.log 2>&1
#
# =============================================================================

set -e

# Configuration
DB_PATH="/mnt/e/psxdata/psx.sqlite"
BACKUP_DIR="/mnt/e/psxdata/backups"
CODE_DIR="/home/adnoman/psx_ohlcv"
LOG_DIR="/mnt/e/psxdata/logs"
RETENTION_DAYS=30
DATE=$(date +%Y%m%d_%H%M%S)
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Parse arguments
DB_ONLY=false
CODE_ONLY=false
QUICK=false

for arg in "$@"; do
    case $arg in
        --db-only)   DB_ONLY=true ;;
        --code-only) CODE_ONLY=true ;;
        --quick)     QUICK=true ;;
    esac
done

# Create directories
mkdir -p "$BACKUP_DIR/db"
mkdir -p "$BACKUP_DIR/code"
mkdir -p "$BACKUP_DIR/config"
mkdir -p "$LOG_DIR"

# Initialize log
LOG_FILE="$LOG_DIR/backup_$DATE.log"

log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[$TIMESTAMP] ERROR: $1" | tee -a "$LOG_FILE" >&2
}

log "=========================================="
log "PSX OHLCV Backup Started"
log "=========================================="

# Track results
BACKUP_RESULTS=""
BACKUP_STATUS="SUCCESS"

# =============================================================================
# Database Backup
# =============================================================================
backup_database() {
    log "Starting database backup..."

    if [ ! -f "$DB_PATH" ]; then
        log_error "Database not found at $DB_PATH"
        BACKUP_STATUS="PARTIAL"
        return 1
    fi

    # Check database integrity first
    log "Checking database integrity..."
    INTEGRITY=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check" 2>&1 | head -1)

    if [ "$INTEGRITY" != "ok" ]; then
        log_error "Database integrity check failed: $INTEGRITY"
        log "Attempting backup anyway..."
    fi

    BACKUP_FILE="$BACKUP_DIR/db/psx_$DATE.sqlite"

    # Use SQLite backup command for consistency
    log "Creating backup using SQLite .backup command..."
    sqlite3 "$DB_PATH" ".backup '$BACKUP_FILE'" 2>&1 | tee -a "$LOG_FILE"

    if [ ! -f "$BACKUP_FILE" ]; then
        log_error "Backup file not created"
        BACKUP_STATUS="FAILED"
        return 1
    fi

    # Get original size
    ORIG_SIZE=$(ls -lh "$BACKUP_FILE" | awk '{print $5}')
    log "Backup created: $ORIG_SIZE"

    # Compress unless quick mode
    if [ "$QUICK" = false ]; then
        log "Compressing backup..."
        gzip "$BACKUP_FILE"
        BACKUP_FILE="$BACKUP_FILE.gz"
        COMP_SIZE=$(ls -lh "$BACKUP_FILE" | awk '{print $5}')
        log "Compressed size: $COMP_SIZE"
    fi

    # Verify backup
    log "Verifying backup..."
    if [ "$QUICK" = false ]; then
        gunzip -t "$BACKUP_FILE" 2>&1 || {
            log_error "Backup verification failed"
            BACKUP_STATUS="FAILED"
            return 1
        }
    fi

    # Record row counts
    ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM eod_ohlcv" 2>/dev/null || echo "N/A")
    log "Database rows (eod_ohlcv): $ROW_COUNT"

    BACKUP_RESULTS="$BACKUP_RESULTS\nDatabase: $BACKUP_FILE (${COMP_SIZE:-$ORIG_SIZE})"
    log "Database backup complete"
}

# =============================================================================
# Code Backup
# =============================================================================
backup_code() {
    log "Starting code backup..."

    if [ ! -d "$CODE_DIR/.git" ]; then
        log_error "Git repository not found at $CODE_DIR"
        BACKUP_STATUS="PARTIAL"
        return 1
    fi

    cd "$CODE_DIR"

    # Get current branch and commit
    BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
    COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    log "Current branch: $BRANCH, commit: $COMMIT"

    # Create git bundle (includes all branches and history)
    BUNDLE_FILE="$BACKUP_DIR/code/psx_code_$DATE.bundle"
    log "Creating git bundle..."
    git bundle create "$BUNDLE_FILE" --all 2>&1 | tee -a "$LOG_FILE"

    if [ ! -f "$BUNDLE_FILE" ]; then
        log_error "Bundle file not created"
        BACKUP_STATUS="PARTIAL"
        return 1
    fi

    BUNDLE_SIZE=$(ls -lh "$BUNDLE_FILE" | awk '{print $5}')
    log "Bundle created: $BUNDLE_SIZE"

    # Verify bundle
    git bundle verify "$BUNDLE_FILE" 2>&1 | head -5 | tee -a "$LOG_FILE"

    BACKUP_RESULTS="$BACKUP_RESULTS\nCode: $BUNDLE_FILE ($BUNDLE_SIZE)"
    log "Code backup complete"
}

# =============================================================================
# Config Backup
# =============================================================================
backup_config() {
    log "Starting config backup..."

    CONFIG_FILE="$BACKUP_DIR/config/config_$DATE.tar.gz"

    # Backup important config files
    tar -czf "$CONFIG_FILE" \
        -C "$CODE_DIR" pyproject.toml \
        -C "$CODE_DIR" .env 2>/dev/null \
        -C "/mnt/e/psxdata" services/*.json 2>/dev/null || true

    if [ -f "$CONFIG_FILE" ]; then
        CONFIG_SIZE=$(ls -lh "$CONFIG_FILE" | awk '{print $5}')
        BACKUP_RESULTS="$BACKUP_RESULTS\nConfig: $CONFIG_FILE ($CONFIG_SIZE)"
        log "Config backup complete: $CONFIG_SIZE"
    else
        log "No config files to backup"
    fi
}

# =============================================================================
# Cleanup Old Backups
# =============================================================================
cleanup_old_backups() {
    log "Cleaning up backups older than $RETENTION_DAYS days..."

    # Count before
    DB_COUNT_BEFORE=$(find "$BACKUP_DIR/db" -name "*.sqlite*" -type f | wc -l)
    CODE_COUNT_BEFORE=$(find "$BACKUP_DIR/code" -name "*.bundle" -type f | wc -l)

    # Delete old files
    find "$BACKUP_DIR/db" -name "*.sqlite*" -mtime +$RETENTION_DAYS -delete 2>/dev/null || true
    find "$BACKUP_DIR/code" -name "*.bundle" -mtime +$RETENTION_DAYS -delete 2>/dev/null || true
    find "$BACKUP_DIR/config" -name "*.tar.gz" -mtime +$RETENTION_DAYS -delete 2>/dev/null || true

    # Count after
    DB_COUNT_AFTER=$(find "$BACKUP_DIR/db" -name "*.sqlite*" -type f | wc -l)
    CODE_COUNT_AFTER=$(find "$BACKUP_DIR/code" -name "*.bundle" -type f | wc -l)

    DELETED_DB=$((DB_COUNT_BEFORE - DB_COUNT_AFTER))
    DELETED_CODE=$((CODE_COUNT_BEFORE - CODE_COUNT_AFTER))

    log "Deleted $DELETED_DB old database backups, $DELETED_CODE old code backups"
}

# =============================================================================
# Main Execution
# =============================================================================

# Run backups based on arguments
if [ "$CODE_ONLY" = false ]; then
    backup_database
fi

if [ "$DB_ONLY" = false ]; then
    backup_code
    backup_config
fi

cleanup_old_backups

# =============================================================================
# Summary
# =============================================================================
log "=========================================="
log "Backup Summary"
log "=========================================="
log "Status: $BACKUP_STATUS"
echo -e "$BACKUP_RESULTS" | tee -a "$LOG_FILE"

# Write to CSV log
CSV_LOG="$BACKUP_DIR/backup_history.csv"
if [ ! -f "$CSV_LOG" ]; then
    echo "date,time,type,status,db_size,code_size,notes" > "$CSV_LOG"
fi

DB_SIZE=$(ls -lh "$BACKUP_DIR/db/psx_$DATE.sqlite"* 2>/dev/null | awk '{print $5}' | head -1 || echo "N/A")
CODE_SIZE=$(ls -lh "$BACKUP_DIR/code/psx_code_$DATE.bundle" 2>/dev/null | awk '{print $5}' || echo "N/A")
echo "$(date +%Y-%m-%d),$(date +%H:%M:%S),full,$BACKUP_STATUS,$DB_SIZE,$CODE_SIZE," >> "$CSV_LOG"

log "=========================================="
log "Backup Complete"
log "=========================================="

# Exit with appropriate code
[ "$BACKUP_STATUS" = "SUCCESS" ] && exit 0 || exit 1
