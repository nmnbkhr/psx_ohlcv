#!/bin/bash
# =============================================================================
# PSX OHLCV Restore Script
# =============================================================================
#
# Usage:
#   ./restore.sh --list                    # List available backups
#   ./restore.sh --db BACKUP_FILE          # Restore specific database backup
#   ./restore.sh --db latest               # Restore latest database backup
#   ./restore.sh --code BUNDLE_FILE        # Restore code from bundle
#   ./restore.sh --recover                 # Recover corrupted database
#
# =============================================================================

set -e

# Configuration
DB_PATH="/mnt/e/psxdata/psx.sqlite"
BACKUP_DIR="/mnt/e/psxdata/backups"
CODE_DIR="/home/adnoman/psx_ohlcv"
LOG_DIR="/mnt/e/psxdata/logs"
DATE=$(date +%Y%m%d_%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# =============================================================================
# List Available Backups
# =============================================================================
list_backups() {
    echo "=========================================="
    echo "Available Database Backups"
    echo "=========================================="
    ls -lht "$BACKUP_DIR/db/"*.sqlite* 2>/dev/null | head -20 || echo "No database backups found"

    echo ""
    echo "=========================================="
    echo "Available Code Backups"
    echo "=========================================="
    ls -lht "$BACKUP_DIR/code/"*.bundle 2>/dev/null | head -10 || echo "No code backups found"

    echo ""
    echo "=========================================="
    echo "Backup Statistics"
    echo "=========================================="
    echo "Database backups: $(find "$BACKUP_DIR/db" -name "*.sqlite*" 2>/dev/null | wc -l)"
    echo "Code backups: $(find "$BACKUP_DIR/code" -name "*.bundle" 2>/dev/null | wc -l)"
    echo "Total backup size: $(du -sh "$BACKUP_DIR" 2>/dev/null | awk '{print $1}')"
}

# =============================================================================
# Restore Database
# =============================================================================
restore_database() {
    BACKUP_FILE="$1"

    # Handle 'latest' keyword
    if [ "$BACKUP_FILE" = "latest" ]; then
        BACKUP_FILE=$(ls -t "$BACKUP_DIR/db/"*.sqlite* 2>/dev/null | head -1)
        if [ -z "$BACKUP_FILE" ]; then
            error "No database backups found"
            exit 1
        fi
        log "Using latest backup: $BACKUP_FILE"
    fi

    # Validate backup file exists
    if [ ! -f "$BACKUP_FILE" ]; then
        error "Backup file not found: $BACKUP_FILE"
        exit 1
    fi

    echo "=========================================="
    echo "Database Restoration"
    echo "=========================================="
    log "Backup file: $BACKUP_FILE"

    # Confirm with user
    read -p "This will replace the current database. Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Restoration cancelled"
        exit 0
    fi

    # Stop services
    log "Stopping services..."
    pkill -f "psxsync" 2>/dev/null || true
    pkill -f "streamlit" 2>/dev/null || true
    sleep 2

    # Backup current database
    if [ -f "$DB_PATH" ]; then
        log "Backing up current database..."
        mv "$DB_PATH" "$DB_PATH.pre_restore.$DATE"
        rm -f "$DB_PATH-shm" "$DB_PATH-wal" 2>/dev/null || true
    fi

    # Decompress if needed
    RESTORE_FILE="$BACKUP_FILE"
    if [[ "$BACKUP_FILE" == *.gz ]]; then
        log "Decompressing backup..."
        RESTORE_FILE="${BACKUP_FILE%.gz}"
        gunzip -k "$BACKUP_FILE"
    fi

    # Copy to production
    log "Copying backup to production..."
    cp "$RESTORE_FILE" "$DB_PATH"

    # Clean up temp file if we decompressed
    if [[ "$BACKUP_FILE" == *.gz ]]; then
        rm -f "$RESTORE_FILE"
    fi

    # Verify integrity
    log "Verifying database integrity..."
    INTEGRITY=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check" 2>&1 | head -1)
    if [ "$INTEGRITY" = "ok" ]; then
        log "Integrity check: PASSED"
    else
        error "Integrity check: FAILED - $INTEGRITY"
        warn "Database may be corrupted, consider using --recover"
    fi

    # Show stats
    log "Database statistics:"
    sqlite3 "$DB_PATH" "
        SELECT 'Tables:', COUNT(*) FROM sqlite_master WHERE type='table'
        UNION ALL
        SELECT 'eod_ohlcv rows:', COUNT(*) FROM eod_ohlcv
        UNION ALL
        SELECT 'Latest date:', MAX(date) FROM eod_ohlcv
    " 2>/dev/null || true

    log "Database restoration complete!"
    log "You can now restart services"
}

# =============================================================================
# Restore Code
# =============================================================================
restore_code() {
    BUNDLE_FILE="$1"

    if [ ! -f "$BUNDLE_FILE" ]; then
        error "Bundle file not found: $BUNDLE_FILE"
        exit 1
    fi

    echo "=========================================="
    echo "Code Restoration"
    echo "=========================================="
    log "Bundle file: $BUNDLE_FILE"

    # Verify bundle
    log "Verifying bundle..."
    if ! git bundle verify "$BUNDLE_FILE" > /dev/null 2>&1; then
        error "Bundle verification failed"
        exit 1
    fi

    # Confirm with user
    read -p "This will replace the current code directory. Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Restoration cancelled"
        exit 0
    fi

    # Backup current code
    if [ -d "$CODE_DIR" ]; then
        log "Backing up current code..."
        mv "$CODE_DIR" "${CODE_DIR}.pre_restore.$DATE"
    fi

    # Clone from bundle
    log "Cloning from bundle..."
    git clone "$BUNDLE_FILE" "$CODE_DIR"

    # Checkout main branch
    cd "$CODE_DIR"
    git checkout enhanced-other-funds 2>/dev/null || git checkout main 2>/dev/null || true

    # Install dependencies
    log "Installing dependencies..."
    pip install -e ".[dev,ui]" 2>&1 | tail -5

    log "Code restoration complete!"
}

# =============================================================================
# Recover Corrupted Database
# =============================================================================
recover_database() {
    echo "=========================================="
    echo "Database Recovery"
    echo "=========================================="

    if [ ! -f "$DB_PATH" ]; then
        error "Database not found at $DB_PATH"
        exit 1
    fi

    log "Checking current database integrity..."
    INTEGRITY=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check" 2>&1 | head -1)

    if [ "$INTEGRITY" = "ok" ]; then
        log "Database integrity is OK. No recovery needed."
        exit 0
    fi

    error "Database is corrupted: $INTEGRITY"
    log "Attempting recovery..."

    # Stop services
    log "Stopping services..."
    pkill -f "psxsync" 2>/dev/null || true
    pkill -f "streamlit" 2>/dev/null || true
    sleep 2

    # Backup corrupted file
    log "Backing up corrupted database..."
    cp "$DB_PATH" "$DB_PATH.corrupted.$DATE"

    # Remove WAL files
    rm -f "$DB_PATH-shm" "$DB_PATH-wal" 2>/dev/null || true

    # Try .recover command
    RECOVERED_DB="$DB_PATH.recovered.$DATE"
    log "Running SQLite .recover command..."
    sqlite3 "$DB_PATH" ".recover" 2>/dev/null | sqlite3 "$RECOVERED_DB" 2>&1

    if [ ! -f "$RECOVERED_DB" ] || [ ! -s "$RECOVERED_DB" ]; then
        error "Recovery failed - no output file created"

        # Try alternative: dump what we can
        log "Trying .dump method..."
        sqlite3 "$DB_PATH" ".dump" 2>/dev/null > "$DB_PATH.dump.$DATE.sql" || true

        if [ -s "$DB_PATH.dump.$DATE.sql" ]; then
            log "Dump created, importing..."
            sqlite3 "$RECOVERED_DB" < "$DB_PATH.dump.$DATE.sql" 2>&1 | head -10
        fi
    fi

    # Verify recovered database
    if [ -f "$RECOVERED_DB" ]; then
        INTEGRITY=$(sqlite3 "$RECOVERED_DB" "PRAGMA integrity_check" 2>&1 | head -1)
        if [ "$INTEGRITY" = "ok" ]; then
            log "Recovery successful! Integrity check: PASSED"

            # Replace corrupted with recovered
            mv "$DB_PATH" "$DB_PATH.bad.$DATE"
            mv "$RECOVERED_DB" "$DB_PATH"

            # Show what we recovered
            log "Recovered data:"
            sqlite3 "$DB_PATH" "
                SELECT 'Tables:', COUNT(*) FROM sqlite_master WHERE type='table'
                UNION ALL
                SELECT 'eod_ohlcv rows:', COUNT(*) FROM eod_ohlcv
            " 2>/dev/null || true

            log "Recovery complete!"
        else
            error "Recovered database also has issues: $INTEGRITY"
            log "Manual intervention may be required"
            exit 1
        fi
    else
        error "All recovery methods failed"
        log "Consider restoring from backup: ./restore.sh --db latest"
        exit 1
    fi
}

# =============================================================================
# Main
# =============================================================================

case "$1" in
    --list|-l)
        list_backups
        ;;
    --db)
        if [ -z "$2" ]; then
            error "Please specify backup file or 'latest'"
            echo "Usage: $0 --db BACKUP_FILE"
            echo "       $0 --db latest"
            exit 1
        fi
        restore_database "$2"
        ;;
    --code)
        if [ -z "$2" ]; then
            error "Please specify bundle file"
            echo "Usage: $0 --code BUNDLE_FILE"
            exit 1
        fi
        restore_code "$2"
        ;;
    --recover|-r)
        recover_database
        ;;
    --help|-h|*)
        echo "PSX OHLCV Restore Script"
        echo ""
        echo "Usage:"
        echo "  $0 --list                 List available backups"
        echo "  $0 --db BACKUP_FILE       Restore specific database backup"
        echo "  $0 --db latest            Restore latest database backup"
        echo "  $0 --code BUNDLE_FILE     Restore code from git bundle"
        echo "  $0 --recover              Recover corrupted database"
        echo "  $0 --help                 Show this help"
        ;;
esac
