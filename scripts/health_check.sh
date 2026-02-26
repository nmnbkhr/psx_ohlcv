#!/bin/bash
# =============================================================================
# PSX OHLCV Health Check Script
# =============================================================================
#
# Usage:
#   ./health_check.sh           # Full health check
#   ./health_check.sh --quick   # Quick check only
#   ./health_check.sh --json    # Output as JSON
#
# =============================================================================

set -e

# Configuration
DB_PATH="/mnt/e/psxdata/psx.sqlite"
BACKUP_DIR="/mnt/e/psxdata/backups"
CODE_DIR="/home/adnoman/pakfindata"
SERVICE_DIR="/mnt/e/psxdata/services"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Parse arguments
QUICK=false
JSON=false
for arg in "$@"; do
    case $arg in
        --quick) QUICK=true ;;
        --json)  JSON=true ;;
    esac
done

# Results tracking
CHECKS_PASSED=0
CHECKS_FAILED=0
CHECKS_WARN=0

check_pass() {
    ((CHECKS_PASSED++))
    [ "$JSON" = false ] && echo -e "${GREEN}[PASS]${NC} $1"
}

check_fail() {
    ((CHECKS_FAILED++))
    [ "$JSON" = false ] && echo -e "${RED}[FAIL]${NC} $1"
}

check_warn() {
    ((CHECKS_WARN++))
    [ "$JSON" = false ] && echo -e "${YELLOW}[WARN]${NC} $1"
}

section() {
    [ "$JSON" = false ] && echo -e "\n${BLUE}=== $1 ===${NC}"
}

# =============================================================================
# Checks
# =============================================================================

section "Database Health"

# Check database exists
if [ -f "$DB_PATH" ]; then
    check_pass "Database file exists"
    DB_SIZE=$(ls -lh "$DB_PATH" | awk '{print $5}')
    [ "$JSON" = false ] && echo "       Size: $DB_SIZE"
else
    check_fail "Database file not found at $DB_PATH"
fi

# Check database integrity
if [ -f "$DB_PATH" ]; then
    INTEGRITY=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check" 2>&1 | head -1)
    if [ "$INTEGRITY" = "ok" ]; then
        check_pass "Database integrity check"
    else
        check_fail "Database integrity check: $INTEGRITY"
    fi
fi

# Check WAL files
if [ -f "$DB_PATH-wal" ]; then
    WAL_SIZE=$(ls -lh "$DB_PATH-wal" | awk '{print $5}')
    if [[ "$WAL_SIZE" == *"M"* ]] || [[ "$WAL_SIZE" == *"G"* ]]; then
        check_warn "Large WAL file: $WAL_SIZE (consider checkpointing)"
    else
        check_pass "WAL file size OK: $WAL_SIZE"
    fi
fi

# =============================================================================
section "Data Freshness"

if [ -f "$DB_PATH" ]; then
    # Check EOD data
    EOD_DATE=$(sqlite3 "$DB_PATH" "SELECT MAX(date) FROM eod_ohlcv" 2>/dev/null || echo "N/A")
    EOD_ROWS=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM eod_ohlcv" 2>/dev/null || echo "0")

    TODAY=$(date +%Y-%m-%d)
    YESTERDAY=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d 2>/dev/null)

    if [ "$EOD_DATE" = "$TODAY" ]; then
        check_pass "EOD data is current: $EOD_DATE ($EOD_ROWS rows)"
    elif [ "$EOD_DATE" = "$YESTERDAY" ]; then
        check_warn "EOD data is 1 day old: $EOD_DATE"
    else
        check_fail "EOD data is stale: $EOD_DATE"
    fi

    # Check live market data
    LIVE_DATE=$(sqlite3 "$DB_PATH" "SELECT MAX(DATE(ts)) FROM regular_market_current" 2>/dev/null || echo "N/A")
    if [ "$LIVE_DATE" = "$TODAY" ]; then
        check_pass "Live market data is current: $LIVE_DATE"
    else
        check_warn "Live market data: $LIVE_DATE"
    fi

    # Check indices
    INDEX_DATE=$(sqlite3 "$DB_PATH" "SELECT MAX(index_date) FROM psx_indices" 2>/dev/null || echo "N/A")
    [ "$JSON" = false ] && echo "       Index data: $INDEX_DATE"
fi

# =============================================================================
section "Services Status"

# Check Streamlit
if pgrep -f "streamlit" > /dev/null; then
    check_pass "Streamlit is running"
else
    check_warn "Streamlit is not running"
fi

# Check service status files
for service in intraday eod_sync fi_sync announcements; do
    STATUS_FILE="$SERVICE_DIR/${service}_status.json"
    if [ -f "$STATUS_FILE" ]; then
        RUNNING=$(grep -o '"running": *[^,}]*' "$STATUS_FILE" 2>/dev/null | grep -o 'true\|false' || echo "unknown")
        if [ "$RUNNING" = "true" ]; then
            check_pass "$service service is running"
        else
            [ "$JSON" = false ] && echo "       $service service: stopped"
        fi
    fi
done

# =============================================================================
if [ "$QUICK" = false ]; then

section "Disk Space"

# Check database disk
DB_DISK=$(df -h "$DB_PATH" 2>/dev/null | tail -1 | awk '{print $5}' | tr -d '%')
if [ -n "$DB_DISK" ]; then
    if [ "$DB_DISK" -lt 80 ]; then
        check_pass "Database disk space OK: ${DB_DISK}% used"
    elif [ "$DB_DISK" -lt 90 ]; then
        check_warn "Database disk space: ${DB_DISK}% used"
    else
        check_fail "Database disk space critical: ${DB_DISK}% used"
    fi
fi

# =============================================================================
section "Backup Status"

# Check backup directory
if [ -d "$BACKUP_DIR/db" ]; then
    BACKUP_COUNT=$(find "$BACKUP_DIR/db" -name "*.sqlite*" -type f | wc -l)
    LATEST_BACKUP=$(ls -t "$BACKUP_DIR/db/"*.sqlite* 2>/dev/null | head -1)

    if [ "$BACKUP_COUNT" -gt 0 ]; then
        check_pass "$BACKUP_COUNT database backups available"
        if [ -n "$LATEST_BACKUP" ]; then
            BACKUP_AGE=$(( ($(date +%s) - $(stat -c %Y "$LATEST_BACKUP" 2>/dev/null || stat -f %m "$LATEST_BACKUP" 2>/dev/null)) / 86400 ))
            if [ "$BACKUP_AGE" -lt 2 ]; then
                check_pass "Latest backup is recent (${BACKUP_AGE}d old)"
            else
                check_warn "Latest backup is ${BACKUP_AGE} days old"
            fi
        fi
    else
        check_fail "No database backups found"
    fi
else
    check_warn "Backup directory not found"
fi

# =============================================================================
section "Code Status"

if [ -d "$CODE_DIR/.git" ]; then
    cd "$CODE_DIR"

    # Check git status
    GIT_STATUS=$(git status --porcelain 2>/dev/null | wc -l)
    if [ "$GIT_STATUS" -eq 0 ]; then
        check_pass "Code is clean (no uncommitted changes)"
    else
        check_warn "$GIT_STATUS uncommitted changes"
    fi

    # Check branch
    BRANCH=$(git branch --show-current 2>/dev/null)
    [ "$JSON" = false ] && echo "       Branch: $BRANCH"

    # Check if up to date with remote
    git fetch origin 2>/dev/null || true
    BEHIND=$(git rev-list HEAD..origin/$BRANCH --count 2>/dev/null || echo "0")
    if [ "$BEHIND" -gt 0 ]; then
        check_warn "Code is $BEHIND commits behind remote"
    fi
fi

fi # End of non-quick checks

# =============================================================================
section "Summary"

TOTAL=$((CHECKS_PASSED + CHECKS_FAILED + CHECKS_WARN))

if [ "$JSON" = true ]; then
    echo "{"
    echo "  \"timestamp\": \"$(date -Iseconds)\","
    echo "  \"checks_passed\": $CHECKS_PASSED,"
    echo "  \"checks_failed\": $CHECKS_FAILED,"
    echo "  \"checks_warned\": $CHECKS_WARN,"
    echo "  \"status\": \"$([ $CHECKS_FAILED -eq 0 ] && echo 'HEALTHY' || echo 'UNHEALTHY')\""
    echo "}"
else
    echo -e "\n${GREEN}Passed: $CHECKS_PASSED${NC} | ${RED}Failed: $CHECKS_FAILED${NC} | ${YELLOW}Warnings: $CHECKS_WARN${NC}"

    if [ $CHECKS_FAILED -eq 0 ]; then
        echo -e "\n${GREEN}System Status: HEALTHY${NC}"
    else
        echo -e "\n${RED}System Status: UNHEALTHY - Check failures above${NC}"
    fi
fi

# Exit with appropriate code
[ $CHECKS_FAILED -eq 0 ] && exit 0 || exit 1
