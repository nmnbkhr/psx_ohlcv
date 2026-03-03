# PSX OHLCV Operations Runbook

## Table of Contents
1. [System Overview](#system-overview)
2. [Common Issues & Resolutions](#common-issues--resolutions)
3. [Backup Procedures](#backup-procedures)
4. [Restoration Procedures](#restoration-procedures)
5. [Maintenance Tasks](#maintenance-tasks)
6. [Emergency Procedures](#emergency-procedures)

---

## System Overview

### Architecture
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Frontend      │────▶│   Backend       │────▶│   Database      │
│   (Streamlit)   │     │   (Python)      │     │   (SQLite)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        │                       │                       │
        ▼                       ▼                       ▼
   Port 8501              CLI: pfsync           /mnt/e/psxdata/
                                                  psx.sqlite
```

### Key Paths
| Component | Path |
|-----------|------|
| Database | `/mnt/e/psxdata/psx.sqlite` |
| Logs | `/mnt/e/psxdata/logs/` |
| Services | `/mnt/e/psxdata/services/` |
| Code | `/home/adnoman/pakfindata/` |
| Backups | `/mnt/e/psxdata/backups/` |

### Services
| Service | Status File | Log File |
|---------|-------------|----------|
| Intraday Sync | `services/intraday_status.json` | `services/intraday.log` |
| EOD Sync | `services/eod_sync_status.json` | `services/eod_sync.log` |
| FI Sync | `services/fi_sync_status.json` | `services/fi_sync.log` |
| Announcements | `services/announcements_status.json` | `services/announcements.log` |

---

## Common Issues & Resolutions

### ISSUE-001: Database Disk Image Malformed

**Symptoms:**
- Error: "database disk image is malformed"
- UI shows error on data pages
- CLI commands fail with SQLite errors

**Severity:** CRITICAL

**Root Cause:**
- Incomplete writes during power loss/crash
- Concurrent write conflicts
- Corrupted WAL files

**Resolution Steps:**
```bash
# 1. Stop all services
pkill -f "pfsync"
pkill -f "streamlit"

# 2. Navigate to database directory
cd /mnt/e/psxdata

# 3. Check integrity
sqlite3 psx.sqlite "PRAGMA integrity_check" | head -20

# 4. If corrupted, backup corrupted file
cp psx.sqlite psx.sqlite.corrupted.$(date +%Y%m%d_%H%M%S)

# 5. Remove WAL files (often source of corruption)
rm -f psx.sqlite-shm psx.sqlite-wal

# 6. Try recovery using .recover command
sqlite3 psx.sqlite ".recover" | sqlite3 psx_recovered.sqlite

# 7. Verify recovered database
sqlite3 psx_recovered.sqlite "PRAGMA integrity_check"
sqlite3 psx_recovered.sqlite "SELECT COUNT(*) FROM eod_ohlcv"

# 8. Replace corrupted with recovered
mv psx.sqlite psx.sqlite.bad
mv psx_recovered.sqlite psx.sqlite

# 9. Verify and restart services
sqlite3 psx.sqlite "PRAGMA integrity_check"
```

**Prevention:**
- Enable automatic backups (see Backup Procedures)
- Don't force-kill sync processes
- Use proper shutdown procedures

---

### ISSUE-002: Dashboard Shows Stale Data

**Symptoms:**
- Dashboard shows "1 day old" or older data
- Market data not updating
- Fresh badge shows orange/red

**Severity:** MEDIUM

**Root Cause:**
- EOD sync not run
- Intraday service stopped
- Data freshness check looking at wrong table

**Resolution Steps:**
```bash
# 1. Check what data is available
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT 'eod_ohlcv', MAX(date) FROM eod_ohlcv
UNION ALL
SELECT 'regular_market_current', MAX(DATE(ts)) FROM regular_market_current
UNION ALL
SELECT 'psx_indices', MAX(index_date) FROM psx_indices
"

# 2. If regular_market_current is current, refresh UI
# The issue may be caching - clear browser cache or restart Streamlit

# 3. If EOD data is stale, run sync
pfsync sync --incremental

# 4. For index data
pfsync market-summary sync

# 5. Restart UI if needed
pkill -f streamlit
cd /home/adnoman/pakfindata
streamlit run src/pakfindata/ui/app.py &
```

---

### ISSUE-003: Service Won't Start

**Symptoms:**
- Service shows as "already running"
- PID file exists but process dead
- Start button doesn't work

**Severity:** LOW

**Resolution Steps:**
```bash
# 1. Check if process actually running
ps aux | grep -E "pfsync|intraday|eod_sync"

# 2. Remove stale PID files
rm -f /mnt/e/psxdata/services/*.pid

# 3. Reset status files
for f in /mnt/e/psxdata/services/*_status.json; do
    echo '{"running": false}' > "$f"
done

# 4. Try starting again via CLI
pfsync fixed-income service start
```

---

### ISSUE-004: UI Not Loading / Streamlit Error

**Symptoms:**
- Blank page
- Streamlit error messages
- Import errors

**Severity:** MEDIUM

**Resolution Steps:**
```bash
# 1. Check if Streamlit running
ps aux | grep streamlit

# 2. Check logs
tail -50 ~/.streamlit/logs/*.log 2>/dev/null

# 3. Verify dependencies
cd /home/adnoman/pakfindata
pip install -e ".[ui]"

# 4. Check for syntax errors
python -c "from pakfindata.ui.app import main; print('OK')"

# 5. Restart with verbose output
streamlit run src/pakfindata/ui/app.py --logger.level=debug
```

---

### ISSUE-005: API Rate Limiting / Fetch Errors

**Symptoms:**
- "Too many requests" errors
- Partial data synced
- Connection timeouts

**Severity:** LOW

**Resolution Steps:**
```bash
# 1. Check recent sync errors
pfsync status

# 2. Wait 5-10 minutes before retrying

# 3. Run with increased delays
pfsync sync --delay 3

# 4. For specific symbols only
pfsync sync --symbols "OGDC,PPL,HBL"
```

---

### ISSUE-006: Missing Tables After Update

**Symptoms:**
- "no such table" errors
- New features not working

**Severity:** MEDIUM

**Resolution Steps:**
```bash
# 1. Reinitialize schema
python -c "
from pakfindata.db import connect, init_schema
con = connect('/mnt/e/psxdata/psx.sqlite')
init_schema(con)
print('Schema initialized')
con.close()
"

# 2. Verify tables exist
sqlite3 /mnt/e/psxdata/psx.sqlite ".tables"
```

---

### ISSUE-007: Git Pull Conflicts

**Symptoms:**
- Cannot pull latest code
- Merge conflicts

**Severity:** LOW

**Resolution Steps:**
```bash
cd /home/adnoman/pakfindata

# 1. Stash local changes
git stash

# 2. Pull latest
git pull origin enhanced-other-funds

# 3. Restore local changes if needed
git stash pop

# 4. If conflicts, reset to remote
git fetch origin
git reset --hard origin/enhanced-other-funds
```

---

### ISSUE-008: High Memory Usage

**Symptoms:**
- System slow
- Out of memory errors
- Streamlit crashes

**Severity:** MEDIUM

**Resolution Steps:**
```bash
# 1. Check memory usage
free -h
ps aux --sort=-%mem | head -10

# 2. Restart Streamlit (clears cache)
pkill -f streamlit
streamlit run src/pakfindata/ui/app.py &

# 3. Clear Streamlit cache
rm -rf ~/.streamlit/cache/*

# 4. Reduce data limits in queries
# Edit UI to use smaller LIMIT values
```

---

## Backup Procedures

### Automated Daily Backup Script

Create `/home/adnoman/pakfindata/scripts/backup.sh`:
```bash
#!/bin/bash
# PSX OHLCV Backup Script
# Run daily via cron: 0 2 * * * /home/adnoman/pakfindata/scripts/backup.sh

set -e

# Configuration
DB_PATH="/mnt/e/psxdata/psx.sqlite"
BACKUP_DIR="/mnt/e/psxdata/backups"
CODE_DIR="/home/adnoman/pakfindata"
RETENTION_DAYS=30
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR/db"
mkdir -p "$BACKUP_DIR/code"

echo "=== PSX OHLCV Backup - $DATE ==="

# 1. Database Backup
echo "Backing up database..."
if [ -f "$DB_PATH" ]; then
    # Use SQLite backup command for consistency
    sqlite3 "$DB_PATH" ".backup '$BACKUP_DIR/db/psx_$DATE.sqlite'"

    # Compress
    gzip "$BACKUP_DIR/db/psx_$DATE.sqlite"

    echo "Database backup: $BACKUP_DIR/db/psx_$DATE.sqlite.gz"
else
    echo "ERROR: Database not found at $DB_PATH"
fi

# 2. Code Backup
echo "Backing up code..."
cd "$CODE_DIR"
git bundle create "$BACKUP_DIR/code/psx_code_$DATE.bundle" --all
echo "Code backup: $BACKUP_DIR/code/psx_code_$DATE.bundle"

# 3. Config Backup
echo "Backing up configs..."
tar -czf "$BACKUP_DIR/config_$DATE.tar.gz" \
    "$CODE_DIR/pyproject.toml" \
    "$CODE_DIR/.env" 2>/dev/null || true

# 4. Cleanup old backups
echo "Cleaning old backups (older than $RETENTION_DAYS days)..."
find "$BACKUP_DIR" -name "*.sqlite.gz" -mtime +$RETENTION_DAYS -delete
find "$BACKUP_DIR" -name "*.bundle" -mtime +$RETENTION_DAYS -delete
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +$RETENTION_DAYS -delete

# 5. Verify backup
echo "Verifying backup..."
BACKUP_SIZE=$(ls -lh "$BACKUP_DIR/db/psx_$DATE.sqlite.gz" 2>/dev/null | awk '{print $5}')
echo "Backup size: $BACKUP_SIZE"

# 6. Log backup
echo "$DATE,database,$BACKUP_DIR/db/psx_$DATE.sqlite.gz,$BACKUP_SIZE,SUCCESS" >> "$BACKUP_DIR/backup_log.csv"

echo "=== Backup Complete ==="
```

### Manual Backup Commands

```bash
# Quick database backup
sqlite3 /mnt/e/psxdata/psx.sqlite ".backup '/mnt/e/psxdata/backups/manual_backup.sqlite'"

# Schema-only backup
sqlite3 /mnt/e/psxdata/psx.sqlite ".schema" > /mnt/e/psxdata/backups/schema.sql

# Full dump (schema + data)
sqlite3 /mnt/e/psxdata/psx.sqlite ".dump" > /mnt/e/psxdata/backups/full_dump.sql

# Specific table backup
sqlite3 /mnt/e/psxdata/psx.sqlite ".dump eod_ohlcv" > /mnt/e/psxdata/backups/eod_ohlcv.sql

# Code backup
cd /home/adnoman/pakfindata
git bundle create ~/psx_backup.bundle --all
```

### Backup Schedule

| Backup Type | Frequency | Retention | Location |
|-------------|-----------|-----------|----------|
| Database (full) | Daily 2AM | 30 days | `/mnt/e/psxdata/backups/db/` |
| Database (incremental) | Hourly | 24 hours | WAL files |
| Code | Daily | 30 days | `/mnt/e/psxdata/backups/code/` |
| Config | Weekly | 90 days | `/mnt/e/psxdata/backups/` |

---

## Restoration Procedures

### Database Restoration

#### From SQLite Backup File
```bash
# 1. Stop all services
pkill -f "pfsync"
pkill -f "streamlit"

# 2. Backup current (even if corrupted)
mv /mnt/e/psxdata/psx.sqlite /mnt/e/psxdata/psx.sqlite.old

# 3. Decompress backup if needed
gunzip /mnt/e/psxdata/backups/db/psx_YYYYMMDD.sqlite.gz

# 4. Copy backup to production
cp /mnt/e/psxdata/backups/db/psx_YYYYMMDD.sqlite /mnt/e/psxdata/psx.sqlite

# 5. Verify integrity
sqlite3 /mnt/e/psxdata/psx.sqlite "PRAGMA integrity_check"

# 6. Check data
sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM eod_ohlcv"

# 7. Restart services
streamlit run /home/adnoman/pakfindata/src/pakfindata/ui/app.py &
```

#### From SQL Dump
```bash
# 1. Stop services
pkill -f "pfsync"

# 2. Create new database from dump
sqlite3 /mnt/e/psxdata/psx_new.sqlite < /mnt/e/psxdata/backups/full_dump.sql

# 3. Verify
sqlite3 /mnt/e/psxdata/psx_new.sqlite "PRAGMA integrity_check"

# 4. Replace production
mv /mnt/e/psxdata/psx.sqlite /mnt/e/psxdata/psx.sqlite.old
mv /mnt/e/psxdata/psx_new.sqlite /mnt/e/psxdata/psx.sqlite
```

#### From Corrupted Database (.recover)
```bash
# Use SQLite's recovery feature
sqlite3 /mnt/e/psxdata/psx.sqlite.corrupted ".recover" | \
    sqlite3 /mnt/e/psxdata/psx_recovered.sqlite

# Verify and replace
sqlite3 /mnt/e/psxdata/psx_recovered.sqlite "PRAGMA integrity_check"
mv /mnt/e/psxdata/psx.sqlite /mnt/e/psxdata/psx.sqlite.bad
mv /mnt/e/psxdata/psx_recovered.sqlite /mnt/e/psxdata/psx.sqlite
```

### Code Restoration

#### From Git Bundle
```bash
# 1. Backup current code
mv /home/adnoman/pakfindata /home/adnoman/pakfindata.old

# 2. Clone from bundle
git clone /mnt/e/psxdata/backups/code/psx_code_YYYYMMDD.bundle /home/adnoman/pakfindata

# 3. Install dependencies
cd /home/adnoman/pakfindata
pip install -e ".[dev,ui]"
```

#### From Git Remote
```bash
# 1. Clone fresh
git clone https://github.com/nmnbkhr/pakfindata.git /home/adnoman/pakfindata_new
cd /home/adnoman/pakfindata_new
git checkout enhanced-other-funds

# 2. Install
pip install -e ".[dev,ui]"

# 3. Replace
mv /home/adnoman/pakfindata /home/adnoman/pakfindata.old
mv /home/adnoman/pakfindata_new /home/adnoman/pakfindata
```

---

## Maintenance Tasks

### Daily Checks
```bash
# Run this daily to check system health
echo "=== Daily Health Check ==="

# Database integrity
sqlite3 /mnt/e/psxdata/psx.sqlite "PRAGMA integrity_check"

# Data freshness
sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT MAX(date) FROM eod_ohlcv"

# Disk space
df -h /mnt/e/psxdata

# Service status
cat /mnt/e/psxdata/services/*_status.json 2>/dev/null | grep -E '"running"|"last_sync"'
```

### Weekly Maintenance
```bash
# 1. Vacuum database (reclaim space)
sqlite3 /mnt/e/psxdata/psx.sqlite "VACUUM"

# 2. Analyze for query optimization
sqlite3 /mnt/e/psxdata/psx.sqlite "ANALYZE"

# 3. Check backup integrity
gunzip -t /mnt/e/psxdata/backups/db/*.gz 2>&1 | grep -v "OK"

# 4. Update code
cd /home/adnoman/pakfindata
git pull origin enhanced-other-funds
pip install -e ".[dev,ui]"
```

### Monthly Tasks
```bash
# 1. Full backup verification
# Restore a backup to temp location and verify

# 2. Clean old data (optional)
sqlite3 /mnt/e/psxdata/psx.sqlite "
DELETE FROM intraday_bars WHERE bar_time < date('now', '-90 days');
DELETE FROM regular_market_snapshots WHERE scraped_at < date('now', '-30 days');
VACUUM;
"

# 3. Review logs for patterns
grep -i error /mnt/e/psxdata/services/*.log | tail -100
```

---

## Emergency Procedures

### Complete System Recovery

If everything is broken:

```bash
# 1. Stop everything
pkill -f python
pkill -f streamlit

# 2. Find latest good backup
ls -lt /mnt/e/psxdata/backups/db/ | head -5

# 3. Restore database
gunzip -k /mnt/e/psxdata/backups/db/psx_LATEST.sqlite.gz
mv /mnt/e/psxdata/psx.sqlite /mnt/e/psxdata/psx.sqlite.emergency
cp /mnt/e/psxdata/backups/db/psx_LATEST.sqlite /mnt/e/psxdata/psx.sqlite

# 4. Restore code
cd /home/adnoman
rm -rf pakfindata
git clone https://github.com/nmnbkhr/pakfindata.git
cd pakfindata
git checkout enhanced-other-funds
pip install -e ".[dev,ui]"

# 5. Reinitialize
python -c "from pakfindata.db import connect, init_schema; c=connect(); init_schema(c)"

# 6. Start UI
streamlit run src/pakfindata/ui/app.py &

# 7. Verify
curl http://localhost:8501 2>/dev/null && echo "UI OK" || echo "UI FAILED"
```

### Contact Information

| Role | Contact | When to Contact |
|------|---------|-----------------|
| Developer | Check GitHub Issues | Code bugs, feature requests |
| System Admin | - | Server/infrastructure issues |

---

## Appendix

### Quick Reference Commands

```bash
# Start UI
streamlit run /home/adnoman/pakfindata/src/pakfindata/ui/app.py

# Sync all EOD data
pfsync sync --incremental

# Check service status
pfsync fixed-income service status

# Quick backup
sqlite3 /mnt/e/psxdata/psx.sqlite ".backup '/mnt/e/psxdata/quick_backup.sqlite'"

# Check database size
ls -lh /mnt/e/psxdata/psx.sqlite

# View recent errors
tail -50 /mnt/e/psxdata/services/*.log | grep -i error
```

### Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.2.0 | 2026-01-30 | Added Fixed Income, SBP integration |
| 0.1.0 | Initial | Base system |

---

*Last Updated: 2026-01-30*
*Document Version: 1.0*
