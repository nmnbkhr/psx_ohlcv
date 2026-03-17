# PSX Tick Collector — Oracle Cloud Free Tier Deployment Guide

## Overview

Deploy tick_service.py on a free Oracle Cloud VM that runs 24/7, collects all 
PSX market data automatically, and syncs back to your local machine on demand.

**Cost: $0 forever** (Always Free tier, not trial)

```
Oracle Cloud VM (free)                    Your Laptop (anywhere)
┌─────────────────────┐                   ┌──────────────────┐
│ tick_service.py      │──collects daily──▶│                  │
│ DPS tick backup      │                   │  rsync on demand │
│ PSXT 1m backup       │                   │  or auto-sync    │
│ WS relay :8765       │                   │                  │
│ tick_bars.db         │◀──SSH tunnel──────│  React frontend  │
│ ~/psxdata/intraday/  │                   │  Streamlit app   │
└─────────────────────┘                   └──────────────────┘
       Always on                            Use from anywhere
```

---

## PART 1: Create Oracle Cloud Account

### Step 1: Sign up

1. Go to: https://www.oracle.com/cloud/free/
2. Click "Start for free"
3. Fill in:
   - Name, email, country (Pakistan)
   - Choose **Home Region**: Pick closest to Pakistan:
     - **UAE East (Abu Dhabi)** ← best latency to PSX
     - Mumbai (India) ← second choice
     - ⚠️ Home region CANNOT be changed later
4. **Credit card**: Required for verification only
   - $0 will be charged for Always Free resources
   - Use a virtual/prepaid card if concerned
   - Tip: Some Pakistani banks work (HBL, Meezan), try a Visa debit
5. Complete verification (phone + email)

### Step 2: Upgrade to Pay-As-You-Go (to get ARM capacity)

⚠️ This sounds scary but is necessary — ARM VMs are rarely available on free-only accounts.

1. Go to: OCI Console → Billing → Upgrade to Pay As You Go
2. Confirm credit card
3. **You will NOT be charged** as long as you only use Always Free resources
4. The upgrade removes capacity restrictions for Always Free shapes

### Step 3: Create the ARM VM

1. Go to: OCI Console → Compute → Instances → Create Instance
2. Configure:

```
Name:           psx-collector
Compartment:    (root)
Availability:   AD-1 (or whichever is available)

Image:          Ubuntu 22.04 (or 24.04)
                ⚠️ Choose "Always Free Eligible" image

Shape:          VM.Standard.A1.Flex (Ampere ARM)
                ⚠️ This is the "Always Free" shape
                OCPU: 2 (of 4 free — keep 2 spare)
                RAM:  12 GB (of 24 free)

Boot volume:    50 GB (of 200 free)

Networking:     Create new VCN + subnet (defaults are fine)
                Assign public IPv4: YES

SSH Key:        Upload your public key (~/.ssh/id_rsa.pub)
                Or generate new key pair and download private key
```

3. Click "Create" — VM provisions in 1-2 minutes

### Step 4: Note your VM's public IP

```
Public IP: xxx.xxx.xxx.xxx  ← save this
Username:  ubuntu
```

### Step 5: Open firewall ports

OCI Console → Networking → Virtual Cloud Networks → your VCN → Security Lists → Default

Add Ingress Rules:

| Source CIDR | Protocol | Port | Description |
|------------|----------|------|-------------|
| 0.0.0.0/0 | TCP | 22 | SSH |
| 0.0.0.0/0 | TCP | 8765 | WS Relay (for React frontend) |

Also open ports in the VM's iptables:
```bash
sudo iptables -I INPUT -p tcp --dport 8765 -j ACCEPT
sudo netfilter-persistent save
```

---

## PART 2: Setup the VM

### Step 1: SSH into the VM

```bash
# From your laptop
ssh ubuntu@xxx.xxx.xxx.xxx
```

### Step 2: Install system dependencies

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv sqlite3 curl git
```

### Step 3: Create project structure

```bash
mkdir -p ~/pakfindata/src/pakfindata/services
mkdir -p ~/psxdata/{intraday,logs}
```

### Step 4: Create Python virtual environment

```bash
cd ~/pakfindata
python3 -m venv .venv
source .venv/bin/activate
pip install websockets requests fastapi uvicorn
```

### Step 5: Copy your code from laptop to VM

From your **laptop** terminal:

```bash
# Copy tick_service.py and ws_relay.py
scp ~/pakfindata/src/pakfindata/services/tick_service.py \
    ubuntu@xxx.xxx.xxx.xxx:~/pakfindata/src/pakfindata/services/

scp ~/pakfindata/src/pakfindata/services/ws_relay.py \
    ubuntu@xxx.xxx.xxx.xxx:~/pakfindata/src/pakfindata/services/

# Copy config if needed
scp ~/pakfindata/src/pakfindata/config.py \
    ubuntu@xxx.xxx.xxx.xxx:~/pakfindata/src/pakfindata/

# Copy __init__.py files
ssh ubuntu@xxx.xxx.xxx.xxx "
  touch ~/pakfindata/src/pakfindata/__init__.py
  touch ~/pakfindata/src/pakfindata/services/__init__.py
"
```

### Step 6: Configure tick_service for cloud paths

SSH into VM and edit the data paths:

```bash
ssh ubuntu@xxx.xxx.xxx.xxx
cd ~/pakfindata

# Check current paths in tick_service.py
grep -n "DATA_ROOT\|SNAPSHOT_PATH\|/mnt/e" src/pakfindata/services/tick_service.py
```

Update paths to use local disk instead of /mnt/e/:

```python
# Change FROM:
DATA_ROOT = Path("/mnt/e/psxdata")

# Change TO:
DATA_ROOT = Path.home() / "psxdata"
```

Either sed it or edit manually:
```bash
sed -i 's|/mnt/e/psxdata|/home/ubuntu/psxdata|g' src/pakfindata/services/tick_service.py
```

### Step 7: Test it works

```bash
cd ~/pakfindata
source .venv/bin/activate
export PYTHONPATH=~/pakfindata/src

# Quick test — connect, print 10 messages, exit
python -m pakfindata.services.tick_service --debug
```

You should see WebSocket connected + raw tick messages.

---

## PART 3: Create the Collector Service

### Step 1: Create the wrapper script

```bash
cat > ~/pakfindata/services/psx_cloud_collector.sh << 'SCRIPT'
#!/bin/bash
# PSX Cloud Collector — runs on Oracle Cloud VM
# Managed by systemd — auto-starts every trading day

set -euo pipefail

PROJ_DIR="$HOME/pakfindata"
PSXDATA="$HOME/psxdata"
INTRADAY="$PSXDATA/intraday"
LOG_DIR="$PSXDATA/logs"
PYTHON="$PROJ_DIR/.venv/bin/python"
DPS_BASE="https://dps.psx.com.pk"
PSXT_BASE="https://psxterminal.com/api"

export PYTHONPATH="$PROJ_DIR/src"

mkdir -p "$LOG_DIR" "$INTRADAY"

DATE=$(TZ="Asia/Karachi" date +%Y-%m-%d)
LOG="$LOG_DIR/collector_${DATE}.log"

log() {
    echo "[$(TZ='Asia/Karachi' date '+%H:%M:%S PKT')] $1" | tee -a "$LOG"
}

# Skip weekends
DOW=$(TZ="Asia/Karachi" date +%u)
if [ "$DOW" -gt 5 ]; then
    log "Weekend — skipping"
    exit 0
fi

log "═══════════════════════════════════════════════"
log "  PSX CLOUD COLLECTOR — $DATE"
log "═══════════════════════════════════════════════"

# ─── PHASE 1: TICK COLLECTOR (market hours) ───────────
log "🚀 Starting tick_service.py..."

cd "$PROJ_DIR"
$PYTHON -m pakfindata.services.tick_service >> "$LOG" 2>&1 &
TICK_PID=$!
log "📡 PID: $TICK_PID"

# Monitor until market closes
while true; do
    H=$(TZ="Asia/Karachi" date +%H)
    M=$(TZ="Asia/Karachi" date +%M)
    
    # Auto-restart if crashed
    if ! kill -0 $TICK_PID 2>/dev/null; then
        log "⚠️ tick_service died — restarting..."
        $PYTHON -m pakfindata.services.tick_service >> "$LOG" 2>&1 &
        TICK_PID=$!
        log "🔄 New PID: $TICK_PID"
    fi
    
    # Exit after 15:40 PKT
    if [ "$H" -ge 16 ] || ([ "$H" -eq 15 ] && [ "$M" -ge 40 ]); then
        break
    fi
    
    sleep 60
done

log "🔔 Market closed. Waiting for EOD flush..."
sleep 30

# Stop tick_service
if kill -0 $TICK_PID 2>/dev/null; then
    kill -SIGTERM $TICK_PID
    wait $TICK_PID 2>/dev/null || true
    log "✅ Tick collector stopped"
fi

# ─── PHASE 2: POST-MARKET BACKUPS ────────────────────
log ""
log "═══ POST-MARKET BACKUPS ═══"

$PYTHON << 'PYEOF'
import requests, csv, time, json
from datetime import datetime, timezone, timedelta
from pathlib import Path

DPS = "https://dps.psx.com.pk"
PSXT = "https://psxterminal.com/api"
PKT = timezone(timedelta(hours=5))
DATE = datetime.now(PKT).strftime("%Y-%m-%d")
OUT = Path.home() / "psxdata" / "intraday"

# Get symbols
try:
    syms = requests.get(f"{PSXT}/symbols", timeout=10).json()["data"]
except:
    syms = []

skip = {"ALLSHR","KSE100","KSE100PR","KSE30","KMI30","KMIALLSHR",
        "BKTI","OGTI","PSXDIV20","UPP9","NITPGI","NBPPGI","MZNPI",
        "JSMFI","ACI","JSGBKTI","HBLTTI","MII30"}
syms = [s for s in syms if s not in skip]
print(f"{len(syms)} symbols")

# ── DPS Ticks ──
print("📥 DPS ticks...")
all_ticks = []
for i, sym in enumerate(syms, 1):
    try:
        r = requests.get(f"{DPS}/timeseries/int/{sym}", timeout=15)
        if r.status_code == 200:
            d = r.json()
            if d.get("status") == 1 and d.get("data"):
                for row in d["data"]:
                    all_ticks.append([sym, row[0], row[1], row[2]])
    except: pass
    if i % 50 == 0: print(f"  [{i}/{len(syms)}] {len(all_ticks):,}")
    time.sleep(0.3)

if all_ticks:
    fp = OUT / f"dps_ticks_{DATE}.csv"
    with open(fp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol","timestamp","price","volume"])
        for row in sorted(all_ticks, key=lambda x: (x[0], x[1])):
            w.writerow(row)
    print(f"✅ dps_ticks_{DATE}.csv: {len(all_ticks):,} ticks")

# ── PSXT 1m klines ──
print("📥 PSXT 1m klines...")
start_ts = int(datetime.now(PKT).replace(hour=9, minute=15, second=0, microsecond=0).timestamp() * 1000)
all_1m = []
for i, sym in enumerate(syms, 1):
    ts = start_ts
    while True:
        try:
            r = requests.get(f"{PSXT}/klines/{sym}/1m?limit=100&startTimestamp={ts}", timeout=10)
            if r.status_code != 200: break
            text = r.text.split("<")[0]
            d = json.loads(text)
            if not d.get("success") or not d.get("data"): break
            all_1m.extend(d["data"])
            if len(d["data"]) < 100: break
            ts = max(b["timestamp"] for b in d["data"]) + 1
            time.sleep(0.2)
        except: break
    if i % 50 == 0: print(f"  [{i}/{len(syms)}] {len(all_1m):,}")
    time.sleep(0.2)

if all_1m:
    fp = OUT / f"psxt_{DATE}_1m.csv"
    with open(fp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol","timestamp","datetime","open","high","low","close","volume"])
        for b in sorted(all_1m, key=lambda x: (x["symbol"], x["timestamp"])):
            dt = datetime.fromtimestamp(b["timestamp"]/1000, PKT).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([b["symbol"], b["timestamp"], dt, b["open"], b["high"], b["low"], b["close"], b["volume"]])
    print(f"✅ psxt_{DATE}_1m.csv: {len(all_1m):,} bars")

print(f"📊 Total: {len(all_ticks):,} ticks + {len(all_1m):,} bars")
PYEOF

log ""
log "✅ All done for $DATE"
log "═══════════════════════════════════════════════"
SCRIPT

chmod +x ~/pakfindata/services/psx_cloud_collector.sh
```

### Step 2: Create systemd service + timer

```bash
# Service
sudo cat > /etc/systemd/system/psx-collector.service << EOF
[Unit]
Description=PSX Market Data Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
ExecStart=/home/ubuntu/pakfindata/services/psx_cloud_collector.sh
WorkingDirectory=/home/ubuntu/pakfindata
Environment=HOME=/home/ubuntu
StandardOutput=append:/home/ubuntu/psxdata/logs/systemd.log
StandardError=append:/home/ubuntu/psxdata/logs/systemd_error.log
Restart=no
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
EOF

# Timer — 9:10 AM PKT = 4:10 AM UTC, Mon-Fri
sudo cat > /etc/systemd/system/psx-collector.timer << EOF
[Unit]
Description=PSX Collector Timer — 9:10 AM PKT Mon-Fri

[Timer]
OnCalendar=Mon..Fri *-*-* 04:10:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
EOF

# Enable
sudo systemctl daemon-reload
sudo systemctl enable psx-collector.timer
sudo systemctl start psx-collector.timer

# Verify
sudo systemctl list-timers | grep psx
```

### Step 3: Set timezone

```bash
sudo timedatectl set-timezone Asia/Karachi
```

---

## PART 4: Data Sync to Your Laptop

### Option A: On-demand sync (manual when you want data)

From your **laptop**:

```bash
# Sync tick_bars.db
rsync -avz ubuntu@xxx.xxx.xxx.xxx:~/psxdata/tick_bars.db /mnt/e/psxdata/

# Sync all intraday CSVs
rsync -avz ubuntu@xxx.xxx.xxx.xxx:~/psxdata/intraday/ /mnt/e/psxdata/intraday/

# Sync everything
rsync -avz ubuntu@xxx.xxx.xxx.xxx:~/psxdata/ /mnt/e/psxdata/cloud_backup/
```

### Option B: Auto-sync daily (cron on VM)

On the **VM**, add to the end of psx_cloud_collector.sh:

```bash
# Sync to your laptop if it's reachable (optional)
# Requires reverse SSH tunnel or your laptop having a public IP
# For most people, Option A (manual rsync) is simpler
```

### Option C: Upload to cloud storage (auto, no laptop needed)

Use Oracle's free 20GB Object Storage:

```bash
# Install OCI CLI
bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)"
oci setup config

# Upload daily backup
oci os object put \
  --bucket-name psx-data \
  --file ~/psxdata/tick_bars.db \
  --name "tick_bars_$(date +%Y-%m-%d).db"
```

### Option D: Connect React frontend from anywhere

SSH tunnel from your laptop to the VM:

```bash
# From your laptop — forwards VM port 8765 to localhost:8765
ssh -L 8765:localhost:8765 ubuntu@xxx.xxx.xxx.xxx -N
```

Now `ws://localhost:8765` on your laptop connects through the tunnel to the VM's relay. Your React app works exactly the same — just needs the tunnel running.

Or expose port 8765 directly (already opened in Part 1 firewall):
```
ws://xxx.xxx.xxx.xxx:8765
```

Update React `ws.ts`:
```typescript
const WS_BASE = "ws://xxx.xxx.xxx.xxx:8765";  // VM public IP
```

---

## PART 5: Monitoring & Alerts

### Status check script (run from anywhere via SSH)

```bash
cat > ~/pakfindata/services/cloud_status.sh << 'EOF'
#!/bin/bash
echo "═══════════════════════════════════════════════"
echo "  PSX CLOUD COLLECTOR STATUS"
echo "═══════════════════════════════════════════════"

DATE=$(date +%Y-%m-%d)
echo "Date: $DATE  Time: $(date +%H:%M:%S) PKT"
echo ""

echo "=== SERVICE ==="
systemctl is-active psx-collector.service && echo "  ✅ Service active" || echo "  ⏸️ Service idle (normal outside market hours)"
systemctl is-active psx-collector.timer && echo "  ✅ Timer active" || echo "  ❌ Timer NOT active"
echo "  Next trigger: $(systemctl list-timers psx-collector.timer --no-pager | tail -2 | head -1 | awk '{print $1, $2}')"

echo ""
echo "=== PROCESSES ==="
if pgrep -f "tick_service" > /dev/null; then
    PID=$(pgrep -f "tick_service" | head -1)
    RSS=$(ps -o rss= -p $PID | tr -d ' ')
    echo "  ✅ tick_service.py (PID: $PID, RAM: $((RSS/1024))MB)"
else
    echo "  ⏸️ tick_service not running (normal outside market hours)"
fi

if curl -s http://localhost:8765/health > /dev/null 2>&1; then
    echo "  ✅ WS Relay on :8765"
else
    echo "  ⏸️ WS Relay not running"
fi

echo ""
echo "=== DATA ==="
# tick_bars.db
DB="$HOME/psxdata/tick_bars.db"
if [ -f "$DB" ]; then
    SIZE=$(stat -c %s "$DB" 2>/dev/null || stat -f %z "$DB")
    echo "  tick_bars.db: $((SIZE/1024/1024)) MB"
    
    # Row counts
    sqlite3 "$DB" "
        SELECT 'ohlcv_5s: ' || COUNT(*) FROM ohlcv_5s;
        SELECT 'raw_ticks: ' || COUNT(*) FROM raw_ticks;
    " 2>/dev/null | sed 's/^/  /'
fi

# Intraday files
echo ""
echo "=== INTRADAY FILES (last 5 days) ==="
ls -lt ~/psxdata/intraday/*.csv 2>/dev/null | head -10 | awk '{print "  " $6, $7, $8, $9}'

# Disk usage
echo ""
echo "=== DISK ==="
echo "  $(du -sh ~/psxdata/ 2>/dev/null | awk '{print "psxdata: " $1}')"
echo "  $(df -h / | tail -1 | awk '{print "Disk: " $3 " used / " $2 " total (" $5 " full)"}')"

# Last log
echo ""
echo "=== LAST LOG (5 lines) ==="
LOG="$HOME/psxdata/logs/collector_${DATE}.log"
if [ -f "$LOG" ]; then
    tail -5 "$LOG" | sed 's/^/  /'
else
    echo "  No log for today (market may not have opened yet)"
fi
EOF

chmod +x ~/pakfindata/services/cloud_status.sh
```

Check from anywhere:
```bash
ssh ubuntu@xxx.xxx.xxx.xxx bash ~/pakfindata/services/cloud_status.sh
```

### Discord/Telegram alerts (optional)

Add to end of psx_cloud_collector.sh:

```bash
# ─── SEND ALERT ───────────────────────────────────────
# Discord webhook (create in your server → Settings → Integrations → Webhooks)
DISCORD_WEBHOOK="https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN"

TICK_COUNT=$(sqlite3 ~/psxdata/tick_bars.db "SELECT COUNT(*) FROM raw_ticks" 2>/dev/null || echo "0")
BAR_COUNT=$(sqlite3 ~/psxdata/tick_bars.db "SELECT COUNT(*) FROM ohlcv_5s" 2>/dev/null || echo "0")
DPS_TICKS=$(wc -l < ~/psxdata/intraday/dps_ticks_${DATE}.csv 2>/dev/null || echo "0")

MSG="📊 **PSX Collector — $DATE**\n"
MSG+="✅ WebSocket: ${TICK_COUNT} ticks, ${BAR_COUNT} bars\n"
MSG+="✅ DPS backup: ${DPS_TICKS} trade records\n"
MSG+="💾 DB: $(du -h ~/psxdata/tick_bars.db | awk '{print $1}')"

curl -s -H "Content-Type: application/json" \
  -d "{\"content\": \"$MSG\"}" \
  "$DISCORD_WEBHOOK" > /dev/null 2>&1

log "📨 Discord alert sent"
```

---

## PART 6: Maintenance

### Update tick_service.py code

From your laptop:
```bash
scp ~/pakfindata/src/pakfindata/services/tick_service.py \
    ubuntu@xxx.xxx.xxx.xxx:~/pakfindata/src/pakfindata/services/
```

### Check logs

```bash
ssh ubuntu@xxx.xxx.xxx.xxx tail -50 ~/psxdata/logs/collector_$(date +%Y-%m-%d).log
```

### Manual run (test)

```bash
ssh ubuntu@xxx.xxx.xxx.xxx
sudo systemctl start psx-collector.service
journalctl -u psx-collector.service -f
```

### Disk cleanup (run monthly)

```bash
# Keep last 30 days of intraday CSVs
find ~/psxdata/intraday/ -name "*.csv" -mtime +30 -delete

# Keep last 30 days of logs
find ~/psxdata/logs/ -name "*.log" -mtime +30 -delete
```

### VM stays running (no idle reclaim)

Oracle reclaims idle VMs after 7 days. Your collector runs 6+ hours daily, 
so it's NEVER idle. But as extra protection:

```bash
# Add a keepalive cron — pings every 6 hours
crontab -e
# Add: 0 */6 * * * curl -s https://dps.psx.com.pk > /dev/null 2>&1
```

---

## COST VERIFICATION CHECKLIST

After setup, verify you're on Always Free:

```bash
# SSH into VM
ssh ubuntu@xxx.xxx.xxx.xxx

# Check instance shape
curl -s http://169.254.169.254/ocd/v2/instance/ | python3 -m json.tool | grep shape
# Should show: VM.Standard.A1.Flex

# Check OCPU count  
curl -s http://169.254.169.254/ocd/v2/instance/ | python3 -m json.tool | grep ocpus
# Should show: 2 (within 4 free limit)

# Check memory
free -h
# Should show ~12GB (within 24 free limit)

# Check disk
df -h /
# Should show 50GB boot volume (within 200 free limit)
```

In OCI Console → Billing → Cost Analysis → verify $0.00

---

## SUMMARY

| What | Where | Auto? |
|------|-------|-------|
| Tick collection (WebSocket) | Oracle VM | ✅ systemd timer, 9:10-15:45 PKT |
| Checkpoint saves | Oracle VM | ✅ every 30 min |
| EOD flush to SQLite | Oracle VM | ✅ at 15:35 PKT |
| DPS tick backup | Oracle VM | ✅ after market close |
| PSX Terminal 1m backup | Oracle VM | ✅ after market close |
| Discord alert | Oracle VM | ✅ after completion |
| Data sync to laptop | Your laptop | 🔧 manual rsync when needed |
| React frontend | Your laptop or VM | 🔧 connect via SSH tunnel or public IP |

**You travel, laptop is off, phone is in your pocket — data still collects.**

First-time setup: ~1 hour.
After that: zero maintenance, zero cost, runs forever.
