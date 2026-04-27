# PSX Cloud Collector — Complete Guide

Everything about your Oracle Cloud PSX tick collector in one file.

---

## Server Details

```
IP:       80.225.73.99
User:     ubuntu
Region:   Saudi Arabia West (Jeddah)
Shape:    VM.Standard.A1.Flex (ARM, 2 CPU, 12GB RAM)
Disk:     46 GB
OS:       Ubuntu 22.04 Minimal aarch64
Cost:     $0 (Always Free tier)
Key:      /home/adnoman/.ssh/oracle-psx.key (WSL2)
          E:\ovcn\ssh-key-2026-03-18.key (Windows)
```

---

## Part 1: Oracle Cloud Account Setup (Done Once)

1. Created account at https://www.oracle.com/cloud/free/
2. Home region: **Saudi Arabia West (Jeddah)** — cannot be changed
3. Upgraded to **Pay-As-You-Go** (needed for ARM capacity, still $0 for free tier)
4. Tax details: checked "Tax information is not available"

---

## Part 2: VM Creation (Done Once)

```
Instance:    psx-collector
Image:       Canonical Ubuntu 22.04 Minimal aarch64
Shape:       VM.Standard.A1.Flex (Always Free Eligible)
OCPU:        2 (of 4 free)
RAM:         12 GB (of 24 free)
Boot disk:   46.6 GB (of 200 free)
VCN:         Created via VCN Wizard (with Internet Connectivity)
Subnet:      Public subnet
Public IP:   80.225.73.99 (auto-assigned)
SSH Key:     Generated during creation, downloaded to E:\ovcn\
```

---

## Part 3: VM Setup (Done Once)

### SSH from WSL2

```bash
# Copy key (done once)
cp /mnt/e/ovcn/ssh-key-2026-03-18.key ~/.ssh/oracle-psx.key
chmod 400 ~/.ssh/oracle-psx.key

# SSH config (done once)
cat >> ~/.ssh/config << 'EOF'
Host psx-cloud
    HostName 80.225.73.99
    User ubuntu
    IdentityFile /home/adnoman/.ssh/oracle-psx.key
    StrictHostKeyChecking no
EOF
chmod 600 ~/.ssh/config

# Connect
ssh psx-cloud
```

### System setup (run on VM via SSH)

```bash
# Timezone
sudo timedatectl set-timezone Asia/Karachi

# Update
sudo apt update && sudo apt upgrade -y
# When asked "Which services should be restarted?" → type 17

# Install essentials
sudo apt install -y python3 python3-pip python3-venv sqlite3 curl git cron
sudo systemctl enable cron && sudo systemctl start cron

# Project structure
mkdir -p ~/pakfindata/src/pakfindata/services
mkdir -p ~/psxdata/{intraday,logs,tick_logs}
touch ~/pakfindata/src/pakfindata/__init__.py
touch ~/pakfindata/src/pakfindata/services/__init__.py

# Python venv
cd ~/pakfindata
python3 -m venv .venv
source .venv/bin/activate
pip install websockets requests
```

### Copy code from laptop (run in WSL2, NOT SSH)

```bash
scp -i ~/.ssh/oracle-psx.key \
    ~/pakfindata/src/pakfindata/services/tick_service.py \
    ubuntu@80.225.73.99:~/pakfindata/src/pakfindata/services/

scp -i ~/.ssh/oracle-psx.key \
    ~/pakfindata/src/pakfindata/config.py \
    ubuntu@80.225.73.99:~/pakfindata/src/pakfindata/
```

### Fix paths for cloud (run on VM via SSH)

```bash
sed -i 's|/mnt/e/psxdata|/home/ubuntu/psxdata|g' ~/pakfindata/src/pakfindata/config.py
sed -i 's|/mnt/e/psxdata|/home/ubuntu/psxdata|g' ~/pakfindata/src/pakfindata/services/tick_service.py
```

---

## Part 4: Services Setup (Done Once)

### Tick Collector Service (run on VM via SSH)

```bash
sudo tee /etc/systemd/system/psx-tick-collector.service << 'EOF'
[Unit]
Description=PSX Tick Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/pakfindata
Environment=HOME=/home/ubuntu
Environment=PYTHONPATH=/home/ubuntu/pakfindata/src
Environment=PYTHONUNBUFFERED=1
Environment=PATH=/home/ubuntu/pakfindata/.venv/bin:/usr/bin:/bin
ExecStart=/home/ubuntu/pakfindata/.venv/bin/python -m pakfindata.services.tick_service
StandardOutput=append:/home/ubuntu/psxdata/logs/tick_collector.log
StandardError=append:/home/ubuntu/psxdata/logs/tick_collector_error.log
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

### Timer — Auto-start Mon-Fri 9:10 AM PKT (run on VM via SSH)

```bash
sudo tee /etc/systemd/system/psx-tick-collector.timer << 'EOF'
[Unit]
Description=PSX Tick Collector Timer

[Timer]
OnCalendar=Mon..Fri *-*-* 04:10:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
EOF
```

### Enable everything (run on VM via SSH)

```bash
sudo systemctl daemon-reload
sudo systemctl enable psx-tick-collector.timer
sudo systemctl start psx-tick-collector.timer
```

### Keepalive cron — prevents Oracle idle reclaim (run on VM via SSH)

```bash
(crontab -l 2>/dev/null; echo "0 */6 * * * curl -s https://dps.psx.com.pk > /dev/null 2>&1") | crontab -
```

---

## Part 5: How It Works — Automatic Daily Cycle

```
Mon-Fri:
  09:10 PKT — systemd timer starts tick_service.py
  09:15 PKT — Market opens, ticks flood in
  09:15-15:30 — Every trade captured (bid/ask/price/volume)
  Every 30m   — Checkpoint saves to DB (crash protection)
  15:30 PKT — Market closes
  15:35 PKT — EOD flush: all ticks → tick_bars.db
  ~15:40 PKT — tick_service exits on its own
  Next morning — Timer fires again

Weekends:
  Timer does not fire
  Keepalive cron runs every 6h (prevents Oracle idle reclaim)

VM Reboot (e.g., Oracle maintenance):
  Timer has Persistent=true
  If reboot happens at 11 AM and missed 9:10 trigger → fires IMMEDIATELY
  Service has Restart=on-failure → auto-restarts on crash
  You lose only the minutes during reboot
```

---

## Part 6: Daily Commands (WSL2)

### Check Status

```bash
# Quick log check
ssh psx-cloud "tail -5 ~/psxdata/logs/tick_collector.log"

# Service status
ssh psx-cloud "sudo systemctl status psx-tick-collector"

# Timer status (when is next trigger?)
ssh psx-cloud "sudo systemctl list-timers | grep psx"

# Check errors
ssh psx-cloud "cat ~/psxdata/logs/tick_collector_error.log"

# Disk usage
ssh psx-cloud "du -sh ~/psxdata/* && df -h /"

# List tick files
ssh psx-cloud "ls -lh ~/psxdata/tick_logs/"

# Live log (Ctrl+C to exit)
ssh psx-cloud "tail -f ~/psxdata/logs/tick_collector.log"
```

### Download Data (WSL2)

```bash
# Download all tick logs
rsync -avz psx-cloud:~/psxdata/tick_logs/ /mnt/e/psxdata/tick_logs_cloud/

# Download specific date
scp psx-cloud:~/psxdata/tick_logs/2026-03-18.jsonl /mnt/e/psxdata/tick_logs_cloud/

# One-command sync (setup once, use daily)
cat > ~/sync_psx_cloud.sh << 'SCRIPT'
#!/bin/bash
echo "📥 Syncing PSX Cloud tick logs..."
rsync -avz --progress psx-cloud:~/psxdata/tick_logs/ /mnt/e/psxdata/tick_logs_cloud/
echo "✅ Done"
du -sh /mnt/e/psxdata/tick_logs_cloud/
SCRIPT
chmod +x ~/sync_psx_cloud.sh

# Then anytime:
bash ~/sync_psx_cloud.sh
```

### Emergency Commands (WSL2)

```bash
# Restart service (if ticks are 0 during market hours)
ssh psx-cloud "sudo systemctl restart psx-tick-collector"

# Start service manually
ssh psx-cloud "sudo systemctl start psx-tick-collector"

# Stop service
ssh psx-cloud "sudo systemctl stop psx-tick-collector"

# Reboot VM
ssh psx-cloud "sudo reboot"
# Wait 60 seconds, then reconnect
ssh psx-cloud
```

---

## Part 7: Cleanup (Monthly)

```bash
# 1. Download everything first
bash ~/sync_psx_cloud.sh

# 2. Verify local copy
ls -lh /mnt/e/psxdata/tick_logs_cloud/

# 3. Delete old files from VM (older than 30 days)
ssh psx-cloud "find ~/psxdata/tick_logs -name '*.jsonl' -mtime +30 -delete"
ssh psx-cloud "find ~/psxdata/logs -name '*.log' -mtime +30 -delete"

# 4. Check disk after cleanup
ssh psx-cloud "du -sh ~/psxdata/* && df -h /"
```

---

## Part 8: Access From Any Device

### WSL2 (Primary)

```bash
ssh psx-cloud
ssh psx-cloud "tail -5 ~/psxdata/logs/tick_collector.log"
```

### Windows PowerShell

```powershell
# First-time: copy key and set permissions
Copy-Item "E:\ovcn\ssh-key-2026-03-18.key" "$HOME\.ssh\oracle-psx.key"
icacls "$HOME\.ssh\oracle-psx.key" /inheritance:r /grant "${env:USERNAME}:(R)"

# Add to SSH config
@"
Host psx-cloud
    HostName 80.225.73.99
    User ubuntu
    IdentityFile C:\Users\$env:USERNAME\.ssh\oracle-psx.key
    StrictHostKeyChecking no
"@ | Out-File -FilePath "$HOME\.ssh\config" -Encoding ASCII -Append

# Use
ssh psx-cloud
ssh psx-cloud "tail -5 ~/psxdata/logs/tick_collector.log"
```

### Android (JuiceSSH or Termux)

**JuiceSSH (easiest):**
1. Install from Play Store
2. Transfer `ssh-key-2026-03-18.key` to phone (email/drive/USB)
3. Connections → New → Address: 80.225.73.99, Port: 22
4. Identity → Username: ubuntu, Private Key: import the .key file
5. Connect

**Termux:**
```bash
pkg update && pkg install openssh
cp /storage/emulated/0/Download/ssh-key-2026-03-18.key ~/.ssh/oracle-psx.key
chmod 400 ~/.ssh/oracle-psx.key
ssh -i ~/.ssh/oracle-psx.key ubuntu@80.225.73.99
```

---

## Part 9: Update Code

When tick_service.py changes on your laptop (WSL2):

```bash
# Upload new code
scp -i ~/.ssh/oracle-psx.key \
    ~/pakfindata/src/pakfindata/services/tick_service.py \
    ubuntu@80.225.73.99:~/pakfindata/src/pakfindata/services/

# Fix paths again
ssh psx-cloud "sed -i 's|/mnt/e/psxdata|/home/ubuntu/psxdata|g' ~/pakfindata/src/pakfindata/services/tick_service.py"

# Restart service
ssh psx-cloud "sudo systemctl restart psx-tick-collector"

# Verify
ssh psx-cloud "tail -10 ~/psxdata/logs/tick_collector.log"
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Permission denied (publickey)` | Use full path: `ssh -i /home/adnoman/.ssh/oracle-psx.key ubuntu@80.225.73.99` |
| `Connection refused` | VM rebooting — wait 2 min, try again |
| `Connection timed out` | Check Oracle Console — VM might be stopped, click Start |
| Ticks: 0 during market hours | `ssh psx-cloud "sudo systemctl restart psx-tick-collector"` |
| Log file empty | `ssh psx-cloud "cat ~/psxdata/logs/tick_collector_error.log"` |
| Disk full | Download data then: `ssh psx-cloud "find ~/psxdata/tick_logs -mtime +30 -delete"` |
| VM stopped by Oracle | Oracle Console → Compute → Instances → Start. Then check timer is active |
| Service not starting | `ssh psx-cloud "sudo systemctl status psx-tick-collector"` and check errors |
| Timer not firing | `ssh psx-cloud "sudo systemctl enable psx-tick-collector.timer && sudo systemctl start psx-tick-collector.timer"` |
| Need to reboot for kernel update | `ssh psx-cloud "sudo reboot"` — timer auto-recovers |

---
## Routine commands

```bash
# Create local folder
mkdir -p /mnt/e/psxdatacloud

# Download everything
rsync -avz --progress psx-cloud:~/psxdata/ /mnt/e/psxdatacloud/

# Verify
du -sh /mnt/e/psxdatacloud/*

# Then delete from VM (keep only the folder structure)
ssh psx-cloud "rm -f ~/psxdata/tick_bars.db && rm -rf ~/psxdata/tick_logs/* && rm -rf ~/psxdata/logs/* && rm -f ~/psxdata/live_snapshot.json"

# Verify cleanup
ssh psx-cloud "du -sh ~/psxdata/* && df -h /"
```

---

## Cost Verification

```bash
# Verify Always Free shape
ssh psx-cloud "curl -s http://169.254.169.254/ocd/v2/instance/ | python3 -m json.tool | grep shape"
# Should show: VM.Standard.A1.Flex

# Check Oracle Console → Billing → Cost Analysis → should show $0.00
```

**If you ONLY use the Always Free shape (A1.Flex, 2 OCPU, 12GB), you will NEVER be charged.**
