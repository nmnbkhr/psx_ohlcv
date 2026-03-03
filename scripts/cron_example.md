# Scheduling PSX OHLCV Sync

## Cron Setup (Asia/Karachi timezone)

PSX market closes at 15:30 PKT. Running the sync at 18:00 ensures all EOD data is available.

### 1. Set timezone (if not already set)

```bash
sudo timedatectl set-timezone Asia/Karachi
```

### 2. Edit crontab

```bash
crontab -e
```

### 3. Add the sync job

```cron
# PSX OHLCV daily sync at 18:00 PKT (after market close)
0 18 * * 1-5 cd /home/adnoman/pakfindata && /opt/miniconda/bin/conda run -n psx python -m pakfindata.cli sync --all --refresh-symbols >> /mnt/e/psxdata/logs/cron.log 2>&1
```

**Breakdown:**

- `0 18 * * 1-5` - Run at 18:00, Monday through Friday
- `cd /home/adnoman/pakfindata` - Change to project directory
- `conda run -n psx` - Run in the `psx` conda environment
- `--refresh-symbols` - Update symbol list before syncing
- `>> /mnt/e/psxdata/logs/cron.log 2>&1` - Append output to cron log

### 4. Verify crontab

```bash
crontab -l
```

### Incremental Mode (faster daily updates)

For daily runs after initial full sync:

```cron
0 18 * * 1-5 cd /home/adnoman/pakfindata && /opt/miniconda/bin/conda run -n psx python -m pakfindata.cli sync --all --incremental >> /mnt/e/psxdata/logs/cron.log 2>&1
```

### Weekly Full Refresh

Run a full refresh on Saturdays to catch any corrections:

```cron
0 10 * * 6 cd /home/adnoman/pakfindata && /opt/miniconda/bin/conda run -n psx python -m pakfindata.cli sync --all --refresh-symbols >> /mnt/e/psxdata/logs/cron.log 2>&1
```

---

## Systemd User Service (WSL2)

For WSL2, systemd user services provide better control than cron.

### 1. Create service file

```bash
mkdir -p ~/.config/systemd/user
```

Create `~/.config/systemd/user/pfsync.service`:

```ini
[Unit]
Description=PSX OHLCV Daily Sync
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/home/adnoman/pakfindata
ExecStart=/opt/miniconda/bin/conda run -n psx python -m pakfindata.cli sync --all --refresh-symbols
StandardOutput=append:/mnt/e/psxdata/logs/systemd.log
StandardError=append:/mnt/e/psxdata/logs/systemd.log

[Install]
WantedBy=default.target
```

### 2. Create timer file

Create `~/.config/systemd/user/pfsync.timer`:

```ini
[Unit]
Description=Run PSX OHLCV sync daily at 18:00 PKT

[Timer]
OnCalendar=Mon..Fri 18:00
Persistent=true

[Install]
WantedBy=timers.target
```

### 3. Enable and start

```bash
# Reload systemd
systemctl --user daemon-reload

# Enable timer to start on boot
systemctl --user enable pfsync.timer

# Start timer now
systemctl --user start pfsync.timer

# Check status
systemctl --user status pfsync.timer
systemctl --user list-timers
```

### 4. Manual run (testing)

```bash
systemctl --user start pfsync.service
journalctl --user -u pfsync.service -f
```

### WSL2 Notes

For systemd to work in WSL2, ensure `/etc/wsl.conf` contains:

```ini
[boot]
systemd=true
```

Then restart WSL:

```powershell
wsl --shutdown
```

---

## Log Rotation

The application logs (`/mnt/e/psxdata/logs/pfsync.log`) rotate automatically (5MB, 3 backups).

For cron/systemd logs, add logrotate config at `/etc/logrotate.d/pfsync`:

```text
/mnt/e/psxdata/logs/cron.log
/mnt/e/psxdata/logs/systemd.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
}
```
