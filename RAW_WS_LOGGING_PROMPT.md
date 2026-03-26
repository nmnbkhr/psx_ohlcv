# Claude Code Prompt: Raw WebSocket Message Logging

## Context

tick_service.py connects to `wss://psxterminal.com/` and receives raw JSON 
messages. Currently only the parsed/normalized version is saved to JSONL.
We want to ALSO save every raw WebSocket message exactly as received.

## What to add

A second file that captures every `ws.recv()` message with zero modification:

```
~/psxdata/tick_logs/raw_ws_YYYY-MM-DD.jsonl
```

One line per message, exactly as received from WebSocket. No parsing, no 
filtering, no field renaming. Just `raw + "\n"`.

## Step 1: Read tick_service.py

```bash
cat ~/pakfindata/src/pakfindata/services/tick_service.py | head -50

# Find the ws.recv() line
grep -n "ws.recv\|await.*recv\|raw.*recv" ~/pakfindata/src/pakfindata/services/tick_service.py

# Find existing JSONL writer (we'll copy the pattern)
grep -n "jsonl\|JSONL\|tick_log\|_writer\|_queue\|write_queue" ~/pakfindata/src/pakfindata/services/tick_service.py

# Find the log file path setup
grep -n "tick_logs\|LOG_DIR\|TICK_LOG" ~/pakfindata/src/pakfindata/services/tick_service.py
```

**STOP — read output before proceeding.**

## Step 2: Add raw message writer

Use the SAME queue + writer thread pattern as existing JSONL logging.

### 2A: Add a raw writer queue and thread

Near the existing JSONL writer setup, add a parallel raw writer:

```python
import queue
import threading
from pathlib import Path
from datetime import datetime, timezone, timedelta

PKT = timezone(timedelta(hours=5))

# Raw WS message queue + writer
_raw_queue: queue.Queue = queue.Queue(maxsize=50000)
_raw_writer_running = False

def _raw_writer_thread():
    """Background thread: drains _raw_queue → raw_ws_YYYY-MM-DD.jsonl"""
    global _raw_writer_running
    _raw_writer_running = True
    
    current_date = None
    fh = None
    
    while _raw_writer_running:
        try:
            # Batch drain for efficiency
            batch = []
            try:
                # Block up to 1 second for first message
                msg = _raw_queue.get(timeout=1.0)
                batch.append(msg)
                # Then drain everything available without blocking
                while not _raw_queue.empty() and len(batch) < 500:
                    batch.append(_raw_queue.get_nowait())
            except queue.Empty:
                continue
            
            if not batch:
                continue
            
            # Check date rollover
            today = datetime.now(PKT).strftime("%Y-%m-%d")
            if today != current_date:
                if fh:
                    fh.close()
                log_dir = Path.home() / "psxdata" / "tick_logs"
                log_dir.mkdir(parents=True, exist_ok=True)
                filepath = log_dir / f"raw_ws_{today}.jsonl"
                fh = open(filepath, "a", buffering=8192)
                current_date = today
            
            # Write batch
            for raw_msg in batch:
                fh.write(raw_msg)
                fh.write("\n")
            fh.flush()
            
        except Exception as e:
            # Never crash the writer thread
            pass
    
    if fh:
        fh.close()


def start_raw_writer():
    """Start the raw message writer thread."""
    t = threading.Thread(target=_raw_writer_thread, daemon=True, name="raw-ws-writer")
    t.start()
    return t


def stop_raw_writer():
    """Signal the raw writer to stop."""
    global _raw_writer_running
    _raw_writer_running = False
```

### 2B: Enqueue every raw message

At the line where `raw = await ws.recv()` (around line 1158), add ONE line 
immediately after receiving:

```python
raw = await asyncio.wait_for(ws.recv(), timeout=15)

# NEW — save raw message (non-blocking, queue absorbs bursts)
try:
    _raw_queue.put_nowait(raw)
except queue.Full:
    pass  # drop if queue full (shouldn't happen)

# ... existing parsing continues unchanged ...
```

### 2C: Start/stop the writer thread

In the main startup (where existing JSONL writer is started):

```python
# Start raw WS writer alongside existing JSONL writer
raw_writer = start_raw_writer()
```

In the shutdown/cleanup section:

```python
# Stop raw writer
stop_raw_writer()
```

## Step 3: DO NOT modify anything else

- ✅ Existing JSONL logging stays unchanged
- ✅ Existing parsing stays unchanged  
- ✅ Existing EOD flush stays unchanged
- ✅ No new dependencies
- ✅ No changes to any other file

The ONLY changes are:
1. New queue + writer thread for raw messages
2. One `_raw_queue.put_nowait(raw)` line after `ws.recv()`
3. Start/stop the writer thread in lifecycle

## Step 4: Output files (side by side)

After this change, tick_logs/ will have:

```
~/psxdata/tick_logs/
├── 2026-03-18.jsonl            ← existing normalized ticks (unchanged)
├── raw_ws_2026-03-18.jsonl     ← NEW raw WebSocket messages
├── 2026-03-19.jsonl
├── raw_ws_2026-03-19.jsonl
└── ...
```

## Step 5: Test locally

```bash
cd ~/pakfindata
source .venv/bin/activate
export PYTHONPATH=~/pakfindata/src

# Run tick_service for 60 seconds
timeout 60 python -m pakfindata.services.tick_service 2>&1 | tail -20

# Check raw file was created
ls -lh ~/psxdata/tick_logs/raw_ws_*.jsonl

# Check content (should be raw WS messages)
head -5 ~/psxdata/tick_logs/raw_ws_$(date +%Y-%m-%d).jsonl

# Verify existing JSONL still works
head -5 ~/psxdata/tick_logs/$(date +%Y-%m-%d).jsonl

# Compare line counts
wc -l ~/psxdata/tick_logs/$(date +%Y-%m-%d).jsonl
wc -l ~/psxdata/tick_logs/raw_ws_$(date +%Y-%m-%d).jsonl
# Raw should have MORE lines (includes ping, welcome, subscribe messages)
```

## Step 6: Deploy to cloud VM

After local test passes:

```bash
# Upload updated tick_service.py to Oracle VM
scp -i ~/.ssh/oracle-psx.key \
    ~/pakfindata/src/pakfindata/services/tick_service.py \
    ubuntu@80.225.73.99:~/pakfindata/src/pakfindata/services/

# Fix paths for cloud
ssh psx-cloud "sed -i 's|/mnt/e/psxdata|/home/ubuntu/psxdata|g' ~/pakfindata/src/pakfindata/services/tick_service.py"

# Restart service
ssh psx-cloud "sudo systemctl restart psx-tick-collector"

# Verify after a few minutes
ssh psx-cloud "ls -lh ~/psxdata/tick_logs/raw_ws_*.jsonl"
ssh psx-cloud "head -3 ~/psxdata/tick_logs/raw_ws_$(date +%Y-%m-%d).jsonl"
```

## Step 7: Update cloud sync script

Update `~/sync_psx_cloud.sh` to also download raw files:

```bash
cat > ~/sync_psx_cloud.sh << 'SCRIPT'
#!/bin/bash
echo "📥 Syncing PSX Cloud tick logs..."
rsync -avz --progress psx-cloud:~/psxdata/tick_logs/ /mnt/e/psxdata/tick_logs_cloud/
echo "✅ Done"
du -sh /mnt/e/psxdata/tick_logs_cloud/
SCRIPT
```

No change needed — rsync already syncs the entire tick_logs/ folder 
which now includes both normalized + raw files.

## IMPORTANT

1. **Queue size 50,000** — at 500 msg/sec peak, this buffers ~100 seconds. 
   If writer thread can't keep up (disk stall), messages are dropped silently. 
   This is intentional — we never block the WebSocket receiver.

2. **Buffered I/O (8192 bytes)** — writes batch to disk, not per-line.
   Efficient even on USB/external drives.

3. **Batch drain (up to 500 messages)** — writer grabs everything available 
   in the queue and writes in one burst. Minimizes disk I/O calls.

4. **Date rollover** — automatically creates new file at midnight PKT.
   No restart needed for multi-day runs.

5. **File size** — ~300-400 MB/day. Cloud VM has 43 GB free (~100+ days).
   Clean up monthly with: `find ~/psxdata/tick_logs -name "raw_ws_*" -mtime +30 -delete`

6. **raw_ws file is a SUPERSET** — contains everything the normalized JSONL 
   has, plus: ping, welcome, subscribe, market status messages.
   Can always re-generate normalized JSONL from raw if needed.
