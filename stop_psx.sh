#!/bin/bash
# ============================================
# PSX Live — Stop all services
# Usage: bash ~/pakfindata/stop_psx.sh
# ============================================

echo "🛑 Stopping PSX services..."

pkill -9 -f "python.*tick_service" 2>/dev/null
pkill -9 -f "vite" 2>/dev/null
pkill -9 -f "esbuild" 2>/dev/null
pkill -9 -f "node.*psx-live" 2>/dev/null

# Free ports
fuser -k 8765/tcp 2>/dev/null
fuser -k 3000/tcp 2>/dev/null

echo "✅ All services stopped"

# Archive tick logs to external drive
JSONL_FILES=(~/psxdata/tick_logs/*.jsonl)
if [ -e "${JSONL_FILES[0]}" ]; then
    echo ""
    echo "📁 Copying .jsonl files to /mnt/e/psxdata/tick_logs..."
    mkdir -p /mnt/e/psxdata/tick_logs
    cp ~/psxdata/tick_logs/*.jsonl /mnt/e/psxdata/tick_logs/

    if [ $? -eq 0 ]; then
        echo "✅ Copy complete"
        echo ""
        read -p "🗑️  Delete local .jsonl files? (y/N): " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            rm ~/psxdata/tick_logs/*.jsonl
            echo "✅ Local .jsonl files deleted"
        else
            echo "⏩ Skipped — local files kept"
        fi
    else
        echo "❌ Copy failed — local files NOT deleted"
    fi
else
    echo "📁 No .jsonl files found in ~/psxdata/tick_logs/"
fi
