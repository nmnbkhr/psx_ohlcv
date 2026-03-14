#!/bin/bash
# ============================================
# PSX Live — Single command to start everything
# Usage: bash ~/pakfindata/start_psx.sh
# ============================================

echo "🔄 Stopping existing processes..."
pkill -9 -f "python.*tick_service" 2>/dev/null
pkill -9 -f "vite" 2>/dev/null
pkill -9 -f "esbuild" 2>/dev/null
pkill -9 -f "node.*psx-live" 2>/dev/null
sleep 2

# Free ports if stuck
fuser -k 8765/tcp 2>/dev/null
fuser -k 3000/tcp 2>/dev/null

# Create directories
mkdir -p ~/psxdata/tick_logs

echo ""
echo "🚀 Starting tick_service (backend)..."
cd ~/pakfindata
nohup /opt/miniconda/envs/psx/bin/python -u -m pakfindata.services.tick_service > ~/psxdata/tick_service.log 2>&1 &
TICK_PID=$!
echo "   PID: $TICK_PID"

echo ""
echo "🌐 Starting psx-live (frontend)..."
cd ~/projects/psx-live
nohup npm run dev > ~/psxdata/psx_live.log 2>&1 &
VITE_PID=$!
echo "   PID: $VITE_PID"

# Wait for services to start
sleep 3

echo ""
echo "✅ All services started"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Frontend:   http://localhost:3000"
echo "  WebSocket:  ws://localhost:8765"
echo ""
echo "  Logs:"
echo "    tail -f ~/psxdata/tick_service.log"
echo "    tail -f ~/psxdata/psx_live.log"
echo ""
echo "  Tick data:  ~/psxdata/tick_logs/"
echo ""
echo "  Stop all:   pkill -f tick_service; pkill -f vite"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Verify
echo ""
ps aux | grep -E "tick_service|vite" | grep -v grep | grep -v claude && echo "" || echo "⚠️  Something failed — check logs above"
