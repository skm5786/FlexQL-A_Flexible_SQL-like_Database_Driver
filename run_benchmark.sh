#!/usr/bin/env bash
# ============================================================
#  FlexQL Performance Benchmark — Build & Run Script
#  Run from the root of your flexql/ project directory.
# ============================================================

set -e  # exit on first error

echo "========================================"
echo "  FlexQL Perf Benchmark — Build & Run"
echo "========================================"

# ── Step 1: Kill any existing server on port 9000 ────────────────────────
echo ""
echo "[1/5] Stopping any existing flexql-server..."
pkill -f "flexql-server" 2>/dev/null || true
sleep 0.5

# ── Step 2: Clean build ───────────────────────────────────────────────────
echo ""
echo "[2/5] Clean build with -O3 -march=native -flto..."
make clean
make -j$(nproc)   # parallel build using all CPU cores

# ── Step 3: Compile benchmark binary ─────────────────────────────────────
echo ""
echo "[3/5] Compiling benchmark binary..."
g++ -std=c++17 -O3 -march=native -I./include \
    benchmark_flexql.cpp src/client/flexql.cpp \
    -o benchmark -lpthread
echo "      benchmark binary compiled OK"

# ── Step 4: Start server ──────────────────────────────────────────────────
echo ""
echo "[4/5] Starting flexql-server on port 9000..."
./bin/flexql-server 9000 &
SERVER_PID=$!
echo "      Server PID: $SERVER_PID"
sleep 1   # give the server time to bind and start accepting

# Verify the server is actually listening
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    exit 1
fi
echo "      Server is running"

# ── Step 5: Run benchmark ─────────────────────────────────────────────────
echo ""
echo "[5/5] Running benchmark..."
echo ""
echo "--- Correctness tests first ---"
./benchmark --unit-test

echo ""
echo "--- 10M row insert benchmark ---"
./benchmark 10000000

# ── Cleanup ───────────────────────────────────────────────────────────────
echo ""
echo "Stopping server (PID $SERVER_PID)..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "Done."