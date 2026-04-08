#!/usr/bin/env bash
# ============================================================
#  FlexQL TA Benchmark Runner
#  Run from your flexql/ project root.
#  Usage: ./run_benchmark.sh [row_count]
#  Default: 10000000 (10M — TA target, must finish < 120s)
# ============================================================
set -e
ROWS=${1:-10000000}
echo "========================================"
echo "  FlexQL TA Benchmark"
echo "  Rows: $ROWS"
echo "========================================"

# Step 1: Kill server
echo "[1/6] Stopping existing server..."
pkill -f "flexql-server" 2>/dev/null || true; sleep 0.5

# Step 2: Clean stale BENCH_ WAL data from crashed runs
echo "[2/6] Cleaning stale WAL session data..."
if [ -d "data" ]; then
    for d in data/BENCH_*/; do [ -d "$d" ] && rm -rf "$d" && echo "  Removed: $d"; done
    if [ -f "data/_registry" ]; then
        grep -v "^BENCH_" data/_registry > data/_registry.tmp 2>/dev/null || true
        mv data/_registry.tmp data/_registry 2>/dev/null || true
    fi
fi

# Step 3: Build
echo "[3/6] Building..."
make clean && make -j$(nproc)

# Step 4: Compile benchmark
echo "[4/6] Compiling benchmark..."
g++ -std=c++17 -O3 -march=native -I./include \
    benchmark_flexql.cpp src/client/flexql.cpp \
    -o benchmark -lpthread
echo "  benchmark compiled OK"

# Step 5: Start server
echo "[5/6] Starting server..."
./bin/flexql-server 9000 &
SERVER_PID=$!
echo "  PID=$SERVER_PID"
sleep 1
kill -0 $SERVER_PID 2>/dev/null || { echo "ERROR: server failed to start"; exit 1; }

cleanup(){ echo "Stopping server..."; kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null; }
trap cleanup EXIT

# Step 6: Run
echo ""
echo "[6/6] Running benchmark..."
echo ""
echo "--- Correctness tests ---"
./benchmark --unit-test

echo ""
echo "--- Insert benchmark: $ROWS rows ---"
./benchmark "$ROWS"

echo ""
echo "WAL data written:"
find data/ -type f 2>/dev/null | sort | head -20 || echo "  (none)"