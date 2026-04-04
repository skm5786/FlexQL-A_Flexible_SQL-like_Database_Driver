#!/usr/bin/env bash
# ============================================================
#  FlexQL Performance Benchmark — Build & Run Script
#  Run from the root of your flexql/ project directory.
#  Usage: ./run_benchmark.sh [row_count]
#  Default row_count: 10000000 (10M — the TA requirement)
# ============================================================

set -e

ROWS=${1:-10000000}

echo "========================================"
echo "  FlexQL Perf Benchmark — Build & Run"
echo "  Row count: $ROWS"
echo "========================================"

# ── Step 1: Kill any existing server ─────────────────────────────────────
echo ""
echo "[1/6] Stopping any existing flexql-server..."
pkill -f "flexql-server" 2>/dev/null || true
sleep 0.5

# ── Step 2: Clean stale BENCH_ WAL directories ───────────────────────────
# If the server was killed mid-run (Ctrl+C, crash, etc.), it left BENCH_<fd>
# directories in data/ and entries in data/_registry.  These would be
# replayed by wal_recover() on next start, but since each new run gets a
# fresh BENCH_<fd>, the old data is orphaned.  Clean them up proactively.
echo ""
echo "[2/6] Cleaning stale session WAL data..."
if [ -d "data" ]; then
    # Remove BENCH_* directories (per-connection session databases)
    for dir in data/BENCH_*/; do
        if [ -d "$dir" ]; then
            echo "  Removing stale: $dir"
            rm -rf "$dir"
        fi
    done
    # Rewrite _registry without any BENCH_ entries
    if [ -f "data/_registry" ]; then
        grep -v "^BENCH_" data/_registry > data/_registry.tmp 2>/dev/null || true
        mv data/_registry.tmp data/_registry 2>/dev/null || true
    fi
    echo "  Cleanup done."
else
    echo "  No data/ directory yet (fresh install)."
fi

# ── Step 3: Clean build ───────────────────────────────────────────────────
echo ""
echo "[3/6] Clean build with -O3 -march=native -flto..."
make clean
make -j$(nproc)

# ── Step 4: Compile benchmark binary ─────────────────────────────────────
echo ""
echo "[4/6] Compiling benchmark binary..."
g++ -std=c++17 -O3 -march=native -I./include \
    benchmark_flexql.cpp src/client/flexql.cpp \
    -o benchmark -lpthread
echo "      benchmark binary compiled OK"

# ── Step 5: Start server ──────────────────────────────────────────────────
echo ""
echo "[5/6] Starting flexql-server on port 9000..."
./bin/flexql-server 9000 &
SERVER_PID=$!
echo "      Server PID: $SERVER_PID"
sleep 1

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start"
    exit 1
fi
echo "      Server is running"

cleanup() {
    echo ""
    echo "Stopping server (PID $SERVER_PID)..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

# ── Step 6: Run benchmark ─────────────────────────────────────────────────
echo ""
echo "[6/6] Running benchmark..."
echo ""
echo "--- Correctness tests (--unit-test) ---"
./benchmark --unit-test

echo ""
echo "--- Insert benchmark ($ROWS rows) ---"
./benchmark "$ROWS"

echo ""
echo "data/ contents after benchmark:"
find data/ -type f 2>/dev/null | sort || echo "  (empty)"