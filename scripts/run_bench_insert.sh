#!/usr/bin/env bash
# ── run_bench_insert.sh ──────────────────────────────────────────────────
# Insert throughput: batch sweep, PK overhead, column overhead, 10M TA target.
# Usage: ./scripts/run_bench_insert.sh [row_count]
# Default: 10M rows (the TA requirement).
# ────────────────────────────────────────────────────────────────────────

set -e
cd "$(dirname "$0")/.."   # ensure we run from flexql/ root

ROWS=${1:-10000000}
PORT=9000

echo "=== FlexQL Insert Benchmark ==="
echo "Rows: $ROWS"
echo ""

# Compile if not already built
if [ ! -f "tests/bench_insert" ]; then
  echo "Compiling bench_insert..."
  g++ -std=c++17 -O3 -march=native -I./include \
      tests/bench_insert.cpp src/client/flexql.cpp \
      -o tests/bench_insert -lpthread
fi

# Start server if not running
SERVER_STARTED=0
if ! lsof -i :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "Starting flexql-server on port $PORT..."
  ./bin/flexql-server $PORT &
  SERVER_PID=$!
  SERVER_STARTED=1
  sleep 1
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT
fi

./tests/bench_insert "$ROWS"
