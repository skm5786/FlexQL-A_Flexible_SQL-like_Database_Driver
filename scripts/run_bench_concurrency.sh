#!/usr/bin/env bash
# ── run_bench_concurrency.sh ─────────────────────────────────────────────
# Concurrency benchmark: read-heavy, write-heavy, mixed 70/30 50/50 20/80.
# Each scenario runs for duration_sec with multiple TCP-connected threads.
# Usage: ./scripts/run_bench_concurrency.sh [seed_rows] [duration_sec]
set -e
cd "$(dirname "$0")/.."
ROWS=${1:-500000}; DUR=${2:-10}; PORT=9000
echo "=== FlexQL Concurrency Benchmark ==="; echo "Seed rows: $ROWS  Duration: ${DUR}s per scenario"; echo ""
if [ ! -f "tests/bench_concurrency" ]; then
  echo "Compiling bench_concurrency..."
  g++ -std=c++17 -O3 -march=native -I./include \
      tests/bench_concurrency.cpp src/client/flexql.cpp \
      -o tests/bench_concurrency -lpthread
fi
if ! lsof -i :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "Starting flexql-server on port $PORT..."
  ./bin/flexql-server $PORT &; SERVER_PID=$!; sleep 1
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT
fi
./tests/bench_concurrency "$ROWS" "$DUR"
