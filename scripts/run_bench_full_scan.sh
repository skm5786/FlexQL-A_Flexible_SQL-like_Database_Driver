#!/usr/bin/env bash
# ── run_bench_full_scan.sh ───────────────────────────────────────────────
# Full table scan: throughput scaling, projection overhead, cache hits,
# and concurrent scans using RW lock.
# Usage: ./scripts/run_bench_full_scan.sh [row_count] [reps] [conc_dur_sec]
set -e
cd "$(dirname "$0")/.."
ROWS=${1:-1000000}; REPS=${2:-10}; DUR=${3:-5}; PORT=9000
echo "=== FlexQL Full Scan Benchmark ==="; echo "Rows: $ROWS  Reps: $REPS  ConcDur: ${DUR}s"; echo ""
if [ ! -f "tests/bench_full_scan" ]; then
  echo "Compiling bench_full_scan..."
  g++ -std=c++17 -O3 -march=native -I./include \
      tests/bench_full_scan.cpp src/client/flexql.cpp \
      -o tests/bench_full_scan -lpthread
fi
if ! lsof -i :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "Starting flexql-server on port $PORT..."
  ./bin/flexql-server $PORT &; SERVER_PID=$!; sleep 1
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT
fi
./tests/bench_full_scan "$ROWS" "$REPS" "$DUR"
