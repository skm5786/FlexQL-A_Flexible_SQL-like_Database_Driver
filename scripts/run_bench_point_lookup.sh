#!/usr/bin/env bash
# ── run_bench_point_lookup.sh ────────────────────────────────────────────
# Point lookup: hash index O(1) vs full scan O(n), scaling test.
# Usage: ./scripts/run_bench_point_lookup.sh [row_count] [repetitions]
# ────────────────────────────────────────────────────────────────────────
set -e
cd "$(dirname "$0")/.."
ROWS=${1:-1000000}; REPS=${2:-500}; PORT=9000
echo "=== FlexQL Point Lookup Benchmark ==="; echo "Rows: $ROWS  Reps: $REPS"; echo ""
if [ ! -f "tests/bench_point_lookup" ]; then
  echo "Compiling bench_point_lookup..."
  g++ -std=c++17 -O3 -march=native -I./include \
      tests/bench_point_lookup.cpp src/client/flexql.cpp \
      -o tests/bench_point_lookup -lpthread
fi
if ! lsof -i :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "Starting flexql-server on port $PORT..."
  ./bin/flexql-server $PORT &; SERVER_PID=$!; sleep 1
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT
fi
./tests/bench_point_lookup "$ROWS" "$REPS"
