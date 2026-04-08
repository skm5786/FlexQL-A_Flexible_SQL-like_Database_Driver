#!/usr/bin/env bash
# ── run_bench_range.sh ───────────────────────────────────────────────────
# Range query benchmark: B+ tree vs full scan at multiple selectivities.
# Usage: ./scripts/run_bench_range.sh [row_count] [repetitions]
# ────────────────────────────────────────────────────────────────────────

set -e
cd "$(dirname "$0")/.."

ROWS=${1:-1000000}
REPS=${2:-50}
PORT=9000

echo "=== FlexQL Range Query Benchmark ==="
echo "Rows: $ROWS  Reps per query: $REPS"
echo ""

if [ ! -f "tests/bench_range_query" ]; then
  echo "Compiling bench_range_query..."
  g++ -std=c++17 -O3 -march=native -I./include \
      tests/bench_range_query.cpp src/client/flexql.cpp \
      -o tests/bench_range_query -lpthread
fi

if ! lsof -i :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "Starting flexql-server on port $PORT..."
  ./bin/flexql-server $PORT &
  SERVER_PID=$!
  sleep 1
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT
fi

./tests/bench_range_query "$ROWS" "$REPS"
