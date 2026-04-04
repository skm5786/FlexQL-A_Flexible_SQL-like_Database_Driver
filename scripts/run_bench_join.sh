#!/usr/bin/env bash
# ── run_bench_join.sh ────────────────────────────────────────────────────
# INNER JOIN benchmark: nested-loop cost at varying table sizes.
# Usage: ./scripts/run_bench_join.sh [orders_count] [repetitions]
set -e
cd "$(dirname "$0")/.."
ORDERS=${1:-50000}; REPS=${2:-20}; PORT=9000
echo "=== FlexQL JOIN Benchmark ==="; echo "Orders: $ORDERS  Reps: $REPS"; echo ""
if [ ! -f "tests/bench_join" ]; then
  echo "Compiling bench_join..."
  g++ -std=c++17 -O3 -march=native -I./include \
      tests/bench_join.cpp src/client/flexql.cpp \
      -o tests/bench_join -lpthread
fi
if ! lsof -i :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "Starting flexql-server on port $PORT..."
  ./bin/flexql-server $PORT &; SERVER_PID=$!; sleep 1
  trap "kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null" EXIT
fi
./tests/bench_join "$ORDERS" "$REPS"
