#!/usr/bin/env bash
# ============================================================
#  FlexQL Master Benchmark Runner
#  Builds all benchmarks and runs them in sequence.
#  Run from your flexql/ project root.
# ============================================================

set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

RESULTS_DIR="bench_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_FILE="${RESULTS_DIR}/run_${TIMESTAMP}.txt"

mkdir -p "$RESULTS_DIR"

log() { echo -e "$*" | tee -a "$RESULTS_FILE"; }
header() { log "\n${BOLD}══════════════════════════════════════════════════${NC}"; log "${BOLD}  $1${NC}"; log "${BOLD}══════════════════════════════════════════════════${NC}"; }

# ── Args ─────────────────────────────────────────────────────────────────
SKIP_BUILD=0
SKIP_SERVER=0
PORT=9000

for arg in "$@"; do
  case $arg in
    --skip-build)  SKIP_BUILD=1  ;;
    --skip-server) SKIP_SERVER=1 ;;
    --port=*)      PORT="${arg#*=}" ;;
  esac
done

header "FlexQL Benchmark Suite  $(date)"
log "Results will be saved to: $RESULTS_FILE"
log "Port: $PORT"

# ── Step 1: Build server ─────────────────────────────────────────────────
if [ $SKIP_BUILD -eq 0 ]; then
  header "Building server and benchmark binaries"
  log "Running: make clean && make -j\$(nproc)"
  make clean
  make -j$(nproc) 2>&1 | tee -a "$RESULTS_FILE"
  log "${GREEN}Server build complete.${NC}"
fi

# ── Step 2: Build benchmark binaries ─────────────────────────────────────
header "Building benchmark binaries"

BENCH_FLAGS="-std=c++17 -O3 -march=native -I./include -lpthread"
BENCHMARKS=(
  "bench_insert"
  "bench_range_query"
  "bench_point_lookup"
  "bench_full_scan"
  "bench_join"
  "bench_concurrency"
)

for bench in "${BENCHMARKS[@]}"; do
  src="tests/${bench}.cpp"
  out="tests/${bench}"
  if [ -f "$src" ]; then
    log "  Compiling $bench..."
    g++ $BENCH_FLAGS "$src" src/client/flexql.cpp -o "$out"
    log "  ${GREEN}OK${NC}  → $out"
  else
    log "  ${YELLOW}SKIP${NC} $src not found"
  fi
done

# ── Step 3: Start server ─────────────────────────────────────────────────
if [ $SKIP_SERVER -eq 0 ]; then
  header "Starting FlexQL server"
  pkill -f "flexql-server" 2>/dev/null || true
  sleep 0.3
  ./bin/flexql-server $PORT &
  SERVER_PID=$!
  log "  Server started (PID $SERVER_PID, port $PORT)"
  sleep 1
  if ! kill -0 $SERVER_PID 2>/dev/null; then
    log "${RED}ERROR: Server failed to start${NC}"
    exit 1
  fi
fi

cleanup() {
  if [ $SKIP_SERVER -eq 0 ] && [ -n "$SERVER_PID" ]; then
    log "\nStopping server (PID $SERVER_PID)..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ── Step 4: Run each benchmark ───────────────────────────────────────────

run_bench() {
  local name="$1"
  local bin="tests/$name"
  shift
  if [ ! -f "$bin" ]; then
    log "  ${YELLOW}SKIP${NC} $bin not found — compile it first"
    return
  fi
  header "Running: $name $*"
  log "$(date)"
  "$bin" "$@" 2>&1 | tee -a "$RESULTS_FILE"
  log "${GREEN}$name complete.${NC}"
}

run_bench bench_insert        10000000
run_bench bench_range_query   1000000
run_bench bench_point_lookup  1000000
run_bench bench_full_scan     1000000
run_bench bench_join           50000
run_bench bench_concurrency   500000 10

# ── Summary ─────────────────────────────────────────────────────────────
header "All benchmarks complete"
log "Full results saved to: $RESULTS_FILE"
log "$(date)"
