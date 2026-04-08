#!/usr/bin/env bash
# ============================================================
#  FlexQL Master Benchmark Runner
#  Run from your flexql/ project root.
#
#  Usage: ./scripts/run_all_benchmarks.sh [--skip-build] [--skip-server]
#
#  Expected total runtime: ~5-10 minutes (including 10M insert TA target)
# ============================================================

set -e

RESULTS_DIR="bench_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_FILE="${RESULTS_DIR}/run_${TIMESTAMP}.txt"
mkdir -p "$RESULTS_DIR"

log()    { echo -e "$*" | tee -a "$RESULTS_FILE"; }
header() { log "\n══════════════════════════════════════════════════";
           log "  $1"; log "══════════════════════════════════════════════════"; }

SKIP_BUILD=0; SKIP_SERVER=0; PORT=9000
for arg in "$@"; do
  case $arg in --skip-build)SKIP_BUILD=1;; --skip-server)SKIP_SERVER=1;; --port=*)PORT="${arg#*=}";; esac
done

header "FlexQL Benchmark Suite  $(date)"
log "Results: $RESULTS_FILE"

# ── Clean stale BENCH_ WAL dirs ──────────────────────────────────────────
header "Cleaning stale session data"
if [ -d "data" ]; then
    for d in data/BENCH_*/; do [ -d "$d" ] && rm -rf "$d" && log "  Removed: $d"; done
    if [ -f "data/_registry" ]; then
        grep -v "^BENCH_" data/_registry > data/_registry.tmp 2>/dev/null || true
        mv data/_registry.tmp data/_registry 2>/dev/null || true
    fi
fi
log "  Clean done."

# ── Build ────────────────────────────────────────────────────────────────
if [ $SKIP_BUILD -eq 0 ]; then
    header "Building server"
    make clean && make -j$(nproc) 2>&1 | tee -a "$RESULTS_FILE"
fi

# ── Build benchmark binaries ─────────────────────────────────────────────
header "Building benchmark binaries"
FLAGS="-std=c++17 -O3 -march=native -I./include -lpthread"
for bench in bench_insert bench_range_query bench_point_lookup bench_full_scan bench_join bench_concurrency; do
    src="tests/${bench}.cpp"
    out="tests/${bench}"
    if [ -f "$src" ]; then
        log "  Compiling $bench..."
        g++ $FLAGS "$src" src/client/flexql.cpp -o "$out"
        log "  OK → $out"
    else
        log "  SKIP: $src not found"
    fi
done

# ── Start server ──────────────────────────────────────────────────────────
if [ $SKIP_SERVER -eq 0 ]; then
    header "Starting FlexQL server"
    pkill -f "flexql-server" 2>/dev/null || true
    sleep 0.3
    ./bin/flexql-server $PORT &
    SERVER_PID=$!
    log "  Server PID=$SERVER_PID port=$PORT"
    sleep 1
    kill -0 $SERVER_PID 2>/dev/null || { log "ERROR: Server failed"; exit 1; }
fi

cleanup(){ [ $SKIP_SERVER -eq 0 ]&&[ -n "$SERVER_PID" ]&&{ log "\nStopping server...";kill $SERVER_PID 2>/dev/null;wait $SERVER_PID 2>/dev/null; }; }
trap cleanup EXIT

run_bench(){
    local name="$1"; local bin="tests/$name"; shift
    [ ! -f "$bin" ]&&{ log "  SKIP $bin not found"; return; }
    header "Running: $name $*"
    log "$(date)"
    timeout 300 "$bin" "$@" 2>&1 | tee -a "$RESULTS_FILE" || log "  [TIMEOUT or ERROR after 300s]"
    log "  $name complete."
}

# ── Run benchmarks ────────────────────────────────────────────────────────
# bench_insert: 100K for sub-tests + 10M for TA target
run_bench bench_insert        10000000

# bench_range_query: 100K rows, 20 reps — ~2 min
run_bench bench_range_query   100000   20

# bench_point_lookup: 100K rows, 200 reps — ~1 min
run_bench bench_point_lookup  100000   200

# bench_full_scan: 50K rows, 5 reps, 3s conc — ~2 min
run_bench bench_full_scan     50000    5   3

# bench_join: 10K orders — ~1 min
run_bench bench_join          10000    10

# bench_concurrency: 50K seed, 5s per scenario — ~3 min
run_bench bench_concurrency   50000    5

header "All benchmarks complete"
log "Results: $RESULTS_FILE"
log "$(date)"