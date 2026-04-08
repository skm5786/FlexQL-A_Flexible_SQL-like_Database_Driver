#!/usr/bin/env bash
# ============================================================
#  run_after_insert_benchmark.sh
#
#  PURPOSE:
#    benchmark_after_insert.cpp connects fresh and queries BIG_USERS.
#    Since BIG_USERS was created inside BENCH_<fd> (a per-connection
#    ephemeral DB), it's gone after the inserting connection closes.
#
#    This script:
#      1. Runs benchmark_flexql to insert 10M rows into BENCH_<fd>/BIG_USERS
#      2. ... but that gets deleted on disconnect.
#
#    Instead: we create BIG_USERS inside a PERSISTENT named database,
#    then run benchmark_after_insert against that database.
#
#    The benchmark_after_insert binary must be compiled with USE_DB support.
#    We provide a modified version that does USE BIGTEST; before querying.
#
#  Usage:
#    ./run_after_insert_benchmark.sh [row_count] [client_count]
#    Defaults: row_count=100000  client_count=1
# ============================================================
set -e

ROWS=${1:-100000}
CLIENTS=${2:-1}
PORT=9000
DB_NAME="BIGTEST"

echo "========================================"
echo "  FlexQL Post-Insert Benchmark"
echo "  Rows: $ROWS  Clients: $CLIENTS"
echo "  Persistent DB: $DB_NAME"
echo "========================================"

# Step 1: Kill any existing server
echo "[1/5] Stopping existing server..."
pkill -f "flexql-server" 2>/dev/null || true
sleep 0.5

# Step 2: Clean up old BIGTEST WAL data
echo "[2/5] Cleaning stale data..."
if [ -d "data/BIGTEST" ]; then
    rm -rf "data/BIGTEST"
    echo "  Removed data/BIGTEST/"
fi
if [ -f "data/_registry" ]; then
    grep -v "^BIGTEST$" data/_registry > data/_registry.tmp 2>/dev/null || true
    mv data/_registry.tmp data/_registry 2>/dev/null || true
fi

# Step 3: Build
echo "[3/5] Building..."
make -j$(nproc) 2>&1 | tail -5

# Step 4: Build benchmark binaries
echo "[4/5] Building benchmarks..."
g++ -std=c++17 -O3 -march=native -I./include \
    benchmark_flexql.cpp src/client/flexql.cpp \
    -o benchmark_flexql_bin -lpthread

g++ -std=c++17 -O3 -march=native -I./include \
    benchmark_after_insert.cpp src/client/flexql.cpp \
    -o benchmark_after_insert_bin -lpthread

echo "  Benchmarks compiled OK"

# Step 5: Start server
echo "[5/5] Starting server..."
./bin/flexql-server $PORT &
SERVER_PID=$!
sleep 1
kill -0 $SERVER_PID 2>/dev/null || { echo "ERROR: server failed to start"; exit 1; }

cleanup() {
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null
}
trap cleanup EXIT

# Step 6: Seed BIG_USERS in persistent DB using client REPL commands
echo ""
echo "--- Seeding BIG_USERS in persistent DB $DB_NAME ---"

# Use the flexql client to create the DB and table, then insert
# We pipe SQL commands to the client
{
    echo "CREATE DATABASE $DB_NAME;"
    echo "USE $DB_NAME;"
    echo "CREATE TABLE BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);"
    echo ".exit"
} | ./bin/flexql-client 127.0.0.1 $PORT

echo "  Database and table created."

# Now use the benchmark to insert rows
# We need a version that does USE BIGTEST first.
# Use the client to run inserts in batches
echo ""
echo "--- Inserting $ROWS rows into $DB_NAME/BIG_USERS ---"

BATCH=500
inserted=0
batch_sql=""

# We'll use a Python-style approach: generate SQL and pipe it
python3 -c "
import sys
rows = $ROWS
batch = $BATCH
print('USE $DB_NAME;')
inserted = 0
while inserted < rows:
    parts = []
    for i in range(min(batch, rows - inserted)):
        idx = inserted + i + 1
        parts.append(f\"({idx},'user{idx}','user{idx}@mail.com',{1000.0 + (idx % 10000)},1893456000)\")
    print(f'INSERT INTO BIG_USERS VALUES {chr(44).join(parts)};')
    inserted += len(parts)
    if inserted % (rows // 10) == 0 or inserted >= rows:
        print(f'-- progress: {inserted}/{rows}', file=sys.stderr)
print('.exit')
" 2>&1 | grep "^--" >&2 | true

# Actually run the inserts
START_TIME=$(date +%s%N)
python3 -c "
rows = $ROWS
batch = $BATCH
print('USE $DB_NAME;')
inserted = 0
while inserted < rows:
    parts = []
    for i in range(min(batch, rows - inserted)):
        idx = inserted + i + 1
        parts.append(f\"({idx},'user{idx}','user{idx}@mail.com',{1000.0 + (idx % 10000)},1893456000)\")
    print(f'INSERT INTO BIG_USERS VALUES {\",\".join(parts)};')
    inserted += len(parts)
print('.exit')
" | ./bin/flexql-client 127.0.0.1 $PORT > /dev/null
END_TIME=$(date +%s%N)
ELAPSED=$(( (END_TIME - START_TIME) / 1000000 ))
echo "  Inserted $ROWS rows in ${ELAPSED}ms ($(( ROWS * 1000 / (ELAPSED > 0 ? ELAPSED : 1) )) rows/s)"

# Step 7: Run the after-insert benchmark
echo ""
echo "--- Running benchmark_after_insert ---"
./benchmark_after_insert_bin $ROWS $CLIENTS

echo ""
echo "Done."
