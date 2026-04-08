/**
 * bench_concurrency.cpp  —  Concurrent Read / Write Benchmark  (Fixed)
 *
 * ROOT CAUSE OF 100% ERRORS (all previous versions):
 *
 *   The setup connection creates BENCH_CONC in its own BENCH_<fd> session DB.
 *   Reader/writer threads each call connect_or_die() → get a NEW connection
 *   → server creates BENCH_<fd2>, BENCH_<fd3> etc. → empty databases.
 *   SELECT/INSERT fail with "table BENCH_CONC does not exist".
 *
 * THE FIX — Use a named shared database:
 *
 *   1. Setup connection: CREATE DATABASE CONC_SHARED; USE CONC_SHARED;
 *      then creates and seeds BENCH_CONC there.
 *   2. All reader/writer threads: USE CONC_SHARED; after connecting.
 *      Now they all operate on the SAME table.
 *
 *   This works because CONC_SHARED is a persistent named database.
 *   wal_is_persistent("CONC_SHARED") returns 1 → WAL writes happen.
 *   All connections that USE it access the same in-memory tables.
 *
 * WRITER DESIGN:
 *   Writers INSERT into BENCH_CONC with unique PK ranges via atomic counter.
 *   No PK — table created with with_pk=false for faster inserts.
 *
 * READER DESIGN:
 *   Readers do full table scans: SELECT * FROM BENCH_CONC
 *   This exercises the RW lock (multiple readers share rdlock).
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_concurrency.cpp src/client/flexql.cpp \
 *       -o tests/bench_concurrency -lpthread
 *
 * Run:
 *   ./tests/bench_concurrency [seed_rows] [duration_sec]
 *   Defaults: seed_rows=50000  duration_sec=5
 */

#include "bench_common.h"
#include <atomic>

/* ── Shared named database — all threads USE this ────────────────────────── */
static const char *SHARED_DB   = "CONC_SHARED";
static const char *SHARED_TABLE = "BENCH_CONC";

/* Set up the shared DB + table in one connection, returns seeded row count */
static long long setup_shared(long long seed_rows) {
    FlexQL *db = connect_or_die();

    /* CREATE DATABASE CONC_SHARED (silently ignore if already exists) */
    char *err = nullptr;
    char sql[256];
    snprintf(sql, sizeof(sql), "CREATE DATABASE %s;", SHARED_DB);
    flexql_exec(db, sql, nullptr, nullptr, &err);
    if (err) { flexql_free(err); err = nullptr; }

    /* USE CONC_SHARED */
    snprintf(sql, sizeof(sql), "USE %s;", SHARED_DB);
    if (flexql_exec(db, sql, nullptr, nullptr, &err) != FLEXQL_OK) {
        fprintf(stderr, "Cannot USE %s: %s\n", SHARED_DB, err?err:"?");
        if (err) flexql_free(err);
        flexql_close(db);
        exit(1);
    }

    /* DROP + CREATE BENCH_CONC — no PK for faster inserts */
    snprintf(sql, sizeof(sql), "DROP TABLE %s;", SHARED_TABLE);
    flexql_exec(db, sql, nullptr, nullptr, &err);
    if (err) { flexql_free(err); err = nullptr; }

    snprintf(sql, sizeof(sql),
             "CREATE TABLE %s("
             "ID DECIMAL NOT NULL,CATEGORY INT NOT NULL,"
             "VALUE DECIMAL NOT NULL,LABEL VARCHAR(64) NOT NULL,"
             "CREATED_AT DECIMAL NOT NULL);", SHARED_TABLE);
    if (flexql_exec(db, sql, nullptr, nullptr, &err) != FLEXQL_OK) {
        fprintf(stderr, "CREATE TABLE failed: %s\n", err?err:"?");
        if (err) flexql_free(err);
        flexql_close(db);
        exit(1);
    }

    /* Seed */
    printf("  Creating and seeding %lld rows...\n", seed_rows);
    Timer st; st.start();
    long long seeded = seed_standard_table(db, SHARED_TABLE, seed_rows, 500, true, 60000.0);
    printf("  Seed complete: %.1f ms  (%.0f rows/s)\n",
           st.elapsed_ms(), seeded * 1000.0 / st.elapsed_ms());

    flexql_close(db);
    return seeded;
}

/* ── Thread that connects and USEs the shared DB ─────────────────────────── */
static FlexQL *thread_connect() {
    FlexQL *db = connect_or_die();
    char sql[256];
    char *err = nullptr;
    snprintf(sql, sizeof(sql), "USE %s;", SHARED_DB);
    if (flexql_exec(db, sql, nullptr, nullptr, &err) != FLEXQL_OK) {
        /* If CONC_SHARED is gone, this is an error */
        if (err) flexql_free(err);
        flexql_close(db);
        return nullptr;
    }
    return db;
}

/* ── Shared state ────────────────────────────────────────────────────────── */
struct SharedState {
    long long            seed_rows;
    std::atomic<long long> next_insert_id;
    std::atomic<int>       stop_flag;
    std::atomic<long long> total_reads;
    std::atomic<long long> total_writes;
    std::atomic<long long> read_errors;
    std::atomic<long long> write_errors;

    SharedState(long long sr) :
        seed_rows(sr), next_insert_id(sr + 1), stop_flag(0),
        total_reads(0), total_writes(0), read_errors(0), write_errors(0) {}

    void reset() {
        total_reads=0; total_writes=0; read_errors=0; write_errors=0;
    }
};

/* ── Reader thread: SELECT * FROM BENCH_CONC ─────────────────────────────── */
struct ReaderArg { SharedState *shared; ThreadResult result; };

static void *reader_thread(void *arg_) {
    ReaderArg *arg = (ReaderArg*)arg_;
    FlexQL *db = thread_connect();
    if (!db) { arg->result.errors = 1; return nullptr; }

    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT * FROM %s;", SHARED_TABLE);

    Timer t; t.start();
    long long ops = 0, errs = 0;
    while (!arg->shared->stop_flag.load(std::memory_order_relaxed)) {
        long long c = 0; char *err = nullptr;
        int rc = flexql_exec(db, sql, cb_count, &c, &err);
        if (rc == FLEXQL_OK) ops++;
        else { errs++; if (err) flexql_free(err); }
    }
    arg->result.ops_completed = ops;
    arg->result.errors        = errs;
    arg->result.elapsed_ms    = t.elapsed_ms();
    arg->shared->total_reads.fetch_add(ops);
    arg->shared->read_errors.fetch_add(errs);
    flexql_close(db);
    return nullptr;
}

/* ── Writer thread: INSERT batch rows ────────────────────────────────────── */
struct WriterArg { SharedState *shared; int batch; ThreadResult result; };

static void *writer_thread(void *arg_) {
    WriterArg *arg = (WriterArg*)arg_;
    FlexQL *db = thread_connect();
    if (!db) { arg->result.errors = 1; return nullptr; }

    Timer t; t.start();
    long long ops = 0, errs = 0;
    while (!arg->shared->stop_flag.load(std::memory_order_relaxed)) {
        long long id_start = arg->shared->next_insert_id.fetch_add(arg->batch);
        std::ostringstream ss;
        ss << "INSERT INTO " << SHARED_TABLE << " VALUES ";
        for (int i = 0; i < arg->batch; i++) {
            long long id = id_start + i;
            if (i > 0) ss << ",";
            ss << "(" << id << "," << (id%10) << "," << (1000.0+(id%10000))
               << ",'label" << id << "'," << (1700000000LL+id) << ")";
        }
        ss << ";";
        char *err = nullptr;
        int rc = flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &err);
        if (rc == FLEXQL_OK) ops += arg->batch;
        else { errs++; if (err) flexql_free(err); }
    }
    arg->result.ops_completed = ops;
    arg->result.errors        = errs;
    arg->result.elapsed_ms    = t.elapsed_ms();
    arg->shared->total_writes.fetch_add(ops);
    arg->shared->write_errors.fetch_add(errs);
    flexql_close(db);
    return nullptr;
}

/* ── Run one scenario ────────────────────────────────────────────────────── */
struct Scenario { const char *name; int nr, nw, batch, dur; };

static void run_scenario(SharedState *sh, const Scenario &sc) {
    printf("\n  Scenario: %-40s  readers=%d  writers=%d  dur=%ds\n",
           sc.name, sc.nr, sc.nw, sc.dur);
    sh->reset();
    sh->stop_flag.store(0);

    int nt = sc.nr + sc.nw;
    std::vector<pthread_t>  tids(nt);
    std::vector<ReaderArg>  rargs(sc.nr);
    std::vector<WriterArg>  wargs(sc.nw);

    for (int i=0;i<sc.nr;i++) {
        rargs[i].shared=sh;
        pthread_create(&tids[i], nullptr, reader_thread, &rargs[i]);
    }
    for (int i=0;i<sc.nw;i++) {
        wargs[i].shared=sh;
        wargs[i].batch=sc.batch;
        pthread_create(&tids[sc.nr+i], nullptr, writer_thread, &wargs[i]);
    }

    struct timespec ts={sc.dur,0}; nanosleep(&ts, nullptr);
    sh->stop_flag.store(1, std::memory_order_release);
    for (int i=0;i<nt;i++) pthread_join(tids[i], nullptr);

    long long tr=sh->total_reads.load(), tw=sh->total_writes.load();
    long long er=sh->read_errors.load(), ew=sh->write_errors.load();
    double ms=(double)sc.dur*1000.0;
    printf("    reads : %8lld ops  %8.0f ops/s  errors=%lld\n", tr, tr*1000.0/ms, er);
    printf("    writes: %8lld ops  %8.0f ops/s  errors=%lld\n", tw, tw*1000.0/ms, ew);
    printf("    total : %8lld ops  %8.0f ops/s\n", tr+tw, (tr+tw)*1000.0/ms);
}

int main(int argc, char **argv) {
    long long seed_rows = (argc>1) ? atoll(argv[1]) : 50000LL;
    int dur_sec         = (argc>2) ? atoi(argv[2])  : 5;

    print_header("FlexQL Concurrency Benchmark");
    printf("  Seed rows : %lld\n", seed_rows);
    printf("  Duration  : %d seconds per scenario\n", dur_sec);

    /* All scenarios share one named database — the fix */
    print_section("Table Setup");
    long long seeded = setup_shared(seed_rows);

    SharedState sh(seeded);

    print_section("Concurrent Scenarios");

    static const Scenario SCENARIOS[] = {
        { "read-heavy  (8R + 0W)",    8, 0,  0, 5 },
        { "write-heavy (0R + 8W)",    0, 8, 50, 5 },
        { "mixed 70/30 (7R + 3W)",    7, 3, 50, 5 },
        { "mixed 50/50 (4R + 4W)",    4, 4, 50, 5 },
        { "mixed 20/80 (2R + 8W)",    2, 8, 50, 5 },
        { "single reader baseline",   1, 0,  0, 5 },
        { "single writer baseline",   0, 1, 50, 5 },
    };
    for (const auto &sc : SCENARIOS) run_scenario(&sh, sc);

    /* Cleanup */
    FlexQL *db = thread_connect();
    if (db) {
        char sql[256];
        snprintf(sql, sizeof(sql), "DROP TABLE %s;", SHARED_TABLE);
        fql_exec_ignore(db, sql);
        snprintf(sql, sizeof(sql), "DROP DATABASE %s;", SHARED_DB);
        fql_exec_ignore(db, sql);
        flexql_close(db);
    }

    printf("\n  Done.\n");
    return 0;
}