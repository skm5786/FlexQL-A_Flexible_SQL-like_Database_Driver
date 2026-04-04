/**
 * bench_concurrency.cpp  —  Concurrent Read / Write Benchmark
 *
 * Spawns multiple client threads, each with its OWN FlexQL connection
 * (one TCP socket per thread, matching real-world usage).
 *
 * Test scenarios:
 *   1. Read-heavy   — 8 reader threads, 0 writers
 *   2. Write-heavy  — 0 readers, 8 writer threads  (separate PK ranges)
 *   3. Mixed 70/30  — 7 reader threads + 3 writer threads
 *   4. Mixed 50/50  — 4 reader threads + 4 writer threads
 *   5. Mixed 20/80  — 2 reader threads + 8 writer threads
 *
 * Each scenario runs for a fixed DURATION_SEC seconds (not a fixed op count)
 * so all threads run simultaneously and throughput is measured as total
 * ops/sec across all threads.
 *
 * Readers issue:  SELECT * FROM T WHERE ID = <random>
 * Writers issue:  INSERT INTO T VALUES (...)  (unique IDs per thread)
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_concurrency.cpp src/client/flexql.cpp \
 *       -o tests/bench_concurrency -lpthread
 *
 * Run:
 *   ./bin/flexql-server 9000 &
 *   ./tests/bench_concurrency [seed_rows] [duration_sec]
 *   # defaults: seed_rows=500000  duration_sec=10
 */

#include "bench_common.h"
#include <atomic>

static const int DURATION_SEC = 10;

/* ── Shared state between threads ──────────────────────────────────────── */
struct SharedState {
    const char  *table;
    long long    seed_rows;        /* rows present before benchmark starts */
    std::atomic<long long> next_insert_id;  /* atomic counter for unique PKs */
    std::atomic<int>       stop_flag;       /* set to 1 to stop all threads */

    /* filled in by run_scenario() */
    std::atomic<long long> total_reads;
    std::atomic<long long> total_writes;
    std::atomic<long long> read_errors;
    std::atomic<long long> write_errors;

    SharedState(const char *t, long long sr)
        : table(t), seed_rows(sr),
          next_insert_id(sr + 1), stop_flag(0),
          total_reads(0), total_writes(0),
          read_errors(0), write_errors(0) {}

    void reset_counters() {
        total_reads = 0; total_writes = 0;
        read_errors = 0; write_errors = 0;
    }
};

/* ── Reader thread ─────────────────────────────────────────────────────── */
struct ReaderArg {
    SharedState *shared;
    unsigned int rng;
    ThreadResult result;
};

static void *reader_thread(void *arg_) {
    ReaderArg *arg = reinterpret_cast<ReaderArg*>(arg_);
    SharedState *s = arg->shared;

    FlexQL *db = connect_or_die();

    Timer t; t.start();
    long long ops = 0, errs = 0;

    while (!s->stop_flag.load(std::memory_order_relaxed)) {
        /* Random PK from the seeded range */
        arg->rng ^= arg->rng << 13;
        arg->rng ^= arg->rng >> 17;
        arg->rng ^= arg->rng << 5;
        long long key = (arg->rng % s->seed_rows) + 1;

        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE ID = %lld;", s->table, key);

        char *err = nullptr;
        long long count = 0;
        int rc = flexql_exec(db, sql, cb_count, &count, &err);
        if (rc != FLEXQL_OK) { errs++; if (err) flexql_free(err); }
        else ops++;
    }

    arg->result.ops_completed = ops;
    arg->result.errors        = errs;
    arg->result.elapsed_ms    = t.elapsed_ms();

    s->total_reads.fetch_add(ops);
    s->read_errors.fetch_add(errs);

    flexql_close(db);
    return nullptr;
}

/* ── Writer thread ─────────────────────────────────────────────────────── */
struct WriterArg {
    SharedState *shared;
    int          batch_size;
    ThreadResult result;
};

static void *writer_thread(void *arg_) {
    WriterArg *arg = reinterpret_cast<WriterArg*>(arg_);
    SharedState *s = arg->shared;
    int bs = arg->batch_size;

    FlexQL *db = connect_or_die();

    Timer t; t.start();
    long long ops = 0, errs = 0;

    while (!s->stop_flag.load(std::memory_order_relaxed)) {
        /* Claim a block of IDs atomically */
        long long id_start = s->next_insert_id.fetch_add(bs);
        long long id_end   = id_start + bs;

        std::ostringstream ss;
        ss << "INSERT INTO " << s->table << " VALUES ";
        for (long long id = id_start; id < id_end; id++) {
            if (id != id_start) ss << ",";
            ss << "(" << id
               << "," << (id % 10)
               << "," << (1000.0 + (id % 10000))
               << ",'label" << id << "'"
               << "," << (1700000000LL + id) << ")";
        }
        ss << ";";

        char *err = nullptr;
        int rc = flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &err);
        if (rc != FLEXQL_OK) { errs++; if (err) flexql_free(err); }
        else ops += bs;
    }

    arg->result.ops_completed = ops;
    arg->result.errors        = errs;
    arg->result.elapsed_ms    = t.elapsed_ms();

    s->total_writes.fetch_add(ops);
    s->write_errors.fetch_add(errs);

    flexql_close(db);
    return nullptr;
}

/* ── Run one scenario ─────────────────────────────────────────────────── */
struct ScenarioCfg {
    const char *name;
    int n_readers;
    int n_writers;
    int writer_batch;   /* rows per INSERT statement per writer */
    int duration_sec;
};

static void run_scenario(SharedState *shared, const ScenarioCfg &cfg) {
    printf("\n  Scenario: %-30s  readers=%d  writers=%d  dur=%ds\n",
           cfg.name, cfg.n_readers, cfg.n_writers, cfg.duration_sec);

    shared->reset_counters();
    shared->stop_flag.store(0);

    int total_threads = cfg.n_readers + cfg.n_writers;
    std::vector<pthread_t>   tids(total_threads);
    std::vector<ReaderArg>   rargs(cfg.n_readers);
    std::vector<WriterArg>   wargs(cfg.n_writers);

    /* Launch readers */
    for (int i = 0; i < cfg.n_readers; i++) {
        rargs[i].shared = shared;
        rargs[i].rng    = (unsigned int)(42 + i * 1337);
        pthread_create(&tids[i], nullptr, reader_thread, &rargs[i]);
    }
    /* Launch writers */
    for (int i = 0; i < cfg.n_writers; i++) {
        wargs[i].shared     = shared;
        wargs[i].batch_size = cfg.writer_batch;
        pthread_create(&tids[cfg.n_readers + i], nullptr,
                       writer_thread, &wargs[i]);
    }

    /* Let them run */
    struct timespec ts = { cfg.duration_sec, 0 };
    nanosleep(&ts, nullptr);
    shared->stop_flag.store(1, std::memory_order_release);

    for (int i = 0; i < total_threads; i++)
        pthread_join(tids[i], nullptr);

    /* Aggregate */
    long long total_r = shared->total_reads.load();
    long long total_w = shared->total_writes.load();
    long long err_r   = shared->read_errors.load();
    long long err_w   = shared->write_errors.load();
    double dur        = (double)cfg.duration_sec * 1000.0;

    double r_tput = (dur > 0) ? total_r * 1000.0 / dur : 0;
    double w_tput = (dur > 0) ? total_w * 1000.0 / dur : 0;

    printf("    reads : %8lld ops  %8.0f ops/s  errors=%lld\n",
           total_r, r_tput, err_r);
    printf("    writes: %8lld ops  %8.0f ops/s  errors=%lld\n",
           total_w, w_tput, err_w);
    printf("    total : %8lld ops  %8.0f ops/s\n",
           total_r + total_w, r_tput + w_tput);
}

int main(int argc, char **argv) {
    long long seed_rows  = (argc > 1) ? atoll(argv[1]) : 500000LL;
    int       dur_sec    = (argc > 2) ? atoi(argv[2])  : DURATION_SEC;

    print_header("FlexQL Concurrency Benchmark");
    printf("  Seed rows : %lld\n", seed_rows);
    printf("  Duration  : %d seconds per scenario\n", dur_sec);

    FlexQL *db = connect_or_die();

    /* ── Setup ─────────────────────────────────────────────────────── */
    print_section("Table Setup");
    const char *T = "BENCH_CONC";
    printf("  Creating and seeding %lld rows...\n", seed_rows);
    create_standard_table(db, T, /*with_pk=*/false);  /* no PK → faster inserts */
    Timer st; st.start();
    seed_standard_table(db, T, seed_rows, 500, true);
    printf("  Seed complete: %.1f ms\n", st.elapsed_ms());

    /* Close the setup connection — each thread opens its own */
    flexql_close(db);

    SharedState shared(T, seed_rows);

    /* ── Scenarios ──────────────────────────────────────────────────── */
    print_section("Concurrent Scenarios");

    static const ScenarioCfg SCENARIOS[] = {
        { "read-heavy  (8R + 0W)",   8, 0, 0,   dur_sec },
        { "write-heavy (0R + 8W)",   0, 8, 50,  dur_sec },
        { "mixed 70/30 (7R + 3W)",   7, 3, 50,  dur_sec },
        { "mixed 50/50 (4R + 4W)",   4, 4, 50,  dur_sec },
        { "mixed 20/80 (2R + 8W)",   2, 8, 50,  dur_sec },
        { "single reader baseline",  1, 0, 0,   dur_sec },
        { "single writer baseline",  0, 1, 50,  dur_sec },
    };
    static const int N_SCEN = (int)(sizeof(SCENARIOS) / sizeof(SCENARIOS[0]));

    for (int i = 0; i < N_SCEN; i++)
        run_scenario(&shared, SCENARIOS[i]);

    /* ── Cleanup ───────────────────────────────────────────────────── */
    db = connect_or_die();
    fql_exec_ignore(db, "DROP TABLE BENCH_CONC;");
    flexql_close(db);

    printf("\n  Done.\n");
    return 0;
}
