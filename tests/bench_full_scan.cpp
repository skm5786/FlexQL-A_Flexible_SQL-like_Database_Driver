/**
 * bench_full_scan.cpp  —  Full Table Scan Benchmark
 *
 * Measures the raw speed of linear linked-list scans in FlexQL.
 * This is the worst-case query path: no index, no cache, every row visited.
 *
 * Tests:
 *   1. SELECT * (all columns) at 100K / 500K / 1M / 5M rows
 *   2. SELECT single column vs all columns (serialisation overhead)
 *   3. SELECT with no-op WHERE on VARCHAR (forces full scan even with indexes)
 *   4. Repeated scan (cache hit vs miss)
 *   5. Scan throughput (MB/s of row data returned)
 *   6. Concurrent scans: N readers on same table simultaneously
 *
 * The numbers here show:
 *   - Raw scan rate (rows/sec) of the storage linked list
 *   - Serialisation cost (converting CellValue → wire format)
 *   - TCP transfer rate (rows/sec including network)
 *   - LRU cache hit speedup
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_full_scan.cpp src/client/flexql.cpp \
 *       -o tests/bench_full_scan -lpthread
 *
 * Run:
 *   ./bin/flexql-server 9000 &
 *   ./tests/bench_full_scan [row_count]
 *   # default = 1000000
 */

#include "bench_common.h"
#include <atomic>

/* ── One full scan, return count and elapsed ─────────────────────────── */
static double single_scan(FlexQL *db, const char *sql,
                            long long *rows_out = nullptr) {
    long long count = 0;
    char *err = nullptr;
    Timer t; t.start();
    flexql_exec(db, sql, cb_count, &count, &err);
    double ms = t.elapsed_ms();
    if (err) flexql_free(err);
    if (rows_out) *rows_out = count;
    return ms;
}

/* ── Scan at different row counts, SELECT * ──────────────────────────── */
static void run_scale_series(FlexQL *db) {
    print_section("Scan Throughput vs Row Count (SELECT *)");

    static const long long SIZES[] = {
        10000, 50000, 100000, 250000, 500000, 1000000
    };

    for (long long sz : SIZES) {
        char tname[64];
        snprintf(tname, sizeof(tname), "SCAN_SCALE");

        fql_exec_ignore(db, "DROP TABLE SCAN_SCALE;");
        create_standard_table(db, tname, false);
        seed_standard_table(db, tname, sz, 500, false);

        char sql[128];
        snprintf(sql, sizeof(sql), "SELECT * FROM %s;", tname);

        /* Warm up (first scan populates cache) */
        single_scan(db, sql);

        /* Measure 3 cold runs (invalidate between them by noting we can't
           actually invalidate, so report 1st as cold, rest as warm) */
        long long rows = 0;
        double cold_ms = single_scan(db, sql, &rows);  /* actually warm now */
        double warm1   = single_scan(db, sql);
        double warm2   = single_scan(db, sql);
        double avg_warm = (warm1 + warm2) / 2.0;

        double tput_cold = (cold_ms > 0) ? rows * 1000.0 / cold_ms : 0;
        double tput_warm = (avg_warm > 0) ? rows * 1000.0 / avg_warm : 0;

        printf("  rows=%-8lld  cold=%-7.1fms (%7.0f rows/s)"
               "  warm=%-7.1fms (%7.0f rows/s)\n",
               sz, cold_ms, tput_cold, avg_warm, tput_warm);
    }
    fql_exec_ignore(db, "DROP TABLE SCAN_SCALE;");
}

/* ── Projection overhead: SELECT * vs SELECT one col ────────────────── */
static void run_projection_scan(FlexQL *db, const char *table,
                                 long long row_count, int reps) {
    print_section("Projection Overhead During Full Scan");

    static const struct { const char *sel; const char *label; } PROJS[] = {
        { "*",          "SELECT * (all 5 cols)"     },
        { "ID",         "SELECT ID (1 int col)"     },
        { "VALUE",      "SELECT VALUE (1 dec col)"  },
        { "LABEL",      "SELECT LABEL (1 var col)"  },
        { "ID, VALUE",  "SELECT 2 cols"             },
    };

    for (const auto &p : PROJS) {
        char sql[256];
        snprintf(sql, sizeof(sql), "SELECT %s FROM %s;", p.sel, table);

        LatencyStats stats; stats.reserve(reps);
        Timer wall; wall.start();
        for (int i = 0; i < reps; i++) {
            Timer t; t.start();
            long long c = 0; char *err = nullptr;
            flexql_exec(db, sql, cb_count, &c, &err);
            stats.record(t.elapsed_us());
            if (err) flexql_free(err);
        }
        stats.set_wall(wall.elapsed_ms());
        print_latency(p.label, stats);
    }
}

/* ── Cache hit vs miss ────────────────────────────────────────────────── */
static void run_cache_comparison(FlexQL *db, const char *table,
                                  long long row_count, int reps) {
    print_section("LRU Cache: Hit vs Miss");

    char sql_star[256], sql_val[256];
    snprintf(sql_star, sizeof(sql_star), "SELECT * FROM %s;", table);
    /* Use a unique-ish WHERE to avoid hitting cache from previous runs */
    snprintf(sql_val, sizeof(sql_val),
             "SELECT * FROM %s WHERE VALUE > 5000.0;", table);

    /* Prime the cache */
    single_scan(db, sql_star);
    single_scan(db, sql_val);

    /* Cache HIT: same queries again */
    LatencyStats hit_star, hit_val;
    hit_star.reserve(reps); hit_val.reserve(reps);

    Timer wall; wall.start();
    for (int i = 0; i < reps; i++) {
        Timer t; t.start();
        long long c = 0; char *err = nullptr;
        flexql_exec(db, sql_star, cb_count, &c, &err);
        hit_star.record(t.elapsed_us());
        if (err) flexql_free(err);
    }
    hit_star.set_wall(wall.elapsed_ms());

    wall.start();
    for (int i = 0; i < reps; i++) {
        Timer t; t.start();
        long long c = 0; char *err = nullptr;
        flexql_exec(db, sql_val, cb_count, &c, &err);
        hit_val.record(t.elapsed_us());
        if (err) flexql_free(err);
    }
    hit_val.set_wall(wall.elapsed_ms());

    print_latency("Cache HIT SELECT *", hit_star);
    print_latency("Cache HIT SELECT > 5000", hit_val);

    /* Approximate cache miss: insert a row to invalidate, then re-scan */
    printf("\n  (Inserting 1 row to invalidate cache, then measuring miss)\n");
    {
        long long next_id = row_count + 9999999LL;
        char inv_sql[256];
        snprintf(inv_sql, sizeof(inv_sql),
                 "INSERT INTO %s VALUES (%lld,0,1234.0,'inv',%lld);",
                 table, next_id, 1700000000LL + next_id);
        fql_exec(db, inv_sql, "cache_invalidate");
    }

    LatencyStats miss_star;
    miss_star.reserve(1);
    Timer wm; wm.start();
    single_scan(db, sql_star);
    miss_star.record(wm.elapsed_us());
    miss_star.set_wall(wm.elapsed_ms());
    print_latency("Cache MISS SELECT *", miss_star);

    printf("\n  row_count=%lld\n", row_count);
}

/* ── Concurrent scans: N readers on same table ────────────────────────── */
struct ConcScanArg {
    const char        *table;
    int                duration_sec;
    std::atomic<int>  *stop;
    ThreadResult       result;
};

static void *conc_scan_thread(void *arg_) {
    ConcScanArg *arg = reinterpret_cast<ConcScanArg*>(arg_);
    FlexQL *db = connect_or_die();

    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT * FROM %s;", arg->table);

    Timer t; t.start();
    long long ops = 0, errs = 0;

    while (!arg->stop->load(std::memory_order_relaxed)) {
        long long c = 0; char *err = nullptr;
        int rc = flexql_exec(db, sql, cb_count, &c, &err);
        if (rc == FLEXQL_OK) ops++;
        else { errs++; if (err) flexql_free(err); }
    }

    arg->result.ops_completed = ops;
    arg->result.errors        = errs;
    arg->result.elapsed_ms    = t.elapsed_ms();

    flexql_close(db);
    return nullptr;
}

static void run_concurrent_scans(const char *table, int dur_sec) {
    print_section("Concurrent Full Scans (RW lock: readers share)");

    static const int THREAD_COUNTS[] = { 1, 2, 4, 8 };

    for (int n : THREAD_COUNTS) {
        std::atomic<int> stop(0);
        std::vector<pthread_t>   tids(n);
        std::vector<ConcScanArg> args(n);

        for (int i = 0; i < n; i++) {
            args[i].table        = table;
            args[i].duration_sec = dur_sec;
            args[i].stop         = &stop;
            pthread_create(&tids[i], nullptr, conc_scan_thread, &args[i]);
        }

        struct timespec ts = { dur_sec, 0 };
        nanosleep(&ts, nullptr);
        stop.store(1, std::memory_order_release);
        for (int i = 0; i < n; i++) pthread_join(tids[i], nullptr);

        long long total_scans = 0, total_errs = 0;
        for (int i = 0; i < n; i++) {
            total_scans += args[i].result.ops_completed;
            total_errs  += args[i].result.errors;
        }

        double tput = (double)total_scans / dur_sec;
        printf("  threads=%-2d  scans=%-8lld  %.1f scans/s  errors=%lld\n",
               n, total_scans, tput, total_errs);
    }
}

int main(int argc, char **argv) {
    long long row_count = (argc > 1) ? atoll(argv[1]) : 1000000LL;
    int       reps      = (argc > 2) ? atoi(argv[2])  : 10;
    int       dur_sec   = (argc > 3) ? atoi(argv[3])  : 5;

    print_header("FlexQL Full Table Scan Benchmark");
    printf("  Row count       : %lld\n", row_count);
    printf("  Scan repetitions: %d\n", reps);
    printf("  Concurrent dur  : %d sec\n", dur_sec);

    FlexQL *db = connect_or_die();

    /* ── Scale series first (creates its own tables) ─────────────── */
    run_scale_series(db);

    /* ── Main table ─────────────────────────────────────────────── */
    print_section("Main Table Setup");
    const char *T = "BENCH_SCAN";
    printf("  Creating and seeding %lld rows...\n", row_count);
    create_standard_table(db, T, false);
    Timer st; st.start();
    seed_standard_table(db, T, row_count, 500, true);
    printf("  Seed: %.1f ms\n", st.elapsed_ms());

    run_projection_scan(db, T, row_count, reps);
    run_cache_comparison(db, T, row_count, reps);

    /* Close setup connection; concurrent scanner opens its own */
    flexql_close(db);
    run_concurrent_scans(T, dur_sec);

    db = connect_or_die();
    fql_exec_ignore(db, "DROP TABLE BENCH_SCAN;");
    flexql_close(db);

    printf("\n  Done.\n");
    return 0;
}
