/**
 * bench_point_lookup.cpp  —  Point Lookup (PK & Non-PK) Benchmark
 *
 * Exercises the three lookup paths in FlexQL:
 *   1. Hash index    — WHERE pk = val       (O(1))
 *   2. B+ tree       — WHERE numeric = val  (O(log n), falls to full scan for EQ)
 *   3. Full scan     — WHERE varchar = val  (O(n))
 *
 * Test matrix:
 *   - Row counts     : 100K, 500K, 1M, 5M
 *   - Key existence  : hit (key exists), miss (key does not exist)
 *   - Access pattern : sequential IDs, random IDs, hot-key (same key repeated)
 *   - Projected cols : SELECT * vs SELECT single col
 *
 * Also measures how lookup latency scales with row count (shows O(1) vs O(n)).
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_point_lookup.cpp src/client/flexql.cpp \
 *       -o tests/bench_point_lookup -lpthread
 *
 * Run:
 *   ./bin/flexql-server 9000 &
 *   ./tests/bench_point_lookup [row_count]
 *   # default = 1000000
 */

#include "bench_common.h"

static unsigned int g_rng_state = 42;

static unsigned int rng_next() {
    /* xorshift32 — fast, deterministic, good enough for benchmarking */
    g_rng_state ^= g_rng_state << 13;
    g_rng_state ^= g_rng_state >> 17;
    g_rng_state ^= g_rng_state << 5;
    return g_rng_state;
}

/* ── Run N point lookups, varying the key each time ─────────────────── */
enum KeyPattern { SEQ, RAND, HOT };

static void run_pk_lookups(FlexQL *db, const char *table,
                            long long row_count, int reps,
                            KeyPattern pattern, const char *pat_label,
                            LatencyStats &stats) {
    long long hot_key = row_count / 2;  /* same key every time for HOT */
    long long seq_key = 1;

    for (int i = 0; i < reps; i++) {
        long long key;
        switch (pattern) {
        case SEQ:  key = (seq_key++ % row_count) + 1;          break;
        case RAND: key = (rng_next() % row_count) + 1;         break;
        case HOT:  key = hot_key;                               break;
        default:   key = 1;
        }

        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE ID = %lld;", table, key);

        long long count = 0;
        char *err = nullptr;
        Timer t; t.start();
        flexql_exec(db, sql, cb_count, &count, &err);
        double us = t.elapsed_us();
        if (err) flexql_free(err);
        stats.record(us);
    }
    (void)pat_label;
}

/* ── Run N lookups for a key that does NOT exist ─────────────────────── */
static void run_pk_miss_lookups(FlexQL *db, const char *table,
                                 long long row_count, int reps,
                                 LatencyStats &stats) {
    /* Keys beyond row_count guaranteed not to exist */
    for (int i = 0; i < reps; i++) {
        long long key = row_count + 1 + (i % 1000);
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE ID = %lld;", table, key);

        long long count = 0;
        char *err = nullptr;
        Timer t; t.start();
        flexql_exec(db, sql, cb_count, &count, &err);
        double us = t.elapsed_us();
        if (err) flexql_free(err);
        stats.record(us);
    }
}

/* ── Run N full-scan varchar lookups ─────────────────────────────────── */
static void run_varchar_scan(FlexQL *db, const char *table,
                              long long row_count, int reps,
                              LatencyStats &stats) {
    for (int i = 0; i < reps; i++) {
        long long key = (rng_next() % row_count) + 1;
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT ID FROM %s WHERE LABEL = 'label%lld';",
                 table, key);

        long long count = 0;
        char *err = nullptr;
        Timer t; t.start();
        flexql_exec(db, sql, cb_count, &count, &err);
        double us = t.elapsed_us();
        if (err) flexql_free(err);
        stats.record(us);
    }
}

/* ── Select only one column vs all columns ─────────────────────────────── */
static void run_projection_comparison(FlexQL *db, const char *table,
                                       long long row_count, int reps) {
    printf("\n  Projection: SELECT * vs SELECT ID (single-col) on PK hit\n");

    LatencyStats star, single;
    star.reserve(reps); single.reserve(reps);

    for (int i = 0; i < reps; i++) {
        long long key = (rng_next() % row_count) + 1;
        char sql_star[256], sql_single[256];
        snprintf(sql_star,   sizeof(sql_star),
                 "SELECT * FROM %s WHERE ID = %lld;", table, key);
        snprintf(sql_single, sizeof(sql_single),
                 "SELECT ID FROM %s WHERE ID = %lld;", table, key);

        long long c = 0; char *err = nullptr;
        Timer t; t.start();
        flexql_exec(db, sql_star, cb_count, &c, &err);
        star.record(t.elapsed_us());
        if (err) flexql_free(err); err = nullptr; c = 0;

        t.start();
        flexql_exec(db, sql_single, cb_count, &c, &err);
        single.record(t.elapsed_us());
        if (err) flexql_free(err);
    }

    Timer w; w.start();
    star.set_wall(w.elapsed_ms()); single.set_wall(w.elapsed_ms());
    print_latency("SELECT *", star);
    print_latency("SELECT ID", single);
}

/* ── Scaling test: how does PK lookup scale with row count ──────────── */
static void run_scaling_test(FlexQL *db) {
    print_section("O(1) Hash Lookup Scaling (should be flat across row counts)");

    static const long long SIZES[] = { 10000, 100000, 500000, 1000000 };
    static const int N_SIZES = (int)(sizeof(SIZES) / sizeof(SIZES[0]));
    const int REPS = 200;

    for (int s = 0; s < N_SIZES; s++) {
        long long sz = SIZES[s];
        char tname[64];
        snprintf(tname, sizeof(tname), "BENCH_SCALE_%lld", sz);

        /* Create and seed */
        create_standard_table(db, tname, true);
        seed_standard_table(db, tname, sz, 500, false);

        /* Random PK hit */
        LatencyStats stats; stats.reserve(REPS);
        Timer wall; wall.start();
        run_pk_lookups(db, tname, sz, REPS, RAND, "rand", stats);
        stats.set_wall(wall.elapsed_ms());

        char label[64];
        snprintf(label, sizeof(label), "PK-hit rows=%-8lld", sz);
        print_latency(label, stats);

        fql_exec_ignore(db, (std::string("DROP TABLE ") + tname + ";").c_str());
    }
}

int main(int argc, char **argv) {
    long long row_count = (argc > 1) ? atoll(argv[1]) : 1000000LL;
    int reps            = (argc > 2) ? atoi(argv[2])  : 500;

    print_header("FlexQL Point Lookup Benchmark");
    printf("  Row count  : %lld\n", row_count);
    printf("  Repetitions: %d per pattern\n", reps);

    FlexQL *db = connect_or_die();

    /* ── Setup ─────────────────────────────────────────────────────── */
    print_section("Table Setup");
    const char *T = "BENCH_POINT";
    printf("  Creating and seeding %lld rows...\n", row_count);
    create_standard_table(db, T, true);
    Timer seed_t; seed_t.start();
    seed_standard_table(db, T, row_count, 500, true);
    printf("  Seed: %.1f ms\n", seed_t.elapsed_ms());

    /* ── PK hash index lookups ─────────────────────────────────────── */
    print_section("PK Hash Index: WHERE ID = val");

    {
        LatencyStats s; s.reserve(reps); Timer w; w.start();
        run_pk_lookups(db, T, row_count, reps, RAND, "rand", s);
        s.set_wall(w.elapsed_ms());
        print_latency("Hash-HIT  random keys", s);
    }
    {
        LatencyStats s; s.reserve(reps); Timer w; w.start();
        run_pk_lookups(db, T, row_count, reps, SEQ, "seq", s);
        s.set_wall(w.elapsed_ms());
        print_latency("Hash-HIT  sequential keys", s);
    }
    {
        LatencyStats s; s.reserve(reps); Timer w; w.start();
        run_pk_lookups(db, T, row_count, reps, HOT, "hot", s);
        s.set_wall(w.elapsed_ms());
        print_latency("Hash-HIT  hot key (cache)", s);
    }
    {
        LatencyStats s; s.reserve(reps); Timer w; w.start();
        run_pk_miss_lookups(db, T, row_count, reps, s);
        s.set_wall(w.elapsed_ms());
        print_latency("Hash-MISS missing keys", s);
    }

    /* ── Full scan: varchar equality ──────────────────────────────── */
    print_section("Full Scan: WHERE LABEL = val (no index)");
    {
        int scan_reps = std::min(reps, 20);  /* full scan is slow — limit reps */
        printf("  Note: limiting to %d reps for full-scan (O(n) operation)\n",
               scan_reps);
        LatencyStats s; s.reserve(scan_reps); Timer w; w.start();
        run_varchar_scan(db, T, row_count, scan_reps, s);
        s.set_wall(w.elapsed_ms());
        print_latency("FullScan varchar-hit", s);
    }

    /* ── Projection comparison ─────────────────────────────────────── */
    print_section("Projection Overhead");
    run_projection_comparison(db, T, row_count, reps);

    /* ── Scaling test ─────────────────────────────────────────────── */
    run_scaling_test(db);

    fql_exec_ignore(db, "DROP TABLE BENCH_POINT;");
    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}
