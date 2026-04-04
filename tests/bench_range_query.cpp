/**
 * bench_range_query.cpp  —  B+ Tree Range Query Benchmark
 *
 * Tests SELECT WHERE col > val / col < val / col BETWEEN at multiple:
 *   - Row counts  : 100K, 500K, 1M, 5M
 *   - Selectivity : 0.01% / 0.1% / 1% / 10% / 50% of rows matching
 *   - Operators   : >, <, >=, <=
 *   - Columns     : indexed DECIMAL (VALUE), non-indexed VARCHAR (LABEL)
 *
 * Also measures cache hit performance (same query 2nd time).
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_range_query.cpp src/client/flexql.cpp \
 *       -o tests/bench_range_query -lpthread
 *
 * Run:
 *   ./bin/flexql-server 9000 &
 *   ./tests/bench_range_query [row_count]
 *   # default row_count = 1000000
 */

#include "bench_common.h"

/* ── Selectivity targets: what fraction of rows each query should return ─ */
struct SelectivityCase {
    const char *name;
    double fraction;     /* 0.0–1.0 */
};

static const SelectivityCase CASES[] = {
    { "0.01%",  0.0001 },
    { "0.1%",   0.001  },
    { "1%",     0.01   },
    { "10%",    0.10   },
    { "50%",    0.50   },
};
static const int N_CASES = (int)(sizeof(CASES) / sizeof(CASES[0]));

/* VALUE column range: 1000.0–10999.0  (width = 10000) */
static const double VAL_MIN = 1000.0;
static const double VAL_MAX = 10999.0;
static const double VAL_WIDTH = VAL_MAX - VAL_MIN;

/* ── Build a WHERE VALUE > threshold that returns ~fraction of rows ──── */
static double threshold_for_gt(double fraction) {
    /* rows with VALUE > T  => fraction of [VAL_MIN, VAL_MAX]
       T = VAL_MAX - fraction * VAL_WIDTH */
    return VAL_MAX - fraction * VAL_WIDTH;
}
static double threshold_for_lt(double fraction) {
    return VAL_MIN + fraction * VAL_WIDTH;
}

/* ── Run one query N times, collect latencies ────────────────────────── */
static void run_repeated(FlexQL *db, const char *sql,
                          int repetitions, LatencyStats &stats) {
    for (int i = 0; i < repetitions; i++) {
        long long count = 0;
        char *err = nullptr;

        Timer t; t.start();
        flexql_exec(db, sql, cb_count, &count, &err);
        double us = t.elapsed_us();

        if (err) flexql_free(err);
        stats.record(us);
    }
}

/* ── Range query suite for one row-count ─────────────────────────────── */
static void run_range_suite(FlexQL *db, const char *table,
                             long long row_count, int repetitions) {
    printf("\n  Table: %s  (%lld rows)  reps=%d\n",
           table, row_count, repetitions);
    printf("  %s\n", std::string(70, '-').c_str());

    /* ── GT queries on indexed VALUE column ─────────────────────────── */
    printf("  Operator: VALUE >  (indexed DECIMAL, B+ tree path)\n");
    for (int c = 0; c < N_CASES; c++) {
        double thresh = threshold_for_gt(CASES[c].fraction);
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE VALUE > %.1f;", table, thresh);

        LatencyStats stats;
        stats.reserve(repetitions);
        Timer wall; wall.start();
        run_repeated(db, sql, repetitions, stats);
        stats.set_wall(wall.elapsed_ms());

        char label[64];
        snprintf(label, sizeof(label),
                 "GT  sel=%-5s ~%lld rows",
                 CASES[c].name,
                 (long long)(row_count * CASES[c].fraction));
        print_latency(label, stats);
    }

    /* ── LT queries on indexed VALUE column ─────────────────────────── */
    printf("\n  Operator: VALUE <  (indexed DECIMAL, B+ tree path)\n");
    for (int c = 0; c < N_CASES; c++) {
        double thresh = threshold_for_lt(CASES[c].fraction);
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE VALUE < %.1f;", table, thresh);

        LatencyStats stats;
        stats.reserve(repetitions);
        Timer wall; wall.start();
        run_repeated(db, sql, repetitions, stats);
        stats.set_wall(wall.elapsed_ms());

        char label[64];
        snprintf(label, sizeof(label),
                 "LT  sel=%-5s ~%lld rows",
                 CASES[c].name,
                 (long long)(row_count * CASES[c].fraction));
        print_latency(label, stats);
    }

    /* ── GTE/LTE on indexed VALUE column ──────────────────────────────── */
    printf("\n  Operator: VALUE >= (indexed DECIMAL, boundary inclusive)\n");
    {
        double thresh = threshold_for_gt(0.10);  /* 10% selectivity */
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE VALUE >= %.1f;", table, thresh);
        LatencyStats stats; stats.reserve(repetitions);
        Timer wall; wall.start();
        run_repeated(db, sql, repetitions, stats);
        stats.set_wall(wall.elapsed_ms());
        print_latency("GTE sel=10%", stats);
    }
    {
        double thresh = threshold_for_lt(0.10);
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE VALUE <= %.1f;", table, thresh);
        LatencyStats stats; stats.reserve(repetitions);
        Timer wall; wall.start();
        run_repeated(db, sql, repetitions, stats);
        stats.set_wall(wall.elapsed_ms());
        print_latency("LTE sel=10%", stats);
    }

    /* ── Full table scan fallback: no-index VARCHAR equality ────────── */
    printf("\n  Full scan: LABEL = (non-indexed VARCHAR)\n");
    {
        /* Pick a label near the middle of the table */
        long long mid_id = row_count / 2;
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE LABEL = 'label%lld';",
                 table, mid_id);
        LatencyStats stats; stats.reserve(repetitions);
        Timer wall; wall.start();
        run_repeated(db, sql, repetitions, stats);
        stats.set_wall(wall.elapsed_ms());
        print_latency("FullScan exact-match", stats);
    }

    /* ── Cache hit: run the same query twice ────────────────────────── */
    printf("\n  Cache hit: same VALUE > query run twice\n");
    {
        double thresh = threshold_for_gt(0.01);  /* 1% selectivity */
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM %s WHERE VALUE > %.1f;", table, thresh);

        /* First pass — cache miss */
        LatencyStats miss_stats; miss_stats.reserve(1);
        Timer wm; wm.start();
        run_repeated(db, sql, 1, miss_stats);
        miss_stats.set_wall(wm.elapsed_ms());
        print_latency("CacheMiss (1st run)", miss_stats);

        /* Second pass — should be a cache hit */
        LatencyStats hit_stats; hit_stats.reserve(repetitions);
        Timer wh; wh.start();
        run_repeated(db, sql, repetitions, hit_stats);
        hit_stats.set_wall(wh.elapsed_ms());
        print_latency("CacheHit  (repeated)", hit_stats);
    }
}

int main(int argc, char **argv) {
    long long row_count = (argc > 1) ? atoll(argv[1]) : 1000000LL;
    int repetitions     = (argc > 2) ? atoi(argv[2])  : 50;

    print_header("FlexQL Range Query Benchmark");
    printf("  Row count  : %lld\n", row_count);
    printf("  Repetitions: %d per query\n", repetitions);

    FlexQL *db = connect_or_die();

    /* ── Create and seed the bench table ─────────────────────────────── */
    print_section("Table Setup");
    const char *TABLE = "BENCH_RANGE";

    printf("  Creating table %s...\n", TABLE);
    if (!create_standard_table(db, TABLE, /*with_pk=*/true)) {
        fprintf(stderr, "Failed to create table\n");
        flexql_close(db);
        return 1;
    }

    printf("  Seeding %lld rows (batch=500)...\n", row_count);
    Timer seed_t; seed_t.start();
    if (!seed_standard_table(db, TABLE, row_count, 500, /*print_progress=*/true)) {
        fprintf(stderr, "Failed to seed table\n");
        flexql_close(db);
        return 1;
    }
    printf("  Seed complete: %.1f ms  (%.0f rows/s)\n",
           seed_t.elapsed_ms(),
           row_count * 1000.0 / seed_t.elapsed_ms());

    /* ── Run range query suite ─────────────────────────────────────── */
    print_section("Range Query Results");
    run_range_suite(db, TABLE, row_count, repetitions);

    /* ── Multi-scale: repeat with 10x fewer rows for comparison ─────── */
    if (row_count >= 500000) {
        long long small = row_count / 10;
        const char *SMALL = "BENCH_RANGE_SMALL";

        print_section("Comparison: 10x fewer rows");
        printf("  Creating smaller table %s (%lld rows)...\n", SMALL, small);
        create_standard_table(db, SMALL, true);
        seed_standard_table(db, SMALL, small, 500, false);
        run_range_suite(db, SMALL, small, repetitions);

        fql_exec_ignore(db, "DROP TABLE BENCH_RANGE_SMALL;");
    }

    fql_exec_ignore(db, "DROP TABLE BENCH_RANGE;");
    flexql_close(db);

    printf("\n  Done.\n");
    return 0;
}
