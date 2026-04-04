/**
 * bench_common.h  —  Shared helpers for all FlexQL benchmark programs.
 *
 * Included by every bench_*.cpp file.  Provides:
 *   - Timer  : wall-clock measurement with lap support
 *   - Stats  : running mean/min/max/p50/p95/p99 for latency distributions
 *   - Report : consistent result printing
 *   - Table  : one-call table creation + bulk seeding helpers
 *   - Assert : lightweight test assertions that don't abort the benchmark
 */

#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <ctime>
#include <cassert>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <pthread.h>
#include "flexql.h"

/* ── Compiler-hint: inline everything in this header ───────────────────── */
#ifdef __GNUC__
#  define BENCH_INLINE __attribute__((always_inline)) inline
#else
#  define BENCH_INLINE inline
#endif

/* ════════════════════════════════════════════════════════════════════════
 *  TIMER
 * ════════════════════════════════════════════════════════════════════════ */
struct Timer {
    using Clock = std::chrono::steady_clock;
    using TP    = std::chrono::time_point<Clock>;

    TP start_tp;
    TP lap_tp;

    void start() {
        start_tp = lap_tp = Clock::now();
    }

    /* Milliseconds since start() */
    double elapsed_ms() const {
        using namespace std::chrono;
        return duration<double, std::milli>(Clock::now() - start_tp).count();
    }

    /* Milliseconds since last lap() call (or start()) */
    double lap_ms() {
        using namespace std::chrono;
        TP now = Clock::now();
        double ms = duration<double, std::milli>(now - lap_tp).count();
        lap_tp = now;
        return ms;
    }

    /* Microseconds since start() */
    double elapsed_us() const {
        using namespace std::chrono;
        return duration<double, std::micro>(Clock::now() - start_tp).count();
    }
};

/* ════════════════════════════════════════════════════════════════════════
 *  LATENCY SAMPLE COLLECTOR
 *  Collects individual query latencies (microseconds) then computes
 *  p50/p95/p99 percentiles, mean, and throughput.
 * ════════════════════════════════════════════════════════════════════════ */
struct LatencyStats {
    std::vector<double> samples_us;   /* per-operation latency in µs */
    long long           total_ops = 0;
    double              wall_ms   = 0.0;

    void reserve(size_t n) { samples_us.reserve(n); }

    void record(double us) {
        samples_us.push_back(us);
        total_ops++;
    }

    void set_wall(double ms) { wall_ms = ms; }

    struct Result {
        double mean_us, min_us, p50_us, p95_us, p99_us, max_us;
        double throughput_ops_per_sec;
        long long total_ops;
        double wall_ms;
    };

    Result compute() {
        Result r{};
        r.total_ops = total_ops;
        r.wall_ms   = wall_ms;
        if (samples_us.empty()) return r;

        std::vector<double> sorted = samples_us;
        std::sort(sorted.begin(), sorted.end());

        double sum = 0.0;
        for (double v : sorted) sum += v;

        auto pct = [&](double p) -> double {
            size_t idx = (size_t)(p * sorted.size() / 100.0);
            if (idx >= sorted.size()) idx = sorted.size() - 1;
            return sorted[idx];
        };

        r.mean_us = sum / sorted.size();
        r.min_us  = sorted.front();
        r.p50_us  = pct(50.0);
        r.p95_us  = pct(95.0);
        r.p99_us  = pct(99.0);
        r.max_us  = sorted.back();
        r.throughput_ops_per_sec = (wall_ms > 0.0)
            ? (total_ops * 1000.0 / wall_ms)
            : 0.0;
        return r;
    }
};

/* ════════════════════════════════════════════════════════════════════════
 *  REPORTING
 * ════════════════════════════════════════════════════════════════════════ */
static void print_header(const char *title) {
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║  %-60s║\n", title);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
}

static void print_section(const char *name) {
    printf("\n── %s %s\n", name,
           std::string(60 - strlen(name) - 4, '─').c_str());
}

static void print_latency(const char *label, LatencyStats &stats) {
    auto r = stats.compute();
    printf("  %-28s  ops=%-8lld  wall=%-8.1f ms  tput=%-10.0f ops/s\n"
           "  %-28s  lat(µs): mean=%-7.1f  p50=%-7.1f  p95=%-7.1f  p99=%-7.1f  max=%-7.1f\n",
           label, r.total_ops, r.wall_ms, r.throughput_ops_per_sec,
           "", r.mean_us, r.p50_us, r.p95_us, r.p99_us, r.max_us);
}

static void print_throughput(const char *label, long long rows,
                              double elapsed_ms) {
    double tput = (elapsed_ms > 0) ? rows * 1000.0 / elapsed_ms : 0.0;
    printf("  %-28s  rows=%-10lld  time=%-8.1f ms  tput=%.0f rows/s\n",
           label, rows, elapsed_ms, tput);
}

static void print_pass(const char *label) {
    printf("  [PASS] %s\n", label);
}
static void print_fail(const char *label, const char *reason = "") {
    printf("  [FAIL] %s  %s\n", label, reason);
}

/* ════════════════════════════════════════════════════════════════════════
 *  FLEXQL HELPERS
 * ════════════════════════════════════════════════════════════════════════ */

/* Exec and die on error */
static bool fql_exec(FlexQL *db, const char *sql,
                     const char *context = nullptr) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql, nullptr, nullptr, &err);
    if (rc != FLEXQL_OK) {
        fprintf(stderr, "  SQL error [%s]: %s\nSQL: %s\n",
                context ? context : "?",
                err ? err : "unknown",
                sql);
        if (err) flexql_free(err);
        return false;
    }
    return true;
}

/* Exec and silently ignore errors (for DROP TABLE IF NOT EXISTS etc.) */
static void fql_exec_ignore(FlexQL *db, const char *sql) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql, nullptr, nullptr, &err);
    (void)rc;
    if (err) flexql_free(err);
}

/* Count-rows callback */
static int cb_count(void *data, int /*argc*/, char ** /*argv*/,
                    char ** /*cols*/) {
    (*reinterpret_cast<long long*>(data))++;
    return 0;
}

/* Collect single column values */
struct ColCollector {
    std::vector<std::string> values;
    int target_col;   /* 0-based index */
};
static int cb_collect_col(void *data, int argc, char **argv, char **) {
    ColCollector *c = reinterpret_cast<ColCollector*>(data);
    if (c->target_col < argc && argv[c->target_col])
        c->values.emplace_back(argv[c->target_col]);
    return 0;
}

/* ── Bulk seed: insert N rows into an already-created table ────────────── */
/* Schema expected: (ID DECIMAL, CATEGORY INT, VALUE DECIMAL,
                     LABEL VARCHAR(64), CREATED_AT DECIMAL)
   This is the "standard bench table" used by all benchmarks. */
static bool seed_standard_table(FlexQL *db, const char *table_name,
                                 long long row_count,
                                 int batch_size = 500,
                                 bool print_progress = true) {
    long long inserted = 0;
    long long next_print = row_count / 10;
    if (next_print <= 0) next_print = row_count;

    Timer t; t.start();

    while (inserted < row_count) {
        std::ostringstream ss;
        ss << "INSERT INTO " << table_name << " VALUES ";

        int in_batch = 0;
        while (in_batch < batch_size && inserted < row_count) {
            long long id   = inserted + 1;
            int  cat       = (int)(id % 10);          /* 0–9 */
            double val     = 1000.0 + (id % 10000);   /* 1000–10999 */
            long long ts   = 1700000000LL + id;        /* monotone timestamps */

            if (in_batch > 0) ss << ",";
            ss << "(" << id
               << "," << cat
               << "," << val
               << ",'label" << id << "'"
               << "," << ts << ")";
            inserted++;
            in_batch++;
        }
        ss << ";";

        if (!fql_exec(db, ss.str().c_str(), "seed")) return false;

        if (print_progress && inserted >= next_print) {
            double elapsed = t.elapsed_ms();
            printf("    seeded %lld/%lld  (%.0f rows/s)\n",
                   inserted, row_count,
                   inserted * 1000.0 / (elapsed > 0 ? elapsed : 1));
            next_print += row_count / 10;
        }
    }
    return true;
}

/* ── Create the standard bench table (drop first if it exists) ────────── */
static bool create_standard_table(FlexQL *db, const char *name,
                                   bool with_pk = true) {
    std::string drop = std::string("DROP TABLE ") + name + ";";
    fql_exec_ignore(db, drop.c_str());

    std::string create = std::string("CREATE TABLE ") + name + "(";
    if (with_pk)
        create += "ID DECIMAL PRIMARY KEY NOT NULL,";
    else
        create += "ID DECIMAL NOT NULL,";
    create += "CATEGORY INT NOT NULL,"
              "VALUE DECIMAL NOT NULL,"
              "LABEL VARCHAR(64) NOT NULL,"
              "CREATED_AT DECIMAL NOT NULL);";

    return fql_exec(db, create.c_str(), "create_standard_table");
}

/* ── Connect or die ─────────────────────────────────────────────────────── */
static FlexQL *connect_or_die(const char *host = "127.0.0.1",
                               int port = 9000) {
    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to FlexQL at %s:%d\n", host, port);
        exit(1);
    }
    return db;
}

/* ── Simple assertion that counts pass/fail ─────────────────────────────── */
struct TestCounter {
    int pass = 0;
    int fail = 0;

    void check(bool cond, const char *label) {
        if (cond) { print_pass(label); pass++; }
        else      { print_fail(label); fail++; }
    }

    void summary() const {
        printf("\n  Results: %d/%d passed", pass, pass + fail);
        if (fail > 0) printf("  (%d FAILED)", fail);
        printf("\n");
    }
};

/* ── Thread argument helper ─────────────────────────────────────────────── */
struct ThreadResult {
    long long ops_completed = 0;
    long long errors        = 0;
    double    elapsed_ms    = 0.0;
};
