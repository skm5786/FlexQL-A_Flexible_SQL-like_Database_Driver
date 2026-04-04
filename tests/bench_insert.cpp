/**
 * bench_insert.cpp  —  Insert Throughput Benchmark
 *
 * Comprehensively tests INSERT performance across:
 *   - Batch sizes    : 1, 10, 50, 100, 500, 1000, 5000
 *   - Row counts     : 100K, 500K, 1M, 5M, 10M
 *   - Schema variants: with PK (hash+btree overhead), without PK (pure append)
 *   - Column counts  : 3, 5, 10 columns
 *   - Value types    : INT/DECIMAL/VARCHAR mix
 *
 * Reports:
 *   - rows/sec at each batch size  → shows optimal batch size
 *   - PK overhead vs no-PK        → shows index cost
 *   - Column count overhead        → shows serialisation cost per row
 *   - Sustained throughput curve   → shows if throughput degrades over time
 *
 * This is the benchmark the TA cares most about — 10M rows in < 120s.
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_insert.cpp src/client/flexql.cpp \
 *       -o tests/bench_insert -lpthread
 *
 * Run:
 *   ./bin/flexql-server 9000 &
 *   ./tests/bench_insert [row_count]
 *   # default = 10000000
 */

#include "bench_common.h"

/* ── Drop and recreate a table, insert N rows with given batch size ─── */
static double insert_rows(FlexQL *db,
                           const char *table_sql,  /* full CREATE TABLE */
                           const char *table_name,
                           long long n_rows,
                           int batch_size,
                           int n_cols,            /* must match table_sql */
                           bool print_progress = false) {
    fql_exec_ignore(db, (std::string("DROP TABLE ") + table_name + ";").c_str());
    if (!fql_exec(db, table_sql, "create")) return -1.0;

    Timer t; t.start();
    long long inserted = 0;
    long long next_print = n_rows / 5;

    while (inserted < n_rows) {
        std::ostringstream ss;
        ss << "INSERT INTO " << table_name << " VALUES ";

        int in_batch = 0;
        while (in_batch < batch_size && inserted < n_rows) {
            long long id = inserted + 1;
            if (in_batch > 0) ss << ",";
            ss << "(";
            for (int c = 0; c < n_cols; c++) {
                if (c > 0) ss << ",";
                if (c == 0)      ss << id;
                else if (c == 1) ss << (id % 10);
                else if (c == 2) ss << (1000.0 + (id % 10000));
                else if (c == 3) ss << "'lbl" << (id % 100) << "'";
                else             ss << (1700000000LL + id);
            }
            ss << ")";
            inserted++;
            in_batch++;
        }
        ss << ";";

        if (!fql_exec(db, ss.str().c_str(), "insert")) return -1.0;

        if (print_progress && inserted >= next_print) {
            double elapsed = t.elapsed_ms();
            printf("    %lld/%lld  (%.0f rows/s)\n",
                   inserted, n_rows,
                   inserted * 1000.0 / (elapsed > 0 ? elapsed : 1));
            next_print += n_rows / 5;
        }
    }

    return t.elapsed_ms();
}

/* ── Batch size sweep ───────────────────────────────────────────────── */
static void run_batch_sweep(FlexQL *db, long long n_rows) {
    print_section("Batch Size Sweep (5-col no-PK table)");
    printf("  rows=%lld — varying batch size\n\n", n_rows);

    static const char *TABLE = "INS_BATCH";
    static const char *CREATE_SQL =
        "CREATE TABLE INS_BATCH("
        "ID DECIMAL NOT NULL, CAT DECIMAL NOT NULL,"
        "VALUE DECIMAL NOT NULL, LABEL VARCHAR(32) NOT NULL,"
        "TS DECIMAL NOT NULL);";

    static const int BATCH_SIZES[] = {
        1, 10, 50, 100, 250, 500, 1000, 2000, 5000
    };

    printf("  %-12s  %-12s  %-12s  %-12s\n",
           "batch_size", "elapsed(ms)", "rows/sec", "vs batch=1");

    double base_tput = 0.0;

    for (int bs : BATCH_SIZES) {
        double ms = insert_rows(db, CREATE_SQL, TABLE, n_rows, bs, 5);
        if (ms < 0) { printf("  batch=%d FAILED\n", bs); continue; }

        double tput = (ms > 0) ? n_rows * 1000.0 / ms : 0;
        if (bs == 1) base_tput = tput;
        double speedup = (base_tput > 0) ? tput / base_tput : 1.0;

        printf("  %-12d  %-12.1f  %-12.0f  %.1fx\n",
               bs, ms, tput, speedup);
    }

    fql_exec_ignore(db, "DROP TABLE INS_BATCH;");
}

/* ── PK overhead: with vs without primary key ───────────────────────── */
static void run_pk_overhead(FlexQL *db, long long n_rows) {
    print_section("Primary Key Overhead (hash index + B+tree)");
    printf("  rows=%lld  batch=500\n\n", n_rows);

    static const char *NO_PK_SQL =
        "CREATE TABLE INS_NOPK("
        "ID DECIMAL NOT NULL, CAT DECIMAL NOT NULL,"
        "VALUE DECIMAL NOT NULL, LABEL VARCHAR(32) NOT NULL,"
        "TS DECIMAL NOT NULL);";

    static const char *WITH_PK_SQL =
        "CREATE TABLE INS_PK("
        "ID DECIMAL PRIMARY KEY NOT NULL, CAT DECIMAL NOT NULL,"
        "VALUE DECIMAL NOT NULL, LABEL VARCHAR(32) NOT NULL,"
        "TS DECIMAL NOT NULL);";

    double ms_nopk = insert_rows(db, NO_PK_SQL,   "INS_NOPK", n_rows, 500, 5);
    double ms_pk   = insert_rows(db, WITH_PK_SQL,  "INS_PK",   n_rows, 500, 5);

    if (ms_nopk > 0 && ms_pk > 0) {
        double tput_nopk = n_rows * 1000.0 / ms_nopk;
        double tput_pk   = n_rows * 1000.0 / ms_pk;
        printf("  No PK    : %.1f ms  %.0f rows/s\n", ms_nopk, tput_nopk);
        printf("  With PK  : %.1f ms  %.0f rows/s\n", ms_pk,   tput_pk);
        printf("  Overhead : %.1fx slower with PK\n", ms_pk / ms_nopk);
    }

    fql_exec_ignore(db, "DROP TABLE INS_NOPK;");
    fql_exec_ignore(db, "DROP TABLE INS_PK;");
}

/* ── Column count overhead ──────────────────────────────────────────── */
static void run_col_count_overhead(FlexQL *db, long long n_rows) {
    print_section("Column Count Overhead");
    printf("  rows=%lld  batch=500\n\n", n_rows);

    static const struct {
        int cols;
        const char *create_sql;
        const char *table_name;
    } SCHEMAS[] = {
        { 3,
          "CREATE TABLE INS_C3(A DECIMAL NOT NULL,"
          "B DECIMAL NOT NULL, C DECIMAL NOT NULL);",
          "INS_C3" },
        { 5,
          "CREATE TABLE INS_C5(A DECIMAL NOT NULL, B DECIMAL NOT NULL,"
          "C DECIMAL NOT NULL, D VARCHAR(32) NOT NULL, E DECIMAL NOT NULL);",
          "INS_C5" },
        { 10,
          "CREATE TABLE INS_C10(A DECIMAL NOT NULL, B DECIMAL NOT NULL,"
          "C DECIMAL NOT NULL, D VARCHAR(32) NOT NULL, E DECIMAL NOT NULL,"
          "F DECIMAL NOT NULL, G DECIMAL NOT NULL, H VARCHAR(32) NOT NULL,"
          "I DECIMAL NOT NULL, J DECIMAL NOT NULL);",
          "INS_C10" },
    };

    printf("  %-10s  %-12s  %-12s\n", "cols", "elapsed(ms)", "rows/sec");

    for (const auto &s : SCHEMAS) {
        double ms = insert_rows(db, s.create_sql, s.table_name,
                                n_rows, 500, s.cols);
        if (ms < 0) { printf("  %d cols FAILED\n", s.cols); continue; }
        printf("  %-10d  %-12.1f  %-12.0f\n",
               s.cols, ms, n_rows * 1000.0 / ms);
        fql_exec_ignore(db, (std::string("DROP TABLE ") + s.table_name + ";").c_str());
    }
}

/* ── Sustained throughput: throughput every 1 second over 30s ──────── */
static void run_sustained_test(FlexQL *db, long long total_rows) {
    print_section("Sustained Throughput (rows/s every second for ~30s)");
    printf("  total_rows=%lld  batch=500\n\n", total_rows);

    static const char *TABLE = "INS_SUSTAINED";
    fql_exec_ignore(db, "DROP TABLE INS_SUSTAINED;");
    fql_exec(db,
             "CREATE TABLE INS_SUSTAINED("
             "ID DECIMAL NOT NULL, CAT DECIMAL NOT NULL,"
             "VALUE DECIMAL NOT NULL, LABEL VARCHAR(32) NOT NULL,"
             "TS DECIMAL NOT NULL);",
             "create_sustained");

    long long inserted = 0;
    Timer global; global.start();
    Timer window; window.start();
    long long window_rows = 0;
    int       tick = 0;

    printf("  %5s  %10s  %12s\n", "sec", "window_rows", "rows/sec");

    while (inserted < total_rows) {
        std::ostringstream ss;
        ss << "INSERT INTO INS_SUSTAINED VALUES ";
        int in_batch = 0;
        while (in_batch < 500 && inserted < total_rows) {
            long long id = inserted + 1;
            if (in_batch > 0) ss << ",";
            ss << "(" << id << ","
               << (id % 10) << ","
               << (1000.0 + (id % 10000)) << ","
               << "'lbl" << (id % 100) << "',"
               << (1700000000LL + id) << ")";
            inserted++; in_batch++; window_rows++;
        }
        ss << ";";
        fql_exec(db, ss.str().c_str(), "ins_sustained");

        double wms = window.elapsed_ms();
        if (wms >= 1000.0) {
            tick++;
            printf("  %5d  %10lld  %12.0f\n",
                   tick, window_rows, window_rows * 1000.0 / wms);
            window_rows = 0;
            window.start();
        }
    }

    double total_ms = global.elapsed_ms();
    printf("\n  Total: %lld rows in %.1f ms = %.0f rows/s\n",
           total_rows, total_ms, total_rows * 1000.0 / total_ms);

    fql_exec_ignore(db, "DROP TABLE INS_SUSTAINED;");
}

/* ── TA benchmark target: 10M rows under 120s ───────────────────────── */
static void run_ta_target(FlexQL *db) {
    print_section("TA Target: 10M rows in under 120 seconds");

    static const long long TARGET = 10000000LL;

    printf("  Creating BIG_USERS table (5 col, no PK)...\n");
    fql_exec_ignore(db, "DROP TABLE BIG_USERS;");
    fql_exec(db,
             "CREATE TABLE BIG_USERS("
             "ID DECIMAL NOT NULL, NAME VARCHAR(64) NOT NULL,"
             "EMAIL VARCHAR(64) NOT NULL, BALANCE DECIMAL NOT NULL,"
             "EXPIRES_AT DECIMAL NOT NULL);",
             "create_big_users");

    printf("  Inserting %lld rows (batch=500)...\n\n", TARGET);

    Timer t; t.start();
    long long inserted = 0;
    long long next_print = TARGET / 10;

    while (inserted < TARGET) {
        std::ostringstream ss;
        ss << "INSERT INTO BIG_USERS VALUES ";
        int in_batch = 0;
        while (in_batch < 500 && inserted < TARGET) {
            long long id = inserted + 1;
            if (in_batch > 0) ss << ",";
            ss << "(" << id
               << ",'user" << id << "'"
               << ",'user" << id << "@mail.com'"
               << "," << (1000.0 + (id % 10000))
               << ",1893456000)";
            inserted++; in_batch++;
        }
        ss << ";";
        if (!fql_exec(db, ss.str().c_str(), "ta_insert")) break;

        if (inserted >= next_print) {
            double elapsed = t.elapsed_ms();
            printf("  Progress: %lld/%lld  %.0f rows/s  eta=%.1fs\n",
                   inserted, TARGET,
                   inserted * 1000.0 / (elapsed > 0 ? elapsed : 1),
                   (TARGET - inserted) / (inserted * 1000.0 / (elapsed > 0 ? elapsed : 1)));
            next_print += TARGET / 10;
        }
    }

    double ms = t.elapsed_ms();
    double tput = (ms > 0) ? TARGET * 1000.0 / ms : 0;

    printf("\n  ─────────────────────────────────────────\n");
    printf("  Rows     : %lld\n", TARGET);
    printf("  Elapsed  : %.1f ms  (%.2f s)\n", ms, ms / 1000.0);
    printf("  Throughput: %.0f rows/s\n", tput);
    printf("  Target   : 120,000 ms (120 s)\n");
    printf("  Result   : %s\n", (ms <= 120000) ? "PASS" : "FAIL");
    printf("  ─────────────────────────────────────────\n");

    fql_exec_ignore(db, "DROP TABLE BIG_USERS;");
}

int main(int argc, char **argv) {
    long long row_count = (argc > 1) ? atoll(argv[1]) : 10000000LL;

    print_header("FlexQL Insert Throughput Benchmark");
    printf("  Main row count: %lld\n", row_count);

    FlexQL *db = connect_or_die();

    /* Quick sub-benchmarks use fewer rows so they finish fast */
    long long sub_rows = std::min(row_count, 500000LL);

    run_batch_sweep(db, sub_rows);
    run_pk_overhead(db, sub_rows);
    run_col_count_overhead(db, sub_rows);

    if (row_count >= 1000000LL)
        run_sustained_test(db, std::min(row_count, 3000000LL));

    run_ta_target(db);

    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}
