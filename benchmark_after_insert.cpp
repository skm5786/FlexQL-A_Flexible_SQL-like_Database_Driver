/**
 * benchmark_after_insert.cpp  —  Post-Insert Query Benchmark (Fixed)
 *
 * PROBLEM WITH ORIGINAL:
 *   The original benchmark connected fresh and immediately queried BIG_USERS.
 *   But BIG_USERS was created inside BENCH_<fd> (an ephemeral per-connection
 *   session database that gets destroyed when the inserting connection closes).
 *   So BIG_USERS doesn't exist in the new connection's database context.
 *
 * FIX:
 *   Option A (used here): Accept a database name argument. Before running
 *   queries, do "USE <dbname>;" to switch to the database containing BIG_USERS.
 *   Default database: BIGTEST (created by run_after_insert_benchmark.sh).
 *
 *   Option B: Modify benchmark_flexql to leave the data in a named DB.
 *   (Requires changing server.cpp session DB to be persistent — covered by
 *    the --persistent flag approach below.)
 *
 * USAGE:
 *   # Standard: run against BIGTEST db (seeded by run_after_insert_benchmark.sh)
 *   ./benchmark_after_insert 100000 1
 *   ./benchmark_after_insert 10000000 4
 *
 *   # With explicit db name:
 *   ./benchmark_after_insert 100000 1 BIGTEST
 *
 * COMPILE:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       benchmark_after_insert.cpp src/client/flexql.cpp \
 *       -o benchmark_after_insert_bin -lpthread
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

struct QueryStats {
    long long rows = 0;
};

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc; (void)argv; (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) stats->rows++;
    return 0;
}

static bool open_db(FlexQL **db, const string &use_db) {
    if (flexql_open("127.0.0.1", 9000, db) != FLEXQL_OK) return false;
    if (!use_db.empty()) {
        string use_sql = "USE " + use_db + ";";
        char *err = nullptr;
        int rc = flexql_exec(*db, use_sql.c_str(), nullptr, nullptr, &err);
        if (rc != FLEXQL_OK) {
            cerr << "[ERROR] USE " << use_db << " failed: " << (err ? err : "?") << "\n";
            if (err) flexql_free(err);
            flexql_close(*db);
            *db = nullptr;
            return false;
        }
    }
    return true;
}

static bool run_concurrent_query_benchmark(const string &label, int client_count,
                                           const vector<string> &sqls,
                                           const string &use_db) {
    atomic<long long> total_rows{0};
    atomic<bool> failed{false};
    mutex err_mu;
    string first_error;
    vector<thread> workers;
    auto bench_start = high_resolution_clock::now();

    for (int i = 0; i < client_count; ++i) {
        workers.emplace_back([&, i]() {
            FlexQL *worker_db = nullptr;
            if (!open_db(&worker_db, use_db)) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) first_error = "client open/USE failed";
                return;
            }

            QueryStats stats;
            char *errMsg = nullptr;
            if (flexql_exec(worker_db, sqls[i].c_str(), count_rows_callback, &stats, &errMsg) != FLEXQL_OK) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) first_error = errMsg ? errMsg : "unknown error";
                if (errMsg) flexql_free(errMsg);
                flexql_close(worker_db);
                return;
            }

            total_rows += stats.rows;
            flexql_close(worker_db);
        });
    }

    for (auto &worker : workers) worker.join();

    auto bench_end = high_resolution_clock::now();
    if (failed.load()) {
        cout << "[FAIL] " << label << " -> " << first_error << "\n";
        return false;
    }

    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    cout << "[PASS] " << label << " | clients=" << client_count
         << " | total_rows=" << total_rows.load()
         << " | elapsed=" << elapsed << " ms\n";
    return true;
}

int main(int argc, char **argv) {
    int client_count = 1;
    long long target_rows = 100000;
    string use_db = "BIGTEST";  /* default persistent DB name */

    if (argc > 1) {
        target_rows = atoll(argv[1]);
        if (target_rows <= 0) {
            cout << "Invalid row count. Use a positive integer.\n";
            return 1;
        }
    }
    if (argc > 2) {
        client_count = atoi(argv[2]);
        if (client_count <= 0) {
            cout << "Invalid client count. Use a positive integer.\n";
            return 1;
        }
    }
    if (argc > 3) {
        use_db = argv[3];
    }

    cout << "Running Post-Insert Query Benchmarks...\n";
    cout << "Database: " << use_db << "\n";
    cout << "Assuming table 'BIG_USERS' contains " << target_rows << " rows.\n";
    cout << "Client count: " << client_count << "\n\n";

    /* Verify connectivity and database existence first */
    {
        FlexQL *test_db = nullptr;
        if (!open_db(&test_db, use_db)) {
            cerr << "ERROR: Cannot connect or USE database '" << use_db << "'.\n";
            cerr << "Make sure:\n";
            cerr << "  1. The server is running on port 9000\n";
            cerr << "  2. Database '" << use_db << "' exists\n";
            cerr << "  3. Table BIG_USERS exists in that database\n";
            cerr << "\nRun: ./run_after_insert_benchmark.sh " << target_rows << " " << client_count << "\n";
            return 1;
        }

        /* Quick row count check */
        long long row_count = 0;
        char *err = nullptr;
        flexql_exec(test_db, "SELECT ID FROM BIG_USERS WHERE ID = 1;",
                    count_rows_callback, &row_count, &err);
        if (err) flexql_free(err);
        flexql_close(test_db);

        if (row_count == 0) {
            cerr << "WARNING: BIG_USERS appears empty or row 1 not found.\n";
            cerr << "Results may not be meaningful.\n\n";
        }
    }

    vector<vector<string>> test_queries;
    vector<string> test_labels;

    auto add_test = [&](const string& label, const string& sql_pattern, bool use_probe) {
        vector<string> sqls;
        for (int i = 0; i < client_count; ++i) {
            long long probe_id = ((long long)i * target_rows / client_count) + 1;
            if (probe_id > target_rows) probe_id = target_rows;
            string sql = sql_pattern;
            if (use_probe) {
                size_t pos = sql.find("<PROBE>");
                if (pos != string::npos) sql.replace(pos, 7, to_string(probe_id));
            }
            sqls.push_back(sql);
        }
        test_queries.push_back(sqls);
        test_labels.push_back(label);
    };

    // 1. Full Table Scan
    add_test("Full Table Select", "SELECT * FROM BIG_USERS;", false);
    // 2. Exact Match (ID = X) -> uses primary index (if any)
    add_test("Primary Index Lookup (ID = X)", "SELECT ID, NAME, BALANCE FROM BIG_USERS WHERE ID = <PROBE>;", true);
    // 3. Inequality (ID != 1)
    add_test("Inequality Check (ID != 1)", "SELECT ID, NAME FROM BIG_USERS WHERE ID != 1;", false);
    // 4. Greater Than (ID > X)
    add_test("Greater Than (ID > X)", "SELECT ID FROM BIG_USERS WHERE ID > <PROBE>;", true);
    // 5. Less Than (ID < X)
    add_test("Less Than (ID < X)", "SELECT ID FROM BIG_USERS WHERE ID < <PROBE>;", true);
    // 6. Greater Than or Equal (ID >= X)
    add_test("Greater Than Eq (ID >= X)", "SELECT ID FROM BIG_USERS WHERE ID >= <PROBE>;", true);
    // 7. Less Than or Equal (ID <= X)
    add_test("Less Than Eq (ID <= X)", "SELECT ID FROM BIG_USERS WHERE ID <= <PROBE>;", true);
    // 8. Balance Match (BALANCE = X)
    add_test("Non-Index Column Match (BALANCE = 1500)", "SELECT ID FROM BIG_USERS WHERE BALANCE = 1500;", false);
    // 9. Balance Greater Than (BALANCE > X)
    add_test("Non-Index Col Greater (BALANCE > 9000)", "SELECT ID FROM BIG_USERS WHERE BALANCE > 9000;", false);
    // 10. String Match (EMAIL = X)
    add_test("String Exact Match (EMAIL = 'user1@mail.com')", "SELECT ID FROM BIG_USERS WHERE EMAIL = 'user1@mail.com';", false);
    // 11. String Inequality (NAME != X)
    add_test("String Inequality (NAME != 'user1')", "SELECT ID FROM BIG_USERS WHERE NAME != 'user1';", false);

    for (size_t i = 0; i < test_queries.size(); ++i) {
        if (!run_concurrent_query_benchmark(test_labels[i], client_count, test_queries[i], use_db)) {
            cout << "Benchmark suite failed at: " << test_labels[i] << "\n";
            return 1;
        }
    }

    cout << "\nAll benchmarks completed successfully!\n";
    return 0;
}