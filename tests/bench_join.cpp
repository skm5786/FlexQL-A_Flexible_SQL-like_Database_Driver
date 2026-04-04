/**
 * bench_join.cpp  —  INNER JOIN Benchmark
 *
 * Tests the nested-loop INNER JOIN executor path at varying:
 *   - Outer table size: 10K, 50K, 100K rows
 *   - Inner table size: 100, 1K, 10K rows (the "dimension" table)
 *   - Join selectivity: each outer row matches 1 / 5 / 10 inner rows
 *   - With and without a WHERE clause on the join result
 *
 * FlexQL's join is a naive nested-loop (O(outer × inner) per query).
 * These benchmarks quantify that cost and show where caching helps.
 *
 * Scenarios:
 *   1. Orders × Customers   (many-to-one): each order has one customer
 *   2. Products × Categories (many-to-one): 10K products, 20 categories
 *   3. Large × Small with WHERE filter on join result
 *   4. Cache hit: same join repeated
 *
 * Build:
 *   g++ -std=c++17 -O3 -march=native -I./include \
 *       tests/bench_join.cpp src/client/flexql.cpp \
 *       -o tests/bench_join -lpthread
 *
 * Run:
 *   ./bin/flexql-server 9000 &
 *   ./tests/bench_join [orders_count]
 *   # default orders_count = 50000
 */

#include "bench_common.h"

/* ── Create ORDERS table: (ORDER_ID, CUSTOMER_ID, AMOUNT, STATUS) ──── */
static bool create_orders(FlexQL *db, const char *name,
                            long long count, long long num_customers) {
    fql_exec_ignore(db, (std::string("DROP TABLE ") + name + ";").c_str());

    std::string sql = std::string("CREATE TABLE ") + name +
        "(ORDER_ID DECIMAL PRIMARY KEY NOT NULL,"
        " CUSTOMER_ID DECIMAL NOT NULL,"
        " AMOUNT DECIMAL NOT NULL,"
        " STATUS DECIMAL NOT NULL);";
    if (!fql_exec(db, sql.c_str(), "create_orders")) return false;

    long long inserted = 0;
    while (inserted < count) {
        int bs = 500;
        std::ostringstream ss;
        ss << "INSERT INTO " << name << " VALUES ";
        int in_batch = 0;
        while (in_batch < bs && inserted < count) {
            long long id  = inserted + 1;
            long long cid = (id % num_customers) + 1;
            double amt    = 100.0 + (id % 9900);
            int status    = (int)(id % 3);  /* 0=pending 1=shipped 2=done */
            if (in_batch > 0) ss << ",";
            ss << "(" << id << "," << cid << "," << amt << "," << status << ")";
            inserted++; in_batch++;
        }
        ss << ";";
        if (!fql_exec(db, ss.str().c_str(), "seed_orders")) return false;
    }
    return true;
}

/* ── Create CUSTOMERS table: (CUSTOMER_ID, REGION, TIER) ─────────── */
static bool create_customers(FlexQL *db, const char *name, long long count) {
    fql_exec_ignore(db, (std::string("DROP TABLE ") + name + ";").c_str());

    std::string sql = std::string("CREATE TABLE ") + name +
        "(CUSTOMER_ID DECIMAL PRIMARY KEY NOT NULL,"
        " REGION DECIMAL NOT NULL,"
        " TIER DECIMAL NOT NULL);";
    if (!fql_exec(db, sql.c_str(), "create_customers")) return false;

    std::ostringstream ss;
    ss << "INSERT INTO " << name << " VALUES ";
    for (long long i = 1; i <= count; i++) {
        if (i > 1) ss << ",";
        ss << "(" << i << "," << (i % 5) << "," << (i % 3) << ")";
    }
    ss << ";";
    return fql_exec(db, ss.str().c_str(), "seed_customers");
}

/* ── Create PRODUCTS / CATEGORIES tables ────────────────────────────── */
static bool create_products(FlexQL *db, const char *prod_name,
                              const char *cat_name,
                              long long n_products, long long n_categories) {
    fql_exec_ignore(db, (std::string("DROP TABLE ") + prod_name + ";").c_str());
    fql_exec_ignore(db, (std::string("DROP TABLE ") + cat_name + ";").c_str());

    /* Categories */
    {
        std::string sql = std::string("CREATE TABLE ") + cat_name +
            "(CAT_ID DECIMAL PRIMARY KEY NOT NULL, NAME DECIMAL NOT NULL);";
        fql_exec(db, sql.c_str(), "create_cat");
        std::ostringstream ss;
        ss << "INSERT INTO " << cat_name << " VALUES ";
        for (long long i = 1; i <= n_categories; i++) {
            if (i > 1) ss << ",";
            ss << "(" << i << "," << i << ")";
        }
        ss << ";";
        fql_exec(db, ss.str().c_str(), "seed_cat");
    }

    /* Products */
    {
        std::string sql = std::string("CREATE TABLE ") + prod_name +
            "(PROD_ID DECIMAL PRIMARY KEY NOT NULL,"
            " CAT_ID DECIMAL NOT NULL, PRICE DECIMAL NOT NULL);";
        fql_exec(db, sql.c_str(), "create_prod");
        long long inserted = 0;
        while (inserted < n_products) {
            int bs = 500;
            std::ostringstream ss;
            ss << "INSERT INTO " << prod_name << " VALUES ";
            int in_batch = 0;
            while (in_batch < bs && inserted < n_products) {
                long long id  = inserted + 1;
                long long cat = (id % n_categories) + 1;
                double price  = 10.0 + (id % 990);
                if (in_batch > 0) ss << ",";
                ss << "(" << id << "," << cat << "," << price << ")";
                inserted++; in_batch++;
            }
            ss << ";";
            fql_exec(db, ss.str().c_str(), "seed_prod");
        }
    }
    return true;
}

/* ── Run one join query N times ─────────────────────────────────────── */
static void bench_join_query(FlexQL *db, const char *label,
                               const char *sql, int reps) {
    LatencyStats stats; stats.reserve(reps);
    Timer wall; wall.start();

    for (int i = 0; i < reps; i++) {
        long long c = 0; char *err = nullptr;
        Timer t; t.start();
        flexql_exec(db, sql, cb_count, &c, &err);
        stats.record(t.elapsed_us());
        if (err) flexql_free(err);
    }
    stats.set_wall(wall.elapsed_ms());
    print_latency(label, stats);
}

int main(int argc, char **argv) {
    long long n_orders   = (argc > 1) ? atoll(argv[1]) : 50000LL;
    int       reps       = (argc > 2) ? atoi(argv[2])  : 20;

    print_header("FlexQL INNER JOIN Benchmark");
    printf("  Orders     : %lld\n", n_orders);
    printf("  Repetitions: %d per query\n", reps);

    FlexQL *db = connect_or_die();

    /* ── Scenario 1: Orders × Customers (many-to-one) ────────────── */
    print_section("Scenario 1: Orders INNER JOIN Customers (many-to-one)");

    static const long long CUST_SIZES[] = { 10, 100, 1000 };
    for (long long n_cust : CUST_SIZES) {
        printf("\n  customers=%lld  orders=%lld\n", n_cust, n_orders);

        create_customers(db, "CUSTOMERS", n_cust);
        create_orders(db, "ORDERS", n_orders, n_cust);

        /* Basic join */
        bench_join_query(db,
            "JOIN no-filter",
            "SELECT * FROM ORDERS INNER JOIN CUSTOMERS "
            "ON ORDERS.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID;",
            reps);

        /* Join with WHERE on outer table */
        bench_join_query(db,
            "JOIN WHERE AMOUNT > 9000",
            "SELECT * FROM ORDERS INNER JOIN CUSTOMERS "
            "ON ORDERS.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID "
            "WHERE ORDERS.AMOUNT > 9000;",
            reps);

        /* Join with WHERE on inner table */
        bench_join_query(db,
            "JOIN WHERE REGION = 2",
            "SELECT * FROM ORDERS INNER JOIN CUSTOMERS "
            "ON ORDERS.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID "
            "WHERE CUSTOMERS.REGION = 2;",
            reps);

        /* Cache hit: repeat the no-filter join */
        bench_join_query(db,
            "Cache HIT (no-filter repeat)",
            "SELECT * FROM ORDERS INNER JOIN CUSTOMERS "
            "ON ORDERS.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID;",
            reps);

        fql_exec_ignore(db, "DROP TABLE ORDERS;");
        fql_exec_ignore(db, "DROP TABLE CUSTOMERS;");
    }

    /* ── Scenario 2: Products × Categories (fan-out join) ─────────── */
    print_section("Scenario 2: Products INNER JOIN Categories");

    static const long long N_CATS[] = { 5, 20, 100 };
    long long n_products = n_orders;  /* same scale */

    for (long long n_cat : N_CATS) {
        printf("\n  products=%lld  categories=%lld\n", n_products, n_cat);

        create_products(db, "PRODUCTS", "CATEGORIES", n_products, n_cat);

        bench_join_query(db,
            "Products JOIN Categories",
            "SELECT * FROM PRODUCTS INNER JOIN CATEGORIES "
            "ON PRODUCTS.CAT_ID = CATEGORIES.CAT_ID;",
            reps);

        bench_join_query(db,
            "Products JOIN Cat WHERE PRICE > 500",
            "SELECT * FROM PRODUCTS INNER JOIN CATEGORIES "
            "ON PRODUCTS.CAT_ID = CATEGORIES.CAT_ID "
            "WHERE PRODUCTS.PRICE > 500;",
            reps);

        fql_exec_ignore(db, "DROP TABLE PRODUCTS;");
        fql_exec_ignore(db, "DROP TABLE CATEGORIES;");
    }

    /* ── Scenario 3: Scaling — join result size ──────────────────── */
    print_section("Scenario 3: Join Result Size Scaling");

    static const long long SCALES[] = {
        1000, 5000, 10000, 50000
    };

    long long n_cust_fixed = 100;
    create_customers(db, "CUSTOMERS", n_cust_fixed);

    for (long long sz : SCALES) {
        create_orders(db, "ORDERS", sz, n_cust_fixed);

        char label[64];
        snprintf(label, sizeof(label), "orders=%-6lld", sz);
        bench_join_query(db, label,
            "SELECT * FROM ORDERS INNER JOIN CUSTOMERS "
            "ON ORDERS.CUSTOMER_ID = CUSTOMERS.CUSTOMER_ID;",
            std::min(reps, 5));

        fql_exec_ignore(db, "DROP TABLE ORDERS;");
    }
    fql_exec_ignore(db, "DROP TABLE CUSTOMERS;");

    flexql_close(db);
    printf("\n  Done.\n");
    return 0;
}
