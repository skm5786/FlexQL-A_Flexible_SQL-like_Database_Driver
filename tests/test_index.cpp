/**
 * test_index.cpp  —  Unit Tests for the Primary Key Hash Index (Lesson 4)
 *
 * Tests the hash index both in isolation (index API directly) and
 * integrated into the storage engine (via table_scan / row_insert).
 *
 * Structure mirrors test_parser.cpp and test_storage.cpp:
 *   - One test function per feature group
 *   - CHECK(condition, description) macro
 *   - Summary at the end
 *
 * Build:
 *   g++ -std=c++17 -I./include \
 *       tests/test_index.cpp \
 *       src/index/index.cpp \
 *       src/storage/storage.cpp \
 *       src/storage/dbmanager.cpp \
 *       -lpthread -o bin/test_index
 *
 * Run:
 *   ./bin/test_index
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <climits>
#include <pthread.h>

#include "common/types.h"
#include "index/index.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

/* ── Test framework ───────────────────────────────────────────────────────── */
static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS  %s\n", msg); } \
    else      {                 printf("  FAIL  %s\n", msg); } \
} while(0)

/* ── Helpers to build CellValue keys easily ───────────────────────────────── */
static CellValue make_int_key(int64_t v) {
    CellValue c{}; c.type = COL_TYPE_INT; c.is_null = 0;
    c.data.int_val = v; return c;
}
static CellValue make_str_key(const char *s) {
    CellValue c{}; c.type = COL_TYPE_VARCHAR; c.is_null = 0;
    c.data.varchar_val = (char*)s; return c;
}
static CellValue make_null_key() {
    CellValue c{}; c.type = COL_TYPE_INT; c.is_null = 1; return c;
}

/* ── Database + Table setup helper ───────────────────────────────────────── */
static void make_int_pk_table(DatabaseManager *mgr, Database **db_out,
                               Table **table_out) {
    dbmgr_init(mgr);
    char *err = nullptr;
    dbmgr_create(mgr, "testdb", &err);
    *db_out = dbmgr_find(mgr, "testdb");

    ColumnDef cols[2];
    memset(cols, 0, sizeof(cols));
    strncpy(cols[0].name, "ID",   63); cols[0].type = COL_TYPE_INT;
    cols[0].constraints = COL_CONSTRAINT_PRIMARY_KEY | COL_CONSTRAINT_NOT_NULL;
    cols[0].col_index = 0;
    strncpy(cols[1].name, "NAME", 63); cols[1].type = COL_TYPE_VARCHAR;
    cols[1].constraints = COL_CONSTRAINT_NOT_NULL;
    cols[1].col_index = 1;

    *table_out = table_create(*db_out, "test", cols, 2, &err);
}

static int insert_row(Table *t, int id, const char *name) {
    char v[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR] = {};
    snprintf(v[0], FLEXQL_MAX_VARCHAR, "%d", id);
    strncpy(v[1], name, FLEXQL_MAX_VARCHAR-1);
    char *err = nullptr;
    int rc = row_insert(t, v, 2, 0, &err);
    free(err);
    return rc;
}

/* Scan callback that counts rows */
static int count_cb(const Row *r, void *arg) {
    (void)r; (*((int*)arg))++; return 0;
}

/* Scan callback that stores the ID of the first matching row */
static int capture_id_cb(const Row *r, void *arg) {
    *((int64_t*)arg) = r->cells[0].data.int_val; return 1; /* stop after 1 */
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 1 — index_create / index_free
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_create_free() {
    printf("\n[index_create / index_free]\n");

    HashIndex *idx = index_create(COL_TYPE_INT);
    CHECK(idx != nullptr,                   "index_create returns non-NULL");
    CHECK(idx->capacity == 16,              "initial capacity = 16");
    CHECK(idx->count == 0,                  "initial count = 0");
    CHECK(idx->tombstones == 0,             "initial tombstones = 0");
    CHECK(idx->pk_type == COL_TYPE_INT,     "pk_type stored correctly");
    CHECK(idx->slots != nullptr,            "slots array allocated");

    index_free(idx);
    CHECK(1,                                "index_free completes without crash");

    index_free(nullptr);
    CHECK(1,                                "index_free(NULL) is safe");

    HashIndex *str_idx = index_create(COL_TYPE_VARCHAR);
    CHECK(str_idx != nullptr,               "index_create for VARCHAR works");
    CHECK(str_idx->pk_type == COL_TYPE_VARCHAR, "pk_type = VARCHAR");
    index_free(str_idx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 2 — index_put / index_get (INT keys)
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_put_get_int() {
    printf("\n[index_put / index_get: INT keys]\n");

    HashIndex *idx = index_create(COL_TYPE_INT);

    /* Fake Row pointers — we just need distinct addresses for testing */
    Row rows[5];
    memset(rows, 0, sizeof(rows));

    CellValue k1 = make_int_key(1);
    CellValue k2 = make_int_key(2);
    CellValue k3 = make_int_key(1000000);
    CellValue kn = make_null_key();

    /* put then get */
    CHECK(index_put(idx, &k1, &rows[0]) == 0,   "put key=1 succeeds");
    CHECK(index_put(idx, &k2, &rows[1]) == 0,   "put key=2 succeeds");
    CHECK(index_put(idx, &k3, &rows[2]) == 0,   "put key=1000000 succeeds");
    CHECK(index_size(idx) == 3,                  "size = 3 after 3 inserts");

    CHECK(index_get(idx, &k1) == &rows[0],       "get key=1 returns rows[0]");
    CHECK(index_get(idx, &k2) == &rows[1],       "get key=2 returns rows[1]");
    CHECK(index_get(idx, &k3) == &rows[2],       "get key=1000000 returns rows[2]");

    /* key not present */
    CellValue k99 = make_int_key(99);
    CHECK(index_get(idx, &k99) == nullptr,       "get missing key returns NULL");

    /* NULL key is never stored */
    CHECK(index_put(idx, &kn, &rows[3]) == -1,   "put NULL key returns -1");
    CHECK(index_get(idx, &kn) == nullptr,         "get NULL key returns NULL");

    /* Negative keys */
    CellValue kneg = make_int_key(-42);
    CHECK(index_put(idx, &kneg, &rows[4]) == 0,  "put negative key succeeds");
    CHECK(index_get(idx, &kneg) == &rows[4],      "get negative key returns correct row");

    /* Update existing key */
    CHECK(index_put(idx, &k1, &rows[3]) == 0,    "put existing key (update) succeeds");
    CHECK(index_get(idx, &k1) == &rows[3],        "updated key returns new row pointer");
    CHECK(index_size(idx) == 4,                   "size unchanged after update");

    index_free(idx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 3 — index_put / index_get (VARCHAR keys)
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_put_get_varchar() {
    printf("\n[index_put / index_get: VARCHAR keys]\n");

    HashIndex *idx = index_create(COL_TYPE_VARCHAR);
    Row rows[4]; memset(rows, 0, sizeof(rows));

    CellValue ka = make_str_key("Alice");
    CellValue kb = make_str_key("Bob");
    CellValue kc = make_str_key("Carol");
    CellValue kx = make_str_key("Zara");  /* not inserted */

    CHECK(index_put(idx, &ka, &rows[0]) == 0,    "put 'Alice' succeeds");
    CHECK(index_put(idx, &kb, &rows[1]) == 0,    "put 'Bob' succeeds");
    CHECK(index_put(idx, &kc, &rows[2]) == 0,    "put 'Carol' succeeds");
    CHECK(index_size(idx) == 3,                   "size = 3");

    CHECK(index_get(idx, &ka) == &rows[0],        "get 'Alice' returns rows[0]");
    CHECK(index_get(idx, &kb) == &rows[1],        "get 'Bob' returns rows[1]");
    CHECK(index_get(idx, &kc) == &rows[2],        "get 'Carol' returns rows[2]");
    CHECK(index_get(idx, &kx) == nullptr,         "get 'Zara' (missing) returns NULL");

    /* Case sensitivity — 'alice' ≠ 'Alice' */
    CellValue klc = make_str_key("alice");
    CHECK(index_get(idx, &klc) == nullptr,        "VARCHAR lookup is case-sensitive");

    index_free(idx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 4 — index_remove and tombstone behaviour
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_remove_tombstone() {
    printf("\n[index_remove / tombstone behaviour]\n");

    HashIndex *idx = index_create(COL_TYPE_INT);
    Row r1, r2, r3; memset(&r1,0,sizeof(r1)); memset(&r2,0,sizeof(r2)); memset(&r3,0,sizeof(r3));

    CellValue k1 = make_int_key(10);
    CellValue k2 = make_int_key(20);
    CellValue k3 = make_int_key(30);

    index_put(idx, &k1, &r1);
    index_put(idx, &k2, &r2);
    index_put(idx, &k3, &r3);
    CHECK(index_size(idx) == 3,               "size = 3 before remove");

    /* Remove middle key */
    int rc = index_remove(idx, &k2);
    CHECK(rc == 0,                             "remove key=20 returns 0");
    CHECK(index_size(idx) == 2,               "size = 2 after remove");
    CHECK(index_get(idx, &k2) == nullptr,     "removed key returns NULL");

    /* Keys on either side of the tombstone still reachable */
    CHECK(index_get(idx, &k1) == &r1,         "key=10 still reachable after tombstone");
    CHECK(index_get(idx, &k3) == &r3,         "key=30 still reachable after tombstone");

    /* Remove non-existent key */
    CellValue k99 = make_int_key(99);
    rc = index_remove(idx, &k99);
    CHECK(rc == -1,                            "remove missing key returns -1");

    /* Re-insert after remove — tombstone slot reused */
    Row r4; memset(&r4,0,sizeof(r4));
    rc = index_put(idx, &k2, &r4);
    CHECK(rc == 0,                             "re-insert after remove succeeds");
    CHECK(index_get(idx, &k2) == &r4,         "re-inserted key found correctly");
    CHECK(index_size(idx) == 3,               "size = 3 after re-insert");

    index_free(idx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 5 — resize / rehash
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_resize() {
    printf("\n[Resize / rehash]\n");

    HashIndex *idx = index_create(COL_TYPE_INT);
    size_t initial_cap = idx->capacity;  /* should be 16 */
    CHECK(initial_cap == 16,                  "initial capacity = 16");

    /* Insert enough entries to trigger a resize.
     * Load factor = 0.7, so 16 * 0.7 = 11.2 → resize at 12th insert.    */
    Row rows[100]; memset(rows,0,sizeof(rows));
    for (int i = 0; i < 12; i++) {
        CellValue k = make_int_key(i);
        index_put(idx, &k, &rows[i]);
    }

    CHECK(idx->capacity > initial_cap,        "capacity grew after 12 inserts");
    CHECK(idx->capacity == 32,                "capacity doubled to 32");
    CHECK(index_size(idx) == 12,              "all 12 entries survived resize");

    /* Verify all entries still findable after resize */
    int all_found = 1;
    for (int i = 0; i < 12; i++) {
        CellValue k = make_int_key(i);
        if (index_get(idx, &k) != &rows[i]) { all_found = 0; break; }
    }
    CHECK(all_found,                          "all 12 entries findable after resize");

    /* Insert 50 more — forces another resize */
    for (int i = 12; i < 62; i++) {
        CellValue k = make_int_key(i);
        index_put(idx, &k, &rows[i % 100]);
    }
    CHECK(index_size(idx) == 62,              "62 total entries after second resize");
    CHECK(idx->capacity >= 64,                "capacity is at least 64 after second resize");

    index_free(idx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 6 — Hash collisions (same initial slot, different keys)
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_hash_collisions() {
    printf("\n[Hash collisions — linear probing correctness]\n");

    /* Use a small capacity to force collisions: capacity=16, insert many
     * keys that hash to the same slot. Linear probing must handle this. */
    HashIndex *idx = index_create(COL_TYPE_INT);
    Row rows[16]; memset(rows,0,sizeof(rows));

    /* Insert 11 entries (just below the 70% threshold of 16).
     * With Knuth hash, some will definitely collide at capacity=16.       */
    int ids[] = {0, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160};
    for (int i = 0; i < 11; i++) {
        CellValue k = make_int_key(ids[i]);
        index_put(idx, &k, &rows[i]);
    }

    /* All entries must be retrievable even with collisions */
    int all_ok = 1;
    for (int i = 0; i < 11; i++) {
        CellValue k = make_int_key(ids[i]);
        if (index_get(idx, &k) != &rows[i]) { all_ok = 0; break; }
    }
    CHECK(all_ok,                             "all entries findable after collisions");
    CHECK(index_size(idx) == 11,              "size = 11 after collision-heavy inserts");

    /* Remove one from the middle of a collision chain */
    CellValue km = make_int_key(ids[5]);
    index_remove(idx, &km);

    /* All remaining entries still accessible through the tombstone */
    for (int i = 0; i < 11; i++) {
        if (i == 5) continue;
        CellValue k = make_int_key(ids[i]);
        Row *found = index_get(idx, &k);
        if (found != &rows[i]) { all_ok = 0; break; }
    }
    CHECK(all_ok,                             "remaining entries findable after mid-chain remove");

    index_free(idx);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 7 — Integration: table_create with index
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_table_create_has_index() {
    printf("\n[Integration: table_create allocates pk_index]\n");

    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    CHECK(t != nullptr,                       "table created successfully");
    CHECK(t->pk_index != nullptr,             "pk_index is NOT NULL for table with PK");
    CHECK(t->pk_col == 0,                     "pk_col = 0 (ID column)");
    CHECK(index_size(t->pk_index) == 0,       "index starts empty");

    /* Table without PK should have NULL index */
    ColumnDef cols2[1]; memset(cols2,0,sizeof(cols2));
    strncpy(cols2[0].name,"VAL",63); cols2[0].type=COL_TYPE_VARCHAR;
    cols2[0].constraints=COL_CONSTRAINT_NONE; cols2[0].col_index=0;
    char *err=nullptr;
    Table *t2 = table_create(db,"nopk",cols2,1,&err);
    CHECK(t2 != nullptr,                      "table without PK created OK");
    CHECK(t2->pk_index == nullptr,            "pk_index is NULL for table without PK");
    free(err);

    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 8 — Integration: row_insert updates the index
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_row_insert_updates_index() {
    printf("\n[Integration: row_insert updates pk_index]\n");

    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    insert_row(t, 1, "Alice");
    CHECK(index_size(t->pk_index) == 1,       "index size = 1 after first insert");

    insert_row(t, 2, "Bob");
    insert_row(t, 3, "Carol");
    CHECK(index_size(t->pk_index) == 3,       "index size = 3 after 3 inserts");

    /* Verify each PK maps to the correct row */
    CellValue k1 = make_int_key(1), k2 = make_int_key(2), k3 = make_int_key(3);
    Row *r1 = index_get(t->pk_index, &k1);
    Row *r2 = index_get(t->pk_index, &k2);
    Row *r3 = index_get(t->pk_index, &k3);

    CHECK(r1 != nullptr,                      "index has entry for ID=1");
    CHECK(r2 != nullptr,                      "index has entry for ID=2");
    CHECK(r3 != nullptr,                      "index has entry for ID=3");

    /* Verify the row pointer leads to the right data */
    CHECK(r1->cells[0].data.int_val == 1,     "index row for ID=1 has ID=1 cell");
    CHECK(strcmp(r1->cells[1].data.varchar_val,"Alice")==0,
                                              "index row for ID=1 has NAME=Alice");
    CHECK(r2->cells[0].data.int_val == 2,     "index row for ID=2 has correct data");
    CHECK(r3->cells[0].data.int_val == 3,     "index row for ID=3 has correct data");

    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 9 — Integration: row_insert PK dupe check uses index (O(1))
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_pk_dupe_check_via_index() {
    printf("\n[Integration: PK duplicate check via index]\n");

    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    insert_row(t, 1, "Alice");
    insert_row(t, 2, "Bob");

    /* Duplicate ID — must be rejected via index O(1) lookup */
    int rc = insert_row(t, 1, "Duplicate");
    CHECK(rc == -1,                           "duplicate PK insert fails");
    CHECK(t->row_count == 2,                  "row_count unchanged after duplicate attempt");
    CHECK(index_size(t->pk_index) == 2,       "index size unchanged after duplicate attempt");

    /* Original row still correct */
    CellValue k1 = make_int_key(1);
    Row *r = index_get(t->pk_index, &k1);
    CHECK(r != nullptr,                       "original row for ID=1 still in index");
    CHECK(strcmp(r->cells[1].data.varchar_val,"Alice")==0,
                                              "original row not overwritten by duplicate attempt");

    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 10 — Integration: table_scan WHERE pk=value uses index (O(1))
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_scan_uses_index() {
    printf("\n[Integration: table_scan WHERE pk=value uses index]\n");

    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    /* Insert 1000 rows */
    for (int i = 1; i <= 1000; i++) {
        char name[32]; snprintf(name,32,"User%d",i);
        insert_row(t, i, name);
    }
    CHECK(t->row_count == 1000,               "1000 rows inserted");
    CHECK(index_size(t->pk_index) == 1000,    "index has 1000 entries");

    /* WHERE ID = 500 — should use index, find exactly 1 row */
    WhereClause w{}; w.has_condition=1;
    strncpy(w.col_name,"ID",sizeof(w.col_name)-1);
    w.op=OP_EQ;
    strncpy(w.value,"500",sizeof(w.value)-1);

    int64_t found_id = -1;
    int n = table_scan(t, &w, capture_id_cb, &found_id);

    CHECK(n == 1,                             "WHERE ID=500 returns exactly 1 row");
    CHECK(found_id == 500,                    "returned row has ID=500");

    /* WHERE ID = 9999 (not present) */
    strncpy(w.value,"9999",sizeof(w.value)-1);
    int count=0;
    n = table_scan(t, &w, count_cb, &count);
    CHECK(n == 0,                             "WHERE ID=9999 (missing) returns 0 rows");

    /* WHERE ID > 500 (range query) must use full scan — still correct */
    WhereClause wr{}; wr.has_condition=1;
    strncpy(wr.col_name,"ID",sizeof(wr.col_name)-1);
    wr.op=OP_GT; strncpy(wr.value,"500",sizeof(wr.value)-1);
    count=0;
    table_scan(t, &wr, count_cb, &count);
    CHECK(count == 500,                       "WHERE ID>500 (full scan) returns 500 rows");

    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 11 — Integration: expired PK slot can be reused
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_expired_pk_reuse() {
    printf("\n[Integration: expired PK slot reuse]\n");

    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    /* Insert row with ID=1 that expires immediately */
    char v[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR]={};
    strncpy(v[0],"1",FLEXQL_MAX_VARCHAR-1);
    strncpy(v[1],"Ghost",FLEXQL_MAX_VARCHAR-1);
    char *err=nullptr;
    time_t past = time(nullptr) - 1;
    row_insert(t, v, 2, past, &err);
    CHECK(t->row_count == 1,                  "row inserted (will expire immediately)");

    /* Now insert ID=1 again — should succeed because the old row is expired */
    int rc = insert_row(t, 1, "Alice");
    CHECK(rc == 0,                            "re-insert expired PK succeeds");
    CHECK(t->row_count == 2,                  "row_count = 2 (includes expired row)");

    /* Scan should only see the live Alice row */
    WhereClause w{}; w.has_condition=1;
    strncpy(w.col_name,"ID",sizeof(w.col_name)-1);
    w.op=OP_EQ; strncpy(w.value,"1",sizeof(w.value)-1);
    int64_t found_id = -1;
    int n = table_scan(t, &w, capture_id_cb, &found_id);
    CHECK(n == 1,                             "scan returns only 1 row for ID=1");

    /* The live row should be Alice (second insert) */
    CellValue k1 = make_int_key(1);
    Row *r = index_get(t->pk_index, &k1);
    CHECK(r != nullptr,                       "index has entry for ID=1");
    if (r) CHECK(strcmp(r->cells[1].data.varchar_val,"Alice")==0,
                                              "index points to Alice (live row), not Ghost");
    free(err);
    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 12 — Performance: O(1) index vs O(n) scan timing
 *  (Not a correctness test — shows the speedup is real)
 * ═══════════════════════════════════════════════════════════════════════════ */
#include <sys/time.h>
static double now_ms() {
    struct timeval tv; gettimeofday(&tv,nullptr);
    return tv.tv_sec*1000.0 + tv.tv_usec/1000.0;
}

static void test_performance() {
    printf("\n[Performance: O(1) index vs O(n) full scan]\n");

    /* Build a table with 100,000 rows */
    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    int N = 100000;
    for (int i = 1; i <= N; i++) {
        char name[32]; snprintf(name,32,"User%d",i);
        insert_row(t, i, name);
    }
    CHECK((int)t->row_count == N,             "100,000 rows inserted");
    CHECK((int)index_size(t->pk_index) == N,  "index has 100,000 entries");

    /* Look up the LAST inserted row (worst case for O(n) scan) */
    WhereClause w{}; w.has_condition=1;
    strncpy(w.col_name,"ID",sizeof(w.col_name)-1);
    w.op=OP_EQ;
    snprintf(w.value,sizeof(w.value),"%d",N); /* look up the last row */

    /* Time 1000 lookups WITH index */
    double t0 = now_ms();
    for (int i = 0; i < 1000; i++) {
        int count=0;
        table_scan(t, &w, count_cb, &count);
    }
    double indexed_ms = now_ms() - t0;

    /* Temporarily disable the index to measure O(n) scan */
    HashIndex *saved_idx = t->pk_index;
    t->pk_index = nullptr;

    double t1 = now_ms();
    for (int i = 0; i < 1000; i++) {
        int count=0;
        table_scan(t, &w, count_cb, &count);
    }
    double scan_ms = now_ms() - t1;

    /* Restore index */
    t->pk_index = saved_idx;

    printf("    Index lookup (1000×): %.2f ms\n", indexed_ms);
    printf("    Full scan   (1000×): %.2f ms\n", scan_ms);
    printf("    Speedup: %.1f×\n", scan_ms / (indexed_ms > 0.01 ? indexed_ms : 0.01));

    CHECK(indexed_ms < scan_ms,               "index lookups faster than full scans");
    /* Speedup should be substantial — at least 10× on 100k rows */
    double speedup = scan_ms / (indexed_ms > 0.01 ? indexed_ms : 0.01);
    CHECK(speedup > 10.0,                     "speedup > 10× on 100k rows");

    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 13 — Concurrency: 2 threads inserting with index active
 * ═══════════════════════════════════════════════════════════════════════════ */
struct ThreadArg {
    Table *table;
    int    start_id;
    int    count;
    int    errors;
};
static void *insert_thread(void *arg) {
    ThreadArg *ta = (ThreadArg*)arg;
    for (int i = 0; i < ta->count; i++) {
        if (insert_row(ta->table, ta->start_id + i,
                       "concurrent_user") != 0)
            ta->errors++;
    }
    return nullptr;
}

static void test_concurrent_insert_with_index() {
    printf("\n[Concurrency: 2 threads inserting with index]\n");

    DatabaseManager mgr; Database *db; Table *t;
    make_int_pk_table(&mgr, &db, &t);

    /* Thread 1: IDs 1–500, Thread 2: IDs 501–1000 */
    ThreadArg a1{t, 1,   500, 0};
    ThreadArg a2{t, 501, 500, 0};

    pthread_t tid1, tid2;
    pthread_create(&tid1, nullptr, insert_thread, &a1);
    pthread_create(&tid2, nullptr, insert_thread, &a2);
    pthread_join(tid1, nullptr);
    pthread_join(tid2, nullptr);

    CHECK(a1.errors == 0,                     "thread 1 had 0 insert errors");
    CHECK(a2.errors == 0,                     "thread 2 had 0 insert errors");
    CHECK((int)t->row_count == 1000,          "1000 rows after concurrent inserts");
    CHECK((int)index_size(t->pk_index) == 1000,"index has 1000 entries after concurrent inserts");

    /* Spot-check a few entries */
    int count=0; WhereClause nw{}; nw.has_condition=0;
    table_scan(t, &nw, count_cb, &count);
    CHECK(count == 1000,                      "scan sees all 1000 rows");

    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  MAIN
 * ═══════════════════════════════════════════════════════════════════════════ */
int main() {
    printf("═══════════════════════════════════════════\n");
    printf(" FlexQL Hash Index Test Suite (Lesson 4)\n");
    printf("═══════════════════════════════════════════\n");

    /* ── Pure index API tests ── */
    test_create_free();
    test_put_get_int();
    test_put_get_varchar();
    test_remove_tombstone();
    test_resize();
    test_hash_collisions();

    /* ── Integration with storage engine ── */
    test_table_create_has_index();
    test_row_insert_updates_index();
    test_pk_dupe_check_via_index();
    test_scan_uses_index();
    test_expired_pk_reuse();

    /* ── Performance and concurrency ── */
    test_performance();
    test_concurrent_insert_with_index();

    printf("\n═══════════════════════════════════════════\n");
    printf(" Results: %d / %d tests passed\n", tests_passed, tests_run);
    printf("═══════════════════════════════════════════\n");
    return (tests_passed == tests_run) ? 0 : 1;
}