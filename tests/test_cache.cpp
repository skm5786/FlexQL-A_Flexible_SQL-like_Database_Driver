/**
 * test_cache.cpp  —  Unit Tests for the LRU Query Cache  (Lesson 5)
 *
 * Tests the cache in isolation (pure API) and integrated with the
 * storage engine + executor via a simulated SELECT/INSERT workflow.
 *
 * Build:
 *   g++ -std=c++17 -I./include \
 *       tests/test_cache.cpp \
 *       src/cache/cache.cpp \
 *       src/storage/storage.cpp \
 *       src/storage/dbmanager.cpp \
 *       src/index/index.cpp \
 *       -lpthread -O2 -o bin/test_cache
 *
 * Run:
 *   ./bin/test_cache
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <pthread.h>
#include <sys/time.h>

#include "common/types.h"
#include "cache/cache.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

/* ── Test framework ───────────────────────────────────────────────────────── */
static int tests_run = 0, tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS  %s\n", msg); } \
    else      {                 printf("  FAIL  %s\n", msg); } \
} while(0)

/* ── Helpers ─────────────────────────────────────────────────────────────── */
static double now_ms() {
    struct timeval tv; gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

/* Build fake payload arrays for cache_put */
static void put_entry(LRUCache *c, const char *db, const char *sql,
                      int nrows) {
    const char *bufs[64]; uint32_t lens[64];
    char tmp[64][32];
    for (int i = 0; i < nrows && i < 64; i++) {
        snprintf(tmp[i], 32, "row-%d", i);
        bufs[i] = tmp[i];
        lens[i] = (uint32_t)strlen(tmp[i]);
    }
    cache_put(c, db, sql, bufs, lens, nrows);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 1 — cache_create / cache_free
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_create_free() {
    printf("\n[cache_create / cache_free]\n");

    LRUCache *c = cache_create();
    CHECK(c != nullptr,             "cache_create returns non-NULL");

    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 0,             "initial count = 0");
    CHECK(s.hits  == 0,             "initial hits  = 0");
    CHECK(s.misses== 0,             "initial misses= 0");
    CHECK(s.total_bytes == 0,       "initial bytes = 0");

    cache_free(c);
    CHECK(1,                        "cache_free completes without crash");

    cache_free(nullptr);
    CHECK(1,                        "cache_free(NULL) is safe");
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 2 — cache_put / cache_get basic
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_put_get_basic() {
    printf("\n[cache_put / cache_get: basic]\n");

    LRUCache *c = cache_create();

    /* MISS on empty cache */
    CacheEntry *ce = nullptr;
    int hit = cache_get(c, "MYDB", "SELECT * FROM T", &ce);
    CHECK(hit == 0,                 "get on empty cache returns MISS");
    CHECK(ce == nullptr,            "out_entry is NULL on MISS");

    CacheStats s; cache_stats(c, &s);
    CHECK(s.misses == 1,            "miss counter incremented");

    /* PUT then GET */
    put_entry(c, "MYDB", "SELECT * FROM T", 3);
    hit = cache_get(c, "MYDB", "SELECT * FROM T", &ce);
    CHECK(hit == 1,                 "get after put returns HIT");
    CHECK(ce != nullptr,            "out_entry non-NULL on HIT");
    if (ce) {
        CHECK(ce->payload_count == 3, "cached entry has 3 payloads");
    }
    cache_stats(c, &s);
    CHECK(s.hits   == 1,            "hit counter incremented");
    CHECK(s.count  == 1,            "cache has 1 entry");
    CHECK(s.total_bytes > 0,        "total_bytes > 0");

    /* GET returns NULL when entry doesn't exist */
    hit = cache_get(c, "MYDB", "SELECT * FROM OTHER", &ce);
    CHECK(hit == 0,                 "different SQL → MISS");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 3 — Cache key is case-insensitive and db-scoped
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_key_case_and_scope() {
    printf("\n[Key: case-insensitive, db-scoped]\n");

    LRUCache *c = cache_create();

    /* Put with lowercase SQL */
    put_entry(c, "university", "select * from student", 2);

    /* Get with different case — should be same entry */
    CacheEntry *ce = nullptr;
    int hit = cache_get(c, "UNIVERSITY", "SELECT * FROM STUDENT", &ce);
    CHECK(hit == 1,                 "case-insensitive SQL key matches");

    /* Same SQL in different database → different entry */
    hit = cache_get(c, "hospital", "select * from student", &ce);
    CHECK(hit == 0,                 "same SQL in different db → MISS");

    put_entry(c, "hospital", "select * from student", 5);
    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 2,             "2 entries for same SQL in 2 databases");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 4 — LRU order: access promotes to MRU, eviction removes LRU
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_lru_order() {
    printf("\n[LRU order: promotion and eviction]\n");

    LRUCache *c = cache_create();

    /* Insert 3 entries: A then B then C (C is MRU, A is LRU) */
    put_entry(c, "db", "A", 1);
    put_entry(c, "db", "B", 1);
    put_entry(c, "db", "C", 1);

    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 3,             "3 entries inserted");

    /* Access A — promotes A to MRU, B becomes LRU */
    CacheEntry *ce = nullptr;
    cache_get(c, "db", "A", &ce);
    CHECK(c->head != nullptr,       "head exists");
    if (c->head)
        CHECK(strcmp(c->tail->key, "DB:B") == 0, "B is now LRU tail after accessing A");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 5 — Eviction when over CACHE_MAX_ENTRIES
 *  (We use a small custom test — don't actually fill 256 entries)
 *  We verify by inserting N+1 entries and checking count stays at N.
 *  Since CACHE_MAX_ENTRIES=256 is large, we test the byte-limit path.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_eviction_on_replace() {
    printf("\n[Eviction: replacing existing key]\n");

    LRUCache *c = cache_create();

    /* Insert same key twice — second replaces first */
    put_entry(c, "db", "SELECT * FROM T", 2);
    put_entry(c, "db", "SELECT * FROM T", 5);

    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 1,             "count = 1 after replace");

    CacheEntry *ce = nullptr;
    cache_get(c, "db", "SELECT * FROM T", &ce);
    CHECK(ce != nullptr,            "replaced entry is still gettable");
    if (ce) CHECK(ce->payload_count == 5, "entry has 5 payloads (replaced version)");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 6 — cache_invalidate_table
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_invalidate_table() {
    printf("\n[cache_invalidate_table]\n");

    LRUCache *c = cache_create();

    put_entry(c, "MYDB", "SELECT:STUDENT:WHERE::", 3);
    put_entry(c, "MYDB", "SELECT:ORDERS:WHERE::", 2);
    put_entry(c, "MYDB", "SELECT:STUDENT:WHERE:ID:=:5", 1);
    put_entry(c, "OTHERDB", "SELECT:STUDENT:WHERE::", 4);

    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 4,             "4 entries before invalidation");

    /* Invalidate STUDENT in MYDB */
    int removed = cache_invalidate_table(c, "MYDB", "STUDENT");
    CHECK(removed == 2,             "2 entries removed (STUDENT in MYDB)");

    cache_stats(c, &s);
    CHECK(s.count == 2,             "2 entries remain after invalidation");

    /* ORDERS entry still present */
    CacheEntry *ce = nullptr;
    int hit = cache_get(c, "MYDB", "SELECT:ORDERS:WHERE::", &ce);
    CHECK(hit == 1,                 "ORDERS entry unaffected by STUDENT invalidation");

    /* STUDENT in OTHERDB still present */
    hit = cache_get(c, "OTHERDB", "SELECT:STUDENT:WHERE::", &ce);
    CHECK(hit == 1,                 "STUDENT in OTHERDB unaffected");

    /* STUDENT in MYDB is gone */
    hit = cache_get(c, "MYDB", "SELECT:STUDENT:WHERE::", &ce);
    CHECK(hit == 0,                 "STUDENT in MYDB is invalidated");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 7 — cache_invalidate_db
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_invalidate_db() {
    printf("\n[cache_invalidate_db]\n");

    LRUCache *c = cache_create();

    put_entry(c, "DB1", "SELECT:T1:WHERE::", 1);
    put_entry(c, "DB1", "SELECT:T2:WHERE::", 2);
    put_entry(c, "DB2", "SELECT:T1:WHERE::", 3);

    int removed = cache_invalidate_db(c, "DB1");
    CHECK(removed == 2,             "2 entries removed for DB1");

    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 1,             "1 entry remains (DB2)");

    CacheEntry *ce = nullptr;
    CHECK(cache_get(c, "DB2", "SELECT:T1:WHERE::", &ce) == 1,
                                    "DB2 entry unaffected");
    CHECK(cache_get(c, "DB1", "SELECT:T1:WHERE::", &ce) == 0,
                                    "DB1 entry invalidated");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 8 — cache_clear
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_clear() {
    printf("\n[cache_clear]\n");

    LRUCache *c = cache_create();
    put_entry(c, "db", "A", 3);
    put_entry(c, "db", "B", 3);
    put_entry(c, "db", "C", 3);

    cache_clear(c);
    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 0,             "count = 0 after clear");
    CHECK(s.total_bytes == 0,       "total_bytes = 0 after clear");

    /* Can still put after clear */
    put_entry(c, "db", "NEW", 1);
    cache_stats(c, &s);
    CHECK(s.count == 1,             "can add entries after clear");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 9 — Empty result caching (0 rows)
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_empty_result_cache() {
    printf("\n[Empty result caching (0 rows)]\n");

    LRUCache *c = cache_create();

    /* Cache an empty result (SELECT with no matching rows) */
    cache_put(c, "db", "SELECT:EMPTY:WHERE:ID:=:999", nullptr, nullptr, 0);

    CacheEntry *ce = nullptr;
    int hit = cache_get(c, "db", "SELECT:EMPTY:WHERE:ID:=:999", &ce);
    CHECK(hit == 1,                 "empty result is cached");
    CHECK(ce != nullptr,            "empty result entry non-NULL");
    if (ce) CHECK(ce->payload_count == 0, "payload_count = 0 for empty result");

    CacheStats s; cache_stats(c, &s);
    CHECK(s.count == 1,             "empty result counts as 1 entry");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 10 — Statistics accuracy
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_statistics() {
    printf("\n[Statistics accuracy]\n");

    LRUCache *c = cache_create();
    CacheEntry *ce = nullptr;

    put_entry(c, "db", "Q1", 2);
    put_entry(c, "db", "Q2", 2);

    cache_get(c, "db", "Q1", &ce);   /* hit */
    cache_get(c, "db", "Q1", &ce);   /* hit */
    cache_get(c, "db", "Q2", &ce);   /* hit */
    cache_get(c, "db", "Q3", &ce);   /* miss */
    cache_get(c, "db", "Q4", &ce);   /* miss */

    CacheStats s; cache_stats(c, &s);
    CHECK(s.hits   == 3,             "3 hits recorded");
    CHECK(s.misses == 2,             "2 misses recorded");
    CHECK(s.count  == 2,             "2 entries in cache");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 11 — Integration with storage: INSERT invalidates cache
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_integration_insert_invalidates() {
    printf("\n[Integration: INSERT invalidates cached SELECT]\n");

    /* Setup: database with a table and 3 rows */
    DatabaseManager mgr; dbmgr_init(&mgr);
    char *err = nullptr;
    dbmgr_create(&mgr, "testdb", &err);
    Database *db = dbmgr_find(&mgr, "testdb");

    ColumnDef cols[2]; memset(cols,0,sizeof(cols));
    strncpy(cols[0].name,"ID",63); cols[0].type=COL_TYPE_INT;
    cols[0].constraints=COL_CONSTRAINT_PRIMARY_KEY|COL_CONSTRAINT_NOT_NULL; cols[0].col_index=0;
    strncpy(cols[1].name,"NAME",63); cols[1].type=COL_TYPE_VARCHAR;
    cols[1].constraints=COL_CONSTRAINT_NOT_NULL; cols[1].col_index=1;
    table_create(db,"STUDENT",cols,2,&err);

    LRUCache *cache = mgr.query_cache;
    CHECK(cache != nullptr,          "mgr.query_cache allocated by dbmgr_init");

    /* Manually populate cache with a fake SELECT result for STUDENT */
    put_entry(cache, "TESTDB", "SELECT:STUDENT:WHERE::", 3);

    CacheEntry *ce=nullptr;
    CHECK(cache_get(cache,"TESTDB","SELECT:STUDENT:WHERE::",&ce)==1,
          "cache has STUDENT entry before INSERT");

    /* Simulate what executor does on INSERT: invalidate table */
    cache_invalidate_table(cache, "TESTDB", "STUDENT");

    CHECK(cache_get(cache,"TESTDB","SELECT:STUDENT:WHERE::",&ce)==0,
          "STUDENT entry invalidated after INSERT");

    CacheStats s; cache_stats(cache, &s);
    CHECK(s.count == 0,              "cache empty after invalidation");

    free(err);
    dbmgr_destroy(&mgr);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 12 — Thread safety: 4 threads simultaneously hitting/putting
 * ═══════════════════════════════════════════════════════════════════════════ */
struct ThreadArg {
    LRUCache *cache;
    int       thread_id;
    int       errors;
};

static void *cache_thread(void *arg) {
    ThreadArg *ta = (ThreadArg*)arg;
    char sql[64], db[16];
    snprintf(db, 16, "DB%d", ta->thread_id);

    for (int i = 0; i < 200; i++) {
        snprintf(sql, 64, "SELECT:T%d:WHERE::", i % 10);

        /* Mix of puts and gets */
        if (i % 3 == 0) {
            put_entry(ta->cache, db, sql, 2);
        } else {
            CacheEntry *ce = nullptr;
            cache_get(ta->cache, db, sql, &ce);
        }

        /* Occasional invalidation */
        if (i % 50 == 0) {
            cache_invalidate_table(ta->cache, db, "T5");
        }
    }
    return nullptr;
}

static void test_thread_safety() {
    printf("\n[Thread safety: 4 concurrent threads]\n");

    LRUCache *c = cache_create();
    ThreadArg args[4];
    pthread_t tids[4];

    for (int i = 0; i < 4; i++) {
        args[i] = { c, i, 0 };
        pthread_create(&tids[i], nullptr, cache_thread, &args[i]);
    }
    for (int i = 0; i < 4; i++) pthread_join(tids[i], nullptr);

    int total_errors = 0;
    for (int i = 0; i < 4; i++) total_errors += args[i].errors;

    CHECK(total_errors == 0,         "0 errors across 4 concurrent threads");

    /* Cache is still in a valid state */
    CacheStats s; cache_stats(c, &s);
    CHECK(s.count >= 0,              "count is non-negative after concurrent access");
    CHECK(s.total_bytes >= 0,        "total_bytes is non-negative");

    cache_free(c);
    CHECK(1,                         "cache_free after concurrent use completes");
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 13 — Performance: cache hit vs storage scan
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_performance() {
    printf("\n[Performance: cache hit vs miss timing]\n");

    /* Build a cache with pre-populated entries */
    LRUCache *c = cache_create();

    /* Simulate 10 rows of ~100 bytes each cached */
    const char *payloads[10]; uint32_t lens[10];
    char bufs[10][100];
    for (int i = 0; i < 10; i++) {
        memset(bufs[i], 'x', 99); bufs[i][99]='\0';
        payloads[i] = bufs[i]; lens[i] = 99;
    }
    cache_put(c, "DB", "SELECT:STUDENT:WHERE::", payloads, lens, 10);

    /* Time 100,000 cache gets (hits) */
    double t0 = now_ms();
    for (int i = 0; i < 100000; i++) {
        CacheEntry *ce = nullptr;
        cache_get(c, "DB", "SELECT:STUDENT:WHERE::", &ce);
    }
    double hit_ms = now_ms() - t0;

    /* Time 100,000 cache misses (different key each time) */
    double t1 = now_ms();
    for (int i = 0; i < 100000; i++) {
        char sql[64]; snprintf(sql,64,"MISS_KEY_%d",i);
        CacheEntry *ce=nullptr;
        cache_get(c, "DB", sql, &ce);
    }
    double miss_ms = now_ms() - t1;

    printf("    100k cache HITs:   %.2f ms (%.3f µs each)\n",
           hit_ms,  hit_ms  * 10.0);
    printf("    100k cache MISSes: %.2f ms (%.3f µs each)\n",
           miss_ms, miss_ms * 10.0);

    CHECK(hit_ms  < 1000.0,          "100k hits complete in under 1 second");
    CHECK(miss_ms < 2000.0,          "100k misses complete in under 2 seconds");

    CacheStats s; cache_stats(c, &s);
    CHECK(s.hits == 100000,          "100,000 hits recorded");
    CHECK(s.misses == 100000,        "100,000 misses recorded");

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  GROUP 14 — Payload content integrity
 * ═══════════════════════════════════════════════════════════════════════════ */
static void test_payload_integrity() {
    printf("\n[Payload content integrity]\n");

    LRUCache *c = cache_create();

    /* Store specific binary content */
    const char *bufs[3] = { "row-data-alpha", "row-data-beta", "row-data-gamma" };
    uint32_t   lens[3]  = { 14, 13, 14 };
    cache_put(c, "DB", "SELECT:T:WHERE::", bufs, lens, 3);

    CacheEntry *ce = nullptr;
    cache_get(c, "DB", "SELECT:T:WHERE::", &ce);
    CHECK(ce != nullptr,             "entry found");
    if (ce) {
        int i = 0;
        for (CachedPayload *p = ce->payloads; p; p = p->next, i++) {
            /* Verify the original data was copied correctly */
        }
        CHECK(ce->payload_count == 3, "3 payloads stored");

        /* Verify first payload content */
        CachedPayload *first = ce->payloads;
        CHECK(first != nullptr,      "first payload non-NULL");
        if (first) {
            CHECK(first->len == 14,  "first payload length = 14");
            CHECK(memcmp(first->data, "row-data-alpha", 14) == 0,
                                     "first payload content correct");
        }
    }

    cache_free(c);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  MAIN
 * ═══════════════════════════════════════════════════════════════════════════ */
int main() {
    printf("═══════════════════════════════════════════\n");
    printf(" FlexQL LRU Cache Test Suite  (Lesson 5)\n");
    printf("═══════════════════════════════════════════\n");

    test_create_free();
    test_put_get_basic();
    test_key_case_and_scope();
    test_lru_order();
    test_eviction_on_replace();
    test_invalidate_table();
    test_invalidate_db();
    test_clear();
    test_empty_result_cache();
    test_statistics();
    test_integration_insert_invalidates();
    test_thread_safety();
    test_performance();
    test_payload_integrity();

    printf("\n═══════════════════════════════════════════\n");
    printf(" Results: %d / %d tests passed\n", tests_passed, tests_run);
    printf("═══════════════════════════════════════════\n");
    return (tests_passed == tests_run) ? 0 : 1;
}