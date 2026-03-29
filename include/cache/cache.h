/**
 * cache.h  —  LRU Query Result Cache
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 5 — THE LRU QUERY CACHE
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * WHY A QUERY CACHE?
 *   Even with the index from Lesson 4, a SELECT still touches the storage
 *   engine.  If the same SELECT runs 1000 times (e.g. a dashboard that
 *   refreshes every second), we still execute 1000 scans.
 *
 *   A query cache maps  SQL_string → result_rows  so that on a cache HIT
 *   the second (and every subsequent) execution never touches the storage
 *   engine at all — it just replays the cached wire payloads.
 *
 *   This is the approach used by MySQL's query cache and many application-
 *   level caches (Redis, Memcached).
 *
 * WHAT IS CACHED?
 *   The key  is the normalised SQL string (e.g. "SELECT * FROM STUDENT").
 *   The value is a list of pre-serialised result row payloads (the exact
 *   byte buffers that would be sent to the client via MSG_RESULT).
 *   Storing pre-serialised payloads means a cache hit is:
 *     loop over cached payloads → send each → send MSG_DONE.
 *   Zero storage access, zero type conversion, zero serialisation.
 *
 * LRU EVICTION POLICY:
 *   LRU = Least Recently Used.  When the cache is full and a new entry
 *   must be added, we evict the entry that was accessed longest ago.
 *
 *   IMPLEMENTATION: doubly-linked list + hash map.
 *     - The doubly-linked list maintains access order:
 *         head (most recent) ↔ … ↔ tail (least recent)
 *     - The hash map maps SQL string → node pointer for O(1) lookup.
 *     - On cache HIT:  move the node to the front of the list.
 *     - On cache MISS: add new node at front; if over capacity, remove tail.
 *     - All operations are O(1).
 *
 *   WHY LRU over FIFO or LFU?
 *     - FIFO ignores access frequency — evicts hot entries unfairly.
 *     - LFU is complex and can get "stuck" on old popular entries.
 *     - LRU is simple, O(1), and performs well in practice for database
 *       query workloads where recent queries tend to repeat.
 *
 * CACHE INVALIDATION:
 *   A stale cache entry would return rows that no longer exist or miss
 *   newly inserted rows.  We invalidate eagerly:
 *     INSERT INTO table_name → invalidate all entries referencing table_name
 *     DROP TABLE / DROP DATABASE → invalidate all affected entries
 *   We do NOT cache non-SELECT queries (INSERT, CREATE, DROP).
 *
 *   NOTE: The cache is per-database.  "SELECT * FROM STUDENT" in database
 *   UNIVERSITY and the same string in database HOSPITAL are separate entries
 *   because they reference different underlying tables.
 *   The cache key includes the database name as a prefix.
 *
 * THREAD SAFETY:
 *   All public functions are protected by a single mutex.
 *   This is a "cache-wide lock" — simple and correct.
 *   A more scalable design would use multiple lock stripes, but for our
 *   use case one lock is sufficient.
 *
 * MEMORY:
 *   Each cached result stores all row payloads as heap-allocated buffers.
 *   The cache tracks total memory used and can also enforce a byte limit.
 *   Default capacity: 256 entries OR 64 MB of serialised data, whichever
 *   is reached first.
 */

#ifndef FLEXQL_CACHE_H
#define FLEXQL_CACHE_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Limits ─────────────────────────────────────────────────────────────── */
#define CACHE_MAX_ENTRIES     256          /* max number of cached queries  */
#define CACHE_MAX_BYTES       (64*1024*1024) /* max total payload bytes: 64MB */
#define CACHE_KEY_MAX         512          /* max SQL key length             */

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  CACHED PAYLOAD  (one serialised result row)
 *
 *  We store each result row as the exact byte sequence that would be sent
 *  over the wire as a MSG_RESULT payload.  On a cache hit, we re-send these
 *  buffers directly without touching the storage engine.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct CachedPayload {
    char               *data;     /* heap-allocated serialised row bytes    */
    uint32_t            len;      /* byte length of data                    */
    struct CachedPayload *next;   /* singly-linked list within one entry    */
} CachedPayload;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  CACHE ENTRY  (one node in the doubly-linked list AND the hash map)
 *
 *  key          — the SQL string (db_name + ":" + SQL)
 *  payloads     — singly-linked list of CachedPayload (the result rows)
 *  payload_count— number of rows cached
 *  total_bytes  — sum of all payload sizes for this entry
 *  prev/next    — doubly-linked list for LRU order (prev=newer, next=older)
 *  hash_next    — singly-linked list for hash collision chaining
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct CacheEntry {
    char            key[CACHE_KEY_MAX];  /* "DBNAME:SELECT ..." */
    CachedPayload  *payloads;            /* head of payload list */
    int             payload_count;
    size_t          total_bytes;
    struct CacheEntry *prev;             /* doubly-linked list (newer)      */
    struct CacheEntry *next;             /* doubly-linked list (older)      */
    struct CacheEntry *hash_next;        /* hash bucket chain               */
} CacheEntry;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  LRU CACHE  (the top-level struct, one per DatabaseManager)
 *
 *  head         — most recently used entry
 *  tail         — least recently used entry (eviction candidate)
 *  buckets[]    — hash map for O(1) key lookup
 *  count        — number of entries currently in cache
 *  total_bytes  — total bytes of all cached payloads
 *  hits/misses  — statistics counters
 *  lock         — mutex protecting all fields
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#define CACHE_BUCKET_COUNT  512   /* must be power of 2 */

#ifndef FLEXQL_CACHE_FORWARD_DECL
#define FLEXQL_CACHE_FORWARD_DECL
typedef struct LRUCache LRUCache;
#endif
struct LRUCache {
    CacheEntry     *head;
    CacheEntry     *tail;
    CacheEntry     *buckets[CACHE_BUCKET_COUNT];
    int             count;
    size_t          total_bytes;
    uint64_t        hits;
    uint64_t        misses;
    uint64_t        evictions;
    pthread_mutex_t lock;
};

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  PUBLIC API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/** Allocate and initialise a new LRU cache. Returns NULL on OOM. */
LRUCache *cache_create(void);

/** Free the cache and all its entries. Safe to call with NULL. */
void cache_free(LRUCache *cache);

/**
 * cache_get — look up a query result in the cache.
 *
 * @param cache     The LRU cache.
 * @param db_name   Current database name (used as key prefix).
 * @param sql       The SQL query string.
 * @param out_entry OUT: pointer to the matching CacheEntry on HIT, NULL on MISS.
 *                  The entry is locked in place (moved to MRU position).
 *                  The caller must NOT free the entry — it belongs to the cache.
 * @return  1 on HIT (entry found), 0 on MISS.
 */
int cache_get(LRUCache *cache, const char *db_name, const char *sql,
              CacheEntry **out_entry);

/**
 * cache_put — store a query result in the cache.
 *
 * @param cache       The LRU cache.
 * @param db_name     Current database name.
 * @param sql         The SQL query string (the cache key, without db prefix).
 * @param payloads    Array of serialised result row buffers.
 * @param sizes       Array of byte lengths matching payloads[].
 * @param count       Number of rows.
 * @return  0 on success, -1 on OOM.
 *
 * If an entry for this key already exists, it is replaced.
 * If the cache is at CACHE_MAX_ENTRIES or CACHE_MAX_BYTES, the LRU entry
 * is evicted first.
 */
int cache_put(LRUCache *cache, const char *db_name, const char *sql,
              const char **payloads, const uint32_t *sizes, int count);

/**
 * cache_invalidate_table — remove all cached entries that reference
 * a specific table in a specific database.
 *
 * Called after every INSERT INTO table_name, DROP TABLE, etc.
 * Entries are matched by checking if the key contains table_name
 * (case-insensitive substring match).
 *
 * @return  Number of entries invalidated.
 */
int cache_invalidate_table(LRUCache *cache, const char *db_name,
                           const char *table_name);

/**
 * cache_invalidate_db — remove all cached entries for a database.
 * Called on DROP DATABASE.
 * @return  Number of entries invalidated.
 */
int cache_invalidate_db(LRUCache *cache, const char *db_name);

/**
 * cache_clear — remove all entries. Useful for testing.
 */
void cache_clear(LRUCache *cache);

/**
 * cache_stats — fill in hit, miss, eviction, count, and byte counts.
 */
typedef struct {
    uint64_t hits;
    uint64_t misses;
    uint64_t evictions;
    int      count;
    size_t   total_bytes;
} CacheStats;

void cache_stats(LRUCache *cache, CacheStats *out);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_CACHE_H */