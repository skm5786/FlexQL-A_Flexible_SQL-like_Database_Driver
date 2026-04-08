/**
 * cache.cpp  —  LRU Query Result Cache  (Lesson 5)
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 5 — LRU CACHE INTERNALS
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * SECTION A — Hash function for cache keys
 * SECTION B — Doubly-linked list helpers (move-to-front, detach, append-tail)
 * SECTION C — Hash map helpers (bucket lookup, insert, remove)
 * SECTION D — Entry allocation / deallocation
 * SECTION E — Eviction logic
 * SECTION F — Public API (create, free, get, put, invalidate, stats)
 */
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cctype>
#include <strings.h>
#include "common/types.h"
#include "cache/cache.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION A — HASH FUNCTION FOR CACHE KEYS
 *
 * LESSON: We reuse djb2 (same family as in index.cpp).
 * The cache key is a string like "UNIVERSITY:SELECT * FROM STUDENT".
 * We hash it to a bucket in [0, CACHE_BUCKET_COUNT).
 *
 * Because cache keys are long strings (up to 512 chars), we use the full
 * djb2 loop rather than a multiplied integer hash.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static uint32_t cache_hash(const char *key) {
    uint32_t h = 5381;
    const unsigned char *p = (const unsigned char*)key;
    while (*p) h = (h * 33) ^ (uint32_t)(*p++);
    return h & (CACHE_BUCKET_COUNT - 1);
}

/* Build the composite cache key: "DBNAME:SQL" */
static void make_key(char *out, const char *db_name, const char *sql) {
    snprintf(out, CACHE_KEY_MAX, "%s:%s", db_name ? db_name : "", sql ? sql : "");
    /* Uppercase the key so SQL is case-insensitive for caching */
    for (char *p = out; *p; p++) *p = (char)toupper((unsigned char)*p);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION B — DOUBLY-LINKED LIST HELPERS
 *
 * LESSON: The doubly-linked list maintains LRU order.
 *   head = most recently used (MRU)
 *   tail = least recently used (LRU) — first candidate for eviction
 *
 * Three operations:
 *   detach       — remove a node from its current position
 *   push_front   — insert a node at the head (mark as MRU)
 *   move_to_front— detach then push_front (used on cache hit)
 *
 * Why doubly-linked (not singly)?
 *   Removing a node from the middle of a singly-linked list requires
 *   knowing the previous node — O(n) scan.  With prev/next pointers,
 *   removal is O(1) regardless of position.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static void list_detach(LRUCache *c, CacheEntry *e) {
    if (e->prev) e->prev->next = e->next;
    else         c->head       = e->next;   /* e was head */
    if (e->next) e->next->prev = e->prev;
    else         c->tail       = e->prev;   /* e was tail */
    e->prev = e->next = nullptr;
}

static void list_push_front(LRUCache *c, CacheEntry *e) {
    e->prev = nullptr;
    e->next = c->head;
    if (c->head) c->head->prev = e;
    else         c->tail       = e;   /* list was empty */
    c->head = e;
}

static void list_move_to_front(LRUCache *c, CacheEntry *e) {
    if (c->head == e) return;  /* already MRU */
    list_detach(c, e);
    list_push_front(c, e);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION C — HASH MAP HELPERS
 *
 * LESSON: The hash map gives O(1) key → CacheEntry* lookup.
 * We use separate chaining (each bucket is a linked list of entries
 * that hash to the same bucket).  This avoids the tombstone complexity
 * of open addressing while still being O(1) amortised.
 *
 * Why separate chaining here but open addressing in index.cpp?
 *   The cache has long string keys and variable-size values.
 *   Open addressing stores keys inline in the slot array — fine for
 *   fixed-size integer/short-string keys (index.cpp) but wasteful
 *   for 512-byte SQL strings.  Chaining stores entries on the heap
 *   and only keeps a pointer per bucket.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static CacheEntry *bucket_find(LRUCache *c, const char *key) {
    uint32_t b = cache_hash(key);
    for (CacheEntry *e = c->buckets[b]; e; e = e->hash_next)
        if (strcmp(e->key, key) == 0) return e;
    return nullptr;
}

static void bucket_insert(LRUCache *c, CacheEntry *e) {
    uint32_t b = cache_hash(e->key);
    e->hash_next  = c->buckets[b];
    c->buckets[b] = e;
}

static void bucket_remove(LRUCache *c, CacheEntry *e) {
    uint32_t b  = cache_hash(e->key);
    CacheEntry **pp = &c->buckets[b];
    while (*pp && *pp != e) pp = &(*pp)->hash_next;
    if (*pp) *pp = e->hash_next;
    e->hash_next = nullptr;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION D — ENTRY ALLOCATION / DEALLOCATION
 *
 * LESSON: One CacheEntry owns:
 *   - Its key string (fixed-size char[])
 *   - A singly-linked list of CachedPayload nodes
 *   - Each CachedPayload owns a heap-allocated data buffer
 *
 * Free order: payload buffers → payload nodes → entry struct
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static void free_entry(LRUCache *c, CacheEntry *e) {
    if (!e) return;
    /* Subtract this entry's byte contribution from total */
    c->total_bytes -= e->total_bytes;
    /* Free all payload buffers */
    CachedPayload *p = e->payloads;
    while (p) {
        CachedPayload *nxt = p->next;
        free(p->data);
        free(p);
        p = nxt;
    }
    free(e);
    c->count--;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION E — EVICTION
 *
 * LESSON: When the cache is over capacity, we remove the tail entry.
 * The tail is the LRU node — the one not accessed for the longest time.
 * This is O(1): just follow the tail pointer.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static void evict_lru(LRUCache *c) {
    if (!c->tail) return;
    CacheEntry *victim = c->tail;
    list_detach(c, victim);
    bucket_remove(c, victim);
    free_entry(c, victim);
    c->evictions++;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION F — PUBLIC API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

LRUCache *cache_create(void) {
    LRUCache *c = (LRUCache*)calloc(1, sizeof(LRUCache));
    if (!c) return nullptr;
    pthread_mutex_init(&c->lock, nullptr);
    return c;
}

void cache_free(LRUCache *c) {
    if (!c) return;
    cache_clear(c);
    pthread_mutex_destroy(&c->lock);
    free(c);
}

/* cache_get
 * LESSON: On a HIT we move the entry to the front of the list (updating
 * its LRU position to MRU).  This is what makes it "Least Recently Used" —
 * every access refreshes the entry's position.
 */
int cache_get(LRUCache *c, const char *db_name, const char *sql,
              CacheEntry **out) {
    if (!c || !sql) { if (out) *out=nullptr; return 0; }
    char key[CACHE_KEY_MAX];
    make_key(key, db_name, sql);

    pthread_mutex_lock(&c->lock);
    CacheEntry *e = bucket_find(c, key);
    if (!e) {
        c->misses++;
        pthread_mutex_unlock(&c->lock);
        if (out) *out = nullptr;
        return 0;
    }
    /* HIT: promote to MRU */
    list_move_to_front(c, e);
    c->hits++;
    pthread_mutex_unlock(&c->lock);
    if (out) *out = e;
    return 1;
}

/* cache_put
 * LESSON: Three steps:
 *   1. If key already exists, remove the old entry (replace semantics).
 *   2. Evict LRU entries until under capacity limits.
 *   3. Allocate new entry, copy all payloads, push to front of list.
 */
int cache_put(LRUCache *c, const char *db_name, const char *sql,
              const char **payloads, const uint32_t *sizes, int count) {
    if (!c || !sql) return -1;
    char key[CACHE_KEY_MAX];
    make_key(key, db_name, sql);

    /* Calculate total size for this new entry */
    size_t new_bytes = 0;
    for (int i = 0; i < count; i++) new_bytes += sizes[i];

    pthread_mutex_lock(&c->lock);

    /* Remove existing entry with same key (replace) */
    CacheEntry *existing = bucket_find(c, key);
    if (existing) {
        list_detach(c, existing);
        bucket_remove(c, existing);
        free_entry(c, existing);
    }

    /* Evict until we have room (by count and by bytes) */
    while (c->count >= CACHE_MAX_ENTRIES ||
           c->total_bytes + new_bytes > CACHE_MAX_BYTES) {
        if (!c->tail) break;
        evict_lru(c);
    }

    /* Allocate new entry */
    CacheEntry *e = (CacheEntry*)calloc(1, sizeof(CacheEntry));
    if (!e) { pthread_mutex_unlock(&c->lock); return -1; }
    strncpy(e->key, key, CACHE_KEY_MAX-1);

    /* Copy all payload buffers */
    CachedPayload **pp = &e->payloads;
    for (int i = 0; i < count; i++) {
        CachedPayload *cp = (CachedPayload*)calloc(1, sizeof(CachedPayload));
        if (!cp) { free_entry(c, e); pthread_mutex_unlock(&c->lock); return -1; }
        cp->data = (char*)malloc(sizes[i]);
        if (!cp->data) { free(cp); free_entry(c,e); pthread_mutex_unlock(&c->lock); return -1; }
        memcpy(cp->data, payloads[i], sizes[i]);
        cp->len  = sizes[i];
        *pp = cp; pp = &cp->next;
    }
    e->payload_count = count;
    e->total_bytes   = new_bytes;

    /* Register in hash map and LRU list */
    bucket_insert(c, e);
    list_push_front(c, e);
    c->count++;
    c->total_bytes += new_bytes;

    pthread_mutex_unlock(&c->lock);
    return 0;
}

/* cache_invalidate_table
 * LESSON: We scan ALL entries and remove any whose key contains
 * "DBNAME:...TABLE_NAME..." (case-insensitive after uppercasing key).
 * This is O(n) but invalidation is rare compared to get/put.
 */
int cache_invalidate_table(LRUCache *c, const char *db_name,
                           const char *table_name) {
    if (!c || !table_name) return 0;

    /* Build the prefix we're looking for: "DBNAME:" */
    char prefix[CACHE_KEY_MAX];
    snprintf(prefix, CACHE_KEY_MAX, "%s:",
             db_name ? db_name : "");
    /* Uppercase both for case-insensitive comparison */
    for (char *p = prefix; *p; p++) *p = (char)toupper((unsigned char)*p);

    char tbl_upper[FLEXQL_MAX_NAME_LEN+1];
    strncpy(tbl_upper, table_name, sizeof(tbl_upper)-1);
    tbl_upper[sizeof(tbl_upper)-1] = '\0';
    for (char *p = tbl_upper; *p; p++) *p = (char)toupper((unsigned char)*p);

    pthread_mutex_lock(&c->lock);
    int removed = 0;
    CacheEntry *e = c->head;
    while (e) {
        CacheEntry *nxt = e->next;
        /* Key must start with our db prefix AND contain the table name */
        if (strncmp(e->key, prefix, strlen(prefix)) == 0 &&
            strstr(e->key, tbl_upper) != nullptr) {
            list_detach(c, e);
            bucket_remove(c, e);
            free_entry(c, e);
            removed++;
        }
        e = nxt;
    }
    pthread_mutex_unlock(&c->lock);
    return removed;
}

int cache_invalidate_db(LRUCache *c, const char *db_name) {
    if (!c || !db_name) return 0;
    char prefix[CACHE_KEY_MAX];
    snprintf(prefix, CACHE_KEY_MAX, "%s:", db_name);
    for (char *p = prefix; *p; p++) *p = (char)toupper((unsigned char)*p);

    pthread_mutex_lock(&c->lock);
    int removed = 0;
    CacheEntry *e = c->head;
    while (e) {
        CacheEntry *nxt = e->next;
        if (strncmp(e->key, prefix, strlen(prefix)) == 0) {
            list_detach(c, e);
            bucket_remove(c, e);
            free_entry(c, e);
            removed++;
        }
        e = nxt;
    }
    pthread_mutex_unlock(&c->lock);
    return removed;
}

void cache_clear(LRUCache *c) {
    if (!c) return;
    pthread_mutex_lock(&c->lock);
    while (c->head) {
        CacheEntry *e = c->head;
        list_detach(c, e);
        bucket_remove(c, e);
        free_entry(c, e);
    }
    memset(c->buckets, 0, sizeof(c->buckets));
    pthread_mutex_unlock(&c->lock);
}

void cache_stats(LRUCache *c, CacheStats *out) {
    if (!c || !out) return;
    pthread_mutex_lock(&c->lock);
    out->hits        = c->hits;
    out->misses      = c->misses;
    out->evictions   = c->evictions;
    out->count       = c->count;
    out->total_bytes = c->total_bytes;
    pthread_mutex_unlock(&c->lock);
}