/**
 * index.h  —  Primary Key Hash Index
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 4 — THE HASH INDEX
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * WHY AN INDEX?
 *   Without an index, every query like  WHERE ID = 5  must walk the
 *   entire linked list of rows until it finds ID=5 — O(n) time.
 *   On 10 million rows that means 10 million comparisons per query.
 *
 *   A hash index solves this by maintaining a separate data structure
 *   that maps  pk_value → Row*  so the lookup is O(1) average time —
 *   one hash computation + a few slot probes, regardless of table size.
 *
 * WHAT KIND OF INDEX?
 *   We use a HASH MAP with OPEN ADDRESSING and LINEAR PROBING.
 *
 *   Hash map: stores (key, value) pairs.  Key = PK cell value. Value = Row*.
 *
 *   Open addressing: all slots live in one flat array (no linked lists
 *   inside the hash map itself).  When a slot is occupied, we probe
 *   the next slot linearly until we find an empty one.
 *
 *   Why open addressing over chaining?
 *     - Better CPU cache behaviour — the probe sequence is contiguous memory.
 *     - No per-entry heap allocations for chain nodes.
 *     - Simpler to implement correctly.
 *     - Works well at load factors ≤ 0.7.
 *
 * LINEAR PROBING EXAMPLE:
 *   Table has capacity=8.  hash("Alice") = 3.
 *
 *   INSERT "Alice":
 *     slot[3] empty  → store here.
 *
 *   INSERT "Bob" where hash("Bob") = 3 also:
 *     slot[3] occupied by Alice → probe slot[4].
 *     slot[4] empty  → store here.
 *
 *   LOOKUP "Bob":
 *     slot[3]: occupied, key="Alice" ≠ "Bob" → probe slot[4].
 *     slot[4]: occupied, key="Bob" = "Bob"   → found!  return Row*.
 *
 * TOMBSTONE DELETION:
 *   When we delete a key, we cannot simply mark the slot empty — that would
 *   break lookup chains.  Instead we mark it as TOMBSTONE.
 *   Tombstones are skipped during lookup (probe continues past them)
 *   but counted as "used" for load-factor calculation.
 *   Tombstones are reused on insert.
 *
 * RESIZE (REHASH):
 *   When the ratio (occupied + tombstones) / capacity exceeds LOAD_FACTOR
 *   (0.7), we allocate a new array 2× larger and re-insert all live entries.
 *   This keeps the average probe sequence length near 1.
 *
 * COMPLEXITY:
 *   index_get:    O(1) average,  O(n) worst case (all keys hash to same slot)
 *   index_put:    O(1) amortised (O(n) on resize, rare)
 *   index_remove: O(1) average
 *
 * PRE-SIZING:
 *   index_create_presized(n) allocates capacity for n entries upfront.
 *   For 10M-row inserts this eliminates ~23 resize operations that would
 *   each copy up to 10M entries — a massive hidden cost.
 *
 * SCOPE:
 *   One HashIndex is created per Table that has a PRIMARY KEY column.
 *   If a table has no PK, pk_index is NULL and the old O(n) scan is used.
 */

#ifndef FLEXQL_INDEX_H
#define FLEXQL_INDEX_H

#include "common/types.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  SLOT STATE
 *
 *  Each slot in the hash array is in one of three states:
 *    EMPTY      — never used, stops a lookup probe chain
 *    OCCUPIED   — holds a live (key, value) pair
 *    TOMBSTONE  — was occupied, key was deleted; probe chain continues
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef enum {
    SLOT_EMPTY     = 0,
    SLOT_OCCUPIED  = 1,
    SLOT_TOMBSTONE = 2
} SlotState;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  HASH SLOT  (one cell in the flat array)
 *
 *  key_int    — used when PK column type is INT or DATETIME
 *  key_str    — used when PK column type is VARCHAR (fixed buffer, no heap)
 *  row        — pointer back into the linked list; NULL for non-OCCUPIED slots
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct {
    SlotState  state;
    ColumnType key_type;                  /* INT or VARCHAR               */
    int64_t    key_int;                   /* used when key_type = INT     */
    char       key_str[FLEXQL_MAX_NAME_LEN * 4]; /* used when key_type = VARCHAR */
    Row       *row;                       /* the data row this key maps to */
} HashSlot;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  HASH INDEX  (the top-level struct held inside Table)
 *
 *  slots      — heap-allocated flat array of HashSlot[capacity]
 *  capacity   — current array size (always a power of 2)
 *  count      — number of OCCUPIED slots (live entries)
 *  tombstones — number of TOMBSTONE slots
 *  pk_type    — column type of the primary key (drives hash + compare logic)
 *
 *  INVARIANT: (count + tombstones) / capacity < HASH_LOAD_FACTOR
 *  Maintained by index_put() which triggers a resize when violated.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#define HASH_INITIAL_CAPACITY  256        /* enough for small tables */
#define HASH_LOAD_FACTOR_NUM   7
#define HASH_LOAD_FACTOR_DEN   10

/* PERF: For 10M-row tables, pre-size to avoid 23 expensive rehash operations.
 * 10M / 0.7 load factor = 14.3M slots → round up to next power-of-2 = 16M.
 * Each rehash at 10M entries copies ~160MB of slot data — very expensive.
 * Pre-sizing eliminates all rehashes at the cost of one 16M × sizeof(HashSlot)
 * = 16M × 56 bytes = 896MB upfront allocation (which we avoid by pre-sizing
 * only when the caller knows the expected row count). */
#define HASH_PRESIZED_CAPACITY (1 << 24) /* 16,777,216 — handles up to 11.7M rows */

/* The forward declaration in common/types.h is:
 *   typedef struct HashIndex HashIndex;
 * We just define the struct body here.  The guard prevents the typedef
 * being re-emitted if this header is included after types.h.              */
#ifndef FLEXQL_INDEX_FORWARD_DECL
#define FLEXQL_INDEX_FORWARD_DECL
typedef struct HashIndex HashIndex;
#endif

struct HashIndex {
    HashSlot  *slots;
    size_t     capacity;
    size_t     count;
    size_t     tombstones;
    ColumnType pk_type;
};

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  PUBLIC API
 *
 *  The extern "C" block gives every function below C linkage (no C++ name
 *  mangling).  The definitions in index.cpp are wrapped in a matching
 *  extern "C" { } block so declaration and definition agree on linkage.
 *  This consistency is required by -flto (link-time optimisation), which
 *  validates linkage at the IR level and produces "undefined reference"
 *  errors if the declaration and definition differ.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#ifdef __cplusplus
extern "C" {
#endif

/**
 * index_create — allocate a new empty hash index for the given PK type.
 *
 * @param pk_type   COL_TYPE_INT or COL_TYPE_VARCHAR (DECIMAL/DATETIME
 *                  are treated as INT internally).
 * @return  Heap-allocated HashIndex*, or NULL on allocation failure.
 */
HashIndex *index_create(ColumnType pk_type);

/**
 * index_create_presized — allocate a hash index pre-sized for expected_rows.
 *
 * @param pk_type       Column type of the primary key.
 * @param expected_rows Anticipated maximum number of rows to be inserted.
 *                      The internal capacity will be the next power-of-2
 *                      above expected_rows / HASH_LOAD_FACTOR so no rehash
 *                      ever occurs during normal operation.
 * @return  Heap-allocated HashIndex*, or NULL on allocation failure.
 *
 * LESSON: Rehashing at 10M rows copies ~160MB of slot data.  With 23 rehashes
 * from capacity=256 to capacity=16M, that is ~2.3GB of data movement just
 * for the index.  Pre-sizing to 16M slots upfront costs one allocation and
 * zero rehashes.  The trade-off: we allocate 16M × 56 bytes = 896MB even if
 * the table ends up with fewer rows.  For the TA benchmark this is acceptable
 * since BIG_USERS always receives exactly 10M rows.
 */
HashIndex *index_create_presized(ColumnType pk_type, size_t expected_rows);

/**
 * index_free — free the index and all its slots.
 * Safe to call with NULL.
 */
void index_free(HashIndex *idx);

/**
 * index_put — insert or update an entry in the index.
 *
 * @param idx   The hash index.
 * @param key   The primary key cell value (INT or VARCHAR).
 * @param row   The Row* to associate with this key.
 * @return  0 on success, -1 on allocation failure (out of memory).
 *
 * LESSON: index_put is called every time row_insert() succeeds.
 *   It keeps the index in sync with the linked list.
 *   If the key already exists (shouldn't happen if PK constraint holds),
 *   the existing entry is updated to point to the new row.
 */
int index_put(HashIndex *idx, const CellValue *key, Row *row);

/**
 * index_put_int — fast path for integer primary keys.
 *
 * Skips the CellValue union dispatch when the PK type is already known
 * to be INT or DECIMAL.  Called by row_insert() on the hot INSERT path.
 * Avoids one level of branching per insertion.
 *
 * @return  0 on success, -1 on OOM.
 */
int index_put_int(HashIndex *idx, int64_t key_int, Row *row);

/**
 * index_get — look up a row by its primary key value.
 *
 * @param idx   The hash index.
 * @param key   The primary key cell value to search for.
 * @return  Pointer to the Row, or NULL if not found.
 *
 * LESSON: This is the O(1) operation that replaces the O(n) list scan.
 *   Called from table_scan() when the WHERE clause is on the PK column.
 */
Row *index_get(const HashIndex *idx, const CellValue *key);

/**
 * index_remove — remove an entry from the index (marks slot as TOMBSTONE).
 *
 * @param idx   The hash index.
 * @param key   The primary key cell value to remove.
 * @return  0 if found and removed, -1 if key not found.
 *
 * LESSON: Called by the expiry background thread (Lesson 6) when it
 *   physically removes an expired row from the linked list.
 */
int index_remove(HashIndex *idx, const CellValue *key);

/**
 * index_size — return the number of live entries in the index.
 */
size_t index_size(const HashIndex *idx);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_INDEX_H */