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
 * SCOPE:
 *   One HashIndex is created per Table that has a PRIMARY KEY column.
 *   If a table has no PK, pk_index is NULL and the old O(n) scan is used.
 */

#ifndef FLEXQL_INDEX_H
#define FLEXQL_INDEX_H

#include "common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

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
#define HASH_INITIAL_CAPACITY  16
#define HASH_LOAD_FACTOR_NUM   7
#define HASH_LOAD_FACTOR_DEN   10

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
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * index_create — allocate a new empty hash index for the given PK type.
 *
 * @param pk_type   COL_TYPE_INT or COL_TYPE_VARCHAR (DECIMAL/DATETIME
 *                  are treated as INT internally).
 * @return  Heap-allocated HashIndex*, or NULL on allocation failure.
 */
HashIndex *index_create(ColumnType pk_type);

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