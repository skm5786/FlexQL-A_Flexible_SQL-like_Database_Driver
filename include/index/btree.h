/**
 * btree.h  —  B+ Tree Index for Range Queries  (Lesson 10)
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * WHY A B+ TREE?
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * The hash index (Lesson 4) is O(1) for equality:  WHERE id = 5
 * But it cannot answer range queries:               WHERE balance > 1000
 * Those still fall through to an O(n) full scan.
 *
 * A B+ tree stores keys in SORTED ORDER, so:
 *   WHERE balance > 1000  → find the first key ≥ 1001, walk right  O(log n + k)
 *   WHERE balance BETWEEN 500 AND 2000  → same, stop at 2000       O(log n + k)
 *   ORDER BY balance       → in-order traversal of leaf nodes       O(n)
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * STRUCTURE
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * A B+ tree of ORDER t:
 *   - Every internal node holds between t-1 and 2t-1 keys.
 *   - Every leaf node holds between t-1 and 2t-1 (key, Row*) pairs.
 *   - All leaves are at the same depth — perfectly balanced always.
 *   - Leaf nodes are doubly-linked: leaf[0] ↔ leaf[1] ↔ leaf[2] ↔ ...
 *     This linkage enables efficient range scans without backtracking.
 *
 * We use ORDER = 32 (each node holds 31–63 keys).  With 1M rows:
 *   Height = ceil(log_32(1M)) ≈ 4 levels
 *   Point lookup: 4 node comparisons — effectively O(1) in practice.
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * SCOPE
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * One BTree is created per non-PK column of type INT or DECIMAL.
 * These are the columns most commonly used in range queries.
 * VARCHAR range queries are less common and left for future work.
 *
 * The B+ tree is built ONCE at table_create time (empty), then populated
 * incrementally by row_insert().  It is rebuilt from WAL on startup.
 * It is NOT separately persisted to disk — it is a derived structure.
 */

#ifndef FLEXQL_BTREE_H
#define FLEXQL_BTREE_H

#include "common/types.h"
#include "storage/storage.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BTREE_ORDER       32   /* Each node: 31–63 keys, t-1 to 2t-1        */
#define BTREE_MAX_KEYS    (2 * BTREE_ORDER - 1)   /* max keys per node = 63  */
#define BTREE_MIN_KEYS    (BTREE_ORDER - 1)       /* min keys per node = 31  */

/* ── B+ tree node ────────────────────────────────────────────────────────── */
typedef struct BTreeNode {
    int    is_leaf;                       /* 1 = leaf, 0 = internal          */
    int    n_keys;                        /* current number of keys           */
    double keys[BTREE_MAX_KEYS];          /* sorted key values (doubles)      */

    /* Leaf: rows[i] = Row* for keys[i].  Internal: unused.                  */
    Row   *rows[BTREE_MAX_KEYS];

    /* Internal: children[i] = child node pointer (n_keys+1 children)        */
    /* Leaf:     children[0] = prev leaf, children[1] = next leaf            */
    struct BTreeNode *children[BTREE_MAX_KEYS + 1];
} BTreeNode;

/* ── B+ tree root struct ─────────────────────────────────────────────────── */
struct BTree {
    BTreeNode  *root;
    ColumnType  key_type;   /* COL_TYPE_INT or COL_TYPE_DECIMAL              */
    size_t      count;      /* number of live entries                        */
};

/* ── Public API ──────────────────────────────────────────────────────────── */

/** Allocate an empty B+ tree for the given key type. */
BTree *btree_create(ColumnType key_type);

/** Free all nodes and the tree itself. Safe to call with NULL. */
void btree_free(BTree *bt);

/**
 * btree_insert — insert (key, row) into the tree.
 * key is read from cell->data.int_val or cell->data.decimal_val.
 * Duplicate keys are allowed (same value, different rows).
 */
int btree_insert(BTree *bt, const CellValue *key, Row *row);

/**
 * btree_range_scan — invoke cb(row, arg) for every live row whose key
 * satisfies:  lo_op(key, lo)  AND  hi_op(key, hi)
 *
 * Pass NULL for lo or hi to mean "unbounded".
 * lo_op / hi_op: OP_GT, OP_GTE (for lo) or OP_LT, OP_LTE (for hi).
 *
 * Returns number of rows visited.
 */
int btree_range_scan(BTree *bt,
                     const CellValue *lo, CompareOp lo_op,
                     const CellValue *hi, CompareOp hi_op,
                     ScanCallback cb, void *arg);

/** Number of entries currently in the tree. */
size_t btree_size(const BTree *bt);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_BTREE_H */