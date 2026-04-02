/**
 * btree.cpp  —  B+ Tree Index  (Lesson 10)
 *
 * Sections:
 *   A. Key extraction and comparison
 *   B. Node allocation / deallocation
 *   C. Leaf insertion (with split on overflow)
 *   D. Internal node insertion and root split
 *   E. Range scan via leaf linked list
 *   F. Public API
 */
#include <cstdlib>
#include <cstring>
#include <cmath>
#include "index/btree.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * A — KEY EXTRACTION AND COMPARISON
 *
 * We store all keys as double regardless of the original column type.
 * int64_t up to 2^53 is representable exactly as double, which covers
 * all practical IDs and timestamps.  For DECIMAL, it is already a double.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static double cell_to_double(const CellValue *c) {
    if (!c || c->is_null) return 0.0;
    switch (c->type) {
    case COL_TYPE_INT:      return (double)c->data.int_val;
    case COL_TYPE_DATETIME: return (double)c->data.int_val;
    case COL_TYPE_DECIMAL:  return c->data.decimal_val;
    default:                return 0.0;
    }
}

/* Compare a stored double key against a CellValue bound. Returns 1 if passes. */
static int key_passes(double k, const CellValue *bound, CompareOp op) {
    if (!bound) return 1;
    double b = cell_to_double(bound);
    switch (op) {
    case OP_GT:  return k >  b;
    case OP_GTE: return k >= b;
    case OP_LT:  return k <  b;
    case OP_LTE: return k <= b;
    case OP_EQ:  return k == b;
    default:     return 1;
    }
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * B — NODE ALLOCATION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static BTreeNode *alloc_node(int is_leaf) {
    BTreeNode *n = (BTreeNode*)calloc(1, sizeof(BTreeNode));
    if (n) n->is_leaf = is_leaf;
    return n;
}

static void free_node(BTreeNode *n) {
    if (!n) return;
    if (!n->is_leaf) {
        for (int i = 0; i <= n->n_keys; i++)
            free_node(n->children[i]);
    }
    free(n);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * C — LEAF INSERTION AND SPLIT
 *
 * LESSON: B+ tree insertion proceeds in two phases:
 *   Phase 1: Descend to the correct leaf (sorted order).
 *   Phase 2: Insert into the leaf. If it overflows (n_keys == MAX_KEYS),
 *            split into two leaves and push the median key up.
 *
 * We implement this with a "split_child" helper that the parent calls
 * when it detects a full child.  This is the standard bottom-up approach.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/* Insert (key, row) into a leaf node — caller guarantees room. */
static void leaf_insert_nonfull(BTreeNode *leaf, double key, Row *row) {
    int i = leaf->n_keys - 1;
    /* Shift entries right to find sorted insertion position */
    while (i >= 0 && leaf->keys[i] > key) {
        leaf->keys[i+1]  = leaf->keys[i];
        leaf->rows[i+1]  = leaf->rows[i];
        i--;
    }
    leaf->keys[i+1] = key;
    leaf->rows[i+1] = row;
    leaf->n_keys++;
}

/* Split leaf->children[idx] (a full leaf) into two leaves,
   pushing the median key up into parent at position idx.          */
static void split_leaf(BTreeNode *parent, int idx) {
    BTreeNode *left  = parent->children[idx];
    BTreeNode *right = alloc_node(1);
    if (!right) return;

    int mid = BTREE_MAX_KEYS / 2;  /* split point */

    /* Copy the upper half of left into right */
    right->n_keys = left->n_keys - mid;
    for (int j = 0; j < right->n_keys; j++) {
        right->keys[j] = left->keys[mid + j];
        right->rows[j] = left->rows[mid + j];
    }
    left->n_keys = mid;

    /* Stitch leaf doubly-linked list:  left ↔ right ↔ left->children[1] */
    right->children[1] = left->children[1];   /* right->next = left's old next */
    right->children[0] = left;                /* right->prev = left */
    if (left->children[1])
        left->children[1]->children[0] = right;  /* old next's prev = right */
    left->children[1] = right;                /* left->next = right */

    /* Push first key of right up into parent */
    for (int j = parent->n_keys; j > idx; j--) {
        parent->keys[j]     = parent->keys[j-1];
        parent->children[j+1] = parent->children[j];
    }
    parent->keys[idx]     = right->keys[0];
    parent->children[idx+1] = right;
    parent->n_keys++;
}

/* Split an internal node (not a leaf) — push median up to parent. */
static void split_internal(BTreeNode *parent, int idx) {
    BTreeNode *left  = parent->children[idx];
    BTreeNode *right = alloc_node(0);
    if (!right) return;

    int mid = BTREE_ORDER - 1;

    right->n_keys = left->n_keys - mid - 1;
    for (int j = 0; j < right->n_keys; j++) {
        right->keys[j]     = left->keys[mid + 1 + j];
        right->children[j] = left->children[mid + 1 + j];
    }
    right->children[right->n_keys] = left->children[left->n_keys];

    double push_up = left->keys[mid];
    left->n_keys   = mid;

    for (int j = parent->n_keys; j > idx; j--) {
        parent->keys[j]       = parent->keys[j-1];
        parent->children[j+1] = parent->children[j];
    }
    parent->keys[idx]       = push_up;
    parent->children[idx+1] = right;
    parent->n_keys++;
}

/* Insert into a subtree rooted at node — node is guaranteed to have room. */
static void insert_nonfull(BTreeNode *node, double key, Row *row) {
    if (node->is_leaf) {
        leaf_insert_nonfull(node, key, row);
        return;
    }
    /* Find the child to descend into */
    int i = node->n_keys - 1;
    while (i >= 0 && key < node->keys[i]) i--;
    i++;  /* child[i] is the subtree for this key */

    BTreeNode *child = node->children[i];
    if (child->n_keys == BTREE_MAX_KEYS) {
        /* Child is full — split it first, then choose which half to descend */
        if (child->is_leaf) split_leaf(node, i);
        else                split_internal(node, i);
        /* After split, the key that was pushed up sits at node->keys[i].
           If our key is >= that separator, go right (child i+1). */
        if (key >= node->keys[i]) i++;
    }
    insert_nonfull(node->children[i], key, row);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * E — RANGE SCAN
 *
 * LESSON: Range scan in a B+ tree:
 *   1. Descend to the first leaf whose keys could match the lower bound.
 *   2. Walk the leaf doubly-linked list rightward, calling cb() for each
 *      key that passes both the lower and upper bound check.
 *   3. Stop when the upper bound is exceeded or the list ends.
 *
 * The linked list makes step 2 O(k) where k is the number of matching rows.
 * Without the linked list, we would need to walk back up the tree for each
 * leaf, which is more complex and slightly slower.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/* Find the leftmost leaf that could contain keys >= lo */
static BTreeNode *find_first_leaf(BTreeNode *node, double lo_val,
                                   int has_lo, int lo_inclusive) {
    while (!node->is_leaf) {
        if (!has_lo) {
            node = node->children[0];
        } else {
            int i = 0;
            while (i < node->n_keys) {
                if (lo_inclusive && lo_val < node->keys[i]) break;
                if (!lo_inclusive && lo_val <= node->keys[i]) break;
                i++;
            }
            node = node->children[i];
        }
    }
    return node;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * F — PUBLIC API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
BTree *btree_create(ColumnType key_type) {
    BTree *bt = (BTree*)calloc(1, sizeof(BTree));
    if (!bt) return nullptr;
    bt->root     = alloc_node(1);  /* start with a single empty leaf */
    bt->key_type = key_type;
    bt->count    = 0;
    return bt;
}

void btree_free(BTree *bt) {
    if (!bt) return;
    free_node(bt->root);
    free(bt);
}

int btree_insert(BTree *bt, const CellValue *key, Row *row) {
    if (!bt || !key || key->is_null) return -1;
    double k = cell_to_double(key);

    BTreeNode *root = bt->root;
    if (root->n_keys == BTREE_MAX_KEYS) {
        /* Root is full — create new root and split old root as first child */
        BTreeNode *new_root = alloc_node(0);
        if (!new_root) return -1;
        new_root->children[0] = root;
        new_root->n_keys      = 0;
        bt->root              = new_root;

        if (root->is_leaf) split_leaf(new_root, 0);
        else               split_internal(new_root, 0);

        insert_nonfull(new_root, k, row);
    } else {
        insert_nonfull(root, k, row);
    }
    bt->count++;
    return 0;
}

int btree_range_scan(BTree *bt,
                     const CellValue *lo, CompareOp lo_op,
                     const CellValue *hi, CompareOp hi_op,
                     ScanCallback cb, void *arg) {
    if (!bt || !bt->root) return 0;

    double lo_val = lo ? cell_to_double(lo) : 0.0;
    int    has_lo = (lo != nullptr);
    int    lo_inc = (lo_op == OP_GTE || lo_op == OP_EQ);

    /* Find the starting leaf */
    BTreeNode *leaf = find_first_leaf(bt->root, lo_val, has_lo, lo_inc);

    int visited = 0;
    time_t now = time(nullptr);

    /* Walk forward through leaves */
    while (leaf) {
        for (int i = 0; i < leaf->n_keys; i++) {
            double k = leaf->keys[i];

            /* Check lower bound */
            if (has_lo && !key_passes(k, lo, lo_op)) continue;

            /* Check upper bound — if we've passed it, we're done */
            if (hi && !key_passes(k, hi, hi_op)) goto done;

            Row *row = leaf->rows[i];
            if (!row) continue;
            /* Skip expired rows */
            if (row->expiry > 0 && row->expiry < now) continue;

            visited++;
            if (cb(row, arg)) goto done;
        }
        leaf = leaf->children[1];  /* follow linked list to next leaf */
    }
done:
    return visited;
}

size_t btree_size(const BTree *bt) {
    return bt ? bt->count : 0;
}