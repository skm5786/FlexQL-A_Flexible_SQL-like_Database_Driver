/**
 * index.cpp  —  Primary Key Hash Index  (Lesson 4 — Open Addressing)
 */
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include "index/index.h"

/* ── Section A: Hash functions ──────────────────────────────────────────────
 * LESSON: Two separate hash functions — one per key type.
 *
 * INT  → Knuth multiplicative hash: key × (2^64/φ), then mask to [0,cap).
 *         The golden-ratio multiplier spreads clustered integers evenly.
 *
 * VARCHAR → djb2a: hash = 5381; for each char: hash = hash*33 XOR c.
 *           The constant 33 (= 2^5+1) gives good spread on ASCII strings.
 *
 * WHY capacity must be a power of 2:
 *   h & (cap-1)  is one AND instruction (fast modulo by a power of 2).
 *   cap % n with arbitrary n requires a division instruction (slow).
 * ─────────────────────────────────────────────────────────────────────────── */
static size_t hash_int(int64_t key, size_t cap) {
    uint64_t k = (uint64_t)key;
    uint64_t h = k * UINT64_C(11400714819323198485);
    return (size_t)(h & (cap - 1));
}
static size_t hash_str(const char *s, size_t cap) {
    uint64_t h = 5381;
    const unsigned char *p = (const unsigned char*)s;
    while (*p) h = (h * 33) ^ (uint64_t)(*p++);
    return (size_t)(h & (cap - 1));
}
static size_t hash_key(const CellValue *k, size_t cap) {
    if (!k || k->is_null) return 0;
    switch (k->type) {
    case COL_TYPE_INT: case COL_TYPE_DATETIME:
        return hash_int(k->data.int_val, cap);
    case COL_TYPE_VARCHAR:
        return k->data.varchar_val ? hash_str(k->data.varchar_val, cap) : 0;
    case COL_TYPE_DECIMAL: {
        int64_t bits; memcpy(&bits, &k->data.decimal_val, 8);
        return hash_int(bits, cap);
    }
    default: return 0;
    }
}

/* ── Section B: Key comparison ──────────────────────────────────────────────
 * LESSON: hash tells WHERE to look; comparison tells WHETHER this is the key.
 * Hash collisions are normal — multiple keys may map to the same initial slot.
 * ─────────────────────────────────────────────────────────────────────────── */
static int keys_equal(const HashSlot *slot, const CellValue *k) {
    if (slot->state != SLOT_OCCUPIED) return 0;
    if (slot->key_type != k->type || k->is_null) return 0;
    switch (k->type) {
    case COL_TYPE_INT: case COL_TYPE_DATETIME:
        return slot->key_int == k->data.int_val;
    case COL_TYPE_VARCHAR:
        return k->data.varchar_val &&
               strcmp(slot->key_str, k->data.varchar_val) == 0;
    case COL_TYPE_DECIMAL: {
        int64_t a, b;
        memcpy(&a, &slot->key_int,        8);
        memcpy(&b, &k->data.decimal_val,  8);
        return a == b;
    }
    default: return 0;
    }
}
static void store_key(HashSlot *slot, const CellValue *k) {
    slot->key_type = k->type;
    switch (k->type) {
    case COL_TYPE_INT: case COL_TYPE_DATETIME:
        slot->key_int = k->data.int_val; break;
    case COL_TYPE_VARCHAR:
        if (k->data.varchar_val) {
            strncpy(slot->key_str, k->data.varchar_val, sizeof(slot->key_str)-1);
            slot->key_str[sizeof(slot->key_str)-1] = '\0';
        } else slot->key_str[0] = '\0';
        break;
    case COL_TYPE_DECIMAL:
        memcpy(&slot->key_int, &k->data.decimal_val, 8); break;
    default: break;
    }
}

/* ── Section C: Probe sequences ─────────────────────────────────────────────
 * LESSON: Linear probing — given starting slot h, check h, h+1, h+2, ...
 * wrapping around with & (cap-1).
 *
 * probe_find:   for LOOKUP — stop at EMPTY (not found) or matching key.
 *               Skip TOMBSTONE slots (the chain continues past them).
 *
 * probe_insert: for INSERT — also returns first TOMBSTONE seen (reuse it)
 *               or first EMPTY slot if no tombstone encountered.
 *
 * TOMBSTONE exists because deleting an OCCUPIED slot by marking it EMPTY
 * would silently break lookup chains for later-inserted keys that were
 * displaced past this position during insertion.
 * ─────────────────────────────────────────────────────────────────────────── */
static size_t probe_find(const HashIndex *idx, const CellValue *k) {
    size_t h = hash_key(k, idx->capacity), cap = idx->capacity;
    for (size_t i = 0; i < cap; i++) {
        size_t pos = (h + i) & (cap - 1);
        if (idx->slots[pos].state == SLOT_EMPTY)        return SIZE_MAX;
        if (keys_equal(&idx->slots[pos], k))            return pos;
    }
    return SIZE_MAX;
}
static size_t probe_insert(const HashIndex *idx, const CellValue *k) {
    size_t h = hash_key(k, idx->capacity), cap = idx->capacity;
    size_t tomb = SIZE_MAX;
    for (size_t i = 0; i < cap; i++) {
        size_t pos = (h + i) & (cap - 1);
        HashSlot *s = &idx->slots[pos];
        if (s->state == SLOT_EMPTY)        return tomb != SIZE_MAX ? tomb : pos;
        if (s->state == SLOT_TOMBSTONE && tomb == SIZE_MAX) tomb = pos;
        if (keys_equal(s, k))              return pos;
    }
    return tomb != SIZE_MAX ? tomb : SIZE_MAX;
}

/* ── Section D: Resize / rehash ─────────────────────────────────────────────
 * LESSON: When (count+tombstones)/capacity >= 0.7, double the array.
 * Re-insert all OCCUPIED entries into the new array.
 * Tombstones dissolve — they are NOT re-inserted, cleaning up dead entries.
 * Load factor drops from ~0.7 to ~0.35 after resize.
 * ─────────────────────────────────────────────────────────────────────────── */
static int resize(HashIndex *idx) {
    size_t new_cap = idx->capacity * 2;
    HashSlot *ns = (HashSlot*)calloc(new_cap, sizeof(HashSlot));
    if (!ns) return -1;

    for (size_t i = 0; i < idx->capacity; i++) {
        if (idx->slots[i].state != SLOT_OCCUPIED) continue;
        /* Re-hash entry into new array */
        CellValue tmp{}; tmp.type = idx->slots[i].key_type; tmp.is_null = 0;
        if (tmp.type == COL_TYPE_VARCHAR)
            tmp.data.varchar_val = idx->slots[i].key_str;
        else
            tmp.data.int_val = idx->slots[i].key_int;

        size_t h = hash_key(&tmp, new_cap);
        for (size_t j = 0; j < new_cap; j++) {
            size_t pos = (h + j) & (new_cap - 1);
            if (ns[pos].state == SLOT_EMPTY) { ns[pos] = idx->slots[i]; break; }
        }
    }
    free(idx->slots);
    idx->slots = ns; idx->capacity = new_cap; idx->tombstones = 0;
    return 0;
}

/* ── Section E: Public API ──────────────────────────────────────────────── */
HashIndex *index_create(ColumnType pk_type) {
    HashIndex *idx = (HashIndex*)calloc(1, sizeof(HashIndex));
    if (!idx) return nullptr;
    idx->slots = (HashSlot*)calloc(HASH_INITIAL_CAPACITY, sizeof(HashSlot));
    if (!idx->slots) { free(idx); return nullptr; }
    idx->capacity = HASH_INITIAL_CAPACITY;
    idx->pk_type  = pk_type;
    return idx;
}

void index_free(HashIndex *idx) {
    if (!idx) return;
    free(idx->slots); free(idx);
}

int index_put(HashIndex *idx, const CellValue *key, Row *row) {
    if (!idx || !key || key->is_null) return -1;
    /* Resize BEFORE insert if needed */
    if ((idx->count + idx->tombstones + 1) * HASH_LOAD_FACTOR_DEN >=
         idx->capacity * HASH_LOAD_FACTOR_NUM)
        if (resize(idx) != 0) return -1;

    size_t pos = probe_insert(idx, key);
    if (pos == SIZE_MAX) return -1;

    HashSlot *s = &idx->slots[pos];
    if      (s->state == SLOT_TOMBSTONE) { idx->tombstones--; idx->count++; }
    else if (s->state == SLOT_EMPTY)     { idx->count++; }
    /* OCCUPIED with same key → update in place, count unchanged */

    s->state = SLOT_OCCUPIED;
    store_key(s, key);
    s->row = row;
    return 0;
}

Row *index_get(const HashIndex *idx, const CellValue *key) {
    if (!idx || !key || key->is_null) return nullptr;
    size_t pos = probe_find(idx, key);
    return pos == SIZE_MAX ? nullptr : idx->slots[pos].row;
}

int index_remove(HashIndex *idx, const CellValue *key) {
    if (!idx || !key || key->is_null) return -1;
    size_t pos = probe_find(idx, key);
    if (pos == SIZE_MAX) return -1;
    idx->slots[pos].state = SLOT_TOMBSTONE;
    idx->slots[pos].row   = nullptr;
    idx->count--;
    idx->tombstones++;
    return 0;
}

size_t index_size(const HashIndex *idx) {
    return idx ? idx->count : 0;
}