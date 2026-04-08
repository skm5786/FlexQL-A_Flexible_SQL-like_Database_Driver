/**
 * arena.cpp  —  Row Arena Allocator  (Lesson 11)
 */
#include <cstdlib>
#include <cstring>
#include "storage/arena.h"

/* Align allocations to 8 bytes — required for correct access to
   int64_t/double fields in CellValue on all architectures.          */
static inline size_t align8(size_t n) { return (n + 7) & ~(size_t)7; }

static ArenaSlab *new_slab(size_t min_size) {
    size_t cap = (min_size > ARENA_SLAB_SIZE) ? min_size : ARENA_SLAB_SIZE;
    ArenaSlab *s = (ArenaSlab*)malloc(sizeof(ArenaSlab));
    if (!s) return nullptr;
    s->base = (char*)calloc(1, cap);
    if (!s->base) { free(s); return nullptr; }
    s->used = 0; s->cap = cap; s->next = nullptr;
    return s;
}

Arena *arena_create(void) {
    Arena *a = (Arena*)calloc(1, sizeof(Arena));
    if (!a) return nullptr;
    a->head = new_slab(ARENA_SLAB_SIZE);
    if (!a->head) { free(a); return nullptr; }
    a->total_slabs = 1;
    return a;
}

void arena_free(Arena *arena) {
    if (!arena) return;
    ArenaSlab *s = arena->head;
    while (s) {
        ArenaSlab *nxt = s->next;
        free(s->base);
        free(s);
        s = nxt;
    }
    free(arena);
}

void *arena_alloc(Arena *arena, size_t size) {
    if (!arena || size == 0) return nullptr;
    size = align8(size);

    /* If current slab has room, bump the pointer */
    if (arena->head && arena->head->used + size <= arena->head->cap) {
        void *p = arena->head->base + arena->head->used;
        arena->head->used += size;
        arena->total_bytes += size;
        return p;  /* already zeroed by calloc at slab creation */
    }

    /* Need a new slab */
    ArenaSlab *s = new_slab(size);
    if (!s) return nullptr;
    s->next     = arena->head;
    arena->head = s;
    arena->total_slabs++;

    void *p = s->base;
    s->used = size;
    arena->total_bytes += size;
    return p;
}

char *arena_alloc_str(Arena *arena, const char *str) {
    if (!str) return nullptr;
    size_t len = strlen(str) + 1;
    char *dst = (char*)arena_alloc(arena, len);
    if (dst) memcpy(dst, str, len);
    return dst;
}