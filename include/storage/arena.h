/**
 * arena.h  —  Row Arena Allocator  (Lesson 11)
 *
 * LESSON 11 — WHY AN ARENA?
 *
 * Every row_insert() currently calls:
 *   calloc(1, sizeof(Row))                  ← 2 syscalls minimum per row
 *   calloc(col_count, sizeof(CellValue))    ←
 *   strdup(varchar_val)  × N varchars       ← N more
 *
 * At 1M rows: 2M+ malloc() calls.  Each call takes ~50–200ns plus
 * the allocator lock.  Total: 100ms–400ms of pure allocation overhead.
 *
 * An arena pre-allocates large slabs and serves requests by bumping
 * a pointer.  Allocation is a single pointer increment — no lock, no
 * free-list search, no system call (most of the time).
 *
 * Memory is freed all at once when the table is dropped (arena_free).
 * This eliminates the per-row free() calls that table_free() previously
 * did in a loop.
 *
 * WHAT USES THE ARENA:
 *   Row structs         → arena_alloc(arena, sizeof(Row))
 *   CellValue arrays    → arena_alloc(arena, N * sizeof(CellValue))
 *   VARCHAR strings     → arena_alloc_str(arena, str)  [null-terminated copy]
 *
 * WHAT DOES NOT USE THE ARENA:
 *   Table structs, ColumnDef arrays, index nodes — these have different
 *   lifetimes and are still individually malloc'd/free'd.
 *
 * SLAB SIZE — WHY 16MB?
 *   With 10M rows at ~200 bytes each (Row + CellValues + strings) = ~2GB.
 *   At 1MB slabs: ~2000 slab allocations, each requiring a malloc() call
 *   and pointer list update.
 *   At 16MB slabs: only ~125 allocations.  Fewer syscalls AND better
 *   cache locality — rows within the same slab are physically contiguous,
 *   so sequential scans hit the L2/L3 cache more often.
 */
#ifndef FLEXQL_ARENA_H
#define FLEXQL_ARENA_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* PERF: 16MB slabs → ~125 allocations for 10M rows vs ~2000 at 1MB.
 * Also improves table_scan() cache locality for sequential row access. */
#define ARENA_SLAB_SIZE  (16 * 1024 * 1024)  /* 16 MB per slab */

typedef struct ArenaSlab {
    char             *base;
    size_t            used;
    size_t            cap;
    struct ArenaSlab *next;
} ArenaSlab;

struct Arena {
    ArenaSlab *head;        /* current (newest) slab          */
    size_t     total_bytes; /* total bytes allocated so far   */
    size_t     total_slabs;
};

/** Allocate and initialise a new Arena. Returns NULL on OOM. */
Arena *arena_create(void);

/** Free all memory owned by the arena. Safe to call with NULL. */
void arena_free(Arena *arena);

/**
 * arena_alloc — allocate `size` bytes from the arena.
 * Returns aligned pointer, or NULL on OOM.
 * The returned memory is zeroed.
 */
void *arena_alloc(Arena *arena, size_t size);

/**
 * arena_alloc_str — duplicate a string into the arena.
 * Returns pointer to null-terminated copy, or NULL on OOM.
 */
char *arena_alloc_str(Arena *arena, const char *str);

#ifdef __cplusplus
}
#endif
#endif