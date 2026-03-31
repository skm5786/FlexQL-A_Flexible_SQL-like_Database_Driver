/**
 * wal.h  —  Write-Ahead Log (WAL) for FlexQL
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 8 — PERSISTENCE: WHY A WRITE-AHEAD LOG?
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * PROBLEM: Until Lesson 8, FlexQL stores all data in RAM.  If the server
 * crashes or restarts, everything is lost.  The TA requirement is:
 *   "disk must be primary storage — RAM is a cache"
 *
 * SOLUTION: Write-Ahead Log (WAL)
 *   Before confirming any INSERT or schema change to the client, write the
 *   change to a disk file and call fdatasync().  This guarantees the data
 *   is on durable storage before the client receives MSG_OK.
 *
 *   On the next server startup, replay the WAL to reconstruct RAM state.
 *   The in-memory data structures (linked list, hash index) are rebuilt from
 *   the WAL — they are DERIVED from disk, not the primary source.
 *
 * WHY WAL SPECIFICALLY (not just write rows to a flat file)?
 *   A WAL is append-only — each INSERT just appends to the end of the file.
 *   Appending is O(1) and much faster than random writes into a heap file.
 *   The WAL is the same approach used by:
 *     - PostgreSQL (pg_wal directory)
 *     - SQLite (WAL mode, -wal file)
 *     - MySQL InnoDB (redo log)
 *     - LevelDB / RocksDB (MANIFEST + SST files)
 *
 * FILE LAYOUT:
 *   data/
 *   ├── _registry            ← one line per named database
 *   └── DBNAME/
 *       └── TABLENAME.wal    ← one WAL file per table
 *
 * WAL ENTRY FORMAT (binary, big-endian):
 *   [8 bytes]  LSN  — Log Sequence Number, monotonically increasing
 *   [4 bytes]  type — WAL entry type (see WalEntryType enum)
 *   [4 bytes]  payload_len — byte length of the payload that follows
 *   [N bytes]  payload — entry-specific data (see below)
 *   [4 bytes]  CRC32 — checksum of (LSN + type + payload_len + payload)
 *              Used to detect truncated or corrupted entries on recovery.
 *
 * ENTRY TYPES AND PAYLOADS:
 *
 *   WAL_CREATE_TABLE:
 *     [4] col_count
 *     [4] pk_col  (-1 if none)
 *     for each column (col_count times):
 *       [64] name (null-terminated, zero-padded)
 *       [4]  ColumnType enum value
 *       [4]  constraints bitmask
 *
 *   WAL_INSERT:
 *     [8]  row_id
 *     [8]  expiry (time_t as int64)
 *     [4]  col_count
 *     for each cell:
 *       [1]  is_null flag
 *       if not null:
 *         INT/DATETIME: [8] int64 value
 *         DECIMAL:      [8] double value (IEEE 754)
 *         VARCHAR:      [4] string_len + [string_len] bytes (no null terminator)
 *
 *   WAL_DROP_TABLE:
 *     (empty payload)
 *
 * RECOVERY:
 *   On startup, for each non-session database:
 *     1. Recreate the Database struct
 *     2. For each TABLENAME.wal in that database's directory:
 *        a. Read WAL_CREATE_TABLE → rebuild Table schema
 *        b. Read WAL_INSERT entries → call row_insert() for each
 *        c. Skip entries whose CRC32 is wrong (truncated last write)
 *     3. Hash indexes are rebuilt automatically by row_insert()
 *
 * SESSION DATABASES:
 *   Names starting with "_SESSION_" are ephemeral (created per-connection).
 *   They are never written to the WAL.  This is checked by wal_is_persistent().
 *
 * GROUP COMMIT:
 *   A batch INSERT (VALUES (r1),(r2),...,(rN)) writes ALL N rows to the WAL
 *   in a single contiguous write, then calls fdatasync() ONCE.
 *   This amortises the disk sync cost: N rows = 1 fdatasync instead of N.
 *   With batch=1000: 1000 syncs for 1M rows instead of 1M syncs.
 */

#ifndef FLEXQL_WAL_H
#define FLEXQL_WAL_H

#include <stdint.h>
#include <stddef.h>
#include "common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ── WAL entry types ─────────────────────────────────────────────────────── */
typedef enum {
    WAL_CREATE_TABLE = 1,
    WAL_INSERT       = 2,
    WAL_DROP_TABLE   = 3,
} WalEntryType;

/* Root data directory (relative to working directory of server process)    */
#define WAL_DATA_DIR   "data"
#define WAL_REGISTRY   "data/_registry"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  PUBLIC API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * wal_is_persistent — returns 1 if this database name should be persisted.
 * Session databases (_SESSION_*) are ephemeral; all others are persistent.
 */
int wal_is_persistent(const char *db_name);

/**
 * wal_ensure_dirs — create data/DBNAME/ directory if it does not exist.
 * Called before any write to a persistent database.
 */
int wal_ensure_dirs(const char *db_name);

/**
 * wal_register_db — append db_name to the registry file.
 * Called when a named database is created (CREATE DATABASE).
 */
int wal_register_db(const char *db_name);

/**
 * wal_unregister_db — remove db_name from the registry file.
 * Called when a database is dropped (DROP DATABASE).
 */
int wal_unregister_db(const char *db_name);

/**
 * wal_write_create_table — append a WAL_CREATE_TABLE entry.
 *
 * @param db_name    Database name (used to locate the WAL file).
 * @param table      The just-created table (schema + pk_col).
 * @return 0 on success, -1 on I/O error.
 *
 * Called from executor after table_create() succeeds.
 * The WAL file is created if it does not exist.
 * fdatasync() is called to ensure the entry is durable.
 */
int wal_write_create_table(const char *db_name, const Table *table);

/**
 * wal_write_insert_batch — append WAL_INSERT entries for a batch of rows.
 *
 * @param db_name    Database name.
 * @param table_name Table name (used to locate the WAL file).
 * @param table      Table schema (for column type lookup during serialisation).
 * @param str_values Array of string value arrays, one per row.
 *                   str_values[r][c] = string value for row r, column c.
 * @param row_count  Number of rows in the batch.
 * @param value_count Number of columns per row.
 * @param expiry     TTL timestamp (0 = never expires), same for all rows.
 * @return 0 on success, -1 on I/O error.
 *
 * KEY PERFORMANCE FEATURE: GROUP COMMIT
 *   All rows are serialised into a single buffer, written with one write()
 *   call, and flushed with ONE fdatasync().
 *   This gives O(1) disk syncs per batch regardless of batch size,
 *   compared to O(N) syncs if we fsynced per row.
 */
int wal_write_insert_batch(const char *db_name, const char *table_name,
                           const Table *table,
                           const char *const *str_values_flat,
                           int row_count, int value_count, time_t expiry);

/**
 * wal_write_drop_table — append a WAL_DROP_TABLE entry.
 * Called when DROP TABLE is executed on a persistent database.
 */
int wal_write_drop_table(const char *db_name, const char *table_name);

/**
 * wal_recover — replay all WAL files to reconstruct the in-memory state.
 *
 * @param mgr    The DatabaseManager to populate.
 * @return Number of databases recovered, or -1 on fatal error.
 *
 * Called once at server startup, before accepting any client connections.
 *
 * Recovery sequence:
 *   1. Open WAL_REGISTRY, read each database name.
 *   2. Call dbmgr_create() for each database.
 *   3. Scan data/DBNAME/ for *.wal files.
 *   4. For each WAL file:
 *      a. Read entries in LSN order.
 *      b. WAL_CREATE_TABLE → call table_create() with saved schema.
 *      c. WAL_INSERT       → call row_insert() with saved cell values.
 *      d. WAL_DROP_TABLE   → remove the table from the database.
 *      e. Bad CRC32        → stop replaying this file (truncated entry).
 *   5. Hash indexes are automatically rebuilt by row_insert().
 */
int wal_recover(DatabaseManager *mgr);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_WAL_H */