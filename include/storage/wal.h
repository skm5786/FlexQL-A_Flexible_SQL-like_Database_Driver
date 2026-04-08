/**
 * wal.h  —  Write-Ahead Log API
 *
 * ADDED: wal_flush_all() — call on server shutdown to flush any buffered
 * WAL records to disk before exiting cleanly.
 */

#ifndef FLEXQL_WAL_H
#define FLEXQL_WAL_H

#include <stdint.h>
#include <stddef.h>
#include "common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    WAL_CREATE_TABLE = 1,
    WAL_INSERT       = 2,
    WAL_DROP_TABLE   = 3,
} WalEntryType;

#define WAL_DATA_DIR   "data"
#define WAL_REGISTRY   "data/_registry"

int wal_is_persistent(const char *db_name);
int wal_ensure_dirs(const char *db_name);
int wal_register_db(const char *db_name);
int wal_unregister_db(const char *db_name);
int wal_write_create_table(const char *db_name, const Table *table);
int wal_write_insert_batch(const char *db_name, const char *table_name,
                           const Table *table,
                           const char *const *str_values_flat,
                           int row_count, int value_count, time_t expiry);
int wal_write_drop_table(const char *db_name, const char *table_name);
int wal_recover(DatabaseManager *mgr);

/* NEW: flush all buffered WAL records to disk — call on clean server exit */
void wal_flush_all(void);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_WAL_H */