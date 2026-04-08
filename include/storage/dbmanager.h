/**
 * dbmanager.h  —  Database Manager
 *
 * LESSON: The DatabaseManager is the root object of the FlexQL engine.
 * It answers three questions:
 *   1. Does database "X" exist?            → dbmgr_find()
 *   2. Create a new database called "X".   → dbmgr_create()
 *   3. Delete database "X" and all its data. → dbmgr_drop()
 *
 * CONCURRENCY DESIGN:
 *   The manager has ONE global mutex (DatabaseManager.lock).
 *   All CREATE/DROP/USE/SHOW operations lock it briefly.
 *   Once a client has a Database* from dbmgr_find(), they use the
 *   per-database schema_lock for table-level operations.
 *   This is a two-level locking hierarchy:
 *     Level 1: DatabaseManager.lock   — for the db registry
 *     Level 2: Database.schema_lock   — for tables within a db
 */

#ifndef FLEXQL_DBMANAGER_H
#define FLEXQL_DBMANAGER_H

#include "common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Initialise the global database manager. Call once at server startup. */
void dbmgr_init(DatabaseManager *mgr);

/** Free all databases and their tables. Call at server shutdown. */
void dbmgr_destroy(DatabaseManager *mgr);

/**
 * dbmgr_create — create a new named database.
 * Returns 0 on success, -1 if the name already exists or limit reached.
 * Thread-safe.
 */
int dbmgr_create(DatabaseManager *mgr, const char *name, char **errmsg);

/**
 * dbmgr_find — find a database by name (case-insensitive).
 * Returns a pointer to the Database, or NULL if not found.
 * Thread-safe.
 */
Database *dbmgr_find(DatabaseManager *mgr, const char *name);

/**
 * dbmgr_drop — delete a database and free all its memory.
 * Returns 0 on success, -1 if not found.
 * Thread-safe.
 */
int dbmgr_drop(DatabaseManager *mgr, const char *name, char **errmsg);

/**
 * dbmgr_list — write all database names into the result buffer.
 * names_out must be a pre-allocated array of FLEXQL_MAX_DATABASES strings.
 * Returns the number of databases written.
 */
int dbmgr_list(DatabaseManager *mgr,
               char names_out[][FLEXQL_MAX_NAME_LEN]);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_DBMANAGER_H */