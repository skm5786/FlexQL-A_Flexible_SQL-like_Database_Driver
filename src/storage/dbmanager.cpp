/**
 * dbmanager.cpp  —  Database Manager Implementation
 *
 * LESSON: This file shows how to manage a dynamic collection of objects
 * safely across multiple threads using:
 *   1. A fixed-size array of pointers (simple, cache-friendly, bounded)
 *   2. A single mutex protecting all structural changes
 *   3. String comparison with strcasecmp for case-insensitive names
 *
 * WHY fixed array over a linked list or hash map?
 *   We cap databases at 64 (FLEXQL_MAX_DATABASES).  A fixed array gives us:
 *     - O(n) linear search — fine for n=64
 *     - No heap allocation for the array itself
 *     - Simple to iterate for SHOW DATABASES
 *   A hash map would be O(1) lookup but adds complexity we don't need here.
 */

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <strings.h>   /* strcasecmp — POSIX, available on Linux & macOS   */
#include "storage/dbmanager.h"
#include "storage/storage.h"
#include "cache/cache.h"

/* ── internal: allocate and initialise one Database ────────────────────── */
static Database *alloc_database(const char *name) {
    Database *db = static_cast<Database *>(calloc(1, sizeof(Database)));
    if (!db) return nullptr;
    /* Store the name uppercased for consistent comparison */
    strncpy(db->name, name, FLEXQL_MAX_NAME_LEN - 1);
    for (char *p = db->name; *p; p++)
        *p = static_cast<char>(toupper(static_cast<unsigned char>(*p)));
    pthread_mutex_init(&db->schema_lock, nullptr);
    db->table_count = 0;
    return db;
}

/* ── internal: free one Database and all its tables ──────────────────────── */
static void free_database(Database *db) {
    if (!db) return;
    /* Free each table and all its rows using the storage layer             */
    for (int i = 0; i < db->table_count; i++) {
        table_free(db->tables[i]);
        db->tables[i] = nullptr;
    }
    pthread_mutex_destroy(&db->schema_lock);
    free(db);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  PUBLIC API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

void dbmgr_init(DatabaseManager *mgr) {
    memset(mgr, 0, sizeof(DatabaseManager));
    pthread_mutex_init(&mgr->lock, nullptr);
    /* Lesson 5: allocate the shared query result cache */
    mgr->query_cache = cache_create();
}

void dbmgr_destroy(DatabaseManager *mgr) {
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->db_count; i++) {
        free_database(mgr->databases[i]);
        mgr->databases[i] = nullptr;
    }
    mgr->db_count = 0;
    pthread_mutex_unlock(&mgr->lock);
    /* Lesson 5: free the query cache */
    cache_free(mgr->query_cache);
    mgr->query_cache = nullptr;
    pthread_mutex_destroy(&mgr->lock);
}

int dbmgr_create(DatabaseManager *mgr, const char *name, char **errmsg) {
    if (!name || name[0] == '\0') {
        if (errmsg) *errmsg = strdup("Database name cannot be empty");
        return -1;
    }

    pthread_mutex_lock(&mgr->lock);

    /* Check for duplicate */
    for (int i = 0; i < mgr->db_count; i++) {
        if (strcasecmp(mgr->databases[i]->name, name) == 0) {
            pthread_mutex_unlock(&mgr->lock);
            char buf[128];
            snprintf(buf, sizeof(buf),
                     "Database '%s' already exists", name);
            if (errmsg) *errmsg = strdup(buf);
            return -1;
        }
    }

    /* Check capacity */
    if (mgr->db_count >= FLEXQL_MAX_DATABASES) {
        pthread_mutex_unlock(&mgr->lock);
        if (errmsg) *errmsg = strdup("Maximum number of databases reached");
        return -1;
    }

    Database *db = alloc_database(name);
    if (!db) {
        pthread_mutex_unlock(&mgr->lock);
        if (errmsg) *errmsg = strdup("Out of memory creating database");
        return -1;
    }

    mgr->databases[mgr->db_count++] = db;
    pthread_mutex_unlock(&mgr->lock);
    return 0;
}

Database *dbmgr_find(DatabaseManager *mgr, const char *name) {
    if (!name) return nullptr;
    pthread_mutex_lock(&mgr->lock);
    Database *found = nullptr;
    for (int i = 0; i < mgr->db_count; i++) {
        if (strcasecmp(mgr->databases[i]->name, name) == 0) {
            found = mgr->databases[i];
            break;
        }
    }
    pthread_mutex_unlock(&mgr->lock);
    return found;
}

int dbmgr_drop(DatabaseManager *mgr, const char *name, char **errmsg) {
    if (!name) {
        if (errmsg) *errmsg = strdup("Database name is NULL");
        return -1;
    }

    pthread_mutex_lock(&mgr->lock);

    int idx = -1;
    for (int i = 0; i < mgr->db_count; i++) {
        if (strcasecmp(mgr->databases[i]->name, name) == 0) {
            idx = i; break;
        }
    }

    if (idx < 0) {
        pthread_mutex_unlock(&mgr->lock);
        char buf[128];
        snprintf(buf, sizeof(buf), "Database '%s' does not exist", name);
        if (errmsg) *errmsg = strdup(buf);
        return -1;
    }

    /* Free the database */
    free_database(mgr->databases[idx]);

    /* Compact the array — shift remaining entries left */
    for (int i = idx; i < mgr->db_count - 1; i++)
        mgr->databases[i] = mgr->databases[i + 1];
    mgr->databases[--mgr->db_count] = nullptr;

    pthread_mutex_unlock(&mgr->lock);
    return 0;
}

int dbmgr_list(DatabaseManager *mgr,
               char names_out[][FLEXQL_MAX_NAME_LEN]) {
    pthread_mutex_lock(&mgr->lock);
    int n = mgr->db_count;
    for (int i = 0; i < n; i++)
        strncpy(names_out[i], mgr->databases[i]->name, FLEXQL_MAX_NAME_LEN-1);
    pthread_mutex_unlock(&mgr->lock);
    return n;
}