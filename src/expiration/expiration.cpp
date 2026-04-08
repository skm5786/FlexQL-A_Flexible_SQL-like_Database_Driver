/**
 * expiration.cpp  —  Background Row Expiry Thread  (Lesson 9)
 */
#include <cstdlib>
#include <ctime>
#include <cstdio>
#include <pthread.h>
#include <unistd.h>
#include "expiration/expiration.h"
#include "storage/storage.h"
#include "index/index.h"

static DatabaseManager  *g_expiry_mgr  = nullptr;
static volatile int      g_expiry_stop = 0;
static pthread_t         g_expiry_tid;

/* Walk one table and physically remove expired rows */
static void purge_table(Table *table) {
    time_t now = time(nullptr);

    pthread_rwlock_wrlock(&table->lock);   /* exclusive — no readers during purge */

    Row  *prev = nullptr;
    Row  *r    = table->head;
    uint64_t removed = 0;

    while (r) {
        Row *next = r->next;

        if (r->expiry > 0 && r->expiry < now) {
            /* Unlink from list */
            if (prev) prev->next   = next;
            else      table->head  = next;
            if (!next) table->tail = prev;   /* was the tail */

            /* Remove from hash index */
            if (table->pk_index && table->pk_col >= 0 &&
                !r->cells[table->pk_col].is_null) {
                index_remove(table->pk_index, &r->cells[table->pk_col]);
            }

            row_free_contents(r);   /* no-op in L11 — arena owns memory */
            free(r);                /* Row struct is arena-owned — skip free */
            /* Note: With the arena, we cannot individually free rows.
             * Expired rows are unlinked and invisible to scans, but their
             * memory is reclaimed when the table is dropped (arena_free).
             * The row pointer is simply abandoned here.                   */
            removed++;
        } else {
            prev = r;
        }
        r = next;
    }

    if (removed > 0 && table->row_count >= removed)
        table->row_count -= removed;

    pthread_rwlock_unlock(&table->lock);

    if (removed > 0)
        printf("[expiry] Purged %llu expired rows from table '%s'\n",
               (unsigned long long)removed, table->name);
}

static void *expiry_thread(void *arg) {
    DatabaseManager *mgr = (DatabaseManager*)arg;
    printf("[expiry] Background expiry thread started (interval=%ds)\n",
           EXPIRY_INTERVAL_SECONDS);

    while (!g_expiry_stop) {
        /* Sleep in 1-second chunks so we can react to stop flag quickly */
        for (int i = 0; i < EXPIRY_INTERVAL_SECONDS && !g_expiry_stop; i++)
            sleep(1);
        if (g_expiry_stop) break;

        /* Walk every database and every table */
        pthread_mutex_lock(&mgr->lock);
        int db_count = mgr->db_count;
        Database *dbs[64]; /* snapshot pointers */
        for (int i = 0; i < db_count && i < 64; i++) dbs[i] = mgr->databases[i];
        pthread_mutex_unlock(&mgr->lock);

        for (int d = 0; d < db_count; d++) {
            Database *db = dbs[d];
            pthread_mutex_lock(&db->schema_lock);
            int tc = db->table_count;
            Table *tables[256];
            for (int i = 0; i < tc && i < 256; i++) tables[i] = db->tables[i];
            pthread_mutex_unlock(&db->schema_lock);

            for (int i = 0; i < tc; i++)
                purge_table(tables[i]);
        }
    }

    printf("[expiry] Background expiry thread stopped.\n");
    return nullptr;
}

void expiry_start(DatabaseManager *mgr) {
    g_expiry_mgr  = mgr;
    g_expiry_stop = 0;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&g_expiry_tid, &attr, expiry_thread, mgr);
    pthread_attr_destroy(&attr);
}

void expiry_stop(void) {
    g_expiry_stop = 1;
    pthread_join(g_expiry_tid, nullptr);
}