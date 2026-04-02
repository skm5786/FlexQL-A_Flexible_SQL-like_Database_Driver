/**
 * storage.cpp  —  Table & Row Storage Engine  (Lessons 10+11 integrated)
 *
 * LESSON 10 — B+ TREE RANGE INDEX:
 *   table_create()  allocates a BTree for every INT/DECIMAL column.
 *   row_insert()    calls btree_insert() for every non-null cell value.
 *   table_scan()    uses btree_range_scan() when the WHERE uses >, <, >=, <=
 *                   on a column that has a B+ tree.
 *   table_free()    calls btree_free() for each column tree.
 *
 * LESSON 11 — ARENA ALLOCATOR:
 *   table_create()  allocates one Arena per table.
 *   row_insert()    uses arena_alloc() for Row struct and CellValue array.
 *                   VARCHAR strings also use arena_alloc_str().
 *   row_free_contents() is now a no-op — arena owns all row memory.
 *   table_free()    calls arena_free() once to release all row memory.
 *                   No more per-row free() loop.
 *
 *   WHY this is faster:
 *     Old: 2M+ malloc() + free() calls for 1M rows (slow, lock contention)
 *     New: N slab allocations (N << M) + 1 arena_free() at drop time
 */
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <cerrno>
#include <ctime>
#include <cstdarg>
#include <cmath>
#include <strings.h>
#include "storage/storage.h"
#include "storage/arena.h"
#include "index/index.h"
#include "index/btree.h"

static void set_err(char **e, const char *msg) {
    if (!e) return; free(*e); *e = strdup(msg);
}
static void set_err_fmt(char **e, const char *fmt, ...) {
    if (!e) return;
    char buf[512]; va_list ap;
    va_start(ap, fmt); vsnprintf(buf, sizeof(buf), fmt, ap); va_end(ap);
    free(*e); *e = strdup(buf);
}

/* ── string_to_cell (unchanged) ──────────────────────────────────────────── */
int string_to_cell(const char *str, ColumnType type,
                   CellValue *out, char **errmsg) {
    memset(out, 0, sizeof(CellValue));
    out->type = type;
    if (!str || str[0] == '\0') { out->is_null = 1; return 0; }
    out->is_null = 0;
    switch (type) {
    case COL_TYPE_INT: {
        char *end = nullptr; errno = 0;
        long long v = strtoll(str, &end, 10);
        if (end == str || *end != '\0' || errno == ERANGE) {
            set_err_fmt(errmsg, "Cannot convert '%s' to INT", str); return -1;
        }
        out->data.int_val = (int64_t)v; return 0;
    }
    case COL_TYPE_DECIMAL: {
        char *end = nullptr; errno = 0;
        double v = strtod(str, &end);
        if (end == str || *end != '\0' || errno == ERANGE) {
            set_err_fmt(errmsg, "Cannot convert '%s' to DECIMAL", str); return -1;
        }
        out->data.decimal_val = v; return 0;
    }
    case COL_TYPE_VARCHAR: {
        out->data.varchar_val = strdup(str);
        if (!out->data.varchar_val) { set_err(errmsg,"Out of memory"); return -1; }
        return 0;
    }
    case COL_TYPE_DATETIME: {
        char *end = nullptr; errno = 0;
        long long ts = strtoll(str, &end, 10);
        if (end != str && *end == '\0' && errno != ERANGE) {
            out->data.datetime_val = (time_t)ts; return 0;
        }
        struct tm tm_val{}; char *parsed = nullptr;
        parsed = strptime(str, "%Y-%m-%d %H:%M:%S", &tm_val);
        if (!parsed) parsed = strptime(str, "%Y-%m-%d", &tm_val);
        if (!parsed) {
            set_err_fmt(errmsg,"Cannot convert '%s' to DATETIME", str); return -1;
        }
        tm_val.tm_isdst = -1;
        out->data.datetime_val = mktime(&tm_val); return 0;
    }
    default: set_err(errmsg,"Unknown column type"); return -1;
    }
}

/* ── cell_to_string (unchanged) ──────────────────────────────────────────── */
char *cell_to_string(const CellValue *cell, char *buf, size_t bufsz) {
    if (!cell || cell->is_null) {
        strncpy(buf,"NULL",bufsz-1); buf[bufsz-1]='\0'; return buf;
    }
    switch (cell->type) {
    case COL_TYPE_INT:
        snprintf(buf,bufsz,"%lld",(long long)cell->data.int_val); break;
    case COL_TYPE_DECIMAL: {
        double d = cell->data.decimal_val;
        if (d == floor(d) && d >= -1e15 && d <= 1e15)
            snprintf(buf, bufsz, "%lld", (long long)d);
        else
            snprintf(buf, bufsz, "%g", d);
        break;
    }
    case COL_TYPE_VARCHAR:
        strncpy(buf,cell->data.varchar_val?cell->data.varchar_val:"NULL",bufsz-1);
        buf[bufsz-1]='\0'; break;
    case COL_TYPE_DATETIME: {
        struct tm *t = localtime(&cell->data.datetime_val);
        if (t) strftime(buf,bufsz,"%Y-%m-%d %H:%M:%S",t);
        else   strncpy(buf,"0000-00-00 00:00:00",bufsz-1); break;
    }
    default: strncpy(buf,"?",bufsz-1);
    }
    buf[bufsz-1]='\0'; return buf;
}

/* ── cell_matches_where (unchanged) ──────────────────────────────────────── */
static int cmp_to_bool(int cmp, CompareOp op) {
    switch(op){
    case OP_EQ: return cmp==0; case OP_NEQ: return cmp!=0;
    case OP_LT: return cmp<0;  case OP_GT:  return cmp>0;
    case OP_LTE:return cmp<=0; case OP_GTE: return cmp>=0;
    default: return 0;
    }
}
int cell_matches_where(const CellValue *cell, const WhereClause *where) {
    if (!where||!where->has_condition) return 1;
    if (cell->is_null) return 0;
    const char *wv = where->value;
    switch (cell->type) {
    case COL_TYPE_INT: {
        int64_t lhs=cell->data.int_val; long long rhs=strtoll(wv,nullptr,10);
        return cmp_to_bool((lhs<(int64_t)rhs)?-1:(lhs>(int64_t)rhs)?1:0, where->op);
    }
    case COL_TYPE_DECIMAL: {
        double lhs=cell->data.decimal_val, rhs=strtod(wv,nullptr);
        return cmp_to_bool((lhs<rhs)?-1:(lhs>rhs)?1:0, where->op);
    }
    case COL_TYPE_VARCHAR: {
        const char *lhs=cell->data.varchar_val?cell->data.varchar_val:"";
        return cmp_to_bool(strcmp(lhs,wv), where->op);
    }
    case COL_TYPE_DATETIME: {
        time_t lhs=cell->data.datetime_val, rhs=(time_t)strtoll(wv,nullptr,10);
        return cmp_to_bool((lhs<rhs)?-1:(lhs>rhs)?1:0, where->op);
    }
    default: return 0;
    }
}

/* ── LESSON 11: row_free_contents is now a no-op ────────────────────────────
 * Arena owns all Row + CellValue memory.
 * VARCHAR strings are also arena-allocated — no individual free needed.
 * The function is kept for API compatibility (expiration.cpp calls it).     */
void row_free_contents(Row *row) {
    (void)row;
    /* All row memory is owned by table->row_arena.
       arena_free() in table_free() releases everything at once.             */
}

/* ── table_free — LESSON 11: drops entire arena ─────────────────────────── */
void table_free(Table *table) {
    if (!table) return;
    /* LESSON 11: free all row memory in one call */
    arena_free(table->row_arena);
    table->row_arena = nullptr;
    table->head = nullptr;
    table->tail = nullptr;

    /* LESSON 10: free all column B+ trees */
    for (int i = 0; i < FLEXQL_MAX_COLUMNS; i++) {
        btree_free(table->col_btree[i]);
        table->col_btree[i] = nullptr;
    }

    index_free(table->pk_index);
    table->pk_index = nullptr;
    pthread_rwlock_destroy(&table->lock);
    free(table);
}

/* ── table_find (unchanged) ──────────────────────────────────────────────── */
Table *table_find(Database *db, const char *name) {
    if (!db||!name) return nullptr;
    pthread_mutex_lock(&db->schema_lock);
    Table *found=nullptr;
    for (int i=0;i<db->table_count&&!found;i++)
        if (strcasecmp(db->tables[i]->name,name)==0) found=db->tables[i];
    pthread_mutex_unlock(&db->schema_lock);
    return found;
}

/* ── table_create — LESSONS 10+11: allocate btrees + arena ─────────────── */
Table *table_create(Database *db, const char *name,
                    ColumnDef *cols, int col_count, char **errmsg) {
    if (!db||!name||!cols||col_count<=0) {
        set_err(errmsg,"Invalid arguments to table_create"); return nullptr;
    }
    pthread_mutex_lock(&db->schema_lock);

    for (int i=0;i<db->table_count;i++) {
        if (strcasecmp(db->tables[i]->name,name)==0) {
            pthread_mutex_unlock(&db->schema_lock);
            set_err_fmt(errmsg,"Table '%s' already exists",name); return nullptr;
        }
    }
    if (db->table_count>=FLEXQL_MAX_TABLES) {
        pthread_mutex_unlock(&db->schema_lock);
        set_err(errmsg,"Maximum number of tables reached"); return nullptr;
    }

    Table *t=(Table*)calloc(1,sizeof(Table));
    if (!t) { pthread_mutex_unlock(&db->schema_lock); set_err(errmsg,"OOM"); return nullptr; }

    strncpy(t->name,name,FLEXQL_MAX_NAME_LEN-1);
    for (char *p=t->name;*p;p++) *p=(char)toupper((unsigned char)*p);

    t->col_count=col_count; t->pk_col=-1;
    for (int i=0;i<col_count;i++) {
        t->schema[i]=cols[i];
        for (char *p=t->schema[i].name;*p;p++) *p=(char)toupper((unsigned char)*p);
        if (cols[i].constraints&COL_CONSTRAINT_PRIMARY_KEY) t->pk_col=i;
    }

    t->head=nullptr; t->tail=nullptr; t->row_count=0; t->next_row_id=1;
    pthread_rwlock_init(&t->lock,nullptr);

    /* LESSON 11: create arena for row memory */
    t->row_arena = arena_create();
    if (!t->row_arena) {
        pthread_mutex_unlock(&db->schema_lock);
        pthread_rwlock_destroy(&t->lock); free(t);
        set_err(errmsg,"OOM creating row arena"); return nullptr;
    }

    /* LESSON 4: hash index for primary key */
    t->pk_index=nullptr;
    if (t->pk_col>=0) {
        t->pk_index=index_create(t->schema[t->pk_col].type);
        if (!t->pk_index) {
            pthread_mutex_unlock(&db->schema_lock);
            arena_free(t->row_arena); pthread_rwlock_destroy(&t->lock); free(t);
            set_err(errmsg,"OOM creating pk index"); return nullptr;
        }
    }

    /* LESSON 10: B+ tree for every INT/DECIMAL column */
    for (int i=0;i<col_count;i++) {
        ColumnType ct = t->schema[i].type;
        if (ct==COL_TYPE_INT || ct==COL_TYPE_DECIMAL || ct==COL_TYPE_DATETIME) {
            t->col_btree[i] = btree_create(ct);
            /* OOM on btree is non-fatal — fall back to full scan */
        }
    }

    db->tables[db->table_count++]=t;
    pthread_mutex_unlock(&db->schema_lock);
    return t;
}

/* ── row_insert — LESSONS 10+11: arena alloc + btree insert ─────────────── */
int row_insert(Table *table,
               const char str_values[][FLEXQL_MAX_VARCHAR],
               int val_count, time_t expiry, char **errmsg) {

    if (val_count!=table->col_count) {
        set_err_fmt(errmsg,"Table '%s' has %d columns but %d values given",
                    table->name,table->col_count,val_count);
        return -1;
    }

    /* LESSON 11: Allocate cells from arena (no individual free needed) */
    CellValue *cells = (CellValue*)arena_alloc(table->row_arena,
                                               table->col_count * sizeof(CellValue));
    if (!cells) { set_err(errmsg,"OOM: arena full"); return -1; }

    for (int i=0;i<table->col_count;i++) {
        const ColumnDef *col=&table->schema[i];
        const char      *sv =str_values[i];
        if ((col->constraints&COL_CONSTRAINT_NOT_NULL)&&sv[0]=='\0') {
            set_err_fmt(errmsg,"Column '%s' cannot be NULL",col->name);
            /* Arena: no cleanup needed — arena will reclaim at table drop  */
            return -1;
        }
        /* Use a temporary CellValue to convert, then fix up VARCHAR */
        CellValue tmp{}; char *cerr=nullptr;
        if (string_to_cell(sv,col->type,&tmp,&cerr)!=0) {
            if (errmsg) { free(*errmsg); *errmsg=cerr; } else free(cerr);
            return -1;
        }
        cells[i] = tmp;
        /* LESSON 11: For VARCHAR, re-allocate the string from arena */
        if (col->type==COL_TYPE_VARCHAR && tmp.data.varchar_val) {
            cells[i].data.varchar_val =
                arena_alloc_str(table->row_arena, tmp.data.varchar_val);
            free(tmp.data.varchar_val); /* free the strdup from string_to_cell */
        }
    }

    /* PK duplicate check under write lock */
    if (table->pk_col>=0 && !cells[table->pk_col].is_null) {
        const CellValue *npk = &cells[table->pk_col];
        pthread_rwlock_wrlock(&table->lock);
        if (table->pk_index) {
            Row *existing=index_get(table->pk_index,npk);
            if (existing) {
                time_t now=time(nullptr);
                if (!(existing->expiry>0&&existing->expiry<now)) {
                    pthread_rwlock_unlock(&table->lock);
                    set_err_fmt(errmsg,"Duplicate PRIMARY KEY in '%s'",
                                table->schema[table->pk_col].name);
                    return -1;
                }
                index_remove(table->pk_index,npk);
            }
        } else {
            time_t now=time(nullptr);
            for (Row *r=table->head;r;r=r->next) {
                if (r->expiry>0&&r->expiry<now) continue;
                const CellValue *epk=&r->cells[table->pk_col];
                if (epk->is_null) continue;
                int dup=0;
                if (npk->type==COL_TYPE_INT)
                    dup=(npk->data.int_val==epk->data.int_val);
                else if (npk->type==COL_TYPE_VARCHAR&&npk->data.varchar_val&&epk->data.varchar_val)
                    dup=(strcmp(npk->data.varchar_val,epk->data.varchar_val)==0);
                if (dup) {
                    pthread_rwlock_unlock(&table->lock);
                    set_err_fmt(errmsg,"Duplicate PRIMARY KEY in '%s'",
                                table->schema[table->pk_col].name);
                    return -1;
                }
            }
        }
        pthread_rwlock_unlock(&table->lock);
    }

    /* LESSON 11: Allocate Row struct from arena */
    Row *row=(Row*)arena_alloc(table->row_arena, sizeof(Row));
    if (!row) { set_err(errmsg,"OOM: arena full"); return -1; }
    row->cells=cells; row->col_count=table->col_count; row->expiry=expiry; row->next=nullptr;

    pthread_rwlock_wrlock(&table->lock);
    row->row_id = table->next_row_id++;
    if (table->tail) table->tail->next=row; else table->head=row;
    table->tail=row;
    table->row_count++;

    if (table->pk_index && table->pk_col>=0 && !row->cells[table->pk_col].is_null)
        index_put(table->pk_index, &row->cells[table->pk_col], row);

    /* LESSON 10: Insert into B+ tree for every indexed column */
    for (int i=0;i<table->col_count;i++) {
        if (table->col_btree[i] && !row->cells[i].is_null)
            btree_insert(table->col_btree[i], &row->cells[i], row);
    }

    pthread_rwlock_unlock(&table->lock);
    return 0;
}

/* ── table_scan — LESSON 10: B+tree fast path for range queries ──────────── */
int table_scan(Table *table, const WhereClause *where,
               ScanCallback cb, void *arg) {
    if (!table||!cb) return 0;

    int wcol=-1;
    if (where&&where->has_condition) {
        for (int i=0;i<table->col_count;i++) {
            if (strcasecmp(table->schema[i].name,where->col_name)==0) {
                wcol=i; break;
            }
        }
        if (wcol<0) return 0;
    }

    /* LESSON 4: Hash index fast-path for WHERE pk = value */
    if (wcol>=0 && wcol==table->pk_col && where->op==OP_EQ && table->pk_index) {
        CellValue lk{}; char *derr=nullptr;
        if (string_to_cell(where->value,table->schema[table->pk_col].type,&lk,&derr)==0) {
            pthread_rwlock_rdlock(&table->lock);
            Row *found=index_get(table->pk_index,&lk);
            int visited=0;
            if (found) {
                time_t now=time(nullptr);
                if (!(found->expiry>0&&found->expiry<now)) { visited=1; cb(found,arg); }
            }
            pthread_rwlock_unlock(&table->lock);
            if (lk.type==COL_TYPE_VARCHAR&&lk.data.varchar_val) free(lk.data.varchar_val);
            free(derr);
            return visited;
        }
        free(derr);
    }

    /* LESSON 10: B+ tree fast-path for range queries (>, <, >=, <=)
     *
     * Condition: WHERE clause uses a range operator AND the column has a
     * B+ tree index.  We do NOT use the B+ tree for EQ (hash index is
     * faster) or for columns without a btree (VARCHAR, or alloc failure).
     *
     * For WHERE col > X:
     *   lo = X (exclusive), hi = NULL (unbounded)
     *   btree_range_scan walks leaves from the first key > X to the end.
     *
     * For WHERE col BETWEEN X AND Y  (future — parsed as two conditions):
     *   lo = X, hi = Y with appropriate operators.                        */
    if (wcol>=0 &&
        table->col_btree[wcol] != nullptr &&
        where->op != OP_EQ && where->op != OP_NEQ) {

        /* Build CellValue from the WHERE value string */
        CellValue bound{}; char *derr=nullptr;
        ColumnType ct = table->schema[wcol].type;
        if (string_to_cell(where->value, ct, &bound, &derr) == 0 && !bound.is_null) {
            free(derr);

            /* Determine lo/hi based on operator */
            const CellValue *lo = nullptr, *hi = nullptr;
            CompareOp lo_op = OP_GT, hi_op = OP_LT;

            pthread_rwlock_rdlock(&table->lock);
            int visited = 0;
            if (where->op == OP_GT || where->op == OP_GTE) {
                lo = &bound; lo_op = where->op;
                visited = btree_range_scan(table->col_btree[wcol],
                                           lo, lo_op, nullptr, OP_LT, cb, arg);
            } else { /* OP_LT, OP_LTE */
                hi = &bound; hi_op = where->op;
                visited = btree_range_scan(table->col_btree[wcol],
                                           nullptr, OP_GT, hi, hi_op, cb, arg);
            }
            pthread_rwlock_unlock(&table->lock);
            if (bound.type==COL_TYPE_VARCHAR&&bound.data.varchar_val)
                free(bound.data.varchar_val);
            return visited;
        }
        free(derr);
        if (bound.type==COL_TYPE_VARCHAR&&bound.data.varchar_val)
            free(bound.data.varchar_val);
    }

    /* Full scan (fallback) */
    time_t now=time(nullptr); int visited=0;
    pthread_rwlock_rdlock(&table->lock);
    for (Row *r=table->head;r;r=r->next) {
        if (r->expiry>0&&r->expiry<now) continue;
        if (wcol>=0&&!cell_matches_where(&r->cells[wcol],where)) continue;
        visited++;
        if (cb(r,arg)) break;
    }
    pthread_rwlock_unlock(&table->lock);
    return visited;
}