/**
 * storage.cpp  —  Table & Row Storage Engine  (Lesson 6 — Correctness fixes)
 *
 * CHANGES FROM LESSON 4/5:
 *
 *   FIX 1 — Row ordering (insertion order):
 *     Old: prepend new row at head → rows come out in REVERSE order
 *     New: append new row at tail  → rows come out in INSERTION order
 *     Required because benchmark asserts exact row order:
 *       {"1|Alice|...", "2|Bob|...", "3|Carol|...", "4|Dave|..."}
 *     Uses the new Table.tail pointer for O(1) append.
 *
 *   FIX 2 — DECIMAL rendering:
 *     Old: snprintf(buf, bufsz, "%g", d)
 *          → 1893456000.0 renders as "1.89346e+09" (WRONG)
 *     New: if d is a whole number within int64 range → render as integer
 *          → 1893456000.0 renders as "1893456000" (CORRECT)
 *     Benchmark expects "1|Alice|1200|1893456000", not "1|Alice|1200|1.89346e+09".
 *
 *   FIX 3 — table_create initialises tail = nullptr:
 *     Required companion to Fix 1.
 *
 *   FIX 4 — table_free handles tail pointer:
 *     No functional change needed (we walk head→tail via next pointers)
 *     but setting tail = nullptr during free is defensive.
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
#include "index/index.h"

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

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * FIX 2 — DECIMAL RENDERING
 *
 * Problem: snprintf "%g" on 1893456000.0 → "1.89346e+09"
 *          Benchmark expects "1893456000"
 *
 * Solution: check if the double is a whole number that fits in int64.
 *   If yes: render as integer with %lld (no decimal point, no exponent).
 *   If no:  fall back to %g for true fractional values like 3.14.
 *
 * Threshold 1e15: beyond this, int64_t cannot represent all doubles
 * exactly (doubles have 53 bits of mantissa = 15-16 significant digits).
 * Keeping within 1e15 ensures the integer cast is lossless.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
char *cell_to_string(const CellValue *cell, char *buf, size_t bufsz) {
    if (!cell || cell->is_null) {
        strncpy(buf,"NULL",bufsz-1); buf[bufsz-1]='\0'; return buf;
    }
    switch (cell->type) {
    case COL_TYPE_INT:
        snprintf(buf,bufsz,"%lld",(long long)cell->data.int_val); break;

    case COL_TYPE_DECIMAL: {
        double d = cell->data.decimal_val;
        /*
         * If d is a whole number (no fractional part) and within the safe
         * integer range for int64, render it without a decimal point.
         * This matches how SQLite renders REAL values that are integers.
         *
         * Examples:
         *   1893456000.0 → "1893456000"  (was "1.89346e+09" with %g)
         *   1200.0       → "1200"        (correct with both)
         *   3.14         → "3.14"        (falls through to %g)
         */
        if (d == floor(d) && d >= -1e15 && d <= 1e15) {
            snprintf(buf, bufsz, "%lld", (long long)d);
        } else {
            snprintf(buf, bufsz, "%g", d);
        }
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

/* ── row_free_contents (unchanged) ──────────────────────────────────────── */
void row_free_contents(Row *row) {
    if (!row||!row->cells) return;
    for (int i=0;i<row->col_count;i++)
        if (row->cells[i].type==COL_TYPE_VARCHAR && row->cells[i].data.varchar_val) {
            free(row->cells[i].data.varchar_val);
            row->cells[i].data.varchar_val=nullptr;
        }
    free(row->cells); row->cells=nullptr;
}

/* ── table_free — clears tail pointer defensively ───────────────────────── */
void table_free(Table *table) {
    if (!table) return;
    Row *r = table->head;
    while (r) { Row *n=r->next; row_free_contents(r); free(r); r=n; }
    table->head = nullptr;
    table->tail = nullptr;          /* FIX 3: clear tail on free             */
    index_free(table->pk_index);
    table->pk_index = nullptr;
    pthread_mutex_destroy(&table->lock);
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

/* ── table_create — initialises tail = nullptr ───────────────────────────── */
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
    if (!t) { pthread_mutex_unlock(&db->schema_lock); set_err(errmsg,"Out of memory"); return nullptr; }

    strncpy(t->name,name,FLEXQL_MAX_NAME_LEN-1);
    for (char *p=t->name;*p;p++) *p=(char)toupper((unsigned char)*p);

    t->col_count=col_count; t->pk_col=-1;
    for (int i=0;i<col_count;i++) {
        t->schema[i]=cols[i];
        for (char *p=t->schema[i].name;*p;p++) *p=(char)toupper((unsigned char)*p);
        if (cols[i].constraints&COL_CONSTRAINT_PRIMARY_KEY) t->pk_col=i;
    }

    t->head = nullptr;
    t->tail = nullptr;             /* FIX 3: initialise tail                 */
    t->row_count = 0;
    t->next_row_id = 1;
    pthread_mutex_init(&t->lock, nullptr);

    t->pk_index = nullptr;
    if (t->pk_col >= 0) {
        ColumnType pk_type = t->schema[t->pk_col].type;
        t->pk_index = index_create(pk_type);
        if (!t->pk_index) {
            pthread_mutex_unlock(&db->schema_lock);
            pthread_mutex_destroy(&t->lock);
            free(t);
            set_err(errmsg,"Out of memory creating primary key index");
            return nullptr;
        }
    }

    db->tables[db->table_count++]=t;
    pthread_mutex_unlock(&db->schema_lock);
    return t;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * FIX 1 — ROW APPEND (insertion order)
 *
 * Old (prepend, O(1)):
 *   row->next = table->head;
 *   table->head = row;
 *   → rows stored newest-first → SELECT returns them newest-first (WRONG)
 *
 * New (append, O(1) with tail pointer):
 *   row->next = nullptr;
 *   if (table->tail) table->tail->next = row;
 *   else             table->head = row;   // first row ever inserted
 *   table->tail = row;
 *   → rows stored oldest-first → SELECT returns insertion order (CORRECT)
 *
 * The tail pointer makes append O(1) — no traversal needed.
 * The only cost vs prepend is one extra pointer write per insert.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int row_insert(Table *table,
               const char str_values[][FLEXQL_MAX_VARCHAR],
               int val_count, time_t expiry, char **errmsg) {

    if (val_count!=table->col_count) {
        set_err_fmt(errmsg,"Table '%s' has %d columns but %d values given",
                    table->name,table->col_count,val_count);
        return -1;
    }

    CellValue *cells=(CellValue*)calloc(table->col_count,sizeof(CellValue));
    if (!cells) { set_err(errmsg,"Out of memory"); return -1; }

    for (int i=0;i<table->col_count;i++) {
        const ColumnDef *col=&table->schema[i];
        const char      *sv =str_values[i];
        if ((col->constraints&COL_CONSTRAINT_NOT_NULL)&&sv[0]=='\0') {
            set_err_fmt(errmsg,"Column '%s' cannot be NULL",col->name);
            for(int j=0;j<i;j++) if(cells[j].type==COL_TYPE_VARCHAR) free(cells[j].data.varchar_val);
            free(cells); return -1;
        }
        if (string_to_cell(sv,col->type,&cells[i],errmsg)!=0) {
            for(int j=0;j<i;j++) if(cells[j].type==COL_TYPE_VARCHAR) free(cells[j].data.varchar_val);
            free(cells); return -1;
        }
    }

    /* PK duplicate check (O(1) with index, O(n) fallback) */
    if (table->pk_col>=0 && !cells[table->pk_col].is_null) {
        const CellValue *npk = &cells[table->pk_col];
        pthread_mutex_lock(&table->lock);
        if (table->pk_index) {
            Row *existing = index_get(table->pk_index, npk);
            if (existing) {
                time_t now = time(nullptr);
                bool expired = (existing->expiry>0 && existing->expiry<now);
                if (!expired) {
                    pthread_mutex_unlock(&table->lock);
                    set_err_fmt(errmsg,"Duplicate PRIMARY KEY value in '%s'",
                                table->schema[table->pk_col].name);
                    for(int i=0;i<table->col_count;i++) if(cells[i].type==COL_TYPE_VARCHAR) free(cells[i].data.varchar_val);
                    free(cells); return -1;
                }
                index_remove(table->pk_index, npk);
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
                    pthread_mutex_unlock(&table->lock);
                    set_err_fmt(errmsg,"Duplicate PRIMARY KEY value in '%s'",
                                table->schema[table->pk_col].name);
                    for(int i=0;i<table->col_count;i++) if(cells[i].type==COL_TYPE_VARCHAR) free(cells[i].data.varchar_val);
                    free(cells); return -1;
                }
            }
        }
        pthread_mutex_unlock(&table->lock);
    }

    Row *row=(Row*)calloc(1,sizeof(Row));
    if (!row) {
        for(int i=0;i<table->col_count;i++) if(cells[i].type==COL_TYPE_VARCHAR) free(cells[i].data.varchar_val);
        free(cells); set_err(errmsg,"Out of memory"); return -1;
    }
    row->cells=cells; row->col_count=table->col_count; row->expiry=expiry;
    row->next=nullptr;

    /* ── FIX 1: APPEND to tail (insertion order) ─────────────────────────
     * Before: row->next = table->head; table->head = row;  (PREPEND)
     * After:  link new row after current tail               (APPEND)
     * Both are O(1). The difference is traversal order.     */
    pthread_mutex_lock(&table->lock);
    row->row_id = table->next_row_id++;

    if (table->tail) {
        table->tail->next = row;   /* link previous last row → new row      */
    } else {
        table->head = row;         /* first row: head and tail point to same */
    }
    table->tail = row;             /* new row is now the last row            */
    table->row_count++;

    if (table->pk_index && table->pk_col>=0 && !row->cells[table->pk_col].is_null)
        index_put(table->pk_index, &row->cells[table->pk_col], row);

    pthread_mutex_unlock(&table->lock);
    return 0;
}

/* ── table_scan (unchanged — walks head→tail which is now insertion order) */
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

    /* Index fast-path: O(1) for WHERE pk = value */
    if (wcol >= 0 &&
        wcol == table->pk_col &&
        where->op == OP_EQ &&
        table->pk_index != nullptr) {

        CellValue lookup_key{};
        char *dummy_err = nullptr;
        if (string_to_cell(where->value,
                           table->schema[table->pk_col].type,
                           &lookup_key, &dummy_err) == 0) {
            pthread_mutex_lock(&table->lock);
            Row *found = index_get(table->pk_index, &lookup_key);
            int visited = 0;
            if (found) {
                time_t now = time(nullptr);
                bool live = !(found->expiry > 0 && found->expiry < now);
                if (live) { visited = 1; cb(found, arg); }
            }
            pthread_mutex_unlock(&table->lock);
            if (lookup_key.type==COL_TYPE_VARCHAR && lookup_key.data.varchar_val)
                free(lookup_key.data.varchar_val);
            free(dummy_err);
            return visited;
        }
        free(dummy_err);
    }

    /* Full table scan — walks head→tail (insertion order after Fix 1) */
    time_t now=time(nullptr);
    int    visited=0;
    pthread_mutex_lock(&table->lock);
    for (Row *r=table->head;r;r=r->next) {
        if (r->expiry>0&&r->expiry<now) continue;
        if (wcol>=0&&!cell_matches_where(&r->cells[wcol],where)) continue;
        visited++;
        if (cb(r,arg)) break;
    }
    pthread_mutex_unlock(&table->lock);
    return visited;
}