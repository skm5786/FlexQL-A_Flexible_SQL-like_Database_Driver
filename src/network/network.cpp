/**
 * network.cpp  —  Query Executor  (Lesson 5 — Cache integrated)
 *
 * Changes from Lesson 3/4:
 *   SELECT      → check cache first; on miss scan + store; on hit replay
 *   INNER JOIN  → check cache first; same pattern
 *   INSERT      → invalidate cache entries for the affected table
 *   DROP TABLE  → invalidate cache entries for that table
 *   DROP DB     → invalidate all cache entries for that database
 */
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <ctime>
#include <strings.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "common/types.h"
#include "storage/dbmanager.h"
#include "storage/storage.h"
#include "storage/wal.h"
#include "cache/cache.h"

/* ── Wire helpers (unchanged) ────────────────────────────────────────────── */
static int _send_all(int fd, const void *buf, size_t len) {
    const char *p=(const char*)buf; size_t rem=len;
    while(rem>0){ssize_t n=send(fd,p,rem,MSG_NOSIGNAL);if(n<=0)return -1;p+=n;rem-=(size_t)n;}
    return 0;
}
static int _send_msg(int fd, MessageType type, const char *payload, uint32_t plen) {
    WireHeader hdr; hdr.msg_type=(uint8_t)type; hdr.payload_len=htonl(plen);
    if(_send_all(fd,&hdr,sizeof(hdr))!=0) return -1;
    if(plen>0&&_send_all(fd,payload,plen)!=0) return -1;
    return 0;
}
static void send_ok(int fd,const char *msg){_send_msg(fd,MSG_OK,msg,(uint32_t)strlen(msg));}
static void send_err(int fd,const char *msg){_send_msg(fd,MSG_ERROR,msg,(uint32_t)strlen(msg));}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * RESULT ROW SERIALISATION (unchanged from Lesson 3)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int send_result_row(int fd, int col_count,
                            const char **col_names, const char **col_values) {
    uint32_t total=sizeof(uint32_t);
    for(int i=0;i<col_count;i++){
        total+=sizeof(uint32_t)+(uint32_t)strlen(col_names[i]);
        total+=sizeof(uint32_t)+(col_values[i]?(uint32_t)strlen(col_values[i]):0);
    }
    char *payload=(char*)malloc(total); if(!payload) return -1;
    char *p=payload;
    uint32_t cn=htonl((uint32_t)col_count); memcpy(p,&cn,4); p+=4;
    for(int i=0;i<col_count;i++){
        uint32_t nlen=(uint32_t)strlen(col_names[i]),nlen_n=htonl(nlen);
        memcpy(p,&nlen_n,4); p+=4; memcpy(p,col_names[i],nlen); p+=nlen;
        uint32_t vlen=col_values[i]?(uint32_t)strlen(col_values[i]):0,vlen_n=htonl(vlen);
        memcpy(p,&vlen_n,4); p+=4;
        if(vlen>0){memcpy(p,col_values[i],vlen);p+=vlen;}
    }
    int rc=_send_msg(fd,MSG_RESULT,payload,total);
    free(payload); return rc;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * LESSON 5 — CACHE-AWARE RESULT BUILDING
 *
 * When we do a SELECT scan, instead of sending each row directly to the
 * client socket, we ALSO capture every serialised payload into a dynamic
 * array.  After the scan completes, we:
 *   1. Store the array in the cache (keyed by db_name + sql).
 *   2. The payloads have already been sent to the client during scan,
 *      so no extra copy is needed for this request.
 *
 * On the NEXT identical SELECT, cache_get() returns the stored payloads
 * and we loop through them sending each directly — zero storage access.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/* Dynamic array to accumulate serialised row payloads during a scan */
struct PayloadBuffer {
    char    **bufs;
    uint32_t *lens;
    int       count;
    int       cap;
};

static void pb_init(PayloadBuffer *pb) {
    pb->cap=32; pb->count=0;
    pb->bufs=(char**)calloc(pb->cap,sizeof(char*));
    pb->lens=(uint32_t*)calloc(pb->cap,sizeof(uint32_t));
}
static void pb_free(PayloadBuffer *pb) {
    for(int i=0;i<pb->count;i++) free(pb->bufs[i]);
    free(pb->bufs); free(pb->lens);
    pb->bufs=nullptr; pb->lens=nullptr; pb->count=0;
}
static int pb_append(PayloadBuffer *pb, const char *data, uint32_t len) {
    if(pb->count>=pb->cap){
        pb->cap*=2;
        pb->bufs=(char**)realloc(pb->bufs,pb->cap*sizeof(char*));
        pb->lens=(uint32_t*)realloc(pb->lens,pb->cap*sizeof(uint32_t));
        if(!pb->bufs||!pb->lens) return -1;
    }
    pb->bufs[pb->count]=(char*)malloc(len);
    if(!pb->bufs[pb->count]) return -1;
    memcpy(pb->bufs[pb->count],data,len);
    pb->lens[pb->count]=len;
    pb->count++;
    return 0;
}

/* Build and send one result row, also storing it in the payload buffer */
static int emit_and_capture(int fd, const Table *table,
                             const SelectList *sel, const Row *row,
                             PayloadBuffer *pb) {
    int ncols; int col_idxs[FLEXQL_MAX_COLUMNS];
    if(sel->select_all){
        ncols=table->col_count;
        for(int i=0;i<ncols;i++) col_idxs[i]=i;
    } else {
        ncols=0;
        for(int s=0;s<sel->col_count;s++){
            const char *cname=sel->col_names[s];
            const char *dot=strchr(cname,'.');
            if(dot) cname=dot+1;
            for(int i=0;i<table->col_count;i++)
                if(strcasecmp(table->schema[i].name,cname)==0){col_idxs[ncols++]=i;break;}
        }
        if(ncols==0) return 0;
    }

    const char *names[FLEXQL_MAX_COLUMNS];
    const char *values[FLEXQL_MAX_COLUMNS];
    char vbufs[FLEXQL_MAX_COLUMNS][64];
    for(int i=0;i<ncols;i++){
        int ci=col_idxs[i];
        names[i]=table->schema[ci].name;
        values[i]=cell_to_string(&row->cells[ci],vbufs[i],sizeof(vbufs[i]));
    }

    /* Build the serialised payload */
    uint32_t total=sizeof(uint32_t);
    for(int i=0;i<ncols;i++){
        total+=sizeof(uint32_t)+(uint32_t)strlen(names[i]);
        total+=sizeof(uint32_t)+(values[i]?(uint32_t)strlen(values[i]):0);
    }
    char *payload=(char*)malloc(total); if(!payload) return -1;
    char *pp=payload;
    uint32_t cn=htonl((uint32_t)ncols); memcpy(pp,&cn,4); pp+=4;
    for(int i=0;i<ncols;i++){
        uint32_t nl=(uint32_t)strlen(names[i]),nl_n=htonl(nl);
        memcpy(pp,&nl_n,4);pp+=4;memcpy(pp,names[i],nl);pp+=nl;
        uint32_t vl=values[i]?(uint32_t)strlen(values[i]):0,vl_n=htonl(vl);
        memcpy(pp,&vl_n,4);pp+=4;if(vl>0){memcpy(pp,values[i],vl);pp+=vl;}
    }

    /* Send to client */
    _send_msg(fd, MSG_RESULT, payload, total);

    /* Store in buffer for later caching */
    if(pb) pb_append(pb, payload, total);

    free(payload);
    return 0;
}

/* Scan callback context for SELECT with caching */
struct ScanCtx {
    int client_fd;
    const Table *table;
    const SelectList *sel;
    PayloadBuffer *pb;
    int rows_sent;
};
static int select_cb(const Row *row, void *arg) {
    ScanCtx *ctx=(ScanCtx*)arg;
    emit_and_capture(ctx->client_fd,ctx->table,ctx->sel,row,ctx->pb);
    ctx->rows_sent++; return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * INNER JOIN (unchanged logic, cache-aware wrapper added)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
struct JoinCtx {
    int client_fd;
    const Table *outer_table, *inner_table;
    int outer_jcol, inner_jcol;
    const SelectList *sel;
    PayloadBuffer *pb;
    int rows_sent;
};
struct InnerCtx {
    int client_fd;
    const Table *outer_table, *inner_table;
    const Row *outer_row;
    int outer_jcol, inner_jcol;
    const SelectList *sel;
    PayloadBuffer *pb;
    int *rows_sent;
};

static int inner_join_inner_cb(const Row *inner_row, void *arg) {
    InnerCtx *ctx=(InnerCtx*)arg;
    const CellValue *ov=&ctx->outer_row->cells[ctx->outer_jcol];
    const CellValue *iv=&inner_row->cells[ctx->inner_jcol];
    WhereClause fw{}; fw.has_condition=1; fw.op=OP_EQ;
    char vbuf[64]; cell_to_string(ov,vbuf,sizeof(vbuf));
    strncpy(fw.value,vbuf,FLEXQL_MAX_VARCHAR-1);
    if(!cell_matches_where(iv,&fw)) return 0;

    int oc=ctx->outer_table->col_count,ic=ctx->inner_table->col_count,tot=oc+ic;
    const char *names[FLEXQL_MAX_COLUMNS*2]; const char *values[FLEXQL_MAX_COLUMNS*2];
    char vbufs[FLEXQL_MAX_COLUMNS*2][64];
    char nbufs[FLEXQL_MAX_COLUMNS*2][FLEXQL_MAX_NAME_LEN*2+2];
    for(int i=0;i<oc;i++){
        snprintf(nbufs[i],sizeof(nbufs[i]),"%s.%s",ctx->outer_table->name,ctx->outer_table->schema[i].name);
        names[i]=nbufs[i]; values[i]=cell_to_string(&ctx->outer_row->cells[i],vbufs[i],sizeof(vbufs[i]));
    }
    for(int i=0;i<ic;i++){
        snprintf(nbufs[oc+i],sizeof(nbufs[oc+i]),"%s.%s",ctx->inner_table->name,ctx->inner_table->schema[i].name);
        names[oc+i]=nbufs[oc+i]; values[oc+i]=cell_to_string(&inner_row->cells[i],vbufs[oc+i],sizeof(vbufs[oc+i]));
    }

    /* Build payload, send, capture */
    uint32_t total=sizeof(uint32_t);
    for(int i=0;i<tot;i++){
        total+=sizeof(uint32_t)+(uint32_t)strlen(names[i]);
        total+=sizeof(uint32_t)+(values[i]?(uint32_t)strlen(values[i]):0);
    }
    char *payload=(char*)malloc(total); char *pp=payload;
    uint32_t cn=htonl((uint32_t)tot); memcpy(pp,&cn,4); pp+=4;
    for(int i=0;i<tot;i++){
        uint32_t nl=(uint32_t)strlen(names[i]),nl_n=htonl(nl);
        memcpy(pp,&nl_n,4);pp+=4;memcpy(pp,names[i],nl);pp+=nl;
        uint32_t vl=values[i]?(uint32_t)strlen(values[i]):0,vl_n=htonl(vl);
        memcpy(pp,&vl_n,4);pp+=4;if(vl>0){memcpy(pp,values[i],vl);pp+=vl;}
    }
    _send_msg(ctx->client_fd,MSG_RESULT,payload,total);
    if(ctx->pb) pb_append(ctx->pb,payload,total);
    free(payload); (*ctx->rows_sent)++; return 0;
}

static int inner_join_outer_cb(const Row *outer_row, void *arg) {
    JoinCtx *ctx=(JoinCtx*)arg;
    InnerCtx ictx{ctx->client_fd,ctx->outer_table,ctx->inner_table,
                  outer_row,ctx->outer_jcol,ctx->inner_jcol,
                  ctx->sel,ctx->pb,&ctx->rows_sent};
    table_scan(const_cast<Table*>(ctx->inner_table),nullptr,inner_join_inner_cb,&ictx);
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * DB-LEVEL HELPERS (SHOW DATABASES / SHOW TABLES)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static void send_single_col_rows(int fd, const char *col_label,
                                  const char names[][FLEXQL_MAX_NAME_LEN], int n) {
    for(int i=0;i<n;i++){
        const char *cn=col_label; const char *cv=names[i];
        send_result_row(fd,1,&cn,&cv);
    }
    _send_msg(fd,MSG_DONE,nullptr,0);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MAIN EXECUTOR
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int executor_execute(DatabaseManager *mgr, Database **current_db,
                     const QueryNode *query, int client_fd, char **errmsg) {
    char buf[512];
    LRUCache *cache = mgr ? mgr->query_cache : nullptr;
    const char *db_name = (*current_db) ? (*current_db)->name : "";

    switch(query->type){

    /* ── CREATE DATABASE ─────────────────────────────────────────────────── */
    case QUERY_CREATE_DB: {
        char *err=nullptr;
        if(dbmgr_create(mgr,query->params.db.db_name,&err)!=0){
            if(errmsg)*errmsg=err; else free(err);
            send_err(client_fd,err?err:"CREATE DATABASE failed"); return -1;
        }
        /* LESSON 8: Persist named databases to registry */
        wal_register_db(query->params.db.db_name);
        snprintf(buf,sizeof(buf),"Database '%s' created successfully.",query->params.db.db_name);
        send_ok(client_fd,buf); return 0;
    }
    /* ── USE ─────────────────────────────────────────────────────────────── */
    case QUERY_USE_DB: {
        Database *db=dbmgr_find(mgr,query->params.db.db_name);
        if(!db){
            snprintf(buf,sizeof(buf),"Unknown database '%s'",query->params.db.db_name);
            if(errmsg)*errmsg=strdup(buf); send_err(client_fd,buf); return -1;
        }
        *current_db=db;
        snprintf(buf,sizeof(buf),"Database changed to '%s'.",db->name);
        send_ok(client_fd,buf); return 0;
    }
    /* ── SHOW DATABASES ───────────────────────────────────────────────────── */
    case QUERY_SHOW_DBS: {
        char names[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
        int n=dbmgr_list(mgr,names);
        if(n==0){send_ok(client_fd,"(no databases)"); return 0;}
        send_single_col_rows(client_fd,"DATABASE",names,n); return 0;
    }
    /* ── DROP DATABASE ────────────────────────────────────────────────────── */
    case QUERY_DROP_DB: {
        if(*current_db&&strcasecmp((*current_db)->name,query->params.db.db_name)==0)
            *current_db=nullptr;
        if(cache) cache_invalidate_db(cache, query->params.db.db_name);
        /* LESSON 8: Remove from registry before dropping from memory */
        wal_unregister_db(query->params.db.db_name);
        char *err=nullptr;
        if(dbmgr_drop(mgr,query->params.db.db_name,&err)!=0){
            if(errmsg)*errmsg=err; else free(err);
            send_err(client_fd,err?err:"DROP DATABASE failed"); return -1;
        }
        snprintf(buf,sizeof(buf),"Database '%s' dropped.",query->params.db.db_name);
        send_ok(client_fd,buf); return 0;
    }
    /* ── SHOW TABLES ──────────────────────────────────────────────────────── */
    case QUERY_SHOW_TABLES: {
        if(!*current_db){send_err(client_fd,"No database selected"); return -1;}
        Database *db=*current_db;
        pthread_mutex_lock(&db->schema_lock);
        int cnt=db->table_count;
        char tnames[FLEXQL_MAX_TABLES][FLEXQL_MAX_NAME_LEN];
        for(int i=0;i<cnt;i++) strncpy(tnames[i],db->tables[i]->name,FLEXQL_MAX_NAME_LEN-1);
        pthread_mutex_unlock(&db->schema_lock);
        if(cnt==0){send_ok(client_fd,"(no tables)"); return 0;}
        send_single_col_rows(client_fd,"TABLE",tnames,cnt); return 0;
    }

    /* ── CREATE TABLE ─────────────────────────────────────────────────────── */
    case QUERY_CREATE_TABLE: {
        if(!*current_db){send_err(client_fd,"No database selected"); return -1;}
        const CreateTableParams *p=&query->params.create;
        char *err=nullptr;
        Table *t=table_create(*current_db,p->table_name,
                               const_cast<ColumnDef*>(p->columns),p->col_count,&err);
        if(!t){
            if(errmsg)*errmsg=err; else free(err);
            send_err(client_fd,err?err:"CREATE TABLE failed"); return -1;
        }
        /* LESSON 8: Persist table schema to WAL (group commit: schema is small) */
        wal_write_create_table(db_name, t);
        snprintf(buf,sizeof(buf),"Table '%s' created.",t->name);
        send_ok(client_fd,buf); return 0;
    }

    /* ── INSERT ───────────────────────────────────────────────────────────── */
    case QUERY_INSERT: {
        if(!*current_db){send_err(client_fd,"No database selected"); return -1;}
        const InsertParams *p=&query->params.insert;
        Table *t=table_find(*current_db,p->table_name);
        if(!t){
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",p->table_name);
            if(errmsg)*errmsg=strdup(buf); send_err(client_fd,buf);
            /* Free batch memory before returning */
            free(p->extra_rows);
            return -1;
        }

        /* ── LESSON 7: Batch INSERT loop ────────────────────────────────────
         *
         * BEFORE (single-row only):
         *   row_insert(t, p->values, p->value_count, p->expiry, &err)
         *   → 1 TCP message = 1 row inserted = 1 MSG_OK reply
         *
         * AFTER (batch):
         *   for i in 0..batch_row_count-1:
         *     row_insert(t, row_i_values, p->value_count, p->expiry, &err)
         *   → 1 TCP message = N rows inserted = 1 MSG_OK reply
         *
         * PERFORMANCE:
         *   Benchmark sends INSERT INTO BIG_USERS VALUES (r1),(r2),...,(rN);
         *   With N=1000: 1M rows = 1000 TCP roundtrips instead of 1,000,000.
         *   Each roundtrip is eliminated entirely — not just made faster.
         *
         * ERROR SEMANTICS:
         *   If any row fails (e.g. PK dupe), we stop and return error.
         *   Rows inserted before the failure remain committed (no rollback).
         *   This matches SQLite behaviour for multi-row INSERT.
         *   Full rollback requires transactions (future work).
         *
         * MEMORY:
         *   Row 0 is in p->values[] (inline, no allocation).
         *   Rows 1..N-1 are in p->extra_rows (heap, freed after all inserts).
         *   extra_rows is NULL when batch_row_count == 1 (single-row path).   */

        /* ── LESSON 8: Write ALL rows to WAL BEFORE in-memory insert ────────
         *
         * WHY BEFORE?
         *   The WAL is our crash-recovery guarantee.  The sequence must be:
         *     1. Write rows to WAL + fdatasync()  ← DURABLE on disk
         *     2. Insert rows into RAM linked list + indexes
         *     3. Send MSG_OK to client
         *
         *   If we crash between step 1 and 2: on restart, WAL replay re-inserts
         *   the rows.  Client gets MSG_OK → data is durable. ✓
         *
         *   If we crash between step 2 and 3: rows are in RAM and WAL.
         *   Client never got MSG_OK so it will retry.  WAL replay may insert
         *   duplicate rows — for tables WITH a PK this is caught by the dupe
         *   check.  For tables WITHOUT a PK, duplicates appear.  Production
         *   databases solve this with transaction IDs (future work).
         *
         * GROUP COMMIT: all N batch rows are written in ONE write() + ONE
         * fdatasync().  Cost: O(1) disk syncs per batch INSERT statement,
         * regardless of batch size.  This is the key to high write throughput.
         *
         * Build a flat string pointer array for wal_write_insert_batch.
         * Row 0 is in p->values, rows 1..N-1 are in p->extra_rows.       */
        if (wal_is_persistent(db_name)) {
            /* Build flat array: row_count * value_count string pointers  */
            int total_vals = p->batch_row_count * p->value_count;
            const char **flat = (const char**)malloc(
                (size_t)total_vals * sizeof(const char*));
            if (flat) {
                for (int r = 0; r < p->batch_row_count; r++) {
                    const char (*rv)[FLEXQL_MAX_VARCHAR] =
                        (r == 0) ? p->values
                                 : p->extra_rows[r-1];
                    for (int c = 0; c < p->value_count; c++)
                        flat[r * p->value_count + c] = rv[c];
                }
                wal_write_insert_batch(db_name, p->table_name, t,
                                       flat,
                                       p->batch_row_count, p->value_count,
                                       p->expiry);
                free(flat);
            }
        }

        char *ins_err = nullptr;
        int   rows_inserted = 0;

        for (int r = 0; r < p->batch_row_count; r++) {
            /* Get the pointer to this row's values array */
            const char (*row_vals)[FLEXQL_MAX_VARCHAR];
            if (r == 0) {
                row_vals = p->values;
            } else {
                row_vals = p->extra_rows[r - 1];
            }

            if (row_insert(t,
                           (const char(*)[FLEXQL_MAX_VARCHAR])row_vals,
                           p->value_count,
                           p->expiry,
                           &ins_err) != 0) {
                /* Insert failed — stop, report, clean up heap, return error */
                if (errmsg) *errmsg = ins_err; else free(ins_err);
                send_err(client_fd, ins_err ? ins_err : "INSERT failed");
                free(p->extra_rows);
                return -1;
            }
            rows_inserted++;
        }

        /* Free the heap-allocated extra rows (NULL-safe) */
        free(p->extra_rows);

        /* Invalidate cache once for all inserted rows */
        if(cache) cache_invalidate_table(cache, db_name, p->table_name);

        /* Report total rows inserted */
        if (p->batch_row_count == 1) {
            send_ok(client_fd, "1 row inserted.");
        } else {
            snprintf(buf, sizeof(buf), "%d rows inserted.", rows_inserted);
            send_ok(client_fd, buf);
        }
        return 0;
    }

    /* ── SELECT ───────────────────────────────────────────────────────────── */
    case QUERY_SELECT: {
        if(!*current_db){send_err(client_fd,"No database selected"); return -1;}
        const SelectParams *p=&query->params.select;

        /* CACHE MISS — find table first (needed for both validation and scan) */
        Table *t=table_find(*current_db,p->table_name);
        if(!t){
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",p->table_name);
            if(errmsg)*errmsg=strdup(buf); send_err(client_fd,buf); return -1;
        }

        /* ── FIX: Validate columns BEFORE cache lookup ───────────────────────
         * IMPORTANT: This check must happen before the cache lookup.
         *
         * Why? The cache key does not include the column list:
         *   "SELECT * FROM T"       → key "SELECT:T:WHERE:::"
         *   "SELECT BADCOL FROM T"  → key "SELECT:T:WHERE:::"  (SAME!)
         * If we checked after the cache, a cached result for "SELECT *"
         * would be returned for "SELECT BADCOL" — hiding the error.
         *
         * By validating first, we return an error before touching the cache
         * for any query referencing an unknown column.                       */
        if (!p->select.select_all) {
            for (int s = 0; s < p->select.col_count; s++) {
                const char *cname = p->select.col_names[s];
                const char *dot = strchr(cname, '.');
                if (dot) cname = dot + 1;
                bool found = false;
                for (int i = 0; i < t->col_count; i++) {
                    if (strcasecmp(t->schema[i].name, cname) == 0) {
                        found = true; break;
                    }
                }
                if (!found) {
                    snprintf(buf, sizeof(buf),
                             "Unknown column '%s' in table '%s'",
                             cname, t->name);
                    if (errmsg) *errmsg = strdup(buf);
                    send_err(client_fd, buf);
                    return -1;
                }
            }
        }

        /* Build cache key and check cache */
        char sql_key[CACHE_KEY_MAX];
        snprintf(sql_key, CACHE_KEY_MAX, "SELECT:%s:WHERE:%s:%s:%s",
                 p->table_name,
                 p->where.has_condition ? p->where.col_name : "",
                 p->where.has_condition ? "=" : "",
                 p->where.has_condition ? p->where.value : "");

        CacheEntry *ce = nullptr;
        if(cache && cache_get(cache, db_name, sql_key, &ce) && ce) {
            for(CachedPayload *cp=ce->payloads; cp; cp=cp->next)
                _send_msg(client_fd, MSG_RESULT, cp->data, cp->len);
            _send_msg(client_fd, MSG_DONE, nullptr, 0);
            return 0;
        }

        PayloadBuffer pb; pb_init(&pb);
        ScanCtx ctx{client_fd, t, &p->select, &pb, 0};
        table_scan(t, &p->where, select_cb, &ctx);
        _send_msg(client_fd, MSG_DONE, nullptr, 0);

        if(cache && pb.count > 0) {
            cache_put(cache, db_name, sql_key,
                      (const char**)pb.bufs, pb.lens, pb.count);
        } else if(cache && pb.count == 0) {
            cache_put(cache, db_name, sql_key, nullptr, nullptr, 0);
        }
        pb_free(&pb);
        return 0;
    }

    /* ── INNER JOIN ───────────────────────────────────────────────────────── */
    case QUERY_INNER_JOIN: {
        if(!*current_db){send_err(client_fd,"No database selected"); return -1;}
        const SelectParams *p=&query->params.select;

        char sql_key[CACHE_KEY_MAX];
        snprintf(sql_key, CACHE_KEY_MAX, "JOIN:%s:%s:ON:%s=%s:WHERE:%s:%s",
                 p->table_name, p->join_table,
                 p->join_col_a, p->join_col_b,
                 p->where.has_condition?p->where.col_name:"",
                 p->where.has_condition?p->where.value:"");

        CacheEntry *ce=nullptr;
        if(cache && cache_get(cache, db_name, sql_key, &ce) && ce) {
            for(CachedPayload *cp=ce->payloads;cp;cp=cp->next)
                _send_msg(client_fd,MSG_RESULT,cp->data,cp->len);
            _send_msg(client_fd,MSG_DONE,nullptr,0);
            return 0;
        }

        Table *outer=table_find(*current_db,p->table_name);
        Table *inner=table_find(*current_db,p->join_table);
        if(!outer){snprintf(buf,sizeof(buf),"Table '%s' does not exist",p->table_name);send_err(client_fd,buf);return -1;}
        if(!inner){snprintf(buf,sizeof(buf),"Table '%s' does not exist",p->join_table);send_err(client_fd,buf);return -1;}

        int ojcol=-1,ijcol=-1;
        for(int i=0;i<outer->col_count;i++) if(strcasecmp(outer->schema[i].name,p->join_col_a)==0){ojcol=i;break;}
        for(int i=0;i<inner->col_count;i++) if(strcasecmp(inner->schema[i].name,p->join_col_b)==0){ijcol=i;break;}
        if(ojcol<0||ijcol<0){send_err(client_fd,"Join column not found");return -1;}

        PayloadBuffer pb; pb_init(&pb);
        JoinCtx jctx{client_fd,outer,inner,ojcol,ijcol,&p->select,&pb,0};
        table_scan(outer,&p->where,inner_join_outer_cb,&jctx);
        _send_msg(client_fd,MSG_DONE,nullptr,0);

        if(cache)
            cache_put(cache,db_name,sql_key,(const char**)pb.bufs,pb.lens,pb.count);
        pb_free(&pb);
        return 0;
    }

    default: {
        send_err(client_fd,"Unknown query type");
        if(errmsg)*errmsg=strdup("Unknown query type");
        return -1;
    }
    }
}