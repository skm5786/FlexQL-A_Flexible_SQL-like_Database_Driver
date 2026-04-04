/**
 * network.cpp  —  Query Executor
 *
 * CHANGE vs previous version:
 *   Added QUERY_DROP_TABLE case in executor_execute().
 *
 *   Without this, "DROP TABLE name;" was parsed into QUERY_DROP_TABLE
 *   by the updated parser but fell through to the default case which
 *   returned MSG_ERROR "Unknown query type".  fql_exec_ignore() swallowed
 *   that error, then CREATE TABLE on the same name failed with "already
 *   exists" — breaking every custom benchmark on re-run.
 *
 *   DROP TABLE implementation:
 *     1. Find the table in current_db
 *     2. Remove it from db->tables[] array
 *     3. Call table_free() to release all memory (arena, indexes, btrees)
 *     4. Write WAL_DROP_TABLE so recovery skips the table's WAL entries
 *     5. Invalidate LRU cache entries for this table
 *
 *   The WAL entry type WAL_DROP_TABLE=3 already existed and was already
 *   replayed in wal_recover().  We just needed to write it on DROP.
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

/* ── Wire helpers ────────────────────────────────────────────────────── */
static int _send_all(int fd, const void *buf, size_t len) {
    const char *p=(const char*)buf; size_t rem=len;
    while(rem>0){ssize_t n=send(fd,p,rem,MSG_NOSIGNAL);
                 if(n<=0)return -1;p+=n;rem-=(size_t)n;}
    return 0;
}
static int _send_msg(int fd, MessageType type,
                      const char *payload, uint32_t plen) {
    WireHeader hdr;
    hdr.msg_type=(uint8_t)type; hdr.payload_len=htonl(plen);
    if(_send_all(fd,&hdr,sizeof(hdr))!=0) return -1;
    if(plen>0&&_send_all(fd,payload,plen)!=0) return -1;
    return 0;
}
static void send_ok(int fd,const char *msg){
    _send_msg(fd,MSG_OK,msg,(uint32_t)strlen(msg));
}
static void send_err(int fd,const char *msg){
    _send_msg(fd,MSG_ERROR,msg,(uint32_t)strlen(msg));
}

/* ── Result row serialisation (unchanged) ────────────────────────────── */
static int send_result_row(int fd, int col_count,
                            const char **col_names,
                            const char **col_values) {
    uint32_t total=sizeof(uint32_t);
    for(int i=0;i<col_count;i++){
        total+=sizeof(uint32_t)+(uint32_t)strlen(col_names[i]);
        total+=sizeof(uint32_t)+
               (col_values[i]?(uint32_t)strlen(col_values[i]):0);
    }
    char *payload=(char*)malloc(total); if(!payload) return -1;
    char *p=payload;
    uint32_t cn=htonl((uint32_t)col_count); memcpy(p,&cn,4); p+=4;
    for(int i=0;i<col_count;i++){
        uint32_t nlen=(uint32_t)strlen(col_names[i]),nlen_n=htonl(nlen);
        memcpy(p,&nlen_n,4); p+=4;
        memcpy(p,col_names[i],nlen); p+=nlen;
        uint32_t vlen=col_values[i]?(uint32_t)strlen(col_values[i]):0;
        uint32_t vlen_n=htonl(vlen);
        memcpy(p,&vlen_n,4); p+=4;
        if(vlen>0){memcpy(p,col_values[i],vlen);p+=vlen;}
    }
    int rc=_send_msg(fd,MSG_RESULT,payload,total);
    free(payload); return rc;
}

/* ── Cache-aware payload buffer (unchanged) ─────────────────────────── */
struct PayloadBuffer {
    char    **bufs; uint32_t *lens; int count; int cap;
};
static void pb_init(PayloadBuffer *pb){
    pb->cap=32;pb->count=0;
    pb->bufs=(char**)calloc(pb->cap,sizeof(char*));
    pb->lens=(uint32_t*)calloc(pb->cap,sizeof(uint32_t));
}
static void pb_free(PayloadBuffer *pb){
    for(int i=0;i<pb->count;i++) free(pb->bufs[i]);
    free(pb->bufs);free(pb->lens);
    pb->bufs=nullptr;pb->lens=nullptr;pb->count=0;
}
static int pb_append(PayloadBuffer *pb,const char *data,uint32_t len){
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
                if(strcasecmp(table->schema[i].name,cname)==0){
                    col_idxs[ncols++]=i;break;
                }
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
        memcpy(pp,&vl_n,4);pp+=4;
        if(vl>0){memcpy(pp,values[i],vl);pp+=vl;}
    }
    _send_msg(fd,MSG_RESULT,payload,total);
    if(pb) pb_append(pb,payload,total);
    free(payload);
    return 0;
}

struct ScanCtx {
    int client_fd; const Table *table;
    const SelectList *sel; PayloadBuffer *pb; int rows_sent;
};
static int select_cb(const Row *row,void *arg){
    ScanCtx *ctx=(ScanCtx*)arg;
    emit_and_capture(ctx->client_fd,ctx->table,ctx->sel,row,ctx->pb);
    ctx->rows_sent++; return 0;
}

/* ── Inner join (unchanged) ─────────────────────────────────────────── */
struct JoinCtx {
    int client_fd;
    const Table *outer_table,*inner_table;
    int outer_jcol,inner_jcol;
    const SelectList *sel; PayloadBuffer *pb; int rows_sent;
};
struct InnerCtx {
    int client_fd;
    const Table *outer_table,*inner_table;
    const Row *outer_row;
    int outer_jcol,inner_jcol;
    const SelectList *sel; PayloadBuffer *pb; int *rows_sent;
};
static int inner_join_inner_cb(const Row *inner_row,void *arg){
    InnerCtx *ctx=(InnerCtx*)arg;
    const CellValue *ov=&ctx->outer_row->cells[ctx->outer_jcol];
    const CellValue *iv=&inner_row->cells[ctx->inner_jcol];
    WhereClause fw{}; fw.has_condition=1; fw.op=OP_EQ;
    char vbuf[64]; cell_to_string(ov,vbuf,sizeof(vbuf));
    strncpy(fw.value,vbuf,FLEXQL_MAX_VARCHAR-1);
    if(!cell_matches_where(iv,&fw)) return 0;
    int oc=ctx->outer_table->col_count,ic=ctx->inner_table->col_count,tot=oc+ic;
    const char *names[FLEXQL_MAX_COLUMNS*2];
    const char *values[FLEXQL_MAX_COLUMNS*2];
    char vbufs[FLEXQL_MAX_COLUMNS*2][64];
    char nbufs[FLEXQL_MAX_COLUMNS*2][FLEXQL_MAX_NAME_LEN*2+2];
    for(int i=0;i<oc;i++){
        snprintf(nbufs[i],sizeof(nbufs[i]),"%s.%s",
                 ctx->outer_table->name,ctx->outer_table->schema[i].name);
        names[i]=nbufs[i];
        values[i]=cell_to_string(&ctx->outer_row->cells[i],
                                  vbufs[i],sizeof(vbufs[i]));
    }
    for(int i=0;i<ic;i++){
        snprintf(nbufs[oc+i],sizeof(nbufs[oc+i]),"%s.%s",
                 ctx->inner_table->name,ctx->inner_table->schema[i].name);
        names[oc+i]=nbufs[oc+i];
        values[oc+i]=cell_to_string(&inner_row->cells[i],
                                     vbufs[oc+i],sizeof(vbufs[oc+i]));
    }
    uint32_t total=sizeof(uint32_t);
    for(int i=0;i<tot;i++){
        total+=sizeof(uint32_t)+(uint32_t)strlen(names[i]);
        total+=sizeof(uint32_t)+(values[i]?(uint32_t)strlen(values[i]):0);
    }
    char *payload=(char*)malloc(total); char *pp=payload;
    uint32_t cn2=htonl((uint32_t)tot); memcpy(pp,&cn2,4); pp+=4;
    for(int i=0;i<tot;i++){
        uint32_t nl=(uint32_t)strlen(names[i]),nl_n=htonl(nl);
        memcpy(pp,&nl_n,4);pp+=4;memcpy(pp,names[i],nl);pp+=nl;
        uint32_t vl=values[i]?(uint32_t)strlen(values[i]):0,vl_n=htonl(vl);
        memcpy(pp,&vl_n,4);pp+=4;
        if(vl>0){memcpy(pp,values[i],vl);pp+=vl;}
    }
    _send_msg(ctx->client_fd,MSG_RESULT,payload,total);
    if(ctx->pb) pb_append(ctx->pb,payload,total);
    free(payload); (*ctx->rows_sent)++; return 0;
}
static int inner_join_outer_cb(const Row *outer_row,void *arg){
    JoinCtx *ctx=(JoinCtx*)arg;
    InnerCtx ictx{ctx->client_fd,ctx->outer_table,ctx->inner_table,
                  outer_row,ctx->outer_jcol,ctx->inner_jcol,
                  ctx->sel,ctx->pb,&ctx->rows_sent};
    table_scan(const_cast<Table*>(ctx->inner_table),nullptr,
               inner_join_inner_cb,&ictx);
    return 0;
}

/* ── DB-level helpers (unchanged) ────────────────────────────────────── */
static void send_single_col_rows(int fd, const char *col_label,
                                  const char names[][FLEXQL_MAX_NAME_LEN],
                                  int n) {
    for(int i=0;i<n;i++){
        const char *cn=col_label; const char *cv=names[i];
        send_result_row(fd,1,&cn,&cv);
    }
    _send_msg(fd,MSG_DONE,nullptr,0);
}

/* ════════════════════════════════════════════════════════════════════════
 *  MAIN EXECUTOR
 * ════════════════════════════════════════════════════════════════════════ */
int executor_execute(DatabaseManager *mgr, Database **current_db,
                     const QueryNode *query, int client_fd,
                     char **errmsg) {
    char buf[512];
    LRUCache *cache = mgr ? mgr->query_cache : nullptr;
    const char *db_name = (*current_db) ? (*current_db)->name : "";

    switch(query->type){

    /* ── CREATE DATABASE ─────────────────────────────────────────── */
    case QUERY_CREATE_DB: {
        char *err=nullptr;
        if(dbmgr_create(mgr,query->params.db.db_name,&err)!=0){
            if(errmsg)*errmsg=err; else free(err);
            send_err(client_fd,err?err:"CREATE DATABASE failed");
            return -1;
        }
        wal_register_db(query->params.db.db_name);
        snprintf(buf,sizeof(buf),"Database '%s' created successfully.",
                 query->params.db.db_name);
        send_ok(client_fd,buf); return 0;
    }

    /* ── USE ─────────────────────────────────────────────────────── */
    case QUERY_USE_DB: {
        Database *db=dbmgr_find(mgr,query->params.db.db_name);
        if(!db){
            snprintf(buf,sizeof(buf),"Unknown database '%s'",
                     query->params.db.db_name);
            if(errmsg)*errmsg=strdup(buf);
            send_err(client_fd,buf); return -1;
        }
        *current_db=db;
        snprintf(buf,sizeof(buf),"Database changed to '%s'.",db->name);
        send_ok(client_fd,buf); return 0;
    }

    /* ── SHOW DATABASES ──────────────────────────────────────────── */
    case QUERY_SHOW_DBS: {
        char names[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
        int n=dbmgr_list(mgr,names);
        if(n==0){send_ok(client_fd,"(no databases)"); return 0;}
        send_single_col_rows(client_fd,"DATABASE",names,n); return 0;
    }

    /* ── DROP DATABASE ───────────────────────────────────────────── */
    case QUERY_DROP_DB: {
        if(*current_db&&
           strcasecmp((*current_db)->name,query->params.db.db_name)==0)
            *current_db=nullptr;
        if(cache) cache_invalidate_db(cache,query->params.db.db_name);
        wal_unregister_db(query->params.db.db_name);
        char *err=nullptr;
        if(dbmgr_drop(mgr,query->params.db.db_name,&err)!=0){
            if(errmsg)*errmsg=err; else free(err);
            send_err(client_fd,err?err:"DROP DATABASE failed"); return -1;
        }
        snprintf(buf,sizeof(buf),"Database '%s' dropped.",
                 query->params.db.db_name);
        send_ok(client_fd,buf); return 0;
    }

    /* ── SHOW TABLES ─────────────────────────────────────────────── */
    case QUERY_SHOW_TABLES: {
        if(!*current_db){
            send_err(client_fd,"No database selected"); return -1;
        }
        Database *db=*current_db;
        pthread_mutex_lock(&db->schema_lock);
        int cnt=db->table_count;
        char tnames[FLEXQL_MAX_TABLES][FLEXQL_MAX_NAME_LEN];
        for(int i=0;i<cnt;i++)
            strncpy(tnames[i],db->tables[i]->name,FLEXQL_MAX_NAME_LEN-1);
        pthread_mutex_unlock(&db->schema_lock);
        if(cnt==0){send_ok(client_fd,"(no tables)"); return 0;}
        send_single_col_rows(client_fd,"TABLE",tnames,cnt); return 0;
    }

    /* ── CREATE TABLE ────────────────────────────────────────────── */
    case QUERY_CREATE_TABLE: {
        if(!*current_db){
            send_err(client_fd,"No database selected"); return -1;
        }
        const CreateTableParams *p=&query->params.create;
        char *err=nullptr;
        Table *t=table_create(*current_db,p->table_name,
                               const_cast<ColumnDef*>(p->columns),
                               p->col_count,&err);
        if(!t){
            if(errmsg)*errmsg=err; else free(err);
            send_err(client_fd,err?err:"CREATE TABLE failed"); return -1;
        }
        wal_write_create_table(db_name,t);
        snprintf(buf,sizeof(buf),"Table '%s' created.",t->name);
        send_ok(client_fd,buf); return 0;
    }

    /* ── DROP TABLE ──────────────────────────────────────────────────
     *
     * NEW: This case was missing.  Without it, DROP TABLE silently
     * returned MSG_ERROR, and CREATE TABLE on the same name then
     * failed with "already exists" — breaking all custom benchmarks
     * which call create_standard_table() → DROP TABLE + CREATE TABLE.
     *
     * Steps:
     *   1. Find the table (error if not found).
     *   2. Write WAL_DROP_TABLE so recovery knows to ignore this table.
     *   3. Invalidate LRU cache entries for this table.
     *   4. Remove from db->tables[] and free all memory.
     */
    case QUERY_DROP_TABLE: {
        if(!*current_db){
            send_err(client_fd,"No database selected"); return -1;
        }
        const char *tname = query->params.drop.table_name;
        Database *db = *current_db;

        pthread_mutex_lock(&db->schema_lock);

        /* Find the table index */
        int tidx = -1;
        for(int i = 0; i < db->table_count; i++){
            if(strcasecmp(db->tables[i]->name, tname) == 0){
                tidx = i; break;
            }
        }

        if(tidx < 0){
            pthread_mutex_unlock(&db->schema_lock);
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",tname);
            if(errmsg)*errmsg=strdup(buf);
            send_err(client_fd,buf); return -1;
        }

        Table *t = db->tables[tidx];

        /* Write WAL_DROP_TABLE BEFORE removing from memory */
        wal_write_drop_table(db_name, t->name);

        /* Invalidate cache */
        if(cache) cache_invalidate_table(cache, db_name, t->name);

        /* Remove from db->tables[] array (compact) */
        for(int i = tidx; i < db->table_count - 1; i++)
            db->tables[i] = db->tables[i+1];
        db->tables[--db->table_count] = nullptr;

        pthread_mutex_unlock(&db->schema_lock);

        /* Free all table memory (arena, indexes, btrees) */
        table_free(t);

        snprintf(buf,sizeof(buf),"Table '%s' dropped.",tname);
        send_ok(client_fd,buf); return 0;
    }

    /* ── INSERT ──────────────────────────────────────────────────── */
    case QUERY_INSERT: {
        if(!*current_db){
            send_err(client_fd,"No database selected"); return -1;
        }
        const InsertParams *p=&query->params.insert;
        Table *t=table_find(*current_db,p->table_name);
        if(!t){
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",
                     p->table_name);
            if(errmsg)*errmsg=strdup(buf);
            send_err(client_fd,buf);
            free(p->extra_rows);
            return -1;
        }

        /* WAL write — stack-allocate flat array for single-row case */
        if(wal_is_persistent(db_name)){
            int total_vals = p->batch_row_count * p->value_count;
            const char *flat_stack[FLEXQL_MAX_COLUMNS];
            const char **flat = nullptr;
            bool heap_alloc = false;
            if(total_vals <= FLEXQL_MAX_COLUMNS){
                flat = flat_stack;
            } else {
                flat = (const char**)malloc(
                    (size_t)total_vals * sizeof(const char*));
                heap_alloc = (flat != nullptr);
            }
            if(flat){
                for(int r=0;r<p->batch_row_count;r++){
                    const char (*rv)[FLEXQL_MAX_VARCHAR]=
                        (r==0)?p->values:p->extra_rows[r-1];
                    for(int c=0;c<p->value_count;c++)
                        flat[r*p->value_count+c]=rv[c];
                }
                wal_write_insert_batch(db_name,p->table_name,t,
                                       flat,p->batch_row_count,
                                       p->value_count,p->expiry);
                if(heap_alloc) free((void*)flat);
            }
        }

        char *ins_err=nullptr; int rows_inserted=0;
        for(int r=0;r<p->batch_row_count;r++){
            const char (*row_vals)[FLEXQL_MAX_VARCHAR]=
                (r==0)?p->values:p->extra_rows[r-1];
            if(row_insert(t,
                          (const char(*)[FLEXQL_MAX_VARCHAR])row_vals,
                          p->value_count,p->expiry,&ins_err)!=0){
                if(errmsg)*errmsg=ins_err; else free(ins_err);
                send_err(client_fd,ins_err?ins_err:"INSERT failed");
                free(p->extra_rows); return -1;
            }
            rows_inserted++;
        }
        free(p->extra_rows);

        if(cache) cache_invalidate_table(cache,db_name,p->table_name);

        if(p->batch_row_count==1){
            send_ok(client_fd,"1 row inserted.");
        } else {
            snprintf(buf,sizeof(buf),"%d rows inserted.",rows_inserted);
            send_ok(client_fd,buf);
        }
        return 0;
    }

    /* ── SELECT ──────────────────────────────────────────────────── */
    case QUERY_SELECT: {
        if(!*current_db){
            send_err(client_fd,"No database selected"); return -1;
        }
        const SelectParams *p=&query->params.select;
        Table *t=table_find(*current_db,p->table_name);
        if(!t){
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",
                     p->table_name);
            if(errmsg)*errmsg=strdup(buf);
            send_err(client_fd,buf); return -1;
        }
        if(!p->select.select_all){
            for(int s=0;s<p->select.col_count;s++){
                const char *cname=p->select.col_names[s];
                const char *dot=strchr(cname,'.');
                if(dot) cname=dot+1;
                bool found=false;
                for(int i=0;i<t->col_count;i++){
                    if(strcasecmp(t->schema[i].name,cname)==0){
                        found=true;break;
                    }
                }
                if(!found){
                    snprintf(buf,sizeof(buf),
                             "Unknown column '%s' in table '%s'",
                             cname,t->name);
                    if(errmsg)*errmsg=strdup(buf);
                    send_err(client_fd,buf); return -1;
                }
            }
        }
        char sql_key[CACHE_KEY_MAX];
        snprintf(sql_key,CACHE_KEY_MAX,"SELECT:%s:WHERE:%s:%s:%s",
                 p->table_name,
                 p->where.has_condition?p->where.col_name:"",
                 p->where.has_condition?"=":"",
                 p->where.has_condition?p->where.value:"");
        CacheEntry *ce=nullptr;
        if(cache&&cache_get(cache,db_name,sql_key,&ce)&&ce){
            for(CachedPayload *cp=ce->payloads;cp;cp=cp->next)
                _send_msg(client_fd,MSG_RESULT,cp->data,cp->len);
            _send_msg(client_fd,MSG_DONE,nullptr,0);
            return 0;
        }
        PayloadBuffer pb; pb_init(&pb);
        ScanCtx ctx{client_fd,t,&p->select,&pb,0};
        table_scan(t,&p->where,select_cb,&ctx);
        _send_msg(client_fd,MSG_DONE,nullptr,0);
        if(cache&&pb.count>0)
            cache_put(cache,db_name,sql_key,
                      (const char**)pb.bufs,pb.lens,pb.count);
        else if(cache&&pb.count==0)
            cache_put(cache,db_name,sql_key,nullptr,nullptr,0);
        pb_free(&pb);
        return 0;
    }

    /* ── INNER JOIN ──────────────────────────────────────────────── */
    case QUERY_INNER_JOIN: {
        if(!*current_db){
            send_err(client_fd,"No database selected"); return -1;
        }
        const SelectParams *p=&query->params.select;
        char sql_key[CACHE_KEY_MAX];
        snprintf(sql_key,CACHE_KEY_MAX,"JOIN:%s:%s:ON:%s=%s:WHERE:%s:%s",
                 p->table_name,p->join_table,
                 p->join_col_a,p->join_col_b,
                 p->where.has_condition?p->where.col_name:"",
                 p->where.has_condition?p->where.value:"");
        CacheEntry *ce=nullptr;
        if(cache&&cache_get(cache,db_name,sql_key,&ce)&&ce){
            for(CachedPayload *cp=ce->payloads;cp;cp=cp->next)
                _send_msg(client_fd,MSG_RESULT,cp->data,cp->len);
            _send_msg(client_fd,MSG_DONE,nullptr,0);
            return 0;
        }
        Table *outer=table_find(*current_db,p->table_name);
        Table *inner=table_find(*current_db,p->join_table);
        if(!outer){
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",
                     p->table_name);
            send_err(client_fd,buf);return -1;
        }
        if(!inner){
            snprintf(buf,sizeof(buf),"Table '%s' does not exist",
                     p->join_table);
            send_err(client_fd,buf);return -1;
        }
        int ojcol=-1,ijcol=-1;
        for(int i=0;i<outer->col_count;i++)
            if(strcasecmp(outer->schema[i].name,p->join_col_a)==0){
                ojcol=i;break;
            }
        for(int i=0;i<inner->col_count;i++)
            if(strcasecmp(inner->schema[i].name,p->join_col_b)==0){
                ijcol=i;break;
            }
        if(ojcol<0||ijcol<0){
            send_err(client_fd,"Join column not found");return -1;
        }
        PayloadBuffer pb; pb_init(&pb);
        JoinCtx jctx{client_fd,outer,inner,ojcol,ijcol,
                     &p->select,&pb,0};
        table_scan(outer,&p->where,inner_join_outer_cb,&jctx);
        _send_msg(client_fd,MSG_DONE,nullptr,0);
        if(cache)
            cache_put(cache,db_name,sql_key,
                      (const char**)pb.bufs,pb.lens,pb.count);
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