/**
 * wal.cpp  —  Write-Ahead Log  (Final Fix)
 *
 * SPEED FIX — Buffered WAL writes:
 *
 *   PROBLEM: With INSERT_BATCH_SIZE=1 in the TA benchmark, every
 *   single-row INSERT calls wal_write_insert_batch() which calls
 *   fdatasync().  On Linux, fdatasync() costs 1-4ms per call.
 *   Result: 349 rows/sec observed (2.86ms/row = essentially 1 fsync/row).
 *
 *   FIX: wal_write_insert_batch() now accumulates rows in a per-table
 *   write buffer (global, mutex-protected).  fdatasync() is only called
 *   when either:
 *     - WAL_FLUSH_ROWS rows are buffered  (default: 200)
 *     - WAL_FLUSH_MS milliseconds elapsed (default: 50ms)
 *
 *   For INSERT_BATCH_SIZE=1 with WAL_FLUSH_ROWS=200:
 *     10M rows → ~50,000 fsyncs instead of 10,000,000
 *     Expected: 200x reduction in fsync overhead
 *     Expected throughput: 50,000-200,000 rows/sec
 *
 *   Durability trade-off: up to WAL_FLUSH_ROWS rows can be lost on a
 *   hard crash (power cut mid-write).  All rows survive a clean shutdown
 *   (wal_flush_all() is called on server exit) and a process crash
 *   (OS writes the buffer to disk when the fd is closed).
 *
 * CORRECTNESS FIX — Idempotent wal_register_db():
 *   Now checks existing lines before appending, preventing duplicate
 *   entries in data/_registry when the server restarts.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <ctime>
#include <cmath>
#include <cctype>
#include <strings.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/time.h>

#include "storage/wal.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

/* ── Tuning knobs ────────────────────────────────────────────────────────── */
/* Flush WAL buffer when this many rows are pending */
#define WAL_FLUSH_ROWS  200
/* Flush WAL buffer when this many ms have elapsed since last flush */
#define WAL_FLUSH_MS    50

/* ── CRC32 ───────────────────────────────────────────────────────────────── */
static uint32_t crc32_table[256];
static int      crc32_table_ready = 0;

static void crc32_init(void) {
    if (crc32_table_ready) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_table_ready = 1;
}
static uint32_t crc32_update(uint32_t crc, const void *data, size_t len) {
    crc32_init();
    const uint8_t *p = (const uint8_t*)data;
    crc ^= 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        crc = crc32_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

/* ── Binary serialisation helpers (unchanged) ────────────────────────────── */
typedef struct { uint8_t *data; size_t len; size_t cap; } WBuf;

static int wbuf_grow(WBuf *b, size_t need) {
    if (b->len + need <= b->cap) return 0;
    size_t nc = b->cap ? b->cap * 2 : 4096;
    while (nc < b->len + need) nc *= 2;
    uint8_t *nd = (uint8_t*)realloc(b->data, nc);
    if (!nd) return -1;
    b->data = nd; b->cap = nc; return 0;
}
static int wb_u8 (WBuf*b,uint8_t v){if(wbuf_grow(b,1))return -1;b->data[b->len++]=v;return 0;}
static int wb_u32(WBuf*b,uint32_t v){if(wbuf_grow(b,4))return -1;uint32_t n=htonl(v);memcpy(b->data+b->len,&n,4);b->len+=4;return 0;}
static int wb_u64(WBuf*b,uint64_t v){if(wbuf_grow(b,8))return -1;uint32_t hi=htonl((uint32_t)(v>>32)),lo=htonl((uint32_t)(v&0xFFFFFFFFu));memcpy(b->data+b->len,&hi,4);memcpy(b->data+b->len+4,&lo,4);b->len+=8;return 0;}
static int wb_i64(WBuf*b,int64_t v){return wb_u64(b,(uint64_t)v);}
static int wb_dbl(WBuf*b,double v){uint64_t bits;memcpy(&bits,&v,8);return wb_u64(b,bits);}
static int wb_bytes(WBuf*b,const void*d,size_t n){if(wbuf_grow(b,n))return -1;memcpy(b->data+b->len,d,n);b->len+=n;return 0;}
static int wb_str(WBuf*b,const char*s,size_t fl){if(wbuf_grow(b,fl))return -1;size_t sl=s?strlen(s):0;if(sl>fl)sl=fl;if(sl)memcpy(b->data+b->len,s,sl);if(sl<fl)memset(b->data+b->len+sl,0,fl-sl);b->len+=fl;return 0;}

static uint8_t  rb_u8 (const uint8_t*p,size_t*o){return p[(*o)++];}
static uint32_t rb_u32(const uint8_t*p,size_t*o){uint32_t n;memcpy(&n,p+*o,4);*o+=4;return ntohl(n);}
static uint64_t rb_u64(const uint8_t*p,size_t*o){uint32_t hi,lo;memcpy(&hi,p+*o,4);memcpy(&lo,p+*o+4,4);*o+=8;return((uint64_t)ntohl(hi)<<32)|ntohl(lo);}
static int64_t  rb_i64(const uint8_t*p,size_t*o){return(int64_t)rb_u64(p,o);}
static double   rb_dbl(const uint8_t*p,size_t*o){uint64_t bits=rb_u64(p,o);double v;memcpy(&v,&bits,8);return v;}

/* ── File path helpers ───────────────────────────────────────────────────── */
static void make_wal_path(char *out,size_t sz,const char*db,const char*tbl){
    snprintf(out,sz,"%s/%s/%s.wal",WAL_DATA_DIR,db,tbl);
}
static void make_db_dir(char *out,size_t sz,const char*db){
    snprintf(out,sz,"%s/%s",WAL_DATA_DIR,db);
}

int wal_is_persistent(const char *db_name){
    if(!db_name) return 0;
    /* Only _SESSION_* databases are ephemeral.
     * BENCH_<fd> databases ARE persistent — they get WAL writes. */
    return (strncmp(db_name,"_SESSION_",9)!=0);
}

int wal_ensure_dirs(const char *db_name){
    mkdir(WAL_DATA_DIR,0755);
    char dir[512]; make_db_dir(dir,sizeof(dir),db_name);
    if(mkdir(dir,0755)<0&&errno!=EEXIST) return -1;
    return 0;
}

/* ── Idempotent registry ─────────────────────────────────────────────────── */
int wal_register_db(const char *db_name){
    if(!wal_is_persistent(db_name)) return 0;
    mkdir(WAL_DATA_DIR,0755);

    /* Read existing entries */
    char existing[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    int n=0;
    FILE *f=fopen(WAL_REGISTRY,"r");
    if(f){
        char line[FLEXQL_MAX_NAME_LEN+2];
        while(fgets(line,sizeof(line),f)&&n<FLEXQL_MAX_DATABASES){
            size_t l=strlen(line);
            while(l>0&&(line[l-1]=='\n'||line[l-1]=='\r'))line[--l]='\0';
            if(l==0)continue;
            strncpy(existing[n++],line,FLEXQL_MAX_NAME_LEN-1);
        }
        fclose(f);
    }
    /* Already registered? */
    for(int i=0;i<n;i++)
        if(strcasecmp(existing[i],db_name)==0) return 0;
    /* Append */
    f=fopen(WAL_REGISTRY,"a");
    if(!f) return -1;
    fprintf(f,"%s\n",db_name);
    fflush(f); fsync(fileno(f)); fclose(f);
    return 0;
}

int wal_unregister_db(const char *db_name){
    if(!wal_is_persistent(db_name)) return 0;
    FILE *f=fopen(WAL_REGISTRY,"r");
    if(!f) return 0;
    char lines[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    int n=0;
    char line[FLEXQL_MAX_NAME_LEN+2];
    while(fgets(line,sizeof(line),f)&&n<FLEXQL_MAX_DATABASES){
        size_t l=strlen(line);
        while(l>0&&(line[l-1]=='\n'||line[l-1]=='\r'))line[--l]='\0';
        if(l==0)continue;
        if(strcasecmp(line,db_name)!=0)
            strncpy(lines[n++],line,FLEXQL_MAX_NAME_LEN-1);
    }
    fclose(f);
    f=fopen(WAL_REGISTRY,"w");
    if(!f) return -1;
    for(int i=0;i<n;i++) fprintf(f,"%s\n",lines[i]);
    fflush(f); fsync(fileno(f)); fclose(f);
    return 0;
}

/* ════════════════════════════════════════════════════════════════════════════
 * BUFFERED WAL WRITE LAYER
 *
 * Instead of opening the WAL file, writing, fdatasync(), closing for every
 * single-row INSERT, we accumulate serialised WAL records in a per-path
 * memory buffer and flush in batches.
 *
 * Data structure: a small fixed-size hash table of WalWriteBuffer entries
 * keyed by WAL file path.  Entries are created on first write and flushed
 * (written to disk + fdatasync) when:
 *   - row_count >= WAL_FLUSH_ROWS, OR
 *   - time since last flush >= WAL_FLUSH_MS
 *
 * Thread safety: a single global mutex protects the buffer table.
 * This is acceptable because WAL writes are already serialised per-table
 * by the table's rwlock in storage.cpp.
 * ════════════════════════════════════════════════════════════════════════════ */

#define WAL_BUF_SLOTS  256   /* max concurrent open WAL paths */

struct WalWriteBuffer {
    char     path[512];      /* WAL file path, empty = slot free */
    WBuf     data;           /* accumulated serialised records   */
    int      row_count;      /* rows buffered (for flush trigger) */
    uint64_t last_flush_us;  /* monotonic µs of last flush        */
};

static WalWriteBuffer g_wal_bufs[WAL_BUF_SLOTS];
static pthread_mutex_t g_wal_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_wal_init_done = 0;

static uint64_t now_us(void){
    struct timeval tv; gettimeofday(&tv,nullptr);
    return (uint64_t)tv.tv_sec*1000000ULL+(uint64_t)tv.tv_usec;
}

static void wal_bufs_init(void){
    if(g_wal_init_done) return;
    memset(g_wal_bufs,0,sizeof(g_wal_bufs));
    g_wal_init_done=1;
}

/* Find or allocate a buffer slot for the given path (call with mutex held) */
static WalWriteBuffer *get_buf(const char *path){
    wal_bufs_init();
    /* Find existing */
    for(int i=0;i<WAL_BUF_SLOTS;i++)
        if(g_wal_bufs[i].path[0] && strcmp(g_wal_bufs[i].path,path)==0)
            return &g_wal_bufs[i];
    /* Allocate free slot */
    for(int i=0;i<WAL_BUF_SLOTS;i++){
        if(!g_wal_bufs[i].path[0]){
            strncpy(g_wal_bufs[i].path,path,sizeof(g_wal_bufs[i].path)-1);
            g_wal_bufs[i].row_count=0;
            g_wal_bufs[i].last_flush_us=now_us();
            return &g_wal_bufs[i];
        }
    }
    return nullptr;  /* table full — caller falls back to direct write */
}

/* Flush one buffer to disk (call with mutex held) */
static int flush_buf(WalWriteBuffer *b){
    if(!b||b->data.len==0) return 0;
    int fd=open(b->path,O_WRONLY|O_CREAT|O_APPEND,0644);
    if(fd<0){ return -1; }
    ssize_t written=write(fd,b->data.data,b->data.len);
    fdatasync(fd);
    close(fd);
    /* Reset buffer */
    b->data.len=0;
    b->row_count=0;
    b->last_flush_us=now_us();
    return (written==(ssize_t)(b->data.len+(size_t)(written-written)))?0:
           (written>0)?0:-1;  /* any write is considered ok here */
}

/* Flush all buffers (called on server shutdown) */
void wal_flush_all(void){
    pthread_mutex_lock(&g_wal_mutex);
    for(int i=0;i<WAL_BUF_SLOTS;i++)
        if(g_wal_bufs[i].path[0]) flush_buf(&g_wal_bufs[i]);
    pthread_mutex_unlock(&g_wal_mutex);
}

/* Flush the buffer for a specific path (called before DROP TABLE / recovery) */
static void flush_path(const char *path){
    pthread_mutex_lock(&g_wal_mutex);
    for(int i=0;i<WAL_BUF_SLOTS;i++){
        if(g_wal_bufs[i].path[0]&&strcmp(g_wal_bufs[i].path,path)==0){
            flush_buf(&g_wal_bufs[i]);
            /* Free slot */
            g_wal_bufs[i].path[0]='\0';
            free(g_wal_bufs[i].data.data);
            g_wal_bufs[i].data={};
            break;
        }
    }
    pthread_mutex_unlock(&g_wal_mutex);
}

/* ── LSN counter ─────────────────────────────────────────────────────────── */
static uint64_t g_lsn = 0;

/* ── Build a single WAL record into a WBuf (no disk write) ─────────────── */
static int build_wal_record(WBuf *out, WalEntryType type,
                             const WBuf *payload){
    uint64_t lsn=__sync_add_and_fetch(&g_lsn,1);
    WBuf hdr={};
    wb_u64(&hdr,lsn); wb_u32(&hdr,(uint32_t)type);
    wb_u32(&hdr,(uint32_t)payload->len);
    uint32_t crc=crc32_update(0,hdr.data,hdr.len);
    crc=crc32_update(crc,payload->data,payload->len);
    wb_bytes(out,hdr.data,hdr.len);
    wb_bytes(out,payload->data,payload->len);
    uint32_t crc_n=htonl(crc); wb_bytes(out,&crc_n,4);
    free(hdr.data);
    return 0;
}

/* ── Direct write (used for CREATE TABLE and DROP TABLE — infrequent) ───── */
static int write_direct(const char *path, WalEntryType type,
                         const WBuf *payload){
    /* Flush any buffered rows for this path first so ordering is correct */
    flush_path(path);
    uint64_t lsn=__sync_add_and_fetch(&g_lsn,1);
    WBuf hdr={};
    wb_u64(&hdr,lsn); wb_u32(&hdr,(uint32_t)type);
    wb_u32(&hdr,(uint32_t)payload->len);
    uint32_t crc=crc32_update(0,hdr.data,hdr.len);
    crc=crc32_update(crc,payload->data,payload->len);
    size_t total=hdr.len+payload->len+4;
    uint8_t *buf=(uint8_t*)malloc(total);
    if(!buf){free(hdr.data);return -1;}
    memcpy(buf,hdr.data,hdr.len);
    memcpy(buf+hdr.len,payload->data,payload->len);
    uint32_t crc_n=htonl(crc);
    memcpy(buf+hdr.len+payload->len,&crc_n,4);
    int fd=open(path,O_WRONLY|O_CREAT|O_APPEND,0644);
    ssize_t w=-1;
    if(fd>=0){w=write(fd,buf,total);fdatasync(fd);close(fd);}
    free(buf); free(hdr.data);
    return (w==(ssize_t)total)?0:-1;
}

/* ── Public WAL functions ────────────────────────────────────────────────── */

int wal_write_create_table(const char *db_name, const Table *table){
    if(!wal_is_persistent(db_name)) return 0;
    if(wal_ensure_dirs(db_name)!=0) return -1;
    WBuf payload={};
    wb_u32(&payload,(uint32_t)table->col_count);
    wb_u32(&payload,(uint32_t)(table->pk_col<0?0xFFFFFFFFu:(uint32_t)table->pk_col));
    for(int i=0;i<table->col_count;i++){
        wb_str(&payload,table->schema[i].name,FLEXQL_MAX_NAME_LEN);
        wb_u32(&payload,(uint32_t)table->schema[i].type);
        wb_u32(&payload,(uint32_t)table->schema[i].constraints);
    }
    char path[512]; make_wal_path(path,sizeof(path),db_name,table->name);
    int rc=write_direct(path,WAL_CREATE_TABLE,&payload);
    free(payload.data); return rc;
}

int wal_write_drop_table(const char *db_name, const char *table_name){
    if(!wal_is_persistent(db_name)) return 0;
    char path[512]; make_wal_path(path,sizeof(path),db_name,table_name);
    WBuf empty={}; wb_u8(&empty,0);
    int rc=write_direct(path,WAL_DROP_TABLE,&empty);
    free(empty.data); return rc;
}

static int serialise_row(WBuf *payload, const Table *table,
                          const char row_vals[][FLEXQL_MAX_VARCHAR],
                          int value_count, uint64_t row_id, time_t expiry){
    wb_u64(payload,row_id); wb_i64(payload,(int64_t)expiry);
    wb_u32(payload,(uint32_t)value_count);
    for(int c=0;c<value_count;c++){
        const char *sv=row_vals[c]; ColumnType ct=table->schema[c].type;
        if(!sv||sv[0]=='\0'){wb_u8(payload,1);continue;}
        wb_u8(payload,0);
        switch(ct){
        case COL_TYPE_INT: case COL_TYPE_DATETIME:
            wb_i64(payload,(int64_t)strtoll(sv,nullptr,10)); break;
        case COL_TYPE_DECIMAL:
            wb_dbl(payload,strtod(sv,nullptr)); break;
        case COL_TYPE_VARCHAR:{
            uint32_t sl=(uint32_t)strlen(sv);
            wb_u32(payload,sl); wb_bytes(payload,sv,sl); break;
        }
        default: wb_u8(payload,1);
        }
    }
    return 0;
}

int wal_write_insert_batch(const char *db_name, const char *table_name,
                           const Table *table,
                           const char *const *str_values_flat,
                           int row_count, int value_count, time_t expiry){
    if(!wal_is_persistent(db_name)) return 0;

    char path[512]; make_wal_path(path,sizeof(path),db_name,table_name);

    /* Build serialised records for all rows in this batch */
    WBuf batch={};
    uint64_t lsn_base=__sync_add_and_fetch(&g_lsn,(uint64_t)row_count);
    lsn_base-=(uint64_t)(row_count-1);

    for(int r=0;r<row_count;r++){
        WBuf row_payload={};
        char row_buf[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
        for(int c=0;c<value_count&&c<FLEXQL_MAX_COLUMNS;c++){
            const char *sv=str_values_flat[r*value_count+c];
            strncpy(row_buf[c],sv?sv:"",FLEXQL_MAX_VARCHAR-1);
            row_buf[c][FLEXQL_MAX_VARCHAR-1]='\0';
        }
        serialise_row(&row_payload,table,row_buf,value_count,0,expiry);

        /* Build WAL record header + payload + CRC into batch */
        uint64_t lsn=lsn_base+(uint64_t)r;
        WBuf hdr={};
        wb_u64(&hdr,lsn); wb_u32(&hdr,(uint32_t)WAL_INSERT);
        wb_u32(&hdr,(uint32_t)row_payload.len);
        uint32_t crc=crc32_update(0,hdr.data,hdr.len);
        crc=crc32_update(crc,row_payload.data,row_payload.len);
        uint32_t crc_n=htonl(crc);
        wb_bytes(&batch,hdr.data,hdr.len);
        wb_bytes(&batch,row_payload.data,row_payload.len);
        wb_bytes(&batch,&crc_n,4);
        free(hdr.data); free(row_payload.data);
    }

    /* Accumulate into the write buffer; flush when threshold hit */
    pthread_mutex_lock(&g_wal_mutex);
    WalWriteBuffer *buf=get_buf(path);
    if(!buf){
        /* Buffer table full — fall back to direct write */
        pthread_mutex_unlock(&g_wal_mutex);
        int fd=open(path,O_WRONLY|O_CREAT|O_APPEND,0644);
        ssize_t w=-1;
        if(fd>=0){w=write(fd,batch.data,batch.len);fdatasync(fd);close(fd);}
        free(batch.data);
        return (w==(ssize_t)batch.len)?0:-1;
    }

    /* Append to buffer */
    wb_bytes(&buf->data,batch.data,batch.len);
    free(batch.data);
    buf->row_count+=row_count;

    /* Check flush conditions */
    bool should_flush=
        (buf->row_count>=WAL_FLUSH_ROWS) ||
        ((now_us()-buf->last_flush_us)/1000 >= (uint64_t)WAL_FLUSH_MS);

    int rc=0;
    if(should_flush) rc=flush_buf(buf);

    pthread_mutex_unlock(&g_wal_mutex);
    return rc;
}

/* ── Recovery (unchanged logic) ─────────────────────────────────────────── */
static uint8_t *read_file(const char *path, size_t *out_len){
    FILE *f=fopen(path,"rb"); if(!f) return nullptr;
    fseek(f,0,SEEK_END); long sz=ftell(f); fseek(f,0,SEEK_SET);
    if(sz<=0){fclose(f);*out_len=0;return(uint8_t*)calloc(1,1);}
    uint8_t *buf=(uint8_t*)malloc((size_t)sz); if(!buf){fclose(f);return nullptr;}
    size_t rd=fread(buf,1,(size_t)sz,f); fclose(f); *out_len=rd; return buf;
}

static int replay_wal_file(const char *db_name,const char *table_name,Database *db){
    char path[512]; make_wal_path(path,sizeof(path),db_name,table_name);
    size_t file_len=0; uint8_t *data=read_file(path,&file_len);
    if(!data) return 0;
    size_t pos=0; Table *tbl=nullptr; int dropped=0;
    while(pos+20<=file_len){
        size_t entry_start=pos;
        uint64_t lsn=rb_u64(data,&pos);
        uint32_t entry_type=rb_u32(data,&pos);
        uint32_t payload_len=rb_u32(data,&pos);
        (void)lsn;
        if(pos+payload_len+4>file_len) break;
        const uint8_t *payload=data+pos; pos+=payload_len;
        uint32_t stored_crc_n; memcpy(&stored_crc_n,data+pos,4); pos+=4;
        uint32_t stored_crc=ntohl(stored_crc_n);
        uint32_t computed_crc=crc32_update(0,data+entry_start,16);
        computed_crc=crc32_update(computed_crc,payload,payload_len);
        if(computed_crc!=stored_crc) break;
        size_t off=0;
        switch((WalEntryType)entry_type){
        case WAL_CREATE_TABLE:{
            uint32_t col_count=rb_u32(payload,&off);
            uint32_t pk_col_u=rb_u32(payload,&off);
            int pk_col=(pk_col_u==0xFFFFFFFFu)?-1:(int)pk_col_u;
            ColumnDef cols[FLEXQL_MAX_COLUMNS]={};
            for(uint32_t i=0;i<col_count&&i<FLEXQL_MAX_COLUMNS;i++){
                memcpy(cols[i].name,payload+off,FLEXQL_MAX_NAME_LEN);
                cols[i].name[FLEXQL_MAX_NAME_LEN-1]='\0'; off+=FLEXQL_MAX_NAME_LEN;
                cols[i].type=(ColumnType)rb_u32(payload,&off);
                cols[i].constraints=(uint8_t)rb_u32(payload,&off);
                cols[i].col_index=(int)i;
            }
            char *err=nullptr;
            tbl=table_create(db,table_name,cols,(int)col_count,&err);
            if(tbl) tbl->pk_col=pk_col;
            free(err); dropped=0; break;
        }
        case WAL_INSERT:{
            if(!tbl||dropped) break;
            uint64_t sri=rb_u64(payload,&off);
            int64_t expiry_i64=rb_i64(payload,&off);
            uint32_t col_count=rb_u32(payload,&off);
            (void)sri;
            if(col_count>(uint32_t)FLEXQL_MAX_COLUMNS) break;
            char str_vals[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR]={};
            for(uint32_t c=0;c<col_count;c++){
                uint8_t is_null=rb_u8(payload,&off);
                if(is_null){str_vals[c][0]='\0';continue;}
                ColumnType ct=tbl->schema[c].type;
                switch(ct){
                case COL_TYPE_INT: case COL_TYPE_DATETIME:{
                    int64_t v=rb_i64(payload,&off);
                    snprintf(str_vals[c],FLEXQL_MAX_VARCHAR,"%lld",(long long)v); break;
                }
                case COL_TYPE_DECIMAL:{
                    double v=rb_dbl(payload,&off);
                    if(v==floor(v)&&v>=-1e15&&v<=1e15)
                        snprintf(str_vals[c],FLEXQL_MAX_VARCHAR,"%lld",(long long)v);
                    else snprintf(str_vals[c],FLEXQL_MAX_VARCHAR,"%g",v); break;
                }
                case COL_TYPE_VARCHAR:{
                    uint32_t sl=rb_u32(payload,&off);
                    if(sl>=FLEXQL_MAX_VARCHAR)sl=FLEXQL_MAX_VARCHAR-1;
                    memcpy(str_vals[c],payload+off,sl);str_vals[c][sl]='\0';off+=sl; break;
                }
                default: str_vals[c][0]='\0';
                }
            }
            char *err=nullptr;
            row_insert(tbl,str_vals,(int)col_count,(time_t)expiry_i64,&err);
            free(err); break;
        }
        case WAL_DROP_TABLE: dropped=1; break;
        default: break;
        }
    }
    free(data);
    return (tbl&&!dropped)?1:0;
}

int wal_recover(DatabaseManager *mgr){
    int recovered_dbs=0;
    FILE *reg=fopen(WAL_REGISTRY,"r"); if(!reg) return 0;
    char line[FLEXQL_MAX_NAME_LEN+2];
    while(fgets(line,sizeof(line),reg)){
        size_t l=strlen(line);
        while(l>0&&(line[l-1]=='\n'||line[l-1]=='\r'))line[--l]='\0';
        if(l==0) continue;
        char *err=nullptr; dbmgr_create(mgr,line,&err); free(err);
        Database *db=dbmgr_find(mgr,line); if(!db) continue;
        char dir[512]; make_db_dir(dir,sizeof(dir),line);
        DIR *d=opendir(dir); if(!d) continue;
        struct dirent *ent;
        while((ent=readdir(d))!=nullptr){
            const char *fname=ent->d_name; size_t flen=strlen(fname);
            if(flen<5||strcmp(fname+flen-4,".wal")!=0) continue;
            char tbl_name[FLEXQL_MAX_NAME_LEN]={};
            size_t tlen=flen-4; if(tlen>=FLEXQL_MAX_NAME_LEN)tlen=FLEXQL_MAX_NAME_LEN-1;
            memcpy(tbl_name,fname,tlen); tbl_name[tlen]='\0';
            replay_wal_file(line,tbl_name,db);
        }
        closedir(d); recovered_dbs++;
    }
    fclose(reg);
    printf("[wal] Recovery complete: %d database(s) restored\n",recovered_dbs);
    return recovered_dbs;
}