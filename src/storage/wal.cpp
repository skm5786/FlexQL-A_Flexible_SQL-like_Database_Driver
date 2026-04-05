/**
 * wal.cpp  —  Write-Ahead Log  (Optimized)
 *
 * SPEED CHANGES:
 *
 * 1. WAL_FLUSH_ROWS raised from 200 → 5000
 *    At 200: 10M rows = 50,000 fsyncs × ~1ms = ~50s just in fsync overhead
 *    At 5000: 10M rows = 2,000 fsyncs × ~1ms = ~2s in fsync overhead
 *
 * 2. WAL_FLUSH_MS raised from 50ms → 200ms
 *    Prevents time-based flushes from firing too often during fast inserts.
 *
 * 3. WAL file opened with O_WRONLY|O_CREAT|O_APPEND (unchanged) but
 *    fdatasync is now only called in flush_buf(), never in the hot path.
 *
 * 4. Per-path buffer slot count raised from 256 → 512 to handle many
 *    concurrent connections each with multiple tables.
 *
 * 5. wal_is_persistent: BENCH_* names are persistent, _SESSION_* are not.
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

/* ── Tuning ──────────────────────────────────────────────────────────────── */
#define WAL_FLUSH_ROWS   5000    /* flush after this many buffered rows      */
#define WAL_FLUSH_MS      200    /* flush after this many ms since last flush */
#define WAL_BUF_SLOTS     512   /* max concurrent open WAL file buffers      */

/* ── CRC32 ───────────────────────────────────────────────────────────────── */
static uint32_t crc32_table[256];
static int crc32_ready = 0;
static void crc32_init(void){
    if(crc32_ready)return;
    for(uint32_t i=0;i<256;i++){uint32_t c=i;for(int j=0;j<8;j++)c=(c&1)?(0xEDB88320u^(c>>1)):(c>>1);crc32_table[i]=c;}
    crc32_ready=1;
}
static uint32_t crc32_update(uint32_t crc,const void*data,size_t len){
    crc32_init();const uint8_t*p=(const uint8_t*)data;crc^=0xFFFFFFFFu;
    for(size_t i=0;i<len;i++)crc=crc32_table[(crc^p[i])&0xFF]^(crc>>8);
    return crc^0xFFFFFFFFu;
}

/* ── WBuf ────────────────────────────────────────────────────────────────── */
typedef struct{uint8_t*data;size_t len;size_t cap;}WBuf;
static int wbuf_grow(WBuf*b,size_t n){if(b->len+n<=b->cap)return 0;size_t nc=b->cap?b->cap*2:65536;while(nc<b->len+n)nc*=2;uint8_t*nd=(uint8_t*)realloc(b->data,nc);if(!nd)return -1;b->data=nd;b->cap=nc;return 0;}
static int wb_u8(WBuf*b,uint8_t v){if(wbuf_grow(b,1))return -1;b->data[b->len++]=v;return 0;}
static int wb_u32(WBuf*b,uint32_t v){if(wbuf_grow(b,4))return -1;uint32_t n=htonl(v);memcpy(b->data+b->len,&n,4);b->len+=4;return 0;}
static int wb_u64(WBuf*b,uint64_t v){if(wbuf_grow(b,8))return -1;uint32_t hi=htonl((uint32_t)(v>>32)),lo=htonl((uint32_t)(v&0xFFFFFFFFu));memcpy(b->data+b->len,&hi,4);memcpy(b->data+b->len+4,&lo,4);b->len+=8;return 0;}
static int wb_i64(WBuf*b,int64_t v){return wb_u64(b,(uint64_t)v);}
static int wb_dbl(WBuf*b,double v){uint64_t bits;memcpy(&bits,&v,8);return wb_u64(b,bits);}
static int wb_bytes(WBuf*b,const void*d,size_t n){if(wbuf_grow(b,n))return -1;memcpy(b->data+b->len,d,n);b->len+=n;return 0;}
static int wb_str(WBuf*b,const char*s,size_t fl){if(wbuf_grow(b,fl))return -1;size_t sl=s?strlen(s):0;if(sl>fl)sl=fl;if(sl)memcpy(b->data+b->len,s,sl);if(sl<fl)memset(b->data+b->len+sl,0,fl-sl);b->len+=fl;return 0;}

static uint8_t  rb_u8(const uint8_t*p,size_t*o){return p[(*o)++];}
static uint32_t rb_u32(const uint8_t*p,size_t*o){uint32_t n;memcpy(&n,p+*o,4);*o+=4;return ntohl(n);}
static uint64_t rb_u64(const uint8_t*p,size_t*o){uint32_t hi,lo;memcpy(&hi,p+*o,4);memcpy(&lo,p+*o+4,4);*o+=8;return((uint64_t)ntohl(hi)<<32)|ntohl(lo);}
static int64_t  rb_i64(const uint8_t*p,size_t*o){return(int64_t)rb_u64(p,o);}
static double   rb_dbl(const uint8_t*p,size_t*o){uint64_t bits=rb_u64(p,o);double v;memcpy(&v,&bits,8);return v;}

/* ── Path helpers ────────────────────────────────────────────────────────── */
static void make_wal_path(char*out,size_t sz,const char*db,const char*tbl){snprintf(out,sz,"%s/%s/%s.wal",WAL_DATA_DIR,db,tbl);}
static void make_db_dir(char*out,size_t sz,const char*db){snprintf(out,sz,"%s/%s",WAL_DATA_DIR,db);}

int wal_is_persistent(const char*db_name){
    if(!db_name)return 0;
    return(strncmp(db_name,"_SESSION_",9)!=0);
}

int wal_ensure_dirs(const char*db_name){
    mkdir(WAL_DATA_DIR,0755);
    char dir[512];make_db_dir(dir,sizeof(dir),db_name);
    if(mkdir(dir,0755)<0&&errno!=EEXIST)return -1;
    return 0;
}

/* ── Idempotent registry ─────────────────────────────────────────────────── */
int wal_register_db(const char*db_name){
    if(!wal_is_persistent(db_name))return 0;
    mkdir(WAL_DATA_DIR,0755);
    char existing[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    int n=0;
    FILE*f=fopen(WAL_REGISTRY,"r");
    if(f){char line[FLEXQL_MAX_NAME_LEN+2];
        while(fgets(line,sizeof(line),f)&&n<FLEXQL_MAX_DATABASES){
            size_t l=strlen(line);while(l>0&&(line[l-1]=='\n'||line[l-1]=='\r'))line[--l]='\0';
            if(l==0)continue;strncpy(existing[n++],line,FLEXQL_MAX_NAME_LEN-1);}fclose(f);}
    for(int i=0;i<n;i++)if(strcasecmp(existing[i],db_name)==0)return 0;
    f=fopen(WAL_REGISTRY,"a");if(!f)return -1;
    fprintf(f,"%s\n",db_name);fflush(f);fsync(fileno(f));fclose(f);return 0;
}

int wal_unregister_db(const char*db_name){
    if(!wal_is_persistent(db_name))return 0;
    FILE*f=fopen(WAL_REGISTRY,"r");if(!f)return 0;
    char lines[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];int n=0;
    char line[FLEXQL_MAX_NAME_LEN+2];
    while(fgets(line,sizeof(line),f)&&n<FLEXQL_MAX_DATABASES){
        size_t l=strlen(line);while(l>0&&(line[l-1]=='\n'||line[l-1]=='\r'))line[--l]='\0';
        if(l==0)continue;if(strcasecmp(line,db_name)!=0)strncpy(lines[n++],line,FLEXQL_MAX_NAME_LEN-1);}
    fclose(f);
    f=fopen(WAL_REGISTRY,"w");if(!f)return -1;
    for(int i=0;i<n;i++)fprintf(f,"%s\n",lines[i]);
    fflush(f);fsync(fileno(f));fclose(f);return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * BUFFERED WAL WRITE LAYER
 * ══════════════════════════════════════════════════════════════════════════ */
struct WalWriteBuffer {
    char     path[512];
    WBuf     data;
    int      row_count;
    uint64_t last_flush_us;
};

static WalWriteBuffer g_wal_bufs[WAL_BUF_SLOTS];
static pthread_mutex_t g_wal_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_wal_init = 0;
static uint64_t g_lsn = 0;

static uint64_t now_us(void){struct timeval tv;gettimeofday(&tv,nullptr);return(uint64_t)tv.tv_sec*1000000ULL+(uint64_t)tv.tv_usec;}

static void wal_bufs_init(void){if(g_wal_init)return;memset(g_wal_bufs,0,sizeof(g_wal_bufs));g_wal_init=1;}

static WalWriteBuffer*get_buf(const char*path){
    wal_bufs_init();
    for(int i=0;i<WAL_BUF_SLOTS;i++)if(g_wal_bufs[i].path[0]&&strcmp(g_wal_bufs[i].path,path)==0)return&g_wal_bufs[i];
    for(int i=0;i<WAL_BUF_SLOTS;i++){if(!g_wal_bufs[i].path[0]){strncpy(g_wal_bufs[i].path,path,sizeof(g_wal_bufs[i].path)-1);g_wal_bufs[i].row_count=0;g_wal_bufs[i].last_flush_us=now_us();return&g_wal_bufs[i];}}
    return nullptr;
}

static int flush_buf(WalWriteBuffer*b){
    if(!b||b->data.len==0){if(b){b->row_count=0;b->last_flush_us=now_us();}return 0;}
    int fd=open(b->path,O_WRONLY|O_CREAT|O_APPEND,0644);
    int rc=-1;
    if(fd>=0){ssize_t w=write(fd,b->data.data,b->data.len);fdatasync(fd);close(fd);rc=(w==(ssize_t)b->data.len)?0:-1;}
    b->data.len=0;b->row_count=0;b->last_flush_us=now_us();
    return rc;
}

void wal_flush_all(void){
    pthread_mutex_lock(&g_wal_mutex);
    for(int i=0;i<WAL_BUF_SLOTS;i++)if(g_wal_bufs[i].path[0])flush_buf(&g_wal_bufs[i]);
    pthread_mutex_unlock(&g_wal_mutex);
}

static void flush_and_free_path(const char*path){
    pthread_mutex_lock(&g_wal_mutex);
    for(int i=0;i<WAL_BUF_SLOTS;i++){
        if(g_wal_bufs[i].path[0]&&strcmp(g_wal_bufs[i].path,path)==0){
            flush_buf(&g_wal_bufs[i]);
            g_wal_bufs[i].path[0]='\0';
            free(g_wal_bufs[i].data.data);
            g_wal_bufs[i].data={};
            break;
        }
    }
    pthread_mutex_unlock(&g_wal_mutex);
}

/* ── Direct write (for CREATE TABLE, DROP TABLE — infrequent) ───────────── */
static int write_direct(const char*path,WalEntryType type,const WBuf*payload){
    flush_and_free_path(path);  /* flush any buffered rows first */
    uint64_t lsn=__sync_add_and_fetch(&g_lsn,1);
    WBuf hdr={};wb_u64(&hdr,lsn);wb_u32(&hdr,(uint32_t)type);wb_u32(&hdr,(uint32_t)payload->len);
    uint32_t crc=crc32_update(0,hdr.data,hdr.len);crc=crc32_update(crc,payload->data,payload->len);
    size_t total=hdr.len+payload->len+4;uint8_t*buf=(uint8_t*)malloc(total);
    if(!buf){free(hdr.data);return -1;}
    memcpy(buf,hdr.data,hdr.len);memcpy(buf+hdr.len,payload->data,payload->len);
    uint32_t cn=htonl(crc);memcpy(buf+hdr.len+payload->len,&cn,4);
    int fd=open(path,O_WRONLY|O_CREAT|O_APPEND,0644);ssize_t w=-1;
    if(fd>=0){w=write(fd,buf,total);fdatasync(fd);close(fd);}
    free(buf);free(hdr.data);return(w==(ssize_t)total)?0:-1;
}

/* ── Public WAL functions ────────────────────────────────────────────────── */
int wal_write_create_table(const char*db_name,const Table*table){
    if(!wal_is_persistent(db_name))return 0;
    if(wal_ensure_dirs(db_name)!=0)return -1;
    WBuf payload={};
    wb_u32(&payload,(uint32_t)table->col_count);
    wb_u32(&payload,(uint32_t)(table->pk_col<0?0xFFFFFFFFu:(uint32_t)table->pk_col));
    for(int i=0;i<table->col_count;i++){wb_str(&payload,table->schema[i].name,FLEXQL_MAX_NAME_LEN);wb_u32(&payload,(uint32_t)table->schema[i].type);wb_u32(&payload,(uint32_t)table->schema[i].constraints);}
    char path[512];make_wal_path(path,sizeof(path),db_name,table->name);
    int rc=write_direct(path,WAL_CREATE_TABLE,&payload);free(payload.data);return rc;
}

int wal_write_drop_table(const char*db_name,const char*table_name){
    if(!wal_is_persistent(db_name))return 0;
    char path[512];make_wal_path(path,sizeof(path),db_name,table_name);
    WBuf empty={};wb_u8(&empty,0);
    int rc=write_direct(path,WAL_DROP_TABLE,&empty);free(empty.data);return rc;
}

static int serialise_row(WBuf*payload,const Table*table,const char rv[][FLEXQL_MAX_VARCHAR],int vc,uint64_t rid,time_t expiry){
    wb_u64(payload,rid);wb_i64(payload,(int64_t)expiry);wb_u32(payload,(uint32_t)vc);
    for(int c=0;c<vc;c++){const char*sv=rv[c];ColumnType ct=table->schema[c].type;
        if(!sv||sv[0]=='\0'){wb_u8(payload,1);continue;}wb_u8(payload,0);
        switch(ct){case COL_TYPE_INT:case COL_TYPE_DATETIME:wb_i64(payload,(int64_t)strtoll(sv,nullptr,10));break;
            case COL_TYPE_DECIMAL:wb_dbl(payload,strtod(sv,nullptr));break;
            case COL_TYPE_VARCHAR:{uint32_t sl=(uint32_t)strlen(sv);wb_u32(payload,sl);wb_bytes(payload,sv,sl);break;}
            default:wb_u8(payload,1);}}
    return 0;
}

int wal_write_insert_batch(const char*db_name,const char*table_name,const Table*table,
                           const char*const*str_values_flat,int row_count,int value_count,time_t expiry){
    if(!wal_is_persistent(db_name))return 0;
    char path[512];make_wal_path(path,sizeof(path),db_name,table_name);

    /* Serialise all rows into one WBuf */
    WBuf batch={};
    uint64_t lsn_base=__sync_add_and_fetch(&g_lsn,(uint64_t)row_count);
    lsn_base-=(uint64_t)(row_count-1);

    for(int r=0;r<row_count;r++){
        WBuf rp={};
        char rb[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
        for(int c=0;c<value_count&&c<FLEXQL_MAX_COLUMNS;c++){const char*sv=str_values_flat[r*value_count+c];strncpy(rb[c],sv?sv:"",FLEXQL_MAX_VARCHAR-1);rb[c][FLEXQL_MAX_VARCHAR-1]='\0';}
        serialise_row(&rp,table,rb,value_count,0,expiry);
        uint64_t lsn=lsn_base+(uint64_t)r;
        WBuf hdr={};wb_u64(&hdr,lsn);wb_u32(&hdr,(uint32_t)WAL_INSERT);wb_u32(&hdr,(uint32_t)rp.len);
        uint32_t crc=crc32_update(0,hdr.data,hdr.len);crc=crc32_update(crc,rp.data,rp.len);
        uint32_t cn=htonl(crc);
        wb_bytes(&batch,hdr.data,hdr.len);wb_bytes(&batch,rp.data,rp.len);wb_bytes(&batch,&cn,4);
        free(hdr.data);free(rp.data);
    }

    /* Append to buffer, flush if threshold reached */
    pthread_mutex_lock(&g_wal_mutex);
    WalWriteBuffer*buf=get_buf(path);
    if(!buf){
        /* Buffer table full — direct write */
        pthread_mutex_unlock(&g_wal_mutex);
        int fd=open(path,O_WRONLY|O_CREAT|O_APPEND,0644);ssize_t w=-1;
        if(fd>=0){w=write(fd,batch.data,batch.len);fdatasync(fd);close(fd);}
        free(batch.data);return(w==(ssize_t)batch.len)?0:-1;
    }

    wb_bytes(&buf->data,batch.data,batch.len);
    free(batch.data);
    buf->row_count+=row_count;

    bool flush=(buf->row_count>=WAL_FLUSH_ROWS)||
               ((now_us()-buf->last_flush_us)/1000>=(uint64_t)WAL_FLUSH_MS);
    int rc=0;
    if(flush)rc=flush_buf(buf);
    pthread_mutex_unlock(&g_wal_mutex);
    return rc;
}

/* ── Recovery ────────────────────────────────────────────────────────────── */
static uint8_t*read_file(const char*path,size_t*out){
    FILE*f=fopen(path,"rb");if(!f)return nullptr;
    fseek(f,0,SEEK_END);long sz=ftell(f);fseek(f,0,SEEK_SET);
    if(sz<=0){fclose(f);*out=0;return(uint8_t*)calloc(1,1);}
    uint8_t*buf=(uint8_t*)malloc((size_t)sz);if(!buf){fclose(f);return nullptr;}
    size_t rd=fread(buf,1,(size_t)sz,f);fclose(f);*out=rd;return buf;
}

static int replay_wal_file(const char*db_name,const char*table_name,Database*db){
    char path[512];make_wal_path(path,sizeof(path),db_name,table_name);
    size_t flen=0;uint8_t*data=read_file(path,&flen);if(!data)return 0;
    size_t pos=0;Table*tbl=nullptr;int dropped=0;
    while(pos+20<=flen){
        size_t es=pos;
        uint64_t lsn=rb_u64(data,&pos);uint32_t et=rb_u32(data,&pos);uint32_t pl=rb_u32(data,&pos);(void)lsn;
        if(pos+pl+4>flen)break;
        const uint8_t*payload=data+pos;pos+=pl;
        uint32_t scn;memcpy(&scn,data+pos,4);pos+=4;
        uint32_t stored=ntohl(scn);
        uint32_t computed=crc32_update(0,data+es,16);computed=crc32_update(computed,payload,pl);
        if(computed!=stored)break;
        size_t off=0;
        switch((WalEntryType)et){
        case WAL_CREATE_TABLE:{
            uint32_t cc=rb_u32(payload,&off);uint32_t pku=rb_u32(payload,&off);
            int pk=(pku==0xFFFFFFFFu)?-1:(int)pku;
            ColumnDef cols[FLEXQL_MAX_COLUMNS]={};
            for(uint32_t i=0;i<cc&&i<(uint32_t)FLEXQL_MAX_COLUMNS;i++){
                memcpy(cols[i].name,payload+off,FLEXQL_MAX_NAME_LEN);cols[i].name[FLEXQL_MAX_NAME_LEN-1]='\0';off+=FLEXQL_MAX_NAME_LEN;
                cols[i].type=(ColumnType)rb_u32(payload,&off);cols[i].constraints=(uint8_t)rb_u32(payload,&off);cols[i].col_index=(int)i;}
            char*err=nullptr;tbl=table_create(db,table_name,cols,(int)cc,&err);if(tbl)tbl->pk_col=pk;free(err);dropped=0;break;}
        case WAL_INSERT:{
            if(!tbl||dropped)break;
            uint64_t sri=rb_u64(payload,&off);int64_t exp=rb_i64(payload,&off);uint32_t cc=rb_u32(payload,&off);(void)sri;
            if(cc>(uint32_t)FLEXQL_MAX_COLUMNS)break;
            char sv[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR]={};
            for(uint32_t c=0;c<cc;c++){uint8_t isnull=rb_u8(payload,&off);if(isnull){sv[c][0]='\0';continue;}
                ColumnType ct=tbl->schema[c].type;
                switch(ct){case COL_TYPE_INT:case COL_TYPE_DATETIME:{int64_t v=rb_i64(payload,&off);snprintf(sv[c],FLEXQL_MAX_VARCHAR,"%lld",(long long)v);break;}
                    case COL_TYPE_DECIMAL:{double v=rb_dbl(payload,&off);if(v==floor(v)&&v>=-1e15&&v<=1e15)snprintf(sv[c],FLEXQL_MAX_VARCHAR,"%lld",(long long)v);else snprintf(sv[c],FLEXQL_MAX_VARCHAR,"%g",v);break;}
                    case COL_TYPE_VARCHAR:{uint32_t sl=rb_u32(payload,&off);if(sl>=FLEXQL_MAX_VARCHAR)sl=FLEXQL_MAX_VARCHAR-1;memcpy(sv[c],payload+off,sl);sv[c][sl]='\0';off+=sl;break;}
                    default:sv[c][0]='\0';}}
            char*err=nullptr;row_insert(tbl,sv,(int)cc,(time_t)exp,&err);free(err);break;}
        case WAL_DROP_TABLE:dropped=1;break;
        default:break;}
    }
    free(data);return(tbl&&!dropped)?1:0;
}

int wal_recover(DatabaseManager*mgr){
    int n=0;FILE*reg=fopen(WAL_REGISTRY,"r");if(!reg)return 0;
    char line[FLEXQL_MAX_NAME_LEN+2];
    while(fgets(line,sizeof(line),reg)){
        size_t l=strlen(line);while(l>0&&(line[l-1]=='\n'||line[l-1]=='\r'))line[--l]='\0';if(l==0)continue;
        char*err=nullptr;dbmgr_create(mgr,line,&err);free(err);
        Database*db=dbmgr_find(mgr,line);if(!db)continue;
        char dir[512];make_db_dir(dir,sizeof(dir),line);
        DIR*d=opendir(dir);if(!d)continue;
        struct dirent*ent;
        while((ent=readdir(d))!=nullptr){const char*fn=ent->d_name;size_t fl=strlen(fn);if(fl<5||strcmp(fn+fl-4,".wal")!=0)continue;
            char tn[FLEXQL_MAX_NAME_LEN]={};size_t tl=fl-4;if(tl>=FLEXQL_MAX_NAME_LEN)tl=FLEXQL_MAX_NAME_LEN-1;memcpy(tn,fn,tl);tn[tl]='\0';
            replay_wal_file(line,tn,db);}
        closedir(d);n++;}
    fclose(reg);printf("[wal] Recovery complete: %d database(s) restored\n",n);return n;
}