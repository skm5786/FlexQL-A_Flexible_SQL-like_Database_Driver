/**
 * wal.cpp  —  Write-Ahead Log Implementation  (Lesson 8)
 *
 * Sections:
 *   A. CRC32 computation
 *   B. Binary serialisation helpers
 *   C. WAL file path construction
 *   D. Registry (database-level persistence)
 *   E. WAL write functions (create_table, insert_batch, drop_table)
 *   F. WAL recovery (startup replay)
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

#include "storage/wal.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION A — CRC32
 *
 * LESSON: A Cyclic Redundancy Check detects corrupted or truncated data.
 * If the server crashes mid-write, the last WAL entry may be partial.
 * We store CRC32 at the end of each entry; on recovery, if the computed
 * CRC of the data doesn't match the stored CRC, we discard that entry
 * (and everything after it) — it was never fully written.
 *
 * We use the standard IEEE 802.3 polynomial 0xEDB88320 (bit-reversed form).
 * This is the same polynomial used by zlib, gzip, and Ethernet frames.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
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

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION B — BINARY SERIALISATION HELPERS
 *
 * LESSON: All multi-byte integers are stored big-endian (network byte order)
 * so the WAL files are readable on any CPU architecture.
 * htonl() = host-to-network-long = convert to big-endian.
 * ntohl() = network-to-host-long = convert from big-endian.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#include <arpa/inet.h>  /* htonl, ntohl */

/* Dynamic write buffer — grows as needed */
typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} WBuf;

static int wbuf_grow(WBuf *b, size_t need) {
    if (b->len + need <= b->cap) return 0;
    size_t new_cap = b->cap ? b->cap * 2 : 4096;
    while (new_cap < b->len + need) new_cap *= 2;
    uint8_t *nd = (uint8_t*)realloc(b->data, new_cap);
    if (!nd) return -1;
    b->data = nd; b->cap = new_cap; return 0;
}
static int wb_u8(WBuf *b, uint8_t v) {
    if (wbuf_grow(b,1)) return -1;
    b->data[b->len++] = v; return 0;
}
static int wb_u32(WBuf *b, uint32_t v) {
    if (wbuf_grow(b,4)) return -1;
    uint32_t n = htonl(v);
    memcpy(b->data+b->len, &n, 4); b->len+=4; return 0;
}
static int wb_u64(WBuf *b, uint64_t v) {
    if (wbuf_grow(b,8)) return -1;
    /* Write as two 32-bit halves in big-endian order */
    uint32_t hi = htonl((uint32_t)(v >> 32));
    uint32_t lo = htonl((uint32_t)(v & 0xFFFFFFFFu));
    memcpy(b->data+b->len,   &hi, 4);
    memcpy(b->data+b->len+4, &lo, 4);
    b->len += 8; return 0;
}
static int wb_i64(WBuf *b, int64_t v) { return wb_u64(b,(uint64_t)v); }
static int wb_dbl(WBuf *b, double v) {
    uint64_t bits; memcpy(&bits, &v, 8); return wb_u64(b, bits);
}
static int wb_bytes(WBuf *b, const void *d, size_t n) {
    if (wbuf_grow(b,n)) return -1;
    memcpy(b->data+b->len, d, n); b->len+=n; return 0;
}
static int wb_str(WBuf *b, const char *s, size_t fixed_len) {
    /* Write fixed_len bytes: string + zero padding */
    if (wbuf_grow(b,fixed_len)) return -1;
    size_t slen = s ? strlen(s) : 0;
    if (slen > fixed_len) slen = fixed_len;
    if (slen) memcpy(b->data+b->len, s, slen);
    if (slen < fixed_len) memset(b->data+b->len+slen, 0, fixed_len-slen);
    b->len += fixed_len; return 0;
}

/* Read helpers */
static uint8_t  rb_u8(const uint8_t *p, size_t *off) {
    return p[(*off)++];
}
static uint32_t rb_u32(const uint8_t *p, size_t *off) {
    uint32_t n; memcpy(&n, p+*off, 4); *off+=4; return ntohl(n);
}
static uint64_t rb_u64(const uint8_t *p, size_t *off) {
    uint32_t hi,lo;
    memcpy(&hi,p+*off,4); memcpy(&lo,p+*off+4,4); *off+=8;
    return ((uint64_t)ntohl(hi)<<32) | ntohl(lo);
}
static int64_t  rb_i64(const uint8_t *p, size_t *off) { return (int64_t)rb_u64(p,off); }
static double   rb_dbl(const uint8_t *p, size_t *off) {
    uint64_t bits=rb_u64(p,off); double v; memcpy(&v,&bits,8); return v;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION C — FILE PATH HELPERS
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static void make_wal_path(char *out, size_t outsz,
                          const char *db_name, const char *table_name) {
    snprintf(out, outsz, "%s/%s/%s.wal", WAL_DATA_DIR, db_name, table_name);
}
static void make_db_dir(char *out, size_t outsz, const char *db_name) {
    snprintf(out, outsz, "%s/%s", WAL_DATA_DIR, db_name);
}

int wal_is_persistent(const char *db_name) {
    if (!db_name) return 0;
    /* Session databases are ephemeral — never persisted */
    return (strncmp(db_name, "_SESSION_", 9) != 0);
}

int wal_ensure_dirs(const char *db_name) {
    /* Create data/ if needed */
    mkdir(WAL_DATA_DIR, 0755);
    /* Create data/DBNAME/ if needed */
    char dir[512];
    make_db_dir(dir, sizeof(dir), db_name);
    if (mkdir(dir, 0755) < 0 && errno != EEXIST) return -1;
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION D — REGISTRY (database-level persistence)
 *
 * LESSON: The registry is a plain text file — one database name per line.
 * It is the first thing read on startup to know which databases existed.
 *
 * We use a simple text format (not binary) because:
 *   - There are at most 64 databases (FLEXQL_MAX_DATABASES)
 *   - Text is human-readable and easy to debug
 *   - We rewrite the whole file on every change (small and infrequent)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int wal_register_db(const char *db_name) {
    if (!wal_is_persistent(db_name)) return 0;
    mkdir(WAL_DATA_DIR, 0755);
    FILE *f = fopen(WAL_REGISTRY, "a");
    if (!f) return -1;
    fprintf(f, "%s\n", db_name);
    fflush(f); fsync(fileno(f));
    fclose(f);
    return 0;
}

int wal_unregister_db(const char *db_name) {
    if (!wal_is_persistent(db_name)) return 0;
    /* Read all lines, write back those that don't match */
    FILE *f = fopen(WAL_REGISTRY, "r");
    if (!f) return 0; /* registry may not exist yet */

    char lines[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    int  n = 0;
    char line[FLEXQL_MAX_NAME_LEN+2];
    while (fgets(line, sizeof(line), f) && n < FLEXQL_MAX_DATABASES) {
        /* Strip newline */
        size_t l = strlen(line);
        while (l > 0 && (line[l-1]=='\n'||line[l-1]=='\r')) line[--l]='\0';
        if (l == 0) continue;
        if (strcasecmp(line, db_name) != 0)
            strncpy(lines[n++], line, FLEXQL_MAX_NAME_LEN-1);
    }
    fclose(f);

    f = fopen(WAL_REGISTRY, "w");
    if (!f) return -1;
    for (int i = 0; i < n; i++) fprintf(f, "%s\n", lines[i]);
    fflush(f); fsync(fileno(f));
    fclose(f);
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION E — WAL WRITE FUNCTIONS
 *
 * LESSON: Every write follows this pattern:
 *   1. Serialise the entry into a WBuf (in-memory byte buffer).
 *   2. Compute CRC32 over the entry content.
 *   3. Build the 20-byte WAL header: LSN + type + payload_len.
 *   4. Open the WAL file in append mode (O_APPEND | O_CREAT).
 *   5. Write header + payload + CRC32 in ONE write() call.
 *      (One write = one kernel operation = cannot be torn between header
 *       and payload by a crash, which is why we batch them.)
 *   6. Call fdatasync() to ensure the data is on durable storage.
 *   7. Close the file.
 *
 * WHY append-only?
 *   Appending never modifies existing data — if a crash happens mid-append,
 *   we just have a partial entry at the tail of the file, which the CRC32
 *   check will reject on recovery.  Existing entries are untouched.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/* Global monotonic LSN — incremented for every WAL write */
static uint64_t g_lsn = 0;

/* Write one complete WAL record (header + payload + crc) to fd */
static int write_wal_record(int fd, WalEntryType type,
                             const WBuf *payload) {
    uint64_t lsn = __sync_add_and_fetch(&g_lsn, 1);

    /* Build 16-byte fixed header: LSN(8) + type(4) + payload_len(4) */
    WBuf hdr = {};
    wb_u64(&hdr, lsn);
    wb_u32(&hdr, (uint32_t)type);
    wb_u32(&hdr, (uint32_t)payload->len);

    /* CRC over header + payload */
    uint32_t crc = crc32_update(0, hdr.data, hdr.len);
    crc = crc32_update(crc, payload->data, payload->len);

    /* Write in one syscall: header + payload + crc(4) */
    size_t total = hdr.len + payload->len + 4;
    uint8_t *buf = (uint8_t*)malloc(total);
    if (!buf) { free(hdr.data); return -1; }

    memcpy(buf,              hdr.data,     hdr.len);
    memcpy(buf + hdr.len,    payload->data, payload->len);
    uint32_t crc_n = htonl(crc);
    memcpy(buf + hdr.len + payload->len, &crc_n, 4);

    ssize_t written = write(fd, buf, total);
    free(buf); free(hdr.data);
    return (written == (ssize_t)total) ? 0 : -1;
}

int wal_write_create_table(const char *db_name, const Table *table) {
    if (!wal_is_persistent(db_name)) return 0;
    if (wal_ensure_dirs(db_name) != 0) return -1;

    /* Serialise schema */
    WBuf payload = {};
    wb_u32(&payload, (uint32_t)table->col_count);
    wb_u32(&payload, (uint32_t)(table->pk_col < 0 ? 0xFFFFFFFFu
                                                   : (uint32_t)table->pk_col));
    for (int i = 0; i < table->col_count; i++) {
        wb_str(&payload, table->schema[i].name, FLEXQL_MAX_NAME_LEN);
        wb_u32(&payload, (uint32_t)table->schema[i].type);
        wb_u32(&payload, (uint32_t)table->schema[i].constraints);
    }

    char path[512];
    make_wal_path(path, sizeof(path), db_name, table->name);
    int fd = open(path, O_WRONLY|O_CREAT|O_APPEND, 0644);
    if (fd < 0) { free(payload.data); return -1; }

    int rc = write_wal_record(fd, WAL_CREATE_TABLE, &payload);
    fdatasync(fd);
    close(fd);
    free(payload.data);
    return rc;
}

/* Serialise one row's cell values (from string form) into the WBuf */
static int serialise_row(WBuf *payload, const Table *table,
                          const char row_vals[][FLEXQL_MAX_VARCHAR],
                          int value_count, uint64_t row_id, time_t expiry) {
    wb_u64(payload, row_id);
    wb_i64(payload, (int64_t)expiry);
    wb_u32(payload, (uint32_t)value_count);

    for (int c = 0; c < value_count; c++) {
        const char *sv = row_vals[c];
        ColumnType  ct = table->schema[c].type;

        if (!sv || sv[0] == '\0') {
            wb_u8(payload, 1); /* is_null = 1 */
            continue;
        }
        wb_u8(payload, 0); /* is_null = 0 */

        switch (ct) {
        case COL_TYPE_INT:
        case COL_TYPE_DATETIME: {
            int64_t v = (int64_t)strtoll(sv, nullptr, 10);
            wb_i64(payload, v);
            break;
        }
        case COL_TYPE_DECIMAL: {
            double v = strtod(sv, nullptr);
            wb_dbl(payload, v);
            break;
        }
        case COL_TYPE_VARCHAR: {
            uint32_t slen = (uint32_t)strlen(sv);
            wb_u32(payload, slen);
            wb_bytes(payload, sv, slen);
            break;
        }
        default:
            wb_u8(payload, 1); /* treat unknown as null */
        }
    }
    return 0;
}

int wal_write_insert_batch(const char *db_name, const char *table_name,
                           const Table *table,
                           const char *const *str_values_flat,
                           int row_count, int value_count, time_t expiry) {
    if (!wal_is_persistent(db_name)) return 0;

    /* Build one payload buffer containing ALL rows.
     * GROUP COMMIT: one write() + one fdatasync() for N rows.        */
    WBuf payload = {};
    /* We write row_count separate WAL_INSERT records packed together.
     * Each record has its own header so recovery can read them one by one. */

    char path[512];
    make_wal_path(path, sizeof(path), db_name, table_name);
    int fd = open(path, O_WRONLY|O_CREAT|O_APPEND, 0644);
    if (fd < 0) { free(payload.data); return -1; }

    /* Serialise all rows into one big buffer, then write + sync once */
    /* We concatenate all WAL records manually for one write() call */
    WBuf all = {};
    uint64_t lsn_base = __sync_add_and_fetch(&g_lsn, (uint64_t)row_count);
    lsn_base -= (uint64_t)(row_count - 1);

    for (int r = 0; r < row_count; r++) {
        /* Build payload for this row */
        WBuf row_payload = {};
        /* str_values_flat is a flat array: row r starts at index r*value_count */
        char row_buf[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
        for(int cc=0;cc<value_count&&cc<FLEXQL_MAX_COLUMNS;cc++){
            const char *sv=str_values_flat[r*value_count+cc];
            strncpy(row_buf[cc], sv?sv:"", FLEXQL_MAX_VARCHAR-1);
            row_buf[cc][FLEXQL_MAX_VARCHAR-1]='\0';
        }
        serialise_row(&row_payload, table, row_buf, value_count,
                      0 /* row_id assigned at recovery */, expiry);

        /* WAL record header */
        uint64_t lsn = lsn_base + (uint64_t)r;
        WBuf hdr = {};
        wb_u64(&hdr, lsn);
        wb_u32(&hdr, (uint32_t)WAL_INSERT);
        wb_u32(&hdr, (uint32_t)row_payload.len);

        uint32_t crc = crc32_update(0, hdr.data, hdr.len);
        crc = crc32_update(crc, row_payload.data, row_payload.len);
        uint32_t crc_n = htonl(crc);

        wb_bytes(&all, hdr.data,        hdr.len);
        wb_bytes(&all, row_payload.data, row_payload.len);
        wb_bytes(&all, &crc_n,          4);

        free(hdr.data);
        free(row_payload.data);
    }

    ssize_t written = write(fd, all.data, all.len);
    fdatasync(fd);
    close(fd);
    int rc = (written == (ssize_t)all.len) ? 0 : -1;
    free(all.data);
    free(payload.data);
    return rc;
}

int wal_write_drop_table(const char *db_name, const char *table_name) {
    if (!wal_is_persistent(db_name)) return 0;

    char path[512];
    make_wal_path(path, sizeof(path), db_name, table_name);
    int fd = open(path, O_WRONLY|O_CREAT|O_APPEND, 0644);
    if (fd < 0) return -1;

    WBuf empty = {};
    wb_u8(&empty, 0); /* placeholder so payload_len > 0 is not required */
    int rc = write_wal_record(fd, WAL_DROP_TABLE, &empty);
    fdatasync(fd);
    close(fd);
    free(empty.data);
    return rc;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SECTION F — RECOVERY
 *
 * LESSON: Recovery is the inverse of writing.
 *   For each WAL file: read entries, verify CRC32, replay the operation.
 *   At the end, the in-memory state is identical to what it was before
 *   the crash.
 *
 * CRC32 is the crash safety net:
 *   If the server crashes mid-write, the last entry's CRC will not match
 *   the computed CRC of the partial data.  We skip that entry and stop
 *   replaying.  All fully-written entries before it are valid.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/* Read entire file into a malloc'd buffer. Returns NULL on error. */
static uint8_t *read_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return nullptr;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz <= 0) { fclose(f); *out_len = 0; return (uint8_t*)calloc(1,1); }
    uint8_t *buf = (uint8_t*)malloc((size_t)sz);
    if (!buf) { fclose(f); return nullptr; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    *out_len = rd;
    return buf;
}

static int replay_wal_file(const char *db_name, const char *table_name,
                           Database *db) {
    char path[512];
    make_wal_path(path, sizeof(path), db_name, table_name);

    size_t   file_len = 0;
    uint8_t *data     = read_file(path, &file_len);
    if (!data) return 0; /* file may not exist yet */

    size_t  pos = 0;
    Table  *tbl = nullptr;
    int     dropped = 0;
    uint64_t next_row_id = 1;

    while (pos + 20 <= file_len) { /* 20 = 8+4+4+4 minimum record size */
        size_t entry_start = pos;

        /* Read header: LSN(8) + type(4) + payload_len(4) */
        uint64_t lsn         = rb_u64(data, &pos);
        uint32_t entry_type  = rb_u32(data, &pos);
        uint32_t payload_len = rb_u32(data, &pos);
        (void)lsn;

        if (pos + payload_len + 4 > file_len) {
            /* Truncated entry — stop here, earlier entries are good */
            break;
        }

        const uint8_t *payload = data + pos;
        pos += payload_len;

        /* Verify CRC */
        uint32_t stored_crc_n;
        memcpy(&stored_crc_n, data + pos, 4); pos += 4;
        uint32_t stored_crc = ntohl(stored_crc_n);

        uint32_t computed_crc = crc32_update(0, data + entry_start, 16); /* header */
        computed_crc = crc32_update(computed_crc, payload, payload_len);

        if (computed_crc != stored_crc) {
            /* Corrupt entry — stop replaying */
            break;
        }

        /* Replay the entry */
        size_t off = 0;
        switch ((WalEntryType)entry_type) {

        case WAL_CREATE_TABLE: {
            /* Rebuild the Table schema */
            uint32_t col_count = rb_u32(payload, &off);
            uint32_t pk_col_u  = rb_u32(payload, &off);
            int pk_col = (pk_col_u == 0xFFFFFFFFu) ? -1 : (int)pk_col_u;

            ColumnDef cols[FLEXQL_MAX_COLUMNS] = {};
            for (uint32_t i = 0; i < col_count && i < FLEXQL_MAX_COLUMNS; i++) {
                /* name: fixed 64 bytes */
                memcpy(cols[i].name, payload+off, FLEXQL_MAX_NAME_LEN);
                cols[i].name[FLEXQL_MAX_NAME_LEN-1] = '\0';
                off += FLEXQL_MAX_NAME_LEN;
                cols[i].type        = (ColumnType)rb_u32(payload, &off);
                cols[i].constraints = (uint8_t)rb_u32(payload, &off);
                cols[i].col_index   = (int)i;
            }

            char *err = nullptr;
            tbl = table_create(db, table_name, cols, (int)col_count, &err);
            if (tbl) {
                /* Override pk_col from the WAL (table_create sets it from constraints) */
                tbl->pk_col = pk_col;
                next_row_id = 1;
            }
            free(err);
            dropped = 0;
            break;
        }

        case WAL_INSERT: {
            if (!tbl || dropped) break;

            /* row_id(8) + expiry(8) + col_count(4) */
            uint64_t saved_row_id = rb_u64(payload, &off);
            int64_t  expiry_i64   = rb_i64(payload, &off);
            uint32_t col_count    = rb_u32(payload, &off);
            (void)saved_row_id;

            if (col_count > (uint32_t)FLEXQL_MAX_COLUMNS) break;

            /* Read each cell, convert to string for row_insert() */
            char str_vals[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR] = {};

            for (uint32_t c = 0; c < col_count; c++) {
                uint8_t is_null = rb_u8(payload, &off);
                if (is_null) { str_vals[c][0] = '\0'; continue; }

                ColumnType ct = tbl->schema[c].type;
                switch (ct) {
                case COL_TYPE_INT:
                case COL_TYPE_DATETIME: {
                    int64_t v = rb_i64(payload, &off);
                    snprintf(str_vals[c], FLEXQL_MAX_VARCHAR, "%lld", (long long)v);
                    break;
                }
                case COL_TYPE_DECIMAL: {
                    double v = rb_dbl(payload, &off);
                    /* Render whole numbers as integers (same as cell_to_string) */
                    if (v == floor(v) && v >= -1e15 && v <= 1e15)
                        snprintf(str_vals[c], FLEXQL_MAX_VARCHAR, "%lld", (long long)v);
                    else
                        snprintf(str_vals[c], FLEXQL_MAX_VARCHAR, "%g", v);
                    break;
                }
                case COL_TYPE_VARCHAR: {
                    uint32_t slen = rb_u32(payload, &off);
                    if (slen >= FLEXQL_MAX_VARCHAR) slen = FLEXQL_MAX_VARCHAR-1;
                    memcpy(str_vals[c], payload+off, slen);
                    str_vals[c][slen] = '\0';
                    off += slen;
                    break;
                }
                default:
                    str_vals[c][0] = '\0';
                }
            }

            char *err = nullptr;
            row_insert(tbl, str_vals, (int)col_count,
                       (time_t)expiry_i64, &err);
            free(err);
            next_row_id++;
            break;
        }

        case WAL_DROP_TABLE: {
            /* Mark as dropped — subsequent INSERT entries are ignored */
            dropped = 1;
            break;
        }

        default:
            /* Unknown entry type — skip */
            break;
        }
    }

    free(data);
    return (tbl && !dropped) ? 1 : 0;
}

int wal_recover(DatabaseManager *mgr) {
    int recovered_dbs = 0;

    /* Read registry */
    FILE *reg = fopen(WAL_REGISTRY, "r");
    if (!reg) {
        /* No registry = fresh server, nothing to recover */
        return 0;
    }

    char line[FLEXQL_MAX_NAME_LEN+2];
    while (fgets(line, sizeof(line), reg)) {
        size_t l = strlen(line);
        while (l > 0 && (line[l-1]=='\n'||line[l-1]=='\r')) line[--l]='\0';
        if (l == 0) continue;

        /* Recreate this database in memory */
        char *err = nullptr;
        dbmgr_create(mgr, line, &err);
        free(err);

        Database *db = dbmgr_find(mgr, line);
        if (!db) continue;

        /* Scan database directory for WAL files */
        char dir[512];
        make_db_dir(dir, sizeof(dir), line);

        DIR *d = opendir(dir);
        if (!d) continue;

        struct dirent *ent;
        while ((ent = readdir(d)) != nullptr) {
            const char *fname = ent->d_name;
            size_t flen = strlen(fname);
            /* Only process *.wal files */
            if (flen < 5 || strcmp(fname + flen - 4, ".wal") != 0) continue;

            /* Extract table name (strip .wal suffix) */
            char tbl_name[FLEXQL_MAX_NAME_LEN] = {};
            size_t tlen = flen - 4;
            if (tlen >= FLEXQL_MAX_NAME_LEN) tlen = FLEXQL_MAX_NAME_LEN-1;
            memcpy(tbl_name, fname, tlen);
            tbl_name[tlen] = '\0';

            replay_wal_file(line, tbl_name, db);
        }
        closedir(d);
        recovered_dbs++;
    }
    fclose(reg);

    printf("[wal] Recovery complete: %d database(s) restored\n", recovered_dbs);
    return recovered_dbs;
}