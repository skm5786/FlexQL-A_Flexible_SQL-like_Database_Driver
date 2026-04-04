/**
 * wal.cpp  —  Write-Ahead Log Implementation  (fixed)
 *
 * CHANGES vs original:
 *
 *   1. wal_register_db() is now IDEMPOTENT.
 *      The original used fopen(WAL_REGISTRY, "a") which appended the db name
 *      every time it was called.  On every server restart + reconnect cycle,
 *      DEFAULT would be appended again and again.  wal_recover() would then
 *      call dbmgr_create("DEFAULT") multiple times, each one failing with
 *      "already exists" and printing an error.  More importantly, the registry
 *      file would grow unboundedly.
 *
 *      Fix: before appending, read all existing lines.  Only write the new
 *      name if it is not already present.
 *
 *   2. No other logic changes — all WAL entry formats, CRC32, recovery
 *      sequence, and group commit behaviour are identical to the original.
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
#include <arpa/inet.h>

#include "storage/wal.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

/* ── Section A: CRC32 ───────────────────────────────────────────────────── */
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

/* ── Section B: Binary serialisation helpers ────────────────────────────── */
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
    if (wbuf_grow(b,fixed_len)) return -1;
    size_t slen = s ? strlen(s) : 0;
    if (slen > fixed_len) slen = fixed_len;
    if (slen) memcpy(b->data+b->len, s, slen);
    if (slen < fixed_len) memset(b->data+b->len+slen, 0, fixed_len-slen);
    b->len += fixed_len; return 0;
}

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
static int64_t  rb_i64(const uint8_t *p, size_t *off) {
    return (int64_t)rb_u64(p,off);
}
static double   rb_dbl(const uint8_t *p, size_t *off) {
    uint64_t bits=rb_u64(p,off); double v; memcpy(&v,&bits,8); return v;
}

/* ── Section C: File path helpers ───────────────────────────────────────── */
static void make_wal_path(char *out, size_t outsz,
                          const char *db_name, const char *table_name) {
    snprintf(out, outsz, "%s/%s/%s.wal",
             WAL_DATA_DIR, db_name, table_name);
}
static void make_db_dir(char *out, size_t outsz, const char *db_name) {
    snprintf(out, outsz, "%s/%s", WAL_DATA_DIR, db_name);
}

int wal_is_persistent(const char *db_name) {
    if (!db_name) return 0;
    /* _SESSION_* databases are ephemeral — never persisted to WAL.
     * Everything else (DEFAULT, user-created named databases) is persistent.
     */
    return (strncmp(db_name, "_SESSION_", 9) != 0);
}

int wal_ensure_dirs(const char *db_name) {
    mkdir(WAL_DATA_DIR, 0755);
    char dir[512];
    make_db_dir(dir, sizeof(dir), db_name);
    if (mkdir(dir, 0755) < 0 && errno != EEXIST) return -1;
    return 0;
}

/* ── Section D: Registry — IDEMPOTENT ──────────────────────────────────────
 *
 * wal_register_db() now checks whether db_name is already in the registry
 * file before appending it.  This prevents duplicate entries when:
 *   - The server restarts and ensures_default_db() is called
 *   - A client explicitly calls CREATE DATABASE on an already-known db
 *   - The benchmark is run multiple times against the same server instance
 *
 * Implementation: read all existing lines into memory, search for db_name
 * (case-insensitive), only append if not found.
 */
int wal_register_db(const char *db_name) {
    if (!wal_is_persistent(db_name)) return 0;
    mkdir(WAL_DATA_DIR, 0755);

    /* ── Read existing registry entries ─────────────────────────────── */
    char existing[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    int  n_existing = 0;

    FILE *f = fopen(WAL_REGISTRY, "r");
    if (f) {
        char line[FLEXQL_MAX_NAME_LEN + 2];
        while (fgets(line, sizeof(line), f) &&
               n_existing < FLEXQL_MAX_DATABASES) {
            size_t l = strlen(line);
            while (l > 0 && (line[l-1] == '\n' || line[l-1] == '\r'))
                line[--l] = '\0';
            if (l == 0) continue;
            strncpy(existing[n_existing++], line, FLEXQL_MAX_NAME_LEN - 1);
        }
        fclose(f);
    }

    /* ── Check if already registered ────────────────────────────────── */
    for (int i = 0; i < n_existing; i++) {
        if (strcasecmp(existing[i], db_name) == 0)
            return 0;   /* already present — nothing to do */
    }

    /* ── Append new entry ───────────────────────────────────────────── */
    f = fopen(WAL_REGISTRY, "a");
    if (!f) return -1;
    fprintf(f, "%s\n", db_name);
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    return 0;
}

int wal_unregister_db(const char *db_name) {
    if (!wal_is_persistent(db_name)) return 0;

    FILE *f = fopen(WAL_REGISTRY, "r");
    if (!f) return 0;

    char lines[FLEXQL_MAX_DATABASES][FLEXQL_MAX_NAME_LEN];
    int  n = 0;
    char line[FLEXQL_MAX_NAME_LEN + 2];
    while (fgets(line, sizeof(line), f) && n < FLEXQL_MAX_DATABASES) {
        size_t l = strlen(line);
        while (l > 0 && (line[l-1] == '\n' || line[l-1] == '\r'))
            line[--l] = '\0';
        if (l == 0) continue;
        if (strcasecmp(line, db_name) != 0)
            strncpy(lines[n++], line, FLEXQL_MAX_NAME_LEN - 1);
    }
    fclose(f);

    f = fopen(WAL_REGISTRY, "w");
    if (!f) return -1;
    for (int i = 0; i < n; i++) fprintf(f, "%s\n", lines[i]);
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    return 0;
}

/* ── Section E: WAL write functions ─────────────────────────────────────── */
static uint64_t g_lsn = 0;

static int write_wal_record(int fd, WalEntryType type,
                             const WBuf *payload) {
    uint64_t lsn = __sync_add_and_fetch(&g_lsn, 1);

    WBuf hdr = {};
    wb_u64(&hdr, lsn);
    wb_u32(&hdr, (uint32_t)type);
    wb_u32(&hdr, (uint32_t)payload->len);

    uint32_t crc = crc32_update(0, hdr.data, hdr.len);
    crc = crc32_update(crc, payload->data, payload->len);

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

    WBuf payload = {};
    wb_u32(&payload, (uint32_t)table->col_count);
    wb_u32(&payload, (uint32_t)(table->pk_col < 0
                                ? 0xFFFFFFFFu
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
            wb_u8(payload, 1);
            continue;
        }
        wb_u8(payload, 0);

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
            wb_u8(payload, 1);
        }
    }
    return 0;
}

int wal_write_insert_batch(const char *db_name, const char *table_name,
                           const Table *table,
                           const char *const *str_values_flat,
                           int row_count, int value_count, time_t expiry) {
    if (!wal_is_persistent(db_name)) return 0;

    WBuf payload = {};
    char path[512];
    make_wal_path(path, sizeof(path), db_name, table_name);
    int fd = open(path, O_WRONLY|O_CREAT|O_APPEND, 0644);
    if (fd < 0) { free(payload.data); return -1; }

    WBuf all = {};
    uint64_t lsn_base =
        __sync_add_and_fetch(&g_lsn, (uint64_t)row_count);
    lsn_base -= (uint64_t)(row_count - 1);

    for (int r = 0; r < row_count; r++) {
        WBuf row_payload = {};
        char row_buf[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
        for (int cc = 0; cc < value_count && cc < FLEXQL_MAX_COLUMNS; cc++) {
            const char *sv = str_values_flat[r * value_count + cc];
            strncpy(row_buf[cc], sv ? sv : "", FLEXQL_MAX_VARCHAR - 1);
            row_buf[cc][FLEXQL_MAX_VARCHAR - 1] = '\0';
        }
        serialise_row(&row_payload, table, row_buf, value_count,
                      0, expiry);

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
    wb_u8(&empty, 0);
    int rc = write_wal_record(fd, WAL_DROP_TABLE, &empty);
    fdatasync(fd);
    close(fd);
    free(empty.data);
    return rc;
}

/* ── Section F: Recovery ─────────────────────────────────────────────────── */
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
    if (!data) return 0;

    size_t   pos = 0;
    Table   *tbl = nullptr;
    int      dropped = 0;

    while (pos + 20 <= file_len) {
        size_t entry_start = pos;

        uint64_t lsn         = rb_u64(data, &pos);
        uint32_t entry_type  = rb_u32(data, &pos);
        uint32_t payload_len = rb_u32(data, &pos);
        (void)lsn;

        if (pos + payload_len + 4 > file_len) break;

        const uint8_t *payload = data + pos;
        pos += payload_len;

        uint32_t stored_crc_n;
        memcpy(&stored_crc_n, data + pos, 4); pos += 4;
        uint32_t stored_crc = ntohl(stored_crc_n);

        uint32_t computed_crc =
            crc32_update(0, data + entry_start, 16);
        computed_crc = crc32_update(computed_crc, payload, payload_len);

        if (computed_crc != stored_crc) break;

        size_t off = 0;
        switch ((WalEntryType)entry_type) {

        case WAL_CREATE_TABLE: {
            uint32_t col_count = rb_u32(payload, &off);
            uint32_t pk_col_u  = rb_u32(payload, &off);
            int pk_col = (pk_col_u == 0xFFFFFFFFu) ? -1 : (int)pk_col_u;

            ColumnDef cols[FLEXQL_MAX_COLUMNS] = {};
            for (uint32_t i = 0;
                 i < col_count && i < FLEXQL_MAX_COLUMNS; i++) {
                memcpy(cols[i].name, payload+off, FLEXQL_MAX_NAME_LEN);
                cols[i].name[FLEXQL_MAX_NAME_LEN - 1] = '\0';
                off += FLEXQL_MAX_NAME_LEN;
                cols[i].type        = (ColumnType)rb_u32(payload, &off);
                cols[i].constraints = (uint8_t)rb_u32(payload, &off);
                cols[i].col_index   = (int)i;
            }

            char *err = nullptr;
            tbl = table_create(db, table_name,
                               cols, (int)col_count, &err);
            if (tbl) tbl->pk_col = pk_col;
            free(err);
            dropped = 0;
            break;
        }

        case WAL_INSERT: {
            if (!tbl || dropped) break;

            uint64_t saved_row_id = rb_u64(payload, &off);
            int64_t  expiry_i64   = rb_i64(payload, &off);
            uint32_t col_count    = rb_u32(payload, &off);
            (void)saved_row_id;

            if (col_count > (uint32_t)FLEXQL_MAX_COLUMNS) break;

            char str_vals[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR] = {};
            for (uint32_t c = 0; c < col_count; c++) {
                uint8_t is_null = rb_u8(payload, &off);
                if (is_null) { str_vals[c][0] = '\0'; continue; }

                ColumnType ct = tbl->schema[c].type;
                switch (ct) {
                case COL_TYPE_INT:
                case COL_TYPE_DATETIME: {
                    int64_t v = rb_i64(payload, &off);
                    snprintf(str_vals[c], FLEXQL_MAX_VARCHAR,
                             "%lld", (long long)v);
                    break;
                }
                case COL_TYPE_DECIMAL: {
                    double v = rb_dbl(payload, &off);
                    if (v == floor(v) && v >= -1e15 && v <= 1e15)
                        snprintf(str_vals[c], FLEXQL_MAX_VARCHAR,
                                 "%lld", (long long)v);
                    else
                        snprintf(str_vals[c], FLEXQL_MAX_VARCHAR, "%g", v);
                    break;
                }
                case COL_TYPE_VARCHAR: {
                    uint32_t slen = rb_u32(payload, &off);
                    if (slen >= FLEXQL_MAX_VARCHAR)
                        slen = FLEXQL_MAX_VARCHAR - 1;
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
            break;
        }

        case WAL_DROP_TABLE:
            dropped = 1;
            break;

        default:
            break;
        }
    }

    free(data);
    return (tbl && !dropped) ? 1 : 0;
}

int wal_recover(DatabaseManager *mgr) {
    int recovered_dbs = 0;

    FILE *reg = fopen(WAL_REGISTRY, "r");
    if (!reg) return 0;

    char line[FLEXQL_MAX_NAME_LEN + 2];
    while (fgets(line, sizeof(line), reg)) {
        size_t l = strlen(line);
        while (l > 0 && (line[l-1] == '\n' || line[l-1] == '\r'))
            line[--l] = '\0';
        if (l == 0) continue;

        char *err = nullptr;
        dbmgr_create(mgr, line, &err);
        free(err);

        Database *db = dbmgr_find(mgr, line);
        if (!db) continue;

        char dir[512];
        make_db_dir(dir, sizeof(dir), line);

        DIR *d = opendir(dir);
        if (!d) continue;

        struct dirent *ent;
        while ((ent = readdir(d)) != nullptr) {
            const char *fname = ent->d_name;
            size_t flen = strlen(fname);
            if (flen < 5 || strcmp(fname + flen - 4, ".wal") != 0)
                continue;

            char tbl_name[FLEXQL_MAX_NAME_LEN] = {};
            size_t tlen = flen - 4;
            if (tlen >= FLEXQL_MAX_NAME_LEN) tlen = FLEXQL_MAX_NAME_LEN - 1;
            memcpy(tbl_name, fname, tlen);
            tbl_name[tlen] = '\0';

            replay_wal_file(line, tbl_name, db);
        }
        closedir(d);
        recovered_dbs++;
    }
    fclose(reg);

    printf("[wal] Recovery complete: %d database(s) restored\n",
           recovered_dbs);
    return recovered_dbs;
}