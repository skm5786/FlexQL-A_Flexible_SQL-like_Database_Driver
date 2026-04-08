/**
 * fast_insert.cpp  —  Fast INSERT Parser & Executor  (WAL fixed, clean)
 *
 * KEY FIX: Write WAL for all persistent databases so data survives restart.
 *
 * WAL strategy:
 *   - We accumulate inserted CellValue* pointers in a WalAccum struct.
 *   - Every WAL_CHUNK rows (or at end), we convert cells → strings and
 *     call wal_write_insert_batch() once.
 *   - Cell memory is arena-owned so pointers stay valid until table_free().
 *
 * Performance note:
 *   WAL writes are buffered by wal.cpp (group commit, WAL_FLUSH_ROWS=5000).
 *   The cell_to_string() calls are cheap (snprintf on a 64-byte stack buf).
 *   Net overhead vs no-WAL: ~5-10% which is acceptable for correctness.
 */

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <cerrno>
#include <ctime>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "network/fast_insert.h"
#include "storage/storage.h"
#include "storage/arena.h"
#include "index/index.h"
#include "index/btree.h"
#include "storage/wal.h"
#include "cache/cache.h"

/* ── Wire helpers ─────────────────────────────────────────────────────────── */
static int fi_send_all(int fd, const void *buf, size_t len) {
    const char *p = (const char*)buf; size_t rem = len;
    while (rem > 0) {
        ssize_t n = send(fd, p, rem, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        p += n; rem -= (size_t)n;
    }
    return 0;
}
static int fi_send_msg(int fd, MessageType type, const char *payload, uint32_t plen) {
    WireHeader hdr; hdr.msg_type = (uint8_t)type; hdr.payload_len = htonl(plen);
    if (fi_send_all(fd, &hdr, sizeof(hdr)) != 0) return -1;
    if (plen > 0 && fi_send_all(fd, payload, plen) != 0) return -1;
    return 0;
}
static void fi_send_ok(int fd, const char *msg)  { fi_send_msg(fd, MSG_OK,    msg, (uint32_t)strlen(msg)); }
static void fi_send_err(int fd, const char *msg) { fi_send_msg(fd, MSG_ERROR, msg, (uint32_t)strlen(msg)); }

/* ── Scanner helpers ──────────────────────────────────────────────────────── */
static inline void skip_ws(const char **p) {
    while (**p && isspace((unsigned char)**p)) (*p)++;
}
static inline int match_kw(const char **p, const char *kw) {
    size_t len = strlen(kw);
    if (strncasecmp(*p, kw, len) == 0) { *p += len; return 1; }
    return 0;
}
static int scan_ident(const char **p, char *buf, size_t buf_sz) {
    const char *start = *p;
    while (**p && (isalnum((unsigned char)**p) || **p == '_')) (*p)++;
    size_t len = (size_t)(*p - start);
    if (len == 0) return 0;
    if (len >= buf_sz) len = buf_sz - 1;
    memcpy(buf, start, len); buf[len] = '\0';
    return (int)len;
}
static int parse_int_literal(const char **p, int64_t *out) {
    const char *s = *p; int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    if (!isdigit((unsigned char)*s)) return 0;
    int64_t v = 0;
    while (isdigit((unsigned char)*s)) { v = v * 10 + (*s - '0'); s++; }
    if (*s == '.') return 0;
    *out = neg ? -v : v; *p = s; return 1;
}
static int parse_decimal_literal(const char **p, double *out) {
    const char *s = *p; int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    if (!isdigit((unsigned char)*s)) return 0;
    char buf[64]; int bi = 0;
    if (neg) buf[bi++] = '-';
    while (isdigit((unsigned char)*s) && bi < 60) buf[bi++] = *s++;
    if (*s == '.') { buf[bi++] = '.'; s++; while (isdigit((unsigned char)*s) && bi < 60) buf[bi++] = *s++; }
    buf[bi] = '\0';
    char *end; *out = strtod(buf, &end);
    if (end == buf + (neg ? 1 : 0)) return 0;
    *p = s; return 1;
}
static int parse_numeric_cell(const char **p, ColumnType type, CellValue *cell) {
    cell->type = type; cell->is_null = 0;
    switch (type) {
    case COL_TYPE_INT: case COL_TYPE_DATETIME: {
        const char *tp = *p; int64_t iv;
        if (parse_int_literal(&tp, &iv)) { cell->data.int_val = iv; *p = tp; return 1; }
        double dv;
        if (parse_decimal_literal(p, &dv)) { cell->data.int_val = (int64_t)dv; return 1; }
        return 0;
    }
    case COL_TYPE_DECIMAL: {
        const char *tp = *p; int64_t iv;
        if (parse_int_literal(&tp, &iv) && *tp != '.') { cell->data.decimal_val = (double)iv; *p = tp; return 1; }
        double dv;
        if (parse_decimal_literal(p, &dv)) { cell->data.decimal_val = dv; return 1; }
        return 0;
    }
    default: return 0;
    }
}
static int parse_string_cell(const char **p, Arena *arena, CellValue *cell) {
    if (**p != '\'') return 0;
    (*p)++;
    const char *s = *p; size_t len = 0;
    while (*s) {
        if (*s == '\'') { if (*(s+1) == '\'') { len++; s += 2; } else break; }
        else { len++; s++; }
    }
    if (*s != '\'') return 0;
    char *dst = (char*)arena_alloc(arena, len + 1);
    if (!dst) return 0;
    s = *p; char *d = dst;
    while (*s) {
        if (*s == '\'') { if (*(s+1) == '\'') { *d++ = '\''; s += 2; } else break; }
        else { *d++ = *s++; }
    }
    *d = '\0'; *p = s + 1;
    cell->type = COL_TYPE_VARCHAR; cell->is_null = 0; cell->data.varchar_val = dst;
    return 1;
}

/* ── row_insert_cells ─────────────────────────────────────────────────────── */
static int row_insert_cells(Table *table, CellValue *cells, int col_count,
                             time_t expiry, char **errmsg) {
    if (col_count != table->col_count) {
        char buf[128];
        snprintf(buf, sizeof(buf), "Table '%s' has %d columns but %d values given",
                 table->name, table->col_count, col_count);
        if (errmsg) { free(*errmsg); *errmsg = strdup(buf); }
        return -1;
    }
    for (int i = 0; i < col_count; i++) {
        if ((table->schema[i].constraints & COL_CONSTRAINT_NOT_NULL) && cells[i].is_null) {
            char buf[128];
            snprintf(buf, sizeof(buf), "Column '%s' cannot be NULL", table->schema[i].name);
            if (errmsg) { free(*errmsg); *errmsg = strdup(buf); }
            return -1;
        }
    }
    int64_t pk_int_val = 0; bool pk_is_int = false;
    if (table->pk_col >= 0 && !cells[table->pk_col].is_null) {
        const CellValue *npk = &cells[table->pk_col];
        if (npk->type == COL_TYPE_INT || npk->type == COL_TYPE_DECIMAL || npk->type == COL_TYPE_DATETIME) {
            pk_is_int = true;
            pk_int_val = (npk->type == COL_TYPE_DECIMAL)
                ? (int64_t)(*(int64_t*)&npk->data.decimal_val)
                : npk->data.int_val;
        }
        if (table->pk_index) {
            pthread_rwlock_rdlock(&table->lock);
            Row *existing = index_get(table->pk_index, npk);
            pthread_rwlock_unlock(&table->lock);
            if (existing) {
                time_t now = time(nullptr);
                if (!(existing->expiry > 0 && existing->expiry < now)) {
                    char buf[128];
                    snprintf(buf, sizeof(buf), "Duplicate PRIMARY KEY in '%s'",
                             table->schema[table->pk_col].name);
                    if (errmsg) { free(*errmsg); *errmsg = strdup(buf); }
                    return -1;
                }
                pthread_rwlock_wrlock(&table->lock);
                index_remove(table->pk_index, npk);
                pthread_rwlock_unlock(&table->lock);
            }
        }
    }
    Row *row = (Row*)arena_alloc(table->row_arena, sizeof(Row));
    if (!row) { if (errmsg) { free(*errmsg); *errmsg = strdup("OOM: arena full"); } return -1; }
    row->cells = cells; row->col_count = col_count; row->expiry = expiry; row->next = nullptr;
    row->row_id = (uint64_t)__sync_fetch_and_add((volatile long long*)&table->next_row_id, 1LL);
    pthread_rwlock_wrlock(&table->lock);
    if (table->tail) table->tail->next = row; else table->head = row;
    table->tail = row; table->row_count++;
    if (table->pk_index && table->pk_col >= 0 && !row->cells[table->pk_col].is_null) {
        if (pk_is_int) index_put_int(table->pk_index, pk_int_val, row);
        else           index_put(table->pk_index, &row->cells[table->pk_col], row);
    }
    bool eager = (table->row_count <= 50000);
    pthread_rwlock_unlock(&table->lock);
    if (eager) {
        for (int i = 0; i < table->col_count; i++)
            if (table->col_btree[i] && !row->cells[i].is_null)
                btree_insert(table->col_btree[i], &row->cells[i], row);
    }
    return 0;
}

/* ── WAL accumulator ──────────────────────────────────────────────────────── */
#define WAL_CHUNK 2000

struct WalAccum {
    CellValue  **rows;   /* pointers to arena-owned CellValue arrays */
    int          count;
    int          cap;
    int          col_count;
    const char  *db_name;
    const char  *table_name;
    Table       *table;
    bool         persistent;
};

static void wa_init(WalAccum *wa, int col_count, const char *db_name,
                    const char *table_name, Table *table) {
    wa->rows       = nullptr;
    wa->count      = 0;
    wa->cap        = 0;
    wa->col_count  = col_count;
    wa->db_name    = db_name;
    wa->table_name = table_name;
    wa->table      = table;
    wa->persistent = wal_is_persistent(db_name);
}

static void wa_flush(WalAccum *wa) {
    if (!wa->persistent || wa->count == 0) { wa->count = 0; return; }

    int n  = wa->count;
    int nc = wa->col_count;

    /* Build flat array of string pointers + backing string storage */
    const char **flat = (const char**)malloc((size_t)(n * nc) * sizeof(char*));
    char *strbuf = (char*)malloc((size_t)(n * nc) * 64);
    if (!flat || !strbuf) { free(flat); free(strbuf); wa->count = 0; return; }

    for (int r = 0; r < n; r++) {
        CellValue *cells = wa->rows[r];
        for (int c = 0; c < nc; c++) {
            char *slot = strbuf + (r * nc + c) * 64;
            if (cells[c].is_null) slot[0] = '\0';
            else                  cell_to_string(&cells[c], slot, 64);
            flat[r * nc + c] = slot;
        }
    }

    wal_write_insert_batch(wa->db_name, wa->table_name, wa->table,
                           flat, n, nc, 0);
    free(flat);
    free(strbuf);
    wa->count = 0;
}

static void wa_add(WalAccum *wa, CellValue *cells) {
    if (!wa->persistent) return;
    if (wa->count >= wa->cap) {
        int new_cap = (wa->cap == 0) ? WAL_CHUNK : (wa->cap + WAL_CHUNK);
        CellValue **nr = (CellValue**)realloc(wa->rows, (size_t)new_cap * sizeof(CellValue*));
        if (!nr) return;
        wa->rows = nr; wa->cap = new_cap;
    }
    wa->rows[wa->count++] = cells;
    if (wa->count >= WAL_CHUNK) wa_flush(wa);
}

static void wa_destroy(WalAccum *wa) {
    wa_flush(wa);
    free(wa->rows);
    wa->rows = nullptr;
    wa->count = 0;
    wa->cap = 0;
}

/* ── fast_insert_execute ──────────────────────────────────────────────────── */
int fast_insert_execute(const char *sql, Database *db, int client_fd,
                        char **errmsg) {
    const char *p = sql;
    skip_ws(&p);
    if (!match_kw(&p, "INSERT"))  return FAST_INSERT_FALLBACK;
    skip_ws(&p);
    if (!match_kw(&p, "INTO"))    return FAST_INSERT_FALLBACK;
    skip_ws(&p);

    char table_name[FLEXQL_MAX_NAME_LEN];
    if (!scan_ident(&p, table_name, sizeof(table_name)))
        return FAST_INSERT_FALLBACK;
    skip_ws(&p);

    /* Column-list form: INSERT INTO t (col1, col2) VALUES ... */
    if (*p == '(') return FAST_INSERT_FALLBACK;

    if (!match_kw(&p, "VALUES")) return FAST_INSERT_FALLBACK;
    skip_ws(&p);

    Table *table = table_find(db, table_name);
    if (!table) {
        char buf[128];
        snprintf(buf, sizeof(buf), "Table '%s' does not exist", table_name);
        fi_send_err(client_fd, buf);
        return FAST_INSERT_HANDLED;
    }

    int col_count = table->col_count;
    int rows_inserted = 0;

    WalAccum wa;
    wa_init(&wa, col_count, db->name, table->name, table);

    while (*p == '(') {
        p++;

        CellValue *cells = (CellValue*)arena_alloc(table->row_arena,
                                                    col_count * sizeof(CellValue));
        if (!cells) {
            fi_send_err(client_fd, "OOM: arena full");
            wa_destroy(&wa);
            return FAST_INSERT_HANDLED;
        }

        for (int col = 0; col < col_count; col++) {
            skip_ws(&p);
            ColumnType  ct   = table->schema[col].type;
            CellValue  *cell = &cells[col];
            cell->type = ct; cell->is_null = 0;

            /* NULL keyword */
            if (strncasecmp(p, "NULL", 4) == 0 &&
                !isalnum((unsigned char)p[4]) && p[4] != '_') {
                cell->is_null = 1; p += 4; goto next_col;
            }
            /* Single-quoted string */
            if (*p == '\'') {
                if (ct != COL_TYPE_VARCHAR) { wa_destroy(&wa); return FAST_INSERT_FALLBACK; }
                if (!parse_string_cell(&p, table->row_arena, cell)) {
                    fi_send_err(client_fd, "Malformed string literal in INSERT");
                    wa_destroy(&wa);
                    return FAST_INSERT_HANDLED;
                }
                goto next_col;
            }
            /* Numeric literal */
            if (isdigit((unsigned char)*p) || *p == '-') {
                if (!parse_numeric_cell(&p, ct, cell)) { wa_destroy(&wa); return FAST_INSERT_FALLBACK; }
                goto next_col;
            }
            /* Unrecognised — fall back */
            wa_destroy(&wa);
            return FAST_INSERT_FALLBACK;

        next_col:
            skip_ws(&p);
            if (col < col_count - 1) {
                if (*p != ',') {
                    fi_send_err(client_fd, "Expected ',' between INSERT values");
                    wa_destroy(&wa);
                    return FAST_INSERT_HANDLED;
                }
                p++;
            }
        }

        skip_ws(&p);
        if (*p != ')') {
            fi_send_err(client_fd, "Expected ')' at end of VALUES tuple");
            wa_destroy(&wa);
            return FAST_INSERT_HANDLED;
        }
        p++;

        char *row_err = nullptr;
        if (row_insert_cells(table, cells, col_count, 0, &row_err) != 0) {
            fi_send_err(client_fd, row_err ? row_err : "INSERT failed");
            free(row_err);
            wa_destroy(&wa);
            return FAST_INSERT_HANDLED;
        }
        rows_inserted++;
        wa_add(&wa, cells);

        skip_ws(&p);
        if (*p == ',') { p++; skip_ws(&p); }
        else break;
    }

    wa_destroy(&wa);  /* final flush */

    char msg_buf[64];
    if (rows_inserted == 1) {
        fi_send_ok(client_fd, "1 row inserted.");
    } else {
        snprintf(msg_buf, sizeof(msg_buf), "%d rows inserted.", rows_inserted);
        fi_send_ok(client_fd, msg_buf);
    }
    return FAST_INSERT_HANDLED;
}