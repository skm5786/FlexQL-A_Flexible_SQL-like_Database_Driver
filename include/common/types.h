/**
 * types.h  —  FlexQL Internal Type Definitions
 *
 * CHANGE: Added QUERY_DROP_TABLE = 15 to QueryType enum, and
 *         DropTableParams struct so the executor knows the table name.
 */

#ifndef FLEXQL_TYPES_H
#define FLEXQL_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <pthread.h>

typedef enum {
    COL_TYPE_INT      = 0,
    COL_TYPE_DECIMAL  = 1,
    COL_TYPE_VARCHAR  = 2,
    COL_TYPE_DATETIME = 3,
    COL_TYPE_UNKNOWN  = 255
} ColumnType;

#define COL_CONSTRAINT_NONE        0x00
#define COL_CONSTRAINT_NOT_NULL    0x01
#define COL_CONSTRAINT_PRIMARY_KEY 0x02
#define COL_CONSTRAINT_UNIQUE      0x04

#define FLEXQL_MAX_NAME_LEN   64
#define FLEXQL_MAX_VARCHAR    4096
#define FLEXQL_MAX_COLUMNS    64
#define FLEXQL_MAX_TABLES     256

typedef struct {
    char        name[FLEXQL_MAX_NAME_LEN];
    ColumnType  type;
    uint8_t     constraints;
    int         col_index;
    int         varchar_max_len;
} ColumnDef;

typedef struct {
    ColumnType type;
    int        is_null;
    union {
        int64_t  int_val;
        double   decimal_val;
        char    *varchar_val;
        time_t   datetime_val;
    } data;
} CellValue;

typedef struct Row {
    uint64_t    row_id;
    CellValue  *cells;
    int         col_count;
    time_t      expiry;
    struct Row *next;
} Row;

#ifndef FLEXQL_INDEX_FORWARD_DECL
#define FLEXQL_INDEX_FORWARD_DECL
typedef struct HashIndex HashIndex;
#endif

#ifndef FLEXQL_BTREE_FORWARD_DECL
#define FLEXQL_BTREE_FORWARD_DECL
typedef struct BTree BTree;
#endif
#ifndef FLEXQL_ARENA_FORWARD_DECL
#define FLEXQL_ARENA_FORWARD_DECL
typedef struct Arena Arena;
#endif

typedef struct Table {
    char            name[FLEXQL_MAX_NAME_LEN];
    ColumnDef       schema[FLEXQL_MAX_COLUMNS];
    int             col_count;
    int             pk_col;
    Row            *head;
    Row            *tail;
    uint64_t        row_count;
    uint64_t        next_row_id;
    pthread_rwlock_t lock;
    struct HashIndex *pk_index;
    BTree           *col_btree[FLEXQL_MAX_COLUMNS];
    Arena           *row_arena;
    struct Table    *next;
} Table;

typedef struct {
    char            name[FLEXQL_MAX_NAME_LEN];
    Table          *tables[FLEXQL_MAX_TABLES];
    int             table_count;
    pthread_mutex_t schema_lock;
} Database;

#define FLEXQL_MAX_DATABASES 64

#ifndef FLEXQL_CACHE_FORWARD_DECL
#define FLEXQL_CACHE_FORWARD_DECL
typedef struct LRUCache LRUCache;
#endif

typedef struct {
    Database       *databases[FLEXQL_MAX_DATABASES];
    int             db_count;
    pthread_mutex_t lock;
    LRUCache       *query_cache;
} DatabaseManager;

/* ── Query types ─────────────────────────────────────────────────────── */
typedef enum {
    QUERY_CREATE_TABLE  = 0,
    QUERY_INSERT        = 1,
    QUERY_SELECT        = 2,
    QUERY_INNER_JOIN    = 3,
    QUERY_CREATE_DB     = 10,
    QUERY_USE_DB        = 11,
    QUERY_SHOW_DBS      = 12,
    QUERY_DROP_DB       = 13,
    QUERY_SHOW_TABLES   = 14,
    /* NEW: DROP TABLE support so benchmarks can clean up tables */
    QUERY_DROP_TABLE    = 15,
    QUERY_UNKNOWN       = 255
} QueryType;

typedef struct {
    char db_name[FLEXQL_MAX_NAME_LEN];
} DbNameParams;

typedef enum {
    OP_EQ  = 0,
    OP_NEQ = 1,
    OP_LT  = 2,
    OP_GT  = 3,
    OP_LTE = 4,
    OP_GTE = 5
} CompareOp;

typedef struct {
    char       col_name[FLEXQL_MAX_NAME_LEN];
    CompareOp  op;
    char       value[FLEXQL_MAX_VARCHAR];
    int        has_condition;
} WhereClause;

typedef struct {
    int    select_all;
    int    col_count;
    char   col_names[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_NAME_LEN];
} SelectList;

typedef struct {
    char      table_name[FLEXQL_MAX_NAME_LEN];
    ColumnDef columns[FLEXQL_MAX_COLUMNS];
    int       col_count;
} CreateTableParams;

/* NEW: DROP TABLE just needs the table name */
typedef struct {
    char table_name[FLEXQL_MAX_NAME_LEN];
} DropTableParams;

#define FLEXQL_MAX_BATCH_ROWS 10000

typedef struct {
    char   table_name[FLEXQL_MAX_NAME_LEN];
    char   values[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
    int    value_count;
    int    batch_row_count;
    int    extra_capacity;
    time_t expiry;
    char (*extra_rows)[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
} InsertParams;

typedef struct {
    char        table_name[FLEXQL_MAX_NAME_LEN];
    SelectList  select;
    WhereClause where;
    int         is_join;
    char        join_table[FLEXQL_MAX_NAME_LEN];
    char        join_col_a[FLEXQL_MAX_NAME_LEN];
    char        join_col_b[FLEXQL_MAX_NAME_LEN];
} SelectParams;

typedef struct {
    QueryType type;
    union {
        CreateTableParams create;
        DropTableParams   drop;        /* NEW */
        InsertParams      insert;
        SelectParams      select;
        DbNameParams      db;
    } params;
} QueryNode;

/* ── Wire protocol ───────────────────────────────────────────────────── */
typedef enum {
    MSG_QUERY   = 0x01,
    MSG_RESULT  = 0x02,
    MSG_DONE    = 0x03,
    MSG_ERROR   = 0x04,
    MSG_OK      = 0x05,
    MSG_ABORT   = 0x06
} MessageType;

#pragma pack(push, 1)
typedef struct {
    uint8_t  msg_type;
    uint32_t payload_len;
} WireHeader;
#pragma pack(pop)

#define FLEXQL_MAX_PAYLOAD (8 * 1024 * 1024)

typedef struct ResultRow {
    int              col_count;
    char           **values;
    char           **col_names;
    struct ResultRow *next;
} ResultRow;

struct FlexQL {
    int    sockfd;
    char   host[256];
    int    port;
    int    connected;
    char  *last_errmsg;
};

#endif /* FLEXQL_TYPES_H */