/**
 * types.h  —  FlexQL Internal Type Definitions
 *
 * LESSON: Separating "public API types" (flexql.h) from "internal types"
 * (this file) is crucial for large projects.  Internal types are never
 * included by library users — only by implementation files.
 *
 * This file defines:
 *   - Column / data types the SQL engine understands
 *   - Schema structures (table layout metadata)
 *   - Row / record representation
 *   - Query AST (Abstract Syntax Tree) nodes
 *   - Network message framing
 *   - Error / result structures
 */

#ifndef FLEXQL_TYPES_H
#define FLEXQL_TYPES_H

#include <stdint.h>   /* uint8_t, int64_t … exact-width integers          */
#include <stddef.h>   /* size_t, NULL                                       */
#include <time.h>     /* time_t  (used for expiration timestamps)           */
#include <pthread.h>  /* pthread_mutex_t  (used in Table / Cache structs)   */

/* ────────────────────────────────────────────────────────────────────────────
 * LESSON — WHY exact-width integers?
 *   "int" on a 32-bit machine is 32 bits; on a 64-bit machine it is still
 *   32 bits — but this is guaranteed only by the C99 standard, not all
 *   compilers.  Using int64_t, uint32_t etc. from <stdint.h> makes the sizes
 *   unambiguous in cross-platform code.
 * ──────────────────────────────────────────────────────────────────────────── */

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  COLUMN (FIELD) DATA TYPES
 *
 *  LESSON: An enum is the idiomatic way to represent a fixed set of named
 *  constants in C.  We store one of these values per column in the schema so
 *  the query engine knows how to parse, compare, and serialize each column.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef enum {
    COL_TYPE_INT      = 0,  /* 64-bit signed integer            */
    COL_TYPE_DECIMAL  = 1,  /* 64-bit IEEE 754 double           */
    COL_TYPE_VARCHAR  = 2,  /* Variable-length UTF-8 string     */
    COL_TYPE_DATETIME = 3,  /* Unix timestamp (seconds since epoch) */
    COL_TYPE_UNKNOWN  = 255
} ColumnType;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  COLUMN CONSTRAINTS (bitmask)
 *
 *  LESSON: Bitmask flags let you combine multiple boolean properties into a
 *  single byte/int without allocating a separate bool for each.  Use bitwise
 *  OR to set, AND to test, and XOR to clear.
 *    e.g.  col.constraints = COL_NOT_NULL | COL_PRIMARY_KEY;
 *          if (col.constraints & COL_NOT_NULL) { ... }
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#define COL_CONSTRAINT_NONE        0x00
#define COL_CONSTRAINT_NOT_NULL    0x01
#define COL_CONSTRAINT_PRIMARY_KEY 0x02
#define COL_CONSTRAINT_UNIQUE      0x04

/* Max identifier length (table / column names)                               */
#define FLEXQL_MAX_NAME_LEN   64
/* Max VARCHAR stored inline before we overflow to a heap string              */
#define FLEXQL_MAX_VARCHAR    4096
/* Max columns per table                                                      */
#define FLEXQL_MAX_COLUMNS    64
/* Max tables in the database                                                 */
#define FLEXQL_MAX_TABLES     256

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  COLUMN DEFINITION  (part of the schema)
 *
 *  LESSON: One ColumnDef is stored per column when a CREATE TABLE executes.
 *  Think of it as the "blueprint" for a column — its name, type, and rules.
 *  The array of ColumnDefs inside a Table is the table's schema.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct {
    char        name[FLEXQL_MAX_NAME_LEN]; /* Column identifier e.g. "EMAIL" */
    ColumnType  type;                       /* INT / DECIMAL / VARCHAR / DATETIME */
    uint8_t     constraints;               /* Bitmask of COL_CONSTRAINT_* flags */
    int         col_index;                 /* 0-based position in the row      */
    /* For VARCHAR: maximum declared length (0 = unlimited up to MAX_VARCHAR)  */
    int         varchar_max_len;
} ColumnDef;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  CELL VALUE  (one field in a row)
 *
 *  LESSON: A tagged union is the C way to store "a value that can be one of
 *  several types".  The  type  field says which union member is active.
 *  C++ std::variant / std::any solve the same problem with type safety.
 *
 *  Memory layout: We store small values (INT, DECIMAL, DATETIME) inline.
 *  For VARCHAR we store a pointer to a heap-allocated string.  The Row
 *  destructor is responsible for free()-ing those pointers.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct {
    ColumnType type;      /* Which union member to read                      */
    int        is_null;   /* 1 if this cell is SQL NULL                      */
    union {
        int64_t  int_val;      /* COL_TYPE_INT                               */
        double   decimal_val;  /* COL_TYPE_DECIMAL                           */
        char    *varchar_val;  /* COL_TYPE_VARCHAR  (heap-allocated)         */
        time_t   datetime_val; /* COL_TYPE_DATETIME (unix timestamp)         */
    } data;
} CellValue;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  ROW  (one record in a table)
 *
 *  LESSON — Row-Major Storage Decision:
 *    We chose ROW-MAJOR storage: all cells of one row are stored together.
 *    The alternative is COLUMN-MAJOR (columnar) where all values for column 0
 *    are together, then all values for column 1, etc.
 *
 *    Row-major is better for OLTP (Online Transaction Processing):
 *      • INSERT writes one contiguous struct — single allocation.
 *      • SELECT * returns a full row — single read.
 *      • WHERE on primary key retrieves the whole row at once.
 *
 *    Column-major is better for OLAP (analytics):
 *      • Aggregating one column (SUM, AVG) touches less memory.
 *      • Better CPU cache efficiency for column scans.
 *
 *    Because FlexQL is an OLTP-style driver (individual inserts/selects),
 *    row-major is the right trade-off.
 *
 *  expiry — Unix timestamp; if non-zero and current time > expiry, the row
 *            is treated as deleted (soft expiration).
 *  row_id — Internal 64-bit monotonic ID assigned at insert time.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct Row {
    uint64_t    row_id;                    /* Monotonically increasing row ID */
    CellValue  *cells;                     /* Heap array of CellValue[col_count] */
    int         col_count;                 /* Number of cells in this row     */
    time_t      expiry;                    /* 0 = never expires               */
    struct Row *next;                      /* Intrusive linked-list for table  */
} Row;

/* Forward declaration — HashIndex is defined in include/index/index.h.    */
#ifndef FLEXQL_INDEX_FORWARD_DECL
#define FLEXQL_INDEX_FORWARD_DECL
typedef struct HashIndex HashIndex;
#endif

/* Forward declarations for Lesson 10 (BTree) and Lesson 11 (Arena).      */
#ifndef FLEXQL_BTREE_FORWARD_DECL
#define FLEXQL_BTREE_FORWARD_DECL
typedef struct BTree BTree;
#endif
#ifndef FLEXQL_ARENA_FORWARD_DECL
#define FLEXQL_ARENA_FORWARD_DECL
typedef struct Arena Arena;
#endif

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  TABLE  (one relation in the database)
 *
 *  Lesson 4:  pk_index — HashIndex for O(1) PK lookup and dupe check.
 *  Lesson 6:  tail     — O(1) append for insertion order.
 *  Lesson 9:  lock     — RWLock for concurrent readers.
 *  Lesson 10: col_btree[FLEXQL_MAX_COLUMNS] — per-column B+ tree range index.
 *             Only created for INT/DECIMAL columns; NULL for others.
 *  Lesson 11: row_arena — bump allocator for Row + CellValue memory.
 *             All Row/CellValue memory is from the arena; table_free()
 *             drops the whole arena instead of iterating the list.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
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
    BTree           *col_btree[FLEXQL_MAX_COLUMNS]; /* L10: one B+tree per col */
    Arena           *row_arena;                     /* L11: bump allocator     */
    struct Table    *next;
} Table;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  DATABASE  (one named database — holds a set of tables)
 *
 *  A database is identified by its name (uppercased).
 *  Each client connection tracks which database is currently selected.
 *
 *  DESIGN: Multiple named databases are managed by DatabaseManager below.
 *  Tables live inside a Database so the same table name can exist in two
 *  different databases without conflict (just like MySQL/PostgreSQL).
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct {
    char            name[FLEXQL_MAX_NAME_LEN]; /* Database name (uppercased) */
    Table          *tables[FLEXQL_MAX_TABLES]; /* Array of Table pointers    */
    int             table_count;
    pthread_mutex_t schema_lock;               /* Protect CREATE TABLE       */
} Database;

/* Max number of databases the server can hold simultaneously                */
#define FLEXQL_MAX_DATABASES 64

/* Forward declaration for LRUCache — defined in include/cache/cache.h      */
#ifndef FLEXQL_CACHE_FORWARD_DECL
#define FLEXQL_CACHE_FORWARD_DECL
typedef struct LRUCache LRUCache;
#endif

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  DATABASE MANAGER  (the server-wide registry of all named databases)
 *
 *  LESSON 5 adds query_cache:
 *    query_cache — one LRU cache shared across all databases.
 *                  Key = "DBNAME:SQL", value = serialised result rows.
 *                  Populated on SELECT; invalidated on INSERT/DROP.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct {
    Database       *databases[FLEXQL_MAX_DATABASES];
    int             db_count;
    pthread_mutex_t lock;         /* Protect CREATE/DROP DATABASE            */
    LRUCache       *query_cache;  /* Lesson 5: SELECT result cache           */
} DatabaseManager;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  QUERY AST — Abstract Syntax Tree Nodes
 *
 *  LESSON: After the SQL string is parsed, we build an AST.
 *    Source text:  "SELECT id, name FROM student WHERE id = 5;"
 *    becomes:
 *      QueryNode {
 *        type = QUERY_SELECT,
 *        select = { cols=["id","name"], table="student",
 *                   where = { col="id", op=OP_EQ, val="5" } }
 *      }
 *
 *  The executor then walks the AST instead of re-parsing the string.
 *  Separating parsing from execution is essential — it lets us add caching:
 *  we cache the AST, not the string.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/* Query types */
typedef enum {
    /* Table-level operations */
    QUERY_CREATE_TABLE  = 0,
    QUERY_INSERT        = 1,
    QUERY_SELECT        = 2,
    QUERY_INNER_JOIN    = 3,
    /* Database-level operations */
    QUERY_CREATE_DB     = 10,  /* CREATE DATABASE name;        */
    QUERY_USE_DB        = 11,  /* USE name;                    */
    QUERY_SHOW_DBS      = 12,  /* SHOW DATABASES;              */
    QUERY_DROP_DB       = 13,  /* DROP DATABASE name;          */
    QUERY_SHOW_TABLES   = 14,  /* SHOW TABLES;                 */
    QUERY_UNKNOWN       = 255
} QueryType;

/* Parameters for CREATE DATABASE / USE / DROP DATABASE */
typedef struct {
    char db_name[FLEXQL_MAX_NAME_LEN];
} DbNameParams;

/* Comparison operators in WHERE clause */
typedef enum {
    OP_EQ  = 0,   /* =   */
    OP_NEQ = 1,   /* !=  */
    OP_LT  = 2,   /* <   */
    OP_GT  = 3,   /* >   */
    OP_LTE = 4,   /* <=  */
    OP_GTE = 5    /* >=  */
} CompareOp;

/* A single WHERE condition: column OP value */
typedef struct {
    char       col_name[FLEXQL_MAX_NAME_LEN];
    CompareOp  op;
    char       value[FLEXQL_MAX_VARCHAR];   /* Raw string; cast at eval time */
    int        has_condition;               /* 0 if no WHERE clause          */
} WhereClause;

/* Column list for SELECT */
typedef struct {
    int    select_all;                             /* 1 if SELECT *           */
    int    col_count;
    char   col_names[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_NAME_LEN];
} SelectList;

/* CREATE TABLE parameters */
typedef struct {
    char      table_name[FLEXQL_MAX_NAME_LEN];
    ColumnDef columns[FLEXQL_MAX_COLUMNS];
    int       col_count;
} CreateTableParams;

/* ── Batch INSERT ─────────────────────────────────────────────────────────
 *
 * LESSON 7: Batch INSERT grammar:
 *   INSERT INTO t VALUES (r1_c1, r1_c2), (r2_c1, r2_c2), ..., (rN_c1, rN_c2);
 *
 * Design: row 0 stays in values[][] (existing layout, zero overhead for
 * single-row inserts).  Rows 1..N-1 are heap-allocated in extra_rows.
 * The executor frees extra_rows after processing the batch.
 *
 * WHY a pointer instead of a large 2D array?
 *   A static char[MAX_BATCH][64][256] embedded here would blow up
 *   InsertParams from 16 KB to 160 MB — too large for the stack.
 *   The pointer keeps InsertParams at 16 KB while batch data lives on
 *   the heap and is freed immediately after each INSERT statement.
 *
 * BACKWARD COMPAT: single-row INSERT sets batch_row_count=1, extra_rows=NULL.
 *   All existing test code continues to work unchanged.
 * ─────────────────────────────────────────────────────────────────────────*/
#define FLEXQL_MAX_BATCH_ROWS 10000  /* max tuples per batch INSERT          */

typedef struct {
    char   table_name[FLEXQL_MAX_NAME_LEN];
    char   values[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR]; /* row 0 (inline) */
    int    value_count;       /* number of columns per row                   */
    int    batch_row_count;   /* total row tuples (1 = single, N = batch)    */
    int    extra_capacity;    /* allocated slots in extra_rows (for doubling)*/
    time_t expiry;
    /* Rows 1..batch_row_count-1. Freed by executor after all rows inserted. */
    char (*extra_rows)[FLEXQL_MAX_COLUMNS][FLEXQL_MAX_VARCHAR];
} InsertParams;

/* SELECT / INNER JOIN parameters */
typedef struct {
    char        table_name[FLEXQL_MAX_NAME_LEN];
    SelectList  select;
    WhereClause where;
    /* JOIN-specific fields */
    int         is_join;
    char        join_table[FLEXQL_MAX_NAME_LEN];
    char        join_col_a[FLEXQL_MAX_NAME_LEN]; /* tableA.column */
    char        join_col_b[FLEXQL_MAX_NAME_LEN]; /* tableB.column */
} SelectParams;

/* Top-level query node */
typedef struct {
    QueryType type;
    union {
        CreateTableParams create;
        InsertParams      insert;
        SelectParams      select;
        DbNameParams      db;      /* For CREATE/USE/DROP DATABASE          */
    } params;
} QueryNode;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  NETWORK MESSAGE PROTOCOL
 *
 *  LESSON — Wire Protocol Design:
 *    Client and server communicate over a TCP socket.
 *    We need a "framing" protocol so the server knows where one message ends
 *    and the next begins (TCP is a stream, not a message-based protocol).
 *
 *    We use a simple LENGTH-PREFIX framing:
 *      [4 bytes: uint32_t message_length] [message_length bytes: payload]
 *
 *    The payload is a JSON-like or plain text string.
 *    This is similar to how PostgreSQL's wire protocol (libpq) works.
 *
 *  Message types:
 *    MSG_QUERY    — client → server: SQL string
 *    MSG_RESULT   — server → client: one row of data
 *    MSG_DONE     — server → client: end of result set
 *    MSG_ERROR    — server → client: error string
 *    MSG_OK       — server → client: non-SELECT success ack
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef enum {
    MSG_QUERY   = 0x01,
    MSG_RESULT  = 0x02,
    MSG_DONE    = 0x03,
    MSG_ERROR   = 0x04,
    MSG_OK      = 0x05,
    MSG_ABORT   = 0x06   /* client tells server to cancel current query      */
} MessageType;

/* Wire header — always 5 bytes: 1 byte type + 4 bytes length (big-endian)   */
#pragma pack(push, 1)
typedef struct {
    uint8_t  msg_type;     /* One of MessageType values                      */
    uint32_t payload_len;  /* Length of the following payload in bytes       */
} WireHeader;
#pragma pack(pop)

/* Maximum allowed payload size (8 MB) — protects against malformed clients  */
#define FLEXQL_MAX_PAYLOAD (8 * 1024 * 1024)

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  QUERY RESULT ROW (used on client side to hold one row of a SELECT result)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct ResultRow {
    int              col_count;
    char           **values;       /* Array of heap-allocated strings        */
    char           **col_names;    /* Array of heap-allocated column names   */
    struct ResultRow *next;
} ResultRow;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  INTERNAL FLEXQL HANDLE DEFINITION
 *  (the actual struct behind the opaque typedef in flexql.h)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
struct FlexQL {
    int    sockfd;                        /* TCP socket file descriptor       */
    char   host[256];                     /* Copy of the host string          */
    int    port;                          /* Server port                      */
    int    connected;                     /* 1 if connection is live          */
    char  *last_errmsg;                   /* Most recent error string         */
};

#endif /* FLEXQL_TYPES_H */