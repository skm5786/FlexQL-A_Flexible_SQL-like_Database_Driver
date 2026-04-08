/**
 * storage.h  —  Table & Row Storage API
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * LESSON 3 — THE STORAGE ENGINE
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * The storage engine is the heart of the database.  It is responsible for:
 *
 *   1. TABLE MANAGEMENT    — creating tables, finding them, freeing them
 *   2. ROW INSERTION       — type-checking values and appending rows
 *   3. TABLE SCANNING      — iterating rows with optional WHERE filtering
 *   4. VALUE CONVERSION    — string → typed CellValue and back
 *
 * DATA FLOW:
 *   Client SQL string
 *     → Lexer (tokens)
 *       → Parser (QueryNode AST)
 *         → Executor (calls storage API)          ← we are here
 *           → Storage (Table + Row structs in RAM)
 *             → Wire serialiser (sends rows to client)
 *
 * DESIGN DECISIONS (must be explained in your documentation):
 *
 *   ROW-MAJOR STORAGE:
 *     All columns of one row are stored together in a Row struct.
 *     Rows are linked in a singly-linked list hanging off Table.head.
 *     New rows are prepended (O(1) insert).
 *     Scan always walks head → tail (insertion order reversed, which is
 *     acceptable for a simple engine — production DBs use a heap file).
 *
 *   TYPE ENFORCEMENT:
 *     INSERT values arrive as raw strings from the parser.
 *     storage converts each string to the correct CellValue type using
 *     the column schema.  Bad types produce an error before insertion.
 *
 *   EXPIRATION:
 *     Every row has an  expiry  field (Unix timestamp, 0 = never expires).
 *     During a scan, rows whose expiry < current time are skipped — they
 *     are "soft-deleted".  A background thread (Lesson 7) will eventually
 *     hard-delete them to reclaim memory.
 *
 *   CONCURRENCY:
 *     Each Table has its own pthread_mutex_t lock.
 *     INSERT locks the table, appends the row, unlocks.
 *     SELECT locks the table, scans, unlocks.
 *     This is a simple "table-level lock" — good enough for correctness;
 *     production DBs use row-level MVCC for better concurrency.
 */

#ifndef FLEXQL_STORAGE_H
#define FLEXQL_STORAGE_H

#include "common/types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  TABLE MANAGEMENT
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * table_create — allocate a new Table, register it in db, return it.
 *
 * @param db        The database to add the table to.
 * @param name      Table name (will be uppercased internally).
 * @param cols      Array of column definitions from the parser.
 * @param col_count Number of columns.
 * @param errmsg    OUT: error string on failure (caller frees).
 * @return  Pointer to the new Table, or NULL on error.
 *
 * Errors:
 *   - Table with same name already exists in db
 *   - db->table_count >= FLEXQL_MAX_TABLES
 *   - Memory allocation failure
 */
Table *table_create(Database   *db,
                    const char *name,
                    ColumnDef  *cols,
                    int         col_count,
                    char      **errmsg);

/**
 * table_find — look up a table by name in a database (case-insensitive).
 * Returns NULL if not found. Thread-safe (acquires db->schema_lock).
 */
Table *table_find(Database *db, const char *name);

/**
 * table_free — free all rows and then the Table itself.
 * Caller must hold db->schema_lock when removing from the db array.
 */
void table_free(Table *table);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  ROW INSERTION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * row_insert — insert one row into a table.
 *
 * @param table       Target table.
 * @param str_values  Array of raw string values from the parser.
 *                    Length must equal table->col_count.
 *                    An empty string ("") represents SQL NULL.
 * @param val_count   Number of values provided.
 * @param expiry      Unix timestamp after which the row is invisible
 *                    (0 = never expires).
 * @param errmsg      OUT: error string on failure (caller frees).
 * @return  0 on success, -1 on error.
 *
 * LESSON: Type enforcement happens here.
 *   Each str_values[i] is converted to the type declared in
 *   table->schema[i].type.  If conversion fails (e.g. "abc" into INT),
 *   the whole insert is rejected and no row is added.
 */
int row_insert(Table      *table,
               const char  str_values[][FLEXQL_MAX_VARCHAR],
               int         val_count,
               time_t      expiry,
               char      **errmsg);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  TABLE SCANNING
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * ScanCallback — called once per matching row during a table scan.
 *
 * @param row   The current row (read-only — do not modify or free).
 * @param arg   User-provided context pointer.
 * @return  0 to continue scanning, 1 to stop early.
 */
typedef int (*ScanCallback)(const Row *row, void *arg);

/**
 * table_scan — iterate every live (non-expired) row in the table.
 *
 * @param table    Table to scan.
 * @param where    Optional WHERE clause (pass has_condition=0 for none).
 * @param cb       Callback invoked for each matching row.
 * @param arg      Forwarded to every cb() call.
 * @return  Number of rows visited (not counting filtered/expired ones).
 *
 * LESSON: This is a FULL TABLE SCAN — O(n) in the number of rows.
 *   For 10M rows this will be slow without an index.
 *   In Lesson 5 we add a primary key index so WHERE pk=value is O(1).
 */
int table_scan(Table       *table,
               const WhereClause *where,
               ScanCallback cb,
               void        *arg);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  VALUE CONVERSION UTILITIES
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * cell_to_string — render a CellValue as a null-terminated string.
 * Used when serialising rows to send over the wire.
 *
 * @param cell    Source cell.
 * @param buf     Output buffer.
 * @param bufsz   Size of buf.
 * @return  buf (always null-terminated).
 */
char *cell_to_string(const CellValue *cell, char *buf, size_t bufsz);

/**
 * string_to_cell — parse a raw string into a CellValue of the given type.
 * An empty string is treated as SQL NULL.
 *
 * @return  0 on success, -1 if the string cannot be parsed as that type.
 */
int string_to_cell(const char *str, ColumnType type, CellValue *out,
                   char **errmsg);

/**
 * cell_matches_where — test whether a CellValue satisfies a WHERE condition.
 * @return  1 if the cell matches, 0 if not.
 */
int cell_matches_where(const CellValue *cell, const WhereClause *where);

/**
 * row_free — free all heap memory owned by a Row (varchar strings + cells).
 * Does NOT free the Row struct itself.
 */
void row_free_contents(Row *row);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_STORAGE_H */