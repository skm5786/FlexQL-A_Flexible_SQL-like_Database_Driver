/**
 * fast_insert.h  —  Fast INSERT Parser
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * WHY THIS EXISTS
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * The normal INSERT path does:
 *   SQL string
 *     → lexer: tokenise every character
 *     → parser: fill InsertParams.values[col] with string copies
 *     → executor: call row_insert()
 *     → row_insert: call string_to_cell() for EVERY column of EVERY row
 *         → strtoll() / strtod() on each numeric string (AGAIN)
 *
 * For 10M rows × 5 columns = 50M strtoll/strtod calls in row_insert() alone,
 * PLUS another 50M implicit conversions in the lexer/parser to copy token text.
 *
 * This fast path:
 *   SQL string
 *     → fast_insert_parse(): scan for VALUES tuples, convert numbers ONCE
 *     → row_insert_cells(): write pre-converted CellValues directly into arena
 *
 * The savings:
 *   - Eliminate ALL strtoll/strtod calls in storage (already done in parser)
 *   - Eliminate all InsertParams heap allocations (extra_rows malloc)
 *   - Eliminate all string copies of numeric values
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * WHAT IT HANDLES
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * Pattern recognised:
 *   INSERT INTO <tablename> VALUES (...), (...), ...;
 *
 * Supports:
 *   - INT literals:     42, -7, 0
 *   - DECIMAL literals: 3.14, 1000.0, -0.5
 *   - VARCHAR literals: 'hello', 'it''s'  (SQL-escaped single quotes)
 *   - NULL keyword
 *   - Multiple tuples in one statement
 *
 * Does NOT handle (falls through to full parser):
 *   - Named column lists: INSERT INTO t (col1, col2) VALUES (...)
 *   - Expressions or function calls as values
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * INTEGRATION
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * Called from executor_execute() before parser_parse() for QUERY_INSERT.
 * If fast_insert_execute() returns FAST_INSERT_HANDLED, skip the parser.
 * If it returns FAST_INSERT_FALLBACK, use the normal parser path.
 */

#ifndef FLEXQL_FAST_INSERT_H
#define FLEXQL_FAST_INSERT_H

#include "common/types.h"
#include "storage/storage.h"
#include "storage/dbmanager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FAST_INSERT_HANDLED  1
#define FAST_INSERT_FALLBACK 0
#define FAST_INSERT_ERROR   -1

/**
 * fast_insert_execute — parse and execute an INSERT statement in one pass.
 *
 * @param sql        The full SQL string (must start with "INSERT" case-insensitively).
 * @param db         The current database (must be non-NULL).
 * @param client_fd  The client socket fd for sending MSG_OK / MSG_ERROR.
 * @param errmsg     OUT: error string on failure (caller frees).
 *
 * @return
 *   FAST_INSERT_HANDLED  — statement was fully executed (success or SQL error sent).
 *   FAST_INSERT_FALLBACK — statement not recognised by fast path; caller should
 *                          use the normal parser + executor.
 *   FAST_INSERT_ERROR    — a fatal communication error occurred.
 */
int fast_insert_execute(const char *sql, Database *db, int client_fd,
                        char **errmsg);

#ifdef __cplusplus
}
#endif
#endif /* FLEXQL_FAST_INSERT_H */