/**
 * flexql.h  —  FlexQL Public Client API
 *
 * LESSON: This is the ONLY header that users of the FlexQL library include.
 *         It follows the "opaque pointer / handle" pattern used in real
 *         database drivers (SQLite, libpq, ODBC).
 *
 * The opaque handle pattern works like this:
 *   - We forward-declare   "struct FlexQL"   here (no members visible).
 *   - The actual definition lives in flexql_internal.h (server-side / impl).
 *   - Users hold a  FlexQL*  but can NEVER dereference it to read members.
 *   - All access goes through the API functions below.
 *
 * Why?
 *   1. ABI stability — we can change internal fields without breaking callers.
 *   2. Encapsulation  — internal state is not accidentally read/written.
 *   3. Mirrors real drivers: sqlite3*, MYSQL*, PGconn* are all opaque.
 */

#ifndef FLEXQL_H
#define FLEXQL_H

/* ── C++ compatibility ───────────────────────────────────────────────────────
 * When this header is included in a C++ translation unit, we wrap every
 * declaration in extern "C" so the linker finds C-mangled symbols.
 * This lets you call the API from both C and C++ code.
 * ──────────────────────────────────────────────────────────────────────────── */
#ifdef __cplusplus
extern "C" {
#endif

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  OPAQUE DATABASE HANDLE
 *
 *  LESSON: "typedef struct FlexQL FlexQL" creates a type alias so callers
 *  write  FlexQL*  instead of  struct FlexQL*.  The struct body is defined
 *  only in flexql_internal.h which is NOT shipped to library users.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef struct FlexQL FlexQL;

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  RETURN / ERROR CODES
 *
 *  LESSON: We use #define integer constants (not an enum) so the codes are
 *  compatible with plain C89 and can be compared with == in both languages.
 *  Additional codes (e.g. FLEXQL_TIMEOUT, FLEXQL_NOMEM) may be added later
 *  without changing the ABI.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
#define FLEXQL_OK       0   /* Operation completed successfully               */
#define FLEXQL_ERROR    1   /* Generic error; check errmsg for details        */
#define FLEXQL_NOMEM    2   /* Memory allocation failure                      */
#define FLEXQL_TIMEOUT  3   /* Network timeout waiting for server response    */
#define FLEXQL_BUSY     4   /* Server busy / resource locked                  */
#define FLEXQL_ABORT    5   /* Query aborted by callback returning 1          */

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  API 1 — flexql_open
 *
 *  Establishes a TCP connection to the FlexQL server and allocates a new
 *  database handle.
 *
 *  Parameters:
 *    host  — IPv4 address or hostname of the server (e.g. "127.0.0.1").
 *    port  — TCP port the server listens on       (e.g. 9000).
 *    db    — OUT: pointer-to-pointer; on success *db points to a new handle.
 *
 *  Returns:
 *    FLEXQL_OK    on success.
 *    FLEXQL_ERROR if the connection cannot be established or host is NULL.
 *    FLEXQL_NOMEM if heap allocation for the handle fails.
 *
 *  LESSON: The double-pointer  FlexQL **db  is the idiomatic C way to return
 *  a newly allocated object.  Caller passes  &mydb  and we write  *db = ...
 *  SQLite's sqlite3_open() uses exactly this pattern.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int flexql_open(const char *host, int port, FlexQL **db);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  API 2 — flexql_close
 *
 *  Closes the TCP connection and frees all memory owned by the handle.
 *  After this call  db  is a dangling pointer — set it to NULL afterwards.
 *
 *  Parameters:
 *    db — a valid handle previously returned by flexql_open.
 *
 *  Returns:
 *    FLEXQL_OK    always (even if db is NULL — safe to call on NULL).
 *    FLEXQL_ERROR if db is invalid (non-NULL but internally corrupted).
 *
 *  LESSON: Always return a code even from "close" — callers may want to log
 *  a failed shutdown.  Accepting NULL silently is defensive programming.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int flexql_close(FlexQL *db);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  CALLBACK PROTOTYPE (used by flexql_exec)
 *
 *  The callback is invoked ONCE PER ROW for SELECT results.
 *
 *  Parameters:
 *    data        — the same void* the caller passed as "arg" to flexql_exec.
 *    columnCount — number of columns in this row.
 *    values      — array of C-strings, one per column (NULL if SQL NULL).
 *    columnNames — array of column name strings in the same order.
 *
 *  Returns:
 *    0  — continue; keep sending more rows.
 *    1  — abort; stop sending rows, flexql_exec returns FLEXQL_ABORT.
 *
 *  LESSON: Passing a void* "user data" pointer through to the callback is the
 *  classic C technique for closures.  The caller can cast it to any struct it
 *  likes (e.g. a result accumulator).  This avoids global variables.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
typedef int (*flexql_callback)(void *data,
                               int   columnCount,
                               char **values,
                               char **columnNames);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  API 3 — flexql_exec
 *
 *  Sends one SQL statement to the server, waits for the response, and for
 *  each result row calls  callback(arg, colCount, values, colNames).
 *
 *  Parameters:
 *    db       — open connection handle.
 *    sql      — NULL-terminated SQL string.
 *    callback — function called per result row; NULL for non-SELECT queries.
 *    arg      — user data forwarded to every callback invocation.
 *    errmsg   — OUT: if non-NULL and execution fails, *errmsg is set to a
 *               heap-allocated error string.  Free it with flexql_free().
 *
 *  Returns:
 *    FLEXQL_OK    on success.
 *    FLEXQL_ERROR on parse / execution failure (details in *errmsg).
 *    FLEXQL_ABORT if callback returned 1.
 *    FLEXQL_NOMEM on allocation failure.
 *
 *  LESSON: The errmsg double-pointer pattern (same as SQLite) allows the
 *  function to allocate a descriptive error message on the heap and hand
 *  ownership to the caller.  The caller must free it with flexql_free().
 *  If you use a plain char* out-param you'd need a fixed-size buffer and
 *  risk truncating the message.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int flexql_exec(FlexQL         *db,
                const char     *sql,
                flexql_callback callback,
                void           *arg,
                char          **errmsg);

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  API 4 — flexql_free
 *
 *  Frees memory that was allocated by the FlexQL library (e.g. errmsg).
 *  Always use this instead of plain free() for library-owned pointers,
 *  because the library may use a custom allocator.
 *
 *  Parameters:
 *    ptr — pointer to free; NULL is safe (no-op).
 *
 *  LESSON: Providing a matching free function is best practice for any C
 *  library that allocates memory on behalf of the caller.  It guarantees the
 *  same allocator that malloc'd the block is used to free it — important when
 *  the library is compiled as a DLL with its own CRT.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
void flexql_free(void *ptr);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* FLEXQL_H */