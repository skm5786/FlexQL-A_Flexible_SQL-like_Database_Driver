/**
 * main.cpp  —  FlexQL REPL Client
 *
 * LESSON: REPL = Read–Eval–Print Loop.
 *   Every interactive command-line tool (bash, python, sqlite3, psql) is a
 *   REPL at its core:
 *     1. READ   — get input from the user
 *     2. EVAL   — send it to the engine / server
 *     3. PRINT  — display the result
 *     4. LOOP   — go back to step 1
 *
 * This file is the client binary entry point.  It:
 *   - Accepts server address/port from command-line arguments
 *   - Calls flexql_open() to connect
 *   - Runs the REPL loop
 *   - Supports multi-line statements (buffers until ';' is seen)
 *   - Handles ".exit" / ".quit" meta-commands
 *   - Calls flexql_close() on exit
 *
 * Build:  g++ -o flexql-client main.cpp flexql_api.cpp -I../../include
 * Run:    ./flexql-client 127.0.0.1 9000
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <unistd.h>

#include "flexql.h"
#include "common/types.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  CALLBACK — prints every SELECT result row to stdout.
 *
 *  LESSON: The callback is the "output side" of the API.
 *          We receive columnCount, values[], and columnNames[].
 *          Here we just print "COLNAME = VALUE" for each column, then a
 *          blank line between rows — matching the expected output format.
 *
 *  Returning 0 tells flexql_exec to keep sending more rows.
 *  Returning 1 would abort the query (useful for LIMIT emulation).
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static int print_row_callback(void       *data,
                               int         columnCount,
                               char      **values,
                               char      **columnNames) {
    (void)data;  /* Unused in this simple callback                           */

    for (int i = 0; i < columnCount; ++i) {
        const char *val = values[i] ? values[i] : "NULL";
        printf("%s = %s\n", columnNames[i], val);
    }
    printf("\n");  /* Blank line between rows                               */
    return 0;      /* Continue                                               */
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  META COMMANDS — special commands starting with '.'
 *
 *  LESSON: Database CLIs (sqlite3, psql) distinguish between:
 *    - SQL statements  (sent to the engine)
 *    - Meta commands   (handled locally by the client)
 *  ".exit", ".quit", ".help", ".tables" are meta commands.
 *  They don't get sent to the server.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static bool handle_meta_command(const std::string &cmd) {
    if (cmd == ".exit" || cmd == ".quit") {
        printf("Connection closed\n");
        return true;   /* Signal the REPL to exit                            */
    }
    if (cmd == ".help") {
        printf("FlexQL REPL commands:\n");
        printf("  .help           Show this help\n");
        printf("  .exit / .quit   Close the connection and exit\n");
        printf("\nSQL commands supported:\n");
        printf("  CREATE TABLE table_name (col TYPE [PRIMARY KEY] [NOT NULL], ...);\n");
        printf("  INSERT INTO table_name VALUES (val1, val2, ...);\n");
        printf("  SELECT * FROM table_name [WHERE col = val];\n");
        printf("  SELECT col1, col2 FROM table_name [WHERE col = val];\n");
        printf("  SELECT * FROM tableA INNER JOIN tableB ON a.col = b.col;\n\n");
        return false;
    }
    printf("Unknown command: %s  (try .help)\n", cmd.c_str());
    return false;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  REPL LOOP
 *
 *  LESSON — Multi-line statement buffering:
 *    SQL statements can span multiple lines:
 *      flexql> SELECT *
 *         ...> FROM student
 *         ...> WHERE id = 5;
 *
 *    We accumulate input lines in a std::string buffer until we see a ';'.
 *    Only then do we send the complete statement to the server.
 *    This is the same approach used by sqlite3's REPL.
 *
 *  LESSON — Prompt:
 *    "flexql> " on first line, "   ...> " on continuation lines.
 *    isatty(STDIN_FILENO) lets us detect whether stdin is a terminal or
 *    a redirected file — in the latter case we suppress the prompt.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
static void run_repl(FlexQL *db) {
    std::string buffer;   /* Accumulated SQL text                           */
    char        line[4096];
    bool        first_line = true;

    bool interactive = isatty(STDIN_FILENO);

    while (true) {
        /* Print prompt */
        if (interactive) {
            if (first_line) printf("flexql> ");
            else            printf("   ...> ");
            fflush(stdout);
        }

        /* Read one line */
        if (!fgets(line, sizeof(line), stdin)) {
            /* EOF — user pressed Ctrl-D                                     */
            printf("\nConnection closed\n");
            break;
        }

        /* Strip trailing newline */
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') line[--len] = '\0';

        std::string trimmed = line;
        /* Trim leading whitespace */
        size_t start = trimmed.find_first_not_of(" \t");
        if (start != std::string::npos) trimmed = trimmed.substr(start);
        else                             trimmed.clear();

        if (trimmed.empty()) continue;

        /* Meta commands must be on their own line and start with '.'       */
        if (first_line && !trimmed.empty() && trimmed[0] == '.') {
            bool should_exit = handle_meta_command(trimmed);
            if (should_exit) break;
            continue;
        }

        /* Accumulate into buffer */
        if (!buffer.empty()) buffer += ' ';
        buffer += trimmed;
        first_line = false;

        /* Check whether the statement is complete (ends with ';')          */
        size_t semi = buffer.rfind(';');
        if (semi == std::string::npos) continue;  /* Not complete yet       */

        /* We have a complete statement — send it to the server             */
        std::string stmt = buffer.substr(0, semi + 1);
        buffer.clear();
        first_line = true;

        char *errmsg = nullptr;
        int rc = flexql_exec(db, stmt.c_str(), print_row_callback,
                             nullptr, &errmsg);
        if (rc != FLEXQL_OK && rc != FLEXQL_ABORT) {
            fprintf(stderr, "Error: %s\n",
                    errmsg ? errmsg : "(unknown error)");
            flexql_free(errmsg);
        }
    }
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  MAIN
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */
int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <host> <port>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 9000\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int         port = atoi(argv[2]);

    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Invalid port: %s\n", argv[2]);
        return 1;
    }

    /* ── Connect ────────────────────────────────────────────────────────── */
    FlexQL *db = nullptr;
    int rc = flexql_open(host, port, &db);
    if (rc != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to FlexQL server at %s:%d\n",
                host, port);
        return 1;
    }

    printf("Connected to FlexQL server\n");

    /* ── Run the REPL ───────────────────────────────────────────────────── */
    run_repl(db);

    /* ── Disconnect ─────────────────────────────────────────────────────── */
    flexql_close(db);
    return 0;
}