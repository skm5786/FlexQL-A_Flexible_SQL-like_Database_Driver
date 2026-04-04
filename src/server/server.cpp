/**
 * server.cpp  —  FlexQL Multithreaded Server
 *
 * ═══════════════════════════════════════════════════════════════════════
 * ROOT CAUSE FIX — "Nothing stored in data/ folder"
 * ═══════════════════════════════════════════════════════════════════════
 *
 * PROBLEM:
 *   The benchmark (benchmark_flexql.cpp) never sends CREATE DATABASE or
 *   USE before issuing CREATE TABLE / INSERT.  The server previously
 *   handled this by auto-creating a _SESSION_<fd> database per connection
 *   and setting that as current_db.
 *
 *   The WAL layer has this check in wal.cpp:
 *
 *     int wal_is_persistent(const char *db_name) {
 *         return (strncmp(db_name, "_SESSION_", 9) != 0);
 *     }
 *
 *   Because _SESSION_<fd> starts with "_SESSION_", wal_is_persistent()
 *   returns 0.  Every wal_write_create_table() and wal_write_insert_batch()
 *   call begins with:
 *
 *     if (!wal_is_persistent(db_name)) return 0;   ← skips all disk writes
 *
 *   Result: data/ stays completely empty.  Server restart = blank state.
 *
 * FIX — Two-part server-side change, benchmark untouched:
 *
 *   1. On connection: auto-create a PERSISTENT database named "DEFAULT"
 *      (no underscore prefix, passes wal_is_persistent()) and set it as
 *      current_db.  The benchmark immediately gets a WAL-backed database
 *      without sending any setup SQL.
 *
 *   2. On disconnect: do NOT drop "DEFAULT".  It is a shared persistent
 *      database that survives connections.  Only the in-memory state for
 *      that connection's context pointer is released (the Database* itself
 *      stays alive in DatabaseManager).
 *
 * DESIGN NOTES:
 *
 *   - "DEFAULT" is created once.  dbmgr_create() returns an error if it
 *     already exists; we silently ignore that error — it just means the
 *     database was already there from a previous connection or WAL recovery.
 *
 *   - WAL recovery on startup replays data/DEFAULT/*.wal and rebuilds all
 *     tables that were ever created via the benchmark.
 *
 *   - Clients that explicitly send CREATE DATABASE mydb; USE mydb; still
 *     work correctly — USE switches current_db away from DEFAULT and all
 *     subsequent operations go into their chosen database.
 *
 *   - The REPL client (flexql-client) is unaffected — it also starts in
 *     DEFAULT but can USE any database it creates.
 *
 *   - Multiple simultaneous benchmark connections all share DEFAULT.  This
 *     is correct: they create tables with unique names (BIG_USERS,
 *     TEST_USERS, TEST_ORDERS) and there is no cross-client naming conflict
 *     in the TA benchmark.  The table-level rwlock handles concurrency.
 *
 *   - "DEFAULT" is registered in data/_registry on first creation, so
 *     wal_recover() finds it on the next startup.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <csignal>
#include <cctype>

#include <sys/socket.h>
#include <sys/select.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>

#include "common/types.h"
#include "parser/parser.h"
#include "storage/dbmanager.h"
#include "storage/wal.h"
#include "expiration/expiration.h"

/* Forward declaration */
int executor_execute(DatabaseManager *mgr, Database **current_db,
                     const QueryNode *query, int client_fd, char **errmsg);

/* ── The name of the auto-created persistent database ───────────────────
 * Must NOT start with "_SESSION_" — that prefix is what wal_is_persistent()
 * uses to decide whether to skip WAL writes.
 * "DEFAULT" is short, obvious, and passes the persistence check.
 */
#define DEFAULT_DB_NAME  "DEFAULT"

/* ── Global database manager ────────────────────────────────────────────── */
static DatabaseManager g_manager;

/* ── Client context ─────────────────────────────────────────────────────── */
typedef struct {
    int              client_fd;
    char             client_ip[64];
    DatabaseManager *mgr;
    Database        *current_db;   /* starts pointing at DEFAULT */
} ClientContext;

/* ── Wire protocol helpers ──────────────────────────────────────────────── */
static int send_all(int fd, const void *buf, size_t len) {
    const char *p = static_cast<const char *>(buf);
    size_t rem = len;
    while (rem > 0) {
        ssize_t n = send(fd, p, rem, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        p += n; rem -= (size_t)n;
    }
    return 0;
}
static int recv_all(int fd, void *buf, size_t len) {
    char  *p   = static_cast<char *>(buf);
    size_t rem = len;
    while (rem > 0) {
        ssize_t n = recv(fd, p, rem, 0);
        if (n <= 0) return -1;
        p += n; rem -= (size_t)n;
    }
    return 0;
}
static int send_message(int fd, MessageType type,
                         const char *payload, uint32_t plen) {
    WireHeader hdr;
    hdr.msg_type    = (uint8_t)type;
    hdr.payload_len = htonl(plen);
    if (send_all(fd, &hdr, sizeof(hdr)) != 0) return -1;
    if (plen > 0 && send_all(fd, payload, plen) != 0) return -1;
    return 0;
}
static int recv_message(int fd, MessageType *type,
                         char **payload, uint32_t *plen) {
    *payload = nullptr; *plen = 0;
    WireHeader hdr;
    if (recv_all(fd, &hdr, sizeof(hdr)) != 0) return -1;
    *type = static_cast<MessageType>(hdr.msg_type);
    uint32_t l = ntohl(hdr.payload_len);
    if (l > FLEXQL_MAX_PAYLOAD) return -1;
    char *buf = static_cast<char *>(malloc(l + 1));
    if (!buf) return -1;
    if (l > 0 && recv_all(fd, buf, l) != 0) { free(buf); return -1; }
    buf[l] = '\0';
    *payload = buf; *plen = l;
    return 0;
}

/* ── Ensure the DEFAULT database exists (idempotent) ────────────────────
 *
 * Called once at server startup (after WAL recovery) AND at the start of
 * every client_worker thread.  If DEFAULT was already recovered from WAL
 * or created by a previous call, dbmgr_create() returns an error which we
 * silently discard — the database is already there.
 *
 * Returns a pointer to the DEFAULT Database, or NULL on unexpected failure.
 */
static Database *ensure_default_db(DatabaseManager *mgr) {
    /* Try to create — silently ignore "already exists" */
    char *err = nullptr;
    dbmgr_create(mgr, DEFAULT_DB_NAME, &err);
    if (err) free(err);   /* "already exists" is not an error for us */

    Database *db = dbmgr_find(mgr, DEFAULT_DB_NAME);
    if (!db) {
        fprintf(stderr, "[server] ERROR: cannot find/create DEFAULT database\n");
    }
    return db;
}

/* ── Worker thread ──────────────────────────────────────────────────────── */
static void *client_worker(void *arg) {
    ClientContext *ctx = static_cast<ClientContext *>(arg);
    int              fd  = ctx->client_fd;
    DatabaseManager *mgr = ctx->mgr;
    char             ip[64];
    strncpy(ip, ctx->client_ip, sizeof(ip));
    free(ctx);

    /* ── FIX: Set current_db to DEFAULT (a persistent, WAL-backed DB) ───
     *
     * Previously: current_db = _SESSION_<fd>  → wal_is_persistent() = 0
     *                                          → NO disk writes ever
     *
     * Now:        current_db = DEFAULT         → wal_is_persistent() = 1
     *                                          → all CREATE TABLE + INSERT
     *                                             written to data/DEFAULT/
     *
     * ensure_default_db() is idempotent — if DEFAULT already exists (from
     * a previous connection or from WAL recovery) it just returns it.
     */
    Database *cur = ensure_default_db(mgr);
    if (!cur) {
        printf("[server] ERROR: no DEFAULT db, closing connection fd=%d\n", fd);
        close(fd);
        return nullptr;
    }

    printf("[server] Client connected: %s (fd=%d, db=%s)\n",
           ip, fd, DEFAULT_DB_NAME);

    while (true) {
        MessageType type;
        char       *payload = nullptr;
        uint32_t    plen    = 0;

        if (recv_message(fd, &type, &payload, &plen) != 0) break;
        if (type == MSG_ABORT) { free(payload); continue; }
        if (type != MSG_QUERY) { free(payload); break; }

        QueryNode  qnode{};
        char      *errmsg = nullptr;

        int rc = parser_parse(payload, &qnode, &errmsg);
        free(payload);

        if (rc != 0) {
            const char *msg = errmsg ? errmsg : "Parse error";
            send_message(fd, MSG_ERROR, msg, (uint32_t)strlen(msg));
            free(errmsg);
            continue;
        }

        rc = executor_execute(mgr, &cur, &qnode, fd, &errmsg);
        (void)rc;
        free(errmsg);
    }

    /* ── Do NOT drop DEFAULT on disconnect ───────────────────────────────
     *
     * Previously we called dbmgr_drop(_SESSION_<fd>) here.
     * DEFAULT is shared and persistent — dropping it would delete all
     * tables and data for every other connected client.
     *
     * Nothing to clean up: DEFAULT persists in DatabaseManager and on disk.
     */
    close(fd);
    printf("[server] Client disconnected: %s\n", ip);
    return nullptr;
}

/* ── Signal handler ─────────────────────────────────────────────────────── */
static volatile int g_running = 1;
static void sigint_handler(int sig) {
    (void)sig;
    g_running = 0;
    const char *msg = "\n[server] Shutting down...\n";
    write(STDERR_FILENO, msg, strlen(msg));
}

/* ── main ───────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    int port = 9000;
    if (argc >= 2) port = atoi(argv[1]);

    dbmgr_init(&g_manager);

    /* Recover persistent data from WAL (replays data/DEFAULT/*.wal etc.) */
    int recovered = wal_recover(&g_manager);
    printf("[server] WAL recovery complete: %d database(s) restored\n",
           recovered);

    /* ── Ensure DEFAULT database exists after recovery ─────────────────
     * If DEFAULT was in data/_registry, wal_recover() already created it.
     * If this is a fresh install (no data/ yet), create it now and register
     * it in the registry so future restarts recover it.
     */
    {
        char *err = nullptr;
        int rc = dbmgr_create(&g_manager, DEFAULT_DB_NAME, &err);
        if (rc == 0) {
            /* Freshly created — register in WAL registry */
            wal_register_db(DEFAULT_DB_NAME);
            printf("[server] Created persistent DEFAULT database "
                   "(data/%s/)\n", DEFAULT_DB_NAME);
        } else {
            /* Already existed from recovery — that's fine */
            printf("[server] DEFAULT database recovered from WAL "
                   "(data/%s/)\n", DEFAULT_DB_NAME);
        }
        if (err) free(err);
    }

    /* Start background expiry thread */
    expiry_start(&g_manager);

    signal(SIGINT,  sigint_handler);
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listen_fd, 128) < 0) { perror("listen"); return 1; }

    printf("[server] FlexQL server listening on port %d\n", port);
    printf("[server] All tables created via benchmark stored in "
           "data/%s/\n", DEFAULT_DB_NAME);

    while (g_running) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(listen_fd, &readfds);

        struct timeval tv; tv.tv_sec = 1; tv.tv_usec = 0;
        int ready = select(listen_fd + 1, &readfds,
                           nullptr, nullptr, &tv);
        if (ready < 0) { if (errno == EINTR) continue; perror("select"); break; }
        if (ready == 0) continue;

        struct sockaddr_in client_addr{};
        socklen_t          client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd,
                               (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) { if (g_running) perror("accept"); continue; }

        /* TCP performance settings */
        int tcp_flag = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
                   &tcp_flag, sizeof(tcp_flag));
        int buf_size = 256 * 1024;
        setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF,
                   &buf_size, sizeof(buf_size));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF,
                   &buf_size, sizeof(buf_size));

        ClientContext *ctx = static_cast<ClientContext*>(
                                 calloc(1, sizeof(ClientContext)));
        ctx->client_fd  = client_fd;
        ctx->mgr        = &g_manager;
        ctx->current_db = nullptr;   /* set inside client_worker */
        inet_ntop(AF_INET, &client_addr.sin_addr,
                  ctx->client_ip, sizeof(ctx->client_ip));

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&tid, &attr, client_worker, ctx);
        pthread_attr_destroy(&attr);
    }

    close(listen_fd);
    expiry_stop();
    dbmgr_destroy(&g_manager);
    printf("[server] Shutdown complete.\n");
    return 0;
}