/**
 * server.cpp  —  FlexQL Multithreaded Server  (All bugs fixed)
 *
 * FIXES IN THIS VERSION:
 *
 * FIX 1 — FAST INSERT FALLBACK BUG:
 *   Previous code freed `payload` BEFORE calling the fallback parser path.
 *   When fast_insert_execute() returned FAST_INSERT_FALLBACK, we called
 *   `free(payload)` and then `continue` — the SQL was lost entirely.
 *   Fix: copy the SQL into a local buffer before calling fast_insert_execute(),
 *   OR don't free until after the fallback has been tried.
 *   We now use a local copy so the fallback always has the SQL.
 *
 * FIX 2 — WAL PERSISTENCE FOR USER-CREATED DATABASES:
 *   User databases (SCHOOL, MYDB, etc.) need WAL to persist across restarts.
 *   wal_is_persistent() already returns 1 for anything not starting with
 *   "_SESSION_", so SCHOOL, MYDB, etc. ARE persistent.
 *   The real problem was that user DBs were never registered in data/_registry
 *   because only BENCH_<fd> databases called wal_register_db().
 *   Fix: executor_execute() QUERY_CREATE_DB now calls wal_register_db().
 *   Also, user databases must NOT be dropped on client disconnect — only
 *   BENCH_<fd> session databases should be cleaned up.
 *
 * FIX 3 — benchmark_after_insert SUPPORT:
 *   benchmark_after_insert expects BIG_USERS table in a persistent named DB.
 *   It connects fresh, runs queries against BIG_USERS.
 *   Since benchmark_flexql creates BIG_USERS inside BENCH_<fd> which gets
 *   deleted on disconnect, benchmark_after_insert finds nothing.
 *   Fix: benchmark_after_insert must USE a named persistent DB.
 *   We document this and provide a wrapper script.
 *   The server itself does not need changes for this — see docs.
 *
 * ARCHITECTURE — Session DB design:
 *   BENCH_<fd>  — ephemeral per-connection DB, cleaned up on disconnect.
 *                 Created automatically; all benchmark runs get clean slate.
 *   USER DBs    — persistent, survive restart via WAL.
 *                 Created via "CREATE DATABASE name;", persisted forever.
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <csignal>
#include <cctype>
#include <dirent.h>

#include <sys/socket.h>
#include <sys/select.h>
#include <sys/stat.h>
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
#include "network/fast_insert.h"

int executor_execute(DatabaseManager *mgr, Database **current_db,
                     const QueryNode *query, int client_fd, char **errmsg);

static DatabaseManager g_manager;

typedef struct {
    int              client_fd;
    char             client_ip[64];
    DatabaseManager *mgr;
    Database        *current_db;
    char             session_db_name[64];
} ClientContext;

/* ── Wire helpers ───────────────────────────────────────────────────────── */
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

/* ── Delete all WAL files for a database directory ──────────────────────── */
static void delete_wal_files(const char *db_name) {
    char dir[512];
    snprintf(dir, sizeof(dir), "data/%s", db_name);

    DIR *d = opendir(dir);
    if (!d) return;

    struct dirent *ent;
    while ((ent = readdir(d)) != nullptr) {
        const char *fname = ent->d_name;
        size_t flen = strlen(fname);
        if (flen < 5 || strcmp(fname + flen - 4, ".wal") != 0) continue;

        char fpath[512];
        snprintf(fpath, sizeof(fpath), "data/%s/%s", db_name, fname);
        unlink(fpath);
    }
    closedir(d);
    rmdir(dir);
}

/* ── is_insert_statement — cheap prefix check for the fast path ─────────── */
static inline int is_insert_statement(const char *sql) {
    while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r'))
        sql++;
    return (strncasecmp(sql, "INSERT", 6) == 0);
}

/* ── is_bench_session_db — true for auto-created BENCH_<fd> DBs only ────── */
static inline int is_bench_session_db(const char *name) {
    return (strncmp(name, "BENCH_", 6) == 0);
}

/* ── Worker thread ──────────────────────────────────────────────────────── */
static void *client_worker(void *arg) {
    ClientContext *ctx = static_cast<ClientContext *>(arg);
    int              fd  = ctx->client_fd;
    DatabaseManager *mgr = ctx->mgr;
    char             session_db[64];
    strncpy(session_db, ctx->session_db_name, sizeof(session_db));
    char             ip[64];
    strncpy(ip, ctx->client_ip, sizeof(ip));
    free(ctx);

    /* ── Create a fresh per-connection ephemeral database ───────────────── */
    char *setup_err = nullptr;
    dbmgr_create(mgr, session_db, &setup_err);
    free(setup_err);

    /* Register in WAL registry so crash recovery finds it */
    wal_register_db(session_db);

    Database *cur = dbmgr_find(mgr, session_db);
    if (!cur) {
        printf("[server] ERROR: cannot create session DB %s fd=%d\n",
               session_db, fd);
        close(fd);
        return nullptr;
    }

    printf("[server] Client connected: %s (fd=%d, db=%s)\n",
           ip, fd, session_db);

    while (true) {
        MessageType type;
        char       *payload = nullptr;
        uint32_t    plen    = 0;

        if (recv_message(fd, &type, &payload, &plen) != 0) break;
        if (type == MSG_ABORT) { free(payload); continue; }
        if (type != MSG_QUERY) { free(payload); break; }

        /* ── PERF: Fast INSERT path ─────────────────────────────────────────
         *
         * BUG FIX: In the previous version, `payload` was freed BEFORE
         * the fallback parser path could use it.  The code was:
         *
         *   int fi_rc = fast_insert_execute(payload, cur, fd, nullptr);
         *   free(payload);   // <-- payload freed here!
         *   if (fi_rc == FAST_INSERT_FALLBACK) {
         *       continue;    // <-- SQL is gone, fallback impossible
         *   }
         *
         * Fix: We keep payload alive until after the fallback parse.
         * For FAST_INSERT_HANDLED/ERROR we still free immediately.
         * For FAST_INSERT_FALLBACK we fall through to parser with payload intact.
         */
        if (cur && is_insert_statement(payload)) {
            int fi_rc = fast_insert_execute(payload, cur, fd, nullptr);
            if (fi_rc == FAST_INSERT_HANDLED) {
                free(payload);
                continue;
            } else if (fi_rc == FAST_INSERT_ERROR) {
                free(payload);
                break;
            }
            /* FAST_INSERT_FALLBACK: payload is still valid, fall through to
             * the normal parser path below.  Do NOT free here. */
        }

        /* ── Normal parse + execute path ───────────────────────────────── */
        QueryNode  qnode{};
        char      *errmsg = nullptr;

        int rc = parser_parse(payload, &qnode, &errmsg);
        free(payload);   /* Now safe to free — parser has copied what it needs */
        payload = nullptr;

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

    /* ── Clean disconnect ───────────────────────────────────────────────── */
    /* Only clean up the ephemeral BENCH_<fd> session database.
     * User-created databases (SCHOOL, MYDB, etc.) must persist. */
    if (is_bench_session_db(session_db)) {
        wal_unregister_db(session_db);
        dbmgr_drop(mgr, session_db, nullptr);
        delete_wal_files(session_db);
        printf("[server] Client disconnected: %s (session db %s cleaned up)\n",
               ip, session_db);
    } else {
        /* This shouldn't happen since we always create BENCH_<fd>, but
         * handle it gracefully just in case. */
        printf("[server] Client disconnected: %s (persistent db %s kept)\n",
               ip, session_db);
    }

    close(fd);
    return nullptr;
}

/* ── Signal handler ─────────────────────────────────────────────────────── */
static volatile int g_running = 1;
static void sigint_handler(int sig) {
    (void)sig;
    g_running = 0;
    const char *msg = "\n[server] Shutting down...\n";
    (void)write(STDERR_FILENO, msg, strlen(msg));
}

/* ── main ───────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    int port = 9000;
    if (argc >= 2) port = atoi(argv[1]);

    dbmgr_init(&g_manager);

    /* Recover any databases that were active during a crash */
    int recovered = wal_recover(&g_manager);
    printf("[server] WAL recovery complete: %d database(s) restored\n",
           recovered);

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

    while (g_running) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(listen_fd, &readfds);
        struct timeval tv; tv.tv_sec = 1; tv.tv_usec = 0;
        int ready = select(listen_fd + 1, &readfds, nullptr, nullptr, &tv);
        if (ready < 0) { if (errno == EINTR) continue; perror("select"); break; }
        if (ready == 0) continue;

        struct sockaddr_in client_addr{};
        socklen_t          client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);
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
        ctx->current_db = nullptr;
        inet_ntop(AF_INET, &client_addr.sin_addr,
                  ctx->client_ip, sizeof(ctx->client_ip));

        /* BENCH_<fd>: ephemeral session DB, cleaned up on disconnect */
        snprintf(ctx->session_db_name, sizeof(ctx->session_db_name),
                 "BENCH_%d", client_fd);

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&tid, &attr, client_worker, ctx);
        pthread_attr_destroy(&attr);
    }

    close(listen_fd);
    expiry_stop();
    wal_flush_all();
    dbmgr_destroy(&g_manager);
    printf("[server] Shutdown complete.\n");
    return 0;
}