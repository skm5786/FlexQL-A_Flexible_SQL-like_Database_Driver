/**
 * server.cpp  —  FlexQL Multithreaded Server  (Lesson 6 — Default DB fix)
 *
 * CHANGE FROM LESSON 5:
 *
 *   FIX — Default database per client connection:
 *
 *   The benchmark never sends CREATE DATABASE or USE.  It connects and
 *   immediately fires CREATE TABLE, INSERT, SELECT.  Our server previously
 *   blocked all table commands when current_db == nullptr, causing every
 *   benchmark command to fail with "No database selected".
 *
 *   Solution: when a client connects, automatically create and select a
 *   session-private database named "_SESSION_<fd>" (e.g. "_SESSION_4").
 *   When the client disconnects, drop it.  This makes each client get its
 *   own isolated namespace with zero client-side setup required.
 *
 *   Why session-private?  If two clients connect simultaneously, they must
 *   not share tables.  Naming the DB after the file descriptor guarantees
 *   uniqueness (the OS never reuses an fd while it is open).
 *
 *   The explicit CREATE DATABASE / USE commands still work — a client that
 *   sends USE university; will switch away from the auto-created session DB
 *   and work in the named database for the rest of its session.
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
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>

#include "common/types.h"
#include "parser/parser.h"
#include "storage/dbmanager.h"

/* Forward declaration */
int executor_execute(DatabaseManager *mgr, Database **current_db,
                     const QueryNode *query, int client_fd, char **errmsg);

/* ── Global database manager ──────────────────────────────────────────────── */
static DatabaseManager g_manager;

/* ── Client context ───────────────────────────────────────────────────────── */
typedef struct {
    int              client_fd;
    char             client_ip[64];
    DatabaseManager *mgr;
    Database        *current_db;
    char             session_db_name[64]; /* auto-created DB name for this fd */
} ClientContext;

/* ── Wire protocol helpers ────────────────────────────────────────────────── */
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

/* ── Worker thread ────────────────────────────────────────────────────────── */
static void *client_worker(void *arg) {
    ClientContext *ctx = static_cast<ClientContext *>(arg);
    int              fd  = ctx->client_fd;
    DatabaseManager *mgr = ctx->mgr;
    char             session_db[64];
    strncpy(session_db, ctx->session_db_name, sizeof(session_db));
    char             ip[64];
    strncpy(ip, ctx->client_ip, sizeof(ip));

    /* ── FIX: Auto-create and select a session database ────────────────────
     * The benchmark connects and immediately runs CREATE TABLE without any
     * CREATE DATABASE or USE command.  We create a private database for
     * this connection so table commands work from the very first query.
     *
     * Name format: "_SESSION_<fd>"  (underscore prefix avoids collisions
     * with user-created databases; fd is unique while the socket is open).  */
    char *setup_err = nullptr;
    dbmgr_create(mgr, session_db, &setup_err);
    free(setup_err); setup_err = nullptr;

    Database *cur = dbmgr_find(mgr, session_db);
    if (!cur) {
        /* This should never happen, but fail safely */
        printf("[server] ERROR: cannot create session DB for fd=%d\n", fd);
        close(fd);
        free(ctx);
        return nullptr;
    }

    free(ctx);   /* ctx was heap-allocated; unpack everything before freeing */
    printf("[server] Client connected: %s (fd=%d, session=%s)\n",
           ip, fd, session_db);

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
            send_message(fd, MSG_ERROR,
                         errmsg ? errmsg : "Parse error",
                         (uint32_t)(errmsg ? strlen(errmsg) : 11));
            free(errmsg);
            continue;
        }

        /* DB-level commands (CREATE DB, USE, SHOW, DROP) need no current_db.
           All other commands use cur (which is always set now).            */
        rc = executor_execute(mgr, &cur, &qnode, fd, &errmsg);
        if (rc != 0) {
            /* Error was already sent to client by executor; log it here    */
        }
        free(errmsg);
    }

    /* ── FIX: Clean up the session database on disconnect ─────────────────
     * Drop the auto-created session database so it doesn't accumulate.
     * If the client ran "USE university;" and created data there, that data
     * stays — we only drop the session-private DB.                         */
    dbmgr_drop(mgr, session_db, nullptr);

    close(fd);
    printf("[server] Client disconnected: %s\n", ip);
    return nullptr;
}

/* ── Signal handler ───────────────────────────────────────────────────────── */
static volatile int g_running = 1;
static void sigint_handler(int sig) {
    (void)sig;
    g_running = 0;
    const char *msg = "\n[server] Shutting down...\n";
    write(STDERR_FILENO, msg, strlen(msg));
}

/* ── main ─────────────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    int port = 9000;
    if (argc >= 2) port = atoi(argv[1]);

    dbmgr_init(&g_manager);

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

        struct timeval tv; tv.tv_sec=1; tv.tv_usec=0;
        int ready = select(listen_fd+1, &readfds, nullptr, nullptr, &tv);
        if (ready < 0) { if (errno==EINTR) continue; perror("select"); break; }
        if (ready == 0) continue;

        struct sockaddr_in client_addr{};
        socklen_t          client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) { if (g_running) perror("accept"); continue; }

        ClientContext *ctx = static_cast<ClientContext*>(
                                 calloc(1, sizeof(ClientContext)));
        ctx->client_fd  = client_fd;
        ctx->mgr        = &g_manager;
        ctx->current_db = nullptr;
        inet_ntop(AF_INET, &client_addr.sin_addr,
                  ctx->client_ip, sizeof(ctx->client_ip));
        /* Build the session database name using the file descriptor          */
        snprintf(ctx->session_db_name, sizeof(ctx->session_db_name),
                 "_SESSION_%d", client_fd);

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&tid, &attr, client_worker, ctx);
        pthread_attr_destroy(&attr);
    }

    close(listen_fd);
    dbmgr_destroy(&g_manager);
    printf("[server] Shutdown complete.\n");
    return 0;
}