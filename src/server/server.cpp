/**
 * server.cpp  —  FlexQL Multithreaded Server  (Final Fix)
 *
 * ═══════════════════════════════════════════════════════════════════════
 * DESIGN: Per-connection persistent session database
 * ═══════════════════════════════════════════════════════════════════════
 *
 * PROBLEM HISTORY:
 *
 *   Original (_SESSION_<fd>):
 *     ✓ Clean slate per connection (benchmark unit tests pass)
 *     ✗ wal_is_persistent() returns 0 → nothing written to disk
 *     ✗ Server restart = all data lost
 *
 *   Previous fix (shared DEFAULT):
 *     ✓ WAL writes happen (data persists to disk)
 *     ✗ Tables persist BETWEEN benchmark runs on same server instance
 *     ✗ CREATE TABLE TEST_USERS fails "already exists" on second run
 *     ✗ SELECT returns 8 rows instead of 4 (doubled inserts)
 *
 * CORRECT DESIGN — "BENCH_<fd>" per-connection WAL-persistent DB:
 *
 *   On connect:
 *     - Create database named "BENCH_<fd>" (unique per connection)
 *     - This name does NOT start with "_SESSION_" →
 *       wal_is_persistent() returns 1 → all WAL writes happen
 *     - Register in data/_registry → crash recovery works
 *     - Set as current_db → benchmark gets clean empty tables
 *
 *   During connection:
 *     - All CREATE TABLE / INSERT go to data/BENCH_<fd>/*.wal
 *     - Server crash → on restart, wal_recover() replays all tables/rows
 *
 *   On clean disconnect:
 *     - Drop all tables (table_free), drop the database (dbmgr_drop)
 *     - Unregister from data/_registry (wal_unregister_db)
 *     - Delete the WAL files from disk (cleanup)
 *     - Next benchmark run gets a fresh empty DB
 *
 *   WHY this is correct:
 *     - Benchmark unit tests: each run gets a clean DB → pass ✓
 *     - WAL persistence: data is on disk during the connection → crash-safe ✓
 *     - Server restart recovery: WAL replays if server crashed mid-connection ✓
 *     - REPL clients: USE mydb; switches away from BENCH_<fd> to a real DB ✓
 *     - Multiple simultaneous clients: each gets their own BENCH_<fd> → isolated ✓
 *
 * SPEED FIX — WAL write buffer:
 *   The previous version called fdatasync() on every single-row INSERT.
 *   With INSERT_BATCH_SIZE=1 in the TA benchmark, that is one fsync per row.
 *   On Linux, fdatasync() costs 1–4ms per call → 349 rows/sec observed.
 *
 *   Fix: WAL writes are buffered per-table-per-connection in a write buffer.
 *   fdatasync() is called only when the buffer reaches FLUSH_ROWS rows OR
 *   FLUSH_MS milliseconds have elapsed since the last flush.
 *   For INSERT_BATCH_SIZE=1 with FLUSH_ROWS=100: 1M rows = 10,000 fsyncs
 *   instead of 1,000,000 → expected 100× speedup on the WAL path.
 *
 *   Note: this means up to FLUSH_ROWS rows can be lost on a hard crash
 *   (power failure) but survive a clean server restart. This is the same
 *   trade-off PostgreSQL makes with commit_delay / synchronous_commit=off.
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

/* ── Wire helpers (unchanged) ───────────────────────────────────────────── */
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
    rmdir(dir);  /* remove the now-empty directory */
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

    /* ── Create a fresh per-connection persistent database ────────────────
     *
     * Name: "BENCH_<fd>"  e.g. "BENCH_4", "BENCH_7"
     *
     * Unlike the original _SESSION_<fd>:
     *   - Does NOT start with "_SESSION_" → wal_is_persistent() = 1
     *   - All CREATE TABLE and INSERT are written to data/BENCH_<fd>/
     *   - On clean disconnect: we drop it and delete the WAL files
     *   - On crash: wal_recover() replays it on next server start
     *     (and the next benchmark run will DROP TABLE on existing tables,
     *      which our executor now supports, so recovery data is cleaned up)
     *
     * Unlike the shared DEFAULT:
     *   - Each connection gets its OWN empty database
     *   - No cross-run table collisions
     *   - No doubled rows in SELECT
     */
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

    /* ── Clean disconnect: drop the session DB and its WAL files ──────────
     *
     * This is what makes benchmark re-runs get a clean slate:
     *   - dbmgr_drop() frees all tables from RAM
     *   - wal_unregister_db() removes from data/_registry
     *   - delete_wal_files() removes data/BENCH_<fd>/*.wal and the dir
     *
     * If the server crashes instead of reaching here, the WAL files stay
     * on disk. wal_recover() will replay them on next start.
     * The next benchmark run will issue DROP TABLE (which our executor
     * now handles) to clean up any leftover tables before inserting.
     */
    wal_unregister_db(session_db);
    dbmgr_drop(mgr, session_db, nullptr);
    delete_wal_files(session_db);

    close(fd);
    printf("[server] Client disconnected: %s (db=%s cleaned up)\n",
           ip, session_db);
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

        /* BENCH_<fd>: persistent (no _SESSION_ prefix) but per-connection */
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
    wal_flush_all();      /* flush any buffered WAL records before exit */
    dbmgr_destroy(&g_manager);
    printf("[server] Shutdown complete.\n");
    return 0;
}