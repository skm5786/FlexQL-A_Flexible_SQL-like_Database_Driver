/**
 * flexql_api.cpp  —  FlexQL Client Library Implementation
 *
 * LESSON: This file implements the four public API functions.
 *         It is compiled into a shared library (libflexql.so) OR linked
 *         directly into the client binary.  Either way, the user only
 *         sees the flexql.h declarations.
 *
 * KEY CONCEPTS IN THIS FILE:
 *   1. TCP socket programming (connect / send / recv)
 *   2. Length-prefix message framing
 *   3. The callback dispatch pattern
 *   4. Error message heap allocation / ownership transfer
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <cassert>

/* POSIX socket headers — Linux / macOS                                      */
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>          /* getaddrinfo / gethostbyname                  */
#include <arpa/inet.h>      /* htonl, ntohl, inet_pton                      */
#include <unistd.h>         /* close()                                       */

#include "flexql.h"
#include "common/types.h"

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  INTERNAL HELPERS
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * set_errmsg — allocates a copy of  msg  and writes the pointer to  *errmsg.
 *
 * LESSON: We use strdup() which calls malloc internally.
 *         The caller is expected to free this with flexql_free().
 *         We always NULL-check before writing to avoid segfaults when the
 *         caller doesn't care about error messages (passes NULL for errmsg).
 */
static void set_errmsg(char **errmsg, const char *msg) {
    if (errmsg) {
        if (*errmsg) free(*errmsg);   /* Free any previous error            */
        *errmsg = strdup(msg);         /* Allocate fresh copy                */
    }
}

/**
 * send_all — ensures all bytes of buf are sent over the socket.
 *
 * LESSON: send() / write() on a socket can return LESS than requested bytes
 *         (partial write) — this is normal for non-blocking sockets and can
 *         also happen in blocking mode if the kernel buffer is full.
 *         A robust implementation MUST loop until all bytes are sent.
 *
 * Returns: 0 on success, -1 on error.
 */
static int send_all(int sockfd, const void *buf, size_t len) {
    const char *ptr = static_cast<const char *>(buf);
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t sent = send(sockfd, ptr, remaining, 0);
        if (sent <= 0) {
            /* sent == 0 means connection closed; < 0 means error            */
            return -1;
        }
        ptr       += sent;
        remaining -= static_cast<size_t>(sent);
    }
    return 0;
}

/**
 * recv_all — ensures all requested bytes are read from the socket.
 *
 * LESSON: Same problem as send — recv() can return fewer bytes than
 *         requested.  We loop until the buffer is full.
 *
 * Returns: 0 on success, -1 on error / connection closed.
 */
static int recv_all(int sockfd, void *buf, size_t len) {
    char  *ptr       = static_cast<char *>(buf);
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t received = recv(sockfd, ptr, remaining, 0);
        if (received <= 0) {
            return -1;
        }
        ptr       += received;
        remaining -= static_cast<size_t>(received);
    }
    return 0;
}

/**
 * send_message — frames and sends one protocol message.
 *
 * LESSON — Length-Prefix Framing:
 *   We write:  [1 byte msg_type] [4 bytes payload_len BE] [payload_len bytes]
 *   "BE" = big-endian = network byte order = what htonl() produces.
 *   The receiver reads the 5-byte header first, then reads exactly
 *   payload_len bytes.  This gives us clean message boundaries over a
 *   TCP stream.
 */
static int send_message(int sockfd, MessageType type,
                        const char *payload, uint32_t payload_len) {
    WireHeader hdr;
    hdr.msg_type    = static_cast<uint8_t>(type);
    hdr.payload_len = htonl(payload_len);  /* Host → Network byte order      */

    if (send_all(sockfd, &hdr, sizeof(hdr)) != 0)       return -1;
    if (payload_len > 0) {
        if (send_all(sockfd, payload, payload_len) != 0) return -1;
    }
    return 0;
}

/**
 * recv_message — reads one protocol message from the socket.
 *
 * On success:  *out_type is set, *out_payload is heap-allocated (caller frees),
 *              *out_len is the payload size.
 * On error:    returns -1, *out_payload is NULL.
 *
 * LESSON: We always allocate payload_len + 1 bytes and null-terminate so
 *         string payloads can be used directly with printf / strcmp.
 */
static int recv_message(int sockfd, MessageType *out_type,
                        char **out_payload, uint32_t *out_len) {
    *out_payload = nullptr;
    *out_len     = 0;

    WireHeader hdr;
    if (recv_all(sockfd, &hdr, sizeof(hdr)) != 0) return -1;

    *out_type = static_cast<MessageType>(hdr.msg_type);
    uint32_t plen = ntohl(hdr.payload_len);  /* Network → Host byte order   */

    if (plen > FLEXQL_MAX_PAYLOAD) return -1; /* Safety: reject huge payloads */

    char *buf = static_cast<char *>(malloc(plen + 1));
    if (!buf) return -1;

    if (plen > 0 && recv_all(sockfd, buf, plen) != 0) {
        free(buf);
        return -1;
    }
    buf[plen]    = '\0';   /* Null-terminate                                 */
    *out_payload = buf;
    *out_len     = plen;
    return 0;
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  RESULT ROW SERIALISATION / DESERIALISATION
 *
 *  LESSON: We need to transmit a row (array of strings + column names) as a
 *  flat byte buffer over the socket.  Our simple format:
 *
 *    [4 bytes: col_count]
 *    for each column:
 *      [4 bytes: name_len] [name_len bytes: column name]
 *      [4 bytes: val_len ] [val_len  bytes: cell value ] (0 bytes = NULL)
 *
 *  This is a hand-rolled serialisation.  Production systems use Protocol
 *  Buffers, MessagePack, or Cap'n Proto for this.
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * parse_result_row — deserialises one MSG_RESULT payload into arrays of
 *                    column names and string values.
 *
 * Returns: 0 on success. Caller must free col_names[i] and values[i].
 */
static int parse_result_row(const char *payload, uint32_t len,
                             int *out_col_count,
                             char ***out_values,
                             char ***out_names) {
    (void)len; /* We trust the data matches the format we defined           */

    const char *p   = payload;
    uint32_t col_count_net;
    memcpy(&col_count_net, p, sizeof(uint32_t));
    int col_count = static_cast<int>(ntohl(col_count_net));
    p += sizeof(uint32_t);

    char **values = static_cast<char **>(calloc(col_count, sizeof(char *)));
    char **names  = static_cast<char **>(calloc(col_count, sizeof(char *)));
    if (!values || !names) {
        free(values); free(names);
        return -1;
    }

    for (int i = 0; i < col_count; ++i) {
        /* Read column name */
        uint32_t nlen_net;
        memcpy(&nlen_net, p, sizeof(uint32_t)); p += sizeof(uint32_t);
        uint32_t nlen = ntohl(nlen_net);
        names[i] = static_cast<char *>(malloc(nlen + 1));
        memcpy(names[i], p, nlen);
        names[i][nlen] = '\0';
        p += nlen;

        /* Read value */
        uint32_t vlen_net;
        memcpy(&vlen_net, p, sizeof(uint32_t)); p += sizeof(uint32_t);
        uint32_t vlen = ntohl(vlen_net);
        if (vlen == 0) {
            values[i] = nullptr; /* SQL NULL                                 */
        } else {
            values[i] = static_cast<char *>(malloc(vlen + 1));
            memcpy(values[i], p, vlen);
            values[i][vlen] = '\0';
            p += vlen;
        }
    }

    *out_col_count = col_count;
    *out_values    = values;
    *out_names     = names;
    return 0;
}

/* Free arrays allocated by parse_result_row */
static void free_row_arrays(int col_count, char **values, char **names) {
    for (int i = 0; i < col_count; ++i) {
        free(values[i]);
        free(names[i]);
    }
    free(values);
    free(names);
}

/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  PUBLIC API IMPLEMENTATIONS
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

/**
 * flexql_open — create a TCP connection to the server.
 *
 * LESSON — socket() / connect():
 *   socket(AF_INET, SOCK_STREAM, 0)  creates a TCP socket.
 *     AF_INET  = IPv4
 *     SOCK_STREAM = reliable, ordered byte stream = TCP
 *   connect(sockfd, addr, addrlen)   initiates the three-way TCP handshake.
 *   getaddrinfo() resolves a hostname to one or more sockaddr structs.
 */
extern "C"
int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db || port <= 0 || port > 65535) return FLEXQL_ERROR;

    /* ── Resolve the host name ─────────────────────────────────────────── */
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;       /* IPv4 only for simplicity          */
    hints.ai_socktype = SOCK_STREAM;

    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%d", port);

    int gai = getaddrinfo(host, port_str, &hints, &res);
    if (gai != 0) return FLEXQL_ERROR;

    /* ── Create socket ──────────────────────────────────────────────────── */
    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) { freeaddrinfo(res); return FLEXQL_ERROR; }

    /* ── Connect ────────────────────────────────────────────────────────── */
    if (connect(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        close(sockfd);
        freeaddrinfo(res);
        return FLEXQL_ERROR;
    }
    freeaddrinfo(res);

    /* ── Allocate handle ────────────────────────────────────────────────── */
    FlexQL *handle = static_cast<FlexQL *>(calloc(1, sizeof(FlexQL)));
    if (!handle) { close(sockfd); return FLEXQL_NOMEM; }

    handle->sockfd    = sockfd;
    handle->port      = port;
    handle->connected = 1;
    strncpy(handle->host, host, sizeof(handle->host) - 1);

    *db = handle;
    return FLEXQL_OK;
}

/**
 * flexql_close — shut down the connection and free the handle.
 *
 * LESSON: Always handle NULL gracefully — callers often write:
 *   flexql_close(db); db = NULL;
 *   and they might call close twice. Safe to call on NULL.
 */
extern "C"
int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_OK;

    if (db->connected && db->sockfd >= 0) {
        shutdown(db->sockfd, SHUT_RDWR);  /* Signal EOF to server           */
        close(db->sockfd);
        db->sockfd    = -1;
        db->connected = 0;
    }
    free(db->last_errmsg);
    free(db);
    return FLEXQL_OK;
}

/**
 * flexql_exec — send SQL to the server, receive results, invoke callback.
 *
 * LESSON — The request/response loop:
 *  1. Send MSG_QUERY with the SQL string as payload.
 *  2. Loop reading messages:
 *       MSG_RESULT  → parse row → call callback
 *       MSG_DONE    → query complete, break loop
 *       MSG_OK      → non-SELECT success, break loop
 *       MSG_ERROR   → set errmsg, return FLEXQL_ERROR
 *  3. If callback returns 1 (abort), send MSG_ABORT and break.
 *
 * This is a synchronous (blocking) API — the call blocks until the server
 * sends MSG_DONE or MSG_ERROR.  An async API would use epoll/kqueue + a
 * separate thread, which we can add as an extension later.
 */
extern "C"
int flexql_exec(FlexQL         *db,
                const char     *sql,
                flexql_callback callback,
                void           *arg,
                char          **errmsg) {
    if (!db || !db->connected || db->sockfd < 0) {
        set_errmsg(errmsg, "Not connected to FlexQL server");
        return FLEXQL_ERROR;
    }
    if (!sql || sql[0] == '\0') {
        set_errmsg(errmsg, "Empty SQL statement");
        return FLEXQL_ERROR;
    }

    /* ── Send the query ─────────────────────────────────────────────────── */
    uint32_t sql_len = static_cast<uint32_t>(strlen(sql));
    if (send_message(db->sockfd, MSG_QUERY, sql, sql_len) != 0) {
        set_errmsg(errmsg, "Failed to send query to server");
        db->connected = 0;
        return FLEXQL_ERROR;
    }

    /* ── Receive result messages ─────────────────────────────────────────── */
    int ret = FLEXQL_OK;
    bool done = false;

    while (!done) {
        MessageType type;
        char       *payload = nullptr;
        uint32_t    plen    = 0;

        if (recv_message(db->sockfd, &type, &payload, &plen) != 0) {
            set_errmsg(errmsg, "Connection lost while reading server response");
            db->connected = 0;
            ret = FLEXQL_ERROR;
            break;
        }

        switch (type) {
        case MSG_OK:
            /* Non-SELECT query succeeded (CREATE TABLE, INSERT, etc.)       */
            free(payload);
            done = true;
            break;

        case MSG_DONE:
            /* SELECT completed — no more rows                               */
            free(payload);
            done = true;
            break;

        case MSG_ERROR:
            set_errmsg(errmsg, payload ? payload : "Unknown server error");
            free(payload);
            ret  = FLEXQL_ERROR;
            done = true;
            break;

        case MSG_RESULT:
            /* One result row — deserialise and invoke callback              */
            if (callback) {
                int    col_count = 0;
                char **values    = nullptr;
                char **names     = nullptr;

                if (parse_result_row(payload, plen, &col_count,
                                     &values, &names) == 0) {
                    int cb_ret = callback(arg, col_count, values, names);
                    free_row_arrays(col_count, values, names);

                    if (cb_ret != 0) {
                        /* Callback requested abort                          */
                        send_message(db->sockfd, MSG_ABORT, nullptr, 0);
                        ret  = FLEXQL_ABORT;
                        done = true;
                    }
                }
            }
            free(payload);
            break;

        default:
            /* Unknown message type — protocol error                         */
            free(payload);
            set_errmsg(errmsg, "Unknown message type from server");
            ret  = FLEXQL_ERROR;
            done = true;
            break;
        }
    }

    return ret;
}

/**
 * flexql_free — free memory allocated by the FlexQL library.
 *
 * LESSON: Wrapping free() in a library function is a common idiom.
 *         Checking for NULL before calling free is safe per the C standard
 *         (free(NULL) is a no-op) but we do it explicitly for clarity.
 */
extern "C"
void flexql_free(void *ptr) {
    free(ptr);
}