/*
 * NetMessenger server  --  IE3010 Network Programming
 * Registration number : IT23862630
 * Port                : 6000 + 2630 = 8630
 * NID tag             : digits 3-6 of 23862630 = 8626
 * Log file            : netmsg_IT23862630.log
 * Storage path        : ./storage/IT23862630/<sender>/<filename>
 *
 * STEP 1: threads, framing, REGISTER / LIST / QUIT, logging, cleanup.
 * STEP 2: BCAST, PMSG, rooms (JOIN / LEAVE / ROOMS / RMSG), room cleanup.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ---- personalised values: defined ONCE, used everywhere ---- */
#define REG_NO        "IT23862630"
#define PORT          8630
#define NID           "8626"
#define LOG_FILE      "netmsg_" REG_NO ".log"
#define STORAGE_ROOT  "./storage/" REG_NO

#define MAX_CLIENTS   64
#define USERNAME_LEN  32
#define LINE_MAX_LEN  2048
#define RECV_BUF_SIZE 8192

typedef struct {
    int  fd;
    char name[USERNAME_LEN];
    int  registered;
    char buf[RECV_BUF_SIZE];     /* per-client receive buffer (framing) */
    size_t len;                  /* bytes currently in buf */
    pthread_mutex_t wlock;       /* serialises writes to this socket */
    char ip[INET_ADDRSTRLEN];
} Client;

static Client *clients[MAX_CLIENTS];
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_lock     = PTHREAD_MUTEX_INITIALIZER;

/* Lock order rule: clients_lock may be held while taking a wlock.
 * NEVER take clients_lock while holding a wlock. */

/* ---------------- logging ---------------- */
static void log_event(const char *fmt, ...)
{
    char ts[32], msg[LINE_MAX_LEN];
    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&log_lock);
    FILE *f = fopen(LOG_FILE, "a");
    if (f) { fprintf(f, "[%s] %s\n", ts, msg); fclose(f); }
    printf("[%s] %s\n", ts, msg);
    fflush(stdout);
    pthread_mutex_unlock(&log_lock);
}

/* ---------------- sending ---------------- */
/* Send exactly n bytes; caller must hold c->wlock if the write must be atomic. */
static int send_raw(Client *c, const char *data, size_t n)
{
    size_t sent = 0;
    while (sent < n) {
        ssize_t r = send(c->fd, data + sent, n - sent, MSG_NOSIGNAL);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        sent += (size_t)r;
    }
    return 0;
}

static int send_all(Client *c, const char *data, size_t n)
{
    pthread_mutex_lock(&c->wlock);
    int rc = send_raw(c, data, n);
    pthread_mutex_unlock(&c->wlock);
    return rc;
}

/* OK/ERR replies: every one ends with " NID:<tag>\n" */
static void reply(Client *c, const char *fmt, ...)
{
    char body[LINE_MAX_LEN], out[LINE_MAX_LEN + 16];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    int n = snprintf(out, sizeof out, "%s NID:%s\n", body, NID);
    send_all(c, out, (size_t)n);
}

/* Forwarded MSG/notification lines: NO NID tag. Skips 'except' and unregistered. */
static void broadcast_line(Client *except, const char *fmt, ...)
{
    char body[LINE_MAX_LEN];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    size_t n = strlen(body);
    body[n++] = '\n';

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        Client *o = clients[i];
        if (o && o != except && o->registered)
            send_all(o, body, n);
    }
    pthread_mutex_unlock(&clients_lock);
}

/* ---------------- rooms ---------------- */
#define MAX_ROOMS    32
#define ROOMNAME_LEN 32

typedef struct {
    int     used;
    char    name[ROOMNAME_LEN];
    Client *members[MAX_CLIENTS];
    int     n;
} Room;

static Room rooms[MAX_ROOMS];
static pthread_mutex_t rooms_lock = PTHREAD_MUTEX_INITIALIZER;
/* rooms_lock and clients_lock are NEVER held at the same time.
 * Inside rooms_lock we may take a client's wlock (via send_all). */

/* Build "<text>\n" into out; returns length including the newline. */
static size_t format_line(char *out, size_t size, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(out, size, fmt, ap);
    va_end(ap);
    if (n < 0) n = 0;
    if ((size_t)n > size - 2) n = (int)(size - 2);
    out[n++] = '\n';
    out[n] = '\0';
    return (size_t)n;
}

/* caller must hold rooms_lock for all helpers below */
static Room *find_room(const char *name)
{
    for (int i = 0; i < MAX_ROOMS; i++)
        if (rooms[i].used && strcmp(rooms[i].name, name) == 0) return &rooms[i];
    return NULL;
}

static int member_index(Room *r, Client *c)
{
    for (int i = 0; i < r->n; i++)
        if (r->members[i] == c) return i;
    return -1;
}

static void room_remove_at(Room *r, int idx)
{
    r->members[idx] = r->members[--r->n];
    if (r->n == 0) r->used = 0;              /* empty room disappears */
}

static void room_send(Room *r, Client *except, const char *line, size_t n)
{
    for (int i = 0; i < r->n; i++)
        if (r->members[i] != except)
            send_all(r->members[i], line, n);
}

/* Called when a client disconnects: remove it from every room. */
static void rooms_remove_client(Client *c)
{
    char line[LINE_MAX_LEN];
    pthread_mutex_lock(&rooms_lock);
    for (int i = 0; i < MAX_ROOMS; i++) {
        Room *r = &rooms[i];
        if (!r->used) continue;
        int idx = member_index(r, c);
        if (idx < 0) continue;
        room_remove_at(r, idx);
        if (r->used) {
            size_t n = format_line(line, sizeof line, "MSG ROOMLEAVE %s %s", r->name, c->name);
            room_send(r, NULL, line, n);
        }
    }
    pthread_mutex_unlock(&rooms_lock);
}

/* ---------------- framing ---------------- */
/* returns 1 = got a line, 0 = disconnect, -1 = line too long */
static int read_line(Client *c, char *out, size_t max)
{
    for (;;) {
        char *nl = memchr(c->buf, '\n', c->len);
        if (nl) {
            size_t n = (size_t)(nl - c->buf);
            size_t m = n < max - 1 ? n : max - 1;
            memcpy(out, c->buf, m);
            out[m] = '\0';
            if (m && out[m - 1] == '\r') out[m - 1] = '\0';
            memmove(c->buf, nl + 1, c->len - n - 1);
            c->len -= n + 1;
            return 1;
        }
        if (c->len == sizeof c->buf) return -1;
        ssize_t r = recv(c->fd, c->buf + c->len, sizeof c->buf - c->len, 0);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return 0;
        c->len += (size_t)r;
    }
}

/* ---------------- commands ---------------- */
static void cmd_register(Client *c, char *args)
{
    if (c->registered) { reply(c, "ERR 009 ALREADY_REGISTERED"); return; }
    if (!args[0] || strchr(args, ' ') || strlen(args) >= USERNAME_LEN) {
        reply(c, "ERR 007 BAD_SYNTAX");
        return;
    }

    pthread_mutex_lock(&clients_lock);
    int taken = 0;
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i] && clients[i]->registered && strcmp(clients[i]->name, args) == 0)
            taken = 1;
    if (!taken) {
        snprintf(c->name, sizeof c->name, "%s", args);
        c->registered = 1;
    }
    pthread_mutex_unlock(&clients_lock);

    if (taken) { reply(c, "ERR 001 USERNAME_TAKEN"); return; }

    reply(c, "OK REGISTERED %s", c->name);
    broadcast_line(c, "MSG JOIN %s", c->name);
    log_event("REGISTER user=%s ip=%s", c->name, c->ip);
}

static void cmd_list(Client *c)
{
    char list[LINE_MAX_LEN] = "";
    size_t used = 0;

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->registered) {
            int w = snprintf(list + used, sizeof list - used, "%s%s",
                             used ? "," : "", clients[i]->name);
            if (w < 0 || (size_t)w >= sizeof list - used) break;
            used += (size_t)w;
        }
    }
    pthread_mutex_unlock(&clients_lock);

    reply(c, "OK USERS %s", list);
}

static void cmd_bcast(Client *c, char *args)
{
    if (!args[0]) { reply(c, "ERR 007 BAD_SYNTAX"); return; }
    broadcast_line(c, "MSG BCAST %s %s", c->name, args);
    reply(c, "OK SENT");
    log_event("BCAST from=%s len=%zu", c->name, strlen(args));
}

static void cmd_pmsg(Client *c, char *args)
{
    char *msg = strchr(args, ' ');
    if (msg) *msg++ = '\0';
    if (!args[0] || !msg || !msg[0]) { reply(c, "ERR 007 BAD_SYNTAX"); return; }

    char line[LINE_MAX_LEN];
    size_t n = format_line(line, sizeof line, "MSG PRIV %s %s", c->name, msg);

    int found = 0;
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] && clients[i]->registered && strcmp(clients[i]->name, args) == 0) {
            send_all(clients[i], line, n);
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&clients_lock);

    if (!found) { reply(c, "ERR 002 USER_NOT_FOUND"); return; }
    reply(c, "OK SENT");
    log_event("PMSG from=%s to=%s len=%zu", c->name, args, strlen(msg));
}

static void cmd_join(Client *c, char *args)
{
    if (!args[0] || strchr(args, ' ') || strlen(args) >= ROOMNAME_LEN) {
        reply(c, "ERR 007 BAD_SYNTAX");
        return;
    }
    char line[LINE_MAX_LEN];
    int full = 0, created = 0;

    pthread_mutex_lock(&rooms_lock);
    Room *r = find_room(args);
    if (!r) {                                    /* create it */
        for (int i = 0; i < MAX_ROOMS; i++)
            if (!rooms[i].used) { r = &rooms[i]; break; }
        if (!r) full = 1;
        else {
            r->used = 1; r->n = 0; created = 1;
            snprintf(r->name, sizeof r->name, "%s", args);
        }
    }
    if (r && member_index(r, c) < 0) {           /* already a member: no-op */
        r->members[r->n++] = c;
        size_t n = format_line(line, sizeof line, "MSG ROOMJOIN %s %s", r->name, c->name);
        room_send(r, c, line, n);
    }
    pthread_mutex_unlock(&rooms_lock);

    if (full) { reply(c, "ERR 012 TOO_MANY_ROOMS"); return; }
    reply(c, "OK JOINED %s", args);
    log_event("JOIN user=%s room=%s%s", c->name, args, created ? " (created)" : "");
}

static void cmd_leave(Client *c, char *args)
{
    if (!args[0] || strchr(args, ' ')) { reply(c, "ERR 007 BAD_SYNTAX"); return; }
    char line[LINE_MAX_LEN];
    int ok = 0;

    pthread_mutex_lock(&rooms_lock);
    Room *r = find_room(args);
    int idx = r ? member_index(r, c) : -1;
    if (r && idx >= 0) {
        room_remove_at(r, idx);
        if (r->used) {
            size_t n = format_line(line, sizeof line, "MSG ROOMLEAVE %s %s", r->name, c->name);
            room_send(r, NULL, line, n);
        }
        ok = 1;
    }
    pthread_mutex_unlock(&rooms_lock);

    if (!ok) { reply(c, "ERR 003 ROOM_NOT_FOUND"); return; }
    reply(c, "OK LEFT %s", args);
    log_event("LEAVE user=%s room=%s", c->name, args);
}

static void cmd_rooms(Client *c)
{
    char list[LINE_MAX_LEN] = "";
    size_t used = 0;

    pthread_mutex_lock(&rooms_lock);
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (!rooms[i].used) continue;
        int w = snprintf(list + used, sizeof list - used, "%s%s",
                         used ? "," : "", rooms[i].name);
        if (w < 0 || (size_t)w >= sizeof list - used) break;
        used += (size_t)w;
    }
    pthread_mutex_unlock(&rooms_lock);

    reply(c, "OK ROOMS %s", list);
}

static void cmd_rmsg(Client *c, char *args)
{
    char *msg = strchr(args, ' ');
    if (msg) *msg++ = '\0';
    if (!args[0] || !msg || !msg[0]) { reply(c, "ERR 007 BAD_SYNTAX"); return; }

    char line[LINE_MAX_LEN];
    size_t n = format_line(line, sizeof line, "MSG ROOM %s %s %s", args, c->name, msg);
    int status = 0;                              /* 0 ok, 1 no room, 2 not member */

    pthread_mutex_lock(&rooms_lock);
    Room *r = find_room(args);
    if (!r) status = 1;
    else if (member_index(r, c) < 0) status = 2;
    else room_send(r, c, line, n);
    pthread_mutex_unlock(&rooms_lock);

    if (status == 1) { reply(c, "ERR 003 ROOM_NOT_FOUND"); return; }
    if (status == 2) { reply(c, "ERR 011 NOT_IN_ROOM"); return; }
    reply(c, "OK SENT");
    log_event("RMSG from=%s room=%s len=%zu", c->name, args, strlen(msg));
}

/* returns 1 if the connection should close */
static int cmd_quit(Client *c)
{
    reply(c, "OK BYE");
    return 1;
}

/* ---------------- lifecycle ---------------- */
static void cleanup_client(Client *c)
{
    int was_registered;

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i] == c) clients[i] = NULL;
    was_registered = c->registered;
    c->registered = 0;
    pthread_mutex_unlock(&clients_lock);

    if (was_registered) {
        broadcast_line(NULL, "MSG LEAVE %s", c->name);
        log_event("DISCONNECT user=%s ip=%s", c->name, c->ip);
    } else {
        log_event("DISCONNECT (unregistered) ip=%s", c->ip);
    }
    rooms_remove_client(c);

    close(c->fd);
    pthread_mutex_destroy(&c->wlock);
    free(c);
}

static void *handle_client(void *arg)
{
    Client *c = arg;
    char line[LINE_MAX_LEN];
    int quit = 0;

    while (!quit) {
        int r = read_line(c, line, sizeof line);
        if (r == 0) break;                         /* disconnect (graceful or not) */
        if (r < 0) { reply(c, "ERR 007 BAD_SYNTAX"); break; }
        if (line[0] == '\0') continue;             /* ignore empty lines */

        char *args = strchr(line, ' ');
        if (args) *args++ = '\0'; else args = line + strlen(line);

        if (strcmp(line, "REGISTER") == 0) {
            cmd_register(c, args);
        } else if (!c->registered) {
            reply(c, "ERR 006 NOT_REGISTERED");
        } else if (strcmp(line, "LIST") == 0) {
            cmd_list(c);
        } else if (strcmp(line, "BCAST") == 0) {
            cmd_bcast(c, args);
        } else if (strcmp(line, "PMSG") == 0) {
            cmd_pmsg(c, args);
        } else if (strcmp(line, "JOIN") == 0) {
            cmd_join(c, args);
        } else if (strcmp(line, "LEAVE") == 0) {
            cmd_leave(c, args);
        } else if (strcmp(line, "ROOMS") == 0) {
            cmd_rooms(c);
        } else if (strcmp(line, "RMSG") == 0) {
            cmd_rmsg(c, args);
        } else if (strcmp(line, "QUIT") == 0) {
            quit = cmd_quit(c);
        }
        /* step 3: SENDFILE */
        else {
            reply(c, "ERR 005 UNKNOWN_COMMAND");
        }
    }
    cleanup_client(c);
    return NULL;
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("bind"); return 1; }
    if (listen(lfd, 16) < 0) { perror("listen"); return 1; }

    log_event("SERVER_START port=%d nid=%s", PORT, NID);

    for (;;) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof ca;
        int fd = accept(lfd, (struct sockaddr *)&ca, &cl);
        if (fd < 0) { if (errno == EINTR) continue; perror("accept"); continue; }

        Client *c = calloc(1, sizeof *c);
        if (!c) { close(fd); continue; }
        c->fd = fd;
        pthread_mutex_init(&c->wlock, NULL);
        inet_ntop(AF_INET, &ca.sin_addr, c->ip, sizeof c->ip);

        int slot = -1;
        pthread_mutex_lock(&clients_lock);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (!clients[i]) { clients[i] = c; slot = i; break; }
        pthread_mutex_unlock(&clients_lock);

        if (slot < 0) {
            reply(c, "ERR 010 SERVER_FULL");
            close(fd); pthread_mutex_destroy(&c->wlock); free(c);
            continue;
        }

        log_event("CONNECT ip=%s", c->ip);
        pthread_t t;
        if (pthread_create(&t, NULL, handle_client, c) != 0) {
            cleanup_client(c);
            continue;
        }
        pthread_detach(t);
    }
}
