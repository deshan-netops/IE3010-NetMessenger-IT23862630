/*
 * NetMessenger server -- IE3010 Network Programming
 * Registration number : IT23862630
 * Port                : 6000 + 2630 = 8630
 * NID tag             : digits 3-6 of 23862630 = 8626
 * Log file            : netmsg_IT23862630.log
 * Storage path        : ./storage/IT23862630/<sender>/<filename>
 *
 * Model: one thread per client (pthreads). ONE mutex (clients_lock) protects
 * all shared state. A client's rooms are stored inside the client itself, so a
 * room exists while at least one client is in it, and disconnect cleanup is free.
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
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ---- personalised values: defined ONCE, used everywhere ---- */
#define REG_NO       "IT23862630"
#define PORT         8630
#define NID          "8626"
#define LOG_FILE     "netmsg_" REG_NO ".log"
#define STORAGE      "./storage/" REG_NO

#define MAX_CLIENTS  64
#define MAX_ROOMS    8                       /* rooms one client may join */
#define NAME_LEN     32
#define LINE_LEN     2048
#define MAX_FILE     (10UL * 1024 * 1024)    /* bigger -> ERR 004 */

typedef struct {
    int  fd, registered, nrooms;
    char name[NAME_LEN], ip[INET_ADDRSTRLEN];
    char rooms[MAX_ROOMS][NAME_LEN];         /* rooms this client has joined */
    char buf[8192];                          /* receive buffer (framing) */
    size_t len;                              /* bytes currently in buf */
    pthread_mutex_t wlock;                   /* one writer per socket at a time */
} Client;

static Client *clients[MAX_CLIENTS];
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_lock     = PTHREAD_MUTEX_INITIALIZER;
/* Lock rule: clients_lock may be held while taking a wlock, never the reverse. */

/* ---------------- logging ---------------- */
static void log_event(const char *fmt, ...)
{
    char ts[32], msg[LINE_LEN];
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
static int send_bytes(int fd, const char *d, size_t n)
{
    while (n > 0) {
        ssize_t r = send(fd, d, n, MSG_NOSIGNAL);   /* NOSIGNAL: dead client != crash */
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) return -1;
        d += r;
        n -= (size_t)r;
    }
    return 0;
}

/* Header + optional data under ONE hold of the write lock: nothing can be
 * interleaved into the middle of a message or a file. */
static void deliver(Client *t, const char *hdr, size_t hl, const char *data, size_t n)
{
    pthread_mutex_lock(&t->wlock);
    if (send_bytes(t->fd, hdr, hl) == 0 && n) send_bytes(t->fd, data, n);
    pthread_mutex_unlock(&t->wlock);
}

/* Build "<text>\n"; returns its length. */
static size_t fmt_line(char *out, size_t size, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(out, size - 1, fmt, ap);
    va_end(ap);
    size_t n = strlen(out);
    out[n++] = '\n';
    out[n] = '\0';
    return n;
}

/* OK / ERR replies: every one ends with " NID:<tag>" */
static void reply(Client *c, const char *fmt, ...)
{
    char body[LINE_LEN], out[LINE_LEN + 16];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    size_t n = fmt_line(out, sizeof out, "%s NID:" NID, body);
    deliver(c, out, n, NULL, 0);
}

/* Notification line (MSG ...) to every registered client except 'except'. No NID tag. */
static void broadcast(Client *except, const char *line, size_t n)
{
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i] && clients[i] != except && clients[i]->registered)
            deliver(clients[i], line, n, NULL, 0);
    pthread_mutex_unlock(&clients_lock);
}

/* ---------------- rooms and routing ---------------- */
/* index of room in c->rooms, or -1.  (caller holds clients_lock) */
static int in_room(Client *c, const char *room)
{
    for (int j = 0; j < c->nrooms; j++)
        if (strcmp(c->rooms[j], room) == 0) return j;
    return -1;
}

/* Send hdr(+data) to a user (room == 0) or to the members of a room (room == 1).
 * returns 0 = delivered, 1 = no such user/room, 2 = room exists but sender isn't in it */
static int send_to(Client *from, const char *target, int room,
                   const char *hdr, size_t hl, const char *data, size_t n)
{
    int status = 1;
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        Client *o = clients[i];
        if (!o || !o->registered) continue;
        if (!room && strcmp(o->name, target) == 0) {
            deliver(o, hdr, hl, data, n);
            status = 0;
            break;
        }
        if (room && in_room(o, target) >= 0) {          /* o is a member of the room */
            if (status == 1) status = in_room(from, target) < 0 ? 2 : 0;
            if (status == 2) break;
            if (o != from) deliver(o, hdr, hl, data, n);
        }
    }
    pthread_mutex_unlock(&clients_lock);
    return status;
}

static void add_name(char *list, const char *name)      /* "a,b,c" builder */
{
    size_t l = strlen(list);
    if (l + strlen(name) + 2 < LINE_LEN) snprintf(list + l, LINE_LEN - l, "%s%s", l ? "," : "", name);
}

/* ---------------- framing ---------------- */
/* 1 = got a line, 0 = disconnect, -1 = line too long */
static int read_line(Client *c, char *out, size_t max)
{
    for (;;) {
        char *nl = memchr(c->buf, '\n', c->len);
        if (nl) {
            size_t n = (size_t)(nl - c->buf), m = n < max - 1 ? n : max - 1;
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

/* Read exactly n bytes (dst == NULL: read and throw away).
 * Bytes already in c->buf (same recv() as the header) are used first. */
static int read_exact(Client *c, char *dst, size_t n)
{
    char tmp[4096];
    while (n > 0) {
        size_t take = c->len < n ? c->len : n;
        if (take) {
            if (dst) { memcpy(dst, c->buf, take); dst += take; }
            memmove(c->buf, c->buf + take, c->len - take);
            c->len -= take;
            n -= take;
            continue;
        }
        ssize_t r = recv(c->fd, dst ? dst : tmp, dst ? n : (n < sizeof tmp ? n : sizeof tmp), 0);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        if (dst) dst += r;
        n -= (size_t)r;
    }
    return 0;
}

/* ---------------- commands ---------------- */
static void cmd_register(Client *c, char *args)
{
    if (c->registered) { reply(c, "ERR 009 ALREADY_REGISTERED"); return; }
    if (!args[0] || strchr(args, ' ') || strlen(args) >= NAME_LEN || strpbrk(args, "/.")) {
        reply(c, "ERR 007 BAD_SYNTAX");                  /* no '/' or '.': names become folder names */
        return;
    }
    int taken = 0;
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i] && clients[i]->registered && strcmp(clients[i]->name, args) == 0) taken = 1;
    if (!taken) { snprintf(c->name, NAME_LEN, "%s", args); c->registered = 1; }
    pthread_mutex_unlock(&clients_lock);

    if (taken) { reply(c, "ERR 001 USERNAME_TAKEN"); return; }
    reply(c, "OK REGISTERED %s", c->name);
    char line[LINE_LEN];
    broadcast(c, line, fmt_line(line, sizeof line, "MSG JOIN %s", c->name));
    log_event("REGISTER user=%s ip=%s", c->name, c->ip);
}

static void cmd_list(Client *c)
{
    char list[LINE_LEN] = "";
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i] && clients[i]->registered) add_name(list, clients[i]->name);
    pthread_mutex_unlock(&clients_lock);
    reply(c, "OK USERS %s", list);
}

static void cmd_bcast(Client *c, char *args)
{
    if (!args[0]) { reply(c, "ERR 007 BAD_SYNTAX"); return; }
    char line[LINE_LEN];
    broadcast(c, line, fmt_line(line, sizeof line, "MSG BCAST %s %s", c->name, args));
    reply(c, "OK SENT");
    log_event("BCAST from=%s len=%zu", c->name, strlen(args));
}

static void cmd_pmsg(Client *c, char *args)              /* args = "<user> <message>" */
{
    char *msg = strchr(args, ' ');
    if (msg) *msg++ = '\0';
    if (!args[0] || !msg || !msg[0]) { reply(c, "ERR 007 BAD_SYNTAX"); return; }
    char line[LINE_LEN];
    size_t n = fmt_line(line, sizeof line, "MSG PRIV %s %s", c->name, msg);
    if (send_to(c, args, 0, line, n, NULL, 0)) { reply(c, "ERR 002 USER_NOT_FOUND"); return; }
    reply(c, "OK SENT");
    log_event("PMSG from=%s to=%s len=%zu", c->name, args, strlen(msg));
}

static void cmd_join(Client *c, char *args)              /* creates the room if nobody is in it */
{
    if (!args[0] || strchr(args, ' ') || strlen(args) >= NAME_LEN) { reply(c, "ERR 007 BAD_SYNTAX"); return; }
    int full = 0;
    pthread_mutex_lock(&clients_lock);
    if (in_room(c, args) < 0) {
        if (c->nrooms == MAX_ROOMS) full = 1;
        else snprintf(c->rooms[c->nrooms++], NAME_LEN, "%s", args);
    }
    pthread_mutex_unlock(&clients_lock);
    if (full) { reply(c, "ERR 012 TOO_MANY_ROOMS"); return; }
    reply(c, "OK JOINED %s", args);
    log_event("JOIN user=%s room=%s", c->name, args);
}

static void cmd_leave(Client *c, char *args)
{
    pthread_mutex_lock(&clients_lock);
    int j = in_room(c, args);
    if (j >= 0) memmove(c->rooms[j], c->rooms[--c->nrooms], NAME_LEN);   /* fill the gap */
    pthread_mutex_unlock(&clients_lock);
    if (j < 0) { reply(c, "ERR 003 ROOM_NOT_FOUND"); return; }
    reply(c, "OK LEFT %s", args);
    log_event("LEAVE user=%s room=%s", c->name, args);
}

static void cmd_rooms(Client *c)
{
    char list[LINE_LEN] = "";
    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i]) continue;
        for (int j = 0; j < clients[i]->nrooms; j++) {
            int seen = 0;                                /* list each room once */
            for (int k = 0; k < i && !seen; k++)
                if (clients[k] && in_room(clients[k], clients[i]->rooms[j]) >= 0) seen = 1;
            if (!seen) add_name(list, clients[i]->rooms[j]);
        }
    }
    pthread_mutex_unlock(&clients_lock);
    reply(c, "OK ROOMS %s", list);
}

static void cmd_rmsg(Client *c, char *args)              /* args = "<room> <message>" */
{
    char *msg = strchr(args, ' ');
    if (msg) *msg++ = '\0';
    if (!args[0] || !msg || !msg[0]) { reply(c, "ERR 007 BAD_SYNTAX"); return; }
    char line[LINE_LEN];
    size_t n = fmt_line(line, sizeof line, "MSG ROOM %s %s %s", args, c->name, msg);
    int st = send_to(c, args, 1, line, n, NULL, 0);
    if (st == 1) { reply(c, "ERR 003 ROOM_NOT_FOUND"); return; }
    if (st == 2) { reply(c, "ERR 011 NOT_IN_ROOM"); return; }
    reply(c, "OK SENT");
    log_event("RMSG from=%s room=%s len=%zu", c->name, args, strlen(msg));
}

/* SENDFILE <target> <filename> <size>\n + exactly <size> raw bytes.
 * Returns 1 if the client vanished mid-file (connection must close). */
static int cmd_sendfile(Client *c, char *args)
{
    char *fname = strchr(args, ' ');
    char *sz = fname ? strchr(fname + 1, ' ') : NULL;
    if (!sz || sz[1] < '0' || sz[1] > '9') { reply(c, "ERR 007 BAD_SYNTAX"); return 0; }
    *fname++ = '\0';
    *sz++ = '\0';
    size_t size = strtoul(sz, NULL, 10);

    /* ALWAYS consume the <size> bytes, even if we reject the file, otherwise the
     * file data would be parsed as commands. data == NULL means "discard". */
    char *data = size <= MAX_FILE ? malloc(size + 1) : NULL;
    if (read_exact(c, data, size) < 0) { free(data); return 1; }
    if (!data) { reply(c, "ERR 004 FILE_TOO_LARGE"); return 0; }
    if (!fname[0] || fname[0] == '.' || strchr(fname, '/') || strlen(fname) > 100) {
        reply(c, "ERR 007 BAD_SYNTAX");                  /* blocks ../ tricks */
        free(data);
        return 0;
    }

    char hdr[LINE_LEN];
    size_t hl = fmt_line(hdr, sizeof hdr, "MSG FILE %s %s %zu", c->name, fname, size);
    int st = send_to(c, args, 0, hdr, hl, data, size);   /* a user first... */
    if (st == 1) st = send_to(c, args, 1, hdr, hl, data, size);   /* ...then a room */

    if (st == 1) reply(c, "ERR 002 USER_NOT_FOUND");
    else if (st == 2) reply(c, "ERR 011 NOT_IN_ROOM");
    else {                                               /* keep a copy on the server */
        char path[512];
        mkdir("./storage", 0755);
        mkdir(STORAGE, 0755);
        snprintf(path, sizeof path, STORAGE "/%s", c->name);
        mkdir(path, 0755);
        snprintf(path, sizeof path, STORAGE "/%s/%s", c->name, fname);
        FILE *f = fopen(path, "wb");
        if (!f) reply(c, "ERR 013 STORAGE_ERROR");
        else {
            fwrite(data, 1, size, f);
            fclose(f);
            reply(c, "OK FILE_RECEIVED %s", fname);
            log_event("FILE from=%s to=%s name=%s size=%zu", c->name, args, fname, size);
        }
    }
    free(data);
    return 0;
}

/* ---------------- lifecycle ---------------- */
static void cleanup_client(Client *c)
{
    pthread_mutex_lock(&clients_lock);                   /* after this nobody can reach c, */
    for (int i = 0; i < MAX_CLIENTS; i++)                /* and its rooms vanish with it   */
        if (clients[i] == c) clients[i] = NULL;
    pthread_mutex_unlock(&clients_lock);

    if (c->registered) {
        char line[LINE_LEN];
        broadcast(NULL, line, fmt_line(line, sizeof line, "MSG LEAVE %s", c->name));
    }
    log_event("DISCONNECT user=%s ip=%s", c->registered ? c->name : "(unregistered)", c->ip);
    close(c->fd);
    pthread_mutex_destroy(&c->wlock);
    free(c);
}

static void *handle_client(void *arg)
{
    Client *c = arg;
    char line[LINE_LEN];

    for (;;) {
        int r = read_line(c, line, sizeof line);
        if (r == 0) break;                               /* disconnect, graceful or not */
        if (r < 0) { reply(c, "ERR 007 BAD_SYNTAX"); break; }
        if (!line[0]) continue;

        char *args = strchr(line, ' ');                  /* split "COMMAND args..." */
        if (args) *args++ = '\0'; else args = line + strlen(line);

        if (strcmp(line, "REGISTER") == 0)  cmd_register(c, args);
        else if (!c->registered)            reply(c, "ERR 006 NOT_REGISTERED");
        else if (strcmp(line, "LIST") == 0)   cmd_list(c);
        else if (strcmp(line, "BCAST") == 0)  cmd_bcast(c, args);
        else if (strcmp(line, "PMSG") == 0)   cmd_pmsg(c, args);
        else if (strcmp(line, "JOIN") == 0)   cmd_join(c, args);
        else if (strcmp(line, "LEAVE") == 0)  cmd_leave(c, args);
        else if (strcmp(line, "ROOMS") == 0)  cmd_rooms(c);
        else if (strcmp(line, "RMSG") == 0)   cmd_rmsg(c, args);
        else if (strcmp(line, "SENDFILE") == 0) { if (cmd_sendfile(c, args)) break; }
        else if (strcmp(line, "QUIT") == 0)   { reply(c, "OK BYE"); break; }
        else reply(c, "ERR 005 UNKNOWN_COMMAND");
    }
    cleanup_client(c);
    return NULL;
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    int lfd = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(PORT),
                                .sin_addr.s_addr = htonl(INADDR_ANY) };
    if (lfd < 0 || bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0 || listen(lfd, 16) < 0) {
        perror("socket/bind/listen");
        return 1;
    }
    log_event("SERVER_START port=%d nid=%s", PORT, NID);

    for (;;) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof ca;
        int fd = accept(lfd, (struct sockaddr *)&ca, &cl);
        if (fd < 0) continue;

        Client *c = calloc(1, sizeof *c);
        c->fd = fd;
        pthread_mutex_init(&c->wlock, NULL);
        inet_ntop(AF_INET, &ca.sin_addr, c->ip, sizeof c->ip);

        int slot = -1;
        pthread_mutex_lock(&clients_lock);
        for (int i = 0; i < MAX_CLIENTS && slot < 0; i++)
            if (!clients[i]) { clients[i] = c; slot = i; }
        pthread_mutex_unlock(&clients_lock);

        pthread_t t;
        if (slot < 0) { reply(c, "ERR 010 SERVER_FULL"); close(fd); free(c); continue; }
        log_event("CONNECT ip=%s", c->ip);
        pthread_create(&t, NULL, handle_client, c);
        pthread_detach(t);
    }
}
