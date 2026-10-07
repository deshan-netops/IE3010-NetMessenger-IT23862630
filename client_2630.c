/*
 * NetMessenger client -- IE3010 (IT23862630, port 8630)
 * Type raw protocol lines (REGISTER alice, BCAST hi, ...) or:
 *   /sendfile <target> <path>      -> builds the SENDFILE header and streams the file
 * Incoming files are saved to ./received/<sender>/<filename>
 * Usage: ./client_2630 [server_ip]
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8630

static char   rbuf[65536];          /* received but not yet processed */
static size_t rlen, rem;            /* bytes in rbuf / file bytes still expected */
static int    in_file;              /* 1 while the next bytes are raw file data */
static FILE  *rf;                   /* NULL while in_file => bytes are discarded */

static int send_bytes(int fd, const char *d, size_t n)
{
    while (n > 0) {
        ssize_t r = send(fd, d, n, MSG_NOSIGNAL);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) return -1;
        d += r;
        n -= (size_t)r;
    }
    return 0;
}

/* Handles partial lines, several lines per recv, and the raw bytes that
 * follow a "MSG FILE <sender> <name> <size>" line. */
static void process_input(void)
{
    for (;;) {
        if (in_file) {
            size_t take = rlen < rem ? rlen : rem;
            if (take == 0) return;
            if (rf) fwrite(rbuf, 1, take, rf);
            memmove(rbuf, rbuf + take, rlen - take);
            rlen -= take;
            rem -= take;
            if (rem == 0) { if (rf) fclose(rf); rf = NULL; in_file = 0; printf("[file received]\n"); }
            continue;
        }
        char *nl = memchr(rbuf, '\n', rlen);
        if (!nl) { if (rlen == sizeof rbuf) rlen = 0; return; }
        *nl = '\0';
        printf("%s\n", rbuf);

        char sender[64], fname[160], path[400];
        unsigned long size;
        int is_file = strncmp(rbuf, "MSG FILE ", 9) == 0 &&
                      sscanf(rbuf + 9, "%63s %159s %lu", sender, fname, &size) == 3;
        rlen -= (size_t)(nl + 1 - rbuf);
        memmove(rbuf, nl + 1, rlen);
        if (!is_file) continue;

        if (!strchr(sender, '/') && !strchr(fname, '/') && fname[0] != '.') {
            mkdir("./received", 0755);
            snprintf(path, sizeof path, "./received/%s", sender);
            mkdir(path, 0755);
            snprintf(path, sizeof path, "./received/%s/%s", sender, fname);
            rf = fopen(path, "wb");
        }
        printf("[receiving '%s' from %s, %lu bytes]\n", fname, sender, size);
        rem = size;
        in_file = rem > 0;
        if (!in_file) { if (rf) fclose(rf); rf = NULL; printf("[file received]\n"); }
    }
}

static int send_file(int fd, const char *target, const char *path)
{
    struct stat st;
    FILE *f = stat(path, &st) == 0 && S_ISREG(st.st_mode) ? fopen(path, "rb") : NULL;
    if (!f) { printf("[cannot read file: %s]\n", path); return 0; }
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;

    char hdr[600], chunk[8192];
    int hl = snprintf(hdr, sizeof hdr, "SENDFILE %s %s %lld\n", target, base, (long long)st.st_size);
    int rc = send_bytes(fd, hdr, (size_t)hl);
    for (long long left = st.st_size; rc == 0 && left > 0; ) {
        size_t got = fread(chunk, 1, left < (long long)sizeof chunk ? (size_t)left : sizeof chunk, f);
        if (got == 0) break;                             /* file shrank: stop */
        rc = send_bytes(fd, chunk, got);
        left -= (long long)got;
    }
    fclose(f);
    if (rc == 0) printf("[sent %lld bytes of '%s' to %s]\n", (long long)st.st_size, base, target);
    return rc;
}

int main(int argc, char **argv)
{
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    signal(SIGPIPE, SIG_IGN);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = { .sin_family = AF_INET, .sin_port = htons(PORT) };
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) {
        perror("connect");
        return 1;
    }
    printf("Connected to %s:%d. Raw protocol lines, or /sendfile <target> <path>\n", host, PORT);
    fflush(stdout);

    char line[2048];
    for (;;) {
        fd_set rs;
        FD_ZERO(&rs);
        FD_SET(STDIN_FILENO, &rs);
        FD_SET(fd, &rs);
        if (select(fd + 1, &rs, NULL, NULL, NULL) < 0) { if (errno == EINTR) continue; break; }

        if (FD_ISSET(fd, &rs)) {                         /* from the server */
            ssize_t n = recv(fd, rbuf + rlen, sizeof rbuf - rlen, 0);
            if (n <= 0) { printf("Server closed the connection.\n"); break; }
            rlen += (size_t)n;
            process_input();
        }
        if (FD_ISSET(STDIN_FILENO, &rs)) {               /* from the keyboard */
            if (!fgets(line, sizeof line, stdin)) break;
            char target[64], path[400];
            if (strncmp(line, "/sendfile ", 10) == 0) {
                if (sscanf(line + 10, "%63s %399s", target, path) != 2) printf("usage: /sendfile <target> <path>\n");
                else if (send_file(fd, target, path) < 0) break;
            } else if (send_bytes(fd, line, strlen(line)) < 0) break;
        }
        fflush(stdout);
    }
    close(fd);
    return 0;
}
