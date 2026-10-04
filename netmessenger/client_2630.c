/*
 * NetMessenger client  --  IE3010 (IT23862630, port 8630)
 * STEP 1: raw-protocol client. Uses select() on stdin + socket.
 * Usage: ./client_2630 [server_ip]
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8630

int main(int argc, char **argv)
{
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    signal(SIGPIPE, SIG_IGN);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(PORT);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
        fprintf(stderr, "bad address: %s\n", host);
        return 1;
    }
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) { perror("connect"); return 1; }
    printf("Connected to %s:%d. Type protocol commands (e.g. REGISTER alice)\n", host, PORT);

    char buf[4096];
    for (;;) {
        fd_set rs;
        FD_ZERO(&rs);
        FD_SET(STDIN_FILENO, &rs);
        FD_SET(fd, &rs);
        if (select(fd + 1, &rs, NULL, NULL, NULL) < 0) { perror("select"); break; }

        if (FD_ISSET(fd, &rs)) {
            ssize_t n = recv(fd, buf, sizeof buf, 0);
            if (n <= 0) { printf("Server closed the connection.\n"); break; }
            fwrite(buf, 1, (size_t)n, stdout);
            fflush(stdout);
        }
        if (FD_ISSET(STDIN_FILENO, &rs)) {
            if (!fgets(buf, sizeof buf, stdin)) break;      /* Ctrl-D */
            size_t len = strlen(buf);
            if (send(fd, buf, len, MSG_NOSIGNAL) < 0) { perror("send"); break; }
        }
    }
    close(fd);
    return 0;
}
