#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>

int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <server_ip> <port>\n", argv[0]);
        return 1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port   = htons((uint16_t)atoi(argv[2]))
    };
    if (inet_pton(AF_INET, argv[1], &addr.sin_addr) != 1) {
        fprintf(stderr, "bad address: %s\n", argv[1]);
        return 1;
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); return 1;
    }

    char buf[4096];
    int maxfd = fd > STDIN_FILENO ? fd : STDIN_FILENO;

    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        FD_SET(fd, &rfds);

        if (select(maxfd + 1, &rfds, NULL, NULL, NULL) < 0) break;

        if (FD_ISSET(fd, &rfds)) {
            int n = read(fd, buf, sizeof(buf));
            if (n <= 0) { puts("[disconnected]"); break; }
            write(STDOUT_FILENO, buf, n);
        }

        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            int n = read(STDIN_FILENO, buf, sizeof(buf));
            if (n <= 0) break;
            write(fd, buf, n);
        }
    }

    close(fd);
    return 0;
}
