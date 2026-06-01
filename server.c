#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>

#define PORT       8080
#define MAX_PEERS  64
#define RBUF_SIZE  4096

typedef struct {
    int  fd;
    char src_ip[INET_ADDRSTRLEN];
    char dst_ip[INET_ADDRSTRLEN];
    int  ready;          /* 0 = waiting for recipient IP, 1 = chatting */
    char rbuf[RBUF_SIZE];
    int  rlen;
} Peer;

/* Buffered messages queued for a recipient who isn't connected yet.
   Multiple senders overwrite from_ip; all their messages are concatenated. */
typedef struct {
    char for_ip[INET_ADDRSTRLEN];
    char from_ip[INET_ADDRSTRLEN];
    char *data;
    int   len;
    int   cap;
} Inbox;

static Peer  peers[MAX_PEERS];
static int   npeers;
static Inbox inboxes[MAX_PEERS];
static int   ninboxes;

/* ── helpers ──────────────────────────────────────────────────── */

static Peer *find_peer(const char *ip)
{
    for (int i = 0; i < npeers; i++)
        if (strcmp(peers[i].src_ip, ip) == 0) return &peers[i];
    return NULL;
}

static Inbox *find_inbox(const char *ip)
{
    for (int i = 0; i < ninboxes; i++)
        if (strcmp(inboxes[i].for_ip, ip) == 0) return &inboxes[i];
    return NULL;
}

static Inbox *get_inbox(const char *for_ip, const char *from_ip)
{
    Inbox *b = find_inbox(for_ip);
    if (!b) {
        if (ninboxes >= MAX_PEERS) return NULL;
        b = &inboxes[ninboxes++];
        strncpy(b->for_ip, for_ip, INET_ADDRSTRLEN - 1);
        b->data = NULL; b->len = b->cap = 0;
    }
    strncpy(b->from_ip, from_ip, INET_ADDRSTRLEN - 1);
    return b;
}

static void inbox_append(Inbox *b, const char *line, int len)
{
    int need = b->len + len + 2;
    if (need > b->cap) {
        b->cap = need < 4096 ? 4096 : need * 2;
        b->data = realloc(b->data, b->cap);
    }
    memcpy(b->data + b->len, line, len);
    b->len += len;
    b->data[b->len++] = '\n';
}

static void drop_inbox(const char *ip)
{
    for (int i = 0; i < ninboxes; i++) {
        if (strcmp(inboxes[i].for_ip, ip) == 0) {
            free(inboxes[i].data);
            inboxes[i] = inboxes[--ninboxes];
            return;
        }
    }
}

static void drop_peer(int idx)
{
    close(peers[idx].fd);
    peers[idx] = peers[--npeers];
}

static void send_str(int fd, const char *s)
{
    write(fd, s, strlen(s));
}

/* ── per-line logic ───────────────────────────────────────────── */

static void flush_inbox(Peer *c)
{
    Inbox *b = find_inbox(c->src_ip);
    if (!b) return;
    write(c->fd, b->data, b->len);
    strncpy(c->dst_ip, b->from_ip, INET_ADDRSTRLEN - 1);
    char msg[64 + INET_ADDRSTRLEN];
    snprintf(msg, sizeof(msg), "[auto-connected to %s]\n", c->dst_ip);
    send_str(c->fd, msg);
    drop_inbox(c->src_ip);
}

static void on_line(Peer *c, const char *line, int len)
{
    if (!c->ready) {
        /* first line from this client is the recipient IP */
        strncpy(c->dst_ip, line, INET_ADDRSTRLEN - 1);
        c->ready = 1;
        printf("[*] %s -> %s\n", c->src_ip, c->dst_ip);

        /* flush any inbox that arrived while we were prompting */
        Inbox *b = find_inbox(c->src_ip);
        if (b) {
            flush_inbox(c);
        } else if (find_peer(c->dst_ip)) {
            send_str(c->fd, "[connected]\n");
        } else {
            send_str(c->fd, "[waiting for recipient]\n");
        }
        return;
    }

    /* chat message: deliver or buffer */
    char buf[RBUF_SIZE + 2];
    memcpy(buf, line, len);
    buf[len] = '\n';

    Peer *dst = find_peer(c->dst_ip);
    if (dst) {
        write(dst->fd, buf, len + 1);
    } else {
        Inbox *b = get_inbox(c->dst_ip, c->src_ip);
        if (b) inbox_append(b, line, len);
    }
}

/* ── connection handling ──────────────────────────────────────── */

static void on_connect(int srv)
{
    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    int fd = accept(srv, (struct sockaddr *)&addr, &alen);
    if (fd < 0) return;
    if (npeers >= MAX_PEERS) { close(fd); return; }

    Peer *c    = &peers[npeers++];
    c->fd      = fd;
    c->ready   = 0;
    c->rlen    = 0;
    c->dst_ip[0] = '\0';
    inet_ntop(AF_INET, &addr.sin_addr, c->src_ip, INET_ADDRSTRLEN);
    printf("[+] %s connected\n", c->src_ip);

    /* if someone already queued messages for this IP, deliver them now
       and skip the recipient-prompt entirely */
    Inbox *b = find_inbox(c->src_ip);
    if (b) {
        c->ready = 1;
        flush_inbox(c);
    } else {
        send_str(fd, "recipient ip: ");
    }
}

static void on_data(int idx)
{
    Peer *c = &peers[idx];
    char tmp[1024];
    int n = read(c->fd, tmp, sizeof(tmp));
    if (n <= 0) {
        printf("[-] %s disconnected\n", c->src_ip);
        drop_peer(idx);
        return;
    }

    int space = (int)sizeof(c->rbuf) - c->rlen;
    if (n > space) n = space;
    memcpy(c->rbuf + c->rlen, tmp, n);
    c->rlen += n;

    /* consume complete newline-delimited lines */
    char *p   = c->rbuf;
    char *end = c->rbuf + c->rlen;
    char *nl;
    while ((nl = memchr(p, '\n', end - p))) {
        int len = (int)(nl - p);
        if (len > 0 && p[len - 1] == '\r') len--;
        p[len] = '\0';
        if (len > 0) on_line(c, p, len);
        p = nl + 1;
    }

    int leftover = (int)(end - p);
    memmove(c->rbuf, p, leftover);
    c->rlen = leftover;
}

/* ── main ─────────────────────────────────────────────────────── */

int main(void)
{
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(PORT),
        .sin_addr.s_addr = INADDR_ANY
    };
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    listen(srv, 16);
    printf("[*] listening on :%d\n", PORT);

    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(srv, &rfds);
        int maxfd = srv;
        for (int i = 0; i < npeers; i++) {
            FD_SET(peers[i].fd, &rfds);
            if (peers[i].fd > maxfd) maxfd = peers[i].fd;
        }

        if (select(maxfd + 1, &rfds, NULL, NULL, NULL) < 0) {
            if (errno == EINTR) continue;
            perror("select"); break;
        }

        if (FD_ISSET(srv, &rfds)) on_connect(srv);

        for (int i = 0; i < npeers; i++) {
            if (FD_ISSET(peers[i].fd, &rfds)) {
                on_data(i);
                i--; /* on_data may have removed peers[i] via drop_peer */
            }
        }
    }

    close(srv);
    return 0;
}
