// Simple C client for mini-distributed-log-system
// Usage: c_client produce|fetch [args]

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define BUF_SIZE 8192

static void usage() {
    fprintf(stderr, "Usage:\n  c_client produce --host HOST --port PORT --topic TOPIC --partition N --value VAL\n  c_client fetch --host HOST --port PORT --topic TOPIC --partition N --offset N\n");
}

static int connect_host(const char *host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

static int send_frame(int sock, const char *json) {
    uint32_t len = (uint32_t)strlen(json);
    uint32_t be = htonl(len);
    if (write(sock, &be, 4) != 4) return -1;
    ssize_t sent = 0;
    while (sent < (ssize_t)len) {
        ssize_t n = write(sock, json + sent, len - sent);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

static char *read_frame(int sock) {
    uint32_t be_len;
    ssize_t r = read(sock, &be_len, 4);
    if (r != 4) return NULL;
    uint32_t len = ntohl(be_len);
    if (len == 0) return NULL;
    if (len > 10 * 1024 * 1024) return NULL; // sanity
    char *buf = malloc(len + 1);
    if (!buf) return NULL;
    size_t got = 0;
    while (got < len) {
        ssize_t n = read(sock, buf + got, len - got);
        if (n <= 0) { free(buf); return NULL; }
        got += n;
    }
    buf[len] = '\0';
    return buf;
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 2; }
    const char *cmd = argv[1];
    const char *host = "127.0.0.1";
    int port = 9000;
    const char *topic = NULL;
    int partition = 0;
    const char *value = NULL;
    int offset = 0;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) { host = argv[++i]; }
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) { port = atoi(argv[++i]); }
        else if (strcmp(argv[i], "--topic") == 0 && i + 1 < argc) { topic = argv[++i]; }
        else if (strcmp(argv[i], "--partition") == 0 && i + 1 < argc) { partition = atoi(argv[++i]); }
        else if (strcmp(argv[i], "--value") == 0 && i + 1 < argc) { value = argv[++i]; }
        else if (strcmp(argv[i], "--offset") == 0 && i + 1 < argc) { offset = atoi(argv[++i]); }
        else { fprintf(stderr, "Unknown arg: %s\n", argv[i]); usage(); return 2; }
    }

    if (!topic) { fprintf(stderr, "--topic is required\n"); usage(); return 2; }

    int sock = connect_host(host, port);
    if (sock < 0) { perror("connect"); return 1; }

    if (strcmp(cmd, "produce") == 0) {
        if (!value) { fprintf(stderr, "--value required for produce\n"); return 2; }
        char json[BUF_SIZE];
        snprintf(json, sizeof(json), "{\"type\":\"PRODUCE\",\"topic\":\"%s\",\"partition\":%d,\"value\":\"%s\"}", topic, partition, value);
        if (send_frame(sock, json) != 0) { perror("send_frame"); close(sock); return 1; }
        char *resp = read_frame(sock);
        if (resp) {
            printf("%s\n", resp);
            free(resp);
        }
    } else if (strcmp(cmd, "fetch") == 0) {
        char json[BUF_SIZE];
        snprintf(json, sizeof(json), "{\"type\":\"FETCH\",\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"max_bytes\":%d}", topic, partition, offset, 4096);
        if (send_frame(sock, json) != 0) { perror("send_frame"); close(sock); return 1; }
        char *resp = read_frame(sock);
        if (resp) {
            printf("%s\n", resp);
            free(resp);
        }
    } else {
        fprintf(stderr, "Unknown command: %s\n", cmd);
        usage();
        close(sock);
        return 2;
    }

    close(sock);
    return 0;
}
