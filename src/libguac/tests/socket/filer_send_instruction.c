/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <CUnit/CUnit.h>
#include <guacamole/client.h>
#include <guacamole/protocol.h>
#include <guacamole/recording.h>
#include <guacamole/socket.h>
#include <guacamole/socket-filer.h>

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MOCK_FILER_MAX_CONTENT 65536

typedef struct mock_filer_server {
    int listen_fd;
    int port;
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t cond;

    int stop;
    int fail_remaining;

    char last_path[256];
    char state[64];
    int zero_byte_offset_put_seen;
    int put_count;

    char content[MOCK_FILER_MAX_CONTENT];
    size_t content_len;
} mock_filer_server;

static int read_exact(int fd, char* buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = recv(fd, buf + off, len - off, 0);
        if (n <= 0)
            return -1;
        off += (size_t) n;
    }
    return 0;
}

static int handle_http_connection(mock_filer_server* srv, int client_fd) {
    char header_buf[4096];

    for (;;) {
        size_t used = 0;
        char* header_end = NULL;

        while (used + 1 < sizeof(header_buf)) {
            ssize_t n = recv(client_fd, header_buf + used, 1, 0);
            if (n <= 0)
                return 0;
            used += (size_t) n;
            header_buf[used] = '\0';
            header_end = strstr(header_buf, "\r\n\r\n");
            if (header_end != NULL)
                break;
        }

        if (header_end == NULL)
            return -1;

        char method[16] = {0};
        char target[512] = {0};
        if (sscanf(header_buf, "%15s %511s", method, target) != 2)
            return -1;

        size_t content_length = 0;
        char state_hdr[64] = {0};
        int connection_close = 0;

        char* line = strstr(header_buf, "\r\n");
        while (line != NULL && line < header_end) {
            line += 2;
            if (line >= header_end)
                break;
            if (strncasecmp(line, "Content-Length:", 15) == 0) {
                content_length = (size_t) strtoull(line + 15, NULL, 10);
            }
            else if (strncasecmp(line, "Seaweed-Admingate-State:", 24) == 0) {
                const char* val = line + 24;
                while (*val == ' ' || *val == '\t')
                    val++;
                size_t i = 0;
                while (val[i] != '\0' && val[i] != '\r' && val[i] != '\n'
                        && i + 1 < sizeof(state_hdr)) {
                    state_hdr[i] = val[i];
                    i++;
                }
                state_hdr[i] = '\0';
            }
            else if (strncasecmp(line, "Connection:", 11) == 0) {
                if (strncasecmp(line + 11, " close", 6) == 0)
                    connection_close = 1;
            }
            line = strstr(line, "\r\n");
        }

        char* body = NULL;
        if (content_length > 0) {
            body = malloc(content_length);
            if (body == NULL)
                return -1;
            if (read_exact(client_fd, body, content_length) != 0) {
                free(body);
                return -1;
            }
        }

        pthread_mutex_lock(&srv->lock);
        int should_fail = 0;
        if (srv->fail_remaining > 0) {
            srv->fail_remaining--;
            should_fail = 1;
        }

        if (!should_fail && strcmp(method, "PUT") == 0) {
            srv->put_count++;

            char* qmark = strchr(target, '?');
            size_t path_len = qmark ? (size_t) (qmark - target) : strlen(target);
            if (path_len >= sizeof(srv->last_path))
                path_len = sizeof(srv->last_path) - 1;
            memcpy(srv->last_path, target, path_len);
            srv->last_path[path_len] = '\0';

            const char* query = qmark ? (qmark + 1) : "";
            if (state_hdr[0] != '\0') {
                snprintf(srv->state, sizeof(srv->state), "%s", state_hdr);
            }

            if (strstr(query, "op=append") != NULL) {
                if (content_length > 0 && srv->content_len + content_length <= MOCK_FILER_MAX_CONTENT) {
                    memcpy(srv->content + srv->content_len, body, content_length);
                    srv->content_len += content_length;
                }
            }
            else if (strncmp(query, "offset=", 7) == 0) {
                size_t offset = (size_t) strtoull(query + 7, NULL, 10);
                if (content_length == 0) {
                    srv->zero_byte_offset_put_seen = 1;
                }
                else if (offset + content_length <= MOCK_FILER_MAX_CONTENT) {
                    memcpy(srv->content + offset, body, content_length);
                    if (offset + content_length > srv->content_len)
                        srv->content_len = offset + content_length;
                }
            }

            pthread_cond_broadcast(&srv->cond);
        }
        pthread_mutex_unlock(&srv->lock);
        free(body);

        const char* resp = should_fail
            ? "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            : "HTTP/1.1 201 Created\r\nConnection: close\r\nContent-Length: 2\r\n\r\n{}";
        if (send(client_fd, resp, strlen(resp), 0) < 0)
            return -1;

        (void) connection_close;
        return 0;
    }
}

static void* mock_filer_thread(void* arg) {
    mock_filer_server* srv = (mock_filer_server*) arg;

    for (;;) {
        pthread_mutex_lock(&srv->lock);
        int stop = srv->stop;
        pthread_mutex_unlock(&srv->lock);
        if (stop)
            break;

        struct pollfd pfd = {
            .fd = srv->listen_fd,
            .events = POLLIN
        };
        int ready = poll(&pfd, 1, 50);
        if (ready <= 0)
            continue;

        int client_fd = accept(srv->listen_fd, NULL, NULL);
        if (client_fd < 0)
            continue;

        handle_http_connection(srv, client_fd);
        close(client_fd);
    }

    return NULL;
}

static void mock_filer_start(mock_filer_server* srv, int initial_failures) {
    memset(srv, 0, sizeof(*srv));
    srv->fail_remaining = initial_failures;
    pthread_mutex_init(&srv->lock, NULL);
    pthread_cond_init(&srv->cond, NULL);

    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    CU_ASSERT_NOT_EQUAL_FATAL(srv->listen_fd, -1);

    int opt = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    CU_ASSERT_EQUAL_FATAL(bind(srv->listen_fd, (struct sockaddr*) &addr, sizeof(addr)), 0);
    CU_ASSERT_EQUAL_FATAL(listen(srv->listen_fd, 8), 0);

    socklen_t len = sizeof(addr);
    CU_ASSERT_EQUAL_FATAL(getsockname(srv->listen_fd, (struct sockaddr*) &addr, &len), 0);
    srv->port = ntohs(addr.sin_port);

    CU_ASSERT_EQUAL_FATAL(pthread_create(&srv->thread, NULL, mock_filer_thread, srv), 0);
}

static void mock_filer_stop(mock_filer_server* srv) {
    pthread_mutex_lock(&srv->lock);
    srv->stop = 1;
    pthread_mutex_unlock(&srv->lock);

    shutdown(srv->listen_fd, SHUT_RDWR);
    pthread_join(srv->thread, NULL);
    close(srv->listen_fd);
    pthread_mutex_destroy(&srv->lock);
    pthread_cond_destroy(&srv->cond);
}

/**
 * Verifies that a Filer-backed socket publishes a snapshot of complete
 * instructions within the configured flush interval while the session is still
 * open (state "recording") and finalizes the object as "complete" upon close.
 */
void test_socket__filer_incremental_snapshot_and_complete(void) {

    mock_filer_server srv;
    mock_filer_start(&srv, 0);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv.port);

    guac_socket_filer_config cfg = {
        .filer_url = url,
        .path_prefix = "/recordings",
        .name = "hist-uuid-001",
        .max_buffer_bytes = 64 * 1024,
        .flush_threshold_bytes = 4096,
        .flush_interval_ms = 120,
        .http_timeout_ms = 2000,
        .close_timeout_ms = 3000
    };

    guac_socket* socket = guac_socket_open_filer(&cfg);
    CU_ASSERT_PTR_NOT_NULL_FATAL(socket);

    guac_protocol_send_name(socket, "demo");
    guac_protocol_send_sync(socket, 1000, 1);

    const char* expected_live = "4.name,4.demo;4.sync,4.1000,1.1;";

    /* Wait up to 1.5s (well within 5s target) for live snapshot to appear */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 1;
    deadline.tv_nsec += 500000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        deadline.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&srv.lock);
    while (srv.content_len < strlen(expected_live)) {
        if (pthread_cond_timedwait(&srv.cond, &srv.lock, &deadline) == ETIMEDOUT)
            break;
    }
    CU_ASSERT_EQUAL(srv.content_len, strlen(expected_live));
    CU_ASSERT_NSTRING_EQUAL(srv.content, expected_live, strlen(expected_live));
    CU_ASSERT_STRING_EQUAL(srv.state, "recording");
    CU_ASSERT_STRING_EQUAL(srv.last_path, "/recordings/hist-uuid-001");
    pthread_mutex_unlock(&srv.lock);

    /* Append one more instruction and close socket */
    guac_protocol_send_sync(socket, 2000, 1);
    guac_socket_free(socket);

    const char* expected_final = "4.name,4.demo;4.sync,4.1000,1.1;4.sync,4.2000,1.1;";

    pthread_mutex_lock(&srv.lock);
    CU_ASSERT_EQUAL(srv.content_len, strlen(expected_final));
    CU_ASSERT_NSTRING_EQUAL(srv.content, expected_final, strlen(expected_final));
    CU_ASSERT_STRING_EQUAL(srv.state, "complete");
    CU_ASSERT_EQUAL(srv.zero_byte_offset_put_seen, 0);
    pthread_mutex_unlock(&srv.lock);

    mock_filer_stop(&srv);

}

/**
 * Verifies that transient Filer HTTP 503 failures are retried automatically at
 * the same byte offset without losing or duplicating data, and that partial
 * bytes written before guac_socket_instruction_end() are never published early.
 */
void test_socket__filer_retries_transient_failure_and_preserves_boundaries(void) {

    mock_filer_server srv;
    mock_filer_start(&srv, 0);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv.port);

    guac_socket_filer_config cfg = {
        .filer_url = url,
        .path_prefix = "/recordings",
        .name = "hist-uuid-retry",
        .max_buffer_bytes = 64 * 1024,
        .flush_threshold_bytes = 16,
        .flush_interval_ms = 60,
        .http_timeout_ms = 1000,
        .close_timeout_ms = 3000
    };

    guac_socket* socket = guac_socket_open_filer(&cfg);
    CU_ASSERT_PTR_NOT_NULL_FATAL(socket);

    /* Wait for initial 0-byte recording creation, then inject 3 HTTP 503 failures */
    pthread_mutex_lock(&srv.lock);
    while (srv.put_count < 1)
        pthread_cond_wait(&srv.cond, &srv.lock);
    srv.fail_remaining = 3;
    pthread_mutex_unlock(&srv.lock);

    /* Complete first instruction (exceeds flush_threshold_bytes=16 -> wakes worker, hits 503s) */
    guac_protocol_send_sync(socket, 1000, 1);

    /* Start a second instruction without ending it yet, wait past flush_interval_ms */
    guac_socket_instruction_begin(socket);
    guac_socket_write_string(socket, "4.name,");

    struct timespec pause_ts = { .tv_sec = 0, .tv_nsec = 180000000L };
    nanosleep(&pause_ts, NULL);

    /* Any snapshot published so far must end on ';' (never mid-instruction) */
    pthread_mutex_lock(&srv.lock);
    if (srv.content_len > 0) {
        CU_ASSERT_EQUAL(srv.content[srv.content_len - 1], ';');
    }
    pthread_mutex_unlock(&srv.lock);

    /* Finish second instruction and close */
    guac_socket_write_string(socket, "2.ok;");
    guac_socket_instruction_end(socket);

    guac_socket_free(socket);

    const char* expected = "4.sync,4.1000,1.1;4.name,2.ok;";
    pthread_mutex_lock(&srv.lock);
    CU_ASSERT_EQUAL(srv.content_len, strlen(expected));
    CU_ASSERT_NSTRING_EQUAL(srv.content, expected, strlen(expected));
    CU_ASSERT_STRING_EQUAL(srv.state, "complete");
    CU_ASSERT_EQUAL(srv.zero_byte_offset_put_seen, 0);
    pthread_mutex_unlock(&srv.lock);

    mock_filer_stop(&srv);

}

/**
 * Verifies that exceeding max_buffer_bytes during a Filer outage never blocks
 * writes, preserves a valid instruction-aligned prefix within the byte cap,
 * marks the final object state as "incomplete", and bounds close wait time
 * when Filer remains unavailable.
 */
void test_socket__filer_overflow_marks_incomplete_and_bounds_close(void) {

    mock_filer_server srv;
    mock_filer_start(&srv, 0);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv.port);

    /* Each "4.sync,4.1000,1.1;" is 18 bytes -> 64 bytes holds 3 instructions (54 bytes) */
    guac_socket_filer_config cfg = {
        .filer_url = url,
        .path_prefix = "/recordings",
        .name = "hist-uuid-overflow",
        .max_buffer_bytes = 64,
        .flush_threshold_bytes = 1024,
        .flush_interval_ms = 500,
        .http_timeout_ms = 500,
        .close_timeout_ms = 2000
    };

    guac_socket* socket = guac_socket_open_filer(&cfg);
    CU_ASSERT_PTR_NOT_NULL_FATAL(socket);

    pthread_mutex_lock(&srv.lock);
    while (srv.put_count < 1)
        pthread_cond_wait(&srv.cond, &srv.lock);
    srv.fail_remaining = 1000;
    pthread_mutex_unlock(&srv.lock);

    /* Write 20 instructions (360 bytes >> 64-byte cap); must not block or fail */
    for (int i = 0; i < 20; i++) {
        CU_ASSERT_EQUAL(guac_protocol_send_sync(socket, 1000 + i, 1), 0);
    }

    /* Restore Filer and close socket */
    pthread_mutex_lock(&srv.lock);
    srv.fail_remaining = 0;
    pthread_mutex_unlock(&srv.lock);

    guac_socket_free(socket);

    const char* expected_prefix =
        "4.sync,4.1000,1.1;"
        "4.sync,4.1001,1.1;"
        "4.sync,4.1002,1.1;";

    pthread_mutex_lock(&srv.lock);
    CU_ASSERT_EQUAL(srv.content_len, strlen(expected_prefix));
    CU_ASSERT_NSTRING_EQUAL(srv.content, expected_prefix, strlen(expected_prefix));
    CU_ASSERT_STRING_EQUAL(srv.state, "incomplete");
    CU_ASSERT_EQUAL(srv.zero_byte_offset_put_seen, 0);
    pthread_mutex_unlock(&srv.lock);

    /* Now verify close_timeout_ms bounds guac_socket_free() when Filer stays failing */
    pthread_mutex_lock(&srv.lock);
    srv.fail_remaining = 8;
    pthread_mutex_unlock(&srv.lock);

    guac_socket_filer_config stuck_cfg = {
        .filer_url = url,
        .path_prefix = "/recordings",
        .name = "hist-uuid-close-timeout",
        .max_buffer_bytes = 4096,
        .flush_threshold_bytes = 16,
        .flush_interval_ms = 50,
        .http_timeout_ms = 200,
        .close_timeout_ms = 180
    };

    guac_socket* stuck_socket = guac_socket_open_filer(&stuck_cfg);
    CU_ASSERT_PTR_NOT_NULL_FATAL(stuck_socket);
    guac_protocol_send_sync(stuck_socket, 9999, 1);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    guac_socket_free(stuck_socket);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000L
        + (t1.tv_nsec - t0.tv_nsec) / 1000000L;
    CU_ASSERT_TRUE(elapsed_ms < 700);

    mock_filer_stop(&srv);

}

/**
 * Verifies that guac_recording_create() preserves filesystem storage by
 * default, sends session recordings to SeaweedFS Filer without touching local
 * disk when GUACD_RECORDING_BACKEND=seaweedfs, and never falls back to local
 * filesystem when SeaweedFS configuration is missing.
 */
void test_socket__recording_backend_opt_in_and_no_fallback(void) {

    char tmp_template[] = "/tmp/guac-filer-rec-XXXXXX";
    char* tmp_dir = mkdtemp(tmp_template);
    CU_ASSERT_PTR_NOT_NULL_FATAL(tmp_dir);

    /* 1. Default (unset): writes local file */
    unsetenv("GUACD_RECORDING_BACKEND");
    unsetenv("GUACD_SEAWEEDFS_FILER_URL");

    guac_client* fs_client = guac_client_alloc();
    CU_ASSERT_PTR_NOT_NULL_FATAL(fs_client);
    fs_client->socket = guac_socket_alloc();

    guac_recording* fs_rec = guac_recording_create(fs_client, tmp_dir,
            "local-rec", 0, 1, 0, 0, 0, 1, 0);
    CU_ASSERT_PTR_NOT_NULL(fs_rec);
    guac_protocol_send_sync(fs_client->socket, 111, 1);
    if (fs_rec != NULL)
        guac_recording_free(fs_rec);
    guac_client_free(fs_client);

    char local_path[512];
    snprintf(local_path, sizeof(local_path), "%s/local-rec", tmp_dir);
    struct stat st;
    CU_ASSERT_EQUAL(stat(local_path, &st), 0);
    unlink(local_path);

    /* 2. seaweedfs without URL: fails without creating local file */
    setenv("GUACD_RECORDING_BACKEND", "seaweedfs", 1);
    unsetenv("GUACD_SEAWEEDFS_FILER_URL");

    guac_client* bad_client = guac_client_alloc();
    CU_ASSERT_PTR_NOT_NULL_FATAL(bad_client);
    bad_client->socket = guac_socket_alloc();

    guac_recording* bad_rec = guac_recording_create(bad_client, tmp_dir,
            "no-fallback-rec", 0, 1, 0, 0, 0, 1, 0);
    CU_ASSERT_PTR_NULL(bad_rec);
    guac_client_free(bad_client);

    char no_fallback_path[512];
    snprintf(no_fallback_path, sizeof(no_fallback_path), "%s/no-fallback-rec", tmp_dir);
    CU_ASSERT_NOT_EQUAL(stat(no_fallback_path, &st), 0);

    /* 3. seaweedfs with valid URL: writes to Filer and creates no local file */
    mock_filer_server srv;
    mock_filer_start(&srv, 0);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv.port);
    setenv("GUACD_SEAWEEDFS_FILER_URL", url, 1);
    setenv("GUACD_SEAWEEDFS_RECORDING_PREFIX", "/recordings-spike", 1);

    guac_client* filer_client = guac_client_alloc();
    CU_ASSERT_PTR_NOT_NULL_FATAL(filer_client);
    filer_client->socket = guac_socket_alloc();

    guac_recording* filer_rec = guac_recording_create(filer_client, tmp_dir,
            "filer-rec-001", 0, 1, 0, 0, 0, 1, 0);
    CU_ASSERT_PTR_NOT_NULL(filer_rec);
    guac_protocol_send_sync(filer_client->socket, 222, 1);
    if (filer_rec != NULL)
        guac_recording_free(filer_rec);
    guac_client_free(filer_client);

    char filer_local_path[512];
    snprintf(filer_local_path, sizeof(filer_local_path), "%s/filer-rec-001", tmp_dir);
    CU_ASSERT_NOT_EQUAL(stat(filer_local_path, &st), 0);

    const char* expected = "4.sync,3.222,1.1;";
    pthread_mutex_lock(&srv.lock);
    CU_ASSERT_EQUAL(srv.content_len, strlen(expected));
    CU_ASSERT_NSTRING_EQUAL(srv.content, expected, strlen(expected));
    CU_ASSERT_STRING_EQUAL(srv.state, "complete");
    CU_ASSERT_STRING_EQUAL(srv.last_path, "/recordings-spike/filer-rec-001");
    pthread_mutex_unlock(&srv.lock);

    mock_filer_stop(&srv);

    unsetenv("GUACD_RECORDING_BACKEND");
    unsetenv("GUACD_SEAWEEDFS_FILER_URL");
    unsetenv("GUACD_SEAWEEDFS_RECORDING_PREFIX");
    rmdir(tmp_dir);

}

/**
 * Verifies that an instruction exceeding max_buffer_bytes when ready_len == 0
 * transitions the live Filer object to "incomplete" before socket close, and
 * that unsafe or shared-area prefixes (/public, /work, /workspace, /, ..) are
 * rejected.
 */
void test_socket__filer_live_empty_overflow_and_prefix_isolation(void) {

    mock_filer_server srv;
    mock_filer_start(&srv, 0);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv.port);

    /* Reject forbidden prefixes */
    const char* bad_prefixes[] = {
        "/",
        "/public",
        "/public/sub",
        "/work",
        "/work/w1",
        "/workspace",
        "/recordings/../public",
        "./recordings"
    };
    for (size_t i = 0; i < sizeof(bad_prefixes) / sizeof(bad_prefixes[0]); i++) {
        guac_socket_filer_config bad_cfg = {
            .filer_url = url,
            .path_prefix = bad_prefixes[i],
            .name = "hist-bad-prefix"
        };
        guac_socket* bad_sock = guac_socket_open_filer(&bad_cfg);
        CU_ASSERT_PTR_NULL(bad_sock);
        if (bad_sock != NULL)
            guac_socket_free(bad_sock);
    }

    /* Single instruction exceeds max_buffer_bytes=12 while ready_len == 0 */
    guac_socket_filer_config cfg = {
        .filer_url = url,
        .path_prefix = "/recordings",
        .name = "hist-live-overflow",
        .max_buffer_bytes = 12,
        .flush_threshold_bytes = 64,
        .flush_interval_ms = 60,
        .http_timeout_ms = 1000,
        .close_timeout_ms = 2000
    };

    guac_socket* socket = guac_socket_open_filer(&cfg);
    CU_ASSERT_PTR_NOT_NULL_FATAL(socket);

    pthread_mutex_lock(&srv.lock);
    while (srv.put_count < 1)
        pthread_cond_wait(&srv.cond, &srv.lock);
    pthread_mutex_unlock(&srv.lock);

    /* "4.sync,4.1000,1.1;" is 18 bytes > 12-byte cap */
    CU_ASSERT_EQUAL(guac_protocol_send_sync(socket, 1000, 1), 0);

    /* Without closing the socket, Filer state must transition to "incomplete" */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 1;

    pthread_mutex_lock(&srv.lock);
    while (strcmp(srv.state, "incomplete") != 0) {
        if (pthread_cond_timedwait(&srv.cond, &srv.lock, &deadline) == ETIMEDOUT)
            break;
    }
    CU_ASSERT_STRING_EQUAL(srv.state, "incomplete");
    CU_ASSERT_EQUAL(srv.content_len, 0);
    pthread_mutex_unlock(&srv.lock);

    guac_socket_free(socket);
    mock_filer_stop(&srv);

}

