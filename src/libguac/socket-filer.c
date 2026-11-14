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

#include "guacamole/error.h"
#include "guacamole/mem.h"
#include "guacamole/socket-filer.h"

#include <curl/curl.h>

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

static pthread_once_t guac_filer_curl_once = PTHREAD_ONCE_INIT;

#define GUAC_FILER_STATE_RECORDING  "recording"
#define GUAC_FILER_STATE_COMPLETE   "complete"
#define GUAC_FILER_STATE_INCOMPLETE "incomplete"

static void guac_filer_curl_global_init(void) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

typedef struct guac_socket_filer_data {

    char object_url[2048];

    size_t max_buffer_bytes;
    size_t flush_threshold_bytes;
    int flush_interval_ms;
    int http_timeout_ms;
    int close_timeout_ms;

    pthread_mutex_t socket_lock;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_t worker;

    /* Unified buffer: [0..ready_len) committed, [ready_len..ready_len+inst_len) staging */
    int in_instruction;
    int inst_overflowed;
    char* buf;
    size_t ready_len;
    size_t inst_len;
    size_t buf_cap;

    /* Exact-sized buffer currently being uploaded; freed upon HTTP 2xx */
    char* inflight_buf;
    size_t inflight_len;

    uint64_t next_offset;
    int init_sent;
    int overflowed;
    int incomplete_synced;
    int closing;
    int64_t close_deadline_ms;
    int aborted;

} guac_socket_filer_data;

static size_t discard_write_cb(char* ptr, size_t size, size_t nmemb, void* userdata) {
    (void) ptr;
    (void) userdata;
    return size * nmemb;
}

static void add_ms_to_timespec(struct timespec* ts, int ms) {
    ts->tv_sec += ms / 1000;
    ts->tv_nsec += (long) (ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += 1;
        ts->tv_nsec -= 1000000000L;
    }
}

static int64_t monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000 + (int64_t) (ts.tv_nsec / 1000000L);
}

static int perform_filer_put(CURL* curl, const char* object_url,
        int is_append, uint64_t offset, const char* body, size_t body_len,
        const char* state_label, int timeout_ms) {

    char request_url[2176];
    if (is_append) {
        if (snprintf(request_url, sizeof(request_url),
                "%s?op=append", object_url) >= (int) sizeof(request_url))
            return -1;
    }
    else {
        if (snprintf(request_url, sizeof(request_url),
                "%s?offset=%" PRIu64, object_url, offset) >= (int) sizeof(request_url))
            return -1;
    }

    char state_header[96];
    snprintf(state_header, sizeof(state_header),
            "Seaweed-Admingate-State: %s", state_label);

    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Expect:");
    headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
    headers = curl_slist_append(headers, state_header);

    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_URL, request_url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body_len > 0 ? body : "");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t) body_len);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_write_cb);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long) timeout_ms);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long) timeout_ms);

    CURLcode res = curl_easy_perform(curl);
    long status = 0;
    if (res == CURLE_OK)
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

    curl_slist_free_all(headers);

    if (res != CURLE_OK || status < 200 || status >= 300)
        return -1;

    return 0;
}

static int ensure_capacity_bounded(char** buf, size_t* cap,
        size_t needed, size_t max_cap) {
    if (needed > max_cap)
        return -1;
    if (needed <= *cap)
        return 0;

    size_t new_cap = *cap == 0 ? 4096 : *cap;
    if (new_cap > max_cap)
        new_cap = max_cap;
    while (new_cap < needed) {
        if (new_cap > max_cap / 2) {
            new_cap = max_cap;
            break;
        }
        new_cap *= 2;
    }

    char* grown = realloc(*buf, new_cap);
    if (grown == NULL)
        return -1;

    *buf = grown;
    *cap = new_cap;
    return 0;
}

static int effective_timeout_ms_locked(const guac_socket_filer_data* data) {
    if (!data->closing)
        return data->http_timeout_ms;

    int64_t remaining = data->close_deadline_ms - monotonic_ms();
    if (remaining <= 0)
        return 0;
    if (remaining < (int64_t) data->http_timeout_ms)
        return (int) remaining;
    return data->http_timeout_ms;
}

static void wait_retry_backoff_locked(guac_socket_filer_data* data, int* backoff_ms) {
    int delay = *backoff_ms;
    if (data->closing) {
        int64_t remaining = data->close_deadline_ms - monotonic_ms();
        if (remaining <= 0) {
            data->aborted = 1;
            return;
        }
        if ((int64_t) delay > remaining)
            delay = (int) remaining;
    }

    struct timespec wake_at;
    clock_gettime(CLOCK_MONOTONIC, &wake_at);
    add_ms_to_timespec(&wake_at, delay);
    while (!data->aborted) {
        if (data->closing && monotonic_ms() >= data->close_deadline_ms) {
            data->aborted = 1;
            break;
        }
        if (pthread_cond_timedwait(&data->cond, &data->lock, &wake_at) == ETIMEDOUT)
            break;
        if (data->closing) {
            int64_t remaining = data->close_deadline_ms - monotonic_ms();
            if (remaining <= 0) {
                data->aborted = 1;
                break;
            }
        }
    }
    if (*backoff_ms < 1000) {
        *backoff_ms *= 2;
        if (*backoff_ms > 1000)
            *backoff_ms = 1000;
    }
}

static int send_zero_byte_state_with_retry(CURL* curl,
        guac_socket_filer_data* data, const char* state_label,
        int* backoff_ms) {
    int timeout_ms = effective_timeout_ms_locked(data);
    if (timeout_ms <= 0) {
        data->aborted = 1;
        return -1;
    }
    pthread_mutex_unlock(&data->lock);
    int rc = perform_filer_put(curl, data->object_url, 1, 0, "", 0,
            state_label, timeout_ms);
    pthread_mutex_lock(&data->lock);
    if (rc == 0) {
        *backoff_ms = 50;
        return 0;
    }
    wait_retry_backoff_locked(data, backoff_ms);
    return -1;
}

static void* guac_socket_filer_worker(void* arg) {
    guac_socket_filer_data* data = (guac_socket_filer_data*) arg;
    CURL* curl = curl_easy_init();
    if (curl == NULL)
        return NULL;

    int backoff_ms = 50;

    for (;;) {
        pthread_mutex_lock(&data->lock);

        if (data->closing && monotonic_ms() >= data->close_deadline_ms)
            data->aborted = 1;

        if (data->aborted) {
            pthread_mutex_unlock(&data->lock);
            break;
        }

        if (!data->init_sent) {
            int was_overflowed = data->overflowed;
            const char* init_state = was_overflowed
                ? GUAC_FILER_STATE_INCOMPLETE
                : GUAC_FILER_STATE_RECORDING;
            if (send_zero_byte_state_with_retry(curl, data, init_state, &backoff_ms) == 0) {
                data->init_sent = 1;
                if (was_overflowed)
                    data->incomplete_synced = 1;
            }
            pthread_mutex_unlock(&data->lock);
            continue;
        }

        while (!data->aborted && !data->closing && data->inflight_len == 0
                && !(data->overflowed && !data->incomplete_synced)
                && data->ready_len < data->flush_threshold_bytes) {
            struct timespec wake_at;
            clock_gettime(CLOCK_MONOTONIC, &wake_at);
            add_ms_to_timespec(&wake_at, data->flush_interval_ms);
            int rc = pthread_cond_timedwait(&data->cond, &data->lock, &wake_at);
            if (rc == ETIMEDOUT && data->ready_len > 0)
                break;
        }

        if (data->closing && monotonic_ms() >= data->close_deadline_ms)
            data->aborted = 1;

        if (data->aborted) {
            pthread_mutex_unlock(&data->lock);
            break;
        }

        if (data->inflight_len == 0 && data->ready_len > 0) {
            size_t batch_len = data->ready_len;
            if (data->inst_len == 0) {
                char* shrunk = realloc(data->buf, batch_len);
                data->inflight_buf = shrunk != NULL ? shrunk : data->buf;
                data->inflight_len = batch_len;
                data->buf = NULL;
                data->buf_cap = 0;
                data->ready_len = 0;
            }
            else {
                char* detached = malloc(batch_len);
                if (detached != NULL) {
                    memcpy(detached, data->buf, batch_len);
                    memmove(data->buf, data->buf + batch_len, data->inst_len);
                    data->ready_len = 0;
                    size_t max_rem = data->max_buffer_bytes - batch_len;
                    if (data->buf_cap > max_rem) {
                        size_t shrink_to = data->inst_len > 0 ? data->inst_len : 1;
                        char* shrunk = realloc(data->buf, shrink_to);
                        if (shrunk != NULL) {
                            data->buf = shrunk;
                            data->buf_cap = shrink_to;
                        }
                    }
                    data->inflight_buf = detached;
                    data->inflight_len = batch_len;
                }
            }
        }

        if (data->inflight_len > 0) {
            int timeout_ms = effective_timeout_ms_locked(data);
            if (timeout_ms <= 0) {
                data->aborted = 1;
                pthread_mutex_unlock(&data->lock);
                break;
            }
            int finalize_with_batch = (data->closing && data->ready_len == 0);
            int send_incomplete = data->overflowed;
            const char* state_label = send_incomplete
                ? GUAC_FILER_STATE_INCOMPLETE
                : (finalize_with_batch ? GUAC_FILER_STATE_COMPLETE : GUAC_FILER_STATE_RECORDING);
            uint64_t offset = data->next_offset;
            size_t batch_len = data->inflight_len;
            const char* batch_ptr = data->inflight_buf;
            pthread_mutex_unlock(&data->lock);

            int rc = perform_filer_put(curl, data->object_url, 0, offset,
                    batch_ptr, batch_len, state_label, timeout_ms);

            pthread_mutex_lock(&data->lock);
            if (rc == 0) {
                data->next_offset += batch_len;
                free(data->inflight_buf);
                data->inflight_buf = NULL;
                data->inflight_len = 0;
                if (send_incomplete)
                    data->incomplete_synced = 1;
                backoff_ms = 50;
                if (finalize_with_batch && data->ready_len == 0
                        && (!data->overflowed || data->incomplete_synced)) {
                    pthread_mutex_unlock(&data->lock);
                    break;
                }
            }
            else {
                wait_retry_backoff_locked(data, &backoff_ms);
            }
            pthread_mutex_unlock(&data->lock);
            continue;
        }

        if (data->overflowed && !data->incomplete_synced && !data->closing) {
            if (send_zero_byte_state_with_retry(curl, data,
                    GUAC_FILER_STATE_INCOMPLETE, &backoff_ms) == 0) {
                data->incomplete_synced = 1;
            }
            pthread_mutex_unlock(&data->lock);
            continue;
        }

        if (data->closing) {
            const char* final_state = data->overflowed
                ? GUAC_FILER_STATE_INCOMPLETE
                : GUAC_FILER_STATE_COMPLETE;
            int done = (send_zero_byte_state_with_retry(curl, data,
                    final_state, &backoff_ms) == 0);
            pthread_mutex_unlock(&data->lock);
            if (done)
                break;
            continue;
        }

        pthread_mutex_unlock(&data->lock);
    }

    curl_easy_cleanup(curl);
    return NULL;
}

static void guac_socket_filer_lock_handler(guac_socket* socket) {
    guac_socket_filer_data* data = (guac_socket_filer_data*) socket->data;
    pthread_mutex_lock(&data->socket_lock);

    pthread_mutex_lock(&data->lock);
    data->in_instruction = 1;
    data->inst_overflowed = data->overflowed;
    data->inst_len = 0;
    pthread_mutex_unlock(&data->lock);
}

static void guac_socket_filer_unlock_handler(guac_socket* socket) {
    guac_socket_filer_data* data = (guac_socket_filer_data*) socket->data;

    pthread_mutex_lock(&data->lock);
    if (data->in_instruction && !data->overflowed && !data->inst_overflowed
            && data->inst_len > 0) {
        data->ready_len += data->inst_len;
        if (data->ready_len >= data->flush_threshold_bytes)
            pthread_cond_signal(&data->cond);
    }
    data->inst_len = 0;
    data->inst_overflowed = 0;
    data->in_instruction = 0;
    pthread_mutex_unlock(&data->lock);

    pthread_mutex_unlock(&data->socket_lock);
}

static ssize_t guac_socket_filer_write_handler(guac_socket* socket,
        const void* buf, size_t count) {

    if (count == 0)
        return 0;

    guac_socket_filer_data* data = (guac_socket_filer_data*) socket->data;

    pthread_mutex_lock(&data->lock);
    if (!data->overflowed) {
        size_t max_local_cap = data->max_buffer_bytes - data->inflight_len;
        size_t local_used = data->ready_len + data->inst_len;
        if (count > max_local_cap || local_used > max_local_cap - count
                || ensure_capacity_bounded(&data->buf, &data->buf_cap,
                        local_used + count, max_local_cap) != 0) {
            data->overflowed = 1;
            data->inst_overflowed = 1;
            data->inst_len = 0;
            pthread_cond_signal(&data->cond);
        }
        else if (data->in_instruction) {
            memcpy(data->buf + data->ready_len + data->inst_len, buf, count);
            data->inst_len += count;
        }
        else {
            memcpy(data->buf + data->ready_len, buf, count);
            data->ready_len += count;
            if (data->ready_len >= data->flush_threshold_bytes)
                pthread_cond_signal(&data->cond);
        }
    }
    pthread_mutex_unlock(&data->lock);

    return (ssize_t) count;
}

static ssize_t guac_socket_filer_flush_handler(guac_socket* socket) {
    (void) socket;
    return 0;
}

void guac_socket_filer_begin_close(guac_socket* socket) {
    guac_socket_filer_data* data = (guac_socket_filer_data*) socket->data;

    pthread_mutex_lock(&data->lock);
    if (!data->closing) {
        data->closing = 1;
        data->close_deadline_ms = monotonic_ms() + (int64_t) data->close_timeout_ms;
    }
    pthread_cond_signal(&data->cond);
    pthread_mutex_unlock(&data->lock);
}

void guac_socket_filer_mark_incomplete(guac_socket* socket) {
    guac_socket_filer_data* data = (guac_socket_filer_data*) socket->data;
    pthread_mutex_lock(&data->lock);
    data->overflowed = 1;
    pthread_cond_signal(&data->cond);
    pthread_mutex_unlock(&data->lock);
}

static int guac_socket_filer_free_handler(guac_socket* socket) {
    guac_socket_filer_data* data = (guac_socket_filer_data*) socket->data;
    guac_socket_filer_begin_close(socket);

    pthread_join(data->worker, NULL);

    free(data->buf);
    free(data->inflight_buf);
    pthread_cond_destroy(&data->cond);
    pthread_mutex_destroy(&data->lock);
    pthread_mutex_destroy(&data->socket_lock);
    guac_mem_free(data);

    return 0;
}

static int is_safe_object_name(const char* name) {
    if (name == NULL || name[0] == '\0')
        return 0;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
        return 0;
    for (const char* p = name; *p != '\0'; p++) {
        if (*p == '/' || *p == '\\' || *p == '?' || *p == '#')
            return 0;
    }
    return 1;
}

static int normalize_isolated_prefix(const char* raw_prefix,
        const char** out_start, size_t* out_len) {

    const char* p = (raw_prefix != NULL && raw_prefix[0] != '\0')
        ? raw_prefix
        : GUAC_SOCKET_FILER_DEFAULT_PATH_PREFIX;

    /* Must be an absolute Filer path (leading '/') without relative prefix */
    if (p[0] != '/')
        return 0;

    while (*p == '/')
        p++;

    size_t len = strlen(p);
    while (len > 0 && p[len - 1] == '/')
        len--;

    /* Reject root "/" with no isolated directory name */
    if (len == 0)
        return 0;

    /* Validate every segment and forbid workspace/public file transfer areas */
    size_t idx = 0;
    int first_segment = 1;
    while (idx < len) {
        size_t seg_start = idx;
        while (idx < len && p[idx] != '/') {
            if (p[idx] == '\\' || p[idx] == '?' || p[idx] == '#')
                return 0;
            idx++;
        }
        size_t seg_len = idx - seg_start;
        if (seg_len == 0)
            return 0;
        if ((seg_len == 1 && p[seg_start] == '.')
                || (seg_len == 2 && p[seg_start] == '.' && p[seg_start + 1] == '.'))
            return 0;
        if (first_segment) {
            if ((seg_len == 6 && strncasecmp(p + seg_start, "public", 6) == 0)
                    || (seg_len == 4 && strncasecmp(p + seg_start, "work", 4) == 0)
                    || (seg_len == 9 && strncasecmp(p + seg_start, "workspace", 9) == 0))
                return 0;
            first_segment = 0;
        }
        if (idx < len && p[idx] == '/')
            idx++;
    }

    *out_start = p;
    *out_len = len;
    return 1;
}

guac_socket* guac_socket_open_filer(const guac_socket_filer_config* config) {

    if (config == NULL || config->filer_url == NULL || config->filer_url[0] == '\0'
            || !is_safe_object_name(config->name)) {
        guac_error = GUAC_STATUS_INVALID_ARGUMENT;
        guac_error_message = "Invalid SeaweedFS Filer socket configuration";
        return NULL;
    }

    if (strncmp(config->filer_url, "http://", 7) != 0
            && strncmp(config->filer_url, "https://", 8) != 0) {
        guac_error = GUAC_STATUS_INVALID_ARGUMENT;
        guac_error_message = "SeaweedFS Filer URL must start with http:// or https://";
        return NULL;
    }

    const char* prefix = NULL;
    size_t prefix_len = 0;
    if (!normalize_isolated_prefix(config->path_prefix, &prefix, &prefix_len)) {
        guac_error = GUAC_STATUS_INVALID_ARGUMENT;
        guac_error_message = "SeaweedFS Filer recording prefix must be an isolated directory";
        return NULL;
    }

    pthread_once(&guac_filer_curl_once, guac_filer_curl_global_init);

    size_t base_len = strlen(config->filer_url);
    while (base_len > 0 && config->filer_url[base_len - 1] == '/')
        base_len--;

    guac_socket* socket = guac_socket_alloc();
    if (socket == NULL)
        return NULL;

    guac_socket_filer_data* data = guac_mem_zalloc(sizeof(guac_socket_filer_data));
    if (data == NULL) {
        guac_mem_free(socket);
        return NULL;
    }

    int written = snprintf(data->object_url, sizeof(data->object_url),
            "%.*s/%.*s/%s",
            (int) base_len, config->filer_url,
            (int) prefix_len, prefix,
            config->name);

    if (written < 0 || (size_t) written >= sizeof(data->object_url)) {
        guac_mem_free(data);
        guac_mem_free(socket);
        guac_error = GUAC_STATUS_INVALID_ARGUMENT;
        guac_error_message = "SeaweedFS Filer URL exceeds maximum length";
        return NULL;
    }

    data->max_buffer_bytes = config->max_buffer_bytes > 0
        ? config->max_buffer_bytes
        : GUAC_SOCKET_FILER_DEFAULT_MAX_BUFFER_BYTES;
    if (config->flush_threshold_bytes > 0) {
        data->flush_threshold_bytes = config->flush_threshold_bytes <= data->max_buffer_bytes
            ? config->flush_threshold_bytes
            : data->max_buffer_bytes;
    }
    else {
        size_t half_cap = data->max_buffer_bytes > 1 ? (data->max_buffer_bytes / 2) : 1;
        data->flush_threshold_bytes = GUAC_SOCKET_FILER_DEFAULT_FLUSH_THRESHOLD_BYTES <= half_cap
            ? GUAC_SOCKET_FILER_DEFAULT_FLUSH_THRESHOLD_BYTES
            : half_cap;
    }
    data->flush_interval_ms = config->flush_interval_ms > 0
        ? config->flush_interval_ms
        : GUAC_SOCKET_FILER_DEFAULT_FLUSH_INTERVAL_MS;
    data->http_timeout_ms = config->http_timeout_ms > 0
        ? config->http_timeout_ms
        : GUAC_SOCKET_FILER_DEFAULT_HTTP_TIMEOUT_MS;
    data->close_timeout_ms = config->close_timeout_ms > 0
        ? config->close_timeout_ms
        : GUAC_SOCKET_FILER_DEFAULT_CLOSE_TIMEOUT_MS;

    pthread_mutex_init(&data->socket_lock, NULL);
    pthread_mutex_init(&data->lock, NULL);

    pthread_condattr_t cond_attr;
    pthread_condattr_init(&cond_attr);
    pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC);
    pthread_cond_init(&data->cond, &cond_attr);
    pthread_condattr_destroy(&cond_attr);

    if (pthread_create(&data->worker, NULL, guac_socket_filer_worker, data) != 0) {
        pthread_cond_destroy(&data->cond);
        pthread_mutex_destroy(&data->lock);
        pthread_mutex_destroy(&data->socket_lock);
        guac_mem_free(data);
        guac_mem_free(socket);
        guac_error = GUAC_STATUS_NO_MEMORY;
        guac_error_message = "Unable to start Filer uploader thread";
        return NULL;
    }

    socket->data = data;
    socket->write_handler  = guac_socket_filer_write_handler;
    socket->flush_handler  = guac_socket_filer_flush_handler;
    socket->lock_handler   = guac_socket_filer_lock_handler;
    socket->unlock_handler = guac_socket_filer_unlock_handler;
    socket->free_handler   = guac_socket_filer_free_handler;

    return socket;
}
