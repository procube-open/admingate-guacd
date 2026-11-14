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

#include "common/io.h"
#include "terminal/typescript.h"

#include <guacamole/file.h>
#include <guacamole/mem.h>
#include <guacamole/error.h>
#include <guacamole/socket-filer.h>
#include <guacamole/timestamp.h>

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

static size_t guac_typescript_env_size(const char* name) {
    const char* raw = getenv(name);
    if (raw == NULL || *raw == '\0')
        return 0;
    char* end;
    unsigned long long value = strtoull(raw, &end, 10);
    return end != raw && *end == '\0' ? (size_t) value : 0;
}

static int guac_typescript_env_int(const char* name, int maximum) {
    const char* raw = getenv(name);
    if (raw == NULL || *raw == '\0')
        return 0;
    char* end;
    long value = strtol(raw, &end, 10);
    if (end == raw || *end != '\0' || value <= 0)
        return 0;
    return value > maximum ? maximum : (int) value;
}

static void guac_typescript_socket_write(guac_socket* socket,
        const void* data, size_t length) {
    guac_socket_instruction_begin(socket);
    guac_socket_write(socket, data, length);
    guac_socket_instruction_end(socket);
}

static guac_terminal_typescript* guac_terminal_typescript_alloc_filer(
        const char* name) {
    guac_terminal_typescript* typescript = guac_mem_zalloc(sizeof(*typescript));
    if (typescript == NULL)
        return NULL;

    if (name == NULL
            || snprintf(typescript->data_filename, sizeof(typescript->data_filename),
                "%s", name) >= sizeof(typescript->data_filename)
            || snprintf(typescript->timing_filename, sizeof(typescript->timing_filename),
                "%s.%s", name, GUAC_TERMINAL_TYPESCRIPT_TIMING_SUFFIX)
                >= sizeof(typescript->timing_filename)) {
        guac_mem_free(typescript);
        guac_error = GUAC_STATUS_INVALID_ARGUMENT;
        guac_error_message = "Invalid typescript filename";
        return NULL;
    }

    /* Leave time for the screen recording to drain before the watchdog. */
    int http_timeout = guac_typescript_env_int("GUACD_SEAWEEDFS_HTTP_TIMEOUT_MS", 1000);
    int close_timeout = guac_typescript_env_int("GUACD_SEAWEEDFS_CLOSE_TIMEOUT_MS", 1000);
    guac_socket_filer_config config = {
        .filer_url = getenv("GUACD_SEAWEEDFS_FILER_URL"),
        .path_prefix = getenv("GUACD_SEAWEEDFS_RECORDING_PREFIX"),
        .name = typescript->data_filename,
        .max_buffer_bytes = guac_typescript_env_size("GUACD_SEAWEEDFS_MAX_BUFFER_BYTES"),
        .flush_threshold_bytes = guac_typescript_env_size("GUACD_SEAWEEDFS_FLUSH_THRESHOLD_BYTES"),
        .flush_interval_ms = guac_typescript_env_int("GUACD_SEAWEEDFS_FLUSH_INTERVAL_MS", 3600000),
        .http_timeout_ms = http_timeout > 0 ? http_timeout : 1000,
        .close_timeout_ms = close_timeout > 0 ? close_timeout : 1000
    };
    typescript->data_socket = guac_socket_open_filer(&config);
    if (typescript->data_socket == NULL) {
        guac_mem_free(typescript);
        return NULL;
    }
    config.name = typescript->timing_filename;
    typescript->timing_socket = guac_socket_open_filer(&config);
    if (typescript->timing_socket == NULL) {
        guac_socket_filer_mark_incomplete(typescript->data_socket);
        guac_socket_free(typescript->data_socket);
        guac_mem_free(typescript);
        return NULL;
    }
    typescript->last_flush = guac_timestamp_current();
    guac_typescript_socket_write(typescript->data_socket,
            GUAC_TERMINAL_TYPESCRIPT_HEADER,
            sizeof(GUAC_TERMINAL_TYPESCRIPT_HEADER) - 1);
    return typescript;
}

guac_terminal_typescript* guac_terminal_typescript_alloc(const char* path,
        const char* name, int create_path, int allow_write_existing) {

    const char* backend = getenv("GUACD_RECORDING_BACKEND");
    if (backend != NULL && *backend != '\0') {
        if (strcasecmp(backend, "seaweedfs") == 0)
            return guac_terminal_typescript_alloc_filer(name);
        if (strcasecmp(backend, "filesystem") != 0) {
            guac_error = GUAC_STATUS_INVALID_ARGUMENT;
            guac_error_message = "Unsupported recording backend";
            return NULL;
        }
    }

    /* Allocate space for new typescript */
    guac_terminal_typescript* typescript =
        guac_mem_zalloc(sizeof(guac_terminal_typescript));
    if (typescript == NULL)
        return NULL;

    guac_open_how data_how = {
        .oflags = O_CREAT | O_WRONLY,
        .mode = S_IRUSR | S_IWUSR | S_IRGRP,
        .filename = typescript->data_filename,
        .filename_size = sizeof(typescript->data_filename)
    };

    if (create_path)
        data_how.flags |= GUAC_O_CREATE_PATH;

    if (!allow_write_existing)
        data_how.flags |= GUAC_O_UNIQUE_SUFFIX;

    /* Attempt to open typescript data file */
    typescript->data_fd = guac_openat(path, name, &data_how);
    if (typescript->data_fd == -1) {
        guac_mem_free(typescript);
        return NULL;
    }

    /* Append suffix to basename */
    if (snprintf(typescript->timing_filename, sizeof(typescript->timing_filename),
                "%s.%s", typescript->data_filename, GUAC_TERMINAL_TYPESCRIPT_TIMING_SUFFIX)
            >= sizeof(typescript->timing_filename)) {
        close(typescript->data_fd);
        guac_mem_free(typescript);
        return NULL;
    }

    guac_open_how timing_how = {
        .oflags = O_CREAT | O_WRONLY,
        .mode = S_IRUSR | S_IWUSR | S_IRGRP
    };

    /* Attempt to open typescript timing file */
    typescript->timing_fd = guac_openat(path, typescript->timing_filename, &timing_how);
    if (typescript->timing_fd == -1) {
        close(typescript->data_fd);
        guac_mem_free(typescript);
        return NULL;
    }

    /* Typescript starts out flushed */
    typescript->length = 0;
    typescript->last_flush = guac_timestamp_current();

    /* Write header */
    guac_common_write(typescript->data_fd, GUAC_TERMINAL_TYPESCRIPT_HEADER,
            sizeof(GUAC_TERMINAL_TYPESCRIPT_HEADER) - 1);

    return typescript;

}

void guac_terminal_typescript_write(guac_terminal_typescript* typescript,
        char c) {

    /* Flush buffer if no space is available */
    if (typescript->length == sizeof(typescript->buffer))
        guac_terminal_typescript_flush(typescript);

    /* Append single byte to buffer */
    typescript->buffer[typescript->length++] = c;

}

void guac_terminal_typescript_flush(guac_terminal_typescript* typescript) {

    /* Do nothing if nothing to flush */
    if (typescript->length == 0)
        return;

    /* Get timestamps of previous and current flush */
    guac_timestamp this_flush = guac_timestamp_current();
    guac_timestamp last_flush = typescript->last_flush;

    /* Calculate time since last flush */
    int elapsed_time = this_flush - last_flush;
    if (elapsed_time > GUAC_TERMINAL_TYPESCRIPT_MAX_DELAY)
        elapsed_time = GUAC_TERMINAL_TYPESCRIPT_MAX_DELAY;

    /* Produce single line of timestamp output */
    char timestamp_buffer[32];
    int timestamp_length = snprintf(timestamp_buffer, sizeof(timestamp_buffer),
            "%0.6f %i\n", elapsed_time / 1000.0, typescript->length);

    /* Calculate actual length of timestamp line */
    if (timestamp_length > sizeof(timestamp_buffer))
        timestamp_length = sizeof(timestamp_buffer);

    if (typescript->data_socket != NULL) {
        guac_typescript_socket_write(typescript->data_socket,
                typescript->buffer, typescript->length);
        guac_typescript_socket_write(typescript->timing_socket,
                timestamp_buffer, timestamp_length);
    }
    else {
        guac_common_write(typescript->timing_fd, timestamp_buffer, timestamp_length);
        guac_common_write(typescript->data_fd, typescript->buffer, typescript->length);
    }

    /* Buffer is now flushed */
    typescript->length = 0;
    typescript->last_flush = this_flush;

}

void guac_terminal_typescript_free(guac_terminal_typescript* typescript) {

    /* Do nothing if no typescript provided */
    if (typescript == NULL)
        return;

    /* Flush any pending data */
    guac_terminal_typescript_flush(typescript);

    if (typescript->data_socket != NULL) {
        guac_typescript_socket_write(typescript->data_socket,
                GUAC_TERMINAL_TYPESCRIPT_FOOTER,
                sizeof(GUAC_TERMINAL_TYPESCRIPT_FOOTER) - 1);
        guac_socket_filer_begin_close(typescript->data_socket);
        guac_socket_filer_begin_close(typescript->timing_socket);
        guac_socket_free(typescript->data_socket);
        guac_socket_free(typescript->timing_socket);
    }
    else {
        guac_common_write(typescript->data_fd, GUAC_TERMINAL_TYPESCRIPT_FOOTER,
                sizeof(GUAC_TERMINAL_TYPESCRIPT_FOOTER) - 1);
        close(typescript->data_fd);
        close(typescript->timing_fd);
    }

    /* Free allocated typescript data */
    guac_mem_free(typescript);

}

