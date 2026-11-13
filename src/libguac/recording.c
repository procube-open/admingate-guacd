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

#include "guacamole/mem.h"
#include "guacamole/client.h"
#include "guacamole/error.h"
#include "guacamole/file.h"
#include "guacamole/protocol.h"
#include "guacamole/recording.h"
#include "guacamole/socket.h"
#include "guacamole/socket-filer.h"
#include "guacamole/timestamp.h"

#ifdef __MINGW32__
#include <direct.h>
#endif

#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

static size_t parse_env_size(const char* env_name) {
    const char* raw = getenv(env_name);
    if (raw == NULL || raw[0] == '\0')
        return 0;
    char* end = NULL;
    unsigned long long val = strtoull(raw, &end, 10);
    if (end == raw || *end != '\0')
        return 0;
    return (size_t) val;
}

static int parse_env_int(const char* env_name) {
    const char* raw = getenv(env_name);
    if (raw == NULL || raw[0] == '\0')
        return 0;
    char* end = NULL;
    long val = strtol(raw, &end, 10);
    if (end == raw || *end != '\0' || val <= 0 || val > 3600000L)
        return 0;
    return (int) val;
}

guac_recording* guac_recording_create(guac_client* client,
        const char* path, const char* name, int create_path,
        int include_output, int include_mouse, int include_touch,
        int include_keys, int allow_write_existing, int include_clipboard) {

    char filename[GUAC_COMMON_RECORDING_MAX_NAME_LENGTH];
    guac_socket* socket = NULL;
    const char* backend = getenv("GUACD_RECORDING_BACKEND");
    const char* resolved_filer_prefix = GUAC_SOCKET_FILER_DEFAULT_PATH_PREFIX;
    int use_seaweedfs = 0;

    if (backend == NULL || backend[0] == '\0'
            || strcasecmp(backend, "filesystem") == 0) {

        guac_open_how how = {
            .oflags = O_CREAT | O_WRONLY,
            .mode = S_IRUSR | S_IWUSR | S_IRGRP,
            .filename = filename,
            .filename_size = sizeof(filename),
            .flags = GUAC_O_LOCKED
        };

        if (create_path)
            how.flags |= GUAC_O_CREATE_PATH;

        if (!allow_write_existing)
            how.flags |= GUAC_O_UNIQUE_SUFFIX;

        /* Attempt to open recording file */
        int fd = guac_openat(path, name, &how);
        if (fd == -1) {
            guac_client_log(client, GUAC_LOG_ERROR, "Creation of recording "
                    "failed: %s: %s", guac_error_message,
                    guac_status_string(guac_error));
            return NULL;
        }

        /* Wrap recording file in socket */
        socket = guac_socket_open(fd);
        if (socket == NULL) {
            guac_client_log(client, GUAC_LOG_ERROR, "Creation of recording "
                    "failed: %s: %s", guac_error_message,
                    guac_status_string(guac_error));
            close(fd);
            return NULL;
        }

    }
    else if (strcasecmp(backend, "seaweedfs") == 0) {

        use_seaweedfs = 1;
        const char* filer_url = getenv("GUACD_SEAWEEDFS_FILER_URL");
        const char* env_prefix = getenv("GUACD_SEAWEEDFS_RECORDING_PREFIX");
        if (env_prefix != NULL && env_prefix[0] != '\0')
            resolved_filer_prefix = env_prefix;

        guac_socket_filer_config filer_cfg = {
            .filer_url = filer_url,
            .path_prefix = resolved_filer_prefix,
            .name = name,
            .max_buffer_bytes = parse_env_size("GUACD_SEAWEEDFS_MAX_BUFFER_BYTES"),
            .flush_threshold_bytes = parse_env_size("GUACD_SEAWEEDFS_FLUSH_THRESHOLD_BYTES"),
            .flush_interval_ms = parse_env_int("GUACD_SEAWEEDFS_FLUSH_INTERVAL_MS"),
            .http_timeout_ms = parse_env_int("GUACD_SEAWEEDFS_HTTP_TIMEOUT_MS"),
            .close_timeout_ms = parse_env_int("GUACD_SEAWEEDFS_CLOSE_TIMEOUT_MS")
        };

        socket = guac_socket_open_filer(&filer_cfg);
        if (socket == NULL) {
            guac_client_log(client, GUAC_LOG_ERROR, "Creation of SeaweedFS "
                    "recording failed: %s: %s", guac_error_message,
                    guac_status_string(guac_error));
            return NULL;
        }

        snprintf(filename, sizeof(filename), "%s", name != NULL ? name : "");

    }
    else {
        guac_client_log(client, GUAC_LOG_ERROR, "Creation of recording "
                "failed: unsupported GUACD_RECORDING_BACKEND \"%s\"", backend);
        return NULL;
    }

    /* Create recording structure with reference to underlying socket */
    guac_recording* recording = guac_mem_alloc(sizeof(guac_recording));
    if (recording == NULL) {
        guac_client_log(client, GUAC_LOG_ERROR, "Creation of recording "
                "failed: %s", guac_status_string(guac_error));
        guac_socket_free(socket);
        return NULL;
    }

    recording->socket = socket;
    recording->include_output = include_output;
    recording->include_mouse = include_mouse;
    recording->include_touch = include_touch;
    recording->include_keys = include_keys;
    recording->include_clipboard = include_clipboard;

    if (include_clipboard)
        recording->clipboard_stream = guac_client_alloc_stream(client);
    else
        recording->clipboard_stream = NULL;

    /* Replace client socket with wrapped recording socket only if including
     * output within the recording */
    if (include_output) {

        guac_socket* client_socket = guac_socket_tee(client->socket, recording->socket);
        if (client_socket == NULL) {
            guac_client_log(client, GUAC_LOG_ERROR, "Creation of recording "
                    "failed: %s", guac_status_string(guac_error));
            guac_socket_free(recording->socket);
            guac_mem_free(recording);
            return NULL;
        }

        client->socket = client_socket;

    }

    /* Recording creation succeeded */
    if (use_seaweedfs) {
        guac_client_log(client, GUAC_LOG_INFO, "Recording of session will be "
                "saved to SeaweedFS Filer prefix \"%s\" as \"%s\".",
                resolved_filer_prefix, filename);
    }
    else {
        guac_client_log(client, GUAC_LOG_INFO, "Recording of session will be "
                "saved within \"%s\" as \"%s\".", path, filename);
    }

    return recording;

}

void guac_recording_free(guac_recording* recording) {

    /* If not including broadcast output, the output socket is not associated
     * with the client, and must be freed manually */
    if (!recording->include_output)
        guac_socket_free(recording->socket);

    /* Free recording itself */
    guac_mem_free(recording);

}

void guac_recording_report_mouse(guac_recording* recording,
        int x, int y, int button_mask) {

    /* Report mouse location only if recording should contain mouse events */
    if (recording->include_mouse)
        guac_protocol_send_mouse(recording->socket, x, y, button_mask,
                guac_timestamp_current());

}

void guac_recording_report_touch(guac_recording* recording,
        int id, int x, int y, int x_radius, int y_radius,
        double angle, double force) {

    /* Report touches only if recording should contain touch events */
    if (recording->include_touch)
        guac_protocol_send_touch(recording->socket, id, x, y,
                x_radius, y_radius, angle, force, guac_timestamp_current());

}

void guac_recording_report_key(guac_recording* recording,
        int keysym, int pressed) {

    /* Report key state only if recording should contain key events */
    if (recording->include_keys)
        guac_protocol_send_key(recording->socket, keysym, pressed,
                guac_timestamp_current());

}

void guac_recording_report_clipboard_begin(guac_recording* recording, guac_stream* stream, char* mimetype) {
    /* Report clipboard only if recording should contain it */
    if (recording->include_clipboard)
        guac_protocol_send_clipboard(recording->socket, stream, mimetype);
}

void guac_recording_report_clipboard_blob(guac_recording* recording, guac_stream* stream, void* data, int length) {
    /* Report clipboard only if recording should contain it */
    if (recording->include_clipboard)
        guac_protocol_send_blob(recording->socket, stream, data, length);
}

void guac_recording_report_clipboard_end(guac_recording* recording, guac_stream* stream) {
    /* Report clipboard only if recording should contain it */
    if (recording->include_clipboard)
        guac_protocol_send_end(recording->socket, stream);
}

void guac_recording_report_clipboard(guac_recording* recording,
        const char* mimetype, const char* data, int length) {

    /* Report clipboard only if recording should contain clipboard changes */
    if (!recording->include_clipboard || !recording->clipboard_stream)
        return;

    guac_socket* socket = recording->socket;
    guac_stream* stream = recording->clipboard_stream;

    const char* current = data;
    int remaining = length;

    guac_protocol_send_clipboard(socket, stream, mimetype);

    while (remaining > 0) {

        int block_size = GUAC_RECORDING_CLIPBOARD_BLOCK_SIZE;
        if (remaining < block_size)
            block_size = remaining;

        guac_protocol_send_blob(socket, stream, current, block_size);

        remaining -= block_size;
        current += block_size;

    }

    guac_protocol_send_end(socket, stream);

}
