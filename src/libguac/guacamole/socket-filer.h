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

#ifndef GUAC_SOCKET_FILER_H
#define GUAC_SOCKET_FILER_H

#include "socket.h"

#include <stddef.h>

/**
 * Default maximum number of bytes buffered in memory for a Filer-backed
 * recording socket (8 MiB).
 */
#define GUAC_SOCKET_FILER_DEFAULT_MAX_BUFFER_BYTES (8U * 1024U * 1024U)

/**
 * Default byte threshold triggering an immediate incremental upload (64 KiB).
 */
#define GUAC_SOCKET_FILER_DEFAULT_FLUSH_THRESHOLD_BYTES (64U * 1024U)

/**
 * Default periodic flush interval in milliseconds (1000 ms).
 */
#define GUAC_SOCKET_FILER_DEFAULT_FLUSH_INTERVAL_MS 1000

/**
 * Default per-request HTTP timeout in milliseconds (2000 ms, kept well within
 * guacd's 5000 ms client-watchdog cleanup ceiling).
 */
#define GUAC_SOCKET_FILER_DEFAULT_HTTP_TIMEOUT_MS 2000

/**
 * Default drain timeout when closing a Filer-backed socket in milliseconds
 * (3000 ms, leaving headroom before guacd's 5000 ms cleanup watchdog).
 */
#define GUAC_SOCKET_FILER_DEFAULT_CLOSE_TIMEOUT_MS 3000

/**
 * Default Filer path prefix used for connection recordings.
 */
#define GUAC_SOCKET_FILER_DEFAULT_PATH_PREFIX "/recordings"

/**
 * Configuration parameters for opening an incremental SeaweedFS Filer
 * recording socket.
 */
typedef struct guac_socket_filer_config {

    /**
     * Base URL of the SeaweedFS Filer HTTP endpoint
     * (for example, "http://seaweedfs-filer:8888").
     */
    const char* filer_url;

    /**
     * Path prefix within Filer under which the recording object is stored
     * (for example, "/recordings"). Defaults to
     * GUAC_SOCKET_FILER_DEFAULT_PATH_PREFIX when NULL or empty.
     */
    const char* path_prefix;

    /**
     * Object name within path_prefix (typically the history UUID).
     */
    const char* name;

    /**
     * Maximum bytes held in memory across staging, queued, and in-flight
     * buffers. Uses GUAC_SOCKET_FILER_DEFAULT_MAX_BUFFER_BYTES when zero.
     */
    size_t max_buffer_bytes;

    /**
     * Queued byte threshold that wakes the uploader immediately. Uses
     * GUAC_SOCKET_FILER_DEFAULT_FLUSH_THRESHOLD_BYTES when zero.
     */
    size_t flush_threshold_bytes;

    /**
     * Periodic flush interval in milliseconds. Uses
     * GUAC_SOCKET_FILER_DEFAULT_FLUSH_INTERVAL_MS when zero or negative.
     */
    int flush_interval_ms;

    /**
     * Timeout for a single Filer HTTP request in milliseconds. Uses
     * GUAC_SOCKET_FILER_DEFAULT_HTTP_TIMEOUT_MS when zero or negative.
     */
    int http_timeout_ms;

    /**
     * Maximum time in milliseconds to wait for remaining buffered data to
     * flush during guac_socket_free(). Uses
     * GUAC_SOCKET_FILER_DEFAULT_CLOSE_TIMEOUT_MS when zero or negative.
     */
    int close_timeout_ms;

} guac_socket_filer_config;

/**
 * Allocates and initializes a write-only guac_socket that incrementally
 * uploads complete Guacamole instructions to SeaweedFS Filer using
 * byte-offset HTTP PUT requests and Seaweed-Admingate-State metadata.
 *
 * @param config
 *     Configuration describing the target Filer endpoint, object path,
 *     buffer ceiling, and flush/timeout intervals.
 *
 * @return
 *     A newly allocated guac_socket, or NULL if configuration is invalid or
 *     resources cannot be allocated.
 */
guac_socket* guac_socket_open_filer(const guac_socket_filer_config* config);

/**
 * Starts bounded draining without waiting for the uploader to finish.
 * The socket must subsequently be freed with guac_socket_free().
 */
void guac_socket_filer_begin_close(guac_socket* socket);

/**
 * Marks the recording incomplete after a related initialization failure.
 */
void guac_socket_filer_mark_incomplete(guac_socket* socket);

#endif
