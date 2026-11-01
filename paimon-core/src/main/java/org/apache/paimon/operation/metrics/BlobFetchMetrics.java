/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobTransferSource;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.rest.HttpClientUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Metrics to measure blob fetches during write. */
public class BlobFetchMetrics implements BlobFetchMetricReporter {

    public static final String GROUP_NAME = "blobFetch";

    public static final String BLOB_FETCH_TOTAL = "blobFetchTotal";
    public static final String BLOB_FETCH_SUCCESS = "blobFetchSuccess";
    public static final String BLOB_FETCH_SUCCESS_BYTES = "blobFetchSuccessBytes";
    public static final String BLOB_FETCH_NULL_WRITTEN = "blobFetchNullWritten";
    public static final String BLOB_FETCH_MISSING_FILE_NULL_WRITTEN =
            "blobFetchMissingFileNullWritten";
    public static final String BLOB_FETCH_FAILURE_NULL_WRITTEN = "blobFetchFailureNullWritten";
    public static final String BLOB_FETCH_FAILURE = "blobFetchFailure";
    public static final String BLOB_FETCH_HTTP_NOT_FOUND = "blobFetchHttpNotFound";
    public static final String BLOB_FETCH_HTTP_CLIENT_ERROR = "blobFetchHttpClientError";
    public static final String BLOB_FETCH_HTTP_SERVER_ERROR = "blobFetchHttpServerError";
    public static final String BLOB_FETCH_HTTP_OTHER_ERROR = "blobFetchHttpOtherError";
    public static final String BLOB_FETCH_INVALID_URI = "blobFetchInvalidUri";
    public static final String BLOB_FETCH_OTHER_ERROR = "blobFetchOtherError";

    public static final String TRANSFER_GROUP_NAME = "blobTransfer";
    public static final String TRANSFER_READ_BYTES = "blobTransferReadBytes";
    public static final String TRANSFER_WRITTEN_BYTES = "blobTransferWrittenBytes";
    public static final String TRANSFER_DURATION_NANOS = "blobTransferDurationNanos";
    public static final String TRANSFER_LATENCY_MILLIS = "blobTransferLatencyMillis";
    public static final String TRANSFER_COMPLETED = "blobTransferCompleted";
    public static final String TRANSFER_FAILED = "blobTransferFailed";

    private final MetricRegistry registry;
    private final String tableName;
    private final String readerIdc;
    private final Map<String, String> sourceIdcs;
    private final Map<String, TransferCounters> transferGroups = new HashMap<>();

    private final MetricGroup metricGroup;
    private final Counter totalCounter;
    private final Counter successCounter;
    private final Counter successBytesCounter;
    private final Counter nullWrittenCounter;
    private final Counter missingFileNullWrittenCounter;
    private final Counter fetchFailureNullWrittenCounter;
    private final Counter failureCounter;
    private final Counter httpNotFoundCounter;
    private final Counter httpClientErrorCounter;
    private final Counter httpServerErrorCounter;
    private final Counter httpOtherErrorCounter;
    private final Counter invalidUriCounter;
    private final Counter otherErrorCounter;

    public BlobFetchMetrics(MetricRegistry registry, String tableName) {
        this(registry, tableName, "", Collections.emptyMap());
    }

    public BlobFetchMetrics(
            MetricRegistry registry,
            String tableName,
            String readerIdc,
            Map<String, String> sourceIdcs) {
        this.registry = registry;
        this.tableName = tableName;
        this.readerIdc = canonicalIdc(readerIdc);
        this.sourceIdcs = new HashMap<>();
        sourceIdcs.forEach(
                (source, idc) -> {
                    if (source != null
                            && !source.equals("unknown")
                            && !source.equals("inline")
                            && source.equals(BlobTransferSource.bounded(source))) {
                        this.sourceIdcs.put(source, canonicalIdc(idc));
                    }
                });
        this.metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        this.totalCounter = metricGroup.counter(BLOB_FETCH_TOTAL);
        this.successCounter = metricGroup.counter(BLOB_FETCH_SUCCESS);
        this.successBytesCounter = metricGroup.counter(BLOB_FETCH_SUCCESS_BYTES);
        this.nullWrittenCounter = metricGroup.counter(BLOB_FETCH_NULL_WRITTEN);
        this.missingFileNullWrittenCounter =
                metricGroup.counter(BLOB_FETCH_MISSING_FILE_NULL_WRITTEN);
        this.fetchFailureNullWrittenCounter = metricGroup.counter(BLOB_FETCH_FAILURE_NULL_WRITTEN);
        this.failureCounter = metricGroup.counter(BLOB_FETCH_FAILURE);
        this.httpNotFoundCounter = metricGroup.counter(BLOB_FETCH_HTTP_NOT_FOUND);
        this.httpClientErrorCounter = metricGroup.counter(BLOB_FETCH_HTTP_CLIENT_ERROR);
        this.httpServerErrorCounter = metricGroup.counter(BLOB_FETCH_HTTP_SERVER_ERROR);
        this.httpOtherErrorCounter = metricGroup.counter(BLOB_FETCH_HTTP_OTHER_ERROR);
        this.invalidUriCounter = metricGroup.counter(BLOB_FETCH_INVALID_URI);
        this.otherErrorCounter = metricGroup.counter(BLOB_FETCH_OTHER_ERROR);
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public void recordSuccess(long bytes) {
        totalCounter.inc();
        successCounter.inc();
        if (bytes > 0) {
            successBytesCounter.inc(bytes);
        }
    }

    @Override
    public void recordMissingFileNullWritten(boolean httpNotFound) {
        totalCounter.inc();
        nullWrittenCounter.inc();
        missingFileNullWrittenCounter.inc();
        if (httpNotFound) {
            httpNotFoundCounter.inc();
        }
    }

    @Override
    public void recordFetchFailureNullWritten(Throwable throwable) {
        totalCounter.inc();
        nullWrittenCounter.inc();
        fetchFailureNullWrittenCounter.inc();
        recordFailureReason(throwable);
    }

    @Override
    public void recordFetchFailure(Throwable throwable) {
        totalCounter.inc();
        failureCounter.inc();
        recordFailureReason(throwable);
    }

    @Override
    public synchronized void recordTransfer(
            String sourceType,
            long readBytes,
            long writtenBytes,
            long durationNanos,
            boolean completed) {
        String source = BlobTransferSource.bounded(sourceType);
        TransferCounters counters = transferGroups.get(source);
        if (counters == null) {
            Map<String, String> variables = new LinkedHashMap<>();
            variables.put("table", tableName);
            variables.put("sourceType", source);
            String sourceIdc = sourceIdcs.getOrDefault(source, "unknown");
            variables.put("readerIdc", readerIdc);
            variables.put("sourceIdc", sourceIdc);
            variables.put(
                    "crossIdc",
                    readerIdc.equals("unknown") || sourceIdc.equals("unknown")
                            ? "unknown"
                            : readerIdc.equals(sourceIdc) ? "same" : "cross");
            counters =
                    new TransferCounters(
                            registry.createMetricGroup(TRANSFER_GROUP_NAME, variables));
            transferGroups.put(source, counters);
        }
        counters.readBytes.inc(Math.max(0, readBytes));
        counters.writtenBytes.inc(Math.max(0, writtenBytes));
        counters.durationNanos.inc(Math.max(0, durationNanos));
        counters.latencyMillis.update(Math.max(0, durationNanos) / 1_000_000L);
        (completed ? counters.completed : counters.failed).inc();
    }

    private static String canonicalIdc(String value) {
        if (value == null || !value.trim().matches("[A-Za-z0-9_-]{1,64}")) {
            return "unknown";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    public void close() {
        metricGroup.close();
        transferGroups.values().forEach(counters -> counters.group.close());
    }

    private static class TransferCounters {
        private final MetricGroup group;
        private final Counter readBytes;
        private final Counter writtenBytes;
        private final Counter durationNanos;
        private final Histogram latencyMillis;
        private final Counter completed;
        private final Counter failed;

        private TransferCounters(MetricGroup group) {
            this.group = group;
            this.readBytes = group.counter(TRANSFER_READ_BYTES);
            this.writtenBytes = group.counter(TRANSFER_WRITTEN_BYTES);
            this.durationNanos = group.counter(TRANSFER_DURATION_NANOS);
            this.latencyMillis = group.histogram(TRANSFER_LATENCY_MILLIS, 1024);
            this.completed = group.counter(TRANSFER_COMPLETED);
            this.failed = group.counter(TRANSFER_FAILED);
        }
    }

    private void recordFailureReason(Throwable throwable) {
        Integer statusCode = HttpClientUtils.getHttpStatusCode(throwable);
        if (statusCode != null) {
            if (statusCode == 404) {
                httpNotFoundCounter.inc();
            } else if (statusCode >= 400 && statusCode < 500) {
                httpClientErrorCounter.inc();
            } else if (statusCode >= 500 && statusCode < 600) {
                httpServerErrorCounter.inc();
            } else {
                httpOtherErrorCounter.inc();
            }
            return;
        }

        if (HttpClientUtils.isInvalidUriException(throwable)) {
            invalidUriCounter.inc();
            return;
        }

        otherErrorCounter.inc();
    }
}
