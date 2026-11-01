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

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.TestMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobFetchMetrics}. */
public class BlobFetchMetricsTest {

    private static final String TABLE_NAME = "myTable";

    @Test
    public void testMetricRegistration() {
        BlobFetchMetrics metrics = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME);
        MetricGroup metricGroup = metrics.getMetricGroup();

        assertThat(metricGroup.getGroupName()).isEqualTo(BlobFetchMetrics.GROUP_NAME);
        assertThat(metricGroup.getAllVariables()).containsEntry("table", TABLE_NAME);
        assertThat(metricGroup.getMetrics().keySet())
                .containsExactlyInAnyOrder(
                        BlobFetchMetrics.BLOB_FETCH_TOTAL,
                        BlobFetchMetrics.BLOB_FETCH_SUCCESS,
                        BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES,
                        BlobFetchMetrics.BLOB_FETCH_NULL_WRITTEN,
                        BlobFetchMetrics.BLOB_FETCH_MISSING_FILE_NULL_WRITTEN,
                        BlobFetchMetrics.BLOB_FETCH_FAILURE_NULL_WRITTEN,
                        BlobFetchMetrics.BLOB_FETCH_FAILURE,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_NOT_FOUND,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_CLIENT_ERROR,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_OTHER_ERROR,
                        BlobFetchMetrics.BLOB_FETCH_INVALID_URI,
                        BlobFetchMetrics.BLOB_FETCH_OTHER_ERROR);
    }

    @Test
    public void testMetricsAreUpdated() {
        BlobFetchMetrics metrics = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME);

        metrics.recordSuccess(10);
        metrics.recordMissingFileNullWritten(true);
        metrics.recordMissingFileNullWritten(false);
        metrics.recordFetchFailureNullWritten(new RuntimeException("HTTP error code: 500"));
        metrics.recordFetchFailure(new RuntimeException("HTTP error code: 429"));
        metrics.recordFetchFailure(
                new IllegalArgumentException("Illegal character in path at index 1"));
        metrics.recordFetchFailure(new RuntimeException("boom"));

        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TOTAL, 7);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES, 10);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_NULL_WRITTEN, 3);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_MISSING_FILE_NULL_WRITTEN, 2);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE_NULL_WRITTEN, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE, 3);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_NOT_FOUND, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_CLIENT_ERROR, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_OTHER_ERROR, 0);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_INVALID_URI, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_OTHER_ERROR, 1);
    }

    @Test
    public void testTransferCountersAccumulateAndSourceCardinalityIsBounded() {
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME);
        metrics.recordTransfer("image_space", 10, 10, 100, true);
        metrics.recordTransfer("image_space", 5, 0, 200, false);
        metrics.recordTransfer("signed-url-with-private-query", -1, -1, -1, false);

        MetricGroup group = registry.transferGroup("image_space");
        assertThat(group.getAllVariables()).containsEntry("crossIdc", "unknown");
        assertThat(group.getAllVariables()).containsEntry("readerIdc", "unknown");
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_READ_BYTES)).isEqualTo(15);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_WRITTEN_BYTES)).isEqualTo(10);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_DURATION_NANOS)).isEqualTo(300);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_COMPLETED)).isEqualTo(1);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_FAILED)).isEqualTo(1);
        assertThat(counter(registry.transferGroup("unknown"), BlobFetchMetrics.TRANSFER_READ_BYTES))
                .isZero();
        assertThat(registry.groups).hasSize(3);
    }

    @Test
    public void testCrossIdcUsesOnlyVerifiedConfiguredPlacement() {
        Map<String, String> placement = new HashMap<>();
        placement.put("image_space", "EA120");
        placement.put("oss", "na61");
        placement.put("cdn", "invalid:placement");
        placement.put("unknown", "na61");
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, "NA61", placement);
        placement.put("image_space", "na61");
        for (String source : new String[] {"image_space", "oss", "cdn", "unknown", "filesystem"}) {
            metrics.recordTransfer(source, 1, 1, 1, true);
        }
        assertThat(registry.transferGroup("image_space").getAllVariables())
                .containsEntry("sourceIdc", "ea120")
                .containsEntry("readerIdc", "na61")
                .containsEntry("crossIdc", "cross");
        assertThat(registry.transferGroup("oss").getAllVariables())
                .containsEntry("crossIdc", "same");
        for (String source : new String[] {"cdn", "unknown", "filesystem"}) {
            assertThat(registry.transferGroup(source).getAllVariables())
                    .containsEntry("crossIdc", "unknown");
        }
        RecordingRegistry invalid = new RecordingRegistry();
        new BlobFetchMetrics(
                        invalid, TABLE_NAME, "bad/idc", Collections.singletonMap("oss", "na61"))
                .recordTransfer("oss", 1, 1, 1, true);
        assertThat(invalid.transferGroup("oss").getAllVariables())
                .containsEntry("crossIdc", "unknown");
    }

    @Test
    public void testTransferLatencyUsesBoundedMillisecondHistogram() {
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME);
        metrics.recordTransfer("oss", 10, 10, 10_000_000L, true);
        metrics.recordTransfer("oss", 5, 0, 30_000_000L, false);
        Histogram latency =
                (Histogram)
                        registry.transferGroup("oss")
                                .getMetrics()
                                .get(BlobFetchMetrics.TRANSFER_LATENCY_MILLIS);
        assertThat(latency.getCount()).isEqualTo(2);
        assertThat(latency.getStatistics().getMean()).isEqualTo(20.0);
        assertThat(latency.getStatistics().getMin()).isEqualTo(10);
        assertThat(latency.getStatistics().getMax()).isEqualTo(30);
        for (int i = 0; i < 1100; i++) {
            metrics.recordTransfer("oss", 0, 0, 1_000_000L, true);
        }
        assertThat(latency.getStatistics().size()).isEqualTo(1024);
    }

    private static long counter(MetricGroup group, String name) {
        return ((Counter) group.getMetrics().get(name)).getCount();
    }

    private static class RecordingRegistry extends TestMetricRegistry {
        private final List<MetricGroup> groups = new ArrayList<>();

        @Override
        public MetricGroup createMetricGroup(String groupName, Map<String, String> variables) {
            MetricGroup group = super.createMetricGroup(groupName, variables);
            groups.add(group);
            return group;
        }

        private MetricGroup transferGroup(String source) {
            return groups.stream()
                    .filter(group -> source.equals(group.getAllVariables().get("sourceType")))
                    .findFirst()
                    .orElseThrow(AssertionError::new);
        }
    }

    private static void assertCounter(BlobFetchMetrics metrics, String name, long expected) {
        assertThat(counter(metrics, name)).isEqualTo(expected);
    }

    private static long counter(BlobFetchMetrics metrics, String name) {
        Map<String, Metric> registeredMetrics = metrics.getMetricGroup().getMetrics();
        return ((Counter) registeredMetrics.get(name)).getCount();
    }
}
