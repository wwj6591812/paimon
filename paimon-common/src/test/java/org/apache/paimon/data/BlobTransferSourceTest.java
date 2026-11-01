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

package org.apache.paimon.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BlobTransferSourceTest {

    @Test
    void classifiesOnlyRecognizedLogicalSources() {
        assertThat(BlobTransferSource.classify(Blob.fromData(new byte[0]))).isEqualTo("inline");
        assertThat(BlobTransferSource.classifyUri("alake://project/table/region=na61"))
                .isEqualTo("alake");
        assertThat(BlobTransferSource.classifyUri("oss://bucket/object")).isEqualTo("oss");
        assertThat(BlobTransferSource.classifyUri("dfs://cluster/blob")).isEqualTo("filesystem");
        assertThat(
                        BlobTransferSource.classifyUri(
                                "https://image.media-ai-acceleration.alibaba-inc.com/image/origin?app_key=test"))
                .isEqualTo("image_space");
        assertThat(BlobTransferSource.classifyUri("https://IMG.ALICDN.COM/x.jpg?signature=test"))
                .isEqualTo("cdn");
        assertThat(BlobTransferSource.classifyUri("https://img.taobaocdn.com/x.jpg"))
                .isEqualTo("cdn");
        assertThat(BlobTransferSource.classifyUri("https://example.com/x.jpg"))
                .isEqualTo("http_other");
    }

    @Test
    void doesNotTrustSpoofedHostsOrInvalidUris() {
        assertThat(BlobTransferSource.classifyUri("https://img.alicdn.com.evil/x.jpg"))
                .isEqualTo("http_other");
        assertThat(
                        BlobTransferSource.classifyUri(
                                "https://image.media-ai-acceleration.alibaba-inc.com/other"))
                .isEqualTo("http_other");
        for (String value :
                new String[] {null, "", "http://", "http://bad host/image", "data:abc"}) {
            assertThat(BlobTransferSource.classifyUri(value)).isEqualTo("unknown");
        }
        assertThat(BlobTransferSource.bounded("https://signed-private-url")).isEqualTo("unknown");
        assertThat(BlobTransferSource.classify(null)).isEqualTo("unknown");
    }

    @Test
    void doesNotInspectDescriptorsOfTransformingSubclasses() {
        BlobRef transformed =
                new BlobRef(null, new BlobDescriptor("https://img.alicdn.com/image", 0, -1)) {
                    @Override
                    public BlobDescriptor toDescriptor() {
                        throw new UnsupportedOperationException("not supported by this stream");
                    }
                };
        assertThat(BlobTransferSource.classify(transformed)).isEqualTo("unknown");
    }
}
