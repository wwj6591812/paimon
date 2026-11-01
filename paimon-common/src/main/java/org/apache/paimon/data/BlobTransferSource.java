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

import java.net.URI;
import java.util.Locale;

/** Bounded logical URI source classes; these do not describe network placement or routing. */
public final class BlobTransferSource {

    private BlobTransferSource() {}

    public static String classify(Blob blob) {
        if (blob != null && blob.getClass() == BlobData.class) {
            return "inline";
        }
        // Subclasses may transform the stream or not support descriptor access at all.
        // Monitoring must not invoke metadata methods that the actual read path avoids.
        if (blob == null || blob.getClass() != BlobRef.class) {
            return "unknown";
        }
        return classifyUri(((BlobRef) blob).toDescriptor().uri());
    }

    static String classifyUri(String value) {
        try {
            if (value == null || value.trim().isEmpty()) {
                return "unknown";
            }
            URI uri = URI.create(value);
            String scheme = uri.getScheme();
            if (scheme == null) {
                return "filesystem";
            }
            switch (scheme.toLowerCase(Locale.ROOT)) {
                case "alake":
                    return "alake";
                case "oss":
                    return "oss";
                case "http":
                case "https":
                    String host = uri.getHost();
                    if (host == null) {
                        return "unknown";
                    }
                    host = host.toLowerCase(Locale.ROOT);
                    if ("image.media-ai-acceleration.alibaba-inc.com".equals(host)
                            && "/image/origin".equals(uri.getPath())) {
                        return "image_space";
                    }
                    if (host.equals("alicdn.com")
                            || host.endsWith(".alicdn.com")
                            || host.equals("taobaocdn.com")
                            || host.endsWith(".taobaocdn.com")) {
                        return "cdn";
                    }
                    return "http_other";
                case "dfs":
                case "hdfs":
                case "pangu":
                case "s3":
                case "s3a":
                case "file":
                    return "filesystem";
                default:
                    return "unknown";
            }
        } catch (IllegalArgumentException | NullPointerException ignored) {
            return "unknown";
        }
    }

    public static String bounded(String value) {
        if (value != null) {
            switch (value) {
                case "image_space":
                case "cdn":
                case "alake":
                case "http_other":
                case "oss":
                case "filesystem":
                case "inline":
                    return value;
                default:
                    break;
            }
        }
        return "unknown";
    }
}
