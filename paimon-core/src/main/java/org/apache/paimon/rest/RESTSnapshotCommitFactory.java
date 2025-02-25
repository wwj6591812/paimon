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

package org.apache.paimon.rest;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;

/** Factory to create {@link SnapshotCommit} for REST Catalog. */
public class RESTSnapshotCommitFactory implements SnapshotCommit.Factory {

    private static final long serialVersionUID = 1L;

    private final RESTCatalogLoader loader;

    public RESTSnapshotCommitFactory(RESTCatalogLoader loader) {
        this.loader = loader;
    }

    @Override
    public SnapshotCommit create(Identifier identifier, SnapshotManager snapshotManager) {
        RESTCatalog catalog = loader.load();
        return new SnapshotCommit() {
            @Override
            public boolean commit(Snapshot snapshot, String branch, List<Partition> statistics) {
                Identifier newIdentifier =
                        new Identifier(
                                identifier.getDatabaseName(), identifier.getTableName(), branch);
                return catalog.commitSnapshot(newIdentifier, snapshot, statistics);
            }

            @Override
            public void close() throws Exception {
                catalog.close();
            }
        };
    }
}
