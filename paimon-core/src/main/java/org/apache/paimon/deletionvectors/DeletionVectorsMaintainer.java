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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Maintainer of deletionVectors index. */
public class DeletionVectorsMaintainer {

    private final IndexFileHandler indexFileHandler;
    private final Map<String, DeletionVector> deletionVectors;
    protected final boolean bitmap64;
    private boolean modified;

    private DeletionVectorsMaintainer(
            IndexFileHandler fileHandler, Map<String, DeletionVector> deletionVectors) {
        this.indexFileHandler = fileHandler;
        this.deletionVectors = deletionVectors;
        this.bitmap64 = indexFileHandler.deletionVectorsIndex().bitmap64();
        this.modified = false;
    }

    private DeletionVector createNewDeletionVector() {
        return bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    }

    /**
     * Notifies a new deletion which marks the specified row position as deleted with the given file
     * name.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param position The row position within the file that has been deleted.
     */
    public void notifyNewDeletion(String fileName, long position) {
        DeletionVector deletionVector =
                deletionVectors.computeIfAbsent(fileName, k -> createNewDeletionVector());
        if (deletionVector.checkedDelete(position)) {
            modified = true;
        }
    }

    /**
     * Notifies a new deletion which marks the specified deletion vector with the given file name.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param deletionVector The deletion vector
     */
    public void notifyNewDeletion(String fileName, DeletionVector deletionVector) {
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * Merge a new deletion which marks the specified deletion vector with the given file name, if
     * the previous deletion vector exist, merge the old one.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param deletionVector The deletion vector
     */
    public void mergeNewDeletion(String fileName, DeletionVector deletionVector) {
        DeletionVector old = deletionVectors.get(fileName);
        if (old != null) {
            deletionVector.merge(old);
        }
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * Removes the specified file's deletion vector, this method is typically used for remove before
     * files' deletion vector in compaction.
     *
     * @param fileName The name of the file whose deletion vector should be removed.
     */
    public void removeDeletionVectorOf(String fileName) {
        if (deletionVectors.containsKey(fileName)) {
            deletionVectors.remove(fileName);
            modified = true;
        }
    }

    /**
     * Write new deletion vectors index file if any modifications have been made.
     *
     * @return A list containing the metadata of the deletion vectors index file, or an empty list
     *     if no changes need to be committed.
     */
    public List<IndexFileMeta> writeDeletionVectorsIndex() {
        if (modified) {
            modified = false;
            return indexFileHandler.writeDeletionVectorsIndex(deletionVectors);
        }
        return Collections.emptyList();
    }

    /**
     * Retrieves the deletion vector associated with the specified file name.
     *
     * @param fileName The name of the file for which the deletion vector is requested.
     * @return An {@code Optional} containing the deletion vector if it exists, or an empty {@code
     *     Optional} if not.
     */
    public Optional<DeletionVector> deletionVectorOf(String fileName) {
        return Optional.ofNullable(deletionVectors.get(fileName));
    }

    public IndexFileHandler indexFileHandler() {
        return indexFileHandler;
    }

    @VisibleForTesting
    public Map<String, DeletionVector> deletionVectors() {
        return deletionVectors;
    }

    public boolean bitmap64() {
        return bitmap64;
    }

    public static Factory factory(IndexFileHandler handler) {
        return new Factory(handler);
    }

    /** Factory to restore {@link DeletionVectorsMaintainer}. */
    public static class Factory {

        private final IndexFileHandler handler;

        public Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        public IndexFileHandler indexFileHandler() {
            return handler;
        }

        public DeletionVectorsMaintainer create(@Nullable List<IndexFileMeta> restoredFiles) {
            if (restoredFiles == null) {
                restoredFiles = Collections.emptyList();
            }
            Map<String, DeletionVector> deletionVectors =
                    new HashMap<>(handler.readAllDeletionVectors(restoredFiles));
            return create(deletionVectors);
        }

        public DeletionVectorsMaintainer create() {
            return create(new HashMap<>());
        }

        public DeletionVectorsMaintainer create(Map<String, DeletionVector> deletionVectors) {
            return new DeletionVectorsMaintainer(handler, deletionVectors);
        }
    }
}
