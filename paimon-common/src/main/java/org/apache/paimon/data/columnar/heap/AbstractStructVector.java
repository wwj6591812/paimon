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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;

/** * Abstract class for vectors that have children. */
public abstract class AbstractStructVector extends AbstractHeapVector
        implements WritableColumnVector {

    protected ColumnVector[] children;

    public AbstractStructVector(int capacity, ColumnVector[] children) {
        super(capacity);
        this.children = children;
    }

    @Override
    public void reset() {
        super.reset();
        for (ColumnVector child : children) {
            if (child instanceof WritableColumnVector) {
                ((WritableColumnVector) child).reset();
            }
        }
    }

    @Override
    public ColumnVector[] getChildren() {
        return children;
    }
}
