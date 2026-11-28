/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.e2e.multipipeline;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;

/**
 * One contiguous slice of the test source's rows.
 *
 * <p>The split tracks how much of its range was already emitted so a checkpoint or savepoint taken
 * while the reader is held snapshots only the not-yet-read rows.
 */
public class SavepointPipelineHoldSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;

    private final long endRow;

    private long nextRow;

    public SavepointPipelineHoldSplit(String splitId, long startRow, long endRow) {
        this.splitId = splitId;
        this.endRow = endRow;
        this.nextRow = startRow;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public long nextRow() {
        return nextRow++;
    }

    public boolean hasRemaining() {
        return nextRow < endRow;
    }

    public long getNextRow() {
        return nextRow;
    }
}
