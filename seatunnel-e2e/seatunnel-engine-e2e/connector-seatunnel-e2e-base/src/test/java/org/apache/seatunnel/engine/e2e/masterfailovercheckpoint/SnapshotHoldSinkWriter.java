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

package org.apache.seatunnel.engine.e2e.masterfailovercheckpoint;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer of {@link SnapshotHoldSink}.
 *
 * <p>Every {@code snapshotState} call is routed through {@link SnapshotHoldGate}, so once the test
 * arms the gate the checkpoint barrier is acknowledged by no one: the master keeps that checkpoint
 * in flight and cannot complete it. The writer keeps a plain row counter only to make the snapshot
 * non-trivial; the state itself is not asserted.
 */
@Slf4j
public class SnapshotHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final String holdKey;

    private final AtomicLong writtenRows;

    public SnapshotHoldSinkWriter(String holdKey) {
        this.holdKey = holdKey;
        this.writtenRows = new AtomicLong();
    }

    @Override
    public void write(SeaTunnelRow element) {
        writtenRows.incrementAndGet();
    }

    /**
     * Parks the checkpoint barrier until the test releases the gate. This is exactly the window in
     * which the checkpoint is triggered, acknowledged by no task and therefore still uncommitted on
     * the active master.
     *
     * @param checkpointId id of the checkpoint whose barrier reached this writer
     * @return empty state; the engine only needs the acknowledgement for this contract
     */
    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        // WARN on purpose: the module's test logging keeps the root logger at WARN.
        log.warn(
                "Sink writer holding checkpoint {} at {} rows on key {}",
                checkpointId,
                writtenRows.get(),
                holdKey);
        SnapshotHoldGate.awaitSnapshot(holdKey);
        return Collections.emptyList();
    }

    @Override
    public void close() {}
}
