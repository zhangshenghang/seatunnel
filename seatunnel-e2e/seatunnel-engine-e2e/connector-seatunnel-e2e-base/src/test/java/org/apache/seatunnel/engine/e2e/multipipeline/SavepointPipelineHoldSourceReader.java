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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Reads the test source's splits and then withholds the end of its input until the test releases
 * {@link SavepointPipelineHoldGate}.
 *
 * <p>Rows are emitted under the {@code checkpointLock} in small batches, like the regular fake
 * source does, so a checkpoint or savepoint barrier can be injected between batches. The hold
 * itself runs outside that lock: the task thread parks, the barrier injection on the coordinator
 * thread can still take the lock, and the pipeline therefore stays {@code RUNNING} across a
 * checkpoint or savepoint instead of merely taking longer to finish.
 */
public class SavepointPipelineHoldSourceReader
        implements SourceReader<SeaTunnelRow, SavepointPipelineHoldSplit> {

    private final Deque<SavepointPipelineHoldSplit> activeSplits = new ConcurrentLinkedDeque<>();

    private final Context context;

    private final String holdKey;

    private final int recordsPerPoll;

    private final long idleSleepMs;

    private volatile boolean noMoreSplits;

    private volatile boolean endSignalled;

    public SavepointPipelineHoldSourceReader(
            Context context, String holdKey, int recordsPerPoll, long idleSleepMs) {
        this.context = context;
        this.holdKey = holdKey;
        this.recordsPerPoll = recordsPerPoll;
        this.idleSleepMs = idleSleepMs;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        SavepointPipelineHoldSplit split = activeSplits.peekFirst();
        if (split == null) {
            if (noMoreSplits && !endSignalled) {
                // Every row was read. Keep the pipeline active until the test releases the gate,
                // then signal the end of the input so the pipeline can trigger its final
                // checkpoint. The wait must stay outside the checkpoint lock.
                endSignalled = true;
                if (SavepointPipelineHoldGate.awaitRelease(holdKey)) {
                    context.signalNoMoreElement();
                }
                return;
            }
            // Wait for splits, still outside the checkpoint lock so barriers can be injected.
            pauseOutsideCheckpointLock();
            return;
        }

        int emitted = 0;
        synchronized (output.getCheckpointLock()) {
            while (emitted < recordsPerPoll && split.hasRemaining()) {
                long row = split.nextRow();
                output.collect(
                        new SeaTunnelRow(
                                new Object[] {row, SavepointPipelineHoldSource.ROW_PREFIX + row}));
                emitted++;
            }
            if (!split.hasRemaining()) {
                activeSplits.pollFirst();
            }
        }
        if (emitted > 0) {
            pauseOutsideCheckpointLock();
        }
    }

    private void pauseOutsideCheckpointLock() throws InterruptedException {
        if (idleSleepMs > 0) {
            Thread.sleep(idleSleepMs);
        }
    }

    @Override
    public List<SavepointPipelineHoldSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(activeSplits);
    }

    @Override
    public void addSplits(List<SavepointPipelineHoldSplit> splits) {
        activeSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
