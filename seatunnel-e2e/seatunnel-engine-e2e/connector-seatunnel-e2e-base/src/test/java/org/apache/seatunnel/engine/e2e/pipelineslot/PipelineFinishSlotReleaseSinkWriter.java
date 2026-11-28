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

package org.apache.seatunnel.engine.e2e.pipelineslot;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer of {@link PipelineFinishSlotReleaseSink}.
 *
 * <p>It has no side effect other than the per-tag row counter and the test-owned gates, so a parked
 * writer holds exactly one pipeline of the job, never the whole cluster. The optional hold gate
 * blocks before the first row; the optional progress gate requires one permit per row, so the
 * writer can only advance as far as the test allows and then parks again. On interruption (the
 * engine cancels a task) the wait is abandoned and the interrupt flag is restored so cancellation
 * stays cooperative.
 */
@Slf4j
public class PipelineFinishSlotReleaseSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final String tag;

    private final String holdGate;

    private final String progressGate;

    private final AtomicLong counter;

    private boolean gateReleased;

    public PipelineFinishSlotReleaseSinkWriter(String tag, String holdGate, String progressGate) {
        this.tag = tag;
        this.holdGate = holdGate;
        this.progressGate = progressGate;
        this.counter = PipelineFinishSlotReleaseSink.counterFor(tag);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (holdGate != null && !gateReleased) {
            log.warn("Writer {} parks before its first row on gate {}", tag, holdGate);
            PipelineFinishSlotReleaseGate.await(holdGate);
            gateReleased = true;
            log.warn("Writer {} left gate {}", tag, holdGate);
        }
        if (progressGate != null) {
            // One permit per row: without a permit the writer stays parked, so the pipeline cannot
            // finish until the test grants the row.
            PipelineFinishSlotReleaseGate.acquirePermit(progressGate);
        }
        counter.incrementAndGet();
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void close() {
        // Nothing to flush: the sink keeps no state and no external resource.
    }
}
