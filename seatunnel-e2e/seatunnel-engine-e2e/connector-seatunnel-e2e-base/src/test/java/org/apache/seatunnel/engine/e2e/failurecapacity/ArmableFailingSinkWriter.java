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

package org.apache.seatunnel.engine.e2e.failurecapacity;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sink writer whose failure is controlled by the test.
 *
 * <p>{@link #WRITER_INSTANCES} counts how many sink writers the engine has created for the job,
 * i.e. how many times the pipeline was (re)deployed, which is the observable retry budget. While
 * {@link #ARMED} is {@code false} the writer accepts rows and checkpoints so the pipeline can reach
 * a steady RUNNING state and complete real checkpoints; once the test arms it, the writer fails the
 * next lifecycle event it processes.
 *
 * <p><b>Arm/reset contract and per-deployment injection.</b> {@link #ARMED} is test-global, so the
 * test must call {@link #reset()} before submitting the job and again in its {@code finally} block
 * after every job it submitted has been stopped. The failure is injected from two places so it does
 * not depend on the source still producing rows: {@link #write(SeaTunnelRow)} fails a row when rows
 * are still arriving, and {@link #prepareCommit()} fails the checkpoint that every deployment keeps
 * receiving for the whole life of a streaming job. The second gate is what makes the failure
 * deterministic: the {@code FakeSource} fixture is finite, so after its rows are drained there may
 * be nothing left for {@code write()} to reject, while checkpoint barriers keep reaching the sink,
 * and a restored attempt fails again on its first post-restore checkpoint. A checkpoint failure is
 * an ordinary task failure, so it consumes the pipeline retry budget like a write failure does.
 */
public class ArmableFailingSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    public static final AtomicBoolean ARMED = new AtomicBoolean(false);
    public static final AtomicInteger WRITER_INSTANCES = new AtomicInteger();
    public static final AtomicInteger WRITTEN_ROWS = new AtomicInteger();

    private final int writerInstance = WRITER_INSTANCES.incrementAndGet();

    @Override
    public void write(SeaTunnelRow element) {
        if (ARMED.get()) {
            throw new IllegalStateException(
                    "armable-failing-sink: injected failure in writer instance " + writerInstance);
        }
        WRITTEN_ROWS.incrementAndGet();
    }

    /**
     * Fails the checkpoint of this deployment once the test has armed the writer.
     *
     * <p>Checkpoint barriers keep arriving for the whole life of a streaming job even when its
     * finite source no longer produces rows, so arming the writer always produces a new task
     * failure: on the current attempt and again on every restored attempt.
     */
    @Override
    public Optional<Void> prepareCommit() {
        if (ARMED.get()) {
            throw new IllegalStateException(
                    "armable-failing-sink: injected checkpoint failure in writer instance "
                            + writerInstance);
        }
        return Optional.empty();
    }

    @Override
    public void close() {}

    public static void reset() {
        ARMED.set(false);
        WRITER_INSTANCES.set(0);
        WRITTEN_ROWS.set(0);
    }
}
