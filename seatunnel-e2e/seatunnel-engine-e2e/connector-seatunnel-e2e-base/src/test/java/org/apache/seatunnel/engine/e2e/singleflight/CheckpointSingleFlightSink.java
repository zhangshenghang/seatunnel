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

package org.apache.seatunnel.engine.e2e.singleflight;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-only sink whose writer parks inside {@link SinkWriter#snapshotState(long)} for the first
 * checkpoint it is asked to take.
 *
 * <p>The Zeta cluster of the engine E2E tests runs inside the test JVM, so the static bookkeeping
 * in this class lets the test observe, without any timing guesswork:
 *
 * <ul>
 *   <li>which checkpoint ids the coordinator actually asked this task to snapshot, in order;
 *   <li>the exact moment the first snapshot started, so the test can observe the coordinator while
 *       that checkpoint is provably still in flight;
 *   <li>that no second snapshot request arrives until the test releases the first one.
 * </ul>
 *
 * <p>The held snapshot is the only slow part; every later snapshot returns immediately, so the
 * checkpoint chain can be observed recovering after the release. This is a fixture only: it does
 * not emulate any connector behaviour, it just makes the checkpoint barrier observable and
 * holdable.
 */
public class CheckpointSingleFlightSink implements SeaTunnelSink<SeaTunnelRow, String, Void, Void> {

    /** Upper bound for the hold so a broken test cannot keep the JVM's task thread forever. */
    private static final long HOLD_TIMEOUT_MINUTES = 2L;

    private static final Object GATE_LOCK = new Object();

    /** Checkpoint ids this sink was asked to snapshot, in call order. */
    private static final List<Long> SNAPSHOT_IDS = new CopyOnWriteArrayList<>();

    private static volatile Gate gate = new Gate();

    /** Resets the fixture before a test submits a job. */
    public static void reset() {
        SNAPSHOT_IDS.clear();
        synchronized (GATE_LOCK) {
            gate = new Gate();
        }
    }

    /** Returns the checkpoint ids this sink was asked to snapshot, in call order. */
    public static List<Long> snapshotIds() {
        return new CopyOnWriteArrayList<>(SNAPSHOT_IDS);
    }

    /** Returns the number of snapshot requests received so far. */
    public static int snapshotCallCount() {
        return SNAPSHOT_IDS.size();
    }

    /** Waits until the writer has parked inside the first snapshot. */
    public static boolean awaitFirstSnapshot(long timeout, TimeUnit unit) throws Exception {
        return gate.firstSnapshotEntered.await(timeout, unit);
    }

    /** Releases the parked first snapshot so the checkpoint can complete. */
    public static void releaseFirstSnapshot() {
        gate.release.countDown();
    }

    @Override
    public String getPluginName() {
        return CheckpointSingleFlightSinkFactory.IDENTIFIER;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, String> createWriter(SinkWriter.Context context) {
        return new HoldingWriter();
    }

    private static final class Gate {
        private final CountDownLatch firstSnapshotEntered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean holdNextSnapshot = new AtomicBoolean(true);
    }

    private static final class HoldingWriter implements SinkWriter<SeaTunnelRow, Void, String> {

        @Override
        public void write(SeaTunnelRow element) {
            // Rows are irrelevant to the checkpoint scheduling contract under test.
        }

        @Override
        public List<String> snapshotState(long checkpointId) throws IOException {
            SNAPSHOT_IDS.add(checkpointId);
            Gate currentGate = gate;
            if (currentGate.holdNextSnapshot.compareAndSet(true, false)) {
                currentGate.firstSnapshotEntered.countDown();
                try {
                    if (!currentGate.release.await(HOLD_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                        throw new IOException(
                                "Timed out waiting for the test to release checkpoint "
                                        + checkpointId);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while holding checkpoint " + checkpointId, e);
                }
            }
            return Collections.singletonList("single-flight-state-" + checkpointId);
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }
}
