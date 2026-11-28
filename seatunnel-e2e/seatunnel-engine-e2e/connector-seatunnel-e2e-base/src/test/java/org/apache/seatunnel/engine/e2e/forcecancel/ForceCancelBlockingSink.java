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

package org.apache.seatunnel.engine.e2e.forcecancel;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only streaming sink whose writer deliberately ignores the cooperative cancellation
 * handshake.
 *
 * <p>Once the test calls {@link #arm()}, the very next {@link SinkWriter#write} call parks the task
 * thread in {@link #awaitRelease()} and swallows every {@link InterruptedException} thrown by the
 * interrupt-based cancel notice, so the task group can never observe {@code cancelRequested} and
 * can never complete its {@code taskFuture}. A graceful cancel therefore has no way to finish this
 * job; only the force-stop path, which marks the vertex/state terminal without waiting for the task
 * acknowledgement, can resolve it.
 *
 * <p>Because the Zeta cluster of the engine E2E tests runs inside the test JVM, the static fields
 * are the observed state shared between the test and the task thread. {@link #release()} is always
 * called from the test's {@code finally} block so the parked task can terminate and the in-process
 * node can shut down.
 */
public class ForceCancelBlockingSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    /** Rows handed to the writer, used by the test to prove the job is actually processing. */
    private static final AtomicLong RECEIVED_ROWS = new AtomicLong();

    /** Number of interrupted sleeps observed while parked, i.e. cooperative cancellations sent. */
    private static final AtomicInteger CANCEL_INTERRUPTS = new AtomicInteger();

    private static final AtomicBoolean ARMED = new AtomicBoolean(false);

    private static final AtomicBoolean RELEASED = new AtomicBoolean(false);

    private static volatile CountDownLatch blockedInWrite = new CountDownLatch(1);

    public static void reset() {
        RECEIVED_ROWS.set(0L);
        CANCEL_INTERRUPTS.set(0);
        ARMED.set(false);
        RELEASED.set(false);
        blockedInWrite = new CountDownLatch(1);
    }

    /** Makes the next write park until {@link #release()} is called. */
    public static void arm() {
        ARMED.set(true);
    }

    /** Lets the parked writer continue so the task can finish and the node can shut down. */
    public static void release() {
        RELEASED.set(true);
    }

    public static long receivedRows() {
        return RECEIVED_ROWS.get();
    }

    public static int cancelInterrupts() {
        return CANCEL_INTERRUPTS.get();
    }

    /** True once a writer has parked inside {@code write} while armed. */
    public static boolean isBlockedInWrite() {
        return blockedInWrite.getCount() == 0L;
    }

    public static boolean isReleased() {
        return RELEASED.get();
    }

    private static void awaitRelease() {
        blockedInWrite.countDown();
        while (!RELEASED.get()) {
            try {
                Thread.sleep(25L);
            } catch (InterruptedException e) {
                // Deliberately swallow the interrupt: the whole point of this fixture is a task
                // that refuses the cooperative cancel handshake.
                CANCEL_INTERRUPTS.incrementAndGet();
            }
        }
    }

    @Override
    public String getPluginName() {
        return ForceCancelBlockingSinkFactory.IDENTIFIER;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new BlockingWriter();
    }

    private static class BlockingWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            RECEIVED_ROWS.incrementAndGet();
            if (ARMED.get()) {
                awaitRelease();
            }
        }

        @Override
        public Optional<Void> prepareCommit() throws IOException {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}
    }
}
