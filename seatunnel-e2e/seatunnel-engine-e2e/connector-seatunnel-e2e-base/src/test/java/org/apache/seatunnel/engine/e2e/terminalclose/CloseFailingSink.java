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

package org.apache.seatunnel.engine.e2e.terminalclose;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only batch sink whose writer fails exactly at the terminal close handshake.
 *
 * <p>Every row is accepted, so the pipeline only fails once the source has finished and the engine
 * asks the writer to close. {@link SinkWriter#close()} always throws, which makes the failure
 * happen in the last phase of the task lifecycle, after all records were written. The job config
 * does not set {@code checkpoint.interval}, so the engine disables periodic checkpointing for this
 * BATCH job and the terminal close is the only remaining failure point. The fixture never fails
 * earlier, so a job that reports {@code FINISHED} here provably ignored a failed terminal close.
 *
 * <p>The Zeta cluster of the engine E2E tests runs inside the test JVM, so these static counters
 * are shared between the test and the task threads.
 */
public class CloseFailingSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    /** Message carried by the close failure, asserted on the client-visible job result. */
    public static final String CLOSE_FAILURE_MESSAGE = "sink close failed at terminal handshake";

    private static final AtomicLong RECEIVED_ROWS = new AtomicLong();

    private static final AtomicInteger CLOSE_CALLS = new AtomicInteger();

    private static final AtomicInteger CLOSE_FAILURES = new AtomicInteger();

    private static final AtomicInteger WRITER_CREATIONS = new AtomicInteger();

    public static void reset() {
        RECEIVED_ROWS.set(0L);
        CLOSE_CALLS.set(0);
        CLOSE_FAILURES.set(0);
        WRITER_CREATIONS.set(0);
    }

    public static long receivedRows() {
        return RECEIVED_ROWS.get();
    }

    public static int closeCalls() {
        return CLOSE_CALLS.get();
    }

    public static int closeFailures() {
        return CLOSE_FAILURES.get();
    }

    /** Counts writer creations, so the test can prove the pipeline was deployed only once. */
    public static int writerCreations() {
        return WRITER_CREATIONS.get();
    }

    @Override
    public String getPluginName() {
        return CloseFailingSinkFactory.IDENTIFIER;
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        WRITER_CREATIONS.incrementAndGet();
        return new FailingCloseWriter();
    }

    private static class FailingCloseWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

        @Override
        public void write(SeaTunnelRow element) {
            RECEIVED_ROWS.incrementAndGet();
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        /**
         * Fails every close attempt. The engine calls close once from the task state machine and
         * may call it again from the task cleanup path; both attempts must leave the job terminal
         * and must never turn the job into a success.
         */
        @Override
        public void close() throws IOException {
            CLOSE_CALLS.incrementAndGet();
            CLOSE_FAILURES.incrementAndGet();
            throw new IOException(
                    CLOSE_FAILURE_MESSAGE + " after " + RECEIVED_ROWS.get() + " rows");
        }
    }
}
