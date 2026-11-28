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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide gates that let the test own the lifetime of a sink writer.
 *
 * <p>The sink tasks and the test run in the same JVM because the test starts an in-process Zeta
 * cluster, so plain latches and semaphores are enough to freeze one pipeline while the rest of the
 * job keeps running.
 *
 * <p>Two kinds of gates exist:
 *
 * <ul>
 *   <li>A <b>hold gate</b> is closed when it is created: a writer that observes it blocks in {@link
 *       #await(String)} before its first row until the test calls {@link #release(String)}.
 *   <li>A <b>progress gate</b> starts without permits: a writer that observes it calls {@link
 *       #acquirePermit(String)} before every row, so it only advances one row per {@link
 *       #grantPermits(String, int)} and then parks again. This is how the test keeps a pipeline
 *       RUNNING without depending on any finite sleep budget of its workload.
 * </ul>
 *
 * <p>Both kinds are owned by the test: {@link #releaseAll()} opens every existing hold gate and
 * grants the parkers of every progress gate enough permits to leave, so a failed assertion cannot
 * leave a writer parked forever.
 */
public final class PipelineFinishSlotReleaseGate {

    private static final ConcurrentMap<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, AtomicLong> WAITING = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Semaphore> PERMITS = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, AtomicLong> PERMIT_WAITING =
            new ConcurrentHashMap<>();

    /** Enough permits to let every parked writer advance past the end of its workload. */
    private static final int RELEASE_ALL_PERMITS = 1_000_000;

    private PipelineFinishSlotReleaseGate() {}

    /** Removes every gate so a repeated run starts with closed latches and empty permits. */
    public static void reset() {
        GATES.clear();
        WAITING.clear();
        PERMITS.clear();
        PERMIT_WAITING.clear();
    }

    /**
     * Opens every hold gate created so far and grants every progress gate enough permits for a
     * parked writer to run to its end. Called from the test's {@code finally} so cleanup never
     * depends on a writer being interrupted by the engine.
     */
    public static void releaseAll() {
        GATES.values().forEach(CountDownLatch::countDown);
        PERMITS.values().forEach(permits -> permits.release(RELEASE_ALL_PERMITS));
    }

    /** Number of writers currently parked on the hold gate {@code key}. */
    public static long waiting(String key) {
        AtomicLong waiting = WAITING.get(key);
        return waiting == null ? 0L : waiting.get();
    }

    /** Number of writers currently parked waiting for a progress permit of {@code key}. */
    public static long permitWaiting(String key) {
        AtomicLong waiting = PERMIT_WAITING.get(key);
        return waiting == null ? 0L : waiting.get();
    }

    /**
     * Blocks the calling writer until the test releases the hold gate.
     *
     * @param key gate key from the sink options
     */
    public static void await(String key) throws IOException {
        CountDownLatch latch = GATES.computeIfAbsent(key, ignored -> new CountDownLatch(1));
        WAITING.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for gate " + key, e);
        } finally {
            AtomicLong waiting = WAITING.get(key);
            if (waiting != null) {
                waiting.decrementAndGet();
            }
        }
    }

    /** Opens the hold gate for {@code key}; writers already blocked proceed. */
    public static void release(String key) {
        GATES.computeIfAbsent(key, ignored -> new CountDownLatch(1)).countDown();
    }

    /**
     * Blocks the calling writer until the test grants a progress permit for {@code key}. One permit
     * maps to exactly one row, so the test decides how many rows the writer may still write.
     *
     * @param key progress gate key from the sink options
     */
    public static void acquirePermit(String key) throws IOException {
        Semaphore permits = PERMITS.computeIfAbsent(key, ignored -> new Semaphore(0));
        PERMIT_WAITING.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
        try {
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for a progress permit of " + key, e);
        } finally {
            AtomicLong waiting = PERMIT_WAITING.get(key);
            if (waiting != null) {
                waiting.decrementAndGet();
            }
        }
    }

    /** Grants {@code count} progress permits to {@code key}; each permit allows one more row. */
    public static void grantPermits(String key, int count) {
        PERMITS.computeIfAbsent(key, ignored -> new Semaphore(0)).release(count);
    }
}
