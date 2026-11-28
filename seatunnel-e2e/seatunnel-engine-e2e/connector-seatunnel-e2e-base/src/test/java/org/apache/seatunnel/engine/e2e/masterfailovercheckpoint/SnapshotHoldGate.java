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

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only synchronization point that keeps a sink writer blocked inside {@code snapshotState}
 * until the test releases it.
 *
 * <p>The engine E2E cluster runs inside the test JVM, so a static registry keyed by a per-test hold
 * key is enough to coordinate the test thread with the task thread that owns the writer. Once the
 * gate is armed every {@code snapshotState} call parks before returning its state, so the
 * checkpoint barrier reaches all tasks and is acknowledged by none of the parked writers: the
 * checkpoint is provably in flight on the active master while the barrier cannot complete.
 *
 * <p>The gate is armed only after the job completed its first checkpoint. That guarantees the
 * pipeline had a real checkpoint sequence to continue from before the failover, without holding the
 * very first checkpoint the coordinator ever triggers.
 *
 * <p>An interrupt delivered while holding (for example from a worker teardown) does not end the
 * hold, exactly like a task thread that keeps running across a master switch. A hard deadline makes
 * sure that a broken test can never leak a task thread forever.
 */
@Slf4j
public final class SnapshotHoldGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(5);

    /** Armed gates keyed by hold key; a missing entry means "never hold". */
    private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    /** Number of writers currently blocked inside snapshotState per hold key. */
    private static final Map<String, AtomicInteger> HOLDING_WRITERS = new ConcurrentHashMap<>();

    /** Number of snapshotState entries observed per hold key. */
    private static final Map<String, AtomicInteger> SNAPSHOT_CALLS = new ConcurrentHashMap<>();

    private SnapshotHoldGate() {}

    /**
     * Arms the gate: every snapshotState call that uses this key blocks until {@link
     * #release(String)}.
     *
     * @param holdKey unique key shared between the test and the sink configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new CountDownLatch(1));
        HOLDING_WRITERS.put(holdKey, new AtomicInteger());
    }

    /**
     * Releases every writer currently blocked for the key and lets later snapshots pass through
     * immediately. Safe to call repeatedly and for keys that were never armed.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void release(String holdKey) {
        CountDownLatch latch = GATES.get(holdKey);
        if (latch != null) {
            latch.countDown();
        }
    }

    /**
     * Releases and forgets the key. Intended for test teardown so that a rerun with the same key
     * can start from a clean registry.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void clear(String holdKey) {
        release(holdKey);
        GATES.remove(holdKey);
        HOLDING_WRITERS.remove(holdKey);
        SNAPSHOT_CALLS.remove(holdKey);
    }

    /**
     * Returns how many writers are blocked inside snapshotState for the key right now.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of held writers, zero when the key is unknown
     */
    public static int holdingWriters(String holdKey) {
        AtomicInteger counter = HOLDING_WRITERS.get(holdKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns how many snapshotState entries this writer family observed for the key.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of observed snapshot requests, zero when the key is unknown
     */
    public static int snapshotCalls(String holdKey) {
        AtomicInteger counter = SNAPSHOT_CALLS.get(holdKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Records that a writer is about to take a snapshot and blocks until the key is released.
     * Returns immediately when the key was never armed or was already released, so the writer of a
     * redeployed pipeline never holds a checkpoint again after the test resumed it.
     *
     * @param holdKey the key configured on the sink
     */
    static void awaitSnapshot(String holdKey) {
        SNAPSHOT_CALLS.computeIfAbsent(holdKey, key -> new AtomicInteger()).incrementAndGet();
        CountDownLatch latch = GATES.get(holdKey);
        if (latch == null) {
            return;
        }
        AtomicInteger counter =
                HOLDING_WRITERS.computeIfAbsent(holdKey, key -> new AtomicInteger());
        counter.incrementAndGet();
        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn(
                            "Hold key {} was not released within {} ms, giving up the hold",
                            holdKey,
                            MAX_HOLD_MILLIS);
                    return;
                }
                try {
                    if (latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                } catch (InterruptedException interruptedException) {
                    // Keep holding across a worker teardown: from the master's point of view the
                    // barrier must stay unacknowledged until the test releases it.
                    interrupted = true;
                }
            }
        } finally {
            counter.decrementAndGet();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
